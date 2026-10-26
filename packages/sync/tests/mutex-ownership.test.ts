import { describe, expect, spyOn, test } from "bun:test";
import { headers } from "@nats-io/nats-core";
import { createMutex } from "../src/mutex.ts";
import { createRuntime } from "../src/runtime.ts";
import type { SyncEvent } from "../src/events.ts";
import { encodeJson } from "../src/codec.ts";
import { connectToCluster } from "./cluster.ts";
import { cleanupNamespaces, testNamespace } from "./helpers.ts";

const fixture = async () => {
  const namespace = testNamespace();
  const nc = await connectToCluster({ name: "mutex-ownership" });
  const events: SyncEvent[] = [];
  const runtime = createRuntime({ connection: nc, namespace, application: "tests", observe: (event) => { events.push(event); } });
  const mutex = createMutex(runtime, { id: "ownership", ttlMs: 60_000, retry: { maxAttempts: 1 } });
  await runtime.ready();
  const ctx = await runtime.context();
  const stream = (await runtime.resources())[0]!.natsNames[0]!;
  const subject = `$KV.${stream.slice(3)}.r.${Buffer.from("shared").toString("base64url")}`;
  const read = ctx.jsm.streams.getMessage.bind(ctx.jsm.streams);
  const publish = ctx.js.publish.bind(ctx.js);
  const request = nc.request.bind(nc);
  const clock = spyOn(Date, "now").mockReturnValue(Date.UTC(2026, 9, 26));
  const readSpy = spyOn(ctx.jsm.streams, "getMessage");
  const publishSpy = spyOn(ctx.js, "publish");
  return {
    namespace, nc, runtime, mutex, stream, subject, readSpy, publishSpy, events,
    loss: () => events.filter((event) => event.type === "lock_lost").at(-1)?.detail,
    current: () => read(stream, { last_by_subj: subject }),
    publish,
    write: async (ownerToken: string, expiresAt: string) => {
      const hdrs = headers();
      hdrs.set("Nats-TTL", "60s");
      return publish(subject, encodeJson("test lease", { ownerToken, expiresAt }, 4_096), { headers: hdrs });
    },
    restore: () => { readSpy.mockRestore(); publishSpy.mockRestore(); clock.mockRestore(); nc.request = request; },
  };
};
const withMutex = async (run: (f: Awaited<ReturnType<typeof fixture>>) => Promise<void>) => {
  const f = await fixture();
  try { await run(f); } finally {
    f.restore();
    await f.runtime.drain({ timeoutMs: 2_000 });
    await cleanupNamespaces(f.nc, [f.namespace]);
    await f.nc.close();
  }
};

describe("authoritative mutex ownership on the three-node cluster", () => {
  test("stale read after acknowledged extension retries without losing the lease", () => withMutex(async (f) => {
    const lock = (await f.mutex.acquire({ resource: "shared" }))!;
    const stale = await f.current();
    expect(await f.mutex.extend(lock)).toBe(true);
    const acknowledged = await f.current();
    expect(acknowledged.seq).toBeGreaterThan(stale.seq);
    f.readSpy.mockResolvedValueOnce(stale);
    const calls = f.readSpy.mock.calls.length;
    expect(await f.mutex.extend(lock)).toBe(true);
    expect(f.readSpy.mock.calls.length - calls).toBe(2);
    expect(lock.fence).toBe(BigInt(stale.seq));
    expect((await f.current()).seq).toBeGreaterThan(acknowledged.seq);
    expect(f.loss()).toBeUndefined();
    expect(await f.mutex.release(lock)).toBe(true);
  }));
  test("acquire, extend and release bypass direct reads and retain delete revisions", () => withMutex(async (f) => {
    const request = f.nc.request.bind(f.nc);
    f.nc.request = async (subject, payload, options) => {
      if (subject.includes(".DIRECT.GET.")) throw new Error("ownership used a direct read");
      return request(subject, payload, options);
    };
    const lock = (await f.mutex.acquire({ resource: "shared" }))!;
    expect(lock).not.toBeNull();
    expect(await f.mutex.acquire({ resource: "shared" })).toBeNull();
    expect(await f.mutex.extend(lock)).toBe(true);
    expect(await f.mutex.release(lock)).toBe(true);
    const next = (await f.mutex.acquire({ resource: "shared" }))!;
    expect(next.fence).toBeGreaterThan(lock.fence);
    expect(await f.mutex.release(next)).toBe(true);
  }));
  test("replacement owner blocks former owner on extend and release", () => withMutex(async (f) => {
    const old = (await f.mutex.acquire({ resource: "shared" }))!;
    expect(await f.mutex.release(old)).toBe(true);
    const next = (await f.mutex.acquire({ resource: "shared" }))!;
    expect(await f.mutex.extend(old)).toBe(false);
    expect(f.loss()).toMatchObject({ reason: "owner_changed", acquisitionRevision: old.fence.toString(), observedRevision: Number(next.fence), attemptedRevision: null });
    expect(JSON.stringify(f.loss())).not.toContain(old.ownerToken);
    expect(JSON.stringify(f.loss())).not.toContain(next.ownerToken);
    expect(await f.mutex.release(old)).toBe(false);
    expect(await f.mutex.extend(next)).toBe(true);
    expect(await f.mutex.release(next)).toBe(true);
  }));
  test("expired record still on the broker is never revived", () => withMutex(async (f) => {
    const lock = (await f.mutex.acquire({ resource: "shared" }))!;
    const expired = await f.write(lock.ownerToken, "2000-01-01T00:00:00.000Z");
    expect(await f.mutex.extend(lock)).toBe(false);
    expect(f.loss()).toMatchObject({ reason: "expired", observedRevision: expired.seq, attemptedRevision: null });
    expect((await f.current()).seq).toBe(expired.seq);
  }));
  test("TTL markers report missing and retain their revision for acquisition", () => withMutex(async (f) => {
    const lock = (await f.mutex.acquire({ resource: "shared" }))!;
    const hdrs = headers();
    hdrs.set("Nats-Marker-Reason", "MaxAge");
    const marker = await f.publish(f.subject, new Uint8Array(), { headers: hdrs });
    expect(await f.mutex.extend(lock)).toBe(false);
    expect(f.loss()).toMatchObject({ reason: "missing", observedRevision: marker.seq });
    const next = (await f.mutex.acquire({ resource: "shared" }))!;
    expect(next.fence).toBeGreaterThan(lock.fence);
    expect(await f.mutex.release(next)).toBe(true);
  }));
  test("three concurrent extensions retry real CAS races and preserve fencing", () => withMutex(async (f) => {
    const lock = (await f.mutex.acquire({ resource: "shared" }))!;
    const initial = await f.current();
    for (let i = 0; i < 3; i++) f.readSpy.mockResolvedValueOnce(initial);
    expect(await Promise.all([f.mutex.extend(lock), f.mutex.extend({ ...lock }), f.mutex.extend({ ...lock })])).toEqual([true, true, true]);
    expect(lock.fence).toBe(BigInt(initial.seq));
    expect((await f.current()).seq).toBe(initial.seq + 3);
    expect(f.loss()).toBeUndefined();
    expect(await f.mutex.acquire({ resource: "shared" })).toBeNull();
    expect(await f.mutex.release(lock)).toBe(true);
  }));
  test("persistent contention stops at three writes and reports revisions", () => withMutex(async (f) => {
    const lock = (await f.mutex.acquire({ resource: "shared" }))!;
    let attempts = 0;
    f.publishSpy.mockImplementation(async (subject, data, options) => {
      if (subject === f.subject) {
        attempts++;
        await f.write(lock.ownerToken, "2099-01-01T00:00:00.000Z");
      }
      return f.publish(subject, data, options);
    });
    expect(await f.mutex.extend(lock)).toBe(false);
    expect(attempts).toBe(3);
    const latest = await f.current();
    expect(f.loss()).toEqual({ lock: "shared", reason: "revision_conflict", acquisitionRevision: lock.fence.toString(), attemptedRevision: latest.seq - 1, observedRevision: latest.seq });
    expect(lock.fence).toBe(BigInt(latest.seq - 3));
  }));
  test.each(["owner_changed", "expired"] as const)("CAS retry stops at %s", (reason) => withMutex(async (f) => {
    const lock = (await f.mutex.acquire({ resource: "shared" }))!;
    let attempts = 0;
    f.publishSpy.mockImplementation(async (subject, data, options) => {
      if (subject === f.subject) {
        attempts++;
        await f.write(reason === "owner_changed" ? "replacement" : lock.ownerToken,
          reason === "expired" ? "2000-01-01T00:00:00.000Z" : "2099-01-01T00:00:00.000Z");
      }
      return f.publish(subject, data, options);
    });
    expect(await f.mutex.extend(lock)).toBe(false);
    expect(attempts).toBe(1);
    expect(f.loss()?.reason).toBe(reason);
    expect(await f.mutex.release({ ...lock, ownerToken: "not-the-owner" })).toBe(false);
  }));
});
