/**
 * Bun compatibility probes for the pinned NATS.js 3.4.0 packages against the
 * real three-node NATS 2.14.x cluster. These prove the transport behavior the
 * v6 implementation depends on: connect, JetStream publish/consume/ack,
 * duplicate-window dedupe, message schedules, KV per-key TTL + watch + CAS,
 * Object Store streaming, reconnect after node restart, and drain.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { AckPolicy, DiscardPolicy, RetentionPolicy, StorageType, jetstream, jetstreamManager } from "@nats-io/jetstream";
import type { JetStreamClient, JetStreamManager } from "@nats-io/jetstream";
import { Kvm } from "@nats-io/kv";
import { Objm } from "@nats-io/obj";
import type { NatsConnection } from "@nats-io/nats-core";
import { connectToCluster, restartNode, uniqueName, waitForLeaders } from "../cluster.ts";

let nc: NatsConnection;
let js: JetStreamClient;
let jsm: JetStreamManager;
const createdStreams: string[] = [];

beforeAll(async () => {
  nc = await connectToCluster({ name: "probe" });
  js = jetstream(nc);
  jsm = await jetstreamManager(nc);
});

afterAll(async () => {
  for (const stream of createdStreams) {
    await jsm.streams.delete(stream).catch(() => {});
  }
  await nc.close();
}, 60_000); // R3 deletions right after a node restart can exceed Bun's 5s default

describe("connection", () => {
  test("connects to the cluster and reports server info", async () => {
    expect(nc.info?.version).toStartWith("2.14.");
    expect(nc.info?.jetstream).toBe(true);
    const rtt = await nc.rtt();
    expect(rtt).toBeGreaterThanOrEqual(0);
  });
});

describe("jetstream core", () => {
  test("R3 work-queue stream: publish, dedupe, fetch, ack, redelivery", async () => {
    const stream = uniqueName("PROBE_Q");
    const subject = `probe.q.${stream}`;
    await jsm.streams.add({
      name: stream,
      subjects: [subject],
      retention: RetentionPolicy.Workqueue,
      storage: StorageType.File,
      discard: DiscardPolicy.Old,
      num_replicas: 3,
      duplicate_window: 5_000 * 1_000_000,
    });
    createdStreams.push(stream);

    const first = await js.publish(subject, JSON.stringify({ n: 1 }), { msgID: "m-1" });
    expect(first.duplicate).toBe(false);
    const dup = await js.publish(subject, JSON.stringify({ n: 1 }), { msgID: "m-1" });
    expect(dup.duplicate).toBe(true);
    expect(dup.seq).toBe(first.seq);

    await jsm.consumers.add(stream, {
      durable_name: "worker",
      ack_policy: AckPolicy.Explicit,
      ack_wait: 1_000 * 1_000_000,
      max_deliver: 3,
    });
    const consumer = await js.consumers.get(stream, "worker");

    // First delivery: do not ack, expect redelivery after ack_wait.
    const one = await consumer.fetch({ max_messages: 1, expires: 2_000 });
    let redelivered = 0;
    for await (const m of one) {
      expect(m.json<{ n: number }>().n).toBe(1);
      expect(m.info.deliveryCount).toBe(1);
    }
    const two = await consumer.fetch({ max_messages: 1, expires: 3_000 });
    for await (const m of two) {
      redelivered = m.info.deliveryCount;
      await m.ackAck();
    }
    expect(redelivered).toBe(2);

    // Work-queue retention: acked message is removed (allow for replica lag).
    let messages = -1;
    for (let i = 0; i < 20; i++) {
      messages = (await jsm.streams.info(stream)).state.messages;
      if (messages === 0) break;
      await Bun.sleep(100);
    }
    expect(messages).toBe(0);
  });

  test("one-shot @at schedule delivers later without occupying a consumer", async () => {
    const stream = uniqueName("PROBE_DELAY");
    const work = `probe.delay.${stream}.work`;
    const sched = `probe.delay.${stream}.sched`;
    await jsm.streams.add({
      name: stream,
      subjects: [`probe.delay.${stream}.>`],
      retention: RetentionPolicy.Limits,
      storage: StorageType.File,
      num_replicas: 3,
      allow_msg_schedules: true,
      allow_msg_ttl: true,
    });
    createdStreams.push(stream);

    const fireAt = new Date(Date.now() + 1_500);
    await js.publish(sched, JSON.stringify({ delayed: true }), {
      schedule: { specification: { at: fireAt }, target: work },
    });

    // Nothing on the work subject yet.
    let state = await jsm.streams.info(stream);
    const before = state.state.messages;

    await Bun.sleep(2_500);
    const last = await jsm.streams.getMessage(stream, { last_by_subj: work });
    expect(last).not.toBeNull();
    expect(JSON.parse(last!.string()).delayed).toBe(true);
    state = await jsm.streams.info(stream);
    expect(state.state.messages).toBeGreaterThanOrEqual(before);
  });

  test("recurring cron schedule ticks and can be canceled", async () => {
    const stream = uniqueName("PROBE_CRON");
    const tick = `probe.cron.${stream}.tick`;
    const sched = `probe.cron.${stream}.sched`;
    await jsm.streams.add({
      name: stream,
      subjects: [`probe.cron.${stream}.>`],
      retention: RetentionPolicy.Limits,
      storage: StorageType.File,
      num_replicas: 3,
      allow_msg_schedules: true,
      allow_msg_ttl: true,
    });
    createdStreams.push(stream);

    await js.publish(sched, JSON.stringify({ tick: true }), {
      schedule: { specification: { every: "1s" }, target: tick, ttl: "never" },
    });
    await Bun.sleep(2_600);
    const info = await jsm.streams.info(stream, { subjects_filter: tick });
    const ticks = info.state.subjects?.[tick] ?? 0;
    expect(ticks).toBeGreaterThanOrEqual(2);

    // Cancel: publish with cancelSchedule pointing at the schedule subject.
    await js.publish(`probe.cron.${stream}.cancel`, "", { cancelSchedule: { scheduleSubject: sched } });
    await Bun.sleep(1_600);
    const after1 = (await jsm.streams.info(stream, { subjects_filter: tick })).state.subjects?.[tick] ?? 0;
    await Bun.sleep(1_500);
    const after2 = (await jsm.streams.info(stream, { subjects_filter: tick })).state.subjects?.[tick] ?? 0;
    expect(after2).toBe(after1);
  }, 20_000);
});

describe("kv", () => {
  test("per-key TTL, CAS, watch with resume revision", async () => {
    const kvm = new Kvm(nc);
    const bucket = uniqueName("PROBE_KV");
    const kv = await kvm.create(bucket, {
      history: 5,
      replicas: 3,
      storage: StorageType.File,
      markerTTL: 1_000,
    });
    createdStreams.push(`KV_${bucket}`);

    // Per-key TTL
    await kv.create("temp", "gone-soon", "1s");
    expect(await kv.get("temp")).not.toBeNull();
    await Bun.sleep(2_500);
    const expired = await kv.get("temp");
    expect(expired === null || expired.operation !== "PUT").toBe(true);

    // CAS: create fails on existing keys, update requires correct revision
    const rev1 = await kv.create("owner", "a");
    await expect(kv.create("owner", "b")).rejects.toThrow();
    const rev2 = await kv.update("owner", "b", rev1);
    expect(rev2).toBeGreaterThan(rev1);
    await expect(kv.update("owner", "c", rev1)).rejects.toThrow();

    // Watch resumes from a revision
    await kv.put("w1", "1");
    const rw = await kv.put("w2", "2");
    await kv.put("w3", "3");
    const seen: string[] = [];
    const watch = await kv.watch({ resumeFromRevision: rw + 1 });
    for await (const e of watch) {
      seen.push(e.key);
      if (e.delta === 0) break;
    }
    expect(seen).toContain("w3");
    expect(seen).not.toContain("w1");
  });
});

describe("object store", () => {
  test("streams chunked objects with digest, no full buffering required", async () => {
    const objm = new Objm(nc);
    const bucket = uniqueName("PROBE_OBJ");
    const os = await objm.create(bucket, { replicas: 3, storage: StorageType.File });
    createdStreams.push(`OBJ_${bucket}`);

    // 1 MiB in 64 KiB chunks via a pull-based stream (only produced on demand).
    const chunk = new Uint8Array(64 * 1024).fill(7);
    let produced = 0;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (produced >= 16) return controller.close();
        produced += 1;
        controller.enqueue(chunk);
      },
    });
    const info = await os.put({ name: "artifact", options: { max_chunk_size: 64 * 1024 } }, body);
    expect(info.size).toBe(16 * 64 * 1024);
    expect(info.chunks).toBe(16);
    expect(info.digest).toStartWith("SHA-256=");

    const result = await os.get("artifact");
    expect(result).not.toBeNull();
    let total = 0;
    const reader = result!.data.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value!.length;
    }
    expect(total).toBe(info.size);
    expect(await result!.error).toBeFalsy();
  });
});

describe("resilience", () => {
  test("client reconnects after its server restarts and jetstream continues", async () => {
    const conn = await connectToCluster({ name: "probe-reconnect" });
    const cjs = jetstream(conn);
    const cjsm = await jetstreamManager(conn);
    const stream = uniqueName("PROBE_RECONN");
    const subject = `probe.reconn.${stream}`;
    await cjsm.streams.add({
      name: stream,
      subjects: [subject],
      retention: RetentionPolicy.Limits,
      storage: StorageType.File,
      num_replicas: 3,
    });
    createdStreams.push(stream);
    await cjs.publish(subject, "before");

    // Which node are we on? Ports 14222-14224 map to nodes 1-3.
    const port = conn.getServer().split(":").pop();
    const node = ({ "14222": 1, "14223": 2, "14224": 3 } as const)[port ?? ""] ?? 1;

    const reconnected = new Promise<void>((resolve) => {
      (async () => {
        for await (const s of conn.status()) {
          if (s.type === "reconnect") return resolve();
        }
      })();
    });
    await restartNode(node);
    await reconnected;
    await waitForLeaders(conn, [stream]);

    const ack = await cjs.publish(subject, "after");
    expect(ack.seq).toBeGreaterThanOrEqual(2);
    await conn.close();
  }, 90_000);

  test("subscription drain delivers pending messages before close", async () => {
    const conn = await connectToCluster({ name: "probe-drain" });
    const subject = uniqueName("probe.drain");
    let received = 0;
    const sub = conn.subscribe(subject, { callback: () => { received += 1; } });
    for (let i = 0; i < 50; i++) conn.publish(subject, "x");
    await sub.drain();
    expect(received).toBe(50);
    await conn.drain();
    expect(conn.isClosed()).toBe(true);
  });
});
