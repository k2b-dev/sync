import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { jetstreamManager } from "@nats-io/jetstream";
import type { NatsConnection } from "@nats-io/nats-core";
import { createSync } from "../src/sync.ts";
import type { Sync } from "../src/sync.ts";
import { RetentionGapError, SyncUsageError } from "../src/errors.ts";
import { connectToCluster, uniqueName } from "./cluster.ts";
import { cleanupNamespaces, collect, testNamespace } from "./helpers.ts";

type Event = { tenant: string; n: number };

let nc: NatsConnection;
let sync: Sync;
const namespace = testNamespace();

const config = (id: string, maxBytes = 64 * 1024 * 1024) => ({
  id,
  retention: { maxAgeMs: 60 * 60 * 1_000, maxBytes },
});

const eventStream = async (id: string): Promise<string> => {
  const jsm = await jetstreamManager(nc);
  for await (const info of jsm.streams.list()) {
    const metadata = info.config.metadata;
    if (metadata?.["sync.namespace"] === namespace && metadata["sync.id"] === id && info.config.subjects.some((s) => s.endsWith(".event"))) {
      return info.config.name;
    }
  }
  throw new Error(`event stream of ${id} missing`);
};

beforeAll(async () => {
  nc = await connectToCluster({ name: "topic-tenants-test" });
  sync = createSync({ connection: nc, namespace, application: "tests" });
  await sync.ready();
});

afterAll(async () => {
  await sync.drain({ timeoutMs: 5_000 });
  await cleanupNamespaces(nc, [namespace]);
  await nc.close();
});

describe("tenant-scoped reads on a shared topic", () => {
  test("many interleaved tenants replay only their own events, in order, without false gaps", async () => {
    const topic = sync.topic<Event>(config("interleaved"));
    const tenants = Array.from({ length: 20 }, (_, i) => `note-${i}`);
    for (let n = 1; n <= 10; n++) {
      await Promise.all(tenants.map((tenant) => topic.publish({ tenantId: tenant, data: { tenant, n } })));
    }
    for (const tenant of tenants) {
      const events = await collect(topic.replay({ tenantId: tenant }));
      expect(events.map((e) => e.data.n)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
      expect(events.every((e) => e.tenantId === tenant)).toBe(true);
      // Sequences are strictly increasing but not contiguous: other tenants sit in between.
      expect(events.some((e, i) => i > 0 && e.sequence !== events[i - 1]!.sequence + 1)).toBe(true);
    }
  }, 60_000);

  test("cursor resume per tenant returns exactly the remainder", async () => {
    const topic = sync.topic<Event>(config("resume"));
    const cursors: string[] = [];
    for (let n = 1; n <= 6; n++) {
      cursors.push((await topic.publish({ tenantId: "a", data: { tenant: "a", n } })).cursor);
      await topic.publish({ tenantId: "b", data: { tenant: "b", n } });
    }
    const rest = await collect(topic.replay({ tenantId: "a", after: cursors[2] }));
    expect(rest.map((e) => e.data.n)).toEqual([4, 5, 6]);
    const bounded = await collect(topic.replay({ tenantId: "a", after: cursors[0], until: cursors[3] }));
    expect(bounded.map((e) => e.data.n)).toEqual([2, 3, 4]);
    // A cursor at the tenant head replays nothing and ends immediately.
    expect(await collect(topic.replay({ tenantId: "a", after: cursors[5] }))).toEqual([]);
    // `until` beyond the tenant's last event ends at that event, not at the stream head.
    const head = await topic.head();
    expect((await collect(topic.replay({ tenantId: "a", after: cursors[4], until: head }))).map((e) => e.data.n)).toEqual([6]);
  }, 30_000);

  test("follow tails one tenant while others write", async () => {
    const topic = sync.topic<Event>(config("follow-tenant"));
    const start = await topic.publish({ tenantId: "x", data: { tenant: "x", n: 0 } });
    const controller = new AbortController();
    const followed = collect(topic.follow({ tenantId: "x", after: start.cursor, signal: controller.signal }), 3);
    await Bun.sleep(200);
    for (let n = 1; n <= 3; n++) {
      await topic.publish({ tenantId: "noise", data: { tenant: "noise", n } });
      await topic.publish({ tenantId: "x", data: { tenant: "x", n } });
    }
    expect((await followed).map((e) => e.data.n)).toEqual([1, 2, 3]);
    controller.abort();
  }, 30_000);

  test("front eviction past a tenant cursor is a real gap; resumeAfter continues at the first retained event", async () => {
    const topic = sync.topic<Event>(config("front-gap"));
    const kept = await topic.publish({ tenantId: "t", data: { tenant: "t", n: 1 } });
    await topic.publish({ tenantId: "t", data: { tenant: "t", n: 2 } });
    await topic.publish({ tenantId: "other", data: { tenant: "other", n: 1 } });
    const survivor = await topic.publish({ tenantId: "t", data: { tenant: "t", n: 3 } });
    const jsm = await jetstreamManager(nc);
    await jsm.streams.purge(await eventStream("front-gap"), { seq: survivor.streamSequence });
    let gap: RetentionGapError | null = null;
    try {
      await collect(topic.replay({ tenantId: "t", after: kept.cursor }));
    } catch (error) {
      gap = error as RetentionGapError;
    }
    expect(gap).toBeInstanceOf(RetentionGapError);
    const resumed = await collect(topic.replay({ tenantId: "t", after: gap!.resumeAfter }));
    expect(resumed.map((e) => e.data.n)).toEqual([3]);
  }, 30_000);

  test("eviction limited to other tenants' events behind the cursor is not a gap", async () => {
    const topic = sync.topic<Event>(config("no-false-gap"));
    await topic.publish({ tenantId: "other", data: { tenant: "other", n: 1 } });
    const cursor = await topic.publish({ tenantId: "t", data: { tenant: "t", n: 1 } });
    await topic.publish({ tenantId: "other", data: { tenant: "other", n: 2 } });
    await topic.publish({ tenantId: "t", data: { tenant: "t", n: 2 } });
    const jsm = await jetstreamManager(nc);
    // Remove everything up to and including the cursor: the window still starts right after it.
    await jsm.streams.purge(await eventStream("no-false-gap"), { seq: cursor.streamSequence + 1 });
    expect((await collect(topic.replay({ tenantId: "t", after: cursor.cursor }))).map((e) => e.data.n)).toEqual([2]);
  }, 30_000);

  test("an idle follower is not overtaken by other tenants' traffic", async () => {
    const topic = sync.topic<Event>(config("idle-follow"));
    const start = await topic.publish({ tenantId: "quiet", data: { tenant: "quiet", n: 0 } });
    const controller = new AbortController();
    const followed = collect(topic.follow({ tenantId: "quiet", after: start.cursor, signal: controller.signal }), 1);
    await Bun.sleep(200);
    let last = start;
    for (let n = 1; n <= 5; n++) last = await topic.publish({ tenantId: "busy", data: { tenant: "busy", n } });
    // Wait for the idle watchdog to prove the follower's position past the busy events.
    await Bun.sleep(6_000);
    const jsm = await jetstreamManager(nc);
    await jsm.streams.purge(await eventStream("idle-follow"), { seq: last.streamSequence + 1 });
    await topic.publish({ tenantId: "quiet", data: { tenant: "quiet", n: 1 } });
    expect((await followed).map((e) => e.data.n)).toEqual([1]);
    controller.abort();
  }, 30_000);
});

describe("dead-letter sizing, inventory and destroy", () => {
  test("deadLetterRetention sizes the DLQ stream independently of the log", async () => {
    const id = uniqueName("dlq-size");
    const topic = sync.topic<Event>({ ...config(id), maxPayloadBytes: 64 * 1024, deadLetterRetention: { maxBytes: 1024 * 1024 } });
    await topic.ready();
    const jsm = await jetstreamManager(nc);
    const limits: number[] = [];
    for await (const info of jsm.streams.list()) {
      if (info.config.metadata?.["sync.namespace"] === namespace && info.config.metadata["sync.id"] === id) limits.push(info.config.max_bytes);
    }
    expect(limits.toSorted((a, b) => a - b)).toEqual([1024 * 1024, 64 * 1024 * 1024]);
    expect(() => sync.topic<Event>({ ...config(uniqueName("dlq-tiny")), deadLetterRetention: { maxBytes: 1_024 } })).toThrow(RangeError);
  });

  test("listTopics finds broker topics by prefix; destroy removes both streams without reprovisioning", async () => {
    const prefix = uniqueName("entity");
    const ids = [`${prefix}:1`, `${prefix}:2`];
    for (const id of ids) await sync.topic<Event>(config(id)).publish({ data: { tenant: id, n: 1 } });
    // A different process sees them too: listing reads the broker, not local declarations.
    const other = createSync({ connection: nc, namespace, application: "tests" });
    const listed = await collect(other.listTopics({ idPrefix: `${prefix}:` }));
    expect(listed.map((entry) => entry.id).toSorted()).toEqual(ids);
    expect(listed[0]!.messages).toBe(1);
    expect(listed[0]!.lastPublishedAt).toBeInstanceOf(Date);

    const doomed = other.topic<Event>(config(ids[0]!));
    expect(await doomed.destroy()).toEqual({ destroyed: true });
    await expect(collect(doomed.replay())).rejects.toBeInstanceOf(SyncUsageError);
    // A later global ready() must not recreate the destroyed streams.
    await other.ready();
    expect((await collect(other.listTopics({ idPrefix: `${prefix}:` }))).map((entry) => entry.id)).toEqual([ids[1]!]);
    const jsm = await jetstreamManager(nc);
    let remaining = 0;
    for await (const info of jsm.streams.list()) {
      if (info.config.metadata?.["sync.namespace"] === namespace && info.config.metadata["sync.id"] === ids[0]) remaining++;
    }
    expect(remaining).toBe(0);
    // Destroying again is a no-op.
    expect(await other.topic<Event>(config(ids[0]!)).destroy()).toEqual({ destroyed: false });
    await other.drain({ timeoutMs: 1_000 });
  }, 30_000);
});
