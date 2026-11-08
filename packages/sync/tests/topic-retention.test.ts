import { afterAll, beforeAll, expect, test } from "bun:test";
import { jetstreamManager } from "@nats-io/jetstream";
import { TimeoutError, type NatsConnection } from "@nats-io/nats-core";
import { ConflictError, CursorMismatchError, ResourceDriftError, StoreFullError, SyncUsageError, createSync, type Sync, type TopicConfig } from "../index.ts";
import { resourceIdentity, streamName } from "../src/naming.ts";
import { connectToCluster, uniqueName } from "./cluster.ts";
import { cleanupNamespaces, collect, testNamespace, waitFor } from "./helpers.ts";

let nc: NatsConnection;
let sync: Sync;
const namespace = testNamespace();
const config = (limits: { maxBytes?: number; maxMessages?: number } = {}): TopicConfig => ({
  id: uniqueName("safe-log"),
  whenFull: "reject",
  retention: { maxBytes: 64 * 1024, ...limits },
  maxPayloadBytes: 1_024,
});
const streamOf = (id: string) => streamName(resourceIdentity(namespace, "topic", id));

beforeAll(async () => {
  nc = await connectToCluster({ name: "topic-retention" });
  sync = createSync({ connection: nc, namespace, application: "tests" });
});

afterAll(async () => {
  await sync.drain({ timeoutMs: 5_000 });
  await cleanupNamespaces(nc, [namespace]);
  await nc.close();
});

test("reject-full preserves every accepted event, dedupes and rejects atomic batches with a public typed error", async () => {
  const options = config({ maxMessages: 3 });
  const topic = sync.topic<number>(options);
  const first = await topic.publish({ data: 1, idempotencyKey: "one" });
  await topic.publish({ data: 2 });
  // Two events do not fit the single remaining slot; no partial append.
  await expect(topic.publishBatch({ events: [{ data: 3 }, { data: 4 }] })).rejects.toBeInstanceOf(StoreFullError);
  await topic.publish({ data: 3 });
  await expect(topic.publish({ data: 4 })).rejects.toBeInstanceOf(StoreFullError);
  await expect(topic.publishBatch({ events: [{ data: 4 }] })).rejects.toBeInstanceOf(StoreFullError);
  expect((await topic.publish({ data: 1, idempotencyKey: "one" })).duplicate).toBe(true);
  expect((await collect(topic.replay({ after: topic.cursorAt(0) }))).map(event => event.data)).toEqual([1, 2, 3]);
  expect(topic.cursorSequence(await topic.head())).toBe(first.streamSequence + 2);
  const jsm = await jetstreamManager(nc);
  const info = await jsm.streams.info(streamOf(options.id));
  expect(info.config.discard).toBe("new");
  expect(info.config.max_age).toBe(0);
  expect(info.config.max_msgs).toBe(3);
}, 30_000);

test("the byte limit rejects both single events and batches without evicting accepted history", async () => {
  const options = config({ maxBytes: 4_096 });
  const topic = sync.topic<string>(options);
  const accepted: string[] = [];
  let refusal: unknown;
  for (let n = 0; n < 30; n++) {
    const data = String(n).padEnd(700, ".");
    try {
      await topic.publish({ data });
      accepted.push(data);
    } catch (error) {
      refusal = error;
      break;
    }
  }
  expect(accepted.length).toBeGreaterThan(0);
  expect(refusal).toBeInstanceOf(StoreFullError);
  expect(refusal).toHaveProperty("cause");
  await expect(topic.publishBatch({ events: [{ data: "x".repeat(700) }, { data: "y".repeat(700) }] })).rejects.toBeInstanceOf(StoreFullError);
  expect((await collect(topic.replay())).map(event => event.data)).toEqual(accepted);
}, 30_000);

test("omitted age retains uncovered events while an explicit age still expires", async () => {
  const retainedConfig = config();
  const expiringConfig = { ...config(), dedupeWindowMs: 100, retention: { maxBytes: 64 * 1024, maxAgeMs: 200 } };
  const retained = sync.topic<string>(retainedConfig);
  await retained.publish({ data: "uncovered" });
  await sync.topic<string>(expiringConfig).publish({ data: "expiring" });
  const jsm = await jetstreamManager(nc);
  await waitFor(async () => (await jsm.streams.info(streamOf(expiringConfig.id))).state.messages === 0);
  expect((await collect(retained.replay())).map(e => e.data)).toEqual(["uncovered"]);
}, 30_000);

test("pruning is inclusive, stream-wide, idempotent and never passes the persisted watermark", async () => {
  const topic = sync.topic<number>(config({ maxMessages: 4 }));
  const a = await topic.publish({ data: 1, tenantId: "a" });
  const b = await topic.publish({ data: 2, tenantId: "b" });
  const nextA = await topic.publish({ data: 3, tenantId: "a" });
  const nextB = await topic.publish({ data: 4, tenantId: "b" });
  // Snapshots cover a through 3 and b through 2: only their minimum is safe.
  const watermark = topic.cursorAt(Math.min(nextA.streamSequence, b.streamSequence));
  expect(await topic.prune({ through: watermark })).toEqual({ purged: 2 });
  expect(await topic.prune({ through: watermark })).toEqual({ purged: 0 });
  expect(await topic.prune({ through: a.cursor })).toEqual({ purged: 0 });
  expect(await topic.prune({ through: topic.cursorAt(0) })).toEqual({ purged: 0 });
  expect((await collect(topic.replay({ tenantId: "a", after: watermark }))).map(e => e.cursor)).toEqual([nextA.cursor]);
  expect((await collect(topic.replay({ tenantId: "b", after: watermark }))).map(e => e.cursor)).toEqual([nextB.cursor]);
  await expect(topic.prune({ through: topic.cursorAt(nextB.streamSequence + 1) })).rejects.toBeInstanceOf(SyncUsageError);
  const foreign = sync.topic<number>(config()).cursorAt(1);
  await expect(topic.prune({ through: foreign })).rejects.toBeInstanceOf(CursorMismatchError);
  await topic.publish({ data: 5 }); // pruning freed room
}, 30_000);

test("a concurrent append survives pruning at the captured head", async () => {
  const options = config();
  const topic = sync.topic<number>(options);
  const snapshot = await topic.publish({ data: 1 });
  // Inject an append after prune's head check but before its purge request.
  const racingConnection = new Proxy(nc, {
    get(target, property) {
      if (property === "request") return async (...args: Parameters<NatsConnection["request"]>) => {
        if (args[0].endsWith(`.STREAM.PURGE.${streamOf(options.id)}`)) await topic.publish({ data: 2 });
        return target.request(...args);
      };
      const value: unknown = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  const peer = createSync({ connection: racingConnection, namespace, application: "tests" });
  try {
    expect(await peer.topic<number>(options).prune({ through: snapshot.cursor })).toEqual({ purged: 1 });
    expect((await collect(topic.replay({ after: snapshot.cursor }))).map(e => e.data)).toEqual([2]);
  } finally {
    await peer.drain({ timeoutMs: 1_000 });
  }
}, 30_000);

test("restart recovers snapshot plus unpruned events; a full stream remains full until pruning", async () => {
  const options = config({ maxMessages: 2 });
  const topic = sync.topic<string>(options);
  const snapshot = await topic.publish({ data: "persisted" });
  const uncovered = await topic.publish({ data: "uncovered" });
  const restartConnection = await connectToCluster({ name: "topic-retention-restart" });
  const restarted = createSync({ connection: restartConnection, namespace, application: "tests" });
  try {
    const recovered = restarted.topic<string>(options);
    await recovered.ready();
    await expect(recovered.publish({ data: "overflow" })).rejects.toBeInstanceOf(StoreFullError);
    expect((await collect(recovered.replay({ after: snapshot.cursor }))).map(e => e.cursor)).toEqual([uncovered.cursor]);
    expect(await recovered.prune({ through: snapshot.cursor })).toEqual({ purged: 1 });
    await recovered.publish({ data: "after-restart" });
    expect((await collect(recovered.replay({ after: snapshot.cursor }))).map(e => e.data)).toEqual(["uncovered", "after-restart"]);
  } finally {
    await restarted.drain({ timeoutMs: 1_000 });
    await restartConnection.close();
  }
}, 30_000);

test("a lost atomic commit acknowledgement stays an unknown outcome, never backpressure", async () => {
  const options = config({ maxMessages: 2 });
  const lossyConnection = new Proxy(nc, {
    get(target, property) {
      if (property === "request") return async (...args: Parameters<NatsConnection["request"]>) => {
        const reply = await target.request(...args);
        if (args[2]?.headers?.get("Nats-Batch-Commit") === "1") throw new TimeoutError();
        return reply;
      };
      const value: unknown = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  const lossy = createSync({ connection: lossyConnection, namespace, application: "tests" });
  try {
    await expect(lossy.topic<number>(options).publishBatch({ events: [{ data: 1 }, { data: 2 }], expectedAfter: null })).rejects.toBeInstanceOf(TimeoutError);
    expect((await collect(sync.topic<number>(options).replay())).map(e => e.data)).toEqual([1, 2]);
  } finally {
    await lossy.drain({ timeoutMs: 1_000 });
  }
}, 30_000);

test("legacy defaults and explicit ages remain; mode or age changes fail drift checks", async () => {
  const options = { ...config({ maxMessages: 2 }), whenFull: undefined, retention: { maxBytes: 64 * 1024, maxAgeMs: 3_600_000, maxMessages: 2 } };
  const topic = sync.topic<number>(options);
  for (const data of [1, 2, 3]) await topic.publish({ data });
  expect((await collect(topic.replay())).map(e => e.data)).toEqual([2, 3]);
  const info = await (await jetstreamManager(nc)).streams.info(streamOf(options.id));
  expect(info.config.discard).toBe("old");
  expect(info.config.max_age).toBe(3_600_000 * 1_000_000);
  for (const change of [{ whenFull: "reject" as const }, { retention: { maxBytes: 64 * 1024, maxMessages: 2 } }]) {
    const peer = createSync({ connection: nc, namespace, application: "tests" });
    try {
      await expect(peer.topic({ ...options, ...change }).ready()).rejects.toBeInstanceOf(ResourceDriftError);
    } finally {
      await peer.drain({ timeoutMs: 1_000 });
    }
  }
  for (const maxAgeMs of [0, -1, NaN]) expect(() => sync.topic({ ...config(), retention: { maxBytes: 1_024, maxAgeMs } })).toThrow(RangeError);
  expect(() => sync.topic({ ...config(), whenFull: "invalid" as "reject" })).toThrow(RangeError);
}, 30_000);

test("optimistic concurrency conflicts remain distinct from capacity errors", async () => {
  const topic = sync.topic<number>(config());
  await topic.publish({ data: 1 });
  await expect(topic.publishBatch({ events: [{ data: 2 }, { data: 3 }], expectedAfter: null })).rejects.toBeInstanceOf(ConflictError);
  expect((await collect(topic.replay())).map(e => e.data)).toEqual([1]);
}, 30_000);
