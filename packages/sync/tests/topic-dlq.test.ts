import { afterAll, beforeAll, expect, test } from "bun:test";
import { connect } from "@nats-io/transport-node";
import { jetstreamManager } from "@nats-io/jetstream";
import { createSync } from "../src/sync.ts";
import { cleanupNamespaces, testNamespace, waitFor } from "./helpers.ts";

const servers = process.env.SYNC_TEST_SERVERS?.split(",");
const integration = servers ? test : test.skip;
const namespace = testNamespace();
let nc: Awaited<ReturnType<typeof connect>>;
let sync: ReturnType<typeof createSync>;
beforeAll(async () => {
  if (!servers) return;
  nc = await connect({ servers, ignoreClusterUpdates: true });
  sync = createSync({ connection: nc, namespace, application: "topic-dlq-tests" });
});
afterAll(async () => {
  if (!servers) return;
  await sync.drain({ timeoutMs: 1_000 });
  await cleanupNamespaces(nc, [namespace]);
  await nc.close();
});
const config = (id: string) => ({ id, retention: { maxAgeMs: 600_000, maxBytes: 1_000_000 } });
const options = { consumer: "sink", recoverDeadLetters: true, delivery: { maxAttempts: 1, ackWaitMs: 1_000 } };

integration("recovery preserves event identity and invokes only its original consumer", async () => {
  const topic = sync.topic<{ n: number }>(config("targeted"));
  let fail = true;
  const handled: Array<{ eventId: string; sequence: number; publishedAt: Date; attempt: number }> = [];
  const other: number[] = [];
  const a = await topic.process(options, async (event) => {
    if (fail) throw new Error("sink unavailable");
    handled.push(event);
  });
  const b = await topic.process({ consumer: "other" }, async (event) => { other.push(event.data.n); });
  const published = await topic.publish({ data: { n: 7 }, orderingKey: "order", meta: { traceId: "trace" } });
  await waitFor(async () => (await topic.deadLetters.list()).length === 1);
  const [entry] = await topic.deadLetters.list();
  expect(entry).toMatchObject({ eventId: published.eventId, consumer: "sink", replayAvailable: true, data: { n: 7 } });
  expect(entry!.event).toMatchObject({ sequence: published.streamSequence, cursor: published.cursor, orderingKey: "order", meta: { traceId: "trace" } });
  await expect(topic.deadLetters.replay({ messageId: entry!.messageId, consumer: "other", tenantId: "default" })).rejects.toThrow("mismatch");
  await expect(topic.deadLetters.replay({ messageId: entry!.messageId, consumer: "sink", tenantId: "other" })).rejects.toThrow("mismatch");
  fail = false;
  const receipt = await topic.deadLetters.replay({ messageId: entry!.messageId, consumer: "sink", tenantId: "default" });
  expect(receipt.completed).toBe(true);
  expect(handled).toHaveLength(1);
  expect(handled[0]).toMatchObject({ eventId: published.eventId, sequence: published.streamSequence, publishedAt: entry!.event!.publishedAt, attempt: 1 });
  expect(other).toEqual([7]);
  expect(await topic.head()).toBe(published.cursor);
  expect(await topic.deadLetters.get(entry!)).toBeNull();
  await expect(topic.deadLetters.replay({ messageId: entry!.messageId, consumer: "sink", tenantId: "default" })).rejects.toThrow("not found");
  await Promise.all([a.drain(), b.drain()]);
}, 15_000);

integration("failed and timed-out replay retains entry; concurrent processes cannot replay until settlement", async () => {
  const topic = sync.topic<number>(config("timeout"));
  let mode: "fail" | "hang" = "fail";
  const release = Promise.withResolvers<void>();
  let invoked = 0;
  let receivedSignal: AbortSignal | undefined;
  const worker = await topic.process(options, async (event) => {
    invoked++;
    if (mode === "fail") throw new Error("still broken");
    receivedSignal = event.signal;
    await release.promise; // intentionally ignores abort
  });
  await topic.publish({ data: 1 });
  await waitFor(async () => (await topic.deadLetters.list()).length === 1);
  const entry = (await topic.deadLetters.list())[0]!;
  const input = { messageId: entry.messageId, consumer: "sink", tenantId: "default" };
  await expect(topic.deadLetters.replay(input)).rejects.toThrow("still broken");
  expect(await topic.deadLetters.get(entry)).not.toBeNull();
  mode = "hang";
  const hanging = topic.deadLetters.replay({ ...input, timeoutMs: 6_000 });
  await waitFor(() => receivedSignal !== undefined);
  await expect(hanging).rejects.toThrow("timed out");
  expect(receivedSignal!.aborted).toBe(true);
  const second = createSync({ connection: nc, namespace, application: "topic-dlq-tests" });
  const secondTopic = second.topic<number>(config("timeout"));
  let secondCalls = 0;
  const otherWorker = await secondTopic.process(options, async () => { secondCalls++; });
  await expect(secondTopic.deadLetters.replay(input)).rejects.toThrow("already active");
  expect(secondCalls).toBe(0);
  // Exceed the original 30s lease: timeout must not free a still-running handler.
  await Bun.sleep(31_000);
  await expect(secondTopic.deadLetters.replay(input)).rejects.toThrow("already active");
  expect(secondCalls).toBe(0);
  expect(invoked).toBe(3);
  expect(await topic.deadLetters.get(entry)).not.toBeNull();
  release.resolve();
  await Bun.sleep(100);
  expect(await topic.deadLetters.get(entry)).not.toBeNull();
  await worker.drain();
  await secondTopic.deadLetters.replay(input);
  expect(secondCalls).toBe(1);
  await otherWorker.drain();
  await second.drain();
}, 60_000);

integration("legacy failures remain inspectable/deletable and unavailable for unsafe replay", async () => {
  const topic = sync.topic<number>(config("legacy"));
  const worker = await topic.process(options, async () => {});
  const jsm = await jetstreamManager(nc);
  let dlq: { name: string; subject: string } | undefined;
  for await (const stream of jsm.streams.list()) {
    if (stream.config.metadata?.["sync.namespace"] === namespace && stream.config.metadata?.["sync.id"] === "legacy" && stream.config.name.includes("D_")) {
      dlq = { name: stream.config.name, subject: stream.config.subjects![0]!.replace(">", "c2luaw") };
    }
  }
  expect(dlq).toBeDefined();
  nc.publish(dlq!.subject, new TextEncoder().encode(JSON.stringify({ v: 6, data: 42, tenantId: "default", publishedAt: new Date().toISOString(), ext: { eventId: "legacy-id", consumer: "sink", reason: "max attempts exhausted", attempts: 5 } })));
  await nc.flush();
  await waitFor(async () => (await topic.deadLetters.list()).length === 1);
  const entry = (await topic.deadLetters.list())[0]!;
  expect(entry.event).toBeUndefined();
  expect(entry.replayAvailable).toBe(false);
  await expect(topic.deadLetters.replay({ messageId: entry.messageId, consumer: "sink", tenantId: "default" })).rejects.toThrow("no recoverable original event");
  expect((await topic.deadLetters.get(entry))!.data).toBe(42);
  expect(await topic.deadLetters.delete(entry)).toBe(true);
  expect(await topic.deadLetters.list()).toEqual([]);
  await worker.drain();
}, 15_000);

integration("controls share opt-in handler across duplicate handles; pagination survives deleted cursor", async () => {
  const topic = sync.topic<number>(config("paging"));
  const worker = await topic.process(options, async () => { throw new Error("fail"); });
  for (const n of [1, 2, 3]) await topic.publish({ data: n });
  await waitFor(async () => (await topic.deadLetters.list()).length === 3);
  const duplicate = sync.topic<number>(config("paging"));
  expect(sync.controls().find((entry) => entry.kind === "topic" && entry.id === "paging")).toBeDefined();
  const first = (await duplicate.deadLetters.list({ limit: 1 }))[0]!;
  expect(first.replayAvailable).toBe(true);
  await topic.deadLetters.delete(first);
  expect((await duplicate.deadLetters.list({ after: first.messageId, limit: 1 }))[0]!.data).toBe(2);
  await expect(topic.deadLetters.list({ limit: 0 })).rejects.toThrow("limit");
  await expect(topic.deadLetters.get({ messageId: "-1" })).rejects.toThrow("sequence");
  await worker.drain();
  expect((await duplicate.deadLetters.list())[0]!.replayAvailable).toBe(false);
}, 15_000);

integration("graceful runtime drain settles recovery and releases its existing lease", async () => {
  const instance = createSync({ connection: nc, namespace, application: "topic-dlq-tests" });
  const topic = instance.topic<number>(config("drain"));
  expect(instance.controls()).toEqual([]); // append/watch-only topics need no controls
  let recover = false;
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  await topic.process(options, async () => {
    if (!recover) throw new Error("initial failure");
    entered.resolve();
    await release.promise;
  });
  await topic.publish({ data: 1 });
  await waitFor(async () => (await topic.deadLetters.list()).length === 1);
  const entry = (await topic.deadLetters.list())[0]!;
  recover = true;
  const replay = topic.deadLetters.replay({ messageId: entry.messageId, consumer: "sink", tenantId: "default" });
  await entered.promise;
  const draining = instance.drain({ timeoutMs: 2_000 });
  release.resolve();
  expect((await replay).completed).toBe(true);
  expect((await draining).timedOut).toBe(false);
  expect(await topic.deadLetters.get(entry)).toBeNull();
}, 15_000);

integration("forced runtime drain aborts recovery and retains the original entry", async () => {
  const instance = createSync({ connection: nc, namespace, application: "topic-dlq-tests" });
  const topic = instance.topic<number>(config("forced-drain"));
  let recover = false;
  const entered = Promise.withResolvers<void>();
  await topic.process(options, async (event) => {
    if (!recover) throw new Error("initial failure");
    entered.resolve();
    await new Promise<void>((resolve) => event.signal.addEventListener("abort", () => resolve(), { once: true }));
  });
  await topic.publish({ data: 1 });
  await waitFor(async () => (await topic.deadLetters.list()).length === 1);
  const entry = (await topic.deadLetters.list())[0]!;
  recover = true;
  const replay = topic.deadLetters.replay({ messageId: entry.messageId, consumer: "sink", tenantId: "default" });
  const rejected = replay.then(() => null, (error: unknown) => error);
  await entered.promise;
  await instance.drain({ timeoutMs: 20 });
  expect(await rejected).toBeInstanceOf(Error);
  expect(await topic.deadLetters.get(entry)).not.toBeNull();
}, 15_000);

integration("existing mutex leases can extend/release during drain but cannot renew after stop", async () => {
  const instance = createSync({ connection: nc, namespace, application: "topic-dlq-tests" });
  const mutex = instance.mutex({ id: "drain-lease" });
  const lock = await mutex.acquire({ resource: "entry" });
  expect(lock).not.toBeNull();
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const topic = instance.topic<number>(config("drain-lease-worker"));
  await topic.process({ consumer: "sink" }, async () => { entered.resolve(); await release.promise; });
  await topic.publish({ data: 1 });
  await entered.promise;
  const draining = instance.drain({ timeoutMs: 2_000 });
  expect(await mutex.extend(lock!)).toBe(true);
  await expect(mutex.acquire({ resource: "new" })).rejects.toThrow("draining");
  expect(await mutex.release(lock!)).toBe(true);
  release.resolve();
  await draining;
  expect(await mutex.extend(lock!)).toBe(false);
}, 15_000);
