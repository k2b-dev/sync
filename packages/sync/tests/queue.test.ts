import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { NatsConnection } from "@nats-io/nats-core";
import { createSync } from "../src/sync.ts";
import type { Sync } from "../src/sync.ts";
import { PayloadTooLargeError, UnsupportedServerError } from "../src/errors.ts";
import { DLQ_HEADROOM_BYTES } from "../src/types.ts";
import { connectToCluster, uniqueName } from "./cluster.ts";
import { cleanupNamespaces, testNamespace, waitFor } from "./helpers.ts";

type Task = { n: number };

let nc: NatsConnection;
let sync: Sync;
const namespace = testNamespace();

beforeAll(async () => {
  nc = await connectToCluster({ name: "queue-test" });
  sync = createSync({ connection: nc, namespace, application: "tests" });
  await sync.ready();
});

afterAll(async () => {
  await sync.drain({ timeoutMs: 5_000 });
  await cleanupNamespaces(nc, [namespace]);
  await nc.close();
});

describe("queue delivery", () => {
  test("competing workers split the work; every message is handled once", async () => {
    const queue = sync.queue<Task>({ id: "split" });
    const seen: number[] = [];
    const byWorker = [0, 0];
    const w1 = await queue.process({ concurrency: 4 }, async (message) => {
      seen.push(message.data.n);
      byWorker[0] += 1;
    });
    const w2 = await queue.process({ concurrency: 4 }, async (message) => {
      seen.push(message.data.n);
      byWorker[1] += 1;
    });
    for (let n = 1; n <= 20; n++) await queue.send({ data: { n } });
    await waitFor(() => seen.length >= 20);
    await Bun.sleep(300);
    expect(seen.toSorted((a, b) => a - b)).toEqual(Array.from({ length: 20 }, (_, i) => i + 1));
    expect(byWorker[0]! + byWorker[1]!).toBe(20);
    await Promise.all([w1.drain(), w2.drain()]);
  });

  test("idempotencyKey dedupes within the window, scoped per tenant", async () => {
    const queue = sync.queue<Task>({ id: "dedupe" });
    const first = await queue.send({ data: { n: 1 }, idempotencyKey: "once" });
    const dup = await queue.send({ data: { n: 1 }, idempotencyKey: "once" });
    const other = await queue.send({ data: { n: 1 }, tenantId: "acme", idempotencyKey: "once" });
    expect(first.duplicate).toBe(false);
    expect(dup.duplicate).toBe(true);
    expect(other.duplicate).toBe(false);
  });

  test("delayMs delivers later without occupying a handler slot", async () => {
    const queue = sync.queue<Task>({ id: "delay" });
    const deliveredAt: number[] = [];
    const start = Date.now();
    const w = await queue.process({}, async () => {
      deliveredAt.push(Date.now() - start);
    });
    await queue.send({ data: { n: 1 }, delayMs: 1_500 });
    await queue.send({ data: { n: 2 } });
    await waitFor(() => deliveredAt.length === 2, 15_000);
    const [immediate, delayed] = deliveredAt.toSorted((a, b) => a - b);
    expect(immediate!).toBeLessThan(1_000);
    expect(delayed!).toBeGreaterThanOrEqual(1_200);
    await w.drain();
  });

  test("failed handler retries with backoff, then dead-letters; requeue works", async () => {
    const queue = sync.queue<Task>({
      id: "retry",
      delivery: { maxAttempts: 2, backoffMs: [100], ackWaitMs: 5_000 },
    });
    const attempts: number[] = [];
    let failing = true;
    const w = await queue.process({}, async (message) => {
      attempts.push(message.attempt);
      if (failing) throw new Error("boom");
    });
    await queue.send({ data: { n: 7 }, idempotencyKey: "fail-1" });
    await waitFor(() => attempts.length >= 2, 15_000);
    await Bun.sleep(400);
    expect(attempts).toEqual([1, 2]);

    const dead = await queue.deadLetters.list();
    expect(dead).toHaveLength(1);
    expect(dead[0]!.data.n).toBe(7);
    expect(dead[0]!.attempts).toBe(2);
    expect(dead[0]!.reason).toBe("max attempts exhausted");
    expect(dead[0]!.error).toContain("boom");

    failing = false;
    await queue.deadLetters.requeue({ messageId: dead[0]!.messageId, idempotencyKey: "requeue-1" });
    await waitFor(() => attempts.length >= 3, 15_000);
    expect(await queue.deadLetters.list()).toHaveLength(0);
    await w.drain();
  });

  test("oversized payloads are rejected locally before publish", async () => {
    const queue = sync.queue<string>({ id: "toobig", maxPayloadBytes: 1_024 });
    await expect(queue.send({ data: "x".repeat(2_000) })).rejects.toBeInstanceOf(PayloadTooLargeError);
  });

  test("crashed handler slot is redelivered after ackWait to another worker", async () => {
    const queue = sync.queue<Task>({
      id: "crash",
      delivery: { ackWaitMs: 2_000, maxAttempts: 3, backoffMs: [100] },
    });
    // Worker 1 "crashes": takes the message and never settles it.
    let hung = 0;
    const w1 = await queue.process({}, async () => {
      hung += 1;
      await new Promise(() => {}); // never resolves — simulates a dead process
    });
    await queue.send({ data: { n: 1 } });
    await waitFor(() => hung === 1);
    // Worker 2 picks it up after ackWait expiry.
    const rescued: number[] = [];
    const w2 = await queue.process({}, async (message) => {
      rescued.push(message.attempt);
    });
    await waitFor(() => rescued.length === 1, 15_000);
    expect(rescued[0]).toBeGreaterThanOrEqual(2);
    w1.stop();
    await w2.drain();
    await w1.drain({ timeoutMs: 100 }); // force-aborts the hung handler
  }, 30_000);
});

describe("queue reader", () => {
  test("manual ack, retry, and deadLetter settlement", async () => {
    const queue = sync.queue<Task>({
      id: "manual",
      delivery: { maxAttempts: 3, backoffMs: [100], ackWaitMs: 5_000 },
    });
    await queue.send({ data: { n: 1 } });
    const reader = await queue.reader();

    const first = await reader.receive({ waitMs: 5_000 });
    expect(first).not.toBeNull();
    expect(first!.data.n).toBe(1);
    await first!.retry({ delayMs: 100 });

    const second = await reader.receive({ waitMs: 5_000 });
    expect(second!.attempt).toBe(2);
    await second!.ack();

    await queue.send({ data: { n: 2 } });
    const third = await reader.receive({ waitMs: 5_000 });
    await third!.deadLetter({ reason: "manually rejected" });
    const dead = await queue.deadLetters.list();
    expect(dead.map((d) => d.reason)).toContain("manually rejected");

    expect(await reader.receive({ waitMs: 300 })).toBeNull();
    await reader.close();
  }, 30_000);
});

describe("partitioned ordering", () => {
  test("same orderingKey is serial and in order; different keys run in parallel", async () => {
    const queue = sync.queue<Task>({
      id: "partitioned",
      ordering: { mode: "partitioned", partitions: 8 },
      delivery: { ackWaitMs: 10_000 },
    });
    const perKey = new Map<string, number[]>();
    let maxConcurrentA = 0;
    let currentA = 0;
    const w = await queue.process({ concurrency: 8 }, async (message) => {
      const key = message.orderingKey!;
      if (key === "a") {
        currentA += 1;
        maxConcurrentA = Math.max(maxConcurrentA, currentA);
      }
      await Bun.sleep(30);
      perKey.get(key)?.push(message.data.n) ?? perKey.set(key, [message.data.n]);
      if (key === "a") currentA -= 1;
    });
    const sends = [];
    for (let n = 1; n <= 8; n++) sends.push(queue.send({ data: { n }, orderingKey: "a" }));
    for (let n = 1; n <= 8; n++) sends.push(queue.send({ data: { n }, orderingKey: "b" }));
    await Promise.all(sends);
    await waitFor(() => (perKey.get("a")?.length ?? 0) + (perKey.get("b")?.length ?? 0) >= 16, 30_000);
    expect(perKey.get("a")).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    expect(perKey.get("b")).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    expect(maxConcurrentA).toBe(1);
    await w.drain();
  }, 45_000);

  test("send without orderingKey fails; reader is refused", async () => {
    const queue = sync.queue<Task>({
      id: uniqueName("part2"),
      ordering: { mode: "partitioned", partitions: 2 },
    });
    await expect(queue.send({ data: { n: 1 } })).rejects.toThrow("requires an orderingKey");
    await expect(queue.reader()).rejects.toThrow("partitioned");
  });
});

describe("hardening regressions", () => {
  test("deadLetters.list terminates after the newest DLQ entry was deleted", async () => {
    const queue = sync.queue<Task>({
      id: "dlq-scan",
      delivery: { maxAttempts: 1, backoffMs: [100], ackWaitMs: 5_000 },
    });
    const w = await queue.process({}, async () => {
      throw new Error("always fails");
    });
    await queue.send({ data: { n: 1 }, idempotencyKey: "d1" });
    await queue.send({ data: { n: 2 }, idempotencyKey: "d2" });
    await waitFor(async () => (await queue.deadLetters.list()).length === 2, 15_000);
    await w.drain();

    const entries = await queue.deadLetters.list();
    // Delete the newest entry (highest sequence): the scan must still
    // terminate instead of waiting for the removed last_seq forever.
    expect(await queue.deadLetters.delete({ messageId: entries[1]!.messageId })).toBe(true);
    const remaining = await queue.deadLetters.list();
    expect(remaining).toHaveLength(1);
    expect(remaining[0]!.messageId).toBe(entries[0]!.messageId);
  }, 30_000);

  test("partitioned processing honors local concurrency below the partition count", async () => {
    const queue = sync.queue<Task>({
      id: "part-cap",
      ordering: { mode: "partitioned", partitions: 4 },
      delivery: { ackWaitMs: 10_000 },
    });
    let active = 0;
    let maxActive = 0;
    const done: number[] = [];
    const w = await queue.process({ concurrency: 1 }, async (message) => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      await Bun.sleep(50);
      active -= 1;
      done.push(message.data.n);
    });
    const sends = [];
    for (let n = 1; n <= 8; n++) sends.push(queue.send({ data: { n }, orderingKey: `k${n % 4}` }));
    await Promise.all(sends);
    await waitFor(() => done.length >= 8, 30_000);
    expect(maxActive).toBe(1);
    await w.drain();
  }, 45_000);

  test("per-key order survives a failing handler (in-place retry, no overtake)", async () => {
    const queue = sync.queue<Task>({
      id: "part-order-fail",
      ordering: { mode: "partitioned", partitions: 2 },
      delivery: { maxAttempts: 3, backoffMs: [300], ackWaitMs: 10_000 },
    });
    const completed: number[] = [];
    let failedOnce = false;
    const w = await queue.process({ concurrency: 2 }, async (message) => {
      if (message.data.n === 2 && !failedOnce) {
        failedOnce = true;
        throw new Error("transient");
      }
      completed.push(message.data.n);
    });
    for (let n = 1; n <= 4; n++) await queue.send({ data: { n }, orderingKey: "same" });
    await waitFor(() => completed.length >= 4, 30_000);
    // Without in-place retry, 3 would overtake 2 during the nak backoff.
    expect(completed).toEqual([1, 2, 3, 4]);
    await w.drain();
  }, 45_000);
});

describe("nats feature surface", () => {
  test("sendBatch is atomic: all messages land, failures persist nothing", async () => {
    const queue = sync.queue<Task>({ id: "batch" });
    const receipt = await queue.sendBatch([{ data: { n: 1 } }, { data: { n: 2 } }, { data: { n: 3 } }]);
    expect(receipt.count).toBe(3);
    expect(receipt.messageIds).toHaveLength(3);

    // An oversized member fails the whole batch before anything is staged.
    const tiny = sync.queue<string>({ id: "batch-fail", maxPayloadBytes: 1_024 });
    await expect(tiny.sendBatch([{ data: "ok" }, { data: "y".repeat(4_000) }])).rejects.toThrow("payload");
    const seen: number[] = [];
    const w = await queue.process({ concurrency: 4 }, async (m) => {
      seen.push(m.data.n);
    });
    await waitFor(() => seen.length >= 3, 15_000);
    expect(seen.toSorted()).toEqual([1, 2, 3]);
    await w.drain();

    await expect(queue.sendBatch([{ data: { n: 1 }, idempotencyKey: "x" }])).rejects.toThrow("idempotencyKey");
    await expect(queue.sendBatch([{ data: { n: 1 }, delayMs: 5_000 }, { data: { n: 2 } }])).rejects.toThrow("delayed");
  }, 30_000);

  test("pause stops global delivery; resume restores it", async () => {
    const queue = sync.queue<Task>({ id: "pausable" });
    const seen: number[] = [];
    const w = await queue.process({}, async (m) => {
      seen.push(m.data.n);
    });
    const paused = await queue.pause();
    expect(paused.paused).toBe(true);
    await queue.send({ data: { n: 1 } });
    await Bun.sleep(1_500);
    expect(seen).toEqual([]); // publishes continue, delivery does not
    const resumed = await queue.resume();
    expect(resumed.paused).toBe(false);
    await waitFor(() => seen.length === 1, 15_000);
    await w.drain();
  }, 30_000);

  test("ttlMs expires unconsumed work", async () => {
    const queue = sync.queue<Task>({ id: "expiring" });
    await queue.send({ data: { n: 1 }, ttlMs: 1_000 });
    await Bun.sleep(2_500);
    const seen: number[] = [];
    const w = await queue.process({}, async (m) => {
      seen.push(m.data.n);
    });
    await Bun.sleep(1_500);
    expect(seen).toEqual([]); // expired before any worker existed
    await w.drain();
  }, 15_000);
});

describe("batch ttl regression", () => {
  test("ttlMs expires every member of a multi-message batch", async () => {
    const queue = sync.queue<Task>({ id: "batch-ttl" });
    await queue.sendBatch([
      { data: { n: 1 }, ttlMs: 1_000 },
      { data: { n: 2 }, ttlMs: 1_000 },
    ]);
    await Bun.sleep(2_500);
    const seen: number[] = [];
    const w = await queue.process({}, async (m) => {
      seen.push(m.data.n);
    });
    await Bun.sleep(1_500);
    expect(seen).toEqual([]);
    await w.drain();
  }, 15_000);
});

describe("server payload guard", () => {
  test("a payload limit above the server max_payload is refused at ready()", async () => {
    const serverMax = nc.info!.max_payload;
    const strict = createSync({ connection: nc, namespace, application: "tests" });
    // The dead-letter envelope adds headroom: a limit that fits alone but not with headroom is refused too.
    strict.queue<Task>({ id: "too-large", maxPayloadBytes: serverMax - DLQ_HEADROOM_BYTES + 1 });
    await expect(strict.ready()).rejects.toBeInstanceOf(UnsupportedServerError);
    await strict.drain({ timeoutMs: 1_000 });

    const fits = createSync({ connection: nc, namespace, application: "tests" });
    fits.queue<Task>({ id: "fits", maxPayloadBytes: serverMax - 8_192 });
    await fits.ready();
    await fits.drain({ timeoutMs: 1_000 });
  }, 20_000);
});
