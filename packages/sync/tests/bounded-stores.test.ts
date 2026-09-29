import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { jetstreamManager } from "@nats-io/jetstream";
import type { StreamConfig } from "@nats-io/jetstream";
import type { NatsConnection } from "@nats-io/nats-core";
import { createSync } from "../src/sync.ts";
import type { Sync } from "../src/sync.ts";
import { ResourceDriftError, StoreFullError, SyncUsageError } from "../src/errors.ts";
import { DEFAULT_MESSAGE_PAYLOAD_BYTES, DLQ_HEADROOM_BYTES, defaultWorkRetention, nanos } from "../src/types.ts";
import { connectToCluster, uniqueName } from "./cluster.ts";
import { cleanupNamespaces, testNamespace, waitFor } from "./helpers.ts";

let nc: NatsConnection;
let sync: Sync;
const namespace = testNamespace();

beforeAll(async () => {
  nc = await connectToCluster({ name: "bounded-stores-test" });
  sync = createSync({ connection: nc, namespace, application: "tests" });
  await sync.ready();
});

afterAll(async () => {
  await sync.drain({ timeoutMs: 5_000 });
  await cleanupNamespaces(nc, [namespace]);
  await nc.close();
});

/** Work and dead-letter stream configs of one queue or job, keyed by role. */
const streamsOf = async (kind: "queue" | "job", id: string): Promise<{ work: StreamConfig; deadLetters: StreamConfig }> => {
  const jsm = await jetstreamManager(nc);
  const found: StreamConfig[] = [];
  for await (const info of jsm.streams.list()) {
    const meta = info.config.metadata;
    if (meta?.["sync.namespace"] === namespace && meta["sync.kind"] === kind && meta["sync.id"] === id && !info.config.name.startsWith("KV_")) {
      found.push(info.config);
    }
  }
  const work = found.find((config) => config.retention === "workqueue");
  const deadLetters = found.find((config) => config.retention === "limits");
  if (work === undefined || deadLetters === undefined) throw new Error(`streams of ${kind} ${id} not found`);
  return { work, deadLetters };
};

/** Send until the queue refuses; returns how many sends were accepted and the refusal. */
const fill = async (send: (n: number) => Promise<unknown>): Promise<{ accepted: number; error: unknown }> => {
  for (let n = 0; n < 1_000; n++) {
    try {
      await send(n);
    } catch (error) {
      return { accepted: n, error };
    }
  }
  throw new Error("the store never filled up");
};

describe("default and dead-letter limits", () => {
  test("a queue without retention reserves 256 messages at its payload limit, capped at 1 GiB", async () => {
    expect(defaultWorkRetention(DEFAULT_MESSAGE_PAYLOAD_BYTES).maxBytes).toBe(256 * (128 * 1024 + DLQ_HEADROOM_BYTES));
    expect(defaultWorkRetention(64 * 1024 ** 2).maxBytes).toBe(1024 ** 3);

    const id = uniqueName("default");
    await sync.queue<string>({ id }).ready();
    const { work, deadLetters } = await streamsOf("queue", id);
    expect(work.max_bytes).toBe(256 * (128 * 1024 + DLQ_HEADROOM_BYTES));
    expect(work.max_age).toBe(nanos(7 * 24 * 60 * 60 * 1_000));
    expect(work.discard).toBe("old");
    expect(deadLetters.max_bytes).toBe(work.max_bytes);

    const small = uniqueName("default-small");
    await sync.queue<string>({ id: small, maxPayloadBytes: 8 * 1024 }).ready();
    expect((await streamsOf("queue", small)).work.max_bytes).toBe(256 * (8 * 1024 + DLQ_HEADROOM_BYTES));
  }, 30_000);

  test("deadLetterRetention sizes a queue's dead-letter stream independently", async () => {
    const id = uniqueName("dlq-size");
    await sync.queue<string>({
      id,
      retention: { maxAgeMs: 3_600_000, maxBytes: 8 * 1024 ** 2 },
      deadLetterRetention: { maxAgeMs: 600_000, maxBytes: 1024 ** 2 },
    }).ready();
    const { work, deadLetters } = await streamsOf("queue", id);
    expect([work.max_bytes, work.max_age]).toEqual([8 * 1024 ** 2, nanos(3_600_000)]);
    expect([deadLetters.max_bytes, deadLetters.max_age]).toEqual([1024 ** 2, nanos(600_000)]);
    expect(() => sync.queue<string>({ id: uniqueName("dlq-tiny"), deadLetterRetention: { maxBytes: 1_024 } })).toThrow(RangeError);
  }, 30_000);

  test("a job's dead-letter stream takes its byte limit from deadLetterRetention and its age from terminalRetentionMs", async () => {
    const id = uniqueName("job-dlq-size");
    await sync.job<string>({ id, terminalRetentionMs: 600_000, deadLetterRetention: { maxBytes: 1024 ** 2 } }).ready();
    const { work, deadLetters } = await streamsOf("job", id);
    expect(work.max_bytes).toBe(256 * (128 * 1024 + DLQ_HEADROOM_BYTES));
    expect([deadLetters.max_bytes, deadLetters.max_age]).toEqual([1024 ** 2, nanos(600_000)]);
  }, 30_000);
});

describe("whenFull: reject", () => {
  const rejecting = (id: string, retention: { maxBytes: number; maxMessages?: number }) => ({
    id,
    whenFull: "reject" as const,
    maxPayloadBytes: 1_024,
    retention: { maxAgeMs: 3_600_000, ...retention },
  });

  test("a full queue refuses new work, keeps every accepted message, and still dedupes", async () => {
    const id = uniqueName("reject");
    const queue = sync.queue<string>(rejecting(id, { maxBytes: 4 * 1024 }));
    const { accepted, error } = await fill((n) => queue.send({ data: String(n).padEnd(200, "."), idempotencyKey: `m-${n}` }));
    expect(accepted).toBeGreaterThan(0);
    expect(error).toBeInstanceOf(StoreFullError);
    expect((error as Error).message).toContain("retention.maxBytes");

    // A retried send of accepted work is a duplicate, not a refusal.
    expect((await queue.send({ data: "0".padEnd(200, "."), idempotencyKey: "m-0" })).duplicate).toBe(true);
    await expect(queue.sendBatch([{ data: "a".repeat(700) }, { data: "b".repeat(700) }])).rejects.toBeInstanceOf(StoreFullError);
    await expect(queue.send({ data: "later", delayMs: 5_000 })).rejects.toBeInstanceOf(SyncUsageError);

    const { work } = await streamsOf("queue", id);
    expect(work.discard).toBe("new");
    expect(work.allow_msg_schedules ?? false).toBe(false);

    // Another process declaring the same queue sees no drift.
    const peer = createSync({ connection: nc, namespace, application: "tests" });
    await peer.queue<string>(rejecting(id, { maxBytes: 4 * 1024 })).ready();
    await peer.drain({ timeoutMs: 1_000 });

    const handled: string[] = [];
    const worker = await queue.process({ concurrency: 4 }, async (message) => {
      handled.push(message.data);
    });
    await waitFor(() => handled.length >= accepted, 15_000);
    await Bun.sleep(300);
    expect(handled.toSorted()).toEqual(Array.from({ length: accepted }, (_, n) => String(n).padEnd(200, ".")).toSorted());
    await worker.drain();
    // Room again once the work is done.
    expect((await queue.send({ data: "after" })).duplicate).toBe(false);
  }, 30_000);

  test("retention.maxMessages refuses the next message", async () => {
    const queue = sync.queue<number>(rejecting(uniqueName("reject-count"), { maxBytes: 1024 ** 2, maxMessages: 3 }));
    const { accepted, error } = await fill((n) => queue.send({ data: n }));
    expect(accepted).toBe(3);
    expect(error).toBeInstanceOf(StoreFullError);
    expect((error as Error).message).toContain("retention.maxMessages");
  }, 30_000);

  test("an existing discard_oldest queue cannot switch in place", async () => {
    const id = uniqueName("switch");
    await sync.queue<string>({ id, retention: { maxAgeMs: 3_600_000, maxBytes: 64 * 1024 } }).ready();
    const peer = createSync({ connection: nc, namespace, application: "tests" });
    const drifted = peer.queue<string>({ id, whenFull: "reject", retention: { maxAgeMs: 3_600_000, maxBytes: 64 * 1024 } });
    await expect(drifted.ready()).rejects.toBeInstanceOf(ResourceDriftError);
    await peer.drain({ timeoutMs: 1_000 });
    expect(() => sync.queue<string>({ id: uniqueName("bad"), whenFull: "drop" as "reject" })).toThrow(RangeError);
  }, 30_000);

  test("a refused coalesced job does not keep its input for the key", async () => {
    const jobs = sync.job<string>(rejecting(uniqueName("reject-job"), { maxBytes: 1024 ** 2, maxMessages: 1 }));
    await jobs.submit({ key: "first", input: "first" });
    await expect(jobs.submit({ key: "task", input: "refused", coalesce: true })).rejects.toBeInstanceOf(StoreFullError);
    await expect(jobs.submit({ key: "later", input: "later", delayMs: 5_000 })).rejects.toBeInstanceOf(SyncUsageError);

    const inputs: string[] = [];
    const continuationErrors: unknown[] = [];
    const worker = await jobs.process({ concurrency: 1 }, async (context) => {
      inputs.push(context.input);
      if (context.input === "retry") {
        try {
          context.resubmit({ delayMs: 1_000 });
        } catch (error) {
          continuationErrors.push(error);
        }
      }
    });
    await waitFor(() => inputs.length === 1, 10_000);
    // The refused generation is gone: the next coalesced submission runs its own input.
    const accepted = await jobs.submit({ key: "task", input: "retry", coalesce: true });
    expect(accepted.duplicate).toBe(false);
    await waitFor(() => inputs.length === 2, 10_000);
    expect(inputs).toEqual(["first", "retry"]);
    expect(continuationErrors).toHaveLength(1);
    expect(continuationErrors[0]).toBeInstanceOf(SyncUsageError);
    await worker.drain();
  }, 30_000);
});
