import { describe, expect, test } from "bun:test";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { Kvm } from "@nats-io/kv";
import type { NatsConnection } from "@nats-io/nats-core";
import { createSync, SyncUsageError } from "../index.ts";
import type { Sync } from "../index.ts";
import { connectToCluster } from "./cluster.ts";
import { cleanupNamespaces, testNamespace, waitFor } from "./helpers.ts";

type Fixture = {
  nc: NatsConnection;
  sync: Sync;
  peer(): Promise<Sync>;
};

const withSync = async (run: (fixture: Fixture) => Promise<void>): Promise<void> => {
  const namespace = testNamespace();
  const connections: NatsConnection[] = [];
  const instances: Sync[] = [];
  const peer = async (): Promise<Sync> => {
    const connection = await connectToCluster({ name: "job-coalescing-regression" });
    connections.push(connection);
    const sync = createSync({ connection, namespace, application: "tests" });
    instances.push(sync);
    await sync.ready();
    return sync;
  };
  const sync = await peer();
  const nc = connections[0]!;
  const request = nc.request;
  try {
    await run({ nc, sync, peer });
  } finally {
    // Fault injection belongs only to this test's caller-owned connection.
    nc.request = request;
    await Promise.all(instances.map((instance) => instance.drain({ timeoutMs: 2_000 })));
    await cleanupNamespaces(nc, [namespace]);
    await Promise.all(connections.map((connection) => connection.close()));
  }
};

const isWork = (subject: string): boolean => subject.startsWith("sync.v6.") && /\.work(?:\.p\d+)?$/.test(subject);

const isReceipt = (payload: Parameters<NatsConnection["request"]>[1]): boolean => {
  if (payload === undefined) return false;
  const value: unknown = JSON.parse(typeof payload === "string" ? payload : new TextDecoder().decode(payload));
  return typeof value === "object" && value !== null && "seq" in value && !("deliverySeq" in value);
};

// 6.2.0 persisted { pending: true }, then { jobId, seq }; its work envelope
// carried ext.coalesce + ext.messageId without a coalesceGeneration field.
const legacyStorage = async ({ nc, sync }: Fixture, id: string) => {
  const resource = (await sync.resources()).find((entry) => entry.kind === "job" && entry.id === id);
  const workStream = resource?.natsNames.find((name) => name.startsWith("S6_J_"));
  const claimsStream = resource?.natsNames.find((name) => name.startsWith("KV_"));
  if (workStream === undefined || claimsStream === undefined) throw new Error("legacy test job was not provisioned");
  const jsm = await jetstreamManager(nc);
  const workFilter = (await jsm.streams.info(workStream)).config.subjects.find((subject) => subject.endsWith(".work"));
  if (workFilter === undefined) throw new Error("legacy test job has no work subject");
  const tenantToken = Buffer.from("default").toString("base64url");
  const key = `c.${tenantToken}.${Buffer.from("same").toString("base64url")}`;
  const claims = await new Kvm(nc).open(claimsStream.slice(3));
  const js = jetstream(nc);
  return {
    claims,
    key,
    readClaim: () => jsm.streams.getMessage(claimsStream, {
      last_by_subj: `$KV.${claimsStream.slice(3)}.${key}`,
    }),
    workCount: async () => (await jsm.streams.info(workStream)).state.messages,
    publish: async (value: string) => {
      const jobId = crypto.randomUUID();
      const ack = await js.publish(workFilter.replace("*", tenantToken), JSON.stringify({
        v: 6,
        data: { value },
        tenantId: "default",
        publishedAt: new Date().toISOString(),
        ext: { key: "same", coalesce: "1", messageId: jobId },
      }), { msgID: jobId });
      return { jobId, seq: ack.seq };
    },
  };
};

describe("coalesced job failure windows", () => {
  test("a later publisher recovers the original input after interruption before publication", () => withSync(async ({ nc, sync, peer }) => {
    const jobs = sync.job<{ value: string }>({ id: "unpublished" });
    const request = nc.request.bind(nc);
    let interrupted = false;
    nc.request = async (subject, payload, options) => {
      if (!interrupted && isWork(subject)) {
        interrupted = true;
        throw new Error("publisher interrupted before work publication");
      }
      return request(subject, payload, options);
    };
    await expect(jobs.submit({ key: "same", input: { value: "original" }, coalesce: true }))
      .rejects.toThrow("publisher interrupted");
    expect(interrupted).toBe(true);

    const otherJobs = (await peer()).job<{ value: string }>({ id: "unpublished" });
    const recovered = await otherJobs.submit({ key: "same", input: { value: "replacement" }, coalesce: true });
    expect(recovered.duplicate).toBe(true);
    expect(recovered.jobId.length).toBeGreaterThan(0);
    expect(recovered.streamSequence).toBeGreaterThan(0);
    const seen: { value: string; jobId: string }[] = [];
    const worker = await otherJobs.process({}, async (context) => {
      seen.push({ value: context.input.value, jobId: context.jobId });
    });
    await waitFor(() => seen.length === 1);
    await worker.drain();
    expect(seen).toEqual([{ value: "original", jobId: recovered.jobId }]);
    expect((await otherJobs.submit({ key: "same", input: { value: "next" }, coalesce: true })).duplicate).toBe(false);
  }), 30_000);

  test("publication retried beyond the broker dedupe window executes one physical delivery", () => withSync(async ({ nc, sync }) => {
    const jobs = sync.job<{ value: string }>({ id: "lost-ack", dedupeWindowMs: 1_000 });
    const request = nc.request.bind(nc);
    let lostAck = false;
    nc.request = async (subject, payload, options) => {
      const response = await request(subject, payload, options);
      if (!lostAck && isWork(subject)) {
        lostAck = true;
        throw new Error("publication succeeded but its acknowledgement was lost");
      }
      return response;
    };
    await expect(jobs.submit({ key: "same", input: { value: "original" }, coalesce: true }))
      .rejects.toThrow("acknowledgement was lost");
    await Bun.sleep(1_200);
    const recovered = await jobs.submit({ key: "same", input: { value: "replacement" }, coalesce: true });
    expect(recovered.duplicate).toBe(true);
    expect(recovered.streamSequence).toBeGreaterThan(1);
    const seen: string[] = [];
    const worker = await jobs.process({ concurrency: 4 }, async (context) => {
      seen.push(context.input.value);
      await Bun.sleep(100);
    });
    await waitFor(() => seen.length >= 1);
    await Bun.sleep(400);
    await worker.drain();
    expect(seen).toEqual(["original"]);
    expect((await jobs.submit({ key: "same", input: { value: "next" }, coalesce: true })).duplicate).toBe(false);
  }), 30_000);

  test("a helper publishing first still leaves one accepted submission", () => withSync(async ({ nc, sync, peer }) => {
    const jobs = sync.job<{ value: string }>({ id: "helper-first" });
    const releasePublisher = Promise.withResolvers<void>();
    const request = nc.request.bind(nc);
    let held = false;
    nc.request = async (subject, payload, options) => {
      if (!held && isWork(subject)) {
        held = true;
        await releasePublisher.promise;
      }
      return request(subject, payload, options);
    };
    const firstSubmission = jobs.submit({ key: "same", input: { value: "original" }, coalesce: true });
    try {
      await waitFor(() => held);
      const otherJobs = (await peer()).job<{ value: string }>({ id: "helper-first" });
      const helper = await otherJobs.submit({ key: "same", input: { value: "replacement" }, coalesce: true });
      expect(helper.duplicate).toBe(true);
      releasePublisher.resolve();
      const first = await firstSubmission;
      expect(first.duplicate).toBe(false);
      expect(first.jobId).toBe(helper.jobId);
      expect(first.streamSequence).toBe(helper.streamSequence);
    } finally {
      releasePublisher.resolve();
      await firstSubmission;
    }
  }), 30_000);

  test("a late publication receipt cannot overwrite a generation created after completion", () => withSync(async ({ nc, sync }) => {
    const jobs = sync.job<{ value: string }>({ id: "late-receipt" });
    const releaseReceipt = Promise.withResolvers<void>();
    const request = nc.request.bind(nc);
    let held = false;
    nc.request = async (subject, payload, options) => {
      if (!held && subject.startsWith("$KV.") && payload?.length && isReceipt(payload)) {
        held = true;
        await releaseReceipt.promise;
      }
      return request(subject, payload, options);
    };
    const firstSubmission = jobs.submit({ key: "same", input: { value: "first" }, coalesce: true });
    try {
      await waitFor(() => held);
      const seen: string[] = [];
      const worker = await jobs.process({}, async (context) => {
        seen.push(context.input.value);
      });
      await waitFor(() => seen.length === 1);
      await worker.drain();
      const next = await jobs.submit({ key: "same", input: { value: "next" }, coalesce: true });
      expect(next.duplicate).toBe(false);
      releaseReceipt.resolve();
      const first = await firstSubmission;
      expect(first.jobId).not.toBe(next.jobId);
      const duplicate = await jobs.submit({ key: "same", input: { value: "discarded" }, coalesce: true });
      expect(duplicate.duplicate).toBe(true);
      expect(duplicate.jobId).toBe(next.jobId);
      expect(duplicate.streamSequence).toBe(next.streamSequence);
      const secondWorker = await jobs.process({}, async (context) => {
        seen.push(context.input.value);
      });
      await waitFor(() => seen.length === 2);
      await secondWorker.drain();
      expect(seen).toEqual(["first", "next"]);
    } finally {
      releaseReceipt.resolve();
      await firstSubmission;
    }
  }), 30_000);

  test("submitMany coalesces concurrent keys and counts duplicates per tenant", () => withSync(async ({ sync }) => {
    const jobs = sync.job<{ value: string }>({ id: "many" });
    const first = await jobs.submit({ key: "existing", input: { value: "original" }, coalesce: true });
    const batch = [
      ...Array.from({ length: 12 }, (_, i) => ({ key: "existing", input: { value: `ignored:${i}` }, coalesce: true })),
      ...Array.from({ length: 12 }, (_, i) => ({ key: "new", input: { value: `new:${i}` }, coalesce: true })),
      { key: "existing", tenantId: "other", input: { value: "other-tenant" }, coalesce: true },
    ];
    expect(await jobs.submitMany(batch, { publishConcurrency: 8, maxPendingBytes: 2_048 }))
      .toEqual({ accepted: 2, duplicates: 23 });
    expect((await jobs.submit({ key: "existing", input: { value: "ignored" }, coalesce: true })).jobId).toBe(first.jobId);
    const seen: string[] = [];
    const worker = await jobs.process({ concurrency: 8 }, async (context) => {
      seen.push(context.input.value);
    });
    await waitFor(() => seen.length >= 3);
    await Bun.sleep(200);
    await worker.drain();
    expect(seen).toHaveLength(3);
    expect(seen).toContain("original");
    expect(seen).toContain("other-tenant");
    expect(seen.filter((value) => value.startsWith("new:"))).toHaveLength(1);
  }), 30_000);

  test("heartbeats preserve a running claim when work retention is shorter than ackWait", () => withSync(async ({ sync }) => {
    const jobs = sync.job<{ value: string }>({
      id: "heartbeat",
      retention: { maxAgeMs: 1_000, maxBytes: 1_024 * 1_024 },
      dedupeWindowMs: 500,
      delivery: { ackWaitMs: 5_000 },
    });
    const finish = Promise.withResolvers<void>();
    let heartbeats = 0;
    let running = false;
    const worker = await jobs.process({}, async (context) => {
      running = true;
      while (running && !context.signal.aborted) {
        await context.heartbeat();
        heartbeats += 1;
        await Promise.race([Bun.sleep(150), finish.promise]);
      }
    });
    try {
      const first = await jobs.submit({ key: "same", input: { value: "first" }, coalesce: true });
      await waitFor(() => heartbeats >= 16);
      const duplicate = await jobs.submit({ key: "same", input: { value: "second" }, coalesce: true });
      expect(duplicate.duplicate).toBe(true);
      expect(duplicate.jobId).toBe(first.jobId);
      running = false;
      finish.resolve();
      await worker.drain();
      expect((await jobs.submit({ key: "same", input: { value: "after" }, coalesce: true })).duplicate).toBe(false);
    } finally {
      running = false;
      finish.resolve();
      await worker.drain();
    }
  }), 30_000);

  test("coalesced continuations retain partition ordering and keep their key claimed", () => withSync(async ({ sync }) => {
    const jobs = sync.job<{ page: number }>({ id: "partitioned", ordering: { mode: "partitioned", partitions: 2 } });
    let firstStarted = false;
    let secondStarted = false;
    const finishFirst = Promise.withResolvers<void>();
    const finishSecond = Promise.withResolvers<void>();
    const seen: string[] = [];
    let active = 0;
    let maxActive = 0;
    const worker = await jobs.process({ concurrency: 4 }, async (context) => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      seen.push(`${context.key}:${context.input.page}`);
      try {
        if (context.key !== "chain") return;
        if (context.input.page === 1) {
          firstStarted = true;
          await finishFirst.promise;
        }
        if (context.input.page === 2) {
          secondStarted = true;
          await finishSecond.promise;
        }
        if (context.input.page < 3) context.resubmit({ input: { page: context.input.page + 1 } });
      } finally {
        active -= 1;
      }
    });
    try {
      await jobs.submit({ key: "chain", input: { page: 1 }, orderingKey: "partition", coalesce: true });
      await waitFor(() => firstStarted);
      await jobs.submit({ key: "other", input: { page: 1 }, orderingKey: "partition" });
      finishFirst.resolve();
      await waitFor(() => secondStarted);
      expect((await jobs.submit({ key: "chain", input: { page: 999 }, orderingKey: "partition", coalesce: true })).duplicate).toBe(true);
      finishSecond.resolve();
      await waitFor(() => seen.includes("chain:3"));
      await worker.drain();
      expect(seen).toEqual(["chain:1", "other:1", "chain:2", "chain:3"]);
      expect(maxActive).toBe(1);
      expect((await jobs.submit({ key: "chain", input: { page: 4 }, orderingKey: "partition", coalesce: true })).duplicate).toBe(false);
    } finally {
      finishFirst.resolve();
      finishSecond.resolve();
      await worker.drain();
    }
  }), 30_000);

  test("DLQ requeue joins an active generation and otherwise reserves a fresh generation", () => withSync(async ({ sync }) => {
    const jobs = sync.job<{ value: string }>({
      id: "requeue",
      delivery: { maxAttempts: 1, backoffMs: [100], ackWaitMs: 5_000 },
    });
    const failedWorker = await jobs.process({}, async () => { throw new Error("expected failure"); });
    await jobs.submit({ key: "same", input: { value: "failed" }, coalesce: true });
    await waitFor(async () => (await jobs.deadLetters.list()).length === 1);
    await failedWorker.drain();
    const firstDead = (await jobs.deadLetters.list())[0]!;
    const current = await jobs.submit({ key: "same", input: { value: "current" }, coalesce: true });
    const joined = await jobs.deadLetters.requeue({ messageId: firstDead.messageId, idempotencyKey: "requeue-active" });
    expect(joined.duplicate).toBe(true);
    expect(joined.messageId).toBe(current.messageId);
    expect(await jobs.deadLetters.list()).toHaveLength(0);

    const seen: string[] = [];
    const worker = await jobs.process({}, async (context) => { seen.push(context.input.value); });
    await waitFor(() => seen.length === 1);
    await worker.drain();
    expect(seen).toEqual(["current"]);

    const failingAgain = await jobs.process({}, async () => { throw new Error("expected failure again"); });
    await jobs.submit({ key: "same", input: { value: "recover-me" }, coalesce: true });
    await waitFor(async () => (await jobs.deadLetters.list()).length === 1);
    await failingAgain.drain();
    const secondDead = (await jobs.deadLetters.list())[0]!;
    const requeued = await jobs.deadLetters.requeue({ messageId: secondDead.messageId, idempotencyKey: "requeue-fresh" });
    expect(requeued.duplicate).toBe(false);
    const duplicate = await jobs.submit({ key: "same", input: { value: "discarded" }, coalesce: true });
    expect(duplicate.duplicate).toBe(true);
    expect(duplicate.jobId).toBe(requeued.messageId);
    const recoveryWorker = await jobs.process({}, async (context) => { seen.push(context.input.value); });
    await waitFor(() => seen.length === 2);
    await recoveryWorker.drain();
    expect(seen).toEqual(["current", "recover-me"]);
    expect((await jobs.submit({ key: "same", input: { value: "next" }, coalesce: true })).duplicate).toBe(false);
  }), 30_000);

  test("an interrupted continuation publication is repaired without rerunning its completed parent", () => withSync(async ({ nc, sync }) => {
    const jobs = sync.job<{ page: number }>({
      id: "continuation-repair",
      ordering: { mode: "partitioned", partitions: 1 },
      delivery: { ackWaitMs: 5_000, backoffMs: [100], maxAttempts: 3 },
    });
    const request = nc.request.bind(nc);
    let workPublications = 0;
    nc.request = async (subject, payload, options) => {
      if (isWork(subject) && ++workPublications === 2) {
        throw new Error("continuation publisher interrupted");
      }
      return request(subject, payload, options);
    };
    const pages: number[] = [];
    const errors: string[] = [];
    const worker = await jobs.process({
      onError: async ({ error }) => {
        errors.push(error.message);
        return { action: "dead_letter", reason: "completed parent must not fail" };
      },
    }, async (context) => {
      pages.push(context.input.page);
      if (context.input.page === 1) context.resubmit({ input: { page: 2 } });
    });
    await jobs.submit({ key: "same", input: { page: 1 }, orderingKey: "partition", coalesce: true });
    await waitFor(() => pages.includes(2));
    await worker.drain();
    expect(workPublications).toBe(3);
    expect(pages).toEqual([1, 2]);
    expect(errors).toEqual([]);
    expect(await jobs.deadLetters.list()).toHaveLength(0);
    expect((await jobs.submit({ key: "same", input: { page: 3 }, orderingKey: "partition", coalesce: true })).duplicate).toBe(false);
  }), 30_000);

  test("a repeated DLQ request dedupes completed work and releases its unused new claim", () => withSync(async ({ nc, sync }) => {
    const jobs = sync.job<{ value: string }>({
      id: "requeue-request",
      delivery: { maxAttempts: 1, backoffMs: [100], ackWaitMs: 5_000 },
    });
    const failingWorker = await jobs.process({}, async () => { throw new Error("expected failure"); });
    await jobs.submit({ key: "same", input: { value: "recover-me" }, coalesce: true });
    await waitFor(async () => (await jobs.deadLetters.list()).length === 1);
    await failingWorker.drain();
    const dead = (await jobs.deadLetters.list())[0]!;
    const request = nc.request.bind(nc);
    let deleteFailed = false;
    nc.request = async (subject, payload, options) => {
      if (subject.includes(".STREAM.MSG.DELETE.S6_JD_")) {
        deleteFailed = true;
        throw new Error("DLQ cleanup interrupted after successful requeue");
      }
      return request(subject, payload, options);
    };
    const first = await jobs.deadLetters.requeue({ messageId: dead.messageId, idempotencyKey: "same-request" });
    nc.request = request;
    expect(deleteFailed).toBe(true);
    expect(first.duplicate).toBe(false);
    expect(await jobs.deadLetters.list()).toHaveLength(1);
    const seen: string[] = [];
    const worker = await jobs.process({}, async (context) => { seen.push(context.input.value); });
    await waitFor(() => seen.length === 1);
    await worker.drain();

    const again = await jobs.deadLetters.requeue({ messageId: dead.messageId, idempotencyKey: "same-request" });
    expect(again.duplicate).toBe(true);
    expect(again.messageId).toBe(first.messageId);
    expect(again.streamSequence).toBe(first.streamSequence);
    expect(await jobs.deadLetters.list()).toHaveLength(0);
    expect(seen).toEqual(["recover-me"]);
    expect((await jobs.submit({ key: "same", input: { value: "next" }, coalesce: true })).duplicate).toBe(false);
  }), 30_000);
});

describe("6.2.0 coalesced job upgrade", () => {
  for (const claimState of ["pending", "receipt"] as const) {
    test(`a queued legacy envelope adopts its ${claimState} claim and releases it on completion`, () => withSync(async (fixture) => {
      const jobs = fixture.sync.job<{ value: string }>({ id: `legacy-${claimState}` });
      await jobs.ready();
      const legacy = await legacyStorage(fixture, `legacy-${claimState}`);
      await legacy.claims.create(legacy.key, JSON.stringify({ pending: true }), "604800s");
      const original = await legacy.publish("original");
      if (claimState === "receipt") await legacy.claims.put(legacy.key, JSON.stringify(original));
      const finish = Promise.withResolvers<void>();
      const seen: { value: string; jobId: string }[] = [];
      const worker = await jobs.process({}, async (context) => {
        seen.push({ value: context.input.value, jobId: context.jobId });
        await finish.promise;
      });
      try {
        await waitFor(() => seen.length === 1);
        const duplicate = await jobs.submit({ key: "same", input: { value: "discarded" }, coalesce: true });
        expect(duplicate.duplicate).toBe(true);
        expect(duplicate.jobId).toBe(original.jobId);
        expect(duplicate.streamSequence).toBe(original.seq);
        finish.resolve();
        await worker.drain();
        expect(seen).toEqual([{ value: "original", jobId: original.jobId }]);
        expect((await jobs.submit({ key: "same", input: { value: "next" }, coalesce: true })).duplicate).toBe(false);
      } finally {
        finish.resolve();
        await worker.drain();
      }
    }), 30_000);
  }

  test("an orphan legacy pending claim rejects submission without deleting it or inventing a receipt", () => withSync(async (fixture) => {
    const jobs = fixture.sync.job<{ value: string }>({ id: "legacy-orphan" });
    await jobs.ready();
    const legacy = await legacyStorage(fixture, "legacy-orphan");
    const pending = JSON.stringify({ pending: true });
    const revision = await legacy.claims.create(legacy.key, pending, "604800s");
    const failure = await jobs.submit({ key: "same", input: { value: "replacement" }, coalesce: true })
      .then(() => null, (error: unknown) => error);
    expect(failure).toBeInstanceOf(SyncUsageError);
    if (!(failure instanceof Error)) throw new Error("legacy pending submission must fail");
    expect(failure.message).toContain("legacy pending coalesced key same");
    expect(failure.message).toContain("no recoverable input");
    expect(failure.message).toContain("stop 6.2.0 writers and resolve the old submission");
    const stored = await legacy.readClaim();
    expect(stored?.seq).toBe(revision);
    expect(stored === null ? null : new TextDecoder().decode(stored.data)).toBe(pending);
    expect(await legacy.workCount()).toBe(0);
  }), 30_000);

  test("an old delivery cannot adopt or release a legacy receipt belonging to another job", () => withSync(async (fixture) => {
    const jobs = fixture.sync.job<{ value: string }>({ id: "legacy-superseded" });
    await jobs.ready();
    const legacy = await legacyStorage(fixture, "legacy-superseded");
    const old = await legacy.publish("obsolete");
    const current = { jobId: crypto.randomUUID(), seq: old.seq + 1 };
    const receipt = JSON.stringify(current);
    const revision = await legacy.claims.put(legacy.key, receipt);
    const seen: string[] = [];
    const worker = await jobs.process({}, async (context) => { seen.push(context.input.value); });
    await waitFor(async () => (await legacy.workCount()) === 0);
    await worker.drain();
    expect(seen).toEqual([]);
    const stored = await legacy.readClaim();
    expect(stored?.seq).toBe(revision);
    expect(stored === null ? null : new TextDecoder().decode(stored.data)).toBe(receipt);
    const duplicate = await jobs.submit({ key: "same", input: { value: "replacement" }, coalesce: true });
    expect(duplicate.duplicate).toBe(true);
    expect(duplicate.jobId).toBe(current.jobId);
    expect(duplicate.streamSequence).toBe(current.seq);
    expect(await jobs.deadLetters.list()).toHaveLength(0);
  }), 30_000);
});
