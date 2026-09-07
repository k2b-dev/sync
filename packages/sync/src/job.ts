import type { KV } from "@nats-io/kv";
import type { JsMsg } from "@nats-io/jetstream";
import { JetStreamApiError } from "@nats-io/jetstream";
import { decodeEnvelope, decodeJson, encodeJson, extString } from "./codec.ts";
import type { Envelope, JsonValue } from "./codec.ts";
import { BatchSubmitError, SyncUsageError, asError } from "./errors.ts";
import { isCasConflict, kvCasPut } from "./kv.ts";
import { assertName, kvBucketName, resourceIdentity, streamName, subjectToken } from "./naming.ts";
import { ensureKv, toStorageType } from "./resources.ts";
import { createQueueCore } from "./queue.ts";
import type { BatchReceipt, DeadLetterStore, PauseInfo, QueueConfig, QueueCore } from "./queue.ts";
import type { SyncRuntime } from "./runtime.ts";
import type { MessageMeta, PublishReceipt } from "./types.ts";
import { DEFAULT_MESSAGE_PAYLOAD_BYTES, DLQ_HEADROOM_BYTES } from "./types.ts";
import type { ProcessOptions, Worker } from "./worker.ts";

// ==========================
// Types
// ==========================

export type JobConfig = QueueConfig & {
  /** Retention for diagnostics and dead letters. Default 7 days. */
  terminalRetentionMs?: number;
};

export type JobSubmit<Input> = {
  /** Required idempotent submission key (the NATS message ID within the dedupe window). */
  key: string;
  input: Input;
  /**
   * Coalescing dedupe: at most one queued-or-running job per key; the key is
   * released when the job settles (success or dead letter) — not after a time
   * window. Coalesced submissions skip the windowed NATS dedupe, so a key can
   * be resubmitted immediately after completion.
   */
  coalesce?: boolean;
  tenantId?: string;
  delayMs?: number;
  at?: Date;
  orderingKey?: string;
  meta?: MessageMeta;
};

export type JobContext<Input> = {
  jobId: string;
  key: string;
  input: Input;
  attempt: number;
  failureCount: number;
  signal: AbortSignal;
  heartbeat(): Promise<void>;
  /**
   * Request a continuation: after this run settles successfully, Sync submits
   * the same key again (fresh input if given) BEFORE acknowledging — a crash
   * in between redelivers and re-requests, so continuations are at-least-once
   * and never lost. Bypasses the dedupe window; with coalesce the claim is
   * carried over instead of released.
   */
  resubmit(options?: { delayMs?: number; input?: Input }): void;
};

export type JobFailureDecision = { action: "retry"; delayMs?: number } | { action: "dead_letter"; reason: string };

export type JobProcessOptions<Input> = ProcessOptions & {
  onError?: (input: { context: JobContext<Input>; error: Error }) => JobFailureDecision | Promise<JobFailureDecision>;
};

export type JobSubmitManyOptions = {
  /** Bounded number of in-flight publish promises. Default 16. */
  publishConcurrency?: number;
  /** Bounded bytes of in-flight publishes — local backpressure. Default 8 MiB. */
  maxPendingBytes?: number;
  signal?: AbortSignal;
};

export type Job<Input> = {
  ready(): Promise<void>;
  submit(job: JobSubmit<Input>): Promise<PublishReceipt & { jobId: string }>;
  submitMany(
    jobs: Iterable<JobSubmit<Input>> | AsyncIterable<JobSubmit<Input>>,
    options?: JobSubmitManyOptions,
  ): Promise<{ accepted: number; duplicates: number }>;
  /**
   * Atomic all-or-nothing submission of up to 1000 jobs (no delays). Unlike
   * submit(), keys are NOT deduplicated (NATS batches carry no message ids) —
   * resubmitting a committed batch duplicates its jobs.
   */
  submitBatch(jobs: JobSubmit<Input>[]): Promise<BatchReceipt>;
  /** Pause delivery on the durable consumer — global, all pods. Submissions continue. */
  pause(options?: { untilMs?: number }): Promise<PauseInfo>;
  resume(): Promise<PauseInfo>;
  process(options: JobProcessOptions<Input>, handler: (context: JobContext<Input>) => Promise<void>): Promise<Worker>;
  deadLetters: DeadLetterStore<{ key: string; input: Input }>;
};

// ==========================
// Job factory
// ==========================

/** Pending input is durable: another submitter or a parent redelivery can finish publishing it. */
type CoalescedClaim<Input> = {
  generation: string;
  input: Input;
  at?: string;
  orderingKey?: string;
  meta?: MessageMeta;
  parent?: string;
  /** Only DLQ requeue uses an explicit windowed request key. */
  publishKey?: string;
  jobId: string;
  seq?: number;
  /** One physical delivery wins, even when publication is retried beyond NATS's dedupe window. */
  deliverySeq?: number;
};

type LegacyClaim = { pending?: boolean; jobId?: string; seq?: number };

export const createJob = <Input>(runtime: SyncRuntime, config: JobConfig): Job<Input> => {
  const terminalRetentionMs = config.terminalRetentionMs ?? 7 * 24 * 60 * 60 * 1_000;
  const retentionMs = config.retention?.maxAgeMs ?? 7 * 24 * 60 * 60 * 1_000;
  const claimRetentionMs = Math.max(retentionMs, config.delivery?.ackWaitMs ?? 30_000);
  // Input still passes the ordinary work-envelope limit. Persisted coordination
  // fields use the same framework headroom already reserved for dead letters.
  const claimMaxBytes = (config.maxPayloadBytes ?? DEFAULT_MESSAGE_PAYLOAD_BYTES) + DLQ_HEADROOM_BYTES;
  const identity = resourceIdentity(runtime.namespace, "job", config.id);
  const claimsBucket = kvBucketName(identity);
  let claims: KV | null = null;

  const claimKey = (tenantId: string, key: string): string =>
    `c.${subjectToken(tenantId, "tenantId")}.${subjectToken(key, "job key")}`;

  const getClaims = async (): Promise<KV> => {
    await core.declarationReady();
    if (claims === null) {
      const ctx = await runtime.context();
      claims = await ctx.kvm.open(claimsBucket);
    }
    return claims;
  };

  const loadClaim = async (tenantId: string, key: string) => {
    await getClaims();
    const ctx = await runtime.context();
    try {
      // KV direct reads can be served by a lagging follower. Admission must
      // never acknowledge accepted work based on a stale missing generation.
      const entry = await ctx.jsm.streams.getMessage(`KV_${claimsBucket}`, {
        last_by_subj: `$KV.${claimsBucket}.${claimKey(tenantId, key)}`,
      });
      if (entry === null) return { claim: null, revision: 0 };
      return {
        claim: entry.header.get("KV-Operation") === ""
          ? decodeJson<CoalescedClaim<Input> | LegacyClaim>(entry.data)
          : null,
        revision: entry.seq,
      };
    } catch (error) {
      if (!(error instanceof JetStreamApiError) || error.code !== 10037) throw error;
      return { claim: null, revision: 0 };
    }
  };

  const putClaim = async (tenantId: string, key: string, claim: CoalescedClaim<Input>, revision: number) => {
    const ctx = await runtime.context();
    // A scheduled message may start its work retention after its due time.
    const waitingMs = Math.max(0, (claim.at === undefined ? 0 : Date.parse(claim.at)) - Date.now());
    return kvCasPut(ctx, claimsBucket, claimKey(tenantId, key),
      encodeJson(`job ${config.id} claim`, claim, claimMaxBytes), revision,
      { ttl: `${Math.max(1, Math.ceil((claimRetentionMs + waitingMs) / 1_000))}s` });
  };

  const generationOf = (envelope: Envelope): string =>
    extString(envelope, "coalesceGeneration") ?? extString(envelope, "messageId") ?? "";

  const heartbeatClaim = async (envelope: Envelope, msg: JsMsg): Promise<void> => {
    const key = extString(envelope, "key") ?? "";
    while (true) {
      const { claim, revision } = await loadClaim(envelope.tenantId, key);
      if (claim === null || !("generation" in claim) || claim.generation !== generationOf(envelope) ||
        claim.deliverySeq !== msg.seq) return;
      if (await putClaim(envelope.tenantId, key, claim, revision) !== null) return;
    }
  };

  /** Release only this generation/delivery, before its acknowledgement. */
  const releaseClaim = async (tenantId: string, key: string, generation: string, deliverySeq?: number): Promise<void> => {
    const store = await getClaims();
    while (true) {
      const { claim, revision } = await loadClaim(tenantId, key);
      if (claim === null || !("generation" in claim) || claim.generation !== generation ||
        (deliverySeq !== undefined && claim.deliverySeq !== deliverySeq)) return;
      try {
        await store.purge(claimKey(tenantId, key), { previousSeq: revision });
        return;
      } catch (error) {
        if (!isCasConflict(error)) throw error;
      }
    }
  };

  const core: QueueCore<Input, { key: string; input: Input }> = createQueueCore<Input, { key: string; input: Input }>(
    runtime,
    {
      ...config,
      dlqMaxAgeMs: terminalRetentionMs,
      extraNatsNames: [`KV_${claimsBucket}`],
      provisionExtra: async (ctx) => {
        claims = await ensureKv(ctx, identity, config.owner ?? runtime.application, claimsBucket, {
          history: 1,
          replicas: config.replicas ?? runtime.defaults.replicas,
          storage: toStorageType(runtime.defaults.storage),
          markerTTL: 1_000,
        });
      },
      admit: (envelope, msg) => admit(envelope, msg),
      heartbeat: (envelope, msg) => extString(envelope, "coalesce") === "1" ? heartbeatClaim(envelope, msg) : Promise.resolve(),
      requeue: async (message, ext): Promise<PublishReceipt> => {
        if (ext.coalesce !== "1") return core.send(message, ext);
        const key = typeof ext.key === "string" ? ext.key : "";
        return submitCoalesced({ key, input: message.data, tenantId: message.tenantId,
          orderingKey: message.orderingKey, meta: message.meta, coalesce: true }, message.idempotencyKey);
      },
      onDeadLetter: async (envelope, msg) => {
        const key = extString(envelope, "key");
        if (envelope !== null && key !== undefined && extString(envelope, "coalesce") === "1") {
          await releaseClaim(envelope.tenantId, key, generationOf(envelope), msg.seq);
        }
      },
    },
    "job",
    (envelope) => ({ key: extString(envelope, "key") ?? "", input: envelope.data as Input }),
  );

  const toSend = (job: JobSubmit<Input>) => {
    assertName(job.key, "job key");
    return {
      message: {
        data: job.input,
        tenantId: job.tenantId,
        // Coalesced submissions dedupe via the claim, not the window — a
        // windowed message id would swallow the resubmit after completion.
        idempotencyKey: job.coalesce === true ? undefined : job.key,
        delayMs: job.delayMs,
        at: job.at,
        orderingKey: job.orderingKey,
        meta: job.meta,
      },
      ext: {
        key: job.key,
        ...(job.coalesce === true ? { coalesce: "1" } : {}),
      } satisfies Record<string, JsonValue>,
    };
  };

  const claimMessage = (tenantId: string, claim: CoalescedClaim<Input>) => ({
    data: claim.input,
    tenantId,
    idempotencyKey: claim.publishKey ?? claim.generation,
    ...(claim.at === undefined ? {} : { at: new Date(claim.at) }),
    ...(claim.orderingKey === undefined ? {} : { orderingKey: claim.orderingKey }),
    ...(claim.meta === undefined ? {} : { meta: claim.meta }),
  });
  const claimExt = (key: string, generation: string) => ({ key, coalesce: "1", coalesceGeneration: generation });

  const prepareClaim = async (job: JobSubmit<Input>, parent?: string, publishKey?: string) => {
    assertName(job.key, "job key");
    if (job.delayMs !== undefined && job.at !== undefined) throw new SyncUsageError("delayMs and at are mutually exclusive");
    const generation = crypto.randomUUID();
    const at = job.at ?? (job.delayMs === undefined ? undefined : new Date(Date.now() + job.delayMs));
    const claim: CoalescedClaim<Input> = {
      generation, input: job.input, jobId: "",
      ...(at === undefined ? {} : { at: at.toISOString() }),
      ...(job.orderingKey === undefined ? {} : { orderingKey: job.orderingKey }),
      ...(job.meta === undefined ? {} : { meta: job.meta }),
      ...(parent === undefined ? {} : { parent }),
      ...(publishKey === undefined ? {} : { publishKey }),
    };
    const prepared = await core.prepareSend(claimMessage(job.tenantId ?? "default", claim), claimExt(job.key, generation));
    claim.jobId = prepared.messageId;
    // Snapshot the accepted input before another caller can help publish it.
    return {
      claim: decodeJson<CoalescedClaim<Input>>(encodeJson(`job ${config.id} claim`, claim, claimMaxBytes)),
      byteLength: prepared.byteLength,
    };
  };

  const publishClaim = async (tenantId: string, key: string, claim: CoalescedClaim<Input>, revision: number): Promise<PublishReceipt> => {
    const receipt = await core.send(claimMessage(tenantId, claim), claimExt(key, claim.generation));
    if (claim.publishKey !== undefined && receipt.duplicate) {
      // A repeated DLQ request can refer to an already completed generation.
      // Requeue is immediate: its physical sequence either still carries this
      // generation, or the newly reserved claim must be released.
      const ctx = await runtime.context();
      let current: Envelope | null;
      try {
        const stored = await ctx.jsm.streams.getMessage(streamName(identity), { seq: receipt.streamSequence });
        current = stored === null ? null : decodeEnvelope(stored.data);
      } catch (error) {
        if (!(error instanceof JetStreamApiError) || error.code !== 10037) throw error;
        current = null;
      }
      if (current === null || generationOf(current) !== claim.generation) {
        await releaseClaim(tenantId, key, claim.generation);
        return receipt;
      }
    }
    // The worker may already have admitted/settled it, or a continuation may
    // own the key. A late receipt is allowed to update only its pending revision.
    await putClaim(tenantId, key, { ...claim, seq: receipt.streamSequence }, revision);
    return receipt;
  };

  const submitCoalesced = async (job: JobSubmit<Input>, publishKey?: string, prepared?: Awaited<ReturnType<typeof prepareClaim>>): Promise<PublishReceipt & { jobId: string }> => {
    const { claim: pending } = prepared ?? await prepareClaim(job, undefined, publishKey);
    const tenantId = job.tenantId ?? "default";
    while (true) {
      const loaded = await loadClaim(tenantId, job.key);
      if (loaded.claim !== null) {
        const claim = loaded.claim;
        if (claim.jobId !== undefined && claim.seq !== undefined) {
          return { jobId: claim.jobId, messageId: claim.jobId, streamSequence: claim.seq, duplicate: true };
        }
        if (!("generation" in claim)) {
          throw new SyncUsageError(`job ${config.id}: legacy pending coalesced key ${job.key} has no recoverable input; stop 6.2.0 writers and resolve the old submission before retrying`);
        }
        const receipt = await publishClaim(tenantId, job.key, claim, loaded.revision);
        return { ...receipt, duplicate: true, jobId: receipt.messageId };
      }
      const revision = await putClaim(tenantId, job.key, pending, loaded.revision);
      if (revision === null) continue;
      // Leave pending input intact on an unknown publish result. A later
      // submit can finish this exact generation rather than lose the job.
      const receipt = await publishClaim(tenantId, job.key, pending, revision);
      return { ...receipt, duplicate: publishKey === undefined ? false : receipt.duplicate, jobId: receipt.messageId };
    }
  };

  const admit = async (envelope: Envelope, msg: JsMsg): Promise<boolean> => {
    if (extString(envelope, "coalesce") !== "1") return true;
    const key = extString(envelope, "key") ?? "";
    const generation = generationOf(envelope);
    while (true) {
      const loaded = await loadClaim(envelope.tenantId, key);
      let claim = loaded.claim;
      if (claim === null || !("generation" in claim)) {
        // Existing 6.2.0 messages carry their original input and can adopt
        // legacy empty/receipt claims. New generations never revive expired
        // claims or run in place of a different accepted generation.
        if (extString(envelope, "coalesceGeneration") !== undefined ||
          (claim?.jobId !== undefined && claim.jobId !== generation)) return false;
        claim = { generation, input: envelope.data as Input, jobId: generation,
          ...(envelope.orderingKey === undefined ? {} : { orderingKey: envelope.orderingKey }),
          ...(envelope.meta === undefined ? {} : { meta: envelope.meta }) };
      }
      if (claim.generation !== generation) {
        // A continuation is persisted before publication. Its parent's
        // redelivery must repair that handoff before acknowledging the parent.
        if (claim.parent === generation && claim.seq === undefined) {
          await publishClaim(envelope.tenantId, key, claim, loaded.revision);
        }
        return false;
      }
      if (claim.deliverySeq !== undefined && claim.deliverySeq !== msg.seq) return false;
      if (await putClaim(envelope.tenantId, key,
        { ...claim, seq: claim.seq ?? msg.seq, deliverySeq: msg.seq }, loaded.revision) !== null) return true;
    }
  };

  const submit: Job<Input>["submit"] = async (job) => {
    if (job.coalesce === true) return submitCoalesced(job);
    const { message, ext } = toSend(job);
    const receipt = await core.send(message, ext);
    return { ...receipt, jobId: receipt.messageId };
  };

  const submitMany: Job<Input>["submitMany"] = async (jobs, options = {}) => {
    const publishConcurrency = options.publishConcurrency ?? 16;
    const maxPendingBytes = options.maxPendingBytes ?? 8 * 1024 * 1024;
    if (!Number.isSafeInteger(publishConcurrency) || publishConcurrency < 1) {
      throw new RangeError("publishConcurrency must be a positive integer");
    }
    if (!Number.isSafeInteger(maxPendingBytes) || maxPendingBytes < 1) {
      throw new RangeError("maxPendingBytes must be a positive integer");
    }

    let accepted = 0;
    let duplicates = 0;
    let pendingBytes = 0;
    let firstError: Error | null = null;
    const inflight = new Set<Promise<void>>();

    const waitForCapacity = async (nextBytes: number): Promise<void> => {
      // Byte backpressure never deadlocks: a single oversized item may fly alone.
      while (
        inflight.size >= publishConcurrency ||
        (inflight.size > 0 && pendingBytes + nextBytes > maxPendingBytes)
      ) {
        await Promise.race(inflight);
        if (firstError !== null) return;
      }
    };

    try {
      for await (const job of jobs) {
        if (options.signal?.aborted) throw new Error("submitMany aborted");
        const prepared = job.coalesce === true
          ? await prepareClaim(job)
          : await core.prepareSend(toSend(job).message, toSend(job).ext);
        await waitForCapacity(prepared.byteLength);
        if (firstError !== null) break;
        pendingBytes += prepared.byteLength;
        const task = ("claim" in prepared ? submitCoalesced(job, undefined, prepared) : prepared.publish())
          .then((receipt) => {
            if (receipt.duplicate) duplicates += 1;
            else accepted += 1;
          })
          .catch((error) => {
            firstError ??= asError(error);
          })
          .finally(() => {
            pendingBytes -= prepared.byteLength;
            inflight.delete(task);
          });
        inflight.add(task);
      }
    } catch (error) {
      firstError ??= asError(error);
    }
    await Promise.all(inflight);
    if (firstError !== null) {
      // Not atomic by design: prior accepted items stay accepted.
      throw new BatchSubmitError("submitMany failed", accepted, duplicates, firstError);
    }
    return { accepted, duplicates };
  };

  const process: Job<Input>["process"] = (options, handler) => {
    const { onError, ...processOptions } = options;
    type Continuation = { requested: boolean; delayMs?: number; input?: Input };
    const toContext = (message: {
      messageId: string;
      data: Input;
      attempt: number;
      signal: AbortSignal;
      heartbeat(): Promise<void>;
    }, key: string, continuation: Continuation): JobContext<Input> => ({
      jobId: message.messageId,
      key,
      input: message.data,
      attempt: message.attempt,
      failureCount: message.attempt - 1,
      signal: message.signal,
      heartbeat: message.heartbeat,
      resubmit: (options = {}) => {
        continuation.requested = true;
        if (options.delayMs !== undefined) continuation.delayMs = options.delayMs;
        if (options.input !== undefined) continuation.input = options.input;
      },
    });
    return core.process(
      processOptions,
      async (message, envelope, msg) => {
        const key = extString(envelope, "key") ?? "";
        const coalesced = extString(envelope, "coalesce") === "1";
        const generation = generationOf(envelope);
        const continuation: Continuation = { requested: false };
        await handler(toContext(message, key, continuation));
        if (continuation.requested) {
          const next: JobSubmit<Input> = {
            key,
            input: continuation.input ?? message.data,
            tenantId: envelope.tenantId,
            orderingKey: envelope.orderingKey,
            meta: envelope.meta,
            ...(continuation.delayMs !== undefined && continuation.delayMs > 0 ? { delayMs: continuation.delayMs } : {}),
          };
          if (!coalesced) {
            await core.send({ ...toSend(next).message, idempotencyKey: undefined }, { key });
            return;
          }
          // Persist the successor before publication. A redelivered parent can
          // finish this handoff without repeating the completed user handler.
          const { claim: successor } = await prepareClaim(next, generation);
          while (true) {
            const { claim, revision } = await loadClaim(envelope.tenantId, key);
            if (claim === null || !("generation" in claim)) return;
            if (claim.generation !== generation) {
              if (claim.parent === generation && claim.seq === undefined) {
                await publishClaim(envelope.tenantId, key, claim, revision);
              }
              return;
            }
            if (claim.deliverySeq !== msg.seq) return;
            const updated = await putClaim(envelope.tenantId, key, successor, revision);
            if (updated === null) continue;
            await publishClaim(envelope.tenantId, key, successor, updated);
            return;
          }
        }
        if (coalesced) await releaseClaim(envelope.tenantId, key, generation, msg.seq);
      },
      onError === undefined
        ? undefined
        : async ({ message, envelope, error }) => {
            // resubmit() inside onError is ignored: continuations exist only
            // on the success path (a retry/dead-letter is its own follow-up).
            const decision = await onError({
              context: toContext(message, extString(envelope, "key") ?? "", { requested: false }),
              error,
            });
            return decision.action === "retry"
              ? { action: "retry", ...(decision.delayMs !== undefined ? { delayMs: decision.delayMs } : {}) }
              : { action: "dead_letter", reason: decision.reason };
          },
    );
  };

  const submitBatch: Job<Input>["submitBatch"] = (jobs) => {
    for (const job of jobs) assertName(job.key, "job key");
    return core.sendBatch(
      jobs.map((job) => ({
        data: job.input,
        tenantId: job.tenantId,
        delayMs: job.delayMs,
        at: job.at,
        orderingKey: job.orderingKey,
        meta: job.meta,
      })),
      (index) => ({ key: jobs[index]!.key }),
    );
  };

  return {
    ready: () => core.declarationReady(),
    submit,
    submitMany,
    submitBatch,
    pause: core.pause,
    resume: core.resume,
    process,
    deadLetters: core.deadLetters,
  };
};
