import { createHash } from "node:crypto";
import { headers as natsHeaders } from "@nats-io/nats-core";
import { AckPolicy, DeliverPolicy, DiscardPolicy, RetentionPolicy } from "@nats-io/jetstream";
import type { JsMsg } from "@nats-io/jetstream";
import { decodeEnvelope, encodeEnvelope, extNumber, extString } from "./codec.ts";
import type { Envelope, JsonValue } from "./codec.ts";
import { confirmedAck, emitRun, runPullLoop, settleSuccess } from "./consume.ts";
import { NotFoundError, SyncUsageError, asError } from "./errors.ts";
import { assertName, dlqStreamName, resourceIdentity, streamName, subjectRoot, subjectToken, assertSubjectLength } from "./naming.ts";
import type { SyncResourceKind } from "./naming.ts";
import { ensureConsumer, ensureStream, toStorageType } from "./resources.ts";
import type { ProvisionContext } from "./resources.ts";
import type { SyncRuntime } from "./runtime.ts";
import {
  DEFAULT_DEDUPE_WINDOW_MS,
  DEFAULT_MESSAGE_PAYLOAD_BYTES,
  DLQ_HEADROOM_BYTES,
  DEFAULT_TENANT,
  assertRetention,
  backoffDelayMs,
  nanos,
  resolveDelivery,
} from "./types.ts";
import type { DeliveryConfig, MessageMeta, OrderingConfig, PublishReceipt, RetentionConfig } from "./types.ts";
import { createWorkerRuntime } from "./worker.ts";
import type { ProcessOptions, Worker } from "./worker.ts";

// ==========================
// Types
// ==========================

export type QueueConfig = {
  id: string;
  owner?: string;
  delivery?: DeliveryConfig;
  retention?: RetentionConfig;
  /** One stream-level NATS duplicate window. Default 120_000. */
  dedupeWindowMs?: number;
  ordering?: OrderingConfig;
  maxPayloadBytes?: number;
  replicas?: number;
};

export type QueueSend<T> = {
  data: T;
  tenantId?: string;
  idempotencyKey?: string;
  orderingKey?: string;
  delayMs?: number;
  at?: Date;
  /**
   * Expiring work: the message is removed from the stream after ttlMs if not
   * yet settled — including between retries. Minimum 1s (NATS).
   */
  ttlMs?: number;
  meta?: MessageMeta;
};

export type QueueMessage<T> = {
  data: T;
  messageId: string;
  attempt: number;
  publishedAt: Date;
  tenantId: string;
  orderingKey?: string;
  meta?: MessageMeta;
  signal: AbortSignal;
  /** In-progress acknowledgement: resets ackWaitMs for this delivery. */
  heartbeat(): Promise<void>;
};

export type QueueDelivery<T> = QueueMessage<T> & {
  ack(): Promise<void>;
  retry(options?: { delayMs?: number; reason?: string }): Promise<void>;
  deadLetter(options: { reason: string; error?: string }): Promise<void>;
};

export type QueueReader<T> = {
  receive(options?: { waitMs?: number; signal?: AbortSignal }): Promise<QueueDelivery<T> | null>;
  stream(options?: { signal?: AbortSignal }): AsyncIterable<QueueDelivery<T>>;
  close(): Promise<void>;
  [Symbol.asyncDispose](): Promise<void>;
};

export type DeadLetter<T> = {
  messageId: string;
  data: T;
  tenantId: string;
  attempts: number;
  failedAt: Date;
  reason: string;
  error?: string;
};

export type DeadLetterStore<T> = {
  list(options?: { limit?: number; after?: string }): Promise<DeadLetter<T>[]>;
  requeue(input: { messageId: string; idempotencyKey: string }): Promise<PublishReceipt>;
  delete(input: { messageId: string }): Promise<boolean>;
};

export type BatchReceipt = {
  /** Stream sequence of the batch's last message. */
  lastSequence: number;
  count: number;
  messageIds: string[];
};

export type PauseInfo = { paused: boolean; pauseUntil?: Date };

export type Queue<T> = {
  ready(): Promise<void>;
  send(message: QueueSend<T>): Promise<PublishReceipt>;
  /** Atomic all-or-nothing enqueue of up to 1000 messages (no delays). */
  sendBatch(messages: QueueSend<T>[]): Promise<BatchReceipt>;
  /** Pause delivery on the durable consumer(s) — global, all pods. Publishes continue. */
  pause(options?: { untilMs?: number }): Promise<PauseInfo>;
  resume(): Promise<PauseInfo>;
  process(options: ProcessOptions, handler: (message: QueueMessage<T>) => Promise<void>): Promise<Worker>;
  reader(options?: { signal?: AbortSignal }): Promise<QueueReader<T>>;
  deadLetters: DeadLetterStore<T>;
};

// ==========================
// Internal queue core (shared with job)
// ==========================

type PreparedSend = {
  messageId: string;
  bytes: Uint8Array;
  byteLength: number;
  target: string;
  /** Delayed sends go through a schedule and cannot join an atomic batch. */
  delayed: boolean;
  ttl?: string;
};

// Part of exported signatures; required for declaration emit.
// fallow-ignore-next-line unused-type
export type QueueCore<T, D = T> = {
  declarationReady(): Promise<void>;
  send(message: QueueSend<T>, ext?: Record<string, JsonValue>): Promise<PublishReceipt>;
  sendBatch(messages: QueueSend<T>[], ext?: (index: number) => Record<string, JsonValue>): Promise<BatchReceipt>;
  pause(options?: { untilMs?: number }): Promise<PauseInfo>;
  resume(): Promise<PauseInfo>;
  /** Encode a message now (byte size known), publish it later — for bounded fan-out. */
  prepareSend(message: QueueSend<T>, ext?: Record<string, JsonValue>): Promise<PreparedSend & { publish(): Promise<PublishReceipt> }>;
  process(
    options: ProcessOptions,
    handler: (message: QueueMessage<T>, envelope: Envelope, msg: JsMsg) => Promise<void>,
    onFailure?: (input: {
      message: QueueMessage<T>;
      envelope: Envelope;
      error: Error;
    }) => Promise<{ action: "retry"; delayMs?: number } | { action: "dead_letter"; reason: string }>,
  ): Promise<Worker>;
  reader(options?: { signal?: AbortSignal }): Promise<QueueReader<T>>;
  deadLetters: DeadLetterStore<D>;
};

const partitionOf = (orderingKey: string, partitions: number): number => {
  const digest = createHash("sha256").update(orderingKey, "utf8").digest();
  return digest.readUInt32BE(0) % partitions;
};

const scopedMessageId = (tenantId: string, key: string): string =>
  `k.${subjectToken(tenantId, "tenantId")}.${subjectToken(key, "idempotencyKey")}`;

export const createQueueCore = <T, D = T>(
  runtime: SyncRuntime,
  config: QueueConfig & {
    dlqMaxAgeMs?: number;
    /** Additional resources provisioned with this declaration (job claims KV). */
    provisionExtra?: (ctx: ProvisionContext) => Promise<void>;
    extraNatsNames?: string[];
    /** Called before a dead-letter transfer settles (job claim release). */
    onDeadLetter?: (envelope: Envelope | null, msg: JsMsg) => Promise<void>;
    /** Admit primitive-owned deliveries before handlers or terminal policies run. */
    admit?: (envelope: Envelope, msg: JsMsg) => Promise<boolean>;
    /** Renew primitive-owned coordination along with the delivery lease. */
    heartbeat?: (envelope: Envelope, msg: JsMsg) => Promise<void>;
    /** Requeue through the primitive's submission protocol when it owns one. */
    requeue?: (message: QueueSend<T>, ext: Record<string, JsonValue>) => Promise<PublishReceipt>;
  },
  kind: SyncResourceKind,
  deadLetterData: (envelope: Envelope) => D = (envelope) => envelope.data as D,
): QueueCore<T, D> => {
  const identity = resourceIdentity(runtime.namespace, kind, config.id);
  const owner = config.owner ?? runtime.application;
  const delivery = resolveDelivery(config.delivery);
  const retention = assertRetention(config.retention ?? { maxAgeMs: 7 * 24 * 60 * 60 * 1_000, maxBytes: 1024 ** 3 });
  const dedupeWindowMs = config.dedupeWindowMs ?? DEFAULT_DEDUPE_WINDOW_MS;
  const maxPayloadBytes = config.maxPayloadBytes ?? DEFAULT_MESSAGE_PAYLOAD_BYTES;
  const replicas = config.replicas ?? runtime.defaults.replicas;
  const storage = toStorageType(runtime.defaults.storage);
  const ordering: OrderingConfig = config.ordering ?? { mode: "none" };
  if (ordering.mode === "partitioned") {
    if (!Number.isSafeInteger(ordering.partitions) || ordering.partitions < 1 || ordering.partitions > 1_024) {
      throw new RangeError("ordering.partitions must be an integer between 1 and 1024");
    }
  }

  const stream = streamName(identity);
  const dlqStream = dlqStreamName(identity);
  const root = subjectRoot(identity);
  const label = `${kind} ${config.id}`;

  const workSubject = (tenantId: string, partition: number | null): string => {
    const base = `${root}.t.${subjectToken(tenantId, "tenantId")}.work`;
    const subject = partition === null ? base : `${base}.p${partition}`;
    assertSubjectLength(subject);
    return subject;
  };
  const dlqSubject = (tenantId: string): string => `${root}.t.${subjectToken(tenantId, "tenantId")}.dlq`;

  const workFilter = ordering.mode === "partitioned" ? `${root}.t.*.work.*` : `${root}.t.*.work`;
  const partitionFilter = (partition: number): string => `${root}.t.*.work.p${partition}`;
  const durableFor = (partition: number | null): string =>
    partition === null ? `S6_QC_${identity.hash}` : `S6_QC_${identity.hash}_p${partition}`;

  const declaration = runtime.declare({
    identity,
    owner,
    configKey: JSON.stringify([
      kind,
      config.id,
      owner,
      delivery,
      retention,
      dedupeWindowMs,
      ordering,
      maxPayloadBytes,
      replicas,
      config.dlqMaxAgeMs ?? null,
    ]),
    natsNames: [stream, dlqStream, ...(config.extraNatsNames ?? [])],
    maxMessageBytes: maxPayloadBytes + DLQ_HEADROOM_BYTES,
    provision: async (ctx: ProvisionContext) => {
      await config.provisionExtra?.(ctx);
      await ensureStream(ctx, identity, owner, {
        name: stream,
        subjects: [workFilter, `${root}.t.*.delay.>`],
        retention: RetentionPolicy.Workqueue,
        // DiscardPolicy.New would be the honest failure mode at the limits,
        // but NATS forbids it on streams with message schedules (delays live
        // here). Retention limits are therefore a hard loss boundary for the
        // oldest pending work — size maxBytes/maxAgeMs generously.
        discard: DiscardPolicy.Old,
        storage,
        num_replicas: replicas,
        max_age: nanos(retention.maxAgeMs),
        max_bytes: retention.maxBytes,
        max_msgs: retention.maxMessages ?? -1,
        max_msg_size: -1,
        duplicate_window: nanos(dedupeWindowMs),
        allow_msg_schedules: true,
        allow_msg_ttl: true,
        allow_atomic: true,
      });
      await ensureStream(ctx, identity, owner, {
        name: dlqStream,
        subjects: [`${root}.t.*.dlq`],
        retention: RetentionPolicy.Limits,
        discard: DiscardPolicy.Old,
        storage,
        num_replicas: replicas,
        max_age: nanos(config.dlqMaxAgeMs ?? retention.maxAgeMs),
        max_bytes: retention.maxBytes,
        max_msgs: -1,
        max_msg_size: -1,
        duplicate_window: nanos(dedupeWindowMs),
      });
    },
    summary: async (ctx: ProvisionContext) => {
      const info = await ctx.jsm.streams.info(stream);
      const dlq = await ctx.jsm.streams.info(dlqStream);
      const consumers: Record<string, JsonValue> = {};
      const partitions = ordering.mode === "partitioned" ? ordering.partitions : 1;
      for (let p = 0; p < partitions; p++) {
        const durable = durableFor(ordering.mode === "partitioned" ? p : null);
        const consumerInfo = await ctx.jsm.consumers.info(stream, durable).catch(() => null);
        if (consumerInfo) {
          consumers[durable] = {
            pending: consumerInfo.num_pending,
            ackPending: consumerInfo.num_ack_pending,
            redelivered: consumerInfo.num_redelivered,
          };
        }
      }
      return {
        messages: info.state.messages,
        bytes: info.state.bytes,
        deadLetters: dlq.state.messages,
        consumers,
      } satisfies Record<string, JsonValue>;
    },
  });

  const ensureWorkConsumers = async (ctx: ProvisionContext): Promise<string[]> => {
    if (ordering.mode === "partitioned") {
      const durables: string[] = [];
      for (let p = 0; p < ordering.partitions; p++) {
        const durable = durableFor(p);
        await ensureConsumer(ctx, stream, {
          durable_name: durable,
          ack_policy: AckPolicy.Explicit,
          filter_subject: partitionFilter(p),
          ack_wait: nanos(delivery.ackWaitMs),
          // Serial per-partition delivery is the whole point of partitioning.
          // No max_deliver: the client-side attempt guard bounds handler runs,
          // and an unbounded redelivery keeps the DLQ transfer crash-safe.
          max_ack_pending: 1,
          deliver_policy: DeliverPolicy.All,
        });
        durables.push(durable);
      }
      return durables;
    }
    const durable = durableFor(null);
    await ensureConsumer(ctx, stream, {
      durable_name: durable,
      ack_policy: AckPolicy.Explicit,
      filter_subject: workFilter,
      ack_wait: nanos(delivery.ackWaitMs),
      max_ack_pending: delivery.maxInFlight,
      deliver_policy: DeliverPolicy.All,
    });
    return [durable];
  };

  // ==========================
  // Send
  // ==========================

  const prepareSend: QueueCore<T, D>["prepareSend"] = async (message, ext) => {
    runtime.assertActive();
    await declaration.ready();
    const tenantId = message.tenantId ?? DEFAULT_TENANT;
    if (message.delayMs !== undefined && message.at !== undefined) {
      throw new SyncUsageError("delayMs and at are mutually exclusive");
    }
    let partition: number | null = null;
    if (ordering.mode === "partitioned") {
      if (message.orderingKey === undefined) {
        throw new SyncUsageError(`${label} is partitioned; send() requires an orderingKey`);
      }
      partition = partitionOf(message.orderingKey, ordering.partitions);
    }
    const messageId = message.idempotencyKey
      ? scopedMessageId(tenantId, message.idempotencyKey)
      : crypto.randomUUID();
    const envelope: Envelope = {
      v: 6,
      data: message.data,
      tenantId,
      publishedAt: new Date().toISOString(),
      ...(message.orderingKey !== undefined ? { orderingKey: message.orderingKey } : {}),
      ...(message.meta !== undefined ? { meta: message.meta } : {}),
      ext: { ...ext, messageId },
    };
    const bytes = encodeEnvelope(label, envelope, maxPayloadBytes);
    const target = workSubject(tenantId, partition);
    const fireAt = message.at ?? (message.delayMs !== undefined ? new Date(Date.now() + message.delayMs) : null);
    if (message.ttlMs !== undefined && (!Number.isSafeInteger(message.ttlMs) || message.ttlMs < 1_000)) {
      throw new RangeError("ttlMs must be an integer of at least 1000");
    }
    const ttl = message.ttlMs !== undefined ? `${Math.ceil(message.ttlMs / 1_000)}s` : undefined;

    const publish = async (): Promise<PublishReceipt> => {
      const ctx = await runtime.context();
      if (fireAt !== null && fireAt.getTime() > Date.now()) {
        const delaySubject = `${root}.t.${subjectToken(tenantId, "tenantId")}.delay.${crypto.randomUUID()}`;
        const ack = await ctx.js.publish(delaySubject, bytes, {
          msgID: messageId,
          schedule: { specification: { at: fireAt }, target, ttl: "never" },
        });
        return { messageId, streamSequence: ack.seq, duplicate: ack.duplicate };
      }
      const headers = ttl !== undefined ? (() => { const h = natsHeaders(); h.set("Nats-TTL", ttl); return h; })() : undefined;
      const ack = await ctx.js.publish(target, bytes, { msgID: messageId, ...(headers !== undefined ? { headers } : {}) });
      return { messageId, streamSequence: ack.seq, duplicate: ack.duplicate };
    };

    return { messageId, bytes, byteLength: bytes.byteLength, target, delayed: fireAt !== null && fireAt.getTime() > Date.now(), ttl, publish };
  };

  const send: QueueCore<T, D>["send"] = async (message, ext) => {
    const prepared = await prepareSend(message, ext);
    return prepared.publish();
  };

  const sendBatch: QueueCore<T, D>["sendBatch"] = async (messages, ext) => {
    runtime.assertActive();
    if (messages.length === 0 || messages.length > 1_000) {
      throw new SyncUsageError("sendBatch takes 1 to 1000 messages");
    }
    if (messages.some((message) => message.idempotencyKey !== undefined)) {
      // NATS batch messages cannot carry Nats-Msg-Id: a batch is atomic but
      // NOT deduplicated. Refuse silently different semantics.
      throw new SyncUsageError("idempotencyKey is not supported in atomic batches (no dedupe); use send()");
    }
    const prepared = await Promise.all(messages.map((message, index) => prepareSend(message, ext?.(index))));
    if (prepared.some((entry) => entry.delayed)) {
      throw new SyncUsageError("delayed sends cannot join an atomic batch");
    }
    const messageIds = prepared.map((entry) => entry.messageId);
    if (prepared.length === 1) {
      const receipt = await prepared[0]!.publish();
      return { lastSequence: receipt.streamSequence, count: 1, messageIds };
    }
    const ctx = await runtime.context();
    const headersFor = (entry: PreparedSend) => {
      if (entry.ttl === undefined) return {};
      const h = natsHeaders();
      h.set("Nats-TTL", entry.ttl);
      return { headers: h };
    };
    const first = prepared[0]!;
    const last = prepared[prepared.length - 1]!;
    try {
      // NATS atomic batch: nothing is persisted unless the commit succeeds.
      const batch = await ctx.js.startBatch(first.target, first.bytes, headersFor(first));
      for (const entry of prepared.slice(1, -1)) {
        batch.add(entry.target, entry.bytes, headersFor(entry));
      }
      const ack = await batch.commit(last.target, last.bytes, headersFor(last));
      return { lastSequence: ack.seq, count: ack.count, messageIds };
    } catch (error) {
      if (/atomic publish is disabled/i.test(asError(error).message)) {
        throw new SyncUsageError(`${label}: the stream predates atomic batches (allow_atomic off); recreate it`);
      }
      throw error;
    }
  };

  const pause: QueueCore<T, D>["pause"] = async (options = {}) => {
    runtime.assertActive();
    await declaration.ready();
    const ctx = await runtime.context();
    await ensureWorkConsumers(ctx);
    const until = new Date(Date.now() + (options.untilMs ?? 365 * 24 * 60 * 60 * 1_000));
    let result: { paused: boolean; pause_until?: string } = { paused: false };
    const partitions = ordering.mode === "partitioned" ? ordering.partitions : 1;
    for (let p = 0; p < partitions; p++) {
      result = await ctx.jsm.consumers.pause(stream, durableFor(ordering.mode === "partitioned" ? p : null), until);
    }
    return { paused: result.paused, ...(result.pause_until !== undefined ? { pauseUntil: new Date(result.pause_until) } : {}) };
  };

  const resume: QueueCore<T, D>["resume"] = async () => {
    runtime.assertActive();
    await declaration.ready();
    const ctx = await runtime.context();
    await ensureWorkConsumers(ctx);
    // Partial failure leaves earlier partitions resumed; the call is
    // idempotent — retry until it succeeds.
    let result: { paused: boolean; pause_until?: string } = { paused: false };
    const partitions = ordering.mode === "partitioned" ? ordering.partitions : 1;
    for (let p = 0; p < partitions; p++) {
      result = await ctx.jsm.consumers.resume(stream, durableFor(ordering.mode === "partitioned" ? p : null));
    }
    return { paused: result.paused };
  };

  // ==========================
  // Delivery handling
  // ==========================

  const toMessage = (envelope: Envelope, msg: JsMsg, signal: AbortSignal, attempt: number): QueueMessage<T> => ({
    data: envelope.data as T,
    messageId: extString(envelope, "messageId") ?? `seq-${msg.seq}`,
    attempt,
    publishedAt: new Date(envelope.publishedAt),
    tenantId: envelope.tenantId,
    orderingKey: envelope.orderingKey,
    meta: envelope.meta,
    signal,
    heartbeat: async () => {
      await config.heartbeat?.(envelope, msg);
      msg.working();
    },
  });

  const deadLetterTransfer = async (
    ctx: ProvisionContext,
    msg: JsMsg,
    envelope: Envelope | null,
    reason: string,
    error?: string,
  ): Promise<void> => {
    if (envelope && config.admit && !(await config.admit(envelope, msg))) {
      await settleSuccess(msg, label, runtime.events, kind);
      return;
    }
    const messageId = extString(envelope, "messageId") ?? `seq-${msg.seq}`;
    const tenantId = envelope?.tenantId ?? DEFAULT_TENANT;
    const dlqEnvelope: Envelope = {
      v: 6,
      data: envelope?.data ?? null,
      ...(envelope?.orderingKey === undefined ? {} : { orderingKey: envelope.orderingKey }),
      ...(envelope?.meta === undefined ? {} : { meta: envelope.meta }),
      tenantId,
      publishedAt: new Date().toISOString(),
      ext: {
        ...envelope?.ext,
        messageId,
        attempts: msg.info.deliveryCount,
        reason,
        failedAt: new Date().toISOString(),
        ...(error !== undefined ? { error: error.slice(0, 2_048) } : {}),
      },
    };
    // Headroom: the transfer adds bookkeeping on top of the original payload
    // and must never be the reason a message cannot be dead-lettered.
    const bytes = encodeEnvelope(`${label} dlq`, dlqEnvelope, maxPayloadBytes + DLQ_HEADROOM_BYTES);
    // The original message ID keys the DLQ dedupe window, so a crash between
    // DLQ publish and source ack repeats the transfer without duplicating it.
    await ctx.js.publish(dlqSubject(tenantId), bytes, { msgID: `dlq.${messageId}` });
    // Terminal settlement: release resources tied to the submission (claims)
    // before the ack, so a crash retries the release with the redelivery.
    await config.onDeadLetter?.(envelope, msg);
    await confirmedAck(msg, label).catch(() => {});
    runtime.events.emit({
      type: "dead_letter",
      resource: config.id,
      kind,
      detail: { messageId, reason },
    });
  };

  const process: QueueCore<T>["process"] = async (options, handler, onFailure) => {
    runtime.assertActive();
    await declaration.ready();
    const ctx = await runtime.context();
    const durables = await ensureWorkConsumers(ctx);

    const wr = createWorkerRuntime(options, { onFinished: () => runtime.unregisterWorker(wr) });
    runtime.registerWorker(wr);
    runtime.events.emit({ type: "worker_started", resource: config.id, kind });

    /** Sleep out a retry delay while holding the delivery alive. */
    const holdWithHeartbeat = async (envelope: Envelope, msg: JsMsg, delayMs: number, signal: AbortSignal): Promise<void> => {
      const step = Math.max(1_000, Math.floor(delivery.ackWaitMs / 2));
      const { promise: abortedPromise, resolve: resolveAborted } = Promise.withResolvers<void>();
      const onAbort = (): void => resolveAborted();
      signal.addEventListener("abort", onAbort, { once: true });
      try {
        let remaining = delayMs;
        while (remaining > 0 && !signal.aborted) {
          const slice = Math.min(step, remaining);
          await Promise.race([Bun.sleep(slice), abortedPromise]);
          remaining -= slice;
          await config.heartbeat?.(envelope, msg);
          msg.working();
        }
      } finally {
        signal.removeEventListener("abort", onAbort);
      }
    };

    const onMessage = async (msg: JsMsg, signal: AbortSignal): Promise<void> => {
      let envelope: Envelope;
      try {
        envelope = decodeEnvelope(msg.data);
      } catch (error) {
        await deadLetterTransfer(ctx, msg, null, "invalid envelope", asError(error).message);
        return;
      }
      if (config.admit && !(await config.admit(envelope, msg))) {
        await settleSuccess(msg, label, runtime.events, kind);
        return;
      }
      let attempt = msg.info.deliveryCount;
      if (attempt > delivery.maxAttempts) {
        await deadLetterTransfer(ctx, msg, envelope, "max attempts exhausted");
        return;
      }

      while (true) {
        const message = toMessage(envelope, msg, signal, attempt);
        const settled = emitRun(runtime.events, config.id, kind, {
          id: message.messageId,
          ...(extString(envelope, "key") !== undefined ? { key: extString(envelope, "key")! } : {}),
          attempt,
        });
        let handlerError: Error | null = null;
        try {
          await handler(message, envelope, msg);
        } catch (error) {
          handlerError = asError(error);
        }
        if (handlerError === null) {
          settled("success");
          // Success settles outside the failure logic: an unconfirmable ack
          // means the delivery was superseded — never a handler failure.
          await settleSuccess(msg, label, runtime.events, kind);
          return;
        }
        if (signal.aborted) {
          msg.nak();
          return;
        }
        // A completed job may already have handed off to its successor.
        // Repair that handoff before applying a failure policy to the parent.
        if (config.admit && !(await config.admit(envelope, msg))) {
          settled("success");
          await settleSuccess(msg, label, runtime.events, kind);
          return;
        }
        runtime.events.emit({ type: "handler_error", resource: config.id, kind, error: handlerError.message });
        let decision: { action: "retry"; delayMs?: number } | { action: "dead_letter"; reason: string } = {
          action: "retry",
        };
        if (onFailure) {
          try {
            decision = await onFailure({ message, envelope, error: handlerError });
          } catch {
            // A throwing failure policy must never settle work accidentally.
            decision = { action: "retry" };
          }
        }
        if (decision.action === "dead_letter") {
          settled("dead_letter");
          await deadLetterTransfer(ctx, msg, envelope, decision.reason, handlerError.message);
          return;
        }
        if (attempt >= delivery.maxAttempts) {
          settled("dead_letter");
          await deadLetterTransfer(ctx, msg, envelope, "max attempts exhausted", handlerError.message);
          return;
        }
        settled("retry");
        const delayMs = decision.delayMs ?? backoffDelayMs(delivery, attempt);
        if (ordering.mode === "partitioned") {
          // A nak would free the MaxAckPending=1 slot and let younger messages
          // of the same partition overtake during the backoff. Retry in place:
          // the delivery is held with heartbeats, so per-key order survives
          // handler failures. A crash mid-hold restarts counting from the
          // NATS deliveryCount, so maxAttempts bounds attempts per lease,
          // not globally across crashes.
          await holdWithHeartbeat(envelope, msg, delayMs, signal);
          if (signal.aborted) {
            msg.nak();
            return;
          }
          if (config.admit && !(await config.admit(envelope, msg))) {
            await settleSuccess(msg, label, runtime.events, kind);
            return;
          }
          attempt += 1;
          runtime.events.emit({ type: "redelivery", resource: config.id, kind, detail: { attempt, inPlace: true } });
          continue;
        }
        msg.nak(delayMs);
        runtime.events.emit({ type: "redelivery", resource: config.id, kind, detail: { attempt } });
        return;
      }
    };

    const loops = durables.map((durable) =>
      runPullLoop(wr, () => ctx.js.consumers.get(stream, durable), onMessage, {
        events: runtime.events,
        resource: config.id,
        ...(ordering.mode === "partitioned" ? { maxBatch: 1, pollExpiresMs: 1_500 } : {}),
      }),
    );
    Promise.all(loops).finally(() => {
      runtime.events.emit({ type: "worker_stopped", resource: config.id, kind });
    });
    return wr.worker;
  };

  // ==========================
  // Manual reader
  // ==========================

  const reader: QueueCore<T>["reader"] = async (options = {}) => {
    runtime.assertActive();
    if (ordering.mode === "partitioned") {
      throw new SyncUsageError(`${label} is partitioned; use process() — reader() supports unpartitioned queues`);
    }
    await declaration.ready();
    const ctx = await runtime.context();
    await ensureWorkConsumers(ctx);
    const durable = durableFor(null);
    let closed = false;
    const controller = new AbortController();
    options.signal?.addEventListener("abort", () => controller.abort(), { once: true });

    const toDelivery = async (msg: JsMsg): Promise<QueueDelivery<T> | null> => {
      let envelope: Envelope;
      try {
        envelope = decodeEnvelope(msg.data);
      } catch (error) {
        await deadLetterTransfer(ctx, msg, null, "invalid envelope", asError(error).message);
        return null;
      }
      const message = toMessage(envelope, msg, controller.signal, msg.info.deliveryCount);
      return {
        ...message,
        ack: () => confirmedAck(msg, label),
        retry: async (retryOptions = {}) => {
          if (message.attempt >= delivery.maxAttempts) {
            await deadLetterTransfer(ctx, msg, envelope, "max attempts exhausted", retryOptions.reason);
            return;
          }
          msg.nak(retryOptions.delayMs ?? backoffDelayMs(delivery, message.attempt));
        },
        deadLetter: (dlOptions) => deadLetterTransfer(ctx, msg, envelope, dlOptions.reason, dlOptions.error),
      };
    };

    const receive = async (receiveOptions: { waitMs?: number; signal?: AbortSignal } = {}) => {
      // Poison messages are dead-lettered and skipped within the wait budget.
      const deadline = Date.now() + (receiveOptions.waitMs ?? 5_000);
      while (!closed && !controller.signal.aborted && !receiveOptions.signal?.aborted) {
        const remaining = deadline - Date.now();
        if (remaining <= 0) return null;
        const consumer = await ctx.js.consumers.get(stream, durable);
        // NATS requires a fetch expiry of at least one second.
        const batch = await consumer.fetch({ max_messages: 1, expires: Math.max(remaining, 1_000) });
        const stop = (): void => batch.stop();
        receiveOptions.signal?.addEventListener("abort", stop, { once: true });
        controller.signal.addEventListener("abort", stop, { once: true });
        let sawMessage = false;
        try {
          for await (const msg of batch) {
            sawMessage = true;
            const queueDelivery = await toDelivery(msg);
            if (queueDelivery !== null) return queueDelivery;
          }
        } finally {
          receiveOptions.signal?.removeEventListener("abort", stop);
          controller.signal.removeEventListener("abort", stop);
        }
        if (!sawMessage) return null; // fetch expired empty — queue is idle
      }
      return null;
    };

    const queueReader: QueueReader<T> = {
      receive,
      stream: (streamOptions = {}) => ({
        async *[Symbol.asyncIterator]() {
          while (!closed && !controller.signal.aborted && !streamOptions.signal?.aborted) {
            const queueDelivery = await receive({ signal: streamOptions.signal });
            if (queueDelivery !== null) yield queueDelivery;
          }
        },
      }),
      close: async () => {
        closed = true;
        controller.abort();
      },
      [Symbol.asyncDispose]: async () => {
        closed = true;
        controller.abort();
      },
    };
    return queueReader;
  };

  // ==========================
  // Dead letter store
  // ==========================

  const scanDeadLetters = async function* (): AsyncGenerator<{ seq: number; envelope: Envelope }> {
    const ctx = await runtime.context();
    const info = await ctx.jsm.streams.info(dlqStream).catch(() => null);
    if (info === null || info.state.messages === 0) return;
    const consumer = await ctx.js.consumers.get(dlqStream);
    // Bounded fetch rounds: per-delivery pending gives deterministic
    // termination, and an empty round covers entries deleted between the
    // info call and the read (an open-ended consume would hang there).
    while (true) {
      const batch = await consumer.fetch({ max_messages: 256, expires: 2_000 });
      let delivered = 0;
      for await (const msg of batch) {
        delivered += 1;
        try {
          yield { seq: msg.seq, envelope: decodeEnvelope(msg.data) };
        } catch {
          // Skip foreign messages.
        }
        if (msg.info.pending === 0) return;
      }
      if (delivered === 0) return;
    }
  };

  const findDeadLetter = async (messageId: string): Promise<{ seq: number; envelope: Envelope } | null> => {
    for await (const entry of scanDeadLetters()) {
      if (entry.envelope.ext?.messageId === messageId) return entry;
    }
    return null;
  };

  const toDeadLetter = (envelope: Envelope): DeadLetter<D> => {
    const error = extString(envelope, "error");
    return {
      messageId: extString(envelope, "messageId") ?? "",
      data: deadLetterData(envelope),
      tenantId: envelope.tenantId,
      attempts: extNumber(envelope, "attempts") ?? 0,
      failedAt: new Date(extString(envelope, "failedAt") ?? envelope.publishedAt),
      reason: extString(envelope, "reason") ?? "unknown",
      ...(error !== undefined ? { error } : {}),
    };
  };

  const deadLetters: DeadLetterStore<D> = {
    list: async (options = {}) => {
      await declaration.ready();
      const limit = options.limit ?? 100;
      const entries: DeadLetter<D>[] = [];
      let skipping = options.after !== undefined;
      for await (const entry of scanDeadLetters()) {
        if (skipping) {
          if (entry.envelope.ext?.messageId === options.after) skipping = false;
          continue;
        }
        entries.push(toDeadLetter(entry.envelope));
        if (entries.length >= limit) break;
      }
      return entries;
    },
    requeue: async (input) => {
      await declaration.ready();
      assertName(input.idempotencyKey, "idempotencyKey");
      const entry = await findDeadLetter(input.messageId);
      if (entry === null) throw new NotFoundError(`dead letter ${input.messageId} not found`);
      if (entry.envelope.ext?.reason === "invalid envelope") {
        throw new SyncUsageError(`dead letter ${input.messageId} is an undecodable poison message; delete it instead`);
      }
      const ctx = await runtime.context();
      // Preserve primitive-specific ext fields (e.g. a job key) on requeue.
      const { attempts: _a, reason: _r, failedAt: _f, error: _e, messageId: _m, ...restExt } = entry.envelope.ext ?? {};
      const receipt = await (config.requeue ?? send)(
        {
          data: entry.envelope.data as T,
          tenantId: entry.envelope.tenantId,
          idempotencyKey: input.idempotencyKey,
          ...(entry.envelope.orderingKey !== undefined ? { orderingKey: entry.envelope.orderingKey } : {}),
          ...(entry.envelope.meta !== undefined ? { meta: entry.envelope.meta } : {}),
        },
        restExt,
      );
      await ctx.jsm.streams.deleteMessage(dlqStream, entry.seq).catch((error) => {
        runtime.events.emit({
          type: "handler_error",
          resource: config.id,
          kind,
          error: `requeued dead letter ${input.messageId} could not be removed from the DLQ: ${asError(error).message}`,
        });
      });
      return receipt;
    },
    delete: async (input) => {
      await declaration.ready();
      const entry = await findDeadLetter(input.messageId);
      if (entry === null) return false;
      const ctx = await runtime.context();
      return ctx.jsm.streams.deleteMessage(dlqStream, entry.seq).catch(() => false);
    },
  };

  return {
    declarationReady: () => declaration.ready(),
    send,
    sendBatch,
    pause,
    resume,
    prepareSend,
    process,
    reader,
    deadLetters,
  };
};

// ==========================
// Public queue factory
// ==========================

export const createQueue = <T>(runtime: SyncRuntime, config: QueueConfig): Queue<T> => {
  const core = createQueueCore<T>(runtime, config, "queue");
  return {
    ready: () => core.declarationReady(),
    send: (message) => core.send(message),
    sendBatch: (messages) => core.sendBatch(messages),
    pause: core.pause,
    resume: core.resume,
    process: (options, handler) => core.process(options, (message) => handler(message)),
    reader: core.reader,
    deadLetters: core.deadLetters,
  };
};
