import { AckPolicy, DeliverPolicy, DiscardPolicy, RetentionPolicy } from "@nats-io/jetstream";
import type { Consumer, JsMsg } from "@nats-io/jetstream";
import { decodeEnvelope, encodeEnvelope, extString } from "./codec.ts";
import type { Envelope, JsonValue } from "./codec.ts";
import { confirmedAck, emitRun, runPullLoop, settleSuccess } from "./consume.ts";
import { ConflictError, CursorMismatchError, NotFoundError, RetentionGapError, SyncUsageError, asError } from "./errors.ts";
import { assertName, resourceIdentity, streamName, dlqStreamName, consumerName, subjectRoot, subjectToken, assertSubjectLength } from "./naming.ts";
import type { ResourceIdentity } from "./naming.ts";
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
import type { DeliveryConfig, MessageMeta, PublishReceipt, RetentionConfig } from "./types.ts";
import { createWorkerRuntime } from "./worker.ts";
import type { ProcessOptions, Worker } from "./worker.ts";

// ==========================
// Types
// ==========================

export type TopicConfig = {
  id: string;
  owner?: string;
  retention: RetentionConfig;
  dedupeWindowMs?: number;
  maxPayloadBytes?: number;
  replicas?: number;
};

/** Opaque, resource-bound cursor: identifies this topic and a stream sequence. */
export type TopicCursor = string;

export type TopicEvent<T> = {
  data: T;
  eventId: string;
  cursor: TopicCursor;
  /** Stream sequence behind `cursor` — monotonic per topic, safe to persist and compare numerically. */
  sequence: number;
  tenantId: string;
  orderingKey?: string;
  publishedAt: Date;
  meta?: MessageMeta;
};

export type TopicLiveEvent<T> = Omit<TopicEvent<T>, "cursor" | "sequence">;

export type TopicPublish<T> = {
  data: T;
  tenantId?: string;
  idempotencyKey?: string;
  orderingKey?: string;
  meta?: MessageMeta;
  /**
   * Optimistic concurrency: the publish succeeds only while the given cursor
   * is still the tenant's latest event. `null` means "no events currently
   * retained" — with retention limits that can also match a tenant whose
   * history aged out. On a lost race the publish throws ConflictError and
   * nothing is written — re-read, rebase, retry.
   */
  expectedAfter?: TopicCursor | null;
};

export type TopicBatchEvent<T> = {
  data: T;
  orderingKey?: string;
  meta?: MessageMeta;
};

export type TopicHub<T> = {
  /**
   * Tail the topic from `after` (or live-only when omitted). Catch-up events
   * come from a replay, then the shared live tail continues seamlessly. A
   * subscriber that falls more than `bufferLimit` events behind is ended with
   * RetentionGapError (resumeAfter = last delivered cursor) — resubscribe.
   */
  subscribe(options?: {
    after?: TopicCursor;
    bufferLimit?: number;
    signal?: AbortSignal;
  }): AsyncIterable<TopicEvent<T>>;
  /** Stop the shared follower and end every subscriber. */
  close(): void;
};

export type TopicProcessOptions = ProcessOptions & {
  consumer: string;
  tenantId?: string;
  delivery?: DeliveryConfig;
  start?: "earliest" | "latest" | { after: TopicCursor };
};

export type Topic<T> = {
  ready(): Promise<void>;
  publish(input: TopicPublish<T>): Promise<PublishReceipt & { eventId: string; cursor: TopicCursor }>;
  /**
   * Atomically append several events for one tenant — with `expectedAfter`
   * this is an optimistic event-sourcing append: all events land or none do.
   * Batch events carry no dedupe ids (NATS batches exclude message ids).
   */
  publishBatch(input: {
    tenantId?: string;
    events: TopicBatchEvent<T>[];
    expectedAfter?: TopicCursor | null;
  }): Promise<{ lastSequence: number; count: number; cursor: TopicCursor; eventIds: string[] }>;
  /**
   * In-process fanout hub: ONE shared follow() feeds any number of local
   * subscribers, each with its own cursor. Built for per-connection tails
   * (WebSocket/SSE) where a follow() per connection would stream the whole
   * topic once per client. One hub per tenant per topic handle: repeated
   * calls return the same hub until it is closed.
   */
  hub(options?: { tenantId?: string }): TopicHub<T>;
  /** Stream sequence of a cursor issued by this topic (CursorMismatchError otherwise). */
  cursorSequence(cursor: TopicCursor): number;
  /** Cursor for a stream sequence of this topic (e.g. one persisted from `TopicEvent.sequence`). */
  cursorAt(sequence: number): TopicCursor;
  /** Pause delivery for one named durable consumer — global, all pods. */
  pauseConsumer(input: { consumer: string; tenantId?: string; untilMs?: number }): Promise<{ paused: boolean; pauseUntil?: Date }>;
  resumeConsumer(input: { consumer: string; tenantId?: string }): Promise<{ paused: boolean }>;
  latestCursor(options?: { tenantId?: string }): Promise<TopicCursor | null>;
  live(options?: { tenantId?: string; signal?: AbortSignal }): AsyncIterable<TopicLiveEvent<T>>;
  replay(options?: {
    tenantId?: string;
    after?: TopicCursor;
    until?: TopicCursor;
    signal?: AbortSignal;
  }): AsyncIterable<TopicEvent<T>>;
  follow(options?: { tenantId?: string; after?: TopicCursor; signal?: AbortSignal }): AsyncIterable<TopicEvent<T>>;
  process(
    options: TopicProcessOptions,
    handler: (event: TopicEvent<T> & { attempt: number; signal: AbortSignal }) => Promise<void>,
  ): Promise<Worker>;
};

// ==========================
// Cursor encoding
// ==========================

const cursorOf = (identity: ResourceIdentity, seq: number): TopicCursor => `s6t.${identity.subjectHash}.${seq}`;

const gapError = (identity: ResourceIdentity, requestedSeq: number, firstRetainedSeq: number): RetentionGapError =>
  new RetentionGapError(
    cursorOf(identity, requestedSeq),
    cursorOf(identity, firstRetainedSeq),
    // `after` is exclusive: resuming must include the first retained event.
    cursorOf(identity, Math.max(0, firstRetainedSeq - 1)),
  );

/** next() that resolves "idle" after idleMs while keeping the pending pull alive across races. */
const withIdleTimeout = (
  iterator: AsyncIterator<JsMsg>,
  idleMs: number,
): (() => Promise<IteratorResult<JsMsg> | "idle">) => {
  let pending: Promise<IteratorResult<JsMsg>> | null = null;
  return async () => {
    pending ??= iterator.next();
    const winner = await Promise.race([pending, Bun.sleep(idleMs).then(() => "idle" as const)]);
    if (winner !== "idle") pending = null;
    return winner;
  };
};

const parseCursor = (identity: ResourceIdentity, cursor: TopicCursor, label: string): number => {
  const parts = cursor.split(".");
  const seq = Number(parts[2]);
  if (parts.length !== 3 || parts[0] !== "s6t" || !Number.isSafeInteger(seq) || seq < 0) {
    throw new CursorMismatchError(`${label} is not a valid topic cursor: ${cursor}`);
  }
  if (parts[1] !== identity.subjectHash) {
    throw new CursorMismatchError(`${label} was issued by a different topic resource`);
  }
  return seq;
};

// ==========================
// Topic factory
// ==========================

export const createTopic = <T>(runtime: SyncRuntime, config: TopicConfig): Topic<T> => {
  const identity = resourceIdentity(runtime.namespace, "topic", config.id);
  const owner = config.owner ?? runtime.application;
  const retention = assertRetention(config.retention);
  const dedupeWindowMs = config.dedupeWindowMs ?? DEFAULT_DEDUPE_WINDOW_MS;
  const maxPayloadBytes = config.maxPayloadBytes ?? DEFAULT_MESSAGE_PAYLOAD_BYTES;
  const replicas = config.replicas ?? runtime.defaults.replicas;
  const storage = toStorageType(runtime.defaults.storage);

  const stream = streamName(identity);
  const dlqStream = dlqStreamName(identity);
  const root = subjectRoot(identity);
  const label = `topic ${config.id}`;
  const eventSubject = (tenantId: string): string => {
    const subject = `${root}.t.${subjectToken(tenantId, "tenantId")}.event`;
    assertSubjectLength(subject);
    return subject;
  };
  const dlqSubject = (consumer: string): string => `${root}.dlq.${subjectToken(consumer, "consumer")}`;

  const declaration = runtime.declare({
    identity,
    owner,
    configKey: JSON.stringify(["topic", config.id, owner, retention, dedupeWindowMs, maxPayloadBytes, replicas]),
    natsNames: [stream, dlqStream],
    maxMessageBytes: maxPayloadBytes + DLQ_HEADROOM_BYTES,
    provision: async (ctx: ProvisionContext) => {
      await ensureStream(ctx, identity, owner, {
        name: stream,
        subjects: [`${root}.t.*.event`],
        retention: RetentionPolicy.Limits,
        discard: DiscardPolicy.Old,
        storage,
        num_replicas: replicas,
        max_age: nanos(retention.maxAgeMs),
        max_bytes: retention.maxBytes,
        max_msgs: retention.maxMessages ?? -1,
        max_msg_size: -1,
        duplicate_window: nanos(dedupeWindowMs),
        allow_atomic: true,
      });
      await ensureStream(ctx, identity, owner, {
        name: dlqStream,
        subjects: [`${root}.dlq.>`],
        retention: RetentionPolicy.Limits,
        discard: DiscardPolicy.Old,
        storage,
        num_replicas: replicas,
        max_age: nanos(retention.maxAgeMs),
        max_bytes: retention.maxBytes,
        max_msgs: -1,
        max_msg_size: -1,
        duplicate_window: nanos(dedupeWindowMs),
      });
    },
    summary: async (ctx: ProvisionContext) => {
      const info = await ctx.jsm.streams.info(stream);
      const dlq = await ctx.jsm.streams.info(dlqStream);
      return {
        messages: info.state.messages,
        bytes: info.state.bytes,
        firstSequence: info.state.first_seq,
        lastSequence: info.state.last_seq,
        consumers: info.state.consumer_count,
        deadLetters: dlq.state.messages,
      } satisfies Record<string, JsonValue>;
    },
  });

  const toEvent = (envelope: Envelope, seq: number): TopicEvent<T> => ({
    data: envelope.data as T,
    eventId: extString(envelope, "eventId") ?? String(seq),
    cursor: cursorOf(identity, seq),
    sequence: seq,
    tenantId: envelope.tenantId,
    orderingKey: envelope.orderingKey,
    publishedAt: new Date(envelope.publishedAt),
    meta: envelope.meta,
  });

  const publish: Topic<T>["publish"] = async (input) => {
    runtime.assertActive();
    await declaration.ready();
    const tenantId = input.tenantId ?? DEFAULT_TENANT;
    const messageId = input.idempotencyKey
      ? `k.${subjectToken(tenantId, "tenantId")}.${subjectToken(input.idempotencyKey, "idempotencyKey")}`
      : crypto.randomUUID();
    const envelope: Envelope = {
      v: 6,
      data: input.data,
      tenantId,
      publishedAt: new Date().toISOString(),
      ...(input.orderingKey !== undefined ? { orderingKey: input.orderingKey } : {}),
      ...(input.meta !== undefined ? { meta: input.meta } : {}),
      ext: { eventId: messageId },
    };
    const bytes = encodeEnvelope(`topic ${config.id}`, envelope, maxPayloadBytes);
    const ctx = await runtime.context();
    const subject = eventSubject(tenantId);
    let expectation: { last_subject_sequence: number } | null = null;
    if (input.expectedAfter !== undefined) {
      expectation = {
        last_subject_sequence: input.expectedAfter === null ? 0 : parseCursor(identity, input.expectedAfter, "expectedAfter"),
      };
    }
    let ack;
    try {
      ack = await ctx.js.publish(subject, bytes, {
        msgID: messageId,
        ...(expectation !== null ? { expect: { lastSubjectSequence: expectation.last_subject_sequence } } : {}),
      });
    } catch (error) {
      if (expectation !== null && /wrong last sequence/i.test(asError(error).message)) {
        throw new ConflictError(
          `topic ${config.id}: expectedAfter no longer matches the tenant's latest event — re-read and retry`,
        );
      }
      throw error;
    }
    return {
      messageId,
      streamSequence: ack.seq,
      duplicate: ack.duplicate,
      eventId: messageId,
      cursor: cursorOf(identity, ack.seq),
    };
  };

  const latestCursor: Topic<T>["latestCursor"] = async (options = {}) => {
    await declaration.ready();
    const ctx = await runtime.context();
    // getMessage maps "no message found" to null in NATS.js 3.4.
    const msg = await ctx.jsm.streams.getMessage(stream, {
      last_by_subj: eventSubject(options.tenantId ?? DEFAULT_TENANT),
    });
    return msg === null ? null : cursorOf(identity, msg.seq);
  };

  const live: Topic<T>["live"] = (options = {}) => {
    const tenantId = options.tenantId ?? DEFAULT_TENANT;
    return {
      [Symbol.asyncIterator]: () => liveIterator(tenantId, options.signal),
    };
  };

  async function* liveIterator(tenantId: string, signal?: AbortSignal): AsyncGenerator<TopicLiveEvent<T>> {
    runtime.assertActive();
    await declaration.ready();
    if (signal?.aborted) return;
    const sub = runtime.nc.subscribe(eventSubject(tenantId));
    runtime.registerLiveSubscription(sub);
    const onAbort = (): void => {
      sub.unsubscribe();
    };
    signal?.addEventListener("abort", onAbort, { once: true });
    if (signal?.aborted) onAbort();
    try {
      for await (const msg of sub) {
        let envelope: Envelope;
        try {
          envelope = decodeEnvelope(msg.data);
        } catch {
          continue;
        }
        // Foreign v6-shaped messages without a Sync event identity are skipped
        // rather than being given a fabricated per-listener id.
        const eventId = envelope.ext?.eventId;
        if (typeof eventId !== "string") continue;
        yield {
          data: envelope.data as T,
          eventId,
          tenantId: envelope.tenantId,
          orderingKey: envelope.orderingKey,
          publishedAt: new Date(envelope.publishedAt),
          meta: envelope.meta,
        };
      }
    } finally {
      signal?.removeEventListener("abort", onAbort);
      runtime.unregisterLiveSubscription(sub);
      if (!sub.isClosed()) sub.unsubscribe();
    }
  }

  /**
   * Durable-log read shared by replay() and follow(). Reads the stream without
   * a server-side subject filter so stream sequences stay contiguous: any gap
   * proves messages were removed and is reported explicitly. When the log is
   * idle, a watchdog re-checks the stream so gaps caused by retention (rather
   * than observed via a delivered message) surface instead of hanging.
   */
  async function* logIterator(options: {
    tenantId: string;
    after?: TopicCursor;
    until?: TopicCursor;
    follow: boolean;
    signal?: AbortSignal;
  }): AsyncGenerator<TopicEvent<T>> {
    runtime.assertActive();
    await declaration.ready();
    if (options.signal?.aborted) return;
    const ctx = await runtime.context();
    const info = await ctx.jsm.streams.info(stream);
    const firstSeq = info.state.first_seq;
    const lastSeq = info.state.last_seq;

    const afterSeq = options.after === undefined ? null : parseCursor(identity, options.after, "after");
    const startSeq = afterSeq === null ? firstSeq : afterSeq + 1;
    if (afterSeq !== null && startSeq < firstSeq) {
      throw gapError(identity, startSeq, firstSeq);
    }
    const untilSeq = options.until === undefined ? lastSeq : parseCursor(identity, options.until, "until");
    if (!options.follow && (untilSeq < startSeq || lastSeq === 0)) return;

    const consumer = await ctx.js.consumers.get(stream, {
      deliver_policy: startSeq <= 1 ? DeliverPolicy.All : DeliverPolicy.StartSequence,
      ...(startSeq > 1 ? { opt_start_seq: startSeq } : {}),
    });
    const messages = await consumer.consume({ max_messages: 256 });
    const onAbort = (): void => messages.stop();
    options.signal?.addEventListener("abort", onAbort, { once: true });
    if (options.signal?.aborted) onAbort();
    // A brand-new stream reports first_seq 0; the first real sequence is 1.
    let expectedSeq = Math.max(startSeq, firstSeq, 1);
    // Without an explicit cursor the caller asked for "earliest available":
    // the first delivered sequence fixes the baseline (retention may advance
    // between the info call and delivery).
    let baselineFixed = afterSeq !== null;
    const next = withIdleTimeout(messages[Symbol.asyncIterator](), 5_000);
    try {
      while (true) {
        const winner = await next();
        if (winner === "idle") {
          if (options.signal?.aborted) return;
          const state = (await ctx.jsm.streams.info(stream)).state;
          if (state.first_seq > expectedSeq) {
            if (!baselineFixed) {
              // No cursor was requested: "earliest available" simply moved.
              expectedSeq = state.first_seq;
              continue;
            }
            // The events we are waiting for were removed while nothing new
            // arrived to make the gap observable through a delivered message.
            throw gapError(identity, expectedSeq, state.first_seq);
          }
          continue;
        }
        if (winner.done === true) return;
        const msg = winner.value;
        if (!baselineFixed) {
          expectedSeq = msg.seq;
          baselineFixed = true;
        }
        if (msg.seq > expectedSeq) {
          // Contiguity broken: messages were removed while reading.
          throw gapError(identity, expectedSeq, msg.seq);
        }
        expectedSeq = msg.seq + 1;
        let envelope: Envelope | null = null;
        try {
          envelope = decodeEnvelope(msg.data);
        } catch {
          envelope = null;
        }
        if (envelope !== null && envelope.tenantId === options.tenantId) {
          yield toEvent(envelope, msg.seq);
        }
        if (!options.follow && msg.seq >= untilSeq) return;
      }
    } finally {
      options.signal?.removeEventListener("abort", onAbort);
      messages.stop();
    }
  }

  const replay: Topic<T>["replay"] = (options = {}) => ({
    [Symbol.asyncIterator]: () =>
      logIterator({
        tenantId: options.tenantId ?? DEFAULT_TENANT,
        after: options.after,
        until: options.until,
        follow: false,
        signal: options.signal,
      }),
  });

  const follow: Topic<T>["follow"] = (options = {}) => ({
    [Symbol.asyncIterator]: () =>
      logIterator({
        tenantId: options.tenantId ?? DEFAULT_TENANT,
        after: options.after,
        follow: true,
        signal: options.signal,
      }),
  });

  const process: Topic<T>["process"] = async (options, handler) => {
    runtime.assertActive();
    assertName(options.consumer, "consumer");
    await declaration.ready();
    const ctx = await runtime.context();
    const delivery = resolveDelivery(options.delivery);
    const tenantId = options.tenantId ?? DEFAULT_TENANT;
    // JSON-encoding keeps ("a.b", "c") and ("a", "b.c") distinct.
    const durable = consumerName(identity, JSON.stringify([options.consumer, tenantId]));
    const start = options.start ?? "earliest";
    const startSeq = typeof start === "object" ? parseCursor(identity, start.after, "start.after") + 1 : null;
    if (startSeq !== null) {
      // A cursor below the retained window must fail like replay/follow do —
      // a fresh durable would otherwise silently skip to the first retained
      // event. Only relevant while the consumer does not exist yet; an
      // existing durable resumes from its own progress.
      const existing = await ctx.jsm.consumers.info(stream, durable).catch(() => null);
      if (existing === null) {
        const state = (await ctx.jsm.streams.info(stream)).state;
        if (startSeq < state.first_seq) throw gapError(identity, startSeq, state.first_seq);
      }
    }

    await ensureConsumer(ctx, stream, {
      durable_name: durable,
      ack_policy: AckPolicy.Explicit,
      filter_subject: eventSubject(tenantId),
      ack_wait: nanos(delivery.ackWaitMs),
      // No max_deliver: the client-side attempt guard bounds handler runs and
      // unbounded redelivery keeps the DLQ transfer crash-safe.
      max_ack_pending: delivery.maxInFlight,
      deliver_policy:
        startSeq !== null ? DeliverPolicy.StartSequence : start === "latest" ? DeliverPolicy.New : DeliverPolicy.All,
      ...(startSeq !== null ? { opt_start_seq: startSeq } : {}),
    });

    const wr = createWorkerRuntime(options, { onFinished: () => runtime.unregisterWorker(wr) });
    runtime.registerWorker(wr);
    runtime.events.emit({ type: "worker_started", resource: config.id, kind: "topic" });

    const deadLetter = async (msg: JsMsg, envelope: Envelope | null, reason: string, error?: string): Promise<void> => {
      const eventId = extString(envelope, "eventId") ?? `seq-${msg.seq}`;
      const dlqEnvelope: Envelope = {
        v: 6,
        data: envelope?.data ?? null,
        tenantId: envelope?.tenantId ?? tenantId,
        publishedAt: new Date().toISOString(),
        ext: {
          eventId,
          consumer: options.consumer,
          attempts: msg.info.deliveryCount,
          reason,
          ...(error !== undefined ? { error: error.slice(0, 2_048) } : {}),
        },
      };
      // Headroom: the transfer adds bookkeeping on top of the original payload
      // and must never be the reason an event cannot be dead-lettered.
      const bytes = encodeEnvelope(`topic ${config.id} dlq`, dlqEnvelope, maxPayloadBytes + DLQ_HEADROOM_BYTES);
      await ctx.js.publish(dlqSubject(options.consumer), bytes, {
        msgID: `dlq.${subjectToken(options.consumer, "consumer")}.${eventId}`,
      });
      await confirmedAck(msg, label).catch(() => {});
      runtime.events.emit({
        type: "dead_letter",
        resource: config.id,
        kind: "topic",
        detail: { consumer: options.consumer, eventId, reason },
      });
    };

    const onMessage = async (msg: JsMsg, signal: AbortSignal): Promise<void> => {
      let envelope: Envelope;
      try {
        envelope = decodeEnvelope(msg.data);
      } catch (error) {
        await deadLetter(msg, null, "invalid envelope", asError(error).message);
        return;
      }
      const attempt = msg.info.deliveryCount;
      if (attempt > delivery.maxAttempts) {
        await deadLetter(msg, envelope, "max attempts exhausted");
        return;
      }
      const settled = emitRun(runtime.events, config.id, "topic", {
        id: extString(envelope, "eventId") ?? String(msg.seq),
        key: options.consumer,
        attempt,
      });
      try {
        await handler({ ...toEvent(envelope, msg.seq), attempt, signal });
      } catch (error) {
        if (signal.aborted) {
          msg.nak();
          return;
        }
        const err = asError(error);
        runtime.events.emit({ type: "handler_error", resource: config.id, kind: "topic", error: err.message });
        if (attempt >= delivery.maxAttempts) {
          settled("dead_letter");
          await deadLetter(msg, envelope, "max attempts exhausted", err.message);
        } else {
          settled("retry");
          msg.nak(backoffDelayMs(delivery, attempt));
          runtime.events.emit({
            type: "redelivery",
            resource: config.id,
            kind: "topic",
            detail: { consumer: options.consumer, attempt },
          });
        }
        return;
      }
      settled("success");
      // Success settles outside the failure logic: an unconfirmable ack means
      // the delivery was superseded — never a handler failure.
      await settleSuccess(msg, label, runtime.events, "topic");
    };

    const getConsumer = (): Promise<Consumer> => ctx.js.consumers.get(stream, durable);
    runPullLoop(wr, getConsumer, onMessage, { events: runtime.events, resource: config.id }).finally(() => {
      runtime.events.emit({ type: "worker_stopped", resource: config.id, kind: "topic" });
    });
    return wr.worker;
  };

  const publishBatch: Topic<T>["publishBatch"] = async (input) => {
    runtime.assertActive();
    await declaration.ready();
    if (input.events.length === 0 || input.events.length > 1_000) {
      throw new SyncUsageError("publishBatch takes 1 to 1000 events");
    }
    const tenantId = input.tenantId ?? DEFAULT_TENANT;
    const subject = eventSubject(tenantId);
    const ctx = await runtime.context();
    const eventIds = input.events.map(() => crypto.randomUUID());
    const encoded = input.events.map((event, index) =>
      encodeEnvelope(`topic ${config.id}`, {
        v: 6,
        data: event.data,
        tenantId,
        publishedAt: new Date().toISOString(),
        ...(event.orderingKey !== undefined ? { orderingKey: event.orderingKey } : {}),
        ...(event.meta !== undefined ? { meta: event.meta } : {}),
        ext: { eventId: eventIds[index]! },
      }, maxPayloadBytes),
    );
    const expect =
      input.expectedAfter !== undefined
        ? {
            lastSubjectSequence:
              input.expectedAfter === null ? 0 : parseCursor(identity, input.expectedAfter, "expectedAfter"),
            lastSubjectSequenceSubject: subject,
          }
        : undefined;
    try {
      if (encoded.length === 1) {
        const ack = await ctx.js.publish(subject, encoded[0]!, {
          ...(expect !== undefined ? { expect: { lastSubjectSequence: expect.lastSubjectSequence } } : {}),
        });
        return { lastSequence: ack.seq, count: 1, cursor: cursorOf(identity, ack.seq), eventIds };
      }
      // The expectation rides on the first staged message; the atomic commit
      // persists all events or none.
      const batch = await ctx.js.startBatch(subject, encoded[0]!, expect !== undefined ? { expect } : {});
      for (const bytes of encoded.slice(1, -1)) batch.add(subject, bytes);
      const ack = await batch.commit(subject, encoded[encoded.length - 1]!);
      return { lastSequence: ack.seq, count: ack.count, cursor: cursorOf(identity, ack.seq), eventIds };
    } catch (error) {
      if (/atomic publish is disabled/i.test(asError(error).message)) {
        throw new SyncUsageError(`topic ${config.id}: the stream predates atomic batches (allow_atomic off); recreate it`);
      }
      // The batch commit error is generic in NATS.js 3.4 — disambiguate a
      // lost OCC race from real faults by re-reading the subject head.
      if (expect !== undefined) {
        const head = await ctx.jsm.streams.getMessage(stream, { last_by_subj: subject }).catch(() => null);
        const headSeq = head === null ? 0 : head.seq;
        if (headSeq !== expect.lastSubjectSequence) {
          throw new ConflictError(
            `topic ${config.id}: expectedAfter no longer matches the tenant's latest event — re-read and retry`,
          );
        }
      }
      throw error;
    }
  };

  const pauseConsumer: Topic<T>["pauseConsumer"] = async (input) => {
    runtime.assertActive();
    assertName(input.consumer, "consumer");
    await declaration.ready();
    const ctx = await runtime.context();
    const durable = consumerName(identity, JSON.stringify([input.consumer, input.tenantId ?? DEFAULT_TENANT]));
    const until = new Date(Date.now() + (input.untilMs ?? 365 * 24 * 60 * 60 * 1_000));
    try {
      const result = await ctx.jsm.consumers.pause(stream, durable, until);
      return { paused: result.paused, ...(result.pause_until !== undefined ? { pauseUntil: new Date(result.pause_until) } : {}) };
    } catch (error) {
      if (/consumer not found/i.test(asError(error).message)) {
        throw new NotFoundError(`topic ${config.id}: consumer ${input.consumer} has not been created yet (no process() ran)`);
      }
      throw error;
    }
  };

  const resumeConsumer: Topic<T>["resumeConsumer"] = async (input) => {
    runtime.assertActive();
    assertName(input.consumer, "consumer");
    await declaration.ready();
    const ctx = await runtime.context();
    const durable = consumerName(identity, JSON.stringify([input.consumer, input.tenantId ?? DEFAULT_TENANT]));
    try {
      const result = await ctx.jsm.consumers.resume(stream, durable);
      return { paused: result.paused };
    } catch (error) {
      if (/consumer not found/i.test(asError(error).message)) {
        throw new NotFoundError(`topic ${config.id}: consumer ${input.consumer} has not been created yet (no process() ran)`);
      }
      throw error;
    }
  };

  const hubs = new Map<string, TopicHub<T>>();

  const hub: Topic<T>["hub"] = (hubOptions = {}) => {
    const tenantId = hubOptions.tenantId ?? DEFAULT_TENANT;
    const existing = hubs.get(tenantId);
    if (existing !== undefined) return existing;
    type Sub = {
      buffer: TopicEvent<T>[];
      limit: number;
      lastSeq: number;
      overflowed: boolean;
      failed: Error | null;
      notify: (() => void) | null;
      done: boolean;
    };
    const subs = new Set<Sub>();
    const follower = new AbortController();
    let followerStarted = false;
    let headSeq = 0;
    let closed = false;
    /** Set when the shared follower died; the hub is retired and re-created on the next hub() call. */
    let failure: Error | null = null;

    const retire = (): void => {
      closed = true;
      if (hubs.get(tenantId) === created) hubs.delete(tenantId);
    };

    const wake = (sub: Sub): void => {
      sub.notify?.();
      sub.notify = null;
    };

    const ensureFollower = (): void => {
      if (followerStarted || closed) return;
      followerStarted = true;
      (async () => {
        try {
          // The shared tail starts at the current head: history is served per
          // subscriber via replay, never through the live buffer.
          const head = await latestCursor({ tenantId });
          if (head !== null) headSeq = parseCursor(identity, head, "cursor");
          for await (const event of follow({ tenantId, ...(head !== null ? { after: head } : {}), signal: follower.signal })) {
            headSeq = parseCursor(identity, event.cursor, "cursor");
            for (const sub of subs) {
              if (sub.done) continue;

              if (sub.buffer.length >= sub.limit) {
                // Slow subscriber: end it explicitly instead of buffering
                // without bound — it resubscribes from its last cursor.
                sub.overflowed = true;
                wake(sub);
                continue;
              }
              sub.buffer.push(event);
              wake(sub);
            }
          }
        } catch (error) {
          failure = asError(error);
          for (const sub of subs) {
            sub.failed = failure;
            wake(sub);
          }
        } finally {
          // A follower that ended (error or close) must not keep serving a
          // memoized hub whose subscribers would wait forever.
          retire();
        }
      })();
    };

    const subscribe: TopicHub<T>["subscribe"] = (options = {}) => ({
      [Symbol.asyncIterator]: () => subscriber(options),
    });

    async function* subscriber(options: {
      after?: TopicCursor;
      bufferLimit?: number;
      signal?: AbortSignal;
    }): AsyncGenerator<TopicEvent<T>> {
      runtime.assertActive();
      if (failure !== null) throw failure;
      if (closed || options.signal?.aborted) return;
      const sub: Sub = {
        buffer: [],
        limit: options.bufferLimit ?? 1_024,
        lastSeq: 0,
        overflowed: false,
        failed: null,
        notify: null,
        done: false,
      };
      // Register BEFORE the catch-up replay so no event can fall between the
      // replay head and the live tail; duplicates are dropped by sequence.
      subs.add(sub);
      ensureFollower();
      // Live-only subscribers must not receive events from before they
      // subscribed (buffered for other subscribers or raced in).
      if (options.after === undefined) sub.lastSeq = headSeq;
      const onAbort = (): void => {
        sub.done = true;
        wake(sub);
      };
      options.signal?.addEventListener("abort", onAbort, { once: true });
      try {
        if (options.after !== undefined) {
          for await (const event of replay({ tenantId, after: options.after })) {
            sub.lastSeq = parseCursor(identity, event.cursor, "cursor");
            yield event;
          }
        }
        while (!sub.done) {
          // A dead follower must surface as an error even though retiring the
          // hub also marks it closed; a plain close() ends subscribers quietly.
          if (sub.failed !== null) throw sub.failed;
          if (closed) return;
          if (sub.overflowed) {
            throw new RetentionGapError(
              cursorOf(identity, sub.lastSeq + 1),
              cursorOf(identity, sub.lastSeq + 1),
              cursorOf(identity, sub.lastSeq),
            );
          }
          const event = sub.buffer.shift();
          if (event === undefined) {
            await new Promise<void>((resolve) => {
              sub.notify = resolve;
            });
            continue;
          }
          const seq = parseCursor(identity, event.cursor, "cursor");
          if (seq <= sub.lastSeq) continue; // replay/live overlap
          sub.lastSeq = seq;
          yield event;
        }
      } finally {
        sub.done = true;
        subs.delete(sub);
        options.signal?.removeEventListener("abort", onAbort);
      }
    }

    const created: TopicHub<T> = {
      subscribe,
      close: () => {
        retire();
        follower.abort();
        for (const sub of subs) {
          sub.done = true;
          wake(sub);
        }
      },
    };
    hubs.set(tenantId, created);
    return created;
  };

  const cursorSequence: Topic<T>["cursorSequence"] = (cursor) => parseCursor(identity, cursor, "cursor");

  const cursorAt: Topic<T>["cursorAt"] = (sequence) => {
    if (!Number.isSafeInteger(sequence) || sequence < 0) throw new RangeError("sequence must be a non-negative integer");
    return cursorOf(identity, sequence);
  };

  return {
    ready: () => declaration.ready(),
    publish,
    publishBatch,
    latestCursor,
    live,
    replay,
    follow,
    process,
    hub,
    cursorSequence,
    cursorAt,
    pauseConsumer,
    resumeConsumer,
  };
};
