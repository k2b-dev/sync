import { AckPolicy, DeliverPolicy, DiscardPolicy, RetentionPolicy } from "@nats-io/jetstream";
import type { JsMsg, StoredMsg } from "@nats-io/jetstream";
import { headers as natsHeaders } from "@nats-io/nats-core";
import type { KV } from "@nats-io/kv";
import { assertValidTimeZone, nextCronTimestamp } from "./cron.ts";
import { decodeJson, encodeJson } from "./codec.ts";
import type { JsonValue } from "./codec.ts";
import { confirmedAck, emitRun, runPullLoop, settleSuccess } from "./consume.ts";
import { NotFoundError, SyncUsageError, asError } from "./errors.ts";
import { assertName, consumerName, resourceIdentity, streamName, subjectRoot, subjectToken, decodeSubjectToken } from "./naming.ts";
import { ensureConsumer, ensureKv, ensureStream, setConsumerPause, toStorageType } from "./resources.ts";
import type { ProvisionContext } from "./resources.ts";
import type { SyncRuntime } from "./runtime.ts";
import { backoffDelayMs, millis, nanos, resolveDelivery } from "./types.ts";
import type { DeliveryConfig, MessageMeta } from "./types.ts";
import { createWorkerRuntime } from "./worker.ts";
import type { ProcessOptions, Worker, WorkerRuntime } from "./worker.ts";

// ==========================
// Types
// ==========================

export type SchedulerConfig = {
  id: string;
  owner?: string;
  /** Handler delivery policy. `maxInFlight` is ignored: schedules execute serially (one in-flight tick per schedule). */
  delivery?: DeliveryConfig;
  replicas?: number;
  /**
   * Tick retention. Global size limits are deliberately not configurable:
   * with discard-old they would eventually evict the broker-side schedule
   * definitions themselves and silently stop the clock (server-verified).
   */
  retention?: {
    /** How long accepted ticks stay retained. Default 7 days. */
    maxAgeMs?: number;
    /** Retained ticks/manual runs per schedule. Default 10_000. */
    maxTicksPerSchedule?: number;
  };
};

export type ScheduleContext = {
  scheduleId: string;
  runId: string;
  runNumber: number;
  slot: Date;
  trigger: "schedule" | "manual";
  attempt: number;
  signal: AbortSignal;
  heartbeat(): Promise<void>;
};

export type ScheduleDefinition = {
  id: string;
  /** Standard five-field cron: minute hour day month weekday. */
  cron: string;
  timezone?: string;
  misfire?: "latest" | "all";
  meta?: MessageMeta;
  process(context: ScheduleContext): Promise<void>;
};

export type ScheduleInfo = {
  id: string;
  cron: string;
  timezone: string;
  misfire: "latest" | "all";
  nextRunAt: Date;
  runNumber: number;
  failureCount: number;
  /** Message of the most recent terminally failed run; cleared by the next success. */
  lastError?: string;
  lastRunId?: string;
  lastCompletedAt?: Date;
  /** True when THIS process has a handler registered (process-local view). */
  handlerAvailable: boolean;
  createdAt: Date;
  updatedAt: Date;
  meta?: MessageMeta;
};

export type Scheduler = {
  ready(): Promise<void>;
  create(config: ScheduleDefinition): Promise<{ created: boolean; updated: boolean }>;
  /** Serve locally created schedules. Like every process(), returns a Worker. */
  process(options?: ProcessOptions): Promise<Worker>;
  delete(input: { id: string }): Promise<boolean>;
  /**
   * Durably accept a manual run. Repeating the same requestId within the
   * stream's duplicate window (120 s) is deduplicated to the same run; beyond
   * the window a repeated requestId is accepted again (at-least-once —
   * consumers needing permanent uniqueness enforce it in their own store).
   */
  runNow(input: { id: string; requestId: string }): Promise<{ runId: string }>;
  /**
   * Wait until a specific run (from runNow) has settled terminally. Resolves
   * `{ completed: true, error? }` when the run finished (error set when it
   * dead-ended), or `{ completed: false }` on timeout — the run stays
   * accepted and will still execute.
   */
  awaitRun(input: { id: string; runId: string; timeoutMs?: number }): Promise<{ completed: boolean; error?: string }>;
  /** Pause execution of one schedule (ticks keep accruing; misfire policy applies on resume). */
  pause(input: { id: string; untilMs?: number }): Promise<{ paused: boolean; pauseUntil?: Date }>;
  resume(input: { id: string }): Promise<{ paused: boolean }>;
  get(input: { id: string }): Promise<ScheduleInfo | null>;
  list(): Promise<ScheduleInfo[]>;
};

type DefRecord = {
  id: string;
  cron: string;
  timezone: string;
  misfire: "latest" | "all";
  meta?: MessageMeta;
  createdAt: string;
  updatedAt: string;
};

type RunRecord = {
  runNumber: number;
  lastRun: string;
  failureCount: number;
  lastError?: string;
  lastRunId?: string;
  lastCompletedAt?: string;
};

// ==========================
// Scheduler factory
// ==========================

export const createScheduler = (runtime: SyncRuntime, config: SchedulerConfig): Scheduler => {
  const identity = resourceIdentity(runtime.namespace, "scheduler", config.id);
  const owner = config.owner ?? runtime.application;
  const delivery = resolveDelivery(config.delivery);
  const retentionMaxAgeMs = config.retention?.maxAgeMs ?? 7 * 24 * 60 * 60 * 1_000;
  const maxTicksPerSchedule = config.retention?.maxTicksPerSchedule ?? 10_000;
  if (!Number.isSafeInteger(retentionMaxAgeMs) || retentionMaxAgeMs <= 0) {
    throw new RangeError("retention.maxAgeMs must be a positive integer");
  }
  if (!Number.isSafeInteger(maxTicksPerSchedule) || maxTicksPerSchedule <= 0) {
    throw new RangeError("retention.maxTicksPerSchedule must be a positive integer");
  }
  const replicas = config.replicas ?? runtime.defaults.replicas;
  const storage = toStorageType(runtime.defaults.storage);
  const stream = streamName(identity);
  const bucket = `S6_SK_${identity.hash}`;
  const root = subjectRoot(identity);
  const label = `scheduler ${config.id}`;

  const schedSubject = (id: string): string => `${root}.sched.${subjectToken(id, "schedule id")}`;
  const tickSubject = (id: string): string => `${root}.tick.${subjectToken(id, "schedule id")}`;
  const manualSubject = (id: string): string => `${root}.run.${subjectToken(id, "schedule id")}`;

  /** Locally registered schedules (handler + definition). */
  const registry = new Map<string, ScheduleDefinition & { timezone: string; misfire: "latest" | "all" }>();
  /** Active workers so create() can attach loops to already started workers. */
  const activeWorkers = new Set<{ wr: WorkerRuntime; served: Set<string> }>();

  let kv: KV | null = null;

  const declaration = runtime.declare({
    identity,
    owner,
    // maxInFlight is ignored (serial per schedule) and excluded so it can
    // never cause declaration conflicts without having an effect.
    configKey: JSON.stringify([
      "scheduler",
      config.id,
      owner,
      { ackWaitMs: delivery.ackWaitMs, maxAttempts: delivery.maxAttempts, backoffMs: delivery.backoffMs },
      { maxAgeMs: retentionMaxAgeMs, maxTicksPerSchedule },
    ]),
    natsNames: [stream, `KV_${bucket}`],
    provision: async (ctx: ProvisionContext) => {
      await ensureStream(ctx, identity, owner, {
        name: stream,
        subjects: [`${root}.sched.*`, `${root}.tick.*`, `${root}.run.*`, `${root}.cancel`],
        retention: RetentionPolicy.Limits,
        discard: DiscardPolicy.Old,
        storage,
        num_replicas: replicas,
        // Ticks age out; schedule messages survive via Nats-TTL: never.
        // Bounds are per subject: global max_msgs/max_bytes with discard-old
        // would evict the oldest messages — the schedule definitions — and
        // silently stop the broker clock (server-verified).
        max_age: nanos(retentionMaxAgeMs),
        max_bytes: -1,
        max_msgs: -1,
        max_msgs_per_subject: maxTicksPerSchedule,
        max_msg_size: -1,
        duplicate_window: nanos(120_000),
        allow_msg_schedules: true,
        allow_msg_ttl: true,
        allow_rollup_hdrs: true,
      });
      kv = await ensureKv(ctx, identity, owner, bucket, {
        history: 1,
        replicas,
        storage,
        markerTTL: 1_000,
      });
    },
    summary: async (ctx: ProvisionContext) => {
      const info = await ctx.jsm.streams.info(stream, { subjects_filter: `${root}.sched.*` });
      const status = await (kv ?? (await ctx.kvm.open(bucket))).status();
      return {
        schedules: Object.keys(info.state.subjects ?? {}).length,
        messages: info.state.messages,
        kvEntries: status.values,
      } satisfies Record<string, JsonValue>;
    },
  });

  const getKv = async (): Promise<KV> => {
    await declaration.ready();
    if (kv === null) {
      const ctx = await runtime.context();
      kv = await ctx.kvm.open(bucket);
    }
    return kv;
  };

  const defKey = (id: string): string => `def.${subjectToken(id, "schedule id")}`;
  const runKey = (id: string): string => `run.${subjectToken(id, "schedule id")}`;

  const loadDef = async (id: string): Promise<{ record: DefRecord; revision: number } | null> => {
    const store = await getKv();
    // A freshly created R3 bucket can briefly have no direct-get responders.
    for (let attempt = 0; ; attempt++) {
      try {
        const entry = await store.get(defKey(id));
        if (entry === null || entry.operation !== "PUT") return null;
        return { record: decodeJson<DefRecord>(entry.value), revision: entry.revision };
      } catch (error) {
        if (attempt >= 4 || !/no responders/i.test(asError(error).message)) throw error;
        await Bun.sleep(150);
      }
    }
  };

  const loadRun = async (id: string): Promise<RunRecord> => {
    const store = await getKv();
    const entry = await store.get(runKey(id));
    if (entry === null || entry.operation !== "PUT") return { runNumber: 0, lastRun: "", failureCount: 0 };
    return decodeJson<RunRecord>(entry.value);
  };

  const saveRun = async (id: string, mutate: (record: RunRecord) => void): Promise<RunRecord> => {
    const store = await getKv();
    for (let attempt = 0; attempt < 10; attempt++) {
      const entry = await store.get(runKey(id));
      const record =
        entry === null || entry.operation !== "PUT"
          ? { runNumber: 0, lastRun: "", failureCount: 0 }
          : decodeJson<RunRecord>(entry.value);
      mutate(record);
      const bytes = encodeJson(label, record, 4_096);
      try {
        if (entry === null || entry.operation !== "PUT") await store.create(runKey(id), bytes);
        else await store.update(runKey(id), bytes, entry.revision);
        return record;
      } catch (error) {
        if (!/wrong last sequence|already exists/i.test(asError(error).message)) throw error;
      }
    }
    throw new Error(`${label}: run record for ${id} is contended`);
  };

  /**
   * Publish (or replace, via per-subject rollup) the broker-side schedule
   * message, then verify the newest retained schedule message really carries
   * this (or a newer) definition revision: with per-subject rollup the last
   * write wins, so a delayed publish of an older revision could otherwise
   * silently reinstate a stale schedule.
   */
  const publishScheduleMessage = async (record: DefRecord, revision: number, attempt: number): Promise<void> => {
    const ctx = await runtime.context();
    const hdrs = natsHeaders();
    // The schedule message must outlive the stream's tick retention.
    hdrs.set("Nats-TTL", "never");
    await ctx.js.publish(
      schedSubject(record.id),
      encodeJson(label, { scheduleId: record.id, trigger: "schedule", revision }, 16 * 1024),
      {
        headers: hdrs,
        msgID: `sched.${subjectToken(record.id, "schedule id")}.${revision}.${attempt}`,
        schedule: {
          // NATS uses six-field cron; Sync's public contract stays five-field.
          specification: { cron: `0 ${record.cron}` },
          target: tickSubject(record.id),
          timezone: record.timezone,
          rollup: "sub",
        },
      },
    );
  };

  /**
   * Publish (via per-subject rollup) the broker-side schedule message and
   * converge: last-write-wins means a delayed publish of an OLDER definition
   * can land after a newer one, so the loop compares the retained message
   * against the current KV head and republishes the head until they match.
   */
  const publishSchedule = async (record: DefRecord, revision: number): Promise<void> => {
    const ctx = await runtime.context();
    const subject = schedSubject(record.id);
    let current = { record, revision };
    for (let attempt = 0; attempt < 5; attempt++) {
      await publishScheduleMessage(current.record, current.revision, attempt);
      const last = await ctx.jsm.streams.getMessage(stream, { last_by_subj: subject }).catch(() => null);
      const retained = last === null ? -1 : (decodeJson<{ revision?: number }>(last.data).revision ?? -1);
      const head = await loadDef(record.id);
      if (head === null) return; // deleted concurrently — delete() cancels the schedule
      if (retained >= head.revision) return;
      current = head; // a newer definition exists or the rollup lost — publish the head
    }
    throw new Error(`${label}: schedule ${record.id} could not be republished consistently`);
  };

  // ==========================
  // Public operations
  // ==========================

  const create: Scheduler["create"] = async (definition) => {
    runtime.assertActive();
    assertName(definition.id, "schedule id");
    const timezone = definition.timezone ?? "UTC";
    assertValidTimeZone(timezone);
    const misfire = definition.misfire ?? "latest";
    try {
      nextCronTimestamp(definition.cron, timezone, Date.now());
    } catch (error) {
      throw new SyncUsageError(`invalid cron for schedule ${definition.id}: ${asError(error).message}`);
    }
    await declaration.ready();

    const store = await getKv();
    const existing = await loadDef(definition.id);
    const now = new Date().toISOString();
    const record: DefRecord = {
      id: definition.id,
      cron: definition.cron,
      timezone,
      misfire,
      ...(definition.meta !== undefined ? { meta: definition.meta } : {}),
      createdAt: existing?.record.createdAt ?? now,
      updatedAt: now,
    };
    const unchanged =
      existing !== null &&
      existing.record.cron === record.cron &&
      existing.record.timezone === record.timezone &&
      existing.record.misfire === record.misfire &&
      JSON.stringify(existing.record.meta ?? null) === JSON.stringify(record.meta ?? null);

    let revision: number | null = null;
    // The result flags reflect what THIS call actually did — a racer that
    // loses the KV create reports neither created nor updated.
    let didCreate = false;
    let didUpdate = false;
    for (let attempt = 0; attempt < 10 && revision === null; attempt++) {
      try {
        const current = attempt === 0 ? existing : await loadDef(definition.id);
        if (current === null) {
          revision = await store.create(defKey(definition.id), encodeJson(label, record, 16 * 1024));
          didCreate = true;
        } else if (unchanged || JSON.stringify({ cron: current.record.cron, timezone: current.record.timezone, misfire: current.record.misfire, meta: current.record.meta ?? null }) === JSON.stringify({ cron: record.cron, timezone: record.timezone, misfire: record.misfire, meta: record.meta ?? null })) {
          revision = current.revision;
        } else {
          record.createdAt = current.record.createdAt;
          revision = await store.update(defKey(definition.id), encodeJson(label, record, 16 * 1024), current.revision);
          didUpdate = true;
        }
      } catch (error) {
        // Two pods creating/updating the same schedule concurrently is normal
        // (CAS conflicts), and a freshly created R3 bucket may briefly have no
        // direct-get responders — reload and retry instead of surfacing raw
        // transport errors.
        if (!/wrong last sequence|already exists|no responders/i.test(asError(error).message)) throw error;
        await Bun.sleep(100);
      }
    }
    if (revision === null) throw new Error(`${label}: schedule ${definition.id} is contended`);
    // Always republish: self-heals a lost schedule message; the stable message
    // ID plus per-subject rollup make this idempotent.
    await publishSchedule(record, revision);

    registry.set(definition.id, { ...definition, timezone, misfire });
    for (const active of activeWorkers) attachLoop(active, definition.id);
    return { created: didCreate, updated: didUpdate };
  };

  const deleteSchedule: Scheduler["delete"] = async (input) => {
    runtime.assertActive();
    await declaration.ready();
    const ctx = await runtime.context();
    const store = await getKv();
    const existing = await loadDef(input.id);
    registry.delete(input.id);
    if (existing === null) return false;
    // Cancel the broker schedule, then remove definition and run state. The
    // cancel must be published on a different subject than the schedule itself.
    await ctx.js.publish(`${root}.cancel`, "", {
      cancelSchedule: { scheduleSubject: schedSubject(input.id) },
    });
    await store.purge(defKey(input.id)).catch(() => {});
    await store.purge(runKey(input.id)).catch(() => {});
    // Drop retained ticks and manual runs: processed-ness lived in the deleted
    // consumer, so a later re-create must not resurrect old history.
    await ctx.jsm.streams.purge(stream, { filter: tickSubject(input.id) }).catch(() => {});
    await ctx.jsm.streams.purge(stream, { filter: manualSubject(input.id) }).catch(() => {});
    await ctx.jsm.consumers.delete(stream, consumerName(identity, input.id)).catch(() => {});
    return true;
  };

  const runNow: Scheduler["runNow"] = async (input) => {
    runtime.assertActive();
    assertName(input.requestId, "requestId");
    await declaration.ready();
    const existing = await loadDef(input.id);
    if (existing === null) throw new NotFoundError(`schedule ${input.id} does not exist`);
    const ctx = await runtime.context();
    const runId = `${input.id}:manual:${input.requestId}`;
    await ctx.js.publish(
      manualSubject(input.id),
      encodeJson(label, { scheduleId: input.id, trigger: "manual", requestId: input.requestId }, 16 * 1024),
      { msgID: `manual.${subjectToken(input.id, "schedule id")}.${subjectToken(input.requestId, "requestId")}` },
    );
    // Durably accepted — not started, not completed.
    return { runId };
  };

  const toInfo = async (record: DefRecord): Promise<ScheduleInfo> => {
    const run = await loadRun(record.id);
    return {
      id: record.id,
      cron: record.cron,
      timezone: record.timezone,
      misfire: record.misfire,
      nextRunAt: new Date(nextCronTimestamp(record.cron, record.timezone, Date.now())),
      runNumber: run.runNumber,
      failureCount: run.failureCount,
      ...(run.lastError !== undefined ? { lastError: run.lastError } : {}),
      ...(run.lastRunId !== undefined ? { lastRunId: run.lastRunId } : {}),
      ...(run.lastCompletedAt !== undefined ? { lastCompletedAt: new Date(run.lastCompletedAt) } : {}),
      handlerAvailable: registry.has(record.id),
      createdAt: new Date(record.createdAt),
      updatedAt: new Date(record.updatedAt),
      ...(record.meta !== undefined ? { meta: record.meta } : {}),
    };
  };

  const get: Scheduler["get"] = async (input) => {
    runtime.assertActive();
    const existing = await loadDef(input.id);
    return existing === null ? null : toInfo(existing.record);
  };

  const list: Scheduler["list"] = async () => {
    runtime.assertActive();
    const store = await getKv();
    const infos: ScheduleInfo[] = [];
    const keys = await store.keys("def.>");
    for await (const rawKey of keys) {
      const id = decodeSubjectToken(rawKey.slice("def.".length));
      const existing = await loadDef(id);
      if (existing !== null) infos.push(await toInfo(existing.record));
    }
    return infos.toSorted((a, b) => a.id.localeCompare(b.id));
  };

  // ==========================
  // Tick execution
  // ==========================

  const handleTick = async (scheduleId: string, msg: JsMsg, signal: AbortSignal): Promise<void> => {
    const definition = registry.get(scheduleId);
    const existing = await loadDef(scheduleId);
    if (definition === undefined || existing === null) {
      // Deleted or foreign schedule: drop the tick permanently.
      msg.term();
      return;
    }
    let payload: { trigger?: string; requestId?: string };
    try {
      payload = decodeJson(msg.data);
    } catch {
      msg.term();
      return;
    }
    const trigger: "schedule" | "manual" = payload.trigger === "manual" ? "manual" : "schedule";
    const slot = new Date(millis(msg.info.timestampNanos));

    const attempt = msg.info.deliveryCount;
    if (attempt > delivery.maxAttempts) {
      // Crash boundary: the handler exhausted its attempts across process
      // deaths; count the failure and drop the slot.
      await saveRun(scheduleId, (r) => {
        r.failureCount += 1;
        r.lastError = "attempts exhausted across process restarts";
        r.lastCompletedAt = new Date().toISOString();
      });
      await confirmedAck(msg, label).catch(() => {});
      return;
    }

    // Misfire "latest": a scheduled tick with a newer tick behind it on the
    // same subject is a stale slot — skip it explicitly. The newest retained
    // tick always executes, so accepted latest work is never lost.
    if (trigger === "schedule" && existing.record.misfire === "latest") {
      const ctx = await runtime.context();
      let last: StoredMsg | null;
      try {
        last = await ctx.jsm.streams.getMessage(stream, { last_by_subj: tickSubject(scheduleId) });
      } catch {
        // "Execute only the newest slot" needs the lookup; retry shortly
        // instead of guessing and executing a possibly stale slot.
        msg.nak(2_000);
        return;
      }
      if (last !== null && last.seq > msg.seq) {
        runtime.events.emit({
          type: "schedule_misfire",
          resource: config.id,
          kind: "scheduler",
          detail: { scheduleId, skippedSlot: slot.toISOString() },
        });
        await confirmedAck(msg, label).catch(() => {});
        return;
      }
    }

    const runIdentity = trigger === "manual" ? `m:${payload.requestId}` : `s:${slot.toISOString()}`;
    const runId = trigger === "manual" ? `${scheduleId}:manual:${payload.requestId}` : `${scheduleId}:${slot.toISOString()}`;
    // Duplicate delivery of the same slot reuses its run number.
    const run = await loadRun(scheduleId);
    const runNumber =
      run.lastRun === runIdentity
        ? run.runNumber
        : (await saveRun(scheduleId, (r) => {
            if (r.lastRun !== runIdentity) {
              r.runNumber += 1;
              r.lastRun = runIdentity;
            }
          })).runNumber;

    runtime.events.emit({
      type: "schedule_tick",
      resource: config.id,
      kind: "scheduler",
      detail: { scheduleId, runId, runNumber, trigger, attempt },
    });
    const settled = emitRun(runtime.events, config.id, "scheduler", { id: runId, key: scheduleId, attempt });
    try {
      await definition.process({
        scheduleId,
        runId,
        runNumber,
        slot,
        trigger,
        attempt,
        signal,
        heartbeat: async () => {
          msg.working();
        },
      });
    } catch (error) {
      if (signal.aborted) {
        msg.nak();
        return;
      }
      const err = asError(error);
      runtime.events.emit({ type: "handler_error", resource: config.id, kind: "scheduler", error: err.message });
      if (attempt >= delivery.maxAttempts) {
        settled("dead_letter");
        await saveRun(scheduleId, (r) => {
          r.failureCount += 1;
          r.lastError = err.message.slice(0, 2_048);
          r.lastRunId = runId;
          r.lastCompletedAt = new Date().toISOString();
        });
        await confirmedAck(msg, label).catch(() => {});
      } else {
        settled("retry");
        msg.nak(backoffDelayMs(delivery, attempt));
      }
      return;
    }
    settled("success");
    // Completion bookkeeping BEFORE the ack: a crash in between redelivers
    // and repeats the (idempotent) write.
    await saveRun(scheduleId, (r) => {
      r.lastRunId = runId;
      r.lastCompletedAt = new Date().toISOString();
      delete r.lastError;
    });
    // Success settles outside the failure logic: an unconfirmable ack means
    // the delivery was superseded — never a handler failure.
    await settleSuccess(msg, label, runtime.events, "scheduler");
  };

  const attachLoop = (active: { wr: WorkerRuntime; served: Set<string> }, scheduleId: string): void => {
    if (active.served.has(scheduleId) || active.wr.stopping) return;
    active.served.add(scheduleId);
    const durable = consumerName(identity, scheduleId);
    const setup = async (): Promise<void> => {
      await ensureScheduleConsumer(scheduleId);
      await runPullLoop(
        active.wr,
        async () => {
          const ctx = await runtime.context();
          try {
            return await ctx.js.consumers.get(stream, durable);
          } catch (error) {
            // delete() removes the consumer; a re-create() needs it back.
            if (registry.has(scheduleId)) {
              await ensureScheduleConsumer(scheduleId);
              return ctx.js.consumers.get(stream, durable);
            }
            throw error;
          }
        },
        async (msg, signal) => {
          try {
            await handleTick(scheduleId, msg, signal);
          } catch (error) {
            runtime.events.emit({ type: "handler_error", resource: config.id, kind: "scheduler", error: asError(error).message });
            msg.nak(5_000);
          }
        },
        {
          events: runtime.events,
          resource: config.id,
          // Stop serving deleted schedules; re-create() re-attaches.
          shouldStop: () => !registry.has(scheduleId),
          maxBatch: 1,
          pollExpiresMs: 1_500,
        },
      );
      active.served.delete(scheduleId);
    };
    setup().catch((error) => {
      runtime.events.emit({ type: "handler_error", resource: config.id, kind: "scheduler", error: asError(error).message });
      active.served.delete(scheduleId);
      // A transient provisioning failure must not permanently disable the
      // schedule on this worker.
      const timer = setTimeout(() => {
        if (!active.wr.stopping && registry.has(scheduleId)) attachLoop(active, scheduleId);
      }, 5_000);
      timer.unref?.();
    });
  };

  const process: Scheduler["process"] = async (options = {}) => {
    runtime.assertActive();
    await declaration.ready();
    const wr = createWorkerRuntime(options, {
      onFinished: () => {
        runtime.unregisterWorker(wr);
        activeWorkers.delete(active);
      },
    });
    const active = { wr, served: new Set<string>() };
    runtime.registerWorker(wr);
    activeWorkers.add(active);
    runtime.events.emit({ type: "worker_started", resource: config.id, kind: "scheduler" });
    for (const scheduleId of registry.keys()) attachLoop(active, scheduleId);
    return wr.worker;
  };

  /** The per-schedule serial consumer (also created lazily by process()). */
  const ensureScheduleConsumer = async (scheduleId: string): Promise<void> => {
    const ctx = await runtime.context();
    await ensureConsumer(ctx, stream, {
      durable_name: consumerName(identity, scheduleId),
      ack_policy: AckPolicy.Explicit,
      filter_subjects: [tickSubject(scheduleId), manualSubject(scheduleId)],
      ack_wait: nanos(delivery.ackWaitMs),
      // No max_deliver: the client-side attempt guard bounds handler runs
      // and unbounded redelivery keeps failure accounting crash-safe.
      // Serial execution per schedule: no overlapping runs.
      max_ack_pending: 1,
      deliver_policy: DeliverPolicy.All,
    });
  };

  const awaitRun: Scheduler["awaitRun"] = async (input) => {
    runtime.assertActive();
    const deadline = Date.now() + (input.timeoutMs ?? 30_000);
    while (Date.now() < deadline) {
      const run = await loadRun(input.id);
      if (run.lastRunId === input.runId && run.lastCompletedAt !== undefined) {
        return { completed: true, ...(run.lastError !== undefined ? { error: run.lastError } : {}) };
      }
      await Bun.sleep(250);
    }
    return { completed: false };
  };

  const pause: Scheduler["pause"] = async (input) => {
    runtime.assertActive();
    const existing = await loadDef(input.id);
    if (existing === null) throw new NotFoundError(`schedule ${input.id} does not exist`);
    await ensureScheduleConsumer(input.id);
    const ctx = await runtime.context();
    const until = new Date(Date.now() + (input.untilMs ?? 365 * 24 * 60 * 60 * 1_000));
    const result = await setConsumerPause(ctx.jsm, stream, consumerName(identity, input.id), until, Boolean(runtime.nc.info?.cluster));
    return { paused: result.paused, ...(result.pause_until !== undefined ? { pauseUntil: new Date(result.pause_until) } : {}) };
  };

  const resume: Scheduler["resume"] = async (input) => {
    runtime.assertActive();
    const existing = await loadDef(input.id);
    if (existing === null) throw new NotFoundError(`schedule ${input.id} does not exist`);
    await ensureScheduleConsumer(input.id);
    const ctx = await runtime.context();
    const result = await setConsumerPause(ctx.jsm, stream, consumerName(identity, input.id), new Date(0), Boolean(runtime.nc.info?.cluster));
    return { paused: result.paused };
  };

  return {
    ready: () => declaration.ready(),
    create,
    process,
    delete: deleteSchedule,
    runNow,
    awaitRun,
    pause,
    resume,
    get,
    list,
  };
};
