import { AckPolicy, DeliverPolicy, DiscardPolicy, RetentionPolicy } from "@nats-io/jetstream";
import type { JsMsg } from "@nats-io/jetstream";
import type { KV } from "@nats-io/kv";
import { decodeJson, encodeJson } from "./codec.ts";
import type { JsonValue } from "./codec.ts";
import { runPullLoop } from "./consume.ts";
import { PayloadTooLargeError, asError } from "./errors.ts";
import { kvCasPut } from "./kv.ts";
import { assertName, kvBucketName, resourceIdentity, subjectRoot, subjectToken, decodeSubjectToken } from "./naming.ts";
import { ensureConsumer, ensureKv, ensureStream, toStorageType } from "./resources.ts";
import type { ProvisionContext } from "./resources.ts";
import type { SyncRuntime } from "./runtime.ts";
import { DEFAULT_MESSAGE_PAYLOAD_BYTES, nanos } from "./types.ts";
import type { MessageMeta } from "./types.ts";
import { createWorkerRuntime } from "./worker.ts";
import type { ProcessOptions, Worker } from "./worker.ts";

// ==========================
// Types
// ==========================

export type PumpItem = { key: string };

export type PumpConfig<Input, Cursor, Item extends PumpItem> = {
  id: string;
  owner?: string;
  /** Items requested per pull(). Default 100. */
  batchSize?: number;
  /** Simultaneously dispatched items inside each active run. Default 1. */
  dispatchConcurrency?: number;
  /** Global ceiling of concurrently leased runs across all pods. Default 100. */
  maxActiveRuns?: number;
  retention?: {
    /** How long terminal run state stays readable. Default 7 days. */
    terminalMs?: number;
  };
  /** Maximum encoded bytes of one persisted run record incl. its page. Default 128 KiB. */
  maxPageBytes?: number;
  replicas?: number;
  retry?: { maxAttempts?: number; backoffMs?: number[] };
  /** Run lease duration: crash takeover latency vs. duplicate-claim safety. Default 60_000, min 2_000. */
  leaseMs?: number;
  pull(context: {
    input: Input;
    cursor: Cursor | null;
    limit: number;
    signal: AbortSignal;
  }): Promise<{ items: Item[]; nextCursor: Cursor | null }>;
  dispatch(context: { input: Input; item: Item; signal: AbortSignal }): Promise<void>;
};

export type PumpStatus = "queued" | "running" | "waiting" | "completed" | "failed" | "canceled";

export type PumpState<Input, Cursor> = {
  key: string;
  input: Input;
  cursor: Cursor | null;
  status: PumpStatus;
  dispatched: number;
  failureCount: number;
  lastError?: string;
  meta?: MessageMeta;
  createdAt: Date;
  updatedAt: Date;
};

export type Pump<Input, Cursor> = {
  ready(): Promise<void>;
  start(input: { key: string; input: Input; meta?: MessageMeta }): Promise<void>;
  process(options?: ProcessOptions): Promise<Worker>;
  get(input: { key: string }): Promise<PumpState<Input, Cursor> | null>;
  cancel(input: { key: string }): Promise<boolean>;
  /** Re-enqueue wake-ups for every non-terminal run. Runs automatically on process(). */
  reconcile(): Promise<{ requeued: number }>;
};

/** The KV record is the truth for a run; wake-up messages are repairable hints. */
type PumpRecord<Input, Cursor, Item extends PumpItem> = {
  key: string;
  input: Input;
  cursor: Cursor | null;
  status: PumpStatus;
  dispatched: number;
  failureCount: number;
  lastError?: string;
  meta?: MessageMeta;
  createdAt: string;
  updatedAt: string;
  lease?: { token: string; until: string };
  page?: { items: Item[]; done: string[]; nextCursor: Cursor | null };
};

const TERMINAL: readonly PumpStatus[] = ["completed", "failed", "canceled"];
/** Bytes a persisted run record may add on top of its page (input, cursor, lease, counters). */
const RECORD_HEADROOM_BYTES = 8_192;

// ==========================
// Pump factory
// ==========================

export const createPump = <Input, Cursor, Item extends PumpItem>(
  runtime: SyncRuntime,
  config: PumpConfig<Input, Cursor, Item>,
): Pump<Input, Cursor> => {
  const identity = resourceIdentity(runtime.namespace, "pump", config.id);
  const owner = config.owner ?? runtime.application;
  const batchSize = config.batchSize ?? 100;
  const dispatchConcurrency = config.dispatchConcurrency ?? 1;
  const maxActiveRuns = config.maxActiveRuns ?? 100;
  const maxPageBytes = config.maxPageBytes ?? DEFAULT_MESSAGE_PAYLOAD_BYTES;
  const terminalMs = config.retention?.terminalMs ?? 7 * 24 * 60 * 60 * 1_000;
  const maxAttempts = config.retry?.maxAttempts ?? 5;
  const backoffMs = config.retry?.backoffMs ?? [1_000, 5_000, 30_000];
  if (!Array.isArray(backoffMs) || backoffMs.length === 0 || backoffMs.some((ms) => !Number.isSafeInteger(ms) || ms <= 0)) {
    throw new RangeError("retry.backoffMs must be a non-empty array of positive integers");
  }
  const leaseMs = config.leaseMs ?? 60_000;
  if (!Number.isSafeInteger(leaseMs) || leaseMs < 2_000) {
    throw new RangeError("leaseMs must be an integer of at least 2000");
  }
  for (const [label, value] of [
    ["batchSize", batchSize],
    ["dispatchConcurrency", dispatchConcurrency],
    ["maxActiveRuns", maxActiveRuns],
    ["retry.maxAttempts", maxAttempts],
  ] as const) {
    if (!Number.isSafeInteger(value) || value < 1) throw new RangeError(`${label} must be a positive integer`);
  }

  const replicas = config.replicas ?? runtime.defaults.replicas;
  const storage = toStorageType(runtime.defaults.storage);
  const bucket = kvBucketName(identity);
  const wakeStream = `S6_PQ_${identity.hash}`;
  const wakeDurable = `S6_PQC_${identity.hash}`;
  const root = subjectRoot(identity);
  const wakeFilter = `${root}.wake.>`;
  const wakeSubject = (key: string): string => `${root}.wake.${subjectToken(key, "pump key")}`;
  const label = `pump ${config.id}`;

  let kv: KV | null = null;

  const declaration = runtime.declare({
    identity,
    owner,
    configKey: JSON.stringify([
      "pump",
      config.id,
      owner,
      batchSize,
      dispatchConcurrency,
      maxActiveRuns,
      maxPageBytes,
      terminalMs,
      maxAttempts,
      backoffMs,
      leaseMs,
    ]),
    natsNames: [`KV_${bucket}`, wakeStream],
    maxMessageBytes: maxPageBytes + RECORD_HEADROOM_BYTES,
    provision: async (ctx: ProvisionContext) => {
      kv = await ensureKv(ctx, identity, owner, bucket, {
        history: 1,
        replicas,
        storage,
        markerTTL: 1_000,
      });
      await ensureStream(ctx, identity, owner, {
        name: wakeStream,
        subjects: [wakeFilter],
        retention: RetentionPolicy.Workqueue,
        discard: DiscardPolicy.Old,
        storage,
        num_replicas: replicas,
        // Wake-ups live until consumed; the KV record is the actual state.
        max_age: 0,
        max_bytes: 64 * 1024 * 1024,
        max_msgs: -1,
        max_msg_size: -1,
      });
    },
    summary: async (ctx: ProvisionContext) => {
      const status = await (kv ?? (await ctx.kvm.open(bucket))).status();
      const wake = await ctx.jsm.streams.info(wakeStream);
      return {
        runs: status.values,
        pendingWakeups: wake.state.messages,
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

  const runKey = (key: string): string => {
    assertName(key, "pump key");
    return `run.${subjectToken(key, "pump key")}`;
  };

  type Loaded = { record: PumpRecord<Input, Cursor, Item>; revision: number } | null;

  const load = async (key: string): Promise<Loaded> => {
    const state = await readRunKey(key);
    return state.record === null ? null : { record: state.record, revision: state.lastRevision };
  };

  /**
   * Raw KV state of a run key. A DEL/PURGE marker (e.g. the terminalMs expiry
   * marker) still owns the subject's last revision — CAS creates must expect
   * it, not 0, or every start() during the marker window silently loses.
   */
  const readRunKey = async (
    key: string,
  ): Promise<{ record: PumpRecord<Input, Cursor, Item> | null; lastRevision: number }> => {
    const store = await getKv();
    const entry = await store.get(runKey(key));
    if (entry === null) return { record: null, lastRevision: 0 };
    if (entry.operation !== "PUT") return { record: null, lastRevision: entry.revision };
    try {
      return { record: decodeJson<PumpRecord<Input, Cursor, Item>>(entry.value), lastRevision: entry.revision };
    } catch {
      return { record: null, lastRevision: entry.revision };
    }
  };

  /**
   * CAS write; returns the new revision or null when the record moved
   * underneath us. Terminal records carry a per-key TTL so run state expires
   * after retention.terminalMs instead of growing the bucket forever. The
   * encode limit gets headroom so lease/status/lastError bookkeeping can
   * never be the write that pushes a page-sized record over the edge.
   */
  const casWrite = async (
    key: string,
    record: PumpRecord<Input, Cursor, Item>,
    previousRevision: number,
  ): Promise<number | null> => {
    await getKv();
    record.updatedAt = new Date().toISOString();
    const bytes = encodeJson(label, record, maxPageBytes + RECORD_HEADROOM_BYTES);
    const ctx = await runtime.context();
    const ttl = TERMINAL.includes(record.status) ? `${Math.max(1, Math.ceil(terminalMs / 1_000))}s` : undefined;
    return kvCasPut(ctx, bucket, runKey(key), bytes, previousRevision, ttl !== undefined ? { ttl } : {});
  };

  /**
   * Enqueue a wake-up unless one is already pending. Work-queue retention
   * makes presence meaningful: a consumed wake-up is gone, a pending or
   * in-flight one is still in the stream — so repair publishes (start()
   * retries, reconcile) are deduplicated by presence, not by a time window.
   */
  const publishWake = async (key: string): Promise<void> => {
    const ctx = await runtime.context();
    const subject = wakeSubject(key);
    const pending = await ctx.jsm.streams.getMessage(wakeStream, { last_by_subj: subject }).catch(() => null);
    if (pending !== null) return;
    await ctx.js.publish(subject, encodeJson(label, { key }, 4_096));
  };

  const toState = (record: PumpRecord<Input, Cursor, Item>): PumpState<Input, Cursor> => ({
    key: record.key,
    input: record.input,
    cursor: record.cursor,
    status: record.status,
    dispatched: record.dispatched,
    failureCount: record.failureCount,
    ...(record.lastError !== undefined ? { lastError: record.lastError } : {}),
    ...(record.meta !== undefined ? { meta: record.meta } : {}),
    createdAt: new Date(record.createdAt),
    updatedAt: new Date(record.updatedAt),
  });

  const emitSettled = (record: PumpRecord<Input, Cursor, Item>): void => {
    runtime.events.emit({
      type: "pump_run_settled",
      resource: config.id,
      kind: "pump",
      detail: {
        key: record.key,
        status: record.status,
        dispatched: record.dispatched,
        failureCount: record.failureCount,
        durationMs: Math.max(0, Date.now() - Date.parse(record.createdAt)),
        ...(record.lastError !== undefined ? { error: record.lastError } : {}),
      },
    });
  };

  // ==========================
  // Public operations
  // ==========================

  const start: Pump<Input, Cursor>["start"] = async (input) => {
    runtime.assertActive();
    await declaration.ready();
    const existing = await load(input.key);
    if (existing !== null && !TERMINAL.includes(existing.record.status)) {
      // Idempotent — but re-enqueue the wake-up: if the original one was lost
      // (crash between record write and publish), retrying start() repairs it.
      await publishWake(input.key);
      return;
    }
    const now = new Date().toISOString();
    const record: PumpRecord<Input, Cursor, Item> = {
      key: input.key,
      input: input.input,
      cursor: null,
      status: "queued",
      dispatched: 0,
      failureCount: 0,
      ...(input.meta !== undefined ? { meta: input.meta } : {}),
      createdAt: now,
      updatedAt: now,
    };
    // Retry the create: the CAS can lose against a concurrent start() OR
    // against the terminalMs expiry marker still holding the last revision.
    for (let attempt = 0; attempt < 5; attempt++) {
      const state = attempt === 0 ? null : await readRunKey(input.key);
      if (state !== null && state.record !== null && !TERMINAL.includes(state.record.status)) {
        // A concurrent start won; make sure its wake-up exists and stop.
        await publishWake(input.key);
        return;
      }
      const previous = state === null ? (existing !== null ? existing.revision : (await readRunKey(input.key)).lastRevision) : state.lastRevision;
      const revision = await casWrite(input.key, record, previous);
      if (revision !== null) {
        runtime.events.emit({ type: "pump_run_started", resource: config.id, kind: "pump", detail: { key: input.key } });
        await publishWake(input.key);
        return;
      }
    }
    throw new Error(`${label}: start(${input.key}) is contended`);
  };

  const get: Pump<Input, Cursor>["get"] = async (input) => {
    runtime.assertActive();
    const loaded = await load(input.key);
    return loaded === null ? null : toState(loaded.record);
  };

  const cancel: Pump<Input, Cursor>["cancel"] = async (input) => {
    runtime.assertActive();
    for (let attempt = 0; attempt < 5; attempt++) {
      const loaded = await load(input.key);
      if (loaded === null || TERMINAL.includes(loaded.record.status)) return false;
      const record = { ...loaded.record, status: "canceled" as const, lease: undefined };
      if ((await casWrite(input.key, record, loaded.revision)) !== null) {
        emitSettled(record);
        return true;
      }
    }
    return false;
  };

  const reconcile: Pump<Input, Cursor>["reconcile"] = async () => {
    runtime.assertActive();
    const store = await getKv();
    let requeued = 0;
    const keys = await store.keys("run.>");
    for await (const rawKey of keys) {
      try {
        const key = decodeSubjectToken(rawKey.slice("run.".length));
        const loaded = await load(key);
        if (loaded === null || TERMINAL.includes(loaded.record.status)) continue;
        await publishWake(key);
        requeued += 1;
      } catch {
        // A foreign/tampered key must not block reconciliation of the rest.
      }
    }
    if (requeued > 0) {
      runtime.events.emit({ type: "pump_recovered", resource: config.id, kind: "pump", detail: { requeued } });
    }
    return { requeued };
  };

  // ==========================
  // Run execution
  // ==========================

  /** Executes one run to a terminal, waiting, lease-blocked, or lease-lost outcome. */
  const executeRun = async (key: string, msg: JsMsg, signal: AbortSignal): Promise<void> => {
    const leaseToken = crypto.randomUUID();
    let loaded = await load(key);

    // Claim or bail out.
    while (true) {
      if (loaded === null || TERMINAL.includes(loaded.record.status)) {
        msg.ack();
        return;
      }
      const lease = loaded.record.lease;
      if (lease !== undefined && new Date(lease.until).getTime() > Date.now() && lease.token !== leaseToken) {
        // Another process owns this run; check again after the lease window.
        msg.nak(Math.max(1_000, new Date(lease.until).getTime() - Date.now()));
        return;
      }
      const claimed: PumpRecord<Input, Cursor, Item> = {
        ...loaded.record,
        status: "running",
        lease: { token: leaseToken, until: new Date(Date.now() + leaseMs).toISOString() },
      };
      let revision: number | null;
      try {
        revision = await casWrite(key, claimed, loaded.revision);
      } catch (error) {
        // A record that cannot even hold its lease bookkeeping is faulty
        // data, not transient contention — surface it and retry later.
        runtime.events.emit({ type: "handler_error", resource: config.id, kind: "pump", error: asError(error).message });
        msg.nak(5_000);
        return;
      }
      if (revision !== null) {
        loaded = { record: claimed, revision };
        break;
      }
      loaded = await load(key);
    }

    let { record, revision } = loaded;
    /** Set once this worker no longer owns the run (canceled or lease taken over). */
    let lost = false;
    /** Serialized so concurrent dispatch checkpoints never race their own CAS. */
    let checkpointChain: Promise<boolean> = Promise.resolve(true);

    /**
     * CAS with lease refresh. Returns false and marks the run as lost when the
     * record was changed by someone else (cancel, takeover): after that this
     * worker must stop dispatching and must not write again.
     */
    const checkpoint = (mutate: (r: PumpRecord<Input, Cursor, Item>) => void): Promise<boolean> => {
      const step = checkpointChain.then(async () => {
        if (lost) return false;
        const next = { ...record, lease: { token: leaseToken, until: new Date(Date.now() + leaseMs).toISOString() } };
        mutate(next);
        const newRevision = await casWrite(key, next, revision);
        if (newRevision === null) {
          const reloaded = await load(key);
          if (reloaded !== null) ({ record, revision } = reloaded);
          if (
            reloaded === null ||
            TERMINAL.includes(reloaded.record.status) ||
            reloaded.record.lease?.token !== leaseToken
          ) {
            lost = true;
          }
          return false;
        }
        record = next;
        revision = newRevision;
        msg.working();
        return true;
      });
      checkpointChain = step.catch(() => false);
      return step;
    };

    const fail = async (error: Error, options: { dropPage?: boolean } = {}): Promise<void> => {
      const failureCount = record.failureCount + 1;
      const terminal = failureCount >= maxAttempts;
      const writeFailure = (dropPage: boolean): Promise<boolean> =>
        checkpoint((r) => {
          r.status = terminal ? "failed" : "waiting";
          r.failureCount = failureCount;
          r.lastError = error.message.slice(0, 2_048);
          r.lease = undefined;
          // Only when the page itself cannot be persisted (oversized) is it
          // dropped; ordinary failures keep the per-item done[] checkpoints.
          if (dropPage) delete r.page;
        });
      try {
        await writeFailure(options.dropPage ?? false);
      } catch (writeError) {
        if (!(writeError instanceof PayloadTooLargeError) || options.dropPage) throw writeError;
        // The failure bookkeeping itself did not fit next to the page: the
        // page has to go, or the run wedges outside the retry policy.
        await writeFailure(true);
      }
      if (lost) {
        msg.nak();
        return;
      }
      runtime.events.emit({ type: "handler_error", resource: config.id, kind: "pump", error: error.message });
      if (terminal) {
        emitSettled(record);
        msg.ack();
      } else {
        msg.nak(backoffMs[Math.min(failureCount - 1, backoffMs.length - 1)]!);
      }
    };

    /** Dispatch remaining page items with bounded concurrency; returns the first error or null. */
    const dispatchPage = async (
      page: NonNullable<PumpRecord<Input, Cursor, Item>["page"]>,
    ): Promise<Error | null> => {
      const doneKeys = new Set(page.done);
      const remaining = page.items.filter((item) => !doneKeys.has(item.key));
      // Element writes are not flow-narrowed, so no cast is needed to read
      // the closure-written result after the workers settle.
      const errors: Error[] = [];
      let index = 0;
      const workers = Array.from({ length: Math.min(dispatchConcurrency, remaining.length) }, async () => {
        while (errors.length === 0 && !signal.aborted && !lost) {
          const item = remaining[index++];
          if (item === undefined) return;
          try {
            await config.dispatch({ input: record.input, item, signal });
          } catch (error) {
            errors.push(asError(error));
            return;
          }
          try {
            await checkpoint((r) => {
              r.page?.done.push(item.key);
            });
          } catch (error) {
            if (!(error instanceof PayloadTooLargeError)) throw error;
            errors.push(error);
            return;
          }
        }
      });
      await Promise.all(workers);
      return errors[0] ?? null;
    };

    while (true) {
      if (lost) {
        // Canceled or taken over: the current owner's state stands; this
        // delivery is stale and simply returns to the wake queue.
        msg.nak();
        return;
      }
      if (signal.aborted || TERMINAL.includes(record.status)) {
        if (TERMINAL.includes(record.status)) msg.ack();
        else {
          await checkpoint((r) => {
            r.lease = undefined;
          });
          msg.nak();
        }
        return;
      }

      // 1. Ensure there is a current page.
      if (record.page === undefined) {
        let pulled: { items: Item[]; nextCursor: Cursor | null };
        try {
          pulled = await config.pull({ input: record.input, cursor: record.cursor, limit: batchSize, signal });
        } catch (error) {
          await fail(asError(error));
          return;
        }
        const keys = new Set(pulled.items.map((item) => item.key));
        if (keys.size !== pulled.items.length) {
          await fail(new Error("pull() returned items with duplicate keys"));
          return;
        }
        if (pulled.items.length === 0 && pulled.nextCursor === null) {
          await checkpoint((r) => {
            r.status = "completed";
            r.lease = undefined;
          });
          if (lost) {
            msg.nak();
            return;
          }
          emitSettled(record);
          msg.ack();
          return;
        }
        let wrote: boolean;
        try {
          wrote = await checkpoint((r) => {
            r.page = { items: pulled.items, done: [], nextCursor: pulled.nextCursor };
          });
        } catch (error) {
          if (error instanceof PayloadTooLargeError) {
            // An unpersistable page must go through the retry/failure policy,
            // never into an infinite redelivery loop.
            await fail(error, { dropPage: true });
            return;
          }
          throw error;
        }
        if (!wrote) continue; // externally changed — re-evaluate at loop head
      }

      // 2. Dispatch remaining page items with bounded concurrency, checkpointing
      //    each completed item key individually. A crash repeats only items
      //    whose sink succeeded after their last confirmed checkpoint.
      const page = record.page!;
      const doneKeys = new Set(page.done);
      const remaining = page.items.filter((item) => !doneKeys.has(item.key));
      let dispatchError: Error | null = null;
      let index = 0;
      const workers = Array.from({ length: Math.min(dispatchConcurrency, remaining.length) }, async () => {
        while (dispatchError === null && !signal.aborted && !lost) {
          const item = remaining[index++];
          if (item === undefined) return;
          try {
            await config.dispatch({ input: record.input, item, signal });
          } catch (error) {
            dispatchError = asError(error);
            return;
          }
          try {
            await checkpoint((r) => {
              r.page?.done.push(item.key);
            });
          } catch (error) {
            if (error instanceof PayloadTooLargeError) {
              dispatchError = error;
              return;
            }
            throw error;
          }
        }
      });
      await Promise.all(workers);
      // TS cannot see the closure writes above and narrows to null.
      const failedDispatch = dispatchError as Error | null;
      if (failedDispatch !== null) {
        await fail(failedDispatch, { dropPage: failedDispatch instanceof PayloadTooLargeError });
        return;
      }
      if (lost || signal.aborted || TERMINAL.includes(record.status)) continue; // handled at loop head

      // 3. Page complete: advance the committed cursor, clear the page.
      const finished = record.page;
      if (finished === undefined) continue; // reloaded without a page — re-evaluate
      const advanced = await checkpoint((r) => {
        r.cursor = finished.nextCursor;
        r.dispatched += finished.items.length;
        r.failureCount = 0;
        delete r.lastError;
        delete r.page;
        if (finished.nextCursor === null) {
          r.status = "completed";
          r.lease = undefined;
        }
      });
      if (!advanced) continue;
      if (record.status === "completed") {
        emitSettled(record);
        msg.ack();
        return;
      }
    }
  };

  const process: Pump<Input, Cursor>["process"] = async (options = {}) => {
    runtime.assertActive();
    await declaration.ready();
    const ctx = await runtime.context();
    await ensureConsumer(ctx, wakeStream, {
      durable_name: wakeDurable,
      ack_policy: AckPolicy.Explicit,
      filter_subject: wakeFilter,
      ack_wait: nanos(leaseMs),
      max_ack_pending: maxActiveRuns,
      deliver_policy: DeliverPolicy.All,
    });
    await reconcile();

    const wr = createWorkerRuntime(options, { onFinished: () => runtime.unregisterWorker(wr) });
    runtime.registerWorker(wr);
    runtime.events.emit({ type: "worker_started", resource: config.id, kind: "pump" });

    const onMessage = async (msg: JsMsg, signal: AbortSignal): Promise<void> => {
      let wake: { key: string };
      try {
        wake = decodeJson<{ key: string }>(msg.data);
        if (typeof wake.key !== "string") throw new Error("invalid wake-up");
      } catch {
        msg.term();
        return;
      }
      try {
        await executeRun(wake.key, msg, signal);
      } catch (error) {
        runtime.events.emit({ type: "handler_error", resource: config.id, kind: "pump", error: asError(error).message });
        msg.nak(5_000);
      }
    };

    runPullLoop(wr, () => ctx.js.consumers.get(wakeStream, wakeDurable), onMessage, {
      events: runtime.events,
      resource: config.id,
    }).finally(() => {
      runtime.events.emit({ type: "worker_stopped", resource: config.id, kind: "pump" });
    });
    return wr.worker;
  };

  return {
    ready: () => declaration.ready(),
    start,
    process,
    get,
    cancel,
    reconcile,
  };
};
