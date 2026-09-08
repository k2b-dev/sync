# @k2b/sync v6 — API reference

All primitives are created from a `Sync` instance and share these conventions:

- Factories perform no I/O; `await handle.ready()` (or the first operation) waits for provisioning. Provisioning refuses (`UnsupportedServerError`) any `maxPayloadBytes`/`maxValueBytes`/`maxPageBytes` the connected server's `max_payload` cannot carry (headers count too — keep a small margin); size the server for the largest publish.
- `tenantId?` defaults to `"default"`. Ids, tenant ids, keys, and consumer names are non-empty UTF-8 strings ≤ 96 bytes.
- `meta?: Record<string, JsonValue>` travels with messages/events.
- `Worker` handles: `{ active, capacity, stop(), drain({ timeoutMs? }), [Symbol.asyncDispose] }`.

```ts
type SyncConfig = {
  connection: NatsConnection;         // already connected, caller-owned
  namespace: string;                  // deployment isolation
  application: string;                // ownership/diagnostics metadata
  defaults?: { replicas?: number; storage?: "file" | "memory" }; // default 3 / "file"
  observe?: (event: SyncEvent) => void | Promise<void>;
};

type Sync = {
  ready(): Promise<void>;
  drain(options?: { timeoutMs?: number }): Promise<{ completed: number; aborted: number; timedOut: boolean }>;
  health(): SyncHealth;               // synchronous local state
  resources(): Promise<SyncResourceSummary[]>;
  controls(): readonly SyncControl[]; // process-local control inventory, no I/O
  events(options?: { signal?: AbortSignal }): AsyncIterable<SyncEvent>;
  queue<T>(config: QueueConfig): Queue<T>;
  topic<T>(config: TopicConfig): Topic<T>;
  job<Input>(config: JobConfig): Job<Input>;
  pump<Input, Cursor, Item extends { key: string }>(config: PumpConfig<Input, Cursor, Item>): Pump<Input, Cursor>;
  scheduler(config: SchedulerConfig): Scheduler;
  mutex(config: MutexConfig): Mutex;
  ephemeral<T>(config: EphemeralConfig): Ephemeral<T>;
  objectStore(config: ObjectStoreConfig): ObjectStore;
};
```

Shared delivery types:

```ts
type DeliveryConfig = {
  ackWaitMs?: number;    // default 30_000 — redelivery timer per delivery
  maxAttempts?: number;  // default 5, including the first attempt
  maxInFlight?: number;  // default 1_000 — GLOBAL MaxAckPending per durable consumer
  backoffMs?: number[];  // default [1_000, 5_000, 30_000, 120_000]; last entry repeats
};
type RetentionConfig = { maxAgeMs: number; maxBytes: number; maxMessages?: number };
// Queue/topic retention is a hard loss boundary (discard-old; NATS forbids
// reject-new on streams with message schedules) — size generously.
type OrderingConfig = { mode: "none" } | { mode: "partitioned"; partitions: number };
type PublishReceipt = { messageId: string; streamSequence: number; duplicate: boolean };
```

## queue

```ts
const q = sync.queue<T>({
  id, owner?, delivery?, retention?,        // retention default 7d / 1 GiB
  dedupeWindowMs?,                          // default 120_000
  ordering?,                                // partitioned ⇒ send() requires orderingKey;
                                            // retries run in place (order survives failures;
                                            // maxAttempts bounds attempts per lease, not across crashes)
  maxPayloadBytes?,                         // default 128 KiB (whole envelope)
  replicas?,
});

await q.send({ data, tenantId?, idempotencyKey?, orderingKey?, delayMs?, at?, meta? }); // → PublishReceipt
// delayMs/at (mutually exclusive) use one-shot broker schedules — no consumer slot used.

const worker = await q.process({ concurrency?, signal? }, async (message) => {
  // message: { data, messageId, attempt, publishedAt, tenantId, orderingKey?, meta?, signal, heartbeat() }
  // resolve → confirmed ack; throw → nak(backoff) until maxAttempts → DLQ
});

const reader = await q.reader();            // unpartitioned queues only
const delivery = await reader.receive({ waitMs? }); // null on timeout; min wait 1s
await delivery.ack();                        // confirmed; StaleDeliveryError on hard failure
await delivery.retry({ delayMs?, reason? }); // on final attempt: dead-letters ("max attempts exhausted", error=reason)
await delivery.deadLetter({ reason, error? });
for await (const d of reader.stream()) { ... }
await reader.close();

await q.deadLetters.list({ limit?, after? });                    // DeadLetter<T>[]
await q.deadLetters.requeue({ messageId, idempotencyKey });      // → PublishReceipt, removes DLQ entry
await q.deadLetters.delete({ messageId });                       // boolean
```

## job

```ts
const j = sync.job<Input>({ ...QueueConfig, terminalRetentionMs? /* DLQ retention, default 7d */ });

await j.submit({ key, input, tenantId?, delayMs?, at?, orderingKey?, meta?, coalesce? });
// → PublishReceipt & { jobId }; key = NATS msgID within dedupe window, per tenant.
// coalesce: true = at most one queued-or-running job per key, released when the
// job settles (success or dead letter) — duplicate submits return the original
// jobId; the key is immediately reusable after completion (no time window).
// The first accepted input wins. Retrying an uncertain publication repairs
// that input; duplicate calls do not replace it. Long runs must heartbeat.

await j.submitMany(iterableOrAsyncIterable, {
  publishConcurrency?,   // default 16 in-flight publish promises
  maxPendingBytes?,      // default 8 MiB in-flight encoded bytes (local backpressure)
  signal?,
}); // → { accepted, duplicates }; throws BatchSubmitError (accepted stay accepted)
// Coalescing has the same behavior as submit(), including within one batch.

await j.process({
  concurrency?, signal?,
  onError?: async ({ context, error }) =>
    ({ action: "retry", delayMs? } | { action: "dead_letter", reason }),
}, async (context) => {
  // context: { jobId, key, input, attempt, failureCount, signal, heartbeat(),
  //   resubmit({ delayMs?, input? }) } — continuation submitted after success,
  //   BEFORE the ack (never lost; fresh msgID, coalesce claim carries over)
});

j.deadLetters // DeadLetterStore<{ key, input }>
```

Coalesced continuations preserve `orderingKey` and `meta` and reserve the successor before publication. A parent redelivery can finish that handoff without rerunning the completed handler. Delivery remains at-least-once. Requeueing a dead letter joins an active coalesced job for the same tenant and key, or creates a fresh generation if the key is free.

When upgrading a 6.2.0 deployment to the corrected coalescing implementation, stop all old producers and workers first; mixed old/new writers are unsupported. Queued legacy messages can adopt their claims. An accepted coalesced message whose claim record is missing (lost/expired) adopts a fresh claim and runs — at-least-once, surfaced as a `redelivery` event with `detail.orphanClaimAdopted` — instead of being acknowledged silently. An orphan legacy pending claim has no stored input and needs reconciliation against application state; submission throws `SyncUsageError` without deleting it. See the repository README's Job section.

## topic

```ts
const t = sync.topic<T>({ id, owner?, retention /* required */, dedupeWindowMs?, maxPayloadBytes?, replicas? });

const r = await t.publish({ data, tenantId?, idempotencyKey?, orderingKey?, meta? });
// → PublishReceipt & { eventId, cursor }

await t.latestCursor({ tenantId? });        // TopicCursor | null, per tenant
await t.head();                             // newest event of ANY tenant (one lookup); cursorAt(0) when empty

for await (const e of t.live({ tenantId?, signal? })) {}    // broadcast, no cursor/replay
for await (const e of t.replay({ tenantId?, after?, until?, signal? })) {} // to head-at-start
for await (const e of t.follow({ tenantId?, after?, signal? })) {}         // stays open
// events: { data, eventId, cursor, sequence, tenantId, orderingKey?, publishedAt, meta? }
// sequence = the stream sequence behind the cursor: monotonic per topic, safe to persist
// as a number and compare; live() events carry neither cursor nor sequence.
// after below retention → RetentionGapError (also for a fresh process({start:{after}}) consumer);
// error.resumeAfter is the cursor that resumes from the first retained event.
// foreign cursor → CursorMismatchError; mid-follow retention loss → RetentionGapError.
// tenantId is a client-side filter: replay/follow stream the WHOLE topic from
// the server — prefer one topic per tenant or process() for high volume.

const h = t.hub({ tenantId? });  // ONE shared follow() for many local subscribers;
                                 // memoized per tenant per topic handle; retired automatically
                                 // when the last subscriber leaves (or on close())
for await (const e of h.subscribe({ after?, bufferLimit? /* default 1024 */, signal? })) {}
// catch-up replay splices into the live tail (seq-deduped); slow subscribers end
// with RetentionGapError (resumeAfter = last delivered cursor) — resubscribe.
// No explicit "caught up" signal: for a deterministic boundary run
// replay({ after, until: latestCursor() }) first, then subscribe({ after: until }).
h.close();

t.cursorSequence(cursor);   // number — CursorMismatchError for foreign/invalid cursors
t.cursorAt(sequence);       // TopicCursor for a persisted sequence (never fabricate cursor strings);
                            // cursorAt(0) = "before the first event" (after is exclusive) — a BIGINT
                            // column defaulting to 0 replays from the start

await t.process({
  consumer,                       // same name competes, different names = independent cursors
  tenantId?, concurrency?, signal?,
  recoverDeadLetters?: true,       // opt into direct operator recovery through this same handler
  delivery?,                      // consumer config — drift-checked across pods
  start?: "earliest" | "latest" | { after: TopicCursor },
}, async (event /* TopicEvent & { attempt, signal } */) => { ... });
// failure: backoff retries → per-consumer DLQ stream
```

Topic controls appear in `sync.controls()` when `process()` first starts and
remain after the worker stops; append/watch-only topics create no admin control.
Every topic exposes `deadLetters.list({ limit?, after? })`, `get({ messageId })`,
and `delete({ messageId })`. `messageId` is the DLQ stream sequence; `eventId`
is the original event identity. A page contains at most 1,000 entries (default
100); `after` remains valid after that entry is deleted. Entries identify the
consumer and tenant, contain the failed payload, and report `replayAvailable`.

With `process({ recoverDeadLetters: true, consumer, ... }, handler)`, recovery
is available through `deadLetters.replay({ messageId, consumer, tenantId,
timeoutMs?, signal? })`. It invokes that existing handler directly, using the
original event identity, sequence, timestamp, and metadata, with `attempt: 1`.
It never publishes into the topic, changes its head, or reruns another
consumer. The result is `{ messageId, eventId, consumer, completed: true }`,
not a publish receipt. Success removes the DLQ entry; a repeated request for
that removed entry reports not found.

Handlers must make effects idempotent by original event identity. A crash
between the effect and DLQ deletion can repeat the effect. Recovery takes an
existing worker slot and a per-entry distributed mutex lease. Opting in
provisions one additional Sync mutex KV resource per topic. Requests default
to 30 seconds and accept at most 120 seconds. Failure or abort retains the
entry. A timed-out handler that ignores abort keeps its lease renewed until
it settles or the runtime stops; an operator request cannot forcibly stop application code.
Broker failure or process death can lose the lease, so it does not replace
idempotent effects. Recovery is unavailable once the local worker stops.

Historical topic DLQ entries without the original sequence and timestamp
remain inspectable and deletable, but cannot be replayed safely. No cursor
is fabricated and no historical message is silently republished.


## pump

```ts
const p = sync.pump<Input, Cursor, Item extends { key: string }>({
  id, owner?,
  batchSize?,             // default 100 (pull limit)
  dispatchConcurrency?,   // default 1, per active run
  maxActiveRuns?,         // default 100 — global leased runs (consumer MaxAckPending)
  retention?: { terminalMs? },    // terminal run state expires after this (default 7d)
  maxPageBytes?,                  // persisted run record incl. page, default 128 KiB
  replicas?,
  retry?: { maxAttempts?, backoffMs? },        // default 5 / [1s, 5s, 30s]
  leaseMs?,               // default 60_000, min 2_000 — crash takeover latency
  pull:     async ({ input, cursor, limit, signal }) => ({ items, nextCursor }), // nextCursor null = done
  dispatch: async ({ input, item, signal }) => {},  // idempotent by item.key
});

await p.start({ key, input, meta? });   // idempotent while active; restarts terminal runs
await p.process({ concurrency?, signal? }); // reconciles lost wake-ups on start
await p.get({ key });                   // PumpState | null: status queued|running|waiting|completed|failed|canceled
await p.cancel({ key });                // boolean, stops at the next checkpoint
await p.reconcile();                    // { requeued } — re-enqueue wake-ups from KV truth
```

Cursor advances only after every page item is checkpointed; a crash repeats only items finished after their last confirmed checkpoint.

Observe events: `pump_run_started { key }` once per created/restarted run, `pump_run_settled { key, status: completed|failed|canceled, dispatched, failureCount, durationMs, error? }` exactly once per run at its terminal transition (emitted by whichever process performed it, including `cancel()`). `durationMs` is measured from `start()` and includes queued/waiting time, unlike `handler_settled.durationMs` (handler time only).

## scheduler

```ts
const s = sync.scheduler({
  id, owner?, replicas?,
  delivery?,   // maxInFlight is ignored: schedules execute serially (one in-flight tick each)
  retention?: { maxAgeMs?, maxTicksPerSchedule? },  // default 7d / 10_000 per schedule
});

await s.create({
  id, cron /* five-field, minute resolution */, timezone? /* IANA, default UTC */,
  misfire?: "latest" | "all",   // default "latest": only newest retained slot after downtime
  meta?,
  process: async (context) => {},
  // context: { scheduleId, runId, runNumber, slot, trigger: "schedule"|"manual", attempt, signal, heartbeat() }
}); // → { created, updated }; republish is self-healing and idempotent

await s.process({ concurrency?, signal? }); // serves locally created schedules; serial per schedule
await s.runNow({ id, requestId }); // → { runId }; durably accepted; requestId dedupes within the 120s window
await s.awaitRun({ id, runId, timeoutMs? }); // { completed, error? } — wait for terminal settlement
await s.get({ id });               // ScheduleInfo | null: cron, timezone, misfire, nextRunAt, runNumber,
                                   // failureCount, lastError?, lastRunId?, lastCompletedAt?, createdAt,
                                   // updatedAt, handlerAvailable (process-local)
await s.list();
await s.delete({ id });            // cancels the broker schedule and drops its retained ticks
```

The broker produces ticks while every app process is offline; a later `start()` executes them per misfire policy. Handler failure retries per `delivery`, then increments `failureCount` and drops the slot.

## mutex

```ts
const m = sync.mutex({ id, owner?, ttlMs? /* default 10_000, rounds up to seconds */, retry? /* { maxAttempts: 10, delayMs: 200 } — total tries incl. the first */, replicas? });

const lock = await m.acquire({ resource, ttlMs?, signal? }); // Lock | null
// Lock: { resource, ownerToken, fence: bigint /* monotonic, stable across extends */, expiresAt }
await m.extend(lock, { ttlMs? });   // boolean; false = lease lost
await m.release(lock);              // boolean; owner-only
await m.withLock({ resource, ttlMs?, signal? }, async (lock) => value); // value | null; always releases
```

A lease cannot stop an expired owner writing to external systems afterwards — persist and compare `fence`, or make effects idempotent.

## ephemeral

```ts
const e = sync.ephemeral<T>({ id, owner?, ttlMs /* required; seconds resolution */, history?, maxEntries? /* snapshot bound, default 10k */, maxValueBytes? /* default 4 KiB */, replicas? });

await e.upsert({ tenantId?, key, value, ttlMs? });  // → EphemeralEntry { key, value, revision, updatedAt, expiresAt }
// snapshot/watch entries omit expiresAt (custom per-key TTLs are unknowable on read)
await e.touch({ tenantId?, key, ttlMs? });          // boolean; CAS-guarded republish with fresh TTL
await e.delete({ tenantId?, key });                 // boolean
await e.snapshot({ tenantId?, prefix? });           // { entries, revision }; SnapshotOverflowError beyond maxEntries
for await (const ev of e.watch({ tenantId?, prefix?, after?, signal? })) {
  // { type: "upsert", entry } | { type: "delete"|"expire", key } | { type: "resync_required", requested, firstAvailable }
}
// watch without `after` first replays current entries as upserts, then streams changes.
// snapshot/watch entries carry updatedAt only; expiresAt exists on upsert results.
```

## objectStore

```ts
const o = sync.objectStore({
  id, owner?, storage?, replicas?, compression?: "none" | "s2",
  retention: { maxAgeMs, maxBytes },  // required, bucket-wide
  maxObjectBytes,                     // required per-object cap, enforced mid-stream
});

const ref = await o.put({ tenantId?, key, body: ReadableStream, metadata?, signal? });
// → ObjectRef { storeId, tenantId, key, size, digest } — plain JSON, safe in payloads
const obj = await o.get(ref, { signal?, idleTimeoutMs? }); // StoredObject | null (null if deleted OR replaced since);
// idleTimeoutMs (default 30s) errors a stalled body instead of hanging forever.
// metadata keys starting with "sync." are rejected.
await o.info({ tenantId?, key });    // SyncObjectInfo | null
await o.delete({ tenantId?, key });  // boolean
for await (const i of o.list({ tenantId?, prefix? })) {}
for await (const ev of o.watch({ tenantId?, prefix?, signal? })) {} // put | delete — changes only, no initial state
```

Refs don't pin: pick `retention.maxAgeMs > max queue residence + retry window + margin`.

## retry (`@k2b/sync/retry`, browser-safe)

```ts
const value = await retry<T>({
  run: async ({ ctx }) => produce(),          // ctx: { attempt }
  after: async ({ ctx }) => {                  // ctx adds { data?, error?, reschedule(), expBackoff() }
    if (ctx.error && ctx.attempt < 5) ctx.reschedule({ delayMs: ctx.expBackoff() });
  },
  signal?,
});
expBackoff(attempt, { baseMs?, maxMs?, jitter? });
isRetryableTransportError(error);   // network-vocabulary heuristics (no Redis codes in v6)
```

## Diagnostics and controls

`health()` returns local runtime state; `resources()` reads sanitized resource
summaries. `controls()` exposes administrative handles for already declared
queue, job, and scheduler resources, plus topics whose `process()` has started,
without additional provisioning or starting workers.

```ts
type SyncControl = Readonly<{ namespace: string; id: string; owner: string } & (
  | { kind: "queue" | "job"; deadLetters: DeadLetterStore<unknown> }
  | { kind: "topic"; deadLetters: TopicDeadLetterStore<unknown> }
  | { kind: "scheduler"; scheduler: Pick<Scheduler, "list" | "runNow" | "awaitRun"> }
)>;
```

Identity is `{ namespace, kind, id }`: a queue and job with the same ID remain
different controls. Repeated declarations produce one control. Scheduler
inspection combines `handlerAvailable` across actual local handles without
merging their callbacks or workers. Controls remain discoverable after workers
stop and operate on the existing durable state, including DLQ tenant IDs and
scheduler run history. The inventory does not discover resources in other
processes. `owner` is metadata, not authorization; applications must protect
administrative endpoints and audit mutations themselves.

## Events

`observe` / `sync.events()` deliver `SyncEvent { type, at, resource?, kind?, detail?, error? }`. Types: `connection`, `ready`, `resource_verified`, `resource_drifted`, `worker_started`, `worker_stopped`, `handler_started` / `handler_settled` (queue/job/topic/scheduler runs: `{ id, key?, attempt, status: success|retry|dead_letter, durationMs }`), `handler_error`, `redelivery`, `dead_letter`, `lock_lost`, `schedule_tick`, `schedule_misfire`, `pump_recovered`, `pump_run_started` / `pump_run_settled`, `object_error`, `watch_resync_required`, `drain_timeout`. Events carry ids, keys, and statuses — never handler results or payloads. Observers cannot block or alter transport work; slow `events()` readers drop events.

## Errors

`SyncError` base; `SyncLifecycleError`, `UnsupportedServerError`, `InvalidNameError` (name/bounds violations), `SyncUsageError` (API misuse: mutually exclusive options, reader on partitioned queues, foreign ObjectRef, reserved metadata, invalid cron), `NotFoundError` (missing dead letter / schedule), `ConflictingResourceDeclarationError`, `ResourceIdentityCollisionError`, `ResourceDriftError { resource, differences }`, `PayloadTooLargeError { actualBytes, maxBytes }`, `ObjectTooLargeError`, `StaleDeliveryError`, `RetentionGapError { requested, firstAvailable, resumeAfter }`, `CursorMismatchError`, `BatchSubmitError { accepted, duplicates }`, `ConflictError` (lost expectedAfter race), `SnapshotOverflowError { maxEntries }`. Config validation throws plain `RangeError`; empty namespace/application throws `SyncLifecycleError`; non-serializable payloads throw `TypeError`.

## NATS feature map

Queue, job, topic-consumer, and scheduler pause/resume calls confirm the applied consumer state before returning. Already delivered work can finish; concurrent control requests can prevent confirmation and cause a bounded failure.

Used and exposed: atomic batch publish (`sendBatch`/`submitBatch`/`publishBatch` — all-or-nothing, NO dedupe ids), per-subject expected sequence (`topic.publish({ expectedAfter })` → optimistic per-tenant event sourcing, `ConflictError` on lost races; also every internal CAS), consumer pausing (`queue/job.pause/resume`, `topic.pauseConsumer`, `scheduler.pause` — global, delivery-side only), per-message TTL (`queue.send({ ttlMs })` expiring work incl. between retries; internal: KV TTLs, schedule definitions), message schedules (delays, scheduler clock), replicas + storage per resource, durable pull consumer groups (`process({ consumer })` / competing workers).

Deliberate non-goals: stream mirrors/sources (infra topology — would fight the drift model; run them operationally, Sync resources stay the source of truth), distributed message tracing (header-based at the NATS layer; trace with NATS tooling, Sync never blocks pass-through), fast-ingest/async persist (`PersistMode` weakens the "publish = quorum accepted" contract), priority groups/pinning (niche; revisit on demand), CRDTs (not a NATS feature; `ephemeral` revisions + `expectedAfter` are the building blocks).
