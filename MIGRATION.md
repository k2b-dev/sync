# Migrating @k2b/sync v6 → v7

v7 changes the default size of queues and jobs and adds two opt-in controls for them. Two changes can break an existing 6.x deployment: the smaller default byte limit and the stricter `terminalRetentionMs`. Everything else is additive.

## Smaller default stores

JetStream reserves a stream's whole `max_bytes` on every replica as soon as the stream exists. In 6.x, a queue or job declared without `retention` got 1 GiB for its work stream and another 1 GiB for its dead-letter stream, so every such declaration reserved 2 GiB per replica.

In v7, a queue or job without `retention` keeps work for 7 days and up to 256 messages at its payload limit plus dead-letter headroom: `min(1 GiB, 256 × (maxPayloadBytes + 4 KiB))`, which is 34,603,008 bytes (33 MiB) at the 128 KiB default. The dead-letter stream uses the same limits unless `deadLetterRetention` says otherwise.

**What breaks:** streams created by 6.x still have `max_bytes` of 1 GiB. Sync does not reconfigure existing resources, so `ready()` (and the first use of the handle) throws `ResourceDriftError` with a difference on `max_bytes` until the declaration and the streams agree. Streams created by v7 are not affected.

Choose one way per queue or job:

1. **Keep the 6.x limits.** Declare them explicitly; the dead-letter stream inherits them:

   ```ts
   sync.queue({ id: "emails", retention: { maxAgeMs: 7 * 24 * 3_600_000, maxBytes: 1024 ** 3 } });
   ```

2. **Adopt the new default.** Lower `max_bytes` of the work stream and the dead-letter stream to the new default before v7 declares them, for example with `nats stream edit <stream> --max-bytes <bytes>`. Only lower a stream that currently holds less than the new limit; otherwise JetStream discards its oldest messages.

   To do this from the application, run it before `ready()`, as Cloud does in `packages/cloud/src/_internal/sync-budget.ts`: list the streams of the namespace with the JetStream manager, keep those whose metadata has `sync.managed: "true"`, the matching `sync.namespace`, `sync.kind` of `queue` or `job`, and the matching `sync.id` (skip `KV_*` buckets, which hold job coalescing claims), then `jsm.streams.update(name, { max_bytes })` each work and dead-letter stream whose `state.bytes` fits. A stream that still holds more keeps its old limit and its declaration keeps matching it until a later start can lower it. If another process changes the streams between the check and the declaration, `ready()` reports `ResourceDriftError` on `max_bytes`; check again and retry.

Size explicit limits for the backlog you expect rather than for the payload limit: work that carries identifiers of a few hundred bytes fits tens of thousands of messages into the default.

## `terminalRetentionMs` must be positive

A job's `terminalRetentionMs` (the age of its dead letters) must now be a positive integer. 6.x passed `0` to NATS as "no age limit"; v7 throws `RangeError: terminalRetentionMs must be a positive integer`.

Declare a long explicit age instead, such as `365 * 24 * 3_600_000`. The existing dead-letter stream then fails `ready()` with `ResourceDriftError` on `max_age` until it has the same age (for example with `nats stream edit <stream> --max-age <age>`). Jobs that never set `terminalRetentionMs`, or set a positive value, need no change.

## New: `deadLetterRetention` for queues and jobs

Dead-letter streams of queues and jobs can now be sized separately from their work streams, as topics already could. Work that rarely fails can keep a much smaller dead-letter store:

```ts
sync.queue({ id: "emails", deadLetterRetention: { maxAgeMs: 30 * 24 * 3_600_000, maxBytes: 8 * 1024 ** 2 } });
sync.job({ id: "reindex", terminalRetentionMs: 30 * 24 * 3_600_000, deadLetterRetention: { maxBytes: 8 * 1024 ** 2 } });
```

- Each field defaults to the matching `retention` value (or the new default).
- `maxBytes` must hold one dead letter: the payload limit plus 4 KiB.
- Jobs accept only `{ maxBytes }`, because `terminalRetentionMs` owns their dead-letter age.
- Changing it on an existing resource is drift like any other limit: apply the change to the stream first, then roll the processes.

## New: `whenFull: "reject"` and `StoreFullError`

At `retention.maxBytes` or `maxMessages`, the default `whenFull: "discard_oldest"` still accepts every send and silently drops the oldest pending work. Queues and jobs can now opt into backpressure instead:

```ts
const emails = sync.queue({ id: "emails", whenFull: "reject" });

try {
  await emails.send({ data });
} catch (error) {
  if (error instanceof StoreFullError) return backOff(); // accepted work is kept
  throw error;
}
```

`whenFull: "reject"` provisions the work stream with NATS DiscardNew. A full queue or job fails `send`, `sendBatch`, `submit`, and `submitBatch` with `StoreFullError`; `submitMany` throws its usual `BatchSubmitError` with the `StoreFullError` as `cause`. Accepted work and deduplication stay intact. `maxAgeMs` still expires pending work.

Constraints:

- **No schedules or delays.** NATS forbids DiscardNew on streams with message schedules, and a schedule can only target its own stream. These queues and jobs refuse `delayMs`, `at`, and job `resubmit({ delayMs })` with `SyncUsageError`. Retry backoff still works.
- **No switch in place.** An existing stream cannot change modes: drain it, delete it, and declare it again.
- **Batches report only certain refusals.** `sendBatch` throws `StoreFullError` only when the server answered the commit without an ack. A lost ack (for example during a leader change) may have stored the batch, so it keeps its unknown outcome.
- **Coalesced submissions release their claim.** A refused `coalesce: true` submission frees the key, so it does not keep input that was never accepted.
- **Continuations need room too.** `resubmit()` publishes the successor while its parent still occupies the stream. At the limit, a plain continuation fails like a handler error (the handler runs again, then the job is dead-lettered after `maxAttempts`); a coalesced continuation waits for room without rerunning the handler, so a stream that holds only such parents stalls. Keep producers well below the limit of jobs that continue themselves.

## Verification checklist

- [ ] `sync.ready()` succeeds against your real cluster from every app, with no `ResourceDriftError` on `max_bytes` or `max_age`.
- [ ] No job passes `terminalRetentionMs: 0`.
- [ ] Explicit `retention` and `deadLetterRetention` fit the backlog you expect on every replica.
- [ ] Producers of `whenFull: "reject"` queues and jobs handle `StoreFullError` and do not use `delayMs` or `at`.

# Migrating @k2b/sync v5 → v6

v6 replaces the Redis server runtime completely with NATS 2.14+ and JetStream. This is a **hard cut**:

- No Redis compatibility layer, no generic transport interface.
- **No migration of v5 Redis state.** Durable Redis queue/topic/job/scheduler state is not readable by v6.
- `@k2b/sync/browser` is removed. Only the local `retry` helper remains browser-safe, via `@k2b/sync/retry`.
- `ratelimit` is removed from Sync. Keep rate limiting where your Redis lives (it left Sync precisely because Redis stays application-owned for sessions/KV).
- Internal APIs, wire formats, and resource layouts are all new.

## Before you upgrade

1. **Provision NATS.** A 2.14+ cluster with JetStream and persistent storage (three nodes recommended). Sync will not create or configure servers.
2. **Drain v5 work.** Stop producers, let workers finish or explicitly disposition accepted Redis work (finish it, export the few items that matter, or accept their loss after checking inactivity). v6 cannot see it.
3. **Audit stored references.** Rows in your database holding v5 cursors, schedule ids, job ids, or resource names must be migrated or reset deliberately — v6 cursors and ids use new formats.

## Construction: modules → one Sync instance

v5 modules were standalone factories bound to a global Redis connection. v6 has one explicit entry point on a caller-owned connection:

```ts
// v5
import { queue } from "@k2b/sync";
const emails = queue<Email>({ id: "emails" });

// v6
import { connect } from "@nats-io/transport-node";
import { createSync } from "@k2b/sync";

const connection = await connect({ servers: [...] });
const sync = createSync({ connection, namespace: "prod", application: "mailer" });
const emails = sync.queue<Email>({ id: "emails" });
await sync.ready();
```

- `namespace` isolates deployments; `application` is ownership/diagnostics metadata.
- Sync reads no environment variables and loads no credentials.
- Create the instance once per process; shut down with `await sync.drain()` **before** closing the connection.

## Per module

| v5 | v6 | Notes |
| --- | --- | --- |
| `ratelimit(...)` | — | Removed. Keep it next to your Redis. |
| `mutex({ id })` | `sync.mutex({ id })` | `Lock.value` → `Lock.ownerToken`; new monotonic `Lock.fence` (bigint) for fencing stale writers. `withLockOrThrow` is gone — check `withLock`'s `null`. `acquire`/`withLock` take a single input object; `retry.maxAttempts` counts total tries. TTLs round up to whole seconds. |
| `queue({ id })` | `sync.queue({ id })` | `recv`/lease API → `process()` (auto-settling) or `reader()` (manual `ack`/`retry`/`deadLetter`). `delayMs` now uses broker-side message schedules. Partitioned per-key ordering is new (`ordering: { mode: "partitioned" }`). |
| `topic({ id })` | `sync.topic({ id, retention })` | Retention is required and explicit. Reads split into `live()` / `replay()` / `follow()` / `process({ consumer })`. Consumer groups are named durable consumers; pending recovery is NATS redelivery. Cursors are new and resource-bound; retention gaps throw `RetentionGapError`. |
| `job({ id })` | `sync.job({ id })` | `submit({ key, input })` — the key is now the idempotency key (deduped within `dedupeWindowMs`). Lifecycle `after` callback → `onError` decision (`retry` / `dead_letter`). No stored results; keep run state in your database. `submitMany` adds bounded fan-out. |
| `pump({ id, pull, dispatch })` | `sync.pump({ id, pull, dispatch })` | Same shape. State/checkpoints now live in NATS KV; `reconcile()` repairs lost wake-ups; `leaseMs` tunes crash takeover. Sinks must stay idempotent by `item.key`. |
| `scheduler({ id })` | `sync.scheduler({ id })` | Same 5-field cron and timezone model. The broker (NATS message schedules) is now the clock: ticks are produced and retained even when no app process runs. Leader election is gone — per-schedule serial consumers replace it; `process()` (not `start()`) serves them. `misfire: "latest" \| "all"` replaces catch-up options. `runNow({ id, requestId })` deduplicates per request id within the 120 s duplicate window. Tick retention is `{ maxAgeMs, maxTicksPerSchedule }` (per schedule, not global bytes). |
| `ephemeral({ id, ttlMs })` | `sync.ephemeral({ id, ttlMs })` | Same role. `watch({ after })` replaces the change-stream reader; a too-old revision yields one `resync_required` event and ends. TTLs round up to whole seconds (min 1s). |
| — | `sync.objectStore({ id, retention, maxObjectBytes })` | New: explicit streamed artifacts with `ObjectRef` for queue/job payloads. Sync never auto-offloads oversized payloads. |
| `retry(...)` | `retry(...)` from `@k2b/sync` or `@k2b/sync/retry` | Unchanged callback model; Redis-specific error codes removed from `isRetryableTransportError()`. |

## Browser consumers

There is no v6 browser runtime. What to do instead:

- **retry** → `@k2b/sync/retry` (no NATS, no Bun).
- Local queues/topics/presence emulation → application-local code or your state library; the v5 browser package's semantics were parity shims, not a contract worth carrying.

## Semantics that changed underneath you

- **At-least-once, everywhere durable.** Redelivery is intentional; handlers must be idempotent. There is no exactly-once mode.
- **Dedupe is windowed.** An `idempotencyKey`/job key deduplicates only within `dedupeWindowMs` (default 2 min). Permanent uniqueness belongs in your database.
- **Late acks are not detectable.** After redelivery, NATS accepts the superseded ack idempotently. `StaleDeliveryError` appears only when an ack cannot be confirmed at all.
- **Global concurrency is `delivery.maxInFlight`** (NATS MaxAckPending) shared across pods; `concurrency` is per-process-per-handle. Neither is a fair semaphore.
- **Resources drift-check.** Changing delivery/retention/partitions of an existing resource throws `ResourceDriftError` on start instead of silently reconfiguring. Apply intentional changes with operational tooling, then roll pods.

## Verification checklist

- [ ] `sync.ready()` succeeds against your real cluster from every app.
- [ ] Workers drain cleanly (`sync.drain()` before `connection.drain()`).
- [ ] Kill -9 a worker pod: its in-flight work redelivers after `ackWaitMs`.
- [ ] Restart everything: accepted jobs, pump runs, and schedule ticks resume.
- [ ] DLQ inspection/requeue wired into your ops tooling.
