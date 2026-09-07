# @k2b/sync

NATS-native distributed synchronization primitives for TypeScript and [Bun](https://bun.sh).

Sync v6 is a small, explicit layer over [NATS](https://nats.io) Core, JetStream, KV, and Object Store. It does not hide the distributed semantics: durable work is at-least-once, handlers must be idempotent, ordering exists only where you declare it, and resource configuration drift fails loudly instead of being patched silently.

Nine primitives: **queue**, **topic**, **job**, **pump**, **scheduler**, **mutex**, **ephemeral**, **objectStore**, and the local **retry** helper.

## Requirements

- NATS Server **2.14+** with JetStream enabled (three-node cluster recommended for production)
- Bun (or Node 22+) with the official NATS.js client for the connection (Sync pins its own `@nats-io/*` dependencies to exactly 3.4.0)
- An **already connected, caller-owned** `NatsConnection` — Sync never reads environment variables, loads credentials, or creates infrastructure

## Installation

```bash
bun add @k2b/sync @nats-io/transport-node
```

## Getting started

```ts
import { connect } from "@nats-io/transport-node";
import { createSync } from "@k2b/sync";

const connection = await connect({ servers: ["nats://nats-0:4222", "nats://nats-1:4222", "nats://nats-2:4222"] });

const sync = createSync({
  connection,
  namespace: "cloud-prod",   // deployment isolation (dev / staging / prod)
  application: "notebooks",  // ownership + diagnostics metadata
});

const runs = sync.job<{ runId: string }>({ id: "workflow-runs" });

await sync.ready(); // verifies the server, provisions resources, fails on drift
```

`createSync()` and the primitive factories perform no I/O. `ready()` verifies the connection and server version, creates missing resources, and compares every existing resource against its declaration — an incompatible difference throws `ResourceDriftError` with a field diff and mutates nothing.

Shutdown order:

```ts
await sync.drain({ timeoutMs: 30_000 }); // stop pulls, finish handlers, nak leftovers
await connection.drain();                // the connection is yours
```

## The two concurrency knobs

These have exactly one meaning everywhere:

- **`process({ concurrency: 64 })`** — at most 64 handlers run simultaneously *in this worker handle in this process*. Never a cluster limit.
- **`delivery.maxInFlight: 512`** — the durable NATS consumer's global unacknowledged-delivery ceiling (`MaxAckPending`), shared by all pods.

Four pods × `concurrency: 64` with `maxInFlight: 512` ⇒ at most `min(256, 512)` running handlers. A dead pod occupies its unacknowledged slots until `ackWaitMs` expires, then the work is redelivered elsewhere.

Sync pulls messages only for currently free local slots — there is no hidden prefetch buffer, so unclaimed work stays on the server for other pods.

## Queue

Durable work with at-least-once delivery, competing consumers, delay, retries, and a DLQ.

```ts
const emails = sync.queue<{ to: string }>({
  id: "emails",
  delivery: { ackWaitMs: 30_000, maxAttempts: 5, maxInFlight: 1_000, backoffMs: [1_000, 5_000, 30_000] },
  retention: { maxAgeMs: 7 * 24 * 3_600_000, maxBytes: 1024 ** 3 },
});

await emails.send({ data: { to: "a@example.com" }, idempotencyKey: "welcome:42" });
await emails.send({ data: { to: "b@example.com" }, delayMs: 60_000 }); // broker-side delay

const worker = await emails.process({ concurrency: 8 }, async (message) => {
  await deliver(message.data);        // resolves → acked; throws → retried, then DLQ
  await message.heartbeat();          // long handlers reset ackWait
});

const dead = await emails.deadLetters.list();
await emails.deadLetters.requeue({ messageId: dead[0].messageId, idempotencyKey: "retry-1" });
```

- `idempotencyKey` deduplicates within `dedupeWindowMs` (default 2 min), scoped per tenant.
- `delayMs`/`at` use one-shot NATS message schedules — no consumer slot is occupied while waiting.
- `reader()` gives manual `ack()` / `retry()` / `deadLetter()` settlement per message.
- `sendBatch([...])` enqueues up to 1000 messages atomically (all-or-nothing; no dedupe ids — NATS batches exclude them). `pause()`/`resume()` stop and restart global delivery without touching publishers. `send({ ttlMs })` makes work expire if not settled in time.
- `ordering: { mode: "partitioned", partitions: 64 }` hashes `orderingKey` to a stable partition with strictly serial per-partition delivery — including across handler failures: partitioned retries happen in place (the delivery is held with heartbeats through the backoff) so younger messages can never overtake a retrying one. The partition count becomes the global in-flight ceiling; this is for per-aggregate processing, not general fan-out.
- Retention limits (`maxAgeMs`/`maxBytes`) are a hard loss boundary: NATS forbids reject-new on streams with message schedules, so at the limits the **oldest pending work is dropped**. Size them generously.

## Job

The normal shape for background tasks: a queue plus a **required idempotent key**, retry policy, and bounded fan-out.

```ts
const runs = sync.job<{ runId: string }>({ id: "workflow-runs" });

await runs.submit({ key: `run:${runId}`, input: { runId } }); // duplicate keys dedupe (windowed)
await runs.submit({ key: "reindex", input, coalesce: true }); // ≤1 queued-or-running; key frees on completion

await runs.submitMany(
  runIds.map((runId) => ({ key: `run:${runId}`, input: { runId } })),
  { publishConcurrency: 128, maxPendingBytes: 8 * 1024 * 1024 }, // bounded promises AND bytes
);

await runs.process(
  {
    concurrency: 64,
    onError: async ({ context, error }) => {
      await persistFailure(context.jobId, error);
      return context.failureCount < 2 ? { action: "retry", delayMs: 5_000 } : { action: "dead_letter", reason: "gave up" };
    },
  },
  async (context) => {
    await runWorkflow(context.input.runId, { signal: context.signal });
  },
);
```

Jobs do not store results or expose `join()` — durable domain status belongs in your database. `submitMany` is not atomic: on failure a `BatchSubmitError` reports the accepted and duplicate counts and prior accepted items stay accepted.

`coalesce: true` keeps the first accepted input for each tenant and key. Both `submit` and `submitMany` use the same claim: duplicates join that job and do not replace its input. If publication fails with an unknown outcome, retrying the submission repairs the original pending job. Handlers still need to be idempotent and call `heartbeat()` during long work.

`context.resubmit({ input, delayMs })` requests a continuation after the handler succeeds. Coalesced continuations retain the key across the handoff and preserve the original ordering key and metadata. Dead-letter requeue also respects an active coalesced key.

**Upgrading existing 6.2.0 deployments:** the coalescing repair in this checkout requires stopping all 6.2.0 producers and workers before starting the corrected code. Do not run old and corrected writers together: old workers can overwrite or delete a newer claim. Existing queued jobs can adopt their legacy claims. A legacy pending claim without a queued message contains no recoverable input; a submission reports `SyncUsageError` instead of inventing a receipt. Reconcile that specific job with application-owned state before clearing its old claim and submitting it again. No streams or claims are automatically reset.

## Topic

A retained event log with four deliberately different reads:

```ts
const events = sync.topic<NotebookEvent>({
  id: "notebook-events",
  retention: { maxAgeMs: 24 * 3_600_000, maxBytes: 256 * 1024 * 1024 },
});

const receipt = await events.publish({ data: event, tenantId: workspaceId });

// Optimistic per-tenant event sourcing: append only if nothing was written since.
await events.publish({ data: event, tenantId: workspaceId, expectedAfter: receipt.cursor }); // ConflictError on lost races
await events.publishBatch({ tenantId: workspaceId, events: [...], expectedAfter: receipt.cursor }); // atomic multi-event append

// 1. live(): core NATS broadcast — best-effort, no replay, every listener sees it.
for await (const event of events.live({ tenantId: workspaceId })) notifySockets(event);

// 2. replay(): from a cursor to the head captured at start, then ends.
for await (const event of events.replay({ tenantId: workspaceId, after: cursor })) apply(event);

// 3. follow(): like replay but stays open for new events.
for await (const event of events.follow({ tenantId: workspaceId, after: cursor })) apply(event);

// 4. process(): named durable consumer — pods with the same name compete,
//    different names own independent cursors.
await events.process({ consumer: "search-indexer", concurrency: 4 }, async (event) => index(event));
```

Cursors are opaque and resource-bound (`CursorMismatchError` elsewhere). If a cursor points below the retained window — in `replay()`, `follow()`, or a fresh `process({ start: { after } })` consumer — Sync throws `RetentionGapError` instead of silently skipping; its `resumeAfter` cursor resumes from the first retained event without losing it. `live()` events carry no cursor and are suitable for invalidate-then-read, not as durable acceptance evidence.

Two costs to know: `tenantId` on replay/follow is a client-side filter, not a partition — a tenant-scoped read streams the whole topic (all tenants) from the server; for high-volume multi-tenant logs prefer one topic per tenant or a durable `process()` consumer. And the per-consumer DLQ stream is write-only through Sync's API — inspect it with NATS tooling.

## Pump

Checkpointed draining of a finite source (imports, backfills, reindexing). The KV run record is the truth; per-item checkpoints mean a crash repeats only ambiguous items.

```ts
const reindex = sync.pump<{ mailbox: string }, string, { key: string }>({
  id: "mail-reindex",
  batchSize: 100,
  dispatchConcurrency: 16,
  pull: async ({ input, cursor, limit }) => fetchPage(input.mailbox, cursor, limit),
  dispatch: async ({ item }) => indexItem(item.key), // must be idempotent by item.key
});

await reindex.start({ key: "mailbox:42", input: { mailbox: "42" } });
await reindex.process({ concurrency: 4 });
const state = await reindex.get({ key: "mailbox:42" }); // queued | running | waiting | completed | failed | canceled
```

Wake-ups are repairable: `process()` reconciles lost wake-ups from KV state on start, and `reconcile()` is callable explicitly.

## Scheduler

NATS 2.14 message schedules are the clock: the broker produces durable ticks **even while every application process is offline**.

```ts
const cron = sync.scheduler({
  id: "maintenance",
  // Tick retention is per schedule (age + count). Global byte limits are
  // deliberately not configurable: with discard-old they would eventually
  // evict the broker-side schedule definitions and silently stop the clock.
  retention: { maxAgeMs: 7 * 24 * 3_600_000, maxTicksPerSchedule: 10_000 },
});

await cron.create({
  id: "cleanup",
  cron: "0 3 * * *",            // five-field cron, minute resolution
  timezone: "Europe/Berlin",
  misfire: "latest",             // or "all": execute every retained slot
  process: async (context) => {
    await cleanup({ signal: context.signal });
  },
});

await cron.process({ concurrency: 4 }); // runs of one schedule never overlap
await cron.runNow({ id: "cleanup", requestId: "manual-1" }); // durably accepted
```

`misfire: "latest"` coalesces ticks that accumulated during downtime and executes only the newest retained slot — the newest accepted slot is never lost. `runNow` returning means the run is durably accepted, not that it started or finished; repeating a `requestId` deduplicates within the 120 s duplicate window (like every other idempotency key in v6). `delete()` cancels the broker schedule and drops its retained ticks; a later `create()` starts fresh.

## Mutex

KV compare-and-set leases with monotonic fencing.

```ts
const locks = sync.mutex({ id: "provider-refresh", ttlMs: 10_000 });

const result = await locks.withLock({ resource: "tenant:42" }, async (lock) => {
  // lock.fence is a monotonic bigint — persist and compare it if stale
  // writes to external systems after lease expiry must be excluded.
  return refresh(lock.fence);
});
```

A lease alone cannot stop an expired owner from writing to PostgreSQL afterwards. Consumers needing strict exclusion compare the `fence` or make effects idempotent.

## Ephemeral

Presence, service registry, and transient state on NATS KV with per-key TTL.

```ts
const registry = sync.ephemeral<{ url: string }>({ id: "services", ttlMs: 15_000 });

await registry.upsert({ key: "api/pod-1", value: { url } });
await registry.touch({ key: "api/pod-1" });          // heartbeat: refresh TTL

const snap = await registry.snapshot({ prefix: "api/" });
for await (const event of registry.watch({ after: snap.revision })) {
  // upsert | delete | expire | resync_required
}
```

A watch without `after` starts by replaying the current entries as upserts, then streams changes. If the watch revision fell out of history, one explicit `resync_required` event is emitted and the watch ends — take a fresh snapshot; Sync never silently skips ahead. TTLs round up to whole seconds (NATS minimum 1s); entries read back via snapshot/watch report `updatedAt` and omit `expiresAt` (a reader cannot know custom per-key TTLs).

## Object store

Explicit large-artifact storage. Sync **never** auto-offloads oversized payloads — you upload explicitly and pass the returned `ObjectRef` (a plain JSON value) through queues and jobs.

```ts
const artifacts = sync.objectStore({
  id: "workflow-artifacts",
  retention: { maxAgeMs: 7 * 24 * 3_600_000, maxBytes: 100 * 1024 ** 3 },
  maxObjectBytes: 512 * 1024 ** 2,
});

const ref = await artifacts.put({ key: `runs/${runId}/input`, body: readableStream });
await runs.submit({ key: runId, input: { runId, artifact: ref } });

const stored = await artifacts.get(ref); // null if deleted or replaced since
// get() bounds the wait between body chunks (idleTimeoutMs, default 30 s):
// an object purged mid-read errors the stream instead of hanging it.
```

Streaming both ways, digest-verified, byte-limited mid-stream (`ObjectTooLargeError`). References do not pin objects: choose bucket retention larger than your maximum queue residence plus retry window, and `delete()` explicitly when an artifact is no longer shared. Permanent end-user files belong in your application's object storage, not here.

## Retry

A local, transport-free helper — also importable from the browser-safe subpath `@k2b/sync/retry`.

```ts
import { retry, expBackoff, isRetryableTransportError } from "@k2b/sync/retry";

const result = await retry({
  run: async () => fetchThing(),
  after: async ({ ctx }) => {
    if (ctx.error && isRetryableTransportError(ctx.error) && ctx.attempt < 5) {
      ctx.reschedule({ delayMs: ctx.expBackoff() });
    }
  },
});
```

## Diagnostics

```ts
sync.health();            // { state, connection, pendingResources, driftedResources, activeWorkers, activeHandlers, droppedEvents }
await sync.resources();   // sanitized per-resource summaries (messages, bytes, consumers, DLQ depth, ...)
for await (const event of sync.events()) { ... } // bounded structured events; slow readers drop events, never block work
```

Observers are contained: a throwing or slow observer can never alter transport settlement.

## Resource model

User-provided ids never become raw NATS names. Every resource is identified by `{ namespace, kind, id }`, hashed into stable stream/KV/bucket names (`S6_Q_…`, `KV_S6_E_…`) and lower-case subject tokens (`sync.v6.<ns>.queue.<hash>.t.<tenant>.work`). The full identity, owner, and API version are stamped into resource metadata.

- `owner` defaults to `application`; every application opening a shared resource must declare the same configuration **and** owner.
- Drift (any semantic difference between declaration and live resource) throws `ResourceDriftError` and never mutates the resource.
- Two conflicting declarations of one resource in the same process fail before any I/O.

## Semantics you must build on

- Durable delivery (queue, job, durable topic consumers, pump, scheduler) is **at-least-once**. Handlers and sinks must be idempotent.
- A successful publish/submit means the stream quorum accepted the message — not that a handler ran.
- A late ack of a delivery that was already redelivered and settled elsewhere is accepted idempotently by NATS; it is not detectable as "stale". `StaleDeliveryError` is thrown when an ack cannot be confirmed at all (e.g. the consumer was deleted).
- Ordering exists only in partitioned queues (per key) and within a single topic reader.
- Payload limits are enforced locally on the complete encoded envelope (default 128 KiB; ephemeral values 4 KiB) before publish.

## Development

```bash
docker compose -f compose.nats.yml up -d --wait  # persistent 3-node NATS 2.14 cluster (ports 14222-14224)
cd packages/sync
bun run test         # parallel suite against the real cluster
bun run test:serial  # fault suite (node restarts) — must run alone
bun run typecheck
```

## Migrating from v5

v6 is a hard cut: Redis is gone, `@k2b/sync/browser` is gone, `ratelimit` is gone, and no v5 state is migrated. See [MIGRATION.md](./MIGRATION.md).

## License

MIT
