---
name: sync
description: "Use this skill for @k2b/sync v6 — NATS-native distributed sync primitives for TypeScript/Bun: queue, topic, job, pump, scheduler, mutex, ephemeral, objectStore, retry. Use when code imports from `@k2b/sync` or `@k2b/sync/retry`, or when building features that need: durable work queues with at-least-once delivery + DLQ + idempotency + partitioned per-key ordering, retained event logs with live broadcast / cursor replay / durable named consumers, durable background jobs with bounded fan-out, checkpointed backfills/imports (pump), broker-durable cron scheduling that survives full app downtime, KV CAS locks with fencing, TTL presence/registries with watch + resync, streamed internal artifacts with ObjectRefs, or abort-aware local retries. Also use for migrating v5 (Redis) code to v6 (NATS hard cut, no browser runtime, no ratelimit)."
---

# @k2b/sync — v6 (NATS-native)

One package, one runtime: Bun/Node on NATS 2.14+ with JetStream. There is no Redis, no browser parity package, and no rate limiter in v6. The only browser-safe subpath is `@k2b/sync/retry`.

## Core pattern

```ts
import { connect } from "@nats-io/transport-node";
import { createSync } from "@k2b/sync";

const connection = await connect({ servers: [...] }); // caller-owned; Sync reads no ENV
const sync = createSync({ connection, namespace: "prod", application: "mailer" });

const emails = sync.queue<Email>({ id: "emails" });   // declaration only, no I/O
await sync.ready();                                    // provision + drift-check, fails loudly

// ... work ...

await sync.drain({ timeoutMs: 30_000 });               // Sync's workers/readers
await connection.drain();                              // then the caller's connection
```

- One Sync instance per process; primitives are declared through it.
- `namespace` isolates deployments; `application` is ownership metadata (default resource owner). A different application opening a shared resource must pass the same `owner` explicitly.
- Redeclaring an existing resource with a different config throws `ResourceDriftError` (nothing is mutated). Same identity + different config in one process throws before I/O.

## Non-negotiable semantics

- Everything durable is **at-least-once** — handlers/sinks must be idempotent. Late acks after redelivery settle idempotently; they are not detectable as stale.
- `concurrency` = local handlers per worker handle per process. `delivery.maxInFlight` = global NATS MaxAckPending per durable consumer across all pods. Never confuse them.
- Idempotency keys dedupe only within `dedupeWindowMs` (default 2 min), scoped per tenant. Permanent uniqueness belongs in the application database.
- Ordering exists only in partitioned queues (`ordering: { mode: "partitioned", partitions: N }`, requires `orderingKey`, serial per partition, partition count caps global in-flight).
- Payloads are JSON, limited locally (128 KiB default envelope; 4 KiB ephemeral). Large artifacts go through `sync.objectStore()` explicitly — Sync never auto-offloads; pass the returned `ObjectRef` in job/queue payloads.
- `tenantId` defaults to `"default"` and is a logical namespace, not a security boundary.

## Choosing a primitive

| Need | Use |
|---|---|
| Background tasks with idempotent submission | `job` (queue + required key + onError policy + `submitMany` fan-out) |
| Raw durable work, manual settlement | `queue` (`process()` auto-settles; `reader()` for manual ack/retry/deadLetter) |
| Event log: replay, independent cursors, competing group consumers | `topic` (`live` / `replay` / `follow` / `process({ consumer })`) |
| Browser cache invalidation fan-out | `topic.live()` (best-effort broadcast, then read source of truth) |
| Finite backfill/import/reindex with checkpoints | `pump` (KV truth, per-item checkpoints, `reconcile()`) |
| Cron that survives full app downtime | `scheduler` (broker-side NATS message schedules, `misfire: "latest" \| "all"`, `runNow`) |
| Short critical section / leader-ish work | `mutex` (KV CAS lease + monotonic `fence` bigint) |
| Presence / service registry / transient KV | `ephemeral` (per-key TTL, `snapshot` + `watch({ after })`, explicit `resync_required`) |
| Shared bounded artifacts for parallel work | `objectStore` (streaming, digest, retention; refs don't pin) |
| Local retries with backoff | `retry` from `@k2b/sync/retry` |

## Failure-path cheatsheet

- Queue/job/topic handler throws → nak with `backoffMs[attempt-1]` → after `maxAttempts` → DLQ. Queue/job use `deadLetters.list/requeue/delete`; topic uses `list/get/delete` and opt-in consumer-only `replay` through the original idempotent handler. Job `onError` can force `{ action: "retry", delayMs }` or `{ action: "dead_letter", reason }`; a throwing onError retries (never accidentally acks).
- Process dies → redelivery after `ackWaitMs` on another pod. Long handlers call `heartbeat()`.
- Topic cursor below retention → `RetentionGapError` (re-snapshot); cursor from another topic → `CursorMismatchError`.
- Ephemeral watch behind history → one `resync_required` event, then the iterator ends.
- `sync.drain()` timeout → handler signals abort, unfinished deliveries are naked for other pods.
- Errors to catch by name: `ResourceDriftError`, `PayloadTooLargeError`, `ObjectTooLargeError`, `BatchSubmitError` (has `accepted`/`duplicates`), `StaleDeliveryError`, `SnapshotOverflowError`.

## Diagnostics

`sync.health()` (sync, for health endpoints), `await sync.resources()` (sanitized live summaries incl. DLQ depth), `sync.events()` / `observe` callback (bounded structured events; observers can never block or alter transport work).

## Testing against a real cluster

The repo ships `compose.nats.yml` (persistent 3-node NATS 2.14 on ports 14222-14224). `bun run test` (parallel files) and `bun run test:serial` (fault tests that restart nodes — never run concurrently with other suites). Tests use a unique `namespace` per run for isolation.

## References

- `references/api.md` — full v6 API surface per primitive with examples.
- Repo `MIGRATION.md` — v5 → v6 hard-cut mapping (modules → `createSync`, removed browser/ratelimit, changed semantics).
