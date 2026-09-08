# Sync v6 → Cloud handoff

Sync v6 (NATS-native hard cut) is implemented and verified against a real three-node NATS 2.14 JetStream cluster. This document is the contract and checklist for the **separate Cloud migration epic**. Nothing in Cloud was modified by the Sync work, and Sync v6 is **not production-ready for Cloud until the migration below is complete and the end-user acceptance suite passes**.

## Current implementation status — 2026-09-08

The checklist below records the original migration requirements; it is not a
live list of missing Cloud features. Cloud has implemented the v6 migration,
including the separate NATS administration surface, generic DLQ health signals,
metrics, and consumer-specific topic recovery. The latter is an unreleased Sync
source change, carried by the Cloud checkout as a reproducible Bun patch on
6.3.2. Publish and adopt that Sync release before publishing the Cloud npm
library; Docker builds apply the workspace patch.

Local acceptance includes a three-node cluster, isolated recovery tests,
authenticated HTTP checks, and verification that the existing stream inventory
survives the system-account configuration update. These checks do not constitute
production acceptance. Production credentials, coordinated backups and cutover,
provider-specific smoke checks, external monitoring, and reviewed legacy cleanup
remain operator acceptance items. The canonical current operator guide is Cloud's
`docs-site/docs/en/operations/nats-operations.md`.

## What Sync v6 gives Cloud

- `createSync({ connection, namespace, application })` on one caller-owned, already connected NATS connection per app process. Sync reads no ENV and loads no credentials.
- Primitives: `queue`, `topic`, `job`, `pump`, `scheduler`, `mutex`, `ephemeral`, `objectStore`, plus browser-safe `retry` via `@k2b/sync/retry`.
- At-least-once durable delivery with explicit `concurrency` (local) vs `delivery.maxInFlight` (global) semantics.
- Broker-durable scheduler ticks (NATS message schedules): schedules keep producing accepted work while every Cloud process is offline.
- Resource provisioning with drift detection: rolling pods can never silently reconfigure a resource (`ResourceDriftError`).
- Diagnostics: `health()`, `resources()` (sanitized summaries incl. DLQ depth, pending, ack-pending), `events()`/`observe` trace hooks that cannot alter transport state.
- Removed: Redis runtime, `@k2b/sync/browser`, `ratelimit`.

Cloud use-case mapping:

| Cloud use | v6 shape |
| --- | --- |
| Workflow runs / AI workflow tasks | `job.process({ concurrency })`; PostgreSQL stays run/task truth |
| Large workflow inputs / intermediate results | `objectStore().put()` + `ObjectRef` in the job payload |
| AI turns, notifications, snapshot work | queue/job competing consumers, local concurrency + global maxInFlight |
| Grids record-event keyed work | partitioned queue by record id |
| Record/notebook/mail event logs | durable topic, independent named consumers |
| Browser invalidation | few process-wide `topic.live()` readers + Cloud-local WebSocket fan-out |
| Notebook presence, app registry | `ephemeral` snapshot/watch |
| Mail backfill / reindexing | `pump` (often dispatching into jobs) |
| Cron workflows / maintenance | `scheduler` (ticks survive complete Cloud restart) |
| Provider refresh, short critical sections | fenced `mutex` + domain idempotency |
| Rate limiting | **stays Cloud-owned Redis logic** (removed from Sync) |

## Cloud-owned migration TODOs

### Framework integration

- [ ] One NATS connection and one Sync instance per app process, created through the Cloud app lifecycle.
- [ ] Expose the same instance as `app.sync` (general code) and `lifecycleContext.sync` (startup/shutdown). A request context alone is insufficient for schedulers, workers, WebSockets, lifecycle services.
- [ ] Do not make Sync user- or request-scoped; tenant/user authorization stays application logic.
- [ ] Report ready only after `await sync.ready()`; on shutdown `await sync.drain()` **before** `connection.drain()`.
- [ ] `defineApp` decision (either is valid, evaluate against build/test imports):
  1. **Recommended:** keep `defineApp()` synchronous; `await app.start()` connects NATS and binds a ready `app.sync`. No Sync I/O during module evaluation.
  2. Breaking alternative: `const app = await defineApp(...)` — only if importing app config for SSR builds/tests/tooling does not require live NATS.
  - No magical unbounded promise queue to hide pre-start access; fail clearly on lifecycle misuse.

### Configuration and authentication

- [ ] `NATS_SERVERS` (bootstrap endpoints), `NATS_CREDS_FILE`, `NATS_TLS_CA_FILE` (mounted secrets, paths only in ENV), `SYNC_NAMESPACE`.
- [ ] One NATS account per Cloud environment; one user credential per app/workload (JWT/NKey `.creds` + TLS). No NATS user per end user.
- [ ] Separate privileged system credential for the admin surface only; app credentials must not access system subjects.
- [ ] Meaningful connection names from app + instance identity. Development may use simpler local auth.
- Client note (verified): when connecting to a cluster whose advertised routes are not reachable from the client (e.g. Docker-internal hostnames), set `ignoreClusterUpdates: true`.

### Consumer migration

- [ ] Inventory every `@k2b/sync` and `@k2b/sync/browser` import.
- [ ] Migrate module-level primitive construction to the app-scoped Sync lifecycle (see `MIGRATION.md` for the per-module v5→v6 mapping).
- [ ] Replace Sync ratelimit consumers with Cloud-owned Redis logic.
- [ ] Replace `@k2b/sync/browser` consumers (only `@k2b/sync/retry` remains browser-safe).
- [ ] Inspect PostgreSQL rows storing Sync resource ids, cursors, schedule ids, workflow/runtime state — v6 cursor and id formats are new; migrate or reset deliberately.
- [ ] Do not claim compatibility from compile success; exercise real durable workflows.

### Cutover and data safety

- [ ] Schedule maintenance; downtime is acceptable.
- [ ] Stop producers and drain or explicitly disposition Redis-backed accepted work before deploying v6 — old Redis queue/topic/job/scheduler state is **not** readable by v6. Finish it, export the small set that matters, or accept its loss after checking inactivity.
- [ ] Preserve application-owned durable data (notebook content/snapshots, workflow definitions, audit data).
- [ ] Back up PostgreSQL and relevant Redis/NATS state; document the rollback boundary.
- [ ] Complete Cloud restart and recovery test before declaring success.

### NATS administration surface

- [ ] Add a NATS admin page next to the Redis/PostgreSQL pages: cluster/node/JetStream health, streams, consumers, lag, pending/redelivery counts, DLQs, KV buckets, schedules, Sync resource ownership (`sync.resources()` + NATS system APIs).
- [ ] Safe inspection first; destructive purge/delete/replay needs separate explicit design and authorization.
- [ ] Metrics, alerting, capacity/retention views, degraded-node visibility.

### End-user acceptance ("just works" after migration)

- [ ] AI durable event streams and task recovery.
- [ ] Notebook collaboration, replay, presence, snapshots, reindexing.
- [ ] Workflows and record events.
- [ ] Mail jobs, commands, schedules, notifications.
- [ ] Independent durable topic cursors and load-balanced consumer groups.
- [ ] Jobs, pumps, and schedules after a complete Cloud restart.
- [ ] Temporary NATS disconnect and loss of one cluster node.
- [ ] Redis-backed sessions, general Cloud KV, and rate limiting unchanged.

Accepted important work must not be silently lost, and ordinary end users must not need to understand the migration.

## Verified by Sync (evidence)

75 tests against the pinned cluster (`compose.nats.yml`, NATS 2.14.3, NATS.js 3.4.0, Bun):

- Multi-process fleets sharing one durable consumer; global `maxInFlight` enforced across 3 OS processes; SIGKILL mid-work loses nothing.
- Complete client outage/restart: accepted jobs processed by a later fresh process.
- One-node loss: R3 publish/consume continue; recovery verified.
- Duplicate delivery at crash boundaries; heartbeat exclusivity; late-ack idempotence.
- Topic replay/follow with independent cursors, explicit retention gaps, competing named consumers, DLQ.
- Queue delay/dedupe/DLQ/requeue, partitioned per-key ordering under concurrency.
- Pump crash recovery without repeating checkpointed items; lost-wake-up reconciliation.
- Scheduler ticks produced during full worker absence; misfire latest/all; runNow idempotence; no overlapping runs.
- Mutex expiry, stale extend/release refusal, monotonic fencing.
- Object store streaming without full buffering, mid-stream size abort, digest-bound refs, retention expiry.
- Resource drift detection without mutation; cross-application owner enforcement.
- Package build + packed fresh Bun/TS consumer (runtime imports and types, incl. `@k2b/sync/retry`).
