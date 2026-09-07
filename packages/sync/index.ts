// @k2b/sync v6 — NATS-native distributed sync primitives for TypeScript and Bun.

export { createSync } from "./src/sync.ts";
export type { Sync, SyncControl } from "./src/sync.ts";

export type {
  DrainResult,
  SyncConfig,
  SyncDefaults,
  SyncHealth,
  SyncResourceSummary,
} from "./src/runtime.ts";

export type { SyncEvent, SyncObserver } from "./src/events.ts";
export type { JsonValue } from "./src/codec.ts";
export type {
  DeliveryConfig,
  MessageMeta,
  OrderingConfig,
  PublishReceipt,
  RetentionConfig,
} from "./src/types.ts";
export type { ProcessOptions, Worker } from "./src/worker.ts";

export type {
  BatchReceipt,
  PauseInfo,
  Queue,
  QueueConfig,
  QueueDelivery,
  QueueMessage,
  QueueReader,
  QueueSend,
  DeadLetter,
  DeadLetterStore,
} from "./src/queue.ts";

export type {
  Topic,
  TopicBatchEvent,
  TopicConfig,
  TopicCursor,
  TopicEvent,
  TopicHub,
  TopicLiveEvent,
  TopicProcessOptions,
  TopicPublish,
} from "./src/topic.ts";

export type {
  Job,
  JobConfig,
  JobContext,
  JobFailureDecision,
  JobProcessOptions,
  JobSubmit,
  JobSubmitManyOptions,
} from "./src/job.ts";

export type { Pump, PumpConfig, PumpItem, PumpState, PumpStatus } from "./src/pump.ts";

export type {
  ScheduleContext,
  ScheduleDefinition,
  ScheduleInfo,
  Scheduler,
  SchedulerConfig,
} from "./src/scheduler.ts";

export type { Lock, Mutex, MutexConfig } from "./src/mutex.ts";

export type {
  Ephemeral,
  EphemeralConfig,
  EphemeralEntry,
  EphemeralEvent,
  EphemeralSnapshot,
} from "./src/ephemeral.ts";

export type {
  ObjectMetadata,
  ObjectRef,
  ObjectStore,
  ObjectStoreConfig,
  ObjectStoreEvent,
  StoredObject,
  SyncObjectInfo,
} from "./src/object-store.ts";

export {
  BatchSubmitError,
  ConflictError,
  ConflictingResourceDeclarationError,
  CursorMismatchError,
  InvalidNameError,
  NotFoundError,
  ObjectTooLargeError,
  PayloadTooLargeError,
  ResourceDriftError,
  ResourceIdentityCollisionError,
  RetentionGapError,
  SnapshotOverflowError,
  StaleDeliveryError,
  SyncError,
  SyncLifecycleError,
  SyncUsageError,
  UnsupportedServerError,
} from "./src/errors.ts";
export type { ResourceDifference } from "./src/errors.ts";

export { expBackoff, isRetryableTransportError, retry } from "./src/retry.ts";
export type { BackoffOptions, RetryAfterCtx, RetryConfig, RetryCtx } from "./src/retry.ts";
