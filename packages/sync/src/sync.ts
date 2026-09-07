import type { SyncEvent } from "./events.ts";
import { createRuntime } from "./runtime.ts";
import type { DrainResult, SyncConfig, SyncHealth, SyncResourceSummary, SyncRuntime } from "./runtime.ts";
import { createEphemeral } from "./ephemeral.ts";
import type { Ephemeral, EphemeralConfig } from "./ephemeral.ts";
import { createJob } from "./job.ts";
import type { Job, JobConfig } from "./job.ts";
import { createMutex } from "./mutex.ts";
import type { Mutex, MutexConfig } from "./mutex.ts";
import { createPump } from "./pump.ts";
import type { Pump, PumpConfig, PumpItem } from "./pump.ts";
import { createObjectStore } from "./object-store.ts";
import type { ObjectStore, ObjectStoreConfig } from "./object-store.ts";
import { createQueue } from "./queue.ts";
import type { DeadLetterStore, Queue, QueueConfig } from "./queue.ts";
import { createScheduler } from "./scheduler.ts";
import type { ScheduleInfo, Scheduler, SchedulerConfig } from "./scheduler.ts";
import { createTopic } from "./topic.ts";
import type { Topic, TopicConfig } from "./topic.ts";

/** Controls for resources declared in this process. Identity includes kind. */
export type SyncControl = Readonly<{ namespace: string; id: string; owner: string } & (
  | { kind: "queue" | "job"; deadLetters: DeadLetterStore<unknown> }
  | { kind: "scheduler"; scheduler: Pick<Scheduler, "list" | "runNow" | "awaitRun"> }
)>;

// ==========================
// Public Sync surface
// ==========================

export type Sync = {
  ready(): Promise<void>;
  drain(options?: { timeoutMs?: number }): Promise<DrainResult>;
  health(): SyncHealth;
  resources(): Promise<SyncResourceSummary[]>;
  /** No I/O or provisioning. Controls retain the existing resource data and handlers. */
  controls(): readonly SyncControl[];
  events(options?: { signal?: AbortSignal }): AsyncIterable<SyncEvent>;

  topic<T>(config: TopicConfig): Topic<T>;
  queue<T>(config: QueueConfig): Queue<T>;
  job<Input>(config: JobConfig): Job<Input>;
  ephemeral<T>(config: EphemeralConfig): Ephemeral<T>;
  objectStore(config: ObjectStoreConfig): ObjectStore;
  mutex(config: MutexConfig): Mutex;
  pump<Input, Cursor, Item extends PumpItem>(config: PumpConfig<Input, Cursor, Item>): Pump<Input, Cursor>;
  scheduler(config: SchedulerConfig): Scheduler;
};

/**
 * Create a Sync instance on an already connected, caller-owned NATS
 * connection. Performs no I/O; `ready()` verifies the server, provisions
 * declared resources, and fails clearly on configuration drift.
 */
export const createSync = (config: SyncConfig): Sync => {
  const runtime: SyncRuntime = createRuntime(config);
  const controls = new Map<string, SyncControl>();
  const schedulerHandles = new Map<string, Scheduler[]>();
  const identity = (id: string, owner?: string) => ({ namespace: runtime.namespace, id, owner: owner ?? runtime.application });
  return {
    ready: () => runtime.ready(),
    drain: (options) => runtime.drain(options),
    health: () => runtime.health(),
    resources: () => runtime.resources(),
    controls: () => [...controls.values()],
    events: (options) => runtime.events.subscribe(options),
    topic: <T>(topicConfig: TopicConfig) => createTopic<T>(runtime, topicConfig),
    queue: <T>(queueConfig: QueueConfig) => {
      const handle = createQueue<T>(runtime, queueConfig);
      controls.set(`queue:${queueConfig.id}`, { ...identity(queueConfig.id, queueConfig.owner), kind: "queue", deadLetters: handle.deadLetters });
      return handle;
    },
    job: <Input>(jobConfig: JobConfig) => {
      const handle = createJob<Input>(runtime, jobConfig);
      controls.set(`job:${jobConfig.id}`, { ...identity(jobConfig.id, jobConfig.owner), kind: "job", deadLetters: handle.deadLetters });
      return handle;
    },
    ephemeral: <T>(ephemeralConfig: EphemeralConfig) => createEphemeral<T>(runtime, ephemeralConfig),
    objectStore: (objectStoreConfig: ObjectStoreConfig) => createObjectStore(runtime, objectStoreConfig),
    mutex: (mutexConfig: MutexConfig) => createMutex(runtime, mutexConfig),
    pump: <Input, Cursor, Item extends PumpItem>(pumpConfig: PumpConfig<Input, Cursor, Item>) =>
      createPump<Input, Cursor, Item>(runtime, pumpConfig),
    scheduler: (schedulerConfig: SchedulerConfig) => {
      const handle = createScheduler(runtime, schedulerConfig);
      const handles = schedulerHandles.get(schedulerConfig.id);
      if (handles) {
        handles.push(handle);
        return handle;
      }
      const declared = [handle];
      schedulerHandles.set(schedulerConfig.id, declared);
      controls.set(`scheduler:${schedulerConfig.id}`, {
        ...identity(schedulerConfig.id, schedulerConfig.owner), kind: "scheduler",
        scheduler: {
          list: async () => {
            const schedules = new Map<string, ScheduleInfo>();
            for (const current of declared) {
              for (const info of await current.list()) {
                const previous = schedules.get(info.id);
                schedules.set(info.id, { ...info, handlerAvailable: info.handlerAvailable || previous?.handlerAvailable === true });
              }
            }
            return [...schedules.values()].sort((a, b) => a.id.localeCompare(b.id));
          },
          runNow: (input) => handle.runNow(input),
          awaitRun: (input) => handle.awaitRun(input),
        },
      });
      return handle;
    },
  };
};
