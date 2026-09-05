import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { Kvm } from "@nats-io/kv";
import { Objm } from "@nats-io/obj";
import type { NatsConnection, Subscription } from "@nats-io/nats-core";
import type { JsonValue } from "./codec.ts";
import {
  ConflictingResourceDeclarationError,
  ResourceDriftError,
  SyncLifecycleError,
  UnsupportedServerError,
  asError,
} from "./errors.ts";
import { createEventHub } from "./events.ts";
import type { EventHub, SyncObserver } from "./events.ts";
import type { ResourceIdentity } from "./naming.ts";
import type { ProvisionContext } from "./resources.ts";
import type { WorkerRuntime } from "./worker.ts";

// ==========================
// Public runtime types
// ==========================

export type SyncDefaults = {
  replicas: number;
  storage: "file" | "memory";
};

export type SyncConfig = {
  /** An already connected NATS connection. Sync never creates its own. */
  connection: NatsConnection;
  /** Deployment isolation (development, staging, production, ...). */
  namespace: string;
  /** Ownership/diagnostics metadata for this process — not an access boundary. */
  application: string;
  defaults?: Partial<SyncDefaults>;
  observe?: SyncObserver;
};

export type SyncHealth = {
  state: "starting" | "ready" | "degraded" | "draining" | "stopped";
  connection: "connected" | "reconnecting" | "closed";
  pendingResources: number;
  driftedResources: number;
  activeWorkers: number;
  activeHandlers: number;
  droppedEvents: number;
};

export type DrainResult = {
  completed: number;
  aborted: number;
  timedOut: boolean;
};

export type SyncResourceSummary = {
  namespace: string;
  kind: string;
  id: string;
  owner: string;
  state: "pending" | "ready" | "drifted" | "failed";
  natsNames: string[];
  error?: string;
  detail?: Record<string, JsonValue>;
};

// ==========================
// Internal runtime
// ==========================

// Part of exported signatures; required for declaration emit.
// fallow-ignore-next-line unused-type
export type Declaration = {
  identity: ResourceIdentity;
  owner: string;
  /** Canonical JSON of the normalized primitive config — used for conflict detection. */
  configKey: string;
  /** NATS resource names this declaration owns (for diagnostics). */
  natsNames: string[];
  /** Largest single NATS message this resource may publish; checked against the server's max_payload. */
  maxMessageBytes?: number;
  provision(ctx: ProvisionContext): Promise<void>;
  summary?(ctx: ProvisionContext): Promise<Record<string, JsonValue>>;
};

type DeclarationEntry = Declaration & {
  state: "pending" | "ready" | "drifted" | "failed";
  error?: Error;
  inflight?: Promise<void>;
};

export type SyncRuntime = {
  readonly nc: NatsConnection;
  readonly namespace: string;
  readonly application: string;
  readonly defaults: SyncDefaults;
  readonly events: EventHub;
  /** Global readiness: verifies the server and provisions all declarations. */
  ready(): Promise<void>;
  /** The shared provision/client context. Only valid after ready(). */
  context(): Promise<ProvisionContext>;
  declare(declaration: Declaration): { ready(): Promise<void> };
  registerWorker(worker: WorkerRuntime): void;
  unregisterWorker(worker: WorkerRuntime): void;
  /** Core NATS subscriptions created by Sync (topic.live) — drained on sync.drain(). */
  registerLiveSubscription(sub: Subscription): void;
  unregisterLiveSubscription(sub: Subscription): void;
  health(): SyncHealth;
  resources(): Promise<SyncResourceSummary[]>;
  drain(options?: { timeoutMs?: number }): Promise<DrainResult>;
  assertActive(): void;
};

const MIN_SERVER = { major: 2, minor: 14 };

export const createRuntime = (config: SyncConfig): SyncRuntime => {
  const { connection: nc, namespace, application } = config;
  if (typeof namespace !== "string" || namespace.length === 0) {
    throw new SyncLifecycleError("namespace must be a non-empty string");
  }
  if (typeof application !== "string" || application.length === 0) {
    throw new SyncLifecycleError("application must be a non-empty string");
  }
  const defaults: SyncDefaults = {
    replicas: config.defaults?.replicas ?? 3,
    storage: config.defaults?.storage ?? "file",
  };
  const events = createEventHub(config.observe);
  const declarations = new Map<string, DeclarationEntry>();
  const workers = new Set<WorkerRuntime>();
  const liveSubs = new Set<Subscription>();

  let state: SyncHealth["state"] = "starting";
  let connectionState: SyncHealth["connection"] = "connected";
  let ctxPromise: Promise<ProvisionContext> | null = null;
  let readyPromise: Promise<void> | null = null;
  let statusMonitorStarted = false;
  let monitorStopped = false;

  const assertActive = (): void => {
    if (state === "draining" || state === "stopped") {
      throw new SyncLifecycleError(`sync is ${state}; create a new instance after drain`);
    }
  };

  const startStatusMonitor = (): void => {
    if (statusMonitorStarted) return;
    statusMonitorStarted = true;
    (async () => {
      for await (const status of nc.status()) {
        if (monitorStopped) break;
        if (status.type === "disconnect") {
          connectionState = "reconnecting";
          events.emit({ type: "connection", detail: { status: "reconnecting" } });
        } else if (status.type === "reconnect") {
          connectionState = "connected";
          events.emit({ type: "connection", detail: { status: "connected" } });
        }
      }
    })().catch(() => {});
    nc.closed().then(() => {
      connectionState = "closed";
      events.emit({ type: "connection", detail: { status: "closed" } });
    });
  };

  const verifyServer = (): void => {
    if (nc.isClosed()) throw new SyncLifecycleError("the supplied NATS connection is closed");
    const info = nc.info;
    if (!info) throw new UnsupportedServerError("the supplied NATS connection has no server info yet");
    if (info.jetstream !== true) throw new UnsupportedServerError("the NATS server does not have JetStream enabled");
    const [major = 0, minor = 0] = info.version.split(".").map((part) => Number.parseInt(part, 10));
    if (major < MIN_SERVER.major || (major === MIN_SERVER.major && minor < MIN_SERVER.minor)) {
      throw new UnsupportedServerError(
        `sync v6 requires NATS Server ${MIN_SERVER.major}.${MIN_SERVER.minor}+, connected to ${info.version}`,
      );
    }
  };

  /**
   * A payload limit above the server's max_payload would fail at the first
   * large publish instead of at startup — refuse it while provisioning.
   */
  const assertFitsServerPayload = (entry: DeclarationEntry): void => {
    const serverMax = nc.info?.max_payload;
    if (entry.maxMessageBytes === undefined || serverMax === undefined || entry.maxMessageBytes <= serverMax) return;
    throw new UnsupportedServerError(
      `${entry.identity.kind} ${entry.identity.id} may publish messages of ${entry.maxMessageBytes} bytes, but the NATS server max_payload is ${serverMax} bytes — lower the limit or raise max_payload`,
    );
  };

  const buildContext = (): Promise<ProvisionContext> => {
    ctxPromise ??= (async () => {
      const jsm = await jetstreamManager(nc);
      const js = jetstream(nc);
      return { jsm, js, kvm: new Kvm(js), objm: new Objm(js) };
    })();
    return ctxPromise;
  };

  const provisionEntry = async (entry: DeclarationEntry, context: ProvisionContext): Promise<void> => {
    if (entry.state === "ready") return;
    if (entry.inflight) return entry.inflight;
    entry.inflight = (async () => {
      try {
        assertFitsServerPayload(entry);
        await entry.provision(context);
        entry.state = "ready";
        entry.error = undefined;
        events.emit({ type: "resource_verified", resource: entry.identity.id, kind: entry.identity.kind });
      } catch (error) {
        const err = asError(error);
        entry.state = err instanceof ResourceDriftError ? "drifted" : "failed";
        entry.error = err;
        events.emit({
          type: "resource_drifted",
          resource: entry.identity.id,
          kind: entry.identity.kind,
          error: err.message,
        });
        throw err;
      } finally {
        entry.inflight = undefined;
      }
    })();
    return entry.inflight;
  };

  const baseReady = (): Promise<void> => {
    if (readyPromise) return readyPromise;
    readyPromise = (async () => {
      verifyServer();
      startStatusMonitor();
      const context = await buildContext();
      const entries = [...declarations.values()];
      const failures: Error[] = [];
      for (const entry of entries) {
        await provisionEntry(entry, context).catch((error) => failures.push(asError(error)));
      }
      if (failures.length > 0) {
        const summary = failures.map((f) => f.message).join(" | ");
        throw failures.length === 1 ? failures[0]! : new SyncLifecycleError(`sync.ready() failed: ${summary}`);
      }
      if (state === "starting") state = "ready";
      events.emit({ type: "ready" });
    })();
    readyPromise.catch(() => {
      // Allow retrying ready() after the caller fixes drift.
      readyPromise = null;
    });
    return readyPromise;
  };

  const ready = async (): Promise<void> => {
    assertActive();
    await baseReady();
    // Declarations registered after the first ready() still provision here.
    const pending = [...declarations.values()].filter((entry) => entry.state === "pending");
    if (pending.length > 0) {
      const context = await buildContext();
      const failures: Error[] = [];
      for (const entry of pending) {
        await provisionEntry(entry, context).catch((error) => failures.push(asError(error)));
      }
      if (failures.length === 1) throw failures[0]!;
      if (failures.length > 1) {
        throw new SyncLifecycleError(`sync.ready() failed: ${failures.map((f) => f.message).join(" | ")}`);
      }
    }
  };

  const declare = (declaration: Declaration): { ready(): Promise<void> } => {
    assertActive();
    const key = `${declaration.identity.kind}:${declaration.identity.id}`;
    const existing = declarations.get(key);
    if (existing) {
      if (existing.configKey !== declaration.configKey) {
        throw new ConflictingResourceDeclarationError(
          `${declaration.identity.kind} ${declaration.identity.id} is already declared in this process with a different configuration`,
        );
      }
      return { ready: () => entryReady(existing) };
    }
    const entry: DeclarationEntry = { ...declaration, state: "pending" };
    declarations.set(key, entry);
    return { ready: () => entryReady(entry) };
  };

  const entryReady = async (entry: DeclarationEntry): Promise<void> => {
    // Already-provisioned declarations stay usable during drain so in-flight
    // handlers can settle (checkpoint, ack); only new provisioning is refused.
    if (entry.state === "ready") return;
    assertActive();
    // Global readiness first: verifies the server exactly once and provisions
    // everything declared before ready(). Late declarations provision here.
    await baseReady();
    await provisionEntry(entry, await buildContext());
  };

  const health = (): SyncHealth => {
    let activeHandlers = 0;
    for (const worker of workers) activeHandlers += worker.active;
    const pendingResources = [...declarations.values()].filter((entry) => entry.state === "pending").length;
    const driftedResources = [...declarations.values()].filter(
      (entry) => entry.state === "drifted" || entry.state === "failed",
    ).length;
    const effective =
      state === "ready" && (connectionState !== "connected" || driftedResources > 0) ? "degraded" : state;
    return {
      state: effective,
      connection: connectionState,
      pendingResources,
      driftedResources,
      activeWorkers: workers.size,
      activeHandlers,
      droppedEvents: events.dropped,
    };
  };

  const resources = async (): Promise<SyncResourceSummary[]> => {
    const context = await buildContext();
    const summaries: SyncResourceSummary[] = [];
    for (const entry of declarations.values()) {
      const summary: SyncResourceSummary = {
        namespace: entry.identity.namespace,
        kind: entry.identity.kind,
        id: entry.identity.id,
        owner: entry.owner,
        state: entry.state,
        natsNames: entry.natsNames,
      };
      if (entry.error) summary.error = entry.error.message;
      if (entry.summary && entry.state === "ready") {
        summary.detail = await entry.summary(context).catch(() => undefined);
      }
      summaries.push(summary);
    }
    return summaries;
  };

  const drain = async (options: { timeoutMs?: number } = {}): Promise<DrainResult> => {
    if (state === "stopped") return { completed: 0, aborted: 0, timedOut: false };
    state = "draining";
    const timeoutMs = options.timeoutMs ?? 30_000;
    const deadline = Date.now() + timeoutMs;
    const snapshot = [...workers];
    // Only handlers settled during this drain count into the result.
    const baseline = new Map(snapshot.map((worker) => [worker, { completed: worker.completed, aborted: worker.aborted }]));
    await Promise.all(
      snapshot.map((worker) => worker.worker.drain({ timeoutMs: Math.max(1, deadline - Date.now()) })),
    );
    for (const sub of [...liveSubs]) {
      // Bounded: a drain flush cannot complete while disconnected.
      await Promise.race([
        sub.drain().catch(() => {}),
        Bun.sleep(Math.min(2_000, Math.max(1, deadline - Date.now()))).then(() => sub.unsubscribe()),
      ]).catch(() => {});
      liveSubs.delete(sub);
    }
    let completed = 0;
    let aborted = 0;
    for (const worker of snapshot) {
      const base = baseline.get(worker)!;
      completed += worker.completed - base.completed;
      aborted += worker.aborted - base.aborted;
    }
    const timedOut = aborted > 0 || Date.now() > deadline;
    if (timedOut) events.emit({ type: "drain_timeout", detail: { aborted } });
    monitorStopped = true;
    state = "stopped";
    return { completed, aborted, timedOut };
  };

  return {
    nc,
    namespace,
    application,
    defaults,
    events,
    ready,
    context: async () => {
      // The built context stays available during drain for the same reason.
      if (ctxPromise !== null) return ctxPromise;
      await ready();
      return buildContext();
    },
    declare,
    registerWorker: (worker) => {
      assertActive();
      workers.add(worker);
    },
    unregisterWorker: (worker) => workers.delete(worker),
    registerLiveSubscription: (sub) => liveSubs.add(sub),
    unregisterLiveSubscription: (sub) => liveSubs.delete(sub),
    health,
    resources,
    drain,
    assertActive,
  };
};
