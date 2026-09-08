import type { KV } from "@nats-io/kv";
import { decodeJson, encodeJson } from "./codec.ts";
import { kvCasPut } from "./kv.ts";
import { asError } from "./errors.ts";
import { assertName, assertSubjectLength, kvBucketName, resourceIdentity, subjectToken } from "./naming.ts";
import { ensureKv, toStorageType } from "./resources.ts";
import type { ProvisionContext } from "./resources.ts";
import type { SyncRuntime } from "./runtime.ts";
import type { JsonValue } from "./codec.ts";

// ==========================
// Types
// ==========================

export type Lock = {
  resource: string;
  ownerToken: string;
  /**
   * Monotonic fencing token: the KV revision of the successful acquisition.
   * A lease alone cannot stop a stale owner from writing to external systems
   * after expiry — persist and compare the fence, or make effects idempotent.
   */
  fence: bigint;
  expiresAt: Date;
};

export type MutexConfig = {
  id: string;
  owner?: string;
  /** Lease TTL. Rounded up to whole seconds (NATS minimum 1s). Default 10_000. */
  ttlMs?: number;
  /** Acquisition retries: total tries including the first. Default { maxAttempts: 10, delayMs: 200 }. */
  retry?: { maxAttempts?: number; delayMs?: number };
  replicas?: number;
};

export type Mutex = {
  ready(): Promise<void>;
  acquire(input: { resource: string; ttlMs?: number; signal?: AbortSignal }): Promise<Lock | null>;
  extend(lock: Lock, options?: { ttlMs?: number }): Promise<boolean>;
  release(lock: Lock): Promise<boolean>;
  withLock<T>(
    input: { resource: string; ttlMs?: number; signal?: AbortSignal },
    fn: (lock: Lock) => Promise<T>,
  ): Promise<T | null>;
};

type LockRecord = {
  ownerToken: string;
  expiresAt: string;
};

// ==========================
// Mutex factory
// ==========================

export const createMutex = (runtime: SyncRuntime, config: MutexConfig): Mutex => {
  const identity = resourceIdentity(runtime.namespace, "mutex", config.id);
  const owner = config.owner ?? runtime.application;
  const defaultTtlMs = config.ttlMs ?? 10_000;
  if (!Number.isSafeInteger(defaultTtlMs) || defaultTtlMs <= 0) {
    throw new RangeError("ttlMs must be a positive integer");
  }
  const maxAttempts = config.retry?.maxAttempts ?? 10;
  const retryDelayMs = config.retry?.delayMs ?? 200;
  if (!Number.isSafeInteger(maxAttempts) || maxAttempts < 1) {
    throw new RangeError("retry.maxAttempts must be a positive integer");
  }
  const replicas = config.replicas ?? runtime.defaults.replicas;
  const storage = toStorageType(runtime.defaults.storage);
  const bucket = kvBucketName(identity);
  const label = `mutex ${config.id}`;

  let kv: KV | null = null;

  const declaration = runtime.declare({
    identity,
    owner,
    configKey: JSON.stringify(["mutex", config.id, owner, replicas]),
    natsNames: [`KV_${bucket}`],
    provision: async (ctx: ProvisionContext) => {
      kv = await ensureKv(ctx, identity, owner, bucket, {
        history: 1,
        replicas,
        storage,
        markerTTL: 1_000,
      });
    },
    summary: async (ctx: ProvisionContext) => {
      const status = await (kv ?? (await ctx.kvm.open(bucket))).status();
      return { locks: status.values } satisfies Record<string, JsonValue>;
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

  const keyOf = (resource: string): string => {
    assertName(resource, "resource");
    const key = `r.${subjectToken(resource, "resource")}`;
    assertSubjectLength(`$KV.${bucket}.${key}`);
    return key;
  };

  const ttlSeconds = (ttlMs: number): number => Math.max(1, Math.ceil(ttlMs / 1_000));

  /** CAS put with per-key TTL: expected-last-subject-sequence plus Nats-TTL headers. */
  const casPutWithTtl = async (key: string, record: LockRecord, previousSeq: number, ttlMs: number): Promise<number | null> => {
    const ctx = await runtime.context();
    return kvCasPut(ctx, bucket, key, encodeJson(label, record, 4_096), previousSeq, { ttl: `${ttlSeconds(ttlMs)}s` });
  };

  /** Last KV state of the resource key: live record, tombstone revision, or nothing. */
  const readKey = async (
    resource: string,
  ): Promise<{ record: LockRecord | null; lastRevision: number }> => {
    const store = await getKv();
    const entry = await store.get(keyOf(resource));
    if (entry === null) return { record: null, lastRevision: 0 };
    if (entry.operation !== "PUT") return { record: null, lastRevision: entry.revision };
    try {
      return { record: decodeJson<LockRecord>(entry.value), lastRevision: entry.revision };
    } catch {
      return { record: null, lastRevision: entry.revision };
    }
  };

  const tryAcquire = async (resource: string, ttlMs: number): Promise<Lock | null> => {
    const key = keyOf(resource);
    const { record: existing, lastRevision } = await readKey(resource);
    if (existing !== null) return null;
    const ownerToken = crypto.randomUUID();
    const expiresAt = new Date(Date.now() + ttlSeconds(ttlMs) * 1_000);
    const record: LockRecord = { ownerToken, expiresAt: expiresAt.toISOString() };
    // A tombstone (expired/released lock) counts as the expected last revision.
    const seq = await casPutWithTtl(key, record, lastRevision, ttlMs);
    if (seq === null) return null;
    // The acquisition revision is the fence; it is preserved across extends.
    return { resource, ownerToken, fence: BigInt(seq), expiresAt };
  };

  const acquire: Mutex["acquire"] = async (input) => {
    runtime.assertActive();
    await declaration.ready();
    const ttlMs = input.ttlMs ?? defaultTtlMs;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      if (input.signal?.aborted) return null;
      const lock = await tryAcquire(input.resource, ttlMs);
      if (lock !== null) return lock;
      if (attempt < maxAttempts) {
        await new Promise<void>((resolve) => {
          const onAbort = (): void => {
            clearTimeout(timer);
            resolve();
          };
          const timer = setTimeout(() => {
            input.signal?.removeEventListener("abort", onAbort);
            resolve();
          }, retryDelayMs);
          input.signal?.addEventListener("abort", onAbort, { once: true });
        });
      }
    }
    return null;
  };

  const extend: Mutex["extend"] = async (lock, options = {}) => {
    // Existing work may finish during graceful drain; stopped workers must not renew leases.
    if (runtime.health().state === "stopped") return false;
    const ttlMs = options.ttlMs ?? defaultTtlMs;
    const current = await readKey(lock.resource);
    if (current.record === null || current.record.ownerToken !== lock.ownerToken) {
      runtime.events.emit({ type: "lock_lost", resource: config.id, kind: "mutex", detail: { lock: lock.resource } });
      return false;
    }
    const expiresAt = new Date(Date.now() + ttlSeconds(ttlMs) * 1_000);
    const record: LockRecord = { ...current.record, expiresAt: expiresAt.toISOString() };
    const seq = await casPutWithTtl(keyOf(lock.resource), record, current.lastRevision, ttlMs);
    if (seq === null) {
      runtime.events.emit({ type: "lock_lost", resource: config.id, kind: "mutex", detail: { lock: lock.resource } });
      return false;
    }
    lock.expiresAt = expiresAt;
    return true;
  };

  const release: Mutex["release"] = async (lock) => {
    // Releasing an existing lease is settlement, including during/after drain.
    const store = await getKv();
    const current = await readKey(lock.resource);
    if (current.record === null || current.record.ownerToken !== lock.ownerToken) return false;
    try {
      await store.delete(keyOf(lock.resource), { previousSeq: current.lastRevision });
      return true;
    } catch {
      return false;
    }
  };

  const withLock: Mutex["withLock"] = async (input, fn) => {
    const lock = await acquire(input);
    if (lock === null) return null;
    try {
      return await fn(lock);
    } finally {
      await release(lock).catch(() => {});
    }
  };

  return {
    ready: () => declaration.ready(),
    acquire,
    extend,
    release,
    withLock,
  };
};
