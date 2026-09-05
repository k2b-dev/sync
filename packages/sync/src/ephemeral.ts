import type { KV } from "@nats-io/kv";
import { decodeJson, encodeJson } from "./codec.ts";
import { kvCasPut, kvPut } from "./kv.ts";
import { CursorMismatchError, SnapshotOverflowError, asError } from "./errors.ts";
import { assertName, assertSubjectLength, decodeSubjectToken, kvBucketName, resourceIdentity, subjectToken } from "./naming.ts";
import { ensureKv, toStorageType } from "./resources.ts";
import type { ProvisionContext } from "./resources.ts";
import type { SyncRuntime } from "./runtime.ts";
import { DEFAULT_EPHEMERAL_PAYLOAD_BYTES, DEFAULT_TENANT } from "./types.ts";
import type { JsonValue } from "./codec.ts";

// ==========================
// Types
// ==========================

export type EphemeralConfig = {
  id: string;
  owner?: string;
  /** Default per-key TTL. Rounded up to whole seconds (NATS minimum 1s). */
  ttlMs: number;
  /** Revisions kept per key. Default 1. */
  history?: number;
  /** Bounded snapshot size. Default 10_000. */
  maxEntries?: number;
  maxValueBytes?: number;
  replicas?: number;
};

export type EphemeralEntry<T> = {
  key: string;
  value: T;
  revision: string;
  /** Time of this revision (with history 1 the creation time is unknowable). */
  updatedAt: Date;
  /** Only present where Sync knows the TTL it wrote (upsert results). */
  expiresAt?: Date;
};

export type EphemeralSnapshot<T> = {
  entries: EphemeralEntry<T>[];
  revision: string;
};

export type EphemeralEvent<T> =
  | { type: "upsert"; entry: EphemeralEntry<T>; revision: string }
  | { type: "delete" | "expire"; key: string; revision: string }
  | { type: "resync_required"; requested: string; firstAvailable: string };

export type Ephemeral<T> = {
  ready(): Promise<void>;
  upsert(input: { tenantId?: string; key: string; value: T; ttlMs?: number }): Promise<EphemeralEntry<T>>;
  /** Refresh a key's TTL by republishing its last value. False if the key is absent. */
  touch(input: { tenantId?: string; key: string; ttlMs?: number }): Promise<boolean>;
  delete(input: { tenantId?: string; key: string }): Promise<boolean>;
  snapshot(input?: { tenantId?: string; prefix?: string }): Promise<EphemeralSnapshot<T>>;
  watch(input?: {
    tenantId?: string;
    prefix?: string;
    after?: string;
    signal?: AbortSignal;
  }): AsyncIterable<EphemeralEvent<T>>;
};

// ==========================
// Ephemeral factory
// ==========================

export const createEphemeral = <T>(runtime: SyncRuntime, config: EphemeralConfig): Ephemeral<T> => {
  const identity = resourceIdentity(runtime.namespace, "ephemeral", config.id);
  const owner = config.owner ?? runtime.application;
  if (!Number.isSafeInteger(config.ttlMs) || config.ttlMs <= 0) {
    throw new RangeError("ttlMs must be a positive integer");
  }
  const history = config.history ?? 1;
  const maxEntries = config.maxEntries ?? 10_000;
  const maxValueBytes = config.maxValueBytes ?? DEFAULT_EPHEMERAL_PAYLOAD_BYTES;
  const replicas = config.replicas ?? runtime.defaults.replicas;
  const storage = toStorageType(runtime.defaults.storage);
  const bucket = kvBucketName(identity);
  const label = `ephemeral ${config.id}`;

  let kv: KV | null = null;

  const declaration = runtime.declare({
    identity,
    owner,
    configKey: JSON.stringify(["ephemeral", config.id, owner, config.ttlMs, history, maxValueBytes, replicas]),
    natsNames: [`KV_${bucket}`],
    maxMessageBytes: maxValueBytes,
    provision: async (ctx: ProvisionContext) => {
      kv = await ensureKv(ctx, identity, owner, bucket, {
        history,
        replicas,
        storage,
        // Expiry markers must outlive watch resume windows; tie them to the TTL.
        markerTTL: Math.max(config.ttlMs, 1_000),
      });
    },
    summary: async (ctx: ProvisionContext) => {
      const status = await (kv ?? (await ctx.kvm.open(bucket))).status();
      return {
        entries: status.values,
        bytes: status.size,
        history: status.history,
      } satisfies Record<string, JsonValue>;
    },
  });

  const getKv = async (): Promise<KV> => {
    await declaration.ready();
    if (kv === null) {
      // Provisioned by another handle for the same declaration in this
      // process; bind without creating.
      const ctx = await runtime.context();
      kv = await ctx.kvm.open(bucket);
    }
    return kv;
  };

  // Keys are token-encoded so any UTF-8 tenant/key is KV-safe and injective.
  const kvKey = (tenantId: string, key: string): string => {
    const raw = `t.${subjectToken(tenantId, "tenantId")}.k.${subjectToken(key, "key")}`;
    // The combined tenant+key encoding must respect the subject bound the
    // rest of the codebase promises.
    assertSubjectLength(`$KV.${bucket}.${raw}`);
    return raw;
  };
  const tenantFilter = (tenantId: string): string => `t.${subjectToken(tenantId, "tenantId")}.k.>`;
  const decodeKvKey = (raw: string): string | null => {
    const parts = raw.split(".");
    if (parts.length !== 4 || parts[0] !== "t" || parts[2] !== "k") return null;
    return decodeSubjectToken(parts[3]!);
  };

  const ttlSeconds = (ttlMs: number | undefined): number => Math.max(1, Math.ceil((ttlMs ?? config.ttlMs) / 1_000));

  const encodeValue = (value: T): Uint8Array => encodeJson(label, value ?? null, maxValueBytes);

  const toEntry = (
    rawKey: string,
    value: T,
    revision: number,
    updated: Date,
    ttlMs: number | undefined | null,
  ): EphemeralEntry<T> => ({
    key: decodeKvKey(rawKey) ?? rawKey,
    value,
    revision: String(revision),
    updatedAt: updated,
    // Read paths cannot know a custom per-key TTL; they omit expiresAt rather
    // than reporting a wrong one.
    ...(ttlMs !== null ? { expiresAt: new Date(updated.getTime() + ttlSeconds(ttlMs) * 1_000) } : {}),
  });

  const upsert: Ephemeral<T>["upsert"] = async (input) => {
    runtime.assertActive();
    assertName(input.key, "key");
    const tenantId = input.tenantId ?? DEFAULT_TENANT;
    await declaration.ready();
    const rawKey = kvKey(tenantId, input.key);
    const now = new Date();
    const ctx = await runtime.context();
    const seq = await kvPut(ctx, bucket, rawKey, encodeValue(input.value), { ttl: `${ttlSeconds(input.ttlMs)}s` });
    return toEntry(rawKey, input.value, seq, now, input.ttlMs);
  };

  const touch: Ephemeral<T>["touch"] = async (input) => {
    runtime.assertActive();
    assertName(input.key, "key");
    const tenantId = input.tenantId ?? DEFAULT_TENANT;
    const store = await getKv();
    const rawKey = kvKey(tenantId, input.key);
    // CAS on the read revision: a concurrent upsert must never be reverted by
    // a heartbeat republishing the value it read.
    for (let attempt = 0; attempt < 5; attempt++) {
      const entry = await store.get(rawKey);
      if (entry === null || entry.operation !== "PUT") return false;
      const ctx = await runtime.context();
      const seq = await kvCasPut(ctx, bucket, rawKey, encodeValue(decodeJson<T>(entry.value)), entry.revision, {
        ttl: `${ttlSeconds(input.ttlMs)}s`,
      });
      if (seq !== null) return true;
      // Lost the race — whoever won also refreshed the key's TTL just now,
      // so the touch goal is already achieved.
      const current = await store.get(rawKey);
      if (current !== null && current.operation === "PUT") return true;
    }
    return false;
  };

  const deleteEntry: Ephemeral<T>["delete"] = async (input) => {
    runtime.assertActive();
    assertName(input.key, "key");
    const tenantId = input.tenantId ?? DEFAULT_TENANT;
    const store = await getKv();
    const rawKey = kvKey(tenantId, input.key);
    const entry = await store.get(rawKey);
    if (entry === null || entry.operation !== "PUT") return false;
    await store.delete(rawKey);
    return true;
  };

  const snapshot: Ephemeral<T>["snapshot"] = async (input = {}) => {
    runtime.assertActive();
    const tenantId = input.tenantId ?? DEFAULT_TENANT;
    const store = await getKv();
    const status = await store.status();
    const revision = String(status.streamInfo.state.last_seq);
    const keys = await store.keys(tenantFilter(tenantId));
    const rawKeys: string[] = [];
    for await (const rawKey of keys) {
      const key = decodeKvKey(rawKey);
      if (key === null) continue;
      if (input.prefix !== undefined && !key.startsWith(input.prefix)) continue;
      rawKeys.push(rawKey);
      // Bound the snapshot before fetching any values.
      if (rawKeys.length > maxEntries) throw new SnapshotOverflowError(label, maxEntries);
    }
    const entries: EphemeralEntry<T>[] = [];
    const chunkSize = 32;
    for (let i = 0; i < rawKeys.length; i += chunkSize) {
      const chunk = rawKeys.slice(i, i + chunkSize);
      const fetched = await Promise.all(chunk.map((rawKey) => store.get(rawKey)));
      for (let j = 0; j < chunk.length; j++) {
        const entry = fetched[j];
        if (entry === null || entry === undefined || entry.operation !== "PUT") continue;
        try {
          entries.push(toEntry(chunk[j]!, decodeJson<T>(entry.value), entry.revision, entry.created, null));
        } catch (error) {
          // One corrupt value must not poison every snapshot.
          runtime.events.emit({ type: "handler_error", resource: config.id, kind: "ephemeral", error: asError(error).message });
        }
      }
    }
    return { entries, revision };
  };

  async function* watchIterator(input: {
    tenantId: string;
    prefix?: string;
    after?: string;
    signal?: AbortSignal;
  }): AsyncGenerator<EphemeralEvent<T>> {
    runtime.assertActive();
    const store = await getKv();

    let resumeFrom: number | null = null;
    if (input.after !== undefined) {
      const after = Number(input.after);
      if (!Number.isSafeInteger(after) || after < 0) {
        throw new CursorMismatchError(`after is not a valid ephemeral revision: ${input.after}`);
      }
      const status = await store.status();
      const firstSeq = status.streamInfo.state.first_seq;
      if (after + 1 < firstSeq) {
        // History no longer covers the requested revision. The caller must
        // take a fresh snapshot; Sync never silently skips to the head.
        runtime.events.emit({
          type: "watch_resync_required",
          resource: config.id,
          kind: "ephemeral",
          detail: { requested: input.after, firstAvailable: String(firstSeq) },
        });
        yield { type: "resync_required", requested: input.after, firstAvailable: String(firstSeq) };
        return;
      }
      resumeFrom = after + 1;
    }

    if (input.signal?.aborted) return;
    const iterator = await store.watch({
      key: tenantFilter(input.tenantId),
      ...(resumeFrom !== null ? { resumeFromRevision: resumeFrom } : {}),
    });
    const onAbort = (): void => iterator.stop();
    input.signal?.addEventListener("abort", onAbort, { once: true });
    if (input.signal?.aborted) onAbort();
    try {
      for await (const entry of iterator) {
        const key = decodeKvKey(entry.key);
        if (key === null) continue;
        if (input.prefix !== undefined && !key.startsWith(input.prefix)) continue;
        const revision = String(entry.revision);
        if (entry.operation === "PUT") {
          let value: T;
          try {
            value = decodeJson<T>(entry.value);
          } catch (error) {
            runtime.events.emit({ type: "handler_error", resource: config.id, kind: "ephemeral", error: asError(error).message });
            continue;
          }
          yield { type: "upsert", entry: toEntry(entry.key, value, entry.revision, entry.created, null), revision };
        } else {
          yield { type: entry.operation === "DEL" ? "delete" : "expire", key, revision };
        }
      }
    } finally {
      input.signal?.removeEventListener("abort", onAbort);
      iterator.stop();
    }
  }

  const watch: Ephemeral<T>["watch"] = (input = {}) => ({
    [Symbol.asyncIterator]: () =>
      watchIterator({
        tenantId: input.tenantId ?? DEFAULT_TENANT,
        prefix: input.prefix,
        after: input.after,
        signal: input.signal,
      }),
  });

  return {
    ready: () => declaration.ready(),
    upsert,
    touch,
    delete: deleteEntry,
    snapshot,
    watch,
  };
};
