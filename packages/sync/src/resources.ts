import { StorageType } from "@nats-io/jetstream";
import type {
  ConsumerConfig,
  JetStreamClient,
  JetStreamManager,
  StreamConfig,
  StreamInfo,
} from "@nats-io/jetstream";
import { Kvm } from "@nats-io/kv";
import type { KV, KvOptions } from "@nats-io/kv";
import { TimeoutError } from "@nats-io/nats-core";
import { Objm } from "@nats-io/obj";
import type { ObjectStore as NatsObjectStore, ObjectStoreOptions } from "@nats-io/obj";
import { ResourceDriftError, ResourceIdentityCollisionError, SyncError, asError } from "./errors.ts";
import type { ResourceDifference } from "./errors.ts";
import type { ResourceIdentity } from "./naming.ts";
import { resourceMetadata } from "./naming.ts";
import { retry } from "./retry.ts";

// ==========================
// Provision context
// ==========================

export type ProvisionContext = {
  jsm: JetStreamManager;
  js: JetStreamClient;
  kvm: Kvm;
  objm: Objm;
};

export const toStorageType = (storage: "file" | "memory"): StorageType =>
  storage === "memory" ? StorageType.Memory : StorageType.File;

const isNotFound = (error: unknown): boolean => {
  const err = error as { code?: unknown; message?: string };
  // JetStreamApiError exposes the JetStream error code; 10059 = stream not
  // found, 10014 = consumer not found. Kvm/Objm surface the same errors but
  // sometimes only as messages — match those precisely, not any "not found".
  return (
    err?.code === 10059 ||
    err?.code === 10014 ||
    /stream not found|consumer not found|bucket not found|no message found/i.test(err?.message ?? "")
  );
};

// ==========================
// Drift comparison
// ==========================

/** Subject lists are set-like; every other array (e.g. backoff) is ordered. */
const SET_LIKE_FIELDS = new Set(["subjects", "filter_subjects"]);

const normalize = (field: string, value: unknown): unknown => {
  if (value === undefined || value === null) return null;
  if (Array.isArray(value) && SET_LIKE_FIELDS.has(field)) return [...value].sort();
  return value;
};

const compareFields = (declared: object, actual: object, fields: string[]): ResourceDifference[] => {
  // NATS config interfaces carry no index signature; widen once here instead
  // of casting at every call site.
  const declaredFields = declared as Record<string, unknown>;
  const actualFields = actual as Record<string, unknown>;
  const differences: ResourceDifference[] = [];
  for (const field of fields) {
    const d = normalize(field, declaredFields[field]);
    const a = normalize(field, actualFields[field]);
    if (JSON.stringify(d) !== JSON.stringify(a)) {
      differences.push({ field, declared: d, actual: a });
    }
  }
  return differences;
};

/**
 * Verify the sync.* identity metadata on an existing resource. A same-named
 * resource with a different full identity hash is a hash collision; a resource
 * without sync metadata is not managed by Sync and is reported as drift.
 */
const checkIdentityMetadata = (
  resource: string,
  identity: ResourceIdentity,
  actual: Record<string, string> | undefined,
): ResourceDifference[] => {
  const managed = actual?.["sync.managed"] === "true";
  if (!managed) {
    return [{ field: "metadata.sync.managed", declared: "true", actual: actual?.["sync.managed"] ?? null }];
  }
  const existingIdentity = actual?.["sync.identity_sha256"];
  if (existingIdentity !== identity.identitySha256) {
    throw new ResourceIdentityCollisionError(
      `${resource} exists with identity ${existingIdentity ?? "<none>"} but ${identity.identitySha256} was declared (namespace=${identity.namespace}, kind=${identity.kind}, id=${identity.id})`,
    );
  }
  const expected = resourceMetadata(identity, "");
  const differences: ResourceDifference[] = [];
  for (const field of ["sync.api", "sync.namespace", "sync.kind", "sync.id"] as const) {
    if (actual?.[field] !== expected[field]) {
      differences.push({ field: `metadata.${field}`, declared: expected[field], actual: actual?.[field] ?? null });
    }
  }
  return differences;
};

// ==========================
// Streams
// ==========================

const STREAM_DRIFT_FIELDS = [
  "subjects",
  "retention",
  "storage",
  "num_replicas",
  "discard",
  "max_age",
  "max_bytes",
  "max_msgs",
  "max_msg_size",
  "duplicate_window",
  "allow_msg_ttl",
  "allow_msg_schedules",
  "allow_rollup_hdrs",
  "max_msgs_per_subject",
];

/**
 * Create the stream if missing, otherwise verify the existing stream matches
 * the declaration. Never updates, purges, or adopts an incompatible stream.
 */
export const ensureStream = async (
  ctx: ProvisionContext,
  identity: ResourceIdentity,
  owner: string,
  config: Partial<StreamConfig> & { name: string; subjects: string[] },
): Promise<{ created: boolean; info: StreamInfo }> => {
  const declared = {
    ...config,
    metadata: { ...resourceMetadata(identity, owner), ...config.metadata },
  };

  let info: StreamInfo | null = null;
  let created = false;
  try {
    try {
      info = await ctx.jsm.streams.info(config.name);
    } catch (error) {
      if (!(error instanceof TimeoutError)) throw error;
      // A concurrent creator can assign the stream before its leader answers
      // INFO. Re-read once; a timeout never establishes that it is absent.
      info = await ctx.jsm.streams.info(config.name);
    }
  } catch (error) {
    if (!isNotFound(error)) throw asError(error);
  }
  if (info === null) {
    try {
      info = await ctx.jsm.streams.add(declared);
      created = true;
    } catch (error) {
      // Another process may have created it concurrently; fall through to verify.
      info = await ctx.jsm.streams.info(config.name).catch(() => null);
      if (info === null) throw asError(error);
    }
  }

  if (!created) {
    const differences = [
      ...checkIdentityMetadata(config.name, identity, info.config.metadata),
      ...compareFields(declared, info.config, STREAM_DRIFT_FIELDS.filter((f) => f in declared)),
    ];
    if (info.config.metadata?.["sync.owner"] !== owner) {
      differences.push({
        field: "metadata.sync.owner",
        declared: owner,
        actual: info.config.metadata?.["sync.owner"] ?? null,
      });
    }
    if (differences.length > 0) throw new ResourceDriftError(config.name, differences);
  }
  return { created, info };
};

// ==========================
// Consumers
// ==========================

const CONSUMER_DRIFT_FIELDS = [
  "filter_subject",
  "filter_subjects",
  "ack_policy",
  "ack_wait",
  "max_deliver",
  "max_ack_pending",
  "backoff",
  "deliver_policy",
  "opt_start_seq",
];

export const ensureConsumer = async (
  ctx: ProvisionContext,
  stream: string,
  config: Partial<ConsumerConfig> & { durable_name: string },
): Promise<{ created: boolean }> => {
  let info = null;
  let created = false;
  try {
    info = await ctx.jsm.consumers.info(stream, config.durable_name);
  } catch (error) {
    if (!isNotFound(error)) throw asError(error);
  }
  if (info === null) {
    try {
      info = await ctx.jsm.consumers.add(stream, config);
      created = true;
    } catch (error) {
      info = await ctx.jsm.consumers.info(stream, config.durable_name).catch(() => null);
      if (info === null) throw asError(error);
    }
  }
  if (!created) {
    const differences = compareFields(config, info.config, CONSUMER_DRIFT_FIELDS.filter((f) => f in config));
    if (differences.length > 0) {
      throw new ResourceDriftError(`${stream}/${config.durable_name}`, differences);
    }
  }
  return { created };
};

// ==========================
// KV buckets
// ==========================

export const ensureKv = async (
  ctx: ProvisionContext,
  identity: ResourceIdentity,
  owner: string,
  bucket: string,
  options: Partial<KvOptions>,
): Promise<KV> => {
  const declared: Partial<KvOptions> = {
    ...options,
    metadata: { ...resourceMetadata(identity, owner), ...options.metadata },
  };
  // Kvm.create opens an existing bucket without verifying its configuration,
  // so drift is always checked from the live status afterwards.
  const kv = await ctx.kvm.create(bucket, declared);
  const status = await kv.status();
  const existingMeta = status.streamInfo.config.metadata;
  const differences = [
    ...checkIdentityMetadata(`KV_${bucket}`, identity, existingMeta),
    ...compareFields(
      declared,
      {
        history: status.history,
        ttl: status.ttl,
        markerTTL: status.markerTTL,
        replicas: status.replicas,
        max_bytes: status.max_bytes,
        maxValueSize: status.maxValueSize <= 0 ? undefined : status.maxValueSize,
        storage: status.storage,
      },
      ["history", "ttl", "markerTTL", "replicas", "max_bytes", "maxValueSize", "storage"].filter((f) => f in declared),
    ),
  ];
  if (existingMeta?.["sync.owner"] !== owner) {
    differences.push({ field: "metadata.sync.owner", declared: owner, actual: existingMeta?.["sync.owner"] ?? null });
  }
  if (differences.length > 0) throw new ResourceDriftError(`KV_${bucket}`, differences);
  return kv;
};

// ==========================
// Object store buckets
// ==========================

export const ensureObjectStore = async (
  ctx: ProvisionContext,
  identity: ResourceIdentity,
  owner: string,
  bucket: string,
  options: Partial<ObjectStoreOptions>,
): Promise<NatsObjectStore> => {
  const declared: Partial<ObjectStoreOptions> = {
    ...options,
    metadata: { ...resourceMetadata(identity, owner), ...options.metadata },
  };
  const os = await ctx.objm.create(bucket, declared);
  const status = await os.status();
  const existingMeta = status.streamInfo.config.metadata;
  const differences = [
    ...checkIdentityMetadata(`OBJ_${bucket}`, identity, existingMeta),
    ...compareFields(
      declared,
      {
        ttl: status.ttl,
        replicas: status.replicas,
        storage: status.storage,
        max_bytes: status.streamInfo.config.max_bytes,
        compression: status.compression,
      },
      ["ttl", "replicas", "storage", "max_bytes", "compression"].filter((f) => f in declared),
    ),
  ];
  if (existingMeta?.["sync.owner"] !== owner) {
    differences.push({ field: "metadata.sync.owner", declared: owner, actual: existingMeta?.["sync.owner"] ?? null });
  }
  if (differences.length > 0) throw new ResourceDriftError(`OBJ_${bucket}`, differences);
  return os;
};

/** A clustered pause acknowledgement confirms the proposal, not its application. */
export const setConsumerPause = async (
  jsm: JetStreamManager,
  stream: string,
  consumer: string,
  until: Date,
  clustered: boolean,
): Promise<{ paused: boolean; pause_until?: string }> => {
  await jsm.consumers.pause(stream, consumer, until);
  const deadline = performance.now() + (jsm.getOptions().timeout ?? 0);
  const unconfirmed = new SyncError(`consumer ${stream}/${consumer}: requested pause state was not confirmed`);
  return retry({
    run: async ({ ctx }) => {
      if (ctx.attempt > 1 && performance.now() >= deadline) throw unconfirmed;
      const info = await jsm.consumers.info(stream, consumer);
      // Before a new clustered consumer elects a leader, INFO may expose only
      // its proposed assignment. Established leader INFO reads applied state.
      if ((clustered && !info.cluster?.leader) ||
        info.config.pause_until === undefined || Date.parse(info.config.pause_until) !== until.getTime()) {
        throw unconfirmed;
      }
      // Use server state: false is omitted, and a short pause may have expired.
      return { paused: info.paused === true, pause_until: info.config.pause_until };
    },
    after: ({ ctx }) => {
      if (ctx.error !== unconfirmed) return;
      const remaining = deadline - performance.now();
      if (remaining > 0) ctx.reschedule({ delayMs: Math.min(ctx.expBackoff(), remaining) });
    },
  });
};
