import type { JsonValue } from "./codec.ts";

// ==========================
// Shared delivery types
// ==========================

export type DeliveryConfig = {
  /** Time NATS waits for an ack before redelivering. Default 30_000. */
  ackWaitMs?: number;
  /** Total attempts including the first. Default 5. */
  maxAttempts?: number;
  /** Global unacknowledged-delivery ceiling per durable consumer (NATS MaxAckPending), shared by all pods. Default 1_000. */
  maxInFlight?: number;
  /** Retry delays; the last entry repeats. Default [1_000, 5_000, 30_000, 120_000]. */
  backoffMs?: number[];
};

// Part of exported signatures; required for declaration emit.
// fallow-ignore-next-line unused-type
export type ResolvedDelivery = Required<DeliveryConfig>;

export type RetentionConfig = {
  maxAgeMs: number;
  maxBytes: number;
  maxMessages?: number;
};

export type OrderingConfig = { mode: "none" } | { mode: "partitioned"; partitions: number };

export type PublishReceipt = {
  messageId: string;
  streamSequence: number;
  duplicate: boolean;
};

// ==========================
// Defaults and validation
// ==========================

const DEFAULT_DELIVERY: ResolvedDelivery = {
  ackWaitMs: 30_000,
  maxAttempts: 5,
  maxInFlight: 1_000,
  backoffMs: [1_000, 5_000, 30_000, 120_000],
};

export const DEFAULT_MESSAGE_PAYLOAD_BYTES = 128 * 1024;
/** Extra bytes a dead-letter envelope may add on top of the payload limit (failure metadata). */
export const DLQ_HEADROOM_BYTES = 4_096;
export const DEFAULT_EPHEMERAL_PAYLOAD_BYTES = 4 * 1024;
export const DEFAULT_DEDUPE_WINDOW_MS = 120_000;
export const DEFAULT_TENANT = "default";

const assertPositiveInt = (value: number, label: string): void => {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${label} must be a positive integer`);
  }
};

export const resolveDelivery = (config: DeliveryConfig = {}): ResolvedDelivery => {
  const delivery: ResolvedDelivery = {
    ackWaitMs: config.ackWaitMs ?? DEFAULT_DELIVERY.ackWaitMs,
    maxAttempts: config.maxAttempts ?? DEFAULT_DELIVERY.maxAttempts,
    maxInFlight: config.maxInFlight ?? DEFAULT_DELIVERY.maxInFlight,
    backoffMs: config.backoffMs ?? DEFAULT_DELIVERY.backoffMs,
  };
  assertPositiveInt(delivery.ackWaitMs, "delivery.ackWaitMs");
  assertPositiveInt(delivery.maxAttempts, "delivery.maxAttempts");
  assertPositiveInt(delivery.maxInFlight, "delivery.maxInFlight");
  if (!Array.isArray(delivery.backoffMs) || delivery.backoffMs.length === 0) {
    throw new RangeError("delivery.backoffMs must be a non-empty array");
  }
  for (const ms of delivery.backoffMs) assertPositiveInt(ms, "delivery.backoffMs entries");
  return delivery;
};

export const assertRetention = (retention: RetentionConfig): RetentionConfig => {
  assertPositiveInt(retention.maxAgeMs, "retention.maxAgeMs");
  assertPositiveInt(retention.maxBytes, "retention.maxBytes");
  if (retention.maxMessages !== undefined) assertPositiveInt(retention.maxMessages, "retention.maxMessages");
  return retention;
};

export const backoffDelayMs = (delivery: ResolvedDelivery, attempt: number): number => {
  const index = Math.min(Math.max(attempt - 1, 0), delivery.backoffMs.length - 1);
  return delivery.backoffMs[index]!;
};

export const nanos = (millis: number): number => millis * 1_000_000;
export const millis = (ns: number): number => Math.round(ns / 1_000_000);

// ==========================
// Common message fields
// ==========================

export type MessageMeta = Record<string, JsonValue>;
