import { JetStreamApiError } from "@nats-io/jetstream";
import { headers as natsHeaders } from "@nats-io/nats-core";
import { asError } from "./errors.ts";
import type { ProvisionContext } from "./resources.ts";

// ==========================
// Direct $KV publishes
// ==========================

// The KV client's put() exposes neither per-message TTL nor CAS headers, so
// these publish to the bucket's documented `$KV.<bucket>.<key>` subject.

// 10071/10164 = wrong last sequence; the numeric codes are the stable
// contract, the message text is only a fallback.
export const isCasConflict = (error: unknown): boolean =>
  (error instanceof JetStreamApiError && (error.code === 10071 || error.code === 10164)) ||
  /wrong last sequence/i.test(asError(error).message);

type KvPublishOptions = {
  /** Per-message TTL as a Go duration string (e.g. "5s", "never"). */
  ttl?: string;
};

const publish = async (
  ctx: ProvisionContext,
  bucket: string,
  key: string,
  bytes: Uint8Array,
  previousSeq: number | undefined,
  options: KvPublishOptions,
): Promise<number> => {
  const hdrs = natsHeaders();
  if (options.ttl !== undefined) hdrs.set("Nats-TTL", options.ttl);
  if (previousSeq !== undefined) hdrs.set("Nats-Expected-Last-Subject-Sequence", String(previousSeq));
  const ack = await ctx.js.publish(`$KV.${bucket}.${key}`, bytes, { headers: hdrs });
  return ack.seq;
};

/** Unconditional put; returns the new revision. */
export const kvPut = (
  ctx: ProvisionContext,
  bucket: string,
  key: string,
  bytes: Uint8Array,
  options: KvPublishOptions = {},
): Promise<number> => publish(ctx, bucket, key, bytes, undefined, options);

/** CAS put; returns the new revision, or null on a lost expected-sequence race. */
export const kvCasPut = async (
  ctx: ProvisionContext,
  bucket: string,
  key: string,
  bytes: Uint8Array,
  previousSeq: number,
  options: KvPublishOptions = {},
): Promise<number | null> => {
  try {
    return await publish(ctx, bucket, key, bytes, previousSeq, options);
  } catch (error) {
    if (isCasConflict(error)) return null;
    throw error;
  }
};
