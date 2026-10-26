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

/** Leader-backed read for ownership/admission decisions; retain tombstone revisions for CAS. */
export const kvReadAuthoritative = async (
  ctx: ProvisionContext,
  bucket: string,
  key: string,
): Promise<{ data: Uint8Array | null; revision: number }> => {
  try {
    // KV direct reads can be served by a lagging follower.
    const entry = await ctx.jsm.streams.getMessage(`KV_${bucket}`, {
      last_by_subj: `$KV.${bucket}.${key}`,
    });
    if (entry === null) return { data: null, revision: 0 };
    // Explicit delete/purge and server-written TTL expiry markers.
    const tombstone = entry.header.get("KV-Operation") !== "" ||
      entry.header.get("Nats-Marker-Reason") !== "" || entry.data.length === 0;
    return { data: tombstone ? null : entry.data, revision: entry.seq };
  } catch (error) {
    if (!(error instanceof JetStreamApiError) || error.code !== 10037) throw error;
    return { data: null, revision: 0 };
  }
};
