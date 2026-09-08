import type { Consumer, JsMsg } from "@nats-io/jetstream";
import type { EventHub } from "./events.ts";
import { StaleDeliveryError, asError } from "./errors.ts";
import type { WorkerRuntime } from "./worker.ts";

// ==========================
// Shared pull loop
// ==========================

/**
 * Pulls messages for exactly the currently free local handler slots. Slots are
 * reserved before each fetch so several loops sharing one worker runtime
 * (partitions, schedules) can never oversubscribe `concurrency`, and unclaimed
 * work stays on the server for other pods.
 */
export const runPullLoop = async (
  wr: WorkerRuntime,
  getConsumer: () => Promise<Consumer>,
  onMessage: (msg: JsMsg, signal: AbortSignal) => Promise<void>,
  options: {
    events: EventHub;
    resource: string;
    shouldStop?: () => boolean;
    /**
     * Upper bound per fetch. Loops over MaxAckPending=1 consumers (partitions,
     * schedules) use 1 so one loop can never hog the shared slot pool while
     * long-polling its own subject.
     */
    maxBatch?: number;
    /**
     * Fetch long-poll duration. Contending 1-slot loops use a short poll so a
     * reserved slot rotates to other partitions quickly when idle. Minimum
     * 1000 (NATS). Default 5000.
     */
    pollExpiresMs?: number;
  },
): Promise<void> => {
  // One stop hook for the loop's lifetime; re-pointed at the current batch.
  let currentBatch: { stop(): void } | null = null;
  wr.stopped.then(() => currentBatch?.stop());

  while (!wr.stopping && !(options.shouldStop?.() ?? false)) {
    const granted = wr.reserve(Math.min(wr.freeSlots(), options.maxBatch ?? Number.POSITIVE_INFINITY));
    if (granted <= 0) {
      await wr.waitForSlot();
      continue;
    }
    let started = 0;
    try {
      const consumer = await getConsumer();
      const batch = await consumer.fetch({ max_messages: granted, expires: Math.max(options.pollExpiresMs ?? 5_000, 1_000) });
      currentBatch = batch;
      // stop() may have fired while the fetch call was in flight — the
      // one-shot stop hook saw currentBatch === null then.
      if (wr.stopping) batch.stop();
      for await (const msg of batch) {
        if (wr.stopping) break; // unhandled deliveries redeliver after ackWait
        started += 1;
        wr.track(
          (signal) =>
            onMessage(msg, signal).catch((error) => {
              // Settlement code itself failed (e.g. claim store unavailable):
              // surface it and hand the delivery back with a short backoff
              // instead of holding it silently until ackWait or redelivering
              // in a hot loop. nak() is a no-op after an ack.
              options.events.emit({
                type: "handler_error",
                resource: options.resource,
                error: `delivery ${msg.seq}: ${asError(error).message}`,
              });
              msg.nak(1_000);
            }),
          { fromReservation: true },
        );
      }
    } catch (error) {
      if (wr.stopping) return;
      options.events.emit({
        type: "handler_error",
        resource: options.resource,
        error: `pull loop: ${asError(error).message}`,
      });
      // Transient transport/consumer failure: back off briefly and retry.
      await Promise.race([Bun.sleep(1_000), wr.stopped]);
    } finally {
      currentBatch = null;
      wr.releaseReserved(granted - started);
    }
    if (started === 0) {
      // Fairness: an empty poll must not synchronously re-grab the freed slot
      // before contending loops (other partitions/schedules) get to run.
      await Promise.race([Bun.sleep(10 + Math.floor(Math.random() * 40)), wr.stopped]);
    }
  }
};

// ==========================
// Confirmed settlement helpers
// ==========================

/** Confirmed ack; throws StaleDeliveryError when NATS no longer accepts the token. */
export const confirmedAck = async (msg: JsMsg, resource: string): Promise<void> => {
  try {
    const accepted = await msg.ackAck();
    if (!accepted) throw new StaleDeliveryError(`${resource}: delivery ${msg.seq} was already settled or expired`);
  } catch (error) {
    if (error instanceof StaleDeliveryError) throw error;
    throw new StaleDeliveryError(`${resource}: ack for delivery ${msg.seq} failed: ${asError(error).message}`);
  }
};

/**
 * Settle a successful handler run. An ack that cannot be confirmed means the
 * delivery was superseded (redelivered elsewhere or the consumer changed) —
 * that is at-least-once noise, never a handler failure: it must not trigger
 * retry policies or dead-lettering for work that completed.
 */
export const settleSuccess = async (
  msg: JsMsg,
  resource: string,
  events: EventHub,
  kind: string,
): Promise<void> => {
  try {
    await confirmedAck(msg, resource);
  } catch (error) {
    events.emit({
      type: "redelivery",
      resource,
      kind,
      detail: { stale: true },
      error: asError(error).message,
    });
  }
};

// ==========================
// Per-run lifecycle events
// ==========================

export type SettleStatus = "success" | "retry" | "dead_letter";

/** Bracket a handler run with started/settled events (status + durationMs). */
export const emitRun = (
  events: EventHub,
  resource: string,
  kind: string,
  detail: { id: string; key?: string; attempt: number },
): ((status: SettleStatus) => void) => {
  events.emit({ type: "handler_started", resource, kind, detail: { ...detail } });
  const startedAt = Date.now();
  return (status) => {
    events.emit({
      type: "handler_settled",
      resource,
      kind,
      detail: { ...detail, status, durationMs: Date.now() - startedAt },
    });
  };
};
