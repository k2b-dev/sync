import { afterAll, beforeAll, expect, test } from "bun:test";
import { AckPolicy, DeliverPolicy, JetStreamApiError, ReplayPolicy, jetstreamManager } from "@nats-io/jetstream";
import type { ConsumerInfo } from "@nats-io/jetstream";
import type { NatsConnection } from "@nats-io/nats-core";
import { setConsumerPause } from "../src/resources.ts";
import { connectToCluster } from "./cluster.ts";

let nc: NatsConnection;
beforeAll(async () => { nc = await connectToCluster({ name: "consumer-pause-confirmation" }); });
afterAll(async () => { await nc.close(); });

const state = (until: Date, paused: boolean, leader = true): ConsumerInfo => ({
  stream_name: "stream",
  name: "consumer",
  created: new Date(0).toISOString(),
  config: {
    ack_policy: AckPolicy.Explicit,
    deliver_policy: DeliverPolicy.All,
    replay_policy: ReplayPolicy.Instant,
    pause_until: until.toISOString(),
  },
  delivered: { consumer_seq: 0, stream_seq: 0 },
  ack_floor: { consumer_seq: 0, stream_seq: 0 },
  num_ack_pending: 0,
  num_redelivered: 0,
  num_waiting: 0,
  num_pending: 0,
  push_bound: false,
  pause_remaining: 0,
  ...(leader ? { cluster: { name: "test", leader: "node" } } : {}),
  // The real API omits false rather than serializing it.
  ...(paused ? { paused: true } : {}),
});

for (const paused of [true, false]) {
  test(`${paused ? "pause" : "resume"} waits for the requested state on the consumer leader`, async () => {
    const jsm = await jetstreamManager(nc);
    const until = new Date(paused ? Date.now() + 60_000 : 0);
    const previous = new Date(paused ? 0 : Date.now() + 120_000);
    let commands = 0;
    let reads = 0;
    jsm.consumers.pause = async (_stream, _consumer, requested) => {
      commands += 1;
      expect(requested?.getTime()).toBe(until.getTime());
      return { paused, pause_until: until.toISOString() };
    };
    jsm.consumers.info = async () => {
      reads += 1;
      if (reads === 1) return state(previous, !paused);
      if (reads === 2) return state(until, paused, false); // Proposed assignment, no leader yet.
      return state(until, paused);
    };
    expect(await setConsumerPause(jsm, "stream", "consumer", until, true))
      .toEqual({ paused, pause_until: until.toISOString() });
    expect(commands).toBe(1);
    expect(reads).toBe(3);
  });
}

test("an expired pause returns current server state instead of the proposal acknowledgement", async () => {
  const jsm = await jetstreamManager(nc);
  const until = new Date(Date.now() - 1);
  jsm.consumers.pause = async () => ({ paused: true, pause_until: until.toISOString() });
  jsm.consumers.info = async () => state(until, false);
  expect(await setConsumerPause(jsm, "stream", "consumer", until, true)).toEqual({
    paused: false, pause_until: until.toISOString(),
  });
});

test("standalone consumers confirm their applied state without cluster metadata", async () => {
  const jsm = await jetstreamManager(nc);
  const until = new Date(0);
  jsm.consumers.pause = async () => ({ paused: false });
  jsm.consumers.info = async () => ({
    ...state(until, false, false),
    config: { ...state(until, false, false).config, pause_until: "1970-01-01T00:00:00Z" },
  });
  expect(await setConsumerPause(jsm, "stream", "consumer", until, false)).toEqual({
    paused: false, pause_until: "1970-01-01T00:00:00Z",
  });
});

test("a superseded pause fails within its confirmation budget without overwriting the newer intent", async () => {
  const jsm = await jetstreamManager(nc);
  jsm.getOptions = () => ({ timeout: 100 });
  const until = new Date(Date.now() + 60_000);
  let commands = 0;
  let reads = 0;
  jsm.consumers.pause = async () => {
    commands += 1;
    return { paused: true, pause_until: until.toISOString() };
  };
  jsm.consumers.info = async () => {
    reads += 1;
    return state(new Date(0), false);
  };
  await expect(setConsumerPause(jsm, "stream", "consumer", until, true))
    .rejects.toThrow("requested pause state was not confirmed");
  expect(commands).toBe(1);
  expect(reads).toBeGreaterThan(0);
});

test("missing consumer failures remain unchanged", async () => {
  const jsm = await jetstreamManager(nc);
  const missing = new JetStreamApiError({ code: 404, err_code: 10014, description: "consumer not found" });
  let reads = 0;
  jsm.consumers.pause = async () => { throw missing; };
  jsm.consumers.info = async () => { reads += 1; throw missing; };
  await expect(setConsumerPause(jsm, "stream", "consumer", new Date(0), true)).rejects.toBe(missing);
  expect(reads).toBe(0);
});
