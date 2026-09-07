import { expect, test } from "bun:test";
import { jetstreamManager } from "@nats-io/jetstream";
import { TimeoutError } from "@nats-io/nats-core";
import type { NatsConnection } from "@nats-io/nats-core";
import { createSync, ResourceDriftError } from "../index.ts";
import type { Sync } from "../index.ts";
import { connectToCluster } from "./cluster.ts";
import { cleanupNamespaces, testNamespace } from "./helpers.ts";

const withExistingStream = async (
  run: (input: { nc: NatsConnection; peer: Sync; stream: string }) => Promise<void>,
): Promise<void> => {
  const namespace = testNamespace();
  const nc = await connectToCluster({ name: "stream-provisioning-regression" });
  const first = createSync({ connection: nc, namespace, application: "tests" });
  const peer = createSync({ connection: nc, namespace, application: "tests" });
  const request = nc.request;
  try {
    await first.queue({ id: "shared" }).send({ data: "original" });
    await peer.ready();
    const stream = (await first.resources()).find((entry) => entry.id === "shared")?.natsNames.find((name) => name.startsWith("S6_Q_"));
    if (stream === undefined) throw new Error("test queue has no work stream");
    await run({ nc, peer, stream });
  } finally {
    nc.request = request;
    await Promise.all([first.drain(), peer.drain()]);
    await cleanupNamespaces(nc, [namespace]);
    await nc.close();
  }
};

test("stream provisioning rechecks a timed-out lookup and preserves existing work", async () => {
  await withExistingStream(async ({ nc, peer, stream }) => {
    const request = nc.request.bind(nc);
    let reads = 0;
    let creates = 0;
    nc.request = async (...args) => {
      if (args[0] === `$JS.API.STREAM.INFO.${stream}` && ++reads === 1) throw new TimeoutError();
      if (args[0] === `$JS.API.STREAM.CREATE.${stream}`) creates += 1;
      return request(...args);
    };
    await peer.queue({ id: "shared" }).send({ data: "next" });
    expect(reads).toBe(2);
    expect(creates).toBe(0);
    const manager = await jetstreamManager(nc);
    expect((await manager.streams.info(stream)).state.messages).toBe(2);
  });
});

test("stream provisioning retries a lookup timeout only once and never assumes absence", async () => {
  await withExistingStream(async ({ nc, peer, stream }) => {
    const request = nc.request.bind(nc);
    const failure = new TimeoutError();
    let reads = 0;
    let creates = 0;
    nc.request = async (...args) => {
      if (args[0] === `$JS.API.STREAM.INFO.${stream}`) {
        reads += 1;
        throw failure;
      }
      if (args[0] === `$JS.API.STREAM.CREATE.${stream}`) creates += 1;
      return request(...args);
    };
    await expect(peer.queue({ id: "shared" }).send({ data: "next" })).rejects.toBe(failure);
    expect(reads).toBe(2);
    expect(creates).toBe(0);
  });
});

test("a recovered stream lookup still rejects declaration drift", async () => {
  await withExistingStream(async ({ nc, peer, stream }) => {
    const request = nc.request.bind(nc);
    let reads = 0;
    nc.request = async (...args) => {
      if (args[0] === `$JS.API.STREAM.INFO.${stream}` && ++reads === 1) throw new TimeoutError();
      return request(...args);
    };
    await expect(peer.queue({ id: "shared", retention: { maxAgeMs: 60_000, maxBytes: 1024 ** 3 } }).send({ data: "next" })).rejects.toBeInstanceOf(ResourceDriftError);
    expect(reads).toBe(2);
  });
});
