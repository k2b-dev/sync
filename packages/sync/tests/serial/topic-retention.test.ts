import { afterAll, beforeAll, expect, test } from "bun:test";
import type { NatsConnection } from "@nats-io/nats-core";
import { createSync, StoreFullError, type Sync } from "../../index.ts";
import { connectToCluster, restartNode, uniqueName } from "../cluster.ts";
import { cleanupNamespaces, collect, testNamespace } from "../helpers.ts";

// Run in its own process, last in test:serial, to isolate every-broker
// restarts from the other suites' provisioning and worker lifetimes.
let nc: NatsConnection;
let sync: Sync;
const namespace = testNamespace();

beforeAll(async () => {
  nc = await connectToCluster({ name: "topic-retention-broker-restart" });
  sync = createSync({ connection: nc, namespace, application: "tests" });
});

afterAll(async () => {
  await sync.drain({ timeoutMs: 2_000 });
  await cleanupNamespaces(nc, [namespace]);
  await nc.close();
});

test("snapshot watermark pruning and uncovered topic events survive every broker restarting", async () => {
  const config = {
    id: uniqueName("snapshot-restart"),
    whenFull: "reject" as const,
    maxPayloadBytes: 1_024,
    retention: { maxBytes: 64 * 1024, maxMessages: 3 },
  };
  const topic = sync.topic<string>(config);
  const persistedSnapshot = await topic.publish({ data: "snapshot" });
  await topic.publish({ data: "edit-2" });
  await topic.publish({ data: "edit-3" });
  await expect(topic.publish({ data: "overflow" })).rejects.toBeInstanceOf(StoreFullError);
  await topic.prune({ through: persistedSnapshot.cursor });
  for (const node of [1, 2, 3] as const) await restartNode(node);
  const peerConnection = await connectToCluster({ name: "snapshot-recovery" });
  const peer = createSync({ connection: peerConnection, namespace, application: "tests" });
  try {
    const recovered = peer.topic<string>(config);
    await recovered.ready();
    expect(await recovered.prune({ through: persistedSnapshot.cursor })).toEqual({ purged: 0 });
    expect((await collect(recovered.replay({ after: persistedSnapshot.cursor }))).map(e => e.data)).toEqual(["edit-2", "edit-3"]);
    await recovered.publish({ data: "edit-4" });
    await expect(recovered.publish({ data: "overflow" })).rejects.toBeInstanceOf(StoreFullError);
    expect((await collect(recovered.replay({ after: persistedSnapshot.cursor }))).map(e => e.data)).toEqual(["edit-2", "edit-3", "edit-4"]);
  } finally {
    await peer.drain({ timeoutMs: 2_000 });
    await peerConnection.close();
  }
}, 30_000);
