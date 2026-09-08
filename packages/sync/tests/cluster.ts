/**
 * Helpers for tests against the persistent three-node NATS cluster from
 * compose.nats.yml. Client ports are pinned to 14222-14224 so the cluster
 * never collides with Cloud containers or a local NATS install.
 */
import { connect } from "@nats-io/transport-node";
import type { NatsConnection } from "@nats-io/nats-core";

const CLUSTER_SERVERS = [
  "nats://127.0.0.1:14222",
  "nats://127.0.0.1:14223",
  "nats://127.0.0.1:14224",
];

export const connectToCluster = async (
  options: { name?: string; servers?: string[] } = {},
): Promise<NatsConnection> => {
  return connect({
    servers: options.servers ?? process.env.SYNC_TEST_SERVERS?.split(",") ?? CLUSTER_SERVERS,
    name: options.name ?? "sync-v6-test",
    // The cluster advertises its internal Docker hostnames (nats-1:4222 ...)
    // which are unreachable from the host. Only the seed list is valid here.
    ignoreClusterUpdates: true,
    maxReconnectAttempts: -1,
    reconnectTimeWait: 250,
    timeout: 3_000,
  });
};

/**
 * Container health does not imply the node rejoined the JetStream meta group.
 * After any restart, wait until a fresh R3 stream can actually be placed so
 * later tests never hit "no suitable peers for placement".
 */
export const waitForPlacementReady = async (): Promise<void> => {
  const { jetstreamManager } = await import("@nats-io/jetstream");
  const nc = await connectToCluster({ name: "placement-probe" });
  try {
    const jsm = await jetstreamManager(nc);
    const name = uniqueName("PROBE_R3_READY");
    for (let i = 0; i < 240; i++) {
      try {
        await jsm.streams.add({ name, subjects: [name.toLowerCase()], num_replicas: 3 });
        await jsm.streams.delete(name);
        return;
      } catch {
        await Bun.sleep(500);
      }
    }
    throw new Error("cluster did not recover R3 placement in time");
  } finally {
    await nc.close();
  }
};

/** Restart one cluster node via Docker; resolves when R3 placement works again. */
export const restartNode = async (node: 1 | 2 | 3): Promise<void> => {
  const name = `sync_test_nats_${node}`;
  const restart = Bun.spawn(["docker", "restart", name], { stdout: "ignore", stderr: "pipe" });
  if ((await restart.exited) !== 0) {
    throw new Error(`docker restart ${name} failed: ${await new Response(restart.stderr).text()}`);
  }
  await waitForPlacementReady();
};

export const stopNode = async (node: 1 | 2 | 3): Promise<void> => {
  const proc = Bun.spawn(["docker", "stop", `sync_test_nats_${node}`], { stdout: "ignore", stderr: "ignore" });
  await proc.exited;
};

export const startNode = async (node: 1 | 2 | 3): Promise<void> => {
  const proc = Bun.spawn(["docker", "start", `sync_test_nats_${node}`], { stdout: "ignore", stderr: "ignore" });
  await proc.exited;
  await waitForPlacementReady();
};

export const uniqueName = (prefix: string): string =>
  `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
