import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { jetstreamManager } from "@nats-io/jetstream";
import type { NatsConnection, Subscription } from "@nats-io/nats-core";
import { createSync } from "../../src/sync.ts";
import type { Sync } from "../../src/sync.ts";
import { StaleDeliveryError } from "../../src/errors.ts";
import { resourceIdentity, streamName } from "../../src/naming.ts";
import { connectToCluster, startNode, stopNode, uniqueName, waitForLeaders } from "../cluster.ts";
import { cleanupNamespaces, namespaceStreams, testNamespace, waitFor } from "../helpers.ts";

let nc: NatsConnection;
let sync: Sync;
const namespace = testNamespace();
const fixture = new URL("../fixtures/queue-worker.ts", import.meta.url).pathname;

type Report = { type: "ready" | "start" | "end"; pid: number; n?: number };

/** Find the physical work-queue stream name for a queue id in this namespace. */
const findQueueStream = async (id: string): Promise<string> => {
  const { jetstreamManager } = await import("@nats-io/jetstream");
  const jsm = await jetstreamManager(nc);
  for await (const info of jsm.streams.list()) {
    const meta = info.config.metadata;
    if (meta?.["sync.kind"] === "queue" && meta["sync.namespace"] === namespace && meta["sync.id"] === id && !info.config.name.includes("D_")) {
      return info.config.name;
    }
  }
  throw new Error(`stream for queue ${id} not found`);
};

const spawnWorker = (env: Record<string, string>): ReturnType<typeof Bun.spawn> =>
  Bun.spawn(["bun", fixture], {
    env: { ...process.env, ...env },
    stdout: "ignore",
    stderr: "inherit",
  });

beforeAll(async () => {
  nc = await connectToCluster({ name: "fault-test" });
  sync = createSync({ connection: nc, namespace, application: "tests" });
  await sync.ready();
});

afterAll(async () => {
  await sync.drain({ timeoutMs: 2_000 });
  await cleanupNamespaces(nc, [namespace]);
  await nc.close();
});

describe("multi-process delivery", () => {
  const queueConfig = {
    id: "fleet",
    delivery: { maxInFlight: 4, ackWaitMs: 2_000, maxAttempts: 10, backoffMs: [200] },
  };

  const runFleet = async (input: {
    reportSubject: string;
    total: number;
    reports: Report[];
    killOneAfterFirstStart: boolean;
  }): Promise<void> => {
    const queue = sync.queue<{ n: number }>({ owner: "tests", ...queueConfig });
    await queue.ready();
    const env = {
      SYNC_NAMESPACE: namespace,
      SYNC_QUEUE_CONFIG: JSON.stringify(queueConfig),
      SYNC_REPORT_SUBJECT: input.reportSubject,
      SYNC_CONCURRENCY: "8",
      SYNC_HANDLER_MS: "300",
    };
    const workers = [spawnWorker(env), spawnWorker(env), spawnWorker(env)];
    try {
      await waitFor(() => input.reports.filter((r) => r.type === "ready").length === 3, 20_000);
      for (let n = 1; n <= input.total; n++) await queue.send({ data: { n } });
      if (input.killOneAfterFirstStart) {
        await waitFor(() => input.reports.some((r) => r.type === "start"), 15_000);
        workers[0]!.kill(9);
      }
      // Every message is eventually handled to completion (at-least-once).
      await waitFor(() => {
        const ended = new Set(input.reports.filter((r) => r.type === "end").map((r) => r.n));
        return ended.size === input.total;
      }, 60_000);
    } finally {
      for (const worker of workers) worker.kill(9);
    }
  };

  test("global maxInFlight caps concurrent handlers across three worker processes", async () => {
    const reportSubject = uniqueName("fault.cap");
    const reports: Report[] = [];
    const sub: Subscription = nc.subscribe(reportSubject, {
      callback: (_err, msg) => {
        reports.push(JSON.parse(msg.string()) as Report);
      },
    });
    try {
      await runFleet({ reportSubject, total: 24, reports, killOneAfterFirstStart: false });
      // Local concurrency was 8 per process; the durable consumer's global
      // maxInFlight of 4 must cap simultaneous handlers across the fleet.
      let active = 0;
      let maxActive = 0;
      for (const report of reports) {
        if (report.type === "start") {
          active += 1;
          maxActive = Math.max(maxActive, active);
        } else if (report.type === "end") {
          active -= 1;
        }
      }
      expect(maxActive).toBeLessThanOrEqual(4);
      expect(maxActive).toBeGreaterThan(1); // it did run in parallel
      // Three processes really shared the work.
      expect(new Set(reports.filter((r) => r.type === "end").map((r) => r.pid)).size).toBe(3);
    } finally {
      sub.unsubscribe();
    }
  }, 90_000);

  test("SIGKILL of one worker mid-fleet loses no accepted work", async () => {
    const reportSubject = uniqueName("fault.kill");
    const reports: Report[] = [];
    const sub: Subscription = nc.subscribe(reportSubject, {
      callback: (_err, msg) => {
        reports.push(JSON.parse(msg.string()) as Report);
      },
    });
    try {
      await runFleet({ reportSubject, total: 24, reports, killOneAfterFirstStart: true });
      const started = reports.filter((r) => r.type === "start").length;
      expect(started).toBeGreaterThanOrEqual(24); // duplicates allowed, loss never
    } finally {
      sub.unsubscribe();
    }
  }, 90_000);
});

describe("client restart", () => {
  test("accepted queue and job state survives a complete client outage", async () => {
    const first = await connectToCluster({ name: "restart-a" });
    const syncA = createSync({ connection: first, namespace, application: "tests" });
    const jobs = syncA.job<{ n: number }>({ owner: "tests", id: "outage" });
    for (let n = 1; n <= 5; n++) await jobs.submit({ key: `job-${n}`, input: { n } });
    await syncA.drain({ timeoutMs: 2_000 });
    await first.close(); // complete client outage — nothing running anywhere

    const second = await connectToCluster({ name: "restart-b" });
    const syncB = createSync({ connection: second, namespace, application: "tests" });
    const jobsB = syncB.job<{ n: number }>({ owner: "tests", id: "outage" });
    const done: number[] = [];
    const worker = await jobsB.process({ concurrency: 2 }, async (context) => {
      done.push(context.input.n);
    });
    await waitFor(() => done.length >= 5, 20_000);
    expect(done.toSorted((a, b) => a - b)).toEqual([1, 2, 3, 4, 5]);
    await worker.drain();
    await syncB.drain({ timeoutMs: 2_000 });
    await second.close();
  }, 45_000);
});

describe("stale settlement", () => {
  test("late ack after redelivery settles idempotently; ack without a consumer fails loudly", async () => {
    const queue = sync.queue<{ n: number }>({
      id: "stale",
      delivery: { ackWaitMs: 1_000, maxAttempts: 5, backoffMs: [100] },
    });
    await queue.send({ data: { n: 1 } });
    const reader = await queue.reader();
    const stale = await reader.receive({ waitMs: 5_000 });
    expect(stale).not.toBeNull();
    // Let the ack window lapse and the redelivery be settled by someone else.
    await Bun.sleep(1_500);
    const fresh = await reader.receive({ waitMs: 5_000 });
    expect(fresh).not.toBeNull();
    expect(fresh!.attempt).toBe(2);
    await fresh!.ack();
    // NATS accepts the late ack of the superseded delivery idempotently —
    // staleness of a token is not server-detectable. At-least-once handlers
    // must be idempotent; this is documented v6 semantics.
    await stale!.ack();
    expect(await reader.receive({ waitMs: 1_000 })).toBeNull(); // settled once

    // A hard ack failure (consumer deleted) is a loud StaleDeliveryError.
    await queue.send({ data: { n: 2 } });
    const doomed = await reader.receive({ waitMs: 5_000 });
    expect(doomed).not.toBeNull();
    const { jetstreamManager } = await import("@nats-io/jetstream");
    const jsm = await jetstreamManager(nc);
    for await (const consumer of jsm.consumers.list(await findQueueStream("stale"))) {
      await jsm.consumers.delete(await findQueueStream("stale"), consumer.name);
    }
    await expect(doomed!.ack()).rejects.toBeInstanceOf(StaleDeliveryError);
    await reader.close();
  }, 30_000);

  test("a slow handler past ackWait produces duplicate delivery, not loss", async () => {
    const queue = sync.queue<{ n: number }>({
      id: "dup",
      delivery: { ackWaitMs: 1_000, maxAttempts: 5, backoffMs: [100] },
    });
    let calls = 0;
    const worker = await queue.process({ concurrency: 2 }, async () => {
      calls += 1;
      if (calls === 1) await Bun.sleep(2_500); // exceeds ackWait without heartbeat
    });
    await queue.send({ data: { n: 1 } });
    await waitFor(() => calls >= 2, 15_000);
    expect(calls).toBeGreaterThanOrEqual(2);
    await worker.drain();
  }, 30_000);

  test("heartbeat keeps a long handler exclusive past ackWait", async () => {
    const queue = sync.queue<{ n: number }>({
      id: "beat",
      delivery: { ackWaitMs: 1_000, maxAttempts: 5, backoffMs: [100] },
    });
    let calls = 0;
    const worker = await queue.process({ concurrency: 2 }, async (message) => {
      calls += 1;
      for (let i = 0; i < 5; i++) {
        await Bun.sleep(500);
        await message.heartbeat();
      }
    });
    await queue.send({ data: { n: 1 } });
    await waitFor(() => calls >= 1, 15_000);
    await Bun.sleep(3_500);
    expect(calls).toBe(1); // never redelivered while heartbeating
    await worker.drain();
  }, 30_000);
});

describe("node loss", () => {
  test("R3 resources stay writable and readable while one NATS node is down", async () => {
    const conn = await connectToCluster({ name: "nodeloss" });
    const localSync = createSync({ connection: conn, namespace, application: "tests" });
    const topic = localSync.topic<{ n: number }>({
      owner: "tests",
      id: "resilient",
      retention: { maxAgeMs: 60 * 60 * 1_000, maxBytes: 32 * 1024 * 1024 },
    });
    await topic.publish({ data: { n: 1 } });

    await stopNode(3);
    try {
      // Failover is not instant: writes resume once new leaders are elected.
      await waitForLeaders(conn, [streamName(resourceIdentity(namespace, "topic", "resilient"))], 3);
      // Publishing and durable processing continue on the two remaining nodes.
      for (let n = 2; n <= 6; n++) await topic.publish({ data: { n } });
      const seen: number[] = [];
      const worker = await topic.process({ consumer: "survivor" }, async (event) => {
        seen.push(event.data.n);
      });
      await waitFor(() => seen.length >= 6, 30_000);
      expect(seen.toSorted((a, b) => a - b)).toEqual([1, 2, 3, 4, 5, 6]);
      await worker.drain();
    } finally {
      await startNode(3); // waits until R3 placement works again
    }
    await localSync.drain({ timeoutMs: 2_000 });
    await conn.close();
  }, 90_000);
});

describe("startup before JetStream answers", () => {
  test("a failed first ready() can be retried on the same instance once JetStream answers", async () => {
    // Node 1 stays up, so the connection itself never drops.
    const conn = await connectToCluster({ name: "late-jetstream", servers: ["nats://127.0.0.1:14222"] });
    const localSync = createSync({ connection: conn, namespace, application: "tests" });
    const queue = localSync.queue<{ n: number }>({ owner: "tests", id: "late-jetstream" });

    await Promise.all([stopNode(2), stopNode(3)]);
    try {
      // Without a meta quorum JetStream API requests fail on node 1.
      await waitFor(async () => {
        const jsm = await jetstreamManager(conn, { timeout: 1_000, checkAPI: false });
        return jsm.getAccountInfo().then(
          () => false,
          () => true,
        );
      }, 30_000);
      await expect(localSync.ready()).rejects.toThrow();
    } finally {
      await Promise.all([startNode(2), startNode(3)]); // wait until R3 placement works again
    }

    // Streams that lost their quorum elect leaders again; later tests and cleanup need them.
    await waitForLeaders(conn, await namespaceStreams(conn, [namespace]));
    await localSync.ready();
    await queue.send({ data: { n: 1 } });
    expect(localSync.health().state).toBe("ready");
    await localSync.drain({ timeoutMs: 2_000 });
    await conn.close();
  }, 120_000);
});
