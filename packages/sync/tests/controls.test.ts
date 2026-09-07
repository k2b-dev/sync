import { afterAll, beforeAll, expect, test } from "bun:test";
import { jetstreamManager } from "@nats-io/jetstream";
import type { NatsConnection } from "@nats-io/nats-core";
import { createSync, type Sync } from "../src/sync.ts";
import { connectToCluster } from "./cluster.ts";
import { cleanupNamespaces, testNamespace, waitFor } from "./helpers.ts";

let nc: NatsConnection;
const namespaces: string[] = [];
const instances: Sync[] = [];
const instance = (namespace = testNamespace()) => {
  namespaces.push(namespace);
  const sync = createSync({ connection: nc, namespace, application: "controls-test" });
  instances.push(sync);
  return sync;
};
beforeAll(async () => { nc = await connectToCluster({ name: "controls-test" }); });
afterAll(async () => {
  await Promise.all(instances.map(sync => sync.drain({ timeoutMs: 2_000 })));
  await cleanupNamespaces(nc, namespaces);
  await nc.close();
});

test("controls discover declarations without provisioning or conflating kinds", async () => {
  const sync = instance();
  expect(sync.controls()).toEqual([]);
  sync.queue({ id: "same" });
  sync.job({ id: "same" });
  sync.queue({ id: "same" });
  sync.mutex({ id: "not-administered" });
  const controls = sync.controls();
  expect(controls.map(({ id, kind, owner }) => ({ id, kind, owner }))).toEqual([
    { id: "same", kind: "queue", owner: "controls-test" },
    { id: "same", kind: "job", owner: "controls-test" },
  ]);
  const jsm = await jetstreamManager(nc);
  const existing: string[] = [];
  for await (const info of jsm.streams.list()) {
    if (info.config.metadata?.["sync.namespace"] === controls[0]!.namespace) existing.push(info.config.name);
  }
  expect(existing).toEqual([]);
  expect(() => sync.queue({ id: "same", owner: "other" })).toThrow();
  expect(sync.controls()).toEqual(controls);
});

test("controls administer the existing tenant-preserving DLQ independently for same-id queue and job", async () => {
  const sync = instance();
  const queue = sync.queue<{ value: string }>({ id: "deliveries", delivery: { maxAttempts: 1 } });
  const job = sync.job<{ value: string }>({ id: "deliveries", delivery: { maxAttempts: 1 } });
  await sync.ready();
  let failQueue = true;
  const delivered: string[] = [];
  await queue.process({}, async message => {
    if (failQueue) throw new Error("queue failure");
    delivered.push(`${message.tenantId}:${message.data.value}`);
  });
  await job.process({}, async () => { throw new Error("job failure"); });
  await queue.send({ tenantId: "tenant-a", data: { value: "queue" } });
  await job.submit({ tenantId: "tenant-b", key: "one", input: { value: "job" } });
  const queueControl = sync.controls().find(control => control.kind === "queue");
  const jobControl = sync.controls().find(control => control.kind === "job");
  if (!queueControl || queueControl.kind === "scheduler" || !jobControl || jobControl.kind === "scheduler") throw new Error("missing controls");
  await waitFor(async () => (await queueControl.deadLetters.list()).length === 1 && (await jobControl.deadLetters.list()).length === 1);
  const [queued] = await queueControl.deadLetters.list();
  const [jobbed] = await jobControl.deadLetters.list();
  expect(queued!.tenantId).toBe("tenant-a");
  expect(jobbed!.tenantId).toBe("tenant-b");
  failQueue = false;
  await queueControl.deadLetters.requeue({ messageId: queued!.messageId, idempotencyKey: "retry" });
  await waitFor(() => delivered.length === 1);
  expect(delivered).toEqual(["tenant-a:queue"]);
  expect(await queueControl.deadLetters.list()).toEqual([]);
  expect((await jobControl.deadLetters.list()).length).toBe(1);
  expect(await jobControl.deadLetters.delete({ messageId: jobbed!.messageId })).toBe(true);
  expect(await job.deadLetters.list()).toEqual([]);
});

test("scheduler controls aggregate actual local handlers and retain durable definitions and run state", async () => {
  const namespace = testNamespace();
  const remoteSync = instance(namespace);
  const remote = remoteSync.scheduler({ id: "schedules" });
  await remote.create({ id: "remote", cron: "0 0 1 1 *", process: async () => {} });
  const sync = instance(namespace);
  sync.scheduler({ id: "schedules" }); // A handler-less inspection handle must not hide the real worker.
  const scheduler = sync.scheduler({ id: "schedules" });
  await scheduler.create({ id: "local", cron: "0 0 1 1 *", process: async () => {} });
  const worker = await scheduler.process();
  const [control] = sync.controls();
  if (control?.kind !== "scheduler") throw new Error("missing scheduler control");
  expect(sync.controls().length).toBe(1);
  const before = await control.scheduler.list();
  expect(before.map(info => [info.id, info.handlerAvailable])).toEqual([["local", true], ["remote", false]]);
  const run = await control.scheduler.runNow({ id: "local", requestId: "manual" });
  const result = await control.scheduler.awaitRun({ id: "local", runId: run.runId, timeoutMs: 5_000 });
  expect(result.completed).toBe(true);
  const after = await control.scheduler.list();
  expect(after[0]!.createdAt).toEqual(before[0]!.createdAt);
  expect(after[0]!.lastRunId).toBe(run.runId);
  expect(after[0]!.lastCompletedAt).toBeInstanceOf(Date);
  await worker.drain();
  expect(sync.controls().length).toBe(1);
  expect((await control.scheduler.list())[0]!.handlerAvailable).toBe(true);
});
