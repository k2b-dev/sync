import { afterAll, beforeAll, expect, test } from "bun:test";
import type { NatsConnection } from "@nats-io/nats-core";
import { createSync, RetentionGapError } from "../index.ts";
import { resourceIdentity, streamName } from "../src/naming.ts";
import { connectToCluster } from "./cluster.ts";
import { cleanupNamespaces, collect, testNamespace } from "./helpers.ts";

let nc: NatsConnection;
const namespace = testNamespace();
beforeAll(async () => { nc = await connectToCluster(); });
afterAll(async () => {
  await cleanupNamespaces(nc, [namespace]);
  await nc.close();
});

test("an idle tenant hub survives another tenant's safe prune before its first watchdog proof", async () => {
  const id = "idle-prune";
  const stream = streamName(resourceIdentity(namespace, "topic", id));
  const created = Promise.withResolvers<void>();
  const idleChecked = Promise.withResolvers<void>();
  let pruned = false;
  const connection = new Proxy(nc, {
    get(target, property) {
      if (property === "request") return async (...args: Parameters<NatsConnection["request"]>) => {
        const reply = await target.request(...args);
        if (args[0].includes(`.CONSUMER.CREATE.${stream}`)) created.resolve();
        if (pruned && args[0].includes(`.CONSUMER.INFO.${stream}.`)) {
          // Let the watchdog consume this reply before publishing the next event.
          setImmediate(() => idleChecked.resolve());
        }
        return reply;
      };
      return Reflect.get(target, property, target);
    },
  });
  const sync = createSync({ connection, namespace, application: "tests" });
  const topic = sync.topic<number>({ id, whenFull: "reject", retention: { maxBytes: 64 * 1024 } });
  await topic.publish({ tenantId: "a", data: 1 });
  const abort = new AbortController();
  const hub = topic.hub({ tenantId: "b" });
  const iterator = hub.subscribe({ signal: abort.signal })[Symbol.asyncIterator]();
  let pending = iterator.next();
  try {
    await Promise.race([created.promise, pending.then(() => { throw new Error("hub ended before consumer creation"); })]);
    const b2 = await topic.publish({ tenantId: "b", data: 2 });
    expect((await pending).value?.cursor).toBe(b2.cursor);
    pending = iterator.next();
    const a3 = await topic.publish({ tenantId: "a", data: 3 });
    await topic.publish({ tenantId: "a", data: 4 });
    expect(await topic.prune({ through: a3.cursor })).toEqual({ purged: 3 });
    pruned = true;
    await Promise.race([idleChecked.promise, pending.then(() => { throw new Error("idle hub ended after unrelated prune"); })]);
    const b5 = await topic.publish({ tenantId: "b", data: 5 });
    expect((await pending).value?.cursor).toBe(b5.cursor);
  } finally {
    abort.abort();
    await hub.close();
    await pending.catch(() => {});
    await iterator.return?.();
    await sync.drain({ timeoutMs: 5_000 });
  }
}, 15_000);

test("an unproven pruned own-tenant event still requires resync", async () => {
  const sync = createSync({ connection: nc, namespace, application: "tests" });
  const topic = sync.topic<number>({ id: "own-gap", whenFull: "reject", retention: { maxBytes: 64 * 1024 } });
  try {
    await topic.publish({ tenantId: "a", data: 1 });
    const b2 = await topic.publish({ tenantId: "b", data: 2 });
    const b3 = await topic.publish({ tenantId: "b", data: 3 });
    await topic.publish({ tenantId: "a", data: 4 });
    await topic.prune({ through: b3.cursor });
    await expect(collect(topic.follow({ tenantId: "b", after: b2.cursor }))).rejects.toBeInstanceOf(RetentionGapError);
  } finally {
    await sync.drain({ timeoutMs: 5_000 });
  }
});
