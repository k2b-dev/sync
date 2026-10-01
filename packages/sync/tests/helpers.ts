import { jetstreamManager } from "@nats-io/jetstream";
import type { NatsConnection } from "@nats-io/nats-core";
import { uniqueName } from "./cluster.ts";

/** Unique per-test-run namespace so runs never collide on the persistent cluster. */
export const testNamespace = (): string => uniqueName("t");

/** Every Sync-managed stream (incl. KV_/OBJ_ backing streams) of the given namespaces. */
export const namespaceStreams = async (nc: NatsConnection, namespaces: string[]): Promise<string[]> => {
  const jsm = await jetstreamManager(nc);
  const wanted = new Set(namespaces);
  const names: string[] = [];
  for await (const info of jsm.streams.list()) {
    const ns = info.config.metadata?.["sync.namespace"];
    if (info.config.metadata?.["sync.managed"] === "true" && ns !== undefined && wanted.has(ns)) {
      names.push(info.config.name);
    }
  }
  return names;
};

/** Delete every Sync-managed stream of the given namespaces. */
export const cleanupNamespaces = async (nc: NatsConnection, namespaces: string[]): Promise<void> => {
  const jsm = await jetstreamManager(nc);
  for (const name of await namespaceStreams(nc, namespaces)) await jsm.streams.delete(name).catch(() => {});
};

export const collect = async <T>(iterable: AsyncIterable<T>, limit?: number): Promise<T[]> => {
  const items: T[] = [];
  for await (const item of iterable) {
    items.push(item);
    if (limit !== undefined && items.length >= limit) break;
  }
  return items;
};

export const waitFor = async (predicate: () => boolean | Promise<boolean>, timeoutMs = 10_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error("waitFor timed out");
    await Bun.sleep(25);
  }
};
