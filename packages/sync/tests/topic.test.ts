import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { jetstreamManager } from "@nats-io/jetstream";
import type { NatsConnection } from "@nats-io/nats-core";
import { createSync } from "../src/sync.ts";
import type { Sync } from "../src/sync.ts";
import { ConflictError, CursorMismatchError, ResourceDriftError, RetentionGapError } from "../src/errors.ts";
import { connectToCluster, uniqueName } from "./cluster.ts";
import { cleanupNamespaces, collect, testNamespace, waitFor } from "./helpers.ts";

type Event = { n: number };

let nc: NatsConnection;
let sync: Sync;
const namespace = testNamespace();

const topicConfig = (id: string) => ({
  id,
  retention: { maxAgeMs: 60 * 60 * 1_000, maxBytes: 64 * 1024 * 1024 },
});

beforeAll(async () => {
  nc = await connectToCluster({ name: "topic-test" });
  sync = createSync({ connection: nc, namespace, application: "tests" });
  await sync.ready();
});

afterAll(async () => {
  await sync.drain({ timeoutMs: 5_000 });
  await cleanupNamespaces(nc, [namespace]);
  await nc.close();
});

describe("topic publish", () => {
  test("returns receipt with cursor; idempotencyKey dedupes within the window", async () => {
    const topic = sync.topic<Event>(topicConfig("pub"));
    const first = await topic.publish({ data: { n: 1 }, idempotencyKey: "evt-1" });
    expect(first.duplicate).toBe(false);
    expect(first.cursor).toContain("s6t.");
    const dup = await topic.publish({ data: { n: 1 }, idempotencyKey: "evt-1" });
    expect(dup.duplicate).toBe(true);
    expect(dup.streamSequence).toBe(first.streamSequence);
    // Same key, different tenant: no dedupe across tenants.
    const other = await topic.publish({ data: { n: 1 }, tenantId: "acme", idempotencyKey: "evt-1" });
    expect(other.duplicate).toBe(false);
  });

  test("latestCursor is per tenant and null when empty", async () => {
    const topic = sync.topic<Event>(topicConfig("cursor"));
    expect(await topic.latestCursor()).toBeNull();
    const receipt = await topic.publish({ data: { n: 1 } });
    expect(await topic.latestCursor()).toBe(receipt.cursor);
    expect(await topic.latestCursor({ tenantId: "acme" })).toBeNull();
  });
});

describe("topic live", () => {
  test("broadcasts to every live listener without replay", async () => {
    const topic = sync.topic<Event>(topicConfig("live"));
    await topic.ready();
    await topic.publish({ data: { n: 0 } }); // before subscribe: must not be seen
    const controller = new AbortController();
    const a = collect(topic.live({ signal: controller.signal }), 2);
    const b = collect(topic.live({ signal: controller.signal }), 2);
    await Bun.sleep(250); // subscriptions in place
    await topic.publish({ data: { n: 1 } });
    await topic.publish({ data: { n: 2 } });
    const [eventsA, eventsB] = await Promise.all([a, b]);
    expect(eventsA.map((e) => e.data.n)).toEqual([1, 2]);
    expect(eventsB.map((e) => e.data.n)).toEqual([1, 2]);
    controller.abort();
  });
});

describe("topic replay and follow", () => {
  test("replays from cursor to captured head, tenant-scoped", async () => {
    const topic = sync.topic<Event>(topicConfig("replay"));
    const r1 = await topic.publish({ data: { n: 1 } });
    await topic.publish({ data: { n: 99 }, tenantId: "acme" });
    await topic.publish({ data: { n: 2 } });
    await topic.publish({ data: { n: 3 } });

    const all = await collect(topic.replay());
    expect(all.map((e) => e.data.n)).toEqual([1, 2, 3]);

    const afterFirst = await collect(topic.replay({ after: r1.cursor }));
    expect(afterFirst.map((e) => e.data.n)).toEqual([2, 3]);

    const acme = await collect(topic.replay({ tenantId: "acme" }));
    expect(acme.map((e) => e.data.n)).toEqual([99]);

    // Events published after replay started are not part of the captured head.
    await topic.publish({ data: { n: 4 } });
    const stillThree = await collect(topic.replay());
    expect(stillThree.map((e) => e.data.n)).toEqual([1, 2, 3, 4]);
  });

  test("independent cursors: two replays do not affect each other", async () => {
    const topic = sync.topic<Event>(topicConfig("cursors"));
    for (let n = 1; n <= 5; n++) await topic.publish({ data: { n } });
    const [a, b] = await Promise.all([collect(topic.replay()), collect(topic.replay())]);
    expect(a.map((e) => e.data.n)).toEqual([1, 2, 3, 4, 5]);
    expect(b.map((e) => e.data.n)).toEqual([1, 2, 3, 4, 5]);
  });

  test("follow keeps yielding new events after the head", async () => {
    const topic = sync.topic<Event>(topicConfig("follow"));
    await topic.publish({ data: { n: 1 } });
    const controller = new AbortController();
    const followed = collect(topic.follow({ signal: controller.signal }), 3);
    await Bun.sleep(250);
    await topic.publish({ data: { n: 2 } });
    await topic.publish({ data: { n: 3 } });
    expect((await followed).map((e) => e.data.n)).toEqual([1, 2, 3]);
    controller.abort();
  });

  test("cursor from another topic is rejected", async () => {
    const a = sync.topic<Event>(topicConfig("mismatch-a"));
    const b = sync.topic<Event>(topicConfig("mismatch-b"));
    const receipt = await a.publish({ data: { n: 1 } });
    await b.publish({ data: { n: 1 } });
    await expect(collect(b.replay({ after: receipt.cursor }))).rejects.toBeInstanceOf(CursorMismatchError);
  });

  test("retention gap fails explicitly instead of silently skipping", async () => {
    const topic = sync.topic<Event>(topicConfig("gap"));
    const first = await topic.publish({ data: { n: 1 } });
    await topic.publish({ data: { n: 2 } });
    const third = await topic.publish({ data: { n: 3 } });
    // Simulate retention removing the first two messages.
    const jsm = await jetstreamManager(nc);
    const streams: string[] = [];
    for await (const info of jsm.streams.list()) {
      if (info.config.metadata?.["sync.id"] === "gap" && !info.config.name.includes("D_")) {
        streams.push(info.config.name);
      }
    }
    expect(streams).toHaveLength(1);
    await jsm.streams.purge(streams[0]!, { seq: Number(third.streamSequence) });
    await expect(collect(topic.replay({ after: first.cursor }))).rejects.toBeInstanceOf(RetentionGapError);
  });
});

describe("topic durable process", () => {
  test("same consumer name competes; different names own independent cursors", async () => {
    const topic = sync.topic<Event>(topicConfig("groups"));
    const shared: number[] = [];
    const independent: number[] = [];
    const w1 = await topic.process({ consumer: "shared", concurrency: 2 }, async (event) => {
      shared.push(event.data.n);
    });
    const w2 = await topic.process({ consumer: "shared", concurrency: 2 }, async (event) => {
      shared.push(event.data.n);
    });
    const w3 = await topic.process({ consumer: "solo" }, async (event) => {
      independent.push(event.data.n);
    });
    for (let n = 1; n <= 10; n++) await topic.publish({ data: { n } });
    await waitFor(() => shared.length >= 10 && independent.length >= 10);
    await Bun.sleep(300);
    expect(shared.toSorted((x, y) => x - y)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    expect(independent.toSorted((x, y) => x - y)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    await Promise.all([w1.drain(), w2.drain(), w3.drain()]);
  });

  test("start latest skips history; start after cursor resumes mid-stream", async () => {
    const topic = sync.topic<Event>(topicConfig("start"));
    const r1 = await topic.publish({ data: { n: 1 } });
    await topic.publish({ data: { n: 2 } });

    const fromCursor: number[] = [];
    const w = await topic.process({ consumer: "resume", start: { after: r1.cursor } }, async (event) => {
      fromCursor.push(event.data.n);
    });
    await waitFor(() => fromCursor.includes(2));
    expect(fromCursor).toEqual([2]);
    await w.drain();
  });

  test("failed handler retries with backoff and dead-letters after maxAttempts", async () => {
    const topic = sync.topic<Event>(topicConfig("retry"));
    const attempts: number[] = [];
    const w = await topic.process(
      {
        consumer: "flaky",
        delivery: { maxAttempts: 3, backoffMs: [100], ackWaitMs: 5_000 },
      },
      async (event) => {
        attempts.push(event.attempt);
        throw new Error("boom");
      },
    );
    await topic.publish({ data: { n: 1 } });
    await waitFor(() => attempts.length >= 3, 15_000);
    await Bun.sleep(500);
    expect(attempts).toEqual([1, 2, 3]);
    await w.drain();

    // DLQ entry exists in the topic's DLQ stream.
    const jsm = await jetstreamManager(nc);
    let dlqMessages = 0;
    for await (const info of jsm.streams.list()) {
      if (info.config.metadata?.["sync.id"] === "retry" && info.config.name.includes("D_")) {
        dlqMessages = info.state.messages;
      }
    }
    expect(dlqMessages).toBe(1);
  });
});

describe("resource declarations", () => {
  test("drifted redeclaration fails clearly and never mutates the stream", async () => {
    const id = uniqueName("drift");
    const topic = sync.topic<Event>(topicConfig(id));
    await topic.ready();

    const second = createSync({ connection: nc, namespace, application: "tests-2" });
    const drifted = second.topic<Event>({ id, retention: { maxAgeMs: 1_000, maxBytes: 1_024 } });
    await expect(drifted.ready()).rejects.toBeInstanceOf(ResourceDriftError);
    expect(second.health().driftedResources).toBe(1);

    // Original declaration still verifies cleanly — nothing was mutated.
    // A different application opening a shared resource declares its owner.
    const third = createSync({ connection: nc, namespace, application: "tests-3" });
    await third.topic<Event>({ ...topicConfig(id), owner: "tests" }).ready();

    // Without the explicit owner the cross-application open is refused.
    const fourth = createSync({ connection: nc, namespace, application: "tests-4" });
    await expect(fourth.topic<Event>(topicConfig(id)).ready()).rejects.toBeInstanceOf(ResourceDriftError);
  });

  test("conflicting local declarations fail before any I/O", () => {
    const id = uniqueName("conflict");
    sync.topic<Event>(topicConfig(id));
    expect(() => sync.topic<Event>({ id, retention: { maxAgeMs: 1, maxBytes: 1 } })).toThrow(
      "already declared in this process",
    );
  });

  test("health and resources report the topic", async () => {
    const health = sync.health();
    expect(health.state).toBe("ready");
    expect(health.connection).toBe("connected");
    const resources = await sync.resources();
    const pub = resources.find((r) => r.id === "pub");
    expect(pub?.kind).toBe("topic");
    expect(pub?.state).toBe("ready");
    expect(pub?.detail?.messages).toBeGreaterThan(0);
  });
});

describe("hardening regressions", () => {
  test("follow on a never-written topic yields the first event ever published", async () => {
    const topic = sync.topic<Event>(topicConfig("empty-follow"));
    await topic.ready();
    const controller = new AbortController();
    const followed = collect(topic.follow({ signal: controller.signal }), 1);
    await Bun.sleep(300);
    await topic.publish({ data: { n: 1 } });
    expect((await followed).map((e) => e.data.n)).toEqual([1]);
    controller.abort();
    // replay on the still-almost-empty topic terminates normally too
    const replayed = await collect(sync.topic<Event>(topicConfig("empty-follow")).replay());
    expect(replayed.map((e) => e.data.n)).toEqual([1]);
  }, 20_000);

  test("RetentionGapError.resumeAfter resumes at the first retained event without losing it", async () => {
    const topic = sync.topic<Event>(topicConfig("gap-resume"));
    const first = await topic.publish({ data: { n: 1 } });
    await topic.publish({ data: { n: 2 } });
    const third = await topic.publish({ data: { n: 3 } });
    const jsm = await jetstreamManager(nc);
    for await (const info of jsm.streams.list()) {
      if (info.config.metadata?.["sync.id"] === "gap-resume" && !info.config.name.includes("D_")) {
        await jsm.streams.purge(info.config.name, { seq: Number(third.streamSequence) });
      }
    }
    let gap: RetentionGapError | null = null;
    try {
      await collect(topic.replay({ after: first.cursor }));
    } catch (error) {
      gap = error as RetentionGapError;
    }
    expect(gap).toBeInstanceOf(RetentionGapError);
    // The documented recovery includes the first retained event (n=3).
    const resumed = await collect(topic.replay({ after: gap!.resumeAfter }));
    expect(resumed.map((e) => e.data.n)).toEqual([3]);
  }, 20_000);

  test("process start.after below the retained window fails instead of skipping", async () => {
    const topic = sync.topic<Event>(topicConfig("proc-gap"));
    const first = await topic.publish({ data: { n: 1 } });
    await topic.publish({ data: { n: 2 } });
    const third = await topic.publish({ data: { n: 3 } });
    const jsm = await jetstreamManager(nc);
    for await (const info of jsm.streams.list()) {
      if (info.config.metadata?.["sync.id"] === "proc-gap" && !info.config.name.includes("D_")) {
        await jsm.streams.purge(info.config.name, { seq: Number(third.streamSequence) });
      }
    }
    await expect(
      topic.process({ consumer: "gapper", start: { after: first.cursor } }, async () => {}),
    ).rejects.toBeInstanceOf(RetentionGapError);
  }, 20_000);

  test("pre-aborted signals return immediately instead of hanging", async () => {
    const topic = sync.topic<Event>(topicConfig("pre-aborted"));
    await topic.publish({ data: { n: 1 } });
    const aborted = AbortSignal.abort();
    const live = await collect(topic.live({ signal: aborted }));
    expect(live).toEqual([]);
    const followed = await collect(topic.follow({ signal: aborted }));
    expect(followed).toEqual([]);
  }, 15_000);

  test("start latest skips history", async () => {
    const topic = sync.topic<Event>(topicConfig("latest-start"));
    await topic.publish({ data: { n: 1 } });
    await topic.publish({ data: { n: 2 } });
    const seen: number[] = [];
    const w = await topic.process({ consumer: "tail", start: "latest" }, async (event) => {
      seen.push(event.data.n);
    });
    await Bun.sleep(400);
    await topic.publish({ data: { n: 3 } });
    await waitFor(() => seen.length >= 1, 15_000);
    await Bun.sleep(200);
    expect(seen).toEqual([3]);
    await w.drain();
  }, 30_000);
});

describe("optimistic concurrency and batches", () => {
  test("expectedAfter guards per-tenant appends (event sourcing)", async () => {
    const topic = sync.topic<Event>(topicConfig("occ"));
    // First event: the tenant must be empty.
    const first = await topic.publish({ data: { n: 1 }, expectedAfter: null });
    await expect(topic.publish({ data: { n: 99 }, expectedAfter: null })).rejects.toBeInstanceOf(ConflictError);
    // Appends chained on the latest cursor succeed; stale cursors conflict.
    const second = await topic.publish({ data: { n: 2 }, expectedAfter: first.cursor });
    await expect(topic.publish({ data: { n: 99 }, expectedAfter: first.cursor })).rejects.toBeInstanceOf(ConflictError);
    // Other tenants are independent versions.
    await topic.publish({ data: { n: 1 }, tenantId: "acme", expectedAfter: null });
    const events = await collect(topic.replay());
    expect(events.map((e) => e.data.n)).toEqual([1, 2]);
    expect(second.cursor).toBe((await topic.latestCursor())!);
  });

  test("publishBatch appends atomically with optimistic concurrency", async () => {
    const topic = sync.topic<Event>(topicConfig("occ-batch"));
    const base = await topic.publish({ data: { n: 1 } });
    const batch = await topic.publishBatch({
      events: [{ data: { n: 2 } }, { data: { n: 3 } }, { data: { n: 4 } }],
      expectedAfter: base.cursor,
    });
    expect(batch.count).toBe(3);
    // The same expectation cannot commit twice — and nothing partial lands.
    await expect(
      topic.publishBatch({ events: [{ data: { n: 90 } }, { data: { n: 91 } }], expectedAfter: base.cursor }),
    ).rejects.toBeInstanceOf(ConflictError);
    const events = await collect(topic.replay());
    expect(events.map((e) => e.data.n)).toEqual([1, 2, 3, 4]);
  });

  test("named consumers can be paused and resumed", async () => {
    const topic = sync.topic<Event>(topicConfig("pause"));
    const seen: number[] = [];
    const w = await topic.process({ consumer: "pausee" }, async (event) => {
      seen.push(event.data.n);
    });
    expect((await topic.pauseConsumer({ consumer: "pausee" })).paused).toBe(true);
    await topic.publish({ data: { n: 1 } });
    await Bun.sleep(1_500);
    expect(seen).toEqual([]);
    await topic.resumeConsumer({ consumer: "pausee" });
    await waitFor(() => seen.length === 1, 15_000);
    await w.drain();
  }, 30_000);
});

describe("fanout hub", () => {
  test("many subscribers share one follower; catch-up splices into the live tail", async () => {
    const topic = sync.topic<Event>(topicConfig("hub"));
    const early = await topic.publish({ data: { n: 1 } });
    await topic.publish({ data: { n: 2 } });
    const hub = topic.hub();
    const controller = new AbortController();
    // A resumes from a cursor (catch-up + live), B is live-only.
    const a: number[] = [];
    const b: number[] = [];
    const runA = (async () => {
      for await (const e of hub.subscribe({ after: early.cursor, signal: controller.signal })) {
        a.push(e.data.n);
        if (a.length >= 3) break;
      }
    })();
    const runB = (async () => {
      for await (const e of hub.subscribe({ signal: controller.signal })) {
        b.push(e.data.n);
        if (b.length >= 2) break;
      }
    })();
    await Bun.sleep(400);
    await topic.publish({ data: { n: 3 } });
    await topic.publish({ data: { n: 4 } });
    await Promise.all([runA, runB]);
    expect(a).toEqual([2, 3, 4]); // no gap, no duplicate at the splice point
    expect(b).toEqual([3, 4]);
    hub.close();
  }, 30_000);

  test("a slow subscriber overflows explicitly with a resumable cursor", async () => {
    const topic = sync.topic<Event>(topicConfig("hub-slow"));
    const hub = topic.hub();
    const first = await topic.publish({ data: { n: 0 } });
    let error: RetentionGapError | null = null;
    const seen: number[] = [];
    const run = (async () => {
      try {
        for await (const e of hub.subscribe({ bufferLimit: 2 })) {
          seen.push(e.data.n);
          await Bun.sleep(500); // slower than the publisher
        }
      } catch (err) {
        error = err as RetentionGapError;
      }
    })();
    await Bun.sleep(300);
    for (let n = 1; n <= 10; n++) await topic.publish({ data: { n } });
    await run;
    expect(error).toBeInstanceOf(RetentionGapError);
    expect(error!.resumeAfter).toBeDefined(); // resubscribe point
    expect(seen.length).toBeGreaterThan(0);
    hub.close();
    void first;
  }, 30_000);
});

describe("run lifecycle events", () => {
  test("handler_started/handler_settled report status and duration", async () => {
    const events: Array<{ type: string; detail?: Record<string, unknown> }> = [];
    const observed = createSync({
      connection: nc,
      namespace,
      application: "tests",
      observe: (e) => {
        if (e.type === "handler_started" || e.type === "handler_settled") {
          events.push({ type: e.type, detail: e.detail as Record<string, unknown> });
        }
      },
    });
    const topic = observed.topic<Event>(topicConfig("lifecycle"));
    let failFirst = true;
    const w = await topic.process(
      { consumer: "obs", delivery: { maxAttempts: 2, backoffMs: [100], ackWaitMs: 5_000 } },
      async () => {
        if (failFirst) {
          failFirst = false;
          throw new Error("first try fails");
        }
      },
    );
    await topic.publish({ data: { n: 1 } });
    await waitFor(() => events.filter((e) => e.type === "handler_settled").length >= 2, 15_000);
    const settled = events.filter((e) => e.type === "handler_settled").map((e) => e.detail?.status);
    expect(settled).toEqual(["retry", "success"]);
    expect(events[0]!.type).toBe("handler_started");
    expect(typeof events.find((e) => e.type === "handler_settled")!.detail?.durationMs).toBe("number");
    await w.drain();
    await observed.drain({ timeoutMs: 1_000 });
  }, 30_000);
});

describe("sequences and cursor helpers", () => {
  test("events expose the stream sequence; cursorSequence/cursorAt round-trip and stay resource-bound", async () => {
    const topic = sync.topic<Event>(topicConfig("sequences"));
    const other = sync.topic<Event>(topicConfig("sequences-other"));
    const first = await topic.publish({ data: { n: 1 } });
    const second = await topic.publish({ data: { n: 2 } });
    const events = await collect(topic.replay());
    expect(events.map((e) => e.sequence)).toEqual([first.streamSequence, second.streamSequence]);
    expect(events[1]!.sequence).toBeGreaterThan(events[0]!.sequence);
    expect(topic.cursorSequence(second.cursor)).toBe(second.streamSequence);
    expect(topic.cursorAt(first.streamSequence)).toBe(first.cursor);
    // A persisted sequence resumes exactly like the cursor it came from.
    const resumed = await collect(topic.replay({ after: topic.cursorAt(first.streamSequence) }));
    expect(resumed.map((e) => e.data.n)).toEqual([2]);
    expect(() => other.cursorSequence(first.cursor)).toThrow(CursorMismatchError);
    expect(() => topic.cursorSequence("1699999999-0")).toThrow(CursorMismatchError);
    expect(() => topic.cursorAt(-1)).toThrow(RangeError);
  }, 20_000);

  test("hub() is memoized per tenant until closed", async () => {
    const topic = sync.topic<Event>(topicConfig("hub-memo"));
    const a = topic.hub();
    expect(topic.hub()).toBe(a);
    expect(topic.hub({ tenantId: "default" })).toBe(a);
    const b = topic.hub({ tenantId: "t2" });
    expect(b).not.toBe(a);
    expect(topic.hub({ tenantId: "t2" })).toBe(b);
    a.close();
    expect(topic.hub()).not.toBe(a);
    b.close();
    topic.hub().close();
  });

  test("a hub whose follower died is retired: subscribers fail loudly and hub() re-creates it", async () => {
    const topic = sync.topic<Event>(topicConfig("hub-dead"));
    const last = await topic.publish({ data: { n: 1 } });
    const dead = topic.hub();
    let error: Error | null = null;
    const run = (async () => {
      try {
        for await (const _ of dead.subscribe()) {
          // live-only; never expected to yield
        }
      } catch (err) {
        error = err as Error;
      }
    })();
    await Bun.sleep(500); // follower is tailing
    // Retention removes everything the follower is anchored to → RetentionGapError mid-follow.
    const jsm = await jetstreamManager(nc);
    for await (const info of jsm.streams.list()) {
      if (info.config.metadata?.["sync.id"] === "hub-dead" && !info.config.name.includes("D_")) {
        await jsm.streams.purge(info.config.name, { seq: Number(last.streamSequence) + 100 });
      }
    }
    await run;
    expect(error).toBeInstanceOf(RetentionGapError);
    await expect(collect(dead.subscribe(), 1)).rejects.toBeInstanceOf(RetentionGapError); // late subscriber, same dead hub
    const fresh = topic.hub();
    expect(fresh).not.toBe(dead);
    const received = collect(fresh.subscribe(), 1);
    await Bun.sleep(300);
    await topic.publish({ data: { n: 2 } });
    expect((await received).map((e) => e.data.n)).toEqual([2]);
    fresh.close();
  }, 30_000);
});
