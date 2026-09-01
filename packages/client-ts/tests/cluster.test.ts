/**
 * A cluster over fake servers (every routing and failure path, deterministic)
 * and over real ones (the same thing end to end, with a server killed under
 * the client).
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import {
  Client,
  ClosedError,
  type ClusterNode,
  type ClusterTransport,
  cluster,
  DEFAULT_FAILOVER,
  op,
  Status,
  StatusError,
  type Transport,
} from "../src/index";
import { http } from "../src/transport/http";
import { tcp } from "../src/transport/tcp";
import {
  encodeRawFrame,
  encodeReplies,
  HEADER_LEN,
  Op,
  Reader,
  type Reply,
} from "../src/wire";
import { must } from "./must";
import { freePort, startServer, type TestServer } from "./server";

const EMPTY = new Uint8Array(0);
const utf8 = new TextDecoder();
const AUTH = /AUTH/;
const REPLIES = /replies/;
const AT_LEAST_ONE = /at least one/;
const DUPLICATE = /duplicate/;
const REFUSED = /refused/;
const AT_LEAST_ONE_FAILURE = /failover.failures/;
const NOT_NEGATIVE = /failover.retryMs/;

/**
 * A server in a Map: it answers frames exactly as the real one does, so a
 * `Client` over a cluster of these exercises every step but the socket.
 */
class Fake implements Transport {
  readonly store = new Map<string, Uint8Array>();
  /** Requests answered, so a test can see which server a call reached. */
  seen = 0;
  /** While set, every request fails with it. */
  fail: Error | null = null;
  /** Answer a batch with one reply too few: a server that miscounts. */
  miscount: boolean = false;
  isOpen = true;

  close(): void {
    this.isOpen = false;
  }

  async ping(): Promise<void> {
    await this.request(encodeRawFrame(Op.Ping, EMPTY));
  }

  request(frame: Uint8Array): Promise<Reply> {
    if (this.fail !== null) return Promise.reject(this.fail);
    this.seen++;
    const op = frame[0] as Op;
    const body = frame.subarray(HEADER_LEN);
    if (op === Op.Batch) {
      const r = new Reader(body);
      const n = r.u32();
      const replies: Reply[] = [];
      for (let i = 0; i < n; i++) replies.push(this.item(r.u8(), r.blob()));
      r.finish();
      if (this.miscount) replies.pop();
      return Promise.resolve({
        status: Status.Ok,
        body: encodeReplies(replies),
      });
    }
    if (op === Op.Ping)
      return Promise.resolve({ status: Status.Ok, body: EMPTY });
    return Promise.resolve(this.item(op, body));
  }

  /** One GET, SET or DEL, answered the way the protocol says. */
  private item(op: number, body: Uint8Array): Reply {
    if (op === Op.Set) {
      const r = new Reader(body);
      const key = utf8.decode(r.blob());
      this.store.set(key, body.subarray(r.pos));
      return { status: Status.Ok, body: EMPTY };
    }
    const key = utf8.decode(body);
    const value = this.store.get(key);
    if (op === Op.Del) {
      return {
        status: this.store.delete(key) ? Status.Ok : Status.NotFound,
        body: EMPTY,
      };
    }
    return value === undefined
      ? { status: Status.NotFound, body: EMPTY }
      : { status: Status.Ok, body: value };
  }
}

/** `n` fake servers under the names a cluster knows them by. */
function fakes(n: number): { fakes: Fake[]; nodes: ClusterNode[] } {
  const made = Array.from({ length: n }, () => new Fake());
  return {
    fakes: made,
    nodes: made.map((f, i) => ({ name: `10.0.0.${i}:4433`, open: () => f })),
  };
}

/** A cluster that drops a server after one failure. */
const quick = { failures: 1, retryMs: 50 };

/** Opening a cluster pings every server; forget those, so a test that counts
 * requests counts only the ones it sent itself. */
function counted(servers: Fake[]): void {
  for (const s of servers) s.seen = 0;
}

describe("routing", () => {
  test("every key goes to the server the ring names, and only there", async () => {
    const { fakes: servers, nodes } = fakes(3);
    const t = await cluster({ nodes });
    const c = await Client.connect(t);
    const keys = Array.from({ length: 200 }, (_, i) => `user:${i}`);
    await Promise.all(keys.map((k, i) => c.set(k, i)));
    for (const key of keys) {
      const owner = t.nodeFor(key);
      const holders = nodes
        .map((n, i) => (must(servers[i]).store.has(key) ? n.name : null))
        .filter((n) => n !== null);
      expect(holders).toEqual([owner]);
    }
    expect(await c.get<number>("user:7")).toBe(7);
    expect(servers.every((s) => s.store.size > 0)).toBe(true);
    expect(t.live()).toEqual(nodes.map((n) => n.name));
    expect(t.isOpen).toBe(true);
  });

  test("one server takes everything, ring or no ring", async () => {
    const { fakes: servers, nodes } = fakes(1);
    const t = await cluster({ nodes });
    const c = await Client.connect(t);
    await c.set("a", 1);
    await c.set("b", 2);
    expect(t.nodeFor("anything")).toBe("10.0.0.0:4433");
    expect(must(servers[0]).store.size).toBe(2);
    expect(await c.del("a")).toBe(true);
    expect(await c.del("a")).toBe(false);
    await c.ping();
  });

  test("keys are bytes, not only strings", async () => {
    const { nodes } = fakes(3);
    const t = await cluster({ nodes });
    const c = await Client.connect(t);
    const key = new TextEncoder().encode("user:7");
    await c.set(key, 7);
    expect(await c.get<number>(key)).toBe(7);
    expect(t.nodeFor(key)).toBe(t.nodeFor("user:7"));
  });

  test("AUTH is not a cluster request", async () => {
    const { nodes } = fakes(2);
    const t = await cluster({ nodes });
    expect(t.request(encodeRawFrame(Op.Auth, EMPTY))).rejects.toThrow(AUTH);
  });
});

describe("failover", () => {
  test("a server that stops answering is dropped and its keys move", async () => {
    const { fakes: servers, nodes } = fakes(3);
    const t = await cluster({ nodes, failover: quick });
    const c = await Client.connect(t);
    const key = Array.from({ length: 100 }, (_, i) => `key:${i}`).find(
      (k) => t.nodeFor(k) === "10.0.0.0:4433",
    );
    await c.set(must(key), 1);
    must(servers[0]).fail = new ClosedError();

    expect(c.get(must(key))).rejects.toBeInstanceOf(ClosedError);
    await Bun.sleep(0);
    expect(t.live()).toEqual(["10.0.0.1:4433", "10.0.0.2:4433"]);
    expect(t.nodeFor(must(key))).not.toBe("10.0.0.0:4433");
    // Cold on the server that took it over: nothing is copied across.
    expect(await c.get(must(key))).toBeNull();
    await c.set(must(key), 2);
    expect(await c.get<number>(must(key))).toBe(2);
  });

  test("a dropped server is let back in after the retry", async () => {
    const { fakes: servers, nodes } = fakes(3);
    const t = await cluster({ nodes, failover: quick });
    const c = await Client.connect(t);
    const key = must(
      Array.from({ length: 100 }, (_, i) => `key:${i}`).find(
        (k) => t.nodeFor(k) === "10.0.0.0:4433",
      ),
    );
    must(servers[0]).fail = new ClosedError();
    expect(c.get(key)).rejects.toBeInstanceOf(ClosedError);
    await Bun.sleep(0);
    expect(t.live().length).toBe(2);

    must(servers[0]).fail = null;
    await Bun.sleep(quick.retryMs + 10);
    expect(t.nodeFor(key)).toBe("10.0.0.0:4433");
    await c.set(key, 3);
    expect(await c.get<number>(key)).toBe(3);
    expect(t.live().length).toBe(3);
  });

  test("a cluster that is all down still tries the key's own server", async () => {
    const { fakes: servers, nodes } = fakes(3);
    const t = await cluster({ nodes, failover: quick });
    const c = await Client.connect(t);
    const keys = nodes.map((n) =>
      must(
        Array.from({ length: 200 }, (_, i) => `key:${i}`).find(
          (k) => t.nodeFor(k) === n.name,
        ),
      ),
    );
    for (const s of servers) s.fail = new ClosedError();
    const failed = await Promise.allSettled(keys.map((k) => c.get(k)));
    expect(failed.map((r) => r.status)).toEqual([
      "rejected",
      "rejected",
      "rejected",
    ]);
    expect(t.live()).toEqual([]);
    // With nothing in the ring a key still goes to its own server, which is
    // what finds out that server is back.
    must(servers[0]).fail = null;
    expect(t.nodeFor(must(keys[0]))).toBe("10.0.0.0:4433");
    expect(await c.get(must(keys[0]))).toBeNull();
    expect(t.live()).toEqual(["10.0.0.0:4433"]);
    // The servers still down keep none of their keys: with one server in the
    // ring again they go to it.
    expect(t.nodeFor(must(keys[1]))).toBe("10.0.0.0:4433");
    expect(await c.get(must(keys[1]))).toBeNull();
  });

  test("a refused call is not a failed server", async () => {
    const { fakes: servers, nodes } = fakes(3);
    const t = await cluster({ nodes, failover: quick });
    const c = await Client.connect(t);
    must(servers[0]).fail = new StatusError(Status.TooLarge, "too big");
    const key = must(
      Array.from({ length: 100 }, (_, i) => `key:${i}`).find(
        (k) => t.nodeFor(k) === "10.0.0.0:4433",
      ),
    );
    expect(c.set(key, 1)).rejects.toBeInstanceOf(StatusError);
    await Bun.sleep(0);
    expect(t.live().length).toBe(3);
    expect(t.nodeFor(key)).toBe("10.0.0.0:4433");
  });

  test("without a policy a key never leaves its server", async () => {
    const { fakes: servers, nodes } = fakes(3);
    const t = await cluster({ nodes, failover: null });
    const c = await Client.connect(t);
    const key = must(
      Array.from({ length: 100 }, (_, i) => `key:${i}`).find(
        (k) => t.nodeFor(k) === "10.0.0.0:4433",
      ),
    );
    must(servers[0]).fail = new ClosedError();
    expect(c.get(key)).rejects.toBeInstanceOf(ClosedError);
    await Bun.sleep(0);
    expect(c.get(key)).rejects.toBeInstanceOf(ClosedError);
    await Bun.sleep(0);
    expect(t.live().length).toBe(3);
    expect(t.nodeFor(key)).toBe("10.0.0.0:4433");
  });

  test("the default policy takes two failures", async () => {
    const { fakes: servers, nodes } = fakes(3);
    expect(DEFAULT_FAILOVER).toEqual({ failures: 2, retryMs: 30_000 });
    const t = await cluster({ nodes });
    const c = await Client.connect(t);
    const key = must(
      Array.from({ length: 100 }, (_, i) => `key:${i}`).find(
        (k) => t.nodeFor(k) === "10.0.0.0:4433",
      ),
    );
    must(servers[0]).fail = new ClosedError();
    expect(c.get(key)).rejects.toBeInstanceOf(ClosedError);
    await Bun.sleep(0);
    expect(t.live().length).toBe(3);
    expect(c.get(key)).rejects.toBeInstanceOf(ClosedError);
    await Bun.sleep(0);
    expect(t.live().length).toBe(2);
  });

  test("ping reaches every server and reports the one that is down", async () => {
    const { fakes: servers, nodes } = fakes(3);
    const t = await cluster({ nodes, failover: quick });
    const c = await Client.connect(t);
    counted(servers);
    await c.ping();
    expect(servers.map((s) => s.seen)).toEqual([1, 1, 1]);
    must(servers[1]).fail = new ClosedError();
    expect(c.ping()).rejects.toBeInstanceOf(ClosedError);
    await Bun.sleep(0);
    expect(t.live().length).toBe(2);
    // A dropped server is pinged too: that is how it is found to be back,
    // whether the ping comes through a client or from the transport itself.
    must(servers[1]).fail = null;
    await t.ping();
    expect(t.live().length).toBe(3);
    await c.ping();
    expect(servers.map((s) => s.seen)).toEqual([4, 3, 4]);
  });
});

describe("batches", () => {
  test("a batch is split over the servers and answered in order", async () => {
    const { fakes: servers, nodes } = fakes(3);
    const t = await cluster({ nodes });
    const c = await Client.connect(t);
    const keys = Array.from({ length: 30 }, (_, i) => `item:${i}`);
    expect(new Set(keys.map((k) => t.nodeFor(k))).size).toBe(3);
    counted(servers);
    await c.batch(keys.map((k, i) => op.set(k, i)));
    expect(servers.map((s) => s.seen)).toEqual([1, 1, 1]);

    const results = await c.batch([
      ...keys.map((k) => op.get<number>(k)),
      op.get("nobody:home"),
      op.del(must(keys[7])),
      op.del("nobody:home"),
      op.set("fresh", "v"),
    ]);
    expect(results.slice(0, keys.length)).toEqual(keys.map((_, i) => i));
    expect(results[keys.length]).toBeNull();
    expect(results[keys.length + 1]).toBe(true);
    expect(results[keys.length + 2]).toBe(false);
    expect(results[keys.length + 3]).toBeUndefined();
    expect(await c.get(must(keys[7]))).toBeNull();
    expect(await c.get<string>("fresh")).toBe("v");
  });

  test("a batch of one server's keys is one request to that server", async () => {
    const { fakes: servers, nodes } = fakes(3);
    const t = await cluster({ nodes });
    const c = await Client.connect(t);
    const key = must(
      Array.from({ length: 100 }, (_, i) => `key:${i}`).find(
        (k) => t.nodeFor(k) === "10.0.0.1:4433",
      ),
    );
    counted(servers);
    const [, value] = await c.batch([op.set(key, 1), op.get<number>(key)]);
    expect(value).toBe(1);
    expect(servers.map((s) => s.seen)).toEqual([0, 1, 0]);
  });

  test("an empty batch asks nobody", async () => {
    const { fakes: servers, nodes } = fakes(2);
    const c = await Client.connect(cluster({ nodes }));
    counted(servers);
    expect(await c.batch([])).toEqual([]);
    expect(servers.map((s) => s.seen)).toEqual([0, 0]);
  });

  test("a batch fails as a whole when one of its servers does", async () => {
    const { fakes: servers, nodes } = fakes(3);
    const t = await cluster({ nodes, failover: quick });
    const c = await Client.connect(t);
    const keys = Array.from({ length: 30 }, (_, i) => `item:${i}`);
    must(servers[0]).fail = new ClosedError();
    expect(c.batch(keys.map((k, i) => op.set(k, i)))).rejects.toBeInstanceOf(
      ClosedError,
    );
    await Bun.sleep(0);
    // The servers that did answer applied their share, and the one that did
    // not is out of the ring.
    expect(must(servers[1]).store.size).toBeGreaterThan(0);
    expect(t.live().length).toBe(2);
    // With it dropped the same batch is served by the two that are left.
    const results = await c.batch(keys.map((k) => op.get<number>(k)));
    expect(results.length).toBe(keys.length);
  });

  test("a server that answers a batch short is a decode error", async () => {
    const { fakes: servers, nodes } = fakes(3);
    const t = await cluster({ nodes });
    const c = await Client.connect(t);
    must(servers[0]).miscount = true;
    const keys = Array.from({ length: 30 }, (_, i) => `item:${i}`);
    expect(c.batch(keys.map((k) => op.get(k)))).rejects.toThrow(REPLIES);
  });
});

describe("opening and closing", () => {
  test("a cluster needs at least one server, named once", async () => {
    expect(cluster({ nodes: [] })).rejects.toThrow(AT_LEAST_ONE);
    const twice = { name: "10.0.0.0:4433", open: () => new Fake() };
    expect(cluster({ nodes: [twice, twice] })).rejects.toThrow(DUPLICATE);
  });

  test("a policy that could never let a server back is refused", async () => {
    const { nodes } = fakes(2);
    expect(
      cluster({ nodes, failover: { failures: 0, retryMs: 50 } }),
    ).rejects.toThrow(AT_LEAST_ONE_FAILURE);
    expect(
      cluster({ nodes, failover: { failures: 1.5, retryMs: 50 } }),
    ).rejects.toThrow(AT_LEAST_ONE_FAILURE);
    expect(
      cluster({ nodes, failover: { failures: 1, retryMs: -1 } }),
    ).rejects.toThrow(NOT_NEGATIVE);
  });

  test("a server that is down at connect starts out of the ring", async () => {
    const { fakes: servers, nodes } = fakes(3);
    let opens = 0;
    const flaky: ClusterNode = {
      name: "10.0.0.0:4433",
      open: () => {
        opens++;
        if (opens === 1) throw new Error("connection refused");
        return must(servers[0]);
      },
    };
    const t = await cluster({
      nodes: [flaky, ...nodes.slice(1)],
      failover: quick,
    });
    expect(t.live()).toEqual(["10.0.0.1:4433", "10.0.0.2:4433"]);
    // Once the retry is due it is opened again, and stays once it answers.
    await Bun.sleep(quick.retryMs + 10);
    const c = await Client.connect(t);
    const key = must(
      Array.from({ length: 100 }, (_, i) => `key:${i}`).find(
        (k) => t.nodeFor(k) === "10.0.0.0:4433",
      ),
    );
    await c.set(key, 1);
    expect(opens).toBe(2);
    expect(must(servers[0]).store.size).toBe(1);
  });

  test("a cluster with nothing up does not connect", async () => {
    const nodes = [0, 1].map((i) => ({
      name: `10.0.0.${i}:4433`,
      open: () => Promise.reject(new Error(`refused ${i}`)),
    }));
    expect(cluster({ nodes })).rejects.toThrow(REFUSED);
  });

  test("a token one server refuses fails the cluster", async () => {
    const { fakes: servers, nodes } = fakes(2);
    const picky: ClusterNode = {
      name: "10.0.0.2:4433",
      open: () => Promise.reject(new StatusError(Status.Unauthorized, "no")),
    };
    expect(cluster({ nodes: [...nodes, picky] })).rejects.toBeInstanceOf(
      StatusError,
    );
    await Bun.sleep(0);
    // The servers that did open are closed again: nobody is left holding one.
    expect(servers.map((s) => s.isOpen)).toEqual([false, false]);
  });

  test("close ends every connection, and everything after it", async () => {
    const { fakes: servers, nodes } = fakes(3);
    const t = await cluster({ nodes });
    const c = await Client.connect(t);
    await c.set("k", 1);
    c.close();
    expect(servers.map((s) => s.isOpen)).toEqual([false, false, false]);
    expect(t.isOpen).toBe(false);
    expect(c.get("k")).rejects.toBeInstanceOf(ClosedError);
    expect(c.batch([op.get("k")])).rejects.toBeInstanceOf(ClosedError);
    // Closing twice is not an error, and the first reason stands.
    c.close();
    expect(t.isOpen).toBe(false);
  });

  test("a connection that arrives after close is closed too", async () => {
    const { fakes: servers, nodes } = fakes(3);
    let attempt = 0;
    // A box, so that assigning from inside the factory is visible to the
    // test's own control flow.
    const gate: { arrive?: () => void } = {};
    const slow: ClusterNode = {
      name: "10.0.0.0:4433",
      open: () => {
        attempt++;
        // Down at first, then slow to answer: the retry is still in flight
        // when the cluster is closed under it.
        if (attempt === 1) throw new Error("connection refused");
        return new Promise<Transport>((resolve) => {
          gate.arrive = () => resolve(must(servers[0]));
        });
      },
    };
    const t = await cluster({
      nodes: [slow, ...nodes.slice(1)],
      failover: quick,
    });
    const c = await Client.connect(t);
    await Bun.sleep(quick.retryMs + 10);
    const key = must(
      Array.from({ length: 100 }, (_, i) => `key:${i}`).find(
        (k) => t.nodeFor(k) === "10.0.0.0:4433",
      ),
    );
    const pending = c.get(key);
    await Bun.sleep(0);
    c.close();
    must(gate.arrive)();
    await pending;
    expect(attempt).toBe(2);
    expect(must(servers[0]).isOpen).toBe(false);
  });
});

describe("over the http transport", () => {
  let servers: TestServer[];

  beforeAll(async () => {
    servers = await Promise.all([startServer(), startServer()]);
  });

  afterAll(async () => {
    await Promise.all(servers.map((s) => s.stop()));
  });

  test("keys spread over servers reached by fetch", async () => {
    const t = await cluster({
      nodes: servers.map((s) => ({
        name: s.url,
        open: () => http({ url: s.url, token: "any" }),
      })),
    });
    const c = await Client.connect(t);
    const keys = Array.from({ length: 40 }, (_, i) => `http:${i}`);
    expect(new Set(keys.map((k) => t.nodeFor(k))).size).toBe(2);
    await Promise.all(keys.map((k, i) => c.set(k, i)));
    expect(await c.batch(keys.map((k) => op.get<number>(k)))).toEqual(
      keys.map((_, i) => i),
    );
    c.close();
  });

  test("a server that is not listening is down, however lazy its transport", async () => {
    // `http` builds its transport without any I/O, so only the round trip
    // the cluster makes as it opens can tell that nobody is there.
    const dead = `http://127.0.0.1:${freePort()}`;
    const t = await cluster({
      nodes: [must(servers[0]), { url: dead }].map((s) => ({
        name: s.url,
        open: () => http({ url: s.url, token: "any" }),
      })),
      failover: quick,
    });
    expect(t.live()).toEqual([must(servers[0]).url]);
    t.close();
    // With none of them listening there is no cluster at all.
    expect(
      cluster({
        nodes: [dead, `http://127.0.0.1:${freePort()}`].map((url) => ({
          name: url,
          open: () => http({ url, token: "any" }),
        })),
      }),
    ).rejects.toThrow();
  });
});

describe("over real servers", () => {
  let servers: TestServer[];
  let t: ClusterTransport;
  let c: Client;

  beforeAll(async () => {
    servers = await Promise.all([startServer(), startServer(), startServer()]);
    t = await cluster({
      nodes: servers.map((s) => ({
        name: `127.0.0.1:${s.port}`,
        open: () => tcp({ port: s.port, token: "any" }),
      })),
      failover: { failures: 1, retryMs: 200 },
    });
    c = await Client.connect(t);
  });

  afterAll(async () => {
    c.close();
    await Promise.all(servers.map((s) => s.stop()));
  });

  test("keys land on the server the ring names", async () => {
    const keys = Array.from({ length: 60 }, (_, i) => `real:${i}`);
    await Promise.all(keys.map((k, i) => c.set(k, i)));
    const direct = await Promise.all(
      servers.map((s) => Client.connect(tcp({ port: s.port, token: "any" }))),
    );
    const held = await Promise.all(
      direct.map((d) => Promise.all(keys.map((k) => d.get<number>(k)))),
    );
    for (const [i, key] of keys.entries()) {
      const owner = t.nodeFor(key);
      for (const [j, s] of servers.entries()) {
        const got = must(held[j])[i];
        expect(got).toBe(`127.0.0.1:${s.port}` === owner ? i : (null as never));
      }
    }
    for (const d of direct) d.close();
  });

  test("a batch spans the servers and comes back in order", async () => {
    const keys = Array.from({ length: 40 }, (_, i) => `batch:${i}`);
    expect(new Set(keys.map((k) => t.nodeFor(k))).size).toBe(3);
    await c.batch(keys.map((k, i) => op.set(k, i * 2)));
    const got = await c.batch(keys.map((k) => op.get<number>(k)));
    expect(got).toEqual(keys.map((_, i) => i * 2));
  });

  test("a killed server's keys move, and move back when it returns", async () => {
    const dying = must(servers[0]);
    const name = `127.0.0.1:${dying.port}`;
    const key = must(
      Array.from({ length: 200 }, (_, i) => `moving:${i}`).find(
        (k) => t.nodeFor(k) === name,
      ),
    );
    await c.set(key, 1);
    await dying.stop();
    expect(c.get(key)).rejects.toBeInstanceOf(Error);
    await Bun.sleep(50);
    expect(t.live()).not.toContain(name);
    expect(await c.get(key)).toBeNull();
    await c.set(key, 2);
    expect(await c.get<number>(key)).toBe(2);

    // Back on the same port: after the retry window the key is its own again.
    servers[0] = await startServer({}, dying.port);
    await Bun.sleep(250);
    expect(t.nodeFor(key)).toBe(name);
    expect(await c.get(key)).toBeNull();
    await c.set(key, 3);
    expect(await c.get<number>(key)).toBe(3);
    expect(t.live()).toContain(name);
  });
});
