import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Client, ClosedError, Op, Status, StatusError } from "../src/index";
import { tcp } from "../src/transport/tcp";
import { must } from "./must";
import { startServer, type TestServer } from "./server";

interface User {
  id: number;
  name: string;
  tags: string[];
  joined: Date;
  avatar: Uint8Array;
}

describe("tcp transport e2e", () => {
  let server: TestServer;
  beforeAll(async () => {
    server = await startServer();
  });
  afterAll(() => server.stop());

  test("single key in, single value out", async () => {
    const c = await Client.connect(tcp({ port: server.port, token: "any" }));
    expect(await c.get("a")).toBeNull();
    await c.set("a", 1);
    expect(await c.get<number>("a")).toBe(1);
    expect(await c.del("a")).toBe(true);
    expect(await c.del("a")).toBe(false);
    expect(await c.get("a")).toBeNull();
    c.close();
    expect(c.isOpen).toBe(false);
  });

  test("several keys in, tuple out", async () => {
    const c = await Client.connect(tcp({ port: server.port, token: "any" }));
    await c.set(["a", "1"], ["b", [0, 1, 2]]);
    const [a, b, missing] = await c.get("a", "b", "c");
    expect(a).toBe("1");
    expect(b).toEqual([0, 1, 2]);
    expect(missing).toBeNull();
    expect(await c.del("a", "zz")).toEqual([true, false]);
    expect(await c.get("a", "b")).toEqual([null, [0, 1, 2]]);
    await c.set(["n", 5], ["s", "five"]);
    const [n, str, none] = await c.get<[number, string, User]>(
      "n",
      "s",
      "nope",
    );
    expect(must(n) + 1).toBe(6);
    expect(must(str).toUpperCase()).toBe("FIVE");
    expect(none).toBeNull();
    c.close();
  });

  test("array in, array out", async () => {
    const c = await Client.connect(tcp({ port: server.port, token: "any" }));
    const keys = Array.from({ length: 20 }, (_, i) => `arr-${i}`);
    await c.set(keys.map((k, i) => [k, i] as const));
    expect(await c.get<number>(keys)).toEqual(keys.map((_, i) => i));
    expect(await c.get(["arr-0"])).toEqual([0]); // one-element array stays an array
    expect(await c.del([...keys, "zz"])).toEqual([
      ...keys.map(() => true),
      false,
    ]);
    expect(await c.get(keys)).toEqual(keys.map(() => null));
    c.close();
  });

  test("objects of any shape round-trip", async () => {
    const c = await Client.connect(tcp({ port: server.port, token: "any" }));
    const u: User = {
      id: 7,
      name: "alice",
      tags: ["x", "y"],
      joined: new Date("2026-01-02T03:04:05.678Z"),
      avatar: new Uint8Array([1, 2, 3]),
    };
    await c.set("user:7", u);
    const got = must(await c.get<User>("user:7"));
    expect(got.id).toBe(7);
    expect(got.name).toBe("alice");
    expect(got.tags).toEqual(["x", "y"]);
    expect(got.joined).toBeInstanceOf(Date);
    expect(got.joined.getTime()).toBe(u.joined.getTime());
    expect([...got.avatar]).toEqual([1, 2, 3]);
    c.close();
  });

  test("every primitive kind", async () => {
    const c = await Client.connect(tcp({ port: server.port, token: "any" }));
    await c.set(
      ["null", null],
      ["bool", false],
      ["int", -42],
      ["float", 3.25],
      ["bigint", 2n ** 62n],
      ["str", "ключ ✓"],
      ["bin", new Uint8Array([255, 0])],
      ["date", new Date(1234567890123)],
      ["arr", [1, "a", null]],
      ["obj", { nested: { deep: true } }],
    );
    const [n, b, i, f, bi, s, bin, d, arr, obj] = await c.get(
      "null",
      "bool",
      "int",
      "float",
      "bigint",
      "str",
      "bin",
      "date",
      "arr",
      "obj",
    );
    expect(n).toBeNull();
    expect(b).toBe(false);
    expect(i).toBe(-42);
    expect(f).toBe(3.25);
    expect(bi).toBe(2n ** 62n);
    expect(s).toBe("ключ ✓");
    expect([...(bin as Uint8Array)]).toEqual([255, 0]);
    expect((d as Date).getTime()).toBe(1234567890123);
    expect(arr).toEqual([1, "a", null]);
    expect(obj).toEqual({ nested: { deep: true } });
    c.close();
  });

  test("binary keys and unicode string keys", async () => {
    const c = await Client.connect(tcp({ port: server.port, token: "any" }));
    const key = new Uint8Array([0, 255, 1, 2]);
    await c.set([
      [key, "bin"],
      ["ключ", "значение"],
      ["", "empty key"],
    ]);
    expect(await c.get(key, "ключ", "", "ключ2")).toEqual([
      "bin",
      "значение",
      "empty key",
      null,
    ]);
    c.close();
  });

  test("large values", async () => {
    const c = await Client.connect(tcp({ port: server.port, token: "any" }));
    const big = new Uint8Array(4 << 20).fill(7);
    big[big.length - 1] = 9;
    await c.set("big", big);
    const got = must(await c.get<Uint8Array>("big"));
    expect(got.length).toBe(big.length);
    expect(Buffer.from(got).equals(Buffer.from(big))).toBe(true);
    const text = "y".repeat(1 << 20);
    await c.set("text", text);
    expect(await c.get<string>("text")).toBe(text);
    c.close();
  });

  test("pipelined concurrent calls share one connection", async () => {
    const c = await Client.connect(tcp({ port: server.port, token: "any" }));
    await Promise.all(
      Array.from({ length: 16 }, async (_, t) => {
        for (let i = 0; i < 50; i++) {
          const k = `t${t}-${i}`;
          // biome-ignore lint/performance/noAwaitInLoops: each task is deliberately sequential; the tasks run concurrently
          await c.set(k, { k, i });
          expect(await c.get<{ k: string; i: number }>(k)).toEqual({ k, i });
        }
      }),
    );
    // Many calls issued in one tick are coalesced into one write and
    // answered in order.
    const results = await Promise.all(
      Array.from({ length: 200 }, (_, i) =>
        c.get<{ k: string }>(`t${i % 16}-${i % 50}`),
      ),
    );
    for (const [i, r] of results.entries()) {
      expect(r?.k).toBe(`t${i % 16}-${i % 50}`);
    }
    c.close();
  });

  test("empty batches", async () => {
    const c = await Client.connect(tcp({ port: server.port, token: "any" }));
    expect(await c.get([])).toEqual([]);
    expect(await c.del([])).toEqual([]);
    await c.set([]);
    c.close();
  });

  test("ping round-trips", async () => {
    const c = await Client.connect(tcp({ port: server.port, token: "any" }));
    await c.ping();
    await c.set("p", 1);
    await Promise.all([c.ping(), c.ping()]);
    expect(await c.get<number>("p")).toBe(1);
    c.close();
    await expect(c.ping()).rejects.toBeInstanceOf(ClosedError);
  });

  test("closed connection rejects in-flight and later calls", async () => {
    const c = await Client.connect(tcp({ port: server.port, token: "any" }));
    const inflight = c.get("x");
    c.close();
    await expect(inflight).rejects.toBeInstanceOf(ClosedError);
    await expect(c.get("x")).rejects.toBeInstanceOf(ClosedError);
    await expect(
      Client.connect(tcp({ port: 1, token: "any" })),
    ).rejects.toBeDefined();
  });
});

test("a server that exits before listening is reported", async () => {
  // An invalid variable makes the server exit before it binds; the harness
  // polls the port instead of reading stderr, so that is what it sees.
  await expect(startServer({ OXICACHE_CAPACITY: "1X" })).rejects.toThrow(
    "server exited before listening",
  );
});

describe("token auth", () => {
  let server: TestServer;
  beforeAll(async () => {
    server = await startServer({ OXICACHE_TOKEN: "s3cret" });
  });
  afterAll(() => server.stop());

  test("wrong token", async () => {
    const err = await Client.connect(
      tcp({ port: server.port, token: "s3cre" }),
    ).catch((e) => e);
    expect(err).toBeInstanceOf(StatusError);
    expect((err as StatusError).status).toBe(Status.Unauthorized);
  });

  test("right token works", async () => {
    const c = await Client.connect(tcp({ port: server.port, token: "s3cret" }));
    await c.set("a", 1);
    expect(await c.get<number>("a")).toBe(1);
    expect(await c.del("a")).toBe(true);
    c.close();
  });
});

describe("desync", () => {
  test("unsolicited frame closes the connection", async () => {
    const ok = new Uint8Array([0, 0, 0, 0, 0]); // status OK, empty body
    let authed = false;
    const fake = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(s) {
          if (!authed) {
            authed = true;
            s.write(ok); // accept AUTH
            return;
          }
          // Reply to the request and send one stray frame in one write.
          s.write(new Uint8Array([...ok, ...ok]));
        },
      },
    });
    try {
      const c = await Client.connect(tcp({ port: fake.port, token: "any" }));
      await c.set("a", 1);
      const err = await c.get("a").catch((e) => e);
      expect(err).toBeInstanceOf(ClosedError);
      expect(c.isOpen).toBe(false);
    } finally {
      fake.stop(true);
    }
  });
});

describe("keepalive", () => {
  test("keepaliveMs must be positive", async () => {
    await Promise.all(
      [0, -1, Number.NaN, Number.POSITIVE_INFINITY, 2147483648].map(
        (keepaliveMs) =>
          expect(tcp({ port: 1, token: "any", keepaliveMs })).rejects.toThrow(
            RangeError,
          ),
      ),
    );
  });

  test("a quiet connection outlives the server idle timeout", async () => {
    const server = await startServer({ OXICACHE_IDLE_TIMEOUT: "1" });
    try {
      // Pinging every 300 ms keeps the connection open across 1.5 s of
      // silence; the default interval (100 s) never comes due, so that
      // client is closed, which is what proves the pings did the work.
      const quiet = await Client.connect(
        tcp({ port: server.port, token: "any", keepaliveMs: 300 }),
      );
      const silent = await Client.connect(
        tcp({ port: server.port, token: "any" }),
      );
      await quiet.set("k", 1);
      await silent.set("k", 1);
      await Bun.sleep(1500);
      expect(await quiet.get<number>("k")).toBe(1);
      await expect(silent.get("k")).rejects.toBeInstanceOf(ClosedError);
      expect(quiet.isOpen).toBe(true);
      expect(silent.isOpen).toBe(false);
      quiet.close();
    } finally {
      server.stop();
    }
  });

  test("PING frames are sent after keepaliveMs without a write", async () => {
    const ok = new Uint8Array([0, 0, 0, 0, 0]);
    const ops: number[] = [];
    const fake = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(s, chunk) {
          // A chunk may carry several frames; walk them by header length.
          let buf = chunk;
          while (buf.length >= 5) {
            ops.push(must(buf[0]));
            const len = new DataView(buf.buffer, buf.byteOffset).getUint32(
              1,
              true,
            );
            buf = buf.subarray(5 + len);
            s.write(ok);
          }
        },
      },
    });
    try {
      const c = await Client.connect(
        tcp({ port: fake.port, token: "any", keepaliveMs: 100 }),
      );
      await Bun.sleep(400);
      expect(ops[0]).toBe(Op.Auth);
      expect(ops.filter((op) => op === Op.Ping).length).toBeGreaterThanOrEqual(
        2,
      );
      expect(c.isOpen).toBe(true);
      // A call re-arms the clock: no PING follows it within the interval.
      await c.set("a", 1);
      const before = ops.length;
      await Bun.sleep(25);
      expect(ops.length).toBe(before);
      c.close();
      const after = ops.length;
      await Bun.sleep(250);
      expect(ops.length).toBe(after); // closing stops the heartbeat
    } finally {
      fake.stop(true);
    }
  });

  test("a heartbeat the server does not answer is not an error", async () => {
    const ok = new Uint8Array([0, 0, 0, 0, 0]);
    let authed = false;
    const fake = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(s) {
          if (!authed) {
            authed = true;
            s.write(ok); // accept AUTH
            return;
          }
          s.end(); // hang up on the first PING
        },
      },
    });
    try {
      const c = await Client.connect(
        tcp({ port: fake.port, token: "any", keepaliveMs: 20 }),
      );
      await Bun.sleep(150);
      // The failed ping is swallowed; the closure is reported by the next call.
      expect(c.isOpen).toBe(false);
      await expect(c.get("a")).rejects.toBeInstanceOf(ClosedError);
    } finally {
      fake.stop(true);
    }
  });
});
