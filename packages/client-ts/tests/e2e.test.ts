import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Client, ClosedError, Status, StatusError } from "../src/index";
import { startServer, type TestServer } from "./server";

interface User {
  id: number;
  name: string;
  tags: string[];
  joined: Date;
  avatar: Uint8Array;
}

describe("client-ts e2e", () => {
  let server: TestServer;
  beforeAll(async () => {
    server = await startServer();
  });
  afterAll(() => server.stop());

  test("single key in, single value out", async () => {
    const c = await Client.connect({ port: server.port });
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
    const c = await Client.connect({ port: server.port });
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
    expect(n! + 1).toBe(6);
    expect(str!.toUpperCase()).toBe("FIVE");
    expect(none).toBeNull();
    c.close();
  });

  test("array in, array out", async () => {
    const c = await Client.connect({ port: server.port });
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
    const c = await Client.connect({ port: server.port });
    const u: User = {
      id: 7,
      name: "alice",
      tags: ["x", "y"],
      joined: new Date("2026-01-02T03:04:05.678Z"),
      avatar: new Uint8Array([1, 2, 3]),
    };
    await c.set("user:7", u);
    const got = await c.get<User>("user:7");
    expect(got).not.toBeNull();
    expect(got!.id).toBe(7);
    expect(got!.name).toBe("alice");
    expect(got!.tags).toEqual(["x", "y"]);
    expect(got!.joined).toBeInstanceOf(Date);
    expect(got!.joined.getTime()).toBe(u.joined.getTime());
    expect([...got!.avatar]).toEqual([1, 2, 3]);
    c.close();
  });

  test("useRecords is on by default and can be turned off", async () => {
    const shape = { id: 1, name: "alice", tags: ["x"] };
    for (const useRecords of [undefined, true, false]) {
      // biome-ignore lint/performance/noAwaitInLoops: the settings share keys, so they must run one after another
      const c = await Client.connect({
        port: server.port,
        ...(useRecords === undefined ? {} : { useRecords }),
      });
      await c.set(["r1", shape], ["r2", { ...shape, id: 2 }]);
      const [a, b] = await c.get<[typeof shape, typeof shape]>("r1", "r2");
      expect(a).toEqual(shape);
      expect(b!.id).toBe(2);
      // A fresh client with the same setting reads it back too.
      const c2 = await Client.connect({
        port: server.port,
        ...(useRecords === undefined ? {} : { useRecords }),
      });
      expect(await c2.get<typeof shape>("r2")).toEqual({ ...shape, id: 2 });
      c.close();
      c2.close();
    }
    // Plain msgpack written by a no-records client is readable by a records client.
    const plain = await Client.connect({
      port: server.port,
      useRecords: false,
    });
    await plain.set("plain", shape);
    const rec = await Client.connect({ port: server.port });
    expect(await rec.get<typeof shape>("plain")).toEqual(shape);
    plain.close();
    rec.close();
  });

  test("every primitive kind", async () => {
    const c = await Client.connect({ port: server.port });
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
    const c = await Client.connect({ port: server.port });
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
    const c = await Client.connect({ port: server.port });
    const big = new Uint8Array(4 << 20).fill(7);
    big[big.length - 1] = 9;
    await c.set("big", big);
    const got = await c.get<Uint8Array>("big");
    expect(got!.length).toBe(big.length);
    expect(Buffer.from(got!).equals(Buffer.from(big))).toBe(true);
    const text = "y".repeat(1 << 20);
    await c.set("text", text);
    expect(await c.get<string>("text")).toBe(text);
    c.close();
  });

  test("pipelined concurrent calls share one connection", async () => {
    const c = await Client.connect({ port: server.port });
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
    const c = await Client.connect({ port: server.port });
    expect(await c.get([])).toEqual([]);
    expect(await c.del([])).toEqual([]);
    await c.set([]);
    c.close();
  });

  test("closed connection rejects in-flight and later calls", async () => {
    const c = await Client.connect({ port: server.port });
    const inflight = c.get("x");
    c.close();
    await expect(inflight).rejects.toBeInstanceOf(ClosedError);
    await expect(c.get("x")).rejects.toBeInstanceOf(ClosedError);
    await expect(Client.connect({ port: 1 })).rejects.toBeDefined();
  });
});

describe("token auth", () => {
  let server: TestServer;
  beforeAll(async () => {
    server = await startServer(["--token", "s3cret"]);
  });
  afterAll(() => server.stop());

  test("unauthenticated request is rejected and disconnected", async () => {
    const c = await Client.connect({ port: server.port });
    const err = await c.get("a").catch((e) => e);
    expect(err).toBeInstanceOf(StatusError);
    expect((err as StatusError).status).toBe(Status.Unauthorized);
    await expect(c.get("a")).rejects.toBeInstanceOf(ClosedError);
  });

  test("wrong token", async () => {
    const err = await Client.connect({
      port: server.port,
      token: "s3cre",
    }).catch((e) => e);
    expect(err).toBeInstanceOf(StatusError);
    expect((err as StatusError).status).toBe(Status.Unauthorized);
  });

  test("right token works, redundant auth is fine", async () => {
    const c = await Client.connect({ port: server.port, token: "s3cret" });
    await c.set("a", 1);
    expect(await c.get<number>("a")).toBe(1);
    await c.auth("s3cret");
    expect(await c.del("a")).toBe(true);
    c.close();
  });
});

describe("desync", () => {
  test("unsolicited frame closes the connection", async () => {
    const ok = new Uint8Array([0, 0, 0, 0, 0]); // status OK, empty body
    const fake = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(s) {
          // Reply to the request, then send one stray frame.
          s.write(ok);
          s.write(ok);
        },
      },
    });
    try {
      const c = await Client.connect({ port: fake.port });
      await c.set("a", 1);
      const err = await c.get("a").catch((e) => e);
      expect(err).toBeInstanceOf(ClosedError);
      expect(c.isOpen).toBe(false);
    } finally {
      fake.stop(true);
    }
  });
});
