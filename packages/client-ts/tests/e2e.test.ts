import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Client, ClosedError, Status, StatusError } from "../src/index";
import { type TestServer, startServer } from "./server";

const text = (b: Uint8Array | null) => (b === null ? null : new TextDecoder().decode(b));

describe("client-ts e2e", () => {
  let server: TestServer;
  beforeAll(async () => {
    server = await startServer();
  });
  afterAll(() => server.stop());

  test("get, set, del over tcp", async () => {
    const c = await Client.connect({ port: server.port });
    expect(await c.get(["a"])).toEqual([null]);
    await c.set([
      ["a", "1"],
      ["b", new Uint8Array([0, 1, 2])],
    ]);
    const got = await c.get(["a", "b", "c"]);
    expect(text(got[0]!)).toBe("1");
    expect([...got[1]!]).toEqual([0, 1, 2]);
    expect(got[2]).toBeNull();
    expect(await c.del(["a", "zz"])).toEqual([true, false]);
    expect(await c.get(["a"])).toEqual([null]);
    c.close();
    expect(c.isOpen).toBe(false);
  });

  test("binary keys and unicode strings", async () => {
    const c = await Client.connect({ port: server.port });
    const key = new Uint8Array([0, 255, 1, 2]);
    await c.set([
      [key, "bin"],
      ["ключ", "значение"],
      ["", "empty key"],
    ]);
    const got = await c.get([key, "ключ", "", "ключ2"]);
    expect(got.map(text)).toEqual(["bin", "значение", "empty key", null]);
    c.close();
  });

  test("large values", async () => {
    const c = await Client.connect({ port: server.port });
    const big = new Uint8Array(4 << 20).fill(7);
    big[big.length - 1] = 9;
    await c.set([["big", big]]);
    const [got] = await c.get(["big"]);
    expect(got!.length).toBe(big.length);
    expect(Buffer.from(got!).equals(Buffer.from(big))).toBe(true);
    c.close();
  });

  test("pipelined concurrent calls share one connection", async () => {
    const c = await Client.connect({ port: server.port });
    await Promise.all(
      Array.from({ length: 16 }, async (_, t) => {
        for (let i = 0; i < 50; i++) {
          const k = `t${t}-${i}`;
          await c.set([[k, k]]);
          const [v] = await c.get([k]);
          expect(text(v!)).toBe(k);
        }
      }),
    );
    // Many calls issued in one tick are coalesced into one write and
    // answered in order.
    const results = await Promise.all(
      Array.from({ length: 200 }, (_, i) => c.get([`t${i % 16}-${i % 50}`])),
    );
    results.forEach((r, i) => expect(text(r[0]!)).toBe(`t${i % 16}-${i % 50}`));
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
    const inflight = c.get(["x"]);
    c.close();
    await expect(inflight).rejects.toBeInstanceOf(ClosedError);
    await expect(c.get(["x"])).rejects.toBeInstanceOf(ClosedError);
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
    const err = await c.get(["a"]).catch((e) => e);
    expect(err).toBeInstanceOf(StatusError);
    expect((err as StatusError).status).toBe(Status.Unauthorized);
    await expect(c.get(["a"])).rejects.toBeInstanceOf(ClosedError);
  });

  test("wrong token", async () => {
    const err = await Client.connect({ port: server.port, token: "s3cre" }).catch((e) => e);
    expect(err).toBeInstanceOf(StatusError);
    expect((err as StatusError).status).toBe(Status.Unauthorized);
  });

  test("right token works, redundant auth is fine", async () => {
    const c = await Client.connect({ port: server.port, token: "s3cret" });
    await c.set([["a", "1"]]);
    expect(text((await c.get(["a"]))[0]!)).toBe("1");
    await c.auth("s3cret");
    expect(await c.del(["a"])).toEqual([true]);
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
      await c.set([["a", "1"]]);
      const err = await c.get(["a"]).catch((e) => e);
      expect(err).toBeInstanceOf(ClosedError);
      expect(c.isOpen).toBe(false);
    } finally {
      fake.stop(true);
    }
  });
});
