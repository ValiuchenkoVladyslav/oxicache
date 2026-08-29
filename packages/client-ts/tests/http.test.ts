import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import {
  Client,
  ClosedError,
  HEADER_LEN,
  Op,
  Status,
  StatusError,
} from "../src/index";
import { http } from "../src/transport/http";
import { must } from "./must";
import { startServer, type TestServer } from "./server";

const AUTH_FRAME = new Uint8Array([Op.Auth, 0, 0, 0, 0]);

describe("http transport e2e", () => {
  let server: TestServer;
  beforeAll(async () => {
    server = await startServer();
  });
  afterAll(() => server.stop());

  test("/health answers with a status only", async () => {
    const res = await fetch(`${server.url}/health`);
    expect(res.status).toBe(200);
    expect((await res.arrayBuffer()).byteLength).toBe(0);
  });

  test("get/set/del in every shape, values are binary msgpack", async () => {
    const c = await Client.connect(http({ url: `${server.url}/` }));
    expect(c.isOpen).toBe(true);
    expect(await c.get("h")).toBeNull();
    await c.set("h", { id: 7, when: new Date(1234567890123), big: 2n ** 70n });
    const got = must(await c.get<{ id: number; when: Date; big: bigint }>("h"));
    expect(got.id).toBe(7);
    expect(got.when.getTime()).toBe(1234567890123);
    expect(got.big).toBe(2n ** 70n);
    await c.set(["a", 1], ["b", "two"]);
    expect(await c.get<[number, string, null]>("a", "b", "zz")).toEqual([
      1,
      "two",
      null,
    ]);
    const keys = Array.from({ length: 30 }, (_, i) => `hk${i}`);
    await c.set(keys.map((k, i) => [k, i] as const));
    expect(await c.get<number>(keys)).toEqual(keys.map((_, i) => i));
    expect(await c.del("a", "nope")).toEqual([true, false]);
    expect(await c.del(keys)).toEqual(keys.map(() => true));
    expect(await c.get([])).toEqual([]);
    // Concurrent calls each get their own exchange and their own answer.
    const many = await Promise.all(
      Array.from({ length: 50 }, (_, i) => c.get(i % 2 ? "b" : "h")),
    );
    expect(many.filter((v) => v === "two").length).toBe(25);
    c.close();
    expect(c.isOpen).toBe(false);
    await expect(c.get("h")).rejects.toBeInstanceOf(ClosedError);
    c.close(); // idempotent
  });

  test("large values", async () => {
    const c = await Client.connect(http(server));
    const big = new Uint8Array(4 << 20).fill(3);
    await c.set("hbig", big);
    const got = must(await c.get<Uint8Array>("hbig"));
    expect(Buffer.from(got).equals(Buffer.from(big))).toBe(true);
    c.close();
  });

  test("server statuses become StatusError", async () => {
    const t = http(server);
    // A malformed body: the frame says one key but carries none.
    const bad = new Uint8Array([Op.Get, 4, 0, 0, 0, 1, 0, 0, 0]);
    const err = await t.request(bad).catch((e) => e);
    expect(err).toBeInstanceOf(StatusError);
    expect((err as StatusError).status).toBe(Status.BadRequest);
    expect((err as Error).message).toContain("unexpected end");
    // An op the server has no path for.
    const unknown = new Uint8Array([9, 0, 0, 0, 0]);
    const fetchTo = (path: string) =>
      http({
        url: server.url,
        fetch: (_u, init) => fetch(server.url + path, init),
      });
    const e404 = await fetchTo("/nope")
      .request(unknown)
      .catch((e) => e);
    expect((e404 as StatusError).status).toBe(Status.UnknownOp);
    const e413 = await http({
      url: server.url,
      fetch: async () => new Response("too big", { status: 413 }),
    })
      .request(bad)
      .catch((e) => e);
    expect((e413 as StatusError).status).toBe(Status.TooLarge);
    // Statuses the server never sends (a proxy's, say) are plain errors.
    const e502 = await http({
      url: server.url,
      fetch: async () => new Response("bad gateway", { status: 502 }),
    })
      .request(bad)
      .catch((e) => e);
    expect(e502).not.toBeInstanceOf(StatusError);
    expect((e502 as Error).message).toBe(
      "server returned HTTP 502: bad gateway",
    );
    // AUTH has nothing to send: the token is a header on every request.
    expect((await t.request(AUTH_FRAME)).length).toBe(0);
  });

  test("a frame body is posted verbatim", async () => {
    let seen: { url: string; body: Uint8Array; headers: Headers } | undefined;
    const t = http({
      url: new URL("http://cache.example/"),
      token: "tok",
      fetch: async (url, init) => {
        seen = {
          url,
          body: new Uint8Array(await new Request(url, init).arrayBuffer()),
          headers: new Headers(init?.headers),
        };
        return new Response(new Uint8Array([0, 0, 0, 0]));
      },
    });
    const frame = new Uint8Array([Op.Del, 2, 0, 0, 0, 7, 8]);
    expect(await t.request(frame)).toEqual(new Uint8Array([0, 0, 0, 0]));
    const s = must(seen);
    expect(s.url).toBe("http://cache.example/del");
    expect([...s.body]).toEqual([...frame.subarray(HEADER_LEN)]);
    expect(s.headers.get("authorization")).toBe("Bearer tok");
    expect(s.headers.get("content-type")).toBe("application/octet-stream");
  });
});

describe("http token auth", () => {
  let server: TestServer;
  beforeAll(async () => {
    server = await startServer(["--token", "s3cret"]);
  });
  afterAll(() => server.stop());

  test("missing or wrong token is Unauthorized, /health needs none", async () => {
    expect((await fetch(`${server.url}/health`)).status).toBe(200);
    for (const token of [undefined, "s3cre"]) {
      // biome-ignore lint/performance/noAwaitInLoops: each case is checked in turn
      const c = await Client.connect(
        http({ url: server.url, ...(token === undefined ? {} : { token }) }),
      );
      const err = await c.get("a").catch((e) => e);
      expect(err).toBeInstanceOf(StatusError);
      expect((err as StatusError).status).toBe(Status.Unauthorized);
      // Unlike TCP, a refused request does not end the transport.
      expect(c.isOpen).toBe(true);
    }
    const c = await Client.connect(http({ url: server.url, token: "s3cret" }));
    await c.set("a", 1);
    expect(await c.get<number>("a")).toBe(1);
    c.close();
  });
});
