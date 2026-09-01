import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import {
  Client,
  ClosedError,
  HEADER_LEN,
  Op,
  op,
  Status,
  StatusError,
} from "../src/index";
import { http } from "../src/transport/http";
import { must } from "./must";
import { CA_PEM, SERVER_PEM, startServer, type TestServer } from "./server";

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

  test("get/set/del/batch, values are binary msgpack", async () => {
    const c = await Client.connect(
      http({ url: `${server.url}/`, token: "any" }),
    );
    expect(c.isOpen).toBe(true);
    expect(await c.get("h")).toBeNull();
    await c.set("h", { id: 7, when: new Date(1234567890123), big: 2n ** 70n });
    const got = must(await c.get<{ id: number; when: Date; big: bigint }>("h"));
    expect(got.id).toBe(7);
    expect(got.when.getTime()).toBe(1234567890123);
    expect(got.big).toBe(2n ** 70n);
    await c.batch([op.set("a", 1), op.set("b", "two")]);
    expect(
      await c.batch([op.get<number>("a"), op.get<string>("b"), op.get("zz")]),
    ).toEqual([1, "two", null]);
    const keys = Array.from({ length: 30 }, (_, i) => `hk${i}`);
    await c.batch(keys.map((k, i) => op.set(k, i)));
    expect(await c.batch(keys.map((k) => op.get<number>(k)))).toEqual(
      keys.map((_, i) => i),
    );
    expect(await c.batch([op.del("a"), op.del("nope")])).toEqual([true, false]);
    expect(await c.del("b")).toBe(true);
    expect(await c.del("b")).toBe(false);
    expect(await c.batch(keys.map((k) => op.del(k)))).toEqual(
      keys.map(() => true),
    );
    expect(await c.batch([])).toEqual([]);
    await c.set("b", "two");
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

  test("ping round-trips as POST /ping", async () => {
    const transport = http({ url: server.url, token: "any" });
    const c = await Client.connect(transport);
    await c.ping();
    // The transport answers a ping of its own, without a client.
    await transport.ping();
    c.close();
    await expect(c.ping()).rejects.toBeInstanceOf(ClosedError);
  });

  test("large values", async () => {
    const c = await Client.connect(http({ url: server.url, token: "any" }));
    const big = new Uint8Array(4 << 20).fill(3);
    await c.set("hbig", big);
    const got = must(await c.get<Uint8Array>("hbig"));
    expect(Buffer.from(got).equals(Buffer.from(big))).toBe(true);
    c.close();
  });

  test("server statuses become StatusError", async () => {
    const t = http({ url: server.url, token: "any" });
    // A malformed body: the SET says a 9-byte key but carries one.
    const bad = new Uint8Array([Op.Set, 5, 0, 0, 0, 9, 0, 0, 0, 1]);
    const err = await t.request(bad).catch((e) => e);
    expect(err).toBeInstanceOf(StatusError);
    expect((err as StatusError).status).toBe(Status.BadRequest);
    expect((err as Error).message).toContain("unexpected end");
    // An op the server has no path for.
    const unknown = new Uint8Array([9, 0, 0, 0, 0]);
    const fetchTo = (path: string) =>
      http({
        url: server.url,
        token: "any",
        fetch: (_u, init) => fetch(server.url + path, init),
      });
    const e404 = await fetchTo("/nope")
      .request(unknown)
      .catch((e) => e);
    expect((e404 as StatusError).status).toBe(Status.UnknownOp);
    const e413 = await http({
      url: server.url,
      token: "any",
      fetch: async () => new Response("too big", { status: 413 }),
    })
      .request(bad)
      .catch((e) => e);
    expect((e413 as StatusError).status).toBe(Status.TooLarge);
    // Statuses the server never sends (a proxy's, say) are plain errors.
    const e502 = await http({
      url: server.url,
      token: "any",
      fetch: async () => new Response("bad gateway", { status: 502 }),
    })
      .request(bad)
      .catch((e) => e);
    expect(e502).not.toBeInstanceOf(StatusError);
    expect((e502 as Error).message).toBe(
      "server returned HTTP 502: bad gateway",
    );
    // AUTH is refused loudly: the token is a header on every request.
    await expect(t.request(AUTH_FRAME)).rejects.toThrow(
      "AUTH is not a request over HTTP",
    );
    // A key the server does not have is a 404 without a body: an answer.
    const miss = await t.request(new Uint8Array([Op.Get, 1, 0, 0, 0, 0x7a]));
    expect(miss).toEqual({ status: Status.NotFound, body: new Uint8Array(0) });
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
    expect(await t.request(frame)).toEqual({
      status: Status.Ok,
      body: new Uint8Array([0, 0, 0, 0]),
    });
    const s = must(seen);
    expect(s.url).toBe("http://cache.example/del");
    expect([...s.body]).toEqual([...frame.subarray(HEADER_LEN)]);
    expect(s.headers.get("authorization")).toBe("Bearer tok");
    expect(s.headers.get("content-type")).toBeNull();
  });
});

describe("http token auth", () => {
  let server: TestServer;
  beforeAll(async () => {
    server = await startServer({ OXICACHE_TOKEN: "s3cret" });
  });
  afterAll(() => server.stop());

  test("wrong token is Unauthorized, /health needs none", async () => {
    expect((await fetch(`${server.url}/health`)).status).toBe(200);
    const c = await Client.connect(http({ url: server.url, token: "s3cre" }));
    const err = await c.get("a").catch((e) => e);
    expect(err).toBeInstanceOf(StatusError);
    expect((err as StatusError).status).toBe(Status.Unauthorized);
    // Unlike TCP, a refused request does not end the transport.
    expect(c.isOpen).toBe(true);
    const ok = await Client.connect(http({ url: server.url, token: "s3cret" }));
    await ok.set("a", 1);
    expect(await ok.get<number>("a")).toBe(1);
    ok.close();
  });
});

describe("http transport over tls", () => {
  let server: TestServer;
  beforeAll(async () => {
    server = await startServer({ OXICACHE_TLS_CERT: SERVER_PEM });
  });
  afterAll(() => server.stop());

  test("a trusted CA gets through, an untrusted one does not", async () => {
    expect(server.url.startsWith("https://")).toBe(true);
    const c = await Client.connect(
      http({ url: server.url, token: "any", tls: { ca: CA_PEM } }),
    );
    await c.set("t", "secure");
    expect(await c.get<string>("t")).toBe("secure");
    c.close();
    // Without the CA the certificate is not trusted, and with the wrong
    // name it is not the server's; plain http is refused. Connecting sends
    // nothing on HTTP, so the first call is where each fails.
    const bare = await Client.connect(http({ url: server.url, token: "any" }));
    await expect(bare.get("t")).rejects.toThrow();
    const wrongName = await Client.connect(
      http({
        url: server.url,
        token: "any",
        tls: { ca: CA_PEM, serverName: "example.com" },
      }),
    );
    await expect(wrongName.get("t")).rejects.toThrow();
    const plain = await Client.connect(
      http({ url: server.url.replace("https", "http"), token: "any" }),
    );
    await expect(plain.get("t")).rejects.toThrow();
  });
});
