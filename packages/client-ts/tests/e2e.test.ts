import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { Socket } from "bun";
import {
  Client,
  ClosedError,
  DecodeError,
  Op,
  op,
  Status,
  StatusError,
} from "../src/index";
import { tcp } from "../src/transport/tcp";
import { type Frame, FrameReader } from "../src/wire";
import { must } from "./must";
import { CA_PEM, SERVER_PEM, startServer, type TestServer } from "./server";

/** A complete response frame. */
function frame(status: number, body: number[] = []): Uint8Array {
  const f = new Uint8Array(5 + body.length);
  f[0] = status;
  new DataView(f.buffer).setUint32(1, body.length, true);
  f.set(body, 5);
  return f;
}

const ok = frame(Status.Ok);
/** The reply to a GET whose value is the MessagePack `1`. */
const oneValue = frame(Status.Ok, [1]);

type Conn = Socket<{ n: number; reader: FrameReader }>;

/** Default connection hook: nothing to do on accept. */
const ignore = () => {
  // nothing to do
};

/** Resolves once `cond` holds, polling every few ms; fails after 2 s. */
async function until(cond: () => boolean): Promise<void> {
  const deadline = Date.now() + 2000;
  while (!cond()) {
    if (Date.now() > deadline) throw new Error("condition never held");
    // biome-ignore lint/performance/noAwaitInLoops: polling is sequential by nature
    await Bun.sleep(5);
  }
}

/**
 * A fake whose first connection hangs up on its first request and which
 * then drops every further connection on accept: reconnects fail with the
 * server "down", and every attempt shows up in the accept count.
 */
function goesDown() {
  return fakeServer(
    (s, _n, f) => {
      if (f.tag === Op.Auth) s.write(ok);
      else s.end();
    },
    (s, n) => {
      if (n > 0) s.end();
    },
  );
}

/**
 * A scripted server: `onFrame` gets every request frame with the ordinal
 * of the connection it came on (0 for the first), so a test can behave
 * differently before and after a reconnect and count accepts.
 */
function fakeServer(
  onFrame: (s: Conn, n: number, f: Frame) => void,
  onOpen: (s: Conn, n: number) => void = ignore,
) {
  let accepts = 0;
  const l = Bun.listen<{ n: number; reader: FrameReader }>({
    hostname: "127.0.0.1",
    port: 0,
    socket: {
      open(s) {
        s.data = { n: accepts++, reader: new FrameReader() };
        onOpen(s, s.data.n);
      },
      data(s, chunk) {
        const r = s.data.reader;
        r.push(chunk);
        for (let f = r.next(); f !== null; f = r.next())
          onFrame(s, s.data.n, f);
      },
    },
  });
  return {
    port: l.port,
    get accepts() {
      return accepts;
    },
    stop: () => l.stop(true),
  };
}

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
  afterAll(async () => {
    await server.stop();
  });

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

  test("a batch answers every op in order", async () => {
    const c = await Client.connect(tcp({ port: server.port, token: "any" }));
    const [, , a, b, missing] = await c.batch([
      op.set("a", "1"),
      op.set("b", [0, 1, 2]),
      op.get("a"),
      op.get("b"),
      op.get("c"),
    ]);
    expect(a).toBe("1");
    expect(b).toEqual([0, 1, 2]);
    expect(missing).toBeNull();
    expect(await c.batch([op.del("a"), op.del("zz")])).toEqual([true, false]);
    expect(await c.batch([op.get("a"), op.get("b")])).toEqual([
      null,
      [0, 1, 2],
    ]);
    const [n, str, none] = await c.batch([
      op.get<number>("n"),
      op.get<string>("s"),
      op.get<User>("nope"),
    ]);
    expect(n).toBeNull();
    await c.batch([op.set("n", 5), op.set("s", "five")]);
    const [n2, str2] = await c.batch([
      op.get<number>("n"),
      op.get<string>("s"),
    ]);
    expect(must(n2) + 1).toBe(6);
    expect(must(str2).toUpperCase()).toBe("FIVE");
    expect(str).toBeNull();
    expect(none).toBeNull();
    // A later op sees an earlier one: set, get, del, get of one key.
    expect(
      await c.batch([
        op.set("seq", 1),
        op.get<number>("seq"),
        op.del("seq"),
        op.get<number>("seq"),
      ]),
    ).toEqual([undefined, 1, true, null]);
    c.close();
  });

  test("a batch built at runtime, of any length", async () => {
    const c = await Client.connect(tcp({ port: server.port, token: "any" }));
    const keys = Array.from({ length: 20 }, (_, i) => `arr-${i}`);
    await c.batch(keys.map((k, i) => op.set(k, i)));
    expect(await c.batch(keys.map((k) => op.get<number>(k)))).toEqual(
      keys.map((_, i) => i),
    );
    expect(await c.batch([op.get("arr-0")])).toEqual([0]);
    expect(await c.batch([...keys, "zz"].map((k) => op.del(k)))).toEqual([
      ...keys.map(() => true),
      false,
    ]);
    expect(await c.batch(keys.map((k) => op.get(k)))).toEqual(
      keys.map(() => null),
    );
    expect(await c.batch([])).toEqual([]);
    c.close();
  });

  test("a refused op rejects the batch, naming the op", async () => {
    // A dedicated 4 MiB server: the shard count follows the host's CPU
    // count, but a value bigger than the whole capacity fits no shard
    // whatever that count is.
    const small = await startServer({ OXICACHE_CAPACITY: "4M" });
    try {
      const c = await Client.connect(tcp({ port: small.port, token: "any" }));
      const huge = new Uint8Array(8 << 20);
      const err = await c
        .batch([op.set("ok", 1), op.set("huge", huge), op.get("ok")])
        .catch((e) => e);
      expect(err).toBeInstanceOf(StatusError);
      expect((err as StatusError).status).toBe(Status.TooLarge);
      expect((err as Error).message).toContain("item 1:");
      // The ops the server accepted were still applied.
      expect(await c.get<number>("ok")).toBe(1);
      await expect(c.set("huge", huge)).rejects.toBeInstanceOf(StatusError);
      // Too many ops never reach the wire.
      expect(() => c.batch(new Array(65537).fill(op.get("x")))).toThrow(
        RangeError,
      );
      c.close();
    } finally {
      await small.stop();
    }
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
    await c.batch([
      op.set("null", null),
      op.set("bool", false),
      op.set("int", -42),
      op.set("float", 3.25),
      op.set("bigint", 2n ** 62n),
      op.set("str", "ключ ✓"),
      op.set("bin", new Uint8Array([255, 0])),
      op.set("date", new Date(1234567890123)),
      op.set("arr", [1, "a", null]),
      op.set("obj", { nested: { deep: true } }),
    ]);
    const [n, b, i, f, bi, s, bin, d, arr, obj] = await c.batch([
      op.get("null"),
      op.get("bool"),
      op.get("int"),
      op.get("float"),
      op.get("bigint"),
      op.get("str"),
      op.get("bin"),
      op.get("date"),
      op.get("arr"),
      op.get("obj"),
    ]);
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
    await c.batch([
      op.set(key, "bin"),
      op.set("ключ", "значение"),
      op.set("", "empty key"),
    ]);
    expect(
      await c.batch([op.get(key), op.get("ключ"), op.get(""), op.get("ключ2")]),
    ).toEqual(["bin", "значение", "empty key", null]);
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

  test("ping round-trips", async () => {
    const transport = await tcp({ port: server.port, token: "any" });
    const c = await Client.connect(transport);
    await c.ping();
    // The transport answers a ping of its own, without a client.
    await transport.ping();
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
  afterAll(async () => {
    await server.stop();
  });

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

describe("desync is permanent", () => {
  test("unsolicited frame closes the connection", async () => {
    const fake = fakeServer((s, _n, f) => {
      if (f.tag === Op.Auth) s.write(ok);
      // Reply to the request and send one stray frame in one write.
      else s.write(new Uint8Array([...ok, ...ok]));
    });
    try {
      const c = await Client.connect(tcp({ port: fake.port, token: "any" }));
      await c.set("a", 1);
      const err = await c.get("a").catch((e) => e);
      expect(err).toBeInstanceOf(ClosedError);
      expect(c.isOpen).toBe(false);
      // A server that does not speak the protocol is not reconnected to.
      await expect(c.get("a")).rejects.toBeInstanceOf(ClosedError);
      expect(fake.accepts).toBe(1);
    } finally {
      fake.stop();
    }
  });

  test("an invalid status byte kills the transport", async () => {
    const fake = fakeServer((s, _n, f) => {
      s.write(f.tag === Op.Auth ? ok : frame(9));
    });
    try {
      const c = await Client.connect(tcp({ port: fake.port, token: "any" }));
      const err = await c.get("a").catch((e) => e);
      expect(err).toBeInstanceOf(DecodeError);
      expect((err as Error).message).toBe("invalid status byte 9");
      const later = await c.get("a").catch((e) => e);
      expect(later).toBeInstanceOf(ClosedError);
      expect((later as Error).cause).toBe(err);
      expect(fake.accepts).toBe(1);
      expect(c.isOpen).toBe(false);
    } finally {
      fake.stop();
    }
  });
});

describe("reconnect", () => {
  test("a server restart is repaired by the next call", async () => {
    let server = await startServer();
    const t = await tcp({ port: server.port, token: "any" });
    const c = await Client.connect(t);
    await c.set("k", 1);
    await server.stop();
    // Down: the call fails with the connection error, not a status.
    const err = await c.get("k").catch((e) => e);
    expect(err).toBeInstanceOf(Error);
    expect(err).not.toBeInstanceOf(StatusError);
    expect(c.isOpen).toBe(false);
    server = await startServer({}, server.port);
    try {
      // Back: the next call reconnects (the cache is new, so the key is gone).
      expect(await c.get("k")).toBeNull();
      await c.set("k", 2);
      expect(await c.get<number>("k")).toBe(2);
      expect(t.reconnects).toBe(1);
      expect(c.isOpen).toBe(true);
      c.close();
    } finally {
      await server.stop();
    }
  });

  test("in-flight calls are retried once", async () => {
    // Connection 0 hangs up on the first GET; every later one answers it.
    const fake = fakeServer((s, n, f) => {
      if (f.tag === Op.Auth) s.write(ok);
      else if (n === 0) s.end();
      else s.write(oneValue);
    });
    try {
      const t = await tcp({ port: fake.port, token: "any" });
      const c = await Client.connect(t);
      // Two calls lost together share the one reconnect.
      expect(await Promise.all([c.get("a"), c.get("b")])).toEqual([1, 1]);
      expect(fake.accepts).toBe(2);
      expect(t.reconnects).toBe(1);
      c.close();
    } finally {
      fake.stop();
    }
  });

  test("a retried call is not retried again", async () => {
    // Every connection hangs up on its first request.
    const fake = fakeServer((s, _n, f) => {
      if (f.tag === Op.Auth) s.write(ok);
      else s.end();
    });
    try {
      const t = await tcp({ port: fake.port, token: "any" });
      const c = await Client.connect(t);
      await expect(c.get("a")).rejects.toBeInstanceOf(ClosedError);
      expect(fake.accepts).toBe(2);
      expect(t.reconnects).toBe(1);
      expect(c.isOpen).toBe(false);
    } finally {
      fake.stop();
    }
  });

  test("a rotated token kills the client", async () => {
    // Connection 0 hangs up on the first GET; every later AUTH is refused.
    const unauthorized = frame(Status.Unauthorized, [...Buffer.from("nope")]);
    const fake = fakeServer((s, n, f) => {
      if (n === 0) {
        if (f.tag === Op.Auth) s.write(ok);
        else s.end();
      } else {
        s.write(unauthorized);
      }
    });
    try {
      const c = await Client.connect(tcp({ port: fake.port, token: "any" }));
      const err = await c.get("a").catch((e) => e);
      expect(err).toBeInstanceOf(StatusError);
      expect((err as StatusError).status).toBe(Status.Unauthorized);
      // The second call does not even try: the answer would be the same.
      await expect(c.get("a")).rejects.toBe(err);
      expect(fake.accepts).toBe(2);
      expect(c.isOpen).toBe(false);
    } finally {
      fake.stop();
    }
  });

  test("status errors do not reconnect", async () => {
    let requests = 0;
    const fake = fakeServer((s, _n, f) => {
      if (f.tag === Op.Auth) s.write(ok);
      else if (requests++ === 0)
        s.write(frame(Status.BadRequest, [...Buffer.from("bad")]));
      else s.write(oneValue);
    });
    try {
      const t = await tcp({ port: fake.port, token: "any" });
      const c = await Client.connect(t);
      const err = await c.get("a").catch((e) => e);
      expect(err).toBeInstanceOf(StatusError);
      expect((err as StatusError).status).toBe(Status.BadRequest);
      expect(await c.get<number>("a")).toBe(1);
      expect(fake.accepts).toBe(1);
      expect(t.reconnects).toBe(0);
      c.close();
    } finally {
      fake.stop();
    }
  });

  test("close during a reconnect is final", async () => {
    // Connection 0 hangs up on the first GET; connection 1 takes its time
    // to answer AUTH, so the transport can be closed while it waits.
    const fake = fakeServer((s, n, f) => {
      if (n === 0) {
        if (f.tag === Op.Auth) s.write(ok);
        else s.end();
      } else {
        setTimeout(() => s.write(ok), 100);
      }
    });
    try {
      const c = await Client.connect(tcp({ port: fake.port, token: "any" }));
      const inflight = c.get("a");
      await until(() => !c.isOpen);
      c.close();
      await expect(inflight).rejects.toBeInstanceOf(ClosedError);
      await Bun.sleep(150);
      expect(fake.accepts).toBe(2);
      expect(c.isOpen).toBe(false);
      await expect(c.get("a")).rejects.toBeInstanceOf(ClosedError);
      expect(fake.accepts).toBe(2);
    } finally {
      fake.stop();
    }
  });

  test("failed reconnects back off", async () => {
    const fake = fakeServer((s) => s.write(ok));
    const c = await Client.connect(tcp({ port: fake.port, token: "any" }));
    fake.stop(); // hangs up and stops listening
    // The first attempt is immediate and refused; each failure starts the
    // next backoff step (100 ms, then 200 ms). The clock for a step is
    // anchored before the call whose failure starts it, so a loaded machine
    // can only lengthen what is measured, never shorten it.
    let before = Date.now();
    await expect(c.ping()).rejects.toThrow("ECONNREFUSED");
    for (const wait of [100, 200]) {
      const next = Date.now();
      // biome-ignore lint/performance/noAwaitInLoops: the waits are sequential by design
      await expect(c.ping()).rejects.toThrow("ECONNREFUSED");
      expect(Date.now() - before).toBeGreaterThanOrEqual(wait);
      before = next;
    }
    expect(c.isOpen).toBe(false);
  });

  test("close during the backoff wait is final, at once", async () => {
    const fake = fakeServer((s) => s.write(ok));
    const c = await Client.connect(tcp({ port: fake.port, token: "any" }));
    fake.stop();
    // Three refusals put the next wait at 400 ms; close() ends it early.
    for (let i = 0; i < 3; i++) {
      // biome-ignore lint/performance/noAwaitInLoops: the failures are sequential by design
      await expect(c.ping()).rejects.toThrow("ECONNREFUSED");
    }
    const t0 = Date.now();
    const waiting = c.ping();
    await Bun.sleep(10);
    c.close();
    await expect(waiting).rejects.toBeInstanceOf(ClosedError);
    expect(Date.now() - t0).toBeLessThan(200);
  });

  test("concurrent callers share one failed attempt", async () => {
    const fake = goesDown();
    try {
      const c = await Client.connect(tcp({ port: fake.port, token: "any" }));
      // The in-flight call's retry is the first attempt (accept 1).
      await expect(c.ping()).rejects.toBeInstanceOf(ClosedError);
      expect(fake.accepts).toBe(2);
      // Eight callers at once: one attempt, one backoff wait, one failure
      // reported to all of them.
      const t0 = Date.now();
      const results = await Promise.allSettled(
        Array.from({ length: 8 }, () => c.ping()),
      );
      for (const r of results) {
        expect(r.status).toBe("rejected");
        expect((r as PromiseRejectedResult).reason).toBeInstanceOf(ClosedError);
      }
      expect(Date.now() - t0).toBeLessThan(500);
      expect(fake.accepts).toBe(3);
    } finally {
      fake.stop();
    }
  });

  test("a call arriving during the backoff joins the attempt", async () => {
    const fake = goesDown();
    try {
      const c = await Client.connect(tcp({ port: fake.port, token: "any" }));
      await expect(c.ping()).rejects.toBeInstanceOf(ClosedError);
      // The backoff clock starts at that failure: the next attempt is no
      // sooner than 100 ms after it. A call 20 ms into the wait neither
      // fails fast nor connects on its own; it fails with the attempt. (The
      // clock is anchored here, not after the sleep, so a stretched sleep
      // on a loaded machine cannot shrink the window being asserted.)
      const failed = Date.now();
      expect(fake.accepts).toBe(2);
      const early = c.ping().catch((e) => e);
      await Bun.sleep(20);
      await expect(c.ping()).rejects.toBeInstanceOf(ClosedError);
      expect(Date.now() - failed).toBeGreaterThanOrEqual(90);
      expect(await early).toBeInstanceOf(ClosedError);
      expect(fake.accepts).toBe(3);
    } finally {
      fake.stop();
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
      // client loses its connection, which is what proves the pings did
      // the work ...
      const quietT = await tcp({
        port: server.port,
        token: "any",
        keepaliveMs: 300,
      });
      const silentT = await tcp({ port: server.port, token: "any" });
      const quiet = await Client.connect(quietT);
      const silent = await Client.connect(silentT);
      await quiet.set("k", 1);
      await silent.set("k", 1);
      await Bun.sleep(1500);
      expect(await quiet.get<number>("k")).toBe(1);
      expect(silent.isOpen).toBe(false);
      // ... and the next call on it reconnects without being told.
      expect(await silent.get<number>("k")).toBe(1);
      expect(silent.isOpen).toBe(true);
      expect(quietT.reconnects).toBe(0);
      expect(silentT.reconnects).toBe(1);
      quiet.close();
      silent.close();
    } finally {
      await server.stop();
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
    // Hang up on the first PING; answer everything after the reconnect.
    const fake = fakeServer((s, _n, f) => {
      if (f.tag === Op.Ping) s.end();
      else s.write(f.tag === Op.Auth ? ok : oneValue);
    });
    try {
      const t = await tcp({ port: fake.port, token: "any", keepaliveMs: 20 });
      const c = await Client.connect(t);
      await Bun.sleep(150);
      // The failed ping is swallowed and does not reconnect by itself; the
      // next call does.
      expect(c.isOpen).toBe(false);
      expect(fake.accepts).toBe(1);
      expect(await c.get<number>("a")).toBe(1);
      expect(t.reconnects).toBe(1);
      c.close();
    } finally {
      fake.stop();
    }
  });
});

describe("tcp transport over tls", () => {
  let server: TestServer;
  beforeAll(async () => {
    server = await startServer({ OXICACHE_TLS_CERT: SERVER_PEM });
  });
  afterAll(async () => {
    await server.stop();
  });

  test("round trip with a private CA, reconnect repeats the handshake", async () => {
    const t = await tcp({
      port: server.port,
      token: "any",
      tls: { ca: CA_PEM },
    });
    const c = await Client.connect(t);
    await c.set("t", "secure");
    expect(await c.get<string>("t")).toBe("secure");
    const port = server.port;
    await server.stop();
    server = await startServer({ OXICACHE_TLS_CERT: SERVER_PEM }, port);
    expect(await c.get("t")).toBeNull();
    expect(t.reconnects).toBe(1);
    c.close();
  });

  test("an untrusted certificate or a plain client is refused", async () => {
    await expect(
      tcp({ port: server.port, token: "any", tls: true }),
    ).rejects.toThrow();
    await expect(
      tcp({
        port: server.port,
        token: "any",
        tls: { ca: CA_PEM, serverName: "example.com" },
      }),
    ).rejects.toThrow();
    // An IP name is checked against the certificate's IP SANs.
    await expect(
      tcp({
        port: server.port,
        token: "any",
        tls: { ca: CA_PEM, serverName: "10.9.9.9" },
      }),
    ).rejects.toThrow("10.9.9.9 is not in the cert");
    const ip = await tcp({
      port: server.port,
      token: "any",
      tls: { ca: CA_PEM, serverName: "127.0.0.1" },
    });
    ip.close();
    await expect(tcp({ port: server.port, token: "any" })).rejects.toThrow(
      ClosedError,
    );
  });
});
