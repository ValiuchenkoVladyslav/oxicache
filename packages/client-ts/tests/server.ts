import { resolve } from "node:path";
import type { Subprocess } from "bun";

const root = resolve(import.meta.dir, "../../..");
const bin = resolve(root, "target/debug/oxicache-server");
/** The fixture leaf certificate + key (`localhost`, `127.0.0.1`, `::1`). */
export const SERVER_PEM = resolve(root, "testdata/tls/server.pem");
/** The fixture CA that issued it, as PEM text for a client to trust. */
export const CA_PEM = await Bun.file(
  resolve(root, "testdata/tls/ca.pem"),
).text();

let built: Promise<void> | undefined;

/** Bun insists on a `data` handler even for sockets that never receive. */
const ignore = () => {
  // nothing to read
};

/** Build the server once per test process. */
function build(): Promise<void> {
  built ??= (async () => {
    const p = Bun.spawn(["cargo", "build", "-p", "oxicache-server"], {
      cwd: root,
      stdout: "inherit",
      stderr: "inherit",
    });
    if ((await p.exited) !== 0) throw new Error("cargo build failed");
  })();
  return built;
}

/** A loopback port that was free a moment ago. */
function freePort(): number {
  const l = Bun.listen({
    hostname: "127.0.0.1",
    port: 0,
    socket: { data: ignore },
  });
  const port = l.port;
  l.stop(true);
  return port;
}

/** Resolves once something accepts on `port`, rejects after `timeoutMs`. */
async function waitForListen(
  port: number,
  alive: () => boolean,
  timeoutMs = 10_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!alive()) throw new Error("server exited before listening");
    // biome-ignore lint/performance/noAwaitInLoops: polling is sequential by nature
    const ok = await new Promise<boolean>((resolve) => {
      const fail = () => resolve(false);
      Bun.connect({
        hostname: "127.0.0.1",
        port,
        socket: {
          open(s) {
            s.end();
            resolve(true);
          },
          data: ignore,
          error: fail,
          connectError: fail,
        },
      }).catch(fail);
    });
    if (ok) return;
    await Bun.sleep(20);
  }
  throw new Error(`server did not listen on ${port} within ${timeoutMs} ms`);
}

export interface TestServer {
  port: number;
  httpPort: number;
  /** Base URL of the HTTP listener, `https://` when started with `OXICACHE_TLS_CERT`. */
  url: string;
  proc: Subprocess;
  /** Kill the server; resolves once it has exited and its ports are free again. */
  stop(): Promise<void>;
}

/**
 * Start a server on random loopback ports (TCP and HTTP) and wait until it is
 * listening. The server is configured by `OXICACHE_*` variables only; `env`
 * adds to or overrides the defaults, and the token is `any` unless it sets one.
 * `port` pins the TCP port, so a server can be restarted where a client
 * expects it.
 */
export async function startServer(
  env: Record<string, string> = {},
  port = freePort(),
): Promise<TestServer> {
  await build();
  const httpPort = freePort();
  const proc = Bun.spawn([bin], {
    cwd: root,
    stdout: "ignore",
    stderr: "ignore",
    env: {
      ...process.env,
      OXICACHE_TCP_ADDR: `127.0.0.1:${port}`,
      OXICACHE_HTTP_ADDR: `127.0.0.1:${httpPort}`,
      OXICACHE_CAPACITY: "256M",
      OXICACHE_TOKEN: "any",
      ...env,
    },
  });
  const stop = async () => {
    proc.kill();
    await proc.exited;
  };
  try {
    await waitForListen(port, () => proc.exitCode === null);
  } catch (e) {
    await stop();
    throw e;
  }
  const scheme = env.OXICACHE_TLS_CERT ? "https" : "http";
  return {
    port,
    httpPort,
    url: `${scheme}://127.0.0.1:${httpPort}`,
    proc,
    stop,
  };
}
