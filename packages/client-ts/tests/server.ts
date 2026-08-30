import { resolve } from "node:path";
import type { Subprocess } from "bun";

const root = resolve(import.meta.dir, "../../..");
const bin = resolve(root, "target/debug/oxicache-server");

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
  /** Base URL of the HTTP listener. */
  url: string;
  proc: Subprocess;
  stop(): void;
}

/**
 * Start a server on random loopback ports (TCP and HTTP) and wait until it is
 * listening. The token is `any` unless `args` set one.
 */
export async function startServer(args: string[] = []): Promise<TestServer> {
  await build();
  const token = args.includes("--token") ? [] : ["--token", "any"];
  const port = freePort();
  const httpPort = freePort();
  const proc = Bun.spawn(
    [
      bin,
      "--addr",
      `127.0.0.1:${port}`,
      "--http-addr",
      `127.0.0.1:${httpPort}`,
      "--capacity",
      "64M",
      "--shards",
      "2",
      ...token,
      ...args,
    ],
    { cwd: root, stdout: "ignore", stderr: "ignore" },
  );
  const stop = () => proc.kill();
  try {
    await waitForListen(port, () => proc.exitCode === null);
  } catch (e) {
    stop();
    throw e;
  }
  return { port, httpPort, url: `http://127.0.0.1:${httpPort}`, proc, stop };
}
