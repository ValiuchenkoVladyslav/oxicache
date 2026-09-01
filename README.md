# oxicache

In-memory cache server in Rust: [S3-FIFO](https://s3fifo.com) eviction, length-prefixed binary
frames over plain TCP (and the same frames over HTTP/1.1 for runtimes without sockets), tokio
multi-threaded runtime. No persistence.

| path | role |
|---|---|
| `crates/wire` (`oxicache-wire`) | binary request/response framing shared by both sides |
| `crates/server` (`oxicache-server`) | `oxicache-server` binary: sharded S3-FIFO engine + TCP and HTTP front ends |
| `crates/client-rs` (`oxicache-client`) | Rust `Client` library (any serde type, stored as MessagePack), `Cluster` over several servers + `oxicache-cli` |
| `packages/client-ts` (`@oxicache/client`) | TypeScript `Client` library: TCP transport for Node and Bun, HTTP transport for anything with `fetch`, `cluster` over several servers |

## Features

- **S3-FIFO eviction.** A sharded, mostly lock-free engine (one shard per CPU) under a fixed
  memory budget: reads take no lock, writes take their shard's mutex only.
- **Protocols.** A binary TCP protocol for anything that can open a socket, and the same frames
  over HTTP/1.1 for edge runtimes and lambdas that only have `fetch`. A value written over TCP
  is readable over HTTP.
- **Pipelining and batches.** Requests on a connection are answered in order, so clients
  pipeline freely; a `BATCH` carries up to 65 536 GET/SET/DEL ops in one round trip and answers
  each one exactly as it would be answered alone — one refused item does not affect its
  neighbours.
- **Token auth, optionally over TLS.** A shared token is required on every connection; there is
  no unauthenticated mode. `OXICACHE_TLS_CERT` puts both listeners on TLS 1.3.
- **Prometheus metrics.** `GET /metrics` reports hits/misses, evictions, auth failures, bytes
  and items used against capacity, open connections, and uptime.
- **Encoding.** The Rust and TypeScript clients both store values as plain MessagePack, so they
  read each other's data. Rust takes any `Serialize`/`DeserializeOwned` type; TypeScript takes
  any MessagePack-representable value, checked at the type level.
- **Clusters.** Several servers behind one keyspace, spread by consistent hashing in the client
  — no proxy, no replication, exactly like memcached. Both clients implement the same ring, so
  they agree on which server owns a key.
- **Reconnects and failover.** TCP clients reconnect lazily on connection loss and re-issue the
  in-flight calls once, with backoff; a cluster drops a server that keeps failing and lets it
  back in later.
- **Bounded by default.** 10 000 connections and a 300 s idle timeout out of the box, 64 MiB
  frames, and an authentication deadline on every new connection.

## Run

The server is configured by environment variables only; it never reads its command line.

```sh
OXICACHE_TOKEN=s3cret OXICACHE_HTTP_ADDR=0.0.0.0:4434 OXICACHE_CAPACITY=1G cargo run --release -p oxicache-server
```

| variable | default | meaning |
|---|---|---|
| `OXICACHE_TCP_ADDR` | `0.0.0.0:4433` | address the TCP front end listens on |
| `OXICACHE_HTTP_ADDR` | unset (HTTP off) | also serve the HTTP API on this address |
| `OXICACHE_CAPACITY` | `1G` | memory budget for cached entries; `K`/`M`/`G` suffixes |
| `OXICACHE_TOKEN` | required | the shared secret; must not be empty |
| `OXICACHE_IDLE_TIMEOUT` | `300` | close a connection that sends nothing for this many seconds |
| `OXICACHE_MAX_CONNS` | `10000` | at most this many open connections, TCP and HTTP together |
| `OXICACHE_TLS_CERT` | unset (plain) | PEM file with the server's certificate chain and private key; set, both listeners speak TLS |

SIGINT or SIGTERM stops accepting and drains open connections before exit.

On glibc systems, run the server with `GLIBC_TUNABLES=glibc.malloc.tcache_count=1024` — worth up
to −20 % server CPU per request under eviction-heavy load (docs/performance.md). The `Dockerfile`
already sets it, so only bare-metal deployments need to.

### Container

The `Dockerfile` builds the release binary and ships it on `debian:bookworm-slim`; it works with
podman and docker alike:

```sh
podman build -t oxicache .
podman run --rm --network host -e OXICACHE_TOKEN=s3cret oxicache
```

Prefer `--network host` for a cache: rootless port mapping routes every byte through the `pasta`
userspace proxy, which caps throughput well below what the server can do (docs/performance.md).
With TLS, mount the PEM and point `OXICACHE_TLS_CERT` at it:
`-v ./server.pem:/server.pem:ro,Z -e OXICACHE_TLS_CERT=/server.pem`.

### CLI

```sh
cargo run --release -p oxicache-client -- set a 1 b 2
cargo run --release -p oxicache-client -- get a b c
cargo run --release -p oxicache-client -- del a
cargo run --release -p oxicache-client -- bench --conns 8 --pipeline 16 --batch 16
cargo run --release -p oxicache-client -- --addr cache.internal:4433 --tls-ca ca.pem get a
```

It reads the token from `--token` / `OXICACHE_TOKEN` and the address from `--addr` /
`OXICACHE_ADDR`.

## Rust client

`get`/`set`/`del` act on one key; `batch` sends any mix of them in one round trip. Keys are any
bytes; values are any `Serialize` type in and any `DeserializeOwned` type out.

```rust
let client = Client::connect(addr, "s3cret").await?;
let secure = Client::connect_tls(addr, Tls::trusting(Path::new("ca.pem"), "cache.internal".try_into()?)?, "s3cret").await?;

#[derive(Serialize, Deserialize)] struct User { id: u64, name: String }
client.set("user:7", User { id: 7, name: "alice".into() }).await?;
let user = client.get::<User>("user:7").await?;      // Option<User>
let gone = client.del("user:7").await?;              // bool

// any mix of ops in one round trip, answered in order; each op hands back a
// typed Slot that reads its own result out of the Outcome
let mut b = Batch::new();
let user = b.get::<User>("user:7");                  // Slot<Option<User>>
b.set("seen:7", now)?;                               // Slot<()>; encodes here
let hits = b.get::<u64>("hits:7");
let dropped = b.del("tmp:7");                        // Slot<bool>
let out = client.batch(b).await?;
let (user, hits, dropped) = (out.get(user)?, out.get(hits)?, out.get(dropped)?);
```

A missing key is `None`/`false`, never an error. A refused op is that slot's `Error::Status`
while the other slots stay readable. `client.ping()` round-trips an empty request; the client
also pings by itself on a quiet connection so the server's idle timeout does not close it.
Dropping the last clone closes the connection.

## TypeScript client

`packages/client-ts` is a `Client` over a pluggable `Transport`, each transport on its own subpath
so a bundle only carries the one it imports:

- `@oxicache/client/transport/tcp` — a `node:net` socket (`node:tls` with `tls`), with the same
  framing and pipelining as the Rust client. Node 18+ and Bun.
- `@oxicache/client/transport/http` — one `fetch` per call against the server's HTTP listener.
  Runs wherever `fetch` does: Bun, Node 18+, edge runtimes, lambdas.

Keys are `string` (UTF-8) or `Uint8Array`; values are any MessagePack-representable data, encoded
with [`@msgpack/msgpack`](docs/msgpack-bench.md) and checked at the type level.

```ts
import { Client, op } from "@oxicache/client";
import { tcp } from "@oxicache/client/transport/tcp";
import { http } from "@oxicache/client/transport/http";

const c = await Client.connect(tcp({ hostname: "127.0.0.1", port: 4433, token: "s3cret" }));
// or, from an edge function:
const c = await Client.connect(http({ url: "http://cache.internal:4434", token: "s3cret" }));
// over TLS, with a private CA:
const c = await Client.connect(tcp({ hostname: "cache.internal", port: 4433, token: "s3cret", tls: { ca } }));
const c = await Client.connect(http({ url: "https://cache.internal:4434", token: "s3cret", tls: { ca } }));

await c.set("user:7", { id: 7, name: "alice", joined: new Date() });
const u = await c.get<User>("user:7");   // User | null
const gone = await c.del("user:7");      // boolean

// any mix of ops in one round trip, answered in order and typed by position
const [user, , hits, dropped] = await c.batch([
  op.get<User>("user:7"),                // User | null
  op.set("seen:7", Date.now()),          // undefined
  op.get<number>("hits:7"),              // number | null
  op.del("tmp:7"),                       // boolean
]);
const users = await c.batch(ids.map((id) => op.get<User>(`user:${id}`)));  // (User | null)[]
c.close();
```

A missing key is `null`/`false`, never an error; a non-OK status rejects with `StatusError` and a
closed transport with `ClosedError`. `http({ fetch })` takes a custom `fetch` for agents or tests.

## Clusters

Several servers, one keyspace: each key belongs to exactly one of them, chosen by consistent
hashing in the client. The servers know nothing of each other — there is no proxy, no replication
and no data failover, exactly as with memcached.

```rust
let cache = Cluster::connect(
    ["10.0.0.1:4433".parse()?, "10.0.0.2:4433".parse()?, "10.0.0.3:4433".parse()?],
    "s3cret",
).await?;
cache.set("user:7", &user).await?;                   // goes to whichever server owns the key
let user = cache.get::<User>("user:7").await?;
let out = cache.batch(b).await?;                     // one request per server, answered in order
cache.node_for("user:7");                            // 10.0.0.2:4433
cache.live();                                        // the servers still in the ring
```

```ts
import { Client, cluster } from "@oxicache/client";
import { tcp } from "@oxicache/client/transport/tcp";

const servers = await cluster({
  nodes: ["10.0.0.1", "10.0.0.2", "10.0.0.3"].map((hostname) => ({
    name: `${hostname}:4433`,                          // the ring identity: address as written
    open: () => tcp({ hostname, port: 4433, token: "s3cret" }),
  })),
});
const c = await Client.connect(servers);              // the cluster is itself a Transport
await c.set("user:7", user);
servers.nodeFor("user:7");                            // "10.0.0.2:4433"
servers.live();
```

A cluster has the same API as one server, one pipelined connection per server, and is cheap to
clone or share. Both clients implement the same ring and are pinned to the same answers by
`testdata/ring.tsv`, so they hit the same server for a key — as long as they are given the same
servers under the same **names**: the Rust client names a server by its `SocketAddr` printed, and
the TypeScript client by the `name` given, character for character.

After a few connection failures in a row a server is dropped from the ring, its keys go to the
next server round, and it is let back in later; one answer of any kind puts it back at once.
Moving keys is the memcached trade, not a free lunch: a key that moves is cold on its new server,
and entries can go stale across an outage, so failover can be turned off where that matters.

## TLS

`OXICACHE_TLS_CERT=/path/to/server.pem` is the whole switch: the file holds the certificate chain
(leaf first) and the private key, both PEM, and its presence makes both listeners speak TLS 1.3.
Clients are not asked for a certificate; the token still authenticates them, now encrypted.

Every client takes the same two things: what to trust (a private CA's PEM, or the runtime's roots)
and the name the certificate must be issued for.

- `oxicache-cli --addr host:port --tls-ca ca.pem …`
- Rust: `Client::connect_tls(addr, Tls::trusting(ca_path, server_name)?, token)`, or a `Tls` built
  from any `rustls::ClientConfig`.
- TypeScript: `tcp({ …, tls: true })` for the system roots or `tcp({ …, tls: { ca } })` for a
  private CA (`serverName` when the certificate is not for `hostname`); `http({ url: "https://…",
  tls: { ca } })` — Bun honours `tls`, other runtimes take a custom `fetch` or
  `NODE_EXTRA_CA_CERTS`.

`testdata/tls` holds a CA and a certificate for `localhost` / `127.0.0.1` / `::1` that the test
suites use; they are for tests only.

## Development

```sh
bun install            # installs the husky pre-commit hook
bun test               # client-ts unit + e2e tests over both transports (builds and spawns the debug server)
cargo test --workspace # wire, server and client-rs unit + e2e tests
```

The pre-commit hook (`.husky/pre-commit`) runs `cargo fmt --check`, `clippy -D warnings`,
`cargo build`, `cargo test` (all features), then the TypeScript type check, `bun build` and `bun test`.
