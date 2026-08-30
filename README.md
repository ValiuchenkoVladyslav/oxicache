# oxicache

In-memory cache server in Rust: [S3-FIFO](https://blog.jasony.me/system/cache/2023/08/01/s3fifo)
eviction, length-prefixed binary frames over plain TCP (and the same frames over HTTP/1.1 for
runtimes without sockets), tokio multi-threaded runtime. No persistence.

## Layout

| path | role |
|---|---|
| `crates/wire` (`oxicache-wire`) | binary request/response framing shared by both sides |
| `crates/server` (`oxicache-server`) | `oxicache-server` binary: sharded S3-FIFO engine + TCP and HTTP front ends |
| `crates/client-rs` (`oxicache-client`) | Rust `Client` library (any serde type, stored as MessagePack) + `oxicache-cli` |
| `packages/client-ts` (`@oxicache/client`) | TypeScript `Client` library: TCP transport for Node and Bun, HTTP transport for anything with `fetch` |

## Protocol

Three commands, all batch-only (a single op is a batch of one), plus AUTH and PING, as length-prefixed frames
on a TCP connection. Requests on one connection are answered in order, so clients pipeline
freely. Values are opaque bytes. Integers are little-endian.

```
request   := u8 op, u32 len, body          response := u8 status, u32 len, body
keys      := u32 count, count × (u32 len, bytes)
entries   := u32 count, count × (u32 klen, key, u32 vlen, value)

op 1 GET  body: keys     -> u32 count, count × (u8 0 | u8 1, u32 len, value)
op 2 SET  body: entries  -> empty
op 3 DEL  body: keys     -> u32 count, count × u8 found
op 4 AUTH body: token    -> empty
op 5 PING body: empty    -> empty
status    := 0 ok | 1 bad request | 2 unknown op | 3 too large | 4 unauthorized (body = message)
```

The server requires a non-empty token (`OXICACHE_TOKEN`); AUTH must be the
first request on every connection, and anything else (PING included) gets status 4 and the
connection is closed. A TCP connection that has not authenticated within 30 s of being
accepted is dropped; that cap is fixed, not a setting. There is no unauthenticated mode on
either side: the Rust
`Client::connect(addr, token)` and the TS transports (`tcp({ …, token })`,
`http({ …, token })`) take the token as a required argument, and the CLI requires `--token` /
`OXICACHE_TOKEN` (it also reads the server address from `OXICACHE_ADDR`). Without TLS the token
travels in clear text — set `OXICACHE_TLS_CERT` (see [TLS](#tls)) or stay on a private network.

A connection that sends nothing for `OXICACHE_IDLE_TIMEOUT` (default 300 s) is closed, so both TCP
clients PING on their own after 100 s without a write — a third of that default, so two lost
heartbeats still leave the connection open. PING is authenticated like everything else and
its body is ignored.

### HTTP

With `OXICACHE_HTTP_ADDR` set the server also listens for HTTP/1.1, carrying the
same binary bodies — nothing is JSON. The op is the path, the request body is the frame body,
the response body is the frame body, and the frame status becomes the HTTP status:

```
POST /get   body: keys      -> 200, body: values
POST /set   body: entries   -> 200, empty
POST /del   body: keys      -> 200, body: flags
POST /ping                  -> 200, empty
GET  /health                -> 200, no body; never needs a token
400 bad request | 401 unauthorized | 404 unknown path | 405 wrong method | 413 too large
```

Every request except `/health` carries `Authorization: Bearer <token>`. Every response sent
before the token has been verified — `/health`, 401, 404, 405 — carries `Connection: close`,
so no connection stays open without the token. Both listeners serve one
cache, so a value written over TCP is readable over HTTP. Keep-alive is on; with
`OXICACHE_TLS_CERT` the listener is `https://`. There is no CORS handling — put a reverse proxy
in front for that.

## Run

The server is configured by environment variables only; it never reads its command line.

```sh
OXICACHE_TOKEN=s3cret OXICACHE_HTTP_ADDR=0.0.0.0:4434 OXICACHE_CAPACITY=1G cargo run --release -p oxicache-server
```

| variable | default | meaning |
|---|---|---|
| `OXICACHE_ADDR` | `0.0.0.0:4433` | address to listen on |
| `OXICACHE_HTTP_ADDR` | unset (HTTP off) | also serve the HTTP API on this address |
| `OXICACHE_CAPACITY` | `1G` | memory budget for cached entries; `K`/`M`/`G` suffixes |
| `OXICACHE_SHARDS` | available CPUs | independent S3-FIFO shards |
| `OXICACHE_TOKEN` | required | the shared secret; must not be empty |
| `OXICACHE_IDLE_TIMEOUT` | `300` | close a connection that sends nothing for this many seconds |
| `OXICACHE_MAX_CONNS` | `10000` | at most this many open connections, TCP and HTTP together; beyond it the listeners stop accepting until one closes |
| `OXICACHE_TLS_CERT` | unset (plain) | PEM file with the server's certificate chain and private key; set, both listeners speak TLS |

A missing token, an unparseable value or an unreadable certificate file is a startup error
naming the variable. SIGINT or SIGTERM stops accepting and drains open connections before exit.

```sh
cargo run --release -p oxicache-client -- set a 1 b 2
cargo run --release -p oxicache-client -- get a b c
cargo run --release -p oxicache-client -- del a
cargo run --release -p oxicache-client -- bench --conns 8 --pipeline 16 --batch 16
cargo run --release -p oxicache-client -- --addr cache.internal:4433 --tls-ca ca.pem get a
```

### TLS

`OXICACHE_TLS_CERT=/path/to/server.pem` is the whole switch: the file holds the certificate
chain (leaf first) and the private key, both PEM, and its presence makes the TCP listener and
the HTTP listener (`https://`) speak TLS 1.3 — there is no per-listener setting and no
plain-text fallback. On TCP the handshake counts against the 30 s cap on unauthenticated
connections; on HTTP it has the same 30 s, then hyper's header timeout applies as before.
Clients are not asked for a certificate; the token still authenticates them, now encrypted.

Every client takes the same two things: what to trust (a private CA's PEM, or the runtime's
roots) and the name the certificate must be issued for (a DNS name or an IP).

- `oxicache-cli --addr host:port --tls-ca ca.pem …` — TLS on, trusting the CA(s) in `ca.pem`,
  verifying the certificate against `host`.
- Rust: `Client::connect_tls(addr, Tls::trusting(ca_path, server_name)?, token)`, or a `Tls`
  built from any `rustls::ClientConfig`.
- TypeScript: `tcp({ …, tls: true })` for the system roots or `tcp({ …, tls: { ca } })` for a
  private CA (`serverName` when the certificate is not for `hostname`); `http({ url:
  "https://…", tls: { ca } })` — Bun honours `tls`, other runtimes take a custom `fetch` or
  `NODE_EXTRA_CA_CERTS`.

`testdata/tls` holds a CA and a certificate for `localhost` / `127.0.0.1` / `::1` that the test
suites use; they are for tests only.

### Rust client API

`get`/`set`/`del` act on one key; `get_multi`/`set_multi`/`del_multi` on several. Keys are
any bytes (`&str`, `String`, `&[u8]`, `Vec<u8>`, `[u8; N]`); a batch of keys is a tuple (up to
16), an array, a `Vec` or a slice, and comes back the same shape — an array for a tuple or
array, a `Vec` for a `Vec` or slice. `Client::connect(addr, token)` authenticates as it
connects (a refused token means no client). Values are any `Serialize` type in and any
`DeserializeOwned` type out, stored as MessagePack through `rmp-serde` with structs as maps —
the same encoding the TypeScript client writes, so both clients read each other's values
(`Vec<u8>` is a MessagePack array; wrap it in `serde_bytes` for a `bin`). `get` and
`get_multi` decode as part of the call: the type argument names the value types only.
`Client::connect_tls(addr, tls, token)` is the same over TLS: `Tls::trusting(ca, server_name)`
trusts a PEM file of CA certificates, or fill `Tls { server_name, config }` with your own
`rustls::ClientConfig`; a refused certificate is an `Error::Io`.

```rust
let client = Client::connect(addr, "s3cret").await?;
let secure = Client::connect_tls(addr, Tls::trusting(Path::new("ca.pem"), "cache.internal".try_into()?)?, "s3cret").await?;

#[derive(Serialize, Deserialize)] struct User { id: u64, name: String }
client.set("user:7", User { id: 7, name: "alice".into() }).await?;
let user = client.get::<User>("user:7").await?;                                   // Option<User>

// several keys: one value type per key for a tuple, one for all otherwise;
// every slot is an Option, None for a missing key
let (user, hits) = client.get_multi::<(User, u64), _>(("user:7", "hits:7")).await?;
let users = client.get_multi::<User, _>(["user:7", "user:8"]).await?;             // [Option<User>; 2]
let users = client.get_multi::<User, _>(ids).await?;                              // Vec<Option<User>>
let (user, hits): (Option<User>, Option<u64>) =                                   // or from the binding
    client.get_multi(("user:7", "hits:7")).await?;
client.set_multi([("a", 1), ("b", 2)]).await?;
let [a, b] = client.del_multi(("a", "b")).await?;
```

A tuple of keys must be paired with a tuple of exactly as many value types; a mismatch does
not compile. An encoding failure surfaces as `Error::Serialize`, a stored value that is not
the named type as `Error::Deserialize`; the connection stays usable after either.
`client.ping()` round-trips an empty request; the client also pings by itself after 100 s
(`oxicache_wire::KEEPALIVE`) without a write, so a quiet connection survives the server's
300 s idle timeout.

A connection lost to the server or the network (EOF, reset, any socket I/O error — the
server's idle close, a restart) is reconnected lazily: nothing happens in the background, the
next call re-connects and re-authenticates, and the calls that were in flight are re-issued
once on the new connection (every op is idempotent); if that connection is lost too they fail
with `Error::Closed`. Concurrent callers wait on the one attempt; failed attempts back off
from 100 ms, doubling to 5 s, reset after a success, and a call that arrives during the wait
waits too. Nothing else reconnects: a refused token on reconnect (rotated) makes the client
dead — every later call fails with that `Unauthorized` error — and so does a server that
answers out of protocol (unsolicited frame, invalid status byte). A status error, a
`Serialize`/`Deserialize` failure or a `ResponseTooLarge` reply is the call's alone and leaves
the client usable. Dropping the last clone closes the connection.

## TypeScript client

`packages/client-ts` is a `Client` over a pluggable `Transport`, each transport on its own
subpath so a bundle only carries the one it imports (the package is `sideEffects: false`):

- `@oxicache/client/transport/tcp` — a `node:net` socket (`node:tls` with `tls`), with the
  same framing, pipelining and in-order response matching as the Rust client. Node 18+ and
  Bun.
- `@oxicache/client/transport/http` — one `fetch` per call against the server's HTTP listener.
  Runs wherever `fetch` does: Bun, Node 18+, edge runtimes, lambdas. Importing it, or the main
  entry, never touches a socket API.

Keys are `string` (UTF-8) or `Uint8Array`; values are any MessagePack-representable data
(`null`, booleans, numbers, `bigint`, strings, `Uint8Array`, `Date`, arrays, plain objects,
class instances), encoded with [`@msgpack/msgpack`](docs/msgpack-bench.md) as plain
MessagePack (`bigint` as extension type 0 holding its decimal string) and checked at the
type level — a value containing a function, `symbol`, `undefined`, `Map` or `Set` is a compile
error.

```ts
import { Client } from "@oxicache/client";
import { tcp } from "@oxicache/client/transport/tcp";
import { http } from "@oxicache/client/transport/http";

const c = await Client.connect(tcp({ hostname: "127.0.0.1", port: 4433, token: "s3cret" }));
// or, from an edge function:
const c = await Client.connect(http({ url: "http://cache.internal:4434", token: "s3cret" }));
// over TLS, with a private CA:
const c = await Client.connect(tcp({ hostname: "cache.internal", port: 4433, token: "s3cret", tls: { ca } }));
const c = await Client.connect(http({ url: "https://cache.internal:4434", token: "s3cret", tls: { ca } }));

await c.set("user:7", { id: 7, name: "alice", joined: new Date() });  // one
await c.set(["a", 1], ["b", ["x", null]]);                             // several
await c.set(entries);                                                  // Entry[] of any length

const u = await c.get<User>("user:7");          // User | null
const [u7, n] = await c.get<[User, number]>("user:7", "n"); // one type per key, length checked
const vs = await c.get<number>(someKeys);       // (number | null)[] for a runtime-length array
const d = await c.del("a");                     // boolean
const ds = await c.del("a", "b");               // [boolean, boolean]
c.close();
```

One key in, one result out; several keys in, a tuple of that length out (up to 16 literal
keys); an array in, an array out for lengths only known at runtime — all enforced by
overloads, so `...spread` of a plain array is a compile error (pass the array). The return
type parameter says what stored values decode to and is not checked at runtime; with several
keys it is a tuple with exactly one type per key. A non-OK status rejects with `StatusError`
(`.status` is the `Status` enum), a closed transport with `ClosedError`. On TCP, calls issued in
the same tick are coalesced into one write; on HTTP each call is its own request and a refused
one does not end the transport. `http({ fetch })` takes a custom `fetch` for agents or tests.
`c.ping()` round-trips an empty request (`POST /ping` on HTTP). The TCP transport also pings
by itself after `keepaliveMs` (default 100 000, a third of the server's 300 s idle timeout)
without a write, on an unref'd timer, so a quiet connection stays open without keeping the
process alive; HTTP needs no heartbeat, since each call is its own request.

The TCP transport reconnects lazily when the connection is lost (closed by the server or the
network: idle timeout, restart, reset): the next call re-connects and re-authenticates, and
in-flight calls are re-issued once on the new connection, failing with `ClosedError` if that
one is lost too. Concurrent callers wait on the one attempt, with a 100 ms → 5 s exponential
backoff between failed attempts (reset after a success); `isOpen` is false while the
connection is down. It never reconnects on a refused token (the transport is dead and every
later call rejects with that `StatusError`), on a protocol desync (an invalid status byte or
an unsolicited frame — dead with `ClosedError`), on any other `StatusError` or encode error
(the call fails, the connection stays), or after `close()`, which is final.

## Development

```sh
bun install            # installs the husky pre-commit hook
bun test               # client-ts unit + e2e tests over both transports (builds and spawns the debug server)
cargo test --workspace # wire, server and client-rs unit + e2e tests
```

The pre-commit hook (`.husky/pre-commit`) runs `cargo fmt --check`, `clippy -D warnings`,
`cargo build`, `cargo test` (all features), then the TypeScript type check, `bun build` and `bun test`.

## Design

- Per-shard cuckoo hash index: buckets are one cache line of eight slots, each slot a
  16-bit hash tag plus the 48-bit address of an entry (refcount, metadata, key and value in
  one allocation; values ≥ 1 KiB are copied in with non-temporal stores, since their
  destination is a cold recycled chunk). A lookup is bucket → entry, two dependent memory accesses; both
  candidate buckets derive from the hash alone, so a batch get prefetches all of them
  before touching any, then all entries. Tags reject absent keys without an entry access.
- Readers pin an epoch (crossbeam-epoch) once per batch and borrow entries under it — no
  locks and no refcount traffic per key. The shard mutex is the single writer; cuckoo
  displacement copies before it clears and is bracketed by a seqlock that a missing
  lookup re-checks, so a present key is never invisible. Replaced and
  evicted entries are retired through the epoch collector and reclaimed after every write
  batch, so memory stays bounded under sustained writes.
- Keys hash once (`rapidhash::fast`, randomly seeded); top bits pick one of N S3-FIFO shards (default: CPU count),
  each a mutex over its small/main/ghost queues, used only by writes and eviction. Reads
  never lock; they bump a relaxed atomic frequency counter capped at 3.
- Entries are immutable; delete/overwrite marks them dead and they are skipped lazily at
  the queue head, with compaction once dead bytes exceed 25 % of the shard budget.
- One tokio task per TCP connection; requests are handled inline and answered in order,
  responses are flushed once no more input is buffered (one write per pipelined batch).
  One listener; accepted connections are spread over the runtime's worker threads. The
  HTTP front end (hyper, HTTP/1.1) is a second listener over the same dispatch, one request
  per exchange; bodies are bounded to the frame limit before and while reading.
  Frames are capped at 64 MiB each way. `OXICACHE_MAX_CONNS` is one semaphore shared by both
  listeners, taken before `accept` so an over-limit peer waits in the kernel backlog
  instead of being accepted and dropped; `OXICACHE_IDLE_TIMEOUT` closes a TCP connection that
  sends nothing for that long (hyper's per-request header timeout does the same for
  HTTP keep-alive). Defaults: 10 000 connections, 300 s idle; the library `Options` can
  lift either with `None`, the binary cannot. TCP clients send a PING after 100 s without
  a write (`oxicache_wire::KEEPALIVE`; the server default is three of them), so a quiet
  connection is not mistaken for a dead one. `Options::tls` (the binary: `OXICACHE_TLS_CERT`)
  wraps both listeners in rustls (TLS 1.3, `ring`); the frame loop is generic over the
  stream, so the plain path is unchanged and a TLS connection is split with `tokio::io::split`
  instead of `into_split`. Still put the server on a private network.
- 64-bit targets only: index slots pack a 48-bit entry address next to a 16-bit tag.
- The client pipelines calls from any number of tasks onto one connection (writer task
  coalesces queued frames into one flush; reader task matches responses in order).
- Both TCP clients reconnect on connection loss only: lazily, on the next call, one shared
  attempt at a time, with a 100 ms → 5 s backoff; in-flight calls are re-issued once on the
  new connection. Never on an auth, status, encode or protocol error, and never after
  `close()`.
