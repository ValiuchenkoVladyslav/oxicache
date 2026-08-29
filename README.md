# oxicache

In-memory cache server in Rust: [S3-FIFO](https://blog.jasony.me/system/cache/2023/08/01/s3fifo)
eviction, length-prefixed binary frames over plain TCP (and the same frames over HTTP/1.1 for
runtimes without sockets), tokio multi-threaded runtime. No persistence.

## Layout

| path | role |
|---|---|
| `crates/wire` (`oxicache-wire`) | binary request/response framing shared by both sides |
| `crates/server` (`oxicache-server`) | `oxicache-server` binary: sharded S3-FIFO engine + TCP and HTTP front ends |
| `crates/client-rs` (`oxicache-client`) | Rust `Client` library (bytes, or any serde type with the `serde` feature) + `oxicache-cli` |
| `packages/client-ts` (`@oxicache/client`) | TypeScript `Client` library: TCP transport for Bun, HTTP transport for anything with `fetch` |

## Protocol

Three commands, all batch-only (a single op is a batch of one), as length-prefixed frames
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
status    := 0 ok | 1 bad request | 2 unknown op | 3 too large | 4 unauthorized (body = message)
```

If the server is started with a token (`--token` or `OXICACHE_TOKEN`), AUTH must be the
first request on every connection; anything else gets status 4 and the connection is closed.
The client library does this in `Client::connect_with_token`, the CLI via `--token` /
`OXICACHE_TOKEN` (the CLI also reads the server address from `OXICACHE_ADDR`). The token travels in clear text — pair it with a private network or a TLS
tunnel.

### HTTP

With `--http-addr` (or `OXICACHE_HTTP_ADDR`) the server also listens for HTTP/1.1, carrying the
same binary bodies — nothing is JSON. The op is the path, the request body is the frame body,
the response body is the frame body, and the frame status becomes the HTTP status:

```
POST /get   body: keys      -> 200, body: values
POST /set   body: entries   -> 200, empty
POST /del   body: keys      -> 200, body: flags
GET  /health                -> 200, no body; never needs a token
400 bad request | 401 unauthorized | 404 unknown path | 405 wrong method | 413 too large
```

With a token, every request except `/health` carries `Authorization: Bearer <token>`; a refused
request gets a 401 and its body is never read, so the connection closes. Both listeners serve one
cache, so a value written over TCP is readable over HTTP. Keep-alive is on; there is no TLS and
no CORS handling — put a reverse proxy in front for either.

## Run

```sh
cargo run --release -p oxicache-server -- --addr 0.0.0.0:4433 --http-addr 0.0.0.0:4434 --capacity 1G
# --shards N      independent S3-FIFO shards (default: CPUs)
# --token T       require AUTH with this secret (or OXICACHE_TOKEN in the environment)
# --http-addr A   also serve the HTTP API on A (off unless given)
# every server flag has an environment variable: OXICACHE_ADDR, OXICACHE_HTTP_ADDR,
# OXICACHE_CAPACITY, OXICACHE_SHARDS, OXICACHE_TOKEN; a flag wins over a differing
# variable, with a warning

cargo run --release -p oxicache-client -- set a 1 b 2
cargo run --release -p oxicache-client -- get a b c
cargo run --release -p oxicache-client -- del a
cargo run --release -p oxicache-client -- bench --conns 8 --pipeline 16 --batch 16
```

### Rust client API

`get`/`set`/`del` act on one key; `get_multi`/`set_multi`/`del_multi` on several. Without
features they take bytes; with `oxicache-client = { features = ["serde"] }` the same names take
any `Serialize` key or value and decode into any `DeserializeOwned` type (MessagePack via
`rmp-serde`), and the byte API stays available as `client.raw()`:

```rust
#[derive(Serialize, Deserialize)] struct User { id: u64, name: String }
client.set("user:7", User { id: 7, name: "alice".into() }).await?;
let user: Option<User> = client.get("user:7").await?;

// several keys: a tuple gives one value type per key, an array or Vec one type for all
let (user, hits): (Option<User>, Option<u64>) = client.get_multi(("user:7", "hits:7")).await?;
let users: [Option<User>; 2] = client.get_multi(["user:7", "user:8"]).await?;
let users: Vec<Option<User>> = client.get_multi(ids).await?;
client.set_multi([("a", 1), ("b", 2)]).await?;
let [a, b] = client.del_multi(("a", "b")).await?;
```

A tuple of keys must be paired with a tuple of exactly as many value types (up to 16); a
mismatch does not compile. Typed keys are MessagePack-encoded, so `"k"` stored through the
typed API is a different key from `b"k"` stored through `client.raw()`.

## TypeScript client

`packages/client-ts` is a `Client` over a pluggable `Transport`, each transport on its own
subpath so a bundle only carries the one it imports (the package is `sideEffects: false`):

- `@oxicache/client/transport/tcp` — `Bun.connect`, with the same framing, pipelining and
  in-order response matching as the Rust client. Bun only.
- `@oxicache/client/transport/http` — one `fetch` per call against the server's HTTP listener.
  Runs wherever `fetch` does: Bun, Node 18+, edge runtimes, lambdas. Importing it, or the main
  entry, never touches Bun's socket API.

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

## Development

```sh
bun install            # installs the husky pre-commit hook
bun test               # client-ts unit + e2e tests over both transports (builds and spawns the debug server)
cargo test --workspace --all-features # wire, server and client-rs unit + e2e tests
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
  Frames are capped at 64 MiB each way; there is no connection limit or idle timeout,
  so put the server on a private network.
- 64-bit targets only: index slots pack a 48-bit entry address next to a 16-bit tag.
- The client pipelines calls from any number of tasks onto one connection (writer task
  coalesces queued frames into one flush; reader task matches responses in order).
