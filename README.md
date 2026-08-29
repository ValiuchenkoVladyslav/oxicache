# oxicache

In-memory cache server in Rust: [S3-FIFO](https://blog.jasony.me/system/cache/2023/08/01/s3fifo)
eviction, length-prefixed binary frames over plain TCP (and the same frames over HTTP/1.1 for
runtimes without sockets), tokio multi-threaded runtime. No persistence.

## Layout

| path | role |
|---|---|
| `crates/wire` (`oxicache-wire`) | binary request/response framing shared by both sides |
| `crates/server` (`oxicache-server`) | `oxicache-server` binary: sharded S3-FIFO engine + TCP and HTTP front ends |
| `crates/client-rs` (`oxicache-client`) | Rust `Client` library (bytes; any serde type through a caller-supplied `Format` with the `serde` feature) + `oxicache-cli` |
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
Every client always presents one: the Rust `Client::connect(addr, format, token)` and the TS
transports (`tcp({ …, token })`, `http({ …, token })`) take it as a required argument, and the
CLI requires `--token` / `OXICACHE_TOKEN` (it also reads the server address from
`OXICACHE_ADDR`). A server started without a token accepts any. The token travels in clear text — pair it with a private network or a TLS
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
# --token T       require AUTH with this secret (or OXICACHE_TOKEN in the environment);
#                 clients always send one, so set it
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

`get`/`set`/`del` act on one key; `get_multi`/`set_multi`/`del_multi` on several. Keys are
any bytes (`&str`, `String`, `&[u8]`, `Vec<u8>`, `[u8; N]`); a batch of keys is a tuple (up to
16), an array, a `Vec` or a slice, and comes back the same shape — an array for a tuple or
array, a `Vec` for a `Vec` or slice. `Client::connect(addr, format, token)` authenticates as
it connects (a refused token means no client) and fixes what values are for the life of the
client — there is no client without a token or a format: `Raw` for bytes (`Bytes` out,
`AsRef<[u8]>` in), or with `oxicache-client = { features = ["serde"] }` any `Format`, which
makes the same method names take any `Serialize` value and decode into any `DeserializeOwned`
type. The crate ships no data format and never inspects the bytes: `Format` is a two-method
trait — JSON, MessagePack, postcard, … — and only that choice decides what the cache stores.

```rust
struct Json;
impl Format for Json {
    fn encode<T: Serialize + ?Sized>(&self, v: &T) -> Result<Vec<u8>, BoxError> { Ok(serde_json::to_vec(v)?) }
    fn decode<T: DeserializeOwned>(&self, b: &[u8]) -> Result<T, BoxError> { Ok(serde_json::from_slice(b)?) }
}
let client = Client::connect(addr, Json, "s3cret").await?;

#[derive(Serialize, Deserialize)] struct User { id: u64, name: String }
client.set("user:7", User { id: 7, name: "alice".into() }).await?;
let user = client.get::<User>("user:7").await?;                            // Option<User>

// several keys: name the value types, one per key for a tuple, one for all otherwise;
// every slot is an Option, None for a missing key
let (user, hits) = client.get_multi(("user:7", "hits:7")).decode::<(User, u64)>().await?;
let users = client.get_multi(["user:7", "user:8"]).decode::<User>().await?; // [Option<User>; 2]
let users = client.get_multi(ids).decode::<User>().await?;                  // Vec<Option<User>>
let (user, hits): (Option<User>, Option<u64>) =                             // or from the binding
    client.get_multi(("user:7", "hits:7")).decode().await?;
let raw = client.get_multi(ids).await?;                                     // Vec<Option<Bytes>>, any client
client.set_multi([("a", 1), ("b", 2)]).await?;
let [a, b] = client.del_multi(("a", "b")).await?;
```

A tuple of keys must be paired with a tuple of exactly as many value types; a mismatch does
not compile. A format failure surfaces as `Error::Serialize` / `Error::Deserialize`; a
`Client::connect(addr, Raw, token)` reads the stored bytes as they are.

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
