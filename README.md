# oxicache

In-memory cache server in Rust: [S3-FIFO](https://blog.jasony.me/system/cache/2023/08/01/s3fifo)
eviction, length-prefixed frames over plain TCP, tokio multi-threaded runtime. No persistence.

## Layout

| path | role |
|---|---|
| `crates/wire` (`oxicache-wire`) | binary request/response framing shared by both sides |
| `crates/server` (`oxicache-server`) | `oxicache-server` binary: sharded S3-FIFO engine + TCP front end |
| `crates/client-rs` (`oxicache-client`) | Rust `Client` library + `oxicache-cli` (get/set/del/bench) |
| `packages/client-ts` (`@oxicache/client`) | TypeScript `Client` library for the Bun runtime |

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

## Run

```sh
cargo run --release -p oxicache-server -- --addr 0.0.0.0:4433 --capacity 1G
# --shards N      independent S3-FIFO shards (default: CPUs)
# --token T       require AUTH with this secret (or OXICACHE_TOKEN in the environment)
# every server flag has an environment variable: OXICACHE_ADDR, OXICACHE_CAPACITY,
# OXICACHE_SHARDS, OXICACHE_TOKEN; a flag wins over a differing variable, with a warning

cargo run --release -p oxicache-client -- set a 1 b 2
cargo run --release -p oxicache-client -- get a b c
cargo run --release -p oxicache-client -- del a
cargo run --release -p oxicache-client -- bench --conns 8 --pipeline 16 --batch 16
```

## TypeScript client (Bun)

`packages/client-ts` is a dependency-free library on `Bun.connect`: the same framing,
pipelining and in-order response matching as the Rust client, in ~300 lines of TypeScript.
Keys and values are `string` (UTF-8) or `Uint8Array`; values come back as `Uint8Array`.

```ts
import { Client } from "@oxicache/client";

const c = await Client.connect({ hostname: "127.0.0.1", port: 4433, token: "s3cret" });
await c.set([["a", "1"], ["b", new Uint8Array([0, 1, 2])]]);
const [a, b, missing] = await c.get(["a", "b", "c"]);   // Uint8Array | null each
const [existed] = await c.del(["a"]);                   // boolean each
c.close();
```

Calls issued in the same tick are coalesced into one write; a non-OK status rejects with
`StatusError` (`.status` is the `Status` enum), a dropped connection with `ClosedError`.

## Development

```sh
bun install            # installs the husky pre-commit hook
bun test               # client-ts unit + e2e tests (builds and spawns the debug server)
cargo test --workspace # wire, server and client-rs unit + e2e tests
```

The pre-commit hook (`.husky/pre-commit`) runs `cargo fmt --check`, `clippy -D warnings`,
`cargo build`, `cargo test`, then the TypeScript type check, `bun build` and `bun test`.

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
  One listener; accepted connections are spread over the runtime's worker threads.
  Frames are capped at 64 MiB each way; there is no connection limit or idle timeout,
  so put the server on a private network.
- 64-bit targets only: index slots pack a 48-bit entry address next to a 16-bit tag.
- The client pipelines calls from any number of tasks onto one connection (writer task
  coalesces queued frames into one flush; reader task matches responses in order).
