# oxicache

In-memory cache server in Rust: [S3-FIFO](https://blog.jasony.me/system/cache/2023/08/01/s3fifo)
eviction, HTTP/3 transport (quinn + h3), tokio multi-threaded runtime. No persistence.

## Crates

| crate | role |
|---|---|
| `oxicache-wire` | binary request/response framing shared by both sides |
| `oxicache-server` | `oxicache-server` binary: sharded S3-FIFO engine + HTTP/3 front end |
| `oxicache-client` | `Client` library + `oxicache-cli` (get/set/del/bench) |

## Protocol

Three commands, all batch-only (a single op is a batch of one), all `POST` over HTTP/3.
Values are opaque bytes. Integers are little-endian.

```
keys      := u32 count, count × (u32 len, bytes)
entries   := u32 count, count × (u32 klen, key, u32 vlen, value)

POST /get  body: keys     -> u32 count, count × (u8 0 | u8 1, u32 len, value)
POST /set  body: entries  -> empty
POST /del  body: keys     -> u32 count, count × u8 found
```

## Run

```sh
cargo run --release -p oxicache-server -- --bind 0.0.0.0:4433 --capacity 1G
# a self-signed cert is generated unless --cert/--key are given
# --endpoints N   QUIC sockets sharing the port via SO_REUSEPORT (default: CPUs)
# --per-core      one single-threaded runtime per endpoint (thread-per-core)
# --shards N      independent S3-FIFO shards (default: CPUs)

cargo run --release -p oxicache-client -- set a 1 b 2
cargo run --release -p oxicache-client -- get a b c
cargo run --release -p oxicache-client -- del a
cargo run --release -p oxicache-client -- bench --conns 8 --pipeline 16 --batch 16
```

The CLI accepts any certificate unless `--ca cert.pem` pins one.

## Design

- Keys hash once (foldhash); top bits pick one of N shards (default: CPU count), each an
  independent S3-FIFO with `capacity / N` bytes.
- Per shard: a concurrent map for lookups (reads never take a lock; they bump a relaxed
  atomic frequency counter capped at 3) plus a mutex over the small/main/ghost queues used
  only by writes and eviction.
- Entries are immutable; delete/overwrite marks them dead and they are skipped lazily at
  the queue head, with compaction once dead bytes exceed 25 % of the shard budget.
- One tokio task per QUIC connection; requests are handled inline on it (see
  `docs/performance.md` for why that beats a task per request). `--endpoints` binds one
  socket per CPU with `SO_REUSEPORT`; `--per-core` pins each to its own runtime.
- Build-time options: `--features mimalloc` (allocator), `--features papaya` (map backend);
  both measured and documented in `docs/`.
