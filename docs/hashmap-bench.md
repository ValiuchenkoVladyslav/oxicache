# Concurrent hashmap selection

Benchmarked 2026-08-25 on a 12-core Linux box (release, fat LTO), 1M pre-populated
`Bytes` keys (16 B) / values (64 B), every `get` clones the `Bytes` out. All maps use
foldhash. Best of 3-4 runs of 3 s each; machine had external load, so treat <20 %
deltas as noise. Rankings were stable across runs.

Versions: dashmap 6.2.1, papaya 0.2.5, scc 3.8.6, flurry 0.5.2, foldhash 0.2.0.

## Throughput (Mops/s)

| Map | 95/5 4T | 8T | 12T | 50/50 4T | 8T | 12T | churn 4T | 8T | 12T |
|---|---|---|---|---|---|---|---|---|---|
| **dashmap 6.2** | **14.2** | **19.2** | 15.3 | **11.1** | **15.6** | **17.6** | **17.5** | **20.3** | **20.0** |
| sharded `RwLock<HashMap>` x64 | 14.0 | 19.3 | 17.5 | 10.7 | 15.1 | 16.9 | 11.3 | 14.2 | 14.0 |
| scc 3.8 | 8.6 | 15.8 | **18.4** | 9.1 | 13.5 | 14.4 | 13.0 | 15.7 | 14.5 |
| papaya 0.2 (pin per op) | 10.3 | 14.9 | 16.0 | 6.2 | 9.0 | 9.8 | 8.0 | 10.2 | 8.6 |
| papaya 0.2 (pin per 256 ops) | 9.8 | 11.0 | 12.8 | 6.9 | 9.2 | 9.3 | 8.0 | 9.8 | 10.6 |
| flurry 0.5 (pin per op) | 7.3 | 11.0 | 12.2 | 5.9 | 9.0 | 10.1 | 8.6 | 10.0 | 9.6 |
| flurry 0.5 (pin per 256 ops) | 8.4 | 11.8 | 11.2 | 5.0 | 8.1 | 6.5 | 5.4 | 6.6 | 6.8 |

## Memory (RSS delta, 1M entries in a 2M-capacity table)

papaya ~138 MB · flurry ~230 MB · dashmap ~286 MB · sharded RwLock ~286 MB · scc ~294 MB
(64 MB of that is the `Bytes` handles themselves).

## Decision: dashmap

- Fastest or tied in every cell at 4-12 threads; clearly best under insert+remove churn,
  which is what S3-FIFO eviction generates on every miss.
- Clone-out access (`get(k).map(|r| r.value().clone())`) keeps the read guard inside one
  expression, so the `!Send` guard never reaches an `.await` and read-lock hold time is a
  single atomic increment.
- Hit-path metadata (frequency, liveness) lives in atomics inside the value, so hits never
  take the shard write lock; `remove_if` gives "remove only if still this entry" for eviction.
- Runner-up: papaya (lock-free reads, ~50 % of the memory, no deadlock class). Switch if
  reader stalls behind write bursts or memory overhead become the limiting factor; the
  wrapper in `crates/server/src/cache/map.rs` is the only file that would change.

## dashmap rules applied in this codebase

1. Never hold a `Ref`/`RefMut` across `.await` or across another map op on the same task
   (same-shard re-entrancy deadlocks). The wrapper only exposes clone-out methods.
2. `get_mut` takes the write lock — not used; metadata is atomic.
3. Eviction walks our own FIFO queues, never `iter()`.
4. Use a randomly seeded hasher (`foldhash::fast::RandomState`) for HashDoS resistance.

## In-situ check: papaya 0.2.5 behind a feature flag (2026-08-25)

Same server, same `oxicache-cli bench` profiles, best of 2, quiet machine:

| profile | dashmap | papaya |
|---|---|---|
| 10 % writes, 128 B, batch 16 | 128k req/s, 39 µs CPU/req | 124k req/s, 42 µs CPU/req |
| 50 % writes, 1 KiB, batch 16 | 40k req/s, 128 µs CPU/req | 37k req/s, 139 µs CPU/req |
| 90 % writes, 4 KiB, batch 8, 12×32 in flight | 16.1k req/s, 345 µs CPU/req | 14.1k req/s, 392 µs CPU/req |

The upstream papaya benchmarks (integer keys, no value clone-out, ahash) show papaya ahead of
dashmap on read-heavy loads; with `Bytes` keys, a `Bytes` clone per hit and the map being a
small share of per-request cost, the difference reverses slightly here. papaya remains
available via `--features papaya` (keys are then allocated separately from values because
papaya documents that `insert` keeps the existing key object).

## Round 3, after the TCP switch (2026-08-25)

With transport cost gone the index dominates. Changes measured (server CPU/req, min of 3):

| build | 8×16, 128 B, 10 % w | 8×16, 1 KiB, 50 % w |
|---|---|---|
| dashmap, `Bytes` keys + `Arc<Entry{key,value: Bytes}>` | 11.0 µs | 62 µs |
| + inline `Key` in bucket, key‖value in one `Box<[u8]>` | 9–10 µs | 40 µs |
| + `triomphe::ThinArc` (refcount + meta + value in one allocation) | 8.2 µs | 37 µs |
| papaya, one pin per batch, no refcount on hits | 9.0 µs | 45 µs |
| papaya + two-pass prefetch | 7.7 µs | 44 µs |
| **dashmap + two-pass prefetch** (default) | **7.2 µs** | **38 µs** |
| dashmap + prefetch + mimalloc | 6.6 µs | 35 µs (but +17 % on 4 KiB writes, +30 % RSS) |

Why: a lookup is a chain of dependent cache misses (control bytes → bucket → entry → value);
`lock`-prefixed atomics are full barriers on x86, so with a refcount bump per hit the misses
of consecutive keys cannot overlap. The two-pass `get_many` resolves all keys first and
prefetches their entries, then reads them. Papaya removes the atomics but boxes every table
entry, which costs ~20 % on inserts; dashmap keeps entries inline in the bucket.


Update 2026-08-26: the papaya feature was removed; with prefetching batch lookups dashmap led on
every profile, so there was no deployment where papaya was the better choice.

Update 2026-08-26: dashmap itself was replaced by a custom per-shard cuckoo index (see
docs/performance.md, round 6): −14 % user CPU on reads, −17…−25 % on writes.
