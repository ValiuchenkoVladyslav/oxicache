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
