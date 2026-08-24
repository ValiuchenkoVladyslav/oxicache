# Allocator evaluation

Measured 2026-08-25 on the 12-core dev box (release, fat LTO, loopback, quiet machine).
`oxicache-cli bench --seconds 6`, 512 MiB capacity, 100k-key space, best of 2 runs.
Server RSS sampled at the end of each run.

| profile | system (glibc 2.41) | mimalloc 0.1.52 | delta |
|---|---|---|---|
| 10 % writes, 128 B values, batch 16 | 128k req/s, 243 MB | 135k req/s, 320 MB | +5 % ops, +30 % RSS |
| 50 % writes, 1 KiB values, batch 16 | 41k req/s, 410 MB | 39k req/s, 550 MB | −5 % ops, +33 % RSS |
| 90 % writes, 4 KiB values, batch 8, 12×32 in flight | 16.5k req/s, 640 MB | 11k req/s, 1.3 GB | −35 % ops, +100 % RSS |

jemalloc (`tikv-jemallocator`) and snmalloc could not be built on this machine (no
`make`/`cmake`), so they are unmeasured rather than rejected.

`perf` on the read-heavy profile: ~12 % of server CPU in glibc `malloc`/`free`, the rest in
the request closure, QUIC packet processing, AES-GCM and `memmove`/`memcmp`. The allocator is
not the bottleneck, so there is little for a different allocator to win.

## Decision

Keep the system allocator. mimalloc stays available as an opt-in
(`cargo build --release -p oxicache-server --features mimalloc`) for small-value, read-heavy
deployments that can spend memory for ~5 % throughput.

## Arenas

A bump arena does not fit the value store: S3-FIFO frees individual entries in an order
unrelated to insertion, which is exactly what bump/region allocators cannot do. The
arena-shaped optimisation that *does* fit is reducing allocations per operation, which was
applied instead:

- key and value of a `set` entry share one allocation (3 → 2 mallocs per entry; the map's
  key object is replaced on overwrite so the old buffer is not pinned);
- request bodies are pre-sized from `content-length` (no regrowth copies);
- `get` responses are encoded into a single exactly-sized buffer.

A size-class slab store (memcached style) would be the next step if allocator time grows;
mimalloc is already that design internally and did not help here.
