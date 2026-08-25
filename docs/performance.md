# Performance notes

All numbers: 12-thread (6-core SMT) Ryzen 5 5600H, loopback, release + fat LTO, quiet
machine. The bench client runs on the same box and is itself the throughput limiter, so
**server CPU per request** (utime+stime / requests) is the metric that reflects server
changes; req/s is reported for context.

## Allocator evaluation

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

## Transport experiments (2026-08-25)

| change | 8–12 conns | 64 conns × 2 in flight | decision |
|---|---|---|---|
| zero-copy single-chunk request body + streaming `get` encoder | noise-level; −27 % CPU/req on 4 KiB writes | — | kept |
| one quinn endpoint per CPU via `SO_REUSEPORT` (`--endpoints`) | noise-level | +8 % req/s (106k → 114k) | kept, default = CPUs |
| 8 MiB stream / 256 MiB connection windows, 4 MiB datagram buffer | −2…−5 % | — | rejected |

## Request handling (2026-08-25)

| change | 8×16, 128 B, 10 % w | 8×16, 1 KiB, 50 % w | 12×32, 4 KiB, 90 % w | 64×2, 128 B, 10 % w | decision |
|---|---|---|---|---|---|
| baseline (task per request) | 40 µs | 141 µs | 426 µs | 48 µs | |
| handle requests inline on the connection task | 32 µs | 122 µs | 291 µs | 44 µs | **default** |
| + thread-per-core runtimes (`--per-core`) | 31 µs | 113 µs | 266 µs | 45 µs | opt-in |
| skip `Arc<Entry>` clone on hits | — | — | — | 48 µs | rejected |
| MTU discovery ceiling 9000 / 65000 | — | — | — | 48 µs | rejected |
| QUIC ACK-frequency extension | — | — | — | 48 µs | rejected |

Why inline wins: all streams of a connection serialise on quinn's connection lock, so a task
per request buys no parallelism, only task allocation, cross-thread wakeups and contention.
The trade is head-of-line blocking inside one connection while a large body is read; the
4 KiB × 8 × 32-in-flight profile shows it still comes out well ahead.

Where the remaining CPU goes (perf, read-heavy profile): ~30 % kernel UDP I/O, the rest is
quinn packet processing, AES-GCM, memcpy and glibc malloc; the cache engine itself is ~7 %
and is bound by three cache misses per lookup (hashbrown control bytes, bucket, entry).

## Rejected after measurement (2026-08-25, final round)

| change | result | decision |
|---|---|---|
| `-C target-cpu=native` build | 33/122/296 µs vs 34/118/286 µs — noise | rejected (ring already dispatches on CPU features) |
| client: prebuilt `Uri`s, no `content-length` header | 135k vs 135k req/s, server 34 µs both | rejected |

## Where it stands

Per-request server CPU on the read-heavy 8×16 profile went from 40 µs to 34 µs over the
optimisation loop (−15 %), and from 426 µs to 286 µs (−33 %) on the 4 KiB write-heavy profile.
The remaining profile is transport-bound: kernel UDP I/O (~30 %), quinn packet
processing, AES-GCM and h3 framing. Further gains would need changes below this project
(quinn/h3 internals, io_uring UDP, or dropping HTTP/3 for raw QUIC streams).

## Transport switch: HTTP/3 → framed TCP (2026-08-25)

Same profiles, same box, same client bench; server CPU per request and req/s:

| profile | HTTP/3 (quinn + h3) | TCP frames | TCP `--per-core` |
|---|---|---|---|
| 8×16, 128 B, 10 % writes | 34 µs, 137k req/s | **11 µs**, 329k req/s | 11 µs, 319k req/s |
| 8×16, 1 KiB, 50 % writes | 118 µs, 43k req/s | 62 µs, 76k req/s | **49 µs**, 81k req/s |
| 12×32, 4 KiB, 90 % writes | 286 µs, 19k req/s | 152 µs, 33k req/s | **130 µs**, 35k req/s |
| 64×2, 128 B, 10 % writes | 44 µs, 115k req/s | **15 µs**, 242k req/s | 17 µs, 247k req/s |

The entire HTTP/3 stack (QUIC packetisation, AES-GCM, ACK handling, h3/qpack framing) was
~70 % of server CPU; plain TCP with a 5-byte frame header removes it. Pipelined requests are
answered in order and the response buffer is flushed only when the reader has no more
buffered input, so a burst of pipelined requests costs one `write` syscall. `--per-core`
is now a clear win on write-heavy pipelined loads (−20 % CPU) and neutral elsewhere.
