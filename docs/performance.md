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

## Index and allocation work after the TCP switch (2026-08-25)

See docs/hashmap-bench.md, round 3, for the per-step numbers. Net effect on the four
profiles (server CPU/req, req/s):

| profile | after TCP switch | now (dashmap + ThinArc + prefetch) |
|---|---|---|
| 8×16, 128 B, 10 % writes | 11 µs, 329k | **7 µs, 421k** |
| 8×16, 1 KiB, 50 % writes | 62 µs, 76k | **39 µs, 126k** |
| 12×32, 4 KiB, 90 % writes | 152 µs, 33k | **133 µs, 40k** |
| 64×2, 128 B, 10 % writes | 15 µs, 242k | **10 µs, 280k** |

mimalloc re-measured here: −8 % CPU on the first two profiles and at 64 connections, but
+17 % on the 4 KiB write-heavy profile and +30 % RSS on all of them; still opt-in.

## Round 4: copies, allocator behaviour, client (2026-08-25)

Server-side, from `perf` after the index work: `dispatch` (the lookups themselves, memory
latency) plus `memmove` of values and glibc malloc.

| change | result | decision |
|---|---|---|
| server: zero-copy request bodies (slices of the read buffer), vectored response writes | noise-level | kept (needed for the next row) |
| server: `get` responses reference cache entries for values ≥ 1 KiB (`writev` reads them in place) | 1 KiB 50/50: 39 → 35 µs; 4 KiB writes: 129 → 122 µs | kept |
| `mallopt` trim/mmap/top-pad tuning (glibc only) | client page faults 88k → 4.5k per 5 s run; server 35 → 34 µs | kept |
| shared `FrameReader`/`FrameWriter` in `oxicache-wire`, used by the client too | client CPU/req: 10.2 → 7.8 µs (read-heavy), 13.8 → 12.2 (64 conns), 15.5 → 16 (1 KiB) | kept |
| client: zero-copy response bodies shared across tasks | contended refcounts, no gain | copies each body out instead |
| `FrameWriter` growing its buffer past 64 KiB | crossed glibc's mmap threshold: every flush faulted fresh pages | bounded at 64 KiB, spills to piece list |

Measuring note: the bench client and server share the box, so a cheaper client shifts the
loopback equilibrium (fewer requests per server read → more syscalls per request on the
server). Compare server CPU/req only across runs with the same client, and prefer
`perf stat` task-clock over shell `time` for client CPU.

### Current numbers (server CPU/req, req/s)

| profile | default | `--per-core` |
|---|---|---|
| 8×16, 128 B, 10 % writes | 7 µs, 451k | 7 µs, 474k |
| 8×16, 1 KiB, 50 % writes | 38 µs, 131k | **27 µs**, 136k |
| 12×32, 4 KiB, 90 % writes | 129 µs, 41k | 123 µs, 41k |
| 64×2, 128 B, 10 % writes | 11 µs, 291k | 13 µs, 308k |

From the HTTP/3 starting point (34 / 118 / 286 / 44 µs): **−80 % / −68…−77 % / −57 % / −75 %**
server CPU per request.

## Round 5: memory-latency and buffer experiments (2026-08-25)

Same method (min of 3 alternating runs, server utime+stime from `/proc`, client task-clock
from `perf stat`). Kernel share of server CPU: 2.4 of 7.8 µs (8×16, 128 B), 14.4 of 38 µs
(1 KiB 50/50; 11.6 of 32 with `--per-core`), 61 of 130 µs (4 KiB writes) — genuine loopback
TCP work, not runtime wakeups.

| change | result | decision |
|---|---|---|
| server: lookup-and-prefetch pass over all keys of a GET/SET before the real pass (no refcount touch in pass 1) | small 7.8 vs 7.8 µs, 1 KiB 37.7 vs 38.0 µs | rejected (noise) |
| server: 256 KiB read buffer | 1 KiB 38 → 53 µs, 4 KiB 130 → 163 µs (user time too: a bigger `BytesMut` cannot be reused in place while zero-copy bodies are alive) | rejected |
| server: 32 KiB read buffer | noise on all profiles | rejected |
| server: `Key` inline 30 → 22 bytes so a bucket is 32 B and never straddles two lines | 1 KiB −0.3…−1.5 µs, small +0…+1 µs | rejected (noise) |
| bench client: key space as one contiguous buffer with fixed stride | client CPU/req 7.8 → 6.7 µs (small), 12.2 → 11.4 (64 conns); +5…10 % req/s; server neutral | kept |

`perf annotate` on `dispatch` (1 KiB profile) puts the samples on the refcount bump of a
GET hit (24 %), bucket key-tag checks (28 %), the `live.swap` of the overwritten entry (17 %)
and the shard mutex/queue state (13 %), i.e. memory latency; but prefetching those lines a
pass earlier did not move the total, so the remaining user time is bounded by the
DRAM-resident working set (100k × ~1.2 KiB ≫ L3) plus glibc's non-tcache path for
chunks over 1032 bytes (`unlink_chunk` + `_int_malloc` ≈ 20 % of user time on 1 KiB values).
