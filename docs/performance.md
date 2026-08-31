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

Keep the system allocator. mimalloc was carried as an opt-in feature for a while and has
since been removed: ~5 % throughput on small-value, read-heavy loads did not justify the
+30 % RSS and the extra build configuration.

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
+17 % on the 4 KiB write-heavy profile and +30 % RSS on all of them; since removed.

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

## Hasher check (2026-08-26)

`rustc-hash` (FxHash) vs `foldhash::fast` for the index and shard selection, same client,
alternating min-of-3: 7.2 vs 7.2 µs/req (497k vs 496k req/s) read-heavy, 38.2 vs 38.4 µs on
1 KiB 50/50 — identical. Hashing a 14-byte key is a few nanoseconds either way; the lookup
cost is the memory latency after the hash. foldhash stays (seeded, HashDoS-resistant).

`rapidhash::fast` 4.5 (the in-memory flavour: no avalanche, sponge mixing) vs `foldhash::fast`,
after round 7: read-heavy 4.38 vs 4.24 µs (pairs split 3/4), 1 KiB 21.9 vs 22.1, 64 conns
4.96 vs 4.94 — noise. Isolated, on the bench's 14-byte keys: foldhash 1.35 ns/key,
rapidhash fast 1.28, rapidhash quality 2.21; a 16-key request differs by ~1 ns of ~4,250.
Adopted anyway (marginally faster, same seeded HashDoS resistance, `quality` flavour
available behind the same API); foldhash removed.

## Round 6: cuckoo index (2026-08-26)

Replaced dashmap with a per-shard cuckoo hash index (`crates/server/src/cache/table.rs`):
eight 8-byte slots per 64-byte bucket, slot = 16-bit tag | 48-bit entry pointer, two
candidate buckets per key computable from the hash, lock-free readers under a
crossbeam-epoch pin taken once per batch, single writer under the existing shard mutex,
copy-before-clear displacement. The lookup chain shrinks from control bytes → bucket → entry
to bucket → entry, and a batch prefetches every key's two buckets up front.

Server CPU per request, user / sys, min of 3 alternating runs, same client binary:

| profile | dashmap (user / sys) | cuckoo (user / sys) | user delta |
|---|---|---|---|
| 8×16, 128 B, 10 % writes | 7.1 total | 6.1 total | **−14 %** (498k → 508k req/s) |
| 8×16, 1 KiB, 50 % writes | 23.8 / 14.2 | 19.8 / 20.0 | **−17 %** (132k → 144k req/s) |
| 12×32, 4 KiB, 90 % writes | 67.6 / 61.9 | 50.4 / 91.0 | **−25 %** (41k → 42k req/s) |
| 64×2, 128 B, 10 % writes | 10.8 total | 10.1 total | −6 % |

The sys-time increase on the write profiles is the loopback batching effect (a faster
server receives smaller pipelined bursts, so more syscalls per request); user time, which
is what the index change affects, fell on every profile. `set` pins the epoch once per
request (`Cache::set_many`). Memory is unchanged (±2 % RSS).

What remains on the write path (perf, 4 KiB writes): memmove of values 39 %, `Table::find`
18 % (the existence check on insert dereferences the entry to compare keys), dead-entry
compaction 8 %, glibc large-chunk malloc/free ~15 %.

## Round 7: data-structure and code review (2026-08-26)

A pass over every hot path looking for better-fitting structures and plain code waste.
Harness change: each run is preceded by a 2 s warm-up, so on the write profiles every set
is a replacement (the earlier numbers included the cheap cache-filling phase). Baselines
below are the round-6 binary under this harness.

| change | measurement (user / sys µs per request, min of 3 alternating) | verdict |
|---|---|---|
| **fix:** reclaim retired entries after each write batch (`Guard::flush`). crossbeam-epoch collects only every 128 pins and at most 8 bags then; a SET batch retires 16 entries per pin, so garbage was produced ~4× faster than freed | 4 KiB writes: RSS 0.9 → **6 GB** in 8 s with a 1 GB budget before; 0.7 GB flat after. Page faults 650k → 8k per 3 s. Total CPU/req −9 %; user rises (82 → 150) because frees now actually happen | kept (bug) |
| own thin entry allocation (header: refcount, len, meta = 64 B; value follows) instead of `triomphe::ThinArc`, values ≥ 1 KiB copied with AVX non-temporal stores | 4 KiB writes: user 154 → 101, sys 127 → 100, +22 % req/s; 1 KiB 50/50: user 27.3 → 22.0, +20 % req/s; 128 B profiles unchanged. Microbench: memcpy into cold scattered 4 KiB chunks 3.6 GB/s vs 11 GB/s streaming | kept |
| borrow request bodies from the read buffer; validated borrowed `Keys`/`Entries` iterators in `oxicache-wire` (no `Vec<Bytes>`, no refcount inc/dec per key) | read-heavy: user 4.26 → 4.08 (−4 %, every pair); 1 KiB neutral | kept |
| branchless tag-match mask over the whole bucket in `Table::find` | read-heavy: 4.31 → 4.19 (−3 %, 7/7 pairs); 1 KiB: 21.9 → 20.5 (−6 %) | kept |
| dead-entry compaction with software prefetch lookahead instead of `VecDeque::retain` | 1 KiB: 27.0 → 25.8 (−5 %, every pair); eviction profile neutral | kept |
| DEL: one epoch pin per request (`Cache::del_many`), flags written straight into the frame | not separately measured (DEL is absent from the bench profiles); removes a pin, a lock cycle and a `Vec` per request | kept |
| 256 KiB read buffer (retried now that bodies are borrowed) | worse on all three profiles (+5…+10 % user) | rejected |
| mimalloc (re-measured now that frees actually run) | 4 KiB writes: user −8 %, sys +11 %, total equal | rejected (feature since removed) |
| `GLIBC_TUNABLES` non-temporal threshold 2 KiB / `malloc.hugetlb=1` | no change (glibc only streams copies ≥ 2 pages; THP no effect) | rejected |
| `get_many` scratch lists: smallvec (current) vs plain `Vec::with_capacity` vs `tinyvec::TinyVec` | read-heavy user: 4.38 vs 4.36 (Vec, equal) vs 4.40 (tinyvec, +5 % in every pair: it zero-initialises the whole inline array and needs `Default` elements); arrayvec has no spill and batches are unbounded | switched to `Vec` (equal cost — +366 instructions/req for two tcache malloc/free pairs, ~25 ns — one dependency fewer) |
| identity hasher for the ghost set, `hash` compare before key compare, merging small `put_slice`s | not pursued: ghost ops happen only on eviction of a DRAM-resident set and cost ~1 ns each; the others are single-cycle work next to a 100 ns miss | — |

Net, round-6 binary vs now, same client, warm cache (user / sys µs per request, req/s):

| profile | before | after |
|---|---|---|
| 8×16, 128 B, 10 % writes | 4.50 / 2.06, 467k | **4.25 / 1.99**, 476k |
| 8×16, 1 KiB, 50 % writes | 22.4 / 22.1, 126k | **20.6 / 12.0**, 142k (total −27 %) |
| 12×32, 4 KiB, 90 % writes | 82.8 / 223.6, 19.8k (leaking) | **101.7 / 102.3**, 22.6k (total −33 %, memory bounded) |
| 64×2, 128 B, 10 % writes | 5.16 / 5.57, 277k | 4.95 / 5.52, 284k |
| 8×16, 128 B, 50 % writes, 1M keys, `--capacity 32M` (evicting) | 9.60 / 5.04, 325k | 10.04 / 3.81, 328k |

The eviction profile pays ~0.4 µs user per write request for the per-batch collection
(`try_advance` scans every registered thread); that is the price of bounded memory.

What remains on the write path (4 KiB): the value copy is now DRAM-write-bandwidth bound
(~6 µs of 64 KiB per request), `Table::find` on insert (~13 %), glibc `_int_malloc` /
`unlink_chunk` for chunks above the tcache limit (~12 %), and the read buffer's realloc
copy for frames larger than 64 KiB (~8 %).

## Round 8: entry line fetch on GET (2026-08-26)

`perf` on the read-heavy profile put 51 % of user time in `Table::find`, 84 % of that on
the first load of the entry header, i.e. the DRAM miss per hit; `perf stat` showed IPC 0.57
and two dTLB misses per key. The miss itself is inherent (a hit must read the entry), but it
was paid in two dependent stages: `find` missed on the header, and `Entry::prefetch` read
`len` from it before it could request the value lines.

| change | 8×16, 128 B, 10 % w (user) | 1 KiB 50/50 | 4 KiB 90 % w | verdict |
|---|---|---|---|---|
| second pass prefetching the header of tag-matching entries (from slot words) | noise | — | — | the OoO core already overlaps the header misses |
| same pass prefetching header + first two value lines | 4.35 → 3.72 (−16 %, every pair) | — | — | kept |
| + 64-byte-aligned entry allocations (192 B entry: 3 lines instead of 4 in 3 of 4 cases) | → 3.41 (−22 % cumulative) | | +2…+3 % (glibc `memalign`) | kept for values < 1 KiB only; 4 KiB neutral |
| `touch()` disabled (diagnostic) | no change | | | freq stores cost nothing |
| `GLIBC_TUNABLES=glibc.malloc.hugetlb=1` | −2 % | | | not adopted (THP pages never materialised) |

Net, same client: read-heavy 4.4 → **3.4 µs** user; 1 KiB 50/50 22–24 → **20.5–21.2**
(5 of 6 pairs); 4 KiB writes neutral.

Next: a size-class slab for entries backed by an `MADV_HUGEPAGE` region — alignment for
free at every size, no glibc `malloc`/`free` (~8 %), and the TLB misses gone.

## Round 9: review fixes and false sharing (2026-08-29)

A full-tree review (bugs, performance, and a separate pass over cache-line layout).
Same harness as round 7 (2 s warm-up, 6 s runs, min of 3 alternating). Baseline is the
round-8 binary re-measured today; the client bench itself is faster than in round 7, so
absolute numbers are lower across the board.

| change | measurement (user / sys µs per request) | verdict |
|---|---|---|
| **fixes** (no perf intent): cap keys/entries per request at 65 536 (a 64 MiB body of empty keys pre-allocated ~800 MB of scratch); grow the read buffer geometrically instead of reserving a frame's claimed length up front; 4 KiB frame limit before authentication; back off 50 ms after a failed `accept` (fd exhaustion spun a worker); reject a SET whose entry cannot fit its shard instead of evicting the whole shard; replace before evicting so overwriting at full capacity does not also evict `cost` bytes of neighbours; drain connections for up to 2 s on Ctrl-C; `M_MMAP_THRESHOLD` 32 MiB (glibc silently rejected 64 MiB); client reports `ResponseTooLarge` instead of `Closed` | neutral on every profile | kept |
| tag from hash bits 32..48 instead of the top 16 (the shard selector uses the top bits, so a 16-shard table had 12 usable tag bits) | neutral on A, B and D: the tag false-positive rate is already ~1/4096 per slot | rejected (no measured win) |
| `Shard` and `Index` layout: reader-hot `table`/`moving` on their own 128-byte line, `len`/`kick_seed`/mutex/queues after it; `#[repr(align(128))]` on `Shard` (stride was 216 B, so half the shards' write counters shared a line with the next shard's reader words); `#[repr(align(64))]` on `Table` | eviction profile 11.5 → 11.2 (6/6 pairs); read-heavy −2 %; 1 KiB 50/50 neutral | kept |
| flush retired entries to the epoch collector on a threshold instead of after every batch: every batch (`Guard::flush` → `try_advance` scans every thread + global queue CAS) | 4 KiB 90 % writes: 53.2 → 46.2 (−13 %). But with a plain threshold the 128 B eviction profile got slower the larger the threshold (32 entries +2 %, 256 entries +7 %), and 1 MiB of 128 B entries between flushes leaked (2.6 GB on a 32 MiB budget: a flush reclaims at most 8 bags of 64 entries) | reworked below |
| same, size-split: small entries (< 1 KiB value) flush at once — their chunks are still cache-hot when freed and the next allocation reuses them warm; large ones batch up to 256 entries / 1 MiB per thread | 4 KiB writes 53.1 → **46.2** (−13 %); 1 KiB 50/50 20.9 → **18.6** (−11 %); eviction profile and read-heavy neutral; RSS bounded over 14 s on both write profiles | kept |

Net, round-8 binary vs now, same client (user / sys µs per request, req/s):

| profile | before | after |
|---|---|---|
| 8×16, 128 B, 10 % writes | 3.17 / 2.13, 500k | 3.17 / 2.08, 501k |
| 8×16, 1 KiB, 50 % writes | 20.9 / 12.6, 137k | **18.6 / 12.2**, 150k |
| 12×32, 4 KiB, 90 % writes | 53.1 / 43.8, 48.3k | **46.2 / 42.2**, 51.5k |
| 8×16, 128 B, 50 % writes, 1M keys, `--capacity 32M` | 11.24 / 4.23, 305k | 11.19 / 4.13, 309k |

Reviewed and left alone: the per-hit refcount on ≥ 1 KiB values (`Entry::clone` in the
GET path puts a `lock xadd` on the header line every reader of that key must load; only
matters for skewed key popularity, which the uniform bench does not exercise — revisit
with the slab allocator, when the entry layout changes anyway); 8 `Acquire` loads per
bucket in `Table::find` (free on x86, costs `ldar`s on aarch64 — unmeasurable here);
`path.contains` in `find_path` (O(n²) over ≤ 256 hops, only at 7/8 load); grouping
`set_many` by shard (16 entries over 16 shards already take ~one lock each).

## Round 10: io_uring and kTLS study (2026-08-30)

Question: would io_uring (fewer syscalls, zero-copy send) or kTLS (kernel-side record
crypto) buy anything? Everything below was re-measured today on the same box (Linux 6.18,
`tls` module available, io_uring enabled): server user/sys CPU per request from `/proc`,
min of 3 alternating 6 s runs after a 2 s warm-up, syscalls per request counted with an
`LD_PRELOAD` shim around `recv`/`writev`/`epoll_wait`, user-side profile from `perf record
-e cycles:u` (no root, so no kernel symbols).

### Baseline: plain vs TLS (rustls 0.23 / ring, tokio-rustls 0.26)

| profile | plain (user / sys) | TLS (user / sys) | TLS cost | syscalls per request, plain → TLS |
|---|---|---|---|---|
| A: 8×16, 128 B, 10 % writes | 4.3 / 2.5 = 6.8 µs, 404k | 5.8 / 4.9 = 10.7 µs, 309k | +57 % | recv 0.18 → 0.21, writev 0.17 → **0.58**, epoll 0.09 |
| B: 8×16, 1 KiB, 50 % writes | 25 / 17.5 = 43 µs, 99k | 36 / 12.8 = 49 µs, 87k | +14 % | recv 0.18 → **0.68**, writev 0.35 → 0.85 |
| C: 12×32, 4 KiB, 90 % writes | 66 / 55 = 120 µs, 27k | 93 / 57 = 150 µs, 24k | +25 % | recv 0.41 → **2.1**, writev 0.37 → 0.96 |
| D: 64×2, 128 B, 10 % writes | 5.1 / 5.9 = 11.0 µs, 256k | 6.9 / 8.1 = 15.0 µs, 216k | +37 % | recv 0.61, writev 0.61 → **1.0** |

Plain TCP makes about half a syscall per request: pipelining already amortises `recv`
and `writev` over ~5 requests, and `epoll_wait` over ~11. The kernel time that remains
(2.5 µs on A for ~2.5 KiB out + 0.5 KiB in) is loopback TCP itself.

Where TLS spends its extra time, from `perf` (user side): ring's AES-GCM is ~10 % of user
time on A (≈ 0.6 µs) and ~9.5 % on C (≈ 9 µs, decrypting 32 KiB of `set` bodies per
request); the rest of the user delta is rustls record framing, tokio-rustls glue and the
extra copy rustls makes between its deframer buffer and ours. The **larger** part of the
small-request overhead was kernel time, and the syscall counts say why: `writev` per
request tripled. No `EAGAIN` or partial writes were involved (counted: zero) — the server
was simply flushing three times per `recv`.

### Cause and fix: one record per `poll_read`

tokio-rustls's `poll_read` goes through rustls's `Reader::fill_buf`, which hands out one
decrypted record per call, and a client writes each request as its own record. So under
TLS the connection loop in `tcp.rs` saw one or two requests per wake, flushed, and polled
again — even though rustls had already decrypted the whole `recv`. `Drained` in
`crates/server/src/tcp.rs` wraps the handshaken stream and, after the inner read has done
its I/O, copies the rest of the decrypted plaintext out through `ServerConnection::reader()`
synchronously (no syscall). Plain TCP is untouched.

| profile | TLS before | TLS with `Drained` | plain, for reference |
|---|---|---|---|
| A | 10.7 µs, 309k | **8.6 µs** (5.8 / 2.9), 345k (−20 % CPU) | 6.8 µs, 404k |
| B | 49 µs | 49 µs (neutral: a 16 KiB body is one record per request anyway) | 43 µs |
| C | 150 µs | 150 µs (neutral) | 120 µs |
| D | 15.0 µs, 216k | **13.0 µs** (6.8 / 6.2), 231k (−13 %) | 11.0 µs, 256k |

`writev` per request on A: 0.58 → 0.22 (plain: 0.17); sys time is back to the plain
level. What TLS still costs after this is user-side: +1.8 µs on A, +6 on B, +30 on C.

### kTLS

kTLS (`setsockopt(SOL_TLS)` after a rustls handshake; the `ktls` crate, 6.0.2 from April
2025, does exactly this on top of tokio-rustls and needs the kernel `tls` module, TLS 1.3
AES-GCM/ChaCha20 which we already restrict to) would move AES-GCM into the kernel and
remove rustls's per-record buffer copy: reads would land in our buffer once, as with plain
TCP. It does not remove the crypto — the kernel's `gcm(aes)` (VAES/AVX2 on this CPU since
6.11) costs about what ring costs per byte, and software kTLS RX is widely reported as
break-even or worse than user-space rustls, because each record goes through the async
crypto API with an `aead_request` allocation. The wins kTLS is known for — `sendfile` /
`splice` of file data through an encrypted socket — do not apply: nothing here comes
from a file.

Upper bound of what it could take back, from the numbers above: the copy and framing
share of the remaining TLS delta, i.e. perhaps 1 µs of A's 8.6 and 10–15 µs of C's 150
(5–10 % of TLS-mode CPU, nothing in plain mode), against: a second I/O path in `tcp.rs`
and `http.rs` (control messages for alerts and `KeyUpdate` arrive as `EMSGSIZE`/`cmsg`
and must be handled by hand; `close_notify` on shutdown), a kernel-module dependency
(`modprobe tls`), TLS 1.3 `KeyUpdate` and 0-RTT caveats, and a client whose TLS path is
`node:tls`/rustls and gains nothing. Not worth it at these request sizes; revisit only if
TLS mode ever carries multi-megabyte values, where the copy dominates.

### io_uring

Syscalls are already ~0.45 per request on the read-heavy profiles and ~0.9 on the 4 KiB
one; at a few hundred nanoseconds each that is 2–4 % of A and under 1 % of C. io_uring
removes the syscall entry/exit and the `epoll_wait`, not the TCP stack, which is where the
sys time is. `IORING_OP_SEND_ZC` needs sends of ~10 KiB and up to beat a copy; ours are
2–11 KiB. Multishot `recv` with provided-buffer rings would also displace the single
reusable read buffer that request bodies are borrowed from (round 7's zero-copy bodies),
so it is a redesign of `FrameReader`, not a drop-in. The runtime options are
`tokio-uring` (maintenance mode), `monoio`/`glommio` (thread-per-core, would replace
tokio wholesale — tokio itself is ~2 % of the profile, `--per-core` already exists for
the scheduling side), or `luring` from loona (an io_uring layer *on top of* tokio, last
commit April 2025, README calls the project experimental, no published numbers). The
tarweb write-up demonstrates zero syscalls per request with io_uring + kTLS but reports
no benchmarks, and needed a kernel patch for `setsockopt` through io_uring at the time
(`SOCKET_URING_OP_SETSOCKOPT` landed in 6.7).

Decision: neither. The measurable TLS overhead that was syscall-shaped is gone with
`Drained`; what is left is crypto and copies at sizes where the kernel does them no
cheaper. If a future profile shows syscalls above ~2 per request (many connections, no
pipelining), the cheap first step is `--per-core` plus `SO_BUSY_POLL`, not io_uring.

## memcached comparison (2026-08-30)

memcached 1.6.45 (`docker.io/library/memcached`, rootless podman on the host network,
`-t 12 -m 1024`) driven by memtier_benchmark 2.5.1, same box, same method: server
user+sys from `/proc` over a 6 s run after a 2 s warm-up, divided by requests. memtier
pipelines 16 requests per connection like our bench; with `--multi-key-get` it turns the
Set:Get ratio into the width of one mget, and memcached has no multi-key set, so the
batched shapes are only comparable per key-op. memtier is a C client and drives ~2× the
requests per second our tokio bench does; the loopback bench is client-bound on both sides,
so only server CPU per request is compared.

| shape | memcached | oxicache (client before / after round 11) |
|---|---|---|
| single key, 8×16, 128 B, 10 % writes | 1.5 µs/req (1.88M req/s; 1.4 µs when rate-limited to 900k) | 2.5 → **1.4 µs/req** (0.9M → 1.4M req/s) |
| single key, 8×16, 1 KiB, 50 % writes | 3.5 µs/req (1.25M req/s) | 3.1–3.8 µs/req (0.6–0.7M) |
| single key, 64×2, 128 B, 10 % writes | 5.3 µs/req (0.65M; 96 % misses — cheaper) | 6.3 µs/req (0.38M, 62 % hits) |
| 16-key batches, 8×16, 128 B, 10 % writes | 1.07 µs per key-op (mget 16 + single-key sets, 9.1 µs/req) | **0.43 → 0.37 µs per key-op** (6.9 → 5.9 µs/req) |
| 16-key batches, 64×2, 128 B | 1.4 µs per key-op (9-key mget) | **0.69 → 0.64 µs per key-op** (11 → 10.3 µs/req) |

So: per key in a batch, oxicache is ~2.5–3× cheaper; per single-key request the two are
now at parity (1.4 vs 1.5 µs). The single-key gap before round 11 was not in user time
(oxicache 0.72 µs/req vs memcached 0.63) and not in syscall count (0.47 per request:
recv 0.17, writev 0.17, epoll 0.07, futex 0.06); it was kernel time, 1.67 vs 0.83 µs/req,
because the server saw ~5.7 requests per `recv` from our client where memtier writes its
16-deep pipeline in one packet. Round 11 fixed that on the client side.

## Round 11: client write packing, busy polling (2026-08-30)

The Rust client's writer already coalesced whatever was queued when it woke, but it woke
on the first queued request: the other callers released by the same reply batch were
still being scheduled, so a flush carried ~1.4 requests and the server got ~5.7 per
`recv`. Now the writer yields one scheduler turn before flushing when the pending flush
is under 16 KiB (`YIELD_BELOW`), then drains again. Server-side packing on single-key
requests went from 0.174 to 0.072 `recv` per request (14 requests per read).

Server CPU per request and req/s, old vs new client, same server binary, 2 alternating runs:

| profile | old client | new client |
|---|---|---|
| 8×16, single key, 128 B, 10 % writes | 2.42 µs, 934k | **1.43 µs, 1.39M** (−41 %) |
| 8×16, 16 keys, 128 B, 10 % writes | 6.8 µs, 424k | **5.95 µs, 480k** (−12 %) |
| 8×16, 16 keys, 1 KiB, 50 % writes | 43.3–45.6 µs, 83–96k | 45.0 µs, 89k (neutral; a request is one 16 KiB flush, above the yield threshold) |
| 64×2, 16 keys, 128 B | 11.5 µs, 255k | **10.3 µs, 310k** (−10 %) |

Without the size cap the 1 KiB profile lost 5–10 % (the yield delayed 16 KiB requests
that already fill their packets); with it, neutral.

The TypeScript client needed nothing: its microtask flush runs after every continuation
released by a `data` event, so the server already sees ~16 requests per `recv` (0.06
`recv`/request measured with the shim). Its lower req/s is its own CPU (msgpack, promise
machinery), not the wire.

Busy polling, tried on the server with the new client: `SO_BUSY_POLL=50` on accepted
sockets, `EPIOCSPARAMS` (50 µs, budget 64) on tokio's epoll fds, both, and
`prefer_busy_poll` — all within ±3 % of the baseline on single-key, 16-key and 64×2
shapes (both settable unprivileged on this kernel). Loopback traffic has no NAPI context
to poll, so nothing can change here; it would need a real NIC to evaluate, and then a
knob, which the server does not have. Rejected, no code kept.

## Round 12: protocol change — single-key ops and BATCH (2026-08-30)

The wire protocol lost its multi-key bodies: GET/SET/DEL carry one key, and BATCH carries any
mix of them as nested frames, answered one nested frame each. The server serves runs of the
same op in a batch together (the old `get_many` prefetch pass, one epoch pin per SET/DEL
run), and streams the replies under a header patched at the end (`FrameWriter::begin`/`end`;
`Piece::Own` keeps spilled chunks mutable for that) — the first version collected replies in
a `Vec` and cloned every hit's entry to know the length up front, which cost +8 % on 16-key
gets and +17 % at 64×2.

Same client build technique as round 11 (the client changed with the protocol, so this is a
client+server comparison), server CPU/req and req/s, 2 runs:

| profile | before (round 11) | after |
|---|---|---|
| 8×16, single key, 128 B | 1.43 µs, 1.39M | 1.35–1.42 µs, 1.44–1.53M |
| 8×16, 16-key batch, 128 B | 5.95 µs, 480k | 6.1–6.2 µs, 435k |
| 8×16, 16-key batch, 1 KiB, 50 % writes | 45 µs, 89k | **38–39 µs, 105k** |
| 64×2, 16-key batch, 128 B | 10.3 µs, 310k | 10.8 µs, 306k |

Batch items carry a 5-byte header each way instead of the 4–5 bytes the packed lists used, so
the small-value batches pay ~1–2 % more bytes and the client encodes/indexes one frame per
item; the 1 KiB profile gains from SET replies no longer being one status for the whole
batch (nothing to roll back) and from the writer's `len` counter replacing a size pass. The
client's `Outcome` indexes replies by offset into the one response buffer — no `Bytes` slice
(refcount) per item, only for the slots that are read.

## Container (2026-08-30)

The `Dockerfile` (rust:1-bookworm builder → debian:bookworm-slim runtime, same release
profile) benched with the same harness, server CPU/req from `/proc` of the containerised
process (visible from the host under rootless podman):

- **`--network host`: containerisation costs nothing.** Alternating runs of the native
  binary and the image binary on the 16-key 128 B profile land in the same 5.9–7.5 µs
  spread; single-key runs are identical (1.36 µs, ~1.5M req/s). The image's binary is
  byte-different (upstream rustc/LLVM 22.1.8 vs Void's 22.1.4) but measures the same. An
  early reading of +20 % for the container was leftover load, not the container.
- **Rootless port mapping (`-p 4433:4433`) is the expensive mode**: every byte crosses the
  `pasta` userspace proxy. 16-key profile: 312k req/s vs ~430k, pasta burns 1.4 µs/req of
  its own (0.45 cores at this load) and the server's own CPU/req rises to ~8.2 µs (worse
  batching through the relay). Use `--network host` for a cache, as the README says.

## Release profile check (2026-08-30)

Is `lto = "fat"`, `codegen-units = 1`, `panic = "abort"` (opt-level 3 by default) optimal?
The two untried knobs, measured min-of-3 alternating on the 16-key 128 B and 1 KiB 50/50
profiles, same harness:

| variant | 16-key 128 B (total µs/req) | 1 KiB 50/50 |
|---|---|---|
| current (fat, opt 3) | 6.40–6.51 | 38.6–40.7 |
| opt-level = 2 | 6.21–6.67 | 38.2–41.0 |
| lto = "thin" | 6.20–6.66 | 38.4–39.6 |

All three overlap; no variant separates from the noise. The profile stays as it is —
`target-cpu=native` was already rejected earlier (ring dispatches on CPU features at
runtime), and the allocator question was settled in the first round. The one untried lever
left is PGO/BOLT, which would need a representative training workload and a two-phase build.

## Round 13: client-side copies and codec scaffolding (2026-08-30)

A findings pass over both clients and the wire codec (the server's own hot path was
covered in rounds 6–9). Same harness: 2 s warm-up, 6 s runs, min of 3 alternating runs,
server user+sys from `/proc`, client task-clock from `perf stat`.

Rust client (same baseline server binary, alternating client binaries):

| change | measurement | verdict |
|---|---|---|
| batch bodies presized (`BatchEncoder` starts at 512 B instead of 4 B; `Batch::with_capacity` when the size is known — the bench sizes read and write batches separately) and SET values serialised straight into the request/batch buffer (`BatchEncoder::set_with` / `put_set_key`) instead of an `rmp_serde::to_vec_named` `Vec` copied in; bench harness reuses its key/slot scratch across iterations | 16-key 128 B: client 8.8–9.4 → 5.9 µs/req, 400k → 470k req/s (+17 %, every pair); 16-key 1 KiB 50/50: client 26 → 21 µs/req, req/s +2 %; single-key unchanged (1.3 µs/req both) | kept |
| client writer `inline_limit` 64 KiB → 4 KiB: request bodies of 4 KiB and up ride as `writev` iovec entries instead of being copied into the coalescing chunk | 1 KiB 50/50: client 21.4 → 19.5 µs/req (−9 %, every pair), req/s +6 %; 4 KiB 90 % writes: req/s +2.2…+3.1 % (every pair) | kept |

Server (same new client, alternating server binaries):

| change | measurement | verdict |
|---|---|---|
| batch GET/DEL/SET runs collected into per-batch scratch `Vec`s with exact length, replacing the `once + from_fn` run iterators whose size hint of 1 made `get_many`'s scratch regrow 1→2→…→16 per run | 16-key 128 B: 6.2 → 5.6 µs/req (−9 %, every pair — recovers the round-12 slip), +2…5 % req/s; 1 KiB and 4 KiB neutral-to-positive | kept |
| `FrameReader::fill` reserves the rest of a pending frame in one step once at least half of it has arrived; bounded by the bytes already received, so round 9's no-trust-the-header property holds | 4 KiB 90 % writes (66 KiB frames): req/s +2.5 % every pair, server CPU/req −3 % avg; the mechanism matters most for frames well past 64 KiB | kept |
| HTTP responses through `FrameWriter::take_bytes` (no copy for a single-piece response, one exact-size copy otherwise) instead of `take`'s per-piece `to_vec` + unsized collect | not measured — there is no HTTP bench; strictly fewer copies and allocations per response | kept |

TypeScript client (same baseline server, alternating checkouts, bun 1.3.14):

| change | measurement | verdict |
|---|---|---|
| shared msgpack `Encoder`/`Decoder` instances (the per-call `encode()` helper constructs an Encoder with a fresh 2 KiB buffer every time); frame encoders store header bytes directly — no per-frame `Writer` + `DataView` — with string keys UTF-8-encoded into a reused scratch; batch replies decoded into results in one pass (no intermediate `Reply` objects); `FrameReader` parses frames in place from a socket chunk when nothing is accumulated (one copy per response byte instead of two); keepalive as one persistent timer instead of a `clearTimeout`/`setTimeout` pair per flush | 16-key 128 B batches: 21.5k → 27.3k req/s (+28 %), client CPU 64 → 46 µs/req; single-key 128 B: 204k → 294k req/s (+43 %), client CPU 5.9 → 3.8 µs/req; every pair | kept |

The 1 KiB and 4 KiB profiles show the round-4 equilibrium effect throughout: a cheaper
client shifts loopback packing, so server CPU/req moves ±4 % between client builds even
under identical server binaries; req/s and client CPU are the comparable numbers above.

Protocol-v1 leftovers dropped along the way: `decodeValues`/`decodeFlags` and the v1
protocol comment in `wire.ts`, stale `_multi` doc comments in the Rust client.

## Metrics counters (2026-08-31)

`GET /metrics` (Prometheus text) added per-shard relaxed counters to the engine —
get/del hits and misses, evictions — on a 128-aligned stats line per shard, plus
front-end counters (auth failures, refused SETs) off the hot path. The first cut
incremented a shard counter per key and cost +3–5 % server CPU/req on all three
profiles (12 threads write the same per-shard lines); `get_many`/`del_many` now tally
locally and add once per batch, which measures neutral. Three alternating pairs,
quiet machine, min-of-3 (baseline → with metrics):

| profile | baseline | with metrics |
|---|---|---|
| 8×16, 16-key batch, 128 B | 5.34–5.57 µs/req | 5.40–5.45 µs/req |
| 8×16, single key, 128 B | 1.24–1.26 µs/req | 1.24 µs/req |
| 8×16, 16-key batch, 1 KiB 50/50 | 36.91–37.74 µs/req | 36.07–36.99 µs/req |

Deltas flip sign between pairs on every profile; the per-key variant's regression was
outside that band and is the shape to avoid in future counters.

## Round 14: glibc fast paths and per-wake epoch pin (2026-08-31)

`perf` on the 16-key 128 B profile put ~15 % of user time in glibc's allocator —
`unlink_chunk` alone at 7.5 % — and 4 % in `Guard::flush`/`try_advance` on the
single-key profile. Same harness as round 13 (2 s warm-up, 6 s runs, min of 3
alternating pairs, server user/sys from `/proc`); profiles: A single key 128 B 10 % w,
B 16-key batch 128 B 10 % w, K 16-key 1 KiB 50/50, W 12×32 16-key 4 KiB 90 % w
(66 KiB frames), E 16-key 128 B 50 % w, 1M keys, `--capacity 32M` (evicting).

| change | measurement | verdict |
|---|---|---|
| small entries allocated 16-aligned with the header placed at the first 64-byte boundary inside a `PAD = 48`-byte-padded allocation (`Meta::pad` records the offset for `Drop`), replacing the round-8 `align = 64` allocation: glibc serves `align > 16` through `_int_memalign`, which bypasses the per-thread tcache, so every small entry paid the arena path | B: user 4.23 → 3.99 µs/req (−5.5 %, every pair), +3 % req/s; E: user −5.5 % (both pairs); A, K, W neutral; RSS flat. `_int_memalign` gone from the profile, `unlink_chunk` 7.5 → 4.1 % | kept |
| one epoch pin per connection wake (`serve_connection` drains every buffered frame under a single guard; `dispatch` takes `&Guard`): the cache's per-request pins become re-entrant counter bumps instead of full fences, and a single-key GET borrows its entry under the wake's pin (`Cache::get_in`) with no refcount round-trip | A: total 1.385 → 1.362 µs/req, better in 6/6 pairs (−1.5 %); B, W neutral | kept |
| `GLIBC_TUNABLES=glibc.malloc.tcache_count=1024` (default 7 chunks per bin; a 16-SET batch frees in bursts that overflow it). Env-only — no `mallopt` — so it ships as a `Dockerfile` `ENV` and a README note rather than in `tune_allocator` | E: user 12.3 → 10.3 (−16 %), total −20 %, +14 % req/s (both pairs); A: +6–7 % req/s, total neutral; B, W neutral | kept (deployment env) |

Still open, in expected-value order: a size-class slab on an `MADV_HUGEPAGE` region
(glibc is still ~14 % of B-profile user time, and 1 KiB+ entries never fit tcache);
PGO — blocked on this box, rustc 1.98 has no matching `llvm-profdata` (no rustup
`llvm-tools`, no Void llvm package installed) and BOLT is likewise absent; the idle
deadline costs ~2–3 % of the single-key profile (`Instant::now` twice plus a timer
(de)registration per wake) but a coarser scheme needs care around the flush-stall
semantics; deferring the small-retire `Guard::flush` from per-SET to per-wake
(round 9 showed cross-request deferral hurts allocator warmth; within a wake it
would not, but it needs a `set_many` variant that hands `Retired` back).

## Round 15: size-class slab on huge pages (2026-08-31)

The round-8 plan, finally executed: entry allocations leave glibc for a process-wide
size-class slab (`crates/server/src/cache/slab.rs`). Per-class free lists under short
mutexes, fronted by per-thread magazines that refill/flush ~64 KiB of chunks at a time
(drained to the lists when a thread exits); chunks are carved from 8 MiB regions,
2 MiB-aligned and `MADV_HUGEPAGE`d. Classes: 64-byte steps to 1 KiB, then four per
doubling to 64 KiB (waste ≤ 1/4 + 63 B); every class is a multiple of 64, so entries are
cache-line aligned for free and round 14's padded-allocation trick is gone. Entries
beyond 64 KiB stay on glibc (exact size, `Meta::class = 255`). Allocation on one thread,
free on whichever thread flushes the epoch collector; the magazines fall back to the
central lists during TLS teardown.

Same harness, both binaries under the shipped `tcache_count=1024`, min of 3 (2 for A/W)
alternating pairs — better in **every pair on every profile**:

| profile | before (user / total µs/req, req/s) | slab | user delta |
|---|---|---|---|
| A: single key 128 B | 0.58 / 1.31, 1.55M | 0.51 / 1.21, 1.63M | **−12 %** |
| B: 16-key 128 B | 3.97 / 5.25, 528k | 3.45 / 4.60, 626k | **−13 %** (+18 % req/s) |
| K: 16-key 1 KiB 50/50 | 21.7 / 36.0, 125k | 14.3 / 28.5, 163k | **−34 %** (+30 % req/s) |
| E: eviction 128 B 50 % w | 10.2 / 12.2, 378k | 8.75 / 10.6, 422k | **−14 %** |
| W: 12×32 16-key 4 KiB 90 % w | 124 / 247, 17.7k | 104 / 227, 18.9k | **−16 %** |

Huge pages materialise (THP `madvise` mode): 270 of 300 MB RSS on `AnonHugePages`
during a B run. RSS on the 4 KiB write profile is flat at 771 MB over 16 s. `perf` on K
afterwards: glibc `malloc` is 1.6 % of user time (the `_int_malloc`/`unlink_chunk`/free
family was ~20 % on this shape); what remains is `Table::find` 20 %, `Shard::set` 20 %,
`dispatch` 15 %, entry drop glue 11 %, the non-temporal value copy 10 %, and dead-entry
compaction ~10 % — compaction is the next visible target.

The trade, measured: slab memory is never returned to the kernel, per class. A server
that serves one value-size mix and then switches (a 1 GB budget of 4 KiB entries, then
128 B entries) plateaus at the *sum* of the two mixes' peaks — 1.22 GB RSS in that
experiment — because the old classes' chunks cannot serve the new sizes. It stops there;
steady mixes and same-size churn reuse chunks exactly. If mix shifts ever matter, the
follow-up is one-way splitting of larger free chunks into smaller classes.
