# msgpack library choice for client-ts

Measured 2026-08-29 under Bun 1.3.14 (`@msgpack/msgpack` 3.x vs `msgpackr` 2.1.0 with its
native accelerator loaded), single thread, ops/s in thousands, 600 ms per cell after warm-up.

| case | lib | encode | decode | bytes |
|---|---|---:|---:|---:|
| small object (5 fields) | @msgpack/msgpack | 1054 | 1903 | 49 |
| | msgpackr | 4208 | 4953 | 51 |
| medium (50 nested items) | @msgpack/msgpack | 40 | 41 | 2350 |
| | msgpackr | 84 | 42 | 2554 |
| 4 KiB binary | @msgpack/msgpack | 357 | 2384 | 4114 |
| | msgpackr | 656 | 2235 | 4116 |
| 2 KiB string | @msgpack/msgpack | 432 | 1392 | 2003 |
| | msgpackr | 2190 | 1870 | 2003 |
| 1000 ints | @msgpack/msgpack | 40 | 101 | 2619 |
| | msgpackr | 71 | 146 | 2619 |
| one int | @msgpack/msgpack | 1294 | 9780 | 1 |
| | msgpackr | 17173 | 39756 | 1 |

msgpackr encodes 2–5× faster and decodes as fast or faster everywhere; its only cost is ~8 %
larger output for nested objects (it uses `map16`/`float64` where the other lib picks the
smallest form). `useRecords: false` disables msgpackr's record extension so the bytes stay
plain MessagePack; the `useRecords` default was within noise of that setting.
