# Build and runtime for the oxicache server; works with docker and podman:
#   podman build -t oxicache .
#   podman run --rm -e OXICACHE_TOKEN=s3cret -p 4433:4433 oxicache
# Configuration is environment variables only; see crates/server/src/main.rs
# for the full list (OXICACHE_TCP_ADDR, OXICACHE_HTTP_ADDR, OXICACHE_CAPACITY,
# OXICACHE_TOKEN, OXICACHE_IDLE_TIMEOUT, OXICACHE_MAX_CONNS, OXICACHE_TLS_CERT).

FROM docker.io/library/rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --locked -p oxicache-server

# glibc runtime: the server tunes the glibc allocator at startup and the
# TLS stack is pure Rust, so nothing else is needed.
FROM docker.io/library/debian:bookworm-slim
COPY --from=build /src/target/release/oxicache-server /usr/local/bin/oxicache-server
USER 65534:65534
# The per-thread tcache holds only 7 chunks per size class by default; a
# 16-item write batch retires entries in bursts that overflow it into the
# arena's slow path. 1024 costs a few hundred KiB per thread at most and
# was measured -20 % server CPU/req on the eviction-heavy profile
# (docs/performance.md, round 14). Env-only: glibc reads it at startup.
ENV GLIBC_TUNABLES=glibc.malloc.tcache_count=1024
EXPOSE 4433
ENTRYPOINT ["/usr/local/bin/oxicache-server"]
