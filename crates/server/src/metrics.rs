//! Operational counters and their Prometheus text rendering. One
//! [`Metrics`] is shared by every listener bound from one
//! [`Options`](crate::tcp::Options) (a clone shares it, like the connection
//! budget), and the HTTP front end serves it at `GET /metrics`.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use crate::cache::Cache;
use crate::tcp::ConnLimit;

/// Counters owned by the front ends: what was refused, not what the cache
/// holds. Cache-level numbers (hits, misses, evictions, bytes, items) live
/// with the cache's shards and are summed when rendered.
pub struct Metrics {
    /// Requests refused for a wrong or missing token: one per TCP
    /// connection, one per HTTP request.
    pub auth_failures: AtomicU64,
    /// SET requests refused because the entry exceeds the per-shard capacity.
    pub set_too_large: AtomicU64,
    start: Instant,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            auth_failures: AtomicU64::new(0),
            set_too_large: AtomicU64::new(0),
            start: Instant::now(),
        }
    }

    /// Every metric, in the Prometheus text exposition format.
    pub fn render(&self, cache: &Cache, limit: Option<&ConnLimit>) -> String {
        use std::fmt::Write;
        let s = cache.stats();
        let mut out = String::with_capacity(2 << 10);
        let mut metric = |name: &str, kind: &str, help: &str, value: u64| {
            let _ = write!(
                out,
                "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}\n"
            );
        };
        metric(
            "oxicache_get_hits_total",
            "counter",
            "GET requests answered with a value",
            s.get_hits,
        );
        metric(
            "oxicache_get_misses_total",
            "counter",
            "GET requests answered not found",
            s.get_misses,
        );
        metric(
            "oxicache_del_hits_total",
            "counter",
            "DEL requests that removed an entry",
            s.del_hits,
        );
        metric(
            "oxicache_del_misses_total",
            "counter",
            "DEL requests answered not found",
            s.del_misses,
        );
        metric(
            "oxicache_evictions_total",
            "counter",
            "Live entries evicted to make room for new ones",
            s.evictions,
        );
        metric(
            "oxicache_set_too_large_total",
            "counter",
            "SET requests refused because the entry exceeds the per-shard capacity",
            self.set_too_large.load(Relaxed),
        );
        metric(
            "oxicache_auth_failures_total",
            "counter",
            "Requests refused for a wrong or missing token",
            self.auth_failures.load(Relaxed),
        );
        metric(
            "oxicache_used_bytes",
            "gauge",
            "Bytes held by cached entries, bookkeeping included",
            s.used_bytes as u64,
        );
        metric(
            "oxicache_capacity_bytes",
            "gauge",
            "Configured memory budget for cached entries",
            s.capacity as u64,
        );
        metric(
            "oxicache_items",
            "gauge",
            "Entries currently cached",
            s.items as u64,
        );
        if let Some(l) = limit {
            metric(
                "oxicache_open_connections",
                "gauge",
                "Connections currently open, TCP and HTTP together",
                l.open() as u64,
            );
            metric(
                "oxicache_max_connections",
                "gauge",
                "Configured cap on open connections",
                l.max().get() as u64,
            );
            metric(
                "oxicache_accept_waits_total",
                "counter",
                "Accepts that had to wait for a free connection slot",
                l.waits(),
            );
        }
        metric(
            "oxicache_uptime_seconds",
            "gauge",
            "Seconds since the server started",
            self.start.elapsed().as_secs(),
        );
        let _ = write!(
            out,
            "# HELP oxicache_build_info Server version, as a label\n\
             # TYPE oxicache_build_info gauge\n\
             oxicache_build_info{{version=\"{}\"}} 1\n",
            env!("CARGO_PKG_VERSION")
        );
        out
    }
}
