//! The `oxicache-server` binary. It is configured by environment variables
//! only and never reads its command line:
//!
//! ```text
//! OXICACHE_TCP_ADDR      address the TCP front end listens on (default 0.0.0.0:4433)
//! OXICACHE_HTTP_ADDR     address for the HTTP front end, same protocol plus /health (default: off)
//! OXICACHE_CAPACITY      memory budget for cached entries, e.g. 512M, 4G (default 1G)
//! OXICACHE_TOKEN         shared secret every client must present; required, non-empty
//! OXICACHE_IDLE_TIMEOUT  close a connection that sends nothing for this many seconds (default 300)
//! OXICACHE_MAX_CONNS     most open connections, TCP and HTTP together (default 10000)
//! OXICACHE_TLS_CERT      PEM file with the certificate chain and private key; set = both
//!                        listeners speak TLS (default: unset, plain TCP and HTTP)
//! ```
//!
//! A missing token or an unparseable value fails startup like a bind error
//! does: the message on stderr and a non-zero exit.

use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use oxicache_server::{Cache, HttpServer, Options, Server};
use tracing::info;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DEFAULT_TCP_ADDR: &str = "0.0.0.0:4433";
const DEFAULT_CAPACITY: &str = "1G";

/// All configuration, read from `OXICACHE_*` variables only.
struct Config {
    tcp_addr: SocketAddr,
    http_addr: Option<SocketAddr>,
    capacity: NonZeroUsize,
    token: String,
    idle_timeout: NonZeroU64,
    max_conns: NonZeroUsize,
    tls_cert: Option<PathBuf>,
}

impl Config {
    fn from_env() -> Result<Self> {
        Ok(Self {
            tcp_addr: Self::env("OXICACHE_TCP_ADDR", Some(DEFAULT_TCP_ADDR), |s| {
                Ok(s.parse()?)
            })?,
            http_addr: Self::env_opt("OXICACHE_HTTP_ADDR", |s| Ok(s.parse()?))?,
            capacity: Self::env(
                "OXICACHE_CAPACITY",
                Some(DEFAULT_CAPACITY),
                Self::parse_size,
            )?,
            token: Self::env("OXICACHE_TOKEN", None, Self::parse_token)?,
            idle_timeout: Self::env(
                "OXICACHE_IDLE_TIMEOUT",
                Some(&oxicache_server::DEFAULT_IDLE_TIMEOUT.as_secs().to_string()),
                |s| Ok(s.parse()?),
            )?,
            max_conns: Self::env(
                "OXICACHE_MAX_CONNS",
                Some(&oxicache_server::DEFAULT_MAX_CONNECTIONS.to_string()),
                |s| Ok(s.parse()?),
            )?,
            tls_cert: Self::env_opt("OXICACHE_TLS_CERT", |s| {
                if s.is_empty() {
                    return Err("must be a path".into());
                }
                Ok(PathBuf::from(s))
            })?,
        })
    }

    /// The variable's value parsed with `parse`, or `default` parsed the same way
    /// when it is unset; a bad or missing value is an error naming the variable.
    fn env<T>(name: &str, default: Option<&str>, parse: impl Fn(&str) -> Result<T>) -> Result<T> {
        match std::env::var(name) {
            Ok(v) => parse(&v).map_err(|e| format!("{name}: {e}").into()),
            Err(_) => match default {
                Some(d) => parse(d),
                None => Err(format!("{name} is required and must not be empty").into()),
            },
        }
    }

    /// Like [`Self::env`], but unset is `None` rather than an error.
    fn env_opt<T>(name: &str, parse: impl Fn(&str) -> Result<T>) -> Result<Option<T>> {
        Self::env(name, None, |s| parse(s).map(Some)).or_else(|e| {
            if std::env::var_os(name).is_none() {
                Ok(None)
            } else {
                Err(e)
            }
        })
    }

    fn parse_token(s: &str) -> Result<String> {
        if s.is_empty() {
            return Err("must not be empty".into());
        }
        Ok(s.to_string())
    }

    fn parse_size(s: &str) -> Result<NonZeroUsize> {
        let s = s.trim();
        let (num, mul) = match s.chars().last() {
            Some(c) if c.is_ascii_digit() => (s, 1usize),
            Some('K' | 'k') => (&s[..s.len() - 1], 1 << 10),
            Some('M' | 'm') => (&s[..s.len() - 1], 1 << 20),
            Some('G' | 'g') => (&s[..s.len() - 1], 1 << 30),
            _ => return Err(format!("unknown size suffix in {s:?}").into()),
        };
        let n = num
            .trim()
            .parse::<usize>()?
            .checked_mul(mul)
            .ok_or_else(|| format!("size {s:?} is too large"))?;
        NonZeroUsize::new(n).ok_or_else(|| "the capacity must not be zero".into())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    oxicache_wire::io::tune_allocator();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?),
        )
        .init();

    let args = Config::from_env()?;

    // One shard per CPU, so writers on different cores rarely share a lock.
    let shards = std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN);
    let cache = Arc::new(Cache::new(args.capacity, shards));
    // One `Options` for both listeners, so they share one connection budget.
    let opts = Options::new(args.token)
        .idle_timeout(Some(Duration::from_secs(args.idle_timeout.get())))
        .max_connections(Some(args.max_conns))
        .tls(
            args.tls_cert
                .as_deref()
                .map(oxicache_server::tls::server_config)
                .transpose()
                .map_err(|e| format!("OXICACHE_TLS_CERT: {e}"))?,
        );
    let server = Server::bind(args.tcp_addr, cache.clone(), opts.clone())?;
    let http = args
        .http_addr
        .map(|a| HttpServer::bind(a, cache, opts))
        .transpose()?;

    info!(
        addr = %server.local_addr(),
        http = http.as_ref().map(|h| h.local_addr().to_string()),
        capacity = args.capacity,
        shards,
        idle_timeout = args.idle_timeout,
        max_conns = args.max_conns,
        tls = args.tls_cert.as_ref().map(|p| p.display().to_string()),
        "cache ready"
    );

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let stop = |mut rx: tokio::sync::watch::Receiver<bool>| async move {
        let _ = rx.changed().await;
    };
    let signal = async {
        let signal = shutdown_signal().await;
        info!(signal, "shutting down");
        let _ = stop_tx.send(true);
    };
    tokio::join!(signal, server.run_until(stop(stop_rx.clone())), async {
        if let Some(h) = http {
            h.run_until(stop(stop_rx)).await;
        }
    });
    Ok(())
}

/// Resolve to the name of the first shutdown signal: SIGINT anywhere, and
/// SIGTERM on unix, since that is what init systems and orchestrators send.
async fn shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => "SIGINT",
            _ = term.recv() => "SIGTERM",
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "SIGINT"
    }
}
