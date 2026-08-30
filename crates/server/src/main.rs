use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use oxicache_server::{Cache, HttpServer, Options, Server};
use oxicache_wire::cli::warn_if_overridden;
use tracing::info;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// In-memory cache server with S3-FIFO eviction; binary protocol over TCP and, optionally, HTTP.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// Address to listen on.
    #[arg(long, env = "OXICACHE_ADDR", default_value = "0.0.0.0:4433")]
    addr: SocketAddr,
    /// Address for the HTTP front end (same binary protocol over HTTP plus
    /// `/health`); unset disables it.
    #[arg(long, env = "OXICACHE_HTTP_ADDR")]
    http_addr: Option<SocketAddr>,
    /// Memory budget for cached entries, e.g. 512M, 4G.
    #[arg(long, env = "OXICACHE_CAPACITY", default_value = "1G", value_parser = parse_size)]
    capacity: NonZeroUsize,
    /// Number of independent cache shards (default: available CPUs).
    #[arg(long, env = "OXICACHE_SHARDS")]
    shards: Option<NonZeroUsize>,
    /// Shared secret every client must present (AUTH frame on TCP, Bearer
    /// header on HTTP). Required and non-empty.
    #[arg(long, env = "OXICACHE_TOKEN", hide_env_values = true, value_parser = parse_token)]
    token: String,
    /// Close a connection that sends nothing for this many seconds (on HTTP:
    /// a keep-alive connection that starts no request).
    #[arg(
        long,
        env = "OXICACHE_IDLE_TIMEOUT",
        default_value_t = NonZeroU64::new(oxicache_server::DEFAULT_IDLE_TIMEOUT.as_secs()).unwrap(),
        value_name = "SECS"
    )]
    idle_timeout: NonZeroU64,
    /// Most open connections, TCP and HTTP together; beyond it the listeners
    /// stop accepting until one closes.
    #[arg(
        long,
        env = "OXICACHE_MAX_CONNS",
        default_value_t = oxicache_server::DEFAULT_MAX_CONNECTIONS,
        value_name = "N"
    )]
    max_conns: NonZeroUsize,
}

fn parse_token(s: &str) -> Result<String> {
    if s.is_empty() {
        return Err("the token must not be empty".into());
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

#[tokio::main]
async fn main() -> Result<()> {
    oxicache_wire::io::tune_allocator();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?),
        )
        .init();
    let args = Args::parse();
    warn_if_overridden(
        "addr",
        "OXICACHE_ADDR",
        Some(&args.addr),
        |s| s.parse().ok(),
        true,
    );
    warn_if_overridden(
        "http-addr",
        "OXICACHE_HTTP_ADDR",
        args.http_addr.as_ref(),
        |s| s.parse().ok(),
        true,
    );
    warn_if_overridden(
        "capacity",
        "OXICACHE_CAPACITY",
        Some(&args.capacity),
        |s| parse_size(s).ok(),
        true,
    );
    warn_if_overridden(
        "shards",
        "OXICACHE_SHARDS",
        args.shards.as_ref(),
        |s| s.parse().ok(),
        true,
    );
    warn_if_overridden(
        "token",
        "OXICACHE_TOKEN",
        Some(&args.token),
        |s| Some(s.to_string()),
        false,
    );
    warn_if_overridden(
        "idle-timeout",
        "OXICACHE_IDLE_TIMEOUT",
        Some(&args.idle_timeout),
        |s| s.parse().ok(),
        true,
    );
    warn_if_overridden(
        "max-conns",
        "OXICACHE_MAX_CONNS",
        Some(&args.max_conns),
        |s| s.parse().ok(),
        true,
    );

    let shards = args
        .shards
        .unwrap_or_else(|| std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN));
    let cache = Arc::new(Cache::new(args.capacity, shards));
    // One `Options` for both listeners, so they share one connection budget.
    let opts = Options::new(args.token)
        .idle_timeout(Some(Duration::from_secs(args.idle_timeout.get())))
        .max_connections(Some(args.max_conns));
    let server = Server::bind(args.addr, cache.clone(), opts.clone())?;
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
