use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use oxicache_server::{Cache, Options, Server};
use tracing::info;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// TCP in-memory cache server with S3-FIFO eviction.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// Address to listen on.
    #[arg(long, env = "OXICACHE_ADDR", default_value = "0.0.0.0:4433")]
    addr: SocketAddr,
    /// Memory budget for cached entries, e.g. 512M, 4G.
    #[arg(long, env = "OXICACHE_CAPACITY", default_value = "1G", value_parser = parse_size)]
    capacity: usize,
    /// Number of independent cache shards (default: available CPUs).
    #[arg(long, env = "OXICACHE_SHARDS")]
    shards: Option<usize>,
    /// Shared secret clients must present once per connection (AUTH frame).
    /// Unset or empty disables authentication.
    #[arg(long, env = "OXICACHE_TOKEN", hide_env_values = true)]
    token: Option<String>,
}

fn parse_size(s: &str) -> Result<usize> {
    let s = s.trim();
    let (num, mul) = match s.chars().last() {
        Some(c) if c.is_ascii_digit() => (s, 1usize),
        Some('K' | 'k') => (&s[..s.len() - 1], 1 << 10),
        Some('M' | 'm') => (&s[..s.len() - 1], 1 << 20),
        Some('G' | 'g') => (&s[..s.len() - 1], 1 << 30),
        _ => return Err(format!("unknown size suffix in {s:?}").into()),
    };
    Ok(num.trim().parse::<usize>()? * mul)
}

/// Warn when a flag overrides a differing value of its environment variable.
/// clap prefers the flag silently; a parsed value that differs from a set
/// variable can only have come from the flag. Values are compared after
/// parsing, so `1G` and `1024M` agree. `show` controls whether the values are
/// printed (not for secrets).
fn warn_if_overridden<T: PartialEq + std::fmt::Display>(
    flag: &str,
    var: &str,
    value: Option<&T>,
    parse: impl Fn(&str) -> Option<T>,
    show: bool,
) {
    let (Ok(env), Some(value)) = (std::env::var(var), value) else {
        return;
    };
    if parse(&env).as_ref() != Some(value) {
        if show {
            eprintln!("warning: --{flag}={value} overrides {var}={env}");
        } else {
            eprintln!("warning: --{flag} overrides a different {var}");
        }
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
    let args = Args::parse();
    warn_if_overridden(
        "addr",
        "OXICACHE_ADDR",
        Some(&args.addr),
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
        args.token.as_ref(),
        |s| Some(s.to_string()),
        false,
    );

    let shards = args
        .shards
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()));
    let cache = Arc::new(Cache::new(args.capacity, shards));
    let token = args.token.filter(|t| !t.is_empty()).map(String::into_bytes);
    let auth = token.is_some();
    let opts = Options { token };
    let server = Arc::new(Server::bind_with(args.addr, cache, opts)?);
    info!(capacity = args.capacity, shards, auth, "cache ready");

    tokio::select! {
        _ = server.run() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
    info!("shutting down");
    Ok(())
}
