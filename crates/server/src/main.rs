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
    #[arg(long, default_value = "0.0.0.0:4433")]
    bind: SocketAddr,
    /// Memory budget for cached entries, e.g. 512M, 4G.
    #[arg(long, default_value = "1G", value_parser = parse_size)]
    capacity: usize,
    /// Number of independent cache shards (default: available CPUs).
    #[arg(long)]
    shards: Option<usize>,
    /// Listeners sharing the port via SO_REUSEPORT (default: available CPUs).
    #[arg(long)]
    endpoints: Option<usize>,
    /// Run each listener on its own single-threaded runtime (thread-per-core).
    #[arg(long)]
    per_core: bool,
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?),
        )
        .init();
    let args = Args::parse();

    let shards = args
        .shards
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()));
    let cache = Arc::new(Cache::new(args.capacity, shards));
    let opts = Options {
        endpoints: args.endpoints.unwrap_or(shards),
    };
    let server = Arc::new(Server::bind_with(args.bind, cache, opts)?);
    info!(capacity = args.capacity, shards, "cache ready");

    if args.per_core {
        let s = server.clone();
        std::thread::spawn(move || s.run_per_core());
        tokio::signal::ctrl_c().await?;
    } else {
        tokio::select! {
            _ = server.run() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    info!("shutting down");
    Ok(())
}
