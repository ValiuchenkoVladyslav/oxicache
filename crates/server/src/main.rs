use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Result, bail};
use clap::Parser;
use oxicache_server::{Cache, Identity, Server};
use tracing::info;

/// HTTP/3 in-memory cache server with S3-FIFO eviction.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// UDP address to listen on.
    #[arg(long, default_value = "0.0.0.0:4433")]
    bind: SocketAddr,
    /// Memory budget for cached entries, e.g. 512M, 4G.
    #[arg(long, default_value = "1G", value_parser = parse_size)]
    capacity: usize,
    /// Number of independent cache shards (default: available CPUs).
    #[arg(long)]
    shards: Option<usize>,
    /// PEM certificate chain; a self-signed cert is generated when omitted.
    #[arg(long, requires = "key")]
    cert: Option<PathBuf>,
    /// PEM private key.
    #[arg(long, requires = "cert")]
    key: Option<PathBuf>,
}

fn parse_size(s: &str) -> Result<usize> {
    let s = s.trim();
    let (num, mul) = match s.chars().last() {
        Some(c) if c.is_ascii_digit() => (s, 1usize),
        Some('K' | 'k') => (&s[..s.len() - 1], 1 << 10),
        Some('M' | 'm') => (&s[..s.len() - 1], 1 << 20),
        Some('G' | 'g') => (&s[..s.len() - 1], 1 << 30),
        _ => bail!("unknown size suffix in {s:?}"),
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
    let identity = match (&args.cert, &args.key) {
        (Some(c), Some(k)) => Identity::from_pem(c, k)?,
        _ => {
            info!("no --cert/--key given, using an ephemeral self-signed certificate");
            Identity::self_signed()?
        }
    };
    let server = Server::bind(args.bind, identity, cache)?;
    info!(capacity = args.capacity, shards, "cache ready");

    tokio::select! {
        _ = server.run() => {}
        _ = tokio::signal::ctrl_c() => info!("shutting down"),
    }
    server.close().await;
    Ok(())
}
