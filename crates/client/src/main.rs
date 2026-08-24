use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use oxicache_client::{Client, Config, Tls};
use rustls_pki_types::{CertificateDer, pem::PemObject};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Command line client for oxicache.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// Server address.
    #[arg(long, default_value = "127.0.0.1:4433")]
    addr: SocketAddr,
    /// Expected server name (SNI).
    #[arg(long, default_value = "localhost")]
    server_name: String,
    /// PEM certificate to pin; without it any certificate is accepted.
    #[arg(long)]
    ca: Option<std::path::PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Fetch one or more keys.
    Get { keys: Vec<String> },
    /// Store key/value pairs given as alternating arguments.
    Set { kv: Vec<String> },
    /// Delete one or more keys.
    Del { keys: Vec<String> },
    /// Run a closed-loop load test.
    Bench {
        /// Concurrent connections.
        #[arg(long, default_value_t = 8)]
        conns: usize,
        /// In-flight requests per connection.
        #[arg(long, default_value_t = 16)]
        pipeline: usize,
        /// Keys per request.
        #[arg(long, default_value_t = 16)]
        batch: usize,
        /// Value size in bytes.
        #[arg(long, default_value_t = 128)]
        value_size: usize,
        /// Key space size.
        #[arg(long, default_value_t = 100_000)]
        keys: usize,
        /// Fraction of requests that are sets (rest are gets).
        #[arg(long, default_value_t = 0.1)]
        write_ratio: f64,
        #[arg(long, default_value_t = 10)]
        seconds: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let tls = match &args.ca {
        Some(p) => Tls::Pinned(
            CertificateDer::pem_file_iter(p)?.collect::<std::result::Result<Vec<_>, _>>()?,
        ),
        None => Tls::Insecure,
    };
    let config = Config {
        server_name: args.server_name.clone(),
        tls,
    };

    match args.cmd {
        Cmd::Get { keys } => {
            let client = Client::connect(args.addr, config).await?;
            let vals = client.get(keys.iter().map(String::as_bytes)).await?;
            for (k, v) in keys.iter().zip(vals) {
                match v {
                    Some(v) => println!("{k}: {}", String::from_utf8_lossy(&v)),
                    None => println!("{k}: (nil)"),
                }
            }
        }
        Cmd::Set { kv } => {
            if kv.len() % 2 != 0 {
                return Err("set expects key value pairs".into());
            }
            let client = Client::connect(args.addr, config).await?;
            let pairs: Vec<(&[u8], &[u8])> = kv
                .chunks(2)
                .map(|c| (c[0].as_bytes(), c[1].as_bytes()))
                .collect();
            client.set(pairs.iter().copied()).await?;
            println!("OK ({} entries)", pairs.len());
        }
        Cmd::Del { keys } => {
            let client = Client::connect(args.addr, config).await?;
            let flags = client.del(keys.iter().map(String::as_bytes)).await?;
            for (k, f) in keys.iter().zip(flags) {
                println!("{k}: {}", if f { "deleted" } else { "(nil)" });
            }
        }
        Cmd::Bench {
            conns,
            pipeline,
            batch,
            value_size,
            keys,
            write_ratio,
            seconds,
        } => {
            bench(
                args.addr,
                config,
                conns,
                pipeline,
                batch,
                value_size,
                keys,
                write_ratio,
                seconds,
            )
            .await?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn bench(
    addr: SocketAddr,
    config: Config,
    conns: usize,
    pipeline: usize,
    batch: usize,
    value_size: usize,
    keyspace: usize,
    write_ratio: f64,
    seconds: u64,
) -> Result<()> {
    let ops = Arc::new(AtomicU64::new(0));
    let reqs = Arc::new(AtomicU64::new(0));
    let hits = Arc::new(AtomicU64::new(0));
    let value = vec![b'x'; value_size];
    let key = |i: usize| format!("bench:{i:08}");

    let mut clients = Vec::with_capacity(conns);
    for _ in 0..conns {
        clients.push(Client::connect(addr, config.clone()).await?);
    }
    let start = Instant::now();
    let deadline = start + Duration::from_secs(seconds);

    let mut tasks = Vec::new();
    for (c, client) in clients.into_iter().enumerate() {
        for p in 0..pipeline {
            let (client, ops, reqs, hits, value) = (
                client.clone(),
                ops.clone(),
                reqs.clone(),
                hits.clone(),
                value.clone(),
            );
            tasks.push(tokio::spawn(async move {
                let mut rng = (c * 1_000_003 + p * 7919 + 1) as u64;
                let mut next = move || {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    rng
                };
                while Instant::now() < deadline {
                    let ks: Vec<String> = (0..batch)
                        .map(|_| key(next() as usize % keyspace))
                        .collect();
                    if (next() % 10_000) as f64 / 10_000.0 < write_ratio {
                        let pairs: Vec<(&[u8], &[u8])> = ks
                            .iter()
                            .map(|k| (k.as_bytes(), value.as_slice()))
                            .collect();
                        client.set(pairs.iter().copied()).await?;
                    } else {
                        let r = client.get(ks.iter().map(String::as_bytes)).await?;
                        hits.fetch_add(r.iter().flatten().count() as u64, Relaxed);
                    }
                    ops.fetch_add(batch as u64, Relaxed);
                    reqs.fetch_add(1, Relaxed);
                }
                Ok::<_, oxicache_client::Error>(())
            }));
        }
    }
    for t in tasks {
        t.await??;
    }
    let secs = start.elapsed().as_secs_f64();
    let (ops, reqs, hits) = (ops.load(Relaxed), reqs.load(Relaxed), hits.load(Relaxed));
    println!(
        "{conns} conns x {pipeline} pipeline, batch {batch}: {:.0} req/s, {:.0} ops/s, hit ratio {:.1}%",
        reqs as f64 / secs,
        ops as f64 / secs,
        100.0 * hits as f64 / ops.max(1) as f64
    );
    Ok(())
}
