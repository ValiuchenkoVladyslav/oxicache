use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use oxicache_client::{Batch, Client, ServerName, Tls};

mod cli;
use cli::warn_if_overridden;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Command line client for oxicache.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// Server address, `host:port`; with --tls-ca the host is also the name
    /// the server's certificate must be issued for.
    #[arg(long, env = "OXICACHE_ADDR", default_value = "127.0.0.1:4433")]
    addr: String,
    /// Shared secret presented on every connection.
    #[arg(long, env = "OXICACHE_TOKEN", hide_env_values = true)]
    token: String,
    /// Connect over TLS, trusting the CA certificates in this PEM file.
    #[arg(long, value_name = "PEM")]
    tls_ca: Option<PathBuf>,
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
    oxicache_wire::io::tune_allocator();
    let args = Args::parse();
    warn_if_overridden(
        "addr",
        "OXICACHE_ADDR",
        Some(&args.addr),
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
    let token = args.token.as_bytes();
    let (host, _) = args
        .addr
        .rsplit_once(':')
        .ok_or("--addr must be host:port")?;
    if let Cmd::Set { kv } = &args.cmd
        && kv.len() % 2 != 0
    {
        return Err("set expects key value pairs".into());
    }
    // `localhost` may resolve to ::1 before 127.0.0.1; the first address
    // that accepts is the server's, for every connection from here on.
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host(&args.addr).await?.collect();
    if addrs.is_empty() {
        return Err(format!("{}: no address", args.addr).into());
    }
    let tls = match args.tls_ca.as_deref() {
        Some(ca) => {
            let name = ServerName::try_from(host.trim_matches(['[', ']']).to_string())?;
            Some(Tls::trusting(ca, name)?)
        }
        None => None,
    };
    let mut last = None;
    let mut connected = None;
    for &a in &addrs {
        match connect(a, tls.as_ref(), token).await {
            Ok(c) => {
                connected = Some((a, c));
                break;
            }
            // Only an address nobody answered on is worth skipping: a
            // server that answered (a refused token, say) is the server.
            Err(e @ (oxicache_client::Error::Io(_) | oxicache_client::Error::Closed)) => {
                last = Some(e);
            }
            Err(e) => return Err(e.into()),
        }
    }
    let Some((addr, client)) = connected else {
        return Err(last.expect("at least one address").into());
    };
    match args.cmd {
        Cmd::Get { keys } => {
            // Any MessagePack value prints; strings without their quotes.
            let vals: Vec<Option<rmpv::Value>> = if let [key] = keys.as_slice() {
                vec![client.get(key).await?]
            } else {
                let mut b = Batch::new();
                let slots: Vec<_> = keys.iter().map(|k| b.get::<rmpv::Value>(k)).collect();
                let out = client.batch(b).await?;
                slots
                    .into_iter()
                    .map(|s| out.get(s))
                    .collect::<oxicache_client::Result<_>>()?
            };
            for (k, v) in keys.iter().zip(vals) {
                match v {
                    Some(rmpv::Value::String(s)) => println!("{k}: {}", s.as_str().unwrap_or("")),
                    Some(v) => println!("{k}: {v}"),
                    None => println!("{k}: (nil)"),
                }
            }
        }
        Cmd::Set { kv } => {
            // Values are stored as MessagePack strings.
            let pairs: Vec<(&str, &str)> = kv
                .chunks(2)
                .map(|c| (c[0].as_str(), c[1].as_str()))
                .collect();
            if let [(k, v)] = pairs.as_slice() {
                client.set(k, v).await?;
            } else {
                let mut b = Batch::new();
                let slots = pairs
                    .iter()
                    .map(|(k, v)| b.set(k, v))
                    .collect::<oxicache_client::Result<Vec<_>>>()?;
                let out = client.batch(b).await?;
                for s in slots {
                    out.get(s)?;
                }
            }
            println!("OK ({} entries)", pairs.len());
        }
        Cmd::Del { keys } => {
            let flags: Vec<bool> = if let [key] = keys.as_slice() {
                vec![client.del(key).await?]
            } else {
                let mut b = Batch::new();
                let slots: Vec<_> = keys.iter().map(|k| b.del(k)).collect();
                let out = client.batch(b).await?;
                slots
                    .into_iter()
                    .map(|s| out.get(s))
                    .collect::<oxicache_client::Result<_>>()?
            };
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
            drop(client);
            bench(
                addr,
                tls,
                token,
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

async fn connect(
    addr: SocketAddr,
    tls: Option<&Tls>,
    token: &[u8],
) -> oxicache_client::Result<Client> {
    match tls {
        Some(tls) => Client::connect_tls(addr, tls.clone(), token).await,
        None => Client::connect(addr, token).await,
    }
}

#[allow(clippy::too_many_arguments)]
async fn bench(
    addr: SocketAddr,
    tls: Option<Tls>,
    token: &[u8],
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
    let value = "x".repeat(value_size);
    // One contiguous buffer with a fixed stride: picking a key is index
    // arithmetic, not a pointer chase into a separate allocation per key.
    let key_len = "bench:00000000".len();
    let keys: Arc<[u8]> = (0..keyspace)
        .flat_map(|i| format!("bench:{i:08}").into_bytes())
        .collect();

    let mut clients = Vec::with_capacity(conns);
    for _ in 0..conns {
        clients.push(connect(addr, tls.as_ref(), token).await?);
    }
    let start = Instant::now();
    let deadline = start + Duration::from_secs(seconds);

    let mut tasks = Vec::new();
    for (c, client) in clients.into_iter().enumerate() {
        for p in 0..pipeline {
            let (client, ops, reqs, hits, value, keys) = (
                client.clone(),
                ops.clone(),
                reqs.clone(),
                hits.clone(),
                value.clone(),
                keys.clone(),
            );
            tasks.push(tokio::spawn(async move {
                let mut rng = (c * 1_000_003 + p * 7919 + 1) as u64;
                let mut next = move || {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    rng
                };
                // Reused across iterations: the bench client caps the whole
                // loopback run, so per-request allocations here cost
                // throughput. The batch buffers are sized for headers, keys
                // and encoded values with room to spare.
                let write_bytes = batch * (key_len + value_size + 24);
                let read_bytes = batch * (key_len + 8);
                let mut ks: Vec<&[u8]> = Vec::with_capacity(batch);
                let mut slots = Vec::with_capacity(batch);
                while Instant::now() < deadline {
                    ks.clear();
                    for _ in 0..batch {
                        let i = next() as usize % keyspace * key_len;
                        ks.push(&keys[i..i + key_len]);
                    }
                    let write = (next() % 10_000) as f64 / 10_000.0 < write_ratio;
                    // Skip over each value without building it: the bench
                    // measures the cache, not deserialisation.
                    type Skip = serde::de::IgnoredAny;
                    if let [k] = ks.as_slice() {
                        if write {
                            client.set(k, value.as_str()).await?;
                        } else if client.get::<Skip>(k).await?.is_some() {
                            hits.fetch_add(1, Relaxed);
                        }
                    } else {
                        let mut b =
                            Batch::with_capacity(if write { write_bytes } else { read_bytes });
                        if write {
                            for k in &ks {
                                b.set(k, value.as_str())?;
                            }
                            client.batch(b).await?;
                        } else {
                            slots.clear();
                            for k in &ks {
                                slots.push(b.get::<Skip>(k));
                            }
                            let out = client.batch(b).await?;
                            for &s in &slots {
                                if out.get(s)?.is_some() {
                                    hits.fetch_add(1, Relaxed);
                                }
                            }
                        }
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
