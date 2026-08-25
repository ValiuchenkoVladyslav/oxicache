//! TCP front end: one tokio task per connection, requests handled inline and
//! answered in order. Responses are flushed once the read side has no more
//! buffered input, so pipelined requests share a single write syscall.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use oxicache_wire::{self as wire, HEADER_LEN, Op, Status};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info};

use crate::cache::Cache;
use crate::error::{Error, Result};

/// Largest request body accepted.
pub const MAX_FRAME: usize = 64 << 20;
const BUF: usize = 64 << 10;

/// Transport tuning for [`Server::bind_with`].
#[derive(Clone, Debug)]
pub struct Options {
    /// Number of listeners sharing the port via `SO_REUSEPORT`, so the kernel
    /// spreads connections over independent accept loops.
    pub endpoints: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self { endpoints: 1 }
    }
}

pub struct Server {
    listeners: Vec<std::net::TcpListener>,
    cache: Arc<Cache>,
}

impl Server {
    /// Bind a single listener on `addr` serving `cache`.
    pub fn bind(addr: SocketAddr, cache: Arc<Cache>) -> Result<Self> {
        Self::bind_with(addr, cache, Options::default())
    }

    /// Bind with explicit [`Options`].
    pub fn bind_with(addr: SocketAddr, cache: Arc<Cache>, opts: Options) -> Result<Self> {
        if opts.endpoints == 0 {
            return Err(Error::NoEndpoints);
        }
        let bind = |source| Error::Bind { addr, source };
        let mut listeners = Vec::with_capacity(opts.endpoints);
        let mut bound = addr;
        for _ in 0..opts.endpoints {
            let l = tcp_listener(bound, opts.endpoints > 1).map_err(bind)?;
            // Port 0 must resolve once so every listener shares the same port.
            bound = l.local_addr().map_err(bind)?;
            listeners.push(l);
        }
        Ok(Self { listeners, cache })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listeners[0].local_addr().expect("bound listener")
    }

    /// Accept connections on the current runtime until the task is dropped.
    pub async fn run(&self) {
        info!(addr = %self.local_addr(), endpoints = self.listeners.len(), "listening (tcp)");
        let mut tasks = tokio::task::JoinSet::new();
        for l in &self.listeners {
            let l = l.try_clone().expect("clone listener");
            tasks.spawn(accept_loop(l, self.cache.clone()));
        }
        while tasks.join_next().await.is_some() {}
    }

    /// Thread-per-core mode: every listener gets its own OS thread running a
    /// single-threaded tokio runtime, so a connection never crosses threads.
    /// Blocks the calling thread.
    pub fn run_per_core(&self) {
        info!(addr = %self.local_addr(), endpoints = self.listeners.len(), "listening (tcp, thread-per-core)");
        std::thread::scope(|scope| {
            for (i, l) in self.listeners.iter().enumerate() {
                let (l, cache) = (l.try_clone().expect("clone listener"), self.cache.clone());
                std::thread::Builder::new()
                    .name(format!("oxicache-{i}"))
                    .spawn_scoped(scope, move || {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .expect("runtime");
                        rt.block_on(accept_loop(l, cache));
                    })
                    .expect("spawn listener thread");
            }
        });
    }
}

fn tcp_listener(addr: SocketAddr, reuse_port: bool) -> std::io::Result<std::net::TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    if reuse_port {
        socket.set_reuse_port(true)?;
    }
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    Ok(socket.into())
}

async fn accept_loop(listener: std::net::TcpListener, cache: Arc<Cache>) {
    let listener = TcpListener::from_std(listener).expect("register listener");
    loop {
        match listener.accept().await {
            Ok((stream, remote)) => {
                let cache = cache.clone();
                tokio::spawn(async move {
                    debug!(%remote, "connection open");
                    if let Err(e) = serve_connection(stream, &cache).await {
                        debug!(%remote, error = %e, "connection closed");
                    }
                });
            }
            Err(e) => debug!(error = %e, "accept failed"),
        }
    }
}

async fn serve_connection(stream: TcpStream, cache: &Cache) -> std::io::Result<()> {
    stream.set_nodelay(true)?;
    let (r, w) = stream.into_split();
    let mut r = BufReader::with_capacity(BUF, r);
    let mut w = BufWriter::with_capacity(BUF, w);
    let mut hdr = [0u8; HEADER_LEN];
    loop {
        match r.read_exact(&mut hdr).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        }
        let (op, len) = wire::decode_header(&hdr);
        if len > MAX_FRAME {
            write_frame(
                &mut w,
                Status::TooLarge,
                Bytes::from_static(b"frame exceeds limit"),
            )
            .await?;
            w.flush().await?;
            return Ok(());
        }
        let mut body = BytesMut::zeroed(len);
        r.read_exact(&mut body).await?;
        let (status, out) = dispatch(op, body.freeze(), cache);
        write_frame(&mut w, status, out).await?;
        if r.buffer().is_empty() {
            w.flush().await?;
        }
    }
}

async fn write_frame<W: AsyncWriteExt + Unpin>(
    w: &mut W,
    status: Status,
    body: Bytes,
) -> std::io::Result<()> {
    w.write_all(&wire::encode_header(status as u8, body.len()))
        .await?;
    if !body.is_empty() {
        w.write_all(&body).await?;
    }
    Ok(())
}

/// Route a request to the cache and produce the response status and body.
pub fn dispatch(op: u8, body: Bytes, cache: &Cache) -> (Status, Bytes) {
    let res = match Op::from_u8(op) {
        Some(Op::Get) => wire::decode_keys(body).map(|keys| {
            let mut out = wire::ValuesEncoder::with_capacity(keys.len(), keys.len() * 64);
            for k in &keys {
                out.push(cache.get(k).as_deref());
            }
            out.finish()
        }),
        Some(Op::Set) => wire::decode_entries(body).map(|entries| {
            for (k, v) in entries {
                // Copy out of the request buffer so cached data never pins the whole
                // body. Key and value share one allocation when the map is known to
                // drop the old key object on overwrite.
                if crate::cache::Map::REPLACES_KEY {
                    let mut buf = BytesMut::with_capacity(k.len() + v.len());
                    buf.extend_from_slice(&k);
                    buf.extend_from_slice(&v);
                    let buf = buf.freeze();
                    cache.set(buf.slice(..k.len()), buf.slice(k.len()..));
                } else {
                    cache.set(Bytes::copy_from_slice(&k), Bytes::copy_from_slice(&v));
                }
            }
            Bytes::new()
        }),
        Some(Op::Del) => wire::decode_keys(body).map(|keys| {
            let flags: Vec<bool> = keys.iter().map(|k| cache.del(k)).collect();
            wire::encode_flags(&flags)
        }),
        None => return (Status::UnknownOp, Bytes::from(format!("unknown op {op}"))),
    };
    match res {
        Ok(out) => (Status::Ok, out),
        Err(e) => (Status::BadRequest, Bytes::from(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_roundtrip() {
        let cache = Cache::new(1 << 20, 2);
        let (st, _) = dispatch(
            Op::Set as u8,
            wire::encode_entries([(&b"k"[..], &b"v"[..])]),
            &cache,
        );
        assert_eq!(st, Status::Ok);
        let (st, body) = dispatch(
            Op::Get as u8,
            wire::encode_keys([&b"k"[..], &b"x"[..]]),
            &cache,
        );
        assert_eq!(st, Status::Ok);
        assert_eq!(
            wire::decode_values(body).unwrap(),
            vec![Some(Bytes::from_static(b"v")), None]
        );
        let (st, body) = dispatch(
            Op::Del as u8,
            wire::encode_keys([&b"k"[..], &b"x"[..]]),
            &cache,
        );
        assert_eq!(st, Status::Ok);
        assert_eq!(wire::decode_flags(body).unwrap(), vec![true, false]);
    }

    #[test]
    fn dispatch_errors() {
        let cache = Cache::new(1 << 20, 1);
        assert_eq!(dispatch(42, Bytes::new(), &cache).0, Status::UnknownOp);
        assert_eq!(
            dispatch(Op::Get as u8, Bytes::from_static(&[9, 0]), &cache).0,
            Status::BadRequest
        );
    }
}
