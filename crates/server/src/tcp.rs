//! TCP front end: one tokio task per connection, requests handled inline and
//! answered in order. Request bodies are zero-copy slices of the read buffer;
//! responses are flushed with one vectored write once the buffered input has
//! been drained, so pipelined requests share a single syscall each way.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use oxicache_wire::io::{FrameReader, FrameWriter};
use oxicache_wire::{self as wire, Op, Status};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info};

use crate::cache::{Cache, Entry};
use crate::error::{Error, Result};

/// Largest request body accepted.
pub const MAX_FRAME: usize = 64 << 20;

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
    let (mut r, mut w) = stream.into_split();
    let mut reader = FrameReader::new(MAX_FRAME);
    let mut out = FrameWriter::new();
    loop {
        // Serve every complete frame already buffered, then flush once.
        loop {
            match reader.next_buffered() {
                Ok(Some((op, body))) => dispatch(op, body, cache, &mut out),
                Ok(None) => break,
                Err(e) => {
                    out.frame(Status::TooLarge as u8, Bytes::from(e.to_string()));
                    out.flush(&mut w).await?;
                    return Ok(());
                }
            }
        }
        out.flush(&mut w).await?;
        if !reader.fill(&mut r).await? {
            return Ok(());
        }
    }
}

/// Route a request to the cache and append the response frame to `out`.
pub fn dispatch(op: u8, body: Bytes, cache: &Cache, out: &mut FrameWriter) {
    let res = match Op::from_u8(op) {
        Some(Op::Get) => wire::decode_keys(body).map(|keys| {
            cache.get_many(keys.iter().map(|k| &k[..]), |entries| {
                let total = 4 + entries
                    .iter()
                    .map(|e| 1 + e.as_ref().map_or(0, |e| 4 + e.value().len()))
                    .sum::<usize>();
                out.header(Status::Ok as u8, total);
                out.put_slice(&(entries.len() as u32).to_le_bytes());
                for e in entries {
                    match e {
                        Some(e) => {
                            out.put_slice(&[1]);
                            out.put_slice(&(e.value().len() as u32).to_le_bytes());
                            if e.value().len() < wire::io::INLINE_BODY {
                                out.put_slice(e.value());
                            } else {
                                out.put_bytes(Bytes::from_owner(Entry::clone(e)));
                            }
                        }
                        None => out.put_slice(&[0]),
                    }
                }
            })
        }),
        Some(Op::Set) => wire::decode_entries(body).map(|entries| {
            // The cache copies key and value into its own allocation, so the
            // request body is released as soon as this returns.
            cache.set_many(entries.iter().map(|(k, v)| (&k[..], &v[..])));
            out.header(Status::Ok as u8, 0);
        }),
        Some(Op::Del) => wire::decode_keys(body).map(|keys| {
            let flags: Vec<bool> = keys.iter().map(|k| cache.del(k)).collect();
            out.frame(Status::Ok as u8, wire::encode_flags(&flags));
        }),
        None => {
            return out.frame(
                Status::UnknownOp as u8,
                Bytes::from(format!("unknown op {op}")),
            );
        }
    };
    if let Err(e) = res {
        out.frame(Status::BadRequest as u8, Bytes::from(e.to_string()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(cache: &Cache, op: u8, body: Bytes) -> (Status, Bytes) {
        let mut out = FrameWriter::new();
        dispatch(op, body, cache, &mut out);
        let raw = out.take();
        let (status, len) = wire::decode_header(raw[..wire::HEADER_LEN].try_into().unwrap());
        assert_eq!(raw.len(), wire::HEADER_LEN + len);
        (
            Status::from_u8(status).unwrap(),
            Bytes::copy_from_slice(&raw[wire::HEADER_LEN..]),
        )
    }

    #[test]
    fn dispatch_roundtrip() {
        let cache = Cache::new(1 << 20, 2);
        let big = vec![9u8; wire::io::INLINE_BODY * 2];
        let (st, _) = call(
            &cache,
            Op::Set as u8,
            wire::encode_entries([(&b"k"[..], &b"v"[..]), (&b"big"[..], &big[..])]),
        );
        assert_eq!(st, Status::Ok);
        let (st, body) = call(
            &cache,
            Op::Get as u8,
            wire::encode_keys([&b"k"[..], &b"x"[..], &b"big"[..]]),
        );
        assert_eq!(st, Status::Ok);
        assert_eq!(
            wire::decode_values(body).unwrap(),
            vec![
                Some(Bytes::from_static(b"v")),
                None,
                Some(Bytes::from(big.clone()))
            ]
        );
        let (st, body) = call(
            &cache,
            Op::Del as u8,
            wire::encode_keys([&b"k"[..], &b"x"[..]]),
        );
        assert_eq!(st, Status::Ok);
        assert_eq!(wire::decode_flags(body).unwrap(), vec![true, false]);
    }

    #[test]
    fn dispatch_errors() {
        let cache = Cache::new(1 << 20, 1);
        assert_eq!(call(&cache, 42, Bytes::new()).0, Status::UnknownOp);
        assert_eq!(
            call(&cache, Op::Get as u8, Bytes::from_static(&[9, 0])).0,
            Status::BadRequest
        );
    }
}
