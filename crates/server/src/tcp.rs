//! TCP front end: one tokio task per connection, requests handled inline and
//! answered in order. Request bodies are zero-copy slices of the read buffer;
//! responses are flushed with one vectored write once the buffered input has
//! been drained, so pipelined requests share a single syscall each way.

use std::io::IoSlice;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use oxicache_wire::{self as wire, HEADER_LEN, Op, Status};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info};

use crate::cache::{Cache, Entry};
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
    let (mut r, mut w) = stream.into_split();
    let mut inbuf = BytesMut::with_capacity(BUF);
    let mut out = Writer::new();
    loop {
        // Serve every complete frame already buffered; bodies are zero-copy
        // slices of the read buffer.
        while inbuf.len() >= HEADER_LEN {
            let (op, len) = wire::decode_header(inbuf[..HEADER_LEN].try_into().unwrap());
            if len > MAX_FRAME {
                out.frame(Status::TooLarge, b"frame exceeds limit");
                out.flush(&mut w).await?;
                return Ok(());
            }
            if inbuf.len() < HEADER_LEN + len {
                inbuf.reserve(HEADER_LEN + len - inbuf.len());
                break;
            }
            inbuf.advance(HEADER_LEN);
            let body = inbuf.split_to(len).freeze();
            dispatch(op, body, cache, &mut out);
        }
        out.flush(&mut w).await?;
        if inbuf.capacity() - inbuf.len() < BUF / 4 {
            inbuf.reserve(BUF);
        }
        if r.read_buf(&mut inbuf).await? == 0 {
            return Ok(());
        }
    }
}

/// Bodies at or above this size are written by reference (`writev`) instead
/// of being copied into the coalescing buffer.
const INLINE_BODY: usize = 1024;
const MAX_IOV: usize = 64;

/// Response writer: small frames coalesce into one buffer, large bodies are
/// queued by reference, and everything goes out with vectored writes.
pub struct Writer {
    chunk: BytesMut,
    pieces: Vec<Bytes>,
}

impl Writer {
    fn new() -> Self {
        Self {
            chunk: BytesMut::with_capacity(BUF),
            pieces: Vec::new(),
        }
    }

    /// Start a frame whose body will total `len` bytes.
    #[inline]
    fn header(&mut self, status: Status, len: usize) {
        self.chunk
            .put_slice(&wire::encode_header(status as u8, len));
    }

    /// Append body bytes by copy into the coalescing buffer.
    #[inline]
    fn put_slice(&mut self, b: &[u8]) {
        self.chunk.put_slice(b);
    }

    /// Append body bytes by reference; they are written straight from `b`.
    fn put_bytes(&mut self, b: Bytes) {
        if !self.chunk.is_empty() {
            self.pieces.push(self.chunk.split().freeze());
        }
        self.pieces.push(b);
    }

    /// Append a whole small frame.
    fn frame(&mut self, status: Status, body: &[u8]) {
        self.header(status, body.len());
        self.put_slice(body);
    }

    #[cfg(test)]
    fn take(&mut self) -> Vec<u8> {
        let mut v: Vec<u8> = self.pieces.drain(..).flat_map(|p| p.to_vec()).collect();
        v.extend_from_slice(&self.chunk.split());
        v
    }

    async fn flush<W: AsyncWriteExt + Unpin>(&mut self, w: &mut W) -> std::io::Result<()> {
        if !self.chunk.is_empty() {
            self.pieces.push(self.chunk.split().freeze());
        }
        let (mut idx, mut off) = (0, 0);
        while idx < self.pieces.len() {
            let mut iov: [IoSlice<'_>; MAX_IOV] = [IoSlice::new(&[]); MAX_IOV];
            let mut n = 0;
            for p in &self.pieces[idx..idx + MAX_IOV.min(self.pieces.len() - idx)] {
                let start = if n == 0 { off } else { 0 };
                iov[n] = IoSlice::new(&p[start..]);
                n += 1;
            }
            let mut written = w.write_vectored(&iov[..n]).await?;
            if written == 0 {
                return Err(std::io::Error::from(std::io::ErrorKind::WriteZero));
            }
            while written > 0 {
                let remaining = self.pieces[idx].len() - off;
                if written >= remaining {
                    written -= remaining;
                    idx += 1;
                    off = 0;
                } else {
                    off += written;
                    written = 0;
                }
            }
        }
        self.pieces.clear();
        if self.chunk.capacity() < BUF / 4 {
            self.chunk = BytesMut::with_capacity(BUF);
        }
        Ok(())
    }
}

/// Route a request to the cache and append the response frame to `out`.
pub fn dispatch(op: u8, body: Bytes, cache: &Cache, out: &mut Writer) {
    let res = match Op::from_u8(op) {
        Some(Op::Get) => wire::decode_keys(body).map(|keys| {
            cache.get_many(keys.iter().map(|k| &k[..]), |entries| {
                let total = 4 + entries
                    .iter()
                    .map(|e| 1 + e.as_ref().map_or(0, |e| 4 + e.value().len()))
                    .sum::<usize>();
                out.header(Status::Ok, total);
                out.put_slice(&(entries.len() as u32).to_le_bytes());
                for e in entries {
                    match e {
                        Some(e) => {
                            out.put_slice(&[1]);
                            out.put_slice(&(e.value().len() as u32).to_le_bytes());
                            if e.value().len() < INLINE_BODY {
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
            for (k, v) in entries {
                // The cache copies key and value into its own allocation, so the
                // request body is released as soon as this returns.
                cache.set(&k, &v);
            }
            out.header(Status::Ok, 0);
        }),
        Some(Op::Del) => wire::decode_keys(body).map(|keys| {
            let flags: Vec<bool> = keys.iter().map(|k| cache.del(k)).collect();
            out.frame(Status::Ok, &wire::encode_flags(&flags));
        }),
        None => return out.frame(Status::UnknownOp, format!("unknown op {op}").as_bytes()),
    };
    if let Err(e) = res {
        out.frame(Status::BadRequest, e.to_string().as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(cache: &Cache, op: u8, body: Bytes) -> (Status, Bytes) {
        let mut out = Writer::new();
        dispatch(op, body, cache, &mut out);
        let raw = out.take();
        let (status, len) = wire::decode_header(raw[..HEADER_LEN].try_into().unwrap());
        assert_eq!(raw.len(), HEADER_LEN + len);
        (
            Status::from_u8(status).unwrap(),
            Bytes::copy_from_slice(&raw[HEADER_LEN..]),
        )
    }

    #[test]
    fn dispatch_roundtrip() {
        let cache = Cache::new(1 << 20, 2);
        let big = vec![9u8; INLINE_BODY * 2];
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
