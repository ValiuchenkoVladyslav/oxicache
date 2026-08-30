//! TCP front end: one tokio task per connection, requests handled inline and
//! answered in order. Request bodies are zero-copy slices of the read buffer;
//! responses are flushed with one vectored write once the buffered input has
//! been drained, so pipelined requests share a single syscall each way.
//!
//! A connection must authenticate with one `Auth` frame before anything
//! else; the check is a single well-predicted branch per frame afterwards.

use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use oxicache_wire::io::{FrameReader, FrameWriter};
use oxicache_wire::{self as wire, Op, Status};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::cache::{Cache, Entry};
use crate::error::{Error, Result};

/// Largest request body accepted from an authenticated peer.
pub const MAX_FRAME: usize = wire::MAX_FRAME;
/// Largest frame accepted before authentication: room for a token, nothing
/// more, so an unauthenticated peer cannot make the server buffer much.
pub const MAX_AUTH_FRAME: usize = 4 << 10;
/// Largest response body produced. A `GET` may name the same large key many
/// times, so the total is bounded here rather than by the request size; it
/// matches what the client accepts.
pub const MAX_RESPONSE: usize = wire::MAX_FRAME;
/// How long in-flight connections get to finish their current batch at shutdown.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
/// Hard cap from accept to a successful AUTH, on both front ends. It is not
/// configurable: an unauthenticated peer gets no say in how long it may sit
/// on a connection slot, and the idle timeout (which it could keep resetting
/// with partial input) does not apply until it has authenticated.
pub const AUTH_TIMEOUT: Duration = Duration::from_secs(30);
/// Pause after a failed `accept`, so fd exhaustion does not spin a worker.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(50);

/// Settings for [`Server::bind`] and [`HttpServer::bind`](crate::HttpServer::bind).
/// Build with [`Options::new`] and the setters; a clone shares the
/// connection budget with the original, so binding both front ends from one
/// `Options` gives them one limit between them.
#[derive(Clone)]
pub struct Options {
    /// Shared secret every connection must present in an `Auth` frame before
    /// its first request. Must not be empty: there is no unauthenticated
    /// mode.
    pub token: Vec<u8>,
    /// Close a connection that sends nothing for this long (on HTTP: a
    /// keep-alive connection that starts no request). `None` never closes.
    pub idle_timeout: Option<Duration>,
    /// Cap on open connections across every listener bound with this
    /// `Options`; `None` is unlimited.
    pub limit: Option<Arc<ConnLimit>>,
}

/// Idle timeout every `Options` starts with: three client heartbeats
/// ([`wire::KEEPALIVE`], 100 s), so a client on the default interval stays
/// connected even if two pings in a row are lost or late.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(3 * wire::KEEPALIVE.as_secs());
/// Connection cap every `Options` starts with.
pub const DEFAULT_MAX_CONNECTIONS: NonZeroUsize = NonZeroUsize::new(10_000).unwrap();

impl Options {
    /// [`DEFAULT_IDLE_TIMEOUT`] and [`DEFAULT_MAX_CONNECTIONS`]; both are
    /// protective, so an embedder has to opt out of them with `None` rather
    /// than remember to opt in.
    pub fn new(token: impl Into<Vec<u8>>) -> Self {
        Self {
            token: token.into(),
            idle_timeout: Some(DEFAULT_IDLE_TIMEOUT),
            limit: Some(Arc::new(ConnLimit::new(DEFAULT_MAX_CONNECTIONS))),
        }
    }

    pub fn idle_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Allow at most `max` open connections in total; `None` lifts the cap.
    pub fn max_connections(mut self, max: Option<NonZeroUsize>) -> Self {
        self.limit = max.map(|n| Arc::new(ConnLimit::new(n)));
        self
    }
}

/// The token is a secret; a `{:?}` in a log must not print it.
impl std::fmt::Debug for Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Options")
            .field("token", &"<redacted>")
            .field("idle_timeout", &self.idle_timeout)
            .field("limit", &self.limit)
            .finish()
    }
}

/// A budget of open connections. Every listener takes a permit *before*
/// `accept`, so a full server stops accepting (peers wait in the backlog)
/// rather than accepting and dropping.
#[derive(Debug)]
pub struct ConnLimit {
    sem: Arc<Semaphore>,
    max: NonZeroUsize,
    /// Accepted connections still being served. Distinct from the permits
    /// in use: a listener parked in `accept` also holds one, and that is
    /// not an open connection.
    open: AtomicUsize,
    /// Accepts that had to wait, for rate-limiting the warning.
    hits: AtomicU64,
}

/// One warning per this many blocked accepts, so a saturated server does
/// not flood the log.
const LIMIT_WARN_EVERY: u64 = 1000;

impl ConnLimit {
    pub fn new(max: NonZeroUsize) -> Self {
        Self {
            sem: Arc::new(Semaphore::new(max.get())),
            max,
            open: AtomicUsize::new(0),
            hits: AtomicU64::new(0),
        }
    }

    pub fn max(&self) -> NonZeroUsize {
        self.max
    }

    /// Connections currently open under this budget.
    pub fn open(&self) -> usize {
        self.open.load(Ordering::Relaxed)
    }

    /// Wait for a free slot; the permit is released when dropped.
    async fn acquire(&self) -> OwnedSemaphorePermit {
        if let Ok(p) = self.sem.clone().try_acquire_owned() {
            return p;
        }
        // Another listener parked in `accept` may hold the last permit
        // while a slot is still free; that is not the limit being hit.
        if self.open() >= self.max.get()
            && self
                .hits
                .fetch_add(1, Ordering::Relaxed)
                .is_multiple_of(LIMIT_WARN_EVERY)
        {
            warn!(max = self.max, "connection limit reached, not accepting");
        }
        self.sem
            .clone()
            .acquire_owned()
            .await
            .expect("never closed")
    }
}

/// Take a permit if there is a limit; both accept loops call this before
/// `accept`.
pub(crate) async fn admit(limit: Option<&Arc<ConnLimit>>) -> Option<OwnedSemaphorePermit> {
    match limit {
        Some(l) => Some(l.acquire().await),
        None => None,
    }
}

/// An accepted connection's slot: counted as open for as long as it lives,
/// and holding the permit that `admit` took for it.
pub(crate) struct Slot {
    _permit: Option<OwnedSemaphorePermit>,
    limit: Option<Arc<ConnLimit>>,
}

impl Slot {
    pub(crate) fn open(
        limit: Option<&Arc<ConnLimit>>,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Self {
        if let Some(l) = limit {
            l.open.fetch_add(1, Ordering::Relaxed);
        }
        Self {
            _permit: permit,
            limit: limit.cloned(),
        }
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        if let Some(l) = &self.limit {
            l.open.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

pub struct Server {
    listener: std::net::TcpListener,
    cache: Arc<Cache>,
    token: Arc<[u8]>,
    idle_timeout: Option<Duration>,
    limit: Option<Arc<ConnLimit>>,
}

impl Server {
    /// Bind `addr` serving `cache`; every connection must authenticate with
    /// `opts.token`.
    pub fn bind(addr: SocketAddr, cache: Arc<Cache>, opts: Options) -> Result<Self> {
        let token = token(&opts)?;
        let listener = listener(addr).map_err(|source| Error::Bind { addr, source })?;
        Ok(Self {
            listener,
            cache,
            token,
            idle_timeout: opts.idle_timeout,
            limit: opts.limit,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr().expect("bound listener")
    }

    /// Connections open under the shared limit, if there is one.
    pub fn open_connections(&self) -> Option<usize> {
        self.limit.as_ref().map(|l| l.open())
    }

    /// Accept connections on the current runtime until the task is dropped.
    pub async fn run(&self) {
        self.run_until(std::future::pending::<()>()).await;
    }

    /// Accept connections until `shutdown` resolves, then stop accepting and
    /// give open connections a moment to flush the batch they are serving.
    pub async fn run_until(&self, shutdown: impl Future<Output = ()>) {
        info!(addr = %self.local_addr(), "listening (tcp)");
        let l = self.listener.try_clone().expect("clone listener");
        let mut conns = JoinSet::new();
        tokio::select! {
            _ = accept_loop(l, self, &mut conns) => {}
            _ = shutdown => {}
        }
        if conns.is_empty() {
            return;
        }
        info!(open = conns.len(), "draining connections");
        let _ = tokio::time::timeout(DRAIN_TIMEOUT, async {
            while conns.join_next().await.is_some() {}
        })
        .await;
        conns.shutdown().await;
    }
}

/// Validate the shared secret; shared by both front ends.
pub(crate) fn token(opts: &Options) -> Result<Arc<[u8]>> {
    if opts.token.is_empty() {
        return Err(Error::EmptyToken);
    }
    Ok(Arc::from(opts.token.as_slice()))
}

/// A non-blocking listening socket with `SO_REUSEADDR`, shared by both front ends.
pub(crate) fn listener(addr: SocketAddr) -> std::io::Result<std::net::TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    Ok(socket.into())
}

async fn accept_loop(listener: std::net::TcpListener, server: &Server, conns: &mut JoinSet<()>) {
    let listener = TcpListener::from_std(listener).expect("register listener");
    loop {
        // Reap finished tasks so the set does not grow with every connection.
        while conns.try_join_next().is_some() {}
        let permit = admit(server.limit.as_ref()).await;
        match listener.accept().await {
            Ok((stream, remote)) => {
                let (cache, token) = (server.cache.clone(), server.token.clone());
                let idle = server.idle_timeout;
                let slot = Slot::open(server.limit.as_ref(), permit);
                conns.spawn(async move {
                    // Held until the connection is done, whatever the reason.
                    let _slot = slot;
                    debug!(%remote, "connection open");
                    match serve_connection(stream, &cache, &token, idle).await {
                        Ok(true) => debug!(%remote, "idle connection closed"),
                        Ok(false) => {}
                        Err(e) => debug!(%remote, error = %e, "connection closed"),
                    }
                });
            }
            Err(e) => {
                warn!(error = %e, "accept failed");
                tokio::time::sleep(ACCEPT_BACKOFF).await;
            }
        }
    }
}

/// Serve one connection to its end; `Ok(true)` means the idle timeout closed it.
async fn serve_connection(
    stream: TcpStream,
    cache: &Cache,
    token: &[u8],
    idle: Option<Duration>,
) -> std::io::Result<bool> {
    stream.set_nodelay(true)?;
    let (mut r, mut w) = stream.into_split();
    let mut authed = false;
    let auth_deadline = Instant::now() + AUTH_TIMEOUT;
    let mut reader = FrameReader::new(MAX_AUTH_FRAME);
    let mut out = FrameWriter::new();
    loop {
        // Serve every complete frame already buffered, then flush once.
        loop {
            match reader.next_buffered() {
                Ok(Some((op, body))) if authed => dispatch(op, body, cache, &mut out),
                Ok(Some((op, body))) => {
                    let ok = op == Op::Auth as u8 && ct_eq(token, body);
                    if ok {
                        authed = true;
                        reader.set_max_frame(MAX_FRAME);
                        out.header(Status::Ok as u8, 0);
                    } else {
                        out.frame(
                            Status::Unauthorized as u8,
                            Bytes::from_static(b"auth required"),
                        );
                        out.flush(&mut w).await?;
                        return Ok(false);
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    out.frame(Status::TooLarge as u8, Bytes::from(e.to_string()));
                    out.flush(&mut w).await?;
                    return Ok(false);
                }
            }
        }
        // Until AUTH the deadline is fixed; after it the idle clock restarts
        // at every wait, so a peer only has to move something within each
        // `idle` window, not finish a request. The flush is under the same
        // clock: a peer that stops reading would otherwise park this task
        // (and its connection slot) forever.
        let deadline = if authed {
            idle.map(|d| Instant::now() + d)
        } else {
            Some(auth_deadline)
        };
        let Some(()) = by(deadline, out.flush(&mut w)).await? else {
            return Ok(true);
        };
        let Some(filled) = by(deadline, reader.fill(&mut r)).await? else {
            return Ok(true);
        };
        if !filled {
            return Ok(false);
        }
    }
}

/// Run `fut` until `deadline`, if there is one; `None` means it hit it.
async fn by<T>(
    deadline: Option<Instant>,
    fut: impl Future<Output = std::io::Result<T>>,
) -> std::io::Result<Option<T>> {
    match deadline {
        Some(at) => match tokio::time::timeout_at(at, fut).await {
            Ok(res) => res.map(Some),
            Err(_) => Ok(None),
        },
        None => fut.await.map(Some),
    }
}

/// Constant-time byte comparison, so a wrong token's reply time does not
/// reveal how many leading bytes matched. The length check short-circuits
/// on purpose: it reveals only the token's length, which is not secret.
pub(crate) fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Route a request to the cache and append the response frame to `out`.
pub fn dispatch(op: u8, body: &[u8], cache: &Cache, out: &mut FrameWriter) {
    let res = match Op::from_u8(op) {
        Some(Op::Get) => wire::keys(body).map(|keys| {
            cache.get_many(keys, |entries| {
                let total = 4 + entries
                    .iter()
                    .map(|e| 1 + e.as_ref().map_or(0, |e| 4 + e.value().len()))
                    .sum::<usize>();
                if total > MAX_RESPONSE {
                    return out.frame(
                        Status::TooLarge as u8,
                        Bytes::from(format!("response of {total} bytes exceeds the limit")),
                    );
                }
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
        Some(Op::Set) => wire::entries(body).map(|entries| {
            // The cache copies key and value into its own allocation, so the
            // request body is released as soon as this returns.
            match cache.set_many(entries) {
                Ok(()) => out.header(Status::Ok as u8, 0),
                Err(e) => out.frame(Status::TooLarge as u8, Bytes::from(e.to_string())),
            }
        }),
        Some(Op::Del) => wire::keys(body).map(|keys| {
            out.header(Status::Ok as u8, 4 + keys.len());
            out.put_slice(&(keys.len() as u32).to_le_bytes());
            cache.del_many(keys, |found| out.put_slice(&[found as u8]));
        }),
        // AUTH here means already authenticated: a no-op. PING is the
        // client's heartbeat; its body is ignored rather than validated, so
        // a future client can attach something without being refused.
        Some(Op::Auth | Op::Ping) => {
            out.header(Status::Ok as u8, 0);
            Ok(())
        }
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
        dispatch(op, &body, cache, &mut out);
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
        let cache = Cache::new(
            NonZeroUsize::new(1 << 20).unwrap(),
            NonZeroUsize::new(2).unwrap(),
        );
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
    fn dispatch_ping_is_ok_and_ignores_its_body() {
        let cache = Cache::new(
            NonZeroUsize::new(1 << 20).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        );
        assert_eq!(
            call(&cache, Op::Ping as u8, Bytes::new()),
            (Status::Ok, Bytes::new())
        );
        assert_eq!(
            call(&cache, Op::Ping as u8, Bytes::from_static(b"anything")),
            (Status::Ok, Bytes::new())
        );
    }

    #[test]
    fn dispatch_errors() {
        let cache = Cache::new(
            NonZeroUsize::new(1 << 20).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        );
        assert_eq!(call(&cache, 42, Bytes::new()).0, Status::UnknownOp);
        assert_eq!(
            call(&cache, Op::Get as u8, Bytes::from_static(&[9, 0])).0,
            Status::BadRequest
        );
        // A count beyond the item limit is rejected before any work is done.
        let mut huge = Vec::new();
        huge.extend_from_slice(&((wire::MAX_ITEMS + 1) as u32).to_le_bytes());
        huge.resize(4 + 4 * (wire::MAX_ITEMS + 1), 0);
        let (st, msg) = call(&cache, Op::Get as u8, Bytes::from(huge));
        assert_eq!(st, Status::BadRequest);
        assert!(
            std::str::from_utf8(&msg)
                .unwrap()
                .contains("exceeds the limit")
        );
    }

    #[test]
    fn oversized_set_is_rejected_whole() {
        let cache = Cache::new(
            NonZeroUsize::new(1 << 20).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        );
        let big = vec![0u8; 1 << 20];
        let (st, _) = call(
            &cache,
            Op::Set as u8,
            wire::encode_entries([(&b"small"[..], &b"v"[..]), (&b"big"[..], &big[..])]),
        );
        assert_eq!(st, Status::TooLarge);
        assert!(
            cache.get(b"small").is_none(),
            "nothing from the batch is stored"
        );
        assert!(cache.get(b"big").is_none());
    }

    fn opts() -> Options {
        Options::new(b"t".to_vec())
    }

    fn server() -> Arc<Server> {
        server_with(opts())
    }

    fn server_with(opts: Options) -> Arc<Server> {
        let cache = Arc::new(Cache::new(
            NonZeroUsize::new(1 << 20).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ));
        Arc::new(Server::bind("127.0.0.1:0".parse().unwrap(), cache, opts).unwrap())
    }

    #[test]
    fn bind_failure_is_reported() {
        let first = server();
        let cache = Arc::new(Cache::new(
            NonZeroUsize::new(1 << 20).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ));
        let err = Server::bind(first.local_addr(), cache.clone(), opts())
            .err()
            .expect("port in use");
        assert!(matches!(err, Error::Bind { addr, .. } if addr == first.local_addr()));
        let err = Server::bind("127.0.0.1:0".parse().unwrap(), cache, Options::new(""))
            .err()
            .expect("empty token");
        assert!(matches!(err, Error::EmptyToken), "{err}");
    }

    /// PING is an op like any other: before AUTH it is refused and the
    /// connection closed, so it cannot be used to probe without a token.
    #[tokio::test]
    async fn ping_before_auth_is_unauthorized() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let s = server();
        let addr = s.local_addr();
        tokio::spawn(async move { s.run().await });
        let mut c = TcpStream::connect(addr).await.unwrap();
        c.write_all(&wire::encode_header(Op::Ping as u8, 0))
            .await
            .unwrap();
        let mut hdr = [0u8; wire::HEADER_LEN];
        c.read_exact(&mut hdr).await.unwrap();
        assert_eq!(wire::decode_header(&hdr).0, Status::Unauthorized as u8);
        let mut rest = Vec::new();
        c.read_to_end(&mut rest).await.unwrap();
        assert_eq!(rest, b"auth required");
    }

    /// A peer that sends a batch and then never reads its reply is closed
    /// by the idle timeout too, so it cannot pin a connection slot.
    #[tokio::test]
    async fn stalled_reader_is_closed_after_the_timeout() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let cache = Arc::new(Cache::new(
            NonZeroUsize::new(64 << 20).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ));
        let opts = opts().idle_timeout(Some(Duration::from_millis(200)));
        let s = Server::bind("127.0.0.1:0".parse().unwrap(), cache, opts).unwrap();
        let addr = s.local_addr();
        tokio::spawn(async move { s.run().await });
        let mut c = TcpStream::connect(addr).await.unwrap();
        auth(&mut c).await;
        // Store a value too big for the socket buffers to absorb unread.
        let big = vec![7u8; 4 << 20];
        let mut set = Vec::new();
        set.extend_from_slice(&1u32.to_le_bytes());
        set.extend_from_slice(&1u32.to_le_bytes());
        set.push(b'k');
        set.extend_from_slice(&(big.len() as u32).to_le_bytes());
        set.extend_from_slice(&big);
        c.write_all(&wire::encode_header(Op::Set as u8, set.len()))
            .await
            .unwrap();
        c.write_all(&set).await.unwrap();
        let mut hdr = [0u8; wire::HEADER_LEN];
        c.read_exact(&mut hdr).await.unwrap();
        assert_eq!(wire::decode_header(&hdr), (Status::Ok as u8, 0));
        // 15 × 4 MiB stays under the response cap but far above what the
        // socket buffers can absorb unread.
        let mut get = Vec::new();
        get.extend_from_slice(&15u32.to_le_bytes());
        for _ in 0..15 {
            get.extend_from_slice(&1u32.to_le_bytes());
            get.push(b'k');
        }
        c.write_all(&wire::encode_header(Op::Get as u8, get.len()))
            .await
            .unwrap();
        c.write_all(&get).await.unwrap();
        // Do not read for a while: the server's flush blocks on our full
        // receive buffer, gives up, and closes. Draining afterwards must
        // then end well short of the full 60 MiB reply.
        tokio::time::sleep(Duration::from_millis(600)).await;
        let mut drained = Vec::new();
        let n = tokio::time::timeout(Duration::from_secs(5), c.read_to_end(&mut drained))
            .await
            .expect("closed by the server")
            .unwrap_or(drained.len());
        assert!(n < 15 * big.len(), "reply was not cut short: {n} bytes");
    }

    /// Authenticate `c` with the test token, as every client must.
    async fn auth(c: &mut TcpStream) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        c.write_all(&wire::encode_header(Op::Auth as u8, 1))
            .await
            .unwrap();
        c.write_all(b"t").await.unwrap();
        let mut hdr = [0u8; wire::HEADER_LEN];
        c.read_exact(&mut hdr).await.unwrap();
        assert_eq!(wire::decode_header(&hdr).0, Status::Ok as u8);
    }

    #[tokio::test]
    async fn shutdown_drains_open_connections() {
        let s = server();
        let addr = s.local_addr();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let run = tokio::spawn(async move {
            s.run_until(async {
                let _ = rx.await;
            })
            .await;
        });
        let conn = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        tx.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!run.is_finished(), "an open connection is drained first");
        drop(conn);
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("drain ends once the connection closes")
            .unwrap();
    }

    #[tokio::test]
    async fn oversized_frame_is_refused() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let s = server();
        let addr = s.local_addr();
        tokio::spawn(async move { s.run().await });
        let mut c = TcpStream::connect(addr).await.unwrap();
        auth(&mut c).await;
        c.write_all(&wire::encode_header(Op::Get as u8, MAX_FRAME + 1))
            .await
            .unwrap();
        let mut hdr = [0u8; wire::HEADER_LEN];
        c.read_exact(&mut hdr).await.unwrap();
        assert_eq!(wire::decode_header(&hdr).0, Status::TooLarge as u8);
        let mut rest = Vec::new();
        c.read_to_end(&mut rest).await.unwrap();
        assert_eq!(rest.len(), wire::decode_header(&hdr).1, "then disconnected");
    }

    #[tokio::test]
    async fn reset_connection_is_reported_not_fatal() {
        let s = server();
        let addr = s.local_addr();
        let srv = s.clone();
        tokio::spawn(async move { srv.run().await });
        let c = TcpStream::connect(addr).await.unwrap();
        socket2::SockRef::from(&c)
            .set_linger(Some(Duration::ZERO))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(c); // RST rather than FIN
        tokio::time::sleep(Duration::from_millis(50)).await;
        // The server is still accepting.
        let (st, _) = call(&s.cache, Op::Get as u8, wire::encode_keys([&b"k"[..]]));
        assert_eq!(st, Status::Ok);
        let mut c = TcpStream::connect(addr).await.unwrap();
        auth(&mut c).await;
        tokio::io::AsyncWriteExt::write_all(&mut c, &wire::encode_header(Op::Get as u8, 0))
            .await
            .unwrap();
        let mut hdr = [0u8; wire::HEADER_LEN];
        tokio::io::AsyncReadExt::read_exact(&mut c, &mut hdr)
            .await
            .unwrap();
        assert_eq!(wire::decode_header(&hdr).0, Status::BadRequest as u8);
    }

    /// The cap is fixed from accept; trickling bytes does not extend it,
    /// and no idle timeout (here: none at all) softens it.
    #[tokio::test(start_paused = true)]
    async fn unauthenticated_connection_is_cut_at_the_hard_cap() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let s = server_with(opts().idle_timeout(None));
        let addr = s.local_addr();
        tokio::spawn(async move { s.run().await });
        let mut c = TcpStream::connect(addr).await.unwrap();
        let start = Instant::now();
        // Half an AUTH header, then silence: never a complete frame.
        c.write_all(&[Op::Auth as u8, 1]).await.unwrap();
        let mut rest = Vec::new();
        tokio::time::timeout(AUTH_TIMEOUT * 2, c.read_to_end(&mut rest))
            .await
            .expect("closed by the server")
            .unwrap();
        assert!(rest.is_empty(), "no reply to an incomplete frame");
        assert!(start.elapsed() >= AUTH_TIMEOUT, "cut early");
    }

    #[tokio::test]
    async fn idle_connection_is_closed_after_the_timeout() {
        use tokio::io::AsyncReadExt;
        let s = server_with(opts().idle_timeout(Some(Duration::from_millis(200))));
        let addr = s.local_addr();
        tokio::spawn(async move { s.run().await });
        let mut c = TcpStream::connect(addr).await.unwrap();
        auth(&mut c).await;
        let start = std::time::Instant::now();
        let mut rest = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), c.read_to_end(&mut rest))
            .await
            .expect("closed by the server")
            .unwrap();
        assert!(rest.is_empty());
        assert!(start.elapsed() >= Duration::from_millis(150), "not before");
    }

    #[tokio::test]
    async fn connections_over_the_limit_wait_for_a_slot() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let s = server_with(opts().max_connections(NonZeroUsize::new(1)));
        let addr = s.local_addr();
        let srv = s.clone();
        tokio::spawn(async move { srv.run().await });
        let mut first = TcpStream::connect(addr).await.unwrap();
        auth(&mut first).await;
        assert_eq!(s.open_connections(), Some(1));
        // The second lands in the backlog: connected, but nobody answers.
        let mut second = TcpStream::connect(addr).await.unwrap();
        second
            .write_all(&wire::encode_header(Op::Auth as u8, 1))
            .await
            .unwrap();
        second.write_all(b"t").await.unwrap();
        let mut hdr = [0u8; wire::HEADER_LEN];
        assert!(
            tokio::time::timeout(Duration::from_millis(300), second.read_exact(&mut hdr))
                .await
                .is_err(),
            "served while the limit is reached"
        );
        drop(first);
        tokio::time::timeout(Duration::from_secs(5), second.read_exact(&mut hdr))
            .await
            .expect("served once a slot frees up")
            .unwrap();
        assert_eq!(wire::decode_header(&hdr).0, Status::Ok as u8);
        assert_eq!(s.open_connections(), Some(1));
    }

    #[test]
    fn oversized_response_is_refused() {
        let cache = Cache::new(
            NonZeroUsize::new(256 << 20).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        );
        let v = vec![0u8; MAX_RESPONSE / 2];
        cache.set(b"a", &v).unwrap();
        cache.set(b"b", &v).unwrap();
        let (st, _) = call(
            &cache,
            Op::Get as u8,
            wire::encode_keys([&b"a"[..], &b"b"[..]]),
        );
        assert_eq!(st, Status::TooLarge);
    }
}
