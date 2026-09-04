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
use crossbeam_epoch as epoch;
use oxicache_wire::io::{FrameReader, FrameWriter};
use oxicache_wire::{self as wire, Op, Status};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info, warn};

use crate::cache::{Cache, Entry};
use crate::error::{Error, Result};
use crate::metrics::Metrics;

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
    pub token: Box<[u8]>,
    /// Close a connection that sends nothing for this long (on HTTP: a
    /// keep-alive connection that starts no request). `None` never closes.
    pub idle_timeout: Option<Duration>,
    /// Cap on open connections across every listener bound with this
    /// `Options`; `None` is unlimited.
    pub limit: Option<Arc<ConnLimit>>,
    /// Serve TLS with this configuration (see
    /// [`tls::server_config`](crate::tls::server_config)); `None` is plain
    /// TCP. The handshake counts against the [`AUTH_TIMEOUT`].
    pub tls: Option<Arc<rustls::ServerConfig>>,
    /// Operation counters, served at `GET /metrics` by the HTTP front end.
    /// A clone shares them, like the connection budget.
    pub metrics: Arc<Metrics>,
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
            token: token.into().into_boxed_slice(),
            idle_timeout: Some(DEFAULT_IDLE_TIMEOUT),
            limit: Some(Arc::new(ConnLimit::new(DEFAULT_MAX_CONNECTIONS))),
            tls: None,
            metrics: Arc::new(Metrics::new()),
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

    pub fn tls(mut self, config: Option<Arc<rustls::ServerConfig>>) -> Self {
        self.tls = config;
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
            .field("tls", &self.tls.is_some())
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

    /// Accepts that had to wait for a free slot.
    pub fn waits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
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
    Some(limit?.acquire().await)
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
    tls: Option<TlsAcceptor>,
    metrics: Arc<Metrics>,
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
            tls: opts.tls.map(TlsAcceptor::from),
            metrics: opts.metrics,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr().expect("bound listener")
    }

    /// Connections open under the shared limit, if there is one.
    pub fn open_connections(&self) -> Option<usize> {
        self.limit.as_ref().map(|l| l.open())
    }

    /// Whether connections are served over TLS.
    pub fn is_tls(&self) -> bool {
        self.tls.is_some()
    }

    /// Accept connections on the current runtime until the task is dropped.
    pub async fn run(&self) {
        self.run_until(std::future::pending::<()>()).await;
    }

    /// Accept connections until `shutdown` resolves, then stop accepting and
    /// give open connections a moment to flush the batch they are serving.
    pub async fn run_until(&self, shutdown: impl Future<Output = ()>) {
        info!(addr = %self.local_addr(), tls = self.is_tls(), "listening (tcp)");
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
    Ok(Arc::from(&*opts.token))
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
                let (idle, tls) = (server.idle_timeout, server.tls.clone());
                let metrics = server.metrics.clone();
                let slot = Slot::open(server.limit.as_ref(), permit);
                conns.spawn(async move {
                    // Held until the connection is done, whatever the reason.
                    let _slot = slot;
                    debug!(%remote, "connection open");
                    let res = serve(stream, tls, &cache, &token, idle, &metrics).await;
                    match res {
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

/// Serve one accepted socket to its end, plain or through a TLS handshake
/// first; `Ok(true)` means the idle timeout closed it. The handshake is
/// under the AUTH deadline, so an unauthenticated peer's time on a slot
/// is capped whether or not it ever completes one.
async fn serve(
    stream: TcpStream,
    tls: Option<TlsAcceptor>,
    cache: &Cache,
    token: &[u8],
    idle: Option<Duration>,
    metrics: &Metrics,
) -> std::io::Result<bool> {
    stream.set_nodelay(true)?;
    let auth_deadline = Instant::now() + AUTH_TIMEOUT;
    if let Some(acceptor) = tls {
        let Some(stream) = by(Some(auth_deadline), acceptor.accept(stream)).await? else {
            return Ok(true);
        };
        let (r, w) = tokio::io::split(Drained(stream));
        serve_connection(r, w, auth_deadline, cache, token, idle, metrics).await
    } else {
        let (r, w) = stream.into_split();
        serve_connection(r, w, auth_deadline, cache, token, idle, metrics).await
    }
}

/// A TLS stream whose reads return every decrypted byte rustls holds, not
/// one record. tokio-rustls's `poll_read` hands out a single plaintext
/// chunk per call (rustls `Reader::fill_buf`), and a client sends each
/// request as its own record, so without this the loop in
/// [`serve_connection`] saw one or two requests per wake and flushed after
/// each: three `writev`s per `recv` instead of one. After the inner read
/// has done its I/O, the rest of the already-decrypted plaintext is copied
/// out synchronously; no extra syscall.
struct Drained(tokio_rustls::server::TlsStream<TcpStream>);

impl AsyncRead for Drained {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::io::Read;
        let before = buf.filled().len();
        std::task::ready!(std::pin::Pin::new(&mut self.0).poll_read(cx, buf))?;
        if buf.filled().len() == before {
            return std::task::Poll::Ready(Ok(()));
        }
        let (_, conn) = self.0.get_mut();
        while buf.remaining() > 0 {
            // SAFETY: `Reader::read` only writes into the slice and reports
            // how much it wrote; nothing reads the uninitialised tail.
            let spare = unsafe {
                let s = buf.unfilled_mut();
                std::slice::from_raw_parts_mut(s.as_mut_ptr().cast::<u8>(), s.len())
            };
            match conn.reader().read(spare) {
                Ok(0) | Err(_) => break,
                Ok(n) => unsafe {
                    buf.assume_init(n);
                    buf.advance(n);
                },
            }
        }
        std::task::Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for Drained {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.0).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.0.is_write_vectored()
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

/// Serve one connection to its end; `Ok(true)` means the AUTH or idle
/// deadline closed it.
async fn serve_connection<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    mut r: R,
    mut w: W,
    auth_deadline: Instant,
    cache: &Cache,
    token: &[u8],
    idle: Option<Duration>,
    metrics: &Metrics,
) -> std::io::Result<bool> {
    let mut authed = false;
    let mut reader = FrameReader::new(MAX_AUTH_FRAME);
    let mut out = FrameWriter::new();
    loop {
        // Serve every complete frame already buffered, then flush once. One
        // epoch pin covers the whole burst: the pins the cache takes per
        // request become re-entrant counter bumps instead of full fences,
        // and a single-key GET borrows its entry under this pin without
        // touching the refcount. The guard must drop before any await (it
        // is not `Send`), so a fatal reply only breaks out here and the
        // flush that closes the connection happens below.
        let close = {
            let guard = epoch::pin();
            loop {
                match reader.next_buffered() {
                    Ok(Some((op, body))) if authed => {
                        dispatch(op, body, cache, metrics, &mut out, &guard)
                    }
                    Ok(Some((op, body))) => {
                        let ok = op == Op::Auth as u8 && ct_eq(token, body);
                        if ok {
                            authed = true;
                            reader.set_max_frame(MAX_FRAME);
                            out.header(Status::Ok as u8, 0);
                        } else {
                            metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
                            out.frame(
                                Status::Unauthorized as u8,
                                Bytes::from_static(b"auth required"),
                            );
                            break true;
                        }
                    }
                    Ok(None) => break false,
                    Err(e) => {
                        out.frame(Status::TooLarge as u8, Bytes::from(e.to_string()));
                        break true;
                    }
                }
            }
        };
        if close {
            out.flush(&mut w).await?;
            return Ok(false);
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
        Some(at) => tokio::time::timeout_at(at, fut).await.ok().transpose(),
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
/// `guard` is the caller's epoch pin; a GET's entry is borrowed under it
/// (and cloned only when the value is written by reference).
pub fn dispatch(
    op: u8,
    body: &[u8],
    cache: &Cache,
    metrics: &Metrics,
    out: &mut FrameWriter,
    guard: &epoch::Guard,
) {
    match Op::from_u8(op) {
        Some(Op::Get) => match cache.get_in(body, guard) {
            Some(e) if e.value().len() > MAX_RESPONSE => out.frame(
                Status::TooLarge as u8,
                Bytes::from(format!(
                    "response of {} bytes exceeds the limit",
                    e.value().len()
                )),
            ),
            Some(e) => value(&e, out),
            None => out.header(Status::NotFound as u8, 0),
        },
        Some(Op::Set) => match wire::set_body(body) {
            // The cache copies key and value into its own allocation, so the
            // request body is released as soon as this returns.
            Ok((k, v)) => match cache.set(k, v) {
                Ok(()) => out.header(Status::Ok as u8, 0),
                Err(e) => {
                    metrics.set_too_large.fetch_add(1, Ordering::Relaxed);
                    out.frame(Status::TooLarge as u8, Bytes::from(e.to_string()))
                }
            },
            Err(e) => out.frame(Status::BadRequest as u8, Bytes::from(e.to_string())),
        },
        Some(Op::Del) => out.header(
            if cache.del(body) {
                Status::Ok
            } else {
                Status::NotFound
            } as u8,
            0,
        ),
        Some(Op::Batch) => batch(body, cache, metrics, out),
        // AUTH here means already authenticated: a no-op. PING is the
        // client's heartbeat; its body is ignored rather than validated, so
        // a future client can attach something without being refused.
        Some(Op::Auth | Op::Ping) => out.header(Status::Ok as u8, 0),
        None => out.frame(
            Status::UnknownOp as u8,
            Bytes::from(format!("unknown op {op}")),
        ),
    }
}

/// An `Ok` frame carrying `e`'s value: copied while small, referenced from
/// the cache entry (and read in place by `writev`) once it is not.
#[inline]
fn value(e: &Entry, out: &mut FrameWriter) {
    let v = e.value();
    out.header(Status::Ok as u8, v.len());
    if v.len() < wire::io::INLINE_BODY {
        out.put_slice(v);
    } else {
        out.put_bytes(Bytes::from_owner(Entry::clone(e)));
    }
}

/// Serve a BATCH body: every item answered as its op would be on its own,
/// in order, written as it is produced under a header patched at the end.
/// Runs of the same op go to the cache together (one epoch pin, prefetched
/// lookups), which is what makes a batch of GETs cheaper than the same GETs
/// pipelined.
fn batch(body: &[u8], cache: &Cache, metrics: &Metrics, out: &mut FrameWriter) {
    let items = match wire::frames(body) {
        Ok(items) => items,
        Err(e) => return out.frame(Status::BadRequest as u8, Bytes::from(e.to_string())),
    };
    let mark = out.begin(Status::Ok as u8);
    out.put_slice(&(items.len() as u32).to_le_bytes());
    // Scratch for the runs below. Collecting a run before the cache call
    // gives it an exact length; the unknown-length iterators used before
    // made the cache's own scratch grow from a size hint of 1.
    let mut keys: Vec<&[u8]> = Vec::new();
    let mut entries: Vec<(&[u8], &[u8])> = Vec::new();
    let mut items = items.peekable();
    while let Some((op, b)) = items.next() {
        match Op::from_u8(op) {
            Some(Op::Get) => {
                keys.clear();
                keys.reserve(items.len() + 1);
                keys.push(b);
                while let Some((_, k)) = items.next_if(|(o, _)| *o == op) {
                    keys.push(k);
                }
                cache.get_many(keys.iter().copied(), |found| {
                    for e in found {
                        match e {
                            Some(e) => value(e, out),
                            None => out.header(Status::NotFound as u8, 0),
                        }
                    }
                });
            }
            Some(Op::Set) => {
                // A run ends at the first item that does not parse; that
                // one is refused on its own and the next run starts after it.
                entries.clear();
                entries.reserve(items.len() + 1);
                let mut bad = None;
                match wire::set_body(b) {
                    Ok(kv) => entries.push(kv),
                    Err(e) => bad = Some(e),
                }
                while bad.is_none() {
                    let Some((_, b)) = items.next_if(|(o, _)| *o == op) else {
                        break;
                    };
                    match wire::set_body(b) {
                        Ok(kv) => entries.push(kv),
                        Err(e) => bad = Some(e),
                    }
                }
                match cache.set_many(entries.iter().copied()) {
                    Ok(()) => {
                        for _ in &entries {
                            out.header(Status::Ok as u8, 0);
                        }
                    }
                    // Something does not fit: store what does, one by one,
                    // so each item answers for itself.
                    Err(_) => {
                        for (k, v) in &entries {
                            match cache.set(k, v) {
                                Ok(()) => out.header(Status::Ok as u8, 0),
                                Err(e) => {
                                    metrics.set_too_large.fetch_add(1, Ordering::Relaxed);
                                    out.frame(Status::TooLarge as u8, Bytes::from(e.to_string()))
                                }
                            }
                        }
                    }
                }
                if let Some(e) = bad {
                    out.frame(Status::BadRequest as u8, Bytes::from(e.to_string()));
                }
            }
            Some(Op::Del) => {
                keys.clear();
                keys.reserve(items.len() + 1);
                keys.push(b);
                while let Some((_, k)) = items.next_if(|(o, _)| *o == op) {
                    keys.push(k);
                }
                cache.del_many(keys.iter().copied(), |found| {
                    out.header(if found { Status::Ok } else { Status::NotFound } as u8, 0)
                });
            }
            _ => out.frame(
                Status::UnknownOp as u8,
                Bytes::from(format!("op {op} is not allowed in a batch")),
            ),
        }
    }
    let total = out.since(mark);
    if total > MAX_RESPONSE {
        out.abort(mark);
        return out.frame(
            Status::TooLarge as u8,
            Bytes::from(format!("response of {total} bytes exceeds the limit")),
        );
    }
    out.end(mark);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(cache: &Cache, op: u8, body: Bytes) -> (Status, Bytes) {
        let mut out = FrameWriter::new();
        dispatch(op, &body, cache, &Metrics::new(), &mut out, &epoch::pin());
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
        let set = |k: &[u8], v: &[u8]| call(&cache, Op::Set as u8, wire::encode_set(k, v));
        let get = |k: &'static [u8]| call(&cache, Op::Get as u8, Bytes::from_static(k));
        let del = |k: &'static [u8]| call(&cache, Op::Del as u8, Bytes::from_static(k));
        assert_eq!(set(b"k", b"v"), (Status::Ok, Bytes::new()));
        assert_eq!(set(b"big", &big), (Status::Ok, Bytes::new()));
        assert_eq!(get(b"k"), (Status::Ok, Bytes::from_static(b"v")));
        assert_eq!(get(b"x"), (Status::NotFound, Bytes::new()));
        assert_eq!(get(b"big"), (Status::Ok, Bytes::from(big.clone())));
        assert_eq!(del(b"k"), (Status::Ok, Bytes::new()));
        assert_eq!(del(b"k"), (Status::NotFound, Bytes::new()));
        assert_eq!(get(b"k").0, Status::NotFound);
        // An empty key is a key.
        assert_eq!(set(b"", b""), (Status::Ok, Bytes::new()));
        assert_eq!(get(b""), (Status::Ok, Bytes::new()));
    }

    /// One nested frame per item, in order, each answered as the op would
    /// be on its own; a later item sees an earlier one's write.
    #[test]
    fn batch_answers_every_item_in_order() {
        let cache = Cache::new(
            NonZeroUsize::new(1 << 20).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        );
        let big = vec![9u8; wire::io::INLINE_BODY * 2];
        let mut b = wire::BatchEncoder::new();
        b.get(b"k");
        b.set(b"k", b"v");
        b.set(b"big", &big);
        b.get(b"k");
        b.get(b"big");
        b.del(b"k");
        b.del(b"k");
        b.get(b"k");
        let (st, body) = call(&cache, Op::Batch as u8, b.finish());
        assert_eq!(st, Status::Ok);
        let replies = wire::decode_replies(body).unwrap();
        let expect: [(Status, &[u8]); 8] = [
            (Status::NotFound, b""),
            (Status::Ok, b""),
            (Status::Ok, b""),
            (Status::Ok, b"v"),
            (Status::Ok, &big),
            (Status::Ok, b""),
            (Status::NotFound, b""),
            (Status::NotFound, b""),
        ];
        assert_eq!(replies.len(), expect.len());
        for (i, ((st, body), (est, eb))) in replies.iter().zip(expect).enumerate() {
            assert_eq!(
                (Status::from_u8(*st).unwrap(), &body[..]),
                (est, eb),
                "item {i}"
            );
        }
        let (st, body) = call(&cache, Op::Batch as u8, wire::BatchEncoder::new().finish());
        assert_eq!(st, Status::Ok);
        assert!(wire::decode_replies(body).unwrap().is_empty());
    }

    /// A refused item does not take its neighbours down with it: the SET
    /// that fits is stored, the one that does not answers TooLarge, a
    /// malformed item is BadRequest, an op that is not GET/SET/DEL is
    /// UnknownOp.
    #[test]
    fn batch_items_fail_individually() {
        use bytes::BufMut;
        let cache = Cache::new(
            NonZeroUsize::new(1 << 20).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        );
        let big = vec![0u8; 1 << 20];
        let mut raw = bytes::BytesMut::new();
        raw.put_u32_le(5);
        for (op, body) in [
            (Op::Set as u8, wire::encode_set(b"small", b"v")),
            (Op::Set as u8, wire::encode_set(b"big", &big)),
            (Op::Set as u8, wire::encode_set(b"after", b"w")),
            (Op::Set as u8, Bytes::from_static(&[9, 0, 0, 0, 1])),
            (Op::Ping as u8, Bytes::new()),
        ] {
            raw.put_slice(&wire::encode_header(op, body.len()));
            raw.put_slice(&body);
        }
        let (st, body) = call(&cache, Op::Batch as u8, raw.freeze());
        assert_eq!(st, Status::Ok);
        let replies = wire::decode_replies(body).unwrap();
        let statuses: Vec<_> = replies
            .iter()
            .map(|(s, _)| Status::from_u8(*s).unwrap())
            .collect();
        assert_eq!(
            statuses,
            [
                Status::Ok,
                Status::TooLarge,
                Status::Ok,
                Status::BadRequest,
                Status::UnknownOp
            ]
        );
        assert!(
            std::str::from_utf8(&replies[4].1)
                .unwrap()
                .contains("not allowed in a batch")
        );
        assert!(cache.get(b"small").is_some());
        assert!(cache.get(b"big").is_none());
        assert!(cache.get(b"after").is_some());
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
        // A SET whose key length runs past the body.
        assert_eq!(
            call(&cache, Op::Set as u8, Bytes::from_static(&[9, 0, 0, 0, 1])).0,
            Status::BadRequest
        );
        // A batch envelope that ends mid-item is refused whole.
        assert_eq!(
            call(
                &cache,
                Op::Batch as u8,
                Bytes::from_static(&[1, 0, 0, 0, 1, 0])
            )
            .0,
            Status::BadRequest
        );
        // A count beyond the item limit is rejected before any work is done.
        let mut huge = Vec::new();
        huge.extend_from_slice(&((wire::MAX_ITEMS + 1) as u32).to_le_bytes());
        huge.resize(4 + wire::HEADER_LEN * (wire::MAX_ITEMS + 1), 0);
        let (st, msg) = call(&cache, Op::Batch as u8, Bytes::from(huge));
        assert_eq!(st, Status::BadRequest);
        assert!(
            std::str::from_utf8(&msg)
                .unwrap()
                .contains("exceeds the limit")
        );
    }

    #[test]
    fn oversized_set_is_refused() {
        let cache = Cache::new(
            NonZeroUsize::new(1 << 20).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        );
        let big = vec![0u8; 1 << 20];
        let (st, _) = call(&cache, Op::Set as u8, wire::encode_set(b"big", &big));
        assert_eq!(st, Status::TooLarge);
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

    /// A refused token is counted; the counter is shared through `Options`.
    #[tokio::test]
    async fn wrong_token_counts_an_auth_failure() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let s = server();
        let addr = s.local_addr();
        let srv = s.clone();
        tokio::spawn(async move { srv.run().await });
        let mut c = TcpStream::connect(addr).await.unwrap();
        c.write_all(&wire::encode_header(Op::Auth as u8, 5))
            .await
            .unwrap();
        c.write_all(b"wrong").await.unwrap();
        let mut hdr = [0u8; wire::HEADER_LEN];
        c.read_exact(&mut hdr).await.unwrap();
        assert_eq!(wire::decode_header(&hdr).0, Status::Unauthorized as u8);
        assert_eq!(s.metrics.auth_failures.load(Ordering::Relaxed), 1);
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
        let set = wire::encode_set(b"k", &big);
        c.write_all(&wire::encode_header(Op::Set as u8, set.len()))
            .await
            .unwrap();
        c.write_all(&set).await.unwrap();
        let mut hdr = [0u8; wire::HEADER_LEN];
        c.read_exact(&mut hdr).await.unwrap();
        assert_eq!(wire::decode_header(&hdr), (Status::Ok as u8, 0));
        // 15 × 4 MiB stays under the response cap but far above what the
        // socket buffers can absorb unread.
        let mut get = wire::BatchEncoder::new();
        for _ in 0..15 {
            get.get(b"k");
        }
        let get = get.finish();
        c.write_all(&wire::encode_header(Op::Batch as u8, get.len()))
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
        let (st, _) = call(&s.cache, Op::Get as u8, Bytes::from_static(b"k"));
        assert_eq!(st, Status::NotFound);
        let mut c = TcpStream::connect(addr).await.unwrap();
        auth(&mut c).await;
        tokio::io::AsyncWriteExt::write_all(&mut c, &wire::encode_header(Op::Set as u8, 0))
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

    #[tokio::test]
    async fn tls_connection_authenticates_and_serves() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let s = server_with(opts().tls(Some(crate::tls::testing::server())));
        assert!(s.is_tls());
        let addr = s.local_addr();
        tokio::spawn(async move { s.run().await });
        let (connector, name) = crate::tls::testing::client();
        let tcp = TcpStream::connect(addr).await.unwrap();
        let mut c = connector.connect(name, tcp).await.unwrap();
        c.write_all(&wire::encode_header(Op::Auth as u8, 1))
            .await
            .unwrap();
        c.write_all(b"t").await.unwrap();
        let mut hdr = [0u8; wire::HEADER_LEN];
        c.read_exact(&mut hdr).await.unwrap();
        assert_eq!(wire::decode_header(&hdr).0, Status::Ok as u8);
        c.write_all(&wire::encode_header(Op::Del as u8, 1))
            .await
            .unwrap();
        c.write_all(b"k").await.unwrap();
        c.read_exact(&mut hdr).await.unwrap();
        assert_eq!(wire::decode_header(&hdr), (Status::NotFound as u8, 0));
        // A plain-text peer is not a TLS client: closed without a reply.
        let mut plain = TcpStream::connect(addr).await.unwrap();
        plain
            .write_all(&wire::encode_header(Op::Auth as u8, 1))
            .await
            .unwrap();
        plain.write_all(b"t").await.unwrap();
        let mut rest = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), plain.read_to_end(&mut rest))
            .await
            .expect("closed by the server")
            .unwrap();
        // At most a TLS alert record (content type 21), never a frame.
        assert!(rest.is_empty() || rest[0] == 0x15, "{rest:?}");
    }

    /// The handshake is under the AUTH cap: a peer that opens a socket to
    /// a TLS listener and never sends a ClientHello is cut at the deadline.
    #[tokio::test(start_paused = true)]
    async fn unfinished_tls_handshake_is_cut_at_the_hard_cap() {
        use tokio::io::AsyncReadExt;
        let s = server_with(
            opts()
                .idle_timeout(None)
                .tls(Some(crate::tls::testing::server())),
        );
        let addr = s.local_addr();
        tokio::spawn(async move { s.run().await });
        let mut c = TcpStream::connect(addr).await.unwrap();
        let start = Instant::now();
        let mut rest = Vec::new();
        tokio::time::timeout(AUTH_TIMEOUT * 2, c.read_to_end(&mut rest))
            .await
            .expect("closed by the server")
            .unwrap();
        assert!(rest.is_empty());
        assert!(start.elapsed() >= AUTH_TIMEOUT, "cut early");
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
        let mut b = wire::BatchEncoder::new();
        b.get(b"a");
        b.get(b"b");
        let (st, _) = call(&cache, Op::Batch as u8, b.finish());
        assert_eq!(st, Status::TooLarge);
    }
}
