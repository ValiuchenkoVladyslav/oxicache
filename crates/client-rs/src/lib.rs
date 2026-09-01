//! TCP client for oxicache. One [`Client`] owns one connection and is cheap
//! to clone; calls from any number of tasks are pipelined onto it and matched
//! to responses in order. Every client authenticates with a token as it
//! connects. Keys are bytes; values are any serde type, stored as MessagePack
//! (`rmp-serde`, structs as maps — the same encoding the TypeScript client
//! writes, so both clients can read each other's values). `get` decodes into
//! the type the caller names; there is no byte-level value API.
//!
//! ```ignore
//! let c = Client::connect(addr, "s3cret").await?;
//! c.set("user:7", &user).await?;
//! let user = c.get::<User>("user:7").await?;          // Option<User>
//! let gone = c.del("user:7").await?;                  // bool
//!
//! // Any number of gets, sets and dels in one round trip, answered in order.
//! let mut b = Batch::new();
//! let user = b.get::<User>("user:7");
//! let hits = b.get::<u64>("hits:7");
//! b.set("seen:7", true)?;
//! let out = c.batch(b).await?;
//! let (user, hits) = (out.get(user)?, out.get(hits)?);  // Option<User>, Option<u64>
//! ```
//!
//! A [`Batch`] hands out a typed [`Slot`] per item; the [`Outcome`] answers
//! each slot as the standalone call would (`Option<V>` for a get, `()` for
//! a set, `bool` for a del) or with that item's own error, so one refused
//! item does not hide the others.
//!
//! The server closes a connection that stays silent for its idle timeout
//! (300 s by default), so a client that has sent nothing for
//! [`KEEPALIVE`](oxicache_wire::KEEPALIVE) (100 s) pings on its own; nothing
//! is needed from the caller to keep a quiet client connected.
//!
//! A lost connection — closed by the server (idle timeout, restart) or the
//! network — is reconnected lazily by the next call, which re-authenticates
//! and re-issues the calls that were in flight once (every op is
//! idempotent). Failed attempts back off from 100 ms to 5 s. A refused token
//! or a server that does not speak the protocol is permanent: the client is
//! dead and every call fails with that error. Status, encoding and decoding
//! errors are the call's alone; the connection stays.
//!
//! Several servers with the keyspace spread over them are a [`Cluster`]:
//! the same calls, with the server for each key picked by consistent
//! hashing, and a server that stops answering dropped from the ring until
//! it is back.
//!
//! A server started with `OXICACHE_TLS_CERT` speaks TLS; connect to it with
//! [`Client::connect_tls`] and a [`Tls`] naming what to trust and who the
//! server must be.
//!
//! With the `tracing` feature (off by default) the client emits
//! connection-lifecycle events under the `oxicache_client` target: connects
//! at DEBUG, reconnects at INFO, failed attempts and their backoff at WARN,
//! fatal errors at ERROR, keepalive pings at TRACE. Requests themselves are
//! never traced.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, PoisonError, RwLock};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use oxicache_wire::io::{BUF, FrameReader, FrameWriter};
use oxicache_wire::{self as wire, Op, Status};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::time::Instant;
use tokio_rustls::TlsConnector;

mod cluster;
mod ring;

pub use cluster::{Cluster, Failover};
pub use rustls;
pub use rustls_pki_types::ServerName;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("connection closed")]
    Closed,
    #[error("server returned {status:?}: {message}")]
    Status { status: Status, message: String },
    #[error("invalid status byte {0}")]
    InvalidStatus(u8),
    #[error("decode: {0}")]
    Decode(#[from] wire::DecodeError),
    #[error("response of {0} bytes exceeds the client limit of {MAX_FRAME}")]
    ResponseTooLarge(usize),
    #[error("serialize: {0}")]
    Serialize(#[from] rmp_serde::encode::Error),
    #[error("deserialize: {0}")]
    Deserialize(#[from] rmp_serde::decode::Error),
    #[error("server answered {got} items for a batch of {expected}")]
    Count { expected: usize, got: usize },
    #[error("{0} items in one batch exceeds the limit of {limit}", limit = wire::MAX_ITEMS)]
    TooManyItems(usize),
    #[error("reading {}: {source}", path.display())]
    Pem {
        path: std::path::PathBuf,
        source: rustls_pki_types::pem::Error,
    },
    #[error("tls: {0}")]
    Tls(#[from] rustls::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Largest response body accepted from the server.
const MAX_FRAME: usize = wire::MAX_FRAME;

/// Wait before the second reconnect attempt in a row; the first is immediate.
const BACKOFF_MIN: Duration = Duration::from_millis(100);
/// Every failed attempt doubles the wait, up to this.
const BACKOFF_MAX: Duration = Duration::from_secs(5);

/// A response as the reader hands it over: `Ok` and `NotFound` with their
/// body, any other status already turned into the error it means.
type Answer = oneshot::Sender<Result<(Status, Bytes)>>;
type Request = (Op, Bytes, Answer);

/// How to speak TLS to the server: what its certificate must chain to
/// and the name (DNS or IP) it must be issued for. Build one with
/// [`Tls::trusting`] for a private CA, or fill the fields with any
/// [`rustls::ClientConfig`] (system roots, client certificates, …).
#[derive(Clone)]
pub struct Tls {
    pub server_name: ServerName<'static>,
    pub config: Arc<rustls::ClientConfig>,
}

impl Tls {
    /// Trust the CA certificates in the PEM file `ca` and require the
    /// server's certificate to be issued for `server_name`. TLS 1.3 only,
    /// like the server.
    pub fn trusting(ca: &Path, server_name: ServerName<'static>) -> Result<Self> {
        use rustls_pki_types::CertificateDer;
        use rustls_pki_types::pem::PemObject;
        let read = |source| Error::Pem {
            path: ca.to_path_buf(),
            source,
        };
        let mut roots = rustls::RootCertStore::empty();
        for cert in CertificateDer::pem_file_iter(ca).map_err(read)? {
            roots.add(cert.map_err(read)?)?;
        }
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(roots)
        .with_no_client_auth();
        Ok(Self {
            server_name,
            config: Arc::new(config),
        })
    }
}

impl std::fmt::Debug for Tls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tls")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

/// A connection to one server, kept alive across losses: cheap to clone,
/// shared by any number of tasks. See the crate docs for what is retried.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

/// An error no reconnect can fix: once seen, the client is dead and every
/// call fails with it.
#[derive(Clone)]
enum Fatal {
    /// The token was refused on (re)connect: rotated or wrong, and no
    /// amount of reconnecting will change the server's mind.
    Unauthorized(String),
    /// The server sent a status byte the protocol does not have.
    InvalidStatus(u8),
    /// The server sent a frame nobody asked for: matching can't be trusted.
    Desync,
}

impl Fatal {
    fn error(&self) -> Error {
        match self {
            Self::Unauthorized(message) => Error::Status {
                status: Status::Unauthorized,
                message: message.clone(),
            },
            Self::InvalidStatus(b) => Error::InvalidStatus(*b),
            Self::Desync => Error::Closed,
        }
    }

    /// Whether a failed connect + AUTH is worth trying again.
    fn of(err: &Error) -> Option<Self> {
        match err {
            Error::Status {
                status: Status::Unauthorized,
                message,
            } => Some(Self::Unauthorized(message.clone())),
            Error::InvalidStatus(b) => Some(Self::InvalidStatus(*b)),
            _ => None,
        }
    }
}

/// Why a connection ended, recorded by the I/O task that ended it before
/// the callers waiting on it are woken, so they know whether to reconnect.
#[derive(Clone)]
enum Lost {
    /// EOF, reset, I/O error, oversized response: the next call reconnects.
    Transient,
    Fatal(Fatal),
}

/// A transient reason a connect + AUTH failed, kept in a form every caller
/// waiting on that attempt can be handed (an [`Error`] is not `Clone`).
#[derive(Clone)]
enum Setback {
    Io(std::io::ErrorKind, String),
    /// The server hung up before answering AUTH.
    Closed,
    /// A status other than `Unauthorized` to AUTH; not our server's doing,
    /// but not proof of a different protocol either.
    Status(Status, String),
    ResponseTooLarge(usize),
}

impl Setback {
    fn error(&self) -> Error {
        match self {
            Self::Io(kind, message) => Error::Io(std::io::Error::new(*kind, message.clone())),
            Self::Closed => Error::Closed,
            Self::Status(status, message) => Error::Status {
                status: *status,
                message: message.clone(),
            },
            Self::ResponseTooLarge(n) => Error::ResponseTooLarge(*n),
        }
    }
}

/// Why a connect + AUTH failed: for good, or for now.
enum Failed {
    Fatal(Fatal),
    Setback(Setback),
}

impl Failed {
    /// Every error AUTH can produce; decoding and counting never happen
    /// on an empty reply, so those variants read as a hang-up.
    fn of(err: Error) -> Self {
        if let Some(fatal) = Fatal::of(&err) {
            return Self::Fatal(fatal);
        }
        Self::Setback(match err {
            Error::Io(e) => Setback::Io(e.kind(), e.to_string()),
            Error::Status { status, message } => Setback::Status(status, message),
            Error::ResponseTooLarge(n) => Setback::ResponseTooLarge(n),
            _ => Setback::Closed,
        })
    }

    fn error(&self) -> Error {
        match self {
            Self::Fatal(f) => f.error(),
            Self::Setback(s) => s.error(),
        }
    }
}

/// One live socket: the channel to its writer task and the tasks
/// themselves, aborted when the connection is replaced or the client
/// dropped.
struct Conn {
    tx: mpsc::Sender<Request>,
    lost: Arc<OnceLock<Lost>>,
    writer: tokio::task::JoinHandle<()>,
    reader: tokio::task::JoinHandle<()>,
}

impl Conn {
    /// Connect and authenticate; the tasks are running when this returns.
    async fn open(
        addr: SocketAddr,
        tls: Option<&Tls>,
        token: &Bytes,
        keepalive: Duration,
    ) -> std::result::Result<Arc<Self>, Failed> {
        let conn = Self::connect(addr, tls, keepalive)
            .await
            .map_err(Failed::of)?;
        match conn.send(Op::Auth, token.clone()).await {
            Ok(_) => Ok(conn),
            // Hung up during AUTH: the reader knows whether the server
            // was speaking another protocol, which is as final as a refused
            // token.
            Err(Error::Closed) => Err(match conn.verdict() {
                Lost::Fatal(f) => Failed::Fatal(f),
                Lost::Transient => Failed::Setback(Setback::Closed),
            }),
            Err(e) => Err(Failed::of(e)),
        }
    }

    /// A failed handshake (refused certificate included) is an I/O error
    /// like a refused connection: worth another try later, since the server
    /// may be restarted with a certificate this client trusts.
    async fn connect(
        addr: SocketAddr,
        tls: Option<&Tls>,
        keepalive: Duration,
    ) -> Result<Arc<Self>> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;

        if let Some(tls) = tls {
            let stream = TlsConnector::from(tls.config.clone())
                .connect(tls.server_name.clone(), stream)
                .await?;
            let (r, w) = tokio::io::split(stream);
            Ok(Self::spawn(r, w, keepalive))
        } else {
            let (r, w) = stream.into_split();
            Ok(Self::spawn(r, w, keepalive))
        }
    }

    /// Start the I/O tasks on a connected stream.
    fn spawn<R, W>(r: R, w: W, keepalive: Duration) -> Arc<Self>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (tx, rx) = mpsc::channel::<Request>(1024);
        let (pending_tx, pending_rx) = mpsc::unbounded_channel::<Answer>();
        let (stop_tx, stop_rx) = oneshot::channel();
        let lost = Arc::new(OnceLock::new());
        let writer = tokio::spawn(write_loop(
            w,
            rx,
            pending_tx,
            keepalive,
            stop_rx,
            lost.clone(),
        ));
        let reader = tokio::spawn(read_loop(r, pending_rx, stop_tx, lost.clone()));
        Arc::new(Self {
            tx,
            lost,
            writer,
            reader,
        })
    }

    /// Queue one request and wait for its reply. `Closed` means this
    /// connection is gone, nothing else does.
    async fn send(&self, op: Op, body: Bytes) -> Result<(Status, Bytes)> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send((op, body, reply))
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Why this connection ended, if it has; `None` while it is live.
    fn lost(&self) -> Option<Lost> {
        self.lost.get().cloned()
    }

    /// A connection whose caller saw `Closed` is gone even if neither task
    /// has said why yet.
    fn verdict(&self) -> Lost {
        self.lost().unwrap_or(Lost::Transient)
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.writer.abort();
        self.reader.abort();
    }
}

/// What a call finds when it starts.
enum Link {
    Up(Arc<Conn>),
    /// Lost; the next call reconnects.
    Down,
    Dead(Fatal),
}

/// When the next reconnect may start, and how long the one after it waits
/// if this one fails too.
struct Backoff {
    delay: Duration,
    not_before: Instant,
    /// What the last attempt failed with, for callers that were waiting on
    /// it; `None` after a success.
    last: Option<Setback>,
}

impl Backoff {
    /// Ready now: the attempt after a loss is immediate, only repeated
    /// failures wait.
    fn fresh() -> Self {
        Self {
            delay: BACKOFF_MIN,
            not_before: Instant::now(),
            last: None,
        }
    }

    fn failed(&mut self, setback: Setback) {
        self.not_before = Instant::now() + self.delay;
        self.delay = (self.delay * 2).min(BACKOFF_MAX);
        self.last = Some(setback);
    }
}

/// The connection supervisor. `link` is all the hot path touches (one read
/// lock, one `Arc` clone); `slow` is taken only to reconnect, so concurrent
/// callers that find the link down wait for the one attempt in progress.
struct Inner {
    addr: SocketAddr,
    tls: Option<Tls>,
    token: Bytes,
    keepalive: Duration,
    link: RwLock<Link>,
    slow: Mutex<Backoff>,
    /// Bumped when an attempt ends, so a caller that queued on `slow`
    /// while one was running can tell it happened and share its outcome
    /// instead of making its own.
    attempts: AtomicU64,
    reconnects: AtomicU64,
}

impl Inner {
    /// The connection to use, `None` if a reconnect is needed first.
    fn current(&self) -> Result<Option<Arc<Conn>>> {
        match &*self.link.read().unwrap_or_else(PoisonError::into_inner) {
            // A connection whose end is already known is not worth a call
            // that would only come back `Closed`.
            Link::Up(c) if c.lost().is_none() => Ok(Some(c.clone())),
            Link::Up(_) | Link::Down => Ok(None),
            Link::Dead(f) => Err(f.error()),
        }
    }

    fn set_link(&self, link: Link) {
        *self.link.write().unwrap_or_else(PoisonError::into_inner) = link;
    }

    /// Get a live connection after `failed` (the one the caller was on, or
    /// `None` if it found the link down) stopped answering: the one another
    /// caller already opened, or a new one. Errors are the attempt's, and
    /// a fatal one is remembered for every later call.
    async fn recover(&self, failed: Option<&Arc<Conn>>) -> Result<Arc<Conn>> {
        let seen = self.attempts.load(Ordering::Acquire);
        let mut backoff = self.slow.lock().await;
        // Under `slow` the link is settled: the caller that got here first
        // has already replaced or condemned it, or failed trying.
        let settled = match &*self.link.read().unwrap_or_else(PoisonError::into_inner) {
            Link::Up(c) => match (c.lost(), failed) {
                (Some(Lost::Fatal(f)), _) => Some(Err(f)),
                (Some(Lost::Transient), _) => None,
                // The writer saw the loss and nobody has recorded why yet.
                (None, Some(f)) if Arc::ptr_eq(c, f) => None,
                (None, _) => Some(Ok(c.clone())),
            },
            Link::Down => None,
            Link::Dead(f) => Some(Err(f.clone())),
        };
        match settled {
            Some(Ok(conn)) => return Ok(conn),
            Some(Err(fatal)) => {
                self.set_link(Link::Dead(fatal.clone()));
                return Err(fatal.error());
            }
            None => self.set_link(Link::Down),
        }
        // An attempt ended while this caller was queued: its failure is
        // this caller's too, rather than the start of a second one.
        if self.attempts.load(Ordering::Acquire) != seen
            && let Some(last) = &backoff.last
        {
            return Err(last.error());
        }
        tokio::time::sleep_until(backoff.not_before).await;
        let outcome = Conn::open(self.addr, self.tls.as_ref(), &self.token, self.keepalive).await;
        self.attempts.fetch_add(1, Ordering::Release);
        match outcome {
            Ok(conn) => {
                #[cfg(feature = "tracing")]
                tracing::info!(addr = %self.addr, "reconnected");
                self.set_link(Link::Up(conn.clone()));
                *backoff = Backoff::fresh();
                self.reconnects.fetch_add(1, Ordering::Relaxed);
                Ok(conn)
            }
            Err(failed) => {
                match &failed {
                    Failed::Fatal(fatal) => {
                        #[cfg(feature = "tracing")]
                        tracing::error!(addr = %self.addr, error = %fatal.error(), "client dead");
                        self.set_link(Link::Dead(fatal.clone()));
                    }
                    Failed::Setback(setback) => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            addr = %self.addr,
                            error = %setback.error(),
                            retry_in = ?backoff.delay,
                            "reconnect failed"
                        );
                        backoff.failed(setback.clone());
                    }
                }
                Err(failed.error())
            }
        }
    }
}

impl Client {
    /// Connect and authenticate with `token` before returning; a wrong
    /// token is an [`Error::Status`] with `Unauthorized`.
    pub async fn connect(addr: SocketAddr, token: impl AsRef<[u8]>) -> Result<Self> {
        Self::connect_with(addr, None, token, wire::KEEPALIVE).await
    }

    /// [`connect`](Self::connect) over TLS: the handshake happens first,
    /// checking the server against `tls`, and every reconnect repeats it.
    /// A failed handshake is an [`Error::Io`].
    pub async fn connect_tls(addr: SocketAddr, tls: Tls, token: impl AsRef<[u8]>) -> Result<Self> {
        Self::connect_with(addr, Some(tls), token, wire::KEEPALIVE).await
    }

    /// A client that has not connected yet: the first call opens the
    /// connection and authenticates, exactly as a reconnect does. A
    /// [`Cluster`] builds its servers this way, so one that is down when
    /// the cluster starts is a server to retry rather than a missing
    /// client.
    pub(crate) fn lazy(
        addr: SocketAddr,
        tls: Option<Tls>,
        token: Bytes,
        keepalive: Duration,
    ) -> Self {
        Self {
            inner: Self::inner(addr, tls, token, keepalive, Link::Down),
        }
    }

    fn inner(
        addr: SocketAddr,
        tls: Option<Tls>,
        token: Bytes,
        keepalive: Duration,
        link: Link,
    ) -> Arc<Inner> {
        Arc::new(Inner {
            addr,
            tls,
            token,
            keepalive,
            link: RwLock::new(link),
            slow: Mutex::new(Backoff::fresh()),
            attempts: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
        })
    }

    /// [`connect`](Self::connect) or [`connect_tls`](Self::connect_tls) with
    /// a heartbeat interval other than [`KEEPALIVE`](wire::KEEPALIVE): after
    /// `keepalive` without a write the client pings so the server's idle
    /// timeout does not close it. Only tests against a server with a short
    /// idle timeout need this.
    #[doc(hidden)]
    pub async fn connect_with(
        addr: SocketAddr,
        tls: Option<Tls>,
        token: impl AsRef<[u8]>,
        keepalive: Duration,
    ) -> Result<Self> {
        assert!(!keepalive.is_zero(), "a zero keepalive would ping non-stop");
        let token = Bytes::copy_from_slice(token.as_ref());
        let conn = Conn::open(addr, tls.as_ref(), &token, keepalive)
            .await
            .map_err(|f| f.error())?;
        #[cfg(feature = "tracing")]
        tracing::debug!(%addr, "connected");
        Ok(Self {
            inner: Self::inner(addr, tls, token, keepalive, Link::Up(conn)),
        })
    }

    /// How many times the connection has been re-established.
    #[doc(hidden)]
    pub fn reconnects(&self) -> u64 {
        self.inner.reconnects.load(Ordering::Relaxed)
    }

    /// One request, re-issued once if the connection it was on is lost;
    /// `body` is a refcounted `Bytes`, so keeping it for that is free.
    async fn call(&self, op: Op, body: Bytes) -> Result<(Status, Bytes)> {
        let inner = &self.inner;
        let mut conn = match inner.current()? {
            Some(conn) => conn,
            None => inner.recover(None).await?,
        };
        let mut retried = false;
        loop {
            match conn.send(op, body.clone()).await {
                Err(Error::Closed) if !retried => {
                    conn = inner.recover(Some(&conn)).await?;
                    retried = true;
                }
                res => return res,
            }
        }
    }

    /// Round-trip an empty request; resolves once the server has answered.
    pub async fn ping(&self) -> Result<()> {
        self.call(Op::Ping, Bytes::new()).await?;
        Ok(())
    }

    /// The value under `key` decoded as `V`, `None` if there is none.
    pub async fn get<V: DeserializeOwned>(&self, key: impl AsRef<[u8]>) -> Result<Option<V>> {
        let (status, body) = self
            .call(Op::Get, Bytes::copy_from_slice(key.as_ref()))
            .await?;
        Option::<V>::reply(status, body)
    }

    /// Store `value` under `key`, replacing what was there.
    pub async fn set<V: Serialize>(&self, key: impl AsRef<[u8]>, value: V) -> Result<()> {
        let key = key.as_ref();
        // The value is serialised straight into the request body; no
        // intermediate buffer.
        let mut body = BytesMut::with_capacity(wire::set_size(key, &[]));
        wire::put_set_key(&mut body, key);
        rmp_serde::encode::write_named(&mut (&mut body).writer(), &value)?;
        self.call(Op::Set, body.freeze()).await?;
        Ok(())
    }

    /// Remove `key`; whether there was anything to remove.
    pub async fn del(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        let (status, body) = self
            .call(Op::Del, Bytes::copy_from_slice(key.as_ref()))
            .await?;
        bool::reply(status, body)
    }

    /// Send every item of `batch` in one request and answer each in order.
    /// An empty batch is a round trip that answers nothing.
    pub async fn batch(&self, batch: Batch) -> Result<Outcome> {
        let n = batch.enc.len();
        if n > wire::MAX_ITEMS {
            return Err(Error::TooManyItems(n));
        }
        self.batch_body(batch.enc.finish(), n).await
    }

    /// Send an encoded BATCH body of `n` items and index the replies. A
    /// [`Cluster`] splits a batch into one body per server and lands here
    /// for the server that gets all of it.
    pub(crate) async fn batch_body(&self, body: Bytes, n: usize) -> Result<Outcome> {
        let (_, body) = self.call(Op::Batch, body).await?;
        let frames = wire::frames(&body)?;
        if frames.len() != n {
            return Err(Error::Count {
                expected: n,
                got: frames.len(),
            });
        }
        // Offsets into the one response buffer: no allocation or refcount
        // per item, only for the slots that are read.
        let base = body.as_ptr() as usize;
        let index = frames
            .map(|(status, b)| (status, (b.as_ptr() as usize - base) as u32, b.len() as u32))
            .collect();
        Ok(Outcome { body, index })
    }
}

/// Items for one [`Client::batch`] call, in the order they are added.
/// Each method hands back the [`Slot`] that reads its answer from the
/// [`Outcome`].
#[derive(Default)]
pub struct Batch {
    enc: wire::BatchEncoder,
}

impl Batch {
    pub fn new() -> Self {
        Self::default()
    }

    /// With room for `bytes` of encoded items (headers included) before the
    /// buffer grows; worth using when the batch's size is roughly known.
    pub fn with_capacity(bytes: usize) -> Self {
        Self {
            enc: wire::BatchEncoder::with_capacity(bytes),
        }
    }

    /// Items so far.
    pub fn len(&self) -> usize {
        self.enc.len()
    }

    pub fn is_empty(&self) -> bool {
        self.enc.is_empty()
    }

    fn slot<T>(&self) -> Slot<T> {
        Slot {
            index: self.enc.len() - 1,
            _reply: std::marker::PhantomData,
        }
    }

    /// Fetch `key`, decoding it as `V` when the outcome is read.
    pub fn get<V: DeserializeOwned>(&mut self, key: impl AsRef<[u8]>) -> Slot<Option<V>> {
        self.enc.get(key.as_ref());
        self.slot()
    }

    /// Store `value` under `key`; encoding happens now, straight into the
    /// batch buffer, so a value that cannot be serialised is refused here
    /// rather than at send time.
    pub fn set<V: Serialize>(&mut self, key: impl AsRef<[u8]>, value: V) -> Result<Slot<()>> {
        self.enc.set_with(key.as_ref(), |out| {
            rmp_serde::encode::write_named(&mut out.writer(), &value)
        })?;
        Ok(self.slot())
    }

    /// Remove `key`.
    pub fn del(&mut self, key: impl AsRef<[u8]>) -> Slot<bool> {
        self.enc.del(key.as_ref());
        self.slot()
    }
}

/// One item's place in a [`Batch`], typed by what it answers with.
pub struct Slot<T> {
    index: usize,
    _reply: std::marker::PhantomData<fn() -> T>,
}

impl<T> Clone for Slot<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Slot<T> {}

impl<T> std::fmt::Debug for Slot<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Slot").field(&self.index).finish()
    }
}

/// The server's answers to a [`Batch`], one per item.
#[derive(Debug)]
pub struct Outcome {
    body: Bytes,
    /// Per item: status, and where its body lies in `body`.
    index: Vec<(u8, u32, u32)>,
}

impl Outcome {
    /// Answers, which is the batch's item count.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// The raw status of item `index`; `None` past the end or for a status
    /// byte this client does not know.
    pub fn status(&self, index: usize) -> Option<Status> {
        self.index
            .get(index)
            .and_then(|(s, _, _)| Status::from_u8(*s))
    }

    /// The answer in `slot`: the item's value, or the error the server
    /// refused it with.
    pub fn get<T: Reply>(&self, slot: Slot<T>) -> Result<T> {
        // A slot only comes from the batch this outcome answers, and the
        // count was checked when it arrived.
        let (status, start, len) = self.index[slot.index];
        let status = Status::from_u8(status).ok_or(Error::InvalidStatus(status))?;
        let (start, len) = (start as usize, len as usize);
        T::reply(status, self.body.slice(start..start + len))
    }
}

mod sealed {
    pub trait Sealed {}
}

/// What a [`Slot`] answers with: `Option<V>` for a get, `()` for a set,
/// `bool` for a del. Implemented here for exactly those.
pub trait Reply: sealed::Sealed + Sized {
    #[doc(hidden)]
    fn reply(status: Status, body: Bytes) -> Result<Self>;
}

/// The error a status other than the ones an op answers with means.
fn refused(status: Status, body: Bytes) -> Error {
    Error::Status {
        status,
        message: String::from_utf8_lossy(&body).into_owned(),
    }
}

impl<V: DeserializeOwned> sealed::Sealed for Option<V> {}
impl<V: DeserializeOwned> Reply for Option<V> {
    fn reply(status: Status, body: Bytes) -> Result<Self> {
        match status {
            Status::Ok => Ok(Some(rmp_serde::from_slice(&body)?)),
            Status::NotFound => Ok(None),
            other => Err(refused(other, body)),
        }
    }
}

impl sealed::Sealed for () {}
impl Reply for () {
    fn reply(status: Status, body: Bytes) -> Result<Self> {
        match status {
            Status::Ok => Ok(()),
            other => Err(refused(other, body)),
        }
    }
}

impl sealed::Sealed for bool {}
impl Reply for bool {
    fn reply(status: Status, body: Bytes) -> Result<Self> {
        match status {
            Status::Ok => Ok(true),
            Status::NotFound => Ok(false),
            other => Err(refused(other, body)),
        }
    }
}

/// A flush this small waits one scheduler turn for more requests to
/// coalesce (see `write_loop`); a larger one goes out at once.
const YIELD_BELOW: usize = BUF / 4;

async fn write_loop<W: AsyncWrite + Unpin>(
    mut w: W,
    mut rx: mpsc::Receiver<Request>,
    pending: mpsc::UnboundedSender<Answer>,
    keepalive: Duration,
    mut stop: oneshot::Receiver<()>,
    lost: Arc<OnceLock<Lost>>,
) {
    // Small request bodies were just encoded and are hot, so copying them
    // into the coalescing buffer is cheap; from 4 KiB up, riding as a
    // `writev` iovec entry beats the copy (measured in round 13).
    let mut out = FrameWriter::with_inline_limit(4 << 10);
    let idle = tokio::time::sleep(keepalive);
    tokio::pin!(idle);
    loop {
        let msg = tokio::select! {
            msg = rx.recv() => match msg {
                Some(msg) => Some(msg),
                None => return,
            },
            () = idle.as_mut() => None,
            _ = &mut stop => return,
        };
        match msg {
            Some(mut msg) => {
                let (mut yielded, mut queued) = (false, 0);
                loop {
                    let (op, body, reply) = msg;
                    if pending.send(reply).is_err() {
                        return;
                    }
                    queued += body.len();
                    out.frame(op as u8, body);
                    match rx.try_recv() {
                        Ok(next) => msg = next,
                        // The callers woken by the last reply batch are
                        // about to queue their next request; one scheduler
                        // turn lets them, so this flush carries those too.
                        // Only worth it while the flush is small: large
                        // bodies already fill a packet each, and holding
                        // them back only delays the server.
                        Err(_) if !yielded && queued < YIELD_BELOW => {
                            yielded = true;
                            tokio::task::yield_now().await;
                            match rx.try_recv() {
                                Ok(next) => msg = next,
                                Err(_) => break,
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
            None => {
                #[cfg(feature = "tracing")]
                tracing::trace!("keepalive ping");
                // The reply goes through the same in-order queue as every
                // request, so the reader matches it; nobody waits on the
                // receiver, and a send to it fails silently.
                let (reply, _discard) = oneshot::channel();
                if pending.send(reply).is_err() {
                    return;
                }
                out.frame(Op::Ping as u8, Bytes::new());
            }
        }
        if let Err(_e) = out.flush(&mut w).await {
            #[cfg(feature = "tracing")]
            tracing::debug!(error = %_e, "write failed");
            // Dropping the write half shuts the socket down for writing, so
            // the reader sees EOF and fails the callers still queued.
            let _ = lost.set(Lost::Transient);
            return;
        }
        idle.as_mut().reset(Instant::now() + keepalive);
    }
}

/// Reads responses and hands each to the oldest pending caller. Records
/// why it stopped before it returns, because returning drops `pending`
/// and that is what wakes the callers who then ask.
async fn read_loop<R: AsyncRead + Unpin>(
    mut r: R,
    mut pending: mpsc::UnboundedReceiver<Answer>,
    stop: oneshot::Sender<()>,
    lost: Arc<OnceLock<Lost>>,
) {
    // Dropped on return, which is what stops the writer.
    let _stop = stop;
    let mut reader = FrameReader::new(MAX_FRAME);
    let why = loop {
        let stop = loop {
            match reader.next_buffered_owned() {
                Ok(Some((status, body))) => {
                    // The writer queues a reply before it sends the request,
                    // so a response with no reply waiting is unsolicited: the
                    // stream is desynchronised and matching can't be trusted.
                    let Ok(reply) = pending.try_recv() else {
                        break Some(Lost::Fatal(Fatal::Desync));
                    };
                    let res = match Status::from_u8(status) {
                        Some(status @ (Status::Ok | Status::NotFound)) => Ok((status, body)),
                        Some(status) => Err(Error::Status {
                            status,
                            message: String::from_utf8_lossy(&body).into_owned(),
                        }),
                        None => Err(Error::InvalidStatus(status)),
                    };
                    let fatal = match &res {
                        Err(Error::InvalidStatus(b)) => Some(Lost::Fatal(Fatal::InvalidStatus(*b))),
                        _ => None,
                    };
                    let _ = reply.send(res);
                    if fatal.is_some() {
                        break fatal;
                    }
                }
                Ok(None) => break None,
                Err(wire::io::FrameTooLarge(n)) => {
                    // Tell the caller why instead of a bare `Closed`; the
                    // stream is desynchronised past this point, so stop. The
                    // fault is the client's limit, not the protocol, so the
                    // next call may reconnect.
                    if let Ok(reply) = pending.try_recv() {
                        let _ = reply.send(Err(Error::ResponseTooLarge(n)));
                    }
                    break Some(Lost::Transient);
                }
            }
        };
        if let Some(why) = stop {
            break why;
        }
        match reader.fill(&mut r).await {
            Ok(true) => {}
            _ => break Lost::Transient,
        }
    };
    #[cfg(feature = "tracing")]
    match &why {
        Lost::Transient => tracing::debug!("connection lost"),
        Lost::Fatal(f) => tracing::error!(error = %f.error(), "connection dead"),
    }
    let _ = lost.set(why);
}
