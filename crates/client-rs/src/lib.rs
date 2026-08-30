//! TCP client for oxicache. One [`Client`] owns one connection and is cheap
//! to clone; calls from any number of tasks are pipelined onto it and matched
//! to responses in order. Every client authenticates with a token as it
//! connects. Keys are bytes; values are any serde type, stored as MessagePack
//! (`rmp-serde`, structs as maps — the same encoding the TypeScript client
//! writes, so both clients can read each other's values). `get` and
//! `get_multi` decode into the type the caller names; there is no byte-level
//! value API.
//!
//! ```ignore
//! let c = Client::connect(addr, "s3cret").await?;
//! c.set("user:7", &user).await?;
//! let user = c.get::<User>("user:7").await?;                                   // Option<User>
//! let (user, hits) = c.get_multi::<(User, u64), _>(("user:7", "hits:7")).await?;
//! let users = c.get_multi::<User, _>(["user:7", "user:8"]).await?;             // [Option<User>; 2]
//! let users = c.get_multi::<User, _>(ids).await?;                              // Vec<Option<User>>
//! let (user, hits): (Option<User>, Option<u64>) =                              // or from the binding
//!     c.get_multi(("user:7", "hits:7")).await?;
//! c.set_multi([("a", 1), ("b", 2)]).await?;
//! ```
//!
//! `get_multi`'s type argument names the value types only — one per key for
//! a tuple of keys (a mismatched count does not compile), a single type for
//! an array, `Vec` or slice of keys — and every slot comes back as an
//! `Option`, `None` for a missing key.
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
//! A server started with `OXICACHE_TLS_CERT` speaks TLS; connect to it with
//! [`Client::connect_tls`] and a [`Tls`] naming what to trust and who the
//! server must be.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, PoisonError, RwLock};
use std::time::Duration;

use bytes::Bytes;
use oxicache_wire::io::{BUF, FrameReader, FrameWriter};
use oxicache_wire::{self as wire, Op, Status};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::time::Instant;
use tokio_rustls::TlsConnector;

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
    #[error("server answered {got} values for {expected} keys")]
    Count { expected: usize, got: usize },
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

type Reply = oneshot::Sender<Result<Bytes>>;
type Request = (Op, Bytes, Reply);

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

/// A batch of keys for the `_multi` methods: a tuple `(K1, K2, …)` of up to
/// 16 keys, an array `[K; N]`, a `Vec<K>` or a `&[K]`, each key any
/// `AsRef<[u8]>`. Tuples and arrays give arrays back, vectors and slices
/// give vectors.
pub trait Keys {
    /// `[T; N]` for tuples and arrays, `Vec<T>` otherwise.
    type Batch<T>;
    fn keys(&self) -> Vec<&[u8]>;
    fn batch<T>(items: Vec<T>) -> Result<Self::Batch<T>>;
}

fn expect_count<T>(items: &[T], expected: usize) -> Result<()> {
    if items.len() == expected {
        Ok(())
    } else {
        Err(Error::Count {
            expected,
            got: items.len(),
        })
    }
}

fn array<T, const N: usize>(items: Vec<T>) -> Result<[T; N]> {
    items.try_into().map_err(|v: Vec<T>| Error::Count {
        expected: N,
        got: v.len(),
    })
}

macro_rules! impl_key_tuples {
    ($($n:literal => ($($i:tt $K:ident),+);)+) => {$(
        impl<$($K: AsRef<[u8]>),+> Keys for ($($K,)+) {
            type Batch<T> = [T; $n];
            fn keys(&self) -> Vec<&[u8]> {
                vec![$(self.$i.as_ref()),+]
            }
            fn batch<T>(items: Vec<T>) -> Result<[T; $n]> {
                array(items)
            }
        }
    )+};
}

impl_key_tuples! {
    1 => (0 K0);
    2 => (0 K0, 1 K1);
    3 => (0 K0, 1 K1, 2 K2);
    4 => (0 K0, 1 K1, 2 K2, 3 K3);
    5 => (0 K0, 1 K1, 2 K2, 3 K3, 4 K4);
    6 => (0 K0, 1 K1, 2 K2, 3 K3, 4 K4, 5 K5);
    7 => (0 K0, 1 K1, 2 K2, 3 K3, 4 K4, 5 K5, 6 K6);
    8 => (0 K0, 1 K1, 2 K2, 3 K3, 4 K4, 5 K5, 6 K6, 7 K7);
    9 => (0 K0, 1 K1, 2 K2, 3 K3, 4 K4, 5 K5, 6 K6, 7 K7, 8 K8);
    10 => (0 K0, 1 K1, 2 K2, 3 K3, 4 K4, 5 K5, 6 K6, 7 K7, 8 K8, 9 K9);
    11 => (0 K0, 1 K1, 2 K2, 3 K3, 4 K4, 5 K5, 6 K6, 7 K7, 8 K8, 9 K9, 10 K10);
    12 => (0 K0, 1 K1, 2 K2, 3 K3, 4 K4, 5 K5, 6 K6, 7 K7, 8 K8, 9 K9, 10 K10, 11 K11);
    13 => (0 K0, 1 K1, 2 K2, 3 K3, 4 K4, 5 K5, 6 K6, 7 K7, 8 K8, 9 K9, 10 K10, 11 K11, 12 K12);
    14 => (0 K0, 1 K1, 2 K2, 3 K3, 4 K4, 5 K5, 6 K6, 7 K7, 8 K8, 9 K9, 10 K10, 11 K11, 12 K12, 13 K13);
    15 => (0 K0, 1 K1, 2 K2, 3 K3, 4 K4, 5 K5, 6 K6, 7 K7, 8 K8, 9 K9, 10 K10, 11 K11, 12 K12, 13 K13, 14 K14);
    16 => (0 K0, 1 K1, 2 K2, 3 K3, 4 K4, 5 K5, 6 K6, 7 K7, 8 K8, 9 K9, 10 K10, 11 K11, 12 K12, 13 K13, 14 K14, 15 K15);
}

impl<K: AsRef<[u8]>, const N: usize> Keys for [K; N] {
    type Batch<T> = [T; N];
    fn keys(&self) -> Vec<&[u8]> {
        self.iter().map(AsRef::as_ref).collect()
    }
    fn batch<T>(items: Vec<T>) -> Result<[T; N]> {
        array(items)
    }
}

impl<K: AsRef<[u8]>> Keys for Vec<K> {
    type Batch<T> = Vec<T>;
    fn keys(&self) -> Vec<&[u8]> {
        self.iter().map(AsRef::as_ref).collect()
    }
    fn batch<T>(items: Vec<T>) -> Result<Vec<T>> {
        Ok(items)
    }
}

impl<K: AsRef<[u8]>> Keys for &[K] {
    type Batch<T> = Vec<T>;
    fn keys(&self) -> Vec<&[u8]> {
        self.iter().map(AsRef::as_ref).collect()
    }
    fn batch<T>(items: Vec<T>) -> Result<Vec<T>> {
        Ok(items)
    }
}

/// Why a client stopped for good. Kept as data rather than an [`Error`]
/// because every later call has to report it again and `Error` is not
/// `Clone`.
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
        match tls {
            None => {
                let (r, w) = stream.into_split();
                Ok(Self::spawn(r, w, keepalive))
            }
            Some(tls) => {
                let stream = TlsConnector::from(tls.config.clone())
                    .connect(tls.server_name.clone(), stream)
                    .await?;
                let (r, w) = tokio::io::split(stream);
                Ok(Self::spawn(r, w, keepalive))
            }
        }
    }

    /// Start the I/O tasks on a connected stream.
    fn spawn<R, W>(r: R, w: W, keepalive: Duration) -> Arc<Self>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (tx, rx) = mpsc::channel::<Request>(1024);
        let (pending_tx, pending_rx) = mpsc::unbounded_channel::<Reply>();
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
    async fn send(&self, op: Op, body: Bytes) -> Result<Bytes> {
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
                self.set_link(Link::Up(conn.clone()));
                *backoff = Backoff::fresh();
                self.reconnects.fetch_add(1, Ordering::Relaxed);
                Ok(conn)
            }
            Err(failed) => {
                match &failed {
                    Failed::Fatal(fatal) => self.set_link(Link::Dead(fatal.clone())),
                    Failed::Setback(setback) => backoff.failed(setback.clone()),
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
        Ok(Self {
            inner: Arc::new(Inner {
                addr,
                tls,
                token,
                keepalive,
                link: RwLock::new(Link::Up(conn)),
                slow: Mutex::new(Backoff::fresh()),
                attempts: AtomicU64::new(0),
                reconnects: AtomicU64::new(0),
            }),
        })
    }

    /// How many times the connection has been re-established.
    #[doc(hidden)]
    pub fn reconnects(&self) -> u64 {
        self.inner.reconnects.load(Ordering::Relaxed)
    }

    /// One request, re-issued once if the connection it was on is lost;
    /// `body` is a refcounted `Bytes`, so keeping it for that is free.
    async fn call(&self, op: Op, body: Bytes) -> Result<Bytes> {
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

    /// Round-trip an empty request: `Ok` proves the connection is alive and
    /// the server is answering. The client also does this on its own after
    /// [`KEEPALIVE`](wire::KEEPALIVE) of silence.
    pub async fn ping(&self) -> Result<()> {
        self.call(Op::Ping, Bytes::new()).await?;
        Ok(())
    }

    /// Fetch one key, decoding its value as `V`; `None` if it is missing.
    pub async fn get<V: DeserializeOwned>(&self, key: impl AsRef<[u8]>) -> Result<Option<V>> {
        Ok(self.get_multi::<(V,), _>((key,)).await?.0)
    }

    /// Fetch many keys, decoding every value as `Vs` says: one slot per key
    /// in request order, `None` for a missing one. `Vs` is a tuple of value
    /// types for a tuple of keys (one per key) and a single type for an
    /// array, `Vec` or slice of keys; the result is an array for a tuple or
    /// array of keys, a `Vec` for a `Vec` or slice. `Vs` may also be left to
    /// inference from the binding.
    pub async fn get_multi<Vs, Ks: Values<Vs>>(&self, keys: Ks) -> Result<Ks::Output> {
        let ks = keys.keys();
        let values = wire::decode_values(
            self.call(Op::Get, wire::encode_keys(ks.iter().copied()))
                .await?,
        )?;
        expect_count(&values, ks.len())?;
        Ks::decode(values)
    }

    /// Store one key/value pair.
    pub async fn set<V: Serialize>(&self, key: impl AsRef<[u8]>, value: V) -> Result<()> {
        self.set_multi([(key, value)]).await
    }

    /// Store many key/value pairs from any iterator of `(key, value)`.
    pub async fn set_multi<K, V, I>(&self, entries: I) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Serialize,
        I: IntoIterator<Item = (K, V)>,
    {
        let entries = entries
            .into_iter()
            .map(|(k, v)| Ok((k, encode(&v)?)))
            .collect::<Result<Vec<(K, Vec<u8>)>>>()?;
        self.call(
            Op::Set,
            wire::encode_entries(entries.iter().map(|(k, v)| (k.as_ref(), v.as_slice()))),
        )
        .await?;
        Ok(())
    }

    /// Delete one key; returns whether it existed.
    pub async fn del(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        Ok(self.del_multi((key,)).await?[0])
    }

    /// Delete many keys; returns whether each one existed, in an array for
    /// a tuple or array of keys and a `Vec` for a `Vec` or slice.
    pub async fn del_multi<Ks: Keys>(&self, keys: Ks) -> Result<Ks::Batch<bool>> {
        let ks = keys.keys();
        let flags = wire::decode_flags(
            self.call(Op::Del, wire::encode_keys(ks.iter().copied()))
                .await?,
        )?;
        expect_count(&flags, ks.len())?;
        Ks::batch(flags)
    }
}

/// MessagePack with structs as maps, so values round-trip with the
/// TypeScript client.
fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    Ok(rmp_serde::to_vec_named(value)?)
}

fn decode<V: DeserializeOwned>(value: Option<Bytes>) -> Result<Option<V>> {
    Ok(value.map(|v| rmp_serde::from_slice(&v)).transpose()?)
}

/// What `get_multi::<Vs, _>` over this key batch returns: `(Option<V1>, …)`
/// for a tuple of keys with `Vs = (V1, …)`, `[Option<V>; N]` or
/// `Vec<Option<V>>` for an array, `Vec` or slice of keys with `Vs = V`.
pub trait Values<Vs>: Keys {
    type Output;
    fn decode(values: Vec<Option<Bytes>>) -> Result<Self::Output>;
}

macro_rules! impl_value_tuples {
    ($($n:literal => ($($i:tt $K:ident $V:ident),+);)+) => {$(
        impl<$($K: AsRef<[u8]>, $V: DeserializeOwned),+> Values<($($V,)+)> for ($($K,)+) {
            type Output = ($(Option<$V>,)+);
            fn decode(values: Vec<Option<Bytes>>) -> Result<Self::Output> {
                expect_count(&values, $n)?;
                let mut it = values.into_iter();
                Ok(($(decode::<$V>(it.next().unwrap())?,)+))
            }
        }
    )+};
}

impl_value_tuples! {
    1 => (0 K0 V0);
    2 => (0 K0 V0, 1 K1 V1);
    3 => (0 K0 V0, 1 K1 V1, 2 K2 V2);
    4 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3);
    5 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4);
    6 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5);
    7 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5, 6 K6 V6);
    8 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5, 6 K6 V6, 7 K7 V7);
    9 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5, 6 K6 V6, 7 K7 V7, 8 K8 V8);
    10 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5, 6 K6 V6, 7 K7 V7, 8 K8 V8, 9 K9 V9);
    11 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5, 6 K6 V6, 7 K7 V7, 8 K8 V8, 9 K9 V9, 10 K10 V10);
    12 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5, 6 K6 V6, 7 K7 V7, 8 K8 V8, 9 K9 V9, 10 K10 V10, 11 K11 V11);
    13 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5, 6 K6 V6, 7 K7 V7, 8 K8 V8, 9 K9 V9, 10 K10 V10, 11 K11 V11, 12 K12 V12);
    14 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5, 6 K6 V6, 7 K7 V7, 8 K8 V8, 9 K9 V9, 10 K10 V10, 11 K11 V11, 12 K12 V12, 13 K13 V13);
    15 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5, 6 K6 V6, 7 K7 V7, 8 K8 V8, 9 K9 V9, 10 K10 V10, 11 K11 V11, 12 K12 V12, 13 K13 V13, 14 K14 V14);
    16 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5, 6 K6 V6, 7 K7 V7, 8 K8 V8, 9 K9 V9, 10 K10 V10, 11 K11 V11, 12 K12 V12, 13 K13 V13, 14 K14 V14, 15 K15 V15);
}

impl<K: AsRef<[u8]>, V: DeserializeOwned, const N: usize> Values<V> for [K; N] {
    type Output = [Option<V>; N];
    fn decode(values: Vec<Option<Bytes>>) -> Result<Self::Output> {
        Self::batch(values.into_iter().map(decode).collect::<Result<_>>()?)
    }
}

impl<K: AsRef<[u8]>, V: DeserializeOwned> Values<V> for Vec<K> {
    type Output = Vec<Option<V>>;
    fn decode(values: Vec<Option<Bytes>>) -> Result<Self::Output> {
        values.into_iter().map(decode).collect()
    }
}

impl<K: AsRef<[u8]>, V: DeserializeOwned> Values<V> for &[K] {
    type Output = Vec<Option<V>>;
    fn decode(values: Vec<Option<Bytes>>) -> Result<Self::Output> {
        values.into_iter().map(decode).collect()
    }
}

/// Writes queued requests, coalescing everything already queued into one
/// flush. After `keepalive` without a write it sends a PING whose reply is
/// discarded, so the server's idle timeout never fires on a quiet client.
/// Stops as soon as the reader does (`stop` resolves when the reader
/// drops its end), so the socket is closed the moment the connection is
/// known to be gone rather than at the next write.
async fn write_loop<W: AsyncWrite + Unpin>(
    mut w: W,
    mut rx: mpsc::Receiver<Request>,
    pending: mpsc::UnboundedSender<Reply>,
    keepalive: Duration,
    mut stop: oneshot::Receiver<()>,
    lost: Arc<OnceLock<Lost>>,
) {
    // Request bodies were just encoded and are hot; copy them into the
    // coalescing buffer unless they are huge.
    let mut out = FrameWriter::with_inline_limit(BUF);
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
            Some(mut msg) => loop {
                let (op, body, reply) = msg;
                if pending.send(reply).is_err() {
                    return;
                }
                out.frame(op as u8, body);
                match rx.try_recv() {
                    Ok(next) => msg = next,
                    Err(_) => break,
                }
            },
            None => {
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
        if out.flush(&mut w).await.is_err() {
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
    mut pending: mpsc::UnboundedReceiver<Reply>,
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
                        Some(Status::Ok) => Ok(body),
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
    let _ = lost.set(why);
}
