//! TCP client for oxicache. One [`Client`] owns one connection and is cheap
//! to clone; calls from any number of tasks are pipelined onto it and matched
//! to responses in order. Keys are bytes; what values are is fixed at
//! [`Client::connect`] by the format it is given: [`Raw`] for bytes, or
//! (with the `serde` feature) any serde type through a
//! [`Format`](typed::Format) — the same method names, the value types
//! picked by the caller.

use std::future::{Future, IntoFuture};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use oxicache_wire::io::{BUF, FrameReader, FrameWriter};
use oxicache_wire::{self as wire, Op, Status};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

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
    /// The [`Format`](typed::Format) failed to encode a value.
    #[cfg(feature = "serde")]
    #[error("serialize: {0}")]
    Serialize(#[source] BoxError),
    /// The [`Format`](typed::Format) failed to decode a value.
    #[cfg(feature = "serde")]
    #[error("deserialize: {0}")]
    Deserialize(#[source] BoxError),
    #[error("server answered {got} values for {expected} keys")]
    Count { expected: usize, got: usize },
}

/// Error type a [`Format`](typed::Format) reports with.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;

/// Largest response body accepted from the server.
const MAX_FRAME: usize = wire::MAX_FRAME;

type Reply = oneshot::Sender<Result<Bytes>>;

/// One connection. `F` says what values are: [`Raw`] bytes, or (with the
/// `serde` feature) anything a [`Format`](typed::Format) can encode.
#[derive(Clone)]
pub struct Client<F> {
    tx: mpsc::Sender<(Op, Bytes, Reply)>,
    _conn: Arc<Connection>,
    format: F,
}

/// Values as they are: `Bytes` out, `AsRef<[u8]>` in.
#[derive(Clone, Copy, Debug, Default)]
pub struct Raw;

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

/// Aborts the I/O tasks when the last clone is dropped.
struct Connection {
    writer: tokio::task::JoinHandle<()>,
    reader: tokio::task::JoinHandle<()>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.writer.abort();
        self.reader.abort();
    }
}

impl<F> Client<F> {
    /// Connect and, if `token` is given, authenticate before returning.
    pub async fn connect_with_token(
        addr: SocketAddr,
        format: F,
        token: Option<&[u8]>,
    ) -> Result<Self> {
        let client = Self::connect(addr, format).await?;
        if let Some(t) = token {
            client.auth(t).await?;
        }
        Ok(client)
    }

    /// Connect; `format` says what values are — [`Raw`] bytes, or any
    /// [`Format`](typed::Format) with the `serde` feature. A client cannot
    /// exist without one.
    pub async fn connect(addr: SocketAddr, format: F) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (r, w) = stream.into_split();
        let (tx, rx) = mpsc::channel::<(Op, Bytes, Reply)>(1024);
        let (pending_tx, pending_rx) = mpsc::unbounded_channel::<Reply>();
        let writer = tokio::spawn(write_loop(w, rx, pending_tx));
        let reader = tokio::spawn(read_loop(r, pending_rx));
        Ok(Self {
            tx,
            _conn: Arc::new(Connection { writer, reader }),
            format,
        })
    }

    pub fn format(&self) -> &F {
        &self.format
    }

    /// Present the server's shared secret. Required once per connection when
    /// the server was started with a token; a no-op otherwise.
    pub async fn auth(&self, token: &[u8]) -> Result<()> {
        self.call(Op::Auth, Bytes::copy_from_slice(token)).await?;
        Ok(())
    }

    async fn call(&self, op: Op, body: Bytes) -> Result<Bytes> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send((op, body, reply))
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Fetch many keys: one slot per key in request order, `None` for a
    /// missing one; an array for a tuple or array of keys, a `Vec` for a
    /// `Vec` or slice. Awaited directly it yields bytes; with the `serde`
    /// feature, `.decode::<Vs>()` on a format client yields values.
    pub fn get_multi<Ks: Keys>(&self, keys: Ks) -> GetMulti<'_, F, Ks> {
        GetMulti { client: self, keys }
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

/// A pending `get_multi`; see [`Client::get_multi`].
#[must_use = "a request is only sent when awaited or decoded"]
pub struct GetMulti<'a, F, Ks> {
    client: &'a Client<F>,
    keys: Ks,
}

impl<F, Ks: Keys> GetMulti<'_, F, Ks> {
    /// Send the request; the reply is one `Option<Bytes>` per key.
    pub(crate) async fn fetch(&self) -> Result<Vec<Option<Bytes>>> {
        let ks = self.keys.keys();
        let values = wire::decode_values(
            self.client
                .call(Op::Get, wire::encode_keys(ks.iter().copied()))
                .await?,
        )?;
        expect_count(&values, ks.len())?;
        Ok(values)
    }
}

impl<'a, F: Sync, Ks: Keys + Send + Sync + 'a> IntoFuture for GetMulti<'a, F, Ks> {
    type Output = Result<Ks::Batch<Option<Bytes>>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { Ks::batch(self.fetch().await?) })
    }
}

impl Client<Raw> {
    /// Fetch one key.
    pub async fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        Ok(self.get_multi((key,)).fetch().await?.pop().flatten())
    }

    /// Store one key/value pair.
    pub async fn set(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        self.set_multi([(key, value)]).await
    }

    /// Store many key/value pairs.
    pub async fn set_multi<K, V, I>(&self, entries: I) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
        I: IntoIterator<Item = (K, V)>,
    {
        let entries: Vec<(K, V)> = entries.into_iter().collect();
        self.call(
            Op::Set,
            wire::encode_entries(entries.iter().map(|(k, v)| (k.as_ref(), v.as_ref()))),
        )
        .await?;
        Ok(())
    }
}

#[cfg(feature = "serde")]
pub mod typed;
#[cfg(feature = "serde")]
pub use typed::{Format, Values};

/// Writes queued requests, coalescing everything already queued into one flush.
async fn write_loop<W: AsyncWrite + Unpin>(
    mut w: W,
    mut rx: mpsc::Receiver<(Op, Bytes, Reply)>,
    pending: mpsc::UnboundedSender<Reply>,
) {
    // Request bodies were just encoded and are hot; copy them into the
    // coalescing buffer unless they are huge.
    let mut out = FrameWriter::with_inline_limit(BUF);
    while let Some(mut msg) = rx.recv().await {
        loop {
            let (op, body, reply) = msg;
            if pending.send(reply).is_err() {
                return;
            }
            out.frame(op as u8, body);
            match rx.try_recv() {
                Ok(next) => msg = next,
                Err(_) => break,
            }
        }
        if out.flush(&mut w).await.is_err() {
            return;
        }
    }
}

/// Reads responses and hands each to the oldest pending caller.
async fn read_loop<R: AsyncRead + Unpin>(mut r: R, mut pending: mpsc::UnboundedReceiver<Reply>) {
    let mut reader = FrameReader::new(MAX_FRAME);
    loop {
        loop {
            match reader.next_buffered_owned() {
                Ok(Some((status, body))) => {
                    // The writer queues a reply before it sends the request,
                    // so a response with no reply waiting is unsolicited: the
                    // stream is desynchronised and matching can't be trusted.
                    let Ok(reply) = pending.try_recv() else {
                        return; // dropping `pending` fails every queued caller with Closed
                    };
                    let res = match Status::from_u8(status) {
                        Some(Status::Ok) => Ok(body),
                        Some(status) => Err(Error::Status {
                            status,
                            message: String::from_utf8_lossy(&body).into_owned(),
                        }),
                        None => Err(Error::InvalidStatus(status)),
                    };
                    let fatal = matches!(res, Err(Error::InvalidStatus(_)));
                    let _ = reply.send(res);
                    if fatal {
                        return;
                    }
                }
                Ok(None) => break,
                Err(wire::io::FrameTooLarge(n)) => {
                    // Tell the caller why instead of a bare `Closed`; the
                    // stream is desynchronised past this point, so stop.
                    if let Ok(reply) = pending.try_recv() {
                        let _ = reply.send(Err(Error::ResponseTooLarge(n)));
                    }
                    return;
                }
            }
        }
        match reader.fill(&mut r).await {
            Ok(true) => {}
            _ => return,
        }
    }
}
