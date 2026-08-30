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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use oxicache_wire::io::{BUF, FrameReader, FrameWriter};
use oxicache_wire::{self as wire, Op, Status};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

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
}

pub type Result<T> = std::result::Result<T, Error>;

/// Largest response body accepted from the server.
const MAX_FRAME: usize = wire::MAX_FRAME;

type Reply = oneshot::Sender<Result<Bytes>>;

/// One connection.
#[derive(Clone)]
pub struct Client {
    tx: mpsc::Sender<(Op, Bytes, Reply)>,
    _conn: Arc<Connection>,
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

impl Client {
    /// Connect and authenticate with `token` before returning; a wrong
    /// token is an [`Error::Status`] with `Unauthorized`.
    pub async fn connect(addr: SocketAddr, token: impl AsRef<[u8]>) -> Result<Self> {
        Self::connect_with(addr, token, wire::KEEPALIVE).await
    }

    /// [`connect`](Self::connect) with a heartbeat interval other than
    /// [`KEEPALIVE`](wire::KEEPALIVE): after `keepalive` without a write the
    /// client pings so the server's idle timeout does not close it. Only
    /// tests against a server with a short idle timeout need this.
    #[doc(hidden)]
    pub async fn connect_with(
        addr: SocketAddr,
        token: impl AsRef<[u8]>,
        keepalive: Duration,
    ) -> Result<Self> {
        assert!(!keepalive.is_zero(), "a zero keepalive would ping non-stop");
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (r, w) = stream.into_split();
        let (tx, rx) = mpsc::channel::<(Op, Bytes, Reply)>(1024);
        let (pending_tx, pending_rx) = mpsc::unbounded_channel::<Reply>();
        let writer = tokio::spawn(write_loop(w, rx, pending_tx, keepalive));
        let reader = tokio::spawn(read_loop(r, pending_rx));
        let client = Self {
            tx,
            _conn: Arc::new(Connection { writer, reader }),
        };
        client
            .call(Op::Auth, Bytes::copy_from_slice(token.as_ref()))
            .await?;
        Ok(client)
    }

    async fn call(&self, op: Op, body: Bytes) -> Result<Bytes> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send((op, body, reply))
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
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
async fn write_loop<W: AsyncWrite + Unpin>(
    mut w: W,
    mut rx: mpsc::Receiver<(Op, Bytes, Reply)>,
    pending: mpsc::UnboundedSender<Reply>,
    keepalive: Duration,
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
            return;
        }
        idle.as_mut().reset(Instant::now() + keepalive);
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
