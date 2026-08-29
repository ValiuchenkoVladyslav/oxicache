//! TCP client for oxicache. One [`Client`] owns one connection and is cheap
//! to clone; calls from any number of tasks are pipelined onto it and matched
//! to responses in order.

use std::net::SocketAddr;
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
    #[cfg(feature = "serde")]
    #[error("serialize: {0}")]
    Serialize(#[from] rmp_serde::encode::Error),
    #[cfg(feature = "serde")]
    #[error("deserialize: {0}")]
    Deserialize(#[from] rmp_serde::decode::Error),
    #[cfg(feature = "serde")]
    #[error("server answered {got} values for {expected} keys")]
    Count { expected: usize, got: usize },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Largest response body accepted from the server.
const MAX_FRAME: usize = wire::MAX_FRAME;

type Reply = oneshot::Sender<Result<Bytes>>;

#[derive(Clone)]
pub struct Client {
    tx: mpsc::Sender<(Op, Bytes, Reply)>,
    _conn: Arc<Connection>,
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
    /// Connect and, if `token` is given, authenticate before returning.
    pub async fn connect_with_token(addr: SocketAddr, token: Option<&[u8]>) -> Result<Self> {
        let client = Self::connect(addr).await?;
        if let Some(t) = token {
            client.auth(t).await?;
        }
        Ok(client)
    }

    /// Present the server's shared secret. Required once per connection when
    /// the server was started with a token; a no-op otherwise.
    pub async fn auth(&self, token: &[u8]) -> Result<()> {
        self.call(Op::Auth, Bytes::copy_from_slice(token)).await?;
        Ok(())
    }

    pub async fn connect(addr: SocketAddr) -> Result<Self> {
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
        })
    }

    async fn call(&self, op: Op, body: Bytes) -> Result<Bytes> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send((op, body, reply))
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// The byte-level API: keys and values as raw bytes, whatever features
    /// are enabled. Without the `serde` feature `Client`'s own `get`/`set`/
    /// `del` are the same thing.
    pub fn raw(&self) -> Raw<'_> {
        Raw(self)
    }
}

/// Byte-level view of a [`Client`]; see [`Client::raw`].
#[derive(Clone, Copy)]
pub struct Raw<'a>(&'a Client);

impl Raw<'_> {
    /// Fetch one key.
    pub async fn get(self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.get_multi([key]).await?.pop().flatten())
    }

    /// Fetch many keys; the result has one slot per key in request order.
    pub async fn get_multi<'a, I>(self, keys: I) -> Result<Vec<Option<Bytes>>>
    where
        I: IntoIterator<Item = &'a [u8]>,
        I::IntoIter: ExactSizeIterator + Clone,
    {
        Ok(wire::decode_values(
            self.0.call(Op::Get, wire::encode_keys(keys)).await?,
        )?)
    }

    /// Store one key/value pair.
    pub async fn set(self, key: &[u8], value: &[u8]) -> Result<()> {
        self.set_multi([(key, value)]).await
    }

    /// Store many key/value pairs.
    pub async fn set_multi<'a, I>(self, entries: I) -> Result<()>
    where
        I: IntoIterator<Item = (&'a [u8], &'a [u8])>,
        I::IntoIter: ExactSizeIterator + Clone,
    {
        self.0.call(Op::Set, wire::encode_entries(entries)).await?;
        Ok(())
    }

    /// Delete one key; returns whether it existed.
    pub async fn del(self, key: &[u8]) -> Result<bool> {
        Ok(self.del_multi([key]).await?.pop().unwrap_or(false))
    }

    /// Delete many keys; returns whether each one existed.
    pub async fn del_multi<'a, I>(self, keys: I) -> Result<Vec<bool>>
    where
        I: IntoIterator<Item = &'a [u8]>,
        I::IntoIter: ExactSizeIterator + Clone,
    {
        Ok(wire::decode_flags(
            self.0.call(Op::Del, wire::encode_keys(keys)).await?,
        )?)
    }
}

/// Byte-level `get`/`set`/`del` on the client itself; with the `serde`
/// feature these names take any serializable type instead (see [`typed`]).
#[cfg(not(feature = "serde"))]
impl Client {
    /// Fetch one key.
    pub async fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        self.raw().get(key.as_ref()).await
    }

    /// Fetch many keys; the result has one slot per key in request order.
    pub async fn get_multi<'a, I>(&self, keys: I) -> Result<Vec<Option<Bytes>>>
    where
        I: IntoIterator<Item = &'a [u8]>,
        I::IntoIter: ExactSizeIterator + Clone,
    {
        self.raw().get_multi(keys).await
    }

    /// Store one key/value pair.
    pub async fn set(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        self.raw().set(key.as_ref(), value.as_ref()).await
    }

    /// Store many key/value pairs.
    pub async fn set_multi<'a, I>(&self, entries: I) -> Result<()>
    where
        I: IntoIterator<Item = (&'a [u8], &'a [u8])>,
        I::IntoIter: ExactSizeIterator + Clone,
    {
        self.raw().set_multi(entries).await
    }

    /// Delete one key; returns whether it existed.
    pub async fn del(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        self.raw().del(key.as_ref()).await
    }

    /// Delete many keys; returns whether each one existed.
    pub async fn del_multi<'a, I>(&self, keys: I) -> Result<Vec<bool>>
    where
        I: IntoIterator<Item = &'a [u8]>,
        I::IntoIter: ExactSizeIterator + Clone,
    {
        self.raw().del_multi(keys).await
    }
}

#[cfg(feature = "serde")]
pub mod typed;
#[cfg(feature = "serde")]
pub use typed::{Keys, ValuesFor};

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
