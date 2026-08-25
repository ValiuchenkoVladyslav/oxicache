//! TCP client for oxicache. One [`Client`] owns one connection and is cheap
//! to clone; calls from any number of tasks are pipelined onto it and matched
//! to responses in order.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use oxicache_wire::{self as wire, HEADER_LEN, Op, Status};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
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
}

pub type Result<T> = std::result::Result<T, Error>;

const BUF: usize = 64 << 10;

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
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (r, w) = stream.into_split();
        let (tx, rx) = mpsc::channel::<(Op, Bytes, Reply)>(1024);
        let (pending_tx, pending_rx) = mpsc::unbounded_channel::<Reply>();
        let writer = tokio::spawn(write_loop(BufWriter::with_capacity(BUF, w), rx, pending_tx));
        let reader = tokio::spawn(read_loop(BufReader::with_capacity(BUF, r), pending_rx));
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

    /// Fetch many keys; the result has one slot per key in request order.
    pub async fn get<'a, I>(&self, keys: I) -> Result<Vec<Option<Bytes>>>
    where
        I: IntoIterator<Item = &'a [u8]>,
        I::IntoIter: ExactSizeIterator + Clone,
    {
        Ok(wire::decode_values(
            self.call(Op::Get, wire::encode_keys(keys)).await?,
        )?)
    }

    /// Store many key/value pairs.
    pub async fn set<'a, I>(&self, entries: I) -> Result<()>
    where
        I: IntoIterator<Item = (&'a [u8], &'a [u8])>,
        I::IntoIter: ExactSizeIterator + Clone,
    {
        self.call(Op::Set, wire::encode_entries(entries)).await?;
        Ok(())
    }

    /// Delete many keys; returns whether each one existed.
    pub async fn del<'a, I>(&self, keys: I) -> Result<Vec<bool>>
    where
        I: IntoIterator<Item = &'a [u8]>,
        I::IntoIter: ExactSizeIterator + Clone,
    {
        Ok(wire::decode_flags(
            self.call(Op::Del, wire::encode_keys(keys)).await?,
        )?)
    }
}

/// Writes queued requests, coalescing everything already queued into one flush.
async fn write_loop<W: AsyncWriteExt + Unpin>(
    mut w: W,
    mut rx: mpsc::Receiver<(Op, Bytes, Reply)>,
    pending: mpsc::UnboundedSender<Reply>,
) {
    while let Some(mut msg) = rx.recv().await {
        loop {
            let (op, body, reply) = msg;
            if pending.send(reply).is_err() {
                return;
            }
            let ok = w
                .write_all(&wire::encode_header(op as u8, body.len()))
                .await
                .is_ok()
                && (body.is_empty() || w.write_all(&body).await.is_ok());
            if !ok {
                return;
            }
            match rx.try_recv() {
                Ok(next) => msg = next,
                Err(_) => break,
            }
        }
        if w.flush().await.is_err() {
            return;
        }
    }
}

/// Reads responses and hands each to the oldest pending caller.
async fn read_loop<R: AsyncReadExt + Unpin>(mut r: R, mut pending: mpsc::UnboundedReceiver<Reply>) {
    let mut hdr = [0u8; HEADER_LEN];
    while let Some(reply) = pending.recv().await {
        let res = async {
            r.read_exact(&mut hdr).await?;
            let (status, len) = wire::decode_header(&hdr);
            let mut body = BytesMut::zeroed(len);
            r.read_exact(&mut body).await?;
            let body = body.freeze();
            match Status::from_u8(status) {
                Some(Status::Ok) => Ok(body),
                Some(status) => Err(Error::Status {
                    status,
                    message: String::from_utf8_lossy(&body).into_owned(),
                }),
                None => Err(Error::InvalidStatus(status)),
            }
        }
        .await;
        let fatal = matches!(res, Err(Error::Io(_) | Error::InvalidStatus(_)));
        let _ = reply.send(res);
        if fatal {
            return; // dropping `pending` fails every queued caller with Closed
        }
    }
}
