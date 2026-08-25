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
}

pub type Result<T> = std::result::Result<T, Error>;

/// Largest response body accepted from the server.
const MAX_FRAME: usize = 64 << 20;

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
                    let Some(reply) = pending.recv().await else {
                        return;
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
                        return; // dropping `pending` fails every queued caller with Closed
                    }
                }
                Ok(None) => break,
                Err(_) => return,
            }
        }
        match reader.fill(&mut r).await {
            Ok(true) => {}
            _ => return,
        }
    }
}
