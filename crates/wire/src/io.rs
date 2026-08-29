//! Zero-copy frame I/O over any async byte stream, shared by server and client.
//!
//! [`FrameReader`] accumulates socket reads in one buffer and hands out
//! frame bodies as slices of it. [`FrameWriter`] coalesces small frames into
//! one buffer, queues large bodies by reference, and flushes everything with
//! vectored writes, so a pipelined burst costs one syscall each way.

use std::io::{self, IoSlice};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{HEADER_LEN, decode_header, encode_header};

/// Read/write buffer size and the granularity of buffer growth.
pub const BUF: usize = 64 << 10;
/// Bodies at or above this size are written by reference instead of copied.
pub const INLINE_BODY: usize = 1024;
const MAX_IOV: usize = 64;

pub struct FrameReader {
    buf: BytesMut,
    max_frame: usize,
    /// Total bytes the frame at the head of the buffer needs, if known.
    need: usize,
    /// Bytes of the frame lent out by the last `next_buffered`, released on
    /// the next call.
    lent: usize,
}

impl FrameReader {
    /// `max_frame` bounds the body length accepted from the peer.
    pub fn new(max_frame: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(BUF),
            max_frame,
            need: 0,
            lent: 0,
        }
    }

    /// Change the accepted body length, e.g. to lift a small pre-authentication
    /// limit once the peer has proven itself.
    pub fn set_max_frame(&mut self, max_frame: usize) {
        self.max_frame = max_frame;
    }

    /// Next complete frame already in the buffer, as `(tag, body)`. The body
    /// borrows the read buffer and is released by the next call on the reader.
    /// `Ok(None)` means more input is needed.
    pub fn next_buffered(&mut self) -> Result<Option<(u8, &[u8])>, FrameTooLarge> {
        self.release();
        let Some(len) = self.head()? else {
            return Ok(None);
        };
        self.lent = HEADER_LEN + len;
        Ok(Some((self.buf[0], &self.buf[HEADER_LEN..self.lent])))
    }

    #[inline]
    fn release(&mut self) {
        self.buf.advance(self.lent);
        self.lent = 0;
    }

    /// Body length of the frame at the head of the buffer, if complete.
    #[inline]
    fn head(&mut self) -> Result<Option<usize>, FrameTooLarge> {
        if self.buf.len() < HEADER_LEN {
            return Ok(None);
        }
        let (_, len) = decode_header(self.buf[..HEADER_LEN].try_into().unwrap());
        if len > self.max_frame {
            return Err(FrameTooLarge(len));
        }
        if self.buf.len() < HEADER_LEN + len {
            self.need = HEADER_LEN + len;
            return Ok(None);
        }
        self.need = 0;
        Ok(Some(len))
    }

    /// Like [`next_buffered`](Self::next_buffered) but the body is copied into
    /// its own allocation, for bodies handed to other tasks.
    pub fn next_buffered_owned(&mut self) -> Result<Option<(u8, Bytes)>, FrameTooLarge> {
        self.release();
        let Some(len) = self.head()? else {
            return Ok(None);
        };
        let tag = self.buf[0];
        self.buf.advance(HEADER_LEN);
        let body = Bytes::copy_from_slice(&self.buf[..len]);
        self.buf.advance(len);
        Ok(Some((tag, body)))
    }

    /// Read more input. Returns `false` at EOF.
    pub async fn fill<R: AsyncRead + Unpin>(&mut self, r: &mut R) -> io::Result<bool> {
        self.release();
        // A buffer grown for one large frame would otherwise stay pinned to
        // an idle connection for its lifetime.
        if self.buf.is_empty() && self.buf.capacity() > 4 * BUF {
            self.buf = BytesMut::with_capacity(BUF);
        }
        // Grow towards a pending large frame geometrically rather than
        // reserving its full claimed length up front: a peer that announces
        // a maximal frame and sends nothing must not pin that much memory.
        let pending = self.need.saturating_sub(self.buf.len());
        let want = pending.min(self.buf.capacity().max(BUF)).max(BUF / 4);
        if self.buf.capacity() - self.buf.len() < want {
            self.buf.reserve(want.max(BUF));
        }
        Ok(r.read_buf(&mut self.buf).await? != 0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("frame body of {0} bytes exceeds the limit")]
pub struct FrameTooLarge(pub usize);

pub struct FrameWriter {
    chunk: BytesMut,
    pieces: Vec<Bytes>,
    inline_limit: usize,
}

impl Default for FrameWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameWriter {
    pub fn new() -> Self {
        Self::with_inline_limit(INLINE_BODY)
    }

    /// Bodies shorter than `limit` are copied into the coalescing buffer;
    /// longer ones are written by reference. Copying is cheaper for data that
    /// is already hot (a freshly encoded request); referencing is cheaper for
    /// data that is not (a cache entry).
    pub fn with_inline_limit(limit: usize) -> Self {
        Self {
            chunk: BytesMut::with_capacity(BUF),
            pieces: Vec::new(),
            inline_limit: limit,
        }
    }

    /// Start a frame whose body will total `len` bytes.
    #[inline]
    pub fn header(&mut self, tag: u8, len: usize) {
        self.chunk.put_slice(&encode_header(tag, len));
    }

    /// Append body bytes by copy into the coalescing buffer. The buffer is
    /// bounded at [`BUF`]: a full one is spilled to the piece list rather than
    /// grown, so the allocation stays small and warm.
    #[inline]
    pub fn put_slice(&mut self, b: &[u8]) {
        if self.chunk.len() + b.len() > BUF && !self.chunk.is_empty() {
            self.pieces.push(self.chunk.split().freeze());
            self.chunk.reserve(BUF);
        }
        self.chunk.put_slice(b);
    }

    /// Append body bytes by reference; large ones are written straight from `b`.
    pub fn put_bytes(&mut self, b: Bytes) {
        if b.len() < self.inline_limit {
            self.put_slice(&b);
        } else {
            if !self.chunk.is_empty() {
                self.pieces.push(self.chunk.split().freeze());
            }
            self.pieces.push(b);
        }
    }

    /// Append a whole frame with an owned body.
    pub fn frame(&mut self, tag: u8, body: Bytes) {
        self.header(tag, body.len());
        self.put_bytes(body);
    }

    pub fn is_empty(&self) -> bool {
        self.chunk.is_empty() && self.pieces.is_empty()
    }

    /// Write everything queued so far.
    pub async fn flush<W: AsyncWrite + Unpin>(&mut self, w: &mut W) -> io::Result<()> {
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
                return Err(io::Error::from(io::ErrorKind::WriteZero));
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
        // Every piece is dropped now, so the chunk's allocation is unique
        // again and `reserve` reclaims it in place instead of allocating.
        self.chunk.reserve(BUF);
        Ok(())
    }

    /// Drain queued output as one contiguous buffer (for tests and callers
    /// that do their own I/O).
    pub fn take(&mut self) -> Vec<u8> {
        let mut v: Vec<u8> = self.pieces.drain(..).flat_map(|p| p.to_vec()).collect();
        v.extend_from_slice(&self.chunk.split());
        v
    }
}

/// Keep freed memory in the process instead of returning it to the kernel.
/// Request/response buffers are allocated and freed in bursts; with glibc's
/// defaults the heap top is trimmed after each burst and page-faulted back
/// in on the next, which makes every copy into a fresh buffer run at
/// page-fault speed. No-op on non-glibc targets.
pub fn tune_allocator() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    // SAFETY: mallopt only adjusts allocator parameters.
    unsafe {
        libc::mallopt(libc::M_TRIM_THRESHOLD, 256 << 20);
        // glibc rejects thresholds above HEAP_MAX_SIZE / 2 (32 MiB on 64-bit)
        // and silently keeps its dynamic default.
        libc::mallopt(libc::M_MMAP_THRESHOLD, 32 << 20);
        libc::mallopt(libc::M_TOP_PAD, 16 << 20);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn roundtrip_through_duplex() {
        let (mut a, mut b) = tokio::io::duplex(1 << 20);
        let big = Bytes::from(vec![5u8; INLINE_BODY * 3]);
        let mut w = FrameWriter::new();
        w.frame(1, Bytes::from_static(b"small"));
        w.frame(2, big.clone());
        w.frame(3, Bytes::new());
        w.flush(&mut a).await.unwrap();

        let mut r = FrameReader::new(1 << 20);
        let mut got = Vec::new();
        while got.len() < 3 {
            while let Some((tag, body)) = r.next_buffered().unwrap() {
                got.push((tag, Bytes::copy_from_slice(body)));
            }
            if got.len() < 3 {
                assert!(r.fill(&mut b).await.unwrap());
            }
        }
        assert_eq!(got[0], (1, Bytes::from_static(b"small")));
        assert_eq!(got[1], (2, big));
        assert_eq!(got[2], (3, Bytes::new()));
    }

    #[tokio::test]
    async fn writer_reuses_its_buffer() {
        let (mut a, mut b) = tokio::io::duplex(1 << 20);
        let mut w = FrameWriter::new();
        w.frame(1, Bytes::from_static(b"x"));
        w.flush(&mut a).await.unwrap();
        let ptr = w.chunk.as_ptr();
        for _ in 0..8 {
            w.frame(1, Bytes::from_static(b"y"));
            w.flush(&mut a).await.unwrap();
            assert_eq!(w.chunk.as_ptr(), ptr, "flush must not reallocate the chunk");
        }
        let mut sink = vec![0u8; 1024];
        let _ = tokio::io::AsyncReadExt::read(&mut b, &mut sink)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn claimed_large_frame_reserves_incrementally() {
        let (mut a, mut b) = tokio::io::duplex(1 << 20);
        let mut r = FrameReader::new(64 << 20);
        tokio::io::AsyncWriteExt::write_all(&mut a, &encode_header(1, 64 << 20))
            .await
            .unwrap();
        assert!(r.fill(&mut b).await.unwrap());
        assert_eq!(r.next_buffered().unwrap(), None);
        tokio::io::AsyncWriteExt::write_all(&mut a, &[0u8; 16]).await.unwrap();
        assert!(r.fill(&mut b).await.unwrap());
        assert!(
            r.buf.capacity() <= 4 * BUF,
            "an unsent frame must not reserve its claimed size: {}",
            r.buf.capacity()
        );
    }

    #[test]
    fn rejects_oversized() {
        let mut r = FrameReader::new(10);
        r.buf.put_slice(&encode_header(1, 11));
        assert_eq!(r.next_buffered(), Err(FrameTooLarge(11)));
    }
}
