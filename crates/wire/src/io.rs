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
        debug_assert!(self.buf.len() >= self.lent);
        // SAFETY: `head` returned `Some` only after checking that the buffer
        // holds at least `HEADER_LEN + len` bytes.
        unsafe {
            Ok(Some((
                *self.buf.get_unchecked(0),
                self.buf.get_unchecked(HEADER_LEN..self.lent),
            )))
        }
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
        // SAFETY: at least `HEADER_LEN` bytes are buffered (checked above).
        let (_, len) = decode_header(unsafe { &*(self.buf.as_ptr() as *const [u8; HEADER_LEN]) });
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
    pieces: Vec<Piece>,
    inline_limit: usize,
    /// Bytes queued in `pieces` and `chunk` together.
    len: usize,
}

/// A spilled coalescing buffer stays mutable so a [`Mark`] inside it can
/// still be patched; a referenced body is shared and never touched.
enum Piece {
    Own(BytesMut),
    Shared(Bytes),
}

impl std::ops::Deref for Piece {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        match self {
            Piece::Own(b) => b,
            Piece::Shared(b) => b,
        }
    }
}

/// Where a frame whose length was not known up front begins; see
/// [`FrameWriter::begin`].
#[derive(Clone, Copy)]
pub struct Mark {
    piece: usize,
    offset: usize,
    len: usize,
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
            len: 0,
        }
    }

    /// Start a frame whose body will total `len` bytes.
    #[inline]
    pub fn header(&mut self, tag: u8, len: usize) {
        self.put_slice(&encode_header(tag, len));
    }

    /// Start a frame whose body length is only known once it is written:
    /// the header goes out with a placeholder that [`end`](Self::end)
    /// patches, so the body can be written as it is produced.
    #[inline]
    pub fn begin(&mut self, tag: u8) -> Mark {
        // The header must not straddle a spill; make room first.
        if self.chunk.len() + HEADER_LEN > BUF && !self.chunk.is_empty() {
            self.spill();
        }
        let mark = Mark {
            piece: self.pieces.len(),
            offset: self.chunk.len(),
            len: self.len,
        };
        self.header(tag, 0);
        mark
    }

    /// Bytes written since `mark`'s header.
    #[inline]
    pub fn since(&self, mark: Mark) -> usize {
        self.len - mark.len - HEADER_LEN
    }

    /// Close the frame begun at `mark` with what has been written since.
    pub fn end(&mut self, mark: Mark) {
        let len = (self.since(mark) as u32).to_le_bytes();
        let at = mark.offset + 1;
        if mark.piece == self.pieces.len() {
            self.chunk[at..at + 4].copy_from_slice(&len);
        } else {
            match &mut self.pieces[mark.piece] {
                Piece::Own(b) => b[at..at + 4].copy_from_slice(&len),
                Piece::Shared(_) => unreachable!("a mark is always in an owned piece"),
            }
        }
    }

    /// Drop everything written since `mark`, header included; the writer is
    /// back where it was before [`begin`](Self::begin).
    pub fn abort(&mut self, mark: Mark) {
        if mark.piece < self.pieces.len() {
            self.pieces.truncate(mark.piece + 1);
            let Some(Piece::Own(mut b)) = self.pieces.pop() else {
                unreachable!("a mark is always in an owned piece")
            };
            b.truncate(mark.offset);
            self.chunk = b;
        } else {
            self.chunk.truncate(mark.offset);
        }
        self.len = mark.len;
    }

    #[inline]
    fn spill(&mut self) {
        self.pieces.push(Piece::Own(self.chunk.split()));
        self.chunk.reserve(BUF);
    }

    /// Append body bytes by copy into the coalescing buffer. The buffer is
    /// bounded at [`BUF`]: a full one is spilled to the piece list rather than
    /// grown, so the allocation stays small and warm.
    #[inline]
    pub fn put_slice(&mut self, b: &[u8]) {
        if self.chunk.len() + b.len() > BUF && !self.chunk.is_empty() {
            self.spill();
        }
        self.chunk.put_slice(b);
        self.len += b.len();
    }

    /// Append body bytes by reference; large ones are written straight from `b`.
    pub fn put_bytes(&mut self, b: Bytes) {
        if b.len() < self.inline_limit {
            self.put_slice(&b);
        } else {
            if !self.chunk.is_empty() {
                self.pieces.push(Piece::Own(self.chunk.split()));
            }
            self.len += b.len();
            self.pieces.push(Piece::Shared(b));
        }
    }

    /// Append a whole frame with an owned body.
    pub fn frame(&mut self, tag: u8, body: Bytes) {
        self.header(tag, body.len());
        self.put_bytes(body);
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Bytes queued.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Write everything queued so far.
    pub async fn flush<W: AsyncWrite + Unpin>(&mut self, w: &mut W) -> io::Result<()> {
        if !self.chunk.is_empty() {
            self.pieces.push(Piece::Own(self.chunk.split()));
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
        self.len = 0;
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
        self.len = 0;
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
        tokio::io::AsyncWriteExt::write_all(&mut a, &[0u8; 16])
            .await
            .unwrap();
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

    #[test]
    fn writer_spills_a_full_chunk() {
        let mut w = FrameWriter::default();
        assert!(w.is_empty());
        w.header(1, BUF + 8);
        w.put_slice(&vec![1u8; BUF]);
        assert!(!w.is_empty());
        assert_eq!(w.pieces.len(), 1, "a chunk that would overflow is spilled");
        w.put_slice(&[2u8; 8]);
        let out = w.take();
        assert_eq!(out.len(), HEADER_LEN + BUF + 8);
        assert_eq!(&out[HEADER_LEN + BUF..], &[2u8; 8]);
        assert!(w.is_empty());
    }

    /// A frame begun without a known length is patched when it ends, even
    /// when its header was spilled to the piece list or a referenced body
    /// split the chunk in between; aborting leaves no trace of it.
    #[test]
    fn marks_patch_and_abort_across_spills() {
        let mut w = FrameWriter::with_inline_limit(16);
        w.frame(9, Bytes::from_static(b"before"));
        let m = w.begin(1);
        w.put_slice(b"ab");
        w.put_bytes(Bytes::from(vec![7u8; 32])); // referenced: splits the chunk
        w.put_slice(&vec![3u8; BUF]); // spills
        w.put_slice(b"cd");
        assert_eq!(w.since(m), 2 + 32 + BUF + 2);
        w.end(m);
        let m2 = w.begin(2);
        w.put_slice(b"dropped");
        w.abort(m2);
        w.frame(9, Bytes::from_static(b"after"));
        let out = w.take();
        let frames = |mut b: &[u8]| {
            let mut v = Vec::new();
            while !b.is_empty() {
                let (tag, len) = decode_header(b[..HEADER_LEN].try_into().unwrap());
                v.push((tag, b[HEADER_LEN..HEADER_LEN + len].to_vec()));
                b = &b[HEADER_LEN + len..];
            }
            v
        };
        let f = frames(&out);
        assert_eq!(f.len(), 3);
        assert_eq!((f[0].0, &f[0].1[..]), (9, &b"before"[..]));
        assert_eq!(f[1].0, 1);
        assert_eq!(f[1].1.len(), 2 + 32 + BUF + 2);
        assert_eq!(&f[1].1[..2], b"ab");
        assert_eq!(&f[1].1[f[1].1.len() - 2..], b"cd");
        assert_eq!((f[2].0, &f[2].1[..]), (9, &b"after"[..]));
        // Aborting a frame whose header is still in the chunk.
        let mut w = FrameWriter::new();
        let m = w.begin(1);
        w.put_slice(b"x");
        w.abort(m);
        assert!(w.is_empty());
        assert!(w.take().is_empty());
    }

    #[tokio::test]
    async fn flush_reports_write_zero() {
        let mut w = FrameWriter::new();
        w.frame(1, Bytes::from_static(b"x"));
        // A full cursor accepts nothing: every write returns zero.
        let mut full = io::Cursor::new(&mut [][..]);
        let err = w.flush(&mut full).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WriteZero);
    }

    #[test]
    fn allocator_tuning_is_harmless() {
        tune_allocator();
        tune_allocator();
        let v = vec![7u8; 1 << 20];
        assert_eq!(v[v.len() - 1], 7);
    }
}
