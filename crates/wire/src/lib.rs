//! The wire protocol shared by the server and the clients.
//!
//! Every message is a frame: `u8 tag, u32 len, body[len]`, little-endian.
//! A request's tag is its [`Op`], a response's is its [`Status`]. Bodies:
//!
//! ```text
//! op 1 GET   key                          -> Ok, value | NotFound
//! op 2 SET   u32 klen, key, value         -> Ok | TooLarge
//! op 3 DEL   key                          -> Ok (deleted) | NotFound
//! op 4 AUTH  token                        -> Ok | Unauthorized
//! op 5 PING  (ignored)                    -> Ok
//! op 6 BATCH u32 count, count × (u8 op, u32 len, body)
//!            -> Ok, u32 count, count × (u8 status, u32 len, body)
//! ```
//!
//! A batch carries GET, SET and DEL items (at most [`MAX_ITEMS`]) and
//! answers each exactly as the standalone op would, in order; a malformed
//! envelope is `BadRequest` for the whole frame, a bad item is its own
//! status. Error bodies are UTF-8 messages.

use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};

pub mod io;

/// Bytes in a frame header: tag plus length.
pub const HEADER_LEN: usize = 5;

/// Request tags.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Op {
    Get = 1,
    Set = 2,
    Del = 3,
    Auth = 4,
    Ping = 5,
    Batch = 6,
}

impl Op {
    pub fn from_u8(b: u8) -> Option<Self> {
        Some(match b {
            1 => Self::Get,
            2 => Self::Set,
            3 => Self::Del,
            4 => Self::Auth,
            5 => Self::Ping,
            6 => Self::Batch,
            _ => return None,
        })
    }
}

/// Response tags. `Ok` and `NotFound` are answers, the rest are refusals
/// whose body is a message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Status {
    Ok = 0,
    BadRequest = 1,
    UnknownOp = 2,
    TooLarge = 3,
    Unauthorized = 4,
    NotFound = 5,
}

impl Status {
    pub fn from_u8(b: u8) -> Option<Self> {
        Some(match b {
            0 => Self::Ok,
            1 => Self::BadRequest,
            2 => Self::UnknownOp,
            3 => Self::TooLarge,
            4 => Self::Unauthorized,
            5 => Self::NotFound,
            _ => return None,
        })
    }
}

#[inline]
pub fn encode_header(tag: u8, len: usize) -> [u8; HEADER_LEN] {
    debug_assert!(
        len <= u32::MAX as usize,
        "frame body length must fit in u32"
    );
    let mut h = [0u8; HEADER_LEN];
    h[0] = tag;
    h[1..].copy_from_slice(&(len as u32).to_le_bytes());
    h
}

#[inline]
pub fn decode_header(h: &[u8; HEADER_LEN]) -> (u8, usize) {
    (h[0], u32::from_le_bytes([h[1], h[2], h[3], h[4]]) as usize)
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("unexpected end of frame: needed {needed} more bytes")]
    Truncated { needed: usize },
    #[error("trailing {0} bytes after frame")]
    Trailing(usize),
    #[error("{0} items in one batch exceeds the limit of {MAX_ITEMS}")]
    TooMany(usize),
}

type Result<T> = std::result::Result<T, DecodeError>;

const U32: usize = 4;

/// Largest frame body either side accepts.
pub const MAX_FRAME: usize = 64 << 20;
/// Most items in one batch.
pub const MAX_ITEMS: usize = 1 << 16;
/// How long a client waits without a write before it pings the server.
pub const KEEPALIVE: Duration = Duration::from_secs(100);

#[inline]
fn split_count(body: &[u8]) -> Result<(usize, &[u8])> {
    let Some((n, rest)) = body.split_first_chunk::<U32>() else {
        return Err(DecodeError::Truncated {
            needed: U32 - body.len(),
        });
    };
    Ok((u32::from_le_bytes(*n) as usize, rest))
}

#[inline]
fn split_len(b: &[u8], len: usize) -> Result<(&[u8], &[u8])> {
    if b.len() < len {
        return Err(DecodeError::Truncated {
            needed: len - b.len(),
        });
    }
    Ok(b.split_at(len))
}

/// Size of a SET body for `key` and `value`.
#[inline]
pub fn set_size(key: &[u8], value: &[u8]) -> usize {
    U32 + key.len() + value.len()
}

/// Append a SET body to `out`.
#[inline]
pub fn put_set(out: &mut BytesMut, key: &[u8], value: &[u8]) {
    put_set_key(out, key);
    out.put_slice(value);
}

/// Append the `u32 klen, key` prefix of a SET body; the value follows it.
#[inline]
pub fn put_set_key(out: &mut BytesMut, key: &[u8]) {
    out.put_u32_le(key.len() as u32);
    out.put_slice(key);
}

/// A SET body.
pub fn encode_set(key: &[u8], value: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(set_size(key, value));
    put_set(&mut out, key, value);
    out.freeze()
}

/// The key and value of a SET body.
#[inline]
pub fn set_body(body: &[u8]) -> Result<(&[u8], &[u8])> {
    let (klen, rest) = split_count(body)?;
    split_len(rest, klen)
}

/// Builds a BATCH body: items in the order they are added.
pub struct BatchEncoder {
    out: BytesMut,
    count: usize,
}

impl Default for BatchEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchEncoder {
    pub fn new() -> Self {
        // Room for a typical batch of small items without regrowing from
        // nothing; callers that know their size use `with_capacity`.
        Self::with_capacity(512)
    }

    /// With room for `bytes` of items before the buffer grows.
    pub fn with_capacity(bytes: usize) -> Self {
        let mut out = BytesMut::with_capacity(U32 + bytes);
        out.put_u32_le(0);
        Self { out, count: 0 }
    }

    /// Items added so far.
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    #[inline]
    fn item(&mut self, op: Op, len: usize) {
        self.out.put_slice(&encode_header(op as u8, len));
        self.count += 1;
    }

    pub fn get(&mut self, key: &[u8]) {
        self.item(Op::Get, key.len());
        self.out.put_slice(key);
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        self.item(Op::Set, set_size(key, value));
        put_set(&mut self.out, key, value);
    }

    /// A SET item whose value bytes are produced by `value` straight into
    /// the buffer — no intermediate allocation — under a length patched
    /// afterwards. An error rolls the item back completely.
    pub fn set_with<E>(
        &mut self,
        key: &[u8],
        value: impl FnOnce(&mut BytesMut) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        let at = self.out.len();
        self.item(Op::Set, 0);
        put_set_key(&mut self.out, key);
        if let Err(e) = value(&mut self.out) {
            self.out.truncate(at);
            self.count -= 1;
            return Err(e);
        }
        let len = ((self.out.len() - at - HEADER_LEN) as u32).to_le_bytes();
        self.out[at + 1..at + HEADER_LEN].copy_from_slice(&len);
        Ok(())
    }

    pub fn del(&mut self, key: &[u8]) {
        self.item(Op::Del, key.len());
        self.out.put_slice(key);
    }

    /// Append an item that is already encoded as `(op, body)`, exactly as
    /// [`frames`] yields it: what a batch being split across servers
    /// re-emits for each of them.
    pub fn push(&mut self, op: u8, body: &[u8]) {
        self.out.put_slice(&encode_header(op, body.len()));
        self.count += 1;
        self.out.put_slice(body);
    }

    /// The body. Panics past [`MAX_ITEMS`]; check [`len`](Self::len) first.
    pub fn finish(mut self) -> Bytes {
        assert!(
            self.count <= MAX_ITEMS,
            "{} items exceed MAX_ITEMS",
            self.count
        );
        self.out[..U32].copy_from_slice(&(self.count as u32).to_le_bytes());
        self.out.freeze()
    }
}

/// The items of a BATCH body, or the replies of a BATCH response: nested
/// frames, validated once up front so iteration cannot fail.
#[derive(Clone, Debug)]
pub struct Frames<'a> {
    rest: &'a [u8],
    left: usize,
}

/// Validate a `u32 count, count × frame` body and iterate its frames as
/// `(tag, body)`.
pub fn frames(body: &[u8]) -> Result<Frames<'_>> {
    let (n, mut rest) = split_count(body)?;
    if n > MAX_ITEMS {
        return Err(DecodeError::TooMany(n));
    }
    for _ in 0..n {
        let (h, r) = split_len(rest, HEADER_LEN)?;
        let (_, len) = decode_header(h.try_into().expect("HEADER_LEN bytes"));
        rest = split_len(r, len)?.1;
    }
    if !rest.is_empty() {
        return Err(DecodeError::Trailing(rest.len()));
    }
    Ok(Frames {
        rest: &body[U32..],
        left: n,
    })
}

impl<'a> Iterator for Frames<'a> {
    type Item = (u8, &'a [u8]);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        // SAFETY: `frames` walked every header and length once already.
        unsafe {
            let h = &*(self.rest.as_ptr() as *const [u8; HEADER_LEN]);
            let (tag, len) = decode_header(h);
            let rest = self.rest.get_unchecked(HEADER_LEN..);
            debug_assert!(rest.len() >= len);
            let (body, rest) = rest.split_at_unchecked(len);
            self.rest = rest;
            Some((tag, body))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }
}

impl ExactSizeIterator for Frames<'_> {}

/// The replies of a BATCH response as owned `(status, body)` pairs sharing
/// the response's allocation.
pub fn decode_replies(body: Bytes) -> Result<Box<[(u8, Bytes)]>> {
    let base = body.as_ptr() as usize;
    Ok(frames(&body)?
        .map(|(tag, b)| {
            let start = b.as_ptr() as usize - base;
            (tag, body.slice(start..start + b.len()))
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip() {
        let h = encode_header(Op::Batch as u8, 0x01020304);
        assert_eq!(decode_header(&h), (6, 0x01020304));
    }

    #[test]
    fn set_body_roundtrip() {
        let b = encode_set(b"key", b"value bytes");
        assert_eq!(b.len(), set_size(b"key", b"value bytes"));
        assert_eq!(set_body(&b).unwrap(), (&b"key"[..], &b"value bytes"[..]));
        assert_eq!(
            set_body(&encode_set(b"", b"")).unwrap(),
            (&b""[..], &b""[..])
        );
        assert!(matches!(
            set_body(&[1, 0]),
            Err(DecodeError::Truncated { needed: 2 })
        ));
        assert!(matches!(
            set_body(&[9, 0, 0, 0, 1]),
            Err(DecodeError::Truncated { needed: 8 })
        ));
    }

    #[test]
    fn batch_roundtrip() {
        let mut b = BatchEncoder::new();
        assert!(b.is_empty());
        b.get(b"a");
        b.set(b"k", b"v");
        b.del(b"");
        assert_eq!(b.len(), 3);
        let body = b.finish();
        let items: Vec<_> = frames(&body).unwrap().collect();
        assert_eq!(
            items,
            vec![
                (Op::Get as u8, &b"a"[..]),
                (Op::Set as u8, &encode_set(b"k", b"v")[..]),
                (Op::Del as u8, &b""[..]),
            ]
        );
        assert_eq!(frames(&BatchEncoder::new().finish()).unwrap().len(), 0);
    }

    #[test]
    fn push_re_emits_a_parsed_item() {
        let mut src = BatchEncoder::new();
        src.get(b"a");
        src.set(b"k", b"v");
        src.del(b"z");
        let body = src.finish();
        // Split the items over two encoders and read them back.
        let (mut even, mut odd) = (BatchEncoder::new(), BatchEncoder::new());
        for (i, (op, item)) in frames(&body).unwrap().enumerate() {
            if i % 2 == 0 {
                even.push(op, item)
            } else {
                odd.push(op, item)
            }
        }
        assert_eq!((even.len(), odd.len()), (2, 1));
        assert_eq!(
            frames(&even.finish()).unwrap().collect::<Vec<_>>(),
            vec![(Op::Get as u8, &b"a"[..]), (Op::Del as u8, &b"z"[..])]
        );
        assert_eq!(
            frames(&odd.finish()).unwrap().collect::<Vec<_>>(),
            vec![(Op::Set as u8, &encode_set(b"k", b"v")[..])]
        );
    }

    #[test]
    fn set_with_writes_in_place() {
        let mut b = BatchEncoder::new();
        b.get(b"a");
        b.set_with(b"k", |out| {
            out.put_slice(b"value");
            Ok::<_, ()>(())
        })
        .unwrap();
        assert_eq!(b.len(), 2);
        // A failed item leaves no trace.
        assert_eq!(b.set_with(b"bad", |_| Err("no")), Err("no"));
        assert_eq!(b.len(), 2);
        b.del(b"z");
        let body = b.finish();
        let items: Vec<_> = frames(&body).unwrap().collect();
        assert_eq!(
            items,
            vec![
                (Op::Get as u8, &b"a"[..]),
                (Op::Set as u8, &encode_set(b"k", b"value")[..]),
                (Op::Del as u8, &b"z"[..]),
            ]
        );
    }

    #[test]
    fn replies_share_the_buffer() {
        let mut out = BytesMut::new();
        out.put_u32_le(2);
        out.put_slice(&encode_header(Status::Ok as u8, 3));
        out.put_slice(b"abc");
        out.put_slice(&encode_header(Status::NotFound as u8, 0));
        let body = out.freeze();
        let r = decode_replies(body.clone()).unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].0, 0);
        assert_eq!(&r[0].1[..], b"abc");
        assert_eq!(r[0].1.as_ptr(), body[9..].as_ptr(), "no copy");
        assert_eq!(r[1], (5, Bytes::new()));
    }

    #[test]
    fn rejects_malformed() {
        assert!(matches!(
            frames(&[1, 0]),
            Err(DecodeError::Truncated { .. })
        ));
        // One item announced, only part of its header present.
        assert!(matches!(
            frames(&[1, 0, 0, 0, 1, 0]),
            Err(DecodeError::Truncated { needed: 3 })
        ));
        // Item claims 100 bytes, has none.
        assert!(matches!(
            frames(&[1, 0, 0, 0, 1, 100, 0, 0, 0]),
            Err(DecodeError::Truncated { needed: 100 })
        ));
        assert!(matches!(
            frames(&[0, 0, 0, 0, 9]),
            Err(DecodeError::Trailing(1))
        ));
        let mut too_many = ((MAX_ITEMS + 1) as u32).to_le_bytes().to_vec();
        too_many.resize(U32 + HEADER_LEN * (MAX_ITEMS + 1), 0);
        assert_eq!(
            frames(&too_many).unwrap_err(),
            DecodeError::TooMany(MAX_ITEMS + 1)
        );
        assert_eq!(
            DecodeError::TooMany(70000).to_string(),
            "70000 items in one batch exceeds the limit of 65536"
        );
    }

    #[test]
    fn unknown_status_byte() {
        assert_eq!(Status::from_u8(5), Some(Status::NotFound));
        assert_eq!(Status::from_u8(6), None);
        assert_eq!(Op::from_u8(6), Some(Op::Batch));
        assert_eq!(Op::from_u8(7), None);
    }
}
