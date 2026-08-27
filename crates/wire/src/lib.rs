//! Binary wire format shared by the oxicache server and client.
//!
//! Transport is a plain TCP stream carrying length-prefixed frames. Requests
//! on one connection are answered in order, so clients may pipeline freely.
//! All integers are little-endian. Values are opaque byte strings; the format
//! never inspects them.
//!
//! ```text
//! request     := u8 op, u32 len, len bytes of body
//! response    := u8 status, u32 len, len bytes of body
//!
//! keys        := u32 count, count × (u32 len, len bytes)
//! entries     := u32 count, count × (u32 klen, key, u32 vlen, value)
//!
//! op GET(1)   body: keys      -> u32 count, count × (u8 0 | u8 1, u32 len, value)
//! op SET(2)   body: entries   -> empty
//! op DEL(3)   body: keys      -> u32 count, count × u8 found
//! op AUTH(4)  body: token     -> empty
//! ```
//!
//! A non-OK status carries a UTF-8 message as its body. When the server is
//! started with a token, AUTH must be the first request on a connection;
//! any other request before a successful AUTH is answered with
//! `Unauthorized` and the connection is closed.

use bytes::{Buf, BufMut, Bytes, BytesMut};

pub mod cli;
pub mod io;

/// Size of a request or response frame header.
pub const HEADER_LEN: usize = 5;

/// Request operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Op {
    Get = 1,
    Set = 2,
    Del = 3,
    Auth = 4,
}

impl Op {
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Op::Get),
            2 => Some(Op::Set),
            3 => Some(Op::Del),
            4 => Some(Op::Auth),
            _ => None,
        }
    }
}

/// Response status.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Status {
    Ok = 0,
    BadRequest = 1,
    UnknownOp = 2,
    TooLarge = 3,
    Unauthorized = 4,
}

impl Status {
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            0 => Some(Status::Ok),
            1 => Some(Status::BadRequest),
            2 => Some(Status::UnknownOp),
            3 => Some(Status::TooLarge),
            4 => Some(Status::Unauthorized),
            _ => None,
        }
    }
}

/// Encode a frame header (request `op` or response `status` as the tag byte).
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

/// Decode a frame header into its tag byte and body length.
#[inline]
pub fn decode_header(h: &[u8; HEADER_LEN]) -> (u8, usize) {
    (h[0], u32::from_le_bytes([h[1], h[2], h[3], h[4]]) as usize)
}

/// Error returned while decoding a malformed frame.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("unexpected end of frame: needed {needed} more bytes")]
    Truncated { needed: usize },
    #[error("invalid tag byte {0}")]
    InvalidTag(u8),
    #[error("trailing {0} bytes after frame")]
    Trailing(usize),
}

type Result<T> = std::result::Result<T, DecodeError>;

const U32: usize = 4;

#[inline]
fn need(buf: &Bytes, n: usize) -> Result<()> {
    if buf.remaining() < n {
        Err(DecodeError::Truncated {
            needed: n - buf.remaining(),
        })
    } else {
        Ok(())
    }
}

#[inline]
fn get_u32(buf: &mut Bytes) -> Result<u32> {
    need(buf, U32)?;
    Ok(buf.get_u32_le())
}

#[inline]
fn get_blob(buf: &mut Bytes) -> Result<Bytes> {
    let len = get_u32(buf)? as usize;
    need(buf, len)?;
    Ok(buf.split_to(len))
}

#[inline]
fn put_blob(out: &mut BytesMut, b: &[u8]) {
    out.put_u32_le(b.len() as u32);
    out.put_slice(b);
}

fn finish(buf: Bytes) -> Result<()> {
    if buf.is_empty() {
        Ok(())
    } else {
        Err(DecodeError::Trailing(buf.len()))
    }
}

/// Exact encoded size of a list of keys.
pub fn keys_size<'a>(keys: impl IntoIterator<Item = &'a [u8]>) -> usize {
    U32 + keys.into_iter().map(|k| U32 + k.len()).sum::<usize>()
}

/// Encode a key list (body of `/get` and `/del`).
pub fn encode_keys<'a, I>(keys: I) -> Bytes
where
    I: IntoIterator<Item = &'a [u8]>,
    I::IntoIter: ExactSizeIterator + Clone,
{
    let it = keys.into_iter();
    let mut out = BytesMut::with_capacity(keys_size(it.clone()));
    out.put_u32_le(it.len() as u32);
    for k in it {
        put_blob(&mut out, k);
    }
    out.freeze()
}

/// Decode a key list. Slices are zero-copy views into `body`.
pub fn decode_keys(mut body: Bytes) -> Result<Vec<Bytes>> {
    let n = get_u32(&mut body)? as usize;
    let mut keys = Vec::with_capacity(n.min(body.len() / U32));
    for _ in 0..n {
        keys.push(get_blob(&mut body)?);
    }
    finish(body)?;
    Ok(keys)
}

/// Encode key/value entries (body of `/set`).
pub fn encode_entries<'a, I>(entries: I) -> Bytes
where
    I: IntoIterator<Item = (&'a [u8], &'a [u8])>,
    I::IntoIter: ExactSizeIterator + Clone,
{
    let it = entries.into_iter();
    let size = U32
        + it.clone()
            .map(|(k, v)| 2 * U32 + k.len() + v.len())
            .sum::<usize>();
    let mut out = BytesMut::with_capacity(size);
    out.put_u32_le(it.len() as u32);
    for (k, v) in it {
        put_blob(&mut out, k);
        put_blob(&mut out, v);
    }
    out.freeze()
}

/// Decode key/value entries. Slices are zero-copy views into `body`.
pub fn decode_entries(mut body: Bytes) -> Result<Vec<(Bytes, Bytes)>> {
    let n = get_u32(&mut body)? as usize;
    let mut entries = Vec::with_capacity(n.min(body.len() / (2 * U32)));
    for _ in 0..n {
        let k = get_blob(&mut body)?;
        let v = get_blob(&mut body)?;
        entries.push((k, v));
    }
    finish(body)?;
    Ok(entries)
}

/// Borrowed view of an encoded key list. Construction validates the whole
/// body, so iteration cannot fail and yields plain slices of it: no
/// allocation and no refcount traffic per key.
#[derive(Clone)]
pub struct Keys<'a> {
    rest: &'a [u8],
    left: usize,
}

/// Validate and borrow a key list (body of `GET` and `DEL`).
pub fn keys(body: &[u8]) -> Result<Keys<'_>> {
    let (n, mut rest) = split_count(body)?;
    for _ in 0..n {
        rest = skip_blob(rest)?.1;
    }
    if !rest.is_empty() {
        return Err(DecodeError::Trailing(rest.len()));
    }
    Ok(Keys {
        rest: &body[U32..],
        left: n,
    })
}

impl<'a> Iterator for Keys<'a> {
    type Item = &'a [u8];
    #[inline]
    fn next(&mut self) -> Option<&'a [u8]> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let (k, rest) = take_blob(self.rest);
        self.rest = rest;
        Some(k)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }
}
impl ExactSizeIterator for Keys<'_> {}

/// Borrowed view of encoded key/value entries; see [`Keys`].
#[derive(Clone)]
pub struct Entries<'a> {
    rest: &'a [u8],
    left: usize,
}

/// Validate and borrow an entry list (body of `SET`).
pub fn entries(body: &[u8]) -> Result<Entries<'_>> {
    let (n, mut rest) = split_count(body)?;
    for _ in 0..n {
        rest = skip_blob(skip_blob(rest)?.1)?.1;
    }
    if !rest.is_empty() {
        return Err(DecodeError::Trailing(rest.len()));
    }
    Ok(Entries {
        rest: &body[U32..],
        left: n,
    })
}

impl<'a> Iterator for Entries<'a> {
    type Item = (&'a [u8], &'a [u8]);
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let (k, rest) = take_blob(self.rest);
        let (v, rest) = take_blob(rest);
        self.rest = rest;
        Some((k, v))
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }
}
impl ExactSizeIterator for Entries<'_> {}

#[inline]
fn split_count(body: &[u8]) -> Result<(usize, &[u8])> {
    let Some((n, rest)) = body.split_first_chunk::<U32>() else {
        return Err(DecodeError::Truncated {
            needed: U32 - body.len(),
        });
    };
    Ok((u32::from_le_bytes(*n) as usize, rest))
}

/// Validate one length-prefixed blob, returning it and the remainder.
#[inline]
fn skip_blob(b: &[u8]) -> Result<(&[u8], &[u8])> {
    let (len, rest) = split_count(b)?;
    if rest.len() < len {
        return Err(DecodeError::Truncated {
            needed: len - rest.len(),
        });
    }
    Ok(rest.split_at(len))
}

/// Split one blob off an already validated buffer.
#[inline]
fn take_blob(b: &[u8]) -> (&[u8], &[u8]) {
    let (len, rest) = b.split_first_chunk::<U32>().expect("validated");
    rest.split_at(u32::from_le_bytes(*len) as usize)
}

/// Encode a `/get` response: one optional value per requested key.
pub fn encode_values<'a, I>(values: I) -> Bytes
where
    I: IntoIterator<Item = Option<&'a [u8]>>,
    I::IntoIter: ExactSizeIterator + Clone,
{
    let it = values.into_iter();
    let size = U32
        + it.clone()
            .map(|v| 1 + v.map_or(0, |v| U32 + v.len()))
            .sum::<usize>();
    let mut out = BytesMut::with_capacity(size);
    out.put_u32_le(it.len() as u32);
    for v in it {
        match v {
            Some(v) => {
                out.put_u8(1);
                put_blob(&mut out, v);
            }
            None => out.put_u8(0),
        }
    }
    out.freeze()
}

/// Incremental encoder for a `/get` response, for callers that produce values
/// one at a time and want a single output buffer.
pub struct ValuesEncoder {
    out: BytesMut,
    count: u32,
}

impl ValuesEncoder {
    pub fn with_capacity(count: usize, bytes_hint: usize) -> Self {
        let mut out = BytesMut::with_capacity(U32 + count * (1 + U32) + bytes_hint);
        out.put_u32_le(0);
        Self { out, count: 0 }
    }

    #[inline]
    pub fn push(&mut self, value: Option<&[u8]>) {
        match value {
            Some(v) => {
                self.out.put_u8(1);
                put_blob(&mut self.out, v);
            }
            None => self.out.put_u8(0),
        }
        self.count += 1;
    }

    pub fn finish(mut self) -> Bytes {
        self.out[..U32].copy_from_slice(&self.count.to_le_bytes());
        self.out.freeze()
    }
}

/// Decode a `/get` response.
pub fn decode_values(mut body: Bytes) -> Result<Vec<Option<Bytes>>> {
    let n = get_u32(&mut body)? as usize;
    let mut values = Vec::with_capacity(n.min(body.len()));
    for _ in 0..n {
        need(&body, 1)?;
        match body.get_u8() {
            0 => values.push(None),
            1 => values.push(Some(get_blob(&mut body)?)),
            t => return Err(DecodeError::InvalidTag(t)),
        }
    }
    finish(body)?;
    Ok(values)
}

/// Encode a `/del` response: one `found` flag per requested key.
pub fn encode_flags(flags: &[bool]) -> Bytes {
    let mut out = BytesMut::with_capacity(U32 + flags.len());
    out.put_u32_le(flags.len() as u32);
    out.extend(flags.iter().map(|&f| f as u8));
    out.freeze()
}

/// Decode a `/del` response.
pub fn decode_flags(mut body: Bytes) -> Result<Vec<bool>> {
    let n = get_u32(&mut body)? as usize;
    need(&body, n)?;
    let flags = body.split_to(n).iter().map(|&b| b != 0).collect();
    finish(body)?;
    Ok(flags)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn borrowed_iterators_match_owned() {
        let ks: [&[u8]; 3] = [b"a", b"", b"hello world"];
        let enc = encode_keys(ks);
        let it = keys(&enc).unwrap();
        assert_eq!(it.len(), 3);
        assert_eq!(it.collect::<Vec<_>>(), ks);
        let es: [(&[u8], &[u8]); 2] = [(b"k1", b"v1"), (b"k2", &[0u8, 255, 1])];
        let enc = encode_entries(es);
        assert_eq!(entries(&enc).unwrap().collect::<Vec<_>>(), es);
        assert!(matches!(keys(&[1, 0]), Err(DecodeError::Truncated { .. })));
        assert!(matches!(
            keys(&[1, 0, 0, 0, 100, 0, 0, 0]),
            Err(DecodeError::Truncated { needed: 100 })
        ));
        assert!(matches!(
            entries(&[0, 0, 0, 0, 9]),
            Err(DecodeError::Trailing(1))
        ));
    }

    #[test]
    fn keys_roundtrip() {
        let keys: [&[u8]; 3] = [b"a", b"", b"hello world"];
        let enc = encode_keys(keys);
        assert_eq!(enc.len(), keys_size(keys));
        let dec = decode_keys(enc).unwrap();
        assert_eq!(dec, keys.map(Bytes::from_static));
    }

    #[test]
    fn entries_roundtrip() {
        let entries: [(&[u8], &[u8]); 2] = [(b"k1", b"v1"), (b"k2", &[0u8, 255, 1])];
        let dec = decode_entries(encode_entries(entries)).unwrap();
        assert_eq!(dec.len(), 2);
        assert_eq!(&dec[1].1[..], &[0u8, 255, 1]);
    }

    #[test]
    fn values_roundtrip() {
        let vals: [Option<&[u8]>; 3] = [Some(b"x"), None, Some(b"")];
        let dec = decode_values(encode_values(vals)).unwrap();
        assert_eq!(
            dec,
            vec![Some(Bytes::from_static(b"x")), None, Some(Bytes::new())]
        );
    }

    #[test]
    fn values_encoder_matches_encode_values() {
        let vals: [Option<&[u8]>; 3] = [Some(b"x"), None, Some(b"")];
        let mut enc = ValuesEncoder::with_capacity(3, 0);
        for v in vals {
            enc.push(v);
        }
        assert_eq!(enc.finish(), encode_values(vals));
    }

    #[test]
    fn flags_roundtrip() {
        let flags = [true, false, true];
        assert_eq!(decode_flags(encode_flags(&flags)).unwrap(), flags);
    }

    #[test]
    fn empty_frames() {
        assert!(
            decode_keys(encode_keys([] as [&[u8]; 0]))
                .unwrap()
                .is_empty()
        );
        assert!(
            decode_values(encode_values([] as [Option<&[u8]>; 0]))
                .unwrap()
                .is_empty()
        );
        assert!(decode_flags(encode_flags(&[])).unwrap().is_empty());
    }

    #[test]
    fn header_roundtrip() {
        let h = encode_header(Op::Set as u8, 0xdead_beef);
        assert_eq!(decode_header(&h), (2, 0xdead_beef));
        assert_eq!(Op::from_u8(3), Some(Op::Del));
        assert_eq!(Op::from_u8(9), None);
        assert_eq!(Status::from_u8(1), Some(Status::BadRequest));
    }

    #[test]
    fn rejects_malformed() {
        assert!(matches!(
            decode_keys(Bytes::from_static(&[1, 0])),
            Err(DecodeError::Truncated { .. })
        ));
        assert!(matches!(
            decode_keys(Bytes::from_static(&[1, 0, 0, 0, 100, 0, 0, 0])),
            Err(DecodeError::Truncated { needed: 100 })
        ));
        assert_eq!(
            decode_values(Bytes::from_static(&[1, 0, 0, 0, 7])),
            Err(DecodeError::InvalidTag(7))
        );
        assert_eq!(
            decode_flags(Bytes::from_static(&[0, 0, 0, 0, 9])),
            Err(DecodeError::Trailing(1))
        );
    }
}
