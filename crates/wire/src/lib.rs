//! Binary wire format shared by the oxicache server and client.
//!
//! Every request is an HTTP/3 `POST` to one of the [`path`] constants with a
//! body encoded as below. All integers are little-endian. Values are opaque
//! byte strings; the format never inspects them.
//!
//! ```text
//! keys        := u32 count, count × (u32 len, len bytes)
//! entries     := u32 count, count × (u32 klen, key, u32 vlen, value)
//!
//! POST /get   body: keys      -> u32 count, count × (u8 0 | u8 1, u32 len, value)
//! POST /set   body: entries   -> empty
//! POST /del   body: keys      -> u32 count, count × u8 found
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Request paths, one per supported command.
pub mod path {
    pub const GET: &str = "/get";
    pub const SET: &str = "/set";
    pub const DEL: &str = "/del";
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
