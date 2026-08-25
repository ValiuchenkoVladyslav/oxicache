//! Map key that keeps short keys inline in the bucket, so equality checks
//! during a lookup never chase a pointer into a separate allocation.

use std::borrow::Borrow;
use std::hash::{Hash, Hasher};

pub const INLINE: usize = 30;

#[derive(Clone, Debug)]
pub enum Key {
    Inline(u8, [u8; INLINE]),
    Heap(Box<[u8]>),
}

impl Key {
    #[inline]
    pub fn new(k: &[u8]) -> Self {
        if k.len() <= INLINE {
            let mut buf = [0u8; INLINE];
            buf[..k.len()].copy_from_slice(k);
            Key::Inline(k.len() as u8, buf)
        } else {
            Key::Heap(k.into())
        }
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Key::Inline(len, buf) => &buf[..*len as usize],
            Key::Heap(b) => b,
        }
    }
}

impl Borrow<[u8]> for Key {
    #[inline]
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}

impl PartialEq for Key {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}
impl Eq for Key {}

impl Hash for Key {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_and_heap_agree() {
        assert_eq!(std::mem::size_of::<Key>(), 32);
        let short = Key::new(b"abc");
        let long = Key::new(&[7u8; 100]);
        assert!(matches!(short, Key::Inline(3, _)));
        assert!(matches!(long, Key::Heap(_)));
        assert_eq!(short.as_slice(), b"abc");
        assert_eq!(long.as_slice(), &[7u8; 100][..]);
        assert_eq!(Key::new(b"abc"), short);
    }
}
