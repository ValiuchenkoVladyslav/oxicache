//! Sharded S3-FIFO cache. Keys are hashed once; the top bits pick a shard and
//! the full hash feeds that shard's ghost queue.

mod map;
mod s3fifo;

pub use map::Map;

use std::hash::{BuildHasher, Hasher};

use bytes::Bytes;
use s3fifo::Shard;

pub struct Cache {
    shards: Box<[Shard]>,
    shift: u32,
    hasher: foldhash::fast::RandomState,
}

impl Cache {
    /// Build a cache with `capacity` bytes split across `shards` (rounded up
    /// to a power of two) independent S3-FIFO instances.
    pub fn new(capacity: usize, shards: usize) -> Self {
        let n = shards.max(1).next_power_of_two();
        let per = capacity / n;
        Self {
            shards: (0..n).map(|_| Shard::new(per)).collect(),
            shift: 64 - n.trailing_zeros(),
            hasher: foldhash::fast::RandomState::default(),
        }
    }

    #[inline]
    fn locate(&self, key: &[u8]) -> (&Shard, u64) {
        let mut h = self.hasher.build_hasher();
        h.write(key);
        let hash = h.finish();
        let idx = if self.shift == 64 {
            0
        } else {
            (hash >> self.shift) as usize
        };
        (&self.shards[idx], hash)
    }

    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.locate(key).0.get(key)
    }

    #[inline]
    pub fn set(&self, key: Bytes, value: Bytes) {
        let (shard, hash) = self.locate(&key);
        shard.set(key, value, hash);
    }

    #[inline]
    pub fn del(&self, key: &[u8]) -> bool {
        self.locate(key).0.del(key)
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(Shard::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn used_bytes(&self) -> usize {
        self.shards.iter().map(Shard::used_bytes).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spreads_across_shards() {
        let c = Cache::new(64 << 20, 12);
        assert_eq!(c.shards.len(), 16);
        for i in 0..10_000u32 {
            c.set(Bytes::from(i.to_string()), Bytes::from_static(b"v"));
        }
        assert_eq!(c.len(), 10_000);
        assert!(
            c.shards.iter().all(|s| s.len() > 300),
            "hash should distribute keys evenly"
        );
        assert_eq!(c.get(b"42").as_deref(), Some(&b"v"[..]));
        assert!(c.del(b"42"));
        assert_eq!(c.len(), 9_999);
    }

    #[test]
    fn single_shard() {
        let c = Cache::new(1 << 20, 1);
        c.set(Bytes::from_static(b"k"), Bytes::from_static(b"v"));
        assert!(c.get(b"k").is_some());
    }
}
