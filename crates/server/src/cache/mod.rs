//! S3-FIFO cache: one lock-free index for all keys, and eviction state
//! sharded by key hash. Keys are hashed once; the top bits pick a shard and
//! the full hash feeds that shard's ghost queue.

mod key;
mod map;
mod s3fifo;

use std::hash::{BuildHasher, Hasher};

use map::Map;
pub use s3fifo::Entry;
use s3fifo::Shard;

pub struct Cache {
    map: Map,
    shards: Box<[Shard]>,
    shift: u32,
    hasher: foldhash::fast::RandomState,
}

impl Cache {
    /// Build a cache with `capacity` bytes split across `shards` (rounded up
    /// to a power of two) independent S3-FIFO eviction shards.
    pub fn new(capacity: usize, shards: usize) -> Self {
        let n = shards.max(1).next_power_of_two();
        let per = capacity / n;
        Self {
            map: Map::new(),
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

    /// Look up one key. The returned entry keeps the value alive; read it
    /// with [`Entry::value`].
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        let e = self.map.get(key)?;
        e.touch();
        Some(e)
    }

    /// Look up many keys and hand the resolved entries (in key order) to `f`.
    /// Lookups run in two passes so the cache misses of independent keys
    /// overlap: the first resolves entries and prefetches them, the second
    /// (inside `f`) reads them. The entries stay valid until `f` returns.
    #[inline]
    pub fn get_many<'a, I, F, R>(&self, keys: I, f: F) -> R
    where
        I: IntoIterator<Item = &'a [u8]>,
        F: FnOnce(&[Option<map::EntryRef<'_>>]) -> R,
    {
        let reader = self.map.read();
        let mut found: smallvec::SmallVec<[Option<map::EntryRef<'_>>; 32]> =
            smallvec::SmallVec::new();
        for k in keys {
            let e = reader.get(k);
            if let Some(e) = &e {
                e.prefetch();
                e.touch();
            }
            found.push(e);
        }
        f(&found)
    }

    #[inline]
    pub fn set(&self, key: &[u8], value: &[u8]) {
        let (shard, hash) = self.locate(key);
        shard.set(&self.map, key, value, hash);
    }

    #[inline]
    pub fn del(&self, key: &[u8]) -> bool {
        self.locate(key).0.del(&self.map, key)
    }

    pub fn len(&self) -> usize {
        self.map.len()
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
            c.set(i.to_string().as_bytes(), b"v");
        }
        assert_eq!(c.len(), 10_000);
        assert!(
            c.shards.iter().all(|s| s.used_bytes() > 0),
            "hash should distribute keys over shards"
        );
        assert_eq!(
            c.get(b"42").map(|e| e.value().to_vec()).as_deref(),
            Some(&b"v"[..])
        );
        assert!(c.del(b"42"));
        assert_eq!(c.len(), 9_999);
    }

    #[test]
    fn get_many_in_order() {
        let c = Cache::new(1 << 20, 1);
        c.set(b"k", b"v");
        let seen: Vec<Option<Vec<u8>>> = c.get_many([&b"k"[..], &b"x"[..], &b"k"[..]], |es| {
            es.iter()
                .map(|e| e.as_ref().map(|e| e.value().to_vec()))
                .collect()
        });
        assert_eq!(seen, vec![Some(b"v".to_vec()), None, Some(b"v".to_vec())]);
    }
}
