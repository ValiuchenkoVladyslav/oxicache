//! S3-FIFO cache. Keys are hashed once; the top bits pick a shard, which owns
//! both the cuckoo index for its keys and the S3-FIFO queues that decide what
//! stays in it. Batch reads pin an epoch once and prefetch every key's
//! candidate buckets before reading any of them.

mod key;
mod s3fifo;
mod table;

use std::hash::{BuildHasher, Hasher};

use crossbeam_epoch as epoch;
pub use s3fifo::Entry;
use s3fifo::Shard;
pub use table::EntryRef;

pub struct Cache {
    shards: Box<[Shard]>,
    shift: u32,
    hasher: foldhash::fast::RandomState,
}

impl Cache {
    /// Build a cache with `capacity` bytes split across `shards` (rounded up
    /// to a power of two) independent S3-FIFO shards.
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

    /// Look up one key. The returned handle keeps the value alive.
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        let (shard, hash) = self.locate(key);
        let guard = epoch::pin();
        let e = shard.get(hash, key, &guard)?;
        e.touch();
        Some(Entry::clone(&e))
    }

    /// Look up many keys and hand the resolved entries (in key order) to `f`.
    /// One epoch pin covers the batch; no refcounts are touched. Candidate
    /// buckets for every key are prefetched first, then the entries, so the
    /// memory accesses of independent keys overlap.
    #[inline]
    pub fn get_many<'a, I, F, R>(&self, keys: I, f: F) -> R
    where
        I: IntoIterator<Item = &'a [u8]>,
        F: FnOnce(&[Option<EntryRef<'_>>]) -> R,
    {
        let guard = epoch::pin();
        let mut located: smallvec::SmallVec<[(&Shard, u64, &[u8]); 32]> = smallvec::SmallVec::new();
        for k in keys {
            let (shard, hash) = self.locate(k);
            shard.prefetch(hash, &guard);
            located.push((shard, hash, k));
        }
        let mut found: smallvec::SmallVec<[Option<EntryRef<'_>>; 32]> = smallvec::SmallVec::new();
        for (shard, hash, k) in located {
            let e = shard.get(hash, k, &guard);
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
        shard.set(key, value, hash, &epoch::pin());
    }

    /// Store many entries under a single epoch pin.
    pub fn set_many<'a, I>(&self, entries: I)
    where
        I: IntoIterator<Item = (&'a [u8], &'a [u8])>,
    {
        let guard = epoch::pin();
        for (k, v) in entries {
            let (shard, hash) = self.locate(k);
            shard.set(k, v, hash, &guard);
        }
    }

    #[inline]
    pub fn del(&self, key: &[u8]) -> bool {
        let (shard, hash) = self.locate(key);
        shard.del(hash, key)
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
            c.set(i.to_string().as_bytes(), b"v");
        }
        assert_eq!(c.len(), 10_000);
        assert!(
            c.shards.iter().all(|s| s.len() > 300),
            "hash should distribute keys evenly"
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
