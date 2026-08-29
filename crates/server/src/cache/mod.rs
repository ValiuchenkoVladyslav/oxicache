//! S3-FIFO cache. Keys are hashed once; the top bits pick a shard, which owns
//! both the cuckoo index for its keys and the S3-FIFO queues that decide what
//! stays in it. Batch reads pin an epoch once and prefetch every key's
//! candidate buckets before reading any of them.

mod key;
mod s3fifo;
mod table;

use std::hash::{BuildHasher, Hasher};

use crossbeam_epoch as epoch;
pub use s3fifo::{Entry, TooLarge};
use s3fifo::Shard;
pub use table::EntryRef;

pub struct Cache {
    shards: Box<[Shard]>,
    shift: u32,
    hasher: rapidhash::fast::RandomState,
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
            hasher: rapidhash::fast::RandomState::default(),
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
        let keys = keys.into_iter();
        let mut located: Vec<(&Shard, u64, &[u8])> = Vec::with_capacity(keys.size_hint().0);
        for k in keys {
            let (shard, hash) = self.locate(k);
            shard.prefetch(hash, &guard);
            located.push((shard, hash, k));
        }
        // Buckets are hot now; issue the entry prefetches for every key
        // before any key's compare stalls on its entry header.
        for (shard, hash, _) in &located {
            shard.prefetch_entries(*hash, &guard);
        }
        let mut found: Vec<Option<EntryRef<'_>>> = Vec::with_capacity(located.len());
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
    pub fn set(&self, key: &[u8], value: &[u8]) -> std::result::Result<(), TooLarge> {
        self.set_many([(key, value)])
    }

    /// Store many entries under a single epoch pin. An entry that could
    /// never fit its shard fails the whole batch before anything is written,
    /// rather than flushing every resident entry of that shard to make room.
    pub fn set_many<'a, I>(&self, entries: I) -> std::result::Result<(), TooLarge>
    where
        I: IntoIterator<Item = (&'a [u8], &'a [u8])>,
        I::IntoIter: Clone,
    {
        let entries = entries.into_iter();
        for (k, v) in entries.clone() {
            self.shards[0].check_fits(k, v)?;
        }
        let guard = epoch::pin();
        let mut retired = false;
        for (k, v) in entries {
            let (shard, hash) = self.locate(k);
            retired |= shard.set(k, v, hash, &guard);
        }
        if retired {
            collect(&guard);
        }
        Ok(())
    }

    #[inline]
    pub fn del(&self, key: &[u8]) -> bool {
        let mut found = false;
        self.del_many([key], |f| found = f);
        found
    }

    /// Delete many keys under a single epoch pin, reporting each result to `f`.
    pub fn del_many<'a, I, F>(&self, keys: I, mut f: F)
    where
        I: IntoIterator<Item = &'a [u8]>,
        F: FnMut(bool),
    {
        let guard = epoch::pin();
        let mut retired = false;
        for k in keys {
            let (shard, hash) = self.locate(k);
            let found = shard.del(hash, k, &guard);
            retired |= found;
            f(found);
        }
        if retired {
            collect(&guard);
        }
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

/// Hand this thread's retired entries to the collector and reclaim what is
/// already safe. The collector otherwise runs only every 128 pins and frees
/// at most 8 bags then, which a batch of replacements outpaces many times
/// over; without this a write-heavy load grows without bound.
#[inline]
fn collect(guard: &epoch::Guard) {
    guard.flush();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spreads_across_shards() {
        let c = Cache::new(64 << 20, 12);
        assert_eq!(c.shards.len(), 16);
        for i in 0..10_000u32 {
            c.set(i.to_string().as_bytes(), b"v").unwrap();
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
        c.set(b"k", b"v").unwrap();
        let seen: Vec<Option<Vec<u8>>> = c.get_many([&b"k"[..], &b"x"[..], &b"k"[..]], |es| {
            es.iter()
                .map(|e| e.as_ref().map(|e| e.value().to_vec()))
                .collect()
        });
        assert_eq!(seen, vec![Some(b"v".to_vec()), None, Some(b"v".to_vec())]);
    }
}
