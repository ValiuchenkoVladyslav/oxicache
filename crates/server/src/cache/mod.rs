//! S3-FIFO cache. Keys are hashed once; the top bits pick a shard, which owns
//! both the cuckoo index for its keys and the S3-FIFO queues that decide what
//! stays in it. Batch reads pin an epoch once and prefetch every key's
//! candidate buckets before reading any of them.

mod key;
mod s3fifo;
mod table;

use std::cell::Cell;
use std::hash::{BuildHasher, Hasher};
use std::num::NonZeroUsize;
use std::sync::atomic::Ordering::Relaxed;

use crossbeam_epoch as epoch;
use s3fifo::Retired;
use s3fifo::Shard;
pub use s3fifo::{Entry, TooLarge};
pub use table::EntryRef;

pub struct Cache {
    shards: Box<[Shard]>,
    shift: u32,
    hasher: rapidhash::fast::RandomState,
    capacity: usize,
}

/// A point-in-time sum of the per-shard counters, plus the configured budget.
#[derive(Clone, Copy, Debug, Default)]
pub struct Stats {
    pub get_hits: u64,
    pub get_misses: u64,
    pub del_hits: u64,
    pub del_misses: u64,
    /// Live entries evicted to make room, as distinct from deleted or
    /// replaced ones: sustained growth here means the capacity is short.
    pub evictions: u64,
    pub items: usize,
    pub used_bytes: usize,
    pub capacity: usize,
}

impl Cache {
    /// Build a cache with `capacity` bytes split across `shards` (rounded up
    /// to a power of two) independent S3-FIFO shards.
    pub fn new(capacity: NonZeroUsize, shards: NonZeroUsize) -> Self {
        let n = shards.get().next_power_of_two();
        let per = capacity.get() / n;
        Self {
            shards: (0..n).map(|_| Shard::new(per)).collect(),
            shift: 64 - n.trailing_zeros(),
            hasher: rapidhash::fast::RandomState::default(),
            capacity: capacity.get(),
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
        debug_assert!(idx < self.shards.len());
        // SAFETY: `shards.len() == 2^(64 - shift)`, so `hash >> shift` is below it.
        (unsafe { self.shards.get_unchecked(idx) }, hash)
    }

    /// Look up one key. The returned handle keeps the value alive.
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        let (shard, hash) = self.locate(key);
        let guard = epoch::pin();
        let Some(e) = shard.get(hash, key, &guard) else {
            shard.stats.get_misses.fetch_add(1, Relaxed);
            return None;
        };
        shard.stats.get_hits.fetch_add(1, Relaxed);
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
        // Tallied locally and added once per batch: a per-key increment on
        // the shared counter lines costs ~4 % server CPU on batch loads.
        let mut hits = 0;
        for (shard, hash, k) in &located {
            let e = shard.get(*hash, k, &guard);
            if let Some(e) = &e {
                hits += 1;
                e.prefetch();
                e.touch();
            }
            found.push(e);
        }
        if let Some((shard, _, _)) = located.first() {
            shard.stats.count_gets(hits, found.len() as u64 - hits);
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
        let mut retired = Retired::default();
        for (k, v) in entries {
            let (shard, hash) = self.locate(k);
            retired += shard.set(k, v, hash, &guard);
        }
        collect(&guard, retired);
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
        let mut retired = Retired::default();
        let (mut hits, mut total) = (0, 0);
        let mut last = None;
        for k in keys {
            let (shard, hash) = self.locate(k);
            let found = shard.del(hash, k, &guard);
            if let Some(r) = found {
                hits += 1;
                retired += r;
            }
            total += 1;
            last = Some(shard);
            f(found.is_some());
        }
        if let Some(shard) = last {
            shard.stats.count_dels(hits, total - hits);
        }
        collect(&guard, retired);
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(Shard::len).sum()
    }

    /// Sum the per-shard counters. Point-in-time, not a snapshot: counters
    /// keep moving while they are read.
    pub fn stats(&self) -> Stats {
        let mut s = Stats {
            capacity: self.capacity,
            ..Stats::default()
        };
        for shard in &self.shards {
            s.get_hits += shard.stats.get_hits.load(Relaxed);
            s.get_misses += shard.stats.get_misses.load(Relaxed);
            s.del_hits += shard.stats.del_hits.load(Relaxed);
            s.del_misses += shard.stats.del_misses.load(Relaxed);
            s.evictions += shard.stats.evictions.load(Relaxed);
            s.items += shard.len();
            s.used_bytes += shard.used_bytes();
        }
        s
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn used_bytes(&self) -> usize {
        self.shards.iter().map(Shard::used_bytes).sum()
    }
}

/// Large entries a thread may retire between flushes. A flush reclaims at
/// most 8 bags of 64 entries, so this must stay well under 512 for
/// reclamation to keep pace with retirement.
const FLUSH_ENTRIES: usize = 256;
/// Bytes of large entries a thread may retire between flushes, so a few
/// huge values do not sit in a bag for hundreds of requests.
const FLUSH_BYTES: usize = 1 << 20;

thread_local! {
    static RETIRED: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
}

/// Hand this thread's retired entries to the collector and reclaim what is
/// already safe. The collector otherwise runs only every 128 pins and frees
/// at most 8 bags then, which a batch of replacements outpaces many times
/// over; without flushing, a write-heavy load grows without bound.
///
/// Small entries (`Retired::small`) flush at once: their chunks are still
/// cache-hot when freed and the next allocation reuses them warm, which
/// deferral measurably loses (+7 % on the eviction profile). Large values
/// are copied with streaming stores and gain nothing from warm chunks, so
/// their retirements are batched up to a threshold, sparing the global
/// epoch/queue traffic (`try_advance` scans every thread) on most batches
/// (−13 % user CPU on the 4 KiB write profile).
#[inline]
fn collect(guard: &epoch::Guard, retired: Retired) {
    if retired.small > 0 {
        RETIRED.with(|r| r.set((0, 0)));
        guard.flush();
        return;
    }
    if retired.large == 0 {
        return;
    }
    let (n, b) = RETIRED.with(|r| {
        let (n, b) = r.get();
        let t = (n + retired.large, b + retired.large_bytes);
        r.set(t);
        t
    });
    if n >= FLUSH_ENTRIES || b >= FLUSH_BYTES {
        RETIRED.with(|r| r.set((0, 0)));
        guard.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spreads_across_shards() {
        let c = Cache::new(
            NonZeroUsize::new(64 << 20).unwrap(),
            NonZeroUsize::new(12).unwrap(),
        );
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
        let c = Cache::new(
            NonZeroUsize::new(1 << 20).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        );
        c.set(b"k", b"v").unwrap();
        let seen: Vec<Option<Vec<u8>>> = c.get_many([&b"k"[..], &b"x"[..], &b"k"[..]], |es| {
            es.iter()
                .map(|e| e.as_ref().map(|e| e.value().to_vec()))
                .collect()
        });
        assert_eq!(seen, vec![Some(b"v".to_vec()), None, Some(b"v".to_vec())]);
    }

    #[test]
    fn stats_sum_across_shards() {
        let c = Cache::new(
            NonZeroUsize::new(1 << 20).unwrap(),
            NonZeroUsize::new(2).unwrap(),
        );
        c.set(b"a", b"1").unwrap();
        c.set(b"b", b"2").unwrap();
        assert!(c.get(b"a").is_some());
        assert!(c.get(b"nope").is_none());
        c.get_many([&b"a"[..], &b"b"[..], &b"x"[..]], |_| ());
        assert!(c.del(b"a"));
        assert!(!c.del(b"a"));
        let s = c.stats();
        assert_eq!((s.get_hits, s.get_misses), (3, 2));
        assert_eq!((s.del_hits, s.del_misses), (1, 1));
        assert_eq!(s.items, 1);
        assert_eq!(s.capacity, 1 << 20);
        assert!(s.used_bytes > 0);
        assert_eq!(s.evictions, 0);
    }

    #[test]
    fn size_accounting() {
        let c = Cache::new(
            NonZeroUsize::new(1 << 20).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        );
        assert!(c.is_empty());
        assert_eq!(c.used_bytes(), 0);
        c.set(b"k", b"v").unwrap();
        assert!(!c.is_empty());
        assert!(c.used_bytes() > 0);
    }

    /// Replacing large values retires them in batches; enough of them must
    /// reach the flush threshold and hand the bag to the collector.
    #[test]
    fn large_retirements_are_batched_then_flushed() {
        let c = Cache::new(
            NonZeroUsize::new(64 << 20).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        );
        let big = vec![1u8; 2048];
        for _ in 0..(FLUSH_ENTRIES + 8) {
            c.set(b"k", &big).unwrap();
        }
        let huge = vec![2u8; FLUSH_BYTES / 2 + 1];
        for _ in 0..3 {
            c.set(b"h", &huge).unwrap();
        }
        assert_eq!(c.len(), 2);
    }
}
