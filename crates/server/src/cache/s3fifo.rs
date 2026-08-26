//! One S3-FIFO shard: the cuckoo [`Index`] for a slice of the key space plus
//! the FIFO queues (small, main, ghost) that decide what stays in it. The
//! queues and the index's single writer are guarded by one mutex.
//!
//! Reads never take the mutex: they hit the index and bump a relaxed atomic
//! frequency counter (capped at 3). Writes take a short critical section to
//! push onto the small/main queues and evict.
//!
//! Entries are immutable once created; removing one marks it `dead` and it is
//! skipped lazily when it reaches the head of its queue. Dead bytes are tracked
//! and the queues are compacted when they accumulate.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering::Relaxed};

use crossbeam_epoch::{self as epoch, Guard};
use parking_lot::Mutex;
use triomphe::ThinArc;

use super::key::Key;
use super::table::{EntryRef, Index, Writer, retire};

/// Maximum frequency value tracked per entry.
const FREQ_CAP: u8 = 3;
/// Approximate per-entry bookkeeping overhead in bytes (Arc, atomics, index slot).
const ENTRY_OVERHEAD: usize = 128;
/// Minimum number of ghost hashes retained regardless of main queue length.
const GHOST_MIN: usize = 64;

/// Per-entry metadata stored in front of the key and value bytes.
pub struct Meta {
    key: Key,
    hash: u64,
    freq: AtomicU8,
    live: AtomicBool,
}

/// A cached item: refcount, metadata, key and value live in one allocation
/// (plus one more for keys longer than [`super::key::INLINE`]) so a hit
/// touches one or two adjacent cache lines.
#[derive(Clone)]
pub struct Entry(ThinArc<Meta, u8>);

impl AsRef<[u8]> for Entry {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.value()
    }
}

impl Entry {
    pub(super) fn new(key: Key, value: &[u8], hash: u64) -> Self {
        let meta = Meta {
            key,
            hash,
            freq: AtomicU8::new(0),
            live: AtomicBool::new(true),
        };
        Self(ThinArc::from_header_and_slice(meta, value))
    }

    #[inline]
    fn meta(&self) -> &Meta {
        &self.0.header.header
    }

    #[inline]
    pub fn key(&self) -> &[u8] {
        self.meta().key.as_slice()
    }

    #[inline]
    pub fn value(&self) -> &[u8] {
        &self.0.slice
    }

    /// Hint the CPU to fetch this entry's header and the first few data lines.
    #[inline]
    pub fn prefetch(&self) {
        #[cfg(target_arch = "x86_64")]
        {
            use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
            let p = self.0.heap_ptr() as *const i8;
            let lines = (self.0.slice.len() / 64).min(3) + 1;
            // SAFETY: prefetch is a pure hint; it never faults or dereferences.
            unsafe {
                for i in 0..lines {
                    _mm_prefetch(p.add(i * 64), _MM_HINT_T0);
                }
            }
        }
    }

    #[inline]
    pub fn ptr_eq(a: &Self, b: &Self) -> bool {
        a.0.heap_ptr() == b.0.heap_ptr()
    }

    #[inline]
    fn cost(&self) -> usize {
        self.key().len() + self.value().len() + ENTRY_OVERHEAD
    }

    #[inline]
    pub(super) fn hash(&self) -> u64 {
        self.meta().hash
    }

    #[inline]
    pub(super) fn into_raw(self) -> *const std::ffi::c_void {
        self.0.into_raw()
    }

    /// # Safety
    /// `p` must come from [`Entry::into_raw`] and the handle must not be
    /// reconstructed more times than it was leaked (use `ManuallyDrop` for
    /// borrowed views).
    #[inline]
    pub(super) unsafe fn from_raw(p: *const std::ffi::c_void) -> Self {
        Self(unsafe { ThinArc::from_raw(p) })
    }

    #[inline]
    pub(super) fn as_ptr(&self) -> *const std::ffi::c_void {
        self.0.heap_ptr()
    }

    #[inline]
    fn freq(&self) -> &AtomicU8 {
        &self.meta().freq
    }

    #[inline]
    fn live(&self) -> &AtomicBool {
        &self.meta().live
    }

    #[inline]
    pub(super) fn touch(&self) {
        let f = self.freq().load(Relaxed);
        if f < FREQ_CAP {
            self.freq().store(f + 1, Relaxed);
        }
    }
}

struct Ghost {
    order: VecDeque<u64>,
    set: HashSet<u64, foldhash::fast::RandomState>,
}

impl Ghost {
    fn take(&mut self, hash: u64) -> bool {
        self.set.remove(&hash)
    }

    fn push(&mut self, hash: u64, limit: usize) {
        if self.set.insert(hash) {
            self.order.push_back(hash);
        }
        while self.order.len() > limit.max(GHOST_MIN) {
            let old = self.order.pop_front().unwrap();
            self.set.remove(&old);
        }
    }
}

struct Queues {
    /// Proof of exclusive index write access; lives inside the mutex.
    w: Writer,
    small: VecDeque<Entry>,
    main: VecDeque<Entry>,
    ghost: Ghost,
    /// Bytes of live entries across both queues.
    used: usize,
    /// Bytes held by the small queue (live and dead).
    small_bytes: usize,
    /// Bytes held by dead entries still sitting in either queue.
    dead_bytes: usize,
}

pub struct Shard {
    index: Index,
    q: Mutex<Queues>,
    capacity: usize,
    small_capacity: usize,
}

impl Shard {
    /// `capacity` is the byte budget of live entries; the small queue gets 10%.
    pub fn new(capacity: usize) -> Self {
        Self {
            index: Index::new(),
            q: Mutex::new(Queues {
                w: Writer::new(),
                small: VecDeque::new(),
                main: VecDeque::new(),
                ghost: Ghost {
                    order: VecDeque::new(),
                    set: HashSet::default(),
                },
                used: 0,
                small_bytes: 0,
                dead_bytes: 0,
            }),
            capacity,
            small_capacity: capacity / 10,
        }
    }

    #[inline]
    pub fn prefetch(&self, hash: u64, guard: &Guard) {
        self.index.prefetch(hash, guard);
    }

    #[inline]
    pub fn get<'g>(&self, hash: u64, key: &[u8], guard: &'g Guard) -> Option<EntryRef<'g>> {
        self.index.get(hash, key, guard)
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn set(&self, key: &[u8], value: &[u8], hash: u64, guard: &Guard) {
        let entry = Entry::new(Key::new(key), value, hash);
        let cost = entry.cost();

        let mut q = self.q.lock();
        while q.used + cost > self.capacity {
            // Not identical: eviction order differs, which is the whole point of S3-FIFO.
            #[allow(clippy::if_same_then_else)]
            let evicted = if q.small_bytes >= self.small_capacity {
                self.evict_small(&mut q, guard) || self.evict_main(&mut q, guard)
            } else {
                self.evict_main(&mut q, guard) || self.evict_small(&mut q, guard)
            };
            if !evicted {
                break;
            }
        }
        if let Some(old) = self.index.insert(&mut q.w, hash, entry.clone(), guard) {
            self.kill(&mut q, &old);
            retire(old, guard);
        }
        q.used += cost;
        if q.ghost.take(hash) {
            q.main.push_back(entry);
        } else {
            q.small_bytes += cost;
            q.small.push_back(entry);
        }
        self.maybe_compact(&mut q);
    }

    pub fn del(&self, hash: u64, key: &[u8]) -> bool {
        let guard = epoch::pin();
        let mut q = self.q.lock();
        let Some(e) = self.index.remove(&mut q.w, hash, key, &guard) else {
            return false;
        };
        self.kill(&mut q, &e);
        retire(e, &guard);
        self.maybe_compact(&mut q);
        true
    }

    pub fn used_bytes(&self) -> usize {
        self.q.lock().used
    }

    /// Mark an entry removed from the index as dead and release its budget.
    fn kill(&self, q: &mut Queues, e: &Entry) {
        if e.live().swap(false, Relaxed) {
            let cost = e.cost();
            q.used -= cost;
            q.dead_bytes += cost;
        }
    }

    fn evict_small(&self, q: &mut Queues, guard: &Guard) -> bool {
        let Some(e) = q.small.pop_front() else {
            return false;
        };
        let cost = e.cost();
        q.small_bytes -= cost;
        if !e.live().load(Relaxed) {
            q.dead_bytes -= cost;
        } else if e.freq().load(Relaxed) > 1 {
            e.freq().store(0, Relaxed);
            q.main.push_back(e);
        } else {
            if let Some(h) = self.index.remove_if_same(&mut q.w, e.hash(), &e, guard) {
                retire(h, guard);
            }
            e.live().store(false, Relaxed);
            q.used -= cost;
            let limit = q.main.len();
            q.ghost.push(e.hash(), limit);
        }
        true
    }

    fn evict_main(&self, q: &mut Queues, guard: &Guard) -> bool {
        loop {
            let Some(e) = q.main.pop_front() else {
                return false;
            };
            let cost = e.cost();
            if !e.live().load(Relaxed) {
                q.dead_bytes -= cost;
                return true;
            }
            let f = e.freq().load(Relaxed);
            if f > 0 {
                e.freq().store(f - 1, Relaxed);
                q.main.push_back(e);
                continue;
            }
            if let Some(h) = self.index.remove_if_same(&mut q.w, e.hash(), &e, guard) {
                retire(h, guard);
            }
            e.live().store(false, Relaxed);
            q.used -= cost;
            return true;
        }
    }

    /// Drop dead entries eagerly once they hold a quarter of the budget.
    fn maybe_compact(&self, q: &mut Queues) {
        if q.dead_bytes <= self.capacity / 4 {
            return;
        }
        q.small.retain(|e| e.live().load(Relaxed));
        q.main.retain(|e| e.live().load(Relaxed));
        q.small_bytes = q.small.iter().map(|e| e.cost()).sum();
        q.dead_bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard(cap: usize) -> Shard {
        Shard::new(cap)
    }

    fn h(k: &[u8]) -> u64 {
        use std::hash::{BuildHasher, Hasher};
        let mut s = foldhash::fast::FixedState::default().build_hasher();
        s.write(k);
        s.finish()
    }

    fn key(i: usize) -> Vec<u8> {
        format!("key-{i:06}").into_bytes()
    }

    fn set(s: &Shard, i: usize, vlen: usize) {
        s.set(&key(i), &vec![0u8; vlen], h(&key(i)), &epoch::pin());
    }

    fn get(s: &Shard, k: &[u8]) -> Option<Vec<u8>> {
        let g = epoch::pin();
        s.get(h(k), k, &g).map(|e| e.value().to_vec())
    }

    fn del(s: &Shard, k: &[u8]) -> bool {
        s.del(h(k), k)
    }

    #[test]
    fn get_set_del() {
        let s = shard(1 << 20);
        assert!(get(&s, b"a").is_none());
        s.set(b"a", b"1", h(b"a"), &epoch::pin());
        assert_eq!(get(&s, b"a").as_deref(), Some(&b"1"[..]));
        s.set(b"a", b"2", h(b"a"), &epoch::pin());
        assert_eq!(get(&s, b"a").as_deref(), Some(&b"2"[..]));
        assert!(del(&s, b"a"));
        assert!(!del(&s, b"a"));
        assert!(get(&s, b"a").is_none());
        assert_eq!(s.used_bytes(), 0);
    }

    #[test]
    fn respects_capacity() {
        let cap = 100 * (ENTRY_OVERHEAD + 10 + 100);
        let s = shard(cap);
        for i in 0..1000 {
            set(&s, i, 100);
        }
        assert!(s.used_bytes() <= cap);
        assert!(s.len() <= 100 && s.len() >= 90);
        assert!(get(&s, &key(999)).is_some());
        assert!(get(&s, &key(0)).is_none());
    }

    #[test]
    fn hot_keys_survive_scan() {
        let cap = 100 * (ENTRY_OVERHEAD + 10 + 100);
        let s = shard(cap);
        for i in 0..10 {
            set(&s, i, 100);
        }
        for _ in 0..3 {
            for i in 0..10 {
                let g = epoch::pin();
                s.get(h(&key(i)), &key(i), &g).unwrap().touch();
            }
        }
        for i in 10..2000 {
            set(&s, i, 100);
        }
        let survivors = (0..10).filter(|&i| get(&s, &key(i)).is_some()).count();
        assert_eq!(
            survivors, 10,
            "hot keys should be promoted to main and survive a scan"
        );
    }

    #[test]
    fn ghost_hit_goes_to_main() {
        let cap = 100 * (ENTRY_OVERHEAD + 10 + 100);
        let s = shard(cap);
        set(&s, 0, 100);
        for i in 1..150 {
            set(&s, i, 100);
        }
        assert!(get(&s, &key(0)).is_none());
        set(&s, 0, 100);
        assert!(
            s.q.lock().main.iter().any(|e| e.key() == key(0)),
            "ghost hit should insert into main"
        );
    }

    #[test]
    fn delete_frees_budget_and_compacts() {
        let cap = 100 * (ENTRY_OVERHEAD + 10 + 100);
        let s = shard(cap);
        for i in 0..100 {
            set(&s, i, 100);
        }
        for i in 0..100 {
            assert!(del(&s, &key(i)));
        }
        assert_eq!(s.used_bytes(), 0);
        let q = s.q.lock();
        assert!(
            q.dead_bytes <= cap / 4,
            "compaction should bound dead bytes"
        );
        assert!(q.small.len() + q.main.len() <= 25);
    }

    #[test]
    fn oversized_entry_is_only_resident() {
        let s = shard(1000);
        set(&s, 0, 100);
        set(&s, 1, 5000);
        assert!(get(&s, &key(0)).is_none());
        assert!(get(&s, &key(1)).is_some());
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn concurrent_stress() {
        let s = std::sync::Arc::new(shard(200 * 1024));
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let s = s.clone();
                std::thread::spawn(move || {
                    for i in 0..20_000 {
                        let k = (i * 7 + t) % 2000;
                        match i % 10 {
                            0..=6 => {
                                get(&s, &key(k));
                            }
                            7 | 8 => set(&s, k, 64),
                            _ => {
                                del(&s, &key(k));
                            }
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert!(s.used_bytes() <= 200 * 1024);
        let q = s.q.lock();
        let live: usize = q
            .small
            .iter()
            .chain(q.main.iter())
            .filter(|e| e.live().load(Relaxed))
            .map(|e| e.cost())
            .sum();
        assert_eq!(live, q.used, "accounting must match live entries");
        assert_eq!(
            s.len(),
            q.small
                .iter()
                .chain(q.main.iter())
                .filter(|e| e.live().load(Relaxed))
                .count()
        );
    }
}
