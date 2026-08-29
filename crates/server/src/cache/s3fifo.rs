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
use std::ptr::NonNull;
use std::sync::atomic::{
    AtomicBool, AtomicU8, AtomicUsize,
    Ordering::{Acquire, Relaxed, Release},
};

use crossbeam_epoch::Guard;
use parking_lot::Mutex;

use super::key::Key;
use super::table::{EntryRef, Index, Writer, retire};

/// Maximum frequency value tracked per entry.
const FREQ_CAP: u8 = 3;
/// Approximate per-entry bookkeeping overhead in bytes (Arc, atomics, index slot).
const ENTRY_OVERHEAD: usize = 128;
/// Minimum number of ghost hashes retained regardless of main queue length.
const GHOST_MIN: usize = 64;

/// An entry whose cost exceeds the byte budget of the shard it hashes to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("entry of {cost} bytes exceeds the per-shard capacity of {capacity} bytes")]
pub struct TooLarge {
    pub cost: usize,
    pub capacity: usize,
}

/// Per-entry metadata stored in front of the key and value bytes.
pub struct Meta {
    key: Key,
    hash: u64,
    freq: AtomicU8,
    live: AtomicBool,
}

/// Allocation header of an entry: refcount, value length, metadata. The
/// value bytes follow immediately; the header is 64 bytes, so they start on
/// a cache line of their own.
#[repr(C)]
struct Header {
    rc: AtomicUsize,
    len: usize,
    meta: Meta,
}

const HEADER: usize = std::mem::size_of::<Header>();
const _: () = assert!(HEADER == 64, "entry header must be exactly one cache line");
/// Values at least this long are copied with non-temporal stores: their
/// destination is a cold, recycled chunk, and streaming past the cache
/// avoids a read-for-ownership per line (2-3x faster on 1-4 KiB copies).
const NT_MIN: usize = 1024;

/// A cached item: refcount, metadata, key and value live in one allocation
/// (plus one more for keys longer than [`super::key::INLINE`]) so a hit
/// touches one or two adjacent cache lines. Thin (one pointer) so an index
/// slot can hold it, immutable once built, atomically refcounted.
pub struct Entry(NonNull<Header>);

// SAFETY: the allocation is immutable after construction apart from atomics.
unsafe impl Send for Entry {}
unsafe impl Sync for Entry {}

impl AsRef<[u8]> for Entry {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.value()
    }
}

impl Clone for Entry {
    #[inline]
    fn clone(&self) -> Self {
        let old = self.header().rc.fetch_add(1, Relaxed);
        if old > isize::MAX as usize {
            std::process::abort();
        }
        Self(self.0)
    }
}

impl Drop for Entry {
    #[inline]
    fn drop(&mut self) {
        if self.header().rc.fetch_sub(1, Release) != 1 {
            return;
        }
        std::sync::atomic::fence(Acquire);
        let len = self.header().len;
        // SAFETY: last handle; nobody else can observe the allocation.
        unsafe {
            std::ptr::drop_in_place(self.0.as_ptr());
            std::alloc::dealloc(self.0.as_ptr() as *mut u8, layout(len));
        }
    }
}

/// Small entries are cache-line aligned so a header plus a short value spans
/// the minimum number of lines (a 192-byte entry from a 16-byte-aligned
/// allocation straddles four lines three times out of four). Large values
/// pay glibc's aligned-allocation overhead without a proportionate gain.
#[inline]
fn layout(len: usize) -> std::alloc::Layout {
    let align = if len < NT_MIN {
        64
    } else {
        std::mem::align_of::<Header>()
    };
    std::alloc::Layout::from_size_align(HEADER + len, align).expect("entry size overflow")
}

impl Entry {
    pub(super) fn new(key: Key, value: &[u8], hash: u64) -> Self {
        let layout = layout(value.len());
        // SAFETY: layout is nonzero-sized; the header is written before use
        // and the value bytes are fully initialised by `copy_value`.
        unsafe {
            let p = std::alloc::alloc(layout) as *mut Header;
            let Some(p) = NonNull::new(p) else {
                std::alloc::handle_alloc_error(layout)
            };
            p.as_ptr().write(Header {
                rc: AtomicUsize::new(1),
                len: value.len(),
                meta: Meta {
                    key,
                    hash,
                    freq: AtomicU8::new(0),
                    live: AtomicBool::new(true),
                },
            });
            copy_value(p.as_ptr().cast::<u8>().add(HEADER), value);
            Self(p)
        }
    }

    #[inline]
    fn header(&self) -> &Header {
        // SAFETY: valid for the life of any handle.
        unsafe { self.0.as_ref() }
    }

    #[inline]
    fn meta(&self) -> &Meta {
        &self.header().meta
    }

    #[inline]
    pub fn key(&self) -> &[u8] {
        self.meta().key.as_slice()
    }

    #[inline]
    pub fn value(&self) -> &[u8] {
        // SAFETY: `len` initialised bytes follow the header.
        unsafe {
            std::slice::from_raw_parts(self.0.as_ptr().cast::<u8>().add(HEADER), self.header().len)
        }
    }

    /// Hint the CPU to fetch this entry's header and the first few data lines.
    #[inline]
    pub fn prefetch(&self) {
        #[cfg(target_arch = "x86_64")]
        {
            use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
            let p = self.0.as_ptr() as *const i8;
            let lines = (self.header().len / 64).min(3) + 1;
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
        a.0 == b.0
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
        let p = self.0.as_ptr() as *const std::ffi::c_void;
        std::mem::forget(self);
        p
    }

    /// # Safety
    /// `p` must come from [`Entry::into_raw`] and the handle must not be
    /// reconstructed more times than it was leaked (use `ManuallyDrop` for
    /// borrowed views).
    #[inline]
    pub(super) unsafe fn from_raw(p: *const std::ffi::c_void) -> Self {
        Self(unsafe { NonNull::new_unchecked(p as *mut Header) })
    }

    #[inline]
    pub(super) fn as_ptr(&self) -> *const std::ffi::c_void {
        self.0.as_ptr() as *const std::ffi::c_void
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

/// Copy `src` to `dst`, streaming large values past the cache.
///
/// # Safety
/// `dst` must be valid for `src.len()` writes.
#[inline]
unsafe fn copy_value(dst: *mut u8, src: &[u8]) {
    #[cfg(target_arch = "x86_64")]
    if src.len() >= NT_MIN && std::is_x86_feature_detected!("avx") {
        return unsafe { copy_nontemporal(dst, src) };
    }
    unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst, src.len()) }
}

/// # Safety
/// `dst` must be valid for `src.len()` writes; requires AVX.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
unsafe fn copy_nontemporal(dst: *mut u8, src: &[u8]) {
    use std::arch::x86_64::{__m256i, _mm_sfence, _mm256_loadu_si256, _mm256_stream_si256};
    const V: usize = 32;
    let head = dst.align_offset(V).min(src.len());
    let body = (src.len() - head) / V * V;
    unsafe {
        std::ptr::copy_nonoverlapping(src.as_ptr(), dst, head);
        let (s, d) = (src.as_ptr().add(head), dst.add(head));
        let mut i = 0;
        while i < body {
            let v = _mm256_loadu_si256(s.add(i) as *const __m256i);
            _mm256_stream_si256(d.add(i) as *mut __m256i, v);
            i += V;
        }
        _mm_sfence();
        let done = head + body;
        std::ptr::copy_nonoverlapping(src.as_ptr().add(done), dst.add(done), src.len() - done);
    }
}

struct Ghost {
    order: VecDeque<u64>,
    set: HashSet<u64, rapidhash::fast::RandomState>,
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
    pub fn prefetch_entries(&self, hash: u64, guard: &Guard) {
        self.index.prefetch_entries(hash, guard);
    }

    #[inline]
    pub fn get<'g>(&self, hash: u64, key: &[u8], guard: &'g Guard) -> Option<EntryRef<'g>> {
        self.index.get(hash, key, guard)
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Whether an entry for `key`/`value` can ever be resident here. Every
    /// shard has the same capacity, so one shard answers for all.
    pub fn check_fits(&self, key: &[u8], value: &[u8]) -> Result<(), TooLarge> {
        let cost = key.len() + value.len() + ENTRY_OVERHEAD;
        if cost > self.capacity {
            return Err(TooLarge {
                cost,
                capacity: self.capacity,
            });
        }
        Ok(())
    }

    /// Insert or replace. Returns whether any entry was retired (replaced or
    /// evicted) under `guard`. Callers are expected to have run
    /// [`check_fits`](Self::check_fits); an oversized entry is stored anyway
    /// and simply evicts everything else.
    pub fn set(&self, key: &[u8], value: &[u8], hash: u64, guard: &Guard) -> bool {
        let entry = Entry::new(Key::new(key), value, hash);
        let cost = entry.cost();
        let mut retired = false;

        let mut q = self.q.lock();
        // Replace first: the old entry's budget is released before deciding
        // how much to evict, so overwriting a key at full capacity does not
        // evict `cost` bytes of neighbours on top of the entry it replaces.
        // The new entry is not in any queue yet, so eviction cannot reach it.
        if let Some(old) = self.index.insert(&mut q.w, hash, entry.clone(), guard) {
            self.kill(&mut q, &old);
            retire(old, guard);
            retired = true;
        }
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
            retired = true;
        }
        q.used += cost;
        if q.ghost.take(hash) {
            q.main.push_back(entry);
        } else {
            q.small_bytes += cost;
            q.small.push_back(entry);
        }
        self.maybe_compact(&mut q);
        retired
    }

    pub fn del(&self, hash: u64, key: &[u8], guard: &Guard) -> bool {
        let mut q = self.q.lock();
        let Some(e) = self.index.remove(&mut q.w, hash, key, guard) else {
            return false;
        };
        self.kill(&mut q, &e);
        retire(e, guard);
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
        compact(&mut q.small);
        compact(&mut q.main);
        q.small_bytes = q.small.iter().map(|e| e.cost()).sum();
        q.dead_bytes = 0;
    }
}

/// Stable in-place removal of dead entries. Each liveness check dereferences
/// an entry, so the walk prefetches a few entries ahead to overlap the misses.
fn compact(q: &mut VecDeque<Entry>) {
    const AHEAD: usize = 8;
    let s = q.make_contiguous();
    let n = s.len();
    let mut w = 0;
    for i in 0..n {
        if let Some(e) = s.get(i + AHEAD) {
            e.prefetch();
        }
        if s[i].live().load(Relaxed) {
            s.swap(w, i);
            w += 1;
        }
    }
    q.truncate(w);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_epoch as epoch;

    fn shard(cap: usize) -> Shard {
        Shard::new(cap)
    }

    fn h(k: &[u8]) -> u64 {
        use std::hash::{BuildHasher, Hasher};
        let mut s = rapidhash::fast::SeedableState::fixed().build_hasher();
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
        s.del(h(k), k, &epoch::pin())
    }

    #[test]
    fn entry_layout_and_copies() {
        for len in [0, 1, 31, 64, 1000, NT_MIN, NT_MIN + 33, 3 * NT_MIN + 7] {
            let v: Vec<u8> = (0..len).map(|i| (i * 31 % 251) as u8).collect();
            let e = Entry::new(Key::new(b"k"), &v, 7);
            assert_eq!(e.value(), &v[..]);
            assert_eq!(e.key(), b"k");
            let c = e.clone();
            assert!(Entry::ptr_eq(&e, &c));
            drop(e);
            assert_eq!(c.value(), &v[..]);
        }
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
    fn replacing_at_full_capacity_evicts_only_the_old_entry() {
        // Ten equal entries fill the shard exactly; overwriting one with a
        // same-sized value must keep the other nine.
        let s = shard(10 * (key(0).len() + 100 + ENTRY_OVERHEAD));
        for i in 0..10 {
            set(&s, i, 100);
        }
        assert_eq!(s.len(), 10);
        set(&s, 3, 100);
        assert_eq!(s.len(), 10, "replacement must not evict neighbours");
        for i in 0..10 {
            assert!(get(&s, &key(i)).is_some(), "lost key {i}");
        }
    }

    #[test]
    fn check_fits_rejects_oversized() {
        let s = shard(1000);
        assert!(s.check_fits(b"k", &[0; 100]).is_ok());
        assert!(s.check_fits(b"k", &[0; 5000]).is_err());
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
