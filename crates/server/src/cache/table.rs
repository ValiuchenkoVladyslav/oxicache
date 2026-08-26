//! Per-shard cuckoo hash index with lock-free readers.
//!
//! Each bucket is one cache line of eight slots; a slot packs a 16-bit hash
//! tag with the 48-bit address of an [`Entry`] allocation. A key has two
//! candidate buckets, both derived from its hash alone, so a lookup is
//! `bucket -> entry`: two dependent memory accesses, and both candidate
//! buckets can be prefetched before either is needed. Tags reject absent
//! keys without touching any entry, which makes the table its own filter.
//!
//! Readers pin an epoch once per batch and borrow entries under it. There is
//! exactly one writer per shard at a time (the shard mutex), enforced by the
//! [`Writer`] token; writers publish slot words with release stores and
//! retire replaced entries and tables through the epoch collector. Cuckoo
//! displacement copies an item to its new slot before clearing the old one,
//! so a present key is never invisible to a concurrent reader.

use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::*};

use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned};

use super::s3fifo::Entry;

const SLOTS: usize = 8;
const MIN_BUCKETS: usize = 64;
/// Grow when the table is this full (in eighths).
const MAX_LOAD_EIGHTHS: usize = 7;
const MAX_KICKS: usize = 256;
const PTR_BITS: u32 = 48;
const PTR_MASK: u64 = (1 << PTR_BITS) - 1;

#[repr(C, align(64))]
struct Bucket([AtomicU64; SLOTS]);

struct Table {
    buckets: Box<[Bucket]>,
    mask: usize,
    /// Whether dropping this table drops the entry handles in its slots.
    /// False for tables retired by a resize, whose handles moved on.
    owns: bool,
}

impl Table {
    fn new(buckets: usize) -> Self {
        let buckets = buckets.next_power_of_two().max(MIN_BUCKETS);
        Self {
            buckets: (0..buckets)
                .map(|_| Bucket(std::array::from_fn(|_| AtomicU64::new(0))))
                .collect(),
            mask: buckets - 1,
            owns: true,
        }
    }

    #[inline]
    fn home(&self, hash: u64) -> usize {
        hash as usize & self.mask
    }

    /// Partial-key cuckoo: the alternate bucket depends only on the home
    /// bucket and the tag, so it can be recomputed from a slot word.
    #[inline]
    fn alt(&self, bucket: usize, tag: u16) -> usize {
        let h = (tag as usize).wrapping_mul(0x9E37_79B9) | 1;
        (bucket ^ h) & self.mask
    }

    fn find(&self, hash: u64, key: &[u8]) -> Option<(usize, usize, u64)> {
        let tag = tag_of(hash);
        let b1 = self.home(hash);
        let b2 = self.alt(b1, tag);
        for b in [b1, b2] {
            // Load the whole bucket and build a match mask without branching
            // per slot; only candidates with the right tag touch an entry.
            let words: [u64; SLOTS] = std::array::from_fn(|i| self.buckets[b].0[i].load(Acquire));
            let mut m = 0u32;
            for (i, &w) in words.iter().enumerate() {
                m |= ((w != 0 && tag_of_word(w) == tag) as u32) << i;
            }
            while m != 0 {
                let i = m.trailing_zeros() as usize;
                m &= m - 1;
                let w = words[i];
                // SAFETY: a nonzero word is a live entry under the guard.
                let e = unsafe { entry_at(w) };
                if e.key() == key {
                    return Some((b, i, w));
                }
            }
        }
        None
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        if !self.owns {
            return;
        }
        for b in self.buckets.iter_mut() {
            for s in b.0.iter_mut() {
                let w = *s.get_mut();
                if w != 0 {
                    // SAFETY: an owning table holds one handle per nonzero slot.
                    unsafe { drop(entry_from_word(w)) };
                }
            }
        }
    }
}

#[inline]
fn tag_of(hash: u64) -> u16 {
    (hash >> 48) as u16
}

#[inline]
fn tag_of_word(w: u64) -> u16 {
    (w >> PTR_BITS) as u16
}

#[inline]
fn word(tag: u16, ptr: *const std::ffi::c_void) -> u64 {
    let p = ptr as u64;
    debug_assert_eq!(p & !PTR_MASK, 0, "entry pointer must fit in 48 bits");
    ((tag as u64) << PTR_BITS) | p
}

/// # Safety
/// `w` must be a slot word whose entry is alive.
#[inline]
unsafe fn entry_at(w: u64) -> ManuallyDrop<Entry> {
    unsafe { ManuallyDrop::new(Entry::from_raw((w & PTR_MASK) as *const std::ffi::c_void)) }
}

/// # Safety
/// Takes ownership of the handle stored in `w`.
#[inline]
unsafe fn entry_from_word(w: u64) -> Entry {
    unsafe { Entry::from_raw((w & PTR_MASK) as *const std::ffi::c_void) }
}

/// Single-writer token: constructed once per shard and kept inside the
/// shard's mutex, so holding `&mut Writer` proves the mutex is held.
pub struct Writer(());

impl Writer {
    pub(super) fn new() -> Self {
        Self(())
    }
}

pub struct Index {
    table: Atomic<Table>,
    len: AtomicUsize,
    kick_seed: AtomicUsize,
}

/// A borrowed entry, valid for the lifetime of the epoch guard it came from.
pub struct EntryRef<'g> {
    inner: ManuallyDrop<Entry>,
    _guard: PhantomData<&'g Guard>,
}

impl Deref for EntryRef<'_> {
    type Target = Entry;
    #[inline]
    fn deref(&self) -> &Entry {
        &self.inner
    }
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}

impl Index {
    pub fn new() -> Self {
        Self {
            table: Atomic::new(Table::new(MIN_BUCKETS)),
            len: AtomicUsize::new(0),
            kick_seed: AtomicUsize::new(1),
        }
    }

    pub fn len(&self) -> usize {
        self.len.load(Relaxed)
    }

    #[inline]
    fn load<'g>(&self, guard: &'g Guard) -> &'g Table {
        // SAFETY: the table pointer is always valid and retired via the guard.
        unsafe { self.table.load(Acquire, guard).deref() }
    }

    /// Prefetch both candidate buckets for `hash`.
    #[inline]
    pub fn prefetch(&self, hash: u64, guard: &Guard) {
        let t = self.load(guard);
        let b1 = t.home(hash);
        let b2 = t.alt(b1, tag_of(hash));
        prefetch_line(&t.buckets[b1]);
        prefetch_line(&t.buckets[b2]);
    }

    #[inline]
    pub fn get<'g>(&self, hash: u64, key: &[u8], guard: &'g Guard) -> Option<EntryRef<'g>> {
        let t = self.load(guard);
        let (_, _, w) = t.find(hash, key)?;
        // SAFETY: found under the guard; the entry outlives it.
        Some(EntryRef {
            inner: unsafe { entry_at(w) },
            _guard: PhantomData,
        })
    }

    /// Insert or replace. Returns the previous handle for `key`, if any.
    pub fn insert(&self, _w: &mut Writer, hash: u64, entry: Entry, guard: &Guard) -> Option<Entry> {
        let tag = tag_of(hash);
        let t = self.load(guard);
        if let Some((b, i, old)) = t.find(hash, entry.key()) {
            t.buckets[b].0[i].store(word(tag, entry.into_raw()), Release);
            // SAFETY: the old handle is no longer reachable from the table.
            return Some(unsafe { entry_from_word(old) });
        }
        let new_word = word(tag, entry.into_raw());
        if self.len.load(Relaxed) * 8 >= t.buckets.len() * SLOTS * MAX_LOAD_EIGHTHS {
            self.grow(guard);
        }
        loop {
            let t = self.load(guard);
            if self.place(t, hash, new_word) {
                self.len.fetch_add(1, Relaxed);
                return None;
            }
            self.grow(guard);
        }
    }

    /// Remove `key`, returning its handle.
    pub fn remove(&self, _w: &mut Writer, hash: u64, key: &[u8], guard: &Guard) -> Option<Entry> {
        let t = self.load(guard);
        let (b, i, w) = t.find(hash, key)?;
        t.buckets[b].0[i].store(0, Release);
        self.len.fetch_sub(1, Relaxed);
        // SAFETY: unlinked; caller retires or keeps the handle.
        Some(unsafe { entry_from_word(w) })
    }

    /// Remove `key` only if it still maps to exactly `entry`.
    pub fn remove_if_same(
        &self,
        _w: &mut Writer,
        hash: u64,
        entry: &Entry,
        guard: &Guard,
    ) -> Option<Entry> {
        let t = self.load(guard);
        let (b, i, w) = t.find(hash, entry.key())?;
        if (w & PTR_MASK) != entry.as_ptr() as u64 {
            return None;
        }
        t.buckets[b].0[i].store(0, Release);
        self.len.fetch_sub(1, Relaxed);
        // SAFETY: unlinked; caller retires or keeps the handle.
        Some(unsafe { entry_from_word(w) })
    }

    /// Cuckoo placement of a fresh word. Every move copies to the destination
    /// before the source is overwritten, so readers never miss a present key.
    fn place(&self, t: &Table, hash: u64, new_word: u64) -> bool {
        let tag = tag_of(hash);
        let b1 = t.home(hash);
        let b2 = t.alt(b1, tag);
        for b in [b1, b2] {
            if let Some(i) = empty_slot(&t.buckets[b]) {
                t.buckets[b].0[i].store(new_word, Release);
                return true;
            }
        }
        // Random walk to find a displacement path, then apply it backwards.
        let mut path: Vec<(usize, usize)> = Vec::with_capacity(16);
        let mut seed = self.kick_seed.load(Relaxed);
        let mut b = if seed & 1 == 0 { b1 } else { b2 };
        let mut dest = None;
        for _ in 0..MAX_KICKS {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let i = (seed >> 33) % SLOTS;
            let w = t.buckets[b].0[i].load(Relaxed);
            path.push((b, i));
            let next = t.alt(b, tag_of_word(w));
            if let Some(j) = empty_slot(&t.buckets[next]) {
                dest = Some((next, j));
                break;
            }
            b = next;
        }
        self.kick_seed.store(seed, Relaxed);
        let Some((mut db, mut di)) = dest else {
            return false;
        };
        for &(sb, si) in path.iter().rev() {
            let w = t.buckets[sb].0[si].load(Relaxed);
            t.buckets[db].0[di].store(w, Release);
            (db, di) = (sb, si);
        }
        t.buckets[db].0[di].store(new_word, Release);
        true
    }

    fn grow(&self, guard: &Guard) {
        let old = self.table.load(Acquire, guard);
        // SAFETY: valid under the guard; single writer.
        let old_t = unsafe { old.deref() };
        let mut n = old_t.buckets.len() * 2;
        'retry: loop {
            let fresh = Table::new(n);
            for b in &old_t.buckets {
                for s in &b.0 {
                    let w = s.load(Relaxed);
                    if w != 0 {
                        // SAFETY: entry alive; we only read its hash.
                        let e = unsafe { entry_at(w) };
                        if !self.place(&fresh, e.hash(), w) {
                            n *= 2;
                            continue 'retry;
                        }
                    }
                }
            }
            let fresh = Owned::new(fresh).into_shared(guard);
            self.table.store(fresh, Release);
            // SAFETY: readers may still hold `old`; its handles moved to
            // `fresh`, so retire it without dropping them.
            unsafe {
                let mut retired: Owned<Table> = old.into_owned();
                retired.owns = false;
                guard.defer_unchecked(move || drop(retired));
            }
            return;
        }
    }
}

impl Drop for Index {
    fn drop(&mut self) {
        // SAFETY: no readers can exist while we hold `&mut self`.
        unsafe {
            let t = self.table.load(Relaxed, epoch::unprotected());
            if !t.is_null() {
                drop(t.into_owned());
            }
        }
    }
}

#[inline]
fn empty_slot(b: &Bucket) -> Option<usize> {
    b.0.iter().position(|s| s.load(Relaxed) == 0)
}

#[inline]
fn prefetch_line<T>(p: &T) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: prefetch is a hint and never faults.
    unsafe {
        std::arch::x86_64::_mm_prefetch(p as *const T as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
}

/// Retire an entry handle: it is dropped once every current reader unpins.
pub fn retire(entry: Entry, guard: &Guard) {
    // SAFETY: `Entry` is Send + 'static; dropping it later is always sound.
    unsafe { guard.defer_unchecked(move || drop(entry)) }
}

#[cfg(test)]
mod tests {
    use super::super::key::Key;
    use super::*;

    fn h(k: &[u8]) -> u64 {
        use std::hash::{BuildHasher, Hasher};
        let mut s = rapidhash::fast::SeedableState::fixed().build_hasher();
        s.write(k);
        s.finish()
    }

    fn e(k: &str, v: &str) -> Entry {
        Entry::new(Key::new(k.as_bytes()), v.as_bytes(), h(k.as_bytes()))
    }

    #[test]
    fn insert_get_remove_and_grow() {
        let idx = Index::new();
        let mut w = Writer::new();
        let g = epoch::pin();
        for i in 0..5000 {
            let k = format!("key{i}");
            assert!(
                idx.insert(&mut w, h(k.as_bytes()), e(&k, &i.to_string()), &g)
                    .is_none()
            );
        }
        assert_eq!(idx.len(), 5000);
        for i in 0..5000 {
            let k = format!("key{i}");
            let got = idx.get(h(k.as_bytes()), k.as_bytes(), &g).expect("present");
            assert_eq!(got.value(), i.to_string().as_bytes());
        }
        assert!(idx.get(h(b"nope"), b"nope", &g).is_none());
        let old = idx
            .insert(&mut w, h(b"key7"), e("key7", "new"), &g)
            .expect("replaced");
        assert_eq!(old.value(), b"7");
        assert_eq!(idx.get(h(b"key7"), b"key7", &g).unwrap().value(), b"new");
        assert_eq!(idx.len(), 5000);
        for i in 0..5000 {
            let k = format!("key{i}");
            assert!(
                idx.remove(&mut w, h(k.as_bytes()), k.as_bytes(), &g)
                    .is_some()
            );
        }
        assert_eq!(idx.len(), 0);
        assert!(idx.remove(&mut w, h(b"key1"), b"key1", &g).is_none());
    }

    #[test]
    fn remove_if_same_checks_identity() {
        let idx = Index::new();
        let mut w = Writer::new();
        let g = epoch::pin();
        let a = e("k", "a");
        let a2 = a.clone();
        idx.insert(&mut w, h(b"k"), a, &g);
        let b = e("k", "b");
        let old = idx.insert(&mut w, h(b"k"), b.clone(), &g).unwrap();
        assert!(
            idx.remove_if_same(&mut w, h(b"k"), &a2, &g).is_none(),
            "stale handle must not remove"
        );
        assert!(idx.remove_if_same(&mut w, h(b"k"), &b, &g).is_some());
        drop(old);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn concurrent_readers_never_miss_present_keys() {
        use std::sync::Arc;
        let idx = Arc::new(Index::new());
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut w = Writer::new();
        {
            let g = epoch::pin();
            for i in 0..200 {
                let k = format!("stable{i}");
                idx.insert(&mut w, h(k.as_bytes()), e(&k, "v"), &g);
            }
        }
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let (idx, stop) = (idx.clone(), stop.clone());
                std::thread::spawn(move || {
                    let mut n = 0u64;
                    while !stop.load(Relaxed) {
                        let g = epoch::pin();
                        for i in 0..200 {
                            let k = format!("stable{i}");
                            assert!(
                                idx.get(h(k.as_bytes()), k.as_bytes(), &g).is_some(),
                                "lost {k}"
                            );
                            n += 1;
                        }
                    }
                    n
                })
            })
            .collect();
        for round in 0..20 {
            let g = epoch::pin();
            for i in 0..2000 {
                let k = format!("churn{round}-{i}");
                idx.insert(&mut w, h(k.as_bytes()), e(&k, "x"), &g);
            }
            for i in 0..2000 {
                let k = format!("churn{round}-{i}");
                if let Some(old) = idx.remove(&mut w, h(k.as_bytes()), k.as_bytes(), &g) {
                    retire(old, &g);
                }
            }
        }
        stop.store(true, Relaxed);
        for r in readers {
            assert!(r.join().unwrap() > 0);
        }
    }
}
