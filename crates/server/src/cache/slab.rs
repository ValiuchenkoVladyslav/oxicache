//! Process-wide size-class slab for entry allocations.
//!
//! Entries are allocated on one worker thread and freed on whichever thread
//! next flushes the epoch collector, so the slab is global: per-class free
//! lists under short mutexes, fronted by unsynchronised per-thread
//! magazines that refill and flush in batches of ~[`BATCH_BYTES`]. Chunks
//! are carved from [`REGION`]-sized mappings aligned to 2 MiB and advised
//! `MADV_HUGEPAGE`, so the entry working set sits on huge pages (glibc's
//! heap left the dTLB missing about twice per looked-up key), and every
//! class size is a multiple of 64, so entries are cache-line aligned for
//! free — the padded-allocation trick this replaces is not needed.
//!
//! Sizes above [`MAX_SLAB`] are not served; the caller keeps those on the
//! global allocator. Memory is never returned to the kernel: the cache is
//! bounded by its byte budget, so the slab holds its high-water mark, the
//! way the arena of a long-lived glibc process does.

use std::cell::RefCell;
use std::ptr::NonNull;

use parking_lot::Mutex;

/// Largest chunk the slab serves.
pub const MAX_SLAB: usize = 64 << 10;
/// Sizes up to this are classed in 64-byte steps; beyond it, four classes
/// per doubling (at most 1/4 of a chunk is padding).
const LINEAR_MAX: usize = 1024;
const LINEAR: usize = LINEAR_MAX / 64;
/// Total classes: 16 linear plus 4 per doubling from 1 KiB to 64 KiB.
pub const CLASSES: usize =
    LINEAR + 4 * (MAX_SLAB.trailing_zeros() - LINEAR_MAX.trailing_zeros()) as usize;
/// A magazine refill or flush moves about this many bytes of chunks, so
/// list-lock traffic amortises over many operations for every class.
const BATCH_BYTES: usize = 64 << 10;
/// Bytes carved from the kernel at a time; a multiple of [`HUGE`].
const REGION: usize = 8 << 20;
const HUGE: usize = 2 << 20;

/// The class serving `size` bytes, or `None` above [`MAX_SLAB`].
#[inline]
pub fn class_of(size: usize) -> Option<usize> {
    debug_assert!(size > 0);
    if size <= LINEAR_MAX {
        return Some(size.div_ceil(64) - 1);
    }
    if size > MAX_SLAB {
        return None;
    }
    // For size in (2^k, 2^(k+1)], classes step by 2^(k-2).
    let k = (usize::BITS - 1 - (size - 1).leading_zeros()) as usize;
    let step = 1 << (k - 2);
    let sub = (size - (1 << k)).div_ceil(step);
    Some(LINEAR + 4 * (k - LINEAR_MAX.trailing_zeros() as usize) + sub - 1)
}

/// Chunk size of `class`; always a multiple of 64.
#[inline]
pub fn class_size(class: usize) -> usize {
    if class < LINEAR {
        return (class + 1) * 64;
    }
    let g = class - LINEAR;
    let k = LINEAR_MAX.trailing_zeros() as usize + g / 4;
    (1 << k) + (g % 4 + 1) * (1 << (k - 2))
}

/// Chunks a magazine moves per refill/flush.
#[inline]
fn batch(class: usize) -> usize {
    (BATCH_BYTES / class_size(class)).clamp(1, 32)
}

/// The shared side: per-class free lists and the bump region chunks are
/// carved from. Chunk addresses travel as `usize` so the containers stay
/// plain `Send + Sync` data.
struct Central {
    free: [Mutex<Vec<usize>>; CLASSES],
    /// `(cursor, end)` of the region being carved; `(0, 0)` before the first.
    bump: Mutex<(usize, usize)>,
}

static CENTRAL: Central = Central {
    free: [const { Mutex::new(Vec::new()) }; CLASSES],
    bump: Mutex::new((0, 0)),
};

impl Central {
    /// Move up to `n` chunks of `class` into `into` (at least one), from
    /// the free list if it has any, else carved fresh from the region.
    fn refill(&self, class: usize, into: &mut Vec<usize>, n: usize) {
        {
            let mut free = self.free[class].lock();
            if !free.is_empty() {
                let at = free.len() - free.len().min(n);
                into.extend(free.drain(at..));
                return;
            }
        }
        let size = class_size(class);
        let need = size * n;
        let mut bump = self.bump.lock();
        if bump.1 - bump.0 < need {
            // The tail that does not fit is wasted: under 1 % of a region.
            let base = map_region();
            *bump = (base, base + REGION);
        }
        let mut cur = bump.0;
        bump.0 += need;
        for _ in 0..n {
            into.push(cur);
            cur += size;
        }
    }

    /// Push everything past `keep` in `from` onto the free list.
    fn flush(&self, class: usize, from: &mut Vec<usize>, keep: usize) {
        self.free[class].lock().extend(from.drain(keep..));
    }
}

/// A `REGION`-byte mapping aligned to [`HUGE`] and advised towards huge
/// pages. Never unmapped; regions live as long as the process.
fn map_region() -> usize {
    #[cfg(unix)]
    // SAFETY: anonymous mapping; only the surplus head and tail of it are
    // unmapped, and nothing has handed out addresses in them yet.
    unsafe {
        let len = REGION + HUGE;
        let p = libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        );
        if p == libc::MAP_FAILED {
            std::alloc::handle_alloc_error(
                std::alloc::Layout::from_size_align(REGION, HUGE).expect("region layout"),
            );
        }
        let addr = p as usize;
        let aligned = (addr + HUGE - 1) & !(HUGE - 1);
        if aligned > addr {
            libc::munmap(p, aligned - addr);
        }
        let tail = addr + len - (aligned + REGION);
        if tail > 0 {
            libc::munmap((aligned + REGION) as *mut libc::c_void, tail);
        }
        #[cfg(target_os = "linux")]
        libc::madvise(aligned as *mut libc::c_void, REGION, libc::MADV_HUGEPAGE);
        aligned
    }
    #[cfg(not(unix))]
    // SAFETY: fresh allocation of a nonzero, valid layout; leaked on purpose.
    unsafe {
        let layout = std::alloc::Layout::from_size_align(REGION, 64).expect("region layout");
        let p = std::alloc::alloc(layout);
        if p.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        p as usize
    }
}

/// Per-thread chunk caches, one list per class; drained to [`CENTRAL`]
/// when the thread exits.
struct Magazines([Vec<usize>; CLASSES]);

impl Drop for Magazines {
    fn drop(&mut self) {
        for (class, v) in self.0.iter_mut().enumerate() {
            if !v.is_empty() {
                CENTRAL.flush(class, v, 0);
            }
        }
    }
}

thread_local! {
    static MAG: RefCell<Magazines> =
        RefCell::new(Magazines(std::array::from_fn(|_| Vec::new())));
}

/// A 64-byte-aligned chunk of at least `size` bytes and the class that
/// [`free`] takes back, or `None` for sizes beyond [`MAX_SLAB`].
#[inline]
pub fn alloc(size: usize) -> Option<(NonNull<u8>, u8)> {
    let class = class_of(size)?;
    let p = MAG
        .try_with(|m| {
            let mut m = m.borrow_mut();
            let v = &mut m.0[class];
            if let Some(p) = v.pop() {
                return p;
            }
            CENTRAL.refill(class, v, batch(class));
            v.pop().expect("refill returns at least one chunk")
        })
        .unwrap_or_else(|_| {
            // The thread is tearing down its TLS (an epoch flush on exit
            // can allocate-free through here); go to the lists directly.
            let mut one = Vec::with_capacity(1);
            CENTRAL.refill(class, &mut one, 1);
            one[0]
        });
    debug_assert_eq!(p % 64, 0);
    // SAFETY: chunk addresses come from `map_region`, never null.
    Some((unsafe { NonNull::new_unchecked(p as *mut u8) }, class as u8))
}

/// Return a chunk to its class. `p` must come from [`alloc`] with this
/// `class` and not have been freed since.
#[inline]
pub fn free(p: NonNull<u8>, class: u8) {
    let class = class as usize;
    debug_assert!(class < CLASSES);
    let done = MAG.try_with(|m| {
        let mut m = m.borrow_mut();
        let v = &mut m.0[class];
        v.push(p.as_ptr() as usize);
        let cap = 2 * batch(class);
        if v.len() > cap {
            CENTRAL.flush(class, v, cap / 2);
        }
    });
    if done.is_err() {
        CENTRAL.free[class].lock().push(p.as_ptr() as usize);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every size maps to a class whose chunk fits it, wastes at most a
    /// quarter (plus the 64-byte step), and is cache-line granular.
    #[test]
    fn classes_cover_all_sizes() {
        let mut prev = 0;
        for c in 0..CLASSES {
            let s = class_size(c);
            assert!(s > prev, "sizes must increase");
            assert_eq!(s % 64, 0);
            assert_eq!(class_of(s), Some(c), "a class serves its own size");
            prev = s;
        }
        assert_eq!(class_size(0), 64);
        assert_eq!(class_size(CLASSES - 1), MAX_SLAB);
        for size in 1..=MAX_SLAB {
            let c = class_of(size).expect("in range");
            let s = class_size(c);
            assert!(s >= size);
            assert!(s - size < size / 4 + 64, "waste at {size}: {}", s - size);
            if c > 0 {
                assert!(class_size(c - 1) < size, "not the tightest class");
            }
        }
        assert_eq!(class_of(MAX_SLAB + 1), None);
    }

    #[test]
    fn chunks_are_aligned_distinct_and_writable() {
        let mut held = Vec::new();
        for size in [1, 64, 65, 240, 1024, 1088, 4160, MAX_SLAB] {
            for _ in 0..10 {
                let (p, c) = alloc(size).expect("slab size");
                assert_eq!(p.as_ptr() as usize % 64, 0);
                assert!(class_size(c as usize) >= size);
                // SAFETY: the chunk is at least `size` writable bytes.
                unsafe {
                    std::ptr::write_bytes(p.as_ptr(), 0xAB, size);
                }
                held.push((p, c));
            }
        }
        let mut addrs: Vec<usize> = held.iter().map(|(p, _)| p.as_ptr() as usize).collect();
        addrs.sort_unstable();
        addrs.dedup();
        assert_eq!(addrs.len(), held.len(), "live chunks never overlap");
        for (p, c) in held {
            free(p, c);
        }
    }

    /// Chunks freed on other threads are reusable everywhere afterwards;
    /// exiting threads drain their magazines back to the shared lists.
    #[test]
    fn cross_thread_churn() {
        let (tx, rx) = std::sync::mpsc::sync_channel::<(usize, u8)>(64);
        let freer = std::thread::spawn(move || {
            for (addr, class) in rx {
                // SAFETY: forwarded straight from `alloc` on the other thread.
                free(unsafe { NonNull::new_unchecked(addr as *mut u8) }, class);
            }
        });
        for round in 0..1000 {
            let size = 64 + (round % 32) * 40;
            let (p, c) = alloc(size).unwrap();
            // SAFETY: chunk is at least `size` bytes.
            unsafe { std::ptr::write_bytes(p.as_ptr(), round as u8, size) };
            tx.send((p.as_ptr() as usize, c)).unwrap();
        }
        drop(tx);
        freer.join().unwrap();
        let (p, c) = alloc(128).unwrap();
        free(p, c);
    }
}
