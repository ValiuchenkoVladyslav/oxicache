//! One S3-FIFO shard: a concurrent index plus FIFO queues guarded by a mutex.
//!
//! Reads never take the mutex: they hit the index, bump a relaxed atomic
//! frequency counter (capped at 3) and clone the value handle. Writes take a
//! short critical section to push onto the small/main queues and evict.
//!
//! Entries are immutable once created; removing one marks it `dead` and it is
//! skipped lazily when it reaches the head of its queue. Dead bytes are tracked
//! and the queues are compacted when they accumulate.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering::Relaxed};

use bytes::Bytes;
use parking_lot::Mutex;

use super::map::Map;

/// Maximum frequency value tracked per entry.
const FREQ_CAP: u8 = 3;
/// Approximate per-entry bookkeeping overhead in bytes (Arc, atomics, index slot).
const ENTRY_OVERHEAD: usize = 96;
/// Minimum number of ghost hashes retained regardless of main queue length.
const GHOST_MIN: usize = 64;

pub struct Entry {
    key: Bytes,
    value: Bytes,
    hash: u64,
    freq: AtomicU8,
    live: AtomicBool,
}

impl Entry {
    #[inline]
    fn cost(&self) -> usize {
        self.key.len() + self.value.len() + ENTRY_OVERHEAD
    }

    #[inline]
    fn touch(&self) {
        let f = self.freq.load(Relaxed);
        if f < FREQ_CAP {
            self.freq.store(f + 1, Relaxed);
        }
    }
}

struct Ghost {
    order: VecDeque<u64>,
    set: HashSet<u64, foldhash::fast::FixedState>,
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
    small: VecDeque<Arc<Entry>>,
    main: VecDeque<Arc<Entry>>,
    ghost: Ghost,
    /// Bytes of live entries across both queues.
    used: usize,
    /// Bytes held by the small queue (live and dead).
    small_bytes: usize,
    /// Bytes held by dead entries still sitting in either queue.
    dead_bytes: usize,
}

pub struct Shard {
    map: Map,
    q: Mutex<Queues>,
    capacity: usize,
    small_capacity: usize,
}

impl Shard {
    /// `capacity` is the byte budget of live entries; the small queue gets 10%.
    pub fn new(capacity: usize) -> Self {
        Self {
            map: Map::new(),
            q: Mutex::new(Queues {
                small: VecDeque::new(),
                main: VecDeque::new(),
                ghost: Ghost { order: VecDeque::new(), set: HashSet::default() },
                used: 0,
                small_bytes: 0,
                dead_bytes: 0,
            }),
            capacity,
            small_capacity: capacity / 10,
        }
    }

    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let e = self.map.get(key)?;
        e.touch();
        Some(e.value.clone())
    }

    pub fn set(&self, key: Bytes, value: Bytes, hash: u64) {
        let entry = Arc::new(Entry {
            key: key.clone(),
            value,
            hash,
            freq: AtomicU8::new(0),
            live: AtomicBool::new(true),
        });
        let cost = entry.cost();

        let mut q = self.q.lock();
        while q.used + cost > self.capacity {
            let evicted = if q.small_bytes >= self.small_capacity {
                self.evict_small(&mut q) || self.evict_main(&mut q)
            } else {
                self.evict_main(&mut q) || self.evict_small(&mut q)
            };
            if !evicted {
                break;
            }
        }
        if let Some(old) = self.map.insert(key, entry.clone()) {
            self.kill(&mut q, &old);
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

    pub fn del(&self, key: &[u8]) -> bool {
        let Some(e) = self.map.remove(key) else { return false };
        let mut q = self.q.lock();
        self.kill(&mut q, &e);
        self.maybe_compact(&mut q);
        true
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn used_bytes(&self) -> usize {
        self.q.lock().used
    }

    /// Mark an entry removed from the index as dead and release its budget.
    fn kill(&self, q: &mut Queues, e: &Entry) {
        if e.live.swap(false, Relaxed) {
            let cost = e.cost();
            q.used -= cost;
            q.dead_bytes += cost;
        }
    }

    fn evict_small(&self, q: &mut Queues) -> bool {
        let Some(e) = q.small.pop_front() else { return false };
        let cost = e.cost();
        q.small_bytes -= cost;
        if !e.live.load(Relaxed) {
            q.dead_bytes -= cost;
        } else if e.freq.load(Relaxed) > 1 {
            e.freq.store(0, Relaxed);
            q.main.push_back(e);
        } else {
            self.map.remove_if_same(&e.key, &e);
            e.live.store(false, Relaxed);
            q.used -= cost;
            let limit = q.main.len();
            q.ghost.push(e.hash, limit);
        }
        true
    }

    fn evict_main(&self, q: &mut Queues) -> bool {
        loop {
            let Some(e) = q.main.pop_front() else { return false };
            let cost = e.cost();
            if !e.live.load(Relaxed) {
                q.dead_bytes -= cost;
                return true;
            }
            let f = e.freq.load(Relaxed);
            if f > 0 {
                e.freq.store(f - 1, Relaxed);
                q.main.push_back(e);
                continue;
            }
            self.map.remove_if_same(&e.key, &e);
            e.live.store(false, Relaxed);
            q.used -= cost;
            return true;
        }
    }

    /// Drop dead entries eagerly once they hold a quarter of the budget.
    fn maybe_compact(&self, q: &mut Queues) {
        if q.dead_bytes <= self.capacity / 4 {
            return;
        }
        q.small.retain(|e| e.live.load(Relaxed));
        q.main.retain(|e| e.live.load(Relaxed));
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

    fn key(i: usize) -> Bytes {
        Bytes::from(format!("key-{i:06}"))
    }

    fn set(s: &Shard, i: usize, vlen: usize) {
        s.set(key(i), Bytes::from(vec![0u8; vlen]), i as u64);
    }

    #[test]
    fn get_set_del() {
        let s = shard(1 << 20);
        assert_eq!(s.get(b"a"), None);
        s.set(Bytes::from_static(b"a"), Bytes::from_static(b"1"), 1);
        assert_eq!(s.get(b"a").as_deref(), Some(&b"1"[..]));
        s.set(Bytes::from_static(b"a"), Bytes::from_static(b"2"), 1);
        assert_eq!(s.get(b"a").as_deref(), Some(&b"2"[..]));
        assert!(s.del(b"a"));
        assert!(!s.del(b"a"));
        assert_eq!(s.get(b"a"), None);
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
        assert!(s.get(&key(999)).is_some());
        assert!(s.get(&key(0)).is_none());
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
                s.get(&key(i));
            }
        }
        for i in 10..2000 {
            set(&s, i, 100);
        }
        let survivors = (0..10).filter(|&i| s.get(&key(i)).is_some()).count();
        assert_eq!(survivors, 10, "hot keys should be promoted to main and survive a scan");
    }

    #[test]
    fn ghost_hit_goes_to_main() {
        let cap = 100 * (ENTRY_OVERHEAD + 10 + 100);
        let s = shard(cap);
        set(&s, 0, 100);
        for i in 1..150 {
            set(&s, i, 100);
        }
        assert!(s.get(&key(0)).is_none());
        set(&s, 0, 100);
        assert!(s.q.lock().main.iter().any(|e| e.key == key(0)), "ghost hit should insert into main");
    }

    #[test]
    fn delete_frees_budget_and_compacts() {
        let cap = 100 * (ENTRY_OVERHEAD + 10 + 100);
        let s = shard(cap);
        for i in 0..100 {
            set(&s, i, 100);
        }
        for i in 0..100 {
            assert!(s.del(&key(i)));
        }
        assert_eq!(s.used_bytes(), 0);
        let q = s.q.lock();
        assert!(q.dead_bytes <= cap / 4, "compaction should bound dead bytes");
        assert!(q.small.len() + q.main.len() <= 25);
    }

    #[test]
    fn oversized_entry_is_only_resident() {
        let s = shard(1000);
        set(&s, 0, 100);
        set(&s, 1, 5000);
        assert!(s.get(&key(0)).is_none());
        assert!(s.get(&key(1)).is_some());
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn concurrent_stress() {
        let s = Arc::new(shard(200 * 1024));
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let s = s.clone();
                std::thread::spawn(move || {
                    for i in 0..20_000 {
                        let k = (i * 7 + t) % 2000;
                        match i % 10 {
                            0..=6 => {
                                s.get(&key(k));
                            }
                            7 | 8 => set(&s, k, 64),
                            _ => {
                                s.del(&key(k));
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
        let live: usize = q.small.iter().chain(q.main.iter()).filter(|e| e.live.load(Relaxed)).map(|e| e.cost()).sum();
        assert_eq!(live, q.used, "accounting must match live entries");
        assert_eq!(s.map.len(), q.small.iter().chain(q.main.iter()).filter(|e| e.live.load(Relaxed)).count());
    }
}
