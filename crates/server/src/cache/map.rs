//! Thin wrapper around the concurrent map used per shard, so the backing
//! crate can be swapped in one place. Only clone-out accessors are exposed so
//! no map guard can outlive a single expression (see docs/hashmap-bench.md).
//! Keys are [`Key`] (short keys inline in the bucket); values are one `Arc`
//! per entry holding key and value bytes in a single allocation.

use super::key::Key;
use super::s3fifo::Entry;

#[cfg(not(feature = "papaya"))]
impl Default for Map {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "papaya")]
impl Default for Map {
    fn default() -> Self {
        Self::new()
    }
}

type Hasher = foldhash::fast::RandomState;

#[cfg(not(feature = "papaya"))]
pub struct Map(dashmap::DashMap<Key, Entry, Hasher>);

#[cfg(not(feature = "papaya"))]
impl Map {
    pub fn new() -> Self {
        Self(dashmap::DashMap::with_hasher(Hasher::default()))
    }

    #[inline]
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        self.0.get(key).map(|g| g.value().clone())
    }

    #[inline]
    pub fn insert(&self, key: Key, entry: Entry) -> Option<Entry> {
        self.0.insert(key, entry)
    }

    #[inline]
    pub fn remove(&self, key: &[u8]) -> Option<Entry> {
        self.0.remove(key).map(|(_, e)| e)
    }

    /// Remove `key` only if it still maps to exactly `entry`.
    #[inline]
    pub fn remove_if_same(&self, key: &[u8], entry: &Entry) {
        self.0.remove_if(key, |_, e| Entry::ptr_eq(e, entry));
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[cfg(feature = "papaya")]
pub struct Map(papaya::HashMap<Key, Entry, Hasher>);

#[cfg(feature = "papaya")]
impl Map {
    pub fn new() -> Self {
        Self(papaya::HashMap::builder().hasher(Hasher::default()).build())
    }

    #[inline]
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        self.0.pin().get(key).cloned()
    }

    #[inline]
    pub fn insert(&self, key: Key, entry: Entry) -> Option<Entry> {
        self.0.pin().insert(key, entry).cloned()
    }

    #[inline]
    pub fn remove(&self, key: &[u8]) -> Option<Entry> {
        self.0.pin().remove(key).cloned()
    }

    #[inline]
    pub fn remove_if_same(&self, key: &[u8], entry: &Entry) {
        let _ = self.0.pin().remove_if(key, |_, e| Entry::ptr_eq(e, entry));
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}
