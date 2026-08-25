//! The concurrent index: one table for the whole cache. Default backend is
//! `dashmap` (fastest inserts); `--features papaya` swaps in a lock-free
//! table whose reads are pinned once per batch (see docs/hashmap-bench.md).
//! Keys are [`Key`] (short keys inline in the bucket); values are [`Entry`]
//! (one allocation).

use super::key::Key;
use super::s3fifo::Entry;

type Hasher = foldhash::fast::RandomState;

/// What a [`Reader`] hands out: a borrow under papaya's guard, or an owned
/// handle (refcount bump) with dashmap, whose guards must not be held across
/// other lookups.
#[cfg(feature = "papaya")]
pub type EntryRef<'a> = &'a Entry;

#[cfg(not(feature = "papaya"))]
/// Owned handle: dashmap guards must not be held across other lookups.
pub type EntryRef<'a> = Entry;
#[cfg(feature = "papaya")]
type Table = papaya::HashMap<Key, Entry, Hasher>;

#[cfg(feature = "papaya")]
pub struct Map(Table);

/// A pinned view for a batch of reads. Entries borrowed from it stay valid
/// until it is dropped, even if removed concurrently.
#[cfg(feature = "papaya")]
pub struct Reader<'a>(papaya::HashMapRef<'a, Key, Entry, Hasher, papaya::LocalGuard<'a>>);

#[cfg(feature = "papaya")]
impl Map {
    pub fn new() -> Self {
        Self(papaya::HashMap::builder().hasher(Hasher::default()).build())
    }

    #[inline]
    pub fn read(&self) -> Reader<'_> {
        Reader(self.0.pin())
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

    /// Remove `key` only if it still maps to exactly `entry`.
    #[inline]
    pub fn remove_if_same(&self, key: &[u8], entry: &Entry) {
        let _ = self.0.pin().remove_if(key, |_, e| Entry::ptr_eq(e, entry));
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[cfg(feature = "papaya")]
impl Reader<'_> {
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<EntryRef<'_>> {
        self.0.get(key)
    }
}

#[cfg(not(feature = "papaya"))]
pub struct Map(dashmap::DashMap<Key, Entry, Hasher>);

#[cfg(not(feature = "papaya"))]
pub struct Reader<'a>(&'a Map);

#[cfg(not(feature = "papaya"))]
impl Map {
    pub fn new() -> Self {
        Self(dashmap::DashMap::with_hasher(Hasher::default()))
    }

    #[inline]
    pub fn read(&self) -> Reader<'_> {
        Reader(self)
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

    #[inline]
    pub fn remove_if_same(&self, key: &[u8], entry: &Entry) {
        self.0.remove_if(key, |_, e| Entry::ptr_eq(e, entry));
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[cfg(not(feature = "papaya"))]
impl Reader<'_> {
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<EntryRef<'_>> {
        self.0.get(key)
    }
}

impl Default for Map {
    fn default() -> Self {
        Self::new()
    }
}
