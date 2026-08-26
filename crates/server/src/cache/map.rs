//! The concurrent index: one `dashmap` table for the whole cache (papaya was
//! measured as an alternative and lost on every profile once batch lookups
//! prefetched entries; see docs/hashmap-bench.md). Keys are [`Key`] (short
//! keys inline in the bucket); values are [`Entry`] (one allocation).

use super::key::Key;
use super::s3fifo::Entry;

type Hasher = foldhash::fast::RandomState;

/// What a [`Reader`] hands out: an owned handle (refcount bump), since
/// dashmap guards must not be held across other lookups.
pub type EntryRef<'a> = Entry;

pub struct Map(dashmap::DashMap<Key, Entry, Hasher>);

/// A view for a batch of reads.
pub struct Reader<'a>(&'a Map);

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
