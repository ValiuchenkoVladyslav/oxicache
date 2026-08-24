//! Thin wrapper around the concurrent map used per shard, so the backing
//! crate can be swapped in one place.

use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;

use super::s3fifo::Entry;

type Hasher = foldhash::fast::FixedState;

pub struct Map(DashMap<Bytes, Arc<Entry>, Hasher>);

impl Map {
    pub fn new() -> Self {
        Self(DashMap::with_hasher(Hasher::default()))
    }

    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Arc<Entry>> {
        self.0.get(key).map(|g| g.value().clone())
    }

    #[inline]
    pub fn insert(&self, key: Bytes, entry: Arc<Entry>) -> Option<Arc<Entry>> {
        self.0.insert(key, entry)
    }

    #[inline]
    pub fn remove(&self, key: &[u8]) -> Option<Arc<Entry>> {
        self.0.remove(key).map(|(_, e)| e)
    }

    /// Remove `key` only if it still maps to exactly `entry`.
    #[inline]
    pub fn remove_if_same(&self, key: &[u8], entry: &Arc<Entry>) {
        self.0.remove_if(key, |_, e| Arc::ptr_eq(e, entry));
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}
