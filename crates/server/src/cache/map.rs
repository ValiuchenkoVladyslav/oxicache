//! Thin wrapper around the concurrent map used per shard, so the backing
//! crate can be swapped in one place. Only clone-out accessors are exposed so
//! no dashmap guard can outlive a single expression (see docs/hashmap-bench.md).

use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;

use super::s3fifo::Entry;

type Hasher = foldhash::fast::RandomState;

pub struct Map(DashMap<Bytes, Arc<Entry>, Hasher>);

impl Map {
    pub fn new() -> Self {
        Self(DashMap::with_hasher(Hasher::default()))
    }

    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Arc<Entry>> {
        self.0.get(key).map(|g| g.value().clone())
    }

    /// Insert, replacing both key and value when present: the stored key may
    /// share an allocation with the old value, so it must not be retained.
    #[inline]
    pub fn insert(&self, key: Bytes, entry: Arc<Entry>) -> Option<Arc<Entry>> {
        match self.0.entry(key) {
            dashmap::Entry::Occupied(o) => Some(o.replace_entry(entry).1),
            dashmap::Entry::Vacant(v) => {
                v.insert(entry);
                None
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overwrite_replaces_key_object() {
        let m = Map::new();
        let a = Bytes::from_static(b"key");
        let b = Bytes::from(b"key".to_vec());
        m.insert(a.clone(), Entry::for_test(a.clone()));
        m.insert(b.clone(), Entry::for_test(b.clone()));
        let stored = m.0.get(&b"key"[..]).map(|r| r.key().as_ptr()).unwrap();
        assert_eq!(stored, b.as_ptr(), "old key object must not be retained");
    }
}
