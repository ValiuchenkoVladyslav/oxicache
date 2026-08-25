//! Thin wrapper around the concurrent map used per shard, so the backing
//! crate can be swapped in one place. Only clone-out accessors are exposed so
//! no map guard can outlive a single expression (see docs/hashmap-bench.md).

use std::sync::Arc;

use bytes::Bytes;

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
pub struct Map(dashmap::DashMap<Bytes, Arc<Entry>, Hasher>);

#[cfg(not(feature = "papaya"))]
impl Map {
    /// Whether `insert` replaces the stored key object on overwrite. When it
    /// does, keys may share an allocation with their value.
    pub const REPLACES_KEY: bool = true;

    pub fn new() -> Self {
        Self(dashmap::DashMap::with_hasher(Hasher::default()))
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
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

    #[cfg(test)]
    fn key_ptr(&self, key: &[u8]) -> Option<*const u8> {
        self.0.get(key).map(|r| r.key().as_ptr())
    }
}

#[cfg(feature = "papaya")]
pub struct Map(papaya::HashMap<Bytes, Arc<Entry>, Hasher>);

#[cfg(feature = "papaya")]
impl Map {
    /// papaya keeps the existing key object on overwrite.
    pub const REPLACES_KEY: bool = false;

    pub fn new() -> Self {
        Self(papaya::HashMap::builder().hasher(Hasher::default()).build())
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Arc<Entry>> {
        self.0.pin().get(key).cloned()
    }

    #[inline]
    pub fn insert(&self, key: Bytes, entry: Arc<Entry>) -> Option<Arc<Entry>> {
        self.0.pin().insert(key, entry).cloned()
    }

    #[inline]
    pub fn remove(&self, key: &[u8]) -> Option<Arc<Entry>> {
        self.0.pin().remove(key).cloned()
    }

    #[inline]
    pub fn remove_if_same(&self, key: &[u8], entry: &Arc<Entry>) {
        let _ = self.0.pin().remove_if(key, |_, e| Arc::ptr_eq(e, entry));
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[cfg(test)]
    fn key_ptr(&self, key: &[u8]) -> Option<*const u8> {
        self.0.pin().get_key_value(key).map(|(k, _)| k.as_ptr())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overwrite_key_retention_matches_contract() {
        let m = Map::new();
        let a = Bytes::from_static(b"key");
        let b = Bytes::from(b"key".to_vec());
        m.insert(a.clone(), Entry::for_test(a.clone()));
        m.insert(b.clone(), Entry::for_test(b.clone()));
        let stored = m.key_ptr(b"key").unwrap();
        if Map::REPLACES_KEY {
            assert_eq!(stored, b.as_ptr(), "old key object must not be retained");
        } else {
            assert!(stored == a.as_ptr() || stored == b.as_ptr());
        }
    }
}
