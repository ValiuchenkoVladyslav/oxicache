//! Typed API, enabled by the `serde` feature: `Client`'s `get`/`set`/`del`
//! and their `_multi` forms take any `Serialize` key or value and decode
//! into any `DeserializeOwned` type (MessagePack via `rmp-serde`, compact
//! form). The byte-level API stays available as [`Client::raw`]; a `&str`
//! key stored through it is a different key from the same `&str` stored
//! through the typed API, which MessagePack-encodes it.
//!
//! The `_multi` forms accept three key shapes (see [`Keys`]):
//!
//! - a tuple of up to 16 keys, with one value type per key:
//!   `get_multi::<_, (User, u64)>(("user:7", "hits:7"))` →
//!   `(Option<User>, Option<u64>)`
//! - an array `[K; N]`, one value type: `[Option<V>; N]`
//! - a `Vec<K>` or `&[K]`, one value type: `Vec<Option<V>>`
//!
//! ```ignore
//! let user: Option<User> = client.get("user:7").await?;
//! let (user, hits): (Option<User>, Option<u64>) =
//!     client.get_multi(("user:7", "hits:7")).await?;
//! let users: Vec<Option<User>> = client.get_multi(ids).await?;
//! client.set_multi([("a", 1), ("b", 2)]).await?;
//! ```

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{Client, Error, Result};

fn encode<K: Serialize>(key: K) -> Result<Vec<u8>> {
    Ok(rmp_serde::to_vec(&key)?)
}

fn decode<V: DeserializeOwned>(value: Option<Bytes>) -> Result<Option<V>> {
    Ok(value.map(|v| rmp_serde::from_slice(&v)).transpose()?)
}

fn expect_count<T>(values: &[T], expected: usize) -> Result<()> {
    if values.len() == expected {
        Ok(())
    } else {
        Err(Error::Count {
            expected,
            got: values.len(),
        })
    }
}

/// A batch of keys for the `_multi` methods: a tuple `(K1, K2, …)` of up to
/// 16 keys, an array `[K; N]`, a `Vec<K>` or a `&[K]`.
pub trait Keys {
    /// The `del_multi` result: `[bool; N]` for tuples and arrays, `Vec<bool>` otherwise.
    type Flags;
    fn encode(self) -> Result<Vec<Vec<u8>>>;
    fn flags(flags: Vec<bool>) -> Result<Self::Flags>;
}

/// Pairs a key batch with what its values decode to. `Vs` is a tuple with
/// one type per key for tuple batches (so `get_multi::<_, (A, B)>((k1, k2,
/// k3))` does not compile) and a single type for arrays and vectors.
pub trait ValuesFor<Vs>: Keys {
    /// `(Option<V1>, Option<V2>, …)`, `[Option<V>; N]` or `Vec<Option<V>>`.
    type Output;
    fn decode(values: Vec<Option<Bytes>>) -> Result<Self::Output>;
}

macro_rules! impl_tuples {
    ($($n:literal => ($($i:tt $K:ident $V:ident),+);)+) => {$(
        impl<$($K: Serialize),+> Keys for ($($K,)+) {
            type Flags = [bool; $n];
            fn encode(self) -> Result<Vec<Vec<u8>>> {
                Ok(vec![$(encode(self.$i)?),+])
            }
            fn flags(flags: Vec<bool>) -> Result<[bool; $n]> {
                expect_count(&flags, $n)?;
                Ok([$(flags[$i]),+])
            }
        }
        impl<$($K: Serialize, $V: DeserializeOwned),+> ValuesFor<($($V,)+)> for ($($K,)+) {
            type Output = ($(Option<$V>,)+);
            fn decode(values: Vec<Option<Bytes>>) -> Result<Self::Output> {
                expect_count(&values, $n)?;
                let mut it = values.into_iter();
                Ok(($(decode::<$V>(it.next().unwrap())?,)+))
            }
        }
    )+};
}

impl_tuples! {
    1 => (0 K0 V0);
    2 => (0 K0 V0, 1 K1 V1);
    3 => (0 K0 V0, 1 K1 V1, 2 K2 V2);
    4 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3);
    5 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4);
    6 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5);
    7 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5, 6 K6 V6);
    8 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5, 6 K6 V6, 7 K7 V7);
    9 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5, 6 K6 V6, 7 K7 V7, 8 K8 V8);
    10 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5, 6 K6 V6, 7 K7 V7, 8 K8 V8, 9 K9 V9);
    11 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5, 6 K6 V6, 7 K7 V7, 8 K8 V8, 9 K9 V9, 10 K10 V10);
    12 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5, 6 K6 V6, 7 K7 V7, 8 K8 V8, 9 K9 V9, 10 K10 V10, 11 K11 V11);
    13 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5, 6 K6 V6, 7 K7 V7, 8 K8 V8, 9 K9 V9, 10 K10 V10, 11 K11 V11, 12 K12 V12);
    14 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5, 6 K6 V6, 7 K7 V7, 8 K8 V8, 9 K9 V9, 10 K10 V10, 11 K11 V11, 12 K12 V12, 13 K13 V13);
    15 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5, 6 K6 V6, 7 K7 V7, 8 K8 V8, 9 K9 V9, 10 K10 V10, 11 K11 V11, 12 K12 V12, 13 K13 V13, 14 K14 V14);
    16 => (0 K0 V0, 1 K1 V1, 2 K2 V2, 3 K3 V3, 4 K4 V4, 5 K5 V5, 6 K6 V6, 7 K7 V7, 8 K8 V8, 9 K9 V9, 10 K10 V10, 11 K11 V11, 12 K12 V12, 13 K13 V13, 14 K14 V14, 15 K15 V15);
}

impl<K: Serialize, const N: usize> Keys for [K; N] {
    type Flags = [bool; N];
    fn encode(self) -> Result<Vec<Vec<u8>>> {
        self.into_iter().map(encode).collect()
    }
    fn flags(flags: Vec<bool>) -> Result<[bool; N]> {
        flags.try_into().map_err(|f: Vec<bool>| Error::Count {
            expected: N,
            got: f.len(),
        })
    }
}

impl<K: Serialize, V: DeserializeOwned, const N: usize> ValuesFor<V> for [K; N] {
    type Output = [Option<V>; N];
    fn decode(values: Vec<Option<Bytes>>) -> Result<Self::Output> {
        expect_count(&values, N)?;
        let decoded = values.into_iter().map(decode).collect::<Result<Vec<_>>>()?;
        Ok(decoded.try_into().unwrap_or_else(|_| unreachable!()))
    }
}

impl<K: Serialize> Keys for Vec<K> {
    type Flags = Vec<bool>;
    fn encode(self) -> Result<Vec<Vec<u8>>> {
        self.into_iter().map(encode).collect()
    }
    fn flags(flags: Vec<bool>) -> Result<Vec<bool>> {
        Ok(flags)
    }
}

impl<K: Serialize, V: DeserializeOwned> ValuesFor<V> for Vec<K> {
    type Output = Vec<Option<V>>;
    fn decode(values: Vec<Option<Bytes>>) -> Result<Self::Output> {
        values.into_iter().map(decode).collect()
    }
}

impl<K: Serialize> Keys for &[K] {
    type Flags = Vec<bool>;
    fn encode(self) -> Result<Vec<Vec<u8>>> {
        self.iter().map(encode).collect()
    }
    fn flags(flags: Vec<bool>) -> Result<Vec<bool>> {
        Ok(flags)
    }
}

impl<K: Serialize, V: DeserializeOwned> ValuesFor<V> for &[K] {
    type Output = Vec<Option<V>>;
    fn decode(values: Vec<Option<Bytes>>) -> Result<Self::Output> {
        values.into_iter().map(decode).collect()
    }
}

impl Client {
    /// Fetch one key, decoding its value as `V`.
    pub async fn get<K: Serialize, V: DeserializeOwned>(&self, key: K) -> Result<Option<V>> {
        decode(self.raw().get(&encode(key)?).await?)
    }

    /// Fetch a batch of keys. For a tuple of keys `Vs` is a tuple with one
    /// value type per key; for an array, `Vec` or slice it is the single
    /// value type. `Vs` is inferred when the binding is annotated:
    /// `let (u, n): (Option<User>, Option<u64>) = c.get_multi(("u", "n")).await?;`
    pub async fn get_multi<Ks: ValuesFor<Vs>, Vs>(&self, keys: Ks) -> Result<Ks::Output> {
        let keys = keys.encode()?;
        let values = self.raw().get_multi(keys.iter().map(Vec::as_slice)).await?;
        expect_count(&values, keys.len())?;
        Ks::decode(values)
    }

    /// Store one key/value pair.
    pub async fn set<K: Serialize, V: Serialize>(&self, key: K, value: V) -> Result<()> {
        self.raw().set(&encode(key)?, &encode(value)?).await
    }

    /// Store many key/value pairs from any iterator of `(key, value)`.
    pub async fn set_multi<K, V, I>(&self, entries: I) -> Result<()>
    where
        K: Serialize,
        V: Serialize,
        I: IntoIterator<Item = (K, V)>,
    {
        let entries = entries
            .into_iter()
            .map(|(k, v)| Ok((encode(k)?, encode(v)?)))
            .collect::<Result<Vec<_>>>()?;
        self.raw()
            .set_multi(entries.iter().map(|(k, v)| (k.as_slice(), v.as_slice())))
            .await
    }

    /// Delete one key; returns whether it existed.
    pub async fn del<K: Serialize>(&self, key: K) -> Result<bool> {
        self.raw().del(&encode(key)?).await
    }

    /// Delete a batch of keys; `[bool; N]` for tuples and arrays,
    /// `Vec<bool>` for a `Vec` or slice.
    pub async fn del_multi<Ks: Keys>(&self, keys: Ks) -> Result<Ks::Flags> {
        let keys = keys.encode()?;
        let flags = self.raw().del_multi(keys.iter().map(Vec::as_slice)).await?;
        expect_count(&flags, keys.len())?;
        Ks::flags(flags)
    }
}
