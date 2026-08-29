//! Typed API, enabled by the `serde` feature: [`Typed`] wraps a [`Client`]
//! and a [`Format`] — any serde data format the caller picks — and its
//! `get`/`set`/`del` and `_multi` forms take any `Serialize` key or value
//! and decode into any `DeserializeOwned` type. The crate ships no format of
//! its own; implementing [`Format`] is two one-line methods around e.g.
//! `serde_json`, `rmp_serde` or `postcard`. Keys go through the format too,
//! so a `&str` key stored through `Typed` is a different key from the same
//! `&str` stored as bytes through the [`Client`].
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
//! struct Json;
//! impl Format for Json {
//!     fn encode<T: Serialize + ?Sized>(&self, v: &T) -> Result<Vec<u8>, BoxError> {
//!         Ok(serde_json::to_vec(v)?)
//!     }
//!     fn decode<T: DeserializeOwned>(&self, b: &[u8]) -> Result<T, BoxError> {
//!         Ok(serde_json::from_slice(b)?)
//!     }
//! }
//! let c = client.typed(Json);
//! let user: Option<User> = c.get("user:7").await?;
//! let (user, hits): (Option<User>, Option<u64>) =
//!     c.get_multi(("user:7", "hits:7")).await?;
//! let users: Vec<Option<User>> = c.get_multi(ids).await?;
//! c.set_multi([("a", 1), ("b", 2)]).await?;
//! ```

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{BoxError, Client, Error, Result};

/// A serde data format: how keys and values become bytes and back. Any
/// format works as long as `decode(encode(x)) == x`; the cache never looks
/// inside.
pub trait Format {
    fn encode<T: Serialize + ?Sized>(&self, value: &T) -> std::result::Result<Vec<u8>, BoxError>;
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> std::result::Result<T, BoxError>;
}

fn encode<F: Format, K: Serialize>(f: &F, key: K) -> Result<Vec<u8>> {
    f.encode(&key).map_err(Error::Serialize)
}

fn decode<F: Format, V: DeserializeOwned>(f: &F, value: Option<Bytes>) -> Result<Option<V>> {
    value
        .map(|v| f.decode(&v).map_err(Error::Deserialize))
        .transpose()
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
    fn encode<F: Format>(self, f: &F) -> Result<Vec<Vec<u8>>>;
    fn flags(flags: Vec<bool>) -> Result<Self::Flags>;
}

/// Pairs a key batch with what its values decode to. `Vs` is a tuple with
/// one type per key for tuple batches (so `get_multi::<_, (A, B)>((k1, k2,
/// k3))` does not compile) and a single type for arrays and vectors.
pub trait ValuesFor<Vs>: Keys {
    /// `(Option<V1>, Option<V2>, …)`, `[Option<V>; N]` or `Vec<Option<V>>`.
    type Output;
    fn decode<F: Format>(f: &F, values: Vec<Option<Bytes>>) -> Result<Self::Output>;
}

macro_rules! impl_tuples {
    ($($n:literal => ($($i:tt $K:ident $V:ident),+);)+) => {$(
        impl<$($K: Serialize),+> Keys for ($($K,)+) {
            type Flags = [bool; $n];
            fn encode<F: Format>(self, f: &F) -> Result<Vec<Vec<u8>>> {
                Ok(vec![$(encode(f, self.$i)?),+])
            }
            fn flags(flags: Vec<bool>) -> Result<[bool; $n]> {
                expect_count(&flags, $n)?;
                Ok([$(flags[$i]),+])
            }
        }
        impl<$($K: Serialize, $V: DeserializeOwned),+> ValuesFor<($($V,)+)> for ($($K,)+) {
            type Output = ($(Option<$V>,)+);
            fn decode<F: Format>(f: &F, values: Vec<Option<Bytes>>) -> Result<Self::Output> {
                expect_count(&values, $n)?;
                let mut it = values.into_iter();
                Ok(($(decode::<F, $V>(f, it.next().unwrap())?,)+))
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
    fn encode<F: Format>(self, f: &F) -> Result<Vec<Vec<u8>>> {
        self.into_iter().map(|k| encode(f, k)).collect()
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
    fn decode<F: Format>(f: &F, values: Vec<Option<Bytes>>) -> Result<Self::Output> {
        expect_count(&values, N)?;
        let decoded = values
            .into_iter()
            .map(|v| decode(f, v))
            .collect::<Result<Vec<_>>>()?;
        Ok(decoded.try_into().unwrap_or_else(|_| unreachable!()))
    }
}

impl<K: Serialize> Keys for Vec<K> {
    type Flags = Vec<bool>;
    fn encode<F: Format>(self, f: &F) -> Result<Vec<Vec<u8>>> {
        self.into_iter().map(|k| encode(f, k)).collect()
    }
    fn flags(flags: Vec<bool>) -> Result<Vec<bool>> {
        Ok(flags)
    }
}

impl<K: Serialize, V: DeserializeOwned> ValuesFor<V> for Vec<K> {
    type Output = Vec<Option<V>>;
    fn decode<F: Format>(f: &F, values: Vec<Option<Bytes>>) -> Result<Self::Output> {
        values.into_iter().map(|v| decode(f, v)).collect()
    }
}

impl<K: Serialize> Keys for &[K] {
    type Flags = Vec<bool>;
    fn encode<F: Format>(self, f: &F) -> Result<Vec<Vec<u8>>> {
        self.iter().map(|k| encode(f, k)).collect()
    }
    fn flags(flags: Vec<bool>) -> Result<Vec<bool>> {
        Ok(flags)
    }
}

impl<K: Serialize, V: DeserializeOwned> ValuesFor<V> for &[K] {
    type Output = Vec<Option<V>>;
    fn decode<F: Format>(f: &F, values: Vec<Option<Bytes>>) -> Result<Self::Output> {
        values.into_iter().map(|v| decode(f, v)).collect()
    }
}

/// A [`Client`] paired with a [`Format`]; see the module docs. Cheap to
/// clone when `F` is.
#[derive(Clone)]
pub struct Typed<F> {
    client: Client,
    format: F,
}

impl<F: Format> Typed<F> {
    pub fn new(client: Client, format: F) -> Self {
        Self { client, format }
    }

    /// The underlying byte-level client.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// The format keys and values go through.
    pub fn format(&self) -> &F {
        &self.format
    }

    /// Fetch one key, decoding its value as `V`.
    pub async fn get<K: Serialize, V: DeserializeOwned>(&self, key: K) -> Result<Option<V>> {
        let f = &self.format;
        decode(f, self.client.get(encode(f, key)?).await?)
    }

    /// Fetch a batch of keys. For a tuple of keys `Vs` is a tuple with one
    /// value type per key; for an array, `Vec` or slice it is the single
    /// value type. `Vs` is inferred when the binding is annotated:
    /// `let (u, n): (Option<User>, Option<u64>) = c.get_multi(("u", "n")).await?;`
    pub async fn get_multi<Ks: ValuesFor<Vs>, Vs>(&self, keys: Ks) -> Result<Ks::Output> {
        let keys = keys.encode(&self.format)?;
        let values = self
            .client
            .get_multi(keys.iter().map(Vec::as_slice))
            .await?;
        expect_count(&values, keys.len())?;
        Ks::decode(&self.format, values)
    }

    /// Store one key/value pair.
    pub async fn set<K: Serialize, V: Serialize>(&self, key: K, value: V) -> Result<()> {
        let f = &self.format;
        self.client.set(encode(f, key)?, encode(f, value)?).await
    }

    /// Store many key/value pairs from any iterator of `(key, value)`.
    pub async fn set_multi<K, V, I>(&self, entries: I) -> Result<()>
    where
        K: Serialize,
        V: Serialize,
        I: IntoIterator<Item = (K, V)>,
    {
        let f = &self.format;
        let entries = entries
            .into_iter()
            .map(|(k, v)| Ok((encode(f, k)?, encode(f, v)?)))
            .collect::<Result<Vec<_>>>()?;
        self.client
            .set_multi(entries.iter().map(|(k, v)| (k.as_slice(), v.as_slice())))
            .await
    }

    /// Delete one key; returns whether it existed.
    pub async fn del<K: Serialize>(&self, key: K) -> Result<bool> {
        self.client.del(encode(&self.format, key)?).await
    }

    /// Delete a batch of keys; `[bool; N]` for tuples and arrays,
    /// `Vec<bool>` for a `Vec` or slice.
    pub async fn del_multi<Ks: Keys>(&self, keys: Ks) -> Result<Ks::Flags> {
        let keys = keys.encode(&self.format)?;
        let flags = self
            .client
            .del_multi(keys.iter().map(Vec::as_slice))
            .await?;
        expect_count(&flags, keys.len())?;
        Ks::flags(flags)
    }
}
