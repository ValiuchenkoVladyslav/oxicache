//! Typed values, enabled by the `serde` feature: a `Client<F>` connected
//! with a [`Format`] takes any `Serialize` value and decodes into any
//! `DeserializeOwned` type, through `F` — any serde data format the caller
//! implements [`Format`] for. The crate ships no format of its own and never
//! looks inside the bytes; keys are always plain bytes.
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
//! let c = Client::connect(addr, Json).await?;
//! c.set("user:7", &user).await?;
//! let user = c.get::<User>("user:7").await?;                          // Option<User>
//! let (user, hits) = c.get_multi(("user:7", "hits:7")).decode::<(User, u64)>().await?;
//! let users = c.get_multi(["user:7", "user:8"]).decode::<User>().await?; // [Option<User>; 2]
//! let users = c.get_multi(ids).decode::<User>().await?;                  // Vec<Option<User>>
//! let raw = c.get_multi(ids).await?;                                     // Vec<Option<Bytes>>
//! c.set_multi([("a", 1), ("b", 2)]).await?;
//! ```
//!
//! `decode`'s type argument names the value types only — one per key for a
//! tuple of keys (a mismatched count does not compile), a single type for
//! an array, `Vec` or slice of keys — and every slot comes back as an
//! `Option`, `None` for a missing key. Annotating the binding instead
//! (`let (u, h): (Option<User>, Option<u64>) = c.get_multi(…).decode().await?`)
//! works too.

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{BoxError, Client, Error, GetMulti, Keys, Result, expect_count};

/// A serde data format: how values become bytes and back. Any format works
/// as long as `decode(encode(x)) == x`; the cache never looks inside.
pub trait Format {
    fn encode<T: Serialize + ?Sized>(&self, value: &T) -> std::result::Result<Vec<u8>, BoxError>;
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> std::result::Result<T, BoxError>;
}

fn decode<F: Format, V: DeserializeOwned>(f: &F, value: Option<Bytes>) -> Result<Option<V>> {
    value
        .map(|v| f.decode(&v).map_err(Error::Deserialize))
        .transpose()
}

/// What `decode::<Vs>()` over this key batch returns: `(Option<V1>, …)`
/// for a tuple of keys with `Vs = (V1, …)`, `[Option<V>; N]` or
/// `Vec<Option<V>>` for an array, `Vec` or slice of keys with `Vs = V`.
pub trait Values<Vs>: Keys {
    type Output;
    fn decode<F: Format>(f: &F, values: Vec<Option<Bytes>>) -> Result<Self::Output>;
}

macro_rules! impl_tuples {
    ($($n:literal => ($($i:tt $K:ident $V:ident),+);)+) => {$(
        impl<$($K: AsRef<[u8]>, $V: DeserializeOwned),+> Values<($($V,)+)> for ($($K,)+) {
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

impl<K: AsRef<[u8]>, V: DeserializeOwned, const N: usize> Values<V> for [K; N] {
    type Output = [Option<V>; N];
    fn decode<F: Format>(f: &F, values: Vec<Option<Bytes>>) -> Result<Self::Output> {
        let decoded = values
            .into_iter()
            .map(|v| decode(f, v))
            .collect::<Result<Vec<_>>>()?;
        Self::batch(decoded)
    }
}

impl<K: AsRef<[u8]>, V: DeserializeOwned> Values<V> for Vec<K> {
    type Output = Vec<Option<V>>;
    fn decode<F: Format>(f: &F, values: Vec<Option<Bytes>>) -> Result<Self::Output> {
        values.into_iter().map(|v| decode(f, v)).collect()
    }
}

impl<K: AsRef<[u8]>, V: DeserializeOwned> Values<V> for &[K] {
    type Output = Vec<Option<V>>;
    fn decode<F: Format>(f: &F, values: Vec<Option<Bytes>>) -> Result<Self::Output> {
        values.into_iter().map(|v| decode(f, v)).collect()
    }
}

impl<F: Format, Ks: Keys> GetMulti<'_, F, Ks> {
    /// Send the request and decode every value as `Vs` says; see the
    /// module docs for the shapes of `Vs`.
    pub async fn decode<Vs>(self) -> Result<Ks::Output>
    where
        Ks: Values<Vs>,
    {
        let values = self.fetch().await?;
        Ks::decode(&self.client.format, values)
    }
}

impl<F: Format> Client<F> {
    /// Fetch one key, decoding its value as `V`.
    pub async fn get<V: DeserializeOwned>(&self, key: impl AsRef<[u8]>) -> Result<Option<V>> {
        Ok(self.get_multi((key,)).decode::<(V,)>().await?.0)
    }

    /// Store one key/value pair.
    pub async fn set<V: Serialize>(&self, key: impl AsRef<[u8]>, value: V) -> Result<()> {
        self.set_multi([(key, value)]).await
    }

    /// Store many key/value pairs from any iterator of `(key, value)`.
    pub async fn set_multi<K, V, I>(&self, entries: I) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Serialize,
        I: IntoIterator<Item = (K, V)>,
    {
        let f = &self.format;
        let entries = entries
            .into_iter()
            .map(|(k, v)| Ok((k, f.encode(&v).map_err(Error::Serialize)?)))
            .collect::<Result<Vec<(K, Vec<u8>)>>>()?;
        self.call(
            oxicache_wire::Op::Set,
            oxicache_wire::encode_entries(entries.iter().map(|(k, v)| (k.as_ref(), v.as_slice()))),
        )
        .await?;
        Ok(())
    }
}
