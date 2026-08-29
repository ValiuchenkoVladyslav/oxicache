#![cfg(feature = "serde")]

use std::collections::BTreeMap;
use std::sync::Arc;

use oxicache_client::{Client, Error};
use oxicache_server::{Cache, Server};
use serde::{Deserialize, Serialize};

async fn start() -> (Arc<Server>, Client) {
    let cache = Arc::new(Cache::new(64 << 20, 2));
    let server = Arc::new(Server::bind("127.0.0.1:0".parse().unwrap(), cache).unwrap());
    let s = server.clone();
    tokio::spawn(async move { s.run().await });
    let client = Client::connect(server.local_addr()).await.unwrap();
    (server, client)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum Role {
    Admin,
    Member { since: u32 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct User {
    id: u64,
    name: String,
    tags: Vec<String>,
    role: Role,
    score: Option<f64>,
    blob: Vec<u8>,
    attrs: BTreeMap<String, i32>,
}

fn alice() -> User {
    User {
        id: 7,
        name: "alice ✓".into(),
        tags: vec!["x".into(), "y".into()],
        role: Role::Member { since: 2020 },
        score: Some(3.5),
        blob: vec![0, 255, 1],
        attrs: BTreeMap::from([("a".into(), 1), ("b".into(), -2)]),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct UserKey {
    tenant: u16,
    id: u64,
}

#[tokio::test]
async fn structs_as_values() {
    let (_server, c) = start().await;
    let u = alice();
    c.set_typed([("user:7", u.clone())]).await.unwrap();
    let got: Vec<Option<User>> = c.get_typed(["user:7", "user:8"]).await.unwrap();
    assert_eq!(got, vec![Some(u), None]);
    assert_eq!(
        c.del_typed(["user:7", "user:8"]).await.unwrap(),
        vec![true, false]
    );
    let got: Vec<Option<User>> = c.get_typed(["user:7"]).await.unwrap();
    assert_eq!(got, vec![None]);
}

#[tokio::test]
async fn structs_as_keys() {
    let (_server, c) = start().await;
    let k1 = UserKey { tenant: 1, id: 7 };
    let k2 = UserKey { tenant: 2, id: 7 };
    c.set_typed([(&k1, alice()), (&k2, User { id: 8, ..alice() })])
        .await
        .unwrap();
    let got: Vec<Option<User>> = c.get_typed([&k2, &k1]).await.unwrap();
    assert_eq!(got[0].as_ref().unwrap().id, 8);
    assert_eq!(got[1].as_ref().unwrap().id, 7);
    // Same fields, different tenant: different key.
    let got: Vec<Option<User>> = c.get_typed([UserKey { tenant: 3, id: 7 }]).await.unwrap();
    assert_eq!(got, vec![None]);
}

#[tokio::test]
async fn primitives_and_collections() {
    let (_server, c) = start().await;
    c.set_typed([(1u32, "one"), (2u32, "two")]).await.unwrap();
    let got: Vec<Option<String>> = c.get_typed([2u32, 1u32, 3u32]).await.unwrap();
    assert_eq!(got, vec![Some("two".into()), Some("one".into()), None]);

    c.set_typed([("nums", vec![1i64, -2, 3])]).await.unwrap();
    c.set_typed([("pair", (1u8, "x".to_string()))])
        .await
        .unwrap();
    let nums: Vec<Option<Vec<i64>>> = c.get_typed(["nums"]).await.unwrap();
    assert_eq!(nums, vec![Some(vec![1, -2, 3])]);
    let pair: Vec<Option<(u8, String)>> = c.get_typed(["pair"]).await.unwrap();
    assert_eq!(pair, vec![Some((1, "x".into()))]);
}

#[tokio::test]
async fn typed_and_raw_keys_are_distinct_namespaces() {
    let (_server, c) = start().await;
    c.set([(&b"k"[..], &b"raw"[..])]).await.unwrap();
    c.set_typed([("k", "typed")]).await.unwrap();
    assert_eq!(
        c.get([&b"k"[..]]).await.unwrap()[0].as_deref(),
        Some(&b"raw"[..])
    );
    let got: Vec<Option<String>> = c.get_typed(["k"]).await.unwrap();
    assert_eq!(got, vec![Some("typed".into())]);
}

#[tokio::test]
async fn wrong_type_is_a_deserialize_error() {
    let (_server, c) = start().await;
    c.set_typed([("s", "not a number")]).await.unwrap();
    let err = c.get_typed::<_, u32, _>(["s"]).await.unwrap_err();
    assert!(matches!(err, Error::Deserialize(_)), "{err}");
    // The connection is still usable afterwards.
    let got: Vec<Option<String>> = c.get_typed(["s"]).await.unwrap();
    assert_eq!(got, vec![Some("not a number".into())]);
}

#[tokio::test]
async fn empty_batches() {
    let (_server, c) = start().await;
    let got: Vec<Option<User>> = c.get_typed(Vec::<&str>::new()).await.unwrap();
    assert!(got.is_empty());
    c.set_typed(Vec::<(&str, User)>::new()).await.unwrap();
    assert!(c.del_typed(Vec::<&str>::new()).await.unwrap().is_empty());
}
