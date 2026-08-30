use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use oxicache_client::{Client, Error};
use oxicache_server::{Cache, Options, Server};
use serde::{Deserialize, Serialize};

async fn start() -> (Arc<Server>, Client) {
    let cache = Arc::new(Cache::new(
        NonZeroUsize::new(64 << 20).unwrap(),
        NonZeroUsize::new(2).unwrap(),
    ));
    let opts = Options::new(b"t".to_vec());
    let server = Arc::new(Server::bind("127.0.0.1:0".parse().unwrap(), cache, opts).unwrap());
    let s = server.clone();
    tokio::spawn(async move { s.run().await });
    let client = Client::connect(server.local_addr(), "t").await.unwrap();
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

#[tokio::test]
async fn single_key_shapes() {
    let (_server, c) = start().await;
    assert_eq!(c.get::<User>("u").await.unwrap(), None);
    c.set("u", alice()).await.unwrap();
    let got: Option<User> = c.get("u").await.unwrap();
    assert_eq!(got, Some(alice()));
    // Any bytes are a key: strings, byte strings, vectors, arrays.
    c.set(b"bytes", &alice()).await.unwrap();
    c.set(vec![1u8, 2], 3u8).await.unwrap();
    assert_eq!(c.get::<User>("bytes").await.unwrap(), Some(alice()));
    assert_eq!(c.get::<u8>([1u8, 2]).await.unwrap(), Some(3));
    assert!(c.del("u").await.unwrap());
    assert!(!c.del("u").await.unwrap());
    assert_eq!(c.get::<User>("u").await.unwrap(), None);
}

#[tokio::test]
async fn tuple_with_a_type_per_key() {
    let (_server, c) = start().await;
    c.set("user", alice()).await.unwrap();
    c.set("hits", 42u64).await.unwrap();
    c.set("tags", vec!["a", "b"]).await.unwrap();

    // Value types only, no `Option`s to spell out.
    let (user, hits, tags, missing) = c
        .get_multi::<(User, u64, Vec<String>, String), _>(("user", "hits", "tags", "nope"))
        .await
        .unwrap();
    assert_eq!(user, Some(alice()));
    assert_eq!(hits, Some(42));
    assert_eq!(tags, Some(vec!["a".to_string(), "b".to_string()]));
    assert_eq!(missing, None);

    // Inferred from the binding.
    let (hits, user): (Option<u64>, Option<User>) = c.get_multi(("hits", "user")).await.unwrap();
    assert_eq!(hits, Some(42));
    assert_eq!(user.map(|u| u.id), Some(7));

    // Wrong type for one slot fails the whole call, and only that call.
    let err = c
        .get_multi::<(User, String), _>(("user", "hits"))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Deserialize(_)), "{err}");

    assert_eq!(
        c.del_multi(("user", "hits", "nope")).await.unwrap(),
        [true, true, false]
    );
    let single: (Option<User>,) = c.get_multi(("user",)).await.unwrap();
    assert_eq!(single, (None,));
}

#[tokio::test]
async fn arrays_and_vecs_share_one_type() {
    let (_server, c) = start().await;
    c.set_multi([("1", "one"), ("2", "two")]).await.unwrap();

    // Array: fixed length in the type.
    let got = c.get_multi::<String, _>(["2", "1", "3"]).await.unwrap();
    assert_eq!(got, [Some("two".into()), Some("one".into()), None]);

    // Vec and slice: runtime length.
    let ids: Vec<String> = (1..=3).map(|i| i.to_string()).collect();
    let got = c.get_multi::<String, _>(ids.clone()).await.unwrap();
    assert_eq!(got, vec![Some("one".into()), Some("two".into()), None]);
    let got: Vec<Option<String>> = c.get_multi(ids.as_slice()).await.unwrap();
    assert_eq!(got.len(), 3);

    assert_eq!(c.del_multi(["1", "9"]).await.unwrap(), [true, false]);
    assert_eq!(
        c.del_multi(vec!["2", "9"]).await.unwrap(),
        vec![true, false]
    );
    assert_eq!(c.del_multi(&["2"][..]).await.unwrap(), vec![false]);
}

#[tokio::test]
async fn sixteen_key_tuple() {
    let (_server, c) = start().await;
    c.set_multi((0u8..16).map(|i| ([i], i as u32 * 10)))
        .await
        .unwrap();
    type U = u32;
    let got = c
        .get_multi::<(U, U, U, U, U, U, U, U, U, U, U, U, U, U, U, U), _>((
            [0u8],
            [1u8],
            [2u8],
            [3u8],
            [4u8],
            [5u8],
            [6u8],
            [7u8],
            [8u8],
            [9u8],
            [10u8],
            [11u8],
            [12u8],
            [13u8],
            [14u8],
            [15u8],
        ))
        .await
        .unwrap();
    assert_eq!(got.0, Some(0));
    assert_eq!(got.15, Some(150));
    let flags = c
        .del_multi((
            [0u8],
            [1u8],
            [2u8],
            [3u8],
            [4u8],
            [5u8],
            [6u8],
            [7u8],
            [8u8],
            [9u8],
            [10u8],
            [11u8],
            [12u8],
            [13u8],
            [14u8],
            [15u8],
        ))
        .await
        .unwrap();
    assert_eq!(flags, [true; 16]);
}

#[tokio::test]
async fn primitives_and_collections() {
    let (_server, c) = start().await;
    c.set("nums", vec![1i64, -2, 3]).await.unwrap();
    c.set("pair", (1u8, "x".to_string())).await.unwrap();
    let nums: Option<Vec<i64>> = c.get("nums").await.unwrap();
    assert_eq!(nums, Some(vec![1, -2, 3]));
    let pair: Option<(u8, String)> = c.get("pair").await.unwrap();
    assert_eq!(pair, Some((1, "x".into())));
}

/// Values are plain MessagePack with structs as maps: what the TypeScript
/// client writes and reads.
#[tokio::test]
async fn values_are_msgpack_maps() {
    let (server, c) = start().await;
    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct P {
        x: u8,
        y: String,
    }
    c.set(
        "p",
        P {
            x: 1,
            y: "z".into(),
        },
    )
    .await
    .unwrap();
    let raw = c.get::<rmpv::Value>("p").await.unwrap().unwrap();
    assert_eq!(
        raw,
        rmpv::Value::Map(vec![("x".into(), 1.into()), ("y".into(), "z".into())])
    );
    // And a map written as a generic value reads back as the struct.
    let c2 = Client::connect(server.local_addr(), "t").await.unwrap();
    c2.set(
        "q",
        rmpv::Value::Map(vec![("x".into(), 1.into()), ("y".into(), "z".into())]),
    )
    .await
    .unwrap();
    assert_eq!(
        c.get::<P>("q").await.unwrap(),
        Some(P {
            x: 1,
            y: "z".into()
        })
    );
}

#[tokio::test]
async fn wrong_type_is_a_deserialize_error() {
    let (_server, c) = start().await;
    c.set("s", "not a number").await.unwrap();
    let err = c.get::<u32>("s").await.unwrap_err();
    assert!(matches!(err, Error::Deserialize(_)), "{err}");
    // The connection is still usable afterwards.
    let got: Option<String> = c.get("s").await.unwrap();
    assert_eq!(got, Some("not a number".into()));
}

#[tokio::test]
async fn empty_batches() {
    let (_server, c) = start().await;
    let got = c.get_multi::<User, _>(Vec::<&str>::new()).await.unwrap();
    assert!(got.is_empty());
    let got: [Option<User>; 0] = c.get_multi([] as [&str; 0]).await.unwrap();
    assert!(got.is_empty());
    c.set_multi(Vec::<(&str, User)>::new()).await.unwrap();
    assert!(c.del_multi(Vec::<&str>::new()).await.unwrap().is_empty());
}

/// A server that answers with the wrong number of values or flags for an
/// array of keys is reported as a count mismatch, not a panic.
#[tokio::test]
async fn wrong_count_from_server_is_an_error() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn scripted(reply: Vec<u8>) -> (std::net::SocketAddr, Scripted) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (hold, held) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            // Accept AUTH, then answer the request with the script.
            let mut hdr = [0u8; 5];
            s.read_exact(&mut hdr).await.unwrap();
            let mut body = vec![0u8; oxicache_wire::decode_header(&hdr).1];
            s.read_exact(&mut body).await.unwrap();
            s.write_all(&oxicache_wire::encode_header(0, 0))
                .await
                .unwrap();
            let mut hdr = [0u8; 5];
            s.read_exact(&mut hdr).await.unwrap();
            let mut body = vec![0u8; oxicache_wire::decode_header(&hdr).1];
            s.read_exact(&mut body).await.unwrap();
            s.write_all(&reply).await.unwrap();
            s.flush().await.unwrap();
            let _ = held.await;
        });
        (addr, Scripted { hold, task })
    }
    struct Scripted {
        hold: tokio::sync::oneshot::Sender<()>,
        task: tokio::task::JoinHandle<()>,
    }
    impl Scripted {
        async fn finish(self) {
            drop(self.hold);
            self.task.await.unwrap();
        }
    }
    // Zero values for two keys.
    let mut reply = oxicache_wire::encode_header(0, 4).to_vec();
    reply.extend_from_slice(&0u32.to_le_bytes());
    let (addr, fake) = scripted(reply).await;
    let c = Client::connect(addr, "t").await.unwrap();
    let err = c.get_multi::<String, _>(["a", "b"]).await.unwrap_err();
    assert_eq!(err.to_string(), "server answered 0 values for 2 keys");
    fake.finish().await;
    // One flag for two keys.
    let mut reply = oxicache_wire::encode_header(0, 5).to_vec();
    reply.extend_from_slice(&1u32.to_le_bytes());
    reply.push(1);
    let (addr, fake) = scripted(reply).await;
    let c = Client::connect(addr, "t").await.unwrap();
    let err = c.del_multi(["a", "b"]).await.unwrap_err();
    assert_eq!(err.to_string(), "server answered 1 values for 2 keys");
    fake.finish().await;
}
