use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use oxicache_client::{Batch, Client, Error};
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
async fn mixed_batch_answers_each_item_in_order() {
    let (_server, c) = start().await;
    c.set("user", alice()).await.unwrap();
    c.set("hits", 42u64).await.unwrap();

    let mut b = Batch::new();
    let user = b.get::<User>("user");
    let hits = b.get::<u64>("hits");
    let missing = b.get::<String>("nope");
    let tags = b.set("tags", vec!["a", "b"]).unwrap();
    let tags_now = b.get::<Vec<String>>("tags");
    let gone = b.del("hits");
    let hits_now = b.get::<u64>("hits");
    let never = b.del("nope");
    assert_eq!(b.len(), 8);
    let out = c.batch(b).await.unwrap();
    assert_eq!(out.len(), 8);

    assert_eq!(out.get(user).unwrap(), Some(alice()));
    assert_eq!(out.get(hits).unwrap(), Some(42));
    assert_eq!(out.get(missing).unwrap(), None);
    out.get(tags).unwrap();
    assert_eq!(
        out.get(tags_now).unwrap(),
        Some(vec!["a".to_string(), "b".to_string()])
    );
    assert!(out.get(gone).unwrap());
    assert_eq!(out.get(hits_now).unwrap(), None, "the earlier DEL is seen");
    assert!(!out.get(never).unwrap());

    // Slots are Copy: an answer can be read twice.
    assert_eq!(out.get(user).unwrap().map(|u| u.id), Some(7));
    // Raw statuses are there too.
    assert_eq!(out.status(0), Some(oxicache_wire::Status::Ok));
    assert_eq!(out.status(2), Some(oxicache_wire::Status::NotFound));
    assert_eq!(out.status(8), None);
}

#[tokio::test]
async fn wrong_type_in_one_slot_fails_only_that_slot() {
    let (_server, c) = start().await;
    c.set("user", alice()).await.unwrap();
    c.set("hits", 42u64).await.unwrap();
    let mut b = Batch::new();
    let user = b.get::<User>("user");
    let hits_as_text = b.get::<String>("hits");
    let out = c.batch(b).await.unwrap();
    assert_eq!(out.get(user).unwrap(), Some(alice()));
    let err = out.get(hits_as_text).unwrap_err();
    assert!(matches!(err, Error::Deserialize(_)), "{err}");
}

#[tokio::test]
async fn refused_item_does_not_hide_the_others() {
    let (_server, c) = start().await;
    let big = "x".repeat(48 << 20);
    let mut b = Batch::new();
    let small = b.set("small", "v").unwrap();
    let huge = b.set("huge", big.as_str()).unwrap();
    let after = b.set("after", "w").unwrap();
    let out = c.batch(b).await.unwrap();
    out.get(small).unwrap();
    out.get(after).unwrap();
    let err = out.get(huge).unwrap_err();
    assert!(
        matches!(
            &err,
            Error::Status {
                status: oxicache_wire::Status::TooLarge,
                ..
            }
        ),
        "{err}"
    );
    assert_eq!(out.status(1), Some(oxicache_wire::Status::TooLarge));
    assert_eq!(c.get::<String>("small").await.unwrap(), Some("v".into()));
    assert_eq!(c.get::<String>("after").await.unwrap(), Some("w".into()));
    assert_eq!(c.get::<String>("huge").await.unwrap(), None);
}

#[tokio::test]
async fn sixteen_key_batches() {
    let (_server, c) = start().await;
    let mut b = Batch::new();
    for i in 0u8..16 {
        b.set([i], i as u32 * 10).unwrap();
    }
    c.batch(b).await.unwrap();
    let mut b = Batch::new();
    let slots: Vec<_> = (0u8..16).map(|i| b.get::<u32>([i])).collect();
    let out = c.batch(b).await.unwrap();
    let got: Vec<Option<u32>> = slots.iter().map(|&s| out.get(s).unwrap()).collect();
    assert_eq!(got[0], Some(0));
    assert_eq!(got[15], Some(150));
    let mut b = Batch::new();
    let slots: Vec<_> = (0u8..16).map(|i| b.del([i])).collect();
    let out = c.batch(b).await.unwrap();
    assert!(slots.iter().all(|&s| out.get(s).unwrap()));
}

#[tokio::test]
async fn unserialisable_value_is_refused_when_added() {
    let (_server, c) = start().await;
    // A value whose `Serialize` refuses.
    struct Bad;
    impl Serialize for Bad {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("no"))
        }
    }
    let bad = Bad;
    let mut b = Batch::new();
    let err = b.set("k", &bad).unwrap_err();
    assert!(matches!(err, Error::Serialize(_)), "{err}");
    assert!(b.is_empty());
    let err = c.set("k", &bad).await.unwrap_err();
    assert!(matches!(err, Error::Serialize(_)), "{err}");
    assert_eq!(c.get::<u8>("k").await.unwrap(), None);
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
async fn empty_batch() {
    let (_server, c) = start().await;
    let b = Batch::new();
    assert!(b.is_empty());
    let out = c.batch(b).await.unwrap();
    assert!(out.is_empty());
    assert_eq!(out.len(), 0);
}

#[tokio::test]
async fn too_many_items_are_refused_before_sending() {
    let (_server, c) = start().await;
    let mut b = Batch::new();
    for _ in 0..=oxicache_wire::MAX_ITEMS {
        b.get::<u8>("k");
    }
    let err = c.batch(b).await.unwrap_err();
    assert!(
        matches!(err, Error::TooManyItems(n) if n == oxicache_wire::MAX_ITEMS + 1),
        "{err}"
    );
    // A full batch goes through.
    let mut b = Batch::new();
    for _ in 0..oxicache_wire::MAX_ITEMS {
        b.get::<u8>("k");
    }
    assert_eq!(c.batch(b).await.unwrap().len(), oxicache_wire::MAX_ITEMS);
}

/// A server that answers a batch with the wrong number of items is
/// reported as a count mismatch, not a panic.
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
    // Zero answers for two items.
    let mut reply = oxicache_wire::encode_header(0, 4).to_vec();
    reply.extend_from_slice(&0u32.to_le_bytes());
    let (addr, fake) = scripted(reply).await;
    let c = Client::connect(addr, "t").await.unwrap();
    let mut b = Batch::new();
    b.get::<String>("a");
    b.del("b");
    let err = c.batch(b).await.unwrap_err();
    assert_eq!(err.to_string(), "server answered 0 items for a batch of 2");
    fake.finish().await;
    // One answer for two items.
    let mut reply = oxicache_wire::encode_header(0, 9).to_vec();
    reply.extend_from_slice(&1u32.to_le_bytes());
    reply.extend_from_slice(&oxicache_wire::encode_header(0, 0));
    let (addr, fake) = scripted(reply).await;
    let c = Client::connect(addr, "t").await.unwrap();
    let mut b = Batch::new();
    b.del("a");
    b.del("b");
    let err = c.batch(b).await.unwrap_err();
    assert_eq!(err.to_string(), "server answered 1 items for a batch of 2");
    fake.finish().await;
}
