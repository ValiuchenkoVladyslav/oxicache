use std::sync::Arc;

use bytes::Bytes;
use oxicache_client::{Client, Config, Tls};
use oxicache_server::{Cache, Identity, Server};

async fn start() -> (Arc<Server>, Client) {
    let cache = Arc::new(Cache::new(64 << 20, 4));
    let server = Arc::new(
        Server::bind(
            "127.0.0.1:0".parse().unwrap(),
            Identity::self_signed().unwrap(),
            cache,
        )
        .unwrap(),
    );
    let s = server.clone();
    tokio::spawn(async move { s.run().await });
    let config = Config {
        server_name: "localhost".into(),
        tls: Tls::Pinned(vec![server.cert().clone()]),
    };
    let client = Client::connect(server.local_addr(), config).await.unwrap();
    (server, client)
}

#[tokio::test]
async fn get_set_del_over_h3() {
    let (server, client) = start().await;
    assert_eq!(client.get([&b"a"[..]]).await.unwrap(), vec![None]);
    client
        .set([(&b"a"[..], &b"1"[..]), (&b"b"[..], &[0u8, 1, 2][..])])
        .await
        .unwrap();
    let got = client.get([&b"a"[..], &b"b"[..], &b"c"[..]]).await.unwrap();
    assert_eq!(
        got,
        vec![
            Some(Bytes::from_static(b"1")),
            Some(Bytes::from_static(&[0, 1, 2])),
            None
        ]
    );
    assert_eq!(
        client.del([&b"a"[..], &b"zz"[..]]).await.unwrap(),
        vec![true, false]
    );
    assert_eq!(client.get([&b"a"[..]]).await.unwrap(), vec![None]);
    server.close().await;
}

#[tokio::test]
async fn insecure_mode_and_large_values() {
    let (server, _) = start().await;
    let client = Client::connect(server.local_addr(), Config::default())
        .await
        .unwrap();
    let big = vec![7u8; 4 << 20];
    client.set([(&b"big"[..], big.as_slice())]).await.unwrap();
    let got = client.get([&b"big"[..]]).await.unwrap();
    assert_eq!(got[0].as_deref(), Some(big.as_slice()));
    server.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_clients() {
    let (server, client) = start().await;
    let tasks: Vec<_> = (0..16)
        .map(|t| {
            let client = client.clone();
            tokio::spawn(async move {
                for i in 0..50 {
                    let k = format!("t{t}-{i}");
                    client.set([(k.as_bytes(), k.as_bytes())]).await.unwrap();
                    let got = client.get([k.as_bytes()]).await.unwrap();
                    assert_eq!(got[0].as_deref(), Some(k.as_bytes()));
                }
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    server.close().await;
}
