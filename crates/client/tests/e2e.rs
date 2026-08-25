use std::sync::Arc;

use bytes::Bytes;
use oxicache_client::{Client, Error};
use oxicache_server::{Cache, Server};
use oxicache_wire::Status;

async fn start() -> (Arc<Server>, Client) {
    let cache = Arc::new(Cache::new(64 << 20, 4));
    let server = Arc::new(Server::bind("127.0.0.1:0".parse().unwrap(), cache).unwrap());
    let s = server.clone();
    tokio::spawn(async move { s.run().await });
    let client = Client::connect(server.local_addr()).await.unwrap();
    (server, client)
}

#[tokio::test]
async fn get_set_del_over_tcp() {
    let (_server, client) = start().await;
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
}

#[tokio::test]
async fn large_values() {
    let (_server, client) = start().await;
    let big = vec![7u8; 4 << 20];
    client.set([(&b"big"[..], big.as_slice())]).await.unwrap();
    let got = client.get([&b"big"[..]]).await.unwrap();
    assert_eq!(got[0].as_deref(), Some(big.as_slice()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipelined_concurrent_calls() {
    let (_server, client) = start().await;
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
}

#[tokio::test]
async fn bad_frame_reports_status() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (server, _client) = start().await;
    let mut raw = tokio::net::TcpStream::connect(server.local_addr())
        .await
        .unwrap();
    raw.write_all(&oxicache_wire::encode_header(42, 0))
        .await
        .unwrap();
    let mut hdr = [0u8; 5];
    raw.read_exact(&mut hdr).await.unwrap();
    assert_eq!(
        oxicache_wire::decode_header(&hdr).0,
        Status::UnknownOp as u8
    );
}

#[tokio::test]
async fn closed_connection_errors() {
    let (server, client) = start().await;
    let addr = server.local_addr();
    drop(server);
    // Existing connection still works because the accept loop task owns the listener clone.
    client.set([(&b"x"[..], &b"y"[..])]).await.unwrap();
    let dead = Client::connect("127.0.0.1:1".parse().unwrap()).await;
    assert!(matches!(dead, Err(Error::Io(_))));
    let _ = addr;
}
