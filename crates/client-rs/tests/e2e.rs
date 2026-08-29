use std::sync::Arc;

use bytes::Bytes;
use oxicache_client::{Client, Error};
use oxicache_server::{Cache, Options, Server};
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
async fn token_auth() {
    let cache = Arc::new(Cache::new(64 << 20, 1));
    let opts = Options {
        token: Some(b"s3cret".to_vec()),
    };
    let server = Arc::new(Server::bind_with("127.0.0.1:0".parse().unwrap(), cache, opts).unwrap());
    let s = server.clone();
    tokio::spawn(async move { s.run().await });
    let addr = server.local_addr();

    // No auth: rejected and disconnected.
    let c = Client::connect(addr).await.unwrap();
    let err = c.get([&b"a"[..]]).await.unwrap_err();
    assert!(
        matches!(
            err,
            Error::Status {
                status: Status::Unauthorized,
                ..
            }
        ),
        "{err}"
    );
    assert!(matches!(
        c.get([&b"a"[..]]).await,
        Err(Error::Closed | Error::Io(_))
    ));

    // Wrong token: rejected.
    let c = Client::connect(addr).await.unwrap();
    assert!(matches!(
        c.auth(b"s3cre").await,
        Err(Error::Status {
            status: Status::Unauthorized,
            ..
        })
    ));

    // Right token, once per connection: everything works, including a
    // redundant second AUTH.
    let c = Client::connect_with_token(addr, Some(b"s3cret"))
        .await
        .unwrap();
    c.set([(&b"a"[..], &b"1"[..])]).await.unwrap();
    assert_eq!(
        c.get([&b"a"[..]]).await.unwrap(),
        vec![Some(Bytes::from_static(b"1"))]
    );
    c.auth(b"s3cret").await.unwrap();
    assert_eq!(c.del([&b"a"[..]]).await.unwrap(), vec![true]);
}

#[tokio::test]
async fn closed_connection_errors() {
    let (server, client) = start().await;
    drop(server);
    // Existing connection still works because the accept loop task owns the listener clone.
    client.set([(&b"x"[..], &b"y"[..])]).await.unwrap();
    let dead = Client::connect("127.0.0.1:1".parse().unwrap()).await;
    assert!(matches!(dead, Err(Error::Io(_))));
}

/// A fake server that answers the first request correctly and then sends one
/// extra, unsolicited OK frame: the client must not hand it to the next caller.
#[tokio::test]
async fn unsolicited_frame_closes_connection() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut hdr = [0u8; 5];
        s.read_exact(&mut hdr).await.unwrap();
        let mut body = vec![0u8; oxicache_wire::decode_header(&hdr).1];
        s.read_exact(&mut body).await.unwrap();
        // Legit SET reply, then a stray one.
        s.write_all(&oxicache_wire::encode_header(0, 0))
            .await
            .unwrap();
        s.write_all(&oxicache_wire::encode_header(0, 0))
            .await
            .unwrap();
        s.flush().await.unwrap();
        // Keep the socket open; the client should still fail.
        let _ = s.read(&mut hdr).await;
    });
    let c = Client::connect(addr).await.unwrap();
    c.set([(&b"a"[..], &b"1"[..])]).await.unwrap();
    // Without detection this call would receive the stray frame as its own
    // reply and decode an empty body as a values list.
    let err = c.get([&b"a"[..]]).await.unwrap_err();
    assert!(matches!(err, Error::Closed), "{err}");
}
