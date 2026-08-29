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
    assert_eq!(
        client.raw().get_multi([&b"a"[..]]).await.unwrap(),
        vec![None]
    );
    client
        .raw()
        .set_multi([(&b"a"[..], &b"1"[..]), (&b"b"[..], &[0u8, 1, 2][..])])
        .await
        .unwrap();
    let got = client
        .raw()
        .get_multi([&b"a"[..], &b"b"[..], &b"c"[..]])
        .await
        .unwrap();
    assert_eq!(
        got,
        vec![
            Some(Bytes::from_static(b"1")),
            Some(Bytes::from_static(&[0, 1, 2])),
            None
        ]
    );
    assert_eq!(
        client
            .raw()
            .del_multi([&b"a"[..], &b"zz"[..]])
            .await
            .unwrap(),
        vec![true, false]
    );
    assert_eq!(
        client.raw().get_multi([&b"a"[..]]).await.unwrap(),
        vec![None]
    );
}

#[tokio::test]
async fn large_values() {
    let (_server, client) = start().await;
    let big = vec![7u8; 4 << 20];
    client
        .raw()
        .set_multi([(&b"big"[..], big.as_slice())])
        .await
        .unwrap();
    let got = client.raw().get_multi([&b"big"[..]]).await.unwrap();
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
                    client
                        .raw()
                        .set_multi([(k.as_bytes(), k.as_bytes())])
                        .await
                        .unwrap();
                    let got = client.raw().get_multi([k.as_bytes()]).await.unwrap();
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
    let err = c.raw().get_multi([&b"a"[..]]).await.unwrap_err();
    assert_eq!(
        err.to_string(),
        "server returned Unauthorized: auth required"
    );
    assert!(matches!(
        c.raw().get_multi([&b"a"[..]]).await,
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
    c.raw().set_multi([(&b"a"[..], &b"1"[..])]).await.unwrap();
    assert_eq!(
        c.raw().get_multi([&b"a"[..]]).await.unwrap(),
        vec![Some(Bytes::from_static(b"1"))]
    );
    c.auth(b"s3cret").await.unwrap();
    assert_eq!(c.raw().del_multi([&b"a"[..]]).await.unwrap(), vec![true]);
}

#[tokio::test]
async fn closed_connection_errors() {
    let (server, client) = start().await;
    drop(server);
    // Existing connection still works because the accept loop task owns the listener clone.
    client
        .raw()
        .set_multi([(&b"x"[..], &b"y"[..])])
        .await
        .unwrap();
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
    let (hold, held) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
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
        let _ = held.await;
    });
    let c = Client::connect(addr).await.unwrap();
    c.raw().set_multi([(&b"a"[..], &b"1"[..])]).await.unwrap();
    // Without detection this call would receive the stray frame as its own
    // reply and decode an empty body as a values list.
    let err = c.raw().get_multi([&b"a"[..]]).await.unwrap_err();
    assert!(matches!(err, Error::Closed), "{err}");
    drop(hold);
    server.await.unwrap();
}

/// A fake server that answers the first request with `reply` verbatim and
/// then holds the socket open until the returned sender is dropped.
async fn scripted(reply: Vec<u8>) -> (std::net::SocketAddr, Scripted) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (hold, held) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
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

/// Handle to a scripted server: `finish` releases the socket and joins it.
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

#[tokio::test]
async fn connect_without_token() {
    let (server, _) = start().await;
    let c = Client::connect_with_token(server.local_addr(), None)
        .await
        .unwrap();
    c.raw().set_multi([(&b"a"[..], &b"1"[..])]).await.unwrap();
}

#[tokio::test]
async fn invalid_status_byte_closes_connection() {
    let (addr, fake) = scripted(oxicache_wire::encode_header(9, 0).to_vec()).await;
    let c = Client::connect(addr).await.unwrap();
    let err = c.raw().get_multi([&b"a"[..]]).await.unwrap_err();
    assert!(matches!(err, Error::InvalidStatus(9)), "{err}");
    let err = c.raw().get_multi([&b"a"[..]]).await.unwrap_err();
    assert!(matches!(err, Error::Closed), "{err}");
    fake.finish().await;
}

#[tokio::test]
async fn oversized_response_is_reported() {
    let too_big = oxicache_wire::MAX_FRAME + 1;
    let (addr, fake) = scripted(oxicache_wire::encode_header(0, too_big).to_vec()).await;
    let c = Client::connect(addr).await.unwrap();
    let err = c.raw().get_multi([&b"a"[..]]).await.unwrap_err();
    assert!(
        matches!(err, Error::ResponseTooLarge(n) if n == too_big),
        "{err}"
    );
    fake.finish().await;
}

#[tokio::test]
async fn malformed_bodies_are_decode_errors() {
    let mut bad = oxicache_wire::encode_header(0, 2).to_vec();
    bad.extend_from_slice(&[9, 0]);
    let (addr, fake) = scripted(bad.clone()).await;
    let c = Client::connect(addr).await.unwrap();
    let err = c.raw().get_multi([&b"a"[..]]).await.unwrap_err();
    assert!(matches!(err, Error::Decode(_)), "{err}");
    fake.finish().await;
    let (addr, fake) = scripted(bad).await;
    let c = Client::connect(addr).await.unwrap();
    let err = c.raw().del_multi([&b"a"[..]]).await.unwrap_err();
    assert!(matches!(err, Error::Decode(_)), "{err}");
    fake.finish().await;
}

/// The peer closes without reading while a large request is still being
/// written (closing with unread data resets the connection): the write
/// fails and the client reports closure.
#[tokio::test]
async fn write_failure_closes_connection() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (s, _) = listener.accept().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        drop(s);
    });
    let c = Client::connect(addr).await.unwrap();
    let big = vec![0u8; 32 << 20];
    let err = c
        .raw()
        .set_multi([(&b"a"[..], &big[..])])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Closed | Error::Io(_)), "{err}");
    let err = c.raw().get_multi([&b"a"[..]]).await.unwrap_err();
    assert!(matches!(err, Error::Closed), "{err}");
}

/// Without `serde` the client's own methods are the byte API.
#[cfg(not(feature = "serde"))]
#[tokio::test]
async fn byte_api_single_and_multi() {
    let (_server, c) = start().await;
    assert_eq!(c.get("a").await.unwrap(), None);
    c.set("a", b"1").await.unwrap();
    c.set_multi([(&b"b"[..], &b"2"[..]), (&b"c"[..], &b"3"[..])])
        .await
        .unwrap();
    assert_eq!(c.get("a").await.unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(
        c.get_multi([&b"b"[..], &b"c"[..], &b"d"[..]])
            .await
            .unwrap()
            .iter()
            .map(|v| v.as_deref().map(<[u8]>::to_vec))
            .collect::<Vec<_>>(),
        vec![Some(b"2".to_vec()), Some(b"3".to_vec()), None]
    );
    assert!(c.del("a").await.unwrap());
    assert!(!c.del("a").await.unwrap());
    assert_eq!(
        c.del_multi([&b"b"[..], &b"c"[..], &b"zz"[..]])
            .await
            .unwrap(),
        vec![true, true, false]
    );
}
