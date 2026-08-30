use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use oxicache_client::{Client, Error};
use oxicache_server::{Cache, Options, Server};
use oxicache_wire::Status;

fn serve(opts: Options) -> Arc<Server> {
    let cache = Arc::new(Cache::new(
        NonZeroUsize::new(64 << 20).unwrap(),
        NonZeroUsize::new(4).unwrap(),
    ));
    let server = Arc::new(Server::bind("127.0.0.1:0".parse().unwrap(), cache, opts).unwrap());
    let s = server.clone();
    tokio::spawn(async move { s.run().await });
    server
}

async fn start() -> (Arc<Server>, Client) {
    let server = serve(Options::new(b"t".to_vec()));
    let client = Client::connect(server.local_addr(), "t").await.unwrap();
    (server, client)
}

#[tokio::test]
async fn ping_round_trips() {
    let (_server, client) = start().await;
    client.ping().await.unwrap();
    client.set("k", 1u8).await.unwrap();
    client.ping().await.unwrap();
    assert_eq!(client.get::<u8>("k").await.unwrap(), Some(1));
}

#[tokio::test]
async fn keepalive_outlives_the_server_idle_timeout() {
    let idle = Duration::from_secs(1);
    let server = serve(Options::new(b"t".to_vec()).idle_timeout(Some(idle)));
    let addr = server.local_addr();
    // Pinging every 200 ms keeps the connection open across 1.5 s of silence;
    // the margins are wide because a loaded runner can stall timers.
    let quiet = Client::connect_with(addr, "t", Duration::from_millis(200))
        .await
        .unwrap();
    // The same silence with a heartbeat that never comes due is fatal, which
    // is what proves the first client survived because of its pings.
    let silent = Client::connect_with(addr, "t", Duration::from_secs(60))
        .await
        .unwrap();
    quiet.set("k", 1u8).await.unwrap();
    silent.set("k", 1u8).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(quiet.get::<u8>("k").await.unwrap(), Some(1));
    assert!(matches!(silent.get::<u8>("k").await, Err(Error::Closed)));
}

#[tokio::test]
async fn get_set_del_over_tcp() {
    let (_server, client) = start().await;
    assert_eq!(
        client.get_multi::<u32, _>([&b"a"[..]]).await.unwrap(),
        [None]
    );
    client
        .set_multi([(&b"a"[..], 1u32), (&b"b"[..], 2)])
        .await
        .unwrap();
    let got = client
        .get_multi::<u32, _>([&b"a"[..], &b"b"[..], &b"c"[..]])
        .await
        .unwrap();
    assert_eq!(got, [Some(1), Some(2), None]);
    assert_eq!(
        client.del_multi([&b"a"[..], &b"zz"[..]]).await.unwrap(),
        [true, false]
    );
    assert_eq!(
        client.get_multi::<u32, _>([&b"a"[..]]).await.unwrap(),
        [None]
    );
}

#[tokio::test]
async fn large_values() {
    let (_server, client) = start().await;
    let big = "7".repeat(4 << 20);
    client
        .set_multi([(&b"big"[..], big.as_str())])
        .await
        .unwrap();
    let got = client.get_multi::<String, _>([&b"big"[..]]).await.unwrap();
    assert_eq!(got[0].as_deref(), Some(big.as_str()));
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
                        .set_multi([(k.as_bytes(), k.as_str())])
                        .await
                        .unwrap();
                    let got = client.get_multi::<String, _>([k.as_bytes()]).await.unwrap();
                    assert_eq!(got[0].as_deref(), Some(k.as_str()));
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
    raw.write_all(&oxicache_wire::encode_header(
        oxicache_wire::Op::Auth as u8,
        1,
    ))
    .await
    .unwrap();
    raw.write_all(b"t").await.unwrap();
    let mut hdr = [0u8; 5];
    raw.read_exact(&mut hdr).await.unwrap();
    assert_eq!(oxicache_wire::decode_header(&hdr).0, Status::Ok as u8);
    raw.write_all(&oxicache_wire::encode_header(42, 0))
        .await
        .unwrap();
    raw.read_exact(&mut hdr).await.unwrap();
    assert_eq!(
        oxicache_wire::decode_header(&hdr).0,
        Status::UnknownOp as u8
    );
}

#[tokio::test]
async fn token_auth() {
    let cache = Arc::new(Cache::new(
        NonZeroUsize::new(64 << 20).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    ));
    let opts = Options::new(b"s3cret".to_vec());
    let server = Arc::new(Server::bind("127.0.0.1:0".parse().unwrap(), cache, opts).unwrap());
    let s = server.clone();
    tokio::spawn(async move { s.run().await });
    let addr = server.local_addr();

    // Wrong (or empty) token: no client comes out of `connect`.
    for wrong in [&b"s3cre"[..], b""] {
        let err = Client::connect(addr, wrong).await.err().unwrap();
        assert_eq!(
            err.to_string(),
            "server returned Unauthorized: auth required"
        );
        assert!(matches!(
            err,
            Error::Status {
                status: Status::Unauthorized,
                ..
            }
        ));
    }

    // Right token: everything works.
    let c = Client::connect(addr, "s3cret").await.unwrap();
    c.set_multi([(&b"a"[..], 1u8)]).await.unwrap();
    assert_eq!(c.get_multi::<u8, _>([&b"a"[..]]).await.unwrap(), [Some(1)]);
    assert_eq!(c.del_multi([&b"a"[..]]).await.unwrap(), [true]);
}

#[tokio::test]
async fn closed_connection_errors() {
    let (server, client) = start().await;
    drop(server);
    // Existing connection still works because the accept loop task owns the listener clone.
    client.set_multi([(&b"x"[..], "y")]).await.unwrap();
    let dead = Client::connect("127.0.0.1:1".parse().unwrap(), "t").await;
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
        accept_auth(&mut s).await;
        let mut hdr = [0u8; 5];
        s.read_exact(&mut hdr).await.unwrap();
        let mut body = vec![0u8; oxicache_wire::decode_header(&hdr).1];
        s.read_exact(&mut body).await.unwrap();
        // Legit SET reply and a stray one, in one write so both are in the
        // client's buffer before the next request is issued.
        let ok = oxicache_wire::encode_header(0, 0);
        s.write_all(&[ok, ok].concat()).await.unwrap();
        s.flush().await.unwrap();
        // Keep the socket open; the client should still fail.
        let _ = held.await;
    });
    let c = Client::connect(addr, "t").await.unwrap();
    c.set_multi([(&b"a"[..], "1")]).await.unwrap();
    // Without detection this call would receive the stray frame as its own
    // reply and decode an empty body as a values list.
    let err = c.get_multi::<u8, _>([&b"a"[..]]).await.unwrap_err();
    assert!(matches!(err, Error::Closed), "{err}");
    drop(hold);
    server.await.unwrap();
}

/// Read the client's AUTH frame and accept it, as any server would.
async fn accept_auth(s: &mut tokio::net::TcpStream) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut hdr = [0u8; 5];
    s.read_exact(&mut hdr).await.unwrap();
    assert_eq!(hdr[0], oxicache_wire::Op::Auth as u8);
    let mut body = vec![0u8; oxicache_wire::decode_header(&hdr).1];
    s.read_exact(&mut body).await.unwrap();
    s.write_all(&oxicache_wire::encode_header(0, 0))
        .await
        .unwrap();
    s.flush().await.unwrap();
}

/// A fake server that accepts AUTH, answers the next request with `reply`
/// verbatim and then holds the socket open until the returned sender is
/// dropped.
async fn scripted(reply: Vec<u8>) -> (std::net::SocketAddr, Scripted) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (hold, held) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        accept_auth(&mut s).await;
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
async fn invalid_status_byte_closes_connection() {
    let (addr, fake) = scripted(oxicache_wire::encode_header(9, 0).to_vec()).await;
    let c = Client::connect(addr, "t").await.unwrap();
    let err = c.get_multi::<u8, _>([&b"a"[..]]).await.unwrap_err();
    assert!(matches!(err, Error::InvalidStatus(9)), "{err}");
    let err = c.get_multi::<u8, _>([&b"a"[..]]).await.unwrap_err();
    assert!(matches!(err, Error::Closed), "{err}");
    fake.finish().await;
}

#[tokio::test]
async fn oversized_response_is_reported() {
    let too_big = oxicache_wire::MAX_FRAME + 1;
    let (addr, fake) = scripted(oxicache_wire::encode_header(0, too_big).to_vec()).await;
    let c = Client::connect(addr, "t").await.unwrap();
    let err = c.get_multi::<u8, _>([&b"a"[..]]).await.unwrap_err();
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
    let c = Client::connect(addr, "t").await.unwrap();
    let err = c.get_multi::<u8, _>([&b"a"[..]]).await.unwrap_err();
    assert!(matches!(err, Error::Decode(_)), "{err}");
    fake.finish().await;
    let (addr, fake) = scripted(bad).await;
    let c = Client::connect(addr, "t").await.unwrap();
    let err = c.del_multi([&b"a"[..]]).await.unwrap_err();
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
        let (mut s, _) = listener.accept().await.unwrap();
        accept_auth(&mut s).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        drop(s);
    });
    let c = Client::connect(addr, "t").await.unwrap();
    let big = "0".repeat(32 << 20);
    let err = c.set_multi([(&b"a"[..], big.as_str())]).await.unwrap_err();
    assert!(matches!(err, Error::Closed | Error::Io(_)), "{err}");
    let err = c.get_multi::<u8, _>([&b"a"[..]]).await.unwrap_err();
    assert!(matches!(err, Error::Closed), "{err}");
}
