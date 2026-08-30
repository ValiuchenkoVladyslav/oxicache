use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use oxicache_client::{Client, Error};
use oxicache_server::{Cache, Options, Server};
use oxicache_wire::Status;

/// A server running on its own task; dropping it stops the accept loop
/// and every connection it serves.
struct Served {
    server: Arc<Server>,
    task: tokio::task::JoinHandle<()>,
}

impl Served {
    fn bind(addr: SocketAddr, opts: Options) -> Self {
        let cache = Arc::new(Cache::new(
            NonZeroUsize::new(64 << 20).unwrap(),
            NonZeroUsize::new(4).unwrap(),
        ));
        let server = Arc::new(Server::bind(addr, cache, opts).unwrap());
        let s = server.clone();
        let task = tokio::spawn(async move { s.run().await });
        Self { server, task }
    }

    fn local_addr(&self) -> SocketAddr {
        self.server.local_addr()
    }

    fn open_connections(&self) -> usize {
        self.server.open_connections().unwrap()
    }

    /// Stop and release the address: the listener is gone when this returns.
    async fn stop(mut self) -> SocketAddr {
        let addr = self.local_addr();
        self.task.abort();
        let _ = (&mut self.task).await;
        addr
    }
}

impl Drop for Served {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn serve(opts: Options) -> Served {
    Served::bind("127.0.0.1:0".parse().unwrap(), opts)
}

async fn start() -> (Served, Client) {
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
    // The same silence with a heartbeat that never comes due loses the
    // connection, which is what proves the first client survived because
    // of its pings; the next call gets a new one without being told.
    let silent = Client::connect_with(addr, "t", Duration::from_secs(60))
        .await
        .unwrap();
    quiet.set("k", 1u8).await.unwrap();
    silent.set("k", 1u8).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(server.open_connections(), 1);
    assert_eq!(quiet.get::<u8>("k").await.unwrap(), Some(1));
    assert_eq!(silent.get::<u8>("k").await.unwrap(), Some(1));
    assert_eq!(server.open_connections(), 2);
    assert_eq!(quiet.reconnects(), 0);
    assert_eq!(silent.reconnects(), 1);
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
    let addr = server.stop().await;
    // Nothing listens: the call fails with the connect error, and so does a
    // fresh connect.
    let err = client.set_multi([(&b"x"[..], "y")]).await.unwrap_err();
    assert!(matches!(err, Error::Io(_) | Error::Closed), "{err}");
    let dead = Client::connect(addr, "t").await;
    assert!(matches!(dead, Err(Error::Io(_))));
}

/// A scripted server: `handle` runs on its own task for every accepted
/// connection, with that connection's ordinal (0 for the first), so a test
/// can behave differently before and after a reconnect and count accepts.
struct Fake {
    addr: SocketAddr,
    accepts: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Fake {
    async fn serve<F, Fut>(handle: F) -> Self
    where
        F: Fn(tokio::net::TcpStream, usize) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepts = Arc::new(AtomicUsize::new(0));
        let count = accepts.clone();
        let task = tokio::spawn(async move {
            let handle = Arc::new(handle);
            loop {
                let (s, _) = listener.accept().await.unwrap();
                let n = count.fetch_add(1, Ordering::SeqCst);
                let handle = handle.clone();
                tokio::spawn(async move { handle(s, n).await });
            }
        });
        Self {
            addr,
            accepts,
            task,
        }
    }

    fn accepts(&self) -> usize {
        self.accepts.load(Ordering::SeqCst)
    }

    /// Stop listening; open connections stay up until their handler ends.
    fn stop(&self) {
        self.task.abort();
    }
}

impl Drop for Fake {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Read the client's AUTH frame and accept it, as any server would.
async fn accept_auth(s: &mut tokio::net::TcpStream) {
    use tokio::io::AsyncWriteExt;
    let (op, _) = read_frame(s).await;
    assert_eq!(op, oxicache_wire::Op::Auth as u8);
    s.write_all(&oxicache_wire::encode_header(0, 0))
        .await
        .unwrap();
    s.flush().await.unwrap();
}

/// One request frame from the client: its op and body.
async fn read_frame(s: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    use tokio::io::AsyncReadExt;
    let mut hdr = [0u8; 5];
    s.read_exact(&mut hdr).await.unwrap();
    let (op, len) = oxicache_wire::decode_header(&hdr);
    let mut body = vec![0u8; len];
    s.read_exact(&mut body).await.unwrap();
    (op, body)
}

/// A complete response frame.
fn frame(status: u8, body: &[u8]) -> Vec<u8> {
    let mut f = oxicache_wire::encode_header(status, body.len()).to_vec();
    f.extend_from_slice(body);
    f
}

/// The reply to a one-key GET whose value is the MessagePack `1u8`.
fn one_value() -> Vec<u8> {
    let value = rmp_serde::to_vec(&1u8).unwrap();
    frame(0, &oxicache_wire::encode_values([Some(value.as_slice())]))
}

/// A fake that accepts AUTH, answers every later request with `reply`
/// verbatim and then holds the socket open until the `Fake` is dropped.
async fn scripted(reply: Vec<u8>) -> Fake {
    Fake::serve(move |mut s, _| {
        let reply = reply.clone();
        async move {
            use tokio::io::AsyncWriteExt;
            accept_auth(&mut s).await;
            loop {
                read_frame(&mut s).await;
                s.write_all(&reply).await.unwrap();
                s.flush().await.unwrap();
            }
        }
    })
    .await
}

/// A fake server that answers the first request correctly and then sends one
/// extra, unsolicited OK frame: the client must not hand it to the next caller.
#[tokio::test]
async fn unsolicited_frame_closes_connection() {
    let fake = Fake::serve(|mut s, _| async move {
        use tokio::io::AsyncWriteExt;
        accept_auth(&mut s).await;
        read_frame(&mut s).await;
        // Legit SET reply and a stray one, in one write so both are in the
        // client's buffer before the next request is issued.
        let ok = oxicache_wire::encode_header(0, 0);
        s.write_all(&[ok, ok].concat()).await.unwrap();
        s.flush().await.unwrap();
        // Keep the socket open; the client should still fail.
        std::future::pending::<()>().await;
    })
    .await;
    let c = Client::connect(fake.addr, "t").await.unwrap();
    c.set_multi([(&b"a"[..], "1")]).await.unwrap();
    // Without detection this call would receive the stray frame as its own
    // reply and decode an empty body as a values list.
    let err = c.get_multi::<u8, _>([&b"a"[..]]).await.unwrap_err();
    assert!(matches!(err, Error::Closed), "{err}");
    // A server that does not speak the protocol is not reconnected to.
    let err = c.get_multi::<u8, _>([&b"a"[..]]).await.unwrap_err();
    assert!(matches!(err, Error::Closed), "{err}");
    assert_eq!(fake.accepts(), 1);
}

#[tokio::test]
async fn invalid_status_byte_is_permanent() {
    let fake = scripted(frame(9, b"")).await;
    let c = Client::connect(fake.addr, "t").await.unwrap();
    let err = c.get_multi::<u8, _>([&b"a"[..]]).await.unwrap_err();
    assert!(matches!(err, Error::InvalidStatus(9)), "{err}");
    // The client is dead with that error: no reconnect, no new accept.
    let err = c.get_multi::<u8, _>([&b"a"[..]]).await.unwrap_err();
    assert!(matches!(err, Error::InvalidStatus(9)), "{err}");
    assert_eq!(fake.accepts(), 1);
    assert_eq!(c.reconnects(), 0);
}

#[tokio::test]
async fn oversized_response_is_reported() {
    let too_big = oxicache_wire::MAX_FRAME + 1;
    let fake = scripted(oxicache_wire::encode_header(0, too_big).to_vec()).await;
    let c = Client::connect(fake.addr, "t").await.unwrap();
    let err = c.get_multi::<u8, _>([&b"a"[..]]).await.unwrap_err();
    assert!(
        matches!(err, Error::ResponseTooLarge(n) if n == too_big),
        "{err}"
    );
    // The client closed that connection itself, so the next call may open
    // another; the call that hit the limit is not retried.
    let err = c.get_multi::<u8, _>([&b"a"[..]]).await.unwrap_err();
    assert!(
        matches!(err, Error::ResponseTooLarge(n) if n == too_big),
        "{err}"
    );
    assert_eq!(fake.accepts(), 2);
    assert_eq!(c.reconnects(), 1);
}

#[tokio::test]
async fn malformed_bodies_are_decode_errors() {
    let bad = frame(0, &[9, 0]);
    let fake = scripted(bad).await;
    let c = Client::connect(fake.addr, "t").await.unwrap();
    let err = c.get_multi::<u8, _>([&b"a"[..]]).await.unwrap_err();
    assert!(matches!(err, Error::Decode(_)), "{err}");
    let err = c.del_multi([&b"a"[..]]).await.unwrap_err();
    assert!(matches!(err, Error::Decode(_)), "{err}");
    // The connection is fine, only the bodies were not.
    assert_eq!(fake.accepts(), 1);
}

/// The peer closes without reading while a large request is still being
/// written (closing with unread data resets the connection): the write
/// fails, the retry finds nobody listening, and the client reports it.
#[tokio::test]
async fn write_failure_is_reported() {
    let fake = Fake::serve(|mut s, _| async move {
        accept_auth(&mut s).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(s);
    })
    .await;
    let c = Client::connect(fake.addr, "t").await.unwrap();
    fake.stop();
    let big = "0".repeat(32 << 20);
    // The retry is refused, so that is the error; nobody accepts again.
    let err = c.set_multi([(&b"a"[..], big.as_str())]).await.unwrap_err();
    assert!(matches!(err, Error::Io(_)), "{err}");
    let err = c.get_multi::<u8, _>([&b"a"[..]]).await.unwrap_err();
    assert!(matches!(err, Error::Io(_)), "{err}");
    assert_eq!(fake.accepts(), 1);
}

#[tokio::test]
async fn a_lost_heartbeat_does_not_reconnect_by_itself() {
    // Connection 0 hangs up on its first request, the PING; later ones
    // answer.
    let fake = Fake::serve(|mut s, n| async move {
        use tokio::io::AsyncWriteExt;
        accept_auth(&mut s).await;
        loop {
            let (op, _) = read_frame(&mut s).await;
            if n == 0 {
                assert_eq!(op, oxicache_wire::Op::Ping as u8);
                return;
            }
            s.write_all(&one_value()).await.unwrap();
            s.flush().await.unwrap();
        }
    })
    .await;
    let c = Client::connect_with(fake.addr, "t", Duration::from_millis(20))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(fake.accepts(), 1);
    assert_eq!(c.reconnects(), 0);
    // The next call is what reconnects.
    assert_eq!(c.get::<u8>("a").await.unwrap(), Some(1));
    assert_eq!(fake.accepts(), 2);
    assert_eq!(c.reconnects(), 1);
}

/// A fake whose first connection hangs up on its first request and which
/// then drops every further connection on accept: reconnects fail with the
/// server "down", and every attempt shows up in the accept count.
async fn goes_down() -> Fake {
    Fake::serve(|mut s, n| async move {
        if n == 0 {
            accept_auth(&mut s).await;
            read_frame(&mut s).await;
        }
    })
    .await
}

#[tokio::test]
async fn concurrent_callers_share_one_failed_attempt() {
    let fake = goes_down().await;
    let c = Client::connect(fake.addr, "t").await.unwrap();
    // The in-flight call's retry is the first attempt (accept 1).
    let err = c.ping().await.unwrap_err();
    assert!(matches!(err, Error::Closed), "{err}");
    assert_eq!(fake.accepts(), 2);
    // Eight callers at once: one attempt, one backoff wait, one failure
    // reported to all of them — not eight connects with growing waits.
    let t = std::time::Instant::now();
    let callers: Vec<_> = (0..8)
        .map(|_| {
            let c = c.clone();
            tokio::spawn(async move { c.ping().await })
        })
        .collect();
    for caller in callers {
        let err = caller.await.unwrap().unwrap_err();
        assert!(matches!(err, Error::Closed), "{err}");
    }
    assert!(
        t.elapsed() < Duration::from_millis(500),
        "{:?}",
        t.elapsed()
    );
    assert_eq!(fake.accepts(), 3);
    assert_eq!(c.reconnects(), 0);
}

#[tokio::test]
async fn a_call_arriving_during_the_backoff_joins_the_attempt() {
    let fake = goes_down().await;
    let c = Client::connect(fake.addr, "t").await.unwrap();
    c.ping().await.unwrap_err();
    // The backoff clock starts at that failure: the next attempt is no
    // sooner than 100 ms after it. A call 20 ms into the wait neither
    // fails fast nor connects on its own; it fails with the attempt. (The
    // clock is anchored here, not after the sleep, so a stretched sleep on
    // a loaded machine cannot shrink the window being asserted.)
    let failed = std::time::Instant::now();
    assert_eq!(fake.accepts(), 2);
    let early = tokio::spawn({
        let c = c.clone();
        async move { c.ping().await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let err = c.ping().await.unwrap_err();
    assert!(matches!(err, Error::Closed), "{err}");
    assert!(
        failed.elapsed() >= Duration::from_millis(90),
        "{:?}",
        failed.elapsed()
    );
    assert!(matches!(early.await.unwrap().unwrap_err(), Error::Closed));
    assert_eq!(fake.accepts(), 3);
}

#[tokio::test]
async fn a_desync_during_reconnect_auth_is_permanent() {
    // Connection 0 hangs up on the first GET; every later connection sends
    // a frame nobody asked for as soon as it is accepted.
    let fake = Fake::serve(|mut s, n| async move {
        use tokio::io::AsyncWriteExt;
        if n == 0 {
            accept_auth(&mut s).await;
            read_frame(&mut s).await;
            return;
        }
        s.write_all(&frame(0, b"")).await.unwrap();
        s.flush().await.unwrap();
        accept_auth(&mut s).await;
        std::future::pending::<()>().await;
    })
    .await;
    let c = Client::connect(fake.addr, "t").await.unwrap();
    // Whether the stray frame lands before AUTH is queued (AUTH fails with
    // the desync verdict) or after (AUTH takes it as its answer and the real
    // one is the stray), the client is dead and does not try a third time.
    let err = c.get::<u8>("a").await.unwrap_err();
    assert!(matches!(err, Error::Closed), "{err}");
    let err = c.get::<u8>("a").await.unwrap_err();
    assert!(matches!(err, Error::Closed), "{err}");
    assert_eq!(fake.accepts(), 2);
}

#[tokio::test]
async fn server_restart_is_repaired_by_the_next_call() {
    let (server, client) = start().await;
    client.set("k", 1u8).await.unwrap();
    let addr = server.stop().await;
    // Down: the call fails with a connection error, not a protocol one.
    let err = client.get::<u8>("k").await.unwrap_err();
    assert!(matches!(err, Error::Io(_) | Error::Closed), "{err}");
    let server = Served::bind(addr, Options::new(b"t".to_vec()));
    assert_eq!(server.local_addr(), addr);
    // Back: the next call reconnects (the cache is new, so the key is gone).
    assert_eq!(client.get::<u8>("k").await.unwrap(), None);
    client.set("k", 2u8).await.unwrap();
    assert_eq!(client.get::<u8>("k").await.unwrap(), Some(2));
    assert_eq!(client.reconnects(), 1);
    assert_eq!(server.open_connections(), 1);
}

/// A fake that hangs up on the first GET of connection 0 and answers it on
/// every later connection: an in-flight call is re-issued once.
async fn hangs_up_once() -> Fake {
    Fake::serve(|mut s, n| async move {
        use tokio::io::AsyncWriteExt;
        accept_auth(&mut s).await;
        loop {
            read_frame(&mut s).await;
            if n == 0 {
                return; // drop with the request unanswered
            }
            s.write_all(&one_value()).await.unwrap();
            s.flush().await.unwrap();
        }
    })
    .await
}

#[tokio::test]
async fn in_flight_calls_are_retried_once() {
    let fake = hangs_up_once().await;
    let c = Client::connect(fake.addr, "t").await.unwrap();
    // Two concurrent calls lost together share the one reconnect.
    let (a, b) = tokio::join!(c.get::<u8>("a"), c.get::<u8>("b"));
    assert_eq!(a.unwrap(), Some(1));
    assert_eq!(b.unwrap(), Some(1));
    assert_eq!(fake.accepts(), 2);
    assert_eq!(c.reconnects(), 1);
}

#[tokio::test]
async fn a_retried_call_is_not_retried_again() {
    let fake = Fake::serve(|mut s, _| async move {
        accept_auth(&mut s).await;
        read_frame(&mut s).await;
        // Every connection hangs up on its first request.
    })
    .await;
    let c = Client::connect(fake.addr, "t").await.unwrap();
    let err = c.get::<u8>("a").await.unwrap_err();
    assert!(matches!(err, Error::Closed), "{err}");
    assert_eq!(fake.accepts(), 2);
    assert_eq!(c.reconnects(), 1);
}

#[tokio::test]
async fn a_rotated_token_kills_the_client() {
    let fake = Fake::serve(|mut s, n| async move {
        use tokio::io::AsyncWriteExt;
        if n == 0 {
            accept_auth(&mut s).await;
            return; // hang up right after AUTH: the token was rotated
        }
        read_frame(&mut s).await;
        s.write_all(&frame(Status::Unauthorized as u8, b"auth required"))
            .await
            .unwrap();
        s.flush().await.unwrap();
    })
    .await;
    let c = Client::connect(fake.addr, "t").await.unwrap();
    for _ in 0..2 {
        let err = c.get::<u8>("a").await.unwrap_err();
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
    }
    // The second call did not even try: the answer would be the same.
    assert_eq!(fake.accepts(), 2);
    assert_eq!(c.reconnects(), 0);
}

#[tokio::test]
async fn status_errors_do_not_reconnect() {
    let fake = Fake::serve(|mut s, _| async move {
        use tokio::io::AsyncWriteExt;
        accept_auth(&mut s).await;
        read_frame(&mut s).await;
        s.write_all(&frame(Status::BadRequest as u8, b"nope"))
            .await
            .unwrap();
        loop {
            read_frame(&mut s).await;
            s.write_all(&frame(0, b"")).await.unwrap();
            s.flush().await.unwrap();
        }
    })
    .await;
    let c = Client::connect(fake.addr, "t").await.unwrap();
    let err = c.set("a", 1u8).await.unwrap_err();
    assert!(
        matches!(
            err,
            Error::Status {
                status: Status::BadRequest,
                ..
            }
        ),
        "{err}"
    );
    c.set("a", 1u8).await.unwrap();
    assert_eq!(fake.accepts(), 1);
    assert_eq!(c.reconnects(), 0);
}

#[tokio::test]
async fn failed_reconnects_back_off() {
    let fake = Fake::serve(|mut s, _| async move {
        accept_auth(&mut s).await;
        // Hang up at once, and the listener goes too (see below).
    })
    .await;
    let c = Client::connect(fake.addr, "t").await.unwrap();
    fake.stop();
    // The first attempt is immediate and refused; each failure starts the
    // next backoff step (100 ms, then 200 ms). The clock for a step is
    // anchored before the call whose failure starts it, so a loaded machine
    // can only lengthen what is measured, never shorten it.
    let mut before = std::time::Instant::now();
    let err = c.ping().await.unwrap_err();
    assert!(matches!(err, Error::Io(_) | Error::Closed), "{err}");
    for wait in [100, 200] {
        let next = std::time::Instant::now();
        let err = c.ping().await.unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err}");
        assert!(
            before.elapsed() >= Duration::from_millis(wait),
            "{:?}",
            before.elapsed()
        );
        before = next;
    }
    assert_eq!(c.reconnects(), 0);
}
