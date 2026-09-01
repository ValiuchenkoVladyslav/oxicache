//! A cluster over real servers: where keys land, what a batch split over
//! several servers answers, and what happens when one of them stops.

use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use oxicache_client::{Batch, Client, Cluster, Error, Failover, ServerName, Tls};
use oxicache_server::{Cache, Options, Server};
use oxicache_wire::{KEEPALIVE, Status};

/// A server on its own task; dropping it stops the accept loop and every
/// connection it serves. The cache is small, so an oversized value is
/// refused without allocating much.
struct Served {
    server: Arc<Server>,
    task: tokio::task::JoinHandle<()>,
}

impl Served {
    fn bind(addr: SocketAddr) -> Self {
        Self::bind_with(addr, Options::new(b"t".to_vec()))
    }

    fn bind_with(addr: SocketAddr, opts: Options) -> Self {
        let cache = Arc::new(Cache::new(
            NonZeroUsize::new(1 << 20).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ));
        let server = Arc::new(Server::bind(addr, cache, opts).unwrap());
        let s = server.clone();
        let task = tokio::spawn(async move { s.run().await });
        Self { server, task }
    }

    fn addr(&self) -> SocketAddr {
        self.server.local_addr()
    }

    /// Stop and release the address: the listener is gone when this
    /// returns, so the address can be bound again.
    async fn stop(mut self) -> SocketAddr {
        let addr = self.addr();
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

fn serve() -> Served {
    Served::bind("127.0.0.1:0".parse().unwrap())
}

/// `n` servers and the addresses they listen on.
fn servers(n: usize) -> (Vec<Served>, Vec<SocketAddr>) {
    let servers: Vec<Served> = (0..n).map(|_| serve()).collect();
    let addrs = servers.iter().map(Served::addr).collect();
    (servers, addrs)
}

/// A cluster that drops a server after one failure and retries it after
/// `retry`, so a test does not wait 30 s to see either.
async fn cluster(addrs: &[SocketAddr], retry: Duration) -> Cluster {
    let failover = Failover {
        failures: NonZeroU32::new(1).unwrap(),
        retry,
    };
    Cluster::connect_with(addrs.to_vec(), None, "t", Some(failover), KEEPALIVE)
        .await
        .unwrap()
}

/// A key the ring gives to `addr`, found by trying keys in order.
fn key_on(c: &Cluster, addr: SocketAddr) -> String {
    (0..10_000)
        .map(|i| format!("key:{i}"))
        .find(|k| c.node_for(k) == addr)
        .expect("some key of 10 000 lands on every server")
}

#[tokio::test]
async fn every_key_lives_on_exactly_one_server() {
    let (servers, addrs) = servers(3);
    let c = cluster(&addrs, Duration::from_secs(30)).await;
    let keys: Vec<String> = (0..300).map(|i| format!("user:{i}")).collect();
    for k in &keys {
        c.set(k, k.len() as u32).await.unwrap();
    }
    // Ask every server for every key directly: the one the ring names has
    // it, the others never heard of it.
    let mut direct = Vec::new();
    for s in &servers {
        direct.push((s.addr(), Client::connect(s.addr(), "t").await.unwrap()));
    }
    let mut held = vec![0; servers.len()];
    for k in &keys {
        let owner = c.node_for(k);
        for (i, (addr, client)) in direct.iter().enumerate() {
            let got = client.get::<u32>(k).await.unwrap();
            if *addr == owner {
                assert_eq!(got, Some(k.len() as u32), "{k} missing from its server");
                held[i] += 1;
            } else {
                assert_eq!(got, None, "{k} also on {addr}");
            }
        }
        assert_eq!(c.get::<u32>(k).await.unwrap(), Some(k.len() as u32));
    }
    assert!(held.iter().all(|&n| n > 0), "keys bunched up: {held:?}");
}

#[tokio::test]
async fn clusters_agree_on_where_a_key_lives() {
    let (_servers, addrs) = servers(4);
    let forward = cluster(&addrs, Duration::from_secs(30)).await;
    let backward = cluster(
        &addrs.iter().rev().copied().collect::<Vec<_>>(),
        Duration::from_secs(30),
    )
    .await;
    for i in 0..500 {
        let key = format!("user:{i}");
        assert_eq!(forward.node_for(&key), backward.node_for(&key), "{key}");
    }
}

#[tokio::test]
async fn a_server_that_stops_answering_is_dropped_and_its_keys_move() {
    let (mut servers, addrs) = servers(3);
    let c = cluster(&addrs, Duration::from_secs(30)).await;
    let dying = servers.remove(0);
    let key = key_on(&c, dying.addr());
    c.set(&key, 1u8).await.unwrap();
    let gone = dying.stop().await;

    // The call that finds the server down is the one that fails.
    let err = c.get::<u8>(&key).await.unwrap_err();
    assert!(matches!(err, Error::Io(_) | Error::Closed), "{err}");
    assert_eq!(c.live(), addrs[1..], "the dead server left the ring");
    assert_ne!(c.node_for(&key), gone);

    // The next call goes to the server that took the key over, where it is
    // cold: nothing is copied between servers.
    assert_eq!(c.get::<u8>(&key).await.unwrap(), None);
    c.set(&key, 2u8).await.unwrap();
    assert_eq!(c.get::<u8>(&key).await.unwrap(), Some(2));
    // Keys that never belonged to it are untouched.
    for i in 0..100 {
        let k = format!("elsewhere:{i}");
        if c.node_for(&k) != gone {
            c.set(&k, 7u8).await.unwrap();
            assert_eq!(c.get::<u8>(&k).await.unwrap(), Some(7));
        }
    }
}

#[tokio::test]
async fn a_dropped_server_is_let_back_in_after_the_retry() {
    let (mut servers, addrs) = servers(3);
    let retry = Duration::from_millis(300);
    let c = cluster(&addrs, retry).await;
    let dying = servers.remove(0);
    let key = key_on(&c, dying.addr());
    let gone = dying.stop().await;
    c.get::<u8>(&key).await.unwrap_err();
    assert_eq!(c.live().len(), 2);

    // Still out while the retry window lasts, whatever happens on the way.
    assert_ne!(c.node_for(&key), gone);
    let back = Served::bind(gone);
    assert_eq!(back.addr(), gone);
    tokio::time::sleep(retry + Duration::from_millis(100)).await;

    assert_eq!(c.node_for(&key), gone, "the key belongs to it again");
    c.set(&key, 3u8).await.unwrap();
    assert_eq!(c.get::<u8>(&key).await.unwrap(), Some(3));
    assert_eq!(c.live().len(), 3, "one answer puts it back for good");
}

#[tokio::test]
async fn a_cluster_that_is_all_down_still_tries_the_key_owner() {
    let (mut servers, addrs) = servers(2);
    let c = cluster(&addrs, Duration::from_secs(30)).await;
    let keys: Vec<String> = addrs.iter().map(|&a| key_on(&c, a)).collect();
    let (first, second) = (servers.remove(0), servers.remove(0));
    let (gone, other) = (first.stop().await, second.stop().await);
    for key in &keys {
        c.get::<u8>(key).await.unwrap_err();
    }
    assert!(c.live().is_empty(), "both servers dropped");
    // With nothing in the ring a key still goes to its own server, which is
    // what finds out that server is back.
    let back = Served::bind(gone);
    assert_eq!(back.addr(), gone);
    assert_eq!(c.node_for(&keys[0]), gone);
    assert_eq!(c.get::<u8>(&keys[0]).await.unwrap(), None);
    assert_eq!(c.live(), vec![gone], "one answer, one server back");
    // The one still down keeps none of its keys: with a server in the ring
    // again they go to it rather than to the server that is not answering.
    assert!(!c.live().contains(&other));
    assert_eq!(c.node_for(&keys[1]), gone);
    assert_eq!(c.get::<u8>(&keys[1]).await.unwrap(), None);
}

#[tokio::test]
async fn a_refused_call_is_not_a_failed_server() {
    let (_servers, addrs) = servers(3);
    let c = cluster(&addrs, Duration::from_secs(30)).await;
    // Bigger than the whole cache: the server answers, it just says no.
    let huge = vec![0u8; 2 << 20];
    for _ in 0..3 {
        let err = c.set("big", &huge).await.unwrap_err();
        assert!(
            matches!(
                err,
                Error::Status {
                    status: Status::TooLarge,
                    ..
                }
            ),
            "{err}"
        );
    }
    assert_eq!(c.live(), addrs, "a refusal is the call's, not the server's");
    c.set("small", 1u8).await.unwrap();
}

#[tokio::test]
async fn without_a_policy_a_key_never_leaves_its_server() {
    let (mut servers, addrs) = servers(3);
    let c = Cluster::connect_with(addrs.clone(), None, "t", None, KEEPALIVE)
        .await
        .unwrap();
    let dying = servers.remove(0);
    let key = key_on(&c, dying.addr());
    let gone = dying.stop().await;
    for _ in 0..3 {
        let err = c.get::<u8>(&key).await.unwrap_err();
        assert!(matches!(err, Error::Io(_) | Error::Closed), "{err}");
    }
    assert_eq!(c.node_for(&key), gone, "no failover, no move");
    assert_eq!(c.live(), addrs);
    // Every other server keeps serving its own share.
    let elsewhere = key_on(&c, addrs[1]);
    c.set(&elsewhere, 1u8).await.unwrap();
    assert_eq!(c.get::<u8>(&elsewhere).await.unwrap(), Some(1));
}

#[tokio::test]
async fn a_batch_is_split_over_the_servers_and_answered_in_order() {
    let (_servers, addrs) = servers(3);
    let c = cluster(&addrs, Duration::from_secs(30)).await;
    let keys: Vec<String> = (0..60).map(|i| format!("item:{i}")).collect();
    let mut owners: Vec<SocketAddr> = keys.iter().map(|k| c.node_for(k)).collect();
    owners.sort_unstable();
    owners.dedup();
    assert_eq!(owners.len(), 3, "the keys must span the cluster");
    let mut b = Batch::new();
    let sets: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| b.set(k, i as u32).unwrap())
        .collect();
    let out = c.batch(b).await.unwrap();
    assert_eq!(out.len(), keys.len());
    for s in sets {
        out.get(s).unwrap();
    }

    // A mix of every op, answered slot by slot in the order it was built.
    let mut b = Batch::new();
    let gets: Vec<_> = keys.iter().map(|k| b.get::<u32>(k)).collect();
    let missing = b.get::<u32>("nobody:home");
    let dropped = b.del(&keys[7]);
    let never = b.del("nobody:home");
    let written = b.set("fresh", "v").unwrap();
    let out = c.batch(b).await.unwrap();
    for (i, slot) in gets.into_iter().enumerate() {
        assert_eq!(out.get(slot).unwrap(), Some(i as u32), "item {i}");
    }
    assert_eq!(out.get(missing).unwrap(), None);
    assert!(out.get(dropped).unwrap());
    assert!(!out.get(never).unwrap());
    out.get(written).unwrap();
    assert_eq!(c.get::<u32>(&keys[7]).await.unwrap(), None, "del landed");
    assert_eq!(
        c.get::<String>("fresh").await.unwrap().as_deref(),
        Some("v")
    );
}

#[tokio::test]
async fn a_batch_of_one_server_s_keys_goes_to_that_server() {
    let (_servers, addrs) = servers(3);
    let c = cluster(&addrs, Duration::from_secs(30)).await;
    let key = key_on(&c, addrs[1]);
    let mut b = Batch::new();
    let write = b.set(&key, 1u8).unwrap();
    let read = b.get::<u8>(&key);
    let out = c.batch(b).await.unwrap();
    out.get(write).unwrap();
    assert_eq!(out.get(read).unwrap(), Some(1), "a get after its own set");
    assert_eq!(c.node_for(&key), addrs[1]);
}

#[tokio::test]
async fn a_batch_fails_while_one_of_its_servers_is_down_and_moves_on_after() {
    let (mut servers, addrs) = servers(3);
    let c = cluster(&addrs, Duration::from_secs(30)).await;
    let dying = servers.remove(0);
    let gone = dying.stop().await;
    let keys: Vec<String> = (0..60).map(|i| format!("item:{i}")).collect();
    assert!(keys.iter().any(|k| c.node_for(k) == gone));

    let mut b = Batch::new();
    for k in &keys {
        b.get::<u32>(k);
    }
    let err = c.batch(b).await.unwrap_err();
    assert!(matches!(err, Error::Io(_) | Error::Closed), "{err}");
    assert_eq!(c.live(), addrs[1..]);

    // With the server out of the ring the same batch is served by the two
    // that are left.
    let mut b = Batch::new();
    let slots: Vec<_> = keys.iter().map(|k| b.get::<u32>(k)).collect();
    let out = c.batch(b).await.unwrap();
    for s in slots {
        assert_eq!(out.get(s).unwrap(), None);
    }
}

#[tokio::test]
async fn an_empty_batch_asks_nobody() {
    let (servers, addrs) = servers(2);
    let c = cluster(&addrs, Duration::from_secs(30)).await;
    for s in servers {
        s.stop().await;
    }
    let out = c.batch(Batch::new()).await.unwrap();
    assert_eq!(out.len(), 0);
    assert!(out.is_empty());
}

#[tokio::test]
async fn one_server_is_a_cluster_too() {
    let (_servers, addrs) = servers(1);
    let c = cluster(&addrs, Duration::from_secs(30)).await;
    c.set("k", 1u8).await.unwrap();
    assert_eq!(c.get::<u8>("k").await.unwrap(), Some(1));
    assert_eq!(c.node_for("k"), addrs[0]);
    assert!(c.del("k").await.unwrap());
    let mut b = Batch::new();
    let slot = b.get::<u8>("k");
    assert_eq!(c.batch(b).await.unwrap().get(slot).unwrap(), None);
    c.ping().await.unwrap();
}

#[tokio::test]
async fn a_server_that_is_down_at_connect_starts_out_of_the_ring() {
    let (mut servers, addrs) = servers(3);
    let gone = servers.remove(0).stop().await;
    let c = cluster(&addrs, Duration::from_millis(300)).await;
    assert_eq!(c.live(), addrs[1..], "nothing is sent to it first");
    let key = format!("key:{}", 1);
    c.set(&key, 1u8).await.unwrap();
    assert_eq!(c.get::<u8>(&key).await.unwrap(), Some(1));
    // Ping is how a caller finds out for itself; it fails while one server
    // is down, and the error names the reason.
    let err = c.ping().await.unwrap_err();
    assert!(matches!(err, Error::Io(_) | Error::Closed), "{err}");
    assert_ne!(c.node_for(&key), gone);
}

#[tokio::test]
async fn a_cluster_with_nothing_up_does_not_connect() {
    let (servers, addrs) = servers(2);
    for s in servers {
        s.stop().await;
    }
    let err = Cluster::connect(addrs, "t").await.unwrap_err();
    assert!(matches!(err, Error::Io(_) | Error::Closed), "{err}");
}

#[tokio::test]
async fn a_token_one_server_refuses_fails_the_cluster() {
    let good = serve();
    let picky = Served::bind("127.0.0.1:0".parse().unwrap());
    let addrs = vec![good.addr(), picky.addr()];
    let err = Cluster::connect(addrs, "wrong").await.unwrap_err();
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

#[tokio::test]
async fn ping_reaches_every_server() {
    let (servers, addrs) = servers(3);
    let c = cluster(&addrs, Duration::from_secs(30)).await;
    c.ping().await.unwrap();
    let open: usize = servers
        .iter()
        .map(|s| s.server.open_connections().unwrap())
        .sum();
    assert_eq!(open, 3, "one connection per server");
}

#[tokio::test]
async fn calls_from_many_tasks_share_the_connections() {
    let (_servers, addrs) = servers(3);
    let c = cluster(&addrs, Duration::from_secs(30)).await;
    let mut tasks = Vec::new();
    for t in 0..16u32 {
        let c = c.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..50u32 {
                let key = format!("t{t}:{i}");
                c.set(&key, i).await.unwrap();
                assert_eq!(c.get::<u32>(&key).await.unwrap(), Some(i));
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
}

#[tokio::test]
async fn the_default_policy_takes_two_failures() {
    let (mut servers, addrs) = servers(3);
    // `connect` is the default policy: two failures in a row, 30 s out.
    let c = Cluster::connect(addrs.clone(), "t").await.unwrap();
    let dying = servers.remove(0);
    let key = key_on(&c, dying.addr());
    let gone = dying.stop().await;
    c.get::<u8>(&key).await.unwrap_err();
    assert_eq!(c.live(), addrs, "one failure is not enough");
    assert_eq!(c.node_for(&key), gone);
    c.get::<u8>(&key).await.unwrap_err();
    assert_eq!(c.live(), addrs[1..], "the second drops it");
    assert_ne!(c.node_for(&key), gone);
}

#[tokio::test]
async fn a_ping_finds_a_dropped_server_before_its_retry_is_due() {
    let (mut servers, addrs) = servers(3);
    // A retry window far longer than the test: only the ping can end it.
    let c = cluster(&addrs, Duration::from_secs(300)).await;
    let dying = servers.remove(0);
    let key = key_on(&c, dying.addr());
    let gone = dying.stop().await;
    c.get::<u8>(&key).await.unwrap_err();
    assert_eq!(c.live(), addrs[1..]);

    let back = Served::bind(gone);
    assert_eq!(back.addr(), gone);
    // Still out: the window has 300 s to run and nothing has asked.
    assert_ne!(c.node_for(&key), gone);
    c.ping().await.unwrap();
    assert_eq!(c.live(), addrs, "the answer to the ping put it back");
    assert_eq!(c.node_for(&key), gone);
}

#[tokio::test]
async fn the_servers_a_failed_batch_did_reach_keep_their_share() {
    let (mut servers, addrs) = servers(2);
    let c = cluster(&addrs, Duration::from_secs(30)).await;
    let dying = servers.remove(0);
    let gone = dying.stop().await;
    let keys: Vec<String> = (0..60).map(|i| format!("item:{i}")).collect();
    assert!(keys.iter().any(|k| c.node_for(k) == gone));

    // Whose share is whose has to be read before the batch: the failure
    // drops the server that is down, and its keys move to the other one.
    let mine: Vec<(usize, String)> = keys
        .iter()
        .enumerate()
        .filter(|(_, k)| c.node_for(k) == addrs[1])
        .map(|(i, k)| (i, k.clone()))
        .collect();
    assert!(!mine.is_empty());
    let mut b = Batch::new();
    for (i, k) in keys.iter().enumerate() {
        b.set(k, i as u32).unwrap();
    }
    c.batch(b).await.unwrap_err();
    // The server that was up wrote its share of the batch: a failure
    // somewhere else does not cut its request short.
    let live = Client::connect(addrs[1], "t").await.unwrap();
    for (i, k) in mine {
        assert_eq!(live.get::<u32>(&k).await.unwrap(), Some(i as u32), "{k}");
    }
}

#[tokio::test]
async fn a_server_that_answers_a_batch_short_is_an_error() {
    let real = serve();
    let miscounting = miscounting_server().await;
    let addrs = vec![real.addr(), miscounting];
    let c = cluster(&addrs, Duration::from_secs(30)).await;
    let keys: Vec<String> = (0..60).map(|i| format!("item:{i}")).collect();
    assert!(keys.iter().any(|k| c.node_for(k) == miscounting));
    assert!(keys.iter().any(|k| c.node_for(k) == real.addr()));

    let mut b = Batch::new();
    for k in &keys {
        b.get::<u8>(k);
    }
    // The answers cannot be matched to the items, so nothing is answered.
    let err = c.batch(b).await.unwrap_err();
    assert!(matches!(err, Error::Count { .. }), "{err}");
}

#[tokio::test]
async fn tls_spreads_keys_over_the_servers_too() {
    let servers: Vec<Served> = (0..3)
        .map(|_| Served::bind_with("127.0.0.1:0".parse().unwrap(), tls_opts()))
        .collect();
    let addrs: Vec<SocketAddr> = servers.iter().map(Served::addr).collect();
    let c = Cluster::connect_tls(addrs.clone(), trusting("127.0.0.1"), "t")
        .await
        .unwrap();
    let keys: Vec<String> = (0..60).map(|i| format!("tls:{i}")).collect();
    let mut owners: Vec<SocketAddr> = keys.iter().map(|k| c.node_for(k)).collect();
    owners.sort_unstable();
    owners.dedup();
    assert_eq!(owners.len(), 3, "the keys must span the cluster");
    let mut b = Batch::new();
    let slots: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| b.set(k, i as u32).unwrap())
        .collect();
    let out = c.batch(b).await.unwrap();
    for s in slots {
        out.get(s).unwrap();
    }
    for (i, k) in keys.iter().enumerate() {
        assert_eq!(c.get::<u32>(k).await.unwrap(), Some(i as u32), "{k}");
    }
    // The certificate is for 127.0.0.1; another name is refused, and one
    // refused server is no cluster.
    let err = Cluster::connect_tls(addrs, trusting("example.com"), "t")
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Io(_)), "{err}");
}

const SERVER_PEM: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../testdata/tls/server.pem");
const CA_PEM: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../testdata/tls/ca.pem");

fn tls_opts() -> Options {
    Options::new(b"t".to_vec()).tls(Some(
        oxicache_server::tls::server_config(std::path::Path::new(SERVER_PEM)).unwrap(),
    ))
}

fn trusting(name: &str) -> Tls {
    Tls::trusting(
        std::path::Path::new(CA_PEM),
        ServerName::try_from(name.to_string()).unwrap(),
    )
    .unwrap()
}

/// A server that authenticates, answers a PING, and then answers every
/// batch with one reply too few. The task lives until the test ends.
async fn miscounting_server() -> SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                loop {
                    let mut header = [0u8; oxicache_wire::HEADER_LEN];
                    if socket.read_exact(&mut header).await.is_err() {
                        return;
                    }
                    let (op, len) = oxicache_wire::decode_header(&header);
                    let mut body = vec![0u8; len];
                    if socket.read_exact(&mut body).await.is_err() {
                        return;
                    }
                    let reply = match oxicache_wire::Op::from_u8(op) {
                        Some(oxicache_wire::Op::Batch) => short_replies(&body),
                        // AUTH, PING and anything else: a plain ok.
                        _ => oxicache_wire::encode_header(Status::Ok as u8, 0).to_vec(),
                    };
                    if socket.write_all(&reply).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    addr
}

/// A BATCH response for `body` with its last reply missing.
fn short_replies(body: &[u8]) -> Vec<u8> {
    let count = u32::from_le_bytes(body[..4].try_into().unwrap()).saturating_sub(1);
    let mut replies = count.to_le_bytes().to_vec();
    for _ in 0..count {
        replies.extend_from_slice(&oxicache_wire::encode_header(Status::NotFound as u8, 0));
    }
    let mut frame = oxicache_wire::encode_header(Status::Ok as u8, replies.len()).to_vec();
    frame.extend_from_slice(&replies);
    frame
}
