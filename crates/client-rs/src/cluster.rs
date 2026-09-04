//! A cluster of independent servers, the way a memcached client uses one:
//! every key belongs to exactly one server, picked by consistent hashing
//! (the ring in `src/ring.rs`), and the servers know nothing of each other.
//! There is no replication and no data failover — a key that moves to
//! another server is simply cold there.

use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use oxicache_wire::{self as wire, Op, Status};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::ring::Ring;
use crate::{Batch, Client, Error, Outcome, Result, Tls};

/// What a cluster does with a server that stops answering, the way
/// memcached clients that eject hosts do it: after `failures` connection
/// failures in a row the server is dropped from the ring, so its keys go
/// to the next server round, and `retry` later it is let back in. One
/// answer of any kind — a value, a miss, even a refusal — puts it back at
/// once and clears the count.
///
/// Dropping a server moves its keys, and moves them back when it returns,
/// so a value written while it was out is not the one a later read finds:
/// a cache entry can go stale across an outage. Cluster-wide that is the
/// same trade memcached makes; [`Cluster::connect_with`] takes `None` for
/// clusters that would rather see the error than the stale value.
#[derive(Clone, Copy, Debug)]
pub struct Failover {
    /// Connection failures in a row before the server is dropped.
    pub failures: NonZeroU32,
    /// How long a dropped server stays out of the ring.
    pub retry: Duration,
}

impl Default for Failover {
    /// Two failures in a row, back in after 30 s.
    fn default() -> Self {
        Self {
            failures: NonZeroU32::new(2).expect("2 is not zero"),
            retry: Duration::from_secs(30),
        }
    }
}

/// One server of a cluster: its client, and what the failure policy knows
/// about it.
struct Node {
    addr: SocketAddr,
    client: Client,
    /// Failures since the last answer and, when the server has been dropped
    /// from the ring, the millisecond after the cluster's epoch it may be
    /// tried again — in one word, so that a call that answers and a call
    /// that fails cannot interleave into "dropped, with a clear count",
    /// which no later answer would clear.
    health: AtomicU64,
}

/// Bits of [`Node::health`] holding the retry deadline; the rest count the
/// failures. 2^48 ms is 8900 years of uptime, and a count is only ever
/// compared with a limit, so both saturate rather than wrap.
const DEADLINE_BITS: u32 = 48;
const DEADLINE_MAX: u64 = (1 << DEADLINE_BITS) - 1;
const FAILURES_MAX: u32 = (u64::MAX >> DEADLINE_BITS) as u32;

/// A server that has never failed: in the ring, with nothing to forgive.
const HEALTHY: u64 = 0;

fn health(failures: u32, deadline: u64) -> u64 {
    (u64::from(failures.min(FAILURES_MAX)) << DEADLINE_BITS) | deadline.min(DEADLINE_MAX)
}

fn failures(health: u64) -> u32 {
    (health >> DEADLINE_BITS) as u32
}

fn deadline(health: u64) -> u64 {
    health & DEADLINE_MAX
}

impl Node {
    fn is_up(&self, now: u64) -> bool {
        deadline(self.health.load(Relaxed)) <= now
    }

    /// The server answered, whatever it answered: it is up, and whatever
    /// it failed with before does not count any more.
    fn answered(&self) {
        // One store, and only for a server with something to clear: an
        // answer from a healthy server leaves the word untouched, so the
        // common path does not write a line every other caller reads.
        if self.health.load(Relaxed) != HEALTHY {
            self.health.store(HEALTHY, Relaxed);
        }
    }

    /// One connection failure; drops the server once they add up.
    fn failed(&self, failover: Option<Failover>, now: u64) {
        let mut seen = self.health.load(Relaxed);
        loop {
            let failures = failures(seen) + 1;
            let deadline = match failover {
                Some(policy) if failures >= policy.failures.get() => {
                    now.saturating_add(retry(policy))
                }
                // Below the limit the server stays where it is, in the ring
                // or in a drop-out window it is already serving.
                _ => deadline(seen),
            };
            match self.health.compare_exchange_weak(
                seen,
                health(failures, deadline),
                Relaxed,
                Relaxed,
            ) {
                Ok(_) => return,
                // Another call moved the word first; count from what it left.
                Err(current) => seen = current,
            }
        }
    }

    /// Out of the ring for a whole retry window, with the count at the
    /// limit so that one failure after it re-drops the server.
    fn drop_out(&self, policy: Failover, now: u64) {
        self.health.store(
            health(policy.failures.get(), now.saturating_add(retry(policy))),
            Relaxed,
        );
    }
}

fn retry(policy: Failover) -> u64 {
    u64::try_from(policy.retry.as_millis()).unwrap_or(u64::MAX)
}

/// Whether an error says the server is not answering, as opposed to
/// answering something the call did not want. A refused token counts: the
/// client behind it is dead, so this server is out until it is replaced.
fn unreachable(e: &Error) -> bool {
    matches!(
        e,
        Error::Io(_)
            | Error::Closed
            | Error::InvalidStatus(_)
            | Error::Status {
                status: Status::Unauthorized,
                ..
            }
    )
}

struct Inner {
    nodes: Box<[Node]>,
    ring: Ring,
    failover: Option<Failover>,
    /// What `down_until` counts milliseconds from.
    epoch: Instant,
}

impl Inner {
    fn now(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    /// Which server owns `key` right now.
    fn index(&self, key: &[u8], now: u64) -> usize {
        if self.nodes.len() == 1 {
            return 0;
        }
        self.ring.locate(key, |i| self.nodes[i].is_up(now))
    }

    fn pick(&self, key: &[u8]) -> &Node {
        &self.nodes[self.index(key, self.now())]
    }

    /// Feed one call's outcome to the failure policy.
    fn note<T>(&self, node: &Node, result: &Result<T>) {
        match result {
            Err(e) if unreachable(e) => node.failed(self.failover, self.now()),
            _ => node.answered(),
        }
    }
}

/// A set of servers with the keyspace spread over them: `get`, `set`,
/// `del` and `batch` as on a [`Client`], each key going to the server the
/// ring gives it.
///
/// Cheap to clone, shared by any number of tasks, one pipelined connection
/// per server. Every client of one cluster must be given the same servers
/// under the same names — the addresses as written, `10.0.0.1:4433` — or
/// they will not agree on where a key lives; the TypeScript client hashes
/// the same names the same way.
///
/// ```ignore
/// let c = Cluster::connect(["10.0.0.1:4433".parse()?, "10.0.0.2:4433".parse()?], "s3cret").await?;
/// c.set("user:7", &user).await?;
/// let user = c.get::<User>("user:7").await?;
/// ```
///
/// A server that stops answering is dropped from the ring by the
/// [`Failover`] policy and its keys move on; the call that runs into the
/// failure gets the error, and the ones after it go to the new server.
/// Nothing is replicated or copied over, so those keys start cold there.
#[derive(Clone)]
pub struct Cluster {
    inner: Arc<Inner>,
}

impl Cluster {
    /// Connect to every server and authenticate with `token`. Servers that
    /// are down are dropped from the ring and retried by the [`Failover`]
    /// policy, but a refused token (or a server that does not speak the
    /// protocol) fails here, and so does having no server up at all.
    ///
    /// Panics if `addrs` is empty or names the same address twice.
    pub async fn connect(
        addrs: impl IntoIterator<Item = SocketAddr>,
        token: impl AsRef<[u8]>,
    ) -> Result<Self> {
        Self::connect_with(
            addrs,
            None,
            token,
            Some(Failover::default()),
            wire::KEEPALIVE,
        )
        .await
    }

    /// [`connect`](Self::connect) over TLS: every server is checked
    /// against `tls`, as [`Client::connect_tls`] checks one.
    pub async fn connect_tls(
        addrs: impl IntoIterator<Item = SocketAddr>,
        tls: Tls,
        token: impl AsRef<[u8]>,
    ) -> Result<Self> {
        Self::connect_with(
            addrs,
            Some(tls),
            token,
            Some(Failover::default()),
            wire::KEEPALIVE,
        )
        .await
    }

    /// [`connect`](Self::connect) with a failure policy of your own, or
    /// `None` to keep every server in the ring however it behaves: a key
    /// then always goes to its own server and a failure is the call's
    /// error, never a move. `keepalive` is [`Client::connect_with`]'s.
    pub async fn connect_with(
        addrs: impl IntoIterator<Item = SocketAddr>,
        tls: Option<Tls>,
        token: impl AsRef<[u8]>,
        failover: Option<Failover>,
        keepalive: Duration,
    ) -> Result<Self> {
        let addrs: Vec<SocketAddr> = addrs.into_iter().collect();
        assert!(!addrs.is_empty(), "a cluster needs at least one server");
        // The ring is built from the addresses as written, and two servers
        // with one name would sit on top of each other.
        let names: Vec<String> = addrs.iter().map(SocketAddr::to_string).collect();
        let mut unique = names.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), names.len(), "duplicate server in a cluster");
        let token = Bytes::copy_from_slice(token.as_ref());
        let nodes = addrs
            .iter()
            .map(|&addr| Node {
                addr,
                client: Client::lazy(addr, tls.clone(), token.clone(), keepalive),
                health: AtomicU64::new(HEALTHY),
            })
            .collect();
        let cluster = Self {
            inner: Arc::new(Inner {
                nodes,
                ring: Ring::new(&names),
                failover,
                epoch: Instant::now(),
            }),
        };
        cluster.open().await?;
        Ok(cluster)
    }

    /// Connect every server at once. One that is already known to be down
    /// starts out of the ring rather than costing the first keys that hash
    /// to it a timeout.
    async fn open(&self) -> Result<()> {
        let inner = &self.inner;
        let mut tasks = JoinSet::new();
        for (i, node) in inner.nodes.iter().enumerate() {
            let client = node.client.clone();
            tasks.spawn(async move { (i, client.ping().await) });
        }
        let mut first: Option<Error> = None;
        let mut up = 0;
        while let Some(joined_result) = tasks.join_next().await {
            let (i, result) = joined(joined_result);
            match result {
                Ok(()) => up += 1,
                // A token this server refuses is not weather: every server
                // of a cluster shares one, so the cluster is misconfigured.
                Err(e) if matches!(e, Error::Status { .. } | Error::InvalidStatus(_)) => {
                    return Err(e);
                }
                Err(e) => {
                    if let Some(policy) = inner.failover {
                        inner.nodes[i].drop_out(policy, inner.now());
                    }
                    first.get_or_insert(e);
                }
            }
        }
        match first {
            // Nothing to spread keys over is not a cluster; the caller
            // hears why rather than getting one that fails every call.
            Some(e) if up == 0 => Err(e),
            _ => Ok(()),
        }
    }

    /// Which server holds `key` right now: its own, or the one that took
    /// it over while that server is out of the ring.
    pub fn node_for(&self, key: impl AsRef<[u8]>) -> SocketAddr {
        self.inner.pick(key.as_ref()).addr
    }

    /// The servers in the ring right now, in the order they were given;
    /// the ones dropped after failing are missing until they are retried.
    pub fn live(&self) -> Vec<SocketAddr> {
        let now = self.inner.now();
        self.inner
            .nodes
            .iter()
            .filter(|n| n.is_up(now))
            .map(|n| n.addr)
            .collect()
    }

    /// The value under `key` decoded as `V`, `None` if there is none.
    pub async fn get<V: DeserializeOwned>(&self, key: impl AsRef<[u8]>) -> Result<Option<V>> {
        let key = key.as_ref();
        let node = self.inner.pick(key);
        let result = node.client.get(key).await;
        self.inner.note(node, &result);
        result
    }

    /// Store `value` under `key`, replacing what was there.
    pub async fn set<V: Serialize>(&self, key: impl AsRef<[u8]>, value: V) -> Result<()> {
        let key = key.as_ref();
        let node = self.inner.pick(key);
        let result = node.client.set(key, value).await;
        self.inner.note(node, &result);
        result
    }

    /// Remove `key`; whether there was anything to remove.
    pub async fn del(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        let key = key.as_ref();
        let node = self.inner.pick(key);
        let result = node.client.del(key).await;
        self.inner.note(node, &result);
        result
    }

    /// Round-trip an empty request to every server, dropped ones included:
    /// the way to find out that a server is back before its retry is due.
    /// Every server is waited for; if any of them fails, the first failure
    /// to come back is the error.
    pub async fn ping(&self) -> Result<()> {
        let inner = &self.inner;
        let mut tasks = JoinSet::new();
        for (i, node) in inner.nodes.iter().enumerate() {
            let client = node.client.clone();
            tasks.spawn(async move { (i, client.ping().await) });
        }
        let mut first = None;
        while let Some(joined_result) = tasks.join_next().await {
            let (i, result) = joined(joined_result);
            inner.note(&inner.nodes[i], &result);
            if let Err(e) = result {
                first.get_or_insert(e);
            }
        }
        first.map_or(Ok(()), Err)
    }

    /// Send every item of `batch` to the server that owns its key — one
    /// request per server, all at once — and answer each slot in the order
    /// the batch was built, exactly as [`Client::batch`] does.
    ///
    /// Every server is waited for, and the whole call fails if any of those
    /// requests does — so a batch is all-or-nothing about the servers it
    /// reaches, not about the writes: the servers that did answer have
    /// applied their share. An empty batch asks nobody anything.
    pub async fn batch(&self, batch: Batch) -> Result<Outcome> {
        let n = batch.len();
        if n > wire::MAX_ITEMS {
            return Err(Error::TooManyItems(n));
        }
        let inner = &self.inner;
        let body = batch.enc.finish();
        let now = inner.now();
        // Where every item goes, before anything is re-encoded: a batch
        // whose keys all land on one server is sent exactly as it was built.
        let mut route: Vec<u32> = Vec::with_capacity(n);
        let mut split = false;
        for (op, item) in wire::frames(&body)? {
            let key = match Op::from_u8(op) {
                Some(Op::Set) => wire::set_body(item)?.0,
                // GET and DEL carry the key alone; a `Batch` builds no
                // other item, and anything else routes by its whole body
                // so that the server it lands on answers it.
                _ => item,
            };
            let at = inner.index(key, now) as u32;
            split |= route.first().is_some_and(|&first| first != at);
            route.push(at);
        }
        let Some(&only) = route.first() else {
            return Ok(Outcome {
                body: Bytes::new(),
                index: Box::new([]),
            });
        };
        if !split {
            // One server takes the batch as it was encoded.
            let node = &inner.nodes[only as usize];
            let result = node.client.batch_body(body, n).await;
            inner.note(node, &result);
            return result;
        }
        // Which items each server gets, and where its answers belong.
        let mut groups: Box<[Option<Group>]> = inner.nodes.iter().map(|_| None).collect();
        for ((op, item), (i, &at)) in wire::frames(&body)?.zip(route.iter().enumerate()) {
            let group = groups[at as usize].get_or_insert_default();
            group.enc.push(op, item);
            group.items.push(i as u32);
        }
        let mut tasks = JoinSet::new();
        for (at, group) in groups.iter_mut().enumerate() {
            let Some(group) = group.take() else { continue };
            let client = inner.nodes[at].client.clone();
            let items = group.items;
            let body = group.enc.finish();
            tasks.spawn(async move { (at, items, client.call(Op::Batch, body).await) });
        }
        // Replies land out of order and are copied into one buffer that
        // the outcome indexes, so slots read as if one server had answered.
        // Every server is waited for even once one has failed: they are all
        // applying their share, and each one's outcome is the failure
        // policy's business.
        let mut index = vec![(Status::Ok as u8, 0u32, 0u32); n].into_boxed_slice();
        let mut merged = BytesMut::new();
        let mut failure = None;
        while let Some(joined_result) = tasks.join_next().await {
            let (at, items, result) = joined(joined_result);
            let node = &inner.nodes[at];
            inner.note(node, &result);
            if let Err(e) =
                result.and_then(|(_, replies)| merge(&mut merged, &mut index, &items, replies))
            {
                failure.get_or_insert(e);
            }
        }
        match failure {
            Some(e) => Err(e),
            None => Ok(Outcome {
                body: merged.freeze(),
                index,
            }),
        }
    }
}

/// Copy one server's replies into the merged body and record where each of
/// them landed, under the batch positions in `items`.
fn merge(
    merged: &mut BytesMut,
    index: &mut [(u8, u32, u32)],
    items: &[u32],
    replies: Bytes,
) -> Result<()> {
    // One server's answers are at most a frame, but several servers'
    // together could pass what an offset into the merged buffer holds.
    let total = merged.len() + replies.len();
    if total > u32::MAX as usize {
        return Err(Error::ResponseTooLarge(total));
    }
    merged.reserve(replies.len());
    let replies = wire::frames(&replies)?;
    if replies.len() != items.len() {
        return Err(Error::Count {
            expected: items.len(),
            got: replies.len(),
        });
    }
    for ((status, body), &i) in replies.zip(items.iter()) {
        index[i as usize] = (status, merged.len() as u32, body.len() as u32);
        merged.put_slice(body);
    }
    Ok(())
}

impl std::fmt::Debug for Cluster {
    /// The servers, each with the shape it is in: `10.0.0.1:4433 up`, or
    /// `down` for one the failure policy has dropped from the ring.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let now = self.inner.now();
        f.debug_list()
            .entries(
                self.inner
                    .nodes
                    .iter()
                    .map(|n| format!("{} {}", n.addr, if n.is_up(now) { "up" } else { "down" })),
            )
            .finish()
    }
}

/// The share of a batch that goes to one server, and where in the batch
/// its items came from.
#[derive(Default)]
struct Group {
    enc: wire::BatchEncoder,
    items: Vec<u32>,
}

/// What a fan-out task returned. A task that is joined at all was not
/// aborted (dropping the set is what aborts the rest when a fan-out gives
/// up early), so the one way one fails to return is a panic in the client,
/// which is re-raised in the caller that was waiting for it.
fn joined<T>(result: std::result::Result<T, tokio::task::JoinError>) -> T {
    result.unwrap_or_else(|e| std::panic::resume_unwind(e.into_panic()))
}
