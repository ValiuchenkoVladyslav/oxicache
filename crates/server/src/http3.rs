//! HTTP/3 front end: one tokio task per QUIC connection, one per request.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use http::{Method, Request, Response, StatusCode};
use oxicache_wire as wire;
use rustls_pki_types::CertificateDer;
use tracing::{debug, info, warn};

use crate::cache::Cache;
use crate::error::{Error, Result};
use crate::tls::Identity;

/// Upper bound on body capacity reserved up front from `content-length`.
const MAX_PREALLOC: usize = 16 << 20;

type H3Conn = h3::server::Connection<h3_quinn::Connection, Bytes>;
type H3Stream = h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>;

pub struct Server {
    endpoints: Vec<quinn::Endpoint>,
    cache: Arc<Cache>,
    cert: CertificateDer<'static>,
}

impl Server {
    /// Bind a single QUIC endpoint on `addr` serving `cache` with the given identity.
    pub fn bind(addr: SocketAddr, identity: Identity, cache: Arc<Cache>) -> Result<Self> {
        Self::bind_with(addr, identity, cache, Options::default())
    }

    /// Bind with explicit transport [`Options`].
    pub fn bind_with(
        addr: SocketAddr,
        identity: Identity,
        cache: Arc<Cache>,
        opts: Options,
    ) -> Result<Self> {
        if opts.endpoints == 0 {
            return Err(Error::NoEndpoints);
        }
        let cert = identity.certs.first().ok_or(Error::NoCertificate)?.clone();
        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(identity.server_config()?)?;
        let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
        config.transport_config(Arc::new(transport_config(&opts)));

        let bind = |source| Error::Bind { addr, source };
        let mut eps = Vec::with_capacity(opts.endpoints);
        let mut bound = addr;
        for _ in 0..opts.endpoints {
            let socket = udp_socket(bound, opts.endpoints > 1).map_err(bind)?;
            let ep = quinn::Endpoint::new(
                quinn::EndpointConfig::default(),
                Some(config.clone()),
                socket,
                Arc::new(quinn::TokioRuntime),
            )
            .map_err(bind)?;
            // Port 0 must resolve once so every endpoint shares the same port.
            bound = ep.local_addr().map_err(bind)?;
            eps.push(ep);
        }
        Ok(Self {
            endpoints: eps,
            cache,
            cert,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.endpoints[0].local_addr().expect("bound endpoint")
    }

    /// The leaf certificate clients may pin when it is self-signed.
    pub fn cert(&self) -> &CertificateDer<'static> {
        &self.cert
    }

    /// Accept connections until the endpoints are closed via [`Server::close`].
    pub async fn run(&self) {
        info!(addr = %self.local_addr(), endpoints = self.endpoints.len(), "listening (h3)");
        let mut tasks = tokio::task::JoinSet::new();
        for ep in &self.endpoints {
            tasks.spawn(accept_loop(ep.clone(), self.cache.clone()));
        }
        while tasks.join_next().await.is_some() {}
    }

    /// Thread-per-core mode: every endpoint gets its own OS thread running a
    /// single-threaded tokio runtime, so a connection never crosses threads.
    /// Blocks the calling thread until all endpoints are closed.
    pub fn run_per_core(&self) {
        info!(addr = %self.local_addr(), endpoints = self.endpoints.len(), "listening (h3, thread-per-core)");
        std::thread::scope(|scope| {
            for (i, ep) in self.endpoints.iter().enumerate() {
                let (ep, cache) = (ep.clone(), self.cache.clone());
                std::thread::Builder::new()
                    .name(format!("oxicache-{i}"))
                    .spawn_scoped(scope, move || {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .expect("runtime");
                        rt.block_on(accept_loop(ep, cache));
                    })
                    .expect("spawn endpoint thread");
            }
        });
    }

    pub async fn close(&self) {
        for ep in &self.endpoints {
            ep.close(0u32.into(), b"shutdown");
        }
        for ep in &self.endpoints {
            ep.wait_idle().await;
        }
    }
}

/// Transport tuning for [`Server::bind_with`].
#[derive(Clone, Debug)]
pub struct Options {
    /// Number of QUIC endpoints sharing the port via `SO_REUSEPORT`, so the
    /// kernel spreads connections over independent sockets and driver tasks.
    /// Connection migration across addresses is not supported when > 1.
    pub endpoints: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self { endpoints: 1 }
    }
}

fn transport_config(_opts: &Options) -> quinn::TransportConfig {
    let mut t = quinn::TransportConfig::default();
    t.max_concurrent_bidi_streams(4096u32.into());
    t
}

fn udp_socket(addr: SocketAddr, reuse_port: bool) -> std::io::Result<std::net::UdpSocket> {
    use socket2::{Domain, Protocol, Socket, Type};
    let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
    if reuse_port {
        socket.set_reuse_port(true)?;
    }
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    Ok(socket.into())
}

async fn accept_loop(ep: quinn::Endpoint, cache: Arc<Cache>) {
    while let Some(incoming) = ep.accept().await {
        let cache = cache.clone();
        tokio::spawn(async move {
            match incoming.await {
                Ok(conn) => serve_connection(conn, cache).await,
                Err(e) => debug!(error = %e, "handshake failed"),
            }
        });
    }
}

/// Requests are handled inline on the connection task rather than spawned:
/// every stream of a connection shares quinn's connection lock anyway, and
/// keeping them on one task avoids task allocation, cross-thread wakeups and
/// lock contention (measured −13…−32 % CPU per request, see docs/performance.md).
async fn serve_connection(conn: quinn::Connection, cache: Arc<Cache>) {
    let remote = conn.remote_address();
    let mut h3: H3Conn = match h3::server::Connection::new(h3_quinn::Connection::new(conn)).await {
        Ok(c) => c,
        Err(e) => return debug!(%remote, error = %e, "h3 setup failed"),
    };
    debug!(%remote, "connection open");
    loop {
        match h3.accept().await {
            Ok(Some(resolver)) => handle(resolver, &cache).await,
            Ok(None) => break,
            Err(e) if e.is_h3_no_error() => break,
            Err(e) => {
                warn!(%remote, error = %e, "connection error");
                break;
            }
        }
    }
    debug!(%remote, "connection closed");
}

async fn handle(resolver: h3::server::RequestResolver<h3_quinn::Connection, Bytes>, cache: &Cache) {
    match resolver.resolve_request().await {
        Ok((req, stream)) => {
            if let Err(e) = serve_request(req, stream, cache).await {
                debug!(error = %e, "request failed");
            }
        }
        Err(e) => debug!(error = %e, "bad request headers"),
    }
}

async fn serve_request(req: Request<()>, mut stream: H3Stream, cache: &Cache) -> Result<()> {
    let hint = req
        .headers()
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok()?.parse::<usize>().ok())
        .unwrap_or(0)
        .min(MAX_PREALLOC);
    let body = read_body(&mut stream, hint).await?;
    let (status, out) = dispatch(req.method(), req.uri().path(), body, cache);
    stream
        .send_response(Response::builder().status(status).body(()).unwrap())
        .await?;
    if !out.is_empty() {
        stream.send_data(out).await?;
    }
    stream.finish().await?;
    Ok(())
}

/// Collect the request body. A single-chunk body is taken without copying:
/// h3-quinn yields `Bytes`, whose `copy_to_bytes` is a refcount bump.
async fn read_body(stream: &mut H3Stream, hint: usize) -> Result<Bytes> {
    let Some(mut first) = stream.recv_data().await? else {
        return Ok(Bytes::new());
    };
    let first = first.copy_to_bytes(first.remaining());
    let Some(mut second) = stream.recv_data().await? else {
        return Ok(first);
    };
    let mut body = BytesMut::with_capacity(hint.max(first.len() + second.remaining()));
    body.extend_from_slice(&first);
    body.put(&mut second);
    while let Some(mut chunk) = stream.recv_data().await? {
        body.put(&mut chunk);
    }
    Ok(body.freeze())
}

/// Route a request to the cache and produce the response status and body.
pub fn dispatch(method: &Method, path: &str, body: Bytes, cache: &Cache) -> (StatusCode, Bytes) {
    if method != Method::POST {
        return (StatusCode::METHOD_NOT_ALLOWED, Bytes::new());
    }
    let res = match path {
        wire::path::GET => wire::decode_keys(body).map(|keys| {
            let mut out = wire::ValuesEncoder::with_capacity(keys.len(), keys.len() * 64);
            for k in &keys {
                out.push(cache.get(k).as_deref());
            }
            out.finish()
        }),
        wire::path::SET => wire::decode_entries(body).map(|entries| {
            for (k, v) in entries {
                // Copy out of the request buffer so cached data never pins the whole
                // body. Key and value share one allocation when the map is known to
                // drop the old key object on overwrite.
                if crate::cache::Map::REPLACES_KEY {
                    let mut buf = BytesMut::with_capacity(k.len() + v.len());
                    buf.extend_from_slice(&k);
                    buf.extend_from_slice(&v);
                    let buf = buf.freeze();
                    cache.set(buf.slice(..k.len()), buf.slice(k.len()..));
                } else {
                    cache.set(Bytes::copy_from_slice(&k), Bytes::copy_from_slice(&v));
                }
            }
            Bytes::new()
        }),
        wire::path::DEL => wire::decode_keys(body).map(|keys| {
            let flags: Vec<bool> = keys.iter().map(|k| cache.del(k)).collect();
            wire::encode_flags(&flags)
        }),
        _ => return (StatusCode::NOT_FOUND, Bytes::new()),
    };
    match res {
        Ok(out) => (StatusCode::OK, out),
        Err(e) => (StatusCode::BAD_REQUEST, Bytes::from(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_roundtrip() {
        let cache = Cache::new(1 << 20, 2);
        let (st, _) = dispatch(
            &Method::POST,
            "/set",
            wire::encode_entries([(&b"k"[..], &b"v"[..])]),
            &cache,
        );
        assert_eq!(st, StatusCode::OK);
        let (st, body) = dispatch(
            &Method::POST,
            "/get",
            wire::encode_keys([&b"k"[..], &b"x"[..]]),
            &cache,
        );
        assert_eq!(st, StatusCode::OK);
        assert_eq!(
            wire::decode_values(body).unwrap(),
            vec![Some(Bytes::from_static(b"v")), None]
        );
        let (st, body) = dispatch(
            &Method::POST,
            "/del",
            wire::encode_keys([&b"k"[..], &b"x"[..]]),
            &cache,
        );
        assert_eq!(st, StatusCode::OK);
        assert_eq!(wire::decode_flags(body).unwrap(), vec![true, false]);
    }

    #[test]
    fn dispatch_errors() {
        let cache = Cache::new(1 << 20, 1);
        assert_eq!(
            dispatch(&Method::GET, "/get", Bytes::new(), &cache).0,
            StatusCode::METHOD_NOT_ALLOWED
        );
        assert_eq!(
            dispatch(&Method::POST, "/nope", Bytes::new(), &cache).0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            dispatch(&Method::POST, "/get", Bytes::from_static(&[9, 0]), &cache).0,
            StatusCode::BAD_REQUEST
        );
    }
}
