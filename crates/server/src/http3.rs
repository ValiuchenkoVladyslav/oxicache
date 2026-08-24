//! HTTP/3 front end: one tokio task per QUIC connection, one per request.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use http::{Method, Request, Response, StatusCode};
use oxicache_wire as wire;
use rustls_pki_types::CertificateDer;
use tracing::{debug, info, warn};

use crate::cache::Cache;
use crate::tls::Identity;

type H3Conn = h3::server::Connection<h3_quinn::Connection, Bytes>;
type H3Stream = h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>;

pub struct Server {
    endpoint: quinn::Endpoint,
    cache: Arc<Cache>,
    cert: CertificateDer<'static>,
}

impl Server {
    /// Bind a QUIC endpoint on `addr` serving `cache` with the given identity.
    pub fn bind(addr: SocketAddr, identity: Identity, cache: Arc<Cache>) -> Result<Self> {
        let cert = identity
            .certs
            .first()
            .context("identity has no certificate")?
            .clone();
        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(identity.server_config()?)?;
        let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
        let mut transport = quinn::TransportConfig::default();
        transport.max_concurrent_bidi_streams(4096u32.into());
        config.transport_config(Arc::new(transport));
        let endpoint =
            quinn::Endpoint::server(config, addr).with_context(|| format!("binding {addr}"))?;
        Ok(Self {
            endpoint,
            cache,
            cert,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.endpoint.local_addr().expect("bound endpoint")
    }

    /// The leaf certificate clients may pin when it is self-signed.
    pub fn cert(&self) -> &CertificateDer<'static> {
        &self.cert
    }

    /// Accept connections until the endpoint is closed via [`Server::close`].
    pub async fn run(&self) {
        info!(addr = %self.local_addr(), "listening (h3)");
        while let Some(incoming) = self.endpoint.accept().await {
            let cache = self.cache.clone();
            tokio::spawn(async move {
                match incoming.await {
                    Ok(conn) => serve_connection(conn, cache).await,
                    Err(e) => debug!(error = %e, "handshake failed"),
                }
            });
        }
    }

    pub async fn close(&self) {
        self.endpoint.close(0u32.into(), b"shutdown");
        self.endpoint.wait_idle().await;
    }
}

async fn serve_connection(conn: quinn::Connection, cache: Arc<Cache>) {
    let remote = conn.remote_address();
    let mut h3: H3Conn = match h3::server::Connection::new(h3_quinn::Connection::new(conn)).await {
        Ok(c) => c,
        Err(e) => return debug!(%remote, error = %e, "h3 setup failed"),
    };
    debug!(%remote, "connection open");
    loop {
        match h3.accept().await {
            Ok(Some(resolver)) => {
                let cache = cache.clone();
                tokio::spawn(async move {
                    match resolver.resolve_request().await {
                        Ok((req, stream)) => {
                            if let Err(e) = serve_request(req, stream, &cache).await {
                                debug!(error = %e, "request failed");
                            }
                        }
                        Err(e) => debug!(error = %e, "bad request headers"),
                    }
                });
            }
            Ok(None) => break,
            Err(e) => {
                warn!(%remote, error = %e, "connection error");
                break;
            }
        }
    }
    debug!(%remote, "connection closed");
}

async fn serve_request(req: Request<()>, mut stream: H3Stream, cache: &Cache) -> Result<()> {
    let mut body = BytesMut::new();
    while let Some(chunk) = stream.recv_data().await? {
        body.put(chunk);
    }
    let (status, out) = dispatch(req.method(), req.uri().path(), body.freeze(), cache);
    stream
        .send_response(Response::builder().status(status).body(()).unwrap())
        .await?;
    if !out.is_empty() {
        stream.send_data(out).await?;
    }
    stream.finish().await?;
    Ok(())
}

/// Route a request to the cache and produce the response status and body.
pub fn dispatch(method: &Method, path: &str, body: Bytes, cache: &Cache) -> (StatusCode, Bytes) {
    if method != Method::POST {
        return (StatusCode::METHOD_NOT_ALLOWED, Bytes::new());
    }
    let res = match path {
        wire::path::GET => wire::decode_keys(body).map(|keys| {
            let values: Vec<Option<Bytes>> = keys.iter().map(|k| cache.get(k)).collect();
            wire::encode_values(values.iter().map(Option::as_deref))
        }),
        wire::path::SET => wire::decode_entries(body).map(|entries| {
            for (k, v) in entries {
                // Copy out of the request buffer so cached data never pins the whole body.
                cache.set(Bytes::copy_from_slice(&k), Bytes::copy_from_slice(&v));
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
