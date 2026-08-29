//! HTTP front end: the same binary protocol as [`tcp`](crate::tcp), one
//! request per HTTP exchange instead of one per frame. The op is the path,
//! the request body is the frame body, the response body is the frame body,
//! and the frame status maps onto the HTTP status. Nothing is JSON.
//!
//! ```text
//! POST /get   body: keys      -> 200, body: values
//! POST /set   body: entries   -> 200, empty
//! POST /del   body: keys      -> 200, body: flags
//! GET  /health                -> 204, empty (no authentication)
//!
//! 400 bad request | 401 unauthorized | 404 unknown path | 405 wrong method
//! 413 too large   (the body is a UTF-8 message, as on TCP)
//! ```
//!
//! With a token configured, every request except `/health` must carry
//! `Authorization: Bearer <token>`. HTTP/1.1 only, keep-alive on; there is
//! no CORS handling.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::{Body, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode, header};
use hyper_util::rt::TokioIo;
use oxicache_wire::io::FrameWriter;
use oxicache_wire::{self as wire, Op, Status};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::cache::Cache;
use crate::error::{Error, Result};
use crate::tcp::{self, MAX_FRAME, Options};

/// How long in-flight requests get to finish at shutdown.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
const ACCEPT_BACKOFF: Duration = Duration::from_millis(50);

pub struct HttpServer {
    listener: std::net::TcpListener,
    cache: Arc<Cache>,
    token: Option<Arc<[u8]>>,
}

impl HttpServer {
    /// Bind `addr` serving `cache`, without authentication.
    pub fn bind(addr: SocketAddr, cache: Arc<Cache>) -> Result<Self> {
        Self::bind_with(addr, cache, Options::default())
    }

    /// Bind with explicit [`Options`].
    pub fn bind_with(addr: SocketAddr, cache: Arc<Cache>, opts: Options) -> Result<Self> {
        let listener = tcp::listener(addr).map_err(|source| Error::Bind { addr, source })?;
        Ok(Self {
            listener,
            cache,
            token: opts.token.map(Arc::from),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr().expect("bound listener")
    }

    /// Accept connections on the current runtime until the task is dropped.
    pub async fn run(&self) {
        self.run_until(std::future::pending::<()>()).await;
    }

    /// Accept connections until `shutdown` resolves, then stop accepting,
    /// tell open connections to finish their current request and close.
    pub async fn run_until(&self, shutdown: impl Future<Output = ()>) {
        info!(addr = %self.local_addr(), "listening (http)");
        let l = self.listener.try_clone().expect("clone listener");
        let (stop_tx, stop_rx) = watch::channel(false);
        let mut conns = JoinSet::new();
        tokio::select! {
            _ = accept_loop(l, self.cache.clone(), self.token.clone(), stop_rx, &mut conns) => {}
            _ = shutdown => {}
        }
        if conns.is_empty() {
            return;
        }
        info!(open = conns.len(), "draining http connections");
        let _ = stop_tx.send(true);
        let _ = tokio::time::timeout(DRAIN_TIMEOUT, async {
            while conns.join_next().await.is_some() {}
        })
        .await;
        conns.shutdown().await;
    }
}

async fn accept_loop(
    listener: std::net::TcpListener,
    cache: Arc<Cache>,
    token: Option<Arc<[u8]>>,
    stop: watch::Receiver<bool>,
    conns: &mut JoinSet<()>,
) {
    let listener = TcpListener::from_std(listener).expect("register listener");
    loop {
        while conns.try_join_next().is_some() {}
        match listener.accept().await {
            Ok((stream, remote)) => {
                let (cache, token, mut stop) = (cache.clone(), token.clone(), stop.clone());
                conns.spawn(async move {
                    debug!(%remote, "http connection open");
                    let _ = stream.set_nodelay(true);
                    let svc = service_fn(move |req| {
                        let (cache, token) = (cache.clone(), token.clone());
                        async move { Ok::<_, hyper::Error>(handle(req, &cache, token.as_deref()).await) }
                    });
                    let conn = http1::Builder::new().serve_connection(TokioIo::new(stream), svc);
                    tokio::pin!(conn);
                    let res = tokio::select! {
                        r = conn.as_mut() => r,
                        _ = stop.changed() => {
                            conn.as_mut().graceful_shutdown();
                            conn.await
                        }
                    };
                    if let Err(e) = res {
                        debug!(%remote, error = %e, "http connection closed");
                    }
                });
            }
            Err(e) => {
                warn!(error = %e, "accept failed");
                tokio::time::sleep(ACCEPT_BACKOFF).await;
            }
        }
    }
}

fn reply(code: StatusCode, body: Bytes, binary: bool) -> Response<Full<Bytes>> {
    let mut res = Response::new(Full::new(body));
    *res.status_mut() = code;
    if res.status() != StatusCode::NO_CONTENT {
        res.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static(if binary {
                "application/octet-stream"
            } else {
                "text/plain; charset=utf-8"
            }),
        );
    }
    res
}

fn text(code: StatusCode, msg: impl Into<Bytes>) -> Response<Full<Bytes>> {
    reply(code, msg.into(), false)
}

fn authorized(req: &Request<Incoming>, token: &[u8]) -> bool {
    req.headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.as_bytes().strip_prefix(b"Bearer "))
        .is_some_and(|t| tcp::ct_eq(token, t))
}

async fn handle(
    req: Request<Incoming>,
    cache: &Cache,
    token: Option<&[u8]>,
) -> Response<Full<Bytes>> {
    let op = match req.uri().path() {
        "/health" => return reply(StatusCode::NO_CONTENT, Bytes::new(), false),
        "/get" => Op::Get,
        "/set" => Op::Set,
        "/del" => Op::Del,
        p => return text(StatusCode::NOT_FOUND, format!("unknown path {p}")),
    };
    if req.method() != Method::POST {
        return text(StatusCode::METHOD_NOT_ALLOWED, "use POST");
    }
    if let Some(t) = token
        && !authorized(&req, t)
    {
        return text(StatusCode::UNAUTHORIZED, "auth required");
    }
    // Refuse by the announced length before reading anything, and by the
    // actual length while reading, so an unannounced (chunked) body is
    // bounded too.
    let claimed = req.body().size_hint().lower() as usize;
    if claimed > MAX_FRAME {
        return text(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!("frame body of {claimed} bytes exceeds the limit"),
        );
    }
    let body = match Limited::new(req.into_body(), MAX_FRAME).collect().await {
        Ok(b) => b.to_bytes(),
        Err(e) if e.is::<http_body_util::LengthLimitError>() => {
            return text(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("frame body exceeds the limit of {MAX_FRAME} bytes"),
            );
        }
        Err(e) => return text(StatusCode::BAD_REQUEST, e.to_string()),
    };
    let mut out = FrameWriter::new();
    tcp::dispatch(op as u8, &body, cache, &mut out);
    let raw = Bytes::from(out.take());
    let (status, len) = wire::decode_header(raw[..wire::HEADER_LEN].try_into().expect("header"));
    debug_assert_eq!(raw.len(), wire::HEADER_LEN + len);
    let body = raw.slice(wire::HEADER_LEN..);
    match Status::from_u8(status) {
        Some(Status::Ok) => reply(StatusCode::OK, body, true),
        Some(Status::BadRequest) => reply(StatusCode::BAD_REQUEST, body, false),
        Some(Status::TooLarge) => reply(StatusCode::PAYLOAD_TOO_LARGE, body, false),
        Some(Status::Unauthorized) => reply(StatusCode::UNAUTHORIZED, body, false),
        // The path already selected a known op, so this cannot happen.
        Some(Status::UnknownOp) | None => reply(StatusCode::INTERNAL_SERVER_ERROR, body, false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    fn server(token: Option<&[u8]>) -> Arc<HttpServer> {
        let cache = Arc::new(Cache::new(1 << 20, 1));
        let opts = Options {
            token: token.map(<[u8]>::to_vec),
        };
        Arc::new(HttpServer::bind_with("127.0.0.1:0".parse().unwrap(), cache, opts).unwrap())
    }

    /// Minimal HTTP/1.1 client: one request on `c`, returns status and body.
    async fn call(
        c: &mut TcpStream,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> (u16, Vec<u8>) {
        let mut req = format!("{method} {path} HTTP/1.1\r\nHost: x\r\n");
        for (k, v) in headers {
            req.push_str(&format!("{k}: {v}\r\n"));
        }
        req.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
        c.write_all(req.as_bytes()).await.unwrap();
        c.write_all(body).await.unwrap();
        let mut buf = Vec::new();
        let head_end = loop {
            let mut chunk = [0u8; 4096];
            let n = c.read(&mut chunk).await.unwrap();
            assert!(n > 0, "connection closed before a response");
            buf.extend_from_slice(&chunk[..n]);
            if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break i + 4;
            }
        };
        let head = String::from_utf8(buf[..head_end].to_vec()).unwrap();
        let status: u16 = head.split_whitespace().nth(1).unwrap().parse().unwrap();
        let len: usize = head
            .lines()
            .find_map(|l| {
                l.to_ascii_lowercase()
                    .strip_prefix("content-length: ")
                    .map(String::from)
            })
            .map_or(0, |v| v.trim().parse().unwrap());
        let mut body = buf[head_end..].to_vec();
        while body.len() < len {
            let mut chunk = vec![0u8; len - body.len()];
            let n = c.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            body.extend_from_slice(&chunk[..n]);
        }
        assert_eq!(body.len(), len);
        (status, body)
    }

    async fn connect(s: &Arc<HttpServer>) -> TcpStream {
        let addr = s.local_addr();
        let srv = s.clone();
        tokio::spawn(async move { srv.run().await });
        TcpStream::connect(addr).await.unwrap()
    }

    #[tokio::test]
    async fn roundtrip_on_one_keepalive_connection() {
        let s = server(None);
        let mut c = connect(&s).await;
        let (st, body) = call(&mut c, "GET", "/health", &[], b"").await;
        assert_eq!((st, body.len()), (204, 0));
        let entries = wire::encode_entries([(&b"k"[..], &b"v"[..])]);
        let (st, body) = call(&mut c, "POST", "/set", &[], &entries).await;
        assert_eq!((st, body.len()), (200, 0));
        let keys = wire::encode_keys([&b"k"[..], &b"x"[..]]);
        let (st, body) = call(&mut c, "POST", "/get", &[], &keys).await;
        assert_eq!(st, 200);
        assert_eq!(
            wire::decode_values(body.into()).unwrap(),
            vec![Some(Bytes::from_static(b"v")), None]
        );
        let (st, body) = call(&mut c, "POST", "/del", &[], &keys).await;
        assert_eq!(st, 200);
        assert_eq!(wire::decode_flags(body.into()).unwrap(), vec![true, false]);
    }

    #[tokio::test]
    async fn errors_map_to_http_statuses() {
        let s = server(None);
        let mut c = connect(&s).await;
        let (st, body) = call(&mut c, "POST", "/nope", &[], b"").await;
        assert_eq!(st, 404);
        assert_eq!(body, b"unknown path /nope");
        assert_eq!(call(&mut c, "GET", "/get", &[], b"").await.0, 405);
        let (st, body) = call(&mut c, "POST", "/get", &[], &[9, 0]).await;
        assert_eq!(st, 400);
        assert!(
            std::str::from_utf8(&body)
                .unwrap()
                .contains("unexpected end")
        );
        // Announced length over the limit: refused without reading the body.
        let mut req = format!(
            "POST /get HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n",
            MAX_FRAME + 1
        );
        req.push_str("");
        c.write_all(req.as_bytes()).await.unwrap();
        let mut buf = vec![0u8; 4096];
        let n = c.read(&mut buf).await.unwrap();
        let head = String::from_utf8_lossy(&buf[..n]);
        assert!(head.starts_with("HTTP/1.1 413"), "{head}");
        // A SET whose entry does not fit the cache: too large from dispatch.
        let mut c = TcpStream::connect(s.local_addr()).await.unwrap();
        let big = vec![0u8; 2 << 20];
        let entries = wire::encode_entries([(&b"big"[..], &big[..])]);
        assert_eq!(call(&mut c, "POST", "/set", &[], &entries).await.0, 413);
    }

    #[tokio::test]
    async fn chunked_body_over_the_limit_is_refused() {
        let s = server(None);
        let mut c = connect(&s).await;
        c.write_all(b"POST /get HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n")
            .await
            .unwrap();
        let chunk = vec![b'a'; 1 << 20];
        let mut sent = 0;
        let mut head = Vec::new();
        // Keep streaming until the server answers (it stops reading at the limit).
        while sent <= MAX_FRAME + (1 << 20) {
            c.write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                .await
                .unwrap();
            if c.write_all(&chunk).await.is_err() {
                break;
            }
            c.write_all(b"\r\n").await.unwrap();
            sent += chunk.len();
            let mut buf = [0u8; 4096];
            match tokio::time::timeout(Duration::from_millis(1), c.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 => {
                    head.extend_from_slice(&buf[..n]);
                    break;
                }
                _ => {}
            }
        }
        if head.is_empty() {
            let mut buf = [0u8; 4096];
            let n = c.read(&mut buf).await.unwrap();
            head.extend_from_slice(&buf[..n]);
        }
        let head = String::from_utf8_lossy(&head);
        assert!(head.starts_with("HTTP/1.1 413"), "{head}");
    }

    #[tokio::test]
    async fn bearer_token_is_required_except_for_health() {
        let s = server(Some(b"s3cret"));
        let mut c = connect(&s).await;
        assert_eq!(call(&mut c, "GET", "/health", &[], b"").await.0, 204);
        let keys = wire::encode_keys([&b"k"[..]]);
        // A refused request's body is never read, so hyper closes the
        // connection after the 401; each attempt gets a fresh one.
        let (st, body) = call(&mut c, "POST", "/get", &[], &keys).await;
        assert_eq!((st, &body[..]), (401, &b"auth required"[..]));
        for auth in ["Bearer s3cre", "Basic s3cret", "bearer s3cret"] {
            let mut c = TcpStream::connect(s.local_addr()).await.unwrap();
            let h = [("Authorization", auth)];
            assert_eq!(
                call(&mut c, "POST", "/get", &h, &keys).await.0,
                401,
                "{auth}"
            );
        }
        let mut c = TcpStream::connect(s.local_addr()).await.unwrap();
        let right = [("Authorization", "Bearer s3cret")];
        assert_eq!(call(&mut c, "POST", "/get", &right, &keys).await.0, 200);
        assert_eq!(call(&mut c, "POST", "/get", &right, &keys).await.0, 200);
    }

    #[test]
    fn bind_failure_is_reported() {
        let first = server(None);
        let cache = Arc::new(Cache::new(1 << 20, 1));
        let err = HttpServer::bind(first.local_addr(), cache)
            .err()
            .expect("port in use");
        assert!(matches!(err, Error::Bind { addr, .. } if addr == first.local_addr()));
    }

    #[tokio::test]
    async fn shutdown_closes_idle_keepalive_connections() {
        let s = server(None);
        let addr = s.local_addr();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let run = tokio::spawn(async move {
            s.run_until(async {
                let _ = rx.await;
            })
            .await;
        });
        let mut c = TcpStream::connect(addr).await.unwrap();
        assert_eq!(call(&mut c, "GET", "/health", &[], b"").await.0, 204);
        tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("an idle keep-alive connection is closed by graceful shutdown")
            .unwrap();
        let mut rest = Vec::new();
        c.read_to_end(&mut rest).await.unwrap();
        assert!(rest.is_empty());
    }

    #[tokio::test]
    async fn broken_request_is_reported_not_fatal() {
        let s = server(None);
        let mut c = connect(&s).await;
        c.write_all(b"NOT HTTP\r\n\r\n").await.unwrap();
        let mut rest = Vec::new();
        c.read_to_end(&mut rest).await.unwrap();
        assert!(String::from_utf8_lossy(&rest).starts_with("HTTP/1.1 400"));
        let mut c = TcpStream::connect(s.local_addr()).await.unwrap();
        assert_eq!(call(&mut c, "GET", "/health", &[], b"").await.0, 204);
    }
}
