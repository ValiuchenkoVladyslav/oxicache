//! HTTP front end: the same binary protocol as [`tcp`](crate::tcp), one
//! request per HTTP exchange instead of one per frame. The op is the path,
//! the request body is the frame body, the response body is the frame body,
//! and the frame status maps onto the HTTP status. Nothing is JSON.
//!
//! ```text
//! POST /get   body: keys      -> 200, body: values
//! POST /set   body: entries   -> 200, empty
//! POST /del   body: keys      -> 200, body: flags
//! POST /ping                  -> 200, empty
//! GET  /health                -> 200, empty (no authentication)
//!
//! 400 bad request | 401 unauthorized | 404 unknown path | 405 wrong method
//! 413 too large   (the body is a UTF-8 message, as on TCP)
//! ```
//!
//! Every request except `/health` must carry
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
use hyper_util::rt::{TokioIo, TokioTimer};
use oxicache_wire::io::FrameWriter;
use oxicache_wire::{self as wire, Op, Status};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::cache::Cache;
use crate::error::{Error, Result};
use crate::tcp::{self, ConnLimit, MAX_FRAME, Options};

/// How long in-flight requests get to finish at shutdown.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
const ACCEPT_BACKOFF: Duration = Duration::from_millis(50);

pub struct HttpServer {
    listener: std::net::TcpListener,
    cache: Arc<Cache>,
    token: Arc<[u8]>,
    idle_timeout: Option<Duration>,
    limit: Option<Arc<ConnLimit>>,
}

impl HttpServer {
    /// Bind `addr` serving `cache`; every request except `/health` must
    /// carry `opts.token`.
    pub fn bind(addr: SocketAddr, cache: Arc<Cache>, opts: Options) -> Result<Self> {
        let token = tcp::token(&opts)?;
        let listener = tcp::listener(addr).map_err(|source| Error::Bind { addr, source })?;
        Ok(Self {
            listener,
            cache,
            token,
            idle_timeout: opts.idle_timeout,
            limit: opts.limit,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr().expect("bound listener")
    }

    /// Connections open under the shared limit, if there is one.
    pub fn open_connections(&self) -> Option<usize> {
        self.limit.as_ref().map(|l| l.open())
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
            _ = accept_loop(l, self, stop_rx, &mut conns) => {}
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
    server: &HttpServer,
    stop: watch::Receiver<bool>,
    conns: &mut JoinSet<()>,
) {
    let listener = TcpListener::from_std(listener).expect("register listener");
    loop {
        while conns.try_join_next().is_some() {}
        let permit = tcp::admit(server.limit.as_ref()).await;
        match listener.accept().await {
            Ok((stream, remote)) => {
                let (cache, token, mut stop) =
                    (server.cache.clone(), server.token.clone(), stop.clone());
                let idle = server.idle_timeout;
                let slot = tcp::Slot::open(server.limit.as_ref(), permit);
                conns.spawn(async move {
                    let _slot = slot;
                    debug!(%remote, "http connection open");
                    let _ = stream.set_nodelay(true);
                    let svc = service_fn(move |req| {
                        let (cache, token) = (cache.clone(), token.clone());
                        async move { Ok::<_, hyper::Error>(handle(req, &cache, &token).await) }
                    });
                    // hyper re-arms its header timeout for every request on
                    // a keep-alive connection, so it doubles as the idle
                    // timeout. `None` is passed explicitly: with a timer
                    // installed hyper would otherwise fall back to its own
                    // 30 s default.
                    let mut builder = http1::Builder::new();
                    builder.timer(TokioTimer::new()).header_read_timeout(idle);
                    let conn = builder.serve_connection(TokioIo::new(stream), svc);
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
    res.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static(if binary {
            "application/octet-stream"
        } else {
            "text/plain; charset=utf-8"
        }),
    );
    res
}

fn text(code: StatusCode, msg: impl Into<Bytes>) -> Response<Full<Bytes>> {
    reply(code, msg.into(), false)
}

/// Every response sent before the token has been verified closes the
/// connection: an unauthenticated peer never gets to keep one alive.
fn closing(mut res: Response<Full<Bytes>>) -> Response<Full<Bytes>> {
    res.headers_mut().insert(
        header::CONNECTION,
        header::HeaderValue::from_static("close"),
    );
    res
}

fn authorized(req: &Request<Incoming>, token: &[u8]) -> bool {
    req.headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.as_bytes().strip_prefix(b"Bearer "))
        .is_some_and(|t| tcp::ct_eq(token, t))
}

async fn handle(req: Request<Incoming>, cache: &Cache, token: &[u8]) -> Response<Full<Bytes>> {
    let op = match req.uri().path() {
        // Status code only, no body: a probe target.
        "/health" => return closing(reply(StatusCode::OK, Bytes::new(), false)),
        "/get" => Op::Get,
        "/set" => Op::Set,
        "/del" => Op::Del,
        "/ping" => Op::Ping,
        p => return closing(text(StatusCode::NOT_FOUND, format!("unknown path {p}"))),
    };
    if req.method() != Method::POST {
        return closing(text(StatusCode::METHOD_NOT_ALLOWED, "use POST"));
    }
    if !authorized(&req, token) {
        return closing(text(StatusCode::UNAUTHORIZED, "auth required"));
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
    use std::num::NonZeroUsize;

    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    fn opts() -> Options {
        Options::new(b"s3cret".to_vec())
    }

    fn server() -> Arc<HttpServer> {
        server_with(opts())
    }

    fn server_with(opts: Options) -> Arc<HttpServer> {
        let cache = Arc::new(Cache::new(
            NonZeroUsize::new(1 << 20).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ));
        Arc::new(HttpServer::bind("127.0.0.1:0".parse().unwrap(), cache, opts).unwrap())
    }

    const AUTH: [(&str, &str); 1] = [("Authorization", "Bearer s3cret")];

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
        let s = server();
        let mut c = connect(&s).await;
        let (st, body) = call(&mut c, "GET", "/health", &[], b"").await;
        assert_eq!((st, body.len()), (200, 0));
        // `/health` closed that one; the token keeps the next one open.
        let mut c = connect(&s).await;
        let entries = wire::encode_entries([(&b"k"[..], &b"v"[..])]);
        let (st, body) = call(&mut c, "POST", "/set", &AUTH, &entries).await;
        assert_eq!((st, body.len()), (200, 0));
        let keys = wire::encode_keys([&b"k"[..], &b"x"[..]]);
        let (st, body) = call(&mut c, "POST", "/get", &AUTH, &keys).await;
        assert_eq!(st, 200);
        assert_eq!(
            wire::decode_values(body.into()).unwrap(),
            vec![Some(Bytes::from_static(b"v")), None]
        );
        let (st, body) = call(&mut c, "POST", "/del", &AUTH, &keys).await;
        assert_eq!(st, 200);
        assert_eq!(wire::decode_flags(body.into()).unwrap(), vec![true, false]);
    }

    #[tokio::test]
    async fn errors_map_to_http_statuses() {
        let s = server();
        let mut c = connect(&s).await;
        let (st, body) = call(&mut c, "POST", "/nope", &[], b"").await;
        assert_eq!(st, 404);
        assert_eq!(body, b"unknown path /nope");
        let mut c = connect(&s).await;
        assert_eq!(call(&mut c, "GET", "/get", &[], b"").await.0, 405);
        let mut c = connect(&s).await;
        let (st, body) = call(&mut c, "POST", "/get", &AUTH, &[9, 0]).await;
        assert_eq!(st, 400);
        assert!(
            std::str::from_utf8(&body)
                .unwrap()
                .contains("unexpected end")
        );
        // Announced length over the limit: refused without reading the body.
        let mut req = format!(
            "POST /get HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer s3cret\r\nContent-Length: {}\r\n\r\n",
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
        assert_eq!(call(&mut c, "POST", "/set", &AUTH, &entries).await.0, 413);
    }

    #[tokio::test]
    async fn chunked_body_over_the_limit_is_refused() {
        let s = server();
        let mut c = connect(&s).await;
        c.write_all(
            b"POST /get HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer s3cret\r\nTransfer-Encoding: chunked\r\n\r\n",
        )
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
    async fn ping_needs_a_token_and_answers_empty() {
        let s = server();
        let mut c = connect(&s).await;
        assert_eq!(call(&mut c, "POST", "/ping", &[], b"").await.0, 401);
        let mut c = connect(&s).await;
        let (st, body) = call(&mut c, "POST", "/ping", &AUTH, b"").await;
        assert_eq!((st, &body[..]), (200, &b""[..]));
        assert_eq!(call(&mut c, "GET", "/ping", &[], b"").await.0, 405);
    }

    #[tokio::test]
    async fn bearer_token_is_required_except_for_health() {
        let s = server();
        let mut c = connect(&s).await;
        assert_eq!(call(&mut c, "GET", "/health", &[], b"").await.0, 200);
        let keys = wire::encode_keys([&b"k"[..]]);
        // Every response before the token is verified closes the
        // connection, so each attempt gets a fresh one.
        let mut c = connect(&s).await;
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
        let first = server();
        let cache = Arc::new(Cache::new(
            NonZeroUsize::new(1 << 20).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ));
        let err = HttpServer::bind(first.local_addr(), cache.clone(), opts())
            .err()
            .expect("port in use");
        assert!(matches!(err, Error::Bind { addr, .. } if addr == first.local_addr()));
        let err = HttpServer::bind("127.0.0.1:0".parse().unwrap(), cache, Options::new(""))
            .err()
            .expect("empty token");
        assert!(matches!(err, Error::EmptyToken));
    }

    #[tokio::test]
    async fn shutdown_closes_idle_keepalive_connections() {
        let s = server();
        let addr = s.local_addr();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let run = tokio::spawn(async move {
            s.run_until(async {
                let _ = rx.await;
            })
            .await;
        });
        let mut c = TcpStream::connect(addr).await.unwrap();
        assert_eq!(call(&mut c, "GET", "/health", &[], b"").await.0, 200);
        tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("an idle keep-alive connection is closed by graceful shutdown")
            .unwrap();
        let mut rest = Vec::new();
        c.read_to_end(&mut rest).await.unwrap();
        assert!(rest.is_empty());
    }

    /// No keep-alive without the token: `/health`, an unknown path, a
    /// wrong method and a refused request all end the connection at once,
    /// even with a body that hyper had already read and no idle timeout.
    #[tokio::test]
    async fn responses_before_auth_close_the_connection() {
        let s = server_with(opts().idle_timeout(None));
        let keys = wire::encode_keys([&b"k"[..]]);
        for (method, path, headers, body, code) in [
            ("GET", "/health", &[][..], &b""[..], 200),
            ("POST", "/nope", &[], &b""[..], 404),
            ("GET", "/get", &[], &b""[..], 405),
            ("POST", "/get", &[], &b""[..], 401),
            (
                "POST",
                "/ping",
                &[("Authorization", "Bearer wrong")],
                &keys[..],
                401,
            ),
        ] {
            let mut c = connect(&s).await;
            assert_eq!(call(&mut c, method, path, headers, body).await.0, code);
            let mut rest = Vec::new();
            tokio::time::timeout(Duration::from_secs(5), c.read_to_end(&mut rest))
                .await
                .expect("closed by the server")
                .unwrap();
            assert!(rest.is_empty(), "{method} {path}");
        }
        // With the token the connection stays open across requests.
        let mut c = connect(&s).await;
        let right = [("Authorization", "Bearer s3cret")];
        assert_eq!(call(&mut c, "POST", "/ping", &right, b"").await.0, 200);
        assert_eq!(call(&mut c, "POST", "/ping", &right, b"").await.0, 200);
    }

    #[tokio::test]
    async fn idle_keepalive_connection_is_closed_after_the_timeout() {
        let s = server_with(opts().idle_timeout(Some(Duration::from_millis(200))));
        let mut c = connect(&s).await;
        assert_eq!(call(&mut c, "POST", "/ping", &AUTH, b"").await.0, 200);
        let start = std::time::Instant::now();
        let mut rest = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), c.read_to_end(&mut rest))
            .await
            .expect("closed by the server")
            .unwrap();
        assert!(rest.is_empty());
        assert!(start.elapsed() >= Duration::from_millis(150), "not before");
    }

    #[tokio::test]
    async fn connection_limit_is_shared_with_the_tcp_front_end() {
        let opts = opts().max_connections(NonZeroUsize::new(1));
        let cache = Arc::new(Cache::new(
            NonZeroUsize::new(1 << 20).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ));
        let t = Arc::new(
            tcp::Server::bind("127.0.0.1:0".parse().unwrap(), cache.clone(), opts.clone()).unwrap(),
        );
        let h = Arc::new(HttpServer::bind("127.0.0.1:0".parse().unwrap(), cache, opts).unwrap());
        let (ts, hs) = (t.clone(), h.clone());
        tokio::spawn(async move { ts.run().await });
        tokio::spawn(async move { hs.run().await });
        // One TCP connection uses up the whole budget...
        let mut over_tcp = TcpStream::connect(t.local_addr()).await.unwrap();
        over_tcp
            .write_all(&wire::encode_header(Op::Auth as u8, 6))
            .await
            .unwrap();
        over_tcp.write_all(b"s3cret").await.unwrap();
        let mut hdr = [0u8; wire::HEADER_LEN];
        over_tcp.read_exact(&mut hdr).await.unwrap();
        assert_eq!(h.open_connections(), Some(1));
        // ...so HTTP is not answered until it closes.
        let mut c = TcpStream::connect(h.local_addr()).await.unwrap();
        c.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 64];
        assert!(
            tokio::time::timeout(Duration::from_millis(300), c.read(&mut buf))
                .await
                .is_err()
        );
        drop(over_tcp);
        let n = tokio::time::timeout(Duration::from_secs(5), c.read(&mut buf))
            .await
            .expect("served once the slot frees up")
            .unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).starts_with("HTTP/1.1 200"));
    }

    #[tokio::test]
    async fn broken_request_is_reported_not_fatal() {
        let s = server();
        let mut c = connect(&s).await;
        c.write_all(b"NOT HTTP\r\n\r\n").await.unwrap();
        let mut rest = Vec::new();
        c.read_to_end(&mut rest).await.unwrap();
        assert!(String::from_utf8_lossy(&rest).starts_with("HTTP/1.1 400"));
        let mut c = TcpStream::connect(s.local_addr()).await.unwrap();
        assert_eq!(call(&mut c, "GET", "/health", &[], b"").await.0, 200);
    }
}
