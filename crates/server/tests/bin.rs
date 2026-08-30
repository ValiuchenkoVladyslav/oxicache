//! The `oxicache-server` binary: environment configuration, startup, serving
//! and clean shutdown on SIGINT or SIGTERM.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use oxicache_wire::{self as wire, Op, Status};

struct Running {
    child: Child,
    /// The tracing log (stdout); errors go to stderr.
    stdout: BufReader<std::process::ChildStdout>,
    stderr: std::process::ChildStderr,
    addr: SocketAddr,
    log: String,
}

fn cmd(args: &[&str], env: &[(&str, &str)]) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_oxicache-server"));
    c.args(args)
        .env_remove("OXICACHE_TCP_ADDR")
        .env_remove("OXICACHE_HTTP_ADDR")
        .env_remove("OXICACHE_CAPACITY")
        .env_remove("OXICACHE_TOKEN")
        .env_remove("OXICACHE_IDLE_TIMEOUT")
        .env_remove("OXICACHE_MAX_CONNS")
        .env_remove("OXICACHE_TLS_CERT")
        .env_remove("RUST_LOG")
        .envs(env.iter().copied())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    c
}

/// Drop ANSI colour sequences so field names can be matched textually.
fn plain(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            for c in chars.by_ref() {
                if c == 'm' {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Start the server on a random port with `env` and wait until it logs the
/// address; the token is `t` unless `env` sets one. (`Running` kills and
/// waits for the child when dropped, so a failure here leaves no zombie.)
#[allow(clippy::zombie_processes)]
fn start(env: &[(&str, &str)]) -> Running {
    let mut full = vec![("OXICACHE_TCP_ADDR", "127.0.0.1:0")];
    if !env.iter().any(|(k, _)| *k == "OXICACHE_TOKEN") {
        full.push(("OXICACHE_TOKEN", "t"));
    }
    full.extend_from_slice(env);
    let mut child = cmd(&[], &full).spawn().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let stderr = child.stderr.take().unwrap();
    let mut log = String::new();
    loop {
        let mut raw = String::new();
        assert!(stdout.read_line(&mut raw).unwrap() > 0, "exited: {log}");
        let line = plain(&raw);
        log.push_str(&line);
        if let Some(rest) = line.split("listening (tcp) addr=").nth(1) {
            let addr = rest.split_whitespace().next().unwrap().parse().unwrap();
            return Running {
                child,
                stdout,
                stderr,
                addr,
                log,
            };
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        // A test that fails midway must not leave a server behind.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Running {
    /// The HTTP listener's address, read from the log once it appears.
    fn http_addr(&mut self) -> SocketAddr {
        loop {
            if let Some(rest) = self.log.split("listening (http) addr=").nth(1) {
                return rest.split_whitespace().next().unwrap().parse().unwrap();
            }
            let mut raw = String::new();
            assert!(
                self.stdout.read_line(&mut raw).unwrap() > 0,
                "exited: {}",
                self.log
            );
            self.log.push_str(&plain(&raw));
        }
    }

    /// SIGINT, then wait; returns whether it exited cleanly and the whole
    /// output, stdout and stderr together.
    fn interrupt(self) -> (bool, String) {
        self.signal("INT")
    }

    fn signal(mut self, sig: &str) -> (bool, String) {
        let ok = Command::new("kill")
            .args([&format!("-{sig}"), &self.child.id().to_string()])
            .status()
            .unwrap()
            .success();
        assert!(ok);
        let mut rest = String::new();
        self.stdout.read_to_string(&mut rest).unwrap();
        self.log.push_str(&plain(&rest));
        rest.clear();
        self.stderr.read_to_string(&mut rest).unwrap();
        self.log.push_str(&rest);
        let status = self.child.wait().unwrap();
        (status.success(), std::mem::take(&mut self.log))
    }
}

/// AUTH with `token`, then GET `key`, on a fresh connection.
fn get_as(addr: SocketAddr, token: &[u8], key: &[u8]) -> (TcpStream, Status, Vec<u8>) {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s.write_all(&wire::encode_header(Op::Auth as u8, token.len()))
        .unwrap();
    s.write_all(token).unwrap();
    let mut hdr = [0u8; wire::HEADER_LEN];
    s.read_exact(&mut hdr).unwrap();
    let (status, len) = wire::decode_header(&hdr);
    if status != Status::Ok as u8 {
        let mut out = vec![0u8; len];
        s.read_exact(&mut out).unwrap();
        return (s, Status::from_u8(status).unwrap(), out);
    }
    let body = wire::encode_keys([key]);
    s.write_all(&wire::encode_header(Op::Get as u8, body.len()))
        .unwrap();
    s.write_all(&body).unwrap();
    let mut hdr = [0u8; wire::HEADER_LEN];
    s.read_exact(&mut hdr).unwrap();
    let (status, len) = wire::decode_header(&hdr);
    let mut out = vec![0u8; len];
    s.read_exact(&mut out).unwrap();
    (s, Status::from_u8(status).unwrap(), out)
}

fn get(addr: SocketAddr, key: &[u8]) -> (TcpStream, Status, Vec<u8>) {
    get_as(addr, b"t", key)
}

#[test]
fn serves_and_shuts_down_cleanly() {
    let r = start(&[("OXICACHE_CAPACITY", "1M")]);
    let (conn, status, body) = get(r.addr, b"missing");
    assert_eq!(status, Status::Ok);
    assert_eq!(wire::decode_values(body.into()).unwrap(), vec![None]);
    // Interrupt with the connection still open: it is drained, not cut.
    let (ok, log) = r.interrupt();
    drop(conn);
    assert!(ok, "{log}");
    assert!(log.contains("cache ready"), "{log}");
    assert!(log.contains("shutting down"), "{log}");
    assert!(log.contains("draining connections"), "{log}");
}

#[test]
fn sigterm_drains_and_exits_cleanly() {
    let r = start(&[("OXICACHE_CAPACITY", "1M")]);
    let (conn, status, _) = get(r.addr, b"missing");
    assert_eq!(status, Status::Ok);
    let (ok, log) = r.signal("TERM");
    drop(conn);
    assert!(ok, "{log}");
    assert!(log.contains("SIGTERM"), "{log}");
    assert!(log.contains("draining connections"), "{log}");
}

#[test]
fn idle_connections_are_closed() {
    // Whole seconds; the shortest timeout is 1 s.
    let r = start(&[("OXICACHE_CAPACITY", "1M"), ("OXICACHE_IDLE_TIMEOUT", "1")]);
    let (mut conn, status, _) = get(r.addr, b"k");
    assert_eq!(status, Status::Ok);
    let mut rest = Vec::new();
    conn.read_to_end(&mut rest).unwrap();
    assert!(rest.is_empty(), "closed without a frame");
    let (ok, log) = r.interrupt();
    assert!(ok, "{log}");
    assert!(log.contains("idle_timeout=1"), "{log}");
}

#[test]
fn connection_limit_holds_the_next_peer_in_the_backlog() {
    let r = start(&[("OXICACHE_CAPACITY", "1M"), ("OXICACHE_MAX_CONNS", "1")]);
    let (first, status, _) = get(r.addr, b"k");
    assert_eq!(status, Status::Ok);
    let mut second = TcpStream::connect(r.addr).unwrap();
    second
        .set_read_timeout(Some(Duration::from_millis(300)))
        .unwrap();
    second
        .write_all(&wire::encode_header(Op::Auth as u8, 1))
        .unwrap();
    second.write_all(b"t").unwrap();
    let mut hdr = [0u8; wire::HEADER_LEN];
    assert!(
        second.read_exact(&mut hdr).is_err(),
        "served over the limit"
    );
    drop(first);
    second
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    second.read_exact(&mut hdr).unwrap();
    assert_eq!(wire::decode_header(&hdr).0, Status::Ok as u8);
    let (ok, log) = r.interrupt();
    assert!(ok, "{log}");
    assert!(log.contains("max_conns=1"), "{log}");
    assert!(log.contains("connection limit reached"), "{log}");
}

#[test]
fn capacity_suffixes_and_defaults() {
    for cap in ["2048", "2k", "2M", "1g"] {
        let r = start(&[("OXICACHE_CAPACITY", cap)]);
        let (ok, log) = r.interrupt();
        assert!(ok, "{log}");
    }
}

/// Run to exit with `env` (plus `args`) and return the plain stderr; the
/// exit must be a failure.
fn fails(args: &[&str], env: &[(&str, &str)]) -> String {
    let out = cmd(args, env).output().unwrap();
    assert!(!out.status.success(), "{args:?} {env:?}");
    plain(&String::from_utf8_lossy(&out.stderr))
}

#[test]
fn token_is_required_and_non_empty() {
    let err = fails(&[], &[("OXICACHE_CAPACITY", "1M")]);
    assert!(err.contains("OXICACHE_TOKEN is required"), "{err}");
    let err = fails(&[], &[("OXICACHE_CAPACITY", "1M"), ("OXICACHE_TOKEN", "")]);
    assert!(err.contains("OXICACHE_TOKEN"), "{err}");
    assert!(err.contains("empty"), "{err}");
}

#[test]
fn rejects_bad_values_naming_the_variable() {
    let token = ("OXICACHE_TOKEN", "t");
    for cap in ["1X", "abc", "0", "99999999999999999999", "999999999999G"] {
        let err = fails(&[], &[token, ("OXICACHE_CAPACITY", cap)]);
        assert!(err.contains("OXICACHE_CAPACITY"), "{cap}: {err}");
    }
    for (var, bad) in [
        ("OXICACHE_TCP_ADDR", "nowhere"),
        ("OXICACHE_HTTP_ADDR", "nowhere"),
        ("OXICACHE_IDLE_TIMEOUT", "0"),
        ("OXICACHE_IDLE_TIMEOUT", "1.5"),
        ("OXICACHE_MAX_CONNS", "0"),
        ("OXICACHE_MAX_CONNS", "-1"),
    ] {
        let err = fails(&[], &[token, (var, bad)]);
        assert!(err.contains(var), "{var}={bad}: {err}");
    }
}

/// One HTTP/1.1 request; returns the status code and body.
fn http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    auth: Option<&str>,
    body: &[u8],
) -> (u16, Vec<u8>) {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n");
    if let Some(a) = auth {
        req.push_str(&format!("Authorization: Bearer {a}\r\n"));
    }
    req.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    s.write_all(req.as_bytes()).unwrap();
    s.write_all(body).unwrap();
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).unwrap();
    let split = raw.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = String::from_utf8_lossy(&raw[..split]);
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, raw[split + 4..].to_vec())
}

#[test]
fn http_front_end_serves_alongside_tcp() {
    let mut r = start(&[
        ("OXICACHE_CAPACITY", "1M"),
        ("OXICACHE_HTTP_ADDR", "127.0.0.1:0"),
        ("OXICACHE_TOKEN", "t0k"),
    ]);
    let h = r.http_addr();
    assert_ne!(h, r.addr);
    assert_eq!(http(h, "GET", "/health", None, b""), (200, vec![]));
    let entries = wire::encode_entries([(&b"k"[..], &b"v"[..])]);
    assert_eq!(http(h, "POST", "/set", None, &entries).0, 401);
    assert_eq!(
        http(h, "POST", "/set", Some("t0k"), &entries),
        (200, vec![])
    );
    // The same cache is behind both front ends.
    let mut s = TcpStream::connect(r.addr).unwrap();
    let auth = b"t0k";
    s.write_all(&wire::encode_header(Op::Auth as u8, auth.len()))
        .unwrap();
    s.write_all(auth).unwrap();
    let mut hdr = [0u8; wire::HEADER_LEN];
    s.read_exact(&mut hdr).unwrap();
    assert_eq!(wire::decode_header(&hdr).0, Status::Ok as u8);
    let keys = wire::encode_keys([&b"k"[..]]);
    s.write_all(&wire::encode_header(Op::Get as u8, keys.len()))
        .unwrap();
    s.write_all(&keys).unwrap();
    s.read_exact(&mut hdr).unwrap();
    let (st, len) = wire::decode_header(&hdr);
    let mut body = vec![0u8; len];
    s.read_exact(&mut body).unwrap();
    assert_eq!(st, Status::Ok as u8);
    assert_eq!(
        wire::decode_values(body.into()).unwrap(),
        vec![Some(b"v".as_slice().into())]
    );
    let (ok, log) = r.interrupt();
    assert!(ok, "{log}");
    assert!(log.contains("http="), "{log}");
    assert!(log.contains("listening (http)"), "{log}");
}

#[test]
fn http_bind_failure_is_fatal() {
    let r = start(&[("OXICACHE_CAPACITY", "1M")]);
    let err = fails(
        &[],
        &[
            ("OXICACHE_TCP_ADDR", "127.0.0.1:0"),
            ("OXICACHE_TOKEN", "t"),
            ("OXICACHE_CAPACITY", "1M"),
            ("OXICACHE_HTTP_ADDR", &r.addr.to_string()),
        ],
    );
    assert!(err.contains("Bind") && err.contains("AddrInUse"), "{err}");
}

const SERVER_PEM: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../testdata/tls/server.pem");
const CA_PEM: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../testdata/tls/ca.pem");

/// A blocking TLS stream to `addr`, trusting the fixture CA.
fn tls_connect(addr: SocketAddr) -> rustls::StreamOwned<rustls::ClientConnection, TcpStream> {
    use rustls_pki_types::pem::PemObject;
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(rustls_pki_types::CertificateDer::from_pem_file(CA_PEM).unwrap())
        .unwrap();
    let config = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let conn = rustls::ClientConnection::new(
        std::sync::Arc::new(config),
        rustls_pki_types::ServerName::try_from("localhost").unwrap(),
    )
    .unwrap();
    let s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    rustls::StreamOwned::new(conn, s)
}

#[test]
fn tls_cert_enables_tls_on_both_listeners() {
    let mut r = start(&[
        ("OXICACHE_CAPACITY", "1M"),
        ("OXICACHE_HTTP_ADDR", "127.0.0.1:0"),
        ("OXICACHE_TLS_CERT", SERVER_PEM),
    ]);
    let h = r.http_addr();
    // TCP: AUTH then PING through the handshake.
    let mut s = tls_connect(r.addr);
    s.write_all(&wire::encode_header(Op::Auth as u8, 1))
        .unwrap();
    s.write_all(b"t").unwrap();
    let mut hdr = [0u8; wire::HEADER_LEN];
    s.read_exact(&mut hdr).unwrap();
    assert_eq!(wire::decode_header(&hdr), (Status::Ok as u8, 0));
    s.write_all(&wire::encode_header(Op::Ping as u8, 0))
        .unwrap();
    s.read_exact(&mut hdr).unwrap();
    assert_eq!(wire::decode_header(&hdr), (Status::Ok as u8, 0));
    // HTTP: /health over TLS.
    let mut s = tls_connect(h);
    s.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut raw = Vec::new();
    let _ = s.read_to_end(&mut raw);
    assert!(
        raw.starts_with(b"HTTP/1.1 200"),
        "{}",
        String::from_utf8_lossy(&raw)
    );
    // Plain clients get nothing.
    let mut plain = TcpStream::connect(r.addr).unwrap();
    plain
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    plain
        .write_all(&wire::encode_header(Op::Auth as u8, 1))
        .unwrap();
    plain.write_all(b"t").unwrap();
    let mut rest = Vec::new();
    plain.read_to_end(&mut rest).unwrap();
    // At most a TLS alert record (content type 21), never a frame.
    assert!(rest.is_empty() || rest[0] == 0x15, "{rest:?}");
    let (ok, log) = r.interrupt();
    assert!(ok, "{log}");
    assert!(
        log.contains("listening (tcp)") && log.contains("tls=true"),
        "{log}"
    );
    assert!(log.contains("server.pem"), "{log}");
}

#[test]
fn tls_cert_must_be_a_readable_pem() {
    for bad in ["", "/nonexistent/server.pem", CA_PEM] {
        let err = fails(
            &[],
            &[
                ("OXICACHE_TOKEN", "t"),
                ("OXICACHE_CAPACITY", "1M"),
                ("OXICACHE_TLS_CERT", bad),
            ],
        );
        assert!(err.contains("OXICACHE_TLS_CERT"), "{bad:?}: {err}");
    }
}
