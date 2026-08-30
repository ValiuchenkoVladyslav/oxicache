//! The `oxicache-server` binary: argument parsing, startup, serving and
//! clean shutdown on SIGINT.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use oxicache_wire::{self as wire, Op, Status};

struct Running {
    child: Child,
    /// The tracing log (stdout); warnings go to stderr.
    stdout: BufReader<std::process::ChildStdout>,
    stderr: std::process::ChildStderr,
    addr: SocketAddr,
    log: String,
}

fn cmd(args: &[&str], env: &[(&str, &str)]) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_oxicache-server"));
    c.args(args)
        .env_remove("OXICACHE_ADDR")
        .env_remove("OXICACHE_HTTP_ADDR")
        .env_remove("OXICACHE_CAPACITY")
        .env_remove("OXICACHE_SHARDS")
        .env_remove("OXICACHE_TOKEN")
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

/// Start the server on a random port and wait until it logs the address.
/// (`Running` kills and waits for the child when dropped, so a failure
/// here leaves no zombie.)
#[allow(clippy::zombie_processes)]
fn start(args: &[&str], env: &[(&str, &str)]) -> Running {
    let mut full = vec!["--addr", "127.0.0.1:0"];
    if !args.contains(&"--token") {
        full.extend_from_slice(&["--token", "t"]);
    }
    full.extend_from_slice(args);
    let mut child = cmd(&full, env).spawn().unwrap();
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
    /// output, log and warnings together.
    fn interrupt(mut self) -> (bool, String) {
        let ok = Command::new("kill")
            .args(["-INT", &self.child.id().to_string()])
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
    let r = start(&["--capacity", "1M", "--shards", "1"], &[]);
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
fn capacity_suffixes_and_defaults() {
    for cap in ["2048", "2k", "2M", "1g"] {
        let r = start(&["--capacity", cap], &[]);
        let (ok, log) = r.interrupt();
        assert!(ok, "{log}");
    }
}

#[test]
fn token_is_required_and_non_empty() {
    for args in [
        &["--capacity", "1M"][..],
        &["--capacity", "1M", "--token", ""],
    ] {
        let out = cmd(args, &[]).output().unwrap();
        assert!(!out.status.success());
        let err = plain(&String::from_utf8_lossy(&out.stderr));
        assert!(err.contains("token"), "{err}");
    }
}

#[test]
fn rejects_bad_capacity() {
    for cap in ["1X", "abc", "99999999999999999999", "999999999999G"] {
        let out = cmd(&["--capacity", cap], &[]).output().unwrap();
        assert!(!out.status.success(), "{cap}");
        let err = plain(&String::from_utf8_lossy(&out.stderr));
        assert!(err.contains("capacity"), "{err}");
    }
}

#[test]
fn token_and_env_overrides() {
    let r = start(
        &["--capacity", "1M", "--shards", "1", "--token", "s3cret"],
        &[
            ("OXICACHE_ADDR", "127.0.0.1:1"),
            ("OXICACHE_CAPACITY", "2M"),
            ("OXICACHE_SHARDS", "2"),
            ("OXICACHE_TOKEN", "other"),
        ],
    );
    let (_conn, status, _) = get_as(r.addr, b"other", b"k");
    assert_eq!(
        status,
        Status::Unauthorized,
        "the flag wins over the variable"
    );
    let (_conn, status, _) = get_as(r.addr, b"s3cret", b"k");
    assert_eq!(status, Status::Ok);
    let (ok, log) = r.interrupt();
    assert!(ok, "{log}");
    for flag in ["--addr=", "--capacity=", "--shards=", "--token overrides"] {
        assert!(log.contains(flag), "{flag}: {log}");
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
    let mut r = start(
        &[
            "--capacity",
            "1M",
            "--shards",
            "1",
            "--http-addr",
            "127.0.0.1:0",
            "--token",
            "t0k",
        ],
        &[("OXICACHE_HTTP_ADDR", "127.0.0.1:1")],
    );
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
    assert!(log.contains("--http-addr="), "{log}");
    assert!(log.contains("listening (http)"), "{log}");
}

#[test]
fn http_bind_failure_is_fatal() {
    let r = start(&["--capacity", "1M"], &[]);
    let out = cmd(
        &[
            "--addr",
            "127.0.0.1:0",
            "--token",
            "t",
            "--capacity",
            "1M",
            "--http-addr",
            &r.addr.to_string(),
        ],
        &[],
    )
    .output()
    .unwrap();
    assert!(!out.status.success());
    let err = plain(&String::from_utf8_lossy(&out.stderr));
    assert!(err.contains("Bind") && err.contains("AddrInUse"), "{err}");
}
