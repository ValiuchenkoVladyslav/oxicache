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

fn get(addr: SocketAddr, key: &[u8]) -> (TcpStream, Status, Vec<u8>) {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
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
        assert!(log.contains("auth=false"), "{log}");
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
    let (_conn, status, _) = get(r.addr, b"k");
    assert_eq!(status, Status::Unauthorized);
    let (ok, log) = r.interrupt();
    assert!(ok, "{log}");
    assert!(log.contains("auth=true"), "{log}");
    for flag in ["--addr=", "--capacity=", "--shards=", "--token overrides"] {
        assert!(log.contains(flag), "{flag}: {log}");
    }
    // An empty token disables authentication.
    let r = start(&["--capacity", "1M", "--token", ""], &[]);
    let (_conn, status, _) = get(r.addr, b"k");
    assert_eq!(status, Status::Ok);
    let (ok, log) = r.interrupt();
    assert!(ok, "{log}");
    assert!(log.contains("auth=false"), "{log}");
}
