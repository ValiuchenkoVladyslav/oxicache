//! The `oxicache-cli` binary against an in-process server.

use std::num::NonZeroUsize;
use std::process::{Command, Output};
use std::sync::Arc;

use oxicache_server::{Cache, Options, Server};

async fn start(token: &[u8]) -> Arc<Server> {
    start_with(Options::new(token.to_vec())).await
}

async fn start_with(opts: Options) -> Arc<Server> {
    let cache = Arc::new(Cache::new(
        NonZeroUsize::new(64 << 20).unwrap(),
        NonZeroUsize::new(2).unwrap(),
    ));
    let server = Arc::new(Server::bind("127.0.0.1:0".parse().unwrap(), cache, opts).unwrap());
    let s = server.clone();
    tokio::spawn(async move { s.run().await });
    server
}

/// Run the CLI on a blocking thread so the server keeps being polled.
async fn cli(args: &[&str], env: &[(&str, &str)]) -> Output {
    let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    let env: Vec<(String, String)> = env
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_oxicache-cli"))
            .args(&args)
            .env_remove("OXICACHE_ADDR")
            .env_remove("OXICACHE_TOKEN")
            .envs(env)
            .output()
            .unwrap()
    })
    .await
    .unwrap()
}

fn text(o: &Output) -> (String, String) {
    (
        String::from_utf8_lossy(&o.stdout).into_owned(),
        String::from_utf8_lossy(&o.stderr).into_owned(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn get_set_del() {
    let server = start(b"t").await;
    let addr = server.local_addr().to_string();
    let env = [("OXICACHE_TOKEN", "t")];
    let out = cli(&["--addr", &addr, "set", "a", "1", "b", "2"], &env).await;
    assert!(out.status.success(), "{}", text(&out).1);
    assert_eq!(text(&out).0, "OK (2 entries)\n");
    let out = cli(&["--addr", &addr, "get", "a", "b", "zz"], &env).await;
    assert!(out.status.success());
    assert_eq!(text(&out).0, "a: 1\nb: 2\nzz: (nil)\n");
    let out = cli(&["--addr", &addr, "del", "a", "zz"], &env).await;
    assert!(out.status.success());
    assert_eq!(text(&out).0, "a: deleted\nzz: (nil)\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn errors_are_reported() {
    let server = start(b"t").await;
    let addr = server.local_addr().to_string();
    let out = cli(&["--addr", &addr, "--token", "t", "set", "a"], &[]).await;
    assert!(!out.status.success());
    assert!(text(&out).1.contains("key value pairs"));
    let out = cli(&["--addr", "127.0.0.1:1", "--token", "t", "get", "a"], &[]).await;
    assert!(!out.status.success());
    // No token at all is a usage error, before any connection.
    let out = cli(&["--addr", &addr, "get", "a"], &[]).await;
    assert!(!out.status.success());
    assert!(text(&out).1.contains("--token"), "{}", text(&out).1);
}

#[tokio::test(flavor = "multi_thread")]
async fn token_and_env_overrides() {
    let server = start(b"s3cret").await;
    let addr = server.local_addr().to_string();
    let out = cli(
        &["--addr", &addr, "--token", "s3cret", "set", "k", "v"],
        &[
            ("OXICACHE_ADDR", "127.0.0.1:1"),
            ("OXICACHE_TOKEN", "other"),
        ],
    )
    .await;
    let (stdout, stderr) = text(&out);
    assert!(out.status.success(), "{stderr}");
    assert_eq!(stdout, "OK (1 entries)\n");
    assert!(stderr.contains("--addr="), "{stderr}");
    assert!(stderr.contains("--token overrides"), "{stderr}");
    let out = cli(
        &["get", "k"],
        &[("OXICACHE_ADDR", &addr), ("OXICACHE_TOKEN", "s3cret")],
    )
    .await;
    assert_eq!(text(&out).0, "k: v\n");
    let out = cli(&["--addr", &addr, "--token", "wrong", "get", "k"], &[]).await;
    assert!(!out.status.success(), "unauthenticated");
    assert!(text(&out).1.contains("Unauthorized"), "{}", text(&out).1);
}

#[tokio::test(flavor = "multi_thread")]
async fn bench_runs() {
    let server = start(b"t").await;
    let addr = server.local_addr().to_string();
    let out = cli(
        &[
            "--addr",
            &addr,
            "--token",
            "t",
            "bench",
            "--conns",
            "2",
            "--pipeline",
            "2",
            "--batch",
            "4",
            "--keys",
            "50",
            "--value-size",
            "16",
            "--write-ratio",
            "0.5",
            "--seconds",
            "1",
        ],
        &[],
    )
    .await;
    let (stdout, stderr) = text(&out);
    assert!(out.status.success(), "{stderr}");
    assert!(stdout.contains("req/s"), "{stdout}");
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_ca_connects_over_tls() {
    const SERVER_PEM: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../testdata/tls/server.pem");
    const CA_PEM: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../testdata/tls/ca.pem");
    let tls = oxicache_server::tls::server_config(std::path::Path::new(SERVER_PEM)).unwrap();
    let server = start_with(Options::new(b"t".to_vec()).tls(Some(tls))).await;
    let port = server.local_addr().port();
    let env = [("OXICACHE_TOKEN", "t")];
    // `localhost` resolves to ::1 too on many hosts; the CLI tries each address.
    let addr = format!("localhost:{port}");
    let out = cli(
        &["--addr", &addr, "--tls-ca", CA_PEM, "set", "a", "1"],
        &env,
    )
    .await;
    assert!(out.status.success(), "{}", text(&out).1);
    let addr = format!("127.0.0.1:{port}");
    let out = cli(&["--addr", &addr, "--tls-ca", CA_PEM, "get", "a"], &env).await;
    assert!(out.status.success(), "{}", text(&out).1);
    assert_eq!(text(&out).0, "a: 1\n");
    // Without --tls-ca the CLI speaks plain TCP and the TLS server hangs up.
    let out = cli(&["--addr", &addr, "get", "a"], &env).await;
    assert!(!out.status.success());
    let out = cli(
        &["--addr", &addr, "--tls-ca", "/nonexistent.pem", "get", "a"],
        &env,
    )
    .await;
    assert!(!out.status.success());
    assert!(
        text(&out).1.contains("/nonexistent.pem"),
        "{}",
        text(&out).1
    );
}
