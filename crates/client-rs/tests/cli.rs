//! The `oxicache-cli` binary against an in-process server.

use std::process::{Command, Output};
use std::sync::Arc;

use oxicache_server::{Cache, Options, Server};

async fn start(token: Option<&[u8]>) -> Arc<Server> {
    let cache = Arc::new(Cache::new(64 << 20, 2));
    let opts = Options {
        token: token.map(<[u8]>::to_vec),
    };
    let server = Arc::new(Server::bind_with("127.0.0.1:0".parse().unwrap(), cache, opts).unwrap());
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
    let server = start(Some(b"t")).await;
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
    let server = start(None).await;
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
    let server = start(Some(b"s3cret")).await;
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
    let server = start(None).await;
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
