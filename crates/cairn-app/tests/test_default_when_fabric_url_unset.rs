//! PR-A (backend-config-url): when `CAIRN_FABRIC_URL` is unset,
//! cairn-app must fall back to the default `valkey://localhost:6379`
//! endpoint. This is an integration-level sanity check on top of the
//! `default_config_from_env_when_url_unset` unit test in
//! `cairn-fabric/src/config.rs`.
//!
//! # Why log inspection (not /health/ready)
//!
//! The test asserts the cairn-app subprocess emits the
//! `host=localhost port=6379` line from `FabricRuntime::start`'s
//! `tracing::info!` — that proves the URL parser + env-var handling
//! fed the correct default into the runtime config. The subprocess
//! is killed the moment the line is observed, so the test is
//! deterministic whether or not a reachable Valkey on
//! `localhost:6379` exists on the host (CI usually doesn't have one;
//! some dev machines do — see the Copilot review on PR #356).

use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::time::timeout;

#[tokio::test]
async fn default_to_localhost_6379_when_cairn_fabric_url_unset() {
    let bin = env!("CARGO_BIN_EXE_cairn-app");

    let mut cmd = Command::new(bin);
    cmd.arg("--mode")
        .arg("team")
        .arg("--port")
        .arg("0")
        .arg("--addr")
        .arg("127.0.0.1")
        .arg("--db")
        .arg("memory")
        // Explicitly remove any inherited CAIRN_FABRIC_URL so the
        // default-path arm in `FabricConfig::from_env` fires.
        .env_remove("CAIRN_FABRIC_URL")
        .env_remove("CAIRN_FABRIC_HOST")
        .env_remove("CAIRN_FABRIC_PORT")
        .env_remove("CAIRN_FABRIC_TLS")
        .env_remove("CAIRN_FABRIC_CLUSTER")
        .env("CAIRN_ADMIN_TOKEN", "default-test-admin-token")
        .env(
            "CAIRN_FABRIC_WAITPOINT_HMAC_SECRET",
            "00000000000000000000000000000000000000000000000000000000000000aa",
        )
        .env("CAIRN_FABRIC_WAITPOINT_HMAC_KID", "cairn-test-default")
        .env("RUST_LOG", "info")
        .env_remove("CAIRN_LOG_DIR")
        // `tracing_subscriber::fmt()` writes to stdout (not stderr) —
        // the `connecting to valkey` info event lands there. Pipe it
        // so we can scan for the default-endpoint fields. Panic text
        // goes to stderr; drop it.
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);

    let mut child = cmd
        .spawn()
        .expect("failed to spawn cairn-app — did cargo build it?");

    let stdout = child.stdout.take().expect("piped stdout present");

    // Read stdout line-by-line and short-circuit the moment we see
    // the `connecting to valkey host=localhost port=6379` tracing
    // event. Killing the subprocess immediately afterwards keeps the
    // test deterministic regardless of whether a reachable Valkey on
    // localhost:6379 lets cairn-app boot all the way to steady state
    // (in which case stdout would never reach EOF and a drain-until-
    // EOF would hang until the test timeout).
    let (matched, transcript) = match timeout(
        Duration::from_secs(30),
        scan_stdout_for_default_endpoint(stdout),
    )
    .await
    {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            panic!("stdout read errored: {e}");
        }
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            panic!("timed out waiting for `connecting to valkey` line");
        }
    };
    let _ = child.kill().await;
    let _ = child.wait().await;

    assert!(
        matched,
        "subprocess did not surface `host=localhost port=6379` on the \
         `connecting to valkey` line — default fallback regressed.\n\
         ---- subprocess stdout ----\n{transcript}\n---- end ----"
    );
}

/// Scan subprocess stdout line-by-line for the boot-time
/// `connecting to valkey host=localhost port=6379` event. Returns
/// `(true, transcript)` on first match, `(false, transcript)` on EOF
/// without a match. The transcript is surfaced in failure messages so
/// regressions show the actual subprocess output, not a blind false.
async fn scan_stdout_for_default_endpoint(
    stdout: tokio::process::ChildStdout,
) -> std::io::Result<(bool, String)> {
    let mut lines = BufReader::new(stdout).lines();
    let mut transcript = String::new();
    while let Some(line) = lines.next_line().await? {
        transcript.push_str(&line);
        transcript.push('\n');
        let stripped = strip_ansi(&line);
        if stripped.contains("connecting to valkey")
            && stripped.contains("host=localhost")
            && stripped.contains("port=6379")
        {
            return Ok((true, transcript));
        }
    }
    Ok((false, transcript))
}

/// Small CSI-escape stripper. Only covers the `ESC [ ... m` SGR
/// sequences that tracing's fmt layer emits; full `ansi_term` or
/// `strip-ansi-escapes` would work but adds a test-only dep the
/// integration test suite can skip.
fn strip_ansi(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' && chars.peek() == Some(&'[') {
            chars.next(); // consume '['
                          // Consume until the SGR terminator 'm' (or any ASCII
                          // letter for robustness); drop everything inside.
            for next in chars.by_ref() {
                if next.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}
