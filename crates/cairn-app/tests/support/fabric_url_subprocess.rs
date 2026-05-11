//! Shared subprocess + stdio-scanning helpers for the
//! `test_cairn_fabric_url_*` test family.
//!
//! Each of those tests spawns a real `cairn-app` binary with a
//! specific `CAIRN_FABRIC_URL` value and asserts something about
//! which log line (or error) surfaces. The plumbing is identical
//! across tests; the differences are the env-var value, the
//! stream being scanned (stdout vs stderr), and the substring
//! predicate.
//!
//! Extracted from three copies (malformed, rediss-TLS, unknown-scheme)
//! plus the original `test_default_when_fabric_url_unset` to address
//! Copilot + Gemini review feedback on PR #553: deduplication keeps
//! behaviour aligned as the subprocess-scanning policy evolves
//! (broader CSI handling, env-var scrub rules, exit-code assertions).
//!
//! The three tests stay in separate files so CI runs them in
//! parallel binaries — consolidating into one file would serialise
//! four subprocess boots and slow the suite noticeably.

use std::collections::HashMap;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::time::timeout;

/// Build a `Command` pre-configured with the baseline cairn-app CLI
/// flags and env vars that every fabric-URL subprocess test needs:
/// team-mode, in-memory DB, missing-sandbox-primitives allowed,
/// admin-token, credential-key, HMAC secret, no log directory. The
/// caller then overlays its scenario-specific env var — typically
/// `CAIRN_FABRIC_URL` — plus stdout/stderr piping policy.
///
/// `admin_token_suffix` and `hmac_kid_suffix` differentiate the
/// admin-token / HMAC kid across tests so nothing shared leaks
/// between subprocess runs if they were ever to be parallelised
/// against the same Valkey.
pub fn cairn_app_command(admin_token_suffix: &str, hmac_kid_suffix: &str) -> Command {
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
        // F65 PR-5: CI runners block unprivileged userns; skip the
        // probe gate so this test can exercise FabricConfig defaults
        // without the boot probe refusing to start.
        .arg("--allow-missing-sandbox-primitives")
        .env(
            "CAIRN_ADMIN_TOKEN",
            format!("{admin_token_suffix}-admin-token"),
        )
        .env(
            "CAIRN_FABRIC_WAITPOINT_HMAC_SECRET",
            "00000000000000000000000000000000000000000000000000000000000000aa",
        )
        .env(
            "CAIRN_FABRIC_WAITPOINT_HMAC_KID",
            format!("cairn-test-{hmac_kid_suffix}"),
        )
        // META #461: team mode refuses to start without a master key.
        .env(
            "CAIRN_CREDENTIAL_KEY",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .env("RUST_LOG", "info")
        .env_remove("CAIRN_LOG_DIR")
        .kill_on_drop(true);
    cmd
}

/// Scrub the fabric-related env vars we care about off the test
/// process's environment before spawning. Per Gemini's env-pollution
/// reference on PR #553 — some CI runners / dev shells inherit stale
/// `CAIRN_FABRIC_HOST/PORT/TLS/CLUSTER` exports from pre-PR-A local
/// workflows. We clear them unconditionally so the test's intent
/// (e.g. "URL unset → default") isn't silently overridden.
///
/// Returns the previous values so the caller can restore on exit.
/// For tokio tests that spawn a subprocess this is usually fine to
/// skip (env_remove on the Command doesn't affect the test process),
/// but we call it on the parent process too so any library code
/// that reads env before the Command spawn sees a clean slate.
pub fn scrub_fabric_env() -> HashMap<String, Option<String>> {
    let keys = [
        "CAIRN_FABRIC_URL",
        "CAIRN_FABRIC_HOST",
        "CAIRN_FABRIC_PORT",
        "CAIRN_FABRIC_TLS",
        "CAIRN_FABRIC_CLUSTER",
    ];
    let mut prev = HashMap::new();
    for k in keys {
        prev.insert(k.to_owned(), std::env::var(k).ok());
        std::env::remove_var(k);
    }
    prev
}

/// Restore env vars previously captured by `scrub_fabric_env`.
/// Pair with a `_guard` pattern or explicit call at end of test to
/// avoid leaving the test process with scrubbed values after the
/// subprocess exits.
pub fn restore_fabric_env(prev: HashMap<String, Option<String>>) {
    for (k, v) in prev {
        match v {
            Some(val) => std::env::set_var(&k, val),
            None => std::env::remove_var(&k),
        }
    }
}

/// Scan `stdout` line-by-line, passing each (ANSI-stripped) line to
/// `predicate`. Returns `(true, transcript)` on first match, `(false,
/// transcript)` on EOF without a match. The transcript is surfaced
/// in failure messages so regressions show the actual subprocess
/// output, not a blind false.
pub async fn scan_stdout<F>(
    stdout: tokio::process::ChildStdout,
    predicate: F,
) -> std::io::Result<(bool, String)>
where
    F: Fn(&str) -> bool,
{
    let mut lines = BufReader::new(stdout).lines();
    let mut transcript = String::new();
    while let Some(line) = lines.next_line().await? {
        transcript.push_str(&line);
        transcript.push('\n');
        if predicate(&strip_ansi(&line)) {
            return Ok((true, transcript));
        }
    }
    Ok((false, transcript))
}

/// Stderr counterpart of [`scan_stdout`]. Matches the raw line
/// (not ANSI-stripped) because stderr log lines rarely carry the
/// tracing fmt layer's SGR codes (`eprintln!` writes plain).
pub async fn scan_stderr<F>(
    stderr: tokio::process::ChildStderr,
    predicate: F,
) -> std::io::Result<(bool, String)>
where
    F: Fn(&str) -> bool,
{
    let mut lines = BufReader::new(stderr).lines();
    let mut transcript = String::new();
    while let Some(line) = lines.next_line().await? {
        transcript.push_str(&line);
        transcript.push('\n');
        if predicate(&line) {
            return Ok((true, transcript));
        }
    }
    Ok((false, transcript))
}

/// Shared timeout budget. 30s matches what every fabric-URL test was
/// using before the extraction; generous enough for cold-start on
/// slow runners.
pub const SCAN_TIMEOUT: Duration = Duration::from_secs(30);

/// Child-exit grace period. The `eprintln!` + `std::process::exit(1)`
/// pattern in `main.rs` resolves `wait` promptly once the log line
/// fires; 5s is overkill but avoids false-positive hangs on
/// overloaded CI.
pub const CHILD_EXIT_GRACE: Duration = Duration::from_secs(5);

/// Run `fut` with [`SCAN_TIMEOUT`] and on timeout kill-and-wait the
/// child. Small convenience to keep each test's top-level code
/// readable.
pub async fn scan_with_timeout<F, T>(child: &mut tokio::process::Child, fut: F, what: &str) -> T
where
    F: std::future::Future<Output = std::io::Result<T>>,
{
    match timeout(SCAN_TIMEOUT, fut).await {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            panic!("{what}: read errored: {e}");
        }
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            panic!("{what}: timed out after {SCAN_TIMEOUT:?}");
        }
    }
}

/// Wait for `child` to exit with [`CHILD_EXIT_GRACE`], panicking (and
/// killing) if it doesn't. Includes `transcript` in the panic message
/// so a hang surfaces the observed subprocess output.
pub async fn wait_exit_within_grace(
    child: &mut tokio::process::Child,
    transcript: &str,
) -> std::process::ExitStatus {
    match timeout(CHILD_EXIT_GRACE, child.wait()).await {
        Ok(Ok(s)) => s,
        other => {
            let _ = child.kill().await;
            panic!(
                "subprocess did not exit after emitting expected line: {other:?}\n\
                 ---- transcript ----\n{transcript}\n---- end ----"
            );
        }
    }
}

/// Strip `ESC [ … letter` CSI SGR sequences from `input`. Tracing's
/// fmt layer emits them when stdout is piped to a terminal-capable
/// sink; tests match post-stripping so the `connecting to valkey
/// host=…` needle is found regardless of colour.
///
/// Fast-path: short-circuit with `input.to_string()` if no escape
/// byte is present, so the common case (no colour) avoids the
/// char-by-char scan. Addresses Gemini suggestion on PR #553.
pub fn strip_ansi(input: &str) -> String {
    if !input.contains('\x1b') {
        return input.to_owned();
    }
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' && chars.peek() == Some(&'[') {
            chars.next(); // consume '['
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
