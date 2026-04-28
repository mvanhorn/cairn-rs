//! Integration tests for #527 — sandbox spawn env isolation.
//!
//! Exercise `spawn_sandboxed_agent` end-to-end with a real forked child
//! (the `sandbox_env_dumper` helper binary in this crate). The child reads
//! `/proc/self/environ` — the kernel's ground-truth view of what reached the
//! process across `exec(2)` — and writes a JSON snapshot over fd 3. The
//! parent test then asserts:
//!
//! 1. **No secrets leak.** Sentinel values set on the parent process for
//!    `CAIRN_ADMIN_TOKEN`, `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`,
//!    `AWS_SECRET_ACCESS_KEY`, `DATABASE_URL`, `CAIRN_CREDENTIAL_KEY`, and
//!    `BEDROCK_API_KEY` do NOT appear in the child's env.
//! 2. **Baseline vars forward.** `PATH`, `HOME`, and `LANG` (when set on
//!    the parent) reach the child with matching values.
//! 3. **Contract vars inject.** `CAIRN_SESSION_ID` and
//!    `CAIRN_SANDBOX_BASE_DIR` carry the values supplied via `SpawnConfig`.
//! 4. **Agent-declared `extra_env` reaches the child verbatim.**
//!
//! Ground rule (from `feedback_integration_tests_only.md`): these are real
//! spawn-fork-exec integration tests — the child is a real process reading
//! `/proc/self/environ`, NOT a mock.

#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use cairn_workspace::sandbox::spawn::{
    build_sandbox_env, spawn_sandboxed_agent, SandboxedAgentHandle, SpawnConfig,
};
use tokio::io::AsyncReadExt;

/// Serialize every test in this file so they don't race on the process-wide
/// env via `std::env::{set_var, remove_var}`. Cargo's test harness runs
/// tests in parallel on separate threads of the same process; two concurrent
/// mutations of the env block are a data race (the warning you see in
/// edition-2024 rust is not hypothetical). Per
/// `feedback_no_such_thing_as_flake.md`, the correct fix is determinism, not
/// `--test-threads=1` or sleep-retry.
fn env_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        // PoisonError means another test panicked while holding the lock.
        // Keep going — the guard the panicked test tried to set up gets
        // torn down by its own `Drop` regardless, so our pre-spawn setup
        // still sees a clean slate.
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Path to the test-helper binary. Cargo sets `CARGO_BIN_EXE_<name>` for
/// every `[[bin]]` entry in the crate when building integration tests.
fn env_dumper_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_sandbox_env_dumper"))
}

/// Sentinel values placed on the parent process before spawn. Each is a
/// unique recognizable string so the assertions can grep for them directly
/// in the child's env dump. Any appearance of any of these strings in the
/// child is a test failure — the whole point of #527 is that secrets never
/// cross the sandbox boundary.
const SENTINELS: &[(&str, &str)] = &[
    ("CAIRN_ADMIN_TOKEN", "sentinel-admin-xyz"),
    ("CAIRN_CREDENTIAL_KEY", "sentinel-credkey-xyz"),
    (
        "DATABASE_URL",
        "postgres://sentinel:sentinel-db-xyz@localhost/sentinel",
    ),
    ("OPENAI_API_KEY", "sentinel-openai-xyz"),
    ("ANTHROPIC_API_KEY", "sentinel-anthropic-xyz"),
    ("ZAI_API_KEY", "sentinel-zai-xyz"),
    ("BEDROCK_API_KEY", "sentinel-bedrock-xyz"),
    ("AWS_SECRET_ACCESS_KEY", "sentinel-aws-xyz"),
    ("AWS_SESSION_TOKEN", "sentinel-aws-session-xyz"),
    ("GCP_SA_KEY", "sentinel-gcp-xyz"),
    ("GOOGLE_APPLICATION_CREDENTIALS", "sentinel-google-xyz"),
    ("AZURE_CLIENT_SECRET", "sentinel-azure-xyz"),
];

/// Pass a `SpawnConfig` through [`spawn_sandboxed_agent`], read one
/// newline-terminated JSON frame from the tool-bridge stream, and return
/// the env map the child observed.
///
/// The whole spawn + read + shutdown path runs inside a tokio current-thread
/// runtime because `spawn_sandboxed_agent` wraps the parent-side socketpair
/// in a `tokio::net::UnixStream` at construction time — that call requires a
/// runtime handle.
fn capture_child_env(config: SpawnConfig) -> BTreeMap<String, String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime for test harness");

    let frame = rt.block_on(async move {
        let mut handle = spawn_sandboxed_agent(config).expect("spawn helper child");
        let frame = drain_env_frame_async(&mut handle).await;

        // Teardown must happen inside block_on too: `shutdown` kills the
        // child and reaps; the `Drop` impl on the handle would otherwise
        // try to do the same after we've already left the runtime context.
        let status = handle
            .shutdown(Duration::from_secs(5))
            .expect("helper child must exit within 5s");
        assert!(
            status.success(),
            "helper child exited non-zero: {:?}",
            status
        );
        frame
    });

    parse_env_json(&frame)
}

/// Read one newline-terminated frame off `handle.stream` inside a tokio
/// runtime. Bounded by a 5-second timeout — if the child hangs, fail loudly
/// (feedback_no_such_thing_as_flake: no sleep+retry, no silent swallow).
async fn drain_env_frame_async(handle: &mut SandboxedAgentHandle) -> String {
    let mut buf = [0u8; 4096];
    let mut pending = Vec::<u8>::new();
    let read_fut = async {
        loop {
            let n = handle
                .stream
                .read(&mut buf)
                .await
                .expect("read from helper child");
            if n == 0 {
                break;
            }
            pending.extend_from_slice(&buf[..n]);
            if let Some(pos) = pending.iter().position(|&b| b == b'\n') {
                pending.truncate(pos);
                break;
            }
        }
        String::from_utf8(pending).expect("helper child must emit UTF-8")
    };

    tokio::time::timeout(Duration::from_secs(5), read_fut)
        .await
        .expect("helper child must produce a frame within 5s")
}

/// Parse the helper's single-frame `{"env":{...}}` JSON using `serde_json`
/// (already a workspace dep), so RFC 8259 escapes and edge cases are handled
/// by the real parser rather than a hand-rolled state machine.
fn parse_env_json(frame: &str) -> BTreeMap<String, String> {
    let value: serde_json::Value = serde_json::from_str(frame)
        .unwrap_or_else(|err| panic!("invalid JSON frame from env dumper: {err}: {frame}"));
    let env = value
        .get("env")
        .and_then(serde_json::Value::as_object)
        .unwrap_or_else(|| panic!("unexpected frame shape (no `env` object): {frame}"));

    env.iter()
        .map(|(key, value)| {
            let s = value
                .as_str()
                .unwrap_or_else(|| panic!("env value for `{key}` must be a string: {frame}"));
            (key.clone(), s.to_owned())
        })
        .collect()
}

/// Set every sentinel as a parent-process env var before the spawn, then
/// tear them down. The test suite runs `cargo test` inside one process with
/// multiple test threads, so we MUST serialize access to the process env
/// AND clean up — otherwise sentinel values leak across tests.
///
/// The guard holds [`env_lock`] for its entire lifetime, so only one test
/// at a time is touching the env. Tests must call `SentinelEnvGuard::set()`
/// BEFORE any spawn or env read.
struct SentinelEnvGuard {
    // Holds the serialization lock. Dropped last on unwind.
    _lock: MutexGuard<'static, ()>,
    // Remember the HOME/LANG values we overrode so we can restore them.
    prior_home: Option<String>,
    prior_lang: Option<String>,
}

impl SentinelEnvGuard {
    fn set() -> Self {
        let lock = env_lock();
        let prior_home = std::env::var("HOME").ok();
        let prior_lang = std::env::var("LANG").ok();
        for (k, v) in SENTINELS {
            // SAFETY-NOTE (no unsafe): set_var is safe on all Rust editions
            // we target. It's only flagged unsound under edition 2024
            // `unsafe_op_in_unsafe_fn` — cairn's toolchain today is edition
            // 2021, and we hold `env_lock` so the mutation is serialized.
            std::env::set_var(k, v);
        }
        // Stable baseline vars so assertions compare against a known value
        // regardless of the test runner's own HOME/LANG.
        std::env::set_var("HOME", "/tmp/sentinel-home");
        std::env::set_var("LANG", "C.UTF-8");
        Self {
            _lock: lock,
            prior_home,
            prior_lang,
        }
    }
}

impl Drop for SentinelEnvGuard {
    fn drop(&mut self) {
        for (k, _) in SENTINELS {
            std::env::remove_var(k);
        }
        match &self.prior_home {
            Some(v) => std::env::set_var("HOME", v),
            None => std::env::remove_var("HOME"),
        }
        match &self.prior_lang {
            Some(v) => std::env::set_var("LANG", v),
            None => std::env::remove_var("LANG"),
        }
    }
}

fn fixture_config(tmp: &tempfile::TempDir, extra_env: Vec<(String, OsString)>) -> SpawnConfig {
    SpawnConfig {
        agent_binary: env_dumper_bin(),
        workspace_id: "wkspc-test-527".to_string(),
        merged_path: tmp.path().to_path_buf(),
        // extra_args after our positional flags — the dumper ignores them.
        extra_args: vec![],
        working_dir: tmp.path().to_path_buf(),
        session_id: "sess-test-527".to_string(),
        sandbox_base_dir: Some(PathBuf::from("/var/lib/cairn-workspaces-test")),
        extra_env,
    }
}

// ─── Test 1 (issue #527 primary): sentinel secrets MUST NOT leak.

#[test]
fn secrets_do_not_leak_into_sandbox_child() {
    let _guard = SentinelEnvGuard::set();
    let tmp = tempfile::tempdir().expect("tmpdir");

    let child_env = capture_child_env(fixture_config(&tmp, vec![]));

    // PRIMARY ASSERTION: none of the sentinel KEYS are present at all.
    for (key, _) in SENTINELS {
        assert!(
            !child_env.contains_key(*key),
            "SECURITY: child inherited forbidden env key `{key}`. \
             Full child env: {child_env:?}"
        );
    }

    // BACKUP ASSERTION: even if a future edit accidentally lets one through
    // under a different key name, the sentinel VALUE must never appear in
    // any env slot. This guards against e.g. someone accidentally adding
    // `*_KEY` to the allowlist.
    for (sentinel_key, sentinel_value) in SENTINELS {
        for (child_key, child_value) in &child_env {
            assert!(
                !child_value.contains(sentinel_value),
                "SECURITY: sentinel value for `{sentinel_key}` appeared in \
                 child env under `{child_key}`: {child_value:?}"
            );
        }
    }
}

// ─── Test 2: baseline env vars (PATH, HOME, LANG) forward when set.

#[test]
fn baseline_vars_are_forwarded() {
    let _guard = SentinelEnvGuard::set();
    let tmp = tempfile::tempdir().expect("tmpdir");

    let child_env = capture_child_env(fixture_config(&tmp, vec![]));

    // PATH is always present in the parent under cargo test; assert by
    // exact-value match with the parent snapshot rather than by hardcoding.
    let parent_path = std::env::var("PATH").expect("parent PATH must be set");
    assert_eq!(
        child_env.get("PATH").map(String::as_str),
        Some(parent_path.as_str()),
        "PATH must forward verbatim"
    );

    assert_eq!(
        child_env.get("HOME").map(String::as_str),
        Some("/tmp/sentinel-home"),
        "HOME must forward verbatim"
    );
    assert_eq!(
        child_env.get("LANG").map(String::as_str),
        Some("C.UTF-8"),
        "LANG must forward verbatim"
    );
}

// ─── Test 3: contract vars are injected from SpawnConfig.

#[test]
fn contract_vars_are_injected_from_config() {
    let _guard = SentinelEnvGuard::set();
    let tmp = tempfile::tempdir().expect("tmpdir");

    let child_env = capture_child_env(fixture_config(&tmp, vec![]));

    assert_eq!(
        child_env.get("CAIRN_SESSION_ID").map(String::as_str),
        Some("sess-test-527"),
        "CAIRN_SESSION_ID must reach the child from SpawnConfig"
    );
    assert_eq!(
        child_env.get("CAIRN_SANDBOX_BASE_DIR").map(String::as_str),
        Some("/var/lib/cairn-workspaces-test"),
        "CAIRN_SANDBOX_BASE_DIR must reach the child from SpawnConfig"
    );
}

// ─── Test 4: explicit extra_env reaches the child verbatim, nothing else.

#[test]
fn extra_env_is_honored_and_nothing_else_leaks() {
    let _guard = SentinelEnvGuard::set();
    let tmp = tempfile::tempdir().expect("tmpdir");

    let declared: Vec<(String, OsString)> = vec![
        ("CAIRN_AGENT_ROLE".to_string(), OsString::from("worker")),
        ("CAIRN_FEATURE_FLAG".to_string(), OsString::from("xyz")),
    ];
    let cfg = fixture_config(&tmp, declared.clone());

    // Derive the expected key set directly from the production source of
    // truth (`build_sandbox_env`) rather than hand-maintaining a duplicate
    // of `FORWARD_ALLOWLIST` + the injection logic here. If a future edit
    // extends the allowlist or changes what gets injected, this assertion
    // follows automatically instead of producing confusing false failures.
    let allowed: std::collections::BTreeSet<String> = build_sandbox_env(&cfg)
        .into_iter()
        .map(|(k, _)| k)
        .collect();

    let child_env = capture_child_env(cfg);

    for (k, v) in &declared {
        let expected = v.to_str().expect("fixture values are UTF-8");
        assert_eq!(
            child_env.get(k).map(String::as_str),
            Some(expected),
            "extra_env entry {k}={v:?} must reach the child"
        );
    }

    for key in child_env.keys() {
        assert!(
            allowed.contains(key),
            "child inherited unexpected env key `{key}` — not produced by \
             build_sandbox_env (i.e. not in FORWARD_ALLOWLIST, not injected, \
             not declared via extra_env). Full env: {child_env:?}"
        );
    }
}

// ─── Test 5: build_sandbox_env produces the same snapshot in isolation.

#[test]
fn build_sandbox_env_matches_child_observed_values() {
    // Pure-logic regression: whatever `build_sandbox_env` returns MUST be
    // what the child sees (modulo values the parent had at spawn time, which
    // we control via the guard). If a refactor makes them diverge, fire.
    let _guard = SentinelEnvGuard::set();
    let tmp = tempfile::tempdir().expect("tmpdir");

    let cfg = fixture_config(
        &tmp,
        vec![("CAIRN_FEATURE_FLAG".to_string(), OsString::from("v1"))],
    );
    let predicted: BTreeMap<String, OsString> = build_sandbox_env(&cfg).into_iter().collect();
    let child_env = capture_child_env(cfg);

    for (k, v) in &predicted {
        let expected = v
            .to_str()
            .expect("test fixture only sets UTF-8 values; verified by env_dumper too");
        assert_eq!(
            child_env.get(k).map(String::as_str),
            Some(expected),
            "build_sandbox_env predicted {k}={v:?} but child saw {:?}",
            child_env.get(k)
        );
    }
    // Every child key must be one `build_sandbox_env` predicted. If the
    // child has MORE keys, something inherited outside the allowlist.
    for k in child_env.keys() {
        assert!(
            predicted.contains_key(k),
            "child observed unexpected key {k} not produced by build_sandbox_env"
        );
    }
}

// ─── Test 6: secrets are not preserved in cmd args either (belt + braces).

#[test]
fn secrets_do_not_appear_in_extra_args() {
    // Defense-in-depth: `extra_args` is positional CLI, not env, but sanity-
    // check that a caller who accidentally puts a secret there doesn't
    // ALSO leak via the env path. (Args are visible via /proc/<pid>/cmdline
    // — that's a separate issue; this test only covers env.)
    let _guard = SentinelEnvGuard::set();
    let tmp = tempfile::tempdir().expect("tmpdir");

    let mut cfg = fixture_config(&tmp, vec![]);
    cfg.extra_args = vec!["--goal".to_string(), "sentinel-admin-xyz".to_string()];
    let child_env = capture_child_env(cfg);

    // The sentinel value appearing in args does NOT mean it appeared in env.
    // Verify env path stays clean.
    for (_, sentinel_value) in SENTINELS {
        for (child_key, child_value) in &child_env {
            assert!(
                !child_value.contains(sentinel_value),
                "sentinel leaked into child env via key `{child_key}`"
            );
        }
    }
}
