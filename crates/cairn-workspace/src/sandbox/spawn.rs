//! Spawn the `--sandboxed-agent` child process (F65 PR-4 process model, locked Q1/Q2).
//!
//! The parent cairn-app calls [`spawn_sandboxed_agent`] to fork itself with
//! the `--sandboxed-agent` flag. A socketpair is created (SOCK_STREAM),
//! one end stays in the parent, the other is duped onto fd 3 in the child via
//! the `command-fds` crate. The child uses fd 3 as its tool-bridge to the
//! parent's tool dispatcher.
//!
//! The child is expected to call
//! [`crate::sandbox::confinement::SandboxConfinement::confine`] with
//! `keep_fd = Some(3)` immediately after reading any startup args; from that
//! point on it can neither escape the Landlock fence nor `mount(2)` anything
//! new.
//!
//! # Environment isolation (issue #527)
//!
//! The sandboxed child MUST NOT inherit the parent cairn-app's environment
//! wholesale. The parent process holds every provider secret (OPENAI_API_KEY,
//! ANTHROPIC_API_KEY, BEDROCK_API_KEY, …), the admin token
//! (CAIRN_ADMIN_TOKEN), the DB URL with embedded password (DATABASE_URL), and
//! — once #461 lands — the credential-encryption key (CAIRN_CREDENTIAL_KEY).
//! None of these belong in the child: the child talks to the parent over fd 3
//! for anything it would have used a secret for. A prompt-injected or buggy
//! child that dumps `/proc/self/environ` would otherwise exfiltrate the whole
//! kit.
//!
//! [`spawn_sandboxed_agent`] therefore calls `.env_clear()` on the `Command`
//! builder and re-injects a tight allowlist via [`build_sandbox_env`]. Every
//! variable the child sees is either:
//!
//! * a locale/PATH baseline the child needs to run at all (PATH, HOME, USER,
//!   LANG, LC_*, TZ, TMPDIR, RUST_LOG, RUST_BACKTRACE) forwarded from the
//!   parent when present;
//! * a cairn-specific contract variable the child reads at startup:
//!   `CAIRN_SESSION_ID` is injected from [`SpawnConfig::session_id`];
//!   `CAIRN_SANDBOX_BASE_DIR` is injected from
//!   [`SpawnConfig::sandbox_base_dir`] (or explicitly via
//!   [`SpawnConfig::extra_env`]) — it is NOT in `FORWARD_ALLOWLIST`, so the
//!   parent's value is never inherited automatically after `env_clear`;
//!   `CAIRN_SANDBOX_DISABLE_*` IS in the allowlist and IS forwarded from the
//!   parent when set (dev/test affordances only);
//! * an explicitly-declared pass-through from
//!   [`SpawnConfig::extra_env`] — the agent's own env contract.
//!
//! Any other env var — including anything matching `*_API_KEY`, `*_SECRET*`,
//! `*_TOKEN*`, `AWS_*`, `GCP_*`, `GOOGLE_*`, `AZURE_*`, `BEDROCK_*`,
//! `DATABASE_URL`, `CAIRN_ADMIN_TOKEN*`, `CAIRN_CREDENTIAL_KEY` — never
//! reaches the child. Callers who genuinely need to pass a non-allowlisted
//! variable (there should be no such case today) must declare it in
//! `extra_env` and audit the decision.

use std::ffi::{OsStr, OsString};
use std::io;
use std::os::unix::io::OwnedFd;
use std::path::PathBuf;

use command_fds::{CommandFdExt, FdMapping};
use tokio::net::UnixStream;

/// Handle returned to the parent after [`spawn_sandboxed_agent`] succeeds.
///
/// Drop semantics: `std::process::Child` on Unix does NOT kill the subprocess
/// on drop. The handle's `Drop` impl below sends `SIGKILL` + `waitpid`
/// explicitly so an orphaned sandboxed-agent cannot outlive its handle.
/// Callers who want a graceful shutdown should call
/// [`SandboxedAgentHandle::shutdown`] with a timeout BEFORE dropping.
pub struct SandboxedAgentHandle {
    /// Parent-side half of the tool-bridge socketpair, wrapped in tokio.
    pub stream: UnixStream,
    /// PID of the child cairn-app subprocess (the sandboxed agent).
    pub pid: u32,
    /// Child-side handle. See `Drop` impl for teardown semantics.
    pub child: std::process::Child,
}

impl SandboxedAgentHandle {
    /// Graceful shutdown: wait for the child to exit (after the caller has
    /// closed the tool-bridge stream by dropping [`Self::stream`] via
    /// `.take_stream()` or `std::mem::take`-ing it). Returns the exit status.
    ///
    /// If the child is still running after `timeout`, this falls back to
    /// SIGKILL. Safe either way — the overlay work dir is reaped right
    /// after.
    pub fn shutdown(
        &mut self,
        timeout: std::time::Duration,
    ) -> io::Result<std::process::ExitStatus> {
        // Poll with a short sleep loop; avoids pulling tokio::time onto
        // the blocking teardown path.
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Ok(status);
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        // Ran out of grace — hard kill.
        let _ = self.child.kill();
        self.child.wait()
    }
}

impl Drop for SandboxedAgentHandle {
    fn drop(&mut self) {
        // Best-effort: if the caller didn't call `shutdown`, kill the child
        // hard so we don't leave an orphaned sandboxed-agent. `try_wait`
        // avoids a hang when the child already exited. SIGKILL is safe
        // because the child is inside our overlayfs overlay and its work
        // dir is about to be reaped; there's no clean-shutdown contract
        // to honor.
        match self.child.try_wait() {
            Ok(Some(_)) => { /* already exited */ }
            Ok(None) | Err(_) => {
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
        }
    }
}

/// Parameters for the spawn.
///
/// `agent_binary` is typically `/proc/self/exe` so the child is the exact
/// same cairn-app build as the parent. `workspace_id` is the opaque F65
/// workspace ID; `merged_path` is the overlayfs mount point the child will
/// see as its workspace root; `extra_args` lets callers thread goal/session
/// info into the child via the CLI.
///
/// `session_id` is the current orchestrator session and is surfaced to the
/// child as `CAIRN_SESSION_ID` — diagnostics, log scoping, and tool-bridge
/// correlation all key off it. `sandbox_base_dir`, when set, is surfaced as
/// `CAIRN_SANDBOX_BASE_DIR` so probes inside the child that stat the sandbox
/// root agree with the parent on the path (see `crates/cairn-app/src/state.rs`).
///
/// `extra_env` is the escape hatch for agent-declared variables. Contents
/// are forwarded verbatim; callers are responsible for making sure no secret
/// sneaks in. The default allowlist + injected vars are the first-class path —
/// reach for `extra_env` only when the agent explicitly declares a
/// pass-through (e.g. a non-secret feature flag).
#[derive(Debug, Clone)]
pub struct SpawnConfig {
    pub agent_binary: PathBuf,
    pub workspace_id: String,
    pub merged_path: PathBuf,
    pub extra_args: Vec<String>,
    /// Working directory for the child. Should equal `merged_path` so the
    /// agent starts at the workspace root.
    pub working_dir: PathBuf,
    /// Orchestrator session ID. Surfaced as `CAIRN_SESSION_ID` to the child.
    pub session_id: String,
    /// Optional workspace-root override, surfaced as `CAIRN_SANDBOX_BASE_DIR`
    /// when set. When `None`, NO `CAIRN_SANDBOX_BASE_DIR` is injected into
    /// the child by this spawn path: the child env is rebuilt from
    /// `FORWARD_ALLOWLIST` (which does NOT include `CAIRN_SANDBOX_BASE_DIR`),
    /// plus the injected contract vars, plus `extra_env` — so the parent's
    /// value is not inherited automatically after `env_clear`. A caller that
    /// wants the variable set while leaving this field as `None` must forward
    /// it explicitly via [`SpawnConfig::extra_env`]. Otherwise the child
    /// falls back to the runtime default cairn-app uses —
    /// `$TMPDIR/cairn-workspace-sandboxes`. See
    /// `cairn-app/src/state.rs::default_sandbox_base_dir`.
    pub sandbox_base_dir: Option<PathBuf>,
    /// Explicit agent-declared environment forwards. Applied AFTER the
    /// static allowlist and the cairn-specific injected vars; a key present
    /// here overrides the earlier value. Values are `OsString` so non-UTF8
    /// paths round-trip losslessly into the child. USE SPARINGLY — this
    /// bypasses the allowlist. NEVER put a secret here; fd 3 is the right
    /// channel.
    pub extra_env: Vec<(String, OsString)>,
}

/// Environment variables forwarded from the parent when present.
///
/// Rules:
/// * No secrets — everything in this list is either locale-related, a
///   standard unix identity var, or a Rust tracing knob.
/// * Everything here must be safe to leak to a prompt-injected agent.
/// * The `CAIRN_SANDBOX_DISABLE_*` entries are dev/test affordances the
///   child reads at startup; production parents never have them set.
///
/// Tests assert that no entry in this list matches any secret-shaped pattern
/// (`*_API_KEY`, `*_SECRET*`, `*_TOKEN*`, `AWS_*`, `GCP_*`, `GOOGLE_*`,
/// `AZURE_*`, `BEDROCK_*`, `DATABASE_URL`, `CAIRN_ADMIN_TOKEN*`,
/// `CAIRN_CREDENTIAL_KEY`).
const FORWARD_ALLOWLIST: &[&str] = &[
    // POSIX identity + search path — PATH is the one var we absolutely need
    // for the child to be able to exec anything; HOME/USER are standard
    // expectations of many toolchains (cargo, git, …).
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "SHELL",
    // Locale — needed for UTF-8 output from many CLIs.
    "LANG",
    "LANGUAGE",
    "LC_ALL",
    "LC_CTYPE",
    "LC_COLLATE",
    "LC_MESSAGES",
    "LC_NUMERIC",
    "LC_MONETARY",
    "LC_TIME",
    "TZ",
    // Temp dir — tools respect this for scratch writes.
    "TMPDIR",
    // Rust tracing knobs — non-sensitive, useful for debugging.
    "RUST_LOG",
    "RUST_BACKTRACE",
    // Dev/test affordances the sandboxed-agent child reads at startup.
    // These must be forwarded so the child's `parse()` matches the
    // confinement-layer decision the parent made. Production parents never
    // have these set.
    "CAIRN_SANDBOX_DISABLE_LANDLOCK",
    "CAIRN_SANDBOX_DISABLE_SECCOMP",
    "CAIRN_SANDBOX_DISABLE_UNSHARE",
];

/// Build the full environment handed to a sandboxed-agent child.
///
/// Deterministic + grep-able: every variable the child sees comes from one of
/// three sources, applied in this order (later wins):
///
/// 1. `FORWARD_ALLOWLIST` — parent's value forwarded if present.
/// 2. `CAIRN_SESSION_ID` + `CAIRN_SANDBOX_BASE_DIR` — injected from `config`.
/// 3. `config.extra_env` — explicit agent-declared forwards.
///
/// Secrets (`*_API_KEY`, `*_SECRET*`, `*_TOKEN*`, `AWS_*`, `GCP_*`,
/// `GOOGLE_*`, `AZURE_*`, `BEDROCK_*`, `DATABASE_URL`, `CAIRN_ADMIN_TOKEN*`,
/// `CAIRN_CREDENTIAL_KEY`) are NEVER forwarded: they're absent from the
/// allowlist, and the tests in this crate assert that the resulting env does
/// not contain any of them even if the parent has them set.
///
/// Exposed as `pub` because the integration tests — and any future service
/// layer that wants to preview what a given `SpawnConfig` will ship to the
/// child — should be able to call it without having to `spawn` first.
///
/// Values are `OsString` so non-UTF8 paths (rare but legal on Unix) round-trip
/// losslessly into `Command::envs`; going through `String` would silently
/// replace invalid bytes via `to_string_lossy` and hand the child a different
/// path than the parent intended.
pub fn build_sandbox_env(config: &SpawnConfig) -> Vec<(String, OsString)> {
    let mut env: Vec<(String, OsString)> = Vec::with_capacity(FORWARD_ALLOWLIST.len() + 4);

    // 1. Forward from the parent, only the allowlisted keys. Use `var_os` so
    //    non-UTF8 values are preserved verbatim instead of dropped.
    for key in FORWARD_ALLOWLIST {
        if let Some(value) = std::env::var_os(key) {
            env.push(((*key).to_string(), value));
        }
    }

    // 2. Inject cairn-specific contract vars from config. These overwrite
    //    any allowlisted same-key (there shouldn't be one for
    //    `CAIRN_SESSION_ID` or `CAIRN_SANDBOX_BASE_DIR` — neither is in
    //    `FORWARD_ALLOWLIST` — but we follow the "later wins" rule
    //    explicitly).
    push_override(&mut env, "CAIRN_SESSION_ID", OsStr::new(&config.session_id));
    if let Some(dir) = &config.sandbox_base_dir {
        push_override(&mut env, "CAIRN_SANDBOX_BASE_DIR", dir.as_os_str());
    }

    // 3. Agent-declared forwards. Last-wins so a caller can override the
    //    injected CAIRN_* vars if they really need to (rare; normally the
    //    config fields above are the right path).
    for (k, v) in &config.extra_env {
        push_override(&mut env, k, v.as_ref());
    }

    env
}

/// Push `(key, value)` to `env`, replacing any prior entry with the same key.
///
/// `std::process::Command::envs` applies entries in iteration order and
/// dedupes on the Command builder itself, so duplicates in the Vec are
/// harmless for spawn; we dedupe here anyway so `build_sandbox_env`'s return
/// value is a clean map (tests inspect it directly).
fn push_override(env: &mut Vec<(String, OsString)>, key: &str, value: &OsStr) {
    if let Some(existing) = env.iter_mut().find(|(k, _)| k == key) {
        existing.1 = value.to_os_string();
    } else {
        env.push((key.to_string(), value.to_os_string()));
    }
}

/// Spawn the sandboxed-agent child and return a handle to talk to it.
///
/// The child inherits the socketpair on fd 3. The `command-fds` crate issues
/// the `dup2` under the hood via `std::os::unix::process::CommandExt::pre_exec`
/// — but inside the library's `unsafe` boundary, NOT ours. That's why we can
/// keep `unsafe_code = "forbid"` at the workspace level.
///
/// The child's environment is REBUILT from scratch via
/// [`build_sandbox_env`] — the parent cairn-app's env (provider keys,
/// DATABASE_URL, CAIRN_ADMIN_TOKEN, …) does NOT leak in. See the module doc
/// for the full rationale (issue #527).
pub fn spawn_sandboxed_agent(config: SpawnConfig) -> io::Result<SandboxedAgentHandle> {
    let (parent_fd, child_fd) = socketpair_stream()?;

    let mut command = std::process::Command::new(&config.agent_binary);
    command
        .arg("--sandboxed-agent")
        .arg("--workspace-id")
        .arg(&config.workspace_id)
        .arg("--merged-path")
        .arg(&config.merged_path)
        .arg("--socket-fd")
        .arg("3")
        .args(&config.extra_args)
        .current_dir(&config.working_dir);

    // Wipe the inherited environment and reinject only the allowlisted +
    // explicitly-declared vars. This is the #527 security fix: before this
    // call, the child inherited every provider API key, the admin token,
    // DATABASE_URL, and whatever else the parent had. An agent that reads
    // `/proc/self/environ` would have exfiltrated the whole set.
    command.env_clear();
    command.envs(build_sandbox_env(&config));

    // Map our child-side fd onto fd 3 in the child.
    // OwnedFd transfers ownership; command-fds calls dup2 in the child during pre_exec.
    command
        .fd_mappings(vec![FdMapping {
            parent_fd: child_fd,
            child_fd: 3,
        }])
        .map_err(|err| io::Error::other(format!("fd_mappings: {err}")))?;

    let child = command.spawn().map_err(|err| {
        io::Error::other(format!("spawn {}: {err}", config.agent_binary.display()))
    })?;
    let pid = child.id();

    // Wrap our parent-side half in tokio.
    let std_stream = parent_fd;
    std_stream.set_nonblocking(true)?;
    let stream = UnixStream::from_std(std_stream)?;

    Ok(SandboxedAgentHandle { stream, pid, child })
}

/// Create a `SOCK_STREAM` socketpair using nix (safe wrapper around
/// `socketpair(2)`). Returns (parent_unix_stream, child_owned_fd).
///
/// Both ends are opened with `FD_CLOEXEC` set (via `SOCK_CLOEXEC`) so the
/// fd numbers we hold cannot leak into unrelated forked children. The
/// child-side fd still reaches its destination on fd 3 in the sandboxed
/// agent because `command-fds` calls `dup2(child_fd, 3)` in the child's
/// pre-exec hook — `dup2` produces a fresh fd number that is implicitly
/// non-CLOEXEC, so fd 3 survives exec.
fn socketpair_stream() -> io::Result<(std::os::unix::net::UnixStream, OwnedFd)> {
    use nix::sys::socket::{socketpair, AddressFamily, SockFlag, SockType};
    let (a, b) = socketpair(
        AddressFamily::Unix,
        SockType::Stream,
        None,
        SockFlag::SOCK_CLOEXEC,
    )
    .map_err(|err| io::Error::other(format!("socketpair: {err}")))?;
    // Parent: UnixStream (tokio-friendly). Child: keep as OwnedFd for command-fds.
    let parent = std::os::unix::net::UnixStream::from(a);
    Ok((parent, b))
}

/// Kernel-level `dup2`-and-exec wiring probe — unit-test hook confirming the
/// fd mapping compiles on the target.
#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_config() -> SpawnConfig {
        SpawnConfig {
            agent_binary: PathBuf::from("/usr/bin/true"),
            workspace_id: "wkspc-abc".to_string(),
            merged_path: PathBuf::from("/tmp/merged"),
            extra_args: vec!["--goal".to_string(), "test".to_string()],
            working_dir: PathBuf::from("/tmp/merged"),
            session_id: "sess-xyz".to_string(),
            sandbox_base_dir: Some(PathBuf::from("/var/lib/cairn-workspaces")),
            extra_env: vec![],
        }
    }

    #[test]
    fn spawn_config_fields_are_debug() {
        let cfg = fixture_config();
        let s = format!("{:?}", cfg);
        assert!(s.contains("wkspc-abc"));
        assert!(s.contains("sess-xyz"));
    }

    #[test]
    fn allowlist_contains_no_secret_shaped_names() {
        // Guard against future edits sneaking a secret-shaped var into the
        // forward list. If a variant is added that matches any pattern
        // below, this test fires before CI.
        for key in FORWARD_ALLOWLIST {
            let upper = key.to_ascii_uppercase();
            assert!(
                !upper.ends_with("_API_KEY"),
                "forbidden allowlist entry: {key}"
            );
            assert!(
                !upper.ends_with("_SECRET"),
                "forbidden allowlist entry: {key}"
            );
            assert!(
                !upper.contains("_SECRET_"),
                "forbidden allowlist entry: {key}"
            );
            assert!(
                !upper.ends_with("_TOKEN"),
                "forbidden allowlist entry: {key}"
            );
            assert!(
                !upper.contains("_TOKEN_"),
                "forbidden allowlist entry: {key}"
            );
            assert!(
                !upper.starts_with("AWS_"),
                "forbidden allowlist entry: {key}"
            );
            assert!(
                !upper.starts_with("GCP_"),
                "forbidden allowlist entry: {key}"
            );
            assert!(
                !upper.starts_with("GOOGLE_"),
                "forbidden allowlist entry: {key}"
            );
            assert!(
                !upper.starts_with("AZURE_"),
                "forbidden allowlist entry: {key}"
            );
            assert!(
                !upper.starts_with("BEDROCK_"),
                "forbidden allowlist entry: {key}"
            );
            assert!(upper != "DATABASE_URL", "forbidden allowlist entry: {key}");
            assert!(
                !upper.starts_with("CAIRN_ADMIN_TOKEN"),
                "forbidden allowlist entry: {key}"
            );
            assert!(
                upper != "CAIRN_CREDENTIAL_KEY",
                "forbidden allowlist entry: {key}"
            );
        }
    }

    #[test]
    fn build_env_injects_session_id_and_base_dir() {
        let cfg = fixture_config();
        let env = build_sandbox_env(&cfg);
        let map: std::collections::HashMap<String, OsString> = env.into_iter().collect();
        assert_eq!(
            map.get("CAIRN_SESSION_ID").map(OsString::as_os_str),
            Some(OsStr::new("sess-xyz"))
        );
        assert_eq!(
            map.get("CAIRN_SANDBOX_BASE_DIR").map(OsString::as_os_str),
            Some(OsStr::new("/var/lib/cairn-workspaces"))
        );
    }

    #[test]
    fn build_env_omits_base_dir_when_none() {
        let mut cfg = fixture_config();
        cfg.sandbox_base_dir = None;
        let env = build_sandbox_env(&cfg);
        assert!(
            !env.iter().any(|(k, _)| k == "CAIRN_SANDBOX_BASE_DIR"),
            "absent sandbox_base_dir must not surface an empty CAIRN_SANDBOX_BASE_DIR"
        );
    }

    #[test]
    fn build_env_extra_env_overrides_injected() {
        // If a caller explicitly declares CAIRN_SESSION_ID in extra_env they
        // win over the config field. This is rare (the field is the normal
        // path) but follows the "later wins" contract we document.
        let mut cfg = fixture_config();
        cfg.extra_env = vec![("CAIRN_SESSION_ID".to_string(), OsString::from("override"))];
        let env = build_sandbox_env(&cfg);
        let session = env
            .iter()
            .find(|(k, _)| k == "CAIRN_SESSION_ID")
            .map(|(_, v)| v.as_os_str());
        assert_eq!(session, Some(OsStr::new("override")));
        // And we still have exactly one entry for the key — no duplicates.
        assert_eq!(
            env.iter().filter(|(k, _)| k == "CAIRN_SESSION_ID").count(),
            1,
            "push_override must dedupe"
        );
    }

    #[test]
    fn build_env_preserves_non_utf8_base_dir_bytes() {
        // Non-UTF8 paths are legal on Unix. Round-tripping through String
        // would hit `to_string_lossy` and replace bytes with U+FFFD, handing
        // the child a *different* path. Using OsString preserves the exact
        // byte sequence.
        use std::os::unix::ffi::OsStringExt;
        let bad_bytes: Vec<u8> = vec![b'/', b't', b'm', b'p', b'/', 0xff, 0xfe, b'/', b'x'];
        let bad_path = PathBuf::from(OsString::from_vec(bad_bytes.clone()));
        let mut cfg = fixture_config();
        cfg.sandbox_base_dir = Some(bad_path.clone());
        let env = build_sandbox_env(&cfg);
        let got = env
            .iter()
            .find(|(k, _)| k == "CAIRN_SANDBOX_BASE_DIR")
            .map(|(_, v)| v.as_os_str());
        assert_eq!(got, Some(bad_path.as_os_str()));
    }
}
