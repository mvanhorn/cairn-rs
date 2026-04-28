//! F65 PR-4: `cairn-app --sandboxed-agent` mode.
//!
//! Process model (locked Q1): cairn-app is a single binary; the parent
//! cairn-app spawns itself with `--sandboxed-agent` for every root-Run
//! attempt. The child inherits the tool-bridge socket on fd 3, applies the
//! three-layer confinement fence, then reads tool-call JSON-RPC off fd 3 and
//! dispatches back to the parent's tool runner.
//!
//! This module is intentionally tiny: everything security-critical lives in
//! `cairn_workspace::sandbox::confinement`. The sandboxed agent binary has
//! two jobs:
//!
//! 1. Apply confinement against the CLI-provided merged path + default OS
//!    read paths.
//! 2. Read/write line-delimited JSON over fd 3. Each inbound frame is an
//!    agent-side probe (e.g. `{"op":"self_test"}`); each outbound frame is
//!    the probe's reply. The parent orchestrator drives the conversation.
//!
//! The actual LLM-driven agent loop is PR-5 territory — PR-4 ships the
//! confinement-plus-bridge skeleton that PR-5 extends.

use std::path::PathBuf;
use std::process::ExitCode;

use cairn_workspace::sandbox::confinement::landlock::default_os_read_paths;
use cairn_workspace::sandbox::confinement::NamespacePolicy;
use cairn_workspace::sandbox::{ConfinementError, SandboxConfinement};

/// CLI flags recognized by the sandboxed-agent mode.
#[derive(Debug, Clone)]
pub struct SandboxedAgentArgs {
    pub workspace_id: String,
    pub merged_path: PathBuf,
    pub socket_fd: i32,
    /// If set, disable one or more confinement layers — dev affordance only;
    /// production ALWAYS has every layer enabled. Read from
    /// `CAIRN_SANDBOX_DISABLE_*` env vars inside `parse`.
    pub disable_landlock: bool,
    pub disable_seccomp: bool,
    /// When true, `confine()` skips `unshare(CLONE_NEWNS)` entirely. Used in
    /// integration tests on hosts where the kernel blocks unprivileged
    /// userns creation (Ubuntu 24.04+ with AppArmor) — lets us verify the
    /// wire protocol and non-namespace layers in isolation. Production MUST
    /// NOT set this.
    pub disable_namespace_unshare: bool,
}

/// Detect the sandboxed-agent mode from command-line args.
///
/// Returns `None` if `--sandboxed-agent` is not among the args — callers fall
/// through to the normal cairn-app boot path.
pub fn detect_and_parse(args: &[String]) -> Option<Result<SandboxedAgentArgs, String>> {
    if !args.iter().any(|a| a == "--sandboxed-agent") {
        return None;
    }
    Some(parse(args))
}

fn parse(args: &[String]) -> Result<SandboxedAgentArgs, String> {
    let mut workspace_id = None;
    let mut merged_path = None;
    let mut socket_fd = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--workspace-id" => {
                i += 1;
                if i >= args.len() {
                    return Err("--workspace-id requires a value".to_string());
                }
                workspace_id = Some(args[i].clone());
            }
            "--merged-path" => {
                i += 1;
                if i >= args.len() {
                    return Err("--merged-path requires a value".to_string());
                }
                merged_path = Some(PathBuf::from(&args[i]));
            }
            "--socket-fd" => {
                i += 1;
                if i >= args.len() {
                    return Err("--socket-fd requires a value".to_string());
                }
                socket_fd = Some(
                    args[i]
                        .parse::<i32>()
                        .map_err(|err| format!("--socket-fd `{}`: {err}", args[i]))?,
                );
            }
            _ => {}
        }
        i += 1;
    }

    Ok(SandboxedAgentArgs {
        workspace_id: workspace_id.ok_or("--sandboxed-agent requires --workspace-id")?,
        merged_path: merged_path.ok_or("--sandboxed-agent requires --merged-path")?,
        socket_fd: socket_fd.ok_or("--sandboxed-agent requires --socket-fd")?,
        disable_landlock: std::env::var_os("CAIRN_SANDBOX_DISABLE_LANDLOCK").is_some(),
        disable_seccomp: std::env::var_os("CAIRN_SANDBOX_DISABLE_SECCOMP").is_some(),
        disable_namespace_unshare: std::env::var_os("CAIRN_SANDBOX_DISABLE_UNSHARE").is_some(),
    })
}

/// Child-process entrypoint. Returns an `ExitCode` — the caller should
/// `std::process::exit()` with it.
pub fn run(args: SandboxedAgentArgs) -> ExitCode {
    eprintln!(
        "[sandboxed-agent] workspace_id={} merged={} socket_fd={}",
        args.workspace_id,
        args.merged_path.display(),
        args.socket_fd
    );

    // Step 1: apply confinement. `SandboxConfinement::confine` handles
    // unshare → close_range (keeping the tool-bridge fd) → Landlock → seccomp
    // in the correct order. Any failure is fatal — we never run the agent
    // loop unconfined.
    let confinement = SandboxConfinement {
        workspace_merged: args.merged_path.clone(),
        extra_read_paths: default_os_read_paths(),
        enable_landlock: !args.disable_landlock,
        enable_seccomp: !args.disable_seccomp,
        namespace_policy: if args.disable_namespace_unshare {
            NamespacePolicy::Skip
        } else {
            NamespacePolicy::MountOnly
        },
    };
    if let Err(err) = confinement.confine(Some(args.socket_fd)) {
        eprintln!("[sandboxed-agent] confinement failed: {err}");
        return exit_code_for(&err);
    }
    eprintln!(
        "[sandboxed-agent] confinement applied (fd {} preserved)",
        args.socket_fd
    );

    // Step 2: bridge loop. Read line-delimited JSON off the tool-bridge fd
    // and reply on the same fd.  This path does NOT use `FromRawFd` — we go
    // straight through `nix::sys::socket::{recv, send}` which take `RawFd`
    // directly; the `unsafe` ffi boundary stays inside nix, keeping this
    // crate inside the workspace-level `unsafe_code = "forbid"` lint.
    match run_bridge_loop(args.socket_fd) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("[sandboxed-agent] bridge loop error: {err}");
            ExitCode::from(2)
        }
    }
}

fn exit_code_for(err: &ConfinementError) -> ExitCode {
    match err {
        ConfinementError::ProbeFailed(_) => ExitCode::from(10),
        ConfinementError::LandlockPartial(_) => ExitCode::from(11),
        ConfinementError::LandlockRuleset(_) => ExitCode::from(12),
        ConfinementError::SeccompLoad(_) => ExitCode::from(13),
        ConfinementError::NamespaceUnshare(_) => ExitCode::from(14),
        ConfinementError::FdCloseFailed(_) => ExitCode::from(15),
        ConfinementError::UnsupportedPlatform(_) => ExitCode::from(16),
        ConfinementError::InvalidPath(_, _) => ExitCode::from(17),
    }
}

fn run_bridge_loop(socket_fd: i32) -> std::io::Result<()> {
    // We use nix::sys::socket::{recv, send} directly on the raw fd. Both take
    // RawFd which keeps the loop inside the crate's `unsafe_code = "forbid"`
    // lint (the unsafe boundary lives inside nix). The fd is non-CLOEXEC
    // because command-fds set it up for us on fd 3.
    use nix::sys::socket::MsgFlags;

    let mut pending = Vec::<u8>::with_capacity(512);
    let mut buf = [0u8; 4096];
    loop {
        // Drain any complete line from `pending` first.
        while let Some(pos) = pending.iter().position(|&b| b == b'\n') {
            let line_bytes: Vec<u8> = pending.drain(..=pos).collect();
            let line = String::from_utf8_lossy(&line_bytes[..line_bytes.len().saturating_sub(1)]);
            let reply = handle_frame(line.trim());
            let payload = format!("{reply}\n");
            send_all(socket_fd, payload.as_bytes())?;
        }

        let n = nix::sys::socket::recv(socket_fd, &mut buf, MsgFlags::empty())
            .map_err(|err| std::io::Error::other(format!("recv: {err}")))?;
        if n == 0 {
            return Ok(());
        }
        pending.extend_from_slice(&buf[..n]);
    }
}

fn send_all(fd: i32, mut data: &[u8]) -> std::io::Result<()> {
    use nix::sys::socket::MsgFlags;
    while !data.is_empty() {
        let written = nix::sys::socket::send(fd, data, MsgFlags::empty())
            .map_err(|err| std::io::Error::other(format!("send: {err}")))?;
        if written == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "socket closed",
            ));
        }
        data = &data[written..];
    }
    Ok(())
}

/// Handle a single JSON-RPC frame.
///
/// Canonical request format:
///   `{"op":"<op-name>"}` — no other fields today.
///
/// Canonical reply format:
///   `{"ok":true,"op":"<op-name>",...}` — `ok` means "request was
///   recognized and handled." Confinement-probe ops carry a separate
///   `denied` field (the security-relevant signal), so a probe that
///   OBSERVES successful denial is `{"ok":true,"denied":true,...}` and a
///   probe that observes an unexpectedly-allowed operation is
///   `{"ok":true,"denied":false,...}`. Callers route on `ok` and inspect
///   `denied` / other fields to decide what happened.
///
/// Parsing is hand-rolled (no serde) to keep the sandboxed binary tiny and
/// avoid pulling a JSON parser onto the confined code path.
fn handle_frame(line: &str) -> String {
    let Some(op) = extract_op_value(line) else {
        return format!(
            "{{\"ok\":false,\"error\":\"malformed frame\",\"raw\":{}}}",
            escape_for_json(line)
        );
    };

    match op.as_str() {
        "self_test" => format!(
            "{{\"ok\":true,\"op\":\"self_test\",\"msg\":\"confined\",\"pid\":{}}}",
            std::process::id()
        ),
        "probe_write_outside" => {
            // Try to open /tmp/<scratch> for write. Under Landlock this MUST
            // fail. Either way `ok` is true (we handled the request); the
            // security signal is `denied`.
            match std::fs::File::create("/tmp/cairn-sandbox-escape-probe") {
                Ok(_) => {
                    "{\"ok\":true,\"op\":\"probe_write_outside\",\"denied\":false}".to_string()
                }
                Err(err) => format!(
                    "{{\"ok\":true,\"op\":\"probe_write_outside\",\"denied\":true,\"errno\":\"{}\"}}",
                    err.kind()
                ),
            }
        }
        "probe_mount" => {
            // Try to mount tmpfs — under seccomp this MUST fail EPERM.
            #[cfg(target_os = "linux")]
            {
                use nix::mount::{mount, MsFlags};
                let r = mount(
                    Some("none"),
                    "/tmp",
                    Some("tmpfs"),
                    MsFlags::empty(),
                    None::<&str>,
                );
                format!(
                    "{{\"ok\":true,\"op\":\"probe_mount\",\"denied\":{},\"errno\":\"{:?}\"}}",
                    r.is_err(),
                    r.err()
                )
            }
            #[cfg(not(target_os = "linux"))]
            {
                "{\"ok\":true,\"op\":\"probe_mount\",\"denied\":false,\"errno\":\"non-linux\"}"
                    .to_string()
            }
        }
        _ => format!(
            "{{\"ok\":false,\"error\":\"unknown op\",\"raw\":{}}}",
            escape_for_json(line)
        ),
    }
}

/// Extract the `op` string value from a JSON frame. Hand-rolled exact-match
/// parser so we don't misroute `{"op":"not_self_test"}` because the frame
/// contains `"self_test"` elsewhere. Returns `None` when the frame doesn't
/// have a parseable `"op":"<value>"` pair.
fn extract_op_value(line: &str) -> Option<String> {
    let key = "\"op\"";
    let key_pos = line.find(key)?;
    let after_key = &line[key_pos + key.len()..];
    let after_colon = after_key.trim_start();
    let after_colon = after_colon.strip_prefix(':')?.trim_start();
    let after_quote = after_colon.strip_prefix('"')?;
    let end = after_quote.find('"')?;
    Some(after_quote[..end].to_string())
}

/// Minimal RFC 8259 string encoder. Takes a Rust string and returns a
/// double-quoted JSON string literal safe to embed in any JSON object.
/// Avoids pulling serde_json onto the confined code path — this binary must
/// stay small and dep-poor.
fn escape_for_json(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x08' => out.push_str("\\b"),
            '\x0c' => out.push_str("\\f"),
            ch if (ch as u32) < 0x20 => {
                use std::fmt::Write;
                let _ = write!(out, "\\u{:04x}", ch as u32);
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_returns_none_without_flag() {
        let args = vec![
            "cairn-app".to_string(),
            "--port".to_string(),
            "3000".to_string(),
        ];
        assert!(detect_and_parse(&args).is_none());
    }

    #[test]
    fn detect_parses_full_flag_set() {
        let args = vec![
            "cairn-app".to_string(),
            "--sandboxed-agent".to_string(),
            "--workspace-id".to_string(),
            "wkspc-xyz".to_string(),
            "--merged-path".to_string(),
            "/tmp/merged".to_string(),
            "--socket-fd".to_string(),
            "3".to_string(),
        ];
        let parsed = detect_and_parse(&args).unwrap().unwrap();
        assert_eq!(parsed.workspace_id, "wkspc-xyz");
        assert_eq!(parsed.merged_path, PathBuf::from("/tmp/merged"));
        assert_eq!(parsed.socket_fd, 3);
    }

    #[test]
    fn detect_returns_err_on_missing_required() {
        let args = vec![
            "cairn-app".to_string(),
            "--sandboxed-agent".to_string(),
            "--workspace-id".to_string(),
            "x".to_string(),
        ];
        let err = detect_and_parse(&args).unwrap().expect_err("must error");
        assert!(err.contains("--merged-path"), "unexpected error: {err}");
    }

    #[test]
    fn handle_self_test_frame() {
        let reply = handle_frame(r#"{"op":"self_test"}"#);
        assert!(reply.contains("\"ok\":true"));
        assert!(reply.contains("self_test"));
    }
}
