//! Probe 2: unprivileged mount namespace unshare.
//!
//! The cairn sandbox runtime scopes every root-Run's mount table to its own
//! namespace. There are two ways a process can legitimately enter a fresh
//! mount namespace:
//!
//! * `CLONE_NEWNS` alone — requires `CAP_SYS_ADMIN`. This is the shape used
//!   by a privileged cairn-app (root user, systemd service, or a binary
//!   with ambient `CAP_SYS_ADMIN`).
//! * `CLONE_NEWUSER | CLONE_NEWNS` — unprivileged, subject to kernel and
//!   AppArmor policy. Ubuntu 24.04+ enables `kernel.apparmor_restrict_
//!   unprivileged_userns=1` by default, blocking unconfined binaries.
//!
//! The probe attempts both paths in re-exec'd children, in order of
//! preference (user-NS first — that's the portable unprivileged path),
//! and reports which one worked. At least ONE path must PASS for the
//! primitive to be considered satisfied.
//!
//! PR-4 consumes this information to pick the right mount-ns strategy at
//! cairn-app start: user-NS nesting if available, capability-based
//! otherwise, loud error if neither.

use std::ffi::OsStr;
use std::fs;

use anyhow::{Context, Result};
use nix::mount::{mount, MsFlags};
use nix::sched::{unshare, CloneFlags};
use nix::unistd::{getgid, getuid};

use crate::{spawn_child_probe, ChildOutcome, ProbeResult};

const NUMBER: u8 = 2;
const NAME: &str = "mount namespace unshare";
const REQUIRED: bool = true;

pub fn probe() -> ProbeResult {
    // Strategy 1: CLONE_NEWUSER | CLONE_NEWNS — the unprivileged path. This
    // is what an unprivileged cairn-app would use.
    let user_plus_mount = run_child("mount_namespace_user_plus_mount");
    if let Some(pass) = interpret_success(
        &user_plus_mount,
        "CLONE_NEWUSER | CLONE_NEWNS (unprivileged user-NS nesting)",
    ) {
        return pass;
    }

    // Strategy 2: CLONE_NEWNS alone — the CAP_SYS_ADMIN path. This is what
    // a privileged cairn-app (systemd-managed, ambient caps) would use.
    let newns_only = run_child("mount_namespace_newns_only");
    if let Some(pass) = interpret_success(&newns_only, "CLONE_NEWNS alone (requires CAP_SYS_ADMIN)")
    {
        return pass;
    }

    // Both paths failed → REQUIRED failure. Surface both attempts' stderr
    // in the detail so operators can see why, and call out AppArmor
    // explicitly when the `apparmor_restrict_unprivileged_userns` sysctl
    // is enabled — that's the single most common root cause on Ubuntu
    // 24.04+.
    let apparmor_hint = detect_apparmor_userns_restriction();
    ProbeResult::fail(
        NUMBER,
        NAME,
        REQUIRED,
        None,
        format!(
            "Neither unshare strategy succeeded.\n\
             - CLONE_NEWUSER|CLONE_NEWNS: {}\n\
             - CLONE_NEWNS only: {}\n\
             {apparmor_hint}\
             Actionable: for PR-4 on Ubuntu 24.04+, cairn must (a) ship an AppArmor profile that allows \
             `userns_create` + CAP_SYS_ADMIN within the child userns for the cairn-app binary, or \
             (b) run cairn-app under a systemd unit with `AmbientCapabilities=CAP_SYS_ADMIN`, or \
             (c) disable the sysctl with `sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0` \
             (weakens host security — use with care).",
            summarize(&user_plus_mount),
            summarize(&newns_only),
        ),
    )
}

/// Best-effort detection: if `/proc/sys/kernel/apparmor_restrict_
/// unprivileged_userns` exists and contains `1`, we almost certainly
/// just hit the Ubuntu 24.04+ AppArmor block. Return a pointed
/// diagnostic line; return empty string when the sysctl is absent or
/// disabled.
fn detect_apparmor_userns_restriction() -> String {
    let path = "/proc/sys/kernel/apparmor_restrict_unprivileged_userns";
    match std::fs::read_to_string(path) {
        Ok(contents) if contents.trim() == "1" => format!(
            "Root cause (likely): {path} = 1 — AppArmor transitions unconfined binaries into the \
             `unprivileged_userns` profile on userns creation and denies CAP_SYS_ADMIN inside, \
             which blocks every post-unshare mount operation.\n"
        ),
        _ => String::new(),
    }
}

fn run_child(name: &'static str) -> Result<ChildOutcome, String> {
    spawn_child_probe(name).map_err(|e| format!("spawn failed: {e:#}"))
}

fn interpret_success(outcome: &Result<ChildOutcome, String>, label: &str) -> Option<ProbeResult> {
    match outcome {
        Ok(o) if o.is_clean_zero() => Some(ProbeResult::pass(
            NUMBER,
            NAME,
            REQUIRED,
            format!("{label}: child process succeeded"),
        )),
        _ => None,
    }
}

fn summarize(outcome: &Result<ChildOutcome, String>) -> String {
    match outcome {
        Ok(o) if o.is_clean_zero() => "ok".to_string(),
        Ok(o) => {
            let status = o
                .signal
                .map(|s| format!("signal {s}"))
                .or_else(|| o.status_code.map(|c| format!("exit {c}")))
                .unwrap_or_else(|| "unknown".to_string());
            let stderr = if o.stderr.is_empty() {
                "(no stderr)"
            } else {
                o.stderr.as_str()
            };
            format!("{status}: {stderr}")
        }
        Err(e) => e.clone(),
    }
}

/// Child-scope body router: the `run_child` dispatcher in `main` routes
/// `--child mount_namespace` to `child_body` (kept for the happy-path
/// legacy callsite). The probe above uses two more specific strategies
/// via `spawn_child_probe`, routed through the dispatcher which calls
/// into `child_body_user_plus_mount` / `child_body_newns_only`.
pub fn child_body() -> Result<()> {
    // Default child handler (legacy): try the unprivileged user-NS path.
    child_body_user_plus_mount()
}

/// Strategy 1 body — unprivileged: `CLONE_NEWUSER | CLONE_NEWNS`.
///
/// After entering the user namespace we MUST write `/proc/self/uid_map`
/// (and disable setgroups + write gid_map) before any mount syscall —
/// otherwise the child is nobody:nobody inside the new userns, which
/// means `mount(/, MS_PRIVATE)` fails with EACCES. This is exactly the
/// sequence cairn-app will perform in PR-4.
pub fn child_body_user_plus_mount() -> Result<()> {
    let original_uid = getuid().as_raw();
    let original_gid = getgid().as_raw();

    unshare(CloneFlags::CLONE_NEWUSER | CloneFlags::CLONE_NEWNS).context(
        "unshare(CLONE_NEWUSER|CLONE_NEWNS) failed — on Ubuntu 24.04+ this is typically \
         blocked by kernel.apparmor_restrict_unprivileged_userns=1",
    )?;

    write_id_maps(original_uid, original_gid)?;
    mount_private_root()
}

/// Strategy 2 body — privileged: `CLONE_NEWNS` alone.
pub fn child_body_newns_only() -> Result<()> {
    unshare(CloneFlags::CLONE_NEWNS).context(
        "unshare(CLONE_NEWNS) failed — requires CAP_SYS_ADMIN or an existing user namespace",
    )?;
    mount_private_root()
}

fn mount_private_root() -> Result<()> {
    mount(
        None::<&OsStr>,
        "/",
        None::<&OsStr>,
        MsFlags::MS_REC | MsFlags::MS_PRIVATE,
        None::<&OsStr>,
    )
    .context("mount(/, MS_PRIVATE | MS_REC) after unshare")?;
    Ok(())
}

/// Map the child's effective UID/GID back to its original host identity
/// inside the fresh user namespace. Without these writes the child is
/// `nobody:nobody` in the new userns and any privileged-seeming operation
/// (including `mount(/, MS_PRIVATE)`) returns EACCES.
///
/// The ordering here is kernel-mandated: `setgroups` MUST be disabled
/// (written as `"deny"`) BEFORE `gid_map` is written, and the single-line
/// `uid_map`/`gid_map` format is the only one that works for unprivileged
/// single-mapping setups.
pub(crate) fn write_id_maps(original_uid: u32, original_gid: u32) -> Result<()> {
    fs::write("/proc/self/setgroups", b"deny")
        .context("write /proc/self/setgroups=deny before gid_map")?;
    fs::write(
        "/proc/self/uid_map",
        format!("0 {original_uid} 1\n").as_bytes(),
    )
    .context("write /proc/self/uid_map with single-line 0 <uid> 1 mapping")?;
    fs::write(
        "/proc/self/gid_map",
        format!("0 {original_gid} 1\n").as_bytes(),
    )
    .context("write /proc/self/gid_map with single-line 0 <gid> 1 mapping")?;
    Ok(())
}
