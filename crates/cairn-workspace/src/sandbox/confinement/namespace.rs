//! Mount namespace + inherited-fd hygiene.
//!
//! Two independent primitives live here:
//!
//! - [`unshare_mount_ns`] wraps `nix::sched::unshare(CLONE_NEWNS)`. The caller
//!   ends up in a fresh mount namespace whose mount table is a copy-on-write
//!   clone of the parent's. Any subsequent `mount(2)` call only modifies this
//!   private table.
//! - [`close_nonstandard_fds`] closes all file descriptors ≥ 3 (or ≥ 4 when a
//!   single keep-fd is supplied), preventing pre-opened descriptor leaks from
//!   bypassing Landlock (see the research guide §6.3).
//!
//! Both primitives are Linux-only. Non-Linux targets get a stub that returns
//! [`ConfinementError::UnsupportedPlatform`] — we never want a silent no-op in
//! a security-critical seam.
//!
//! The fd walk uses `/proc/self/fd` iteration + `nix::unistd::close` so we
//! stay inside the `unsafe_code = "forbid"` workspace lint. `close_range(2)`
//! would be faster but nix 0.29 does not expose it; the proc-fs walk is
//! O(n_open_fds) which is fine for the 3–10 fd regime we expect.

use super::ConfinementError;

#[cfg(target_os = "linux")]
use std::os::unix::io::RawFd;

/// `unshare(CLONE_NEWNS)` — make the caller's mount table private.
///
/// On Ubuntu 24.04+ with `kernel.apparmor_restrict_unprivileged_userns=1`
/// this fails with `EPERM`; the caller is expected to surface a named
/// primitive failure via the kernel probe (see `confinement/probe.rs`) rather
/// than attempt to silently continue.
#[cfg(target_os = "linux")]
pub fn unshare_mount_ns() -> Result<(), ConfinementError> {
    use nix::sched::{unshare, CloneFlags};
    unshare(CloneFlags::CLONE_NEWNS)
        .map_err(|err| ConfinementError::NamespaceUnshare(format!("CLONE_NEWNS: {err}")))
}

#[cfg(not(target_os = "linux"))]
pub fn unshare_mount_ns() -> Result<(), ConfinementError> {
    Err(ConfinementError::UnsupportedPlatform(
        "unshare(CLONE_NEWNS) requires Linux".to_string(),
    ))
}

/// `unshare(CLONE_NEWNS | CLONE_NEWNET)` — make the caller's mount *and* network
/// namespaces private. Used when the sandbox policy requests
/// [`crate::sandbox::NetworkPolicy::Isolated`].
#[cfg(target_os = "linux")]
pub fn unshare_mount_and_network() -> Result<(), ConfinementError> {
    use nix::sched::{unshare, CloneFlags};
    unshare(CloneFlags::CLONE_NEWNS | CloneFlags::CLONE_NEWNET).map_err(|err| {
        ConfinementError::NamespaceUnshare(format!("CLONE_NEWNS|CLONE_NEWNET: {err}"))
    })
}

#[cfg(not(target_os = "linux"))]
pub fn unshare_mount_and_network() -> Result<(), ConfinementError> {
    Err(ConfinementError::UnsupportedPlatform(
        "unshare(CLONE_NEWNS|CLONE_NEWNET) requires Linux".to_string(),
    ))
}

/// Close all file descriptors ≥ 3 except `keep_fd` (if `Some`).
///
/// Uses a `/proc/self/fd` walk + `nix::unistd::close`. `close_range(2)` would
/// be slightly faster but is not exposed by nix 0.29; at the 3–10 open-fd
/// regime we expect at sandbox entry this is fine.
///
/// stdin (0), stdout (1), stderr (2) are intentionally preserved — the sandboxed
/// agent needs them for its human-visible log stream. If `keep_fd` is `Some(3)`
/// (the typical tool-bridge socket), fd 3 also survives; everything else ≥ 3
/// is closed.
#[cfg(target_os = "linux")]
pub fn close_nonstandard_fds(keep_fd: Option<RawFd>) -> Result<(), ConfinementError> {
    if let Some(keep) = keep_fd {
        if keep < 3 {
            return Err(ConfinementError::FdCloseFailed(format!(
                "keep_fd must be >= 3 (got {keep})"
            )));
        }
    }

    let entries = std::fs::read_dir("/proc/self/fd")
        .map_err(|err| ConfinementError::FdCloseFailed(format!("read /proc/self/fd: {err}")))?;

    // Collect fd numbers first so that iterating + closing doesn't invalidate
    // the readdir cursor (readdir on /proc/self/fd includes its own fd).
    let mut candidates = Vec::with_capacity(32);
    for entry in entries.flatten() {
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Ok(fd_num): Result<i32, _> = name.parse() else {
            continue;
        };
        if fd_num < 3 {
            continue;
        }
        if let Some(keep) = keep_fd {
            if fd_num == keep {
                continue;
            }
        }
        candidates.push(fd_num);
    }

    for fd in candidates {
        // EBADF is expected if another close happened in the meantime (e.g.
        // the proc-fs dir we iterated was itself a transient fd that the
        // readdir already released). Fatal on any other errno.
        match nix::unistd::close(fd) {
            Ok(()) => {}
            Err(nix::errno::Errno::EBADF) => {}
            Err(err) => {
                return Err(ConfinementError::FdCloseFailed(format!(
                    "close({fd}): {err}"
                )))
            }
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn close_nonstandard_fds(
    _keep_fd: Option<std::os::unix::io::RawFd>,
) -> Result<(), ConfinementError> {
    Err(ConfinementError::UnsupportedPlatform(
        "close_nonstandard_fds requires Linux".to_string(),
    ))
}
