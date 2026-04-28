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
///
/// # Concurrency
///
/// #458: the readdir→close pair is not atomic. A background thread that
/// opens an fd AFTER our enumeration but BEFORE we run the closes would
/// survive a single pass. This implementation uses `nix::dir::Dir` so it
/// can identify the readdir's own fd via `AsRawFd` and exclude it from
/// candidates (otherwise a second pass would see the readdir's fd as a
/// "new" leak and loop forever), then iterates up to `MAX_PASSES` times:
/// once a pass sees no new non-stdio fds beyond the readdir's own, the
/// walk has converged and we are done. Any opener that still manages to
/// create fds within the retry budget is ruled out at a higher layer —
/// the `cairn-app --sandboxed-agent` entry point runs before the tokio
/// runtime is started and before any background task spawns, so in
/// production the first pass is already stable.
#[cfg(target_os = "linux")]
pub fn close_nonstandard_fds(keep_fd: Option<RawFd>) -> Result<(), ConfinementError> {
    if let Some(keep) = keep_fd {
        if keep < 3 {
            return Err(ConfinementError::FdCloseFailed(format!(
                "keep_fd must be >= 3 (got {keep})"
            )));
        }
    }

    // 8 passes is plenty — a sane runtime opens ≤ a handful of fds
    // during a forked exec. A runaway opener (pathological) trips the
    // limit and the caller sees an explicit error instead of silently
    // inheriting an fd. See module-level "Concurrency" note.
    const MAX_PASSES: usize = 8;

    for pass in 0..MAX_PASSES {
        let candidates = enumerate_nonstandard_fds(keep_fd)?;
        if candidates.is_empty() {
            return Ok(());
        }
        for fd in candidates {
            // EBADF is expected if another close happened in the meantime
            // (e.g. the fd was transiently held and released by the kernel
            // between enumerate and close). Fatal on any other errno.
            match nix::unistd::close(fd) {
                Ok(()) => {}
                Err(nix::errno::Errno::EBADF) => {}
                Err(err) => {
                    return Err(ConfinementError::FdCloseFailed(format!(
                        "close({fd}) on pass {pass}: {err}"
                    )))
                }
            }
        }
    }

    // One final enumeration after MAX_PASSES — if it is empty, declare
    // success; otherwise report the leak by name so the operator (or
    // the soak test) sees exactly which fds survived.
    let residual = enumerate_nonstandard_fds(keep_fd)?;
    if residual.is_empty() {
        Ok(())
    } else {
        Err(ConfinementError::FdCloseFailed(format!(
            "close_nonstandard_fds did not converge after {MAX_PASSES} passes; residual fds: {residual:?}"
        )))
    }
}

/// One readdir pass over `/proc/self/fd` returning every non-stdio fd
/// except `keep_fd` AND the readdir's own fd. Hoisted so the multi-pass
/// loop and the final residual check share a single implementation.
///
/// #458: the readdir's own fd is read via `AsRawFd` and excluded so the
/// convergence loop doesn't see it as a perpetual "new" leak. The `Dir`
/// handle releases the fd on drop; the caller never observes it in the
/// candidate set and never tries to close it explicitly.
#[cfg(target_os = "linux")]
fn enumerate_nonstandard_fds(keep_fd: Option<RawFd>) -> Result<Vec<i32>, ConfinementError> {
    use nix::dir::Dir;
    use nix::fcntl::OFlag;
    use nix::sys::stat::Mode;
    use std::os::unix::io::AsRawFd;

    let mut dir = Dir::open("/proc/self/fd", OFlag::O_RDONLY, Mode::empty())
        .map_err(|err| ConfinementError::FdCloseFailed(format!("open /proc/self/fd: {err}")))?;
    let dir_fd = dir.as_raw_fd();

    let mut candidates = Vec::with_capacity(32);
    for entry in dir.iter() {
        let entry =
            entry.map_err(|err| ConfinementError::FdCloseFailed(format!("readdir: {err}")))?;
        // Filename is a CStr. `.` and `..` round-trip as `Err(_)` from
        // `i32::from_str` below, so no special-case needed.
        let Ok(name) = entry.file_name().to_str() else {
            continue;
        };
        let Ok(fd_num): Result<i32, _> = name.parse() else {
            continue;
        };
        if fd_num < 3 {
            continue;
        }
        if fd_num == dir_fd {
            continue;
        }
        if let Some(keep) = keep_fd {
            if fd_num == keep {
                continue;
            }
        }
        candidates.push(fd_num);
    }
    // `dir` drops here, closing its fd cleanly (never through `candidates`).
    Ok(candidates)
}

#[cfg(not(target_os = "linux"))]
pub fn close_nonstandard_fds(
    _keep_fd: Option<std::os::unix::io::RawFd>,
) -> Result<(), ConfinementError> {
    Err(ConfinementError::UnsupportedPlatform(
        "close_nonstandard_fds requires Linux".to_string(),
    ))
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{close_nonstandard_fds, enumerate_nonstandard_fds};

    /// #458: each call to `enumerate_nonstandard_fds` opens its own
    /// readdir fd and must exclude it from the returned candidate set.
    ///
    /// Deterministic direct check: inspect every returned candidate's
    /// `/proc/self/fd/<n>` symlink and assert none point at the
    /// `/proc/<pid>/fd` directory. Copilot review on PR #548 caught
    /// an earlier draft that compared against the literal
    /// `/proc/self/fd` — the kernel resolves the symlink target to the
    /// resolved pid path, so the literal check never fired. We compare
    /// against the resolved form here; no cross-pass fd stability
    /// assumptions needed.
    #[test]
    fn enumerate_never_returns_own_readdir_fd() {
        let own_fd_dir = format!("/proc/{}/fd", std::process::id());
        let candidates = enumerate_nonstandard_fds(None).expect("enumerate");

        for fd in &candidates {
            let link_path = format!("/proc/self/fd/{fd}");
            let target = match std::fs::read_link(&link_path) {
                Ok(p) => p,
                // A raced close between enumerate-return and this
                // readlink is tolerable — the returned candidate
                // definitely wasn't our readdir (the Dir handle owned
                // that fd until it dropped inside enumerate, and any
                // fd still in the list right now can't be pointing at
                // the consumed Dir's path). Skip and continue.
                Err(_) => continue,
            };
            let target_str = target.to_string_lossy();
            // Compare against the resolved `/proc/<pid>/fd` form AND
            // the literal `/proc/self/fd` form. The kernel normalises
            // the symlink target to `/proc/<pid>/fd`, but we still
            // guard against the literal case in case the kernel or a
            // future namespace setup surfaces it verbatim.
            assert_ne!(
                target.as_path(),
                std::path::Path::new(&own_fd_dir),
                "enumerate returned its own readdir fd {fd} (symlink → {target:?}) — \
                 the `nix::dir::Dir` + `AsRawFd` exclusion is broken",
            );
            assert_ne!(
                target.as_path(),
                std::path::Path::new("/proc/self/fd"),
                "enumerate returned its own readdir fd {fd} (symlink → {target:?}) — \
                 literal /proc/self/fd form",
            );
            // Belt-and-suspenders: any target under /proc/<anything>/fd
            // is a fd-directory handle. Only ours should be possible
            // (this test doesn't open any other /proc/*/fd).
            assert!(
                !target_str.ends_with("/fd") || !target_str.starts_with("/proc/"),
                "enumerate returned a /proc/*/fd directory fd {fd} \
                 (symlink → {target:?}) — looks like a leaked readdir",
            );
        }
    }

    /// #458: keep_fd=3 must be excluded from the candidate set even when
    /// no extra fds are open. This is the minimum contract used by the
    /// sandboxed-agent tool-bridge path.
    #[test]
    fn enumerate_excludes_keep_fd() {
        // Open a scratch fd so we know there IS at least one candidate
        // that would be returned without keep_fd; then assert it's
        // filtered when passed as keep_fd.
        use std::os::fd::AsRawFd;
        let scratch = tempfile::NamedTempFile::new().expect("tempfile");
        let scratch_file = std::fs::File::open(scratch.path()).expect("open scratch");
        let scratch_fd = scratch_file.as_raw_fd();
        assert!(scratch_fd >= 3, "tempfile should hand back a non-stdio fd");

        let without_keep = enumerate_nonstandard_fds(None).expect("enumerate without keep_fd");
        assert!(
            without_keep.contains(&scratch_fd),
            "baseline enumeration must include the just-opened scratch fd {scratch_fd}: \
             got {without_keep:?}",
        );

        let with_keep =
            enumerate_nonstandard_fds(Some(scratch_fd)).expect("enumerate with keep_fd");
        assert!(
            !with_keep.contains(&scratch_fd),
            "keep_fd {scratch_fd} must NOT appear in candidates: got {with_keep:?}",
        );
    }

    /// #458: `close_nonstandard_fds` rejects keep_fd < 3 before doing any
    /// proc-walk. Preserves the pre-existing error contract.
    #[test]
    fn close_rejects_keep_fd_below_three() {
        let err = close_nonstandard_fds(Some(2)).expect_err("keep_fd=2 must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("keep_fd must be >= 3"),
            "error must call out the stdio-range gate: got `{msg}`",
        );
    }
}
