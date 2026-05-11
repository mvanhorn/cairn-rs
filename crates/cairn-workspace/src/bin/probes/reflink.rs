//! Probe 6: reflink (`FICLONE`) on `/tmp`.
//!
//! Informational-only on filesystems that deliberately lack reflink
//! support (ext4). `reflink-copy 0.1` is the cairn-workspace crate's
//! existing reflink frontend — we reuse it here so the probe measures
//! the exact same code path PR-4 will use.
//!
//! Expected outcomes:
//!
//! * **ext4** (this Graviton host): `reflink()` returns an io::Error with
//!   `raw_os_error() == Some(EOPNOTSUPP)`. Recorded as `InfoExpected` and
//!   the probe binary still exits 0. PR-4 must default to
//!   `reflink-copy::reflink_or_copy()` (auto byte-copy fallback) and
//!   emit `WorkspaceBackendDegraded { reason: "ext4-fallback-full-copy" }`
//!   on first use.
//! * **btrfs / XFS (reflink=1) / bcachefs**: reflink succeeds; the probe
//!   reports `PASS` with the fast-path confirmation so operators know
//!   PR-4 can take the O(inodes), near-zero-disk reflink path on this
//!   deployment.
//!
//! We prepare the probe as follows:
//!
//! 1. Create a 4 KiB source file inside a tempdir rooted at `/tmp` so
//!    the source + destination sit on the same filesystem (reflink
//!    requires identical FS).
//! 2. Call `reflink_copy::reflink(src, dst)` directly. This does NOT
//!    fall back — we want to see the raw FICLONE outcome.
//! 3. Interpret the error. Reflink is NOT a REQUIRED primitive — PR-4
//!    degrades to copy-fallback regardless of the cause — so every
//!    outcome below maps to `InfoExpected` (non-failing). The detail
//!    line carries the distinction:
//!    - EOPNOTSUPP → filesystem does not implement FICLONE (expected on
//!      ext4 / tmpfs).
//!    - EXDEV → probe bug (src and dst on different FS).
//!    - EPERM / EACCES → security-policy denial (AppArmor, SELinux,
//!      LSM). Called out loudly in the detail because the same policy
//!      likely denies other operations too.
//!    - other → unusual errno; detail line surfaces it for
//!      investigation.

use std::fs;
use std::io;

use tempfile::TempDir;

use crate::ProbeResult;

const NUMBER: u8 = 6;
const NAME: &str = "reflink (FICLONE) on /tmp";

/// A 4 KiB source body — one filesystem block on ext4 / btrfs / XFS.
const PROBE_BODY: &[u8] = &[0xA5u8; 4096];

pub fn probe() -> ProbeResult {
    // Rooted at /tmp so any reflink-capable tmpfs / XFS reflink config
    // is measured where cairn's workspace snapshots will actually land.
    // `TempDir::new_in` pins the parent, guaranteeing src and dst share
    // a filesystem (a reflink prerequisite).
    let scratch = match TempDir::new_in("/tmp") {
        Ok(t) => t,
        Err(err) => {
            return ProbeResult::fail(
                NUMBER,
                NAME,
                false,
                None,
                format!("failed to create tempdir under /tmp: {err}"),
            );
        }
    };

    let src = scratch.path().join("src.bin");
    let dst = scratch.path().join("dst.bin");
    if let Err(err) = fs::write(&src, PROBE_BODY) {
        return ProbeResult::fail(
            NUMBER,
            NAME,
            false,
            None,
            format!("failed to seed source file at {}: {err}", src.display()),
        );
    }

    let fs_label = detect_fs_label("/tmp");

    match reflink_copy::reflink(&src, &dst) {
        Ok(()) => {
            // Unexpected on ext4 — announce loudly so PR-4 can enable the
            // fast path on this host.
            ProbeResult::pass(
                NUMBER,
                NAME,
                false,
                format!(
                    "reflink({}) succeeded on /tmp ({fs_label}) — PR-4 can default to the \
                     reflink fast path (`reflink_copy::reflink`) on this deployment. \
                     Near-zero disk per snapshot.",
                    src.display()
                ),
            )
        }
        Err(err) => classify_reflink_error(err, &fs_label),
    }
}

fn classify_reflink_error(err: io::Error, fs_label: &str) -> ProbeResult {
    let errno = err.raw_os_error();
    let label = errno
        .map(describe_errno)
        .unwrap_or_else(|| format!("{err:?}"));

    // EOPNOTSUPP == 95 on Linux — this is the signature of an ext4 / tmpfs
    // filesystem that doesn't implement FICLONE. That's the expected
    // outcome on the production host today; record as InfoExpected so
    // the probe still exits 0.
    if errno == Some(nix::libc::EOPNOTSUPP) {
        return ProbeResult::info_expected(
            NUMBER,
            NAME,
            Some(label),
            format!(
                "FICLONE returned EOPNOTSUPP on /tmp ({fs_label}) — expected on ext4 / tmpfs. \
                 PR-4 MUST use `reflink_copy::reflink_or_copy` (automatic copy fallback) and \
                 emit `WorkspaceBackendDegraded {{ reason: \"ext4-fallback-full-copy\" }}` on \
                 first use. Operators who want the fast path provision btrfs or \
                 XFS-with-reflink EBS at the workspace root."
            ),
        );
    }

    // EXDEV (cross-device) shouldn't happen — we seeded src and dst in the
    // same tempdir. If it does, that's a probe bug, not a host issue. Still
    // report info-level.
    if errno == Some(nix::libc::EXDEV) {
        return ProbeResult::info_expected(
            NUMBER,
            NAME,
            Some(label),
            format!(
                "FICLONE returned EXDEV on /tmp ({fs_label}) — source and destination unexpectedly \
                 on different filesystems. This is a probe bug, not a host issue."
            ),
        );
    }

    // EPERM / EACCES surface a security-policy denial (AppArmor,
    // SELinux, etc.). Reflink is NOT a REQUIRED primitive — PR-4
    // degrades to copy-fallback regardless of the cause — but a policy
    // denial is a different operational story from "filesystem does
    // not implement it." Surface it loudly in the detail so operators
    // see the distinction.
    if errno == Some(nix::libc::EPERM) || errno == Some(nix::libc::EACCES) {
        return ProbeResult::info_expected(
            NUMBER,
            NAME,
            Some(label),
            format!(
                "FICLONE on /tmp ({fs_label}) failed with {err} — this is a security-policy denial \
                 (AppArmor / SELinux / LSM), not a filesystem limitation. PR-4's copy-fallback \
                 path still works (reflink is not REQUIRED), but the underlying policy likely \
                 denies other operations too and is worth an investigation."
            ),
        );
    }

    // Anything else (EINVAL, ENOSYS, ENOTSUP, etc.) — unusual but the
    // probe still falls through to the info-only classification because
    // reflink is not a REQUIRED primitive.
    ProbeResult::info_expected(
        NUMBER,
        NAME,
        Some(label),
        format!(
            "FICLONE on /tmp ({fs_label}) failed with {err}. Not the expected ext4 EOPNOTSUPP \
             and not a clear policy denial. PR-4's copy-fallback path still works; investigate \
             the errno if reflink is expected to be available on this deployment."
        ),
    )
}

/// Translate common errnos to a short label plus their numeric value.
fn describe_errno(errno: i32) -> String {
    let name = match errno {
        e if e == nix::libc::EOPNOTSUPP => "EOPNOTSUPP",
        e if e == nix::libc::EXDEV => "EXDEV",
        e if e == nix::libc::EPERM => "EPERM",
        e if e == nix::libc::EACCES => "EACCES",
        e if e == nix::libc::EINVAL => "EINVAL",
        e if e == nix::libc::ENOTSUP => "ENOTSUP",
        e if e == nix::libc::ENOSYS => "ENOSYS",
        _ => "errno",
    };
    format!("{name} ({errno})")
}

/// Best-effort filesystem label for human consumption. Reads
/// `/proc/self/mountinfo` to find the mount covering `path` and returns
/// the reported FS type (e.g. `ext4`, `btrfs`, `xfs`). Returns
/// `"unknown"` on any parse failure — this is labelling, not a hard
/// requirement.
fn detect_fs_label(path: &str) -> String {
    let mountinfo = match fs::read_to_string("/proc/self/mountinfo") {
        Ok(s) => s,
        Err(_) => return "unknown".to_string(),
    };

    // mountinfo fields (space-separated): id parentId major:minor root
    // mountpoint options ... `-` fstype source options
    // The mountpoint is the 5th whitespace-separated field; fstype is
    // after the `-` separator.
    let mut best: Option<(&str, &str)> = None;
    for line in mountinfo.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let sep_idx = match fields.iter().position(|f| *f == "-") {
            Some(i) => i,
            None => continue,
        };
        if fields.len() < sep_idx + 2 {
            continue;
        }
        let mountpoint = match fields.get(4) {
            Some(m) => *m,
            None => continue,
        };
        let fstype = fields[sep_idx + 1];
        // Pick the longest mountpoint prefix that matches — handles
        // nested mounts (e.g. `/` and `/tmp` where `/tmp` is a separate
        // tmpfs or XFS volume).
        if path.starts_with(mountpoint)
            && best
                .map(|(prev, _)| mountpoint.len() > prev.len())
                .unwrap_or(true)
        {
            best = Some((mountpoint, fstype));
        }
    }
    best.map(|(_, ft)| ft.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::{describe_errno, detect_fs_label};

    #[test]
    fn describe_errno_names_common_codes() {
        assert!(describe_errno(nix::libc::EOPNOTSUPP).starts_with("EOPNOTSUPP"));
        assert!(describe_errno(nix::libc::EPERM).starts_with("EPERM"));
        assert!(describe_errno(nix::libc::EACCES).starts_with("EACCES"));
    }

    #[test]
    fn describe_errno_unknown_falls_back_to_errno_label() {
        let s = describe_errno(9999);
        assert!(s.starts_with("errno"));
        assert!(s.contains("9999"));
    }

    #[test]
    fn detect_fs_label_returns_string_for_root() {
        // We cannot assert a specific FS type (varies by host), but the
        // helper must always return a string — the empty string is also
        // valid for "unknown".
        let label = detect_fs_label("/");
        assert!(!label.is_empty());
    }
}
