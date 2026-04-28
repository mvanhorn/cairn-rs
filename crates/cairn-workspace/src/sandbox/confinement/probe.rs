//! Kernel + filesystem primitive probe.
//!
//! Two modes of use:
//!
//! 1. `load_from_markdown` — read the canonical findings doc published by the
//!    separate `f65_kernel_probe` spike binary (`docs/design/
//!    f65-kernel-probe-findings.md`). This is the fast-path used at cairn-app
//!    boot on the production host where the probe has already run.
//! 2. `run_live_probe` — when no findings file is available (dev box, CI
//!    runner), exercise each primitive in-process and synthesize findings.
//!    This is slower (~100 ms) but avoids the chicken-and-egg "must ship probe
//!    before running cairn-app" problem.
//!
//! Either way, the returned [`ProbeFindings`] lets
//! [`ProbeFindings::assert_required`] gate cairn-app boot: if any REQUIRED
//! primitive is FAIL the app MUST refuse to start (per locked decision Q8).

use std::fs;
use std::path::Path;
use std::time::SystemTime;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Status {
    Pass,
    Fail(String),
    Unknown,
}

impl Status {
    pub fn is_pass(&self) -> bool {
        matches!(self, Self::Pass)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReflinkStatus {
    Supported,
    Eopnotsupp,
    Unknown,
}

#[derive(Clone, Debug)]
pub struct ProbeFindings {
    pub kernel_version: String,
    pub overlayfs_unprivileged: Status,
    pub landlock_v1_fully_enforced: Status,
    pub seccomp_bpf: Status,
    pub mount_namespace_unshare: Status,
    pub reflink_ioctl: ReflinkStatus,
    pub probed_at: SystemTime,
    pub source: ProbeSource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProbeSource {
    /// Findings were read from the canonical markdown file.
    FindingsDoc,
    /// Findings were synthesized by running primitives in-process.
    LiveProbe,
}

/// Errors raised by [`ProbeFindings::load_from_markdown`] and
/// [`ProbeFindings::assert_required`].
#[derive(Debug)]
pub enum ProbeError {
    FindingsFileMissing(String),
    FindingsFileMalformed(String),
    PrimitiveFailed {
        primitive: &'static str,
        detail: String,
        suggested_fix: String,
    },
}

impl std::fmt::Display for ProbeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FindingsFileMissing(msg) => write!(f, "kernel probe findings missing: {msg}"),
            Self::FindingsFileMalformed(msg) => {
                write!(f, "kernel probe findings malformed: {msg}")
            }
            Self::PrimitiveFailed {
                primitive,
                detail,
                suggested_fix,
            } => write!(
                f,
                "kernel primitive `{primitive}` failed: {detail}\nSuggested fix: {suggested_fix}"
            ),
        }
    }
}

impl std::error::Error for ProbeError {}

impl ProbeFindings {
    /// Unknown baseline — useful for tests that construct synthesized results.
    pub fn unknown() -> Self {
        Self {
            kernel_version: "unknown".to_string(),
            overlayfs_unprivileged: Status::Unknown,
            landlock_v1_fully_enforced: Status::Unknown,
            seccomp_bpf: Status::Unknown,
            mount_namespace_unshare: Status::Unknown,
            reflink_ioctl: ReflinkStatus::Unknown,
            probed_at: SystemTime::now(),
            source: ProbeSource::LiveProbe,
        }
    }

    /// Parse `docs/design/f65-kernel-probe-findings.md` (or any file in the
    /// same format) into a [`ProbeFindings`].
    ///
    /// Looks for markdown summary rows such as
    /// `| 2 | mount namespace unshare | PASS | ... |`.
    pub fn load_from_markdown(path: &Path) -> Result<Self, ProbeError> {
        let contents = fs::read_to_string(path).map_err(|err| {
            ProbeError::FindingsFileMissing(format!("read {}: {err}", path.display()))
        })?;
        Self::parse_markdown(&contents)
    }

    /// Extract findings from a markdown string. Exposed for unit testing.
    pub fn parse_markdown(contents: &str) -> Result<Self, ProbeError> {
        let mut findings = Self::unknown();
        findings.source = ProbeSource::FindingsDoc;

        for line in contents.lines() {
            // Kernel version header: "**Host:** `Linux ... 6.17.0 ...`"
            if let Some(host) = line.strip_prefix("**Host:**") {
                // Pull kernel version from the backtick'd portion.
                if let Some(start) = host.find('`') {
                    if let Some(end) = host[start + 1..].find('`') {
                        let host_line = &host[start + 1..start + 1 + end];
                        // "Linux <hostname> <kernel> ..."
                        if let Some(kernel) = host_line.split_whitespace().nth(2) {
                            findings.kernel_version = kernel.to_string();
                        }
                    }
                }
                continue;
            }

            // Table rows: "| N | primitive | RESULT | detail |"
            let Some(row) = extract_table_row(line) else {
                continue;
            };
            // Copilot caught this: row indexing without a bounds check could
            // panic on a malformed findings doc and turn a diagnostic into a
            // crash. Accept only rows with the expected 3+ cells (number,
            // primitive, result); shorter rows are silently skipped (they're
            // not the summary rows we care about).
            if row.len() < 3 {
                continue;
            }
            let primitive = row[1].to_lowercase();
            let result = &row[2];

            if primitive.contains("linux kernel") {
                // nothing to record beyond parsed kernel_version
            } else if primitive.contains("mount namespace unshare") {
                findings.mount_namespace_unshare = parse_status(result);
            } else if primitive.contains("overlayfs") {
                findings.overlayfs_unprivileged = parse_status(result);
            } else if primitive.contains("landlock") {
                findings.landlock_v1_fully_enforced = parse_status(result);
            } else if primitive.contains("seccomp") {
                findings.seccomp_bpf = parse_status(result);
            } else if primitive.contains("reflink") {
                findings.reflink_ioctl = parse_reflink(result);
            }
        }

        Ok(findings)
    }

    /// Bail loudly if any REQUIRED primitive reports FAIL.
    ///
    /// REQUIRED = `mount_namespace_unshare`, `overlayfs_unprivileged`,
    /// `landlock_v1_fully_enforced`, `seccomp_bpf`. Reflink is advisory only
    /// (ext4 hosts degrade to byte-copy).
    pub fn assert_required(&self) -> Result<(), ProbeError> {
        match &self.mount_namespace_unshare {
            Status::Fail(detail) => {
                return Err(ProbeError::PrimitiveFailed {
                    primitive: "mount_namespace_unshare",
                    detail: detail.clone(),
                    suggested_fix: mount_ns_fix(),
                });
            }
            Status::Unknown => {
                return Err(ProbeError::PrimitiveFailed {
                    primitive: "mount_namespace_unshare",
                    detail: "primitive was not probed; findings source could not confirm support"
                        .to_string(),
                    suggested_fix: mount_ns_fix(),
                });
            }
            Status::Pass => {}
        }
        match &self.overlayfs_unprivileged {
            Status::Fail(detail) => {
                return Err(ProbeError::PrimitiveFailed {
                    primitive: "overlayfs_unprivileged",
                    detail: detail.clone(),
                    suggested_fix: overlayfs_fix(),
                });
            }
            Status::Unknown => {
                return Err(ProbeError::PrimitiveFailed {
                    primitive: "overlayfs_unprivileged",
                    detail: "primitive was not probed".to_string(),
                    suggested_fix: overlayfs_fix(),
                });
            }
            Status::Pass => {}
        }
        match &self.landlock_v1_fully_enforced {
            Status::Fail(detail) => {
                return Err(ProbeError::PrimitiveFailed {
                    primitive: "landlock_v1_fully_enforced",
                    detail: detail.clone(),
                    suggested_fix: landlock_fix(),
                });
            }
            Status::Unknown => {
                return Err(ProbeError::PrimitiveFailed {
                    primitive: "landlock_v1_fully_enforced",
                    detail: "primitive was not probed".to_string(),
                    suggested_fix: landlock_fix(),
                });
            }
            Status::Pass => {}
        }
        match &self.seccomp_bpf {
            Status::Fail(detail) => {
                return Err(ProbeError::PrimitiveFailed {
                    primitive: "seccomp_bpf",
                    detail: detail.clone(),
                    suggested_fix: seccomp_fix(),
                });
            }
            Status::Unknown => {
                return Err(ProbeError::PrimitiveFailed {
                    primitive: "seccomp_bpf",
                    detail: "primitive was not probed".to_string(),
                    suggested_fix: seccomp_fix(),
                });
            }
            Status::Pass => {}
        }
        Ok(())
    }

    pub fn reflink_available(&self) -> bool {
        matches!(self.reflink_ioctl, ReflinkStatus::Supported)
    }
}

fn extract_table_row(line: &str) -> Option<Vec<String>> {
    let trimmed = line.trim();
    if !trimmed.starts_with('|') || !trimmed.ends_with('|') {
        return None;
    }
    // Skip the markdown separator row `| --- | --- | ... |`.
    if trimmed.contains("---") && !trimmed.contains("PASS") && !trimmed.contains("FAIL") {
        return None;
    }
    let cells: Vec<String> = trimmed
        .trim_matches('|')
        .split('|')
        .map(|c| c.trim().to_string())
        .collect();
    // Header row starts with `#` in cell 0; summary rows start with a digit.
    if cells.is_empty() {
        return None;
    }
    if cells[0].chars().next().is_some_and(|c| c.is_ascii_digit()) {
        Some(cells)
    } else {
        None
    }
}

fn parse_status(cell: &str) -> Status {
    let upper = cell.to_ascii_uppercase();
    if upper.starts_with("PASS") {
        Status::Pass
    } else if upper.contains("FAIL") {
        Status::Fail(cell.to_string())
    } else {
        Status::Unknown
    }
}

fn parse_reflink(cell: &str) -> ReflinkStatus {
    let upper = cell.to_ascii_uppercase();
    if upper.starts_with("PASS") {
        ReflinkStatus::Supported
    } else if upper.contains("EOPNOTSUPP") || upper.contains("FAIL") {
        ReflinkStatus::Eopnotsupp
    } else {
        ReflinkStatus::Unknown
    }
}

fn mount_ns_fix() -> String {
    "ensure the kernel is Linux ≥ 5.13 and unprivileged user namespaces are enabled. On \
     Ubuntu 24.04+ run `sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0`, \
     or ship an AppArmor profile that grants `userns_create` + CAP_SYS_ADMIN to cairn-app, \
     or run cairn-app under `systemd` with `AmbientCapabilities=CAP_SYS_ADMIN`."
        .to_string()
}

fn overlayfs_fix() -> String {
    "overlayfs mount requires an unprivileged user namespace or CAP_SYS_ADMIN. See the \
     mount_namespace_unshare remediation; the two primitives fail for the same AppArmor reason \
     on Ubuntu 24.04+."
        .to_string()
}

fn landlock_fix() -> String {
    "Landlock requires Linux ≥ 5.13 with `CONFIG_SECURITY_LANDLOCK=y`. Most distros ship it; \
     enable it at boot with `lsm=landlock,...` if absent."
        .to_string()
}

fn seccomp_fix() -> String {
    "seccomp-BPF requires Linux ≥ 3.17 with `CONFIG_SECCOMP_FILTER=y`. Any supported distro has it."
        .to_string()
}

/// Synthesize [`ProbeFindings`] via lightweight in-process heuristics.
/// Intended for dev / CI envs where the canonical findings doc is absent.
///
/// This is a **heuristic** probe, not a live exercise of each primitive:
///
/// - `mount_namespace_unshare` reads
///   `/proc/sys/kernel/apparmor_restrict_unprivileged_userns` and the
///   presence of `/proc/self/ns/mnt`. It does NOT call `unshare(2)`, because
///   unshare from the test runner would pollute the whole process's mount
///   table for subsequent tests. The canonical spike binary
///   (`f65_kernel_probe`) does fork a child and perform the real unshare;
///   that output lands in `docs/design/f65-kernel-probe-findings.md` and
///   should be preferred via `ProbeFindings::load_from_markdown`.
/// - `overlayfs_unprivileged` checks `/proc/filesystems` for the `overlay`
///   entry (module loaded).
/// - `landlock_v1_fully_enforced` builds a ruleset but does not call
///   `restrict_self`.
/// - `seccomp_bpf` constructs a filter but does not load it.
/// - `reflink_ioctl` actually calls `FICLONE` on a tiny scratch file in
///   `/tmp` (this is a no-op unless it succeeds, so it's safe).
///
/// Call once at boot and cache the result. Production boots should prefer
/// the canonical findings doc so the forking live probe's observations are
/// used instead of these heuristics.
#[cfg(target_os = "linux")]
pub fn run_live_probe() -> ProbeFindings {
    let kernel_version = fs::read_to_string("/proc/sys/kernel/osrelease")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    ProbeFindings {
        kernel_version,
        overlayfs_unprivileged: live_probe_overlayfs(),
        landlock_v1_fully_enforced: live_probe_landlock(),
        seccomp_bpf: live_probe_seccomp(),
        mount_namespace_unshare: live_probe_mount_ns(),
        reflink_ioctl: live_probe_reflink(),
        probed_at: SystemTime::now(),
        source: ProbeSource::LiveProbe,
    }
}

#[cfg(not(target_os = "linux"))]
pub fn run_live_probe() -> ProbeFindings {
    let mut findings = ProbeFindings::unknown();
    findings.source = ProbeSource::LiveProbe;
    findings.kernel_version = "non-linux".to_string();
    findings.mount_namespace_unshare = Status::Fail("not linux".to_string());
    findings.overlayfs_unprivileged = Status::Fail("not linux".to_string());
    findings.landlock_v1_fully_enforced = Status::Fail("not linux".to_string());
    findings.seccomp_bpf = Status::Fail("not linux".to_string());
    findings
}

#[cfg(target_os = "linux")]
fn live_probe_mount_ns() -> Status {
    // We can't safely unshare from the main test runner (it would pollute the
    // whole process). Inspect the kernel's /proc signalling instead.
    let proc_ns = std::path::Path::new("/proc/self/ns/mnt");
    if !proc_ns.exists() {
        return Status::Fail("/proc/self/ns/mnt missing".to_string());
    }
    // Check the AppArmor gate that trips on Ubuntu 24.04+.
    if let Ok(s) = fs::read_to_string("/proc/sys/kernel/apparmor_restrict_unprivileged_userns") {
        if s.trim() == "1" {
            return Status::Fail(
                "kernel.apparmor_restrict_unprivileged_userns=1 — unprivileged userns blocked"
                    .to_string(),
            );
        }
    }
    Status::Pass
}

#[cfg(target_os = "linux")]
fn live_probe_overlayfs() -> Status {
    // Presence of "overlay" in /proc/filesystems tells us the module is loaded.
    match fs::read_to_string("/proc/filesystems") {
        Ok(s) if s.contains("overlay") => Status::Pass,
        Ok(_) => Status::Fail("overlayfs module not registered in /proc/filesystems".to_string()),
        Err(err) => Status::Fail(format!("read /proc/filesystems: {err}")),
    }
}

#[cfg(target_os = "linux")]
fn live_probe_landlock() -> Status {
    // Use the landlock crate's ABI probe — does not mutate the process.
    use landlock::{Access, AccessFs, Ruleset, RulesetAttr, ABI};
    // Attempt to build a trivial ruleset; if construction succeeds the kernel
    // supports at least ABI V1.
    match Ruleset::default().handle_access(AccessFs::from_all(ABI::V1)) {
        Ok(builder) => match builder.create() {
            Ok(_) => {
                // Just check it COULD be created; don't actually restrict_self
                // since that'd affect the rest of the process.
                Status::Pass
            }
            Err(err) => Status::Fail(format!("landlock ruleset create: {err}")),
        },
        Err(err) => Status::Fail(format!("landlock handle_access: {err}")),
    }
}

#[cfg(target_os = "linux")]
fn live_probe_seccomp() -> Status {
    // Construct a filter but don't apply it — application is process-wide.
    // seccompiler rejects filters where `match_action == mismatch_action`
    // (the filter would be a no-op), so we pass distinct actions here just
    // to exercise the construction path. We never load this filter.
    use seccompiler::{SeccompAction, SeccompFilter, TargetArch};
    let arch = match std::env::consts::ARCH {
        "aarch64" => TargetArch::aarch64,
        "x86_64" => TargetArch::x86_64,
        other => return Status::Fail(format!("unsupported arch {other}")),
    };
    let rules: std::collections::BTreeMap<i64, Vec<seccompiler::SeccompRule>> =
        std::collections::BTreeMap::new();
    match SeccompFilter::new(
        rules,
        SeccompAction::Errno(libc::EPERM as u32),
        SeccompAction::Allow,
        arch,
    ) {
        Ok(_) => Status::Pass,
        Err(err) => Status::Fail(format!("seccomp filter construction: {err}")),
    }
}

#[cfg(target_os = "linux")]
fn live_probe_reflink() -> ReflinkStatus {
    // Fast path: if /tmp is on btrfs/xfs with reflink, probe a tiny file clone.
    use std::io::Write;
    let scratch = match tempfile::tempdir() {
        Ok(t) => t,
        Err(_) => return ReflinkStatus::Unknown,
    };
    let src = scratch.path().join("src");
    let dst = scratch.path().join("dst");
    if fs::File::create(&src)
        .and_then(|mut f| f.write_all(b"probe"))
        .is_err()
    {
        return ReflinkStatus::Unknown;
    }
    match reflink_copy::reflink(&src, &dst) {
        Ok(()) => ReflinkStatus::Supported,
        Err(err) => {
            if err.raw_os_error() == Some(libc::EOPNOTSUPP) {
                ReflinkStatus::Eopnotsupp
            } else {
                ReflinkStatus::Unknown
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"# F65 kernel + filesystem probe findings

**Host:** `Linux ip-172-31-31-201 6.17.0-1010-aws #10~24.04.1-Ubuntu SMP aarch64 aarch64 aarch64 GNU/Linux`
**Run at:** 2026-04-28T01:36:18.874140695+00:00

## Summary

| # | Primitive | Result | Detail |
|---|-----------|--------|--------|
| 1 | Linux kernel >= 5.13 | PASS | 6.17.0-1010-aws — 6.17 ≥ 5.13 |
| 2 | mount namespace unshare | FAIL | Neither unshare strategy succeeded. |
| 3 | overlayfs unprivileged mount (xino=on, metacopy=off) | FAIL (exit 1) | child reported: write /proc/self/setgroups=deny before gid_map: Permission denied |
| 4 | Landlock FullyEnforced | PASS | restrict_self() returned RulesetStatus::FullyEnforced |
| 5 | seccomp-BPF deny list | PASS | installed filter with 6 blocked syscalls |
| 6 | reflink (FICLONE) on /tmp | FAIL (EOPNOTSUPP (95) — expected on ext4) | FICLONE returned EOPNOTSUPP on /tmp (ext4) |
"#;

    #[test]
    fn parses_findings_markdown() {
        let f = ProbeFindings::parse_markdown(SAMPLE).expect("parse");
        assert_eq!(f.kernel_version, "6.17.0-1010-aws");
        assert!(matches!(f.mount_namespace_unshare, Status::Fail(_)));
        assert!(matches!(f.overlayfs_unprivileged, Status::Fail(_)));
        assert_eq!(f.landlock_v1_fully_enforced, Status::Pass);
        assert_eq!(f.seccomp_bpf, Status::Pass);
        assert_eq!(f.reflink_ioctl, ReflinkStatus::Eopnotsupp);
    }

    #[test]
    fn assert_required_bails_on_fail() {
        let f = ProbeFindings::parse_markdown(SAMPLE).expect("parse");
        let err = f.assert_required().expect_err("should fail");
        match err {
            ProbeError::PrimitiveFailed { primitive, .. } => {
                assert_eq!(primitive, "mount_namespace_unshare");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn assert_required_passes_when_all_pass() {
        let markdown = r#"| # | Primitive | Result | Detail |
|---|-----------|--------|--------|
| 2 | mount namespace unshare | PASS | ok |
| 3 | overlayfs unprivileged mount | PASS | ok |
| 4 | Landlock FullyEnforced | PASS | ok |
| 5 | seccomp-BPF deny list | PASS | ok |
| 6 | reflink | PASS | ok |
"#;
        let f = ProbeFindings::parse_markdown(markdown).expect("parse");
        f.assert_required().expect("should pass");
        assert!(f.reflink_available());
    }

    #[test]
    fn reflink_unsupported_is_not_required() {
        let markdown = r#"| # | Primitive | Result | Detail |
|---|-----------|--------|--------|
| 2 | mount namespace unshare | PASS | ok |
| 3 | overlayfs unprivileged mount | PASS | ok |
| 4 | Landlock FullyEnforced | PASS | ok |
| 5 | seccomp-BPF deny list | PASS | ok |
| 6 | reflink | FAIL (EOPNOTSUPP) | ext4 |
"#;
        let f = ProbeFindings::parse_markdown(markdown).expect("parse");
        f.assert_required().expect("reflink is not required");
        assert!(!f.reflink_available());
    }
}
