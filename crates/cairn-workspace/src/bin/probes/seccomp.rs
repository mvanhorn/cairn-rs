//! Probe 5: seccomp-BPF deny list with `SCMP_ACT_ERRNO(EPERM)`.
//!
//! Cairn's sandbox runtime installs a seccomp filter that blocks a small
//! set of escape-enabling syscalls (see `orchestrator-session-
//! architecture.md` §4.3.2 and `agent-knowledge/agentic-sandbox-
//! architectures.md` §7 implementation checklist). The match action must
//! be `SCMP_ACT_ERRNO(EPERM)` — NOT `SCMP_ACT_KILL` — so a tool that
//! accidentally issues a blocked syscall fails cleanly rather than being
//! `SIGSYS`-killed mid-operation. Clean-fail recovery is a product
//! requirement, not an optimization.
//!
//! Test shape (runs in a re-exec'd child so the parent's filter tree
//! stays empty):
//!
//! 1. Build a `SeccompFilter` with the blocked syscalls (mount,
//!    umount2, pivot_root, ptrace, bpf, perf_event_open) mapped to
//!    `SeccompAction::Errno(EPERM)`. Mismatch action is `Allow`.
//! 2. Install the filter via `seccompiler::apply_filter`.
//! 3. Directly invoke `ptrace(PTRACE_TRACEME)` via libc. The outcome
//!    MUST be `Err(EPERM)` — any other outcome (including being killed
//!    by SIGSYS) is a probe failure.
//! 4. Exit 0. The parent interprets `signal == Some(31) (SIGSYS)` as
//!    "filter was installed as SCMP_ACT_KILL rather than EPERM," which
//!    is a separate, very specific failure mode.

use anyhow::{bail, Context, Result};
use nix::errno::Errno;
use nix::libc;
use nix::sys::ptrace;
use seccompiler::{apply_filter, BpfProgram, SeccompAction, SeccompFilter, TargetArch};

use crate::{spawn_child_probe, ProbeResult};

const NUMBER: u8 = 5;
const NAME: &str = "seccomp-BPF deny list";
const REQUIRED: bool = true;

/// Syscalls the deny list covers. Names match the research guide and the
/// arch doc exactly.
const BLOCKED_SYSCALLS: &[(&str, i64)] = &[
    ("mount", libc::SYS_mount),
    ("umount2", libc::SYS_umount2),
    ("pivot_root", libc::SYS_pivot_root),
    ("ptrace", libc::SYS_ptrace),
    ("bpf", libc::SYS_bpf),
    ("perf_event_open", libc::SYS_perf_event_open),
];

pub fn probe() -> ProbeResult {
    let outcome = match spawn_child_probe("seccomp") {
        Ok(o) => o,
        Err(err) => {
            return ProbeResult::fail(
                NUMBER,
                NAME,
                REQUIRED,
                None,
                format!("failed to spawn seccomp child: {err:#}"),
            );
        }
    };

    if outcome.is_clean_zero() {
        ProbeResult::pass(
            NUMBER,
            NAME,
            REQUIRED,
            format!(
                "installed filter with {} blocked syscalls; ptrace(PTRACE_TRACEME) \
                 returned EPERM as expected (not SIGSYS kill)",
                BLOCKED_SYSCALLS.len()
            ),
        )
    } else if outcome.signal == Some(libc::SIGSYS) {
        // Very specific failure mode: the filter was installed with KILL
        // instead of ERRNO(EPERM), or the child bypassed our filter
        // builder. Flag it loudly.
        ProbeResult::fail(
            NUMBER,
            NAME,
            REQUIRED,
            Some("SIGSYS".to_string()),
            "child was killed by SIGSYS — the filter blocked a syscall with KILL rather than \
             ERRNO(EPERM). PR-4 must use SeccompAction::Errno, not SeccompAction::KillProcess. \
             Clean-fail recovery is a product requirement."
                .to_string(),
        )
    } else {
        let sig = outcome.signal.map(|s| format!("signal {s}"));
        let code = outcome.status_code.map(|c| format!("exit {c}"));
        ProbeResult::fail(
            NUMBER,
            NAME,
            REQUIRED,
            sig.or(code),
            format!(
                "child reported: {}",
                if outcome.stderr.is_empty() {
                    "(no stderr)"
                } else {
                    outcome.stderr.as_str()
                }
            ),
        )
    }
}

/// Child-scope body. See the module comment for test shape.
pub fn child_body() -> Result<()> {
    let target_arch = current_target_arch()
        .context("resolve TargetArch — cairn currently targets aarch64 and x86_64")?;

    // Build the rule map. Empty rule vectors match the syscall regardless
    // of arguments — exactly what the deny list wants.
    let rules = BLOCKED_SYSCALLS
        .iter()
        .map(|(_name, nr)| (*nr, Vec::new()))
        .collect();

    // NOTE parameter order: seccompiler::SeccompFilter::new takes
    // (rules, mismatch_action, match_action, target_arch). `Allow` is
    // the mismatch action (let everything else through) and
    // `Errno(EPERM)` is the match action (our deny list).
    let filter = SeccompFilter::new(
        rules,
        SeccompAction::Allow,
        SeccompAction::Errno(libc::EPERM as u32),
        target_arch,
    )
    .context("build SeccompFilter")?;

    let program: BpfProgram = filter.try_into().context("compile SeccompFilter to BPF")?;
    apply_filter(&program).context("apply_filter")?;

    // Trigger one of the blocked syscalls and assert the errno is EPERM.
    // `nix::sys::ptrace::traceme()` wraps `ptrace(PTRACE_TRACEME, ...)` as
    // a safe function and surfaces the kernel errno via `Errno`.
    //
    // PTRACE_TRACEME is the right probe because:
    // * It has no side effects unless we later fork children (we won't).
    // * It's blocked by our filter.
    // * It's a thin libc wrapper, so we see the real kernel errno.
    match ptrace::traceme() {
        Ok(()) => {
            bail!(
                "ptrace(PTRACE_TRACEME) succeeded — seccomp filter was NOT installed (or does \
                 not cover this syscall)"
            );
        }
        Err(Errno::EPERM) => {
            // Expected: filter installed with SCMP_ACT_ERRNO(EPERM).
        }
        Err(other) => {
            bail!(
                "ptrace(PTRACE_TRACEME) returned errno {other:?} — expected EPERM; the filter is \
                 installed but with the wrong action (not SCMP_ACT_ERRNO(EPERM))"
            );
        }
    }

    Ok(())
}

#[cfg(target_arch = "aarch64")]
fn current_target_arch() -> Result<TargetArch> {
    Ok(TargetArch::aarch64)
}
#[cfg(target_arch = "x86_64")]
fn current_target_arch() -> Result<TargetArch> {
    Ok(TargetArch::x86_64)
}
#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
fn current_target_arch() -> Result<TargetArch> {
    anyhow::bail!(
        "seccompiler 0.5 supports aarch64 + x86_64 only; build is on an unsupported target"
    )
}

#[cfg(test)]
mod tests {
    use super::{current_target_arch, BLOCKED_SYSCALLS};

    #[test]
    fn every_blocked_syscall_has_distinct_number() {
        let mut nums: Vec<i64> = BLOCKED_SYSCALLS.iter().map(|(_, n)| *n).collect();
        nums.sort_unstable();
        let len_before = nums.len();
        nums.dedup();
        assert_eq!(
            nums.len(),
            len_before,
            "duplicate syscall number in deny list"
        );
    }

    #[test]
    fn blocked_list_covers_architecture_escape_surface() {
        let names: Vec<&str> = BLOCKED_SYSCALLS.iter().map(|(n, _)| *n).collect();
        for expected in [
            "mount",
            "umount2",
            "pivot_root",
            "ptrace",
            "bpf",
            "perf_event_open",
        ] {
            assert!(
                names.contains(&expected),
                "deny list missing required syscall: {expected}"
            );
        }
    }

    #[test]
    fn target_arch_resolves_on_build_host() {
        // current_target_arch returns Err only on unsupported archs.
        assert!(current_target_arch().is_ok());
    }
}
