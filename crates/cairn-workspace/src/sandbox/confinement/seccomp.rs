//! seccomp-BPF deny-list for the sandboxed agent.
//!
//! Arch doc §4.3 layer 4 + locked Q8 + the research guide §6.5 all agree:
//!
//! * **Default action: Allow.** This is a deny-list, not an allow-list.
//!   Agent tools need hundreds of syscalls (read/write/openat/stat/mmap/…)
//!   and enumerating an allow-list breaks every new toolchain release.
//! * **Denied syscalls:** `mount`, `umount2`, `pivot_root`, `ptrace`, `bpf`,
//!   `perf_event_open`. Any additional primitive that could let the agent
//!   escape Landlock (e.g. install its own seccomp bypass) goes here.
//! * **Deny action: `Errno(EPERM)`**, NOT `SIGSYS` / `Kill`. EPERM lets the
//!   agent observe the policy decision and surface a clean error; SIGSYS
//!   kills the agent process, which collapses the sandbox into "crash
//!   recovery" territory for what is actually a policy decision (locked Q8
//!   in the user prompt, matching arch §4.3.2).
//!
//! The filter is built, compiled to BPF, and loaded via
//! `seccompiler::apply_filter` — which is safe (no raw pointers touched from
//! user code).

use std::collections::BTreeMap;

use seccompiler::{BpfProgram, SeccompAction, SeccompFilter, SeccompRule, TargetArch};

use super::ConfinementError;

/// Apply the production seccomp deny-list to the calling process.
///
/// Process-wide: the filter applies to the caller and all descendants.
/// Per Q8 this is called from the `--sandboxed-agent` child immediately before
/// the agent loop starts.
pub fn apply() -> Result<(), ConfinementError> {
    let program = build_program()?;
    seccompiler::apply_filter(&program)
        .map_err(|err| ConfinementError::SeccompLoad(format!("apply_filter: {err}")))
}

/// Build (but don't apply) the BPF program. Used for unit tests and for the
/// kernel probe. Exposing this makes the "what does the deny-list contain?"
/// question testable without actually installing the filter in the test runner
/// (which would be catastrophic — every subsequent test would run confined).
pub fn build_program() -> Result<BpfProgram, ConfinementError> {
    let arch = target_arch()?;
    let rules = build_rules()?;
    SeccompFilter::new(
        rules,
        /* mismatch_action */ SeccompAction::Allow,
        /* match_action */ SeccompAction::Errno(libc::EPERM as u32),
        arch,
    )
    .and_then(TryInto::try_into)
    .map_err(|err| ConfinementError::SeccompLoad(format!("build filter: {err}")))
}

/// The list of denied syscalls, in the format seccompiler wants (syscall
/// number keyed map → vec of rules, where an empty rule-vec means "match any
/// invocation of this syscall").
fn build_rules() -> Result<BTreeMap<i64, Vec<SeccompRule>>, ConfinementError> {
    let mut rules: BTreeMap<i64, Vec<SeccompRule>> = BTreeMap::new();
    for nr in denied_syscall_numbers() {
        rules.insert(nr, vec![]); // any-args match → match_action (EPERM).
    }
    Ok(rules)
}

/// List of denied syscall numbers, resolved per-arch. Centralized so the unit
/// test can diff against the canonical deny-list.
pub fn denied_syscall_numbers() -> Vec<i64> {
    // libc's SYS_* constants give us the arch-specific syscall number table.
    vec![
        libc::SYS_mount,
        libc::SYS_umount2,
        libc::SYS_pivot_root,
        libc::SYS_ptrace,
        libc::SYS_bpf,
        libc::SYS_perf_event_open,
    ]
}

fn target_arch() -> Result<TargetArch, ConfinementError> {
    match std::env::consts::ARCH {
        "aarch64" => Ok(TargetArch::aarch64),
        "x86_64" => Ok(TargetArch::x86_64),
        other => Err(ConfinementError::UnsupportedPlatform(format!(
            "seccomp filter not supported on arch {other}; cairn supports aarch64 and x86_64 only"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_program_succeeds_on_supported_arch() {
        let program = build_program().expect("build");
        // A BPF program is a Vec<sock_filter>; assert non-empty.
        assert!(!program.is_empty(), "BPF program should have instructions");
    }

    #[test]
    fn denied_list_is_canonical_6_syscalls() {
        let denied = denied_syscall_numbers();
        assert_eq!(denied.len(), 6, "expected exactly 6 denied syscalls");
        assert!(denied.contains(&libc::SYS_mount));
        assert!(denied.contains(&libc::SYS_umount2));
        assert!(denied.contains(&libc::SYS_pivot_root));
        assert!(denied.contains(&libc::SYS_ptrace));
        assert!(denied.contains(&libc::SYS_bpf));
        assert!(denied.contains(&libc::SYS_perf_event_open));
    }
}
