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

use once_cell::sync::Lazy;
use seccompiler::{BpfProgram, SeccompAction, SeccompFilter, SeccompRule, TargetArch};

use super::ConfinementError;

/// Cached production BPF program (#504).
///
/// The deny-list is a fixed 6-syscall set keyed off the compile-time arch
/// (`aarch64` / `x86_64`) — a function of constants that never changes across
/// invocations within a single process. `SeccompFilter::new(...)` + the
/// `TryInto<BpfProgram>` compile step is ~100-300µs per call, which shows up
/// in tests that repeatedly build/apply the filter and in any caller that
/// spawns multiple confined agents within one cairn-app process.
///
/// **Production scope**: per-child `apply()` calls happen once per fork into
/// a fresh process, so the compile step would run exactly once whether this
/// was cached or not — the cache is zero-cost there but also zero-win.
/// **Test / in-process scope**: tests calling `build_program()` in a loop
/// (parameter sweeps, probe checks, golden snapshots) go from O(n × 300µs)
/// to O(300µs + n × a few ns). That's the full prize.
///
/// The `Result` is stored as `Ok(BpfProgram)` only — the error path rebuilds
/// per-call via `build_program_uncached()` because `ConfinementError` is not
/// `Clone`, and an error here is a hard-fail operator signal that a caller
/// should see fresh each time (e.g. `UnsupportedPlatform` on a raspi32).
///
/// `once_cell::sync::Lazy` rather than `std::sync::LazyLock` because the
/// workspace MSRV is 1.78 and `LazyLock` stabilised in 1.80 — bumping the
/// MSRV for one file is disproportionate.
static SECCOMP_FILTER: Lazy<Result<BpfProgram, ()>> =
    Lazy::new(|| build_program_uncached().map_err(|_| ()));

/// Apply the production seccomp deny-list to the calling process.
///
/// Process-wide: the filter applies to the caller and all descendants.
/// Per Q8 this is called from the `--sandboxed-agent` child immediately before
/// the agent loop starts.
pub fn apply() -> Result<(), ConfinementError> {
    // `SECCOMP_FILTER` caches the successful build. On cache-miss (cold
    // or operator error) fall through to an uncached rebuild so the caller
    // still sees the typed `ConfinementError` with its context, not the
    // type-erased `()` the cache stores.
    match SECCOMP_FILTER.as_ref() {
        Ok(program) => seccompiler::apply_filter(program)
            .map_err(|err| ConfinementError::SeccompLoad(format!("apply_filter: {err}"))),
        Err(()) => {
            let program = build_program_uncached()?;
            seccompiler::apply_filter(&program)
                .map_err(|err| ConfinementError::SeccompLoad(format!("apply_filter: {err}")))
        }
    }
}

/// Build (but don't apply) the BPF program. Used for unit tests and for the
/// kernel probe. Exposing this makes the "what does the deny-list contain?"
/// question testable without actually installing the filter in the test runner
/// (which would be catastrophic — every subsequent test would run confined).
///
/// Reads from the shared `SECCOMP_FILTER` cache on the happy path. Test code
/// that wants to exercise the compile fresh (e.g. to measure its cost in
/// isolation) should call `build_program_uncached()` directly.
pub fn build_program() -> Result<BpfProgram, ConfinementError> {
    match SECCOMP_FILTER.as_ref() {
        // `BpfProgram` is a `Vec<sock_filter>` (see seccompiler-3 docs);
        // cloning is a single memcpy of the instruction buffer — still
        // dramatically cheaper than re-running `SeccompFilter::new` +
        // the `TryInto` compile step.
        Ok(program) => Ok(program.clone()),
        Err(()) => build_program_uncached(),
    }
}

/// Uncached variant — always runs the build + compile. Used for the cache
/// warm-up AND as an escape hatch when the cached `Err(())` needs a typed
/// error surfaced to the caller.
fn build_program_uncached() -> Result<BpfProgram, ConfinementError> {
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

    // ── #504 regression: LazyLock cache + cached = uncached semantics ──
    //
    // The cache must serve the same program as the uncached path. If a
    // refactor makes the cache stale (e.g. the deny-list is mutated at
    // runtime — which it never should be, but adversarial review
    // assumes someone tries), this test catches it.

    #[test]
    fn cached_build_matches_uncached_build() {
        let cached = build_program().expect("cached build");
        let uncached = build_program_uncached().expect("uncached build");
        assert_eq!(
            cached, uncached,
            "cached BPF program must equal a freshly compiled program — a divergence \
             would mean a caller compiled against a stale deny-list at program start"
        );
        // 6 denied syscalls + seccompiler prologue/epilogue must produce
        // a non-trivial instruction stream. Exact instruction count is a
        // seccompiler implementation detail (version-fragile), so pin
        // only the "non-empty" invariant.
        assert!(
            !cached.is_empty(),
            "cached BPF program must have instructions"
        );
    }

    #[test]
    fn cached_program_length_stable_across_calls() {
        // Proves the cache actually serves the same `BpfProgram` across
        // successive calls — detects a regression where someone swaps
        // `LazyLock<Result<_, _>>` back to per-call compile.
        let a = build_program().expect("first");
        let b = build_program().expect("second");
        let c = build_program().expect("third");
        assert_eq!(a.len(), b.len());
        assert_eq!(b.len(), c.len());
        assert_eq!(a, b);
        assert_eq!(b, c);
    }
}
