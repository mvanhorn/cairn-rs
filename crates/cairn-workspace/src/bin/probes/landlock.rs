//! Probe 4: Landlock `RulesetStatus::FullyEnforced` + enforcement verification.
//!
//! Cairn's sandbox runtime (see `orchestrator-session-architecture.md`
//! §4.3.2 and `agent-knowledge/agentic-sandbox-architectures.md` §6.3)
//! MUST assert `RulesetStatus::FullyEnforced` after `restrict_self()`.
//! Partial enforcement is a silent security hole — the research guide
//! calls this out explicitly, and the non-negotiables list in the spike
//! prompt repeats it.
//!
//! The probe body runs in a re-exec'd child so the parent's process
//! state stays untouched (Landlock is a one-way restriction; once
//! `restrict_self` succeeds, it cannot be undone in that process).
//!
//! Test shape:
//!
//! 1. In the child, create a tempdir we own (the "sandbox root").
//! 2. Build a ruleset at the highest ABI supported by landlock 0.4
//!    (`ABI::V6` → Linux 6.12+). The host kernel is 6.17, so every
//!    access right `AccessFs::from_all(ABI::V6)` is available, making
//!    `FullyEnforced` reachable.
//! 3. Grant full read + write on the sandbox root, read on `/` so the
//!    process can still exec the Rust runtime / load shared libs.
//! 4. Call `restrict_self()`; assert `RulesetStatus::FullyEnforced` and
//!    `no_new_privs: true`.
//! 5. Write-verify enforcement: writing a file inside the sandbox root
//!    must succeed; writing a file under `/home/$USER` (not in the
//!    ruleset) must return `EACCES`. Both cases are checked — a
//!    ruleset that fails OPEN would pass the first but also pass the
//!    second, and a ruleset that is over-restrictive would pass the
//!    second but fail the first.

use std::fs;
use std::io::ErrorKind;

use anyhow::{bail, Context, Result};
use landlock::{
    Access, AccessFs, PathBeneath, PathFd, Ruleset, RulesetAttr, RulesetCreatedAttr, RulesetStatus,
    ABI,
};
use tempfile::TempDir;

use crate::{spawn_child_probe, ProbeResult};

const NUMBER: u8 = 4;
const NAME: &str = "Landlock FullyEnforced";
const REQUIRED: bool = true;
/// Probe targets the highest ABI the 0.4 crate exposes (`V6` → Linux
/// 6.12+). The production host is 6.17, so every access right is
/// available and `FullyEnforced` is the expected outcome. A lower
/// effective ABI would surface as `PartiallyEnforced` — which the probe
/// flags as a REQUIRED failure.
const TARGET_ABI: ABI = ABI::V6;

pub fn probe() -> ProbeResult {
    let outcome = match spawn_child_probe("landlock") {
        Ok(o) => o,
        Err(err) => {
            return ProbeResult::fail(
                NUMBER,
                NAME,
                REQUIRED,
                None,
                format!("failed to spawn landlock child: {err:#}"),
            );
        }
    };

    if outcome.is_clean_zero() {
        ProbeResult::pass(
            NUMBER,
            NAME,
            REQUIRED,
            format!(
                "restrict_self() returned RulesetStatus::FullyEnforced at ABI::{TARGET_ABI:?}; \
                 in-sandbox write succeeded and out-of-sandbox write was denied with EACCES"
            ),
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
                "child reported: {}\n\
                 Actionable: Landlock requires kernel ≥ 5.13 (probe #1 covers the floor). \
                 If the kernel supports the LSM but the ABI is lower than V6, PR-4 must \
                 fall back to `AccessFs::from_all(ABI::V1)` and still assert FullyEnforced \
                 at that lower level.",
                if outcome.stderr.is_empty() {
                    "(no stderr)"
                } else {
                    outcome.stderr.as_str()
                }
            ),
        )
    }
}

/// Child-scope body. See the module comment for the test shape.
pub fn child_body() -> Result<()> {
    // 1. Create the sandbox root we'll grant write access to.
    let sandbox = TempDir::new().context("create sandbox root tempdir")?;
    let sandbox_path = sandbox.path().to_path_buf();

    // Resolve HOME so the out-of-sandbox write target is on the same host
    // as the parent runs on. Fall back to `/etc` (always present, never
    // writable without root) if HOME is unset — the test still exercises
    // EACCES in that case.
    let out_of_sandbox_target = std::env::var_os("HOME")
        .map(|h| std::path::PathBuf::from(h).join(".cairn-f65-probe-escape-target"))
        .unwrap_or_else(|| std::path::PathBuf::from("/etc/.cairn-f65-probe-escape-target"));

    // 2. Build + create the ruleset at TARGET_ABI with full FS access set.
    //    We handle every access right the ABI exposes — that is the only
    //    way `restrict_self()` can return `FullyEnforced` at this ABI on
    //    a kernel that supports it.
    let access_all = AccessFs::from_all(TARGET_ABI);

    // Open path fds for `add_rule` BEFORE `restrict_self` is called, but
    // AFTER we've built every rule object we'll need. Landlock applies to
    // newly-opened fds; already-open fds are exempt — but the ruleset
    // object itself uses the fds it was built from, so the standard
    // pattern is fine.
    let sandbox_fd = PathFd::new(&sandbox_path)
        .with_context(|| format!("open sandbox path fd {}", sandbox_path.display()))?;
    let root_fd = PathFd::new("/").context("open / fd for read-only rule")?;

    let status = Ruleset::default()
        .handle_access(access_all)
        .context("handle_access(from_all)")?
        .create()
        .context("create Landlock ruleset")?
        // Grant full access (everything TARGET_ABI covers) inside the sandbox root.
        .add_rule(PathBeneath::new(sandbox_fd, access_all))
        .context("add_rule(sandbox root, all access)")?
        // Grant read-only traversal on `/` so stdlib + libc can be loaded.
        .add_rule(PathBeneath::new(root_fd, AccessFs::from_read(TARGET_ABI)))
        .context("add_rule(/, read-only)")?
        .restrict_self()
        .context("restrict_self")?;

    // 3. Assert FullyEnforced. Partial enforcement is a silent security
    //    hole — per the research guide §6.3 and the spike prompt's
    //    non-negotiables list, we bail loud.
    match status.ruleset {
        RulesetStatus::FullyEnforced => {}
        RulesetStatus::PartiallyEnforced => {
            bail!(
                "ruleset reported PartiallyEnforced at ABI::{TARGET_ABI:?} — refusing to proceed. \
                 See agent-knowledge/agentic-sandbox-architectures.md §6.3."
            );
        }
        RulesetStatus::NotEnforced => {
            bail!("ruleset reported NotEnforced — the kernel does not support Landlock at all");
        }
    }
    if !status.no_new_privs {
        bail!("restrict_self() did NOT set no_new_privs; this is a Landlock contract violation");
    }

    // 4. Write-verify. Both directions must behave as configured.
    let in_sandbox = sandbox_path.join("inside.txt");
    fs::write(&in_sandbox, b"inside-ok")
        .with_context(|| format!("write inside sandbox at {}", in_sandbox.display()))?;

    // The out-of-sandbox write must fail with EACCES (or PermissionDenied
    // in io::Error terms). If it succeeds, the ruleset is not enforcing
    // and we return a clear failure.
    match fs::write(&out_of_sandbox_target, b"ESCAPED") {
        Ok(()) => {
            // Defense in depth: remove any escaped file if we somehow wrote
            // it, then bail with the details.
            let _ = fs::remove_file(&out_of_sandbox_target);
            bail!(
                "out-of-sandbox write at {} SUCCEEDED — Landlock ruleset is not actually enforcing",
                out_of_sandbox_target.display()
            );
        }
        Err(err) if err.kind() == ErrorKind::PermissionDenied => {
            // Expected.
        }
        Err(err) => {
            bail!(
                "out-of-sandbox write at {} failed with an unexpected error kind: {:?} ({})",
                out_of_sandbox_target.display(),
                err.kind(),
                err
            );
        }
    }

    Ok(())
}
