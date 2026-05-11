//! Landlock LSM ruleset + `restrict_self()` fence.
//!
//! F65 PR-4 contract (arch §4.3 layer 3):
//!
//! * Grant R+W on the overlayfs merged workspace path.
//! * Grant R-only on the binaries/libraries the agent's toolchain needs
//!   (typically `/lib`, `/lib64`, `/usr/lib`, `/usr/bin`).
//! * Apply the ruleset with `restrict_self()`.
//! * Assert `RulesetStatus::FullyEnforced`; bail with
//!   [`ConfinementError::LandlockPartial`] if the kernel returns anything
//!   else. Anything short of FullyEnforced means the ABI the caller asked for
//!   is not available, which in the arch doc's threat model is equivalent to
//!   "no confinement at all" — we refuse to run the agent.
//!
//! Process-wide: `restrict_self()` restricts the calling task **and all
//! descendants**. The F65 process model (Q1 locked) confines the
//! `--sandboxed-agent` child process specifically, not the parent cairn-app.

use std::path::Path;
use std::path::PathBuf;

use landlock::{
    Access, AccessFs, PathBeneath, PathFd, Ruleset, RulesetAttr, RulesetCreatedAttr, RulesetStatus,
    ABI,
};

use super::ConfinementError;

/// Build and apply the production Landlock ruleset.
///
/// The path slice layout is intentionally explicit — callers pass both the
/// workspace merged path (R+W) and the set of read-only OS paths. Callers are
/// expected to use the defaults from [`default_os_read_paths`] for production
/// child processes.
pub fn apply(
    workspace_merged: &Path,
    extra_read_paths: &[PathBuf],
) -> Result<(), ConfinementError> {
    let abi = ABI::V1;

    let ruleset = Ruleset::default()
        .handle_access(AccessFs::from_all(abi))
        .map_err(|err| {
            ConfinementError::LandlockRuleset(format!("handle_access(AccessFs::all): {err}"))
        })?
        .create()
        .map_err(|err| ConfinementError::LandlockRuleset(format!("ruleset create: {err}")))?;

    // R+W on the overlayfs merged workspace.
    let rw_access = AccessFs::from_all(abi);
    let rw_fd = PathFd::new(workspace_merged).map_err(|err| {
        ConfinementError::LandlockRuleset(format!(
            "PathFd::new({}): {err}",
            workspace_merged.display()
        ))
    })?;
    let ruleset = ruleset
        .add_rule(PathBeneath::new(rw_fd, rw_access))
        .map_err(|err| {
            ConfinementError::LandlockRuleset(format!("add RW rule for workspace: {err}"))
        })?;

    // R-only on each supplied path. Callers are responsible for filtering
    // out non-existent paths before calling apply() — e.g. via
    // `default_os_read_paths()` which runs `path.exists()` once. Here we
    // treat every supplied path as required: if `PathFd::new` fails the
    // whole ruleset build fails, so operators can see the misconfiguration
    // instead of getting a silently-unlocked Landlock.
    let ro_access = AccessFs::from_read(abi);
    let mut ruleset = ruleset;
    for path in extra_read_paths {
        let fd = PathFd::new(path).map_err(|err| {
            ConfinementError::LandlockRuleset(format!("PathFd::new({}): {err}", path.display()))
        })?;
        ruleset = ruleset
            .add_rule(PathBeneath::new(fd, ro_access))
            .map_err(|err| {
                ConfinementError::LandlockRuleset(format!(
                    "add RO rule for {}: {err}",
                    path.display()
                ))
            })?;
    }

    let status = ruleset
        .restrict_self()
        .map_err(|err| ConfinementError::LandlockRuleset(format!("restrict_self: {err}")))?;

    match status.ruleset {
        RulesetStatus::FullyEnforced => Ok(()),
        RulesetStatus::PartiallyEnforced => Err(ConfinementError::LandlockPartial(
            "PartiallyEnforced — kernel supports only a subset of the requested ABI".to_string(),
        )),
        RulesetStatus::NotEnforced => Err(ConfinementError::LandlockPartial(
            "NotEnforced — kernel has no Landlock support at runtime".to_string(),
        )),
    }
}

/// Default set of read-only paths every sandboxed agent needs to execute its
/// toolchain (bash, python, cargo, coreutils, …). Callers can extend this list
/// but should never shrink it — stripping `/usr/bin` for example would make
/// every `bash` invocation fail with `EACCES`.
pub fn default_os_read_paths() -> Vec<PathBuf> {
    [
        "/lib",
        "/lib64",
        "/usr/lib",
        "/usr/lib64",
        "/usr/bin",
        "/usr/local/lib",
        "/etc/alternatives",
    ]
    .into_iter()
    .map(PathBuf::from)
    .filter(|p| p.exists())
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_os_read_paths_are_all_absolute() {
        for p in default_os_read_paths() {
            assert!(p.is_absolute(), "{} must be absolute", p.display());
        }
    }

    #[test]
    fn default_os_read_paths_skip_nonexistent() {
        // On any Linux host /lib or /usr/lib exists; we should always get a
        // non-empty list. On non-Linux CI this test is still informative.
        let paths = default_os_read_paths();
        #[cfg(target_os = "linux")]
        assert!(
            !paths.is_empty(),
            "expected at least one default OS read path on Linux"
        );
        // And nothing nonexistent.
        for p in paths {
            assert!(p.exists(), "{} should exist after filter", p.display());
        }
    }
}
