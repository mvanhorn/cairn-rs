//! F65 PR-4 sandbox confinement.
//!
//! Implements the three-layer Linux kernel confinement fence that wraps the
//! agent subprocess before the agent touches any tool state:
//!
//! 1. **Mount namespace** (`namespace::unshare_mount_ns`) — the overlayfs mount
//!    lives only inside this process's mount table; host and sibling sandboxes
//!    cannot see it.
//! 2. **Landlock LSM** (`landlock::apply`) — bounds the filesystem reachable
//!    via path-based access; asserts `RulesetStatus::FullyEnforced` or bails
//!    loudly.
//! 3. **seccomp-BPF** (`seccomp::apply`) — deny-list for the handful of
//!    syscalls (`mount`, `umount2`, `pivot_root`, `ptrace`, `bpf`,
//!    `perf_event_open`) that could let an in-sandbox agent escape Landlock.
//!    Returns `EPERM` (not `SIGSYS`) per arch doc §4.3.2.
//!
//! The three layers are composed by [`SandboxConfinement::confine`] in the
//! correct order; reorderings open CVE-class bypasses (see `docs/design/
//! orchestrator-session-architecture.md` §4.3 + the research guide §6).
//!
//! Everything compiles only on Linux; non-Linux builds get a stub that returns
//! `ConfinementError::UnsupportedPlatform` so the sandbox is never silently a
//! no-op.

pub mod namespace;
pub mod probe;

#[cfg(target_os = "linux")]
pub mod landlock;
#[cfg(target_os = "linux")]
pub mod seccomp;

use std::path::{Path, PathBuf};

pub use probe::{ProbeError, ProbeFindings, ReflinkStatus, Status};

/// Recoverable confinement errors.
///
/// Every variant is named by primitive so operator-visible logs can point at
/// the exact failing layer.
#[derive(Debug)]
pub enum ConfinementError {
    /// `unshare(CLONE_NEWNS)` (and friends) failed, usually because the host
    /// kernel blocks unprivileged user namespaces (see the probe's
    /// [`ProbeFindings`] for the likely remediation).
    NamespaceUnshare(String),
    /// Landlock `restrict_self()` returned a non-`FullyEnforced` status;
    /// confinement is not complete and the agent MUST NOT run.
    LandlockPartial(String),
    /// Failed to build the Landlock ruleset (path translation, ABI detection,
    /// …).
    LandlockRuleset(String),
    /// `seccompiler::apply_filter` failed to load the BPF program. The
    /// inner string carries the seccompiler error message.
    SeccompLoad(String),
    /// Closing an inherited file descriptor failed. Any leak through this
    /// boundary could bypass Landlock, so this is fatal.
    FdCloseFailed(String),
    /// Probe reported a REQUIRED primitive as unavailable; refusing to
    /// confine would leave the agent unconfined.
    ProbeFailed(String),
    /// Compiled for a non-Linux host or one the seccompiler crate cannot
    /// handle (e.g. neither aarch64 nor x86_64).
    UnsupportedPlatform(String),
    /// A workspace path supplied by the caller does not exist or is not a
    /// directory. Landlock would have accepted a missing path silently,
    /// which would defeat confinement, so we refuse.
    InvalidPath(PathBuf, String),
}

impl std::fmt::Display for ConfinementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NamespaceUnshare(msg) => write!(f, "mount namespace unshare failed: {msg}"),
            Self::LandlockPartial(status) => write!(
                f,
                "Landlock ruleset is not FullyEnforced (got {status}); refusing to run agent \
                 without complete confinement"
            ),
            Self::LandlockRuleset(msg) => write!(f, "Landlock ruleset construction failed: {msg}"),
            Self::SeccompLoad(msg) => write!(f, "seccomp-BPF filter apply failed: {msg}"),
            Self::FdCloseFailed(msg) => {
                write!(f, "failed to close inherited file descriptor: {msg}")
            }
            Self::ProbeFailed(msg) => write!(f, "kernel primitive probe failed: {msg}"),
            Self::UnsupportedPlatform(msg) => write!(f, "platform unsupported: {msg}"),
            Self::InvalidPath(path, msg) => {
                write!(f, "invalid confinement path {}: {}", path.display(), msg)
            }
        }
    }
}

impl std::error::Error for ConfinementError {}

/// Policy knob for the mount-namespace layer.
///
/// The `Skip` variant is a dev/test affordance that MUST NOT reach
/// production. `SandboxConfinement::production()` builds with `MountOnly`
/// and has no way to set `Skip`. Child processes that read CLI flags /
/// env vars + select `Skip` log a loud warning AND refuse to apply
/// when `CAIRN_SANDBOX_REQUIRED=1` (see `cairn-app` binary for the gate).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum NamespacePolicy {
    /// `unshare(CLONE_NEWNS)` — mount only. Default. Network stays shared
    /// with the host so the agent can install toolchain deps.
    #[default]
    MountOnly,
    /// `unshare(CLONE_NEWNS | CLONE_NEWNET)` — mount AND network. Agent
    /// gets a fresh netns with loopback only (locked Q7: policy-driven,
    /// default Shared but cairn-app can opt per-session into Isolated).
    MountAndNetwork,
    /// Skip `unshare` entirely. Dev affordance only — production MUST use
    /// MountOnly or MountAndNetwork. Used in tests where the host kernel
    /// blocks `unshare(CLONE_NEWNS)` (e.g. Ubuntu 24.04 AppArmor gate).
    /// Setting this causes a loud runtime warning; cairn-app refuses to
    /// proceed when `CAIRN_SANDBOX_REQUIRED=1`.
    Skip,
}

impl NamespacePolicy {
    /// Returns true iff the policy is the test-only Skip variant.
    pub fn is_test_skip(self) -> bool {
        matches!(self, Self::Skip)
    }
}

/// Confinement configuration handed from the parent cairn-app to the
/// `--sandboxed-agent` child via command-line arguments and inherited fds.
///
/// The child reconstructs this struct, then calls [`SandboxConfinement::confine`]
/// to install the full fence. The ONLY resource that survives the fence is
/// the tool-bridge socket (fd 3).
#[derive(Debug, Clone)]
pub struct SandboxConfinement {
    /// The overlayfs merged directory (RW mount) the agent sees as the root
    /// of the workspace. Landlock grants R+W access beneath this path.
    pub workspace_merged: PathBuf,
    /// Additional read-only paths the agent needs — typically `/lib`,
    /// `/lib64`, `/usr/lib`, `/usr/bin`. Callers supply the exact set to
    /// minimize the ambient-authority surface.
    pub extra_read_paths: Vec<PathBuf>,
    /// Whether to install the seccomp deny-list. Exposed so integration tests
    /// can build a config without seccomp to verify layer composition, NOT
    /// exposed to operators — production ALWAYS sets this to `true`.
    pub enable_seccomp: bool,
    /// Whether to install Landlock. Same dev-only affordance as
    /// `enable_seccomp`; production ALWAYS sets this to `true`.
    pub enable_landlock: bool,
    /// Mount-namespace policy. Default `MountOnly` (shared network). Set to
    /// `MountAndNetwork` for per-session network isolation, or `Skip` in
    /// tests where the host kernel blocks `unshare(CLONE_NEWNS)`.
    pub namespace_policy: NamespacePolicy,
}

impl SandboxConfinement {
    /// Build the production configuration with Landlock, seccomp, AND a
    /// private mount namespace. Callers MUST pass the merged overlayfs mount
    /// point plus the read-only OS paths they need.
    pub fn production(workspace_merged: PathBuf, extra_read_paths: Vec<PathBuf>) -> Self {
        Self {
            workspace_merged,
            extra_read_paths,
            enable_seccomp: true,
            enable_landlock: true,
            namespace_policy: NamespacePolicy::MountOnly,
        }
    }

    /// Apply the full four-layer confinement fence to the calling process.
    ///
    /// Order is load-bearing:
    /// 0. validate paths (fail loud BEFORE we touch the fd table or unshare,
    ///    so an invalid config can't half-confine the calling process).
    /// 1. `unshare(CLONE_NEWNS)` (and optionally `CLONE_NEWNET`) — the
    ///    overlay mount lives only in this process's mount table. Skipped
    ///    when `namespace_policy == Skip` (dev/test only).
    /// 2. close fds ≥ 4 (keep stdin/stdout/stderr + the tool-bridge socket
    ///    passed in `keep_fd`) — prevents pre-opened-fd bypass of Landlock.
    /// 3. apply Landlock (needs file I/O to `PathFd::new`).
    /// 4. apply seccomp (last, so we don't block the syscalls Landlock
    ///    needs during `restrict_self()`).
    ///
    /// Returns on success; any error AFTER step 0 leaves the process in an
    /// unknown partial-confinement state. Callers MUST treat any such error
    /// as fatal and exit non-zero — there is no safe way to unwind partial
    /// confinement. The step-0 path-validation error is idempotent and can
    /// be surfaced to operator UX.
    #[cfg(target_os = "linux")]
    pub fn confine(
        &self,
        keep_fd: Option<std::os::unix::io::RawFd>,
    ) -> Result<(), ConfinementError> {
        self.validate_paths()?;

        match self.namespace_policy {
            NamespacePolicy::MountOnly => namespace::unshare_mount_ns()?,
            NamespacePolicy::MountAndNetwork => namespace::unshare_mount_and_network()?,
            NamespacePolicy::Skip => {
                // Test-only affordance. Production callers MUST NOT use Skip.
            }
        }

        namespace::close_nonstandard_fds(keep_fd)?;

        if self.enable_landlock {
            landlock::apply(&self.workspace_merged, &self.extra_read_paths)?;
        }

        if self.enable_seccomp {
            seccomp::apply()?;
        }

        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    pub fn confine(
        &self,
        _keep_fd: Option<std::os::unix::io::RawFd>,
    ) -> Result<(), ConfinementError> {
        Err(ConfinementError::UnsupportedPlatform(
            "sandbox confinement requires Linux 5.13+; this binary was compiled for a different \
             target"
                .to_string(),
        ))
    }

    fn validate_paths(&self) -> Result<(), ConfinementError> {
        if !self.workspace_merged.is_dir() {
            return Err(ConfinementError::InvalidPath(
                self.workspace_merged.clone(),
                "workspace merged path does not exist or is not a directory".to_string(),
            ));
        }
        for path in &self.extra_read_paths {
            if !path.exists() {
                return Err(ConfinementError::InvalidPath(
                    path.clone(),
                    "read-only confinement path does not exist".to_string(),
                ));
            }
        }
        Ok(())
    }
}

/// Borrowed view used by in-crate helpers that don't need to take ownership.
#[derive(Debug)]
pub struct SandboxConfinementRef<'a> {
    pub workspace_merged: &'a Path,
    pub extra_read_paths: &'a [PathBuf],
    pub enable_seccomp: bool,
    pub enable_landlock: bool,
}

impl<'a> From<&'a SandboxConfinement> for SandboxConfinementRef<'a> {
    fn from(value: &'a SandboxConfinement) -> Self {
        Self {
            workspace_merged: &value.workspace_merged,
            extra_read_paths: &value.extra_read_paths,
            enable_seccomp: value.enable_seccomp,
            enable_landlock: value.enable_landlock,
        }
    }
}
