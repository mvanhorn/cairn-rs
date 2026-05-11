//! `f65_kernel_probe` — F65 PR-4 pre-flight: exercises every kernel and
//! filesystem primitive cairn's sandbox runtime will depend on, unprivileged,
//! on the target host. See `docs/design/orchestrator-session-architecture.md`
//! §4.3.2 and §13 item 3 for the full motivation.
//!
//! Six primitives are probed:
//!
//! 1. Linux kernel version ≥ 5.13 (Landlock LSM shipped 5.13)
//! 2. Mount namespace unshare (`CLONE_NEWNS`), unprivileged, inside a
//!    re-exec'd child process so the parent never touches its own mount
//!    table.
//! 3. Overlayfs unprivileged mount with `xino=on` — the overlay the sandbox
//!    runtime will stack per root-Run.
//! 4. Landlock `RulesetStatus::FullyEnforced`, then verification that the
//!    ruleset is actually enforced by attempting a write outside the
//!    allowed hierarchy and asserting `EACCES`.
//! 5. seccomp-BPF deny list with `SCMP_ACT_ERRNO(EPERM)` covering `mount`,
//!    `umount2`, `pivot_root`, `ptrace`, `bpf`, `perf_event_open`. The
//!    child installs the filter, then calls `ptrace(PTRACE_TRACEME)` — we
//!    assert the child exits `0` (its own `EPERM` assertion succeeded),
//!    not that it was killed by a signal.
//! 6. Reflink (`FICLONE`) on `/tmp`. Informational — this host is ext4,
//!    which returns `EOPNOTSUPP`. The probe records the errno so PR-4 can
//!    wire the documented `copy_dir_all` fallback and emit
//!    `WorkspaceBackendDegraded`.
//!
//! REQUIRED primitives (1–5) failing → the binary exits `1`. Reflink
//! failing on ext4 with `EOPNOTSUPP` is an expected informational outcome
//! and does not fail the probe.
//!
//! The binary runs unprivileged. If a probe reports that it needs `sudo`,
//! that is treated as a REQUIRED failure.
//!
//! Invocation:
//!
//! ```bash
//! cargo run -p cairn-workspace --bin f65_kernel_probe --features kernel-probe -- \
//!     --out docs/design/f65-kernel-probe-findings.md
//! ```
//!
//! The binary also prints a markdown summary table to stdout regardless of
//! whether `--out` is provided.
//!
//! ## Why re-exec children instead of `fork()`?
//!
//! The cairn workspace forbids `unsafe_code` at the crate root. `nix::fork`
//! is `unsafe` (correct — fork in a multi-threaded process is a footgun
//! family). We avoid `unsafe` by re-exec'ing the current binary via
//! `std::process::Command` with an internal `--child <name>` flag. The
//! parent process therefore never mutates its own mount namespace,
//! Landlock ruleset, or seccomp filter — those one-way operations happen
//! in a fresh process that exits cleanly afterwards.

#![deny(clippy::all)]

use std::env;
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::time::SystemTime;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};

mod probes {
    pub mod kernel_version;
    pub mod landlock;
    pub mod mount_namespace;
    pub mod overlayfs;
    pub mod reflink;
    pub mod seccomp;
}

use probes::{
    kernel_version, landlock as landlock_probe, mount_namespace, overlayfs, reflink, seccomp,
};

/// Outcome of a single primitive probe.
#[derive(Debug, Clone)]
pub struct ProbeResult {
    pub number: u8,
    pub name: &'static str,
    pub required: bool,
    pub status: ProbeStatus,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeStatus {
    Pass,
    /// Primitive-level failure. `errno_hint` is set when the underlying
    /// syscall surfaced an errno we want operators to see.
    Fail {
        errno_hint: Option<String>,
    },
    /// Informational-only outcome. Used by the reflink probe on filesystems
    /// that deliberately lack reflink support (ext4). The probe binary does
    /// NOT exit non-zero for this status.
    InfoExpected {
        errno_hint: Option<String>,
    },
}

impl ProbeResult {
    pub fn pass(number: u8, name: &'static str, required: bool, detail: impl Into<String>) -> Self {
        Self {
            number,
            name,
            required,
            status: ProbeStatus::Pass,
            detail: detail.into(),
        }
    }

    pub fn fail(
        number: u8,
        name: &'static str,
        required: bool,
        errno_hint: Option<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            number,
            name,
            required,
            status: ProbeStatus::Fail { errno_hint },
            detail: detail.into(),
        }
    }

    pub fn info_expected(
        number: u8,
        name: &'static str,
        errno_hint: Option<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            number,
            name,
            required: false,
            status: ProbeStatus::InfoExpected { errno_hint },
            detail: detail.into(),
        }
    }

    fn passed(&self) -> bool {
        matches!(self.status, ProbeStatus::Pass)
    }

    fn is_required_failure(&self) -> bool {
        self.required && matches!(self.status, ProbeStatus::Fail { .. })
    }

    /// Short markdown cell for the summary table.
    fn status_cell(&self) -> String {
        match &self.status {
            ProbeStatus::Pass => "PASS".to_string(),
            ProbeStatus::Fail { errno_hint } => match errno_hint {
                Some(hint) => format!("FAIL ({hint})"),
                None => "FAIL".to_string(),
            },
            // `InfoExpected` is informational-only; the probe does not
            // exit 1 when a primitive ends in this state. The cell
            // deliberately does NOT hardcode a filesystem name — each
            // probe's `detail` field carries the specific context
            // (ext4 fallback, cross-device, etc.).
            ProbeStatus::InfoExpected { errno_hint } => match errno_hint {
                Some(hint) => format!("INFO ({hint})"),
                None => "INFO".to_string(),
            },
        }
    }
}

/// CLI args; hand-rolled to avoid pulling in clap for two flags.
struct Cli {
    out_path: Option<PathBuf>,
    child_probe: Option<String>,
}

impl Cli {
    fn parse() -> Result<Self> {
        let mut out_path: Option<PathBuf> = None;
        let mut child_probe: Option<String> = None;
        let mut iter = env::args().skip(1);
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--out" | "-o" => {
                    let value = iter.next().ok_or_else(|| {
                        anyhow!("--out requires a path argument (e.g. --out docs/design/f65-kernel-probe-findings.md)")
                    })?;
                    out_path = Some(PathBuf::from(value));
                }
                "--child" => {
                    // Internal flag: re-exec marker telling the binary to
                    // run a single child-scoped primitive body (mount-ns /
                    // overlayfs / landlock / seccomp) and then exit. Not
                    // part of the operator-facing contract.
                    let value = iter
                        .next()
                        .ok_or_else(|| anyhow!("--child requires a child name"))?;
                    child_probe = Some(value);
                }
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => {
                    return Err(anyhow!(
                        "unknown argument: {other}. Run with --help for usage."
                    ));
                }
            }
        }
        Ok(Self {
            out_path,
            child_probe,
        })
    }
}

fn print_help() {
    println!("f65_kernel_probe — cairn F65 sandbox pre-flight probe");
    println!();
    println!("Usage:");
    println!("  f65_kernel_probe [--out <path>]");
    println!();
    println!("Flags:");
    println!("  --out, -o <path>   Write the findings markdown to <path>. Also printed to stdout.");
    println!("  --help, -h         Print this message.");
}

fn main() -> ExitCode {
    let cli = match Cli::parse() {
        Ok(c) => c,
        Err(err) => {
            eprintln!("error: {err}");
            return ExitCode::from(2);
        }
    };

    // Re-exec branch: when invoked with `--child <name>`, run exactly that
    // primitive's child body in this fresh process and exit. See the
    // module-level comment for why we avoid `fork()` / `unsafe`.
    if let Some(name) = cli.child_probe.as_deref() {
        return run_child(name);
    }

    // Capture the timestamp BEFORE running the probes so the findings
    // doc's "Run at" marks the start of the run, not the end. Individual
    // probes can take seconds (re-exec'd children, overlayfs mount, etc.)
    // and operators expect the timestamp to correspond to the moment the
    // probe was invoked.
    let started_at: DateTime<Utc> = SystemTime::now().into();
    let results = run_all_probes();
    let report = render_report(&results, started_at);

    println!("{report}");

    if let Some(path) = cli.out_path.as_ref() {
        if let Err(err) = write_report(path, &report) {
            eprintln!(
                "error: failed to write findings doc to {}: {err}",
                path.display()
            );
            return ExitCode::from(2);
        }
        eprintln!("findings written to {}", path.display());
    }

    // Any REQUIRED failure is an exit-1 condition. Reflink on ext4 returning
    // EOPNOTSUPP is informational and does NOT fail the probe.
    if results.iter().any(ProbeResult::is_required_failure) {
        ExitCode::from(1)
    } else {
        ExitCode::from(0)
    }
}

/// Dispatch a single child-scope primitive body when the binary is
/// re-exec'd with `--child <name>`.
fn run_child(name: &str) -> ExitCode {
    let result = match name {
        "mount_namespace" => mount_namespace::child_body(),
        "mount_namespace_user_plus_mount" => mount_namespace::child_body_user_plus_mount(),
        "mount_namespace_newns_only" => mount_namespace::child_body_newns_only(),
        "overlayfs" => overlayfs::child_body(),
        "landlock" => landlock_probe::child_body(),
        "seccomp" => seccomp::child_body(),
        other => Err(anyhow!("unknown --child primitive: {other}")),
    };
    match result {
        Ok(()) => ExitCode::from(0),
        Err(err) => {
            // Parent captures stderr as the detail line of the probe's
            // failure message.
            eprintln!("{err:#}");
            ExitCode::from(1)
        }
    }
}

/// Runs every probe in order. Each primitive is independent; a failure in
/// one does not short-circuit the rest — we want operators to see the full
/// picture.
fn run_all_probes() -> Vec<ProbeResult> {
    vec![
        kernel_version::probe(),
        mount_namespace::probe(),
        overlayfs::probe(),
        landlock_probe::probe(),
        seccomp::probe(),
        reflink::probe(),
    ]
}

fn render_report(results: &[ProbeResult], started_at: DateTime<Utc>) -> String {
    let mut out = String::new();
    let host = host_uname().unwrap_or_else(|_| "unknown".to_string());
    let git_sha = git_short_sha().unwrap_or_else(|| "unknown".to_string());

    writeln!(out, "# F65 kernel + filesystem probe findings").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "**Host:** `{host}`").unwrap();
    writeln!(out, "**Run at:** {}", started_at.to_rfc3339()).unwrap();
    writeln!(out, "**Probe binary:** `f65_kernel_probe` @ `{git_sha}`").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "## Summary").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "| # | Primitive | Result | Detail |").unwrap();
    writeln!(out, "|---|-----------|--------|--------|").unwrap();
    for r in results {
        writeln!(
            out,
            "| {} | {} | {} | {} |",
            r.number,
            r.name,
            r.status_cell(),
            one_line(&r.detail),
        )
        .unwrap();
    }
    writeln!(out).unwrap();
    writeln!(out, "## Per-primitive details").unwrap();
    writeln!(out).unwrap();
    for r in results {
        writeln!(out, "### {}. {}", r.number, r.name).unwrap();
        writeln!(out).unwrap();
        writeln!(out, "- Required: {}", if r.required { "yes" } else { "no" }).unwrap();
        writeln!(out, "- Result: {}", r.status_cell()).unwrap();
        writeln!(out, "- Detail:").unwrap();
        for line in r.detail.lines() {
            writeln!(out, "  {line}").unwrap();
        }
        writeln!(out).unwrap();
    }
    writeln!(out, "## Implications for PR-4").unwrap();
    writeln!(out).unwrap();
    let all_required_pass = results.iter().all(|r| !r.is_required_failure());
    if all_required_pass {
        writeln!(
            out,
            "- All REQUIRED primitives PASS — the sandbox runtime described in `docs/design/orchestrator-session-architecture.md` §4.3.2 can be implemented as specified."
        )
        .unwrap();
    } else {
        writeln!(
            out,
            "- **REQUIRED primitive FAILURE** — PR-4 is blocked until the listed failure is resolved on this host."
        )
        .unwrap();
    }
    let reflink_pass = results
        .iter()
        .find(|r| r.name.starts_with("reflink"))
        .map(ProbeResult::passed)
        .unwrap_or(false);
    if reflink_pass {
        writeln!(
            out,
            "- Reflink (`FICLONE`) succeeded on `/tmp` — PR-4 can default to the reflink fast path on this deployment."
        )
        .unwrap();
    } else {
        writeln!(
            out,
            "- Reflink unsupported on this deployment — PR-4 default path is `reflink-copy` (crate) with automatic byte-copy fallback. Emit `WorkspaceBackendDegraded {{ reason: \"ext4-fallback-full-copy\" }}` on first use. Operators who want the fast path provision btrfs or XFS-with-reflink EBS at the workspace root (operator guidance only; no code change)."
        )
        .unwrap();
    }
    writeln!(
        out,
        "- cairn-app boot-time check MUST re-run the probe logic (or read this findings doc) and fail loud with a named error if any REQUIRED primitive regresses."
    )
    .unwrap();
    out
}

fn one_line(s: &str) -> String {
    s.lines().next().unwrap_or("").to_string()
}

fn write_report(path: &Path, report: &str) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, report)
}

fn host_uname() -> io::Result<String> {
    let out = Command::new("uname").arg("-a").output()?;
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn git_short_sha() -> Option<String> {
    let out = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

/// Re-exec this binary with `--child <name>` and return the captured outcome.
/// Used by every probe whose body cannot safely run in the parent (mount-ns,
/// overlayfs, Landlock, seccomp — all one-way / parent-polluting operations).
pub(crate) fn spawn_child_probe(name: &'static str) -> Result<ChildOutcome> {
    let exe = env::current_exe().context("resolve current_exe for child re-exec")?;
    let output = Command::new(&exe)
        .arg("--child")
        .arg(name)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("spawn --child {name}"))?;
    Ok(ChildOutcome {
        status_code: output.status.code(),
        signal: extract_signal(&output.status),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
    })
}

#[derive(Debug)]
pub(crate) struct ChildOutcome {
    pub status_code: Option<i32>,
    pub signal: Option<i32>,
    pub stderr: String,
}

impl ChildOutcome {
    pub fn is_clean_zero(&self) -> bool {
        self.status_code == Some(0) && self.signal.is_none()
    }
}

// Extract a signal number in a portable (unix-only) way without pulling in
// the `nix` `signal` helpers at the parent level.
#[cfg(unix)]
fn extract_signal(status: &std::process::ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;
    status.signal()
}
#[cfg(not(unix))]
fn extract_signal(_status: &std::process::ExitStatus) -> Option<i32> {
    None
}
