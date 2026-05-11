//! Test-only helper binary for the F65 PR-4 sandbox primitive suite.
//!
//! Lives in `cairn-workspace/src/bin/` so integration tests in the same
//! crate get `CARGO_BIN_EXE_sandbox_primitive_harness` set automatically by
//! cargo. Intentionally kept small and dep-light — no anyhow, no chrono,
//! no serde — so it compiles on every Linux CI runner without feature
//! flags.
//!
//! The binary applies a single primitive to its own process, exercises an
//! assertion that validates the primitive is actually enforcing, and exits
//! 0 on success or non-zero on failure. Operator-visible detail lands on
//! stderr; the parent test asserts on the exit code and stderr content.
//!
//! Modes (each an argv entry after the program name):
//!
//! - `landlock-confine-and-verify <sandbox_path> <outside_path>`:
//!   build the production Landlock ruleset granting R+W under
//!   `<sandbox_path>` and R-only on a minimal OS path set, call
//!   `restrict_self()`, then write `<sandbox_path>/inside.txt` (MUST
//!   succeed) and write `<outside_path>` (MUST fail with EACCES). Exits
//!   0 iff both hold.
//!
//! - `seccomp-ptrace-me`:
//!   install the production seccomp deny list (via
//!   `cairn_workspace::sandbox::confinement::seccomp::apply`) and call
//!   `ptrace(PTRACE_TRACEME)`. Under the deny list this MUST return
//!   EPERM. Exits 0 iff denied.
//!
//! - `close-range-scratch <keep_fd> <expected_closed_fd>`:
//!   caller opens scratch fds before spawning; the fds land in the
//!   child via `command-fds`. Run `close_nonstandard_fds(Some(keep_fd))`
//!   then assert `fstat(keep_fd)` succeeds and
//!   `fstat(expected_closed_fd)` fails with EBADF.
//!
//! - `mount-ns-inode`:
//!   read and print the `/proc/self/ns/mnt` symlink target, then exit 0.
//!   Used in combination with an external `unshare(2)` harness to prove
//!   the child's mount-ns inode differs from the parent's. This mode
//!   does not apply any primitive on its own.
//!
//! All modes are Linux-only; non-Linux builds compile but emit a clear
//! message and exit non-zero — they should never be invoked off-platform.

#![deny(clippy::all)]

use std::env;
use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    let mode = match args.get(1) {
        Some(m) => m.clone(),
        None => {
            eprintln!("usage: sandbox_primitive_harness <mode> [args]");
            return ExitCode::from(2);
        }
    };

    #[cfg(target_os = "linux")]
    {
        match mode.as_str() {
            "landlock-confine-and-verify" => linux::landlock_confine_and_verify(&args[2..]),
            "seccomp-ptrace-me" => linux::seccomp_ptrace_me(),
            "close-range-scratch" => linux::close_range_scratch(&args[2..]),
            "mount-ns-inode" => linux::mount_ns_inode(),
            other => {
                eprintln!("unknown mode: {other}");
                ExitCode::from(2)
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = mode;
        eprintln!("sandbox_primitive_harness is Linux-only");
        ExitCode::from(2)
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use std::path::PathBuf;
    use std::process::ExitCode;

    use cairn_workspace::sandbox::confinement::landlock::default_os_read_paths;
    use cairn_workspace::sandbox::confinement::namespace::close_nonstandard_fds;
    use cairn_workspace::sandbox::confinement::seccomp;
    use cairn_workspace::sandbox::confinement::NamespacePolicy;
    use cairn_workspace::sandbox::SandboxConfinement;

    /// Build the production Landlock ruleset, apply it, then verify:
    /// - writes under `<sandbox_path>` succeed
    /// - writes to `<outside_path>` fail with EACCES
    pub(crate) fn landlock_confine_and_verify(args: &[String]) -> ExitCode {
        if args.len() != 2 {
            eprintln!("usage: landlock-confine-and-verify <sandbox_path> <outside_path>");
            return ExitCode::from(2);
        }
        let sandbox_path = PathBuf::from(&args[0]);
        let outside_path = PathBuf::from(&args[1]);

        // Use the production SandboxConfinement struct (the same path that
        // real child processes take), with seccomp off and unshare skipped
        // so this binary can run without CAP_SYS_ADMIN or a fresh userns.
        // Landlock is the primitive under test — we enforce it at full
        // production strength.
        let conf = SandboxConfinement {
            workspace_merged: sandbox_path.clone(),
            extra_read_paths: default_os_read_paths(),
            enable_landlock: true,
            enable_seccomp: false,
            namespace_policy: NamespacePolicy::Skip,
        };
        if let Err(err) = conf.confine(None) {
            eprintln!("confine failed: {err}");
            return ExitCode::from(10);
        }

        // Inside-sandbox write — must succeed.
        let inside = sandbox_path.join("inside.txt");
        if let Err(err) = std::fs::write(&inside, b"inside-ok") {
            eprintln!("write inside sandbox {}: {err}", inside.display());
            return ExitCode::from(11);
        }

        // Outside-sandbox write — must fail with EACCES/PermissionDenied.
        match std::fs::write(&outside_path, b"ESCAPED") {
            Ok(()) => {
                // Defense-in-depth: remove the escaped file so we don't
                // leak into the host.
                let _ = std::fs::remove_file(&outside_path);
                eprintln!(
                    "outside-sandbox write at {} SUCCEEDED — Landlock not enforcing",
                    outside_path.display()
                );
                ExitCode::from(12)
            }
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                // Expected.
                ExitCode::from(0)
            }
            Err(err) => {
                eprintln!(
                    "outside-sandbox write at {} failed with unexpected error: {err}",
                    outside_path.display()
                );
                ExitCode::from(13)
            }
        }
    }

    /// Apply the production seccomp deny list, then call
    /// `ptrace(PTRACE_TRACEME)`. Must return EPERM.
    pub(crate) fn seccomp_ptrace_me() -> ExitCode {
        // Install the same BPF program `SandboxConfinement::confine` would.
        if let Err(err) = seccomp::apply() {
            eprintln!("seccomp apply failed: {err}");
            return ExitCode::from(10);
        }
        match nix::sys::ptrace::traceme() {
            Ok(()) => {
                eprintln!("ptrace(TRACEME) succeeded — seccomp did NOT deny");
                ExitCode::from(11)
            }
            Err(nix::errno::Errno::EPERM) => ExitCode::from(0),
            Err(other) => {
                eprintln!("ptrace(TRACEME) failed with unexpected errno: {other}");
                ExitCode::from(12)
            }
        }
    }

    /// Assert `close_nonstandard_fds` closed every non-kept inherited fd
    /// but left `keep_fd` intact.
    pub(crate) fn close_range_scratch(args: &[String]) -> ExitCode {
        if args.len() != 2 {
            eprintln!("usage: close-range-scratch <keep_fd> <expected_closed_fd>");
            return ExitCode::from(2);
        }
        let keep_fd: i32 = match args[0].parse() {
            Ok(n) => n,
            Err(err) => {
                eprintln!("bad keep_fd `{}`: {err}", args[0]);
                return ExitCode::from(2);
            }
        };
        let expected_closed_fd: i32 = match args[1].parse() {
            Ok(n) => n,
            Err(err) => {
                eprintln!("bad expected_closed_fd `{}`: {err}", args[1]);
                return ExitCode::from(2);
            }
        };

        // Pre-close sanity: both fds must be open BEFORE we call
        // close_nonstandard_fds. The parent test set them up via command-fds.
        if let Err(err) = nix::sys::stat::fstat(keep_fd) {
            eprintln!("keep_fd {keep_fd} was not open before close: {err}");
            return ExitCode::from(10);
        }
        if let Err(err) = nix::sys::stat::fstat(expected_closed_fd) {
            eprintln!("expected_closed_fd {expected_closed_fd} was not open before close: {err}");
            return ExitCode::from(11);
        }

        if let Err(err) = close_nonstandard_fds(Some(keep_fd)) {
            eprintln!("close_nonstandard_fds failed: {err}");
            return ExitCode::from(12);
        }

        // keep_fd must still be valid.
        if let Err(err) = nix::sys::stat::fstat(keep_fd) {
            eprintln!("keep_fd {keep_fd} was unexpectedly closed: {err}");
            return ExitCode::from(13);
        }
        // expected_closed_fd must now be closed (EBADF).
        match nix::sys::stat::fstat(expected_closed_fd) {
            Ok(_) => {
                eprintln!(
                    "expected_closed_fd {expected_closed_fd} still open after close_range — \
                     fd not actually closed"
                );
                ExitCode::from(14)
            }
            Err(nix::errno::Errno::EBADF) => ExitCode::from(0),
            Err(other) => {
                eprintln!(
                    "expected_closed_fd {expected_closed_fd} fstat failed with non-EBADF: {other}"
                );
                ExitCode::from(15)
            }
        }
    }

    /// Print the mount-ns inode symlink target. Used to prove
    /// `unshare(CLONE_NEWNS)` split the namespace (parent diffs against
    /// the child's output).
    pub(crate) fn mount_ns_inode() -> ExitCode {
        match std::fs::read_link("/proc/self/ns/mnt") {
            Ok(link) => {
                println!("{}", link.display());
                ExitCode::from(0)
            }
            Err(err) => {
                eprintln!("read_link(/proc/self/ns/mnt): {err}");
                ExitCode::from(1)
            }
        }
    }
}
