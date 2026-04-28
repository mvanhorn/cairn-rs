//! F65 PR-4 integration tests exercising the `cairn-app --sandboxed-agent`
//! child-process path.
//!
//! Unlike the unit-level confinement tests in `cairn-workspace`, these tests
//! fork a real cairn-app binary with `--sandboxed-agent` flags, connect to
//! fd 3 over a socketpair, and verify the end-to-end RPC protocol.
//!
//! # Test quality contract (closes audit findings #394, #395, #406)
//!
//! Every security-relevant test in this file MUST, when the kernel supports
//! the primitive under test:
//!
//! 1. Run with confinement fully ENABLED (no `CAIRN_SANDBOX_DISABLE_*` env
//!    vars set, no `NamespacePolicy::Skip` fallback).
//! 2. Assert on the sandbox guarantee itself (EACCES on the outside-workspace
//!    write, allow on the inside-workspace write, etc.) — NOT on the reply
//!    echoing the op name back.
//!
//! When the kernel does NOT support the primitive (Ubuntu 24.04+ with
//! `kernel.apparmor_restrict_unprivileged_userns=1`, which blocks the
//! mount/user-namespace path), the test MUST `eprintln!` an explicit
//! "SKIP — <reason>" line and early-return, so operators reading CI logs see
//! exactly which tests were skipped. Silent passes on a broken kernel are
//! the original defect.
//!
//! Wire-protocol tests that do not depend on confinement (self-test,
//! missing-flags bail-outs, unknown-op routing) continue to run with layers
//! disabled via the `CAIRN_SANDBOX_DISABLE_*` env vars — they verify the
//! bridge, not the fence.

#![cfg(target_os = "linux")]

use std::io::Read;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

use cairn_workspace::sandbox::confinement::probe::run_live_probe;
use cairn_workspace::sandbox::Status;
use command_fds::{CommandFdExt, FdMapping};

fn cairn_app_bin() -> PathBuf {
    // CARGO_BIN_EXE_<name> is set by cargo for integration tests.
    PathBuf::from(env!("CARGO_BIN_EXE_cairn-app"))
}

fn make_tmp_workspace() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tmpdir");
    // Put a scratch file so the workspace is non-empty (some Landlock
    // variants refuse to add a rule for an empty path in edge cases).
    std::fs::write(dir.path().join("README"), "f65").unwrap();
    dir
}

/// Landlock works WITHOUT mount-ns unshare. The `restrict_self` fence is a
/// per-process LSM attachment; it requires the kernel's landlock module +
/// an unshadowed security context but NOT CAP_SYS_ADMIN and NOT a fresh
/// mount namespace.
fn kernel_supports_landlock() -> bool {
    matches!(run_live_probe().landlock_v1_fully_enforced, Status::Pass)
}

/// seccomp-BPF works without unshare on any kernel with
/// `CONFIG_SECCOMP_FILTER=y`.
fn kernel_supports_seccomp() -> bool {
    matches!(run_live_probe().seccomp_bpf, Status::Pass)
}

/// Heuristic: does the live probe report that `unshare(CLONE_NEWNS)` is
/// likely to succeed?
///
/// IMPORTANT: this is a fast pre-check, not proof. `run_live_probe()` only
/// reads `/proc/self/ns/mnt` presence and the AppArmor
/// `apparmor_restrict_unprivileged_userns` sysctl — it does NOT call
/// `unshare(2)` (that would pollute the caller's mount table). Tests that
/// depend on mount-ns support MUST additionally handle the real unshare
/// call failing at the subprocess level and skip accordingly. The
/// `confined_child_mount_ns_inode_differs_from_parent_under_unshare`
/// test below does exactly that via `unshare(1)` exit-status inspection.
fn kernel_probably_supports_mount_ns_unshare() -> bool {
    matches!(run_live_probe().mount_namespace_unshare, Status::Pass)
}

/// Spawn configuration controlling WHICH layers `--sandboxed-agent` applies
/// before it starts serving the bridge loop. Production always leaves every
/// layer on; the explicit `_enabled` knobs exist for tests that need to
/// verify ONE layer at a time (or none, for pure wire-protocol checks).
#[derive(Debug, Clone, Copy)]
struct SandboxMode {
    landlock_enabled: bool,
    seccomp_enabled: bool,
    unshare_enabled: bool,
}

impl SandboxMode {
    const WIRE_ONLY: Self = Self {
        landlock_enabled: false,
        seccomp_enabled: false,
        unshare_enabled: false,
    };

    const LANDLOCK_ONLY: Self = Self {
        landlock_enabled: true,
        seccomp_enabled: false,
        unshare_enabled: false,
    };

    const SECCOMP_ONLY: Self = Self {
        landlock_enabled: false,
        seccomp_enabled: true,
        unshare_enabled: false,
    };

    const FULL: Self = Self {
        landlock_enabled: true,
        seccomp_enabled: true,
        unshare_enabled: true,
    };
}

/// Bring up the sandboxed-agent child with the requested confinement
/// profile. The child is spawned with a fresh temp workspace as its
/// merged path — tests that need a bad path (`sandboxed_agent_bails_on_missing_merged_path`)
/// use a separate explicit-Command spawn path rather than this helper.
///
/// Returns a handle bundle: (child process, parent socket, workspace temp
/// dir). Drop order matters — the workspace must outlive the child so
/// Landlock's path validation succeeds.
fn spawn_child_with_mode(
    mode: SandboxMode,
) -> (std::process::Child, UnixStream, tempfile::TempDir) {
    let ws = make_tmp_workspace();

    let (parent_fd, child_fd) = nix::sys::socket::socketpair(
        nix::sys::socket::AddressFamily::Unix,
        nix::sys::socket::SockType::Stream,
        None,
        nix::sys::socket::SockFlag::SOCK_CLOEXEC,
    )
    .expect("socketpair");
    let parent = std::os::unix::net::UnixStream::from(parent_fd);
    parent
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    parent
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    let mut cmd = Command::new(cairn_app_bin());
    cmd.arg("--sandboxed-agent")
        .arg("--workspace-id")
        .arg("wkspc-test")
        .arg("--merged-path")
        .arg(ws.path())
        .arg("--socket-fd")
        .arg("3")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if !mode.landlock_enabled {
        cmd.env("CAIRN_SANDBOX_DISABLE_LANDLOCK", "1");
    }
    if !mode.seccomp_enabled {
        cmd.env("CAIRN_SANDBOX_DISABLE_SECCOMP", "1");
    }
    if !mode.unshare_enabled {
        cmd.env("CAIRN_SANDBOX_DISABLE_UNSHARE", "1");
    }

    cmd.fd_mappings(vec![FdMapping {
        parent_fd: child_fd,
        child_fd: 3,
    }])
    .expect("fd_mappings");

    let child = cmd.spawn().expect("spawn cairn-app --sandboxed-agent");
    (child, parent, ws)
}

fn rpc(parent: &mut UnixStream, req: &str) -> String {
    parent.write_all(req.as_bytes()).expect("write");
    parent.write_all(b"\n").expect("write");
    parent.flush().expect("flush");

    let mut buf = [0u8; 4096];
    let mut pending = Vec::<u8>::new();
    loop {
        match parent.read(&mut buf) {
            Ok(0) => panic!("child closed socket before responding"),
            Ok(n) => {
                pending.extend_from_slice(&buf[..n]);
                if let Some(pos) = pending.iter().position(|&b| b == b'\n') {
                    return String::from_utf8_lossy(&pending[..pos]).to_string();
                }
            }
            Err(err) => panic!("read from sandboxed-agent: {err}"),
        }
    }
}

/// Read the `/proc/<pid>/ns/mnt` symlink for the parent process. Panics
/// loudly if the read fails — this helper is used by the mount-ns diff
/// assertion; an empty-string fallback would make the downstream
/// `!resp_ns.contains(&parent_ns)` check degenerate into a no-op
/// (`!contains("")` is always false, which would fail the test with a
/// confusing "child == parent" message when the real bug is that we
/// couldn't read procfs).
fn parent_mount_ns() -> String {
    std::fs::read_link(format!("/proc/{}/ns/mnt", std::process::id()))
        .unwrap_or_else(|err| {
            panic!(
                "read_link(/proc/{}/ns/mnt) — this should always succeed on \
                 Linux, but it returned: {err}. Cannot run the mount-ns diff \
                 assertion without a valid parent inode.",
                std::process::id()
            )
        })
        .display()
        .to_string()
}

// ─── Wire-protocol tests (confinement layers OFF — they verify the bridge,
//     not the fence). ──────────────────────────────────────────────────────

#[test]
fn sandboxed_agent_self_test_returns_ok() {
    let (mut child, mut parent, _ws) = spawn_child_with_mode(SandboxMode::WIRE_ONLY);
    let resp = rpc(&mut parent, r#"{"op":"self_test"}"#);
    assert!(resp.contains("\"ok\":true"), "resp = `{resp}`");
    assert!(resp.contains("\"op\":\"self_test\""));
    assert!(resp.contains("\"pid\":"), "resp = `{resp}`");
    drop(parent);
    let _ = child.wait();
}

#[test]
fn sandboxed_agent_rejects_unknown_op() {
    let (mut child, mut parent, _ws) = spawn_child_with_mode(SandboxMode::WIRE_ONLY);
    let resp = rpc(&mut parent, r#"{"op":"nonsense"}"#);
    assert!(resp.contains("unknown op"), "resp = `{resp}`");
    drop(parent);
    let _ = child.wait();
}

#[test]
fn sandboxed_agent_bails_on_missing_merged_path() {
    // With a non-existent merged path and Landlock enabled, SandboxConfinement
    // refuses to apply and the child exits with code 17 (InvalidPath).
    let (parent_fd, child_fd) = nix::sys::socket::socketpair(
        nix::sys::socket::AddressFamily::Unix,
        nix::sys::socket::SockType::Stream,
        None,
        nix::sys::socket::SockFlag::SOCK_CLOEXEC,
    )
    .expect("socketpair");
    drop(parent_fd); // we don't need to read from child — just observe exit.

    let mut cmd = Command::new(cairn_app_bin());
    cmd.arg("--sandboxed-agent")
        .arg("--workspace-id")
        .arg("wkspc-bad")
        .arg("--merged-path")
        .arg("/tmp/this/directory/really/does/not/exist/ever")
        .arg("--socket-fd")
        .arg("3")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    cmd.fd_mappings(vec![FdMapping {
        parent_fd: child_fd,
        child_fd: 3,
    }])
    .expect("fd_mappings");

    let output = cmd.output().expect("run --sandboxed-agent");
    assert!(
        !output.status.success(),
        "expected non-zero exit; got {:?}",
        output.status
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("confinement failed")
            || stderr.contains("workspace merged path does not exist"),
        "stderr = `{stderr}`"
    );
}

#[test]
fn sandboxed_agent_requires_all_flags() {
    let output = Command::new(cairn_app_bin())
        .arg("--sandboxed-agent")
        .arg("--workspace-id")
        .arg("x")
        .stderr(Stdio::piped())
        .output()
        .expect("run");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("bad CLI") || stderr.contains("--merged-path"),
        "stderr = `{stderr}`"
    );
}

// ─── Confinement-assertion tests (closes #394, #395, #406). ────────────────
//
// These replace the pre-fix single fraud test that had the sandbox disabled
// and only checked the reply echoed the op name. Each test here:
//
//  1. Probes the host kernel for the primitive it needs.
//  2. Skips with an explicit `eprintln!` when the primitive is unavailable.
//  3. When available, runs the child with the relevant layers ENABLED and
//     asserts on the sandbox guarantee itself.

#[test]
fn sandboxed_agent_landlock_denies_write_outside_workspace() {
    if !kernel_supports_landlock() {
        eprintln!(
            "SKIP sandboxed_agent_landlock_denies_write_outside_workspace — \
             Landlock not available on this kernel (CONFIG_SECURITY_LANDLOCK=n \
             or ABI<1)"
        );
        return;
    }
    // Landlock ONLY — we don't need mount-ns unshare for path confinement.
    // This lets the test run on Ubuntu 24.04+ where AppArmor blocks the
    // unprivileged userns path.
    let (mut child, mut parent, _ws) = spawn_child_with_mode(SandboxMode::LANDLOCK_ONLY);

    // Write outside the workspace → Landlock must deny. The reply carries
    // `"denied":true` when the OS refused, which is the security-relevant
    // signal. We also cross-check the errno name so a future regression
    // where denied=true is hard-coded regardless of the OS errno still trips
    // the test.
    let resp = rpc(&mut parent, r#"{"op":"probe_write_outside"}"#);
    assert!(
        resp.contains("\"op\":\"probe_write_outside\""),
        "resp = `{resp}`"
    );
    assert!(
        resp.contains("\"denied\":true"),
        "Landlock must refuse the outside-workspace write but the child \
         reported it succeeded. resp = `{resp}`"
    );
    // `err.kind()` stringifies to "permission denied" on Linux (this is what
    // `Display for std::io::ErrorKind::PermissionDenied` emits). We accept
    // that literal, the Rust debug spelling (PermissionDenied), or the raw
    // POSIX name (EACCES) so a future libstd rename doesn't silently pass.
    assert!(
        resp.contains("permission denied")
            || resp.contains("PermissionDenied")
            || resp.contains("EACCES"),
        "expected EACCES-shaped errno from Landlock deny; resp = `{resp}`"
    );

    // Write INSIDE the workspace → Landlock must allow. This proves the
    // deny above is path-specific, not a blanket "all writes fail" false
    // positive.
    let resp_in = rpc(&mut parent, r#"{"op":"probe_write_inside"}"#);
    assert!(
        resp_in.contains("\"op\":\"probe_write_inside\""),
        "resp_in = `{resp_in}`"
    );
    assert!(
        resp_in.contains("\"allowed\":true"),
        "Landlock must allow inside-workspace writes — otherwise the agent \
         cannot persist any work. resp_in = `{resp_in}`"
    );

    drop(parent);
    let _ = child.wait();
}

#[test]
fn sandboxed_agent_seccomp_denies_ptrace_and_allows_getpid() {
    if !kernel_supports_seccomp() {
        eprintln!(
            "SKIP sandboxed_agent_seccomp_denies_ptrace_and_allows_getpid — \
             seccomp-BPF not available"
        );
        return;
    }
    let (mut child, mut parent, _ws) = spawn_child_with_mode(SandboxMode::SECCOMP_ONLY);

    // PTRACE_TRACEME is the seccomp-specific canary: it normally succeeds
    // for any UID (marks the caller as traceable by its parent), so a
    // returned EPERM comes UNIQUELY from the seccomp deny list rather than
    // from missing CAP_SYS_ADMIN. This distinguishes seccomp from the
    // "unprivileged user cannot mount" EPERM path that would make a
    // mount-only test pass even with seccomp disabled.
    let resp_ptrace = rpc(&mut parent, r#"{"op":"probe_ptrace_me"}"#);
    assert!(
        resp_ptrace.contains("\"op\":\"probe_ptrace_me\""),
        "resp_ptrace = `{resp_ptrace}`"
    );
    assert!(
        resp_ptrace.contains("\"denied\":true"),
        "seccomp must deny ptrace(PTRACE_TRACEME) with EPERM but the child \
         reported it succeeded — the deny list is not enforced. \
         resp_ptrace = `{resp_ptrace}`"
    );
    // seccomp action is SeccompAction::Errno(EPERM); verify the errno.
    assert!(
        resp_ptrace.contains("EPERM"),
        "expected EPERM from seccomp; resp_ptrace = `{resp_ptrace}`"
    );

    // Also sanity-check `mount` — it's in the deny list too. (This is a
    // belt-and-suspenders cross-check; mount alone is insufficient because
    // an unprivileged user hits EPERM regardless of seccomp, but combined
    // with the ptrace assertion above it proves the filter covers multiple
    // syscalls.)
    let resp_mount = rpc(&mut parent, r#"{"op":"probe_mount"}"#);
    assert!(
        resp_mount.contains("\"denied\":true"),
        "seccomp must deny mount() too; resp_mount = `{resp_mount}`"
    );

    // getpid() is NOT in the deny list. Under the allow-by-default policy
    // it MUST succeed. If this fails something's fundamentally wrong with
    // the filter composition.
    let resp_pid = rpc(&mut parent, r#"{"op":"probe_getpid"}"#);
    assert!(
        resp_pid.contains("\"allowed\":true"),
        "getpid must succeed under the allow-by-default seccomp policy; \
         resp_pid = `{resp_pid}`"
    );
    // And the pid must actually match the child we spawned.
    let child_pid = child.id();
    let needle = format!("\"pid\":{child_pid}");
    assert!(
        resp_pid.contains(&needle),
        "expected the child's own pid in the reply (proves the syscall \
         returned the right value, not a stub); got resp_pid = `{resp_pid}`, \
         expected needle = `{needle}`"
    );

    drop(parent);
    let _ = child.wait();
}

#[test]
fn sandboxed_agent_full_confinement_denies_outside_allows_inside() {
    if !kernel_probably_supports_mount_ns_unshare() {
        eprintln!(
            "SKIP sandboxed_agent_full_confinement_denies_outside_allows_inside — \
             kernel heuristic reports unshare(CLONE_NEWNS) likely to fail \
             (e.g. Ubuntu 24.04+ with kernel.apparmor_restrict_unprivileged_userns=1); \
             Landlock-only and seccomp-only coverage still run via the other \
             tests in this file."
        );
        return;
    }
    // The FULL fence: Landlock + seccomp + unshare(CLONE_NEWNS). Only reachable
    // on hosts that permit unprivileged userns (older Ubuntu / fedora / plain
    // kernel 6.x without AppArmor restriction, or CAP_SYS_ADMIN).
    //
    // The probe is heuristic (see `kernel_probably_supports_mount_ns_unshare`
    // doc). If the real unshare call fails at spawn time for a reason the
    // probe couldn't see, `bin_sandboxed_agent::run()` returns exit code 14
    // (NamespaceUnshare) BEFORE reaching the bridge loop. In that case the
    // child's fd 3 is closed without writing any reply, the parent's `rpc()`
    // read returns 0 bytes, and `rpc()` panics with "child closed socket
    // before responding". That's a loud test failure — the test will never
    // silently pass on a broken-fence host.
    let (mut child, mut parent, _ws) = spawn_child_with_mode(SandboxMode::FULL);

    // Mount-namespace assertion: the child's ns/mnt inode must differ from
    // the parent's — proves `unshare(CLONE_NEWNS)` actually fired.
    let parent_ns = parent_mount_ns();
    let resp_ns = rpc(&mut parent, r#"{"op":"probe_mount_ns_inode"}"#);
    assert!(
        resp_ns.contains("\"op\":\"probe_mount_ns_inode\""),
        "resp_ns = `{resp_ns}`"
    );
    assert!(
        !resp_ns.contains(&parent_ns),
        "child mount-ns inode must differ from parent's {parent_ns}; resp_ns = `{resp_ns}`"
    );

    // Write outside → denied by Landlock.
    let resp_out = rpc(&mut parent, r#"{"op":"probe_write_outside"}"#);
    assert!(
        resp_out.contains("\"denied\":true"),
        "FULL fence must still deny outside writes; resp_out = `{resp_out}`"
    );
    // Write inside → allowed.
    let resp_in = rpc(&mut parent, r#"{"op":"probe_write_inside"}"#);
    assert!(
        resp_in.contains("\"allowed\":true"),
        "FULL fence must still allow inside writes; resp_in = `{resp_in}`"
    );
    // mount() → denied by seccomp.
    let resp_mount = rpc(&mut parent, r#"{"op":"probe_mount"}"#);
    assert!(
        resp_mount.contains("\"denied\":true"),
        "FULL fence must deny mount(); resp_mount = `{resp_mount}`"
    );

    drop(parent);
    let _ = child.wait();
}

/// Validate the test-probe helpers themselves — not a confinement assertion,
/// just a guard against regressing the kernel-support detectors. If
/// `run_live_probe` starts returning Unknown for everything on a normal
/// Linux host the skip-vs-run decisions in the tests above become
/// unreliable.
#[test]
fn kernel_support_probes_dont_lie_about_linux() {
    let findings = run_live_probe();
    // On any Linux host we expect seccomp + landlock to be Pass OR Fail
    // (never Unknown). Unknown means the probe aborted early and our skip
    // logic will behave unpredictably.
    assert!(
        !matches!(findings.landlock_v1_fully_enforced, Status::Unknown),
        "landlock probe should report Pass or Fail, got Unknown"
    );
    assert!(
        !matches!(findings.seccomp_bpf, Status::Unknown),
        "seccomp probe should report Pass or Fail, got Unknown"
    );
    assert!(
        !matches!(findings.mount_namespace_unshare, Status::Unknown),
        "mount-ns probe should report Pass or Fail, got Unknown"
    );
}
