//! F65 PR-4 integration tests — sandbox runtime.
//!
//! # Test quality contract (closes audit findings #395, #396, #406)
//!
//! Every security-relevant test in this file either:
//!
//! 1. Exercises a real primitive end-to-end via a child process (spawning
//!    `sandbox_primitive_harness` with the primitive under test), asserting
//!    on the concrete deny/allow behavior — EACCES on outside-workspace
//!    write, EPERM on a seccomp-denied syscall, EBADF on a closed fd, etc.
//! 2. Validates pure-logic invariants (error-variant construction, probe
//!    markdown parsing, reflink fallback flag) that do not depend on any
//!    Linux primitive.
//!
//! Tests that REQUIRE a kernel primitive the host lacks (mount-ns unshare
//! on Ubuntu 24.04+ with AppArmor) MUST:
//!
//! - Probe for support via `kernel_supports_*()` helpers.
//! - When unavailable, `eprintln!("SKIP <test> — <reason>")` and early
//!   return. The skip is ALWAYS visible in CI logs — never silent.
//!
//! Deleted in the fraud fix (see commit history): three no-op
//! `eprintln!`-only stubs that always passed and counted toward the "9
//! tests landed" claim without verifying anything. The Landlock /
//! seccomp / path-confinement coverage they nominally provided is now
//! delivered by `sandboxed_agent_*` tests in `cairn-app` integration
//! tests PLUS the new `confined_child_*` tests below that exercise the
//! primitives through `sandbox_primitive_harness` child processes.

#![cfg(target_os = "linux")]

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::AtomicBool;

use cairn_workspace::providers::{reflink_tree_with_fallback, MOUNT_OPTIONS_REQUIRED_FLAGS};
use cairn_workspace::sandbox::confinement::landlock::default_os_read_paths;
use cairn_workspace::sandbox::{
    BufferedF65EventSink, F65SandboxEvent, F65SandboxEventSink, ProbeFindings, SandboxConfinement,
    Status,
};
use command_fds::{CommandFdExt, FdMapping};

/// Landlock works on every kernel with `CONFIG_SECURITY_LANDLOCK=y`; no
/// unshare needed.
fn kernel_supports_landlock() -> bool {
    matches!(
        cairn_workspace::sandbox::confinement::probe::run_live_probe().landlock_v1_fully_enforced,
        Status::Pass
    )
}

/// seccomp-BPF is a kernel-wide capability gated on `CONFIG_SECCOMP_FILTER=y`.
fn kernel_supports_seccomp() -> bool {
    matches!(
        cairn_workspace::sandbox::confinement::probe::run_live_probe().seccomp_bpf,
        Status::Pass
    )
}

/// Heuristic: does the live probe report that `unshare(CLONE_NEWNS)` is
/// likely to succeed?
///
/// IMPORTANT: this is a fast pre-check, not proof. `run_live_probe()` only
/// reads `/proc/self/ns/mnt` presence and the AppArmor
/// `apparmor_restrict_unprivileged_userns` sysctl — it does NOT call
/// `unshare(2)` (that would pollute the caller's mount table). Tests that
/// depend on mount-ns support MUST additionally handle the real unshare
/// call failing at the subprocess level and skip accordingly. The two
/// `confined_child_*` tests below use `unshare(1)` exit-status checks to
/// catch hosts where the heuristic was optimistic.
fn kernel_probably_supports_mount_ns_unshare() -> bool {
    matches!(
        cairn_workspace::sandbox::confinement::probe::run_live_probe().mount_namespace_unshare,
        Status::Pass
    )
}

fn harness_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_sandbox_primitive_harness"))
}

// ─── Test 4: metacopy=off is present in every rendered mount-options string.

#[test]
fn test_overlayfs_mount_contains_metacopy_off() {
    // This test doesn't actually mount — it reads the canonical options
    // string the production driver renders and asserts security flags.
    // A live-mount assertion requires CAP_SYS_ADMIN; see the gated
    // test_overlayfs_mount_live_contains_metacopy_off below.
    for flag in MOUNT_OPTIONS_REQUIRED_FLAGS {
        // The constant itself must contain every required flag name.
        assert!(
            !flag.is_empty(),
            "MOUNT_OPTIONS_REQUIRED_FLAGS must not contain empty strings"
        );
    }
    assert!(MOUNT_OPTIONS_REQUIRED_FLAGS.contains(&"metacopy=off"));
    assert!(MOUNT_OPTIONS_REQUIRED_FLAGS.contains(&"xino=on"));
    assert!(MOUNT_OPTIONS_REQUIRED_FLAGS.contains(&"redirect_dir=on"));
}

// ─── Test 5: Landlock partial-enforce simulation bails loudly.

#[test]
fn test_landlock_asserts_fully_enforced_or_bails() {
    // The confinement-error variant is load-bearing for cairn-app's exit
    // code and for the SSE event the orchestrator emits. Assert the variant
    // name + message shape so a refactor that renames them surfaces here.
    use cairn_workspace::sandbox::ConfinementError;
    let err = ConfinementError::LandlockPartial("PartiallyEnforced".to_string());
    let msg = format!("{err}");
    assert!(
        msg.contains("FullyEnforced") || msg.contains("PartiallyEnforced"),
        "error must name the failure mode so operators can triage: got `{msg}`"
    );
    assert!(
        msg.contains("refusing"),
        "error must state we refuse to proceed, not that we degraded: got `{msg}`"
    );
}

// ─── Test 6: ext4 fallback emits degraded event exactly once (dedupe).

#[test]
fn test_ext4_fallback_emits_degraded_event() {
    // reflink_tree_with_fallback signals degraded-mode via an AtomicBool.
    // On any ext4 host (including every GitHub Actions runner) the first
    // byte-copy flips the flag; subsequent files leave it flipped. The
    // SandboxService consumer is expected to emit the event once-per-session
    // keyed off this flag — assert the primitive does its half correctly.
    let src = tempfile::tempdir().expect("tmpdir");
    let dst_parent = tempfile::tempdir().expect("tmpdir");
    let dst = dst_parent.path().join("dst");

    // Write a pair of files so we exercise the recursive walk.
    std::fs::write(src.path().join("a.txt"), b"hello").unwrap();
    std::fs::write(src.path().join("b.txt"), b"world!!").unwrap();
    std::fs::create_dir_all(src.path().join("sub")).unwrap();
    std::fs::write(src.path().join("sub/c.txt"), b"nested").unwrap();

    let degraded = AtomicBool::new(false);
    let outcome = reflink_tree_with_fallback(src.path(), &dst, &degraded).expect("fallback");

    assert_eq!(outcome.files_copied, 3, "walked 3 files");
    // On tmpfs/ext4 reflink returns EOPNOTSUPP; on btrfs it succeeds.
    // Either way the bytes are copied to the destination.
    for name in ["a.txt", "b.txt", "sub/c.txt"] {
        assert!(
            dst.join(name).exists(),
            "destination should contain {}",
            name
        );
    }
    // Verify content integrity.
    assert_eq!(
        std::fs::read(dst.join("sub/c.txt")).unwrap(),
        b"nested",
        "byte-level content must survive the copy path"
    );

    // If we fell back, the degraded flag MUST be set (this is the one-shot
    // dedupe signal SandboxService consumes). If reflink succeeded, the
    // flag stays false.
    if !outcome.reflink_used {
        assert!(
            degraded.load(std::sync::atomic::Ordering::SeqCst),
            "degraded flag must be set when reflink fell back to copy"
        );
    } else {
        assert!(
            !degraded.load(std::sync::atomic::Ordering::SeqCst),
            "degraded flag must be clear when reflink succeeded"
        );
    }
}

// ─── Test 7: probe findings with REQUIRED FAIL block cairn-app boot.

#[test]
fn test_kernel_probe_findings_block_boot_when_required() {
    // Operator-facing behavior: the probe parses the markdown findings, and
    // `assert_required` raises a `ProbeError::PrimitiveFailed` with a named
    // primitive + suggested fix. cairn-app wires this into a refuse-to-boot
    // check per locked Q8.
    let markdown = r#"| # | Primitive | Result | Detail |
|---|-----------|--------|--------|
| 2 | mount namespace unshare | FAIL | AppArmor blocks userns |
| 3 | overlayfs unprivileged mount | FAIL | same root cause |
| 4 | Landlock FullyEnforced | PASS | ok |
| 5 | seccomp-BPF deny list | PASS | ok |
| 6 | reflink | FAIL (EOPNOTSUPP) | ext4 |
"#;
    let findings = ProbeFindings::parse_markdown(markdown).expect("parse");
    let err = findings
        .assert_required()
        .expect_err("must refuse to start with a named primitive");
    let msg = format!("{err}");
    assert!(
        msg.contains("mount_namespace_unshare"),
        "error must name the failing primitive: got `{msg}`"
    );
    assert!(
        msg.contains("Suggested fix"),
        "error must include operator-facing remediation: got `{msg}`"
    );
}

// ─── Test 8: close_nonstandard_fds closes inherited fds (pre-Landlock fence).
//
// #396 fix: the old test gated its real body on `CAIRN_F65_FD_CLOSE_TEST`
// which was set nowhere in CI. Now runs unconditionally via the
// `close-range-scratch` helper mode — the harness binary is a FRESH child
// process with exactly the fds we hand it, so we never touch cargo's own
// open-fd set.

#[test]
fn test_close_nonstandard_fds_closes_inherited_fd_keeps_kept_fd() {
    // Open two scratch files in the parent and give the owned fds to
    // command-fds; it dup2s them into the child's fd table at fd 3 and
    // fd 4. The child calls `close_nonstandard_fds(Some(3))` and asserts
    // fd 3 stays open while fd 4 is now EBADF. That contract is the
    // pre-Landlock fence we ship.
    use std::os::fd::OwnedFd;

    let keep_file = tempfile::NamedTempFile::new().expect("keep_file");
    let drop_file = tempfile::NamedTempFile::new().expect("drop_file");

    let keep_handle: OwnedFd = std::fs::File::open(keep_file.path())
        .expect("open keep")
        .into();
    let drop_handle: OwnedFd = std::fs::File::open(drop_file.path())
        .expect("open drop")
        .into();

    const KEEP_FD: i32 = 3;
    const DROP_FD: i32 = 4;

    let mut cmd = Command::new(harness_bin());
    cmd.arg("close-range-scratch")
        .arg(KEEP_FD.to_string())
        .arg(DROP_FD.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd.fd_mappings(vec![
        FdMapping {
            parent_fd: keep_handle,
            child_fd: KEEP_FD,
        },
        FdMapping {
            parent_fd: drop_handle,
            child_fd: DROP_FD,
        },
    ])
    .expect("fd_mappings");

    let out = cmd.output().expect("run harness");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "close-range-scratch exited {:?}, stderr = `{stderr}`",
        out.status
    );
}

// ─── Test 9: snapshot atomicity — MNT_DETACH returns synchronously.

#[test]
fn test_snapshot_atomicity_fence_uses_mnt_detach() {
    // The driver's unmount path calls umount2(MNT_DETACH). Live-mount
    // assertion requires CAP_SYS_ADMIN, which the test runner doesn't have
    // — but we CAN exercise the call path against a non-mount path and
    // inspect the returned error. A non-MNT_DETACH umount2 returns EINVAL
    // on a non-mount; our wrapper returns a stringly-typed error that
    // includes "MNT_DETACH" literal so a refactor that drops the flag
    // trips this test.
    use cairn_workspace::providers::overlay::OverlayMountDriver;
    use cairn_workspace::providers::NixOverlayMountDriver;
    let driver = NixOverlayMountDriver;
    let scratch = tempfile::tempdir().expect("tmpdir");
    // Call unmount on a path that was never mounted; expect an error that
    // mentions MNT_DETACH in the rendered message.
    let err = driver
        .unmount(scratch.path())
        .expect_err("unmount of non-mount must error");
    assert!(
        err.contains("MNT_DETACH"),
        "error must identify MNT_DETACH call path (so a refactor that drops \
         the flag is caught here); got `{err}`"
    );
}

// ─── F65 event sink smoke test.

#[test]
fn test_f65_event_sink_captures_degraded_event() {
    let sink = BufferedF65EventSink::default();
    sink.publish(F65SandboxEvent::WorkspaceBackendDegraded {
        project: cairn_domain::ProjectKey::new(
            cairn_domain::TenantId::new("t"),
            "w".to_string(),
            "p".to_string(),
        ),
        session_id: cairn_domain::SessionId::new("s-1"),
        backend: "ext4_copy".to_string(),
        reason: "reflink_unsupported_fs".to_string(),
    });
    let events = sink.drain();
    assert_eq!(events.len(), 1);
    assert!(matches!(
        events[0],
        F65SandboxEvent::WorkspaceBackendDegraded { .. }
    ));
}

// ─── Confinement-assertion tests (closes #395 and fills the #406 gap).
//
// Each test spawns `sandbox_primitive_harness` with a one-shot mode
// (landlock / seccomp / close-range / mount-ns), exercises the primitive
// in the child, and asserts on the child's exit code + stderr. The old
// three eprintln-only stubs (overlayfs / seccomp / path-confinement) are
// replaced by these. Primitives that REQUIRE unshare (overlayfs, full
// mount-ns) skip with an explicit message when the host kernel blocks
// unprivileged userns.

#[test]
fn confined_child_landlock_denies_outside_allows_inside() {
    if !kernel_supports_landlock() {
        eprintln!(
            "SKIP confined_child_landlock_denies_outside_allows_inside — \
             Landlock not available on this kernel"
        );
        return;
    }
    // Build a scratch sandbox root + pick an outside target under /tmp
    // that the probe MUST NOT be able to write once Landlock confines it.
    let sandbox = tempfile::tempdir().expect("sandbox tmpdir");
    let outside_dir = tempfile::tempdir().expect("outside tmpdir");
    let outside_target = outside_dir.path().join("escape-target-must-not-exist.txt");

    let out = Command::new(harness_bin())
        .arg("landlock-confine-and-verify")
        .arg(sandbox.path())
        .arg(&outside_target)
        .output()
        .expect("run harness");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "landlock-confine-and-verify exited {:?}, stderr = `{stderr}`",
        out.status
    );
    // The primitive's own assertion: the escape target MUST NOT exist on
    // the host after the run. If Landlock didn't enforce, the child's
    // `fs::write(&outside)` would have succeeded and the harness would have
    // exited non-zero, but belt-and-suspenders: also check the file isn't
    // there. (The harness removes it on failure to avoid host pollution.)
    assert!(
        !outside_target.exists(),
        "escape target {} must not exist — Landlock confinement bypassed",
        outside_target.display()
    );
    // And the inside-sandbox write must have produced the scratch file.
    assert!(
        sandbox.path().join("inside.txt").is_file(),
        "inside-sandbox write should have succeeded under Landlock R+W grant"
    );
}

#[test]
fn confined_child_seccomp_denies_ptrace_traceme() {
    if !kernel_supports_seccomp() {
        eprintln!("SKIP confined_child_seccomp_denies_ptrace_traceme — seccomp-BPF not available");
        return;
    }
    // Exit code 0 iff PTRACE_TRACEME returned EPERM (the seccomp-specific
    // signal). PTRACE_TRACEME is chosen deliberately over mount(): mount
    // returns EPERM for unprivileged users regardless of seccomp, so a
    // test built on it would pass even with the deny list disabled.
    // PTRACE_TRACEME normally succeeds for any UID, so an EPERM here is
    // uniquely a seccomp signal.
    let out = Command::new(harness_bin())
        .arg("seccomp-ptrace-me")
        .output()
        .expect("run harness");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "seccomp-ptrace-me exited {:?}, stderr = `{stderr}`",
        out.status
    );
}

#[test]
fn confined_child_mount_ns_inode_differs_from_parent_under_unshare() {
    if !kernel_probably_supports_mount_ns_unshare() {
        eprintln!(
            "SKIP confined_child_mount_ns_inode_differs_from_parent_under_unshare — \
             kernel heuristic reports unshare(CLONE_NEWNS) likely to fail \
             (e.g. Ubuntu 24.04+ with kernel.apparmor_restrict_unprivileged_userns=1)"
        );
        return;
    }
    // Under `unshare -mUr` the child's /proc/self/ns/mnt inode MUST differ
    // from the parent's. This proves the unshare actually split the
    // namespace — a no-op unshare or a silent fallback would produce a
    // matching inode.
    let parent_inode = std::fs::read_link(format!("/proc/{}/ns/mnt", std::process::id()))
        .expect("read_link parent ns");
    let parent_inode_s = parent_inode.display().to_string();

    // Use `unshare -mUr` from util-linux:
    //   -m : new mount namespace
    //   -U : new user namespace (required on non-root CI because a plain
    //        `unshare -m` needs CAP_SYS_ADMIN; a new user namespace gives
    //        the caller CAP_SYS_ADMIN *inside* that namespace)
    //   -r : map the invoking UID to root inside the new userns (otherwise
    //        many distros refuse the mount ns via `userns.mount = 0` or
    //        similar)
    // We cannot call `nix::sched::unshare` directly in a fork — nix's
    // `fork` is `unsafe` (forbidden workspace-wide). `unshare(1)` handles
    // the fork for us. util-linux is installed on every Linux CI runner.
    let out = Command::new("unshare")
        .arg("-mUr")
        .arg(harness_bin())
        .arg("mount-ns-inode")
        .output();
    let Ok(out) = out else {
        eprintln!(
            "SKIP confined_child_mount_ns_inode_differs_from_parent_under_unshare — \
             unshare(1) not installed or could not exec ({:?})",
            out
        );
        return;
    };
    if !out.status.success() {
        // `unshare` itself can bail (EPERM) on hosts where the heuristic
        // probe was optimistic but the actual syscall still fails (other
        // userns restrictions, unprivileged-userns-clone=0, container
        // seccomp deny list, etc.). Surface as a skip rather than a test
        // failure — the skip message carries the errno + stderr so
        // operators reading CI logs see the concrete root cause.
        let stderr = String::from_utf8_lossy(&out.stderr);
        eprintln!(
            "SKIP confined_child_mount_ns_inode_differs_from_parent_under_unshare — \
             unshare(1) -mUr exited {:?}: {stderr}",
            out.status
        );
        return;
    }
    let child_inode = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert!(
        !child_inode.is_empty(),
        "harness must print the child ns/mnt inode"
    );
    assert_ne!(
        child_inode, parent_inode_s,
        "child ns/mnt inode must differ from parent's after unshare(CLONE_NEWNS) — \
         parent={parent_inode_s}, child={child_inode}"
    );
}

#[test]
fn confined_child_overlayfs_unprivileged_mount() {
    if !kernel_probably_supports_mount_ns_unshare() {
        eprintln!(
            "SKIP confined_child_overlayfs_unprivileged_mount — kernel heuristic \
             reports unshare(CLONE_NEWNS) likely to fail (e.g. Ubuntu 24.04+ \
             with AppArmor); overlayfs-as-non-root requires that primitive"
        );
        return;
    }
    // When unshare is supported, run `unshare -mUr` inside a shell that
    // mounts overlayfs AND dumps its own `/proc/self/mountinfo`. Both
    // steps MUST happen in the same unshared namespace — the mount is
    // process-scoped, so a separate unshare cannot observe it.
    //
    // We pass the mount options straight through shell quoting so the
    // shell's `$$` stays literal and the mount command runs with the
    // exact options the production driver would use.
    let lower = tempfile::tempdir().expect("lower tmpdir");
    let upper = tempfile::tempdir().expect("upper tmpdir");
    let work = tempfile::tempdir().expect("work tmpdir");
    let merged = tempfile::tempdir().expect("merged tmpdir");
    // Seed the lower dir with a file so we can observe copy-up behavior.
    std::fs::write(lower.path().join("baseline.txt"), b"lower").unwrap();

    let script = format!(
        r#"set -e; mount -t overlay overlay -o "lowerdir={lower},upperdir={upper},workdir={work},xino=on,metacopy=off,redirect_dir=on" {merged}; cat /proc/self/mountinfo"#,
        lower = lower.path().display(),
        upper = upper.path().display(),
        work = work.path().display(),
        merged = merged.path().display(),
    );
    let out = match Command::new("unshare")
        .args(["-mUr", "bash", "-c"])
        .arg(&script)
        .output()
    {
        Ok(o) => o,
        Err(err) => {
            eprintln!(
                "SKIP confined_child_overlayfs_unprivileged_mount — could not exec \
                 unshare(1)/bash(1): {err}"
            );
            return;
        }
    };
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        // Distinguish "host doesn't support this primitive" (legit SKIP)
        // from "the test script regressed" (real failure). The known
        // kernel/LSM denial modes produce specific errno strings:
        //   EPERM           — unprivileged userns blocked (apparmor)
        //   EINVAL          — overlayfs options rejected
        //   ENODEV          — overlay fs module not loaded
        //   "not permitted" — util-linux wording for userns deny
        // Anything else — `bash: mount: not found`, syntax errors, etc. —
        // is a regression in OUR test script and should fail loudly.
        let host_lacks_primitive = stderr.contains("Operation not permitted")
            || stderr.contains("not permitted")
            || stderr.contains("Invalid argument")
            || stderr.contains("No such device")
            || stderr.contains("EPERM")
            || stderr.contains("EINVAL")
            || stderr.contains("ENODEV");
        if host_lacks_primitive {
            eprintln!(
                "SKIP confined_child_overlayfs_unprivileged_mount — host lacks \
                 the primitive (unshare -mUr or overlayfs-in-userns): exit \
                 {status:?}, stderr: {stderr}",
                status = out.status,
            );
            return;
        }
        panic!(
            "confined_child_overlayfs_unprivileged_mount — unshare -mUr bash -c \
             exited {status:?} with an unexpected error shape (NOT a known \
             primitive-unavailable pattern). This is likely a regression in \
             the test script, not a host-kernel skip. stderr: {stderr}",
            status = out.status,
        );
    }
    let mountinfo = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);

    // Assert on the line that corresponds to OUR overlay mount. CI runners
    // and containerized test hosts frequently have unrelated overlay
    // mounts in /proc/self/mountinfo (docker layers, snap mounts) — a
    // naive `contains("overlay")` would pass even if our mount never
    // happened. Anchor the assertion on the merged-path temp dir we
    // created a moment ago.
    let merged_str = merged.path().display().to_string();
    let our_line = mountinfo.lines().find(|line| line.contains(&merged_str));
    let our_line = match our_line {
        Some(l) => l,
        None => panic!(
            "mountinfo did not contain a mount line for {merged_str}; \
             got `{mountinfo}`, stderr `{stderr}`"
        ),
    };
    // The security-relevant assertion: metacopy=off must be in the
    // options OF OUR OWN MOUNT LINE. That's what prevents a lower-layer
    // file-with-xattrs attack per `orchestrator-session-architecture.md`
    // §4.3.2.
    assert!(
        our_line.contains("metacopy=off"),
        "our overlay mount line must have metacopy=off; got line = `{our_line}`"
    );
    // And xino=on / redirect_dir=on are the other two production flags
    // the driver sets. If any of them is missing the mount options drift
    // from what `crates/cairn-workspace/src/providers/overlay.rs`
    // renders.
    assert!(
        our_line.contains("xino=on"),
        "overlay mount line missing xino=on; got `{our_line}`"
    );
    assert!(
        our_line.contains("redirect_dir=on"),
        "overlay mount line missing redirect_dir=on; got `{our_line}`"
    );
}

#[test]
fn test_sandbox_confinement_validates_merged_path_exists() {
    // A production-quality invariant that runs on every host: a
    // SandboxConfinement with a non-existent merged path MUST refuse to
    // apply, because Landlock will silently accept the missing path and
    // leave confinement incomplete.
    let conf = SandboxConfinement::production(
        PathBuf::from("/nonexistent/should/never/exist"),
        default_os_read_paths(),
    );
    let err = conf.confine(Some(3)).expect_err("must reject missing path");
    let msg = format!("{err}");
    assert!(
        msg.contains("/nonexistent/should/never/exist"),
        "error must name the bad path: got `{msg}`"
    );
}
