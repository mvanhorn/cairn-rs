//! Probe 3: unprivileged overlayfs mount with `xino=on`.
//!
//! Exercises the exact mount shape cairn's sandbox runtime will use per
//! root-Run:
//!
//! ```text
//! mount -t overlay overlay \
//!   -o lowerdir=<lower>,upperdir=<upper>,workdir=<work>,xino=on,metacopy=off \
//!   <merged>
//! ```
//!
//! Notes:
//!
//! * `xino=on` (kernel 5.9+) preserves inode stability across layers —
//!   git-inside-workspace relies on this.
//! * `metacopy=off` is explicit: the security research guide (§2.1 and
//!   §7 Rank 1) flags `metacopy=on` as a known privilege-escalation
//!   vector. The probe itself MUST NOT introduce the kernel vuln that
//!   PR-4 will be hardened against.
//! * The probe runs inside a fresh user + mount namespace so the mount is
//!   invisible to the parent process and automatically cleaned up when
//!   the child exits.

use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use nix::mount::{mount, MsFlags};
use nix::sched::{unshare, CloneFlags};
use nix::unistd::{getgid, getuid};
use tempfile::TempDir;

use crate::probes::mount_namespace::write_id_maps;
use crate::{spawn_child_probe, ProbeResult};

const NUMBER: u8 = 3;
const NAME: &str = "overlayfs unprivileged mount (xino=on, metacopy=off)";
const REQUIRED: bool = true;

pub fn probe() -> ProbeResult {
    let outcome = match spawn_child_probe("overlayfs") {
        Ok(o) => o,
        Err(err) => {
            return ProbeResult::fail(
                NUMBER,
                NAME,
                REQUIRED,
                None,
                format!("failed to spawn overlayfs child: {err:#}"),
            );
        }
    };

    if outcome.is_clean_zero() {
        ProbeResult::pass(
            NUMBER,
            NAME,
            REQUIRED,
            "overlayfs mount succeeded with xino=on + metacopy=off; merged dir contained expected file",
        )
    } else {
        let sig_hint = outcome.signal.map(|s| format!("signal {s}"));
        let code_hint = outcome.status_code.map(|c| format!("exit {c}"));
        let errno_hint = sig_hint.or(code_hint);
        let apparmor_hint = detect_apparmor_userns_restriction();
        ProbeResult::fail(
            NUMBER,
            NAME,
            REQUIRED,
            errno_hint,
            format!(
                "child reported: {}\n\
                 {apparmor_hint}\
                 Actionable: overlayfs mount requires (a) an unprivileged user namespace with \
                 working uid_map/gid_map (blocked by AppArmor policy above), or \
                 (b) CAP_SYS_ADMIN for the cairn-app process. See primitive #2 for the full \
                 remediation options.",
                if outcome.stderr.is_empty() {
                    "(no stderr)"
                } else {
                    outcome.stderr.as_str()
                }
            ),
        )
    }
}

/// Same detection the mount-namespace probe uses, duplicated here to keep
/// the two probes independent (each reports its own actionable root
/// cause without cross-reference).
fn detect_apparmor_userns_restriction() -> String {
    let path = "/proc/sys/kernel/apparmor_restrict_unprivileged_userns";
    match std::fs::read_to_string(path) {
        Ok(contents) if contents.trim() == "1" => format!(
            "Root cause (likely): {path} = 1 — AppArmor blocks CAP_SYS_ADMIN inside the fresh userns.\n"
        ),
        _ => String::new(),
    }
}

/// Child-scope body: build the overlayfs inputs in a tempdir, enter a
/// fresh user + mount namespace, mount the overlay, verify the merged
/// view shows the expected lowerdir file, then exit. Unmount is implicit
/// on process exit (mount namespace is destroyed with the child).
pub fn child_body() -> Result<()> {
    // Lay out a minimal overlay tree: one file in the lower layer, an
    // empty upper, and an empty work.
    let scratch = TempDir::new().context("create tempdir for overlay layers")?;
    let root = scratch.path();
    let lower = root.join("lower");
    let upper = root.join("upper");
    let work = root.join("work");
    let merged = root.join("merged");
    fs::create_dir(&lower).with_context(|| format!("mkdir {}", lower.display()))?;
    fs::create_dir(&upper).with_context(|| format!("mkdir {}", upper.display()))?;
    fs::create_dir(&work).with_context(|| format!("mkdir {}", work.display()))?;
    fs::create_dir(&merged).with_context(|| format!("mkdir {}", merged.display()))?;

    let lower_marker = lower.join("marker.txt");
    fs::write(&lower_marker, b"lower-layer-canary")
        .with_context(|| format!("write marker {}", lower_marker.display()))?;

    // Unprivileged userns + mount ns. Without the userns hop, the mount
    // syscall would need CAP_SYS_ADMIN. We also need a UID/GID map — see
    // `mount_namespace::write_id_maps` for the rationale.
    let original_uid = getuid().as_raw();
    let original_gid = getgid().as_raw();
    unshare(CloneFlags::CLONE_NEWUSER | CloneFlags::CLONE_NEWNS).context(
        "unshare(CLONE_NEWUSER|CLONE_NEWNS) failed — typically blocked by \
         kernel.apparmor_restrict_unprivileged_userns=1 on Ubuntu 24.04+",
    )?;
    write_id_maps(original_uid, original_gid)?;

    // Make the inherited mount tree private so nothing we do escapes.
    mount(
        None::<&str>,
        "/",
        None::<&str>,
        MsFlags::MS_REC | MsFlags::MS_PRIVATE,
        None::<&str>,
    )
    .context("mount(/, MS_PRIVATE|MS_REC) before overlayfs")?;

    let opts = build_overlay_options(&lower, &upper, &work);
    mount(
        Some("overlay"),
        &merged,
        Some("overlay"),
        MsFlags::empty(),
        Some(opts.as_os_str()),
    )
    .with_context(|| format!("mount overlayfs at {}", merged.display()))?;

    // Verify the merged view is wired correctly: reading the lower-layer
    // marker through the merged path must show the expected contents.
    let merged_marker = merged.join("marker.txt");
    let contents = fs::read_to_string(&merged_marker)
        .with_context(|| format!("read merged marker {}", merged_marker.display()))?;
    if contents != "lower-layer-canary" {
        return Err(anyhow::anyhow!(
            "merged marker had unexpected contents: {contents:?}"
        ));
    }

    // Intentionally do NOT umount — the mount namespace dies with the
    // child process and takes the overlay with it. `umount2(MNT_DETACH)`
    // here would also be fine but is unnecessary and adds a failure mode.
    Ok(())
}

fn build_overlay_options(lower: &PathBuf, upper: &PathBuf, work: &PathBuf) -> OsString {
    let mut s = OsString::new();
    s.push("lowerdir=");
    s.push(lower);
    s.push(",upperdir=");
    s.push(upper);
    s.push(",workdir=");
    s.push(work);
    // xino=on: stable cross-layer inode numbers (required for git).
    // metacopy=off: explicitly disable the known-vulnerable metadata-only
    // copy-up mode. See agent-knowledge/agentic-sandbox-architectures.md §2.1.
    s.push(",xino=on,metacopy=off");
    s
}

#[cfg(test)]
mod tests {
    use super::build_overlay_options;
    use std::ffi::OsString;
    use std::path::PathBuf;

    #[test]
    fn overlay_options_include_required_flags() {
        let opts = build_overlay_options(
            &PathBuf::from("/lower"),
            &PathBuf::from("/upper"),
            &PathBuf::from("/work"),
        );
        let s = opts.into_string().unwrap();
        assert!(s.contains("lowerdir=/lower"));
        assert!(s.contains("upperdir=/upper"));
        assert!(s.contains("workdir=/work"));
        assert!(s.contains("xino=on"));
        assert!(s.contains("metacopy=off"));
    }

    #[test]
    fn overlay_options_reject_metacopy_on() {
        // A future refactor must not accidentally flip metacopy on — this
        // asserts the string never contains the dangerous form.
        let opts = build_overlay_options(
            &PathBuf::from("/a"),
            &PathBuf::from("/b"),
            &PathBuf::from("/c"),
        );
        let s: OsString = opts;
        let s = s.into_string().unwrap();
        assert!(
            !s.contains("metacopy=on"),
            "metacopy=on is a known vuln surface; the probe must never enable it"
        );
    }
}
