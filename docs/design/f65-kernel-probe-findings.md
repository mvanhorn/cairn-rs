# F65 kernel + filesystem probe findings

**Host:** `Linux ip-172-31-31-201 6.17.0-1010-aws #10~24.04.1-Ubuntu SMP Wed Mar 18 22:38:17 UTC 2026 aarch64 aarch64 aarch64 GNU/Linux`
**Run at:** 2026-04-28T02:20:42.593200382+00:00
**Probe binary:** `f65_kernel_probe` @ `9bf91de3`

## Summary

| # | Primitive | Result | Detail |
|---|-----------|--------|--------|
| 1 | Linux kernel >= 5.13 | PASS | 6.17.0-1010-aws — 6.17 ≥ 5.13 |
| 2 | mount namespace unshare | FAIL | Neither unshare strategy succeeded. |
| 3 | overlayfs unprivileged mount (xino=on, metacopy=off) | FAIL (exit 1) | child reported: write /proc/self/setgroups=deny before gid_map: Permission denied (os error 13) |
| 4 | Landlock FullyEnforced | PASS | restrict_self() returned RulesetStatus::FullyEnforced at ABI::V6; in-sandbox write succeeded and out-of-sandbox write was denied with EACCES |
| 5 | seccomp-BPF deny list | PASS | installed filter with 6 blocked syscalls; ptrace(PTRACE_TRACEME) returned EPERM as expected (not SIGSYS kill) |
| 6 | reflink (FICLONE) on /tmp | INFO (EOPNOTSUPP (95)) | FICLONE returned EOPNOTSUPP on /tmp (ext4) — expected on ext4 / tmpfs. PR-4 MUST use `reflink_copy::reflink_or_copy` (automatic copy fallback) and emit `WorkspaceBackendDegraded { reason: "ext4-fallback-full-copy" }` on first use. Operators who want the fast path provision btrfs or XFS-with-reflink EBS at the workspace root. |

## Per-primitive details

### 1. Linux kernel >= 5.13

- Required: yes
- Result: PASS
- Detail:
  6.17.0-1010-aws — 6.17 ≥ 5.13

### 2. mount namespace unshare

- Required: yes
- Result: FAIL
- Detail:
  Neither unshare strategy succeeded.
  - CLONE_NEWUSER|CLONE_NEWNS: exit 1: write /proc/self/setgroups=deny before gid_map: Permission denied (os error 13)
  - CLONE_NEWNS only: exit 1: unshare(CLONE_NEWNS) failed — requires CAP_SYS_ADMIN or an existing user namespace: EPERM: Operation not permitted
  Root cause (likely): /proc/sys/kernel/apparmor_restrict_unprivileged_userns = 1 — AppArmor transitions unconfined binaries into the `unprivileged_userns` profile on userns creation and denies CAP_SYS_ADMIN inside, which blocks every post-unshare mount operation.
  Actionable: for PR-4 on Ubuntu 24.04+, cairn must (a) ship an AppArmor profile that allows `userns_create` + CAP_SYS_ADMIN within the child userns for the cairn-app binary, or (b) run cairn-app under a systemd unit with `AmbientCapabilities=CAP_SYS_ADMIN`, or (c) disable the sysctl with `sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0` (weakens host security — use with care).

### 3. overlayfs unprivileged mount (xino=on, metacopy=off)

- Required: yes
- Result: FAIL (exit 1)
- Detail:
  child reported: write /proc/self/setgroups=deny before gid_map: Permission denied (os error 13)
  Root cause (likely): /proc/sys/kernel/apparmor_restrict_unprivileged_userns = 1 — AppArmor blocks CAP_SYS_ADMIN inside the fresh userns.
  Actionable: overlayfs mount requires (a) an unprivileged user namespace with working uid_map/gid_map (blocked by AppArmor policy above), or (b) CAP_SYS_ADMIN for the cairn-app process. See primitive #2 for the full remediation options.

### 4. Landlock FullyEnforced

- Required: yes
- Result: PASS
- Detail:
  restrict_self() returned RulesetStatus::FullyEnforced at ABI::V6; in-sandbox write succeeded and out-of-sandbox write was denied with EACCES

### 5. seccomp-BPF deny list

- Required: yes
- Result: PASS
- Detail:
  installed filter with 6 blocked syscalls; ptrace(PTRACE_TRACEME) returned EPERM as expected (not SIGSYS kill)

### 6. reflink (FICLONE) on /tmp

- Required: no
- Result: INFO (EOPNOTSUPP (95))
- Detail:
  FICLONE returned EOPNOTSUPP on /tmp (ext4) — expected on ext4 / tmpfs. PR-4 MUST use `reflink_copy::reflink_or_copy` (automatic copy fallback) and emit `WorkspaceBackendDegraded { reason: "ext4-fallback-full-copy" }` on first use. Operators who want the fast path provision btrfs or XFS-with-reflink EBS at the workspace root.

## Implications for PR-4

- **REQUIRED primitive FAILURE** — PR-4 is blocked until the listed failure is resolved on this host.
- Reflink unsupported on this deployment — PR-4 default path is `reflink-copy` (crate) with automatic byte-copy fallback. Emit `WorkspaceBackendDegraded { reason: "ext4-fallback-full-copy" }` on first use. Operators who want the fast path provision btrfs or XFS-with-reflink EBS at the workspace root (operator guidance only; no code change).
- cairn-app boot-time check MUST re-run the probe logic (or read this findings doc) and fail loud with a named error if any REQUIRED primitive regresses.
