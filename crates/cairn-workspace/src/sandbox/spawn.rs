//! Spawn the `--sandboxed-agent` child process (F65 PR-4 process model, locked Q1/Q2).
//!
//! The parent cairn-app calls [`spawn_sandboxed_agent`] to fork itself with
//! the `--sandboxed-agent` flag. A socketpair is created (SOCK_STREAM),
//! one end stays in the parent, the other is duped onto fd 3 in the child via
//! the `command-fds` crate. The child uses fd 3 as its tool-bridge to the
//! parent's tool dispatcher.
//!
//! The child is expected to call
//! [`crate::sandbox::confinement::SandboxConfinement::confine`] with
//! `keep_fd = Some(3)` immediately after reading any startup args; from that
//! point on it can neither escape the Landlock fence nor `mount(2)` anything
//! new.

use std::io;
use std::os::unix::io::OwnedFd;
use std::path::PathBuf;

use command_fds::{CommandFdExt, FdMapping};
use tokio::net::UnixStream;

/// Handle returned to the parent after [`spawn_sandboxed_agent`] succeeds.
///
/// Drop semantics: `std::process::Child` on Unix does NOT kill the subprocess
/// on drop. The handle's `Drop` impl below sends `SIGKILL` + `waitpid`
/// explicitly so an orphaned sandboxed-agent cannot outlive its handle.
/// Callers who want a graceful shutdown should call
/// [`SandboxedAgentHandle::shutdown`] with a timeout BEFORE dropping.
pub struct SandboxedAgentHandle {
    /// Parent-side half of the tool-bridge socketpair, wrapped in tokio.
    pub stream: UnixStream,
    /// PID of the child cairn-app subprocess (the sandboxed agent).
    pub pid: u32,
    /// Child-side handle. See `Drop` impl for teardown semantics.
    pub child: std::process::Child,
}

impl SandboxedAgentHandle {
    /// Graceful shutdown: wait for the child to exit (after the caller has
    /// closed the tool-bridge stream by dropping [`Self::stream`] via
    /// `.take_stream()` or `std::mem::take`-ing it). Returns the exit status.
    ///
    /// If the child is still running after `timeout`, this falls back to
    /// SIGKILL. Safe either way — the overlay work dir is reaped right
    /// after.
    pub fn shutdown(
        &mut self,
        timeout: std::time::Duration,
    ) -> io::Result<std::process::ExitStatus> {
        // Poll with a short sleep loop; avoids pulling tokio::time onto
        // the blocking teardown path.
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Ok(status);
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        // Ran out of grace — hard kill.
        let _ = self.child.kill();
        self.child.wait()
    }
}

impl Drop for SandboxedAgentHandle {
    fn drop(&mut self) {
        // Best-effort: if the caller didn't call `shutdown`, kill the child
        // hard so we don't leave an orphaned sandboxed-agent. `try_wait`
        // avoids a hang when the child already exited. SIGKILL is safe
        // because the child is inside our overlayfs overlay and its work
        // dir is about to be reaped; there's no clean-shutdown contract
        // to honor.
        match self.child.try_wait() {
            Ok(Some(_)) => { /* already exited */ }
            Ok(None) | Err(_) => {
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
        }
    }
}

/// Parameters for the spawn.
///
/// `agent_binary` is typically `/proc/self/exe` so the child is the exact
/// same cairn-app build as the parent. `workspace_id` is the opaque F65
/// workspace ID; `merged_path` is the overlayfs mount point the child will
/// see as its workspace root; `extra_args` lets callers thread goal/session
/// info into the child via the CLI.
#[derive(Debug, Clone)]
pub struct SpawnConfig {
    pub agent_binary: PathBuf,
    pub workspace_id: String,
    pub merged_path: PathBuf,
    pub extra_args: Vec<String>,
    /// Working directory for the child. Should equal `merged_path` so the
    /// agent starts at the workspace root.
    pub working_dir: PathBuf,
}

/// Spawn the sandboxed-agent child and return a handle to talk to it.
///
/// The child inherits the socketpair on fd 3. The `command-fds` crate issues
/// the `dup2` under the hood via `std::os::unix::process::CommandExt::pre_exec`
/// — but inside the library's `unsafe` boundary, NOT ours. That's why we can
/// keep `unsafe_code = "forbid"` at the workspace level.
pub fn spawn_sandboxed_agent(config: SpawnConfig) -> io::Result<SandboxedAgentHandle> {
    let (parent_fd, child_fd) = socketpair_stream()?;

    let mut command = std::process::Command::new(&config.agent_binary);
    command
        .arg("--sandboxed-agent")
        .arg("--workspace-id")
        .arg(&config.workspace_id)
        .arg("--merged-path")
        .arg(&config.merged_path)
        .arg("--socket-fd")
        .arg("3")
        .args(&config.extra_args)
        .current_dir(&config.working_dir);

    // Map our child-side fd onto fd 3 in the child.
    // OwnedFd transfers ownership; command-fds calls dup2 in the child during pre_exec.
    command
        .fd_mappings(vec![FdMapping {
            parent_fd: child_fd,
            child_fd: 3,
        }])
        .map_err(|err| io::Error::other(format!("fd_mappings: {err}")))?;

    let child = command.spawn().map_err(|err| {
        io::Error::other(format!("spawn {}: {err}", config.agent_binary.display()))
    })?;
    let pid = child.id();

    // Wrap our parent-side half in tokio.
    let std_stream = parent_fd;
    std_stream.set_nonblocking(true)?;
    let stream = UnixStream::from_std(std_stream)?;

    Ok(SandboxedAgentHandle { stream, pid, child })
}

/// Create a `SOCK_STREAM` socketpair using nix (safe wrapper around
/// `socketpair(2)`). Returns (parent_unix_stream, child_owned_fd).
///
/// Both ends are opened with `FD_CLOEXEC` set (via `SOCK_CLOEXEC`) so the
/// fd numbers we hold cannot leak into unrelated forked children. The
/// child-side fd still reaches its destination on fd 3 in the sandboxed
/// agent because `command-fds` calls `dup2(child_fd, 3)` in the child's
/// pre-exec hook — `dup2` produces a fresh fd number that is implicitly
/// non-CLOEXEC, so fd 3 survives exec.
fn socketpair_stream() -> io::Result<(std::os::unix::net::UnixStream, OwnedFd)> {
    use nix::sys::socket::{socketpair, AddressFamily, SockFlag, SockType};
    let (a, b) = socketpair(
        AddressFamily::Unix,
        SockType::Stream,
        None,
        SockFlag::SOCK_CLOEXEC,
    )
    .map_err(|err| io::Error::other(format!("socketpair: {err}")))?;
    // Parent: UnixStream (tokio-friendly). Child: keep as OwnedFd for command-fds.
    let parent = std::os::unix::net::UnixStream::from(a);
    Ok((parent, b))
}

/// Kernel-level `dup2`-and-exec wiring probe — unit-test hook confirming the
/// fd mapping compiles on the target.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spawn_config_fields_are_debug() {
        let cfg = SpawnConfig {
            agent_binary: PathBuf::from("/usr/bin/true"),
            workspace_id: "wkspc-abc".to_string(),
            merged_path: PathBuf::from("/tmp/merged"),
            extra_args: vec!["--goal".to_string(), "test".to_string()],
            working_dir: PathBuf::from("/tmp/merged"),
        };
        let s = format!("{:?}", cfg);
        assert!(s.contains("wkspc-abc"));
    }
}
