//! F65 PR-4 session-scoped sandbox integration.
//!
//! This module is the surgical overlay on top of the pre-existing RFC 016/020
//! sandbox machinery (`SandboxService`, `OverlayProvider`, `ReflinkProvider`,
//! recovery registry). It adds:
//!
//! * [`SessionSandbox`] — the value returned to the orchestrator on a
//!   successful session-attempt provision. Carries the `merged` path the
//!   child agent will see as its workspace root, plus the per-attempt
//!   `upper` and `work` paths so teardown knows what to reflink and reap.
//! * [`F65SandboxEventSink`] — trait the orchestrator (cairn-app) implements
//!   to flow `SessionAttemptStarted` / `WorkspaceSnapshotCreated` /
//!   `WorkspaceBackendDegraded` events into the `EventLog`. cairn-workspace
//!   stays ignorant of the store (locked Q9).
//! * [`NetworkPolicy`] — per-session network-namespace policy. Locked Q7:
//!   default `Shared` (agent inherits host netns so cargo/npm/pip work);
//!   `Isolated` enables `CLONE_NEWNET` + loopback only.
//!
//! The actual provision/terminate methods live in `service.rs` as
//! `provision_for_session` / `terminate_for_session` — this module hosts
//! the types they use.

use std::path::PathBuf;
use std::sync::Arc;

use cairn_domain::{ProjectKey, RunId, SessionId, WorkspaceId, WorkspaceSnapshotId};

use crate::sandbox::{SandboxConfinement, SandboxPolicy};

/// Network-namespace policy per session.
///
/// Locked Q7: default `Shared` because agents need to install toolchain deps
/// (cargo, pip, npm). Operators can lock it down per-session via
/// `SandboxPolicy.network = Isolated`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkPolicy {
    /// Child agent shares the parent's network namespace. The default.
    #[default]
    Shared,
    /// Child agent gets its own network namespace with loopback only.
    /// Requires `CLONE_NEWNET` (available since Linux 2.6.24).
    Isolated,
}

/// Per-session termination reason passed to [`super::service::SandboxService::terminate_for_session`].
///
/// Mirrors the shape the orchestrator will eventually serialize into the
/// `SessionOutcomeEmitted` event body (PR-5). PR-4 only uses this to decide
/// whether to preserve the upper dir on failure (for post-mortem) or reap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TerminationReason {
    /// Session ran to completion without tripping a breaker.
    Complete,
    /// Session terminated due to a circuit-breaker trip (PR-3 feature).
    BreakerTripped { which: String },
    /// Session was cancelled by the operator.
    OperatorCancel,
    /// Session crashed — agent process died.
    Crashed { detail: String },
}

/// Returned to the orchestrator when a session sandbox has been provisioned.
///
/// Carries every path the confined child needs; the confinement bundle is
/// shipped to the child via CLI args + fd 3.
#[derive(Debug, Clone)]
pub struct SessionSandbox {
    pub workspace_id: WorkspaceId,
    pub session_id: SessionId,
    pub root_run_id: RunId,
    pub project: ProjectKey,
    /// Overlayfs merged mount point — the child sees this as its workspace.
    pub merged: PathBuf,
    /// RW upperdir for this attempt. Reflinked to snapshot_dir at teardown.
    pub upper: PathBuf,
    /// Overlayfs workdir. Reaped at teardown.
    pub work: PathBuf,
    /// Reflinked lower dir the session started from. Immutable.
    pub lower: PathBuf,
    /// Confinement config to hand to the `--sandboxed-agent` child. The
    /// parent cairn-app does NOT confine itself with this.
    pub confinement: SandboxConfinement,
    /// Network policy applied to this session.
    pub network: NetworkPolicy,
    /// Populated on resume paths: points to the snapshot directory the
    /// upperdir was seeded from. `None` for a fresh provision. PR-5
    /// carries this field so `terminate_for_session` can record the
    /// snapshot lineage (parent_snapshot_id) when the new snapshot
    /// derives from a previous one.
    pub base_snapshot_id: Option<WorkspaceSnapshotId>,
}

/// Per-session provision request passed into
/// [`super::service::SandboxService::provision_for_session`].
///
/// Carries everything the PR-5 provision path needs to mount a fresh
/// overlay OR (when `base_snapshot_id` is `Some`) seed the upperdir from
/// a prior snapshot. The service dispatches internally — callers use the
/// public `provision_for_session` + `restore_from_snapshot` pair (the
/// service disambiguates on `base_snapshot_id.is_some()`).
#[derive(Clone, Debug)]
pub struct SessionProvisionSpec {
    pub session_id: SessionId,
    pub root_run_id: RunId,
    pub project: ProjectKey,
    pub policy: SandboxPolicy,
    pub network: NetworkPolicy,
    pub attempt_number: u32,
    pub max_attempts: u32,
    /// When `Some`, the service reflinks the named snapshot into a fresh
    /// upperdir-seed and mounts the overlay over that. Used by the resume
    /// path. `None` = regular fresh provision.
    pub base_snapshot_id: Option<WorkspaceSnapshotId>,
}

/// Events the F65 session path emits. The adapter in cairn-app translates
/// these into `cairn_domain::RuntimeEvent` variants and appends them via
/// `EventLog::append`, which drives the PR-2 projection writers.
#[derive(Debug, Clone)]
pub enum F65SandboxEvent {
    SessionAttemptStarted {
        project: ProjectKey,
        session_id: SessionId,
        root_run_id: RunId,
        attempt_number: u32,
        max_attempts: u32,
    },
    WorkspaceSnapshotCreated {
        project: ProjectKey,
        snapshot_id: WorkspaceSnapshotId,
        workspace_id: WorkspaceId,
        session_id: SessionId,
    },
    /// F65 PR-5: a snapshot has been reaped either by the GC sweeper
    /// (`reason = "ttl_expired"`) or by the admin DELETE endpoint
    /// (`reason = "operator_cleared"`). `reason` rides on the cairn-workspace
    /// event shape; the domain event shape shipped in PR-1 carries only
    /// identity so the reason surfaces on telemetry spans + metrics labels.
    WorkspaceSnapshotReaped {
        project: ProjectKey,
        snapshot_id: WorkspaceSnapshotId,
        reason: String,
    },
    WorkspaceBackendDegraded {
        project: ProjectKey,
        session_id: SessionId,
        backend: String,
        reason: String,
    },
    /// F65 PR-5 (#359): crash-recovery successfully unmounted a dangling
    /// overlay left by a previous cairn-app exit.
    SandboxCrashRecovered {
        project: ProjectKey,
        session_id: SessionId,
        run_id: RunId,
    },
}

/// Trait implemented by cairn-app to flow F65 sandbox events into the event
/// log. cairn-workspace holds only `Arc<dyn F65SandboxEventSink>`.
pub trait F65SandboxEventSink: Send + Sync + 'static {
    fn publish(&self, event: F65SandboxEvent);
}

/// F65 PR-5 writer hook: back-fill the `bytes`, `reflink_used`,
/// `snapshot_path`, and `parent_snapshot_id` columns on an existing
/// `workspace_snapshots` projection row.
///
/// Declared in cairn-workspace (not cairn-store) so the workspace crate
/// keeps its "no store dependency" posture (architecture order:
/// domain → store → runtime → workspace). cairn-app bridges this to the
/// real `cairn_store::projections::WorkspaceSnapshotWriter` via a thin
/// adapter. A `NoopWorkspaceSnapshotWriter` default keeps tests
/// decoupled from the store.
#[async_trait::async_trait]
pub trait WorkspaceSnapshotWriter: Send + Sync + 'static {
    async fn stamp_metadata(
        &self,
        snapshot_id: &WorkspaceSnapshotId,
        snapshot_path: &str,
        bytes: u64,
        reflink_used: bool,
        parent_snapshot_id: Option<&WorkspaceSnapshotId>,
    ) -> Result<(), String>;
}

/// Drop-on-the-floor default when no store writer is wired. Matches the
/// [`NoopF65EventSink`] pattern: the service still functions, metadata
/// stamp just doesn't land.
#[derive(Debug, Default)]
pub struct NoopWorkspaceSnapshotWriter;

#[async_trait::async_trait]
impl WorkspaceSnapshotWriter for NoopWorkspaceSnapshotWriter {
    async fn stamp_metadata(
        &self,
        _snapshot_id: &WorkspaceSnapshotId,
        _snapshot_path: &str,
        _bytes: u64,
        _reflink_used: bool,
        _parent_snapshot_id: Option<&WorkspaceSnapshotId>,
    ) -> Result<(), String> {
        Ok(())
    }
}

/// In-memory writer that records the metadata stamps for test assertions.
/// Equivalent of [`BufferedF65EventSink`] for the stamp side.
#[derive(Debug, Default)]
pub struct BufferedWorkspaceSnapshotWriter {
    stamps: std::sync::Mutex<Vec<BufferedStamp>>,
}

/// A single recorded metadata stamp captured by
/// [`BufferedWorkspaceSnapshotWriter`].
#[derive(Debug, Clone)]
pub struct BufferedStamp {
    pub snapshot_id: WorkspaceSnapshotId,
    pub snapshot_path: String,
    pub bytes: u64,
    pub reflink_used: bool,
    pub parent_snapshot_id: Option<WorkspaceSnapshotId>,
}

impl BufferedWorkspaceSnapshotWriter {
    pub fn drain(&self) -> Vec<BufferedStamp> {
        let mut guard = self.stamps.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut *guard)
    }

    pub fn len(&self) -> usize {
        self.stamps.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait::async_trait]
impl WorkspaceSnapshotWriter for BufferedWorkspaceSnapshotWriter {
    async fn stamp_metadata(
        &self,
        snapshot_id: &WorkspaceSnapshotId,
        snapshot_path: &str,
        bytes: u64,
        reflink_used: bool,
        parent_snapshot_id: Option<&WorkspaceSnapshotId>,
    ) -> Result<(), String> {
        self.stamps
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(BufferedStamp {
                snapshot_id: snapshot_id.clone(),
                snapshot_path: snapshot_path.to_owned(),
                bytes,
                reflink_used,
                parent_snapshot_id: parent_snapshot_id.cloned(),
            });
        Ok(())
    }
}

/// An event sink that drops everything — useful for tests and for the path
/// where the orchestrator hasn't wired in its adapter yet.
#[derive(Debug, Default)]
pub struct NoopF65EventSink;

impl F65SandboxEventSink for NoopF65EventSink {
    fn publish(&self, _event: F65SandboxEvent) {}
}

/// In-memory event sink that buffers events for test assertions.
#[derive(Debug, Default)]
pub struct BufferedF65EventSink {
    events: std::sync::Mutex<Vec<F65SandboxEvent>>,
}

impl BufferedF65EventSink {
    /// Drain buffered events.
    ///
    /// Poison-tolerant — a panic in a thread holding the lock must not
    /// cascade into the event-sink path. Losing observability because one
    /// thread panicked would be monumentally unhelpful, so we recover the
    /// inner value from the poisoned mutex and continue.
    pub fn drain(&self) -> Vec<F65SandboxEvent> {
        let mut guard = self.events.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut *guard)
    }

    pub fn len(&self) -> usize {
        self.events.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl F65SandboxEventSink for BufferedF65EventSink {
    fn publish(&self, event: F65SandboxEvent) {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(event);
    }
}

/// Reference-counted event sink facade. Exposed as a `pub type` so cairn-app
/// can pass `Arc::new(MyAdapter)` without the usual Arc<dyn …> boilerplate.
pub type SharedF65EventSink = Arc<dyn F65SandboxEventSink>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffered_sink_collects_and_drains() {
        let sink = BufferedF65EventSink::default();
        sink.publish(F65SandboxEvent::WorkspaceBackendDegraded {
            project: ProjectKey::new(
                cairn_domain::TenantId::new("t"),
                "w".to_string(),
                "p".to_string(),
            ),
            session_id: SessionId::new("s1"),
            backend: "ext4_copy".to_string(),
            reason: "reflink_unsupported_fs".to_string(),
        });
        assert_eq!(sink.len(), 1);
        assert_eq!(sink.drain().len(), 1);
        assert!(sink.is_empty());
    }

    #[test]
    fn network_policy_default_is_shared() {
        assert_eq!(NetworkPolicy::default(), NetworkPolicy::Shared);
    }
}
