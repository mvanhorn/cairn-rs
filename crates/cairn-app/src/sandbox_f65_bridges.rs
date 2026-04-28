//! F65 PR-5: bridge adapters between cairn-workspace's sandbox-facing
//! traits and cairn-store / cairn-runtime.
//!
//! Three adapters live here:
//!
//! 1. [`StoreF65EventSink`] — adapts `cairn_workspace::F65SandboxEventSink`
//!    (sync `publish`) to `cairn_store::EventLog::append` (async). Uses
//!    tokio::spawn for fire-and-forget delivery; the caller is already
//!    inside a tokio runtime because every F65 sink call flows from an
//!    async service method. Append failure logs loud via `eprintln!` —
//!    silently losing operator-visible events would mask capacity
//!    regressions the arch doc explicitly wants surfaced.
//!
//! 2. [`StoreSnapshotWriter`] — adapts cairn-workspace's local
//!    `WorkspaceSnapshotWriter` trait to the cairn-store counterpart.
//!    cairn-workspace holds no cairn-store dep, so this bridge is the
//!    integration seam.
//!
//! 3. [`StoreSnapshotGcSource`] — adapts `cairn_store`'s
//!    `WorkspaceSnapshotReadModel` + `SessionReadModel` to the
//!    `SnapshotGcSource` trait the sweeper consumes. Enforces the
//!    two-predicate gate (age past TTL + parent session closed) in-
//!    process because portability rules prohibit Postgres-specific
//!    WHERE clauses (see `feedback_no_db_specific_features.md`).

use std::sync::Arc;

use async_trait::async_trait;
use cairn_domain::SessionState;
use cairn_domain::{
    EventEnvelope, EventId, EventSource, RuntimeEvent, SandboxCrashRecovered,
    SessionAttemptStarted, SessionId, WorkspaceBackendDegraded, WorkspaceSnapshotCreated,
    WorkspaceSnapshotId, WorkspaceSnapshotReaped,
};
use cairn_store::projections::{
    SessionReadModel, WorkspaceSnapshotReadModel, WorkspaceSnapshotWriter,
};
use cairn_store::{EventLog, InMemoryStore};
use cairn_workspace::sandbox::f65::{
    F65SandboxEvent, F65SandboxEventSink, WorkspaceSnapshotWriter as WorkspaceWriterTrait,
};
use cairn_workspace::sandbox::snapshot_gc::{SnapshotGcCandidate, SnapshotGcSource};

/// Bridge from cairn-workspace F65 events to the real cairn-store
/// event log. Fire-and-forget: the sync `publish` hands off via
/// `tokio::spawn` to an async append on the store.
pub struct StoreF65EventSink {
    store: Arc<InMemoryStore>,
}

impl StoreF65EventSink {
    pub fn new(store: Arc<InMemoryStore>) -> Self {
        Self { store }
    }

    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or_default()
    }

    fn wrap(event: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
        EventEnvelope::for_runtime_event(
            EventId::new(format!("evt_{}", StoreF65EventSink::now_ms())),
            EventSource::Runtime,
            event,
        )
    }
}

impl F65SandboxEventSink for StoreF65EventSink {
    fn publish(&self, event: F65SandboxEvent) {
        let at_ms = Self::now_ms();
        let runtime_event = match event {
            F65SandboxEvent::SessionAttemptStarted {
                project,
                session_id,
                root_run_id,
                attempt_number,
                max_attempts,
            } => RuntimeEvent::SessionAttemptStarted(SessionAttemptStarted {
                project,
                session_id,
                root_run_id,
                attempt_number,
                max_attempts,
                at_ms,
            }),
            F65SandboxEvent::WorkspaceSnapshotCreated {
                project,
                snapshot_id,
                workspace_id,
                session_id,
            } => RuntimeEvent::WorkspaceSnapshotCreated(WorkspaceSnapshotCreated {
                project,
                snapshot_id,
                workspace_id,
                session_id,
                at_ms,
            }),
            F65SandboxEvent::WorkspaceSnapshotReaped {
                project,
                snapshot_id,
                reason: _reason,
            } => RuntimeEvent::WorkspaceSnapshotReaped(WorkspaceSnapshotReaped {
                project,
                snapshot_id,
                at_ms,
            }),
            F65SandboxEvent::WorkspaceBackendDegraded {
                project,
                session_id,
                backend,
                reason,
            } => RuntimeEvent::WorkspaceBackendDegraded(WorkspaceBackendDegraded {
                project,
                session_id,
                backend,
                reason,
                at_ms,
            }),
            F65SandboxEvent::SandboxCrashRecovered {
                project,
                session_id,
                run_id,
            } => RuntimeEvent::SandboxCrashRecovered(SandboxCrashRecovered {
                project,
                session_id,
                run_id,
                at_ms,
            }),
        };
        let envelope = Self::wrap(runtime_event);
        let store = self.store.clone();
        // Fire-and-forget. Append failures are logged loudly because
        // operator dashboards + metrics rely on these events landing.
        tokio::spawn(async move {
            if let Err(err) = store.append(std::slice::from_ref(&envelope)).await {
                eprintln!("F65 event append failed: {err}; observability may be degraded");
            }
        });
    }
}

/// Bridge cairn-workspace's local `WorkspaceSnapshotWriter` trait to the
/// cairn-store `WorkspaceSnapshotWriter`. cairn-workspace holds no
/// cairn-store dep; this bridge satisfies both sides.
pub struct StoreSnapshotWriter<W: WorkspaceSnapshotWriter + Send + Sync + 'static> {
    inner: Arc<W>,
}

impl<W: WorkspaceSnapshotWriter + Send + Sync + 'static> StoreSnapshotWriter<W> {
    pub fn new(inner: Arc<W>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<W: WorkspaceSnapshotWriter + Send + Sync + 'static> WorkspaceWriterTrait
    for StoreSnapshotWriter<W>
{
    async fn stamp_metadata(
        &self,
        snapshot_id: &WorkspaceSnapshotId,
        snapshot_path: &str,
        bytes: u64,
        reflink_used: bool,
        parent_snapshot_id: Option<&WorkspaceSnapshotId>,
    ) -> Result<(), String> {
        self.inner
            .stamp_metadata(
                snapshot_id,
                snapshot_path,
                bytes,
                reflink_used,
                parent_snapshot_id,
            )
            .await
            .map_err(|err| err.to_string())
    }
}

/// Source for the GC sweeper. Uses the InMemoryStore's direct
/// `list_snapshots_for_gc` helper because the read-model traits lack
/// a portable "enumerate all snapshots globally" method — shipping
/// one would force a Postgres-specific join/offset pagination shape.
pub struct StoreSnapshotGcSource {
    store: Arc<InMemoryStore>,
}

impl StoreSnapshotGcSource {
    pub fn new(store: Arc<InMemoryStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl SnapshotGcSource for StoreSnapshotGcSource {
    async fn list_reap_candidates(
        &self,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<Vec<SnapshotGcCandidate>, String> {
        let rows = self.store.list_snapshots_for_gc(now_ms, ttl_ms);
        Ok(rows
            .into_iter()
            .map(|r| SnapshotGcCandidate {
                snapshot_id: r.snapshot_id,
                session_id: r.session_id,
                project: r.project,
                created_at_ms: r.created_at,
            })
            .collect())
    }
}

/// Test helper: a simple GC source backed by an explicit
/// `(session_id, snapshots)` list. The integration test uses this to
/// drive `sweep_once` deterministically without building a full store.
pub struct FixedSessionsGcSource<S, R>
where
    S: WorkspaceSnapshotReadModel + 'static,
    R: SessionReadModel + 'static,
{
    pub snapshots: Arc<S>,
    pub sessions: Arc<R>,
    pub session_ids: Vec<SessionId>,
}

#[async_trait]
impl<S, R> SnapshotGcSource for FixedSessionsGcSource<S, R>
where
    S: WorkspaceSnapshotReadModel + 'static,
    R: SessionReadModel + 'static,
{
    async fn list_reap_candidates(
        &self,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<Vec<SnapshotGcCandidate>, String> {
        let mut out = Vec::new();
        for session_id in &self.session_ids {
            let session = match self.sessions.get(session_id).await {
                Ok(Some(s)) => s,
                Ok(None) | Err(_) => continue,
            };
            // Only closed sessions qualify for TTL reap (plan Q10 /
            // arch §4.3.5). The domain enum in this tree exposes
            // Open/Completed/Failed/Archived; all non-Open are
            // considered "closed" for GC purposes. The plan's
            // BudgetExhausted/Aborted values map to Failed in the
            // current lifecycle.
            if matches!(session.state, SessionState::Open) {
                continue;
            }
            let snapshots = self
                .snapshots
                .list_by_session(session_id)
                .await
                .map_err(|e| e.to_string())?;
            for snap in snapshots {
                if snap.reaped_at.is_some() {
                    continue;
                }
                let age_ms = now_ms.saturating_sub(snap.created_at);
                if age_ms < ttl_ms {
                    continue;
                }
                out.push(SnapshotGcCandidate {
                    snapshot_id: snap.snapshot_id,
                    session_id: session_id.clone(),
                    project: snap.project,
                    created_at_ms: snap.created_at,
                });
            }
        }
        Ok(out)
    }
}
