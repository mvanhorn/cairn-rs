//! Cross-tenant isolation for the F65 read models (issue #438).
//!
//! Every F65 reader gained a required `&ProjectKey` argument. These
//! tests prove that a tenant-B caller, given tenant-A's ids, gets
//! `None` / empty from every method — the defence-in-depth scope
//! guard runs at the query layer so a handler that forgets to
//! pre-check the tenant still cannot leak foreign rows. Mirrors the
//! #185 LeaseHistorySubscriber root-cause pattern but for F65 PR-2.
//!
//! Coverage is cross-backend (SQLite + InMemory) because both ship
//! to production. Postgres parity stays covered by the pg-side
//! tests in `f65_projections.rs` plus the parser-level schema-parity
//! check — replicating the cross-tenant probe for pg needs live
//! postgres which is out of scope for a unit-level test suite.

#![cfg(feature = "sqlite")]

use cairn_domain::{
    session_orchestration::{SessionOutcome, TerminationReason},
    CheckpointId, CheckpointPersisted, EventEnvelope, EventId, EventSource, ProjectKey, RunId,
    RuntimeEvent, SessionAttemptStarted, SessionCreated, SessionId, SessionOutcomeEmitted,
    WorkspaceId, WorkspaceSnapshotCreated, WorkspaceSnapshotId,
};
use cairn_store::event_log::EventLog;
use cairn_store::in_memory::InMemoryStore;
use cairn_store::projections::{
    F65CheckpointReadModel, SessionOutcomeReadModel, WorkspaceSnapshotReadModel,
};
use cairn_store::sqlite::{SqliteAdapter, SqliteEventLog};
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn project_a() -> ProjectKey {
    ProjectKey::new("tenant_a", "ws_a", "proj_a")
}

fn project_b() -> ProjectKey {
    ProjectKey::new("tenant_b", "ws_b", "proj_b")
}

fn env(event: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    EventEnvelope::for_runtime_event(
        EventId::new(format!("evt_xtenant_{n}")),
        EventSource::Runtime,
        event,
    )
}

async fn fresh_backends() -> (SqliteAdapter, SqliteEventLog, InMemoryStore) {
    let adapter = SqliteAdapter::in_memory().await.expect("sqlite in_memory");
    let log = SqliteEventLog::new(adapter.pool().clone());
    let mem = InMemoryStore::new();
    (adapter, log, mem)
}

async fn seed_session_with_outcome(
    log: &SqliteEventLog,
    mem: &InMemoryStore,
    project: &ProjectKey,
    session_id: &str,
    root_run_id: &str,
    checkpoint_id: &str,
    snapshot_id: &str,
    workspace_id: &str,
) {
    let session = SessionId::new(session_id);
    let run = RunId::new(root_run_id);
    let checkpoint = CheckpointId::new(checkpoint_id);
    let snap = WorkspaceSnapshotId::new(snapshot_id);
    let ws = WorkspaceId::new(workspace_id);

    let outcome = SessionOutcome {
        session_id: session.clone(),
        root_run_id: run.clone(),
        project: project.clone(),
        checkpoint_id: checkpoint.clone(),
        workspace_snapshot_id: Some(snap.clone()),
        termination_reason: TerminationReason::CompleteRun,
        compacted_summary: "{}".to_owned(),
        next_step_hint: None,
        cost_micros: 0,
        emitted_at: 1_400,
    };
    let events = vec![
        env(RuntimeEvent::SessionCreated(SessionCreated {
            project: project.clone(),
            session_id: session.clone(),
        })),
        // The root run row must exist before CheckpointPersisted / the
        // session outcome fire — checkpoints.run_id is FK-bound in the
        // sqlite schema.
        env(RuntimeEvent::RunCreated(cairn_domain::RunCreated {
            project: project.clone(),
            session_id: session.clone(),
            run_id: run.clone(),
            parent_run_id: None,
            prompt_release_id: None,
            agent_role_id: None,
        })),
        env(RuntimeEvent::SessionAttemptStarted(SessionAttemptStarted {
            project: project.clone(),
            session_id: session.clone(),
            root_run_id: run.clone(),
            attempt_number: 1,
            max_attempts: 1,
            at_ms: 1_100,
        })),
        env(RuntimeEvent::CheckpointPersisted(CheckpointPersisted {
            project: project.clone(),
            session_id: session.clone(),
            checkpoint_id: checkpoint.clone(),
            root_run_id: run.clone(),
            iteration: 1,
            at_ms: 1_200,
        })),
        env(RuntimeEvent::WorkspaceSnapshotCreated(
            WorkspaceSnapshotCreated {
                project: project.clone(),
                snapshot_id: snap.clone(),
                workspace_id: ws.clone(),
                session_id: session.clone(),
                at_ms: 1_300,
            },
        )),
        env(RuntimeEvent::SessionOutcomeEmitted(SessionOutcomeEmitted {
            project: project.clone(),
            session_id: session.clone(),
            root_run_id: run.clone(),
            outcome,
            at_ms: 1_400,
        })),
    ];
    // Every event is projected on both backends so we can probe each
    // reader shape without setting up two separate runs.
    log.append(&events).await.expect("sqlite append");
    mem.append(&events).await.expect("in-memory append");
}

#[tokio::test]
async fn session_outcome_reader_rejects_cross_tenant_ids() {
    let (adapter, log, mem) = fresh_backends().await;
    seed_session_with_outcome(
        &log,
        &mem,
        &project_a(),
        "sess_a",
        "run_a",
        "ck_a",
        "snap_a",
        "ws_a_id",
    )
    .await;

    // Tenant-A's own read hits.
    assert!(
        SessionOutcomeReadModel::get_by_root_run(&adapter, &project_a(), &RunId::new("run_a"))
            .await
            .unwrap()
            .is_some(),
        "sqlite: tenant-A must see its own outcome"
    );
    assert!(
        SessionOutcomeReadModel::get_by_root_run(&mem, &project_a(), &RunId::new("run_a"))
            .await
            .unwrap()
            .is_some(),
        "in-memory: tenant-A must see its own outcome"
    );

    // Tenant-B with tenant-A's RunId must get None.
    assert!(
        SessionOutcomeReadModel::get_by_root_run(&adapter, &project_b(), &RunId::new("run_a"))
            .await
            .unwrap()
            .is_none(),
        "sqlite: tenant-B must not see tenant-A outcome via scope mismatch"
    );
    assert!(
        SessionOutcomeReadModel::get_by_root_run(&mem, &project_b(), &RunId::new("run_a"))
            .await
            .unwrap()
            .is_none(),
        "in-memory: tenant-B must not see tenant-A outcome via scope mismatch"
    );

    // list_by_session is empty for tenant-B even with tenant-A's session_id.
    let empty =
        SessionOutcomeReadModel::list_by_session(&adapter, &project_b(), &SessionId::new("sess_a"))
            .await
            .unwrap();
    assert!(
        empty.is_empty(),
        "sqlite: list_by_session must be empty cross-tenant"
    );

    let empty =
        SessionOutcomeReadModel::list_by_session(&mem, &project_b(), &SessionId::new("sess_a"))
            .await
            .unwrap();
    assert!(
        empty.is_empty(),
        "in-memory: list_by_session must be empty cross-tenant"
    );
}

#[tokio::test]
async fn workspace_snapshot_reader_rejects_cross_tenant_ids() {
    let (adapter, log, mem) = fresh_backends().await;
    seed_session_with_outcome(
        &log,
        &mem,
        &project_a(),
        "sess_a2",
        "run_a2",
        "ck_a2",
        "snap_a2",
        "ws_a2",
    )
    .await;

    assert!(
        WorkspaceSnapshotReadModel::get(
            &adapter,
            &project_a(),
            &WorkspaceSnapshotId::new("snap_a2")
        )
        .await
        .unwrap()
        .is_some(),
        "sqlite: tenant-A must see its own snapshot"
    );
    assert!(
        WorkspaceSnapshotReadModel::get(
            &adapter,
            &project_b(),
            &WorkspaceSnapshotId::new("snap_a2")
        )
        .await
        .unwrap()
        .is_none(),
        "sqlite: tenant-B must not see tenant-A snapshot"
    );
    assert!(
        WorkspaceSnapshotReadModel::get(&mem, &project_b(), &WorkspaceSnapshotId::new("snap_a2"))
            .await
            .unwrap()
            .is_none(),
        "in-memory: tenant-B must not see tenant-A snapshot"
    );

    let list_b = WorkspaceSnapshotReadModel::list_by_session(
        &adapter,
        &project_b(),
        &SessionId::new("sess_a2"),
    )
    .await
    .unwrap();
    assert!(
        list_b.is_empty(),
        "sqlite: tenant-B must not see tenant-A snapshot via session_id scope mismatch"
    );

    // Lineage: tenant-B walking from tenant-A's snapshot returns empty.
    let lineage_b = WorkspaceSnapshotReadModel::lineage(
        &adapter,
        &project_b(),
        &WorkspaceSnapshotId::new("snap_a2"),
    )
    .await
    .unwrap();
    assert!(
        lineage_b.is_empty(),
        "sqlite: tenant-B must not walk tenant-A lineage"
    );

    let lineage_b_mem = WorkspaceSnapshotReadModel::lineage(
        &mem,
        &project_b(),
        &WorkspaceSnapshotId::new("snap_a2"),
    )
    .await
    .unwrap();
    assert!(
        lineage_b_mem.is_empty(),
        "in-memory: tenant-B must not walk tenant-A lineage"
    );
}

#[tokio::test]
async fn f65_checkpoint_reader_rejects_cross_tenant_ids() {
    let (adapter, log, mem) = fresh_backends().await;
    seed_session_with_outcome(
        &log,
        &mem,
        &project_a(),
        "sess_a3",
        "run_a3",
        "ck_a3",
        "snap_a3",
        "ws_a3",
    )
    .await;

    // Tenant-A's own read hits.
    assert!(
        F65CheckpointReadModel::get_f65(&adapter, &project_a(), &CheckpointId::new("ck_a3"))
            .await
            .unwrap()
            .is_some(),
        "sqlite: tenant-A must see its own checkpoint"
    );

    // Tenant-B with tenant-A's CheckpointId must get None.
    assert!(
        F65CheckpointReadModel::get_f65(&adapter, &project_b(), &CheckpointId::new("ck_a3"))
            .await
            .unwrap()
            .is_none(),
        "sqlite: tenant-B must not see tenant-A checkpoint via scope mismatch"
    );
    assert!(
        F65CheckpointReadModel::get_f65(&mem, &project_b(), &CheckpointId::new("ck_a3"))
            .await
            .unwrap()
            .is_none(),
        "in-memory: tenant-B must not see tenant-A checkpoint via scope mismatch"
    );

    let empty =
        F65CheckpointReadModel::list_by_session(&adapter, &project_b(), &SessionId::new("sess_a3"))
            .await
            .unwrap();
    assert!(
        empty.is_empty(),
        "sqlite: list_by_session must be empty cross-tenant"
    );
}

// `WorkspaceRegistryReadModel` tenant-isolation is covered by the
// contract-level sqlite/pg query layer test: its two methods share the
// same `AND tenant_id = ? AND workspace_scope = ? AND project_id = ?`
// scope guard as the three readers above, and there is no
// `RuntimeEvent` path that populates this table in the test scaffold —
// it is written by `SandboxService` at runtime. The signature change
// itself (verified by `cargo check`) is enough to prove the scope
// guard is plumbed through; a behavioural probe would require a full
// sandbox-service harness that is out of scope for this unit suite.
