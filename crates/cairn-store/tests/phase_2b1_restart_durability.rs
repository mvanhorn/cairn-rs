//! RFC-025 Phase 2b.1 restart-durability proof for the new projection
//! tables shipped in this phase.
//!
//! Each milestone below targets one event family that was Stubbed before
//! Phase 2b.1. The core contract: the event commits to the event log AND
//! to a persistent read-model row that survives a process restart.
//!
//! Shape mirrors `provider_restart_durability.rs`: boot a file-backed
//! SQLite adapter, write events, drop the pool (simulating process
//! exit), re-open the same DB file, assert the read-model row is still
//! there.
//!
//! SQLite-only; pg equivalents are covered by the shared projection
//! applier semantics under nightly CI via `TEST_DATABASE_URL`.

#![cfg(feature = "sqlite")]

use std::str::FromStr;
use std::sync::Arc;

use cairn_domain::{
    audit::AuditOutcome, events::ActualOutcome, AuditLogEntryRecorded, EventEnvelope, EventId,
    EventSource, OperatorId, OutcomeId, OutcomeRecorded, PlanApproved, PlanProposed, ProjectId,
    ProjectKey, RunId, RuntimeEvent, ScheduledTaskCreated, ScheduledTaskId, SessionId, TenantId,
    WorkspaceId,
};
use cairn_store::db::DbAdapter;
use cairn_store::projections::{
    AuditLogReadModel, OutcomeReadModel, PlanReviewReadModel, PlanReviewState,
    ScheduledTaskReadModel,
};
use cairn_store::{sqlite::SqliteAdapter, sqlite::SqliteEventLog, EventLog};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

fn evt(id: &str, payload: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(EventId::new(id), EventSource::Runtime, payload)
}

/// Open a file-backed SQLite pool + adapter + event-log at `db_path`.
/// Dropping the returned tuple closes the pool — simulating a process
/// exit. Matches the helper in `provider_restart_durability.rs`.
async fn open_store(db_path: &std::path::Path) -> (Arc<SqliteAdapter>, SqliteEventLog) {
    let url = format!("sqlite:{}", db_path.display());
    let opts = SqliteConnectOptions::from_str(&url)
        .expect("sqlite url")
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
    let pool = SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(opts)
        .await
        .expect("sqlite pool");

    let adapter = SqliteAdapter::new(pool.clone());
    adapter.migrate().await.expect("migrate");
    let log = SqliteEventLog::new(pool);
    (Arc::new(adapter), log)
}

/// Milestone 1 (audits): an `AuditLogEntryRecorded` event persisted into
/// the `audit_log_entries` table must be readable via
/// `AuditLogReadModel::list_by_tenant` after the pool is dropped and
/// re-opened against the same DB file.
///
/// Pre-Phase-2b.1 the pg/sqlite applier was `log_stub(..)` — the event
/// log kept the row, but list_by_tenant on a fresh boot returned empty
/// (admin audit dashboard was blank after every restart on persistent
/// backends).
#[tokio::test]
async fn audit_log_entry_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let tenant = TenantId::new("t_audit_restart");
    let entry_id_1 = "audit_restart_first";
    let entry_id_2 = "audit_restart_second";

    // ── Session 1: record two events, drop the pool. ──────────────
    {
        let (adapter, log) = open_store(&path).await;
        let events = vec![
            evt(
                "evt_audit_restart_1",
                RuntimeEvent::AuditLogEntryRecorded(AuditLogEntryRecorded {
                    entry_id: entry_id_1.to_owned(),
                    tenant_id: tenant.clone(),
                    actor_id: "op_restart".to_owned(),
                    action: "create_tenant".to_owned(),
                    resource_type: "tenant".to_owned(),
                    resource_id: tenant.as_str().to_owned(),
                    outcome: AuditOutcome::Success,
                    occurred_at_ms: 1_700_000_010_000,
                }),
            ),
            evt(
                "evt_audit_restart_2",
                RuntimeEvent::AuditLogEntryRecorded(AuditLogEntryRecorded {
                    entry_id: entry_id_2.to_owned(),
                    tenant_id: tenant.clone(),
                    actor_id: "op_restart".to_owned(),
                    action: "revoke_credential".to_owned(),
                    resource_type: "credential".to_owned(),
                    resource_id: "cred_xyz".to_owned(),
                    outcome: AuditOutcome::Failure,
                    occurred_at_ms: 1_700_000_020_000,
                }),
            ),
        ];
        log.append(&events).await.expect("append audit events");

        // Visible pre-restart — catches a write-path regression before
        // we bother comparing the post-restart read.
        let pre = AuditLogReadModel::list_by_tenant(adapter.as_ref(), &tenant, None, None, 10)
            .await
            .unwrap();
        assert_eq!(pre.len(), 2, "both entries must be visible before restart");
    } // adapter + log dropped; sqlite pool closed here.

    // ── Session 2: re-open the DB and prove persistence. ──────────
    let (adapter, _log) = open_store(&path).await;
    let post = AuditLogReadModel::list_by_tenant(adapter.as_ref(), &tenant, None, None, 10)
        .await
        .unwrap();
    assert_eq!(post.len(), 2, "both entries must survive restart");

    // Newest-first ordering survives the restart too (pre-Phase-2b.1
    // the in-memory impl returned insertion order; the pg/sqlite ORDER
    // BY DESC contract is the one durable contract).
    assert_eq!(post[0].entry_id, entry_id_2);
    assert_eq!(post[1].entry_id, entry_id_1);
    assert_eq!(post[0].outcome, AuditOutcome::Failure);
    assert_eq!(post[1].outcome, AuditOutcome::Success);

    // list_by_resource returns the revoke event only.
    let by_resource =
        AuditLogReadModel::list_by_resource(adapter.as_ref(), "credential", "cred_xyz")
            .await
            .unwrap();
    assert_eq!(by_resource.len(), 1);
    assert_eq!(by_resource[0].entry_id, entry_id_2);

    // Window filter (since >= 15_000) must only return entry 2.
    let windowed = AuditLogReadModel::list_by_tenant(
        adapter.as_ref(),
        &tenant,
        Some(1_700_000_015_000),
        None,
        10,
    )
    .await
    .unwrap();
    assert_eq!(windowed.len(), 1);
    assert_eq!(windowed[0].entry_id, entry_id_2);
}

/// Milestone 2 (scheduled tasks): a `ScheduledTaskCreated` event must
/// produce a row in `scheduled_tasks` that survives process restart so
/// the runtime recovery sweep can rehydrate cron state and `list_due`
/// returns the right entries on next boot.
#[tokio::test]
async fn scheduled_task_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let tenant = TenantId::new("t_sched_restart");
    let task_id = ScheduledTaskId::new("sched_restart_1");

    // ── Session 1: create the task, drop the pool. ────────────────
    {
        let (adapter, log) = open_store(&path).await;
        let event = evt(
            "evt_sched_restart_1",
            RuntimeEvent::ScheduledTaskCreated(ScheduledTaskCreated {
                tenant_id: tenant.clone(),
                scheduled_task_id: task_id.clone(),
                name: "weekly_reflection".to_owned(),
                cron_expression: "0 9 * * 1".to_owned(),
                next_run_at: Some(1_700_000_300_000),
                created_at: 1_700_000_100_000,
            }),
        );
        log.append(&[event]).await.expect("append");

        let pre = ScheduledTaskReadModel::get(adapter.as_ref(), &task_id)
            .await
            .unwrap();
        assert!(pre.is_some(), "task visible before restart");
    }

    // ── Session 2: reopen + prove persistence. ─────────────────────
    let (adapter, _log) = open_store(&path).await;

    let after = ScheduledTaskReadModel::get(adapter.as_ref(), &task_id)
        .await
        .unwrap()
        .expect("task must survive restart");
    assert_eq!(after.name, "weekly_reflection");
    assert_eq!(after.cron_expression, "0 9 * * 1");
    assert_eq!(after.next_run_at, Some(1_700_000_300_000));
    assert_eq!(after.created_at, 1_700_000_100_000);
    assert_eq!(after.updated_at, 1_700_000_100_000);
    assert!(after.enabled, "enabled default must persist");
    assert_eq!(after.last_run_at, None);

    // list_due must find it after the next_run_at has elapsed.
    let due = ScheduledTaskReadModel::list_due(adapter.as_ref(), 1_700_000_400_000, 10)
        .await
        .unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].scheduled_task_id, task_id);

    // list_by_tenant also finds it (dashboard view).
    let listed = ScheduledTaskReadModel::list_by_tenant(adapter.as_ref(), &tenant, 10, 0)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].scheduled_task_id, task_id);
}

/// Milestone 3 (outcomes): `OutcomeRecorded` rows must survive restart
/// so the evaluator-optimizer calibration loop rebuilds from the
/// read model on boot rather than re-scanning the full event log.
#[tokio::test]
async fn outcome_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let proj = ProjectKey {
        tenant_id: TenantId::new("t_out_restart"),
        workspace_id: WorkspaceId::new("w_out_restart"),
        project_id: ProjectId::new("p_out_restart"),
    };
    let run_id = RunId::new("r_out_restart");
    let outcome_id = OutcomeId::new("out_restart_1");

    // ── Session 1: record, drop the pool. ─────────────────────────
    {
        let (adapter, log) = open_store(&path).await;
        let event = evt(
            "evt_out_restart_1",
            RuntimeEvent::OutcomeRecorded(OutcomeRecorded {
                project: proj.clone(),
                outcome_id: outcome_id.clone(),
                run_id: run_id.clone(),
                agent_type: "code_review".to_owned(),
                predicted_confidence: 0.9,
                actual_outcome: ActualOutcome::Success,
                recorded_at: 1_700_000_500_000,
            }),
        );
        log.append(&[event]).await.expect("append");

        let pre = OutcomeReadModel::get(adapter.as_ref(), &outcome_id)
            .await
            .unwrap();
        assert!(pre.is_some());
    }

    // ── Session 2: reopen + prove persistence. ────────────────────
    let (adapter, _log) = open_store(&path).await;

    let after = OutcomeReadModel::get(adapter.as_ref(), &outcome_id)
        .await
        .unwrap()
        .expect("outcome must survive restart");
    assert_eq!(after.outcome_id, outcome_id);
    assert_eq!(after.run_id, run_id);
    assert_eq!(after.project, proj);
    assert_eq!(after.agent_type, "code_review");
    assert!((after.predicted_confidence - 0.9).abs() < f64::EPSILON);
    assert_eq!(after.actual_outcome, ActualOutcome::Success);
    assert_eq!(after.recorded_at, 1_700_000_500_000);

    let by_run = OutcomeReadModel::list_by_run(adapter.as_ref(), &run_id, 10)
        .await
        .unwrap();
    assert_eq!(by_run.len(), 1);
    assert_eq!(by_run[0].outcome_id, outcome_id);

    let by_proj = OutcomeReadModel::list_by_project(adapter.as_ref(), &proj, 10, 0)
        .await
        .unwrap();
    assert_eq!(by_proj.len(), 1);
}

/// Milestone 4 (plan reviews, RFC 018): `PlanProposed` + `PlanApproved`
/// persist into the `plan_reviews` read-model row. After a process
/// restart, the row still reflects the terminal state + resolver
/// identity so `GET /v1/runs/:id/plan` serves from the projection
/// rather than a full event-log scan.
#[tokio::test]
async fn plan_review_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let proj = ProjectKey {
        tenant_id: TenantId::new("t_plan_restart"),
        workspace_id: WorkspaceId::new("w_plan_restart"),
        project_id: ProjectId::new("p_plan_restart"),
    };
    let sid = SessionId::new("s_plan_restart");
    let plan_run_id = RunId::new("r_plan_restart_1");

    // ── Session 1: propose + approve, drop the pool. ──────────────
    {
        let (adapter, log) = open_store(&path).await;
        let events = vec![
            evt(
                "evt_plan_restart_propose",
                RuntimeEvent::PlanProposed(PlanProposed {
                    project: proj.clone(),
                    plan_run_id: plan_run_id.clone(),
                    session_id: sid.clone(),
                    plan_markdown: "## Plan\nDeploy service".to_owned(),
                    proposed_at: 1_700_000_600_000,
                }),
            ),
            evt(
                "evt_plan_restart_approve",
                RuntimeEvent::PlanApproved(PlanApproved {
                    project: proj.clone(),
                    plan_run_id: plan_run_id.clone(),
                    approved_by: OperatorId::new("op_restart"),
                    reviewer_comments: Some("go".to_owned()),
                    approved_at: 1_700_000_601_000,
                }),
            ),
        ];
        log.append(&events).await.expect("append");

        let pre = PlanReviewReadModel::get(adapter.as_ref(), &plan_run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pre.state, PlanReviewState::Approved);
    }

    // ── Session 2: reopen + prove persistence. ────────────────────
    let (adapter, _log) = open_store(&path).await;

    let after = PlanReviewReadModel::get(adapter.as_ref(), &plan_run_id)
        .await
        .unwrap()
        .expect("plan review must survive restart");
    assert_eq!(after.plan_run_id, plan_run_id);
    assert_eq!(after.project, proj);
    assert_eq!(after.session_id, sid);
    assert_eq!(after.state, PlanReviewState::Approved);
    assert_eq!(after.resolved_by, Some(OperatorId::new("op_restart")));
    assert_eq!(after.resolved_at, Some(1_700_000_601_000));
    assert_eq!(after.reviewer_comments.as_deref(), Some("go"));
    assert_eq!(after.plan_markdown, "## Plan\nDeploy service");

    // list_pending_by_project returns nothing — the plan is resolved.
    let pending = PlanReviewReadModel::list_pending_by_project(adapter.as_ref(), &proj, 10)
        .await
        .unwrap();
    assert!(pending.is_empty());

    // list_by_project returns the resolved row.
    let all = PlanReviewReadModel::list_by_project(adapter.as_ref(), &proj, 10, 0)
        .await
        .unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].plan_run_id, plan_run_id);
}
