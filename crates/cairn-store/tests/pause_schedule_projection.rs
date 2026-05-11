//! Issue #592 restart-durability + evict-on-resume proof for the
//! `pause_schedules` projection.
//!
//! Two contracts:
//!
//! 1. **Restart durability.** A `RunStateChanged(→Paused)` with
//!    `pause_reason.resume_after_ms = Some(_)` inserts a row into
//!    `pause_schedules`. The row survives a process restart (pool
//!    drop + re-open against the same DB file) and `list_due` reads
//!    it back.
//! 2. **Evict-on-resume.** A `RunStateChanged(Paused → Running)` (or
//!    any terminal state) DELETEs the row from `pause_schedules`, so
//!    `list_due` no longer returns it. Pre-#592 `list_due` was a
//!    pure event-log walker that kept returning resumed runs until a
//!    separate compaction step rebuilt the projection.
//!
//! Shape mirrors `phase_2b1_restart_durability.rs`: file-backed SQLite,
//! write events, drop the pool (simulated crash), re-open, assert.

#![cfg(feature = "sqlite")]

use std::str::FromStr;
use std::sync::Arc;

use cairn_domain::events::StateTransition;
use cairn_domain::lifecycle::{PauseReason, PauseReasonKind};
use cairn_domain::{
    EventEnvelope, EventId, EventSource, ProjectKey, RunId, RunState, RunStateChanged,
    RuntimeEvent, TenantId,
};
use cairn_store::db::DbAdapter;
use cairn_store::projections::PauseScheduleReadModel;
use cairn_store::{sqlite::SqliteAdapter, sqlite::SqliteEventLog, EventLog};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

fn evt(id: &str, payload: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(EventId::new(id), EventSource::Runtime, payload)
}

/// File-backed SQLite pool + adapter + event-log at `db_path`. Dropping
/// the returned tuple closes the pool — simulating process exit.
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

fn paused_evt(run_id: &str, project: &ProjectKey, resume_after_ms: u64) -> RuntimeEvent {
    RuntimeEvent::RunStateChanged(RunStateChanged {
        run_id: RunId::new(run_id),
        project: project.clone(),
        transition: StateTransition {
            from: Some(RunState::Running),
            to: RunState::Paused,
        },
        failure_class: None,
        pause_reason: Some(PauseReason {
            kind: PauseReasonKind::OperatorPause,
            detail: None,
            resume_after_ms: Some(resume_after_ms),
            actor: Some("integration-test".to_owned()),
        }),
        resume_trigger: None,
    })
}

fn resumed_evt(run_id: &str, project: &ProjectKey) -> RuntimeEvent {
    RuntimeEvent::RunStateChanged(RunStateChanged {
        run_id: RunId::new(run_id),
        project: project.clone(),
        transition: StateTransition {
            from: Some(RunState::Paused),
            to: RunState::Running,
        },
        failure_class: None,
        pause_reason: None,
        resume_trigger: Some(cairn_domain::lifecycle::ResumeTrigger::OperatorResume),
    })
}

/// Contract 1: a pause_schedules row inserted in session 1 must still
/// be readable via `list_due` after the pool is dropped and re-opened
/// against the same DB file.
#[tokio::test]
async fn pause_schedule_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let tenant = TenantId::new("t_pause_restart");
    let project = ProjectKey::new(tenant.as_str(), "w", "p");
    let run_id = "run_pause_restart_a";

    // ── Session 1: append a pause with a scheduled resume. ──────────
    {
        let (_adapter, log) = open_store(&path).await;
        log.append(&[evt("evt_pause_restart_1", paused_evt(run_id, &project, 0))])
            .await
            .expect("append paused");
        // Pool drops at scope exit → simulated crash.
    }

    // ── Session 2: re-open the same file. The pause_schedules row
    //     must have survived the restart. `list_due` with a now_ms far
    //     in the future returns it.
    {
        let (adapter, _log) = open_store(&path).await;
        let due = PauseScheduleReadModel::list_due(adapter.as_ref(), &tenant, u64::MAX / 2, 100)
            .await
            .expect("list_due after restart");
        assert_eq!(
            due.len(),
            1,
            "pause_schedules row must survive a process restart — pre-#592 the \
             projection was a pure event-log walker in the in-memory shadow, and \
             pg/sqlite had no table at all, so `list_due` on a fresh pg/sqlite \
             boot returned empty until main.rs replayed the full event log into \
             the in-memory shadow (O(N) at boot)."
        );
        assert_eq!(due[0].run_id.as_str(), run_id);
        assert_eq!(due[0].project.tenant_id, tenant);
    }
}

/// Contract 2: a `RunStateChanged(Paused → Running)` DELETEs the
/// pause_schedules row. Pre-#592 `list_due` on the in-memory shadow
/// kept returning the resumed run because it re-computed from the
/// event log without pruning evicted entries (the handler had to
/// filter post-fetch via `runs.get`, which the unbounded-scan
/// comment at `list_due_run_resumes_handler` called out). Now the
/// projection table makes evict-on-resume atomic inside the same
/// transaction as the RunStateChanged commit.
#[tokio::test]
async fn pause_schedule_evicted_on_resume() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let tenant = TenantId::new("t_pause_evict");
    let project = ProjectKey::new(tenant.as_str(), "w", "p");
    let run_id = "run_pause_evict_a";

    let (adapter, log) = open_store(&path).await;

    // Append a pause with a scheduled resume. list_due sees it.
    log.append(&[evt("evt_pause_evict_1", paused_evt(run_id, &project, 0))])
        .await
        .expect("append paused");
    let before = PauseScheduleReadModel::list_due(adapter.as_ref(), &tenant, u64::MAX / 2, 100)
        .await
        .expect("list_due before resume");
    assert_eq!(
        before.len(),
        1,
        "after Paused commit the projection row must be live: {before:?}"
    );

    // Resume the run. list_due must NOT return the resumed run.
    log.append(&[evt("evt_pause_evict_2", resumed_evt(run_id, &project))])
        .await
        .expect("append resumed");
    let after = PauseScheduleReadModel::list_due(adapter.as_ref(), &tenant, u64::MAX / 2, 100)
        .await
        .expect("list_due after resume");
    assert!(
        after.is_empty(),
        "pause_schedules row must be evicted on the matching RunStateChanged \
         resume — #592 regressed otherwise: {after:?}"
    );
}

/// Contract 2b: terminal transitions (Completed / Failed / Canceled)
/// also evict. A run that completes while paused must not remain in
/// list_due for the handler to resurrect — the pre-#592 walker could
/// do that because it only checked for `Running`.
#[tokio::test]
async fn pause_schedule_evicted_on_terminal_completion() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let tenant = TenantId::new("t_pause_complete");
    let project = ProjectKey::new(tenant.as_str(), "w", "p");
    let run_id = "run_pause_complete_a";

    let (adapter, log) = open_store(&path).await;

    log.append(&[evt("evt_pause_complete_1", paused_evt(run_id, &project, 0))])
        .await
        .expect("append paused");

    // Fail transition.
    let failed = RuntimeEvent::RunStateChanged(RunStateChanged {
        run_id: RunId::new(run_id),
        project: project.clone(),
        transition: StateTransition {
            from: Some(RunState::Paused),
            to: RunState::Failed,
        },
        failure_class: Some(cairn_domain::lifecycle::FailureClass::ExecutionError),
        pause_reason: None,
        resume_trigger: None,
    });
    log.append(&[evt("evt_pause_complete_2", failed)])
        .await
        .expect("append failed");

    let after = PauseScheduleReadModel::list_due(adapter.as_ref(), &tenant, u64::MAX / 2, 100)
        .await
        .expect("list_due after failure");
    assert!(
        after.is_empty(),
        "pause_schedules row must be evicted on Paused → Failed, not just → \
         Running — the pre-#592 walker only checked for Running/Completed: \
         {after:?}"
    );
}

/// Copilot #595: `resume_at_ms` must be derived from the event's
/// durable wall-clock (`event_time_ms`), NOT the projection-apply
/// wall-clock. Proof: drive `SqliteSyncProjection::apply_async`
/// directly with a fixed `event_time_ms` that is deliberately far
/// from `now_millis()` — the row's `resume_at_ms` must equal
/// `event_time_ms + resume_after_ms`, not `now_ms + resume_after_ms`.
///
/// Rebuild safety: on `ProjectionRebuilder::rebuild_*` the caller
/// passes `StoredEvent.stored_at`; the guarantee this test locks in
/// is that `apply_async` honors whatever `event_time_ms` the caller
/// forwards (and does not fabricate `now` for the scheduled-resume
/// row).
#[tokio::test]
async fn pause_schedule_uses_event_time_not_apply_time() {
    use cairn_store::projections::PauseScheduleReadModel;
    use cairn_store::sqlite::SqliteSyncProjection;

    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let tenant = TenantId::new("t_pause_event_time");
    let project = ProjectKey::new(tenant.as_str(), "w", "p");
    let run_id = "run_pause_event_time_a";

    // Fixed historical event time (2023-11-14, years before any
    // test run). If the projection uses apply-time wall-clock
    // instead of the bound event timestamp, `resume_at_ms` will
    // differ from `event_time_ms + resume_after_ms` by the full
    // gap between the frozen constant and `now`.
    let event_time_ms: u64 = 1_700_000_000_000; // 2023-11-14, frozen
    let resume_after_ms: u64 = 60_000; // 60 seconds
    let expected_resume_at_ms: u64 = event_time_ms + resume_after_ms;

    let (adapter, _log) = open_store(&path).await;

    // Drive the projection applier directly so we control
    // `event_time_ms` rather than inheriting the event-log's
    // live-append wall-clock.
    let envelope = evt(
        "evt_pause_event_time_1",
        paused_evt(run_id, &project, resume_after_ms),
    );
    let mut tx = adapter.pool().begin().await.expect("tx begin");
    SqliteSyncProjection::apply_async(&mut tx, &envelope, event_time_ms)
        .await
        .expect("apply projection");
    tx.commit().await.expect("tx commit");

    let due = PauseScheduleReadModel::list_due(adapter.as_ref(), &tenant, u64::MAX / 2, 100)
        .await
        .expect("list_due");
    assert_eq!(
        due.len(),
        1,
        "projection must have inserted exactly one row"
    );
    assert_eq!(
        due[0].resume_at_ms, expected_resume_at_ms,
        "resume_at_ms must be event_time_ms + resume_after_ms, not \
         apply-time-derived — pre-#595 fix the projection computed \
         `resume_at_ms = SystemTime::now() + resume_after_ms` so on a \
         ProjectionRebuilder replay it shifted every row to rebuild time"
    );
    assert_eq!(
        due[0].created_at_ms, event_time_ms,
        "created_at_ms must also track event time so rebuilds are \
         byte-identical for the data-bearing columns"
    );
}

/// Pause without `resume_after_ms` must NOT land in pause_schedules —
/// those are approval waitpoints / indefinite holds, scheduled resume
/// is explicitly off. Guards against a regression that would land
/// every single paused run (including WaitingApproval passes that
/// collapse to Paused) into the scheduler queue.
#[tokio::test]
async fn pause_without_resume_after_ms_does_not_schedule() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let tenant = TenantId::new("t_pause_no_timer");
    let project = ProjectKey::new(tenant.as_str(), "w", "p");
    let run_id = "run_pause_no_timer_a";

    let (adapter, log) = open_store(&path).await;

    let paused_no_timer = RuntimeEvent::RunStateChanged(RunStateChanged {
        run_id: RunId::new(run_id),
        project: project.clone(),
        transition: StateTransition {
            from: Some(RunState::Running),
            to: RunState::Paused,
        },
        failure_class: None,
        pause_reason: Some(PauseReason {
            kind: PauseReasonKind::PolicyHold,
            detail: Some("approval:pending".to_owned()),
            resume_after_ms: None, // ← the whole point
            actor: None,
        }),
        resume_trigger: None,
    });
    log.append(&[evt("evt_pause_no_timer_1", paused_no_timer)])
        .await
        .expect("append paused");

    let due = PauseScheduleReadModel::list_due(adapter.as_ref(), &tenant, u64::MAX / 2, 100)
        .await
        .expect("list_due");
    assert!(
        due.is_empty(),
        "a pause with resume_after_ms=None must not create a pause_schedules \
         row — the scheduled-resume queue is for timer-fired resumes only, \
         approval holds resume on signal: {due:?}"
    );
}
