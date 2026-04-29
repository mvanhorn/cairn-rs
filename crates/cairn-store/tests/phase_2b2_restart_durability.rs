//! RFC-025 Phase 2b.2 restart-durability proof for the `external_workers`
//! projection table shipped in this phase.
//!
//! Pre-Phase-2b.2 the pg/sqlite applier was `log_stub(..)` for the four
//! `ExternalWorker*` variants: the event log kept the event, but
//! `ExternalWorkerReadModel::get` / `list_by_tenant` on a fresh boot
//! returned empty. Operators' fleet catalog was wiped by every restart
//! on persistent backends (GAP-005).
//!
//! Shape mirrors `phase_2b1_restart_durability.rs`: boot a file-backed
//! SQLite adapter, write the lifecycle events, drop the pool
//! (simulating process exit), re-open the same DB file, assert the
//! read-model row is still there.
//!
//! SQLite-only; pg equivalents are covered by the shared projection
//! applier semantics under nightly CI via `TEST_DATABASE_URL`.

#![cfg(feature = "sqlite")]

use std::str::FromStr;
use std::sync::Arc;

use cairn_domain::{
    workers::ExternalWorkerReport, EventEnvelope, EventId, EventSource, ExternalWorkerReactivated,
    ExternalWorkerRegistered, ExternalWorkerReported, ExternalWorkerSuspended, ProjectKey,
    RuntimeEvent, TaskId, TenantId, WorkerId,
};
use cairn_store::db::DbAdapter;
use cairn_store::projections::ExternalWorkerReadModel;
use cairn_store::{sqlite::SqliteAdapter, sqlite::SqliteEventLog, EventLog};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

fn evt(id: &str, payload: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(EventId::new(id), EventSource::Runtime, payload)
}

/// Sentinel ProjectKey for tenant-scoped worker events (no real project).
fn sentinel(tenant: &str) -> ProjectKey {
    ProjectKey::new(tenant, "_", "_")
}

/// Open a file-backed SQLite pool + adapter + event-log at `db_path`.
/// Dropping the returned tuple closes the pool — simulating a process
/// exit. Matches the helper in `phase_2b1_restart_durability.rs`.
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

/// The full worker lifecycle (Register → Heartbeat → Suspend → Reactivate)
/// persisted across a simulated restart must round-trip every projected
/// field: status canonicalisation, heartbeat timestamp + alive flag,
/// current_task_id (preserved from the active heartbeat because no
/// terminal outcome was reported), tenant scoping.
#[tokio::test]
async fn external_worker_lifecycle_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let tenant = TenantId::new("t_worker_restart");
    let worker_id = WorkerId::new("w_restart_1");

    // ── Session 1: drive the full lifecycle, drop the pool. ───────
    {
        let (adapter, log) = open_store(&path).await;
        let events = vec![
            evt(
                "evt_ew_restart_reg",
                RuntimeEvent::ExternalWorkerRegistered(ExternalWorkerRegistered {
                    sentinel_project: sentinel("t_worker_restart"),
                    worker_id: worker_id.clone(),
                    tenant_id: tenant.clone(),
                    display_name: "Restart Bot".to_owned(),
                    registered_at: 1_700_000_000_000,
                }),
            ),
            evt(
                "evt_ew_restart_hb",
                RuntimeEvent::ExternalWorkerReported(ExternalWorkerReported {
                    report: ExternalWorkerReport {
                        project: sentinel("t_worker_restart"),
                        worker_id: worker_id.clone(),
                        run_id: None,
                        task_id: TaskId::new("task_restart_1"),
                        lease_token: 1,
                        reported_at_ms: 1_700_000_001_000,
                        progress: None,
                        outcome: None,
                    },
                }),
            ),
            evt(
                "evt_ew_restart_suspend",
                RuntimeEvent::ExternalWorkerSuspended(ExternalWorkerSuspended {
                    sentinel_project: sentinel("t_worker_restart"),
                    worker_id: worker_id.clone(),
                    tenant_id: tenant.clone(),
                    suspended_at: 1_700_000_002_000,
                    reason: Some("maintenance window".to_owned()),
                }),
            ),
            evt(
                "evt_ew_restart_react",
                RuntimeEvent::ExternalWorkerReactivated(ExternalWorkerReactivated {
                    sentinel_project: sentinel("t_worker_restart"),
                    worker_id: worker_id.clone(),
                    tenant_id: tenant.clone(),
                    reactivated_at: 1_700_000_003_000,
                }),
            ),
        ];
        log.append(&events).await.expect("append worker lifecycle");

        // Visible pre-restart — catches a write-path regression before
        // we bother comparing the post-restart read.
        let pre = ExternalWorkerReadModel::get(adapter.as_ref(), &worker_id)
            .await
            .unwrap()
            .expect("worker must be visible before restart");
        assert_eq!(pre.status, "active", "reactivated worker is active");
        assert!(pre.health.is_alive, "heartbeat flipped is_alive=true");
        assert_eq!(pre.health.last_heartbeat_ms, 1_700_000_001_000);
        assert_eq!(pre.current_task_id, Some(TaskId::new("task_restart_1")));
    } // adapter + log dropped; sqlite pool closed here.

    // ── Session 2: re-open the DB and prove persistence. ──────────
    let (adapter, _log) = open_store(&path).await;

    let after = ExternalWorkerReadModel::get(adapter.as_ref(), &worker_id)
        .await
        .unwrap()
        .expect("worker must survive restart");
    assert_eq!(after.worker_id, worker_id);
    assert_eq!(after.tenant_id, tenant);
    assert_eq!(after.display_name, "Restart Bot");
    assert_eq!(
        after.status, "active",
        "Reactivated status must survive restart (was last suspended → reactivated)"
    );
    assert_eq!(after.registered_at, 1_700_000_000_000);
    assert!(
        after.health.is_alive,
        "heartbeat-established is_alive=true must persist"
    );
    assert_eq!(after.health.last_heartbeat_ms, 1_700_000_001_000);
    assert_eq!(
        after.health.active_task_count, 0,
        "projection does not track active_task_count writes yet"
    );
    assert_eq!(
        after.current_task_id,
        Some(TaskId::new("task_restart_1")),
        "active-heartbeat current_task_id must persist (no terminal outcome reported)"
    );

    // list_by_tenant on a cold adapter must find the row — the failure
    // mode pre-2b.2 was exactly this query returning empty because the
    // applier was `log_stub`.
    let listed = ExternalWorkerReadModel::list_by_tenant(adapter.as_ref(), &tenant, 10, 0)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].worker_id, worker_id);
}

/// Multi-worker scoping across restart: two workers registered under
/// different tenants in session 1 must still be tenant-isolated in
/// session 2 (regression for a cross-tenant leak).
#[tokio::test]
async fn external_worker_tenant_isolation_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let tenant_a = TenantId::new("t_iso_a");
    let tenant_b = TenantId::new("t_iso_b");

    {
        let (_adapter, log) = open_store(&path).await;
        let events = vec![
            evt(
                "evt_iso_a_1",
                RuntimeEvent::ExternalWorkerRegistered(ExternalWorkerRegistered {
                    sentinel_project: sentinel("t_iso_a"),
                    worker_id: WorkerId::new("w_iso_a_1"),
                    tenant_id: tenant_a.clone(),
                    display_name: "A1".to_owned(),
                    registered_at: 1_700_000_100_000,
                }),
            ),
            evt(
                "evt_iso_a_2",
                RuntimeEvent::ExternalWorkerRegistered(ExternalWorkerRegistered {
                    sentinel_project: sentinel("t_iso_a"),
                    worker_id: WorkerId::new("w_iso_a_2"),
                    tenant_id: tenant_a.clone(),
                    display_name: "A2".to_owned(),
                    registered_at: 1_700_000_101_000,
                }),
            ),
            evt(
                "evt_iso_b_1",
                RuntimeEvent::ExternalWorkerRegistered(ExternalWorkerRegistered {
                    sentinel_project: sentinel("t_iso_b"),
                    worker_id: WorkerId::new("w_iso_b_1"),
                    tenant_id: tenant_b.clone(),
                    display_name: "B1".to_owned(),
                    registered_at: 1_700_000_102_000,
                }),
            ),
        ];
        log.append(&events).await.expect("append");
    }

    // Session 2: list_by_tenant must return tenant-scoped results only.
    let (adapter, _log) = open_store(&path).await;
    let a_workers = ExternalWorkerReadModel::list_by_tenant(adapter.as_ref(), &tenant_a, 10, 0)
        .await
        .unwrap();
    let b_workers = ExternalWorkerReadModel::list_by_tenant(adapter.as_ref(), &tenant_b, 10, 0)
        .await
        .unwrap();
    assert_eq!(a_workers.len(), 2);
    assert_eq!(b_workers.len(), 1);
    assert!(a_workers.iter().all(|w| w.tenant_id.as_str() == "t_iso_a"));
    assert!(b_workers.iter().all(|w| w.tenant_id.as_str() == "t_iso_b"));
    assert_eq!(b_workers[0].worker_id.as_str(), "w_iso_b_1");
}
