//! RFC-025 Phase 2a.2 milestone 3: retention_policies projection tests.
//!
//! Parity with pg/sqlite is covered by `projection_parity.rs`; this
//! file focuses on restart durability and replay semantics.

use cairn_domain::{
    EventEnvelope, EventId, EventSource, RetentionPolicySet, RuntimeEvent, TenantId,
};
use cairn_store::{
    projections::RetentionPolicyReadModel, sqlite::SqliteAdapter, EventLog, InMemoryStore,
};

fn evt(id: &str, payload: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(EventId::new(id), EventSource::Runtime, payload)
}

fn set_policy(
    tenant: &str,
    policy_id: &str,
    full_history_days: u32,
    current_state_days: u32,
    max_events: Option<u64>,
) -> RuntimeEvent {
    RuntimeEvent::RetentionPolicySet(RetentionPolicySet {
        tenant_id: TenantId::new(tenant),
        policy_id: policy_id.to_owned(),
        full_history_days,
        current_state_days,
        max_events_per_entity: max_events,
    })
}

// ── 1. Upsert semantics — latest write wins ─────────────────────────────────

#[tokio::test]
async fn policy_set_twice_keeps_latest_in_memory() {
    let store = InMemoryStore::new();
    store
        .append(&[
            evt("e1", set_policy("t_up", "pol_a", 7, 30, Some(100))),
            evt("e2", set_policy("t_up", "pol_b", 14, 60, None)),
        ])
        .await
        .unwrap();

    let pol = RetentionPolicyReadModel::get_by_tenant(&store, &TenantId::new("t_up"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pol.policy_id, "pol_b");
    assert_eq!(pol.full_history_days, 14);
    // `None` max_events maps to 0 sentinel on every backend.
    assert_eq!(pol.max_events_per_entity, 0);
}

// ── 2. Tenant isolation ─────────────────────────────────────────────────────

#[tokio::test]
async fn retention_policies_are_scoped_to_tenant_in_memory() {
    let store = InMemoryStore::new();
    store
        .append(&[
            evt("e1", set_policy("t_a", "pol_a", 1, 1, None)),
            evt("e2", set_policy("t_b", "pol_b", 2, 2, None)),
        ])
        .await
        .unwrap();

    let a = RetentionPolicyReadModel::get_by_tenant(&store, &TenantId::new("t_a"))
        .await
        .unwrap()
        .unwrap();
    let b = RetentionPolicyReadModel::get_by_tenant(&store, &TenantId::new("t_b"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.full_history_days, 1);
    assert_eq!(b.full_history_days, 2);

    let ghost = RetentionPolicyReadModel::get_by_tenant(&store, &TenantId::new("t_ghost"))
        .await
        .unwrap();
    assert!(ghost.is_none());
}

// ── 3. Restart durability on sqlite ─────────────────────────────────────────

#[tokio::test]
async fn sqlite_retention_policy_survives_adapter_restart() {
    use cairn_store::db::DbAdapter;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use std::str::FromStr;

    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let url = format!("sqlite:{}", tmp.path().display());

    // Boot 1.
    {
        let opts = SqliteConnectOptions::from_str(&url)
            .expect("sqlite url")
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await
            .unwrap();
        let adapter = SqliteAdapter::new(pool.clone());
        adapter.migrate().await.unwrap();
        let log = cairn_store::sqlite::SqliteEventLog::new(pool);
        log.append(&[evt(
            "e1",
            set_policy("t_durable", "pol_durable", 30, 365, Some(5_000)),
        )])
        .await
        .unwrap();
    }

    // Boot 2.
    let opts2 = SqliteConnectOptions::from_str(&url)
        .expect("sqlite url")
        .create_if_missing(false)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
    let pool2 = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opts2)
        .await
        .unwrap();
    let adapter2 = SqliteAdapter::new(pool2);
    adapter2.migrate().await.unwrap();
    let pol = RetentionPolicyReadModel::get_by_tenant(&adapter2, &TenantId::new("t_durable"))
        .await
        .unwrap()
        .expect("policy survives restart");
    assert_eq!(pol.policy_id, "pol_durable");
    assert_eq!(pol.full_history_days, 30);
    assert_eq!(pol.current_state_days, 365);
    assert_eq!(pol.max_events_per_entity, 5_000);
}
