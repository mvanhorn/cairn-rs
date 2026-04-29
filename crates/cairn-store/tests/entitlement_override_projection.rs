//! RFC-025 Phase 2a.2 milestone 4: entitlement_overrides projection tests.
//!
//! Parity with pg/sqlite is covered by `projection_parity.rs`; this
//! file focuses on restart durability and cross-tenant isolation.

use cairn_domain::{
    EntitlementOverrideSet, EventEnvelope, EventId, EventSource, RuntimeEvent, TenantId,
};
use cairn_store::{projections::LicenseReadModel, sqlite::SqliteAdapter, EventLog, InMemoryStore};

fn evt(id: &str, payload: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(EventId::new(id), EventSource::Runtime, payload)
}

fn set_override(
    tenant: &str,
    feature: &str,
    allowed: bool,
    reason: Option<&str>,
    set_at_ms: u64,
) -> RuntimeEvent {
    RuntimeEvent::EntitlementOverrideSet(EntitlementOverrideSet {
        tenant_id: TenantId::new(tenant),
        feature: feature.to_owned(),
        allowed,
        reason: reason.map(str::to_owned),
        set_at_ms,
    })
}

// ── 1. Upsert on (tenant, feature) — latest write wins ─────────────────────

#[tokio::test]
async fn override_set_twice_keeps_latest_in_memory() {
    let store = InMemoryStore::new();
    store
        .append(&[
            evt(
                "e1",
                set_override("t_up", "f_a", true, Some("initial"), 100),
            ),
            evt(
                "e2",
                set_override("t_up", "f_a", false, Some("revoked"), 200),
            ),
        ])
        .await
        .unwrap();

    let rows = LicenseReadModel::list_overrides(&store, &TenantId::new("t_up"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(!rows[0].allowed);
    assert_eq!(rows[0].reason.as_deref(), Some("revoked"));
    assert_eq!(rows[0].set_at_ms, 200);
}

// ── 2. Tenant isolation ────────────────────────────────────────────────────

#[tokio::test]
async fn overrides_are_scoped_to_tenant_in_memory() {
    let store = InMemoryStore::new();
    store
        .append(&[
            evt("e1", set_override("t_a", "f_a", true, None, 100)),
            evt("e2", set_override("t_b", "f_b", true, None, 200)),
        ])
        .await
        .unwrap();

    let a = LicenseReadModel::list_overrides(&store, &TenantId::new("t_a"))
        .await
        .unwrap();
    let b = LicenseReadModel::list_overrides(&store, &TenantId::new("t_b"))
        .await
        .unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].feature, "f_a");
    assert_eq!(b.len(), 1);
    assert_eq!(b[0].feature, "f_b");

    let empty = LicenseReadModel::list_overrides(&store, &TenantId::new("t_ghost"))
        .await
        .unwrap();
    assert!(empty.is_empty());
}

// ── 3. Multi-feature list ordering ─────────────────────────────────────────

#[tokio::test]
async fn multiple_features_return_sorted_in_memory() {
    let store = InMemoryStore::new();
    // Insert out of alphabetical order so the sort is actually exercised.
    store
        .append(&[
            evt(
                "e1",
                set_override("t_sort", "multi_provider", true, None, 100),
            ),
            evt(
                "e2",
                set_override("t_sort", "eval_matrices", true, None, 200),
            ),
            evt(
                "e3",
                set_override("t_sort", "governance_compliance", true, None, 300),
            ),
        ])
        .await
        .unwrap();

    let rows = LicenseReadModel::list_overrides(&store, &TenantId::new("t_sort"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    let features: Vec<&str> = rows.iter().map(|r| r.feature.as_str()).collect();
    assert_eq!(
        features,
        vec!["eval_matrices", "governance_compliance", "multi_provider"]
    );
}

// ── 4. Restart durability on sqlite ────────────────────────────────────────

#[tokio::test]
async fn sqlite_overrides_survive_adapter_restart() {
    use cairn_store::db::DbAdapter;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use std::str::FromStr;

    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let url = format!("sqlite:{}", tmp.path().display());

    // Boot 1 — write two overrides.
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
        log.append(&[
            evt(
                "e1",
                set_override("t_durable", "eval_matrices", true, Some("pilot"), 1_000),
            ),
            evt(
                "e2",
                set_override("t_durable", "multi_provider", false, None, 2_000),
            ),
        ])
        .await
        .unwrap();
    }

    // Boot 2 — rehydrate and verify both rows.
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

    let rows = LicenseReadModel::list_overrides(&adapter2, &TenantId::new("t_durable"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    // Sorted by feature ASC.
    assert_eq!(rows[0].feature, "eval_matrices");
    assert!(rows[0].allowed);
    assert_eq!(rows[0].reason.as_deref(), Some("pilot"));
    assert_eq!(rows[0].set_at_ms, 1_000);
    assert_eq!(rows[1].feature, "multi_provider");
    assert!(!rows[1].allowed);
    assert!(rows[1].reason.is_none());
    assert_eq!(rows[1].set_at_ms, 2_000);
}
