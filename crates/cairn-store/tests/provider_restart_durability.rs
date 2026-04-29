//! RFC-025 Phase 3 restart-durability proof for provider_bindings +
//! provider_connections projections.
//!
//! The core Phase 3 contract: operator-configured provider state survives
//! a process restart. Before Phase 3 the pg/sqlite projection arms for
//! `ProviderBindingCreated` / `ProviderBindingStateChanged` /
//! `ProviderConnectionRegistered` / `ProviderConnectionDeleted` were
//! `log_stub` no-ops — only the InMemoryStore had state, and a restart
//! wiped it. An operator saw `GET /v1/providers/connections` return empty
//! one tick after the register event succeeded.
//!
//! This test boots a file-backed SQLite DB, writes both event types,
//! closes the pool (simulating process exit), re-opens the same DB file,
//! and asserts the projection rows are still there with the expected
//! content. The parity harness in `projection_parity.rs` proves the
//! write side matches across backends; this harness proves the read
//! side also survives a cold restart.
//!
//! SQLite-only because the pg equivalent needs `TEST_DATABASE_URL`; the
//! pg durability contract is covered by the shared projection applier
//! semantics (same `ON CONFLICT` clauses) verified by parity tests that
//! run under nightly CI.

#![cfg(feature = "sqlite")]

use std::str::FromStr;
use std::sync::Arc;

use cairn_domain::providers::{
    OperationKind, ProviderBindingSettings, ProviderConnectionStatus, StructuredOutputMode,
};
use cairn_domain::tenancy::TenantKey;
use cairn_domain::{
    EventEnvelope, EventId, EventSource, ProjectId, ProjectKey, ProviderBindingCreated,
    ProviderBindingId, ProviderBindingStateChanged, ProviderConnectionId,
    ProviderConnectionRegistered, ProviderModelId, RuntimeEvent, TenantId, WorkspaceId,
};
use cairn_store::db::DbAdapter;
use cairn_store::projections::{ProviderBindingReadModel, ProviderConnectionReadModel};
use cairn_store::{sqlite::SqliteAdapter, sqlite::SqliteEventLog, EventLog};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

fn evt(id: &str, payload: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(EventId::new(id), EventSource::Runtime, payload)
}

fn proj() -> ProjectKey {
    ProjectKey {
        tenant_id: TenantId::new("t_restart"),
        workspace_id: WorkspaceId::new("w_restart"),
        project_id: ProjectId::new("p_restart"),
    }
}

fn settings() -> ProviderBindingSettings {
    ProviderBindingSettings {
        temperature_milli: Some(500),
        max_output_tokens: Some(2048),
        timeout_ms: Some(15_000),
        structured_output_mode: StructuredOutputMode::Default,
        required_capabilities: vec![],
        disabled_capabilities: vec![],
        ..Default::default()
    }
}

/// Open a file-backed SQLite pool + adapter + event-log. The pool holds
/// `max_connections` connections against the same on-disk DB file, so
/// dropping the tuple at the end of scope simulates a process exit.
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

#[tokio::test]
async fn provider_connection_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let tenant = TenantId::new("t_restart");
    let conn_id = ProviderConnectionId::new("conn_restart_1");

    // ── Session 1: register the connection, drop the pool. ────────
    {
        let (adapter, log) = open_store(&path).await;
        let register = evt(
            "evt_restart_conn_reg",
            RuntimeEvent::ProviderConnectionRegistered(ProviderConnectionRegistered {
                tenant: TenantKey::new(tenant.as_str()),
                provider_connection_id: conn_id.clone(),
                provider_family: "openai".to_owned(),
                adapter_type: "responses".to_owned(),
                supported_models: vec!["gpt-4o".to_owned(), "gpt-4o-mini".to_owned()],
                status: ProviderConnectionStatus::Active,
                registered_at: 1_700_000_000_000,
            }),
        );
        log.append(&[register]).await.expect("append register");

        // Confirm visible pre-restart.
        let pre = ProviderConnectionReadModel::get(adapter.as_ref(), &conn_id)
            .await
            .unwrap();
        assert!(pre.is_some(), "connection must be visible before restart");
    } // adapter + log dropped; sqlite pool closed here.

    // ── Session 2: reopen the DB, prove persistence. ─────────────
    let (adapter, _log) = open_store(&path).await;
    let after = ProviderConnectionReadModel::get(adapter.as_ref(), &conn_id)
        .await
        .unwrap()
        .expect("connection must survive restart");
    assert_eq!(after.provider_connection_id, conn_id);
    assert_eq!(after.provider_family, "openai");
    assert_eq!(after.adapter_type, "responses");
    assert_eq!(after.supported_models, vec!["gpt-4o", "gpt-4o-mini"]);
    assert_eq!(after.status, ProviderConnectionStatus::Active);
    assert_eq!(after.created_at, 1_700_000_000_000);

    // list_by_tenant must also find it.
    let listed = ProviderConnectionReadModel::list_by_tenant(adapter.as_ref(), &tenant, 10, 0)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].provider_connection_id, conn_id);
}

#[tokio::test]
async fn provider_binding_with_state_change_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let project = proj();
    let binding_id = ProviderBindingId::new("pb_restart_1");
    let conn_id = ProviderConnectionId::new("conn_restart_b");
    let model_id = ProviderModelId::new("gpt-4o");

    // ── Session 1: create binding, then deactivate it via state change.
    {
        let (_adapter, log) = open_store(&path).await;
        let events = vec![
            evt(
                "evt_restart_bind_create",
                RuntimeEvent::ProviderBindingCreated(ProviderBindingCreated {
                    project: project.clone(),
                    provider_binding_id: binding_id.clone(),
                    provider_connection_id: conn_id.clone(),
                    provider_model_id: model_id.clone(),
                    operation_kind: OperationKind::Generate,
                    settings: settings(),
                    policy_id: None,
                    active: true,
                    created_at: 1_700_000_000_000,
                    estimated_cost_micros: None,
                }),
            ),
            evt(
                "evt_restart_bind_deactivate",
                RuntimeEvent::ProviderBindingStateChanged(ProviderBindingStateChanged {
                    project: project.clone(),
                    provider_binding_id: binding_id.clone(),
                    active: false,
                    changed_at: 1_700_000_100_000,
                }),
            ),
        ];
        log.append(&events).await.expect("append");
    }

    // ── Session 2: reopen, confirm binding is present AND still deactivated.
    let (adapter, _log) = open_store(&path).await;
    let binding = ProviderBindingReadModel::get(adapter.as_ref(), &binding_id)
        .await
        .unwrap()
        .expect("binding must survive restart");
    assert_eq!(binding.provider_binding_id, binding_id);
    assert_eq!(binding.project, project);
    assert_eq!(binding.provider_connection_id, conn_id);
    assert_eq!(binding.provider_model_id, model_id);
    assert_eq!(binding.operation_kind, OperationKind::Generate);
    assert!(
        !binding.active,
        "deactivation must survive restart (Phase 3 regression guard)"
    );
    assert_eq!(binding.settings, settings());

    // list_active must exclude it.
    let active =
        ProviderBindingReadModel::list_active(adapter.as_ref(), &project, OperationKind::Generate)
            .await
            .unwrap();
    assert!(
        active.is_empty(),
        "deactivated binding must not appear in list_active after restart"
    );

    // list_by_project still finds it (audit / operator dashboard view).
    let all = ProviderBindingReadModel::list_by_project(adapter.as_ref(), &project, 10, 0)
        .await
        .unwrap();
    assert_eq!(all.len(), 1);
    assert!(!all[0].active);
}
