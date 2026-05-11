//! RFC-025 Phase 2b.3 restart-durability proofs.
//!
//! One lifecycle round-trip per service this milestone flips
//! Stubbed → Projected. Pre-Phase-2b.3 the pg/sqlite applier was
//! `log_stub(..)` for each variant below: the event log kept the
//! event, but the read-model on a fresh boot returned empty. Operator
//! surfaces (ingest-job catalog, default-settings store, channels,
//! notification ledger, checkpoint strategy) were wiped by every
//! restart on persistent backends.
//!
//! Shape mirrors `phase_2b1_restart_durability.rs` / `phase_2b2_*`:
//! boot a file-backed SQLite adapter, write the lifecycle events, drop
//! the pool (simulating process exit), re-open the same DB file,
//! assert the read-model row is still there.
//!
//! SQLite-only; pg equivalents are covered by the shared projection
//! applier semantics under nightly CI via `TEST_DATABASE_URL`.

#![cfg(feature = "sqlite")]

use std::str::FromStr;
use std::sync::Arc;

use cairn_domain::{
    notification_prefs::NotificationChannel, ChannelCreated, ChannelId, ChannelMessageConsumed,
    ChannelMessageSent, CheckpointStrategySet, DefaultSettingCleared, DefaultSettingSet,
    EventEnvelope, EventId, EventSource, IngestJobCompleted, IngestJobId, IngestJobStarted,
    IngestJobState, NotificationPreferenceSet, NotificationSent, OwnershipKey, ProjectKey, RunId,
    RuntimeEvent, Scope, SourceId, TenantId,
};
use cairn_store::db::DbAdapter;
use cairn_store::projections::{
    ChannelReadModel, CheckpointStrategyReadModel, DefaultsReadModel, IngestJobReadModel,
    NotificationReadModel,
};
use cairn_store::{sqlite::SqliteAdapter, sqlite::SqliteEventLog, EventLog};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

fn evt(id: &str, payload: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(EventId::new(id), EventSource::Runtime, payload)
}

fn project(tag: &str) -> ProjectKey {
    ProjectKey::new(format!("t_{tag}"), format!("w_{tag}"), format!("p_{tag}"))
}

/// Open a file-backed SQLite pool + adapter + event-log at `db_path`.
/// Dropping the returned tuple closes the pool — simulating a process
/// exit. Matches the helper in `phase_2b1_restart_durability.rs` /
/// `phase_2b2_restart_durability.rs`.
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

/// Full ingest-job lifecycle (Started → Completed-with-success) persists
/// every projected field across a simulated restart: state flip, error
/// message (None on success), updated_at advance, document_count.
#[tokio::test]
async fn ingest_job_success_lifecycle_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let proj = project("ingest_ok");
    let job_id = IngestJobId::new("job_restart_ok");

    // ── Session 1: Start → Complete(success) then drop the pool. ──
    {
        let (adapter, log) = open_store(&path).await;
        let events = vec![
            evt(
                "evt_ij_start",
                RuntimeEvent::IngestJobStarted(IngestJobStarted {
                    project: proj.clone(),
                    job_id: job_id.clone(),
                    source_id: Some(SourceId::new("src_restart")),
                    document_count: 42,
                    started_at: 1_700_000_000_000,
                }),
            ),
            evt(
                "evt_ij_done",
                RuntimeEvent::IngestJobCompleted(IngestJobCompleted {
                    project: proj.clone(),
                    job_id: job_id.clone(),
                    success: true,
                    error_message: None,
                    completed_at: 1_700_000_010_000,
                }),
            ),
        ];
        log.append(&events).await.expect("append lifecycle");

        // Pre-restart sanity — catches write-path regressions before
        // bothering to compare post-restart.
        let pre = IngestJobReadModel::get(adapter.as_ref(), &job_id)
            .await
            .unwrap()
            .expect("job must be visible before restart");
        assert_eq!(pre.state, IngestJobState::Completed);
        assert_eq!(pre.document_count, 42);
        assert_eq!(pre.updated_at, 1_700_000_010_000);
        assert!(pre.error_message.is_none());
    } // adapter + log dropped; sqlite pool closed here.

    // ── Session 2: re-open the DB and prove persistence. ──
    let (adapter, _log) = open_store(&path).await;

    let after = IngestJobReadModel::get(adapter.as_ref(), &job_id)
        .await
        .unwrap()
        .expect("job must survive restart");
    assert_eq!(after.id, job_id);
    assert_eq!(after.project, proj);
    assert_eq!(
        after.source_id.as_ref().map(|s| s.as_str()),
        Some("src_restart")
    );
    assert_eq!(after.document_count, 42);
    assert_eq!(
        after.state,
        IngestJobState::Completed,
        "Completed-with-success must persist as Completed"
    );
    assert_eq!(after.error_message, None);
    assert_eq!(after.created_at, 1_700_000_000_000);
    assert_eq!(after.updated_at, 1_700_000_010_000);

    // list_by_project on a cold adapter must find the row — the failure
    // mode pre-2b.3 was exactly this query returning empty because the
    // applier was `log_stub`.
    let listed = IngestJobReadModel::list_by_project(adapter.as_ref(), &proj, 10, 0)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, job_id);
}

/// Completed(success=false) persists as `Failed` with the error message
/// across restart.
#[tokio::test]
async fn ingest_job_failure_lifecycle_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let proj = project("ingest_err");
    let job_id = IngestJobId::new("job_restart_err");

    {
        let (_adapter, log) = open_store(&path).await;
        let events = vec![
            evt(
                "evt_ij_err_start",
                RuntimeEvent::IngestJobStarted(IngestJobStarted {
                    project: proj.clone(),
                    job_id: job_id.clone(),
                    source_id: None,
                    document_count: 7,
                    started_at: 1_700_000_100_000,
                }),
            ),
            evt(
                "evt_ij_err_done",
                RuntimeEvent::IngestJobCompleted(IngestJobCompleted {
                    project: proj.clone(),
                    job_id: job_id.clone(),
                    success: false,
                    error_message: Some("chunk embed timeout".to_owned()),
                    completed_at: 1_700_000_101_000,
                }),
            ),
        ];
        log.append(&events).await.expect("append failure lifecycle");
    }

    let (adapter, _log) = open_store(&path).await;
    let after = IngestJobReadModel::get(adapter.as_ref(), &job_id)
        .await
        .unwrap()
        .expect("job must survive restart");
    assert_eq!(
        after.state,
        IngestJobState::Failed,
        "Completed(success=false) must persist as Failed"
    );
    assert_eq!(after.error_message.as_deref(), Some("chunk embed timeout"));
    assert_eq!(after.updated_at, 1_700_000_101_000);
    assert_eq!(after.source_id, None, "source_id=None round-trips");
}

/// Project scoping across restart: two jobs in different projects must
/// still be project-isolated in session 2 (regression for a cross-project
/// leak in the read-model query).
#[tokio::test]
async fn ingest_job_project_isolation_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let proj_a = project("iso_a");
    let proj_b = project("iso_b");

    {
        let (_adapter, log) = open_store(&path).await;
        let events = vec![
            evt(
                "evt_iso_a",
                RuntimeEvent::IngestJobStarted(IngestJobStarted {
                    project: proj_a.clone(),
                    job_id: IngestJobId::new("job_iso_a"),
                    source_id: None,
                    document_count: 1,
                    started_at: 1_700_000_200_000,
                }),
            ),
            evt(
                "evt_iso_b",
                RuntimeEvent::IngestJobStarted(IngestJobStarted {
                    project: proj_b.clone(),
                    job_id: IngestJobId::new("job_iso_b"),
                    source_id: None,
                    document_count: 1,
                    started_at: 1_700_000_201_000,
                }),
            ),
        ];
        log.append(&events).await.expect("append iso jobs");
    }

    let (adapter, _log) = open_store(&path).await;
    let list_a = IngestJobReadModel::list_by_project(adapter.as_ref(), &proj_a, 10, 0)
        .await
        .unwrap();
    let list_b = IngestJobReadModel::list_by_project(adapter.as_ref(), &proj_b, 10, 0)
        .await
        .unwrap();
    assert_eq!(list_a.len(), 1);
    assert_eq!(list_b.len(), 1);
    assert_eq!(list_a[0].id.as_str(), "job_iso_a");
    assert_eq!(list_b[0].id.as_str(), "job_iso_b");
}

/// Replay safety: appending `IngestJobStarted` twice for the same
/// `job_id` must leave the first-write state intact (ON CONFLICT DO
/// NOTHING). Also: a `IngestJobCompleted` that arrives before `Started`
/// is a no-op (UPDATE of a non-existent row).
#[tokio::test]
async fn ingest_job_replay_is_idempotent_across_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let proj = project("ij_replay");
    let job_id = IngestJobId::new("job_replay");

    {
        let (_adapter, log) = open_store(&path).await;
        let events = vec![
            // First Started
            evt(
                "evt_rep_1",
                RuntimeEvent::IngestJobStarted(IngestJobStarted {
                    project: proj.clone(),
                    job_id: job_id.clone(),
                    source_id: Some(SourceId::new("src_first")),
                    document_count: 10,
                    started_at: 1_700_000_300_000,
                }),
            ),
            // Replayed Started — must NOT clobber source_id or
            // document_count (ON CONFLICT DO NOTHING).
            evt(
                "evt_rep_2",
                RuntimeEvent::IngestJobStarted(IngestJobStarted {
                    project: proj.clone(),
                    job_id: job_id.clone(),
                    source_id: Some(SourceId::new("src_second")),
                    document_count: 9999,
                    started_at: 1_700_000_301_000,
                }),
            ),
            // A Completed with `success=true` — state must become
            // Completed, error_message None.
            evt(
                "evt_rep_done",
                RuntimeEvent::IngestJobCompleted(IngestJobCompleted {
                    project: proj.clone(),
                    job_id: job_id.clone(),
                    success: true,
                    error_message: None,
                    completed_at: 1_700_000_302_000,
                }),
            ),
            // Orphan Completed — the job does not exist on this
            // job_id, so the UPDATE must be a no-op (no row inserted).
            evt(
                "evt_rep_orphan",
                RuntimeEvent::IngestJobCompleted(IngestJobCompleted {
                    project: proj.clone(),
                    job_id: IngestJobId::new("job_no_start"),
                    success: true,
                    error_message: None,
                    completed_at: 1_700_000_303_000,
                }),
            ),
        ];
        log.append(&events).await.expect("append replay chain");
    }

    let (adapter, _log) = open_store(&path).await;

    // First write wins on Started; Completed flipped state.
    let after = IngestJobReadModel::get(adapter.as_ref(), &job_id)
        .await
        .unwrap()
        .expect("job must survive restart");
    assert_eq!(
        after.source_id.as_ref().map(|s| s.as_str()),
        Some("src_first"),
        "first Started write wins; replayed Started must not clobber"
    );
    assert_eq!(after.document_count, 10);
    assert_eq!(after.state, IngestJobState::Completed);
    assert_eq!(after.updated_at, 1_700_000_302_000);

    // Orphan Completed did not materialize a row.
    let orphan = IngestJobReadModel::get(adapter.as_ref(), &IngestJobId::new("job_no_start"))
        .await
        .unwrap();
    assert!(
        orphan.is_none(),
        "Completed without a prior Started must not create a row (UPDATE no-op)"
    );
}

// ── RFC-025 Phase 2b.3 m2: default_settings ──────────────────────────

/// `DefaultSettingSet` payloads don't carry a `project` field (they're
/// `OwnershipKey::System`), so use a raw envelope instead of
/// `for_runtime_event`.
fn sys_evt(id: &str, payload: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope {
        event_id: EventId::new(id),
        source: EventSource::Runtime,
        ownership: OwnershipKey::System,
        causation_id: None,
        correlation_id: None,
        payload,
    }
}

/// Set → Set (upsert) → Cleared → Set (resurrect). Every edge must
/// survive restart with last-write-wins semantics and hard-delete on
/// Cleared. This exercises the full projection lifecycle in one
/// chain so a missing arm surfaces as a read mismatch.
#[tokio::test]
async fn default_settings_lifecycle_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let scope_id = "tenant_ds_restart";
    let key_a = "default.model".to_owned();
    let key_b = "default.timeout_ms".to_owned();
    let key_c = "default.experimental".to_owned();

    {
        let (adapter, log) = open_store(&path).await;
        let events = vec![
            sys_evt(
                "evt_ds_a1",
                RuntimeEvent::DefaultSettingSet(DefaultSettingSet {
                    scope: Scope::Tenant,
                    scope_id: scope_id.to_owned(),
                    key: key_a.clone(),
                    value: serde_json::json!("glm-4.7"),
                }),
            ),
            sys_evt(
                "evt_ds_a2",
                RuntimeEvent::DefaultSettingSet(DefaultSettingSet {
                    scope: Scope::Tenant,
                    scope_id: scope_id.to_owned(),
                    key: key_a.clone(),
                    value: serde_json::json!("glm-5"),
                }),
            ),
            sys_evt(
                "evt_ds_b",
                RuntimeEvent::DefaultSettingSet(DefaultSettingSet {
                    scope: Scope::Tenant,
                    scope_id: scope_id.to_owned(),
                    key: key_b.clone(),
                    value: serde_json::json!(30000),
                }),
            ),
            sys_evt(
                "evt_ds_c_set",
                RuntimeEvent::DefaultSettingSet(DefaultSettingSet {
                    scope: Scope::Tenant,
                    scope_id: scope_id.to_owned(),
                    key: key_c.clone(),
                    value: serde_json::json!(true),
                }),
            ),
            sys_evt(
                "evt_ds_c_clear",
                RuntimeEvent::DefaultSettingCleared(DefaultSettingCleared {
                    scope: Scope::Tenant,
                    scope_id: scope_id.to_owned(),
                    key: key_c.clone(),
                }),
            ),
        ];
        log.append(&events).await.expect("append defaults");

        let pre_a = DefaultsReadModel::get(adapter.as_ref(), Scope::Tenant, scope_id, &key_a)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pre_a.value, serde_json::json!("glm-5"));
        let pre_c = DefaultsReadModel::get(adapter.as_ref(), Scope::Tenant, scope_id, &key_c)
            .await
            .unwrap();
        assert!(pre_c.is_none(), "Cleared must hard-delete the row");
    }

    let (adapter, log) = open_store(&path).await;

    // Upsert-on-Set winner survived.
    let after_a = DefaultsReadModel::get(adapter.as_ref(), Scope::Tenant, scope_id, &key_a)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after_a.value, serde_json::json!("glm-5"));
    assert_eq!(after_a.scope, Scope::Tenant);
    assert_eq!(after_a.key, key_a);

    // Integer JSON value round-tripped.
    let after_b = DefaultsReadModel::get(adapter.as_ref(), Scope::Tenant, scope_id, &key_b)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after_b.value, serde_json::json!(30000));

    // Cleared key stayed deleted.
    let after_c = DefaultsReadModel::get(adapter.as_ref(), Scope::Tenant, scope_id, &key_c)
        .await
        .unwrap();
    assert!(
        after_c.is_none(),
        "Cleared row must not resurrect on restart"
    );

    // list_by_scope ordering is by `key ASC` — so `default.model` sorts
    // before `default.timeout_ms`.
    let listed = DefaultsReadModel::list_by_scope(adapter.as_ref(), Scope::Tenant, scope_id)
        .await
        .unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].key, key_a);
    assert_eq!(listed[1].key, key_b);

    // Post-restart set resurrects the cleared key.
    log.append(&[sys_evt(
        "evt_ds_c_resurrect",
        RuntimeEvent::DefaultSettingSet(DefaultSettingSet {
            scope: Scope::Tenant,
            scope_id: scope_id.to_owned(),
            key: key_c.clone(),
            value: serde_json::json!(false),
        }),
    )])
    .await
    .unwrap();
    let resurrected = DefaultsReadModel::get(adapter.as_ref(), Scope::Tenant, scope_id, &key_c)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resurrected.value, serde_json::json!(false));
}

/// Scope isolation: defaults set at System vs Tenant for the same
/// (scope_id, key) must not bleed into each other's read view.
#[tokio::test]
async fn default_settings_scope_isolation_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let shared_id = "shared_id";
    let shared_key = "shared.key".to_owned();

    {
        let (_adapter, log) = open_store(&path).await;
        let events = vec![
            sys_evt(
                "evt_ds_sys",
                RuntimeEvent::DefaultSettingSet(DefaultSettingSet {
                    scope: Scope::System,
                    scope_id: shared_id.to_owned(),
                    key: shared_key.clone(),
                    value: serde_json::json!("system_value"),
                }),
            ),
            sys_evt(
                "evt_ds_ten",
                RuntimeEvent::DefaultSettingSet(DefaultSettingSet {
                    scope: Scope::Tenant,
                    scope_id: shared_id.to_owned(),
                    key: shared_key.clone(),
                    value: serde_json::json!("tenant_value"),
                }),
            ),
        ];
        log.append(&events).await.expect("append scoped defaults");
    }

    let (adapter, _log) = open_store(&path).await;
    let sys = DefaultsReadModel::get(adapter.as_ref(), Scope::System, shared_id, &shared_key)
        .await
        .unwrap()
        .unwrap();
    let ten = DefaultsReadModel::get(adapter.as_ref(), Scope::Tenant, shared_id, &shared_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sys.value, serde_json::json!("system_value"));
    assert_eq!(sys.scope, Scope::System);
    assert_eq!(ten.value, serde_json::json!("tenant_value"));
    assert_eq!(ten.scope, Scope::Tenant);
}

// ── RFC-025 Phase 2b.3 m3: channels + channel_messages ───────────────

/// Channel lifecycle across restart: Created + two messages (one
/// consumed, one pending) must round-trip every field — including the
/// consume-tracking columns — through drop-and-reopen.
#[tokio::test]
async fn channel_lifecycle_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let proj = project("ch_restart");
    let chan = ChannelId::new("chan_restart");

    {
        let (adapter, log) = open_store(&path).await;
        let events = vec![
            evt(
                "evt_chan_create",
                RuntimeEvent::ChannelCreated(ChannelCreated {
                    channel_id: chan.clone(),
                    project: proj.clone(),
                    name: "ops-room".to_owned(),
                    capacity: 100,
                    created_at_ms: 1_700_400_000_000,
                }),
            ),
            evt(
                "evt_chan_msg1",
                RuntimeEvent::ChannelMessageSent(ChannelMessageSent {
                    channel_id: chan.clone(),
                    project: proj.clone(),
                    message_id: "msg_1".to_owned(),
                    sender_id: "op_alice".to_owned(),
                    body: "deploy ready".to_owned(),
                    sent_at_ms: 1_700_400_010_000,
                }),
            ),
            evt(
                "evt_chan_msg1_consumed",
                RuntimeEvent::ChannelMessageConsumed(ChannelMessageConsumed {
                    channel_id: chan.clone(),
                    project: proj.clone(),
                    message_id: "msg_1".to_owned(),
                    consumed_by: "runner_x".to_owned(),
                    consumed_at_ms: 1_700_400_011_000,
                }),
            ),
            evt(
                "evt_chan_msg2",
                RuntimeEvent::ChannelMessageSent(ChannelMessageSent {
                    channel_id: chan.clone(),
                    project: proj.clone(),
                    message_id: "msg_2".to_owned(),
                    sender_id: "op_bob".to_owned(),
                    body: "ack".to_owned(),
                    sent_at_ms: 1_700_400_020_000,
                }),
            ),
        ];
        log.append(&events).await.expect("append channel lifecycle");

        let pre = ChannelReadModel::get_channel(adapter.as_ref(), &chan)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pre.name, "ops-room");
        assert_eq!(pre.capacity, 100);
    }

    let (adapter, _log) = open_store(&path).await;

    let after = ChannelReadModel::get_channel(adapter.as_ref(), &chan)
        .await
        .unwrap()
        .expect("channel must survive restart");
    assert_eq!(after.channel_id, chan);
    assert_eq!(after.project, proj);
    assert_eq!(after.name, "ops-room");
    assert_eq!(after.capacity, 100);
    assert_eq!(after.created_at, 1_700_400_000_000);

    let listed = ChannelReadModel::list_channels(adapter.as_ref(), &proj, 10, 0)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].channel_id, chan);

    let msgs = ChannelReadModel::list_messages(adapter.as_ref(), &chan, 10)
        .await
        .unwrap();
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].message_id, "msg_1");
    assert_eq!(
        msgs[0].consumed_by.as_deref(),
        Some("runner_x"),
        "Consumed update must persist across restart"
    );
    assert_eq!(msgs[0].consumed_at_ms, Some(1_700_400_011_000));
    assert_eq!(msgs[1].message_id, "msg_2");
    assert_eq!(
        msgs[1].consumed_by, None,
        "Pending message stays unconsumed across restart"
    );
    assert_eq!(msgs[1].consumed_at_ms, None);
}

/// Replay safety: a second ChannelCreated / ChannelMessageSent with the
/// same PK is first-write-wins on pg/sqlite (ON CONFLICT DO NOTHING).
/// An orphan Consumed (no prior Sent) is a no-op UPDATE.
#[tokio::test]
async fn channel_replay_is_idempotent_across_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let proj = project("ch_replay");
    let chan = ChannelId::new("chan_replay");

    {
        let (_adapter, log) = open_store(&path).await;
        let events = vec![
            evt(
                "evt_chrp_1",
                RuntimeEvent::ChannelCreated(ChannelCreated {
                    channel_id: chan.clone(),
                    project: proj.clone(),
                    name: "first".to_owned(),
                    capacity: 10,
                    created_at_ms: 1_700_500_000_000,
                }),
            ),
            // Replayed Created with same ID but different name +
            // capacity — ON CONFLICT DO NOTHING keeps first write.
            evt(
                "evt_chrp_2",
                RuntimeEvent::ChannelCreated(ChannelCreated {
                    channel_id: chan.clone(),
                    project: proj.clone(),
                    name: "second".to_owned(),
                    capacity: 999,
                    created_at_ms: 1_700_500_001_000,
                }),
            ),
            // Orphan Consumed (no Sent) — UPDATE must be no-op.
            evt(
                "evt_chrp_orphan",
                RuntimeEvent::ChannelMessageConsumed(ChannelMessageConsumed {
                    channel_id: chan.clone(),
                    project: proj.clone(),
                    message_id: "msg_ghost".to_owned(),
                    consumed_by: "never".to_owned(),
                    consumed_at_ms: 1_700_500_002_000,
                }),
            ),
        ];
        log.append(&events).await.expect("append replay chain");
    }

    let (adapter, _log) = open_store(&path).await;
    let after = ChannelReadModel::get_channel(adapter.as_ref(), &chan)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.name, "first", "first Created write wins on replay");
    assert_eq!(after.capacity, 10);

    let msgs = ChannelReadModel::list_messages(adapter.as_ref(), &chan, 10)
        .await
        .unwrap();
    assert!(msgs.is_empty(), "orphan Consumed must not synthesise a row");
}

// ── RFC-025 Phase 2b.3 m4: notifications ─────────────────────────────

/// Preferences are tenant-scoped tenant×operator key-value rows. Same
/// (tenant, operator) upserts; two distinct operators coexist; both
/// survive restart with their event_type + channel vectors intact.
#[tokio::test]
async fn notification_preferences_survive_restart_with_upsert_semantics() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let tenant = TenantId::new("t_notif_pref");

    {
        let (adapter, log) = open_store(&path).await;
        let events = vec![
            sys_evt(
                "evt_np_alice_1",
                RuntimeEvent::NotificationPreferenceSet(NotificationPreferenceSet {
                    tenant_id: tenant.clone(),
                    operator_id: "alice".to_owned(),
                    event_types: vec!["run.failed".to_owned(), "budget.crossed".to_owned()],
                    channels: vec![NotificationChannel {
                        kind: "email".to_owned(),
                        target: "alice@example.com".to_owned(),
                    }],
                    set_at_ms: 1_700_700_000_000,
                }),
            ),
            // Alice upserts — last write wins on the vector fields.
            sys_evt(
                "evt_np_alice_2",
                RuntimeEvent::NotificationPreferenceSet(NotificationPreferenceSet {
                    tenant_id: tenant.clone(),
                    operator_id: "alice".to_owned(),
                    event_types: vec!["run.failed".to_owned()],
                    channels: vec![
                        NotificationChannel {
                            kind: "email".to_owned(),
                            target: "alice@example.com".to_owned(),
                        },
                        NotificationChannel {
                            kind: "slack".to_owned(),
                            target: "#ops".to_owned(),
                        },
                    ],
                    set_at_ms: 1_700_700_010_000,
                }),
            ),
            sys_evt(
                "evt_np_bob",
                RuntimeEvent::NotificationPreferenceSet(NotificationPreferenceSet {
                    tenant_id: tenant.clone(),
                    operator_id: "bob".to_owned(),
                    event_types: vec!["approval.requested".to_owned()],
                    channels: vec![NotificationChannel {
                        kind: "webhook".to_owned(),
                        target: "https://ops.example/hook".to_owned(),
                    }],
                    set_at_ms: 1_700_700_020_000,
                }),
            ),
        ];
        log.append(&events).await.expect("append prefs");

        let pre_alice = NotificationReadModel::get_preferences(adapter.as_ref(), &tenant, "alice")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pre_alice.channels.len(), 2);
    }

    let (adapter, _log) = open_store(&path).await;

    let alice = NotificationReadModel::get_preferences(adapter.as_ref(), &tenant, "alice")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        alice.event_types,
        vec!["run.failed".to_owned()],
        "Upsert wins — second event_types replaced the first"
    );
    assert_eq!(alice.channels.len(), 2);
    assert_eq!(alice.channels[1].kind, "slack");

    let bob = NotificationReadModel::get_preferences(adapter.as_ref(), &tenant, "bob")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bob.channels[0].kind, "webhook");

    let listed = NotificationReadModel::list_preferences_by_tenant(adapter.as_ref(), &tenant)
        .await
        .unwrap();
    assert_eq!(listed.len(), 2);
    // Sorted by operator_id ASC.
    assert_eq!(listed[0].operator_id, "alice");
    assert_eq!(listed[1].operator_id, "bob");
}

/// NotificationSent delivery audit rows survive restart with
/// `delivered` + `delivery_error` intact, and the `failed` list
/// filter works on a cold adapter.
#[tokio::test]
async fn notification_sent_audit_survives_restart_including_failures() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let tenant = TenantId::new("t_notif_sent");

    {
        let (_adapter, log) = open_store(&path).await;
        let events = vec![
            sys_evt(
                "evt_ns_ok",
                RuntimeEvent::NotificationSent(NotificationSent {
                    record_id: "rec_ok".to_owned(),
                    tenant_id: tenant.clone(),
                    operator_id: "alice".to_owned(),
                    event_type: "run.finished".to_owned(),
                    channel_kind: "email".to_owned(),
                    channel_target: "alice@example.com".to_owned(),
                    payload: serde_json::json!({"run_id": "r1", "status": "ok"}),
                    sent_at_ms: 1_700_800_000_000,
                    delivered: true,
                    delivery_error: None,
                }),
            ),
            sys_evt(
                "evt_ns_fail",
                RuntimeEvent::NotificationSent(NotificationSent {
                    record_id: "rec_fail".to_owned(),
                    tenant_id: tenant.clone(),
                    operator_id: "bob".to_owned(),
                    event_type: "approval.requested".to_owned(),
                    channel_kind: "webhook".to_owned(),
                    channel_target: "https://example/hook".to_owned(),
                    payload: serde_json::json!({"appr_id": "a1"}),
                    sent_at_ms: 1_700_800_010_000,
                    delivered: false,
                    delivery_error: Some("webhook 502".to_owned()),
                }),
            ),
            // Replay of rec_ok — first-write-wins.
            sys_evt(
                "evt_ns_replay",
                RuntimeEvent::NotificationSent(NotificationSent {
                    record_id: "rec_ok".to_owned(),
                    tenant_id: tenant.clone(),
                    operator_id: "alice".to_owned(),
                    event_type: "IMPOSTOR".to_owned(),
                    channel_kind: "email".to_owned(),
                    channel_target: "alice@example.com".to_owned(),
                    payload: serde_json::json!({}),
                    sent_at_ms: 1_700_800_099_000,
                    delivered: false,
                    delivery_error: Some("replayed".to_owned()),
                }),
            ),
        ];
        log.append(&events).await.expect("append sent");
    }

    let (adapter, _log) = open_store(&path).await;

    let all = NotificationReadModel::list_sent_notifications(adapter.as_ref(), &tenant, 0)
        .await
        .unwrap();
    assert_eq!(all.len(), 2);
    // Sorted by (sent_at_ms ASC, record_id ASC).
    assert_eq!(all[0].record_id, "rec_ok");
    assert_eq!(
        all[0].event_type, "run.finished",
        "first-write-wins: replayed rec_ok did not clobber"
    );
    assert!(all[0].delivered);
    assert_eq!(all[1].record_id, "rec_fail");
    assert!(!all[1].delivered);
    assert_eq!(all[1].delivery_error.as_deref(), Some("webhook 502"));

    // since_ms filter
    let recent = NotificationReadModel::list_sent_notifications(
        adapter.as_ref(),
        &tenant,
        1_700_800_005_000,
    )
    .await
    .unwrap();
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].record_id, "rec_fail");

    // failed-only filter
    let failed = NotificationReadModel::list_failed_notifications(adapter.as_ref(), &tenant)
        .await
        .unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].record_id, "rec_fail");
}

// ── RFC-025 Phase 2b.3 m5: checkpoint_strategies ─────────────────────

/// Setting → updating (upsert) → reading across restart. Every field
/// round-trips: strategy_id, interval_ms, max_checkpoints (with the
/// "0 → default 10" rehydration rule), trigger_on_task_complete. The
/// sentinel `project` returned by the read model is the shared
/// `_strategy/_strategy/_strategy` triple; cross-run isolation is
/// enforced.
#[tokio::test]
async fn checkpoint_strategy_upsert_lifecycle_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let run = RunId::new("run_cs_restart");

    {
        let (adapter, log) = open_store(&path).await;
        let events = vec![
            sys_evt(
                "evt_cs_v1",
                RuntimeEvent::CheckpointStrategySet(CheckpointStrategySet {
                    strategy_id: "strat_v1".to_owned(),
                    description: "v1".to_owned(),
                    set_at_ms: 1_700_900_000_000,
                    run_id: Some(run.clone()),
                    interval_ms: 60_000,
                    max_checkpoints: 5,
                    trigger_on_task_complete: false,
                }),
            ),
            // Upsert — last write wins.
            sys_evt(
                "evt_cs_v2",
                RuntimeEvent::CheckpointStrategySet(CheckpointStrategySet {
                    strategy_id: "strat_v2".to_owned(),
                    description: "v2".to_owned(),
                    set_at_ms: 1_700_900_010_000,
                    run_id: Some(run.clone()),
                    interval_ms: 120_000,
                    max_checkpoints: 0, // triggers the default-10 rehydration
                    trigger_on_task_complete: true,
                }),
            ),
            // run_id = None — skipped; no row.
            sys_evt(
                "evt_cs_skip",
                RuntimeEvent::CheckpointStrategySet(CheckpointStrategySet {
                    strategy_id: "strat_orphan".to_owned(),
                    description: "no run".to_owned(),
                    set_at_ms: 1_700_900_020_000,
                    run_id: None,
                    interval_ms: 0,
                    max_checkpoints: 0,
                    trigger_on_task_complete: false,
                }),
            ),
        ];
        log.append(&events).await.expect("append strategies");

        let pre = CheckpointStrategyReadModel::get_by_run(adapter.as_ref(), &run)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pre.strategy_id, "strat_v2");
    }

    let (adapter, _log) = open_store(&path).await;

    let after = CheckpointStrategyReadModel::get_by_run(adapter.as_ref(), &run)
        .await
        .unwrap()
        .expect("strategy must survive restart");
    assert_eq!(after.strategy_id, "strat_v2");
    assert_eq!(after.interval_ms, 120_000);
    assert_eq!(
        after.max_checkpoints, 10,
        "0 in the event rehydrates to the default 10"
    );
    assert!(after.trigger_on_task_complete);
    assert_eq!(after.run_id, run);
    assert_eq!(
        after.project,
        cairn_store::projections::checkpoint_strategy_sentinel_project(),
        "project carries the shared sentinel on pg/sqlite, matching in-memory"
    );

    // Another run never got a strategy — read returns None.
    let missing = CheckpointStrategyReadModel::get_by_run(
        adapter.as_ref(),
        &RunId::new("run_never_configured"),
    )
    .await
    .unwrap();
    assert!(missing.is_none());
}
