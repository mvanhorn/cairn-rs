//! RFC-025 Phase 2b.4 restart-durability proofs.
//!
//! Covers every projection this phase flips Stubbed → Projected or
//! Stubbed → Ephemeral across the four milestones:
//!
//! * Milestone 2 — **eval catalog**: `EvalDatasetCreated`,
//!   `EvalDatasetEntryAdded`, `EvalRubricCreated`, `EvalBaselineSet`,
//!   `EvalBaselineLocked`.
//! * Milestone 3 — **operator profiles**: `OperatorProfileCreated`,
//!   `OperatorProfileUpdated`.
//! * Milestone 4 — **run costs + route policy**: `RunCostUpdated`,
//!   `RunCostAlertSet`, `RunCostAlertTriggered`, `RoutePolicyUpdated`.
//!
//! Pre-Phase-2b.4 the pg/sqlite applier was `log_stub(..)` for each
//! of these variants — the event log kept the event, but the read
//! model on a fresh boot returned empty. Every restart wiped the
//! relevant operator surface on persistent backends.
//!
//! Shape mirrors `phase_2b3_restart_durability.rs`: boot a file-backed
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
    providers::RoutePolicyRule, tenancy::WorkspaceRole, EvalBaselineLocked, EvalBaselineSet,
    EvalDatasetCreated, EvalDatasetEntryAdded, EvalRubricCreated, EventEnvelope, EventId,
    EventSource, OperatorId, OperatorProfileCreated, OperatorProfileUpdated, OwnershipKey,
    ProjectId, ProjectKey, RoutePolicyCreated, RoutePolicyUpdated, RunCostAlertSet,
    RunCostAlertTriggered, RunCostUpdated, RunId, RuntimeEvent, TenantId, WorkspaceId,
};
use cairn_store::db::DbAdapter;
use cairn_store::projections::{
    EvalBaselineReadModel, EvalDatasetReadModel, EvalRubricReadModel, OperatorProfileReadModel,
    RoutePolicyReadModel, RunCostAlertReadModel, RunCostReadModel,
};
use cairn_store::{sqlite::SqliteAdapter, sqlite::SqliteEventLog, EventLog};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

// ── helpers (shared pattern with phase_2b1/2b2/2b3) ──────────────────────

/// Build a system-scoped envelope. Used for events that genuinely do
/// not carry a `ProjectKey` / `TenantKey` on the payload — eval
/// catalog, permission decisions, operator profiles (tenant-scoped
/// but not project-scoped), run cost alerts, and route policy
/// updates. Production envelopes for tenant-only events land with
/// `OwnershipKey::System` via the service layer today; matching that
/// here keeps the test envelope shape aligned with the production
/// write path.
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

/// Build a project-scoped envelope via the same derivation path the
/// production write path uses (`EventEnvelope::for_runtime_event`
/// reads `project()` off the payload). Applies to `RunCostUpdated`
/// which carries `project: ProjectKey` on its struct body; using
/// this helper instead of `sys_evt` keeps the envelope's ownership
/// in lockstep with the payload, as production would.
fn proj_evt(id: &str, payload: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(EventId::new(id), EventSource::Runtime, payload)
}

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

// ── eval datasets ────────────────────────────────────────────────────────

/// `EvalDatasetCreated` + `EvalDatasetEntryAdded` persist across a
/// simulated restart: the dataset row and its ordered entries are both
/// visible on the cold boot.
#[tokio::test]
async fn eval_dataset_with_entries_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let dataset_id = "ds_restart_1".to_owned();

    // ── Session 1: create dataset + add two entries, then drop pool. ──
    {
        let (adapter, log) = open_store(&path).await;
        let events = vec![
            sys_evt(
                "evt_ds_create",
                RuntimeEvent::EvalDatasetCreated(EvalDatasetCreated {
                    dataset_id: dataset_id.clone(),
                    name: "golden_set".to_owned(),
                    created_at_ms: 1_700_100_000_000,
                }),
            ),
            sys_evt(
                "evt_ds_entry_a",
                RuntimeEvent::EvalDatasetEntryAdded(EvalDatasetEntryAdded {
                    dataset_id: dataset_id.clone(),
                    entry_id: "entry_a".to_owned(),
                    added_at_ms: 1_700_100_001_000,
                }),
            ),
            sys_evt(
                "evt_ds_entry_b",
                RuntimeEvent::EvalDatasetEntryAdded(EvalDatasetEntryAdded {
                    dataset_id: dataset_id.clone(),
                    entry_id: "entry_b".to_owned(),
                    added_at_ms: 1_700_100_002_000,
                }),
            ),
        ];
        log.append(&events).await.expect("append dataset lifecycle");

        // Pre-restart sanity.
        let pre = EvalDatasetReadModel::get_dataset(adapter.as_ref(), &dataset_id)
            .await
            .unwrap()
            .expect("dataset must be visible before restart");
        assert_eq!(pre.name, "golden_set");
        assert_eq!(pre.entries.len(), 2, "both entries visible pre-restart");
    }

    // ── Session 2: cold boot on the same DB file. ──
    let (adapter, _log) = open_store(&path).await;

    let after = EvalDatasetReadModel::get_dataset(adapter.as_ref(), &dataset_id)
        .await
        .unwrap()
        .expect("dataset must survive restart");
    assert_eq!(after.dataset_id, dataset_id);
    assert_eq!(after.name, "golden_set");
    assert_eq!(after.created_at_ms, 1_700_100_000_000);
    assert_eq!(
        after.entries.len(),
        2,
        "both entries survive restart (failure mode pre-2b.4 was empty entries)"
    );
    let tags: Vec<&str> = after
        .entries
        .iter()
        .flat_map(|e| e.tags.iter().map(String::as_str))
        .collect();
    assert_eq!(tags, vec!["entry_a", "entry_b"]);

    // list_by_tenant with the sentinel empty tenant returns the same row.
    let listed = EvalDatasetReadModel::list_by_tenant(adapter.as_ref(), &TenantId::new(""), 10, 0)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].dataset_id, dataset_id);
}

/// Replayed `EvalDatasetEntryAdded` with a duplicate entry_id is a
/// no-op (idempotent): the entry list length stays at 1 after a replay.
#[tokio::test]
async fn eval_dataset_entry_dedupe_on_replay() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let dataset_id = "ds_dedupe".to_owned();
    let (adapter, log) = open_store(&path).await;

    // Create + add entry twice with the same entry_id.
    let base_events = vec![
        sys_evt(
            "evt_ds_dedupe_create",
            RuntimeEvent::EvalDatasetCreated(EvalDatasetCreated {
                dataset_id: dataset_id.clone(),
                name: "dedupe".to_owned(),
                created_at_ms: 1_700_200_000_000,
            }),
        ),
        sys_evt(
            "evt_ds_dedupe_entry_1",
            RuntimeEvent::EvalDatasetEntryAdded(EvalDatasetEntryAdded {
                dataset_id: dataset_id.clone(),
                entry_id: "dup_entry".to_owned(),
                added_at_ms: 1_700_200_001_000,
            }),
        ),
    ];
    log.append(&base_events).await.expect("append base");
    // Replay the same entry_id via a new event_id (event log PK keeps
    // the envelope unique; the dedup guard is on the projection PK).
    log.append(&[sys_evt(
        "evt_ds_dedupe_entry_2",
        RuntimeEvent::EvalDatasetEntryAdded(EvalDatasetEntryAdded {
            dataset_id: dataset_id.clone(),
            entry_id: "dup_entry".to_owned(),
            added_at_ms: 1_700_200_002_000,
        }),
    )])
    .await
    .expect("append replay");

    let ds = EvalDatasetReadModel::get_dataset(adapter.as_ref(), &dataset_id)
        .await
        .unwrap()
        .expect("dataset exists");
    assert_eq!(
        ds.entries.len(),
        1,
        "duplicate entry_id must be deduped by the projection PK"
    );
}

/// Regression for Copilot PR #596 review on `sqlite/adapter.rs:5589`:
/// `EvalDatasetReadModel::list_by_tenant` must not exceed SQLite's
/// legacy 999 host-parameter cap when bulk-loading dataset entries via
/// `IN (?, ?, …)`. With `limit = 1000` and one parameter per returned
/// dataset_id, a single IN-clause would try to bind 1000 parameters and
/// fail on legacy builds. The chunked loop at `SQLITE_IN_CHUNK = 900`
/// splits the work across two queries and merges the results.
#[tokio::test]
async fn eval_dataset_list_by_tenant_chunks_above_sqlite_var_limit() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let (adapter, log) = open_store(&path).await;

    // 1000 datasets (> 900, the chunk threshold), each with one entry.
    // 2000 events total — well inside the batched INSERT path.
    let n = 1000usize;
    let mut events: Vec<EventEnvelope<RuntimeEvent>> = Vec::with_capacity(n * 2);
    for i in 0..n {
        let dataset_id = format!("ds_chunk_{i:04}");
        events.push(sys_evt(
            &format!("evt_chunk_ds_{i:04}"),
            RuntimeEvent::EvalDatasetCreated(EvalDatasetCreated {
                dataset_id: dataset_id.clone(),
                name: format!("chunk_{i:04}"),
                // Distinct created_at_ms so the `(created_at_ms ASC,
                // dataset_id ASC)` ordering is unambiguous.
                created_at_ms: 1_700_300_000_000 + i as u64,
            }),
        ));
        events.push(sys_evt(
            &format!("evt_chunk_entry_{i:04}"),
            RuntimeEvent::EvalDatasetEntryAdded(EvalDatasetEntryAdded {
                dataset_id,
                entry_id: format!("e_{i:04}"),
                added_at_ms: 1_700_300_000_000 + i as u64,
            }),
        ));
    }
    log.append(&events)
        .await
        .expect("append 1000-dataset fixture");

    // Sentinel tenant ("") returns every row. limit = 1000 forces the
    // IN-clause to exceed the legacy 999 cap unless chunked.
    let listed = EvalDatasetReadModel::list_by_tenant(adapter.as_ref(), &TenantId::new(""), n, 0)
        .await
        .expect("list_by_tenant must chunk IN-clause under 999-var cap");
    assert_eq!(
        listed.len(),
        n,
        "all 1000 datasets returned (failure mode pre-chunk was sqlite var-cap error)"
    );
    // Spot-check a row from each chunk: index 0 (first chunk) and
    // index 950 (second chunk, past the 900 boundary).
    assert_eq!(listed[0].dataset_id, "ds_chunk_0000");
    assert_eq!(listed[0].entries.len(), 1);
    assert_eq!(listed[0].entries[0].tags, vec!["e_0000".to_string()]);
    assert_eq!(listed[950].dataset_id, "ds_chunk_0950");
    assert_eq!(listed[950].entries.len(), 1);
    assert_eq!(listed[950].entries[0].tags, vec!["e_0950".to_string()]);
}

// ── eval rubrics ─────────────────────────────────────────────────────────

/// `EvalRubricCreated` persists across restart.
#[tokio::test]
async fn eval_rubric_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let rubric_id = "rub_restart".to_owned();
    {
        let (_adapter, log) = open_store(&path).await;
        log.append(&[sys_evt(
            "evt_rub_create",
            RuntimeEvent::EvalRubricCreated(EvalRubricCreated {
                rubric_id: rubric_id.clone(),
                name: "safety_v1".to_owned(),
                created_at_ms: 1_700_300_000_000,
            }),
        )])
        .await
        .expect("append rubric");
    }

    let (adapter, _log) = open_store(&path).await;
    let after = EvalRubricReadModel::get_rubric(adapter.as_ref(), &rubric_id)
        .await
        .unwrap()
        .expect("rubric must survive restart");
    assert_eq!(after.rubric_id, rubric_id);
    assert_eq!(after.name, "safety_v1");
    assert_eq!(after.created_at_ms, 1_700_300_000_000);
    assert!(after.dimensions.is_empty());
}

// ── eval baselines ───────────────────────────────────────────────────────

/// `EvalBaselineSet` persists across restart with the synthesized
/// `{baseline_id}[{metric}={value}]` display name.
#[tokio::test]
async fn eval_baseline_set_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let baseline_id = "bl_restart".to_owned();
    {
        let (_adapter, log) = open_store(&path).await;
        log.append(&[sys_evt(
            "evt_bl_set",
            RuntimeEvent::EvalBaselineSet(EvalBaselineSet {
                baseline_id: baseline_id.clone(),
                metric: "task_success_rate".to_owned(),
                value: "0.95".to_owned(),
                set_at_ms: 1_700_400_000_000,
            }),
        )])
        .await
        .expect("append baseline");
    }

    let (adapter, _log) = open_store(&path).await;
    let after = EvalBaselineReadModel::get_baseline(adapter.as_ref(), &baseline_id)
        .await
        .unwrap()
        .expect("baseline must survive restart");
    assert_eq!(after.baseline_id, baseline_id);
    assert_eq!(
        after.name,
        format!("{baseline_id}[task_success_rate=0.95]"),
        "display name must be synthesized from metric + value so the \
         row is parity-identical with the in-memory applier"
    );
    assert!(!after.locked, "newly-set baseline starts unlocked");
    assert_eq!(after.created_at_ms, 1_700_400_000_000);
}

/// `EvalBaselineLocked` flips `locked=true` and later `EvalBaselineSet`
/// events on the same baseline are ignored (WHERE locked = 0 gate).
#[tokio::test]
async fn eval_baseline_locked_is_absorbing() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let baseline_id = "bl_lock".to_owned();
    let (adapter, log) = open_store(&path).await;

    log.append(&[
        sys_evt(
            "evt_bl_lock_set",
            RuntimeEvent::EvalBaselineSet(EvalBaselineSet {
                baseline_id: baseline_id.clone(),
                metric: "latency_p50_ms".to_owned(),
                value: "400".to_owned(),
                set_at_ms: 1_700_500_000_000,
            }),
        ),
        sys_evt(
            "evt_bl_lock_lock",
            RuntimeEvent::EvalBaselineLocked(EvalBaselineLocked {
                baseline_id: baseline_id.clone(),
                locked_at_ms: 1_700_500_001_000,
            }),
        ),
        // Later Set event must NOT overwrite the locked baseline.
        sys_evt(
            "evt_bl_lock_ignored",
            RuntimeEvent::EvalBaselineSet(EvalBaselineSet {
                baseline_id: baseline_id.clone(),
                metric: "policy_pass_rate".to_owned(),
                value: "0.99".to_owned(),
                set_at_ms: 1_700_500_002_000,
            }),
        ),
    ])
    .await
    .expect("append lock lifecycle");

    let after = EvalBaselineReadModel::get_baseline(adapter.as_ref(), &baseline_id)
        .await
        .unwrap()
        .expect("baseline exists");
    assert!(after.locked, "lock flag must be on");
    assert!(
        after.name.contains("latency_p50_ms=400"),
        "post-lock Set event must be dropped: expected name to still reflect the \
         pre-lock metric, got: {}",
        after.name
    );
}

// ── operator profiles (milestone 3) ──────────────────────────────────────

/// `OperatorProfileCreated` persists across restart with every
/// populated field (display_name, email, role, tenant_id, created_at).
#[tokio::test]
async fn operator_profile_created_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let tenant = TenantId::new("t_op_create");
    let profile_id = OperatorId::new("op_create");

    {
        let (_adapter, log) = open_store(&path).await;
        log.append(&[sys_evt(
            "evt_op_create",
            RuntimeEvent::OperatorProfileCreated(OperatorProfileCreated {
                tenant_id: tenant.clone(),
                profile_id: profile_id.clone(),
                display_name: "Ada Lovelace".to_owned(),
                email: "ada@example.com".to_owned(),
                role: WorkspaceRole::Admin,
            }),
        )])
        .await
        .expect("append create");
    }

    let (adapter, _log) = open_store(&path).await;
    let after = OperatorProfileReadModel::get(adapter.as_ref(), &profile_id)
        .await
        .unwrap()
        .expect("profile must survive restart");
    assert_eq!(after.operator_id, profile_id);
    assert_eq!(after.tenant_id, tenant);
    assert_eq!(after.display_name, "Ada Lovelace");
    assert_eq!(after.email.as_deref(), Some("ada@example.com"));
    assert_eq!(after.role, "admin");
}

/// `OperatorProfileUpdated` patches only `Some(_)` fields — a partial
/// update that leaves `email` at None does NOT clobber the row's
/// existing email, matching the in-memory `if let Some(email)` guard.
#[tokio::test]
async fn operator_profile_updated_is_patch_shape() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let tenant = TenantId::new("t_op_patch");
    let profile_id = OperatorId::new("op_patch");
    let (adapter, log) = open_store(&path).await;

    log.append(&[
        sys_evt(
            "evt_op_patch_create",
            RuntimeEvent::OperatorProfileCreated(OperatorProfileCreated {
                tenant_id: tenant.clone(),
                profile_id: profile_id.clone(),
                display_name: "Grace Hopper".to_owned(),
                email: "grace@example.com".to_owned(),
                role: WorkspaceRole::Member,
            }),
        ),
        // Patch only display_name; email stays unchanged.
        sys_evt(
            "evt_op_patch_update",
            RuntimeEvent::OperatorProfileUpdated(OperatorProfileUpdated {
                tenant_id: tenant.clone(),
                profile_id: profile_id.clone(),
                display_name: Some("RADM Grace Hopper".to_owned()),
                email: None,
            }),
        ),
    ])
    .await
    .expect("append patch lifecycle");

    let after = OperatorProfileReadModel::get(adapter.as_ref(), &profile_id)
        .await
        .unwrap()
        .expect("profile exists");
    assert_eq!(after.display_name, "RADM Grace Hopper");
    assert_eq!(
        after.email.as_deref(),
        Some("grace@example.com"),
        "email must be preserved when the patch carries None"
    );
    // role is also immutable via Updated — the in-memory applier skips
    // it and so does the projection.
    assert_eq!(after.role, "member");
}

/// `OperatorProfileUpdated` against a missing operator is a silent
/// no-op (the UPDATE affects 0 rows; no phantom row is created).
#[tokio::test]
async fn operator_profile_update_on_missing_is_noop() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let tenant = TenantId::new("t_op_missing");
    let profile_id = OperatorId::new("op_never_created");
    let (adapter, log) = open_store(&path).await;

    log.append(&[sys_evt(
        "evt_op_missing_update",
        RuntimeEvent::OperatorProfileUpdated(OperatorProfileUpdated {
            tenant_id: tenant,
            profile_id: profile_id.clone(),
            display_name: Some("Phantom".to_owned()),
            email: Some("phantom@example.com".to_owned()),
        }),
    )])
    .await
    .expect("append update on missing");

    let after = OperatorProfileReadModel::get(adapter.as_ref(), &profile_id)
        .await
        .unwrap();
    assert!(
        after.is_none(),
        "update on missing must not materialize a phantom row"
    );
}

/// `list_by_tenant` returns only rows in the given tenant, ordered by
/// `operator_id ASC` for deterministic cross-backend parity.
#[tokio::test]
async fn operator_profile_list_by_tenant_scoped_and_ordered() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let tenant_a = TenantId::new("t_list_a");
    let tenant_b = TenantId::new("t_list_b");
    let (adapter, log) = open_store(&path).await;

    log.append(&[
        sys_evt(
            "evt_list_a2",
            RuntimeEvent::OperatorProfileCreated(OperatorProfileCreated {
                tenant_id: tenant_a.clone(),
                profile_id: OperatorId::new("op_a2"),
                display_name: "Second A".to_owned(),
                email: "a2@example.com".to_owned(),
                role: WorkspaceRole::Viewer,
            }),
        ),
        sys_evt(
            "evt_list_b1",
            RuntimeEvent::OperatorProfileCreated(OperatorProfileCreated {
                tenant_id: tenant_b.clone(),
                profile_id: OperatorId::new("op_b1"),
                display_name: "Tenant B".to_owned(),
                email: "b1@example.com".to_owned(),
                role: WorkspaceRole::Admin,
            }),
        ),
        sys_evt(
            "evt_list_a1",
            RuntimeEvent::OperatorProfileCreated(OperatorProfileCreated {
                tenant_id: tenant_a.clone(),
                profile_id: OperatorId::new("op_a1"),
                display_name: "First A".to_owned(),
                email: "a1@example.com".to_owned(),
                role: WorkspaceRole::Admin,
            }),
        ),
    ])
    .await
    .expect("append list fixture");

    let rows = OperatorProfileReadModel::list_by_tenant(adapter.as_ref(), &tenant_a, 10, 0)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "tenant filter excludes op_b1");
    // `operator_id ASC` orders a1 before a2.
    assert_eq!(rows[0].operator_id.as_str(), "op_a1");
    assert_eq!(rows[1].operator_id.as_str(), "op_a2");
}

/// Locking a baseline that does not exist is a silent no-op (matches
/// the in-memory `if let Some(baseline)` guard).
#[tokio::test]
async fn eval_baseline_lock_on_missing_is_noop() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let (adapter, log) = open_store(&path).await;
    log.append(&[sys_evt(
        "evt_bl_missing_lock",
        RuntimeEvent::EvalBaselineLocked(EvalBaselineLocked {
            baseline_id: "bl_never_set".to_owned(),
            locked_at_ms: 1_700_600_000_000,
        }),
    )])
    .await
    .expect("append lock on missing");

    let after = EvalBaselineReadModel::get_baseline(adapter.as_ref(), "bl_never_set")
        .await
        .unwrap();
    assert!(after.is_none(), "no phantom row from lock-on-missing");
}

// ── run costs + cost alerts (milestone 4) ─────────────────────────────────

/// Multiple `RunCostUpdated` events accumulate in-place and the total
/// survives restart.
#[tokio::test]
async fn run_cost_accumulates_and_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let run_id = RunId::new("run_cost_accum");
    let proj = ProjectKey {
        tenant_id: TenantId::new("t_rc"),
        workspace_id: WorkspaceId::new("w_rc"),
        project_id: ProjectId::new("p_rc"),
    };

    {
        let (_adapter, log) = open_store(&path).await;
        log.append(&[
            proj_evt(
                "evt_rc_1",
                RuntimeEvent::RunCostUpdated(RunCostUpdated {
                    project: proj.clone(),
                    run_id: run_id.clone(),
                    delta_cost_micros: 12_500,
                    delta_tokens_in: 100,
                    delta_tokens_out: 40,
                    provider_call_id: "pc_1".to_owned(),
                    updated_at_ms: 1_700_700_000_000,
                    session_id: None,
                    tenant_id: Some(TenantId::new("t_rc")),
                }),
            ),
            proj_evt(
                "evt_rc_2",
                RuntimeEvent::RunCostUpdated(RunCostUpdated {
                    project: proj.clone(),
                    run_id: run_id.clone(),
                    delta_cost_micros: 7_500,
                    delta_tokens_in: 50,
                    delta_tokens_out: 20,
                    provider_call_id: "pc_2".to_owned(),
                    updated_at_ms: 1_700_700_001_000,
                    session_id: None,
                    tenant_id: Some(TenantId::new("t_rc")),
                }),
            ),
        ])
        .await
        .expect("append accumulation");
    }

    let (adapter, _log) = open_store(&path).await;
    let after = RunCostReadModel::get_run_cost(adapter.as_ref(), &run_id)
        .await
        .unwrap()
        .expect("run cost must survive restart");
    assert_eq!(
        after.total_cost_micros, 20_000,
        "deltas accumulate across restart"
    );
    assert_eq!(after.total_tokens_in, 150);
    assert_eq!(after.total_tokens_out, 60);
    assert_eq!(after.provider_calls, 2);
    assert_eq!(after.token_in, 150);
    assert_eq!(after.token_out, 60);
}

/// `RunCostAlertSet` persists + a subsequent `RunCostAlertTriggered`
/// updates the existing row in place across restart.
#[tokio::test]
async fn run_cost_alert_set_then_triggered_survives_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let run_id = RunId::new("run_alert");
    let tenant = TenantId::new("t_alert");

    {
        let (_adapter, log) = open_store(&path).await;
        log.append(&[
            sys_evt(
                "evt_rca_set",
                RuntimeEvent::RunCostAlertSet(RunCostAlertSet {
                    run_id: run_id.clone(),
                    tenant_id: tenant.clone(),
                    threshold_micros: 50_000,
                    set_at_ms: 1_700_800_000_000,
                }),
            ),
            sys_evt(
                "evt_rca_trig",
                RuntimeEvent::RunCostAlertTriggered(RunCostAlertTriggered {
                    run_id: run_id.clone(),
                    tenant_id: tenant.clone(),
                    threshold_micros: 50_000,
                    actual_cost_micros: 52_500,
                    triggered_at_ms: 1_700_800_001_000,
                }),
            ),
        ])
        .await
        .expect("append alert lifecycle");
    }

    let (adapter, _log) = open_store(&path).await;
    let after = RunCostAlertReadModel::get_alert(adapter.as_ref(), &run_id)
        .await
        .unwrap()
        .expect("alert must survive restart");
    assert_eq!(after.threshold_micros, 50_000);
    assert_eq!(after.triggered_at_ms, 1_700_800_001_000);
    assert_eq!(after.actual_cost_micros, 52_500);

    // list_triggered_by_tenant surfaces only triggered alerts; this one
    // qualifies.
    let listed = RunCostAlertReadModel::list_triggered_by_tenant(adapter.as_ref(), &tenant, 10, 0)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].run_id, run_id);
}

/// `RunCostAlertSet` re-set on an already-triggered alert rearms it
/// (`triggered_at_ms` + `actual_cost_micros` reset to 0). Matches the
/// in-memory `insert` that clobbers the previous record.
#[tokio::test]
async fn run_cost_alert_reset_clears_trigger() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let run_id = RunId::new("run_rearm");
    let tenant = TenantId::new("t_rearm");
    let (adapter, log) = open_store(&path).await;

    log.append(&[
        sys_evt(
            "evt_rearm_set",
            RuntimeEvent::RunCostAlertSet(RunCostAlertSet {
                run_id: run_id.clone(),
                tenant_id: tenant.clone(),
                threshold_micros: 10_000,
                set_at_ms: 1_700_900_000_000,
            }),
        ),
        sys_evt(
            "evt_rearm_trig",
            RuntimeEvent::RunCostAlertTriggered(RunCostAlertTriggered {
                run_id: run_id.clone(),
                tenant_id: tenant.clone(),
                threshold_micros: 10_000,
                actual_cost_micros: 11_000,
                triggered_at_ms: 1_700_900_001_000,
            }),
        ),
        // Re-set with a new threshold — rearms the alert (clears the
        // triggered-at + actual fields).
        sys_evt(
            "evt_rearm_reset",
            RuntimeEvent::RunCostAlertSet(RunCostAlertSet {
                run_id: run_id.clone(),
                tenant_id: tenant.clone(),
                threshold_micros: 25_000,
                set_at_ms: 1_700_900_002_000,
            }),
        ),
    ])
    .await
    .expect("append rearm lifecycle");

    let after = RunCostAlertReadModel::get_alert(adapter.as_ref(), &run_id)
        .await
        .unwrap()
        .expect("alert exists");
    assert_eq!(after.threshold_micros, 25_000, "new threshold wins");
    assert_eq!(after.triggered_at_ms, 0, "rearm clears triggered_at");
    assert_eq!(after.actual_cost_micros, 0, "rearm clears actual cost");
}

/// `RunCostAlertTriggered` against a missing alert is silently ignored
/// (UPDATE with WHERE run_id on a non-existent row affects 0 rows).
#[tokio::test]
async fn run_cost_alert_trigger_on_missing_is_noop() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();
    let (adapter, log) = open_store(&path).await;

    let run_id = RunId::new("run_missing");
    log.append(&[sys_evt(
        "evt_missing_trig",
        RuntimeEvent::RunCostAlertTriggered(RunCostAlertTriggered {
            run_id: run_id.clone(),
            tenant_id: TenantId::new("t_any"),
            threshold_micros: 100,
            actual_cost_micros: 101,
            triggered_at_ms: 1_700_950_000_000,
        }),
    )])
    .await
    .expect("append missing trigger");

    let after = RunCostAlertReadModel::get_alert(adapter.as_ref(), &run_id)
        .await
        .unwrap();
    assert!(
        after.is_none(),
        "no phantom alert row from trigger-on-missing"
    );
}

// ── route policy updates (milestone 4) ─────────────────────────────────

/// `RoutePolicyUpdated` bumps `updated_at_ms` on the existing row.
/// `Created` seeds the row first; the Updated event carries only the
/// policy_id + new updated_at_ms.
#[tokio::test]
async fn route_policy_updated_bumps_timestamp() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    let policy_id = "rp_bump".to_owned();
    let tenant = TenantId::new("t_rp");
    let (adapter, log) = open_store(&path).await;

    log.append(&[
        sys_evt(
            "evt_rp_create",
            RuntimeEvent::RoutePolicyCreated(RoutePolicyCreated {
                tenant_id: tenant.clone(),
                policy_id: policy_id.clone(),
                name: "default".to_owned(),
                rules: vec![RoutePolicyRule {
                    rule_id: "rule_a".to_owned(),
                    policy_id: policy_id.clone(),
                    priority: 1,
                    description: Some("primary".to_owned()),
                }],
                enabled: true,
            }),
        ),
        sys_evt(
            "evt_rp_update",
            RuntimeEvent::RoutePolicyUpdated(RoutePolicyUpdated {
                policy_id: policy_id.clone(),
                updated_at_ms: 1_701_000_000_000,
            }),
        ),
    ])
    .await
    .expect("append route policy lifecycle");

    let after = RoutePolicyReadModel::get(adapter.as_ref(), &policy_id)
        .await
        .unwrap()
        .expect("policy exists");
    assert_eq!(after.policy_id, policy_id);
    assert_eq!(after.tenant_id, tenant.as_str());
    assert_eq!(
        after.updated_at_ms, 1_701_000_000_000,
        "Updated event must bump updated_at_ms on the existing row"
    );
    assert_eq!(after.rules.len(), 1, "rules body preserved across Updated");
}

/// `RoutePolicyUpdated` against a missing policy_id is silently ignored
/// (UPDATE affects 0 rows; no phantom row is created). Matches the
/// in-memory `if let Some(p)` guard.
#[tokio::test]
async fn route_policy_update_on_missing_is_noop() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();
    let (adapter, log) = open_store(&path).await;

    log.append(&[sys_evt(
        "evt_rp_missing",
        RuntimeEvent::RoutePolicyUpdated(RoutePolicyUpdated {
            policy_id: "rp_never_created".to_owned(),
            updated_at_ms: 1_701_100_000_000,
        }),
    )])
    .await
    .expect("append update on missing");

    let after = RoutePolicyReadModel::get(adapter.as_ref(), "rp_never_created")
        .await
        .unwrap();
    assert!(after.is_none(), "no phantom route_policies row");
}
