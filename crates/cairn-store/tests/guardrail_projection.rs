//! RFC-025 Phase 2a.2 milestone 2: guardrail_policies + guardrail_evaluations
//! projection integration tests.
//!
//! The existing `crates/cairn-store/tests/projection_parity.rs` covers
//! byte-parity between in-memory and sqlite. This file focuses on
//! semantic contracts (upsert, tenant isolation, replay idempotency,
//! restart durability).

use cairn_domain::policy::{
    GuardrailDecisionKind, GuardrailRule, GuardrailRuleEffect, GuardrailSubjectType,
};
use cairn_domain::{
    EventEnvelope, EventId, EventSource, GuardrailPolicyCreated, GuardrailPolicyEvaluated,
    RuntimeEvent, TenantId,
};
use cairn_store::{
    projections::{GuardrailEvaluationReadModel, GuardrailReadModel},
    sqlite::SqliteAdapter,
    EventLog, InMemoryStore,
};

fn evt(id: &str, payload: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(EventId::new(id), EventSource::Runtime, payload)
}

fn policy_rule() -> GuardrailRule {
    GuardrailRule {
        subject_type: GuardrailSubjectType::Tool,
        subject_id: Some("fs.write".into()),
        action: "invoke".into(),
        effect: GuardrailRuleEffect::Deny,
        conditions: vec![],
    }
}

// ── 1. Policy upsert semantics ──────────────────────────────────────────────

#[tokio::test]
async fn policy_created_twice_refreshes_rule_set_in_memory() {
    let store = InMemoryStore::new();
    let tenant_id = TenantId::new("t_m2_up");
    store
        .append(&[
            evt(
                "e1",
                RuntimeEvent::GuardrailPolicyCreated(GuardrailPolicyCreated {
                    tenant_id: tenant_id.clone(),
                    policy_id: "pol_1".into(),
                    name: "v1".into(),
                    rules: vec![policy_rule()],
                }),
            ),
            evt(
                "e2",
                RuntimeEvent::GuardrailPolicyCreated(GuardrailPolicyCreated {
                    tenant_id: tenant_id.clone(),
                    policy_id: "pol_1".into(),
                    name: "v2".into(),
                    rules: vec![],
                }),
            ),
        ])
        .await
        .unwrap();

    let pol = GuardrailReadModel::get_policy(&store, "pol_1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pol.name, "v2", "latest create wins");
    assert!(pol.rules.is_empty(), "ruleset replaced");
}

// ── 2. Tenant-scoped list ───────────────────────────────────────────────────

#[tokio::test]
async fn list_policies_is_scoped_to_the_requested_tenant_in_memory() {
    let store = InMemoryStore::new();
    store
        .append(&[
            evt(
                "e1",
                RuntimeEvent::GuardrailPolicyCreated(GuardrailPolicyCreated {
                    tenant_id: TenantId::new("t_a"),
                    policy_id: "pol_a".into(),
                    name: "a".into(),
                    rules: vec![],
                }),
            ),
            evt(
                "e2",
                RuntimeEvent::GuardrailPolicyCreated(GuardrailPolicyCreated {
                    tenant_id: TenantId::new("t_b"),
                    policy_id: "pol_b".into(),
                    name: "b".into(),
                    rules: vec![],
                }),
            ),
        ])
        .await
        .unwrap();

    let a_list = GuardrailReadModel::list_policies(&store, &TenantId::new("t_a"), 100, 0)
        .await
        .unwrap();
    let b_list = GuardrailReadModel::list_policies(&store, &TenantId::new("t_b"), 100, 0)
        .await
        .unwrap();
    assert_eq!(a_list.len(), 1);
    assert_eq!(b_list.len(), 1);
    assert_eq!(a_list[0].policy_id, "pol_a");
    assert_eq!(b_list[0].policy_id, "pol_b");
}

// ── 3. Evaluation audit trail: replay dedup + most-recent-first order ───────

#[tokio::test]
async fn evaluation_audit_dedupes_on_composite_key_in_memory() {
    let store = InMemoryStore::new();
    let tenant_id = TenantId::new("t_m2_eval");
    // Same composite key → collapsed to one row.
    let payload = GuardrailPolicyEvaluated {
        tenant_id: tenant_id.clone(),
        policy_id: "pol_e".into(),
        subject_type: GuardrailSubjectType::Run,
        subject_id: Some("run_99".into()),
        action: "start".into(),
        decision: GuardrailDecisionKind::Allowed,
        reason: None,
        evaluated_at_ms: 5_000,
    };
    store
        .append(&[
            evt(
                "e1",
                RuntimeEvent::GuardrailPolicyEvaluated(payload.clone()),
            ),
            evt(
                "e2",
                RuntimeEvent::GuardrailPolicyEvaluated(payload.clone()),
            ),
        ])
        .await
        .unwrap();

    let rows = GuardrailEvaluationReadModel::list_evaluations(&store, &tenant_id, 100)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "composite-key replay must dedupe");
}

#[tokio::test]
async fn evaluation_list_is_most_recent_first_in_memory() {
    let store = InMemoryStore::new();
    let tenant_id = TenantId::new("t_m2_order");
    for (i, ts) in [1_000u64, 5_000, 3_000, 9_000].iter().enumerate() {
        store
            .append(&[evt(
                &format!("e{i}"),
                RuntimeEvent::GuardrailPolicyEvaluated(GuardrailPolicyEvaluated {
                    tenant_id: tenant_id.clone(),
                    policy_id: format!("pol_{i}"),
                    subject_type: GuardrailSubjectType::Tool,
                    subject_id: None,
                    action: "invoke".into(),
                    decision: GuardrailDecisionKind::Allowed,
                    reason: None,
                    evaluated_at_ms: *ts,
                }),
            )])
            .await
            .unwrap();
    }
    let rows = GuardrailEvaluationReadModel::list_evaluations(&store, &tenant_id, 100)
        .await
        .unwrap();
    let ts: Vec<_> = rows.iter().map(|r| r.evaluated_at_ms).collect();
    assert_eq!(ts, vec![9_000, 5_000, 3_000, 1_000]);
}

// ── 4. Cross-tenant isolation on evaluations ────────────────────────────────

#[tokio::test]
async fn list_evaluations_is_scoped_to_tenant_in_memory() {
    let store = InMemoryStore::new();
    let shared = GuardrailPolicyEvaluated {
        tenant_id: TenantId::new("t_x"),
        policy_id: "pol_x".into(),
        subject_type: GuardrailSubjectType::Task,
        subject_id: Some("task_1".into()),
        action: "run".into(),
        decision: GuardrailDecisionKind::Allowed,
        reason: None,
        evaluated_at_ms: 1_000,
    };
    let mut other = shared.clone();
    other.tenant_id = TenantId::new("t_y");
    other.policy_id = "pol_y".into();
    store
        .append(&[
            evt("e1", RuntimeEvent::GuardrailPolicyEvaluated(shared)),
            evt("e2", RuntimeEvent::GuardrailPolicyEvaluated(other)),
        ])
        .await
        .unwrap();

    let x_rows = GuardrailEvaluationReadModel::list_evaluations(&store, &TenantId::new("t_x"), 100)
        .await
        .unwrap();
    let y_rows = GuardrailEvaluationReadModel::list_evaluations(&store, &TenantId::new("t_y"), 100)
        .await
        .unwrap();
    assert_eq!(x_rows.len(), 1);
    assert_eq!(y_rows.len(), 1);
    assert_eq!(x_rows[0].policy_id, "pol_x");
    assert_eq!(y_rows[0].policy_id, "pol_y");
}

// ── 5. Restart durability on sqlite ─────────────────────────────────────────

#[tokio::test]
async fn sqlite_guardrails_survive_adapter_restart() {
    use cairn_store::db::DbAdapter;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use std::str::FromStr;

    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let url = format!("sqlite:{}", tmp.path().display());

    let tenant_id = TenantId::new("t_m2_durable");
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
            .expect("pool");
        let adapter = SqliteAdapter::new(pool.clone());
        adapter.migrate().await.expect("migrate");
        let log = cairn_store::sqlite::SqliteEventLog::new(pool);
        log.append(&[
            evt(
                "e1",
                RuntimeEvent::GuardrailPolicyCreated(GuardrailPolicyCreated {
                    tenant_id: tenant_id.clone(),
                    policy_id: "pol_durable".into(),
                    name: "durable".into(),
                    rules: vec![policy_rule()],
                }),
            ),
            evt(
                "e2",
                RuntimeEvent::GuardrailPolicyEvaluated(GuardrailPolicyEvaluated {
                    tenant_id: tenant_id.clone(),
                    policy_id: "pol_durable".into(),
                    subject_type: GuardrailSubjectType::Tool,
                    subject_id: Some("fs.write".into()),
                    action: "invoke".into(),
                    decision: GuardrailDecisionKind::Denied,
                    reason: Some("matched".into()),
                    evaluated_at_ms: 4_242,
                }),
            ),
        ])
        .await
        .unwrap();
    }
    // Boot 2 — fresh pool, same file.
    let opts2 = SqliteConnectOptions::from_str(&url)
        .expect("sqlite url")
        .create_if_missing(false)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
    let pool2 = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opts2)
        .await
        .expect("pool 2");
    let adapter2 = SqliteAdapter::new(pool2);
    adapter2.migrate().await.expect("migrate 2");

    let pol = GuardrailReadModel::get_policy(&adapter2, "pol_durable")
        .await
        .unwrap()
        .expect("policy survives restart");
    assert_eq!(pol.name, "durable");
    assert_eq!(pol.rules.len(), 1);
    let evals = GuardrailEvaluationReadModel::list_evaluations(&adapter2, &tenant_id, 100)
        .await
        .unwrap();
    assert_eq!(evals.len(), 1);
    assert_eq!(evals[0].evaluated_at_ms, 4_242);
    assert_eq!(evals[0].decision, GuardrailDecisionKind::Denied);
}

// ── 6. ON CONFLICT DO UPDATE resets `enabled` on replay (Gemini review #571) ─
//
// The in-memory applier always produces `enabled: true` when
// `GuardrailPolicyCreated` is applied. The sqlite ON CONFLICT DO UPDATE
// must reset `enabled` to the INSERT-bound value so a replay of the same
// event rehydrates to byte-equal state across backends even if the row
// was manually toggled to false between writes (an operator/ops scenario
// or a future `GuardrailPolicyDisabled` event). Regression guard for
// the review fix on PR #571.

#[tokio::test]
async fn policy_created_replay_resets_enabled_on_sqlite() {
    use cairn_store::db::DbAdapter;

    let adapter = SqliteAdapter::in_memory().await.unwrap();
    adapter.migrate().await.unwrap();
    let log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

    let tenant_id = TenantId::new("t_m2_reset");
    log.append(&[evt(
        "e1",
        RuntimeEvent::GuardrailPolicyCreated(GuardrailPolicyCreated {
            tenant_id: tenant_id.clone(),
            policy_id: "pol_reset".into(),
            name: "v1".into(),
            rules: vec![policy_rule()],
        }),
    )])
    .await
    .unwrap();

    // Simulate a manual disable outside the event-sourced path
    // (operator action, partial migration, or a future
    // `GuardrailPolicyDisabled` event that could target this row).
    sqlx::query("UPDATE guardrail_policies SET enabled = 0 WHERE policy_id = ?")
        .bind("pol_reset")
        .execute(adapter.pool())
        .await
        .unwrap();
    let (enabled_after_disable,): (bool,) =
        sqlx::query_as("SELECT enabled FROM guardrail_policies WHERE policy_id = ?")
            .bind("pol_reset")
            .fetch_one(adapter.pool())
            .await
            .unwrap();
    assert!(
        !enabled_after_disable,
        "disable must stick before the replay"
    );

    // Replay the same `GuardrailPolicyCreated`. Without the
    // `enabled = excluded.enabled` clause on ON CONFLICT DO UPDATE, the
    // refreshed row would stay `enabled = 0` and drift from the in-memory
    // applier's `enabled: true`.
    log.append(&[evt(
        "e2",
        RuntimeEvent::GuardrailPolicyCreated(GuardrailPolicyCreated {
            tenant_id: tenant_id.clone(),
            policy_id: "pol_reset".into(),
            name: "v2".into(),
            rules: vec![policy_rule()],
        }),
    )])
    .await
    .unwrap();

    let (enabled_after_replay,): (bool,) =
        sqlx::query_as("SELECT enabled FROM guardrail_policies WHERE policy_id = ?")
            .bind("pol_reset")
            .fetch_one(adapter.pool())
            .await
            .unwrap();
    assert!(
        enabled_after_replay,
        "replay of GuardrailPolicyCreated must reset enabled to true to match in-memory applier"
    );
}

// ── 7. Tenant-scoped PK on guardrail_evaluations (Copilot review #571) ──────
//
// Two tenants evaluating the same `policy_id` (notably a shared
// runtime-emitted policy like `"implicit_allow"`) for the same subject
// in the same millisecond must both persist — the prior PK
// `(policy_id, subject_type, subject_id, action, evaluated_at_ms)` would
// collapse them under `ON CONFLICT DO NOTHING` and drop audit rows.
// The PK now leads with `tenant_id` across pg + sqlite + in-memory.

#[tokio::test]
async fn two_tenants_same_evaluation_key_both_persist_in_memory() {
    let store = InMemoryStore::new();
    let at_ms = 9_999;
    store
        .append(&[
            evt(
                "e1",
                RuntimeEvent::GuardrailPolicyEvaluated(GuardrailPolicyEvaluated {
                    tenant_id: TenantId::new("tenant_alpha"),
                    policy_id: "implicit_allow".into(),
                    subject_type: GuardrailSubjectType::Tool,
                    subject_id: Some("fs.write".into()),
                    action: "invoke".into(),
                    decision: GuardrailDecisionKind::Allowed,
                    reason: None,
                    evaluated_at_ms: at_ms,
                }),
            ),
            evt(
                "e2",
                RuntimeEvent::GuardrailPolicyEvaluated(GuardrailPolicyEvaluated {
                    tenant_id: TenantId::new("tenant_beta"),
                    policy_id: "implicit_allow".into(),
                    subject_type: GuardrailSubjectType::Tool,
                    subject_id: Some("fs.write".into()),
                    action: "invoke".into(),
                    decision: GuardrailDecisionKind::Allowed,
                    reason: None,
                    evaluated_at_ms: at_ms,
                }),
            ),
        ])
        .await
        .unwrap();

    let alpha_rows =
        GuardrailEvaluationReadModel::list_evaluations(&store, &TenantId::new("tenant_alpha"), 100)
            .await
            .unwrap();
    let beta_rows =
        GuardrailEvaluationReadModel::list_evaluations(&store, &TenantId::new("tenant_beta"), 100)
            .await
            .unwrap();
    assert_eq!(alpha_rows.len(), 1, "tenant_alpha row must persist");
    assert_eq!(beta_rows.len(), 1, "tenant_beta row must persist");
}

#[tokio::test]
async fn two_tenants_same_evaluation_key_both_persist_in_sqlite() {
    let adapter = SqliteAdapter::in_memory().await.unwrap();
    let log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());
    let at_ms = 9_999;
    log.append(&[
        evt(
            "e1",
            RuntimeEvent::GuardrailPolicyEvaluated(GuardrailPolicyEvaluated {
                tenant_id: TenantId::new("tenant_alpha"),
                policy_id: "implicit_allow".into(),
                subject_type: GuardrailSubjectType::Tool,
                subject_id: Some("fs.write".into()),
                action: "invoke".into(),
                decision: GuardrailDecisionKind::Allowed,
                reason: None,
                evaluated_at_ms: at_ms,
            }),
        ),
        evt(
            "e2",
            RuntimeEvent::GuardrailPolicyEvaluated(GuardrailPolicyEvaluated {
                tenant_id: TenantId::new("tenant_beta"),
                policy_id: "implicit_allow".into(),
                subject_type: GuardrailSubjectType::Tool,
                subject_id: Some("fs.write".into()),
                action: "invoke".into(),
                decision: GuardrailDecisionKind::Allowed,
                reason: None,
                evaluated_at_ms: at_ms,
            }),
        ),
    ])
    .await
    .unwrap();

    let alpha_rows = GuardrailEvaluationReadModel::list_evaluations(
        &adapter,
        &TenantId::new("tenant_alpha"),
        100,
    )
    .await
    .unwrap();
    let beta_rows = GuardrailEvaluationReadModel::list_evaluations(
        &adapter,
        &TenantId::new("tenant_beta"),
        100,
    )
    .await
    .unwrap();
    assert_eq!(
        alpha_rows.len(),
        1,
        "sqlite PK must not collapse across tenants"
    );
    assert_eq!(
        beta_rows.len(),
        1,
        "sqlite PK must not collapse across tenants"
    );
}
