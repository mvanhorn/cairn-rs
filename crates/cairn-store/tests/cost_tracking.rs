//! Per-provider cost tracking integration tests (RFC 009 + GAP-003).
//!
//! Validates the full cost pipeline through InMemoryStore:
//! - ProviderCallCompleted events are projected into run_costs and session_costs.
//! - Derived RunCostUpdated / SessionCostUpdated events land in the event log.
//! - Multiple calls to the same run/session accumulate correctly.
//! - Budget enforcement: SessionCostUpdated events accumulate spend against a
//!   ProviderBudget, and the budget read-model reflects the updated total.

use std::sync::Arc;

use cairn_domain::ids::{ProviderBindingId, ProviderCallId};
use cairn_domain::providers::{OperationKind, ProviderBudgetPeriod, ProviderCallStatus};
use cairn_domain::{
    EventEnvelope, EventId, EventSource, ProjectKey, ProviderCallCompleted, ProviderConnectionId,
    ProviderModelId, RouteAttemptId, RouteDecisionId, RunId, RuntimeEvent, SessionCostUpdated,
    SessionId, TenantId,
};
use cairn_store::{
    projections::{ProviderBudgetReadModel, RunCostReadModel, SessionCostReadModel},
    EventLog, InMemoryStore,
};

// ── helpers ──────────────────────────────────────────────────────────────────

fn project() -> ProjectKey {
    ProjectKey::new("tenant_cost", "ws_cost", "proj_cost")
}

fn tenant_id() -> TenantId {
    TenantId::new("tenant_cost")
}

fn run_id(n: u8) -> RunId {
    RunId::new(format!("run_{n}"))
}

fn session_id(n: u8) -> SessionId {
    SessionId::new(format!("session_{n}"))
}

/// Construct a ProviderCallCompleted event with the given cost and token counts.
fn call_completed(
    call_n: u8,
    run: Option<RunId>,
    sess: Option<SessionId>,
    cost_micros: u64,
    tokens_in: u32,
    tokens_out: u32,
) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(
        EventId::new(format!("evt_pcc_{call_n}")),
        EventSource::Runtime,
        RuntimeEvent::ProviderCallCompleted(ProviderCallCompleted {
            project: project(),
            provider_call_id: ProviderCallId::new(format!("call_{call_n}")),
            route_decision_id: RouteDecisionId::new(format!("rd_{call_n}")),
            route_attempt_id: RouteAttemptId::new(format!("ra_{call_n}")),
            provider_binding_id: ProviderBindingId::new("binding_1"),
            provider_connection_id: ProviderConnectionId::new("conn_openai"),
            provider_model_id: ProviderModelId::new("gpt-4o"),
            operation_kind: OperationKind::Generate,
            status: ProviderCallStatus::Succeeded,
            latency_ms: Some(250),
            input_tokens: Some(tokens_in),
            output_tokens: Some(tokens_out),
            cost_micros: Some(cost_micros),
            completed_at: (call_n as u64) * 1_000,
            session_id: sess,
            run_id: run,
            error_class: None,
            raw_error_message: None,
            retry_count: 0,
            task_id: None,
            prompt_release_id: None,
            fallback_position: 0,
            started_at: 0,
            finished_at: 0,
        }),
    )
}

/// Directly append a SessionCostUpdated event (for budget enforcement tests).
fn session_cost_event(
    call_n: u8,
    sess: SessionId,
    cost_micros: u64,
    tokens_in: u64,
    tokens_out: u64,
) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(
        EventId::new(format!("evt_scu_{call_n}")),
        EventSource::System,
        RuntimeEvent::SessionCostUpdated(SessionCostUpdated {
            project: project(),
            session_id: sess,
            tenant_id: tenant_id(),
            delta_cost_micros: cost_micros,
            delta_tokens_in: tokens_in,
            delta_tokens_out: tokens_out,
            provider_call_id: format!("call_{call_n}"),
            updated_at_ms: (call_n as u64) * 1_000,
        }),
    )
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// RFC 009 §4 — run-level cost accumulates correctly across multiple calls.
#[tokio::test]
async fn run_cost_accumulates_across_calls() {
    let store = Arc::new(InMemoryStore::new());

    // Two calls for the same run, different costs.
    store
        .append(&[
            call_completed(1, Some(run_id(1)), None, 1_500, 100, 50),
            call_completed(2, Some(run_id(1)), None, 2_000, 200, 80),
        ])
        .await
        .unwrap();

    let rec = RunCostReadModel::get_run_cost(store.as_ref(), &run_id(1))
        .await
        .unwrap()
        .expect("run cost record should exist after ProviderCallCompleted");

    assert_eq!(rec.total_cost_micros, 3_500, "costs should accumulate");
    assert_eq!(rec.total_tokens_in, 300, "input tokens should accumulate");
    assert_eq!(rec.total_tokens_out, 130, "output tokens should accumulate");
    assert_eq!(rec.provider_calls, 2, "call count should be 2");
}

/// RFC 009 §4 — separate runs keep independent cost records.
#[tokio::test]
async fn run_costs_are_independent_per_run() {
    let store = Arc::new(InMemoryStore::new());

    store
        .append(&[
            call_completed(1, Some(run_id(1)), None, 1_000, 100, 40),
            call_completed(2, Some(run_id(2)), None, 5_000, 400, 200),
        ])
        .await
        .unwrap();

    let r1 = RunCostReadModel::get_run_cost(store.as_ref(), &run_id(1))
        .await
        .unwrap()
        .unwrap();
    let r2 = RunCostReadModel::get_run_cost(store.as_ref(), &run_id(2))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(r1.total_cost_micros, 1_000);
    assert_eq!(r2.total_cost_micros, 5_000);
}

/// RFC 009 §4 — session-level cost accumulates across calls sharing a session.
#[tokio::test]
async fn session_cost_accumulates_across_calls() {
    let store = Arc::new(InMemoryStore::new());

    store
        .append(&[
            call_completed(1, None, Some(session_id(1)), 3_000, 150, 60),
            call_completed(2, None, Some(session_id(1)), 4_500, 300, 120),
            call_completed(3, None, Some(session_id(1)), 500, 50, 10),
        ])
        .await
        .unwrap();

    let rec = SessionCostReadModel::get_session_cost(store.as_ref(), &session_id(1))
        .await
        .unwrap()
        .expect("session cost record should exist");

    assert_eq!(rec.total_cost_micros, 8_000);
    assert_eq!(rec.total_tokens_in, 500);
    assert_eq!(rec.total_tokens_out, 190);
    assert_eq!(rec.provider_calls, 3);
}

/// Derived RunCostUpdated event is emitted into the event log by SyncProjection
/// when a ProviderCallCompleted event with a run_id is appended.
#[tokio::test]
async fn derived_run_cost_updated_event_in_log() {
    let store = Arc::new(InMemoryStore::new());

    store
        .append(&[call_completed(1, Some(run_id(1)), None, 2_000, 100, 50)])
        .await
        .unwrap();

    // Read all events from the log and find a RunCostUpdated for our run.
    let events = EventLog::read_stream(store.as_ref(), None, 50)
        .await
        .unwrap();

    let has_run_cost_updated = events.iter().any(|e| {
        matches!(
            &e.envelope.payload,
            RuntimeEvent::RunCostUpdated(r) if r.run_id == run_id(1)
                && r.delta_cost_micros == 2_000
                && r.delta_tokens_in == 100
                && r.delta_tokens_out == 50
        )
    });

    assert!(
        has_run_cost_updated,
        "derived RunCostUpdated event must be present in the event log after ProviderCallCompleted"
    );
}

/// Derived SessionCostUpdated event is emitted into the log when ProviderCallCompleted
/// includes a session_id.
#[tokio::test]
async fn derived_session_cost_updated_event_in_log() {
    let store = Arc::new(InMemoryStore::new());

    store
        .append(&[call_completed(1, None, Some(session_id(1)), 1_800, 90, 30)])
        .await
        .unwrap();

    let events = EventLog::read_stream(store.as_ref(), None, 50)
        .await
        .unwrap();

    let has_session_cost_updated = events.iter().any(|e| {
        matches!(
            &e.envelope.payload,
            RuntimeEvent::SessionCostUpdated(s)
                if s.session_id == session_id(1)
                    && s.delta_cost_micros == 1_800
                    && s.delta_tokens_in == 90
                    && s.delta_tokens_out == 30
        )
    });

    assert!(
        has_session_cost_updated,
        "derived SessionCostUpdated event must be present in the event log"
    );
}

/// Budget enforcement: SessionCostUpdated events accumulate spend against the
/// tenant budget; the read-model reflects the running total.
#[tokio::test]
async fn budget_spend_accumulates_from_session_cost_events() {
    use cairn_domain::events::ProviderBudgetSet;

    let store = Arc::new(InMemoryStore::new());

    // Establish a budget: 10_000 µUSD daily limit.
    store
        .append(&[EventEnvelope::for_runtime_event(
            EventId::new("evt_budget_set"),
            EventSource::System,
            RuntimeEvent::ProviderBudgetSet(ProviderBudgetSet {
                tenant_id: tenant_id(),
                budget_id: "budget_daily".to_owned(),
                period: ProviderBudgetPeriod::Daily,
                limit_micros: 10_000,
                alert_threshold_percent: Some(80),
            }),
        )])
        .await
        .unwrap();

    // Verify budget starts at zero spend.
    let budget = ProviderBudgetReadModel::get_by_tenant_period(
        store.as_ref(),
        &tenant_id(),
        ProviderBudgetPeriod::Daily,
    )
    .await
    .unwrap()
    .expect("budget record should exist after ProviderBudgetSet");

    assert_eq!(budget.current_spend_micros, 0, "spend starts at zero");
    assert_eq!(budget.limit_micros, 10_000);

    // Append SessionCostUpdated events; each increments the budget spend.
    store
        .append(&[
            session_cost_event(1, session_id(1), 3_000, 150, 60),
            session_cost_event(2, session_id(1), 4_000, 200, 80),
        ])
        .await
        .unwrap();

    let budget = ProviderBudgetReadModel::get_by_tenant_period(
        store.as_ref(),
        &tenant_id(),
        ProviderBudgetPeriod::Daily,
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(
        budget.current_spend_micros, 7_000,
        "spend should accumulate to 7_000 µUSD after two events"
    );

    // Still under limit — not yet exceeded.
    assert!(
        budget.current_spend_micros < budget.limit_micros,
        "budget should not yet be exceeded"
    );

    // Push over the limit.
    store
        .append(&[session_cost_event(3, session_id(1), 4_000, 200, 80)])
        .await
        .unwrap();

    let budget = ProviderBudgetReadModel::get_by_tenant_period(
        store.as_ref(),
        &tenant_id(),
        ProviderBudgetPeriod::Daily,
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(budget.current_spend_micros, 11_000);
    assert!(
        budget.current_spend_micros > budget.limit_micros,
        "budget should be exceeded after 11_000 µUSD spend against 10_000 µUSD limit"
    );
}

/// Zero-cost calls (e.g. cached responses) must still register a call count
/// without inflating costs.
#[tokio::test]
async fn zero_cost_call_increments_count_without_inflating_totals() {
    let store = Arc::new(InMemoryStore::new());

    store
        .append(&[
            call_completed(1, Some(run_id(1)), None, 500, 100, 50),
            // Zero-cost call (cache hit or free model).
            call_completed(2, Some(run_id(1)), None, 0, 100, 50),
        ])
        .await
        .unwrap();

    let rec = RunCostReadModel::get_run_cost(store.as_ref(), &run_id(1))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        rec.total_cost_micros, 500,
        "cost should not include zero-cost call"
    );
    assert_eq!(rec.provider_calls, 2, "call count includes zero-cost call");
}

// ── #727: RunCostReadModel::list_by_session is session-scoped ───────────────

/// Pre-fix, `list_by_session` returned ALL run_costs rows regardless
/// of session (and `_session_id` was even a `_`-prefixed unused arg
/// on PG/SQLite). Codex's PR #727 closes this on all three backends.
///
/// Threat model: the `GET /v1/sessions/:id/cost` handler first
/// confirms tenant ownership of the session, then calls
/// `list_by_session(session_id)` and trusts the result. Pre-fix,
/// that result included other tenants' run costs (any session_id
/// would return everything). Post-fix, only run-cost rows whose
/// `run_id` resolves through the `runs` projection to the requested
/// session_id are returned.
///
/// Pre/post differ on this test:
///   pre-fix:  list_by_session(session_1) returns BOTH run_a and
///             run_b's costs → assertion `len() == 1` fails.
///   post-fix: returns only run_a's cost → passes.
#[tokio::test]
async fn run_cost_list_by_session_filters_to_requested_session() {
    use cairn_domain::{RunCreated, SessionCreated};

    let store = Arc::new(InMemoryStore::new());
    let session_a = SessionId::new("sess_a");
    let session_b = SessionId::new("sess_b");
    let run_a = RunId::new("run_in_session_a");
    let run_b = RunId::new("run_in_session_b");

    // Seed sessions + runs so the in-memory `state.runs` lookup
    // finds both rows. Each run is tied to a different session.
    store
        .append(&[
            EventEnvelope::for_runtime_event(
                EventId::new("evt_sess_a"),
                EventSource::Runtime,
                RuntimeEvent::SessionCreated(SessionCreated {
                    project: project(),
                    session_id: session_a.clone(),
                }),
            ),
            EventEnvelope::for_runtime_event(
                EventId::new("evt_sess_b"),
                EventSource::Runtime,
                RuntimeEvent::SessionCreated(SessionCreated {
                    project: project(),
                    session_id: session_b.clone(),
                }),
            ),
            EventEnvelope::for_runtime_event(
                EventId::new("evt_run_a"),
                EventSource::Runtime,
                RuntimeEvent::RunCreated(RunCreated {
                    project: project(),
                    session_id: session_a.clone(),
                    run_id: run_a.clone(),
                    parent_run_id: None,
                    prompt_release_id: None,
                    agent_role_id: None,
                }),
            ),
            EventEnvelope::for_runtime_event(
                EventId::new("evt_run_b"),
                EventSource::Runtime,
                RuntimeEvent::RunCreated(RunCreated {
                    project: project(),
                    session_id: session_b.clone(),
                    run_id: run_b.clone(),
                    parent_run_id: None,
                    prompt_release_id: None,
                    agent_role_id: None,
                }),
            ),
        ])
        .await
        .unwrap();

    // Cost calls on both runs.
    store
        .append(&[
            EventEnvelope::for_runtime_event(
                EventId::new("evt_pcc_a"),
                EventSource::Runtime,
                RuntimeEvent::ProviderCallCompleted(ProviderCallCompleted {
                    project: project(),
                    provider_call_id: ProviderCallId::new("call_a"),
                    route_decision_id: RouteDecisionId::new("rd_a"),
                    route_attempt_id: RouteAttemptId::new("ra_a"),
                    provider_binding_id: ProviderBindingId::new("binding_1"),
                    provider_connection_id: ProviderConnectionId::new("conn"),
                    provider_model_id: ProviderModelId::new("gpt-4o"),
                    operation_kind: OperationKind::Generate,
                    status: ProviderCallStatus::Succeeded,
                    latency_ms: Some(100),
                    input_tokens: Some(50),
                    output_tokens: Some(20),
                    cost_micros: Some(1_000),
                    completed_at: 1_000,
                    session_id: Some(session_a.clone()),
                    run_id: Some(run_a.clone()),
                    error_class: None,
                    raw_error_message: None,
                    retry_count: 0,
                    task_id: None,
                    prompt_release_id: None,
                    fallback_position: 0,
                    started_at: 0,
                    finished_at: 0,
                }),
            ),
            EventEnvelope::for_runtime_event(
                EventId::new("evt_pcc_b"),
                EventSource::Runtime,
                RuntimeEvent::ProviderCallCompleted(ProviderCallCompleted {
                    project: project(),
                    provider_call_id: ProviderCallId::new("call_b"),
                    route_decision_id: RouteDecisionId::new("rd_b"),
                    route_attempt_id: RouteAttemptId::new("ra_b"),
                    provider_binding_id: ProviderBindingId::new("binding_1"),
                    provider_connection_id: ProviderConnectionId::new("conn"),
                    provider_model_id: ProviderModelId::new("gpt-4o"),
                    operation_kind: OperationKind::Generate,
                    status: ProviderCallStatus::Succeeded,
                    latency_ms: Some(100),
                    input_tokens: Some(50),
                    output_tokens: Some(20),
                    cost_micros: Some(2_000),
                    completed_at: 2_000,
                    session_id: Some(session_b.clone()),
                    run_id: Some(run_b.clone()),
                    error_class: None,
                    raw_error_message: None,
                    retry_count: 0,
                    task_id: None,
                    prompt_release_id: None,
                    fallback_position: 0,
                    started_at: 0,
                    finished_at: 0,
                }),
            ),
        ])
        .await
        .unwrap();

    // list_by_session(session_a) returns ONLY run_a's cost.
    let a_costs = RunCostReadModel::list_by_session(store.as_ref(), &session_a)
        .await
        .expect("list_by_session(session_a)");
    assert_eq!(
        a_costs.len(),
        1,
        "session_a must see exactly its own run cost; got {a_costs:?}"
    );
    assert_eq!(a_costs[0].run_id, run_a);
    assert_eq!(a_costs[0].total_cost_micros, 1_000);

    // list_by_session(session_b) returns ONLY run_b's cost.
    let b_costs = RunCostReadModel::list_by_session(store.as_ref(), &session_b)
        .await
        .expect("list_by_session(session_b)");
    assert_eq!(
        b_costs.len(),
        1,
        "session_b must see exactly its own run cost; got {b_costs:?}"
    );
    assert_eq!(b_costs[0].run_id, run_b);
    assert_eq!(b_costs[0].total_cost_micros, 2_000);

    // Sanity: an unknown session returns empty (not all rows).
    let unknown = RunCostReadModel::list_by_session(store.as_ref(), &SessionId::new("nope"))
        .await
        .expect("list_by_session(unknown)");
    assert!(
        unknown.is_empty(),
        "unknown session must return empty; got {unknown:?}",
    );
}
