//! Integration tests for #661 subagent-spawn observability.
//!
//! These tests drive the metrics-tap end-to-end: append events to a
//! real `InMemoryStore` with a live `MetricsTap` attached, then
//! assert against the rendered Prometheus output. The tap task
//! consumes the same event shapes a production orchestrator emits,
//! so these tests protect the wire path (event → counter/gauge/
//! histogram → scrape) that the issue relies on for the dogfood
//! diagnostic loop.
//!
//! The `on_decide_completed`-driven spawn counter is covered
//! separately by a unit test on `TracingEmitter` — this suite
//! focuses on the tap surface, where `SubagentSpawned` +
//! `RunStateChanged` flow through.

#![cfg(feature = "metrics-core")]

mod support;

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::Router;
use cairn_api::auth::AuthPrincipal;
use cairn_api::bootstrap::BootstrapConfig;
use cairn_app::metrics::AppMetrics;
use cairn_app::metrics_tap::MetricsTap;
use cairn_app::AppState;
use cairn_domain::events::CheckpointPersisted;
use cairn_domain::tenancy::TenantKey;
use cairn_domain::{
    CheckpointId, EventEnvelope, EventId, EventSource, ProjectId, ProjectKey, RunCreated, RunId,
    RunState, RunStateChanged, RuntimeEvent, SessionCreated, SessionId, StateTransition,
    SubagentSpawned, TaskId, TenantId, WorkspaceId,
};
use cairn_store::event_log::EventLog;
use cairn_store::InMemoryStore;
use serde_json::Value;
use support::metrics_wait::wait_for_metrics;

async fn setup() -> (Arc<InMemoryStore>, Arc<AppMetrics>, MetricsTap) {
    let store = Arc::new(InMemoryStore::new());
    let metrics = Arc::new(AppMetrics::default());
    let tap = MetricsTap::spawn(store.clone(), metrics.clone());
    (store, metrics, tap)
}

fn project() -> ProjectKey {
    ProjectKey {
        tenant_id: TenantId::new("t"),
        workspace_id: WorkspaceId::new("w"),
        project_id: ProjectId::new("p"),
    }
}

fn envelope(event: RuntimeEvent, id_suffix: &str) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(
        EventId::new(format!("evt_subagent_test_{id_suffix}")),
        EventSource::Runtime,
        event,
    )
}

/// A parent run that spawns a subagent, then the parent terminates.
/// The tap must observe the spawn (→ `inline=false` for the window)
/// and emit a histogram sample at the parent's iteration high-water.
#[tokio::test]
async fn subagent_spawn_lowers_inline_run_ratio_and_observes_iterations() {
    let (store, metrics, tap) = setup().await;
    let project = project();
    let parent_run = RunId::new("r_parent_a");
    let session = SessionId::new("s_parent");

    // Iteration 4 checkpoint — sets the high-water.
    store
        .append(&[envelope(
            RuntimeEvent::CheckpointPersisted(CheckpointPersisted {
                project: project.clone(),
                checkpoint_id: CheckpointId::new("ckpt_parent_1"),
                session_id: session.clone(),
                root_run_id: parent_run.clone(),
                iteration: 4,
                at_ms: 0,
            }),
            "ckpt1",
        )])
        .await
        .unwrap();

    // Orchestrator spawns an executor subagent.
    store
        .append(&[envelope(
            RuntimeEvent::SubagentSpawned(SubagentSpawned {
                project: project.clone(),
                parent_run_id: parent_run.clone(),
                parent_task_id: None,
                child_task_id: TaskId::new("tk_exec_child"),
                child_session_id: SessionId::new("s_child"),
                child_run_id: None,
                goal: "observability-test-goal".to_owned(),
                role: "executor".to_owned(),
            }),
            "spawn1",
        )])
        .await
        .unwrap();

    // Parent reaches a terminal state. Use Completed so the tap
    // observes the iteration histogram + the inline-window outcome.
    store
        .append(&[envelope(
            RuntimeEvent::RunStateChanged(RunStateChanged {
                project: project.clone(),
                run_id: parent_run.clone(),
                transition: StateTransition {
                    from: Some(RunState::Running),
                    to: RunState::Completed,
                },
                failure_class: None,
                pause_reason: None,
                resume_trigger: None,
            }),
            "term1",
        )])
        .await
        .unwrap();

    // Delegated: inline=false → ratio 0/1 = 0.0. Iterations
    // histogram observed value 4 → bucket `le=5` catches it.
    wait_for_metrics(
        &metrics,
        &[
            "cairn_orchestrator_iterations_per_run_count 1",
            "cairn_orchestrator_iterations_per_run_sum 4",
            "cairn_orchestrator_inline_run_ratio 0.000000",
            "cairn_orchestrator_inline_run_ratio_samples 1",
        ],
    )
    .await;

    tap.shutdown().await;
}

/// A parent run that never delegates. Inline-ratio should land at
/// 1.0 (1/1 inline), iterations observed at the high-water mark.
#[tokio::test]
async fn inline_only_run_records_inline_outcome_at_terminal() {
    let (store, metrics, tap) = setup().await;
    let project = project();
    let run = RunId::new("r_inline_b");
    let session = SessionId::new("s_inline");

    // Three checkpoints — tap keeps the max.
    for (i, iteration) in [1, 8, 3].iter().enumerate() {
        store
            .append(&[envelope(
                RuntimeEvent::CheckpointPersisted(CheckpointPersisted {
                    project: project.clone(),
                    checkpoint_id: CheckpointId::new(format!("ckpt_b_{i}")),
                    session_id: session.clone(),
                    root_run_id: run.clone(),
                    iteration: *iteration,
                    at_ms: 0,
                }),
                &format!("ckpt_b_{i}"),
            )])
            .await
            .unwrap();
    }

    store
        .append(&[envelope(
            RuntimeEvent::RunStateChanged(RunStateChanged {
                project,
                run_id: run.clone(),
                transition: StateTransition {
                    from: Some(RunState::Running),
                    to: RunState::Completed,
                },
                failure_class: None,
                pause_reason: None,
                resume_trigger: None,
            }),
            "term_b",
        )])
        .await
        .unwrap();

    // iteration=8 and inline=true. Ratio 1/1 = 1.0.
    wait_for_metrics(
        &metrics,
        &[
            "cairn_orchestrator_iterations_per_run_sum 8",
            "cairn_orchestrator_inline_run_ratio 1.000000",
        ],
    )
    .await;

    tap.shutdown().await;
}

/// The observation fires on Failed and Canceled terminals too, not
/// only Completed. This is load-bearing for #661: a run that trips
/// the round breaker is the exact "too-many-iterations" signal the
/// histogram is there to surface — skipping non-Completed
/// terminals would hide it.
#[tokio::test]
async fn histogram_observes_failed_and_canceled_terminals() {
    let (store, metrics, tap) = setup().await;
    let project = project();

    for (run_str, terminal_state) in [
        ("r_failed_c", RunState::Failed),
        ("r_canceled_c", RunState::Canceled),
    ] {
        let run_id = RunId::new(run_str);
        store
            .append(&[envelope(
                RuntimeEvent::CheckpointPersisted(CheckpointPersisted {
                    project: project.clone(),
                    checkpoint_id: CheckpointId::new(format!("ckpt_{run_str}")),
                    session_id: SessionId::new(format!("s_{run_str}")),
                    root_run_id: run_id.clone(),
                    iteration: 10,
                    at_ms: 0,
                }),
                &format!("ckpt_{run_str}"),
            )])
            .await
            .unwrap();

        store
            .append(&[envelope(
                RuntimeEvent::RunStateChanged(RunStateChanged {
                    project: project.clone(),
                    run_id,
                    transition: StateTransition {
                        from: Some(RunState::Running),
                        to: terminal_state,
                    },
                    failure_class: None,
                    pause_reason: None,
                    resume_trigger: None,
                }),
                &format!("term_{run_str}"),
            )])
            .await
            .unwrap();
    }

    // Two observations, each at iteration 10. Sum = 20.
    wait_for_metrics(
        &metrics,
        &[
            "cairn_orchestrator_iterations_per_run_count 2",
            "cairn_orchestrator_iterations_per_run_sum 20",
            "cairn_orchestrator_inline_run_ratio_samples 2",
        ],
    )
    .await;

    tap.shutdown().await;
}

/// Verify the Prometheus scrape surface renders the always-on
/// subagent counter + ratio-samples gauge even on a store with zero
/// events. This is the "dogfood diagnostic" shape: operators see
/// `cairn_orchestrator_subagent_spawn_total 0` at the `/metrics`
/// endpoint the moment the server boots.
#[tokio::test]
async fn metrics_endpoint_always_renders_subagent_surface() {
    let (_, metrics, tap) = setup().await;
    let rendered = metrics.render_prometheus();

    assert!(
        rendered.contains("# HELP cairn_orchestrator_subagent_spawn_total"),
        "help line must render on an empty store;\n{rendered}"
    );
    assert!(
        rendered.contains("cairn_orchestrator_subagent_spawn_total 0"),
        "counter must render as 0 on an empty store — the diagnostic signal;\n{rendered}"
    );
    assert!(
        rendered.contains("cairn_orchestrator_inline_run_ratio_samples 0"),
        "samples gauge must render as 0 on an empty store;\n{rendered}"
    );

    tap.shutdown().await;
}

// ── HTTP-wire tests for GET /v1/runs/:id subagent counts ─────────────────

const HTTP_TOKEN: &str = "subagent-obs-test-token";
const HTTP_TENANT: &str = "acme";
const HTTP_WORKSPACE: &str = "prod";
const HTTP_PROJECT: &str = "dogfood";

fn http_project() -> ProjectKey {
    ProjectKey::new(HTTP_TENANT, HTTP_WORKSPACE, HTTP_PROJECT)
}

fn seed_principal(state: &AppState) {
    state.service_tokens.register(
        HTTP_TOKEN.to_string(),
        AuthPrincipal::Operator {
            operator_id: cairn_domain::OperatorId::new("subagent_obs_op"),
            tenant: TenantKey::new(HTTP_TENANT),
        },
    );
}

fn http_envelope(id: &str, event: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(EventId::new(id), EventSource::System, event)
}

fn session_created(session_id: &str) -> EventEnvelope<RuntimeEvent> {
    http_envelope(
        &format!("sc_{session_id}"),
        RuntimeEvent::SessionCreated(SessionCreated {
            project: http_project(),
            session_id: SessionId::new(session_id),
        }),
    )
}

fn run_created(
    run_id: &str,
    session_id: &str,
    parent_run_id: Option<&str>,
) -> EventEnvelope<RuntimeEvent> {
    http_envelope(
        &format!("rc_{run_id}"),
        RuntimeEvent::RunCreated(RunCreated {
            project: http_project(),
            session_id: SessionId::new(session_id),
            run_id: RunId::new(run_id),
            parent_run_id: parent_run_id.map(RunId::new),
            prompt_release_id: None,
            agent_role_id: None,
        }),
    )
}

fn run_state_transition(
    run_id: &str,
    from: Option<RunState>,
    to: RunState,
) -> EventEnvelope<RuntimeEvent> {
    http_envelope(
        &format!("rsc_{run_id}_{to:?}"),
        RuntimeEvent::RunStateChanged(RunStateChanged {
            project: http_project(),
            run_id: RunId::new(run_id),
            transition: StateTransition { from, to },
            failure_class: None,
            pause_reason: None,
            resume_trigger: None,
        }),
    )
}

async fn http_get(app: Router, uri: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {HTTP_TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let res = tower::ServiceExt::oneshot(app, req).await.unwrap();
    let status = res.status();
    let bytes = to_bytes(res.into_body(), 10 * 1024 * 1024).await.unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or_else(|err| {
            panic!(
                "response body is not valid JSON: {err}\nbody = {}",
                String::from_utf8_lossy(&bytes)
            )
        })
    };
    (status, json)
}

/// End-to-end: a parent run with two child runs (one completed, one
/// failed). `GET /v1/runs/:parent` must surface counts 2/1/1 on the
/// three new fields.
#[tokio::test]
async fn get_run_detail_returns_subagent_counts() {
    let (router, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    seed_principal(&state);

    // Parent run + two child runs under the same session.
    state
        .runtime
        .store
        .append(&[
            session_created("sess_detail"),
            run_created("run_parent_detail", "sess_detail", None),
            run_created("run_child_ok", "sess_detail", Some("run_parent_detail")),
            run_created("run_child_bad", "sess_detail", Some("run_parent_detail")),
            run_state_transition("run_child_ok", Some(RunState::Pending), RunState::Running),
            run_state_transition("run_child_ok", Some(RunState::Running), RunState::Completed),
            run_state_transition("run_child_bad", Some(RunState::Pending), RunState::Running),
            run_state_transition("run_child_bad", Some(RunState::Running), RunState::Failed),
        ])
        .await
        .expect("append events");

    let (status, body) = http_get(router, "/v1/runs/run_parent_detail").await;
    assert_eq!(status, StatusCode::OK, "detail 200: {body}");

    let run = body
        .get("run")
        .unwrap_or_else(|| panic!("detail body missing 'run' field: {body}"));

    // Counts come from `build_run_record_view_with_subagents` walking
    // `list_by_parent_run`. Two children total, one completed, one failed.
    assert_eq!(
        run.get("subagents_spawned").and_then(Value::as_u64),
        Some(2),
        "parent should show 2 spawned children: {body}"
    );
    assert_eq!(
        run.get("subagents_completed").and_then(Value::as_u64),
        Some(1),
        "one child reached Completed terminal: {body}"
    );
    assert_eq!(
        run.get("subagents_failed").and_then(Value::as_u64),
        Some(1),
        "one child reached Failed terminal: {body}"
    );
}

/// An inline-only run (no children) reports zeroes on all three
/// fields. This is the pre-#662 dogfood shape: the orchestrator LLM
/// never delegated, so the counts are flat zeros and the operator
/// sees that immediately.
#[tokio::test]
async fn get_run_detail_reports_zero_counts_for_inline_run() {
    let (router, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    seed_principal(&state);

    state
        .runtime
        .store
        .append(&[
            session_created("sess_inline"),
            run_created("run_inline_only", "sess_inline", None),
        ])
        .await
        .expect("append events");

    let (status, body) = http_get(router, "/v1/runs/run_inline_only").await;
    assert_eq!(status, StatusCode::OK, "detail 200: {body}");

    let run = body
        .get("run")
        .unwrap_or_else(|| panic!("detail body missing 'run' field: {body}"));

    assert_eq!(
        run.get("subagents_spawned").and_then(Value::as_u64),
        Some(0)
    );
    assert_eq!(
        run.get("subagents_completed").and_then(Value::as_u64),
        Some(0)
    );
    assert_eq!(run.get("subagents_failed").and_then(Value::as_u64), Some(0));
}

/// The list endpoint (`GET /v1/runs`) keeps the response shape flat:
/// the subagent count fields are populated only by the detail
/// endpoint. The list hits per-run `list_by_parent_run` at fan-out
/// cost we don't pay on batch scans.
#[tokio::test]
async fn list_endpoint_omits_subagent_count_fields() {
    let (router, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    seed_principal(&state);

    state
        .runtime
        .store
        .append(&[
            session_created("sess_list"),
            run_created("run_parent_list", "sess_list", None),
            run_created("run_child_list", "sess_list", Some("run_parent_list")),
        ])
        .await
        .expect("append events");

    let (status, body) = http_get(
        router,
        &format!(
            "/v1/runs?tenant_id={HTTP_TENANT}&workspace_id={HTTP_WORKSPACE}&project_id={HTTP_PROJECT}"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "list 200: {body}");

    // The list payload returns both runs; the parent must NOT carry
    // the detail-only subagent fields. `skip_serializing_if =
    // Option::is_none` on the view keeps the wire shape tight.
    let items = body["items"]
        .as_array()
        .unwrap_or_else(|| panic!("list missing items: {body}"));
    let parent = items
        .iter()
        .find(|r| r["run_id"] == "run_parent_list")
        .unwrap_or_else(|| panic!("parent not found in list: {body}"));
    assert!(
        parent.get("subagents_spawned").is_none(),
        "list must not populate subagents_spawned — detail-only field: {body}"
    );
    assert!(
        parent.get("subagents_completed").is_none(),
        "list must not populate subagents_completed: {body}"
    );
    assert!(
        parent.get("subagents_failed").is_none(),
        "list must not populate subagents_failed: {body}"
    );
}
