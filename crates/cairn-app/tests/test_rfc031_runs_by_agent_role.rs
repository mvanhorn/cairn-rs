//! RFC 031 PR-D3 — `GET /v1/runs?agent_role_id=<id>` exact-match
//! filter. Powers the `AgentRoleRetractModal` probe: the UI issues
//! `getRuns({ agent_role_id: role, status: 'running' })` when the
//! modal opens so the operator sees "N runs currently using this
//! role" before committing to the retract.
//!
//! # Contract
//!
//! - Exact-match equality on the run's `agent_role_id`.
//! - Runs without a role set never match the filter.
//! - Invalid `status` values return 422 (unchanged from PR-C).
//! - Unknown `agent_role_id` returns 200 + empty list (not 404 —
//!   the probe is a count, and an unknown role answering zero is
//!   the correct count).
//!
//! Seeds two roles (`alpha` × 2, `beta` × 1) + one roleless run,
//! then hits the endpoint with each filter combination and asserts
//! membership + cardinality. Skipped: the combined
//! `agent_role_id` + `status` intersection — that's a store-layer
//! composition already covered by the unit test in in_memory.rs.

mod support;

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Router,
};
use cairn_api::auth::AuthPrincipal;
use cairn_api::bootstrap::BootstrapConfig;
use cairn_app::AppState;
use cairn_domain::tenancy::TenantKey;
use cairn_domain::{
    EventEnvelope, EventId, EventSource, ProjectKey, RunCreated, RunId, RuntimeEvent,
    SessionCreated, SessionId,
};
use cairn_store::event_log::EventLog;
use serde_json::Value;

const TOKEN: &str = "rfc031-pr-d3-runs-filter-token";
const TENANT: &str = "acme";
const WORKSPACE: &str = "prod";
const PROJECT: &str = "minecraft";

fn bearer() -> String {
    format!("Bearer {TOKEN}")
}

fn seed_principal(state: &AppState) {
    state.service_tokens.register(
        TOKEN.to_string(),
        AuthPrincipal::Operator {
            operator_id: cairn_domain::OperatorId::new("rfc031_op"),
            tenant: TenantKey::new(TENANT),
        },
    );
}

fn project() -> ProjectKey {
    ProjectKey::new(TENANT, WORKSPACE, PROJECT)
}

fn envelope(id: &str, event: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(EventId::new(id), EventSource::System, event)
}

fn session_created_event(session_id: &str) -> EventEnvelope<RuntimeEvent> {
    envelope(
        &format!("rfc031_sc_{session_id}"),
        RuntimeEvent::SessionCreated(SessionCreated {
            project: project(),
            session_id: SessionId::new(session_id),
        }),
    )
}

fn run_created_event(
    session_id: &str,
    run_id: &str,
    agent_role_id: Option<&str>,
) -> EventEnvelope<RuntimeEvent> {
    envelope(
        &format!("rfc031_rc_{run_id}"),
        RuntimeEvent::RunCreated(RunCreated {
            project: project(),
            session_id: SessionId::new(session_id),
            run_id: RunId::new(run_id),
            parent_run_id: None,
            prompt_release_id: None,
            agent_role_id: agent_role_id.map(str::to_owned),
        }),
    )
}

async fn http_get(app: Router, uri: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", bearer())
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

fn run_ids(list_body: &Value) -> Vec<&str> {
    list_body["items"]
        .as_array()
        .unwrap_or_else(|| panic!("list body missing items: {list_body}"))
        .iter()
        .map(|r| r["run_id"].as_str().unwrap())
        .collect()
}

async fn seed_runs(state: &AppState) {
    let session_id = "sess_probe";
    state
        .runtime
        .store
        .append(&[
            session_created_event(session_id),
            run_created_event(session_id, "r_alpha_1", Some("alpha")),
            run_created_event(session_id, "r_alpha_2", Some("alpha")),
            run_created_event(session_id, "r_beta_1", Some("beta")),
            run_created_event(session_id, "r_none", None),
        ])
        .await
        .expect("append");
}

fn list_url(agent_role_id: Option<&str>) -> String {
    let mut url =
        format!("/v1/runs?tenant_id={TENANT}&workspace_id={WORKSPACE}&project_id={PROJECT}");
    if let Some(role) = agent_role_id {
        url.push_str(&format!("&agent_role_id={role}"));
    }
    url
}

/// No filter → all four runs visible.
#[tokio::test]
async fn list_without_filter_returns_every_run() {
    let (router, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    seed_principal(&state);
    seed_runs(&state).await;

    let (status, body) = http_get(router, &list_url(None)).await;
    assert_eq!(status, StatusCode::OK, "200: {body}");
    let mut ids = run_ids(&body);
    ids.sort();
    assert_eq!(ids, ["r_alpha_1", "r_alpha_2", "r_beta_1", "r_none"]);
}

/// `agent_role_id=alpha` → only the two alpha runs.
#[tokio::test]
async fn list_filtered_by_agent_role_returns_exact_matches() {
    let (router, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    seed_principal(&state);
    seed_runs(&state).await;

    let (status, body) = http_get(router, &list_url(Some("alpha"))).await;
    assert_eq!(status, StatusCode::OK, "200: {body}");
    let mut ids = run_ids(&body);
    ids.sort();
    assert_eq!(ids, ["r_alpha_1", "r_alpha_2"]);
}

/// `agent_role_id=beta` → the single beta run. Confirms the filter
/// distinguishes between roles rather than defaulting to any-role.
#[tokio::test]
async fn list_filtered_by_other_role_returns_only_that_role() {
    let (router, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    seed_principal(&state);
    seed_runs(&state).await;

    let (status, body) = http_get(router, &list_url(Some("beta"))).await;
    assert_eq!(status, StatusCode::OK, "200: {body}");
    assert_eq!(run_ids(&body), ["r_beta_1"]);
}

/// A role with no runs returns an empty list, not 404 — the probe
/// is a count and "zero runs use this retired role" is the correct
/// successful answer.
#[tokio::test]
async fn list_filtered_by_unknown_role_returns_empty_list() {
    let (router, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    seed_principal(&state);
    seed_runs(&state).await;

    let (status, body) = http_get(router, &list_url(Some("nonexistent"))).await;
    assert_eq!(status, StatusCode::OK, "200: {body}");
    assert!(
        run_ids(&body).is_empty(),
        "expected empty list for unknown role: {body}",
    );
}
