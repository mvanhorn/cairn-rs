//! #744: any `OrchestratorError` that bubbles out of the loop must
//! finalize the run to terminal `Failed` BEFORE the HTTP error
//! response is built. Pre-fix the catch-all `Err(_)` arm in
//! `drive_run_iteration` returned 5xx but left the run row at
//! `state=Running` forever — the R13 dogfood symptom (parent stuck
//! `running` for 6+ minutes, no `failure_class`, no `pause_reason`).
//!
//! Test shape: drive `/orchestrate` with a mock LLM provider that
//! always returns HTTP 401. Inside the orchestrator's DECIDE phase
//! this produces `OrchestratorError::ProviderAuthFailed`, which the
//! handler maps to HTTP 503. Pre-fix the run stayed at `Running`;
//! post-fix the run is finalized `Failed(ExecutionError)` so
//! `GET /v1/runs/:id` reflects the terminal state.
//!
//! Why this specific shape: `ProviderAuthFailed` is the most
//! deterministically-reachable error variant in the catch-all bucket
//! (no race, no infrastructure prerequisite). The fix is generic —
//! every variant except `Runtime::NotFound` /
//! `Runtime::InvalidTransition` / `AllProvidersExhausted` (handled
//! separately) now finalizes — so this single representative test
//! locks in the contract for the whole class.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

const MOCK_MODEL: &str = "openrouter/744-auth-fail";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
}

/// Always returns HTTP 401. cairn-providers classifies this as a
/// non-retryable auth failure → DECIDE returns
/// `OrchestratorError::ProviderAuthFailed`.
async fn chat_handler(
    State(state): State<MockState>,
    Json(_body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    state.hits.fetch_add(1, Ordering::SeqCst);
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "error": {
                "message": "forced upstream 401 for #744 regression test",
                "code": 401,
                "type": "invalid_api_key",
            }
        })),
    )
}

async fn spawn_mock() -> (String, Arc<AtomicUsize>) {
    let state = MockState {
        hits: Arc::new(AtomicUsize::new(0)),
    };
    let hits = state.hits.clone();
    let app = Router::new()
        .route("/chat/completions", post(chat_handler))
        .route("/v1/chat/completions", post(chat_handler))
        .route(
            "/v1/models",
            get(|| async { Json(json!({ "data": [{ "id": MOCK_MODEL }] })) }),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    (format!("http://{addr}"), hits)
}

#[tokio::test]
async fn provider_auth_failure_finalizes_run_to_failed() {
    let h = LiveHarness::setup().await;
    let (mock_url, hits) = spawn_mock().await;

    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_744_{suffix}");
    let session_id = format!("sess_744_{suffix}");
    let run_id = format!("run_744_{suffix}");

    // 1. Credential.
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id":     "openrouter",
            "plaintext_value": format!("sk-744-{suffix}"),
        }))
        .send()
        .await
        .expect("credential reaches server");
    assert_eq!(r.status().as_u16(), 201);
    let credential_id = r
        .json::<Value>()
        .await
        .unwrap()
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_owned();

    // 2. Connection.
    let r = h
        .client()
        .post(format!("{}/v1/providers/connections", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id":              tenant,
            "provider_connection_id": connection_id,
            "provider_family":        "openrouter",
            "adapter_type":           "openrouter",
            "supported_models":       [MOCK_MODEL],
            "credential_id":          credential_id,
            "endpoint_url":           mock_url,
        }))
        .send()
        .await
        .expect("connection reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "connection: {}",
        r.text().await.unwrap_or_default(),
    );

    // 3. Defaults.
    for key in ["generate_model", "brain_model"] {
        let r = h
            .client()
            .put(format!(
                "{}/v1/settings/defaults/system/system/{}",
                h.base_url, key,
            ))
            .bearer_auth(&h.admin_token)
            .json(&json!({ "value": MOCK_MODEL }))
            .send()
            .await
            .expect("defaults reach server");
        assert_eq!(r.status().as_u16(), 200);
    }

    // 4. Session + run.
    let r = h
        .client()
        .post(format!("{}/v1/sessions", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id":    tenant,
            "workspace_id": workspace,
            "project_id":   project,
            "session_id":   session_id,
        }))
        .send()
        .await
        .expect("session reaches server");
    assert_eq!(r.status().as_u16(), 201);

    let r = h
        .client()
        .post(format!("{}/v1/runs", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id":    tenant,
            "workspace_id": workspace,
            "project_id":   project,
            "session_id":   session_id,
            "run_id":       run_id,
        }))
        .send()
        .await
        .expect("run reaches server");
    assert_eq!(r.status().as_u16(), 201);

    // 5. Orchestrate — every LLM call returns 401, DECIDE returns
    //    `OrchestratorError::ProviderAuthFailed`. HTTP 503.
    let r = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal":           "exercise auth-failure path for #744 regression",
            "max_iterations": 1,
        }))
        .send()
        .await
        .expect("orchestrate reaches server");
    let status = r.status().as_u16();
    let body = r.text().await.unwrap_or_default();
    assert_eq!(
        status, 503,
        "expected 503 provider_auth_failed; got {status}: {body}"
    );
    assert!(
        hits.load(Ordering::SeqCst) >= 1,
        "mock provider should have been called at least once"
    );

    // 6. #744 core regression guard. Pre-fix the run stayed at
    //    `state=running` because the catch-all `Err(_)` arm never
    //    called `finalize_run_failure`. Post-fix the run is terminal
    //    `failed` with `failure_class=execution_error`.
    let r = h
        .client()
        .get(format!("{}/v1/runs/{}", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("get run reaches server");
    assert_eq!(r.status().as_u16(), 200);
    let run_body = r.json::<Value>().await.expect("run json");
    let run_state = run_body
        .get("run")
        .and_then(|v| v.get("state"))
        .and_then(|v| v.as_str())
        .expect("run.state is a string");
    assert_eq!(
        run_state, "failed",
        "#744: run state must be `failed` after a non-retryable provider \
         auth failure — pre-fix returned 503 but left the run at \
         `running` forever (R13 dogfood symptom). Got state={run_state:?} \
         body={run_body}",
    );
    let failure_class = run_body
        .get("run")
        .and_then(|v| v.get("failure_class"))
        .and_then(|v| v.as_str())
        .expect("failure_class is a string post-finalize");
    assert_eq!(
        failure_class, "execution_error",
        "#744: failure_class must be `execution_error` for non-NotFound, \
         non-InvalidTransition, non-AllProvidersExhausted loop errors. \
         Got failure_class={failure_class:?}",
    );
}
