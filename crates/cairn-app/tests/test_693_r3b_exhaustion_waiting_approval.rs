//! Issue #693 R3-B — when all providers are exhausted and the
//! orchestrate handler submits the `escalate_to_operator` approval
//! card, the run lifecycle state must flip to `WaitingApproval` so
//! `GET /v1/runs/:id` reflects "blocked on operator" instead of the
//! stale `running` that was the dogfood finding.
//!
//! Regression guard:
//! * Mock provider fails every request with a retryable 500 so the
//!   decide phase walks the fallback chain and ends in
//!   `AllProvidersExhausted` after every listed model has been tried.
//! * Orchestrate returns HTTP 502 `all_providers_exhausted` — existing
//!   canonical-envelope test (`test_api_canonical_envelope.rs`) covers
//!   the body shape; here we focus on the side-effect ordering.
//! * The `escalate_to_operator` tool-call-approval card must be pending
//!   (pre-existing behaviour — if this regresses, the first assertion
//!   trips first and points at that regression, not the state flip).
//! * `GET /v1/runs/:id` must report `state=waiting_approval`. Pre-fix
//!   this returns `running`, which is the #693 R3-B symptom.

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

const MODEL_A: &str = "openrouter/exhaust-model-a";
const MODEL_B: &str = "openrouter/exhaust-model-b";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
}

/// Always returns a retryable 500 so cairn's provider classifier marks
/// the error as `server_error` (fallback-eligible). After every
/// listed model has been tried, the orchestrator hits
/// `AllProvidersExhausted`.
async fn spawn_exhausting_mock() -> (String, Arc<AtomicUsize>) {
    let state = MockState {
        hits: Arc::new(AtomicUsize::new(0)),
    };
    let hits = state.hits.clone();

    async fn chat_handler(
        State(state): State<MockState>,
        Json(_body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        state.hits.fetch_add(1, Ordering::SeqCst);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": {
                    "message": "forced upstream 500 for exhaustion dogfood test",
                    "code": 500,
                }
            })),
        )
    }

    let app = Router::new()
        .route("/chat/completions", post(chat_handler))
        .route("/v1/chat/completions", post(chat_handler))
        .route(
            "/v1/models",
            get(|| async {
                Json(json!({
                    "data": [
                        {"id": MODEL_A},
                        {"id": MODEL_B},
                    ]
                }))
            }),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    (format!("http://{addr}"), hits)
}

/// #693 R3-B regression: after providers-exhausted escalation, `GET
/// /v1/runs/:id` reports `state=waiting_approval`, matching the pending
/// `escalate_to_operator` approval card (rather than the pre-fix stale
/// `running`).
#[tokio::test]
async fn exhaustion_flips_run_to_waiting_approval_and_leaves_card_pending() {
    let h = LiveHarness::setup().await;
    let (mock_url, hits) = spawn_exhausting_mock().await;

    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_r3b_{suffix}");
    let session_id = format!("sess_r3b_{suffix}");
    let run_id = format!("run_r3b_{suffix}");

    // 1. Credential.
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": "openrouter",
            "plaintext_value": format!("sk-r3b-{suffix}"),
        }))
        .send()
        .await
        .expect("credential reaches server");
    assert_eq!(r.status().as_u16(), 201);
    let credential_id = r
        .json::<Value>()
        .await
        .expect("credential json")
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_owned();

    // 2. Provider connection with two models — both will 500, forcing
    //    exhaustion after both are tried.
    let r = h
        .client()
        .post(format!("{}/v1/providers/connections", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id": tenant,
            "provider_connection_id": connection_id,
            "provider_family": "openrouter",
            "adapter_type": "openrouter",
            "supported_models": [MODEL_A, MODEL_B],
            "credential_id": credential_id,
            "endpoint_url": mock_url,
        }))
        .send()
        .await
        .expect("connection reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "connection: {}",
        r.text().await.unwrap_or_default()
    );

    // 3. System defaults pin MODEL_A as the preferred.
    for key in ["generate_model", "brain_model"] {
        let r = h
            .client()
            .put(format!(
                "{}/v1/settings/defaults/system/system/{}",
                h.base_url, key,
            ))
            .bearer_auth(&h.admin_token)
            .json(&json!({ "value": MODEL_A }))
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
            "tenant_id": tenant,
            "workspace_id": workspace,
            "project_id": project,
            "session_id": session_id,
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
            "tenant_id": tenant,
            "workspace_id": workspace,
            "project_id": project,
            "session_id": session_id,
            "run_id": run_id,
        }))
        .send()
        .await
        .expect("run reaches server");
    assert_eq!(r.status().as_u16(), 201);

    // 5. Orchestrate — every model will 500, exhausting the fallback
    //    chain. Expect 502 all_providers_exhausted.
    let r = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id,))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "exercise exhaustion path",
            "max_iterations": 1,
        }))
        .send()
        .await
        .expect("orchestrate reaches server");
    let status = r.status().as_u16();
    let body = r.text().await.unwrap_or_default();
    assert_eq!(
        status, 502,
        "expected 502 all_providers_exhausted; got {status}: {body}"
    );
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    assert_eq!(
        parsed.get("code").and_then(|v| v.as_str()),
        Some("all_providers_exhausted"),
        "expected canonical all_providers_exhausted envelope: {body}"
    );
    // Sanity: the mock was actually hit (proves we exercised the
    // provider path, not some earlier short-circuit).
    assert!(
        hits.load(Ordering::SeqCst) >= 1,
        "mock provider should have been called at least once"
    );

    // 6. Pre-existing behaviour: the escalate_to_operator tool-call
    //    approval card must be pending. Listed first so a regression
    //    here trips the assertion pointing at the card subsystem, not
    //    the state flip.
    let r = h
        .client()
        .get(format!(
            "{}/v1/approvals?state=pending&kind=tool_call&run_id={}",
            h.base_url, run_id,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("approvals list reaches server");
    assert_eq!(r.status().as_u16(), 200);
    let approvals_body = r.json::<Value>().await.expect("approvals json");
    let items = approvals_body
        .get("items")
        .and_then(|v| v.as_array())
        .expect("items array");
    let has_escalate_card = items.iter().any(|item| {
        item.get("tool_name").and_then(|v| v.as_str()) == Some("escalate_to_operator")
            && item.get("state").and_then(|v| v.as_str()) == Some("pending")
            && item.get("run_id").and_then(|v| v.as_str()) == Some(run_id.as_str())
    });
    assert!(
        has_escalate_card,
        "expected a pending escalate_to_operator card for run {run_id}; got: {approvals_body}"
    );

    // 7. #693 R3-B core regression guard: `GET /v1/runs/:id` must
    //    report `state=waiting_approval` — not `running`. Pre-fix this
    //    returns `running` because the orchestrate handler never
    //    transitioned the run's lifecycle after emitting the card.
    let r = h
        .client()
        .get(format!("{}/v1/runs/{}", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("get run reaches server");
    assert_eq!(r.status().as_u16(), 200);
    let run_body = r.json::<Value>().await.expect("run json");
    let state = run_body
        .get("run")
        .and_then(|v| v.get("state"))
        .and_then(|v| v.as_str())
        .expect("run.state is a string");
    assert_eq!(
        state, "waiting_approval",
        "#693 R3-B: run state must be waiting_approval after providers-exhausted escalation, \
         not {state:?} — full body: {run_body}"
    );
}
