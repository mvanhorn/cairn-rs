//! #756: tool-call-approval suspension must flip projection state to
//! `WaitingApproval` so `GET /v1/runs/:id` reflects the real
//! suspension. The orchestrator loop's F26 path correctly returns
//! `LoopTermination::WaitingApproval` and submits an approval card —
//! but pre-fix the post-loop handler in `drive_run_iteration` did
//! NOT call `enter_waiting_approval`, leaving the projection at
//! `state=Running` while the loop is genuinely suspended.
//!
//! R15b dogfood symptom (2026-05-08): operator dashboards filtering
//! `state=waiting_approval` to find rows that need attention missed
//! a real waiting-approval row because it reported `running`.
//!
//! ## Pre-fix vs post-fix
//!
//! Pre-fix: state="running", pause_reason=null. Approval card
//! exists in `/v1/approvals?state=pending` — the row is genuinely
//! blocked, but the projection lies.
//!
//! Post-fix: state="waiting_approval". Operators looking at running
//! rows + waiting-approval rows see the truth.
//!
//! Same shape as #693 R3-B's symptom (which closed via PR #585), but
//! that fix only covered the providers-exhausted branch. This test
//! covers the general F26 tool-call-approval path.

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

const MOCK_MODEL: &str = "openrouter/756-approval-fixture";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
}

/// Returns one `bash` action with `requires_approval=true`. The
/// orchestrator loop's F26 path picks this up, submits a tool-call
/// approval card, and returns `LoopTermination::WaitingApproval`.
async fn chat_handler(
    State(state): State<MockState>,
    Json(_body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let n = state.hits.fetch_add(1, Ordering::SeqCst);
    let content = json!([{
        "action_type":       "invoke_tool",
        "description":       "#756 fixture: bash needing approval",
        "tool_name":          "bash",
        "tool_args":          { "command": "echo hello" },
        "confidence":        0.99,
        "requires_approval": true,
    }]);
    (
        StatusCode::OK,
        Json(json!({
            "id":      format!("mock-756-{n}"),
            "choices": [{
                "index":   0,
                "message": { "role": "assistant", "content": content.to_string() },
                "finish_reason": "stop",
            }],
            "usage": {
                "prompt_tokens":     10,
                "completion_tokens": 6,
                "total_tokens":      16,
            },
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
async fn tool_call_approval_suspension_flips_state_to_waiting_approval() {
    let h = LiveHarness::setup().await;
    let (mock_url, hits) = spawn_mock().await;

    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_756_{suffix}");
    let session_id = format!("sess_756_{suffix}");
    let run_id = format!("run_756_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": "openrouter",
            "plaintext_value": format!("sk-756-{suffix}"),
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

    let r = h
        .client()
        .post(format!("{}/v1/providers/connections", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id": tenant,
            "provider_connection_id": connection_id,
            "provider_family": "openrouter",
            "adapter_type": "openrouter",
            "supported_models": [MOCK_MODEL],
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
        r.text().await.unwrap_or_default(),
    );

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

    let r = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id,))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "exercise tool-call approval suspension",
            "max_iterations": 1,
        }))
        .send()
        .await
        .expect("orchestrate reaches server");
    let status = r.status().as_u16();
    let body = r.text().await.unwrap_or_default();
    assert_eq!(
        status, 202,
        "expected 202 waiting_approval; got {status}: {body}"
    );
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    assert_eq!(
        parsed.get("termination").and_then(|v| v.as_str()),
        Some("waiting_approval"),
        "expected termination=waiting_approval envelope: {body}"
    );
    // max_iterations=1 above means exactly one DECIDE-phase LLM call
    // (which proposes the bash tool requiring approval, hits F26,
    // suspends). An unexpected retry or extra iteration would push
    // this above 1 and likely indicate a regression in the loop's
    // approval-pending suspension path.
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "mock provider should have been called exactly once (one DECIDE turn before F26 suspension)"
    );

    // Pending tool-call approval card MUST exist (locks in the F26
    // submission path so a regression there trips here first).
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
    assert!(
        !items.is_empty(),
        "expected at least one pending tool-call approval card; got: {approvals_body}"
    );

    // #756 core regression guard: `GET /v1/runs/:id` MUST report
    // `state=waiting_approval`. Pre-fix the loop's F26 termination
    // returned without flipping projection state, so this read
    // returned `running`.
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
        "#756: run state must be `waiting_approval` after the F26 tool-call \
         approval suspension; pre-fix this returned `running` because the \
         post-loop handler never called enter_waiting_approval. Got \
         state={state:?}, body={run_body}",
    );
}
