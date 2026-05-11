//! Issue #830 regression — the strict completion gate's sentinel scan
//! must reject `complete_run` when `final_answer` opens with a
//! failure-admission phrase.
//!
//! **Pathology (dogfood R27, 2026-05-10, post-#825).** Despite
//! `ActionType::FailRun` being published in tool_defs AND every role
//! prompt documenting `fail_run` as the verb for blocked runs,
//! glm-4.7 still called `complete_run` with `final_answer` = `"Task
//! incomplete. The following was NOT performed: cargo init was never
//! executed, src/main.rs was never written, ..."`. cairn-app flipped
//! the run to `state=completed` because the gate only inspected
//! `verification_acc.errors` (build-error lines), not the model's own
//! self-admission text.
//!
//! This file drives a full HTTP run with a scripted provider that
//! emits a native `complete_run` tool-call with an admission-shaped
//! `final_answer` on EVERY turn. The gate should reject 3 times and
//! terminate the run with `state=failed` +
//! `failure_class=verification_rejected`.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

const MODEL_ID: &str = "openrouter/test-830-sentinel-model";

/// R27-shape admission. The exact-phrase match is intentional — if
/// the scan drops `"was never executed"` / `"was never written"` /
/// `"were never added"` from the sentinel list this test must fail.
const ADMISSION_FINAL_ANSWER: &str = "Task incomplete. The repository was \
cloned and branch was created, but the following required steps were NOT \
performed: cargo init was never executed, src/main.rs was never written, \
dependencies were never added, cargo check was never run.";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
}

/// Scripted provider: EVERY turn emits `complete_run` with the
/// admission final_answer. This is the R27 pathology shape — glm-4.7
/// kept reaching for complete_run even after rejections.
async fn spawn_mock() -> (String, Arc<AtomicUsize>) {
    let state = MockState {
        hits: Arc::new(AtomicUsize::new(0)),
    };
    let hits = state.hits.clone();

    async fn chat_handler(
        State(state): State<MockState>,
        Json(_body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        let n = state.hits.fetch_add(1, Ordering::SeqCst);
        let tool_call = json!({
            "id": format!("call_{n}"),
            "type": "function",
            "function": {
                "name": "complete_run",
                "arguments": json!({ "final_answer": ADMISSION_FINAL_ANSWER }).to_string()
            }
        });
        (
            StatusCode::OK,
            Json(json!({
                "id":      format!("mock-830-{n}"),
                "choices": [{
                    "index":   0,
                    "message": {
                        "role":       "assistant",
                        "content":    "",
                        "tool_calls": [tool_call]
                    },
                    "finish_reason": "tool_calls",
                }],
                "usage": {
                    "prompt_tokens":     12,
                    "completion_tokens": 8,
                    "total_tokens":      20,
                },
            })),
        )
    }

    let app = Router::new()
        .route("/chat/completions", post(chat_handler))
        .route("/v1/chat/completions", post(chat_handler))
        .route(
            "/v1/models",
            get(|| async { Json(json!({ "data": [{ "id": MODEL_ID }] })) }),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(25)).await;
    (format!("http://{addr}"), hits)
}

/// Provision credential + provider connection + defaults + session +
/// run — scoped to a #830 suffix so this file's state doesn't collide
/// with test_660 / test_825 when run in parallel.
async fn provision_run(h: &LiveHarness, mock_url: &str) -> (String, String, String) {
    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_830_{suffix}");
    let session_id = format!("sess_830_{suffix}");
    let run_id = format!("run_830_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id":     "openrouter",
            "plaintext_value": format!("sk-830-{suffix}"),
        }))
        .send()
        .await
        .expect("credential");
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
            "tenant_id":              tenant,
            "provider_connection_id": connection_id,
            "provider_family":        "openrouter",
            "adapter_type":           "openrouter",
            "supported_models":       [MODEL_ID],
            "credential_id":          credential_id,
            "endpoint_url":           mock_url,
        }))
        .send()
        .await
        .expect("connection");
    assert_eq!(r.status().as_u16(), 201);

    for key in ["generate_model", "brain_model"] {
        let r = h
            .client()
            .put(format!(
                "{}/v1/settings/defaults/system/system/{}",
                h.base_url, key,
            ))
            .bearer_auth(&h.admin_token)
            .json(&json!({ "value": MODEL_ID }))
            .send()
            .await
            .expect("defaults");
        assert_eq!(r.status().as_u16(), 200);
    }

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
        .expect("session");
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
        .expect("run");
    assert_eq!(r.status().as_u16(), 201);

    (tenant, session_id, run_id)
}

/// Poll the run read-model up to 2s for `state == expected_state`.
/// Same pattern as `test_660_completion_gate::fetch_run_state` — the
/// orchestrate handler returns before the run-state projection has
/// applied the terminal event.
async fn fetch_run_state(h: &LiveHarness, run_id: &str, expected_state: &str) -> Value {
    use std::time::Instant;
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut backoff_us: u64 = 100;
    loop {
        let r = h
            .client()
            .get(format!("{}/v1/runs/{}", h.base_url, run_id))
            .bearer_auth(&h.admin_token)
            .send()
            .await
            .expect("get run");
        let body: Value = r.json().await.expect("run json");
        let state = body
            .get("run")
            .and_then(|r| r.get("state"))
            .and_then(Value::as_str)
            .or_else(|| body.get("state").and_then(Value::as_str))
            .unwrap_or("<missing>");
        if state == expected_state || Instant::now() >= deadline {
            return body;
        }
        tokio::time::sleep(Duration::from_micros(backoff_us)).await;
        backoff_us = (backoff_us * 2).min(5_000);
    }
}

/// #830 — when the LLM emits `complete_run` with an admission-shaped
/// `final_answer`, the sentinel scan must reject and the 3-strike cap
/// must fire with `verification_rejected:`. Pre-#830, the first
/// iteration would flip the run to `state=completed` (R27 pathology).
#[tokio::test]
async fn sentinel_gate_rejects_admission_complete_run_and_fails_after_three_rejects() {
    let h = LiveHarness::setup().await;
    let (mock_url, hits) = spawn_mock().await;
    let (_tenant, _session, run_id) = provision_run(&h, &mock_url).await;

    let orch = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal":           "#830: model should call fail_run, not complete_run with admission",
            "max_iterations": 10,
        }))
        .timeout(Duration::from_secs(60))
        .send()
        .await
        .expect("orchestrate");
    let orch_status = orch.status().as_u16();
    let orch_body: Value = orch.json().await.unwrap_or(Value::Null);

    assert_eq!(
        orch_status, 200,
        "orchestrate must return 200 even on sentinel-gate termination: body={orch_body}",
    );

    let termination = orch_body
        .get("termination")
        .and_then(Value::as_str)
        .unwrap_or("<missing>");
    assert_eq!(
        termination, "failed",
        "#830: admission-shaped complete_run MUST terminate the run as failed, \
         NOT completed. Got {termination}. body={orch_body}",
    );

    let reason = orch_body
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or("");
    assert!(
        reason.starts_with("verification_rejected:"),
        "#830: terminal reason must carry the `verification_rejected:` prefix \
         so classify_failed_reason routes to FailureClass::VerificationRejected. \
         Got: {reason:?}"
    );
    assert!(
        reason.contains("self-reporting failure") || reason.contains("sentinel"),
        "#830: terminal reason should identify the admission-sentinel path \
         so operators can distinguish it from an error-bucket termination. \
         Got: {reason:?}"
    );

    // Run projection: state=failed, failure_class=verification_rejected.
    let run = fetch_run_state(&h, &run_id, "failed").await;
    let state = run
        .get("run")
        .and_then(|r| r.get("state"))
        .and_then(Value::as_str)
        .or_else(|| run.get("state").and_then(Value::as_str))
        .unwrap_or("<missing>");
    assert_eq!(
        state, "failed",
        "#830: run projection must reflect the sentinel-gate termination. \
         Got {state}. full={run}",
    );
    let failure_class = run
        .get("run")
        .and_then(|r| r.get("failure_class"))
        .and_then(Value::as_str)
        .or_else(|| run.get("failure_class").and_then(Value::as_str))
        .unwrap_or("<missing>");
    assert_eq!(
        failure_class, "verification_rejected",
        "#830: failure_class must be `verification_rejected` (same class as \
         #660's error-bucket path; different trigger). Got {failure_class}.",
    );

    // The mock was hit at least 3 times — once per rejected attempt.
    assert!(
        hits.load(Ordering::SeqCst) >= 3,
        "#830: gate's 3-strike cap requires at least 3 DECIDE turns. \
         Got {} calls.",
        hits.load(Ordering::SeqCst)
    );
}
