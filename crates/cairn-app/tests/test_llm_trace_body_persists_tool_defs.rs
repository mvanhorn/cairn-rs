//! Dogfood R7 regression: `GET /v1/sessions/:id/llm-traces/:trace_id/body`
//! must return the `tools[]` array the request shipped with alongside
//! the existing `system_prompt` / `messages_json` / `response_text` /
//! `tool_calls_json` fields.
//!
//! # Why this test exists
//!
//! Dogfood R7 (2026-05-06) hit a diagnostic wall on issue #702: the
//! parent run emitted five identical `spawn_subagent` calls in a row,
//! and the persisted trace body surfaced everything EXCEPT the
//! `tools[]` array shipped TO the model. Debugging "did the model
//! have `complete_run` available when it chose to re-spawn?" required
//! re-reading the orchestrator source, which is worse than useless
//! when the suspicion is the orchestrator IS buggy.
//!
//! The fix threads `tool_defs_json` through:
//!
//! - `DecideOutput` (cairn-orchestrator)
//! - `LlmCompletionRecorded` domain event (cairn-domain)
//! - `LlmCompletionBodyRecord` projection (cairn-store)
//! - pg + sqlite + in-memory appliers (cairn-store)
//! - pg + sqlite adapter SELECTs (cairn-store)
//! - V071 migration (cairn-store pg)
//! - `GET /v1/sessions/:id/llm-traces/:trace_id/body` response
//!   (cairn-app)
//!
//! This test drives the outer layer: orchestrate a run against a mock
//! LLM, then fetch the trace body and assert `tool_defs_json` carries
//! a non-empty OpenAI-shape array that includes `complete_run` (always
//! registered) and the ancillary builtin tools.

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

const MOCK_MODEL: &str = "openrouter/tool-defs-observability";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
}

/// Mock OpenAI-compat endpoint returning a single `complete_run`
/// action so the orchestrator loop terminates in one iteration.
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
        let content = json!([{
            "action_type":       "complete_run",
            "description":       "done",
            "confidence":        0.99,
            "requires_approval": false,
        }]);
        (
            StatusCode::OK,
            Json(json!({
                "id":      format!("mock-tooldefs-{n}"),
                "choices": [{
                    "index":   0,
                    "message": {
                        "role":    "assistant",
                        "content": content.to_string(),
                    },
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
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), hits)
}

/// Provision credential + connection + binding + system defaults +
/// session + run. Returns `(session_id, run_id)`.
async fn provision_run(h: &LiveHarness, mock_url: &str) -> (String, String) {
    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_tooldefs_{suffix}");
    let session_id = format!("sess_tooldefs_{suffix}");
    let run_id = format!("run_tooldefs_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id":     "openrouter",
            "plaintext_value": format!("sk-tooldefs-{suffix}"),
        }))
        .send()
        .await
        .expect("credential reaches server");
    assert_eq!(r.status().as_u16(), 201, "credential");
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
            "supported_models":       [MOCK_MODEL],
            "credential_id":          credential_id,
            "endpoint_url":           mock_url,
        }))
        .send()
        .await
        .expect("connection");
    assert_eq!(r.status().as_u16(), 201, "connection");

    let r = h
        .client()
        .post(format!("{}/v1/providers/bindings", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id":              tenant,
            "workspace_id":           workspace,
            "project_id":             project,
            "provider_connection_id": connection_id,
            "operation_kind":         "generate",
            "provider_model_id":      MOCK_MODEL,
        }))
        .send()
        .await
        .expect("binding");
    assert_eq!(r.status().as_u16(), 201);

    for key in ["brain_model", "generate_model", "worker_model"] {
        let r = h
            .client()
            .put(format!(
                "{}/v1/settings/defaults/system/system/{}",
                h.base_url, key
            ))
            .bearer_auth(&h.admin_token)
            .json(&json!({ "value": MOCK_MODEL }))
            .send()
            .await
            .expect("default");
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

    (session_id, run_id)
}

/// The trace body endpoint must return the `tool_defs_json` field
/// populated with the tools[] array the request shipped with.
#[tokio::test]
async fn llm_trace_body_includes_tool_defs_json() {
    let h = LiveHarness::setup().await;
    let (session_id, run_id) = {
        let (mock_url, _hits) = spawn_mock().await;
        provision_run(&h, &mock_url).await
    };

    // Drive one orchestrate iteration. The mock returns complete_run
    // so the parent finishes on turn 0.
    let r = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "tool_defs observability smoke",
            "max_iterations": 1,
        }))
        .send()
        .await
        .expect("orchestrate");
    let status = r.status().as_u16();
    assert!(
        status == 200 || status == 202,
        "orchestrate status={status}"
    );

    // List traces for the session and grab the first one (ordered by
    // recorded_at_ms desc via the list endpoint contract).
    let r = h
        .client()
        .get(format!(
            "{}/v1/sessions/{}/llm-traces",
            h.base_url, session_id
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("list traces");
    assert_eq!(r.status().as_u16(), 200);
    let body: Value = r.json().await.unwrap();
    let traces = body
        .get("traces")
        .and_then(|v| v.as_array())
        .expect("traces array");
    let trace_id = traces
        .iter()
        .find(|t| t.get("run_id").and_then(|v| v.as_str()) == Some(run_id.as_str()))
        .and_then(|t| t.get("trace_id"))
        .and_then(|v| v.as_str())
        .expect("trace for this run")
        .to_owned();

    // Fetch the body.
    let r = h
        .client()
        .get(format!(
            "{}/v1/sessions/{}/llm-traces/{}/body",
            h.base_url, session_id, trace_id
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("trace body");
    assert_eq!(r.status().as_u16(), 200, "trace body status");
    let body: Value = r.json().await.expect("body json");

    // Core assertion: tool_defs_json is present, parseable as a JSON
    // array, and non-empty — the orchestrator always ships at least
    // complete_run + spawn_subagent + the builtin tool descriptors.
    let tool_defs_raw = body
        .get("tool_defs_json")
        .and_then(|v| v.as_str())
        .expect("tool_defs_json field exists and is a string");
    assert!(
        !tool_defs_raw.is_empty() && tool_defs_raw != "[]",
        "tool_defs_json must be a non-empty JSON array; got {tool_defs_raw:?}. \
         Pre-fix this field did not exist in the response (omitted by the \
         handler) or was empty (never populated from DecideOutput). The fix \
         threads the tools[] array through DecideOutput → \
         LlmCompletionRecorded → projection → HTTP response.",
    );

    let defs: Value = serde_json::from_str(tool_defs_raw)
        .unwrap_or_else(|e| panic!("tool_defs_json must parse as JSON: {e}; raw={tool_defs_raw}"));
    let defs_arr = defs.as_array().expect("tool_defs_json is a JSON array");
    assert!(
        defs_arr.len() >= 2,
        "expect at least complete_run + spawn_subagent; got {} entries",
        defs_arr.len(),
    );

    // complete_run must appear in the tools list — the key
    // observability claim is "we persist what the model had available."
    let has_complete_run = defs_arr.iter().any(|d| {
        d.get("function")
            .and_then(|f| f.get("name"))
            .and_then(|n| n.as_str())
            == Some("complete_run")
    });
    assert!(
        has_complete_run,
        "tool_defs_json must include complete_run entry; instead got names: {:?}",
        defs_arr
            .iter()
            .filter_map(|d| d
                .get("function")
                .and_then(|f| f.get("name"))
                .and_then(|n| n.as_str()))
            .collect::<Vec<_>>(),
    );
}
