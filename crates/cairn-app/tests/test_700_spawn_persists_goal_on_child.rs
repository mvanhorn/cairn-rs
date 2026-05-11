//! #700 regression: `FabricTaskServiceAdapter::spawn_subagent` must
//! persist the LLM's delegation goal onto the **child run's** defaults
//! so that `ChildRunDriver::dispatch_child_iteration` + F49 auto-resume
//! both read the real goal via `resolve_run_string_default(..., "goal")`
//! instead of falling through to the `"Execute the run objective."`
//! placeholder.
//!
//! # The bug
//!
//! Dogfood R6 (2026-05-06) with R5-A (native `spawn_subagent` tool_def)
//! showed that Nemotron-3-super-120b emits a perfectly-shaped tool_call:
//!
//! ```json
//! { "role": "researcher", "goal": "Find 3 Rust circuit breaker patterns" }
//! ```
//!
//! The execute path in `cairn-orchestrator/src/execute_impl.rs:1224-1257`
//! extracts `goal` correctly and hands it to
//! `TaskService::spawn_subagent(..., goal, role)`. The adapter then:
//!
//! 1. Creates the child `RunRecord` with `parent_run_id` + `agent_role_id`.
//! 2. Emits `BridgeEvent::SubagentSpawned { goal, role, ... }`.
//! 3. Returns — but *never persists `goal` on the child run's defaults*.
//!
//! When `ChildRunDriver` claims the child and calls `drive_run_iteration`
//! with `OrchestrateRequest::default()` (goal=None), the helper reads
//! `resolve_run_string_default(..., "goal")` → None → falls through to
//! `"Execute the run objective."`. The subagent's LLM sees no goal and
//! produces empty output; the parent auto-resumes, re-DECIDEs, emits
//! another identical spawn. Observed loop: 6 spawns before I canceled.
//!
//! Issue: https://github.com/avifenesh/cairn-rs/issues/700
//!
//! # What this test asserts
//!
//! After the LLM proposes `spawn_subagent` with `goal=X`, the child
//! run's `goal` default MUST round-trip to `X` via the HTTP settings
//! endpoint. That's the exact read path `resolve_run_string_default`
//! uses, so this test locks in that the child's first orchestrator
//! iteration will see `X` — not the placeholder.
//!
//! # Pre-fix behaviour
//!
//! Running this test on main at `6a16326c` (the PR-merge that shipped
//! R5-B, the commit immediately before this fix) produces:
//!
//!   * `GET /v1/settings/defaults/project/<project>/run:<child_run_id>:goal`
//!     returns 404 — the default was never written.
//!   * Assertion `body.value == DELEGATED_GOAL` fails.
//!
//! Post-fix: the adapter writes `run:<child_run_id>:goal = <goal>`
//! in the defaults service right after `start_with_role` succeeds.

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

const MOCK_MODEL: &str = "openrouter/700-goal-persistence";
const DELEGATED_GOAL: &str = "locate-distinct-marker-700-xyz";
const DELEGATED_ROLE: &str = "researcher";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
}

/// Mock OpenAI-compat endpoint: first call proposes `spawn_subagent`
/// with the delegated goal+role; subsequent calls return `complete_run`
/// so the parent's orchestrator loop terminates cleanly if tickled.
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
        let content = if n == 0 {
            json!([{
                "action_type":       "spawn_subagent",
                "description":       "#700 regression: delegate with a distinct goal marker",
                "tool_name":         DELEGATED_ROLE,
                "tool_args":         { "goal": DELEGATED_GOAL },
                "confidence":        0.95,
                "requires_approval": false,
            }])
        } else {
            json!([{
                "action_type":       "complete_run",
                "description":       "parent done",
                "confidence":        0.99,
                "requires_approval": false,
            }])
        };
        (
            StatusCode::OK,
            Json(json!({
                "id":      format!("mock-700-{n}"),
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

/// Provision tenant + credential + connection + model defaults +
/// session + parent run. Returns `(session_id, parent_run_id,
/// project_id)`.
async fn provision_run(h: &LiveHarness, mock_url: &str) -> (String, String, String) {
    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_700_{suffix}");
    let session_id = format!("sess_700_{suffix}");
    let run_id = format!("run_700_{suffix}");

    // Credential
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id":     "openrouter",
            "plaintext_value": format!("sk-700-{suffix}"),
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

    // Provider connection
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
        "provider connection: {}",
        r.text().await.unwrap_or_default()
    );

    // Binding for generate
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
        .expect("binding reaches server");
    assert_eq!(r.status().as_u16(), 201, "binding");

    // System defaults — brain/generate/worker all land on the mock.
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
            .expect("default reaches server");
        assert_eq!(r.status().as_u16(), 200, "default {key}");
    }

    // Session
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
    assert_eq!(r.status().as_u16(), 201, "session");

    // Parent run
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
    assert_eq!(r.status().as_u16(), 201, "run");

    (session_id, run_id, project)
}

/// #700: spawn_subagent's goal must land on the child run's `goal`
/// default so the downstream driver + auto-resume paths read the real
/// objective instead of the `"Execute the run objective."` fallback.
#[tokio::test]
async fn spawn_subagent_persists_goal_on_child_run_default() {
    let h = LiveHarness::setup().await;
    let (_session_id, parent_run_id, project_id) = {
        let (mock_url, _hits) = spawn_mock().await;
        provision_run(&h, &mock_url).await
    };

    // Drive one orchestrate iteration. The mock returns spawn_subagent
    // on the first call; execute extracts goal+role, the adapter
    // creates the child run and MUST write the goal to defaults.
    let r = h
        .client()
        .post(format!(
            "{}/v1/runs/{}/orchestrate",
            h.base_url, parent_run_id
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "#700 parent run goal",
            "max_iterations": 1,
        }))
        .send()
        .await
        .expect("orchestrate reaches server");
    let status = r.status().as_u16();
    assert!(
        status == 200 || status == 202,
        "orchestrate must succeed on spawn_subagent proposal; status={status}",
    );

    // Find the child run id. `GET /v1/runs/:id/children` returns the
    // freshly-created child; we use its run_id for the defaults read.
    let r = h
        .client()
        .get(format!("{}/v1/runs/{}/children", h.base_url, parent_run_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("children reaches server");
    assert_eq!(r.status().as_u16(), 200, "GET children");
    let body: Value = r.json().await.unwrap();
    let items = body
        .get("items")
        .and_then(|v| v.as_array())
        .expect("children response items");
    assert_eq!(
        items.len(),
        1,
        "exactly one child expected after one spawn; got {items:?}",
    );
    let child_run_id = items[0]
        .get("run_id")
        .and_then(|v| v.as_str())
        .expect("child has run_id");

    // Core assertion: the child's `goal` default is populated with the
    // LLM's delegation goal verbatim. The key format mirrors
    // `helpers::run_default_key(child_run_id, "goal")`:
    // `run:<child_run_id>:goal`, scoped to the parent's project.
    //
    // Pre-fix this 404s because the adapter never wrote the default.
    // Post-fix it returns 200 with `value == DELEGATED_GOAL`.
    let key = format!("run:{child_run_id}:goal");
    let r = h
        .client()
        .get(format!(
            "{}/v1/settings/defaults/project/{}/{}",
            h.base_url, project_id, key
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("defaults GET reaches server");

    assert_eq!(
        r.status().as_u16(),
        200,
        "#700: child run's `goal` default MUST exist after spawn_subagent. \
         Pre-fix `FabricTaskServiceAdapter::spawn_subagent` emits the \
         SubagentSpawned event but never writes this default, so \
         `drive_run_iteration`'s `resolve_run_string_default(..., \"goal\")` \
         returns None and the child's LLM sees the generic \
         \"Execute the run objective.\" placeholder instead of the \
         delegation goal. Body: {}",
        r.text().await.unwrap_or_default(),
    );

    let body: Value = r.json().await.expect("defaults body json");
    assert_eq!(
        body.get("value").and_then(|v| v.as_str()),
        Some(DELEGATED_GOAL),
        "#700: child run's goal default must match the LLM's \
         `tool_args[\"goal\"]` verbatim. Body: {body}",
    );
}
