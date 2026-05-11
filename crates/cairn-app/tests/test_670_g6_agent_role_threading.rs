//! #670 G6: `agent_role_id` threading into spawned child runs.
//!
//! The LLM's `spawn_subagent` action proposal carries `tool_name`
//! as the delegated role (e.g. `"researcher"`, `"executor"`). G6
//! threads that value through:
//!
//!   `ActionProposal.tool_name`
//!     → `TaskService::spawn_subagent(..., role: String)`
//!     → `FabricRunService::start_with_role(..., agent_role_id)`
//!     → `BridgeEvent::ExecutionCreated.agent_role_id`
//!     → `RuntimeEvent::RunCreated.agent_role_id`
//!     → `runs.agent_role_id` projection column
//!     → `RunRecord.agent_role_id` on the JSON surface
//!
//! Pre-G6 the adapter dropped `role` on the floor at the first
//! edge: the child's RunRecord was created with `agent_role_id =
//! None` and the child's orchestrator loop defaulted to
//! `"orchestrator"` on every spawn — regardless of what the parent
//! LLM delegated.
//!
//! # Test
//!
//! Boots cairn-app against a mock LLM. Parent's first turn returns
//! `spawn_subagent` with `tool_name: "researcher"`. Then asserts:
//!
//! 1. `GET /v1/runs/:parent/children` returns exactly one child.
//! 2. The child's `agent_role_id` field equals `"researcher"`.
//!
//! Assertion 2 is the G6 contract. Pre-fix `agent_role_id` is null.

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

const MOCK_MODEL: &str = "openrouter/670-g6-agent-role";
const DELEGATED_ROLE: &str = "researcher";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
}

async fn spawn_mock() -> String {
    let state = MockState {
        hits: Arc::new(AtomicUsize::new(0)),
    };

    async fn chat_handler(
        State(state): State<MockState>,
        Json(_body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        let n = state.hits.fetch_add(1, Ordering::SeqCst);
        let content = if n == 0 {
            // Parent's first turn: spawn_subagent with role=researcher.
            json!([{
                "action_type":       "spawn_subagent",
                "description":       "#670 G6: delegate to a researcher",
                "tool_name":         DELEGATED_ROLE,
                "tool_args":         { "goal": "G6 delegation" },
                "confidence":        0.95,
                "requires_approval": false,
            }])
        } else {
            // Fallback: any other turn completes the run. Keeps the
            // parent's orchestrate POST from hanging if the loop
            // iterates past the spawn (G5 auto-resume uses this path
            // too).
            json!([{
                "action_type":       "complete_run",
                "description":       "#670 G6: parent done",
                "confidence":        0.99,
                "requires_approval": false,
            }])
        };
        (
            StatusCode::OK,
            Json(json!({
                "id":      format!("mock-g6-{n}"),
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
    format!("http://{addr}")
}

async fn provision_run(h: &LiveHarness, mock_url: &str) -> String {
    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_g6_{suffix}");
    let session_id = format!("sess_g6_{suffix}");
    let run_id = format!("run_g6_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id":     "openrouter",
            "plaintext_value": format!("sk-g6-{suffix}"),
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
    assert_eq!(r.status().as_u16(), 201);

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
            .expect("defaults reaches server");
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

    run_id
}

/// Regression test: `spawn_subagent` from the LLM passes the
/// delegated role as `agent_role_id` on the child's RunRecord.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn llm_spawn_threads_role_onto_child_run_record() {
    // G5 auto-resume is harmless here but not required; the test
    // drives the parent to completion via the mock's fallback
    // branch. Disable the child-run driver explicitly so the child's
    // lifecycle doesn't race the parent's children-list read.
    std::env::set_var("CAIRN_CHILD_RUN_DRIVER_ENABLED", "false");

    let h = LiveHarness::setup().await;
    let mock_url = spawn_mock().await;
    let parent_run_id = provision_run(&h, &mock_url).await;

    // Drive one parent orchestrate iteration. Mock returns
    // spawn_subagent on call 0; the adapter creates the child
    // RunRecord via start_with_role threading role="researcher".
    let r = h
        .client()
        .post(format!(
            "{}/v1/runs/{}/orchestrate",
            h.base_url, parent_run_id
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "G6 parent goal",
            "max_iterations": 1,
        }))
        .send()
        .await
        .expect("orchestrate reaches server");
    let status = r.status().as_u16();
    assert!(
        status == 200 || status == 202,
        "parent orchestrate must succeed; status={status} body={}",
        r.text().await.unwrap_or_default(),
    );

    // Assertion 1: child count.
    let r = h
        .client()
        .get(format!("{}/v1/runs/{}/children", h.base_url, parent_run_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("children reaches server");
    assert_eq!(r.status().as_u16(), 200);
    let body: Value = r.json().await.expect("children json");
    let children = body
        .get("items")
        .and_then(|v| v.as_array())
        .expect("children response has items array");
    assert_eq!(
        children.len(),
        1,
        "expected exactly one child after spawn_subagent; body={body}",
    );

    // Assertion 2 (G6 contract): child's agent_role_id equals the
    // delegated role. Pre-G6 this field is null — the fabric path
    // dropped the role argument on the floor. Post-G6 the
    // FabricTaskServiceAdapter::spawn_subagent path calls
    // `fabric.runs.start_with_role(..., Some(role))` which threads
    // through BridgeEvent::ExecutionCreated → RunCreated →
    // RunRecord.agent_role_id.
    let child = &children[0];
    let observed = child.get("agent_role_id").and_then(|v| v.as_str());
    assert_eq!(
        observed,
        Some(DELEGATED_ROLE),
        "#670 G6: child's agent_role_id must match the parent's delegated role. \
         expected={DELEGATED_ROLE:?} observed={observed:?} full child={child}",
    );
}
