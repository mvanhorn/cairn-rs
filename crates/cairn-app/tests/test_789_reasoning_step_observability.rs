//! #789 regression: per-iteration reasoning step observability.
//!
//! Verifies the full flow:
//!   1. Mock LLM returns a `bash` tool call.
//!   2. POST /orchestrate runs one iteration, suspending on
//!      approval.
//!   3. `record_reasoning_step` emits `RunReasoningStepRecorded`
//!      after the DECIDE callback.
//!   4. The InMemoryStore projection apply pushes the record onto
//!      the per-run vec.
//!   5. `GET /v1/runs/:id/trajectory` returns the step.
//!   6. `GET /v1/admin/agents/live` lists the active run with its
//!      current_action populated from the latest step.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

const MOCK_MODEL: &str = "openrouter/789-mock";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
}

async fn mock_chat_handler(
    State(state): State<MockState>,
    Json(_body): Json<Value>,
) -> Json<Value> {
    let n = state.hits.fetch_add(1, Ordering::SeqCst);
    Json(json!({
        "id": format!("chatcmpl-{n}"),
        "object": "chat.completion",
        "created": 0,
        "model": MOCK_MODEL,
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "I'm thinking through the goal carefully. Let me start by inspecting the repository state to understand what's there.",
                "tool_calls": [{
                    "id": format!("tc_789_{n}"),
                    "type": "function",
                    "function": {
                        "name": "bash",
                        "arguments": json!({ "command": format!("echo iteration-{n}"), "description": "exploring" }).to_string(),
                    },
                }],
            },
            "finish_reason": "tool_calls",
        }],
        "usage": {"prompt_tokens": 10, "completion_tokens": 30, "total_tokens": 40},
    }))
}

async fn spawn_mock() -> (String, Arc<AtomicUsize>) {
    let state = MockState {
        hits: Arc::new(AtomicUsize::new(0)),
    };
    let hits = state.hits.clone();
    let app = Router::new()
        .route("/chat/completions", post(mock_chat_handler))
        .route("/v1/chat/completions", post(mock_chat_handler))
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
    tokio::time::sleep(Duration::from_millis(25)).await;
    (format!("http://{addr}"), hits)
}

#[tokio::test]
async fn trajectory_endpoint_returns_reasoning_step_after_decide() {
    let h = LiveHarness::setup().await;
    let (mock_url, hits) = spawn_mock().await;

    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_789_{suffix}");
    let session_id = format!("sess_789_{suffix}");
    let run_id = format!("run_789_{suffix}");

    // Bootstrap.
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id":     "openrouter",
            "plaintext_value": format!("sk-789-{suffix}"),
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
            .expect("defaults reach server");
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

    // #806: orchestrator no longer carries `bash` in its tool
    // allowlist — workspace inspection now spawns a status-checker.
    // The mock LLM in this test returns a `bash` tool call, which
    // the orchestrator role would now refuse to surface. Pin the run's
    // agent_role to `status-checker` (which keeps bash for read-only
    // inspection) so the trajectory shape this test is exercising
    // continues to render.
    let r = h
        .client()
        .put(format!(
            "{}/v1/settings/defaults/tenant/{}/run:{}:agent_role",
            h.base_url, tenant, run_id,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "value": "status-checker" }))
        .send()
        .await
        .expect("run-scoped agent_role default reaches server");
    assert_eq!(r.status().as_u16(), 200);

    // POST /orchestrate to drive one DECIDE iteration.
    let _ = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal":           "exercise #789 reasoning-step capture",
            "max_iterations": 1,
        }))
        .send()
        .await;

    // Wait briefly for the DECIDE callback to land + emit the
    // RunReasoningStep event.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut trajectory_items: Vec<Value> = vec![];
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let r = h
            .client()
            .get(format!("{}/v1/runs/{}/trajectory", h.base_url, run_id))
            .bearer_auth(&h.admin_token)
            .send()
            .await
            .expect("trajectory endpoint reachable");
        if r.status().as_u16() == 200 {
            let body = r.json::<Value>().await.unwrap_or(Value::Null);
            trajectory_items = body
                .get("items")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            if !trajectory_items.is_empty() {
                break;
            }
        }
    }

    assert!(
        !trajectory_items.is_empty(),
        "#789: trajectory should have at least 1 reasoning step after the first DECIDE; mock hits={}",
        hits.load(Ordering::SeqCst),
    );

    let first = &trajectory_items[0];
    let reasoning_compact = first
        .get("reasoning_compact")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        reasoning_compact.contains("thinking through") || reasoning_compact.contains("inspecting"),
        "reasoning_compact should contain the model's chain-of-thought; got: {reasoning_compact:?}",
    );
    let proposed = first
        .get("proposed_action")
        .expect("proposed_action present");
    assert_eq!(
        proposed.get("kind").and_then(|v| v.as_str()),
        Some("tool_call"),
        "top proposal should be a tool_call",
    );
    assert_eq!(
        proposed.get("tool_name").and_then(|v| v.as_str()),
        Some("bash"),
        "top proposal tool_name should be bash",
    );

    // #805: trajectory items must surface `proposal_count` so
    // operators can detect multi-proposal iterations (parallel tool
    // batches) where `proposed_action` is the top-1 view. The mock
    // returns a single tool_call per response so count should be 1.
    let proposal_count = first.get("proposal_count").and_then(|v| v.as_u64());
    assert_eq!(
        proposal_count,
        Some(1),
        "#805: trajectory must surface `proposal_count` for the iteration; got {proposal_count:?}",
    );

    // Live agents endpoint should list the run with current_action
    // populated.
    let r = h
        .client()
        .get(format!("{}/v1/admin/agents/live", h.base_url))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("live agents endpoint reachable");
    assert_eq!(r.status().as_u16(), 200);
    let body = r.json::<Value>().await.unwrap_or(Value::Null);
    let agents = body
        .get("agents")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let our_agent = agents
        .iter()
        .find(|a| a.get("run_id").and_then(|v| v.as_str()) == Some(&run_id));
    assert!(
        our_agent.is_some(),
        "live agents list should include our active run; got {} agents",
        agents.len(),
    );
    let agent = our_agent.unwrap();
    let current_action = agent.get("current_action").expect("current_action present");
    assert_eq!(
        current_action.get("kind").and_then(|v| v.as_str()),
        Some("tool_call"),
    );
    assert_eq!(
        current_action.get("tool_name").and_then(|v| v.as_str()),
        Some("bash"),
    );
}
