//! Issue #670 G1+G2 regression: the orchestrator's `spawn_subagent`
//! execute branch must emit `RuntimeEvent::SubagentSpawned` AND carry
//! the LLM's delegation context (`goal` + `role`) end-to-end to the
//! `subagent_spawns` projection.
//!
//! # The bug
//!
//! Baseline audit in `/tmp/issue-667-baseline.md` (filed as epic #670):
//! the front half of subagent spawning is wired from the LLM prompt
//! down to `TaskService::spawn_subagent`, but the back half is not.
//! Two concrete defects this test closes:
//!
//! 1. **G1** — `TaskService::spawn_subagent` never emits
//!    `RuntimeEvent::SubagentSpawned`. The `subagent_spawns`
//!    projection stays empty in production; every operator dashboard
//!    reading that read model shows zero spawn activity forever.
//!
//! 2. **G2** — the execute branch at `execute_impl.rs:1138-1172`
//!    mints a fresh child task id but silently drops the LLM's
//!    `proposal.tool_name` (role) and `proposal.tool_args["goal"]`
//!    (sub-goal). Even if spawn emitted an event, the context would
//!    be empty.
//!
//! # The test
//!
//! Boots `cairn-app` against a real Valkey-backed fabric + mock OpenAI
//! provider that returns a `spawn_subagent` JSON action on the first
//! iteration, then `complete_run` on the second so the orchestrator
//! loop terminates cleanly. Drives `POST /v1/runs/:id/orchestrate`
//! and then reads the `subagent_spawns` projection via the new
//! `GET /v1/runs/:id/subagent-spawns` endpoint.
//!
//! # Assertion
//!
//! Post-fix (with the G1+G2 commits applied):
//!   * exactly one spawn row exists
//!   * `goal == "analyze the failing test module"` (verbatim from the LLM)
//!   * `role == "researcher"` (verbatim from the LLM)
//!   * `parent_run_id` matches the orchestrate run
//!
//! Pre-fix (run this test on `main` at `d918be31` without this
//! branch's G1+G2 changes):
//!   * zero spawn rows
//!   * the assertion on `items.len() == 1` fires
//!
//! Verification ceremony lives in the PR body.

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

const MOCK_MODEL: &str = "openrouter/670-g1g2-regression";

/// The LLM proposal that drives this test. Shape matches
/// `build_system_prompt` in `decide_impl.rs` — `tool_name` carries
/// the role, `tool_args.goal` carries the delegated sub-goal.
const DELEGATED_GOAL: &str = "analyze the failing test module";
const DELEGATED_ROLE: &str = "researcher";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
}

/// Mock OpenAI-compatible chat completions endpoint. First call returns
/// `spawn_subagent`; every subsequent call returns `complete_run` so
/// the orchestrator loop terminates cleanly on the next iteration.
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
            // LLM proposes a subagent spawn. `tool_name` is the role,
            // `tool_args.goal` is the delegated sub-goal. Both must
            // round-trip through execute → TaskService → event →
            // projection verbatim (that's what G2 enforces).
            json!([{
                "action_type":       "spawn_subagent",
                "description":       "#670 G1+G2: delegate the analysis to a researcher",
                "tool_name":         DELEGATED_ROLE,
                "tool_args":         { "goal": DELEGATED_GOAL },
                "confidence":        0.95,
                "requires_approval": false,
            }])
        } else {
            // After the spawn lands the orchestrator returns 202 with
            // a child_task_id. If the test code re-orchestrates we
            // want the run to finish cleanly rather than dangling.
            json!([{
                "action_type":       "complete_run",
                "description":       "#670 regression: parent done after delegating",
                "confidence":        0.99,
                "requires_approval": false,
            }])
        };
        (
            StatusCode::OK,
            Json(json!({
                "id":      format!("mock-670-{n}"),
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

/// Provision tenant credential + connection + defaults + session + run
/// so orchestrate has everything it needs to reach the mock LLM.
async fn provision_run(h: &LiveHarness, mock_url: &str) -> (String, String) {
    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_670_{suffix}");
    let session_id = format!("sess_670_{suffix}");
    let run_id = format!("run_670_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id":     "openrouter",
            "plaintext_value": format!("sk-670-{suffix}"),
        }))
        .send()
        .await
        .expect("credential reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "credential: {}",
        r.text().await.unwrap_or_default()
    );
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
    assert_eq!(
        r.status().as_u16(),
        201,
        "connection: {}",
        r.text().await.unwrap_or_default()
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
            .expect("defaults reaches server");
        assert_eq!(
            r.status().as_u16(),
            200,
            "defaults {key}: {}",
            r.text().await.unwrap_or_default()
        );
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

    (session_id, run_id)
}

/// Regression test: LLM proposes `spawn_subagent` → execute extracts
/// goal+role → `TaskService::spawn_subagent` emits `SubagentSpawned`
/// → `subagent_spawns` projection has a row with the delegation
/// context.
#[tokio::test]
async fn llm_spawn_subagent_emits_event_with_goal_and_role() {
    let h = LiveHarness::setup().await;
    let (mock_url, hits) = spawn_mock().await;
    let (_session_id, run_id) = provision_run(&h, &mock_url).await;

    // Drive one orchestrate iteration. The mock returns
    // `spawn_subagent` on the first call; the handler executes it
    // (emitting `SubagentSpawned`) and returns 202 with child_task_id.
    let r = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "#670 parent run goal",
            "max_iterations": 1,
        }))
        .send()
        .await
        .expect("orchestrate reaches server");
    let status = r.status().as_u16();
    let body: Value = r.json().await.unwrap_or(Value::Null);
    assert!(
        status == 200 || status == 202,
        "orchestrate must succeed on spawn_subagent proposal; status={status} body={body}",
    );
    assert!(
        hits.load(Ordering::SeqCst) >= 1,
        "mock LLM must have been called at least once",
    );

    // Read back the subagent-spawns audit row via the new #670 endpoint.
    let r = h
        .client()
        .get(format!("{}/v1/runs/{}/subagent-spawns", h.base_url, run_id,))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("list subagent-spawns reaches server");
    assert_eq!(
        r.status().as_u16(),
        200,
        "GET /v1/runs/:id/subagent-spawns: {}",
        r.text().await.unwrap_or_default()
    );

    let body: Value = r.json().await.expect("subagent-spawns json");
    let items = body
        .get("items")
        .and_then(|v| v.as_array())
        .expect("subagent-spawns response has items array");

    // Core assertion: exactly one row. Pre-fix this is zero because
    // `TaskService::spawn_subagent` never emits `SubagentSpawned`
    // and the projection stays empty.
    assert_eq!(
        items.len(),
        1,
        "#670 G1: subagent_spawns projection must have exactly one row \
         after the LLM proposes spawn_subagent. Observed {} rows. \
         Pre-fix this test fails with 0 rows because \
         TaskService::spawn_subagent never emits the event. \
         Full response: {body}",
        items.len(),
    );

    let row = &items[0];

    // G2: the goal must round-trip verbatim from the LLM proposal.
    assert_eq!(
        row.get("goal").and_then(|v| v.as_str()),
        Some(DELEGATED_GOAL),
        "#670 G2: subagent_spawns.goal must match the LLM's \
         tool_args[\"goal\"] verbatim. Pre-fix execute drops this \
         string before TaskService sees it. Row: {row}",
    );

    // G2: the role must round-trip from the LLM proposal.
    assert_eq!(
        row.get("role").and_then(|v| v.as_str()),
        Some(DELEGATED_ROLE),
        "#670 G2: subagent_spawns.role must match the LLM's tool_name \
         verbatim. Pre-fix execute drops this string before TaskService \
         sees it. Row: {row}",
    );

    // Setup invariant: the spawn's parent_run_id matches the run we
    // orchestrated. If this ever fails the test is proving the wrong
    // thing — likely a tenant-scope bug, not the #670 gap.
    assert_eq!(
        row.get("parent_run_id").and_then(|v| v.as_str()),
        Some(run_id.as_str()),
        "#670 setup invariant: parent_run_id must match orchestrate run. \
         Row: {row}",
    );

    // Setup invariant: child task id must be a non-empty string minted
    // by the execute layer.
    let child_task_id = row
        .get("child_task_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        !child_task_id.is_empty(),
        "#670 setup invariant: child_task_id must be populated. Row: {row}",
    );
}
