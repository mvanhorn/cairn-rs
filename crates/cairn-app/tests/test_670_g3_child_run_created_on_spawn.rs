//! Issue #670 G3 regression: when the orchestrator's
//! `spawn_subagent` execute branch fires, a real child `RunRecord`
//! must be created at spawn time.
//!
//! # The bug
//!
//! Before this PR (post-G1+G2): the execute layer passed
//! `child_run_id: None` through to
//! `TaskService::spawn_subagent`. The `FabricTaskServiceAdapter`
//! override emitted `SubagentSpawned` with `child_run_id: None`
//! and never created a child `RunRecord`. Consequence:
//!
//! * `GET /v1/runs/:parent/children` returned `[]` — operator UIs
//!   looking for the child saw nothing.
//! * `subagent_spawns.child_run_id` was always NULL — audit rows
//!   pointed at a run that didn't exist.
//!
//! # The fix (G3)
//!
//! * `execute_impl.rs` mints `run_subagent_<child_task_id>` and
//!   passes it to `TaskService::spawn_subagent`.
//! * `FabricTaskServiceAdapter::spawn_subagent` calls
//!   `fabric.runs.start(...)` BEFORE submitting the task + emitting
//!   `SubagentSpawned`, so the child `RunRecord` exists before any
//!   downstream row references it.
//! * `SubagentSpawned.child_run_id` now carries the real id.
//!
//! # The test
//!
//! Boots `cairn-app` against a mock LLM that returns a
//! `spawn_subagent` proposal on iteration 1. Drives orchestrate,
//! then asserts:
//!
//! 1. `GET /v1/runs/<parent>/children` returns exactly one child.
//! 2. The child's `parent_run_id` equals the orchestrate run id.
//! 3. The child's `session_id` equals the parent's (we co-locate
//!    children on the parent's session until G6 introduces proper
//!    child sessions).
//! 4. `GET /v1/runs/<parent>/subagent-spawns` returns one row whose
//!    `child_run_id` matches the row in `/children`.
//!
//! Pre-fix: assertion 1 fails (empty `children` list) OR assertion 4
//! fails (the spawn row's `child_run_id` is null/missing). Post-fix
//! all four pass. Verified via stash-test-pop ceremony in the PR body.

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

const MOCK_MODEL: &str = "openrouter/670-g3-child-runs";
const DELEGATED_GOAL: &str = "G3 child run creation smoke test";
const DELEGATED_ROLE: &str = "researcher";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
}

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
                "description":       "#670 G3: delegate to a researcher so a child RunRecord is created",
                "tool_name":         DELEGATED_ROLE,
                "tool_args":         { "goal": DELEGATED_GOAL },
                "confidence":        0.95,
                "requires_approval": false,
            }])
        } else {
            json!([{
                "action_type":       "complete_run",
                "description":       "#670 G3: parent done after delegating",
                "confidence":        0.99,
                "requires_approval": false,
            }])
        };
        (
            StatusCode::OK,
            Json(json!({
                "id":      format!("mock-670-g3-{n}"),
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

async fn provision_run(h: &LiveHarness, mock_url: &str) -> (String, String) {
    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_670g3_{suffix}");
    let session_id = format!("sess_670g3_{suffix}");
    let run_id = format!("run_670g3_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id":     "openrouter",
            "plaintext_value": format!("sk-670g3-{suffix}"),
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

    (session_id, run_id)
}

/// Regression test: `spawn_subagent` from the LLM creates a real
/// child `RunRecord` that's visible via `/children` and linked
/// from the spawn audit row.
#[tokio::test]
async fn llm_spawn_creates_child_run_and_links_to_spawn_row() {
    let h = LiveHarness::setup().await;
    let (mock_url, _hits) = spawn_mock().await;
    let (session_id, run_id) = provision_run(&h, &mock_url).await;

    // Drive one orchestrate iteration. Mock returns spawn_subagent
    // on call 1; the execute layer mints a child_run_id and the
    // adapter creates the child RunRecord.
    let r = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "#670 G3 parent run goal",
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

    // Assertion 1: GET /v1/runs/:parent/children returns exactly one
    // child. Pre-fix this returns an empty list because
    // spawn_subagent passed child_run_id=None and never created a
    // RunRecord.
    let r = h
        .client()
        .get(format!("{}/v1/runs/{}/children", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("list children reaches server");
    assert_eq!(
        r.status().as_u16(),
        200,
        "GET /children: {}",
        r.text().await.unwrap_or_default(),
    );
    let children_body: Value = r.json().await.expect("children json");
    let children = children_body
        .get("items")
        .and_then(|v| v.as_array())
        .expect("children response has items array");
    assert_eq!(
        children.len(),
        1,
        "#670 G3: parent must have exactly one child RunRecord after \
         spawn_subagent. Observed {} children. Pre-fix this is 0 because \
         the execute layer passed child_run_id=None. Full response: {children_body}",
        children.len(),
    );

    let child = &children[0];

    // Assertion 2: the child's parent_run_id matches the orchestrate
    // run id. Protects against a bug where the link is flipped or
    // cross-tenant.
    assert_eq!(
        child.get("parent_run_id").and_then(|v| v.as_str()),
        Some(run_id.as_str()),
        "#670 G3: child.parent_run_id must match the orchestrate run. Child: {child}",
    );

    // Assertion 3: child session_id matches the parent's. G6 will
    // introduce proper child sessions; for now we co-locate.
    assert_eq!(
        child.get("session_id").and_then(|v| v.as_str()),
        Some(session_id.as_str()),
        "#670 G3: child must co-locate on parent's session until G6. \
         Child: {child}",
    );

    let child_run_id = child
        .get("run_id")
        .and_then(|v| v.as_str())
        .expect("child has run_id")
        .to_owned();

    // Assertion 4: GET /v1/runs/:parent/subagent-spawns returns one
    // row whose `child_run_id` matches the run we just read from
    // /children. Pre-fix this is null (and #671 already shipped the
    // endpoint, so the row exists but with child_run_id=null).
    let r = h
        .client()
        .get(format!("{}/v1/runs/{}/subagent-spawns", h.base_url, run_id,))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("subagent-spawns reaches server");
    assert_eq!(r.status().as_u16(), 200);
    let spawns_body: Value = r.json().await.expect("spawns json");
    let spawns = spawns_body
        .get("items")
        .and_then(|v| v.as_array())
        .expect("spawns response has items array");
    assert_eq!(spawns.len(), 1, "exactly one subagent spawn row expected");
    let spawn_row = &spawns[0];
    assert_eq!(
        spawn_row.get("child_run_id").and_then(|v| v.as_str()),
        Some(child_run_id.as_str()),
        "#670 G3: subagent_spawns.child_run_id must be populated and \
         match the RunRecord in /children. Pre-fix this is null because \
         spawn_subagent never created a child run. Row: {spawn_row}",
    );
}
