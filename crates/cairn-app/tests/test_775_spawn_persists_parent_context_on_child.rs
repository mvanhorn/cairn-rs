//! #775 regression: `FabricTaskServiceAdapter::spawn_subagent` must
//! persist the LLM's optional `parent_context` onto the **child run's**
//! defaults so that `drive_run_iteration`'s
//! `resolve_run_string_default(..., "parent_context")` reads the value
//! into `OrchestrationContext.parent_context`, and `build_user_message`
//! renders the `## Parent context` section in the child's first DECIDE
//! prompt.
//!
//! # The bug this guards against
//!
//! Pre-fix the parent_context field flowed:
//! `tool_args` → `execute_impl` → `TaskService::spawn_subagent` →
//! `BridgeEvent::SubagentSpawned` → `RuntimeEvent::SubagentSpawned`
//! event log. But there was no read path back into the child's
//! `OrchestrationContext`. The orchestrate handler always set
//! `parent_context: None`, so the `## Parent context` section the
//! base prompt referenced never rendered. Feature looked complete
//! in unit tests (which inject the field directly into
//! `OrchestrationContext`) but was silently dead in production.
//!
//! # What this test asserts
//!
//! Same pattern as `test_700_spawn_persists_goal_on_child`: after
//! the LLM proposes `spawn_subagent` with `parent_context=X`, the
//! child run's `parent_context` default round-trips to `X` via the
//! HTTP settings endpoint. That's the exact read path
//! `resolve_run_string_default(..., "parent_context")` uses.
//!
//! Issue: https://github.com/avifenesh/cairn-rs/issues/775

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

const MOCK_MODEL: &str = "openrouter/775-parent-context-persistence";
const DELEGATED_GOAL: &str = "775-parent-context-test-goal";
const DELEGATED_ROLE: &str = "researcher";
const PARENT_CONTEXT: &str = "previous attempt looped on `gh auth status`; do not call it again — \
     the workspace at /tmp/cairn-runs/<run_id>/repo already has gh credentials";

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
                "description":       "#775 regression: spawn with parent_context set",
                "tool_name":         DELEGATED_ROLE,
                "tool_args":         {
                    "goal":           DELEGATED_GOAL,
                    "parent_context": PARENT_CONTEXT,
                },
                "confidence":        0.95,
                "requires_approval": false,
            }])
        } else {
            json!([{
                "action_type":       "complete_run",
                "description":       "parent post-spawn complete",
                "confidence":        0.99,
                "requires_approval": false,
            }])
        };
        (
            StatusCode::OK,
            Json(json!({
                "id": format!("775-mock-{}", n),
                "object": "chat.completion",
                "created": 1_710_000_000,
                "model": MOCK_MODEL,
                "choices": [{
                    "index":         0,
                    "message":       { "role": "assistant", "content": content.to_string() },
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
    (format!("http://{addr}"), hits)
}

async fn provision_run(h: &LiveHarness, mock_url: &str) -> (String, String, String) {
    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_775_{suffix}");
    let session_id = format!("sess_775_{suffix}");
    let run_id = format!("run_775_{suffix}");

    // Credential.
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id":     "openrouter",
            "plaintext_value": format!("sk-775-{suffix}"),
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

    // Connection.
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

    // Defaults.
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

    // Session + run.
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

    (session_id, run_id, project)
}

#[tokio::test]
async fn spawn_subagent_persists_parent_context_on_child_run_default() {
    let h = LiveHarness::setup().await;
    let (_session_id, parent_run_id, project_id) = {
        let (mock_url, _hits) = spawn_mock().await;
        provision_run(&h, &mock_url).await
    };

    // Drive one orchestrate iteration. The parent's mock returns
    // spawn_subagent with parent_context set on the first call.
    let r = h
        .client()
        .post(format!(
            "{}/v1/runs/{}/orchestrate",
            h.base_url, parent_run_id
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "#775 parent run goal",
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

    // Find the child run id.
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

    // ── Core #775 assertion ──────────────────────────────────────
    // The child's `parent_context` default must be populated with
    // the LLM's parent_context verbatim. Key format mirrors the
    // goal-persistence pattern: `run:<child_run_id>:parent_context`,
    // scoped to the parent's project. This is the read path
    // `resolve_run_string_default(..., "parent_context")` uses, so
    // landing this default proves the child's first DECIDE prompt
    // will include the `## Parent context` section.
    //
    // Pre-fix this 404s — fabric_adapter::spawn_subagent emitted
    // the SubagentSpawned event with parent_context but never
    // persisted it to defaults, so the child's
    // OrchestrationContext.parent_context was always None and the
    // base prompt's `## Parent context` reference was inert in
    // production.
    let key = format!("run:{child_run_id}:parent_context");
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
        "#775: child run's `parent_context` default MUST exist after \
         spawn_subagent when the LLM provided one. Pre-fix the adapter \
         emitted the SubagentSpawned event but never wrote the default, \
         so OrchestrationContext.parent_context was always None and the \
         `## Parent context` section never rendered in the child prompt. \
         Body: {}",
        r.text().await.unwrap_or_default(),
    );

    let body: Value = r.json().await.expect("defaults body json");
    let value = body
        .get("value")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    // #813: post-fix, the runtime auto-prepends a `Workspace path: <abs>`
    // header to the child's parent_context so sub-agents know where to
    // `cd` on their first DECIDE without a discovery round-trip. The
    // LLM-supplied parent_context follows. Pre-#813 the child's
    // parent_context was the LLM's string verbatim; post-#813 we
    // assert the LLM's string is CONTAINED (so the LLM intent is
    // preserved) AND the workspace_line prefix is present.
    assert!(
        value.contains(PARENT_CONTEXT),
        "#775 + #813: child run's parent_context must contain the LLM's \
         `tool_args[\"parent_context\"]` verbatim. value={value}",
    );
    assert!(
        value.contains("Workspace path:"),
        "#813: child run's parent_context must auto-include the \
         resolved workspace path so sub-agents skip the discovery \
         loop on their first DECIDE. value={value}",
    );
}

#[tokio::test]
async fn spawn_subagent_omits_parent_context_default_when_unset() {
    // Parallel test — when the LLM does NOT supply parent_context,
    // no default row is written. Ensures the persistence path is
    // gated by `Some(_)` and we don't pollute the projection with
    // empty rows for every spawn.

    let h = LiveHarness::setup().await;

    // Mock returns spawn_subagent WITHOUT parent_context.
    let state = MockState {
        hits: Arc::new(AtomicUsize::new(0)),
    };
    let _hits = state.hits.clone();
    async fn chat_no_pc(
        State(state): State<MockState>,
        Json(_body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        let n = state.hits.fetch_add(1, Ordering::SeqCst);
        let content = if n == 0 {
            json!([{
                "action_type":       "spawn_subagent",
                "description":       "#775: spawn without parent_context",
                "tool_name":         DELEGATED_ROLE,
                "tool_args":         { "goal": DELEGATED_GOAL },
                "confidence":        0.95,
                "requires_approval": false,
            }])
        } else {
            json!([{
                "action_type":       "complete_run",
                "description":       "parent post-spawn complete",
                "confidence":        0.99,
                "requires_approval": false,
            }])
        };
        (
            StatusCode::OK,
            Json(json!({
                "id": format!("775b-mock-{}", n),
                "object": "chat.completion",
                "created": 1_710_000_000,
                "model": MOCK_MODEL,
                "choices": [{
                    "index":         0,
                    "message":       { "role": "assistant", "content": content.to_string() },
                    "finish_reason": "stop",
                }],
                "usage": { "prompt_tokens": 10, "completion_tokens": 6, "total_tokens": 16 },
            })),
        )
    }
    let app = Router::new()
        .route("/chat/completions", post(chat_no_pc))
        .route("/v1/chat/completions", post(chat_no_pc))
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
    let mock_url = format!("http://{addr}");

    let (_session_id, parent_run_id, project_id) = provision_run(&h, &mock_url).await;

    let r = h
        .client()
        .post(format!(
            "{}/v1/runs/{}/orchestrate",
            h.base_url, parent_run_id
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "#775b parent run goal",
            "max_iterations": 1,
        }))
        .send()
        .await
        .expect("orchestrate reaches server");
    let status = r.status().as_u16();
    assert!(status == 200 || status == 202);

    let r = h
        .client()
        .get(format!("{}/v1/runs/{}/children", h.base_url, parent_run_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("children reaches server");
    let body: Value = r.json().await.unwrap();
    let items = body
        .get("items")
        .and_then(|v| v.as_array())
        .expect("children");
    assert_eq!(items.len(), 1);
    let child_run_id = items[0]
        .get("run_id")
        .and_then(|v| v.as_str())
        .expect("child has run_id");

    // #775 + #813: pre-#813 this test asserted 404 — the persistence
    // path was gated on `Some(_)` so a spawn without LLM-supplied
    // parent_context wrote no row. Post-#813 the runtime ALWAYS
    // injects a `Workspace path: <abs>` header into the child's
    // parent_context so sub-agents skip the discovery loop on their
    // first DECIDE. The row exists; its content is the workspace
    // line alone (no LLM-supplied tail).
    let key = format!("run:{child_run_id}:parent_context");
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
        "#813: parent_context is auto-populated with the workspace \
         path on every spawn, even when the LLM omits parent_context. \
         A 404 here means the workspace-path auto-inject regressed. \
         Body: {}",
        r.text().await.unwrap_or_default(),
    );
    let body: Value = r.json().await.expect("defaults body json");
    let value = body
        .get("value")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(
        value.contains("Workspace path:"),
        "#813: parent_context must contain the workspace_line when \
         the LLM omits parent_context (auto-inject is the only \
         source). value={value}",
    );
}
