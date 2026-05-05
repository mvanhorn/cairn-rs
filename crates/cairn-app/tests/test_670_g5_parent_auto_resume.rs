//! #670 G5: parent auto-resume on subagent completion.
//!
//! End-to-end contract. The LLM proposes `spawn_subagent` → parent's
//! execute phase calls `FabricTaskServiceAdapter::spawn_subagent`
//! which (a) creates the child RunRecord, (b) increments the root's
//! descendant counter, (c) suspends the parent via
//! `RunService::enter_waiting_subagent` on the `child_completed:
//! <child_task_id>` waitpoint BEFORE the method returns (closes the
//! signal-before-suspend race). Parent's orchestrate loop yields
//! `LoopTermination::WaitingSubagent` and HTTP responds 202.
//!
//! The `ChildRunDriver` tick picks up the Pending child and runs it
//! through `drive_run_iteration`. Child's mock returns complete_run;
//! child reaches `Completed`; `RunService::complete` fires the
//! `deliver_child_completed_signal` + schedules the parent's
//! auto-resume via `ParentAutoResume`. Parent's FF execution resumes;
//! the cairn-app-side callback re-invokes `drive_run_iteration` on
//! the parent; parent's mock returns complete_run; parent reaches
//! `Completed`.
//!
//! Assertion: the parent ends in `Completed` state WITHOUT the
//! operator (this test) ever POSTing a second `/orchestrate`. Pre-G5
//! the parent would stall in `waiting_dependency` forever.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

const MOCK_MODEL: &str = "openrouter/670-g5-parent-auto-resume";
const DELEGATED_ROLE: &str = "researcher";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
    child_calls: Arc<AtomicUsize>,
}

async fn chat_handler(
    State(state): State<MockState>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let n = state.hits.fetch_add(1, Ordering::SeqCst);
    let is_child_prompt = body.to_string().contains("run_subagent_");
    let content = if is_child_prompt {
        state.child_calls.fetch_add(1, Ordering::SeqCst);
        // Child always completes on its first orchestrator turn.
        json!([{
            "action_type":       "complete_run",
            "description":       "G5 child complete",
            "confidence":        0.99,
            "requires_approval": false,
        }])
    } else if n == 0 {
        // Parent's first turn: spawn_subagent.
        json!([{
            "action_type":       "spawn_subagent",
            "description":       "G5 parent delegate",
            "tool_name":         DELEGATED_ROLE,
            "tool_args":         { "goal": "G5 child goal" },
            "confidence":        0.95,
            "requires_approval": false,
        }])
    } else {
        // Parent's subsequent turns (post-resume): complete.
        json!([{
            "action_type":       "complete_run",
            "description":       "G5 parent complete",
            "confidence":        0.99,
            "requires_approval": false,
        }])
    };
    (
        StatusCode::OK,
        Json(json!({
            "id":      format!("mock-g5-{n}"),
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

async fn spawn_mock() -> (String, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let state = MockState {
        hits: Arc::new(AtomicUsize::new(0)),
        child_calls: Arc::new(AtomicUsize::new(0)),
    };
    let hits = state.hits.clone();
    let child_calls = state.child_calls.clone();
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
    (format!("http://{addr}"), hits, child_calls)
}

async fn provision_run(h: &LiveHarness, mock_url: &str) -> String {
    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_g5_{suffix}");
    let session_id = format!("sess_g5_{suffix}");
    let run_id = format!("run_g5_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id":     "openrouter",
            "plaintext_value": format!("sk-g5-{suffix}"),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parent_auto_resumes_after_child_completes() {
    // Driver default-on; no opt-out in scope.
    std::env::remove_var("CAIRN_CHILD_RUN_DRIVER_ENABLED");

    let harness = LiveHarness::setup().await;
    let (mock_url, hits, child_calls) = spawn_mock().await;
    let parent_run_id = provision_run(&harness, &mock_url).await;

    // Drive one parent orchestrate. Mock returns spawn_subagent on
    // call 0. The adapter creates the child, suspends the parent on
    // `child_completed:<child_task_id>`, and the loop terminates with
    // WaitingSubagent. HTTP returns 202. At this point the parent is
    // in `waiting_dependency`.
    let r = harness
        .client()
        .post(format!(
            "{}/v1/runs/{}/orchestrate",
            harness.base_url, parent_run_id
        ))
        .bearer_auth(&harness.admin_token)
        .json(&json!({
            "goal": "G5 parent goal",
            "max_iterations": 3,
        }))
        .send()
        .await
        .expect("parent orchestrate reaches server");
    let status = r.status().as_u16();
    assert!(
        status == 200 || status == 202,
        "parent orchestrate must succeed; status={status} body={}",
        r.text().await.unwrap_or_default(),
    );

    // Poll parent until it reaches `completed`. The end-to-end chain:
    //
    //   1. Driver ticks (500ms) → picks up child (Pending).
    //   2. Child's drive_run_iteration → mock complete_run → Completed.
    //   3. RunService::complete on child fires the terminal hook →
    //      spawned task delivers `child_completed:<task>` signal.
    //   4. Parent's FF execution resumes; ParentAutoResume callback
    //      calls drive_run_iteration(parent).
    //   5. Parent's mock returns complete_run → parent Completed.
    //
    // 12s deadline: 1s for driver pickup + 1s mock round-trip + 2s
    // signal delivery + 1s parent auto-resume round-trip + CI slack.
    let deadline = Duration::from_secs(12);
    let start = Instant::now();
    let final_state = loop {
        let r = harness
            .client()
            .get(format!("{}/v1/runs/{}", harness.base_url, parent_run_id))
            .bearer_auth(&harness.admin_token)
            .send()
            .await
            .expect("parent get reaches server");
        let body: Value = r.json().await.unwrap();
        let state = body
            .get("run")
            .and_then(|r| r.get("state"))
            .and_then(|s| s.as_str())
            .unwrap_or("<missing>")
            .to_owned();
        if matches!(state.as_str(), "completed" | "failed" | "canceled") {
            break state;
        }
        if start.elapsed() > deadline {
            panic!(
                "parent did not reach terminal state within {deadline:?}; \
                 last state={state}. Full chain is broken: enter_waiting_subagent, \
                 child terminal hook, signal delivery, or parent auto-resume \
                 callback — check G5 logs.\n\
                 parent body={body}",
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };

    assert_eq!(
        final_state, "completed",
        "parent must complete via G5 auto-resume, not fail; observed={final_state}",
    );

    // Assert mock call counts: parent call 0 (spawn_subagent) + parent
    // call 1+ (post-resume complete_run) = at least 2 parent calls;
    // child call = exactly 1.
    let total_hits = hits.load(Ordering::SeqCst);
    let total_child = child_calls.load(Ordering::SeqCst);
    assert!(
        total_hits >= 2,
        "mock must have at least 2 parent calls (spawn + post-resume); got={total_hits}",
    );
    assert_eq!(
        total_child, 1,
        "mock must have exactly 1 child call; got={total_child}",
    );

    // Also assert the child reached completed (terminal hook fired
    // from a Completed child, not a Failed one — important because
    // Failed goes through a different terminal FCALL).
    let r = harness
        .client()
        .get(format!(
            "{}/v1/runs/{}/children",
            harness.base_url, parent_run_id
        ))
        .bearer_auth(&harness.admin_token)
        .send()
        .await
        .expect("children reaches server");
    assert_eq!(r.status().as_u16(), 200);
    let children: Value = r.json().await.unwrap();
    let items = children
        .get("items")
        .and_then(|v| v.as_array())
        .expect("children returns {items: [...]}");
    assert_eq!(
        items.len(),
        1,
        "expected exactly one child; body={children}"
    );
    let child_state = items[0]
        .get("state")
        .and_then(|s| s.as_str())
        .unwrap_or("<missing>");
    assert_eq!(
        child_state, "completed",
        "child must have reached completed (G5 terminal hook for Completed path); \
         observed={child_state}",
    );
}
