//! #750: when a subagent exhausts every provider in the routed chain,
//! the child run must terminate `Failed(AllProvidersExhausted)` (not
//! suspend in `WaitingApproval` per #693 R3-B). Terminating fires G5's
//! `child_completed:<task_id>` signal with `success=false`, which
//! resumes the parent's `drive_run_iteration` so the parent can see the
//! failure in its `step_history` and decide what to do next (retry,
//! escalate, give up).
//!
//! Pre-fix symptom (R14b dogfood, 2026-05-08): child sat in
//! `waiting_approval`, parent sat in `waiting_dependency`, no progress
//! ever — the parent's only waitpoint shape is `child_completed`, which
//! G5 only fires on terminal states (`Completed` / `Failed` /
//! `Canceled`). `WaitingApproval` is a suspension, so the signal never
//! fires.
//!
//! Regression guard:
//! * Parent's mock: first turn → `spawn_subagent`; subsequent turns →
//!   `complete_run` (so once the parent observes the child's failure
//!   it can finish).
//! * Child's mock: every request returns HTTP 500. This walks the
//!   fallback chain and ends in `AllProvidersExhausted`.
//! * Assertion: parent reaches `completed`, child reaches `failed` with
//!   `failure_class=all_providers_exhausted`. Pre-fix the parent never
//!   leaves `waiting_dependency` — the deadline-bound poll loop trips
//!   with the symptom in the panic message.
//! * Assertion: child run did NOT get an `escalate_to_operator`
//!   approval card (we skip card submission for child runs because
//!   the card would be attached to a `Failed` run, which is misleading
//!   for operators). Top-level operator-initiated runs still get the
//!   card via `test_693_r3b_exhaustion_waiting_approval`.

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

const MOCK_MODEL_A: &str = "openrouter/750-child-exhaust-a";
const MOCK_MODEL_B: &str = "openrouter/750-child-exhaust-b";
const DELEGATED_ROLE: &str = "researcher";

#[derive(Clone)]
struct MockState {
    parent_calls: Arc<AtomicUsize>,
    child_calls: Arc<AtomicUsize>,
}

/// Routes every request to one of two paths:
/// * Child (system prompt contains "technical analyst" — researcher
///   role): respond 500 so the orchestrator walks the fallback chain
///   and ends in `AllProvidersExhausted`.
/// * Parent (orchestrator role): respond with `spawn_subagent` on
///   call 0, then `complete_run` on every subsequent call (parent's
///   resumed iteration after observing child failure should converge
///   on completion).
async fn chat_handler(
    State(state): State<MockState>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let body_text = body.to_string();
    let is_child_prompt = body_text.contains("technical analyst");

    if is_child_prompt {
        state.child_calls.fetch_add(1, Ordering::SeqCst);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": {
                    "message": "forced upstream 500 for #750 child-exhaust regression test",
                    "code": 500,
                }
            })),
        );
    }

    let n = state.parent_calls.fetch_add(1, Ordering::SeqCst);
    let content = if n == 0 {
        json!([{
            "action_type":       "spawn_subagent",
            "description":       "#750 parent delegate to researcher",
            "tool_name":         DELEGATED_ROLE,
            "tool_args":         { "goal": "#750 child goal that will hit providers-exhausted" },
            "confidence":        0.95,
            "requires_approval": false,
        }])
    } else {
        // Post-resume turn: complete. The parent's step_history now
        // contains the child's failure (visible to the LLM via the
        // subagent step record); the parent's "decision" here is
        // simply to finish — exact policy is out of scope for this
        // test, the contract being asserted is "parent gets to make a
        // decision at all".
        json!([{
            "action_type":       "complete_run",
            "description":       "#750 parent observed child-failed and completes",
            "confidence":        0.99,
            "requires_approval": false,
        }])
    };
    (
        StatusCode::OK,
        Json(json!({
            "id":      format!("mock-750-{n}"),
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
        parent_calls: Arc::new(AtomicUsize::new(0)),
        child_calls: Arc::new(AtomicUsize::new(0)),
    };
    let parent_calls = state.parent_calls.clone();
    let child_calls = state.child_calls.clone();
    let app = Router::new()
        .route("/chat/completions", post(chat_handler))
        .route("/v1/chat/completions", post(chat_handler))
        .route(
            "/v1/models",
            get(|| async {
                Json(json!({
                    "data": [
                        { "id": MOCK_MODEL_A },
                        { "id": MOCK_MODEL_B },
                    ]
                }))
            }),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(25)).await;
    (format!("http://{addr}"), parent_calls, child_calls)
}

async fn provision_run(h: &LiveHarness, mock_url: &str) -> String {
    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_750_{suffix}");
    let session_id = format!("sess_750_{suffix}");
    let run_id = format!("run_750_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id":     "openrouter",
            "plaintext_value": format!("sk-750-{suffix}"),
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
            "supported_models":       [MOCK_MODEL_A, MOCK_MODEL_B],
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
            .json(&json!({ "value": MOCK_MODEL_A }))
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

    run_id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_run_providers_exhausted_terminates_failed_and_resumes_parent() {
    // Driver default-on; no opt-out in scope.
    std::env::remove_var("CAIRN_CHILD_RUN_DRIVER_ENABLED");

    let harness = LiveHarness::setup().await;
    let (mock_url, parent_calls, child_calls) = spawn_mock().await;
    let parent_run_id = provision_run(&harness, &mock_url).await;

    // Drive one parent orchestrate. Mock returns spawn_subagent on call
    // 0. Adapter creates the child, suspends the parent on
    // `child_completed:<child_task_id>`, loop terminates with
    // WaitingSubagent. HTTP 200 or 202.
    let r = harness
        .client()
        .post(format!(
            "{}/v1/runs/{}/orchestrate",
            harness.base_url, parent_run_id
        ))
        .bearer_auth(&harness.admin_token)
        .json(&json!({
            "goal":           "#750 parent goal — observe child failure",
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

    // Poll parent until terminal. Pre-fix the parent never leaves
    // `waiting_dependency` because the child suspends in
    // `waiting_approval` and G5 never fires `child_completed`. Post-fix:
    //   1. Driver picks up Pending child.
    //   2. Child's drive_run_iteration → mock returns 500 → walks
    //      fallback chain → AllProvidersExhausted.
    //   3. (Fix) Child run flips Failed(AllProvidersExhausted).
    //   4. RunService::fail fires G5 child_completed signal,
    //      success=false.
    //   5. Parent's FF execution resumes; ParentAutoResume calls
    //      drive_run_iteration(parent).
    //   6. Parent's mock returns complete_run → parent Completed.
    //
    // 30s deadline: 2x the G5 happy-path budget. Provider exhaustion
    // walks every model and adds a few seconds of mock round-trips
    // before the terminal flip.
    let deadline = Duration::from_secs(30);
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
                "#750 regression: parent did not reach terminal within {deadline:?}; \
                 last state={state}. Pre-fix symptom is exactly this — child stuck in \
                 waiting_approval, parent stuck in waiting_dependency, no progress. \
                 Check that the child run was flipped to Failed(AllProvidersExhausted) \
                 and that G5 child_completed signal fired.\n\
                 parent body={body}",
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };

    assert_eq!(
        final_state, "completed",
        "parent must complete after observing child's providers-exhausted failure; \
         observed={final_state}",
    );

    // Sanity on call counts.
    let total_parent = parent_calls.load(Ordering::SeqCst);
    let total_child = child_calls.load(Ordering::SeqCst);
    assert!(
        total_parent >= 2,
        "parent mock must be called at least twice (spawn + post-resume complete); got={total_parent}",
    );
    assert!(
        total_child >= 1,
        "child mock must be called at least once (provider chain walk); got={total_child}",
    );

    // Child must have terminated `failed` with the new
    // AllProvidersExhausted class. Pre-fix the child was in
    // `waiting_approval`; post-fix it's `failed`.
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
    let child = &items[0];
    let child_state = child
        .get("state")
        .and_then(|s| s.as_str())
        .unwrap_or("<missing>");
    assert_eq!(
        child_state, "failed",
        "#750: child must terminate `failed` (not `waiting_approval`) so G5 fires \
         child_completed signal; observed={child_state}, body={child}",
    );
    let failure_class = child
        .get("failure_class")
        .and_then(|s| s.as_str())
        .unwrap_or("<missing>");
    assert_eq!(
        failure_class, "all_providers_exhausted",
        "#750: child failure_class must be `all_providers_exhausted` — distinguishes \
         this terminal from generic execution-error so operators can filter on it; \
         observed={failure_class}, body={child}",
    );

    // For child runs we skip the `escalate_to_operator` approval card —
    // the card is operator-actionable, but we just terminated the run,
    // so attaching a card to a `Failed` run is misleading. The card
    // path stays live for top-level (operator-initiated) runs, covered
    // by `test_693_r3b_exhaustion_waiting_approval`.
    let child_run_id = child
        .get("run_id")
        .and_then(|s| s.as_str())
        .expect("child has run_id");
    let r = harness
        .client()
        .get(format!(
            "{}/v1/approvals?state=pending&kind=tool_call&run_id={}",
            harness.base_url, child_run_id,
        ))
        .bearer_auth(&harness.admin_token)
        .send()
        .await
        .expect("approvals list reaches server");
    assert_eq!(r.status().as_u16(), 200);
    let approvals_body = r.json::<Value>().await.expect("approvals json");
    let items = approvals_body
        .get("items")
        .and_then(|v| v.as_array())
        .expect("items array");
    let has_card = items
        .iter()
        .any(|item| item.get("tool_name").and_then(|v| v.as_str()) == Some("escalate_to_operator"));
    assert!(
        !has_card,
        "#750: child run must NOT have an escalate_to_operator card (would be \
         attached to a Failed run, misleading for operators); body={approvals_body}",
    );
}
