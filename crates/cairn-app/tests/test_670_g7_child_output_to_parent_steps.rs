//! #670 G7: surface subagent child's `completion_summary` onto the
//! parent's `step_history` so the parent's next decide turn can see
//! what the child actually accomplished.
//!
//! # Scenario
//!
//! 1. Parent spawns a child (`spawn_subagent`, G3 + G6).
//! 2. Parent suspends on `child_completed:<task_id>` (G5).
//! 3. Child runs a single iteration and terminates with a
//!    `completion_summary` set via `complete_run`'s description
//!    field — the orchestrator's `LoopTermination::Completed` path
//!    emits `RunCompletionAnnotated` whose `completion_summary`
//!    lands on the child's `RunRecord`.
//! 4. G5 auto-resume re-drives the parent's `drive_run_iteration`.
//!    BEFORE the parent's next decide turn runs, the G7 seed-step
//!    helper prepends a `StepSummary { action_kind: "subagent_complete",
//!    summary: "child <id> (completed): <summary>" }` into
//!    `ctx.step_history`.
//! 5. The orchestrator's `decide_impl::build_user_prompt` renders
//!    the parent's step_history into the LLM prompt's user-message
//!    section (iteration-tagged, newest-first).
//!
//! # Assertion
//!
//! The parent's post-resume LLM call contains the child's
//! completion_summary string in its prompt. Captured by a mock that
//! records every `/chat/completions` request body and greps it
//! for the summary text.
//!
//! Pre-G7 the step_history is always empty at
//! `drive_run_iteration` entry, so the summary text never lands in
//! the prompt — the assertion fails with "not found in any
//! recorded prompt".

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

const MOCK_MODEL: &str = "openrouter/670-g7-child-output";
const CHILD_SUMMARY_MARKER: &str = "G7-CHILD-MARKER-8f3a1e: researcher found the signal";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
    recorded: Arc<Mutex<Vec<String>>>,
}

async fn chat_handler(
    State(state): State<MockState>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let n = state.hits.fetch_add(1, Ordering::SeqCst);
    let body_text = body.to_string();
    // Record the inbound request body verbatim. We grep this later
    // for the child-summary marker to prove G7 wired the child's
    // completion_summary into the parent's post-resume prompt.
    state.recorded.lock().unwrap().push(body_text.clone());

    // Distinguish parent vs child by role: the parent uses the
    // orchestrator role ("senior engineer") and the child uses the
    // researcher role ("technical analyst"). Matching the run-id
    // substring (the G5 test's approach) is unreliable for G7
    // because the child's summary is seeded into the PARENT's
    // step_history, so the parent's prompt also carries the child's
    // run_id.
    let is_child_prompt = body_text.contains("technical analyst");

    let content = if is_child_prompt {
        // Child's first (and only) turn: complete with a summary
        // carrying the marker string. `description` is plumbed onto
        // `RunCompletionAnnotated.completion_summary` by the
        // orchestrator's LoopTermination::Completed path.
        json!([{
            "action_type":       "complete_run",
            "description":       CHILD_SUMMARY_MARKER,
            "confidence":        0.99,
            "requires_approval": false,
        }])
    } else if n == 0 {
        // Parent's first turn: spawn_subagent.
        json!([{
            "action_type":       "spawn_subagent",
            "description":       "G7 parent delegates to researcher",
            "tool_name":         "researcher",
            "tool_args":         { "goal": "G7 child goal" },
            "confidence":        0.95,
            "requires_approval": false,
        }])
    } else {
        // Parent's post-resume turns: complete. The assertion
        // doesn't depend on what the parent does after resume — it
        // only cares that the prompt on this turn carried the
        // child's summary.
        json!([{
            "action_type":       "complete_run",
            "description":       "G7 parent done",
            "confidence":        0.99,
            "requires_approval": false,
        }])
    };
    (
        StatusCode::OK,
        Json(json!({
            "id":      format!("mock-g7-{n}"),
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

async fn spawn_mock() -> (String, Arc<Mutex<Vec<String>>>) {
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let state = MockState {
        hits: Arc::new(AtomicUsize::new(0)),
        recorded: recorded.clone(),
    };
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
    (format!("http://{addr}"), recorded)
}

async fn provision_run(h: &LiveHarness, mock_url: &str) -> String {
    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_g7_{suffix}");
    let session_id = format!("sess_g7_{suffix}");
    let run_id = format!("run_g7_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id":     "openrouter",
            "plaintext_value": format!("sk-g7-{suffix}"),
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
async fn child_completion_summary_lands_in_parent_prompt() {
    // Driver default-on so the child actually runs post-spawn.
    std::env::remove_var("CAIRN_CHILD_RUN_DRIVER_ENABLED");

    let h = LiveHarness::setup().await;
    let (mock_url, recorded) = spawn_mock().await;
    let parent_run_id = provision_run(&h, &mock_url).await;

    // Drive the parent. Mock returns spawn_subagent → child runs
    // → child completes with summary → G5 auto-resumes parent
    // → parent's next drive_run_iteration seeds step_history with
    // the child's summary → parent's LLM call carries the marker.
    let r = h
        .client()
        .post(format!(
            "{}/v1/runs/{}/orchestrate",
            h.base_url, parent_run_id
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "G7 parent goal",
            "max_iterations": 3,
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

    // Poll for parent terminal. 15s deadline covers the full G5
    // signal chain: driver tick (1s) + child turn (1s) + signal
    // delivery (1s) + parent auto-resume (1s) + CI slack.
    let deadline = Duration::from_secs(15);
    let start = Instant::now();
    loop {
        let r = h
            .client()
            .get(format!("{}/v1/runs/{}", h.base_url, parent_run_id))
            .bearer_auth(&h.admin_token)
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
            assert_eq!(state, "completed", "parent must complete; observed={state}",);
            break;
        }
        if start.elapsed() > deadline {
            panic!("parent did not reach terminal state within {deadline:?}; last state={state}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // G7 assertion: at least one of the recorded mock requests
    // carries the child's completion_summary marker. Pre-G7 this
    // never happens — the parent's step_history is empty at
    // drive_run_iteration entry, so the summary never makes it
    // into the user prompt.
    let recorded_bodies = recorded.lock().unwrap();
    let hits: Vec<_> = recorded_bodies
        .iter()
        .enumerate()
        .filter(|(_, body)| body.contains(CHILD_SUMMARY_MARKER))
        .collect();

    // Isolate parent-side prompts (not child-side). The child's own
    // complete_run ActionProposal carries the marker in its
    // description, which shows up in the child's loop-internal
    // artifacts — but would NOT show up on the PARENT's prompt
    // without G7.
    //
    // Disambiguation: the parent is seeded with the orchestrator
    // role (system prompt starts "You are a senior engineer…") and
    // the child is seeded with the researcher role ("technical
    // analyst…"). Checking for the researcher's marker text lets us
    // filter out child-role prompts cleanly — matching on the
    // `run_subagent_` run-id substring would miss-classify the
    // parent's resume call because G7 seeds the child's run-id into
    // the PARENT's step_history.
    let parent_side_hits: Vec<_> = hits
        .iter()
        .filter(|(_, body)| !body.contains("technical analyst"))
        .collect();

    assert!(
        !parent_side_hits.is_empty(),
        "#670 G7: parent's post-resume LLM call must include the child's \
         completion_summary in its prompt. Marker: {CHILD_SUMMARY_MARKER:?}. \
         Total recorded mock calls: {n}, total marker hits: {hits_len}, \
         parent-side marker hits: 0. Pre-G7 this assertion fails because \
         `drive_run_iteration` seeded `step_history` as `vec![]` — the \
         helper in `subagent_steps.rs` + the `step_history` seed in \
         `loop_runner.rs::run_inner` together populate it.",
        n = recorded_bodies.len(),
        hits_len = hits.len(),
    );
}
