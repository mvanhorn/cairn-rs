//! Issue #651 regression — auto-resume preserves the operator goal.
//!
//! Reproduces the dogfood-round-2 CRITICAL: after an operator approves
//! a tool-call approval, the F49 auto-resume worker POSTs
//! `/v1/runs/:id/orchestrate` with an empty JSON body. Pre-#651 the
//! orchestrate handler resolved `body.goal = None`, then
//! `default_goal = resolve_run_string_default(..., "goal")` — also
//! `None` because the handler never persisted the original goal — and
//! fell through to `"Execute the run objective."`. The LLM saw the
//! generic fallback and completed the run with
//! "Without a clear project objective in the prompt...".
//!
//! Flow:
//!
//!   1. `LiveHarness` — real cairn-app subprocess.
//!   2. Mock LLM records every user-message it sees. Turn 1 emits an
//!      approval-gated `bash` call. Turn 2+ emits `complete_run`.
//!   3. `POST /orchestrate` with `{"goal":"TEST_MARKER_GOAL_<suffix>",
//!      "max_iterations":5}` — expect 202 `waiting_approval`.
//!   4. Approve the tool call via `/v1/approvals/:id/approve`.
//!   5. Do NOT re-POST `/orchestrate` — auto-resume fires.
//!   6. Poll until the run reaches a terminal state.
//!   7. **The assertion**: every LLM call after turn 1 (i.e. the
//!      auto-resumed iterations) must include the `TEST_MARKER_GOAL`
//!      string in the user-message content. The orchestrator renders
//!      `ctx.goal` into the prompt as `## Goal\n<goal>` — if the fix is
//!      in place the marker shows up in every call; if the fix regresses
//!      only turn-1 carries the marker and later calls carry the
//!      fallback string.

mod support;

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

const MODEL_ID: &str = "openrouter/preserve-goal-model";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
    /// Captured user-message content for every chat/completions call.
    /// Each entry is the serialized JSON of the request body so the
    /// test can assert on the `## Goal\n<marker>` block verbatim.
    captured_bodies: Arc<Mutex<Vec<String>>>,
    marker_path: String,
    marker_content: String,
}

async fn spawn_llm_mock(
    marker_path: String,
    marker_content: String,
) -> (String, Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>) {
    let state = MockState {
        hits: Arc::new(AtomicUsize::new(0)),
        captured_bodies: Arc::new(Mutex::new(Vec::new())),
        marker_path,
        marker_content,
    };
    let hits = state.hits.clone();
    let bodies = state.captured_bodies.clone();

    async fn chat_handler(
        State(state): State<MockState>,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        let n = state.hits.fetch_add(1, Ordering::SeqCst);
        // Serialize the entire request body; the test greps for the
        // TEST_MARKER_GOAL substring. This is cheaper than walking the
        // `messages` array and covers system + user messages both.
        if let Ok(serialized) = serde_json::to_string(&body) {
            if let Ok(mut guard) = state.captured_bodies.lock() {
                guard.push(serialized);
            }
        }
        let content_json = if n == 0 {
            // Turn 1: propose the approval-gated bash call.
            json!([{
                "action_type": "invoke_tool",
                "description": "write marker file so #651 test can verify auto-resume replayed the goal",
                "confidence": 0.99,
                "tool_name": "bash",
                "tool_args": {
                    "command": format!(
                        "printf '%s' {:?} > {:?}",
                        state.marker_content, state.marker_path
                    )
                },
                "requires_approval": true
            }])
        } else {
            // Turn 2+: wrap the run.
            json!([{
                "action_type": "complete_run",
                "description": "goal was preserved across auto-resume",
                "confidence": 0.99,
                "requires_approval": false
            }])
        };

        (
            StatusCode::OK,
            Json(json!({
                "id": format!("mock-651-{n}"),
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": content_json.to_string(),
                    },
                    "finish_reason": "stop",
                }],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 5,
                    "total_tokens": 15,
                }
            })),
        )
    }

    let app = Router::new()
        .route("/chat/completions", post(chat_handler))
        .route("/v1/chat/completions", post(chat_handler))
        .route(
            "/v1/models",
            get(|| async { Json(json!({"data":[{"id": MODEL_ID}]})) }),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    (format!("http://{addr}"), hits, bodies)
}

struct MarkerFileGuard(PathBuf);
impl Drop for MarkerFileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[tokio::test]
async fn auto_resume_replays_operator_goal_not_fallback_string() {
    let suffix = uuid::Uuid::new_v4().simple().to_string()[..12].to_owned();
    let marker_goal = format!("TEST_MARKER_GOAL_{suffix}");
    let marker_path = std::env::temp_dir().join(format!("cairn-651-{suffix}.txt"));
    let marker_content = format!("goal-preserved-{suffix}");
    let _guard = MarkerFileGuard(marker_path.clone());

    let h = LiveHarness::setup().await;
    let (mock_url, hits, bodies) = spawn_llm_mock(
        marker_path.to_string_lossy().into_owned(),
        marker_content.clone(),
    )
    .await;

    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let suffix2 = h.project.clone();
    let connection_id = format!("conn_651_{suffix2}");
    let session_id = format!("sess_651_{suffix2}");
    let run_id = format!("run_651_{suffix2}");

    // ── Credential + provider connection + defaults ────────────────────────
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": "openrouter",
            "plaintext_value": format!("sk-651-{suffix2}"),
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
            "tenant_id": tenant,
            "provider_connection_id": connection_id,
            "provider_family": "openrouter",
            "adapter_type": "openrouter",
            "supported_models": [MODEL_ID],
            "credential_id": credential_id,
            "endpoint_url": mock_url,
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
                h.base_url, key
            ))
            .bearer_auth(&h.admin_token)
            .json(&json!({ "value": MODEL_ID }))
            .send()
            .await
            .expect("defaults reach server");
        assert_eq!(r.status().as_u16(), 200);
    }

    // ── Session + run (no `prompt` field on the run — goal flows via
    //    `/orchestrate` only, which is the exact path #651 regresses). ──
    let r = h
        .client()
        .post(format!("{}/v1/sessions", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id": tenant,
            "workspace_id": workspace,
            "project_id": project,
            "session_id": session_id,
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
            "tenant_id": tenant,
            "workspace_id": workspace,
            "project_id": project,
            "session_id": session_id,
            "run_id": run_id,
            // NOTE: deliberately no `prompt` — isolate the orchestrate path.
        }))
        .send()
        .await
        .expect("run reaches server");
    assert_eq!(r.status().as_u16(), 201);

    // ── First orchestrate — carries the marker goal. ───────────────────────
    let first_orch_res = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": marker_goal,
            "max_iterations": 5,
            "approval_timeout_ms": 30_000u64,
        }))
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
        .expect("orchestrate request reaches server");
    let first_status = first_orch_res.status().as_u16();
    let first_body_text = first_orch_res.text().await.unwrap_or_default();
    assert_eq!(
        first_status, 202,
        "first orchestrate should return 202 WaitingApproval, got {first_status}: {first_body_text}"
    );

    // ── Find the pending proposal. ─────────────────────────────────────────
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut call_id: Option<String> = None;
    while std::time::Instant::now() < deadline {
        let r = h
            .client()
            .get(format!(
                "{}/v1/approvals?run_id={}&state=pending&kind=tool_call",
                h.base_url, run_id
            ))
            .header("X-Cairn-Tenant", &tenant)
            .header("X-Cairn-Workspace", &workspace)
            .header("X-Cairn-Project", &project)
            .bearer_auth(&h.admin_token)
            .send()
            .await
            .expect("list approvals reaches server");
        if r.status().as_u16() == 200 {
            let body: Value = r.json().await.expect("list json");
            let items = body
                .get("items")
                .and_then(|v| v.as_array())
                .cloned()
                .or_else(|| body.as_array().cloned())
                .unwrap_or_default();
            if let Some(first) = items.first() {
                if first.get("tool_name").and_then(|v| v.as_str()) == Some("bash") {
                    if let Some(cid) = first.get("call_id").and_then(|v| v.as_str()) {
                        call_id = Some(cid.to_owned());
                        break;
                    }
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let call_id = call_id.expect("pending tool-call approval did not appear within 10s");

    // ── Approve. ───────────────────────────────────────────────────────────
    let r = h
        .client()
        .post(format!("{}/v1/approvals/{}/approve", h.base_url, call_id))
        .header("X-Cairn-Tenant", &tenant)
        .header("X-Cairn-Workspace", &workspace)
        .header("X-Cairn-Project", &project)
        .bearer_auth(&h.admin_token)
        .json(&json!({"scope": {"type": "once"}}))
        .send()
        .await
        .expect("approve reaches server");
    assert_eq!(
        r.status().as_u16(),
        200,
        "approve: {}",
        r.text().await.unwrap_or_default()
    );

    // ── Do NOT re-POST /orchestrate. Poll until terminal state. ────────────
    let poll_deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let mut final_run_state: Option<String> = None;
    let mut final_completion_summary = String::new();
    while std::time::Instant::now() < poll_deadline {
        let r = h
            .client()
            .get(format!("{}/v1/runs/{}", h.base_url, run_id))
            .header("X-Cairn-Tenant", &tenant)
            .header("X-Cairn-Workspace", &workspace)
            .header("X-Cairn-Project", &project)
            .bearer_auth(&h.admin_token)
            .send()
            .await
            .expect("run fetch reaches server");
        if r.status().as_u16() == 200 {
            let body: Value = r.json().await.unwrap_or(Value::Null);
            let state = body
                .pointer("/run/state")
                .and_then(|v| v.as_str())
                .or_else(|| body.get("state").and_then(|v| v.as_str()))
                .unwrap_or("")
                .to_lowercase();
            if state == "completed" || state == "failed" || state == "canceled" {
                final_run_state = Some(state);
                final_completion_summary = body
                    .pointer("/completion/summary")
                    .and_then(|v| v.as_str())
                    .map(ToOwned::to_owned)
                    .unwrap_or_default();
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    let final_state = final_run_state.unwrap_or_else(|| {
        panic!(
            "run never left waiting_approval within 60s — #651 fix regressed? LLM hits={}",
            hits.load(Ordering::SeqCst)
        )
    });

    // The companion auto-resume test covers terminal-state semantics; for
    // #651 the regression signal lives in the captured LLM prompts.
    assert!(
        final_state == "completed" || final_state == "failed",
        "run should reach a terminal state, got {final_state:?}"
    );

    // ── The regression guard: every LLM call after turn 1 must carry the
    //    TEST_MARKER_GOAL substring. Pre-fix, turn 1 has the marker but
    //    turn 2+ (auto-resumed) does not.
    let captured = bodies.lock().unwrap().clone();
    assert!(
        captured.len() >= 2,
        "expected at least 2 LLM calls (initial + auto-resumed), got {}. hits={}",
        captured.len(),
        hits.load(Ordering::SeqCst)
    );
    for (i, body_text) in captured.iter().enumerate() {
        assert!(
            body_text.contains(&marker_goal),
            "LLM call #{i} did NOT contain the operator goal marker {marker_goal:?}. \
             This is the #651 regression: the auto-resume kick dropped the goal and \
             the orchestrator rendered the fallback \"Execute the run objective.\" \
             into the prompt. Captured body (truncated): {}",
            &body_text[..body_text.len().min(400)]
        );
    }

    // Completion summary must not claim the LLM had no objective — the
    // exact dogfood-v2 symptom that triggered #651.
    let lowered = final_completion_summary.to_lowercase();
    assert!(
        !lowered.contains("no objective") && !lowered.contains("no clear project objective"),
        "completion summary claims the LLM had no objective — #651 regression. Summary: {final_completion_summary:?}"
    );

    // Filesystem side-effect still works (the same guard #646 uses).
    assert!(
        marker_path.exists(),
        "marker file missing — bash never ran post-approve: {marker_path:?}"
    );
    let got = std::fs::read_to_string(&marker_path).expect("read marker file");
    assert_eq!(got.trim(), marker_content);
}
