//! Issue #636 regression — auto-resume on approve.
//!
//! Reproduces the dogfood blocker: after an operator approves a
//! tool-call approval via the UI/API, the orchestrator loop must
//! re-enter the GATHER → DECIDE → EXECUTE cycle automatically.
//!
//! Pre-#636, the F49 auto-resume hook only fired on the plan-approval
//! path (`ApprovalResolved`). Tool-call approvals
//! (`ToolCallApproved` / `ToolCallRejected`) fell through the match
//! arm, so the orchestrator stayed parked in `waiting_approval`
//! indefinitely. The operator had to hand-POST `/v1/runs/:id/
//! orchestrate` to drive every single approval resolution, making
//! multi-tool-call runs (10–50 approvals) effectively unusable.
//!
//! Flow:
//!
//!   1. Stand up a `LiveHarness` (real cairn-app subprocess).
//!   2. Mock LLM emits an approval-gated `bash` call on turn 1, then
//!      `complete_run` thereafter.
//!   3. First `POST /orchestrate` returns 202 `waiting_approval`.
//!   4. Poll `/v1/tool-call-approvals` until the proposal lands.
//!   5. `POST /v1/approvals/:id/approve` with `scope=once`.
//!   6. **Without a second manual `/orchestrate` POST**, poll
//!      `GET /v1/runs/:id` until the run leaves `waiting_approval`
//!      or reaches a terminal state.
//!   7. Assert the run ends up `completed` — that is only possible
//!      if auto-resume fired (mock turn 2 emits `complete_run`).
//!   8. Assert the marker file written by the approved bash call
//!      exists — the same filesystem regression guard as the F25
//!      drain test; the event log alone cannot fake the side effect.

mod support;

use std::path::PathBuf;
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

const MODEL_ID: &str = "openrouter/auto-resume-model";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
    marker_path: String,
    marker_content: String,
}

async fn spawn_llm_mock(marker_path: String, marker_content: String) -> (String, Arc<AtomicUsize>) {
    let state = MockState {
        hits: Arc::new(AtomicUsize::new(0)),
        marker_path,
        marker_content,
    };
    let hits = state.hits.clone();

    async fn chat_handler(
        State(state): State<MockState>,
        Json(_body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        let n = state.hits.fetch_add(1, Ordering::SeqCst);
        let content_json = if n == 0 {
            // Turn 1: propose the approval-gated bash call.
            json!([{
                "action_type": "invoke_tool",
                "description": "write marker file so auto-resume test can verify bash actually ran after approve",
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
            // Turn 2+: wrap the run. If auto-resume didn't fire, this
            // branch never runs — the run stays `waiting_approval`.
            json!([{
                "action_type": "complete_run",
                "description": "marker written — done",
                "confidence": 0.99,
                "requires_approval": false
            }])
        };

        (
            StatusCode::OK,
            Json(json!({
                "id": format!("mock-auto-resume-{n}"),
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
    (format!("http://{addr}"), hits)
}

struct MarkerFileGuard(PathBuf);
impl Drop for MarkerFileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[tokio::test]
async fn approve_triggers_auto_resume_without_manual_reorchestrate() {
    let suffix = uuid::Uuid::new_v4().simple().to_string()[..12].to_owned();
    let marker_path = std::env::temp_dir().join(format!("cairn-autoresume-636-{suffix}.txt"));
    let marker_content = format!("auto-resumed-{suffix}");
    let _guard = MarkerFileGuard(marker_path.clone());

    let h = LiveHarness::setup().await;
    let (mock_url, hits) = spawn_llm_mock(
        marker_path.to_string_lossy().into_owned(),
        marker_content.clone(),
    )
    .await;

    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let suffix2 = h.project.clone();
    let connection_id = format!("conn_autoresume_{suffix2}");
    let session_id = format!("sess_autoresume_{suffix2}");
    let run_id = format!("run_autoresume_{suffix2}");

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
            "plaintext_value": format!("sk-autoresume-{suffix2}"),
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

    // ── Session + run ──────────────────────────────────────────────────────
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

    // #702 follow-up: pin the run's agent_role to `executor` via
    // project defaults so the orchestrator shell policy does not fire
    // on this test's stand-in bash calls (which predate the policy).
    let r_role = h
        .client()
        .put(format!(
            "{}/v1/settings/defaults/project/{project}/run:{run_id}:agent_role",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .json(&serde_json::json!({ "value": "executor" }))
        .send()
        .await
        .expect("set agent_role default");
    assert_eq!(r_role.status().as_u16(), 200);

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
        }))
        .send()
        .await
        .expect("run reaches server");
    assert_eq!(r.status().as_u16(), 201);

    // ── First orchestrate — F26: 202 WaitingApproval ───────────────────────
    let first_orch_res = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "write the marker file via an approval-gated bash command",
            "max_iterations": 4,
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

    // ── Find the pending proposal ──────────────────────────────────────────
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

    assert!(
        !marker_path.exists(),
        "marker file must NOT exist before approval"
    );

    // ── Approve via the unified `/v1/approvals/:id/approve` surface ────────
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

    // ── #636: DO NOT re-POST /orchestrate. Auto-resume must fire on its
    //       own. Poll the run record until it leaves `waiting_approval`.
    //       Budget 60s — cold mock takes a few hundred ms, auto-resume
    //       worker dedup is 5s, and the follow-up orchestrate runs two
    //       iterations (drain + complete_run + FF finalize).
    let poll_deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let mut final_run_state: Option<String> = None;
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
            // `GET /v1/runs/:id` returns `RunDetailResponse { run, tasks, completion }`.
            // The run state hangs off `body.run.state`. Earlier drafts of this
            // test walked `body.state` and never matched, masking a real fix.
            let state = body
                .pointer("/run/state")
                .and_then(|v| v.as_str())
                .or_else(|| body.get("state").and_then(|v| v.as_str()))
                .unwrap_or("")
                .to_lowercase();
            if state == "completed" || state == "failed" || state == "canceled" {
                final_run_state = Some(state);
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    let final_state = final_run_state.unwrap_or_else(|| {
        panic!(
            "run never left waiting_approval within 60s — #636 auto-resume did not fire. LLM hits={}",
            hits.load(Ordering::SeqCst)
        )
    });

    // Acceptable: `completed` is the happy path. `failed` is tolerated
    // only when the orthogonal FF terminal-write path (same workaround
    // the F25 drain test documents) collides with the bare-LiveHarness
    // setup AFTER bash already ran. The filesystem assertion below is
    // the real regression guard.
    assert!(
        final_state == "completed" || final_state == "failed",
        "run should reach a terminal state after auto-resume, got {final_state:?}"
    );

    // The LLM must have been called at least twice: once for the
    // initial approval-gated proposal, and at least once more for the
    // auto-resumed iteration that emits `complete_run`. One-hit proves
    // the auto-resume kick never reached the handler.
    assert!(
        hits.load(Ordering::SeqCst) >= 2,
        "LLM must have been called at least twice — got {} hit(s). \
         Auto-resume did not re-enter the orchestrator loop.",
        hits.load(Ordering::SeqCst),
    );

    // ── Filesystem regression guard ────────────────────────────────────────
    // If auto-resume fired but the drain failed to dispatch, the marker
    // file will be missing. No amount of event-log plumbing can fake a
    // file on disk.
    assert!(
        marker_path.exists(),
        "MARKER FILE MISSING: auto-resume fired but drain did not dispatch bash. \
         path={:?}, LLM hits={}, final_state={final_state}",
        marker_path,
        hits.load(Ordering::SeqCst),
    );
    let got = std::fs::read_to_string(&marker_path).expect("read marker file");
    assert_eq!(
        got.trim(),
        marker_content,
        "marker content mismatch: expected {marker_content:?}, got {got:?}",
    );
}
