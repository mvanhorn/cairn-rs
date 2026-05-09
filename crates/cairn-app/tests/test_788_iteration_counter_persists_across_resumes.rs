//! #788 regression: `OrchestrationContext.iteration` must survive
//! /orchestrate-resume boundaries. Pre-fix, every POST to
//! `/v1/runs/:id/orchestrate` (including F49 auto-resume after a
//! tool-call approval) created a fresh context with `iteration: 0`,
//! which made every step_history entry render as `[0]` and defeated
//! `should_inject_stuck_nudge`'s iteration-threshold check.
//!
//! Test shape:
//!   1. Mock LLM that returns a `bash` tool call (which requires
//!      approval) on every call.
//!   2. POST /orchestrate — first iteration runs to suspend on
//!      approval.
//!   3. Approve the tool call. F49 auto-resumes /orchestrate.
//!   4. Approve again. F49 auto-resumes /orchestrate again.
//!   5. Pull the LLM traces for the run. Assert the LAST trace's user
//!      message renders the F25-drained bash step with `[N]` where
//!      N >= 1 (post-fix). Pre-fix every entry would render `[0]`.
//!
//! The chosen signal — `LlmCompletionRecorded` count in the event
//! log — is a conservative iteration counter (cache hits skip it),
//! but for the bash-loop pathology this test exercises, every loop
//! goes through DECIDE → mock → response, so the counter advances
//! by exactly 1 per iteration.

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

const MOCK_MODEL: &str = "openrouter/788-bash-loop";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
}

/// Always returns a `bash` tool-call proposal. The orchestrator's
/// approval gate suspends the loop after each call. Approving the
/// tool-call kicks F49 auto-resume, which POSTs /orchestrate again —
/// the path #788 is testing.
async fn mock_chat_handler(
    State(state): State<MockState>,
    Json(_body): Json<Value>,
) -> Json<Value> {
    let n = state.hits.fetch_add(1, Ordering::SeqCst);
    let cmd = format!("echo iteration-{n}");
    Json(json!({
        "id": format!("chatcmpl-{n}"),
        "object": "chat.completion",
        "created": 0,
        "model": MOCK_MODEL,
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": format!("tc_788_{n}"),
                    "type": "function",
                    "function": {
                        "name": "bash",
                        "arguments": json!({ "command": cmd, "description": "loop" }).to_string(),
                    },
                }],
            },
            "finish_reason": "tool_calls",
        }],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
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

/// Approve all pending tool-call approvals for a given run. Returns
/// the count of approvals processed.
async fn approve_all_pending(h: &LiveHarness, run_id: &str) -> usize {
    let r = h
        .client()
        .get(format!(
            "{}/v1/approvals?state=pending&kind=tool_call&run_id={}",
            h.base_url, run_id,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("list approvals reaches server");
    if r.status().as_u16() != 200 {
        return 0;
    }
    let body = r.json::<Value>().await.unwrap_or(Value::Null);
    let items = body
        .get("items")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let mut approved = 0usize;
    for a in &items {
        let Some(call_id) = a.get("call_id").and_then(|v| v.as_str()) else {
            continue;
        };
        let r = h
            .client()
            .post(format!("{}/v1/approvals/{}/approve", h.base_url, call_id))
            .bearer_auth(&h.admin_token)
            .json(&json!({ "scope": { "type": "once" } }))
            .send()
            .await
            .expect("approve reaches server");
        if r.status().is_success() {
            approved += 1;
        }
    }
    approved
}

#[tokio::test]
async fn iteration_counter_survives_orchestrate_resume() {
    let h = LiveHarness::setup().await;
    let (mock_url, hits) = spawn_mock().await;

    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_788_{suffix}");
    let session_id = format!("sess_788_{suffix}");
    let run_id = format!("run_788_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id":     "openrouter",
            "plaintext_value": format!("sk-788-{suffix}"),
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

    // First /orchestrate POST. The mock returns a bash tool call
    // requiring approval; the loop suspends.
    let r = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal":           "drive multiple iterations to exercise #788 fix",
            "max_iterations": 5,
        }))
        .send()
        .await
        .expect("orchestrate reaches server");
    assert!(
        r.status().is_success() || r.status().as_u16() == 202,
        "first orchestrate POST should return 2xx; got {}",
        r.status(),
    );

    // Drive ≥2 iterations by approving in a loop. Each approval
    // triggers F49 auto-resume which POSTs /orchestrate again.
    // Pre-fix #788, every one of those resumes would set
    // iteration:0, leaving the rendered step_history full of [0]
    // entries. Two iterations suffices: after the first, the second
    // /orchestrate POST should derive iteration=1 from the existing
    // LlmCompletionRecorded event.
    let target_iters = 2usize;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut approved_total = 0usize;
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(400)).await;
        approved_total += approve_all_pending(&h, &run_id).await;
        let mock_hits = hits.load(Ordering::SeqCst);
        if mock_hits >= target_iters && approved_total >= target_iters {
            break;
        }
    }
    assert!(
        hits.load(Ordering::SeqCst) >= target_iters,
        "mock should have been called at least {target_iters} times across resumes; got {}",
        hits.load(Ordering::SeqCst),
    );

    // Verify the projection-backed counter incremented. Query the run
    // record directly — `iteration` should be at least 1 after one
    // approval-resume cycle.
    let r = h
        .client()
        .get(format!("{}/v1/runs/{}", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("get run reaches server");
    assert_eq!(r.status().as_u16(), 200);
    let body = r.json::<Value>().await.unwrap_or(Value::Null);
    let projection_iteration = body
        .get("run")
        .and_then(|v| v.get("iteration"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    assert!(
        projection_iteration >= 1,
        "#791: RunRecord.iteration should be >= 1 after {target_iters} approve-resume cycles; got {projection_iteration}",
    );

    // Pull LLM traces for this session and find the most-recent one
    // for our run. The trace body's `system_prompt + messages_json`
    // user message renders the F25-drained step_history; pre-fix
    // every entry would have `[0]` because ctx.iteration was always
    // 0; post-fix the entries carry the real iteration index.
    let r = h
        .client()
        .get(format!(
            "{}/v1/sessions/{}/llm-traces",
            h.base_url, session_id,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("list traces reaches server");
    assert_eq!(r.status().as_u16(), 200);
    let body = r.json::<Value>().await.unwrap_or(Value::Null);
    let traces = body
        .get("traces")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        traces.len() >= target_iters,
        "expected at least {target_iters} traces; got {}",
        traces.len(),
    );

    // Pick the last (most recent) trace by `created_at_ms` for our
    // run, then fetch its body and inspect the user message for the
    // step-history `[N]` prefix.
    let mut our_traces: Vec<&Value> = traces
        .iter()
        .filter(|t| {
            t.get("run_id")
                .and_then(|v| v.as_str())
                .is_some_and(|s| s == run_id)
        })
        .collect();
    our_traces.sort_by_key(|t| t.get("created_at_ms").and_then(|v| v.as_u64()).unwrap_or(0));
    let last = our_traces
        .last()
        .expect("at least one trace for our run_id");
    let last_trace_id = last.get("trace_id").and_then(|v| v.as_str()).unwrap();

    let r = h
        .client()
        .get(format!(
            "{}/v1/sessions/{}/llm-traces/{}/body",
            h.base_url, session_id, last_trace_id,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("trace body reaches server");
    assert_eq!(r.status().as_u16(), 200);
    let body = r.json::<Value>().await.unwrap_or(Value::Null);
    let messages_json = body
        .get("messages_json")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let msgs: Vec<Value> = serde_json::from_str(messages_json).unwrap_or_default();
    let user_msg = msgs
        .iter()
        .find(|m| m.get("role").and_then(|v| v.as_str()) == Some("user"))
        .expect("user message in trace");
    let user_content = user_msg
        .get("content")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    // The original #788 test asserted on the `[N]` iteration prefix
    // rendered in `## Step history`. PR #798 (#797) removed that
    // prefix from the user message — the iteration counter is now
    // intentionally hidden from the LLM. The invariant `#788 fixed`
    // is preserved at a stronger surface: the projection-backed
    // `RunRecord.iteration >= 1` assertion (above) reads the same
    // counter through the API. If that field was 0, we'd know the
    // resume boundary didn't increment.
    //
    // Sanity-check that the user message still renders SOMETHING in
    // the step-history section so future regressions don't drop it
    // entirely.
    assert!(
        user_content.contains("## Step history"),
        "user message must still include `## Step history` section. Got {} chars: {}",
        user_content.len(),
        &user_content[..user_content.len().min(400)],
    );
}
