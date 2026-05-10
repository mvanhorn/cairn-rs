//! Issue #825 regression — when the agent emits `ActionType::FailRun`,
//! cairn must flip the run to `state=failed` with
//! `failure_class=model_reported_failure`, NOT `state=completed`.
//!
//! **Pathology (dogfood R26, 2026-05-10).** Sub-agents dispatched for
//! M1-7 and M1-8 correctly diagnosed their own blocker (missing
//! precondition: `src/main.rs` did not exist yet) and called
//! `complete_run` with `final_answer` summaries beginning
//! `"## Status: Blocked"` / `"## Status: Partially Complete - Cannot
//! provide final deliverables"`. cairn-app flipped the run to
//! `state=completed`. Operator dashboards / `failure_class` filters
//! treated both as successes; the failure signal was buried in
//! free-form summary text.
//!
//! Root cause: the model had no truthful terminal verb for "I tried, I
//! cannot proceed." `CompleteRun` is success-only, `EscalateToOperator`
//! is mid-run approval gating. The fix adds `ActionType::FailRun`
//! routed to `RunService::fail(FailureClass::ModelReportedFailure)`.
//!
//! This file drives a full HTTP run with a scripted provider that
//! emits a native `fail_run` tool-call on its first turn. The test
//! asserts the full chain:
//!
//!   1. orchestrate response body has `termination = "failed"`
//!   2. the response `reason` carries the `model_reported_failure:`
//!      prefix (the wire contract for `classify_failed_reason`)
//!   3. the run projection has `state = "failed"` AND
//!      `failure_class = "model_reported_failure"`
//!   4. the agent's original reason text is preserved end-to-end.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

const MODEL_ID: &str = "openrouter/test-825-fail-run-model";

/// The agent's reason — we assert this survives end-to-end from the
/// provider-side `fail_run.arguments.reason` into
/// `response.reason` (prefixed) and the run projection's failure
/// audit trail.
const AGENT_REASON: &str = "blocked: src/main.rs does not exist; depends on M1-1 landing first";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
}

/// Scripted provider: EVERY turn proposes `fail_run` with the same
/// reason. In practice the loop only makes one DECIDE call because
/// FailRun is terminal — the second+ turns exist only as a safety net
/// in case derive_signal or execute_impl regresses and fails to
/// propagate the terminal.
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
        // Native tool-call shape — the preferred path for post-#697
        // providers. If this shape regresses we fall through to
        // parse_proposals, which recognises the JSON-action shape
        // for fail_run via the `"fail_run" => ActionType::FailRun`
        // branch in parse_one.
        let tool_call = json!({
            "id": format!("call_{n}"),
            "type": "function",
            "function": {
                "name": "fail_run",
                "arguments": json!({ "reason": AGENT_REASON }).to_string()
            }
        });
        (
            StatusCode::OK,
            Json(json!({
                "id":      format!("mock-825-{n}"),
                "choices": [{
                    "index":   0,
                    "message": {
                        "role":       "assistant",
                        "content":    "",
                        "tool_calls": [tool_call]
                    },
                    "finish_reason": "tool_calls",
                }],
                "usage": {
                    "prompt_tokens":     12,
                    "completion_tokens": 8,
                    "total_tokens":      20,
                },
            })),
        )
    }

    let app = Router::new()
        .route("/chat/completions", post(chat_handler))
        .route("/v1/chat/completions", post(chat_handler))
        .route(
            "/v1/models",
            get(|| async { Json(json!({ "data": [{ "id": MODEL_ID }] })) }),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(25)).await;
    (format!("http://{addr}"), hits)
}

/// Provision credential + provider connection + defaults + session +
/// run. Mirrors `test_660_completion_gate::provision_run` but scoped
/// to a #825 suffix so the two files don't collide on shared store
/// state when run in parallel.
async fn provision_run(h: &LiveHarness, mock_url: &str) -> (String, String, String) {
    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_825_{suffix}");
    let session_id = format!("sess_825_{suffix}");
    let run_id = format!("run_825_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id":     "openrouter",
            "plaintext_value": format!("sk-825-{suffix}"),
        }))
        .send()
        .await
        .expect("credential");
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
            "supported_models":       [MODEL_ID],
            "credential_id":          credential_id,
            "endpoint_url":           mock_url,
        }))
        .send()
        .await
        .expect("connection");
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
            .json(&json!({ "value": MODEL_ID }))
            .send()
            .await
            .expect("defaults");
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
        .expect("session");
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
        .expect("run");
    assert_eq!(r.status().as_u16(), 201);

    (tenant, session_id, run_id)
}

/// Read back the run record so tests can assert the terminal state +
/// failure_class honestly reflects what FailRun did. Same polling
/// pattern as `test_660_completion_gate::fetch_run_state`: the
/// orchestrate handler returns before the run-state projection has
/// applied the terminal event.
async fn fetch_run_state(h: &LiveHarness, run_id: &str, expected_state: &str) -> Value {
    use std::time::Instant;
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut backoff_us: u64 = 100;
    loop {
        let r = h
            .client()
            .get(format!("{}/v1/runs/{}", h.base_url, run_id))
            .bearer_auth(&h.admin_token)
            .send()
            .await
            .expect("get run");
        assert_eq!(r.status().as_u16(), 200, "get run: {run_id}");
        let body: Value = r.json().await.expect("run json");
        let state = body
            .get("run")
            .and_then(|r| r.get("state"))
            .and_then(Value::as_str)
            .or_else(|| body.get("state").and_then(Value::as_str))
            .unwrap_or("<missing>");
        if state == expected_state || Instant::now() >= deadline {
            return body;
        }
        tokio::time::sleep(Duration::from_micros(backoff_us)).await;
        backoff_us = (backoff_us * 2).min(5_000);
    }
}

/// #825 — when the agent emits `fail_run`, the run must terminate with
/// `state=failed` + `failure_class=model_reported_failure`. Pre-#825,
/// the only terminal verb available was `complete_run`; a blocked
/// agent would emit that with a "Status: Blocked" summary and the run
/// flipped to `state=completed` (R26 pathology).
///
/// Negative-before-after: before ActionType::FailRun existed, no scripted
/// provider could reach this state legitimately — there was no verb to
/// emit. The test literally could not be written pre-#825.
#[tokio::test]
async fn fail_run_terminal_flips_state_to_failed_with_model_reported_failure() {
    let h = LiveHarness::setup().await;
    let (mock_url, hits) = spawn_mock().await;
    let (_tenant, _session, run_id) = provision_run(&h, &mock_url).await;

    let orch = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal":           "#825: the agent should emit fail_run",
            "max_iterations": 5,
        }))
        .timeout(Duration::from_secs(60))
        .send()
        .await
        .expect("orchestrate");
    let orch_status = orch.status().as_u16();
    let orch_body: Value = orch.json().await.unwrap_or(Value::Null);

    assert_eq!(
        orch_status, 200,
        "orchestrate must return 200 even on fail_run termination: body={orch_body}",
    );

    // The handler's response body uses `termination` as the canonical
    // wire label — same contract #660 / F47 / F38 test against. For
    // FailRun the expected value is `"failed"`.
    let termination = orch_body
        .get("termination")
        .and_then(Value::as_str)
        .unwrap_or("<missing>");
    assert_eq!(
        termination, "failed",
        "#825: fail_run MUST map to termination=failed, NOT completed. \
         Without this, R26's 'Status: Blocked in complete_run summary' \
         pathology comes back — the whole point of FailRun is that \
         blocked runs surface as failed. body={orch_body}",
    );
    assert_ne!(
        termination, "completed",
        "#825 regression guard: fail_run MUST NOT be routed to the \
         completed path; that would defeat the entire feature."
    );

    // The `reason` field must carry the wire-contract prefix that
    // `classify_failed_reason` keys on to map to
    // FailureClass::ModelReportedFailure. The agent's original reason
    // text follows the prefix.
    let reason = orch_body
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or("");
    assert!(
        reason.starts_with("model_reported_failure:"),
        "failure reason must use the `model_reported_failure:` prefix — \
         that prefix is the contract between \
         cairn-orchestrator::execute_impl::derive_signal and \
         cairn-app::handlers::runs::helpers::classify_failed_reason. \
         Got: {reason:?}"
    );
    assert!(
        reason.contains(AGENT_REASON),
        "failure reason must preserve the agent's original `reason` \
         text so operators see WHY without parsing any other channel. \
         Expected substring: {AGENT_REASON:?}. Got: {reason:?}"
    );

    // The run projection must reflect the terminal failure.
    let run = fetch_run_state(&h, &run_id, "failed").await;
    let state = run
        .get("run")
        .and_then(|r| r.get("state"))
        .and_then(Value::as_str)
        .or_else(|| run.get("state").and_then(Value::as_str))
        .unwrap_or("<missing>");
    assert_eq!(
        state, "failed",
        "#825: run projection must reflect FailRun as state=failed. \
         Got {state}. full={run}",
    );
    let failure_class = run
        .get("run")
        .and_then(|r| r.get("failure_class"))
        .and_then(Value::as_str)
        .or_else(|| run.get("failure_class").and_then(Value::as_str))
        .unwrap_or("<missing>");
    assert_eq!(
        failure_class, "model_reported_failure",
        "#825: failure_class must be `model_reported_failure` so \
         operator dashboards can distinguish agent-declared failure \
         from generic `execution_error`. Got {failure_class}. \
         full={run}",
    );

    // Sanity: the mock was called exactly once — FailRun is
    // terminal on the first DECIDE turn, so there's no iteration 2
    // DECIDE call. If this asserts >1 it means derive_signal
    // regressed and failed to set LoopSignal::Failed on
    // FailRun+Succeeded.
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "FailRun is terminal — loop must exit after a single DECIDE + \
         EXECUTE round. If this fires the loop is not recognising \
         FailRun as terminal."
    );
}
