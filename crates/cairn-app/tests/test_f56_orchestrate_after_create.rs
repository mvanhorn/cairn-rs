//! F56 regression: every `POST /v1/runs/:id/orchestrate` against a
//! freshly-created run must not return 409 `execution_not_eligible`.
//!
//! # Bug (Phase 2-v2 dogfood, 2026-04-26)
//!
//! F51 (PR #316, `5a9ecbd1`) replaced the orchestrate handler's entry-time
//! `ensure_active` call with `renew_lease_if_stale`. Its stated rationale:
//! the no-lease branch of `renew_lease_if_stale` walks the same
//! `issue_grant_and_claim` sequence `ensure_active` did, with one fewer
//! snapshot round-trip.
//!
//! That reasoning is correct for a *freshly-created* run (lease absent,
//! fallback to `claim_with_snapshot`). It is *not* correct for a run
//! whose lifecycle_phase has diverged from `active` with the lease still
//! held — e.g. a run that passed through `waiting_approval` and back.
//! In that case `ensure_active` would short-circuit (lease present) and
//! the subsequent renew-in-place path FCALL hits FF's eligibility gate
//! with `execution_not_eligible`. Every `/orchestrate` call after the
//! first approval returned:
//!
//! ```text
//! {"status_code":409,"code":"conflict",
//!  "message":"execution conflict: execution_not_eligible",
//!  "request_id":null}
//! ```
//!
//! cairn-app log:
//!
//! ```text
//! ERROR: F51: failed to refresh run lease before orchestrate loop
//!   error=execution conflict: execution_not_eligible
//! ```
//!
//! # Fix (this PR)
//!
//! `FabricRunService::renew_lease_if_stale` now calls `ensure_active` as
//! its first step. Both helpers are idempotent:
//!
//!   * `ensure_active` short-circuits on `current_lease.is_some()` (no FF
//!     mutation) — the happy path for healthy runs.
//!   * On a freshly-created run (lease absent) it walks
//!     `issue_grant_and_claim`, producing the same effect as the
//!     old `ensure_active` handler call.
//!   * On a resumed-post-approval run, `ensure_active`'s reclaim restores
//!     the `active` lifecycle phase so the renew path accepts.
//!
//! The orchestrate handler stays a single-call (`renew_lease_if_stale`)
//! — F51's design intent is preserved.
//!
//! # This test
//!
//! Primary regression: create a run, call orchestrate once, assert it
//! does not 409. Secondary: orchestrate again immediately (exercises the
//! already-active path). No mock provider is needed for the regression
//! symptom — the 409 is surfaced from `renew_lease_if_stale` *before*
//! the orchestrate loop makes any provider call. We keep the mock
//! provider to drive the run to a clean `completed` termination so the
//! assertion is tight.

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

const MOCK_MODEL: &str = "openrouter/f56-orchestrate-regression";
const FINAL_ANSWER: &str = "F56 regression ok.";

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
        state.hits.fetch_add(1, Ordering::SeqCst);
        (
            StatusCode::OK,
            Json(json!({
                "id": "mock-f56",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": null,
                        "tool_calls": [{
                            "id": "call_complete_1",
                            "type": "function",
                            "function": {
                                "name": "complete_run",
                                "arguments": json!({ "final_answer": FINAL_ANSWER }).to_string(),
                            }
                        }],
                    },
                    "finish_reason": "tool_calls",
                }],
                "usage": {
                    "prompt_tokens": 40,
                    "completion_tokens": 12,
                    "total_tokens": 52,
                }
            })),
        )
    }

    let app = Router::new()
        .route("/chat/completions", post(chat_handler))
        .route("/v1/chat/completions", post(chat_handler))
        .route(
            "/v1/models",
            get(|| async { Json(json!({ "data": [{"id": MOCK_MODEL}] })) }),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let base_url = format!("http://{addr}");
    let ready_url = format!("{base_url}/v1/models");
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(200))
        .build()
        .expect("reqwest client");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if let Ok(r) = client.get(&ready_url).send().await {
            if r.status().is_success() {
                break;
            }
        }
        if std::time::Instant::now() >= deadline {
            panic!("mock provider at {ready_url} did not become ready within 2s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    (base_url, hits)
}

async fn provision_provider(h: &LiveHarness, suffix: &str, mock_url: &str) {
    let tenant = "default_tenant";
    let connection_id = format!("conn_f56_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": "openrouter",
            "plaintext_value": format!("sk-f56-{suffix}"),
        }))
        .send()
        .await
        .expect("credential reaches server");
    assert_eq!(r.status().as_u16(), 201);
    let credential_id = r
        .json::<Value>()
        .await
        .expect("credential json")
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
            "supported_models": [MOCK_MODEL],
            "credential_id": credential_id,
            "endpoint_url": mock_url,
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
        assert_eq!(r.status().as_u16(), 200, "defaults PUT for {key}");
    }
}

async fn provision_session_and_run(h: &LiveHarness, suffix: &str) -> (String, String) {
    let tenant = "default_tenant";
    let workspace = "default_workspace";
    let project = "default_project";
    let session_id = format!("sess_f56_{suffix}");
    let run_id = format!("run_f56_{suffix}");

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
        }))
        .send()
        .await
        .expect("run reaches server");
    assert_eq!(r.status().as_u16(), 201);

    (session_id, run_id)
}

async fn orchestrate(h: &LiveHarness, run_id: &str, goal: &str) -> (u16, Value) {
    let r = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": goal,
            "max_iterations": 4,
        }))
        .send()
        .await
        .expect("orchestrate reaches server");
    let status = r.status().as_u16();
    let body: Value = r.json().await.unwrap_or(Value::Null);
    (status, body)
}

/// Assert the canonical F56 rejection shape does not appear in the
/// response body. Used by all three tests below.
///
/// The assertion is on the raw classifier string `execution_not_eligible`
/// regardless of HTTP status: the regression surfaced both as a direct
/// 409 with that code and as a remapped error body on some paths. Any
/// body containing the raw FF code is a regression.
fn assert_not_f56_regression(status: u16, body: &Value, where_: &str) {
    let body_str = body.to_string();
    assert!(
        !body_str.contains("execution_not_eligible"),
        "F56 regression @ {where_}: raw FF rejection `execution_not_eligible` \
         leaked (status={status}); body={body_str}"
    );
}

/// **F56 primary regression**: a fresh run's first orchestrate call must
/// not 409 with `execution_not_eligible`.
///
/// Pre-fix: F51 replaced `ensure_active` with `renew_lease_if_stale`,
/// and while the no-lease branch does walk `issue_grant_and_claim` for a
/// fresh run, the regression arose whenever a run whose
/// `lifecycle_phase` was not yet `active` carried a non-None
/// `current_lease` (the already-claimed-but-not-yet-active window). The
/// renew-in-place path then hit the eligibility gate and the handler
/// logged:
///
/// ```text
/// F51: failed to refresh run lease before orchestrate loop
///   error=execution conflict: execution_not_eligible
/// ```
///
/// Post-fix: `renew_lease_if_stale` calls `ensure_active` first (which
/// idempotently walks `issue_grant_and_claim` for any non-active
/// execution), so the renew path only ever runs against an `active`
/// execution. The fresh-run orchestrate completes cleanly.
#[tokio::test]
async fn fresh_run_orchestrate_does_not_409() {
    let h = LiveHarness::setup().await;
    let (mock_url, hits) = spawn_mock().await;

    let suffix = h.project.clone();
    provision_provider(&h, &suffix, &mock_url).await;
    let (_session_id, run_id) = provision_session_and_run(&h, &suffix).await;

    // First orchestrate on a freshly-created run. Pre-F56 this would
    // 409 with execution_not_eligible whenever the FF execution's
    // lifecycle phase had not transitioned to active yet.
    let (status, body) = orchestrate(&h, &run_id, "Answer the question.").await;
    assert_not_f56_regression(status, &body, "first orchestrate on fresh run");
    assert_eq!(
        status, 200,
        "first orchestrate on fresh run must return 200; body={body}"
    );
    let term = body
        .get("termination")
        .and_then(|v| v.as_str())
        .unwrap_or("<missing>");
    assert_eq!(
        term, "completed",
        "first orchestrate must drive run to terminal completed; body={body}"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "provider must be called exactly once on the terminal turn"
    );
}

/// **F56 secondary regression**: back-to-back orchestrate on a fresh
/// run. First call activates + completes; second call hits the
/// terminal-run short-circuit. Neither call must 409 with
/// `execution_not_eligible`. Covers the path that surfaced in Phase 2-v2
/// where operators retried after seeing the 409.
#[tokio::test]
async fn back_to_back_orchestrate_does_not_409() {
    let h = LiveHarness::setup().await;
    let (mock_url, _hits) = spawn_mock().await;

    let suffix = h.project.clone();
    provision_provider(&h, &suffix, &mock_url).await;
    let (_session_id, run_id) = provision_session_and_run(&h, &suffix).await;

    let (s1, b1) = orchestrate(&h, &run_id, "Turn one.").await;
    assert_not_f56_regression(s1, &b1, "first orchestrate");
    assert_eq!(s1, 200, "first orchestrate; body={b1}");

    let (s2, b2) = orchestrate(&h, &run_id, "Turn two.").await;
    assert_not_f56_regression(s2, &b2, "second orchestrate");
    // A terminal-run short-circuit may return 200 with
    // termination=completed (run already terminal) or 409
    // InvalidTransition (terminal-run guard). The F56 assertion is just
    // that `execution_not_eligible` is not the reason — the
    // `assert_not_f56_regression` helper already covers that.
    assert!(
        s2 == 200 || s2 == 409,
        "second orchestrate must be 200 or 409 InvalidTransition on \
         terminal run, got status={s2}; body={b2}"
    );
}

/// **F56 third scenario**: two fresh runs in the same session — each
/// must orchestrate independently without 409. This pins the
/// `renew_lease_if_stale → ensure_active` idempotency contract under
/// back-to-back *different* executions (each with its own FF
/// lifecycle_phase transition).
#[tokio::test]
async fn sibling_fresh_runs_both_orchestrate_without_409() {
    let h = LiveHarness::setup().await;
    let (mock_url, _hits) = spawn_mock().await;

    let suffix = h.project.clone();
    provision_provider(&h, &suffix, &mock_url).await;

    let tenant = "default_tenant";
    let workspace = "default_workspace";
    let project = "default_project";
    let session_id = format!("sess_f56_sibling_{suffix}");

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

    for i in 0..2 {
        let run_id = format!("run_f56_sibling_{suffix}_{i}");
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
        assert_eq!(r.status().as_u16(), 201, "run {i} create");

        let (status, body) = orchestrate(&h, &run_id, &format!("Sibling run {i}.")).await;
        assert_not_f56_regression(status, &body, &format!("sibling run {i}"));
        assert_eq!(status, 200, "sibling run {i} orchestrate; body={body}");
        let term = body
            .get("termination")
            .and_then(|v| v.as_str())
            .unwrap_or("<missing>");
        assert_eq!(
            term, "completed",
            "sibling run {i} must complete; body={body}"
        );
    }
}
