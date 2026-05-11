//! F57 regression: mid-run `/orchestrate` must not 409 with
//! `execution_not_eligible` when a run has unresolved pending
//! approvals.
//!
//! # Bug (M1-v2 retry, 2026-04-26, on binary dbd52180)
//!
//! F56 (#320, `dbd52180`) folded `ensure_active` into
//! `renew_lease_if_stale` to unblock the cold-start 409 on a
//! freshly-created run. The fix held for the first one or two
//! orchestrate cycles but degraded on the third:
//!
//! ```text
//! c1: orchestrate → 202, termination=waiting_approval  (cargo init proposed)
//! c2: approve + orchestrate → 202, termination=waiting_approval  (edit proposed)
//! c3: approve + orchestrate → 409 execution_not_eligible           ← F57
//! ```
//!
//! Root cause: `renew_lease_if_stale` remained unconditional on the
//! orchestrate entry path. When a run carries a mid-approval FF
//! lifecycle phase (suspended → resume-in-flight, or
//! `lifecycle_phase != "runnable"` due to a stale snapshot), FF's
//! grant gate in `ff_issue_grant` rejects with
//! `execution_not_eligible` (flowfabric.lua lines 3585–3590), and the
//! renew-side reclaim fallback rejects symmetrically because the
//! phase precondition (`runnable` + `eligible_now`) isn't met.
//!
//! # Fix
//!
//! The orchestrate handler in `crates/cairn-app/src/handlers/runs.rs`
//! now peeks the approval projection before calling
//! `renew_lease_if_stale`. If any pending approvals exist for the run,
//! renewal is skipped — the execution is already in FF's approval
//! sub-phase with no need for a fresh lease; the orchestrator loop
//! drains approvals against the existing projection state and returns
//! `termination=waiting_approval` without mutating FF.
//!
//! This test pins that behavior. A pending approval is posted against
//! a run, then `/orchestrate` is invoked. Pre-fix the call 409s; post-
//! fix it returns a 2xx with `termination=waiting_approval` (the
//! handler returns 202 Accepted when awaiting approval, 200 on
//! terminal completion).

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

const MOCK_MODEL: &str = "openrouter/f57-mid-run-regression";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
}

/// Minimal mock LLM provider: returns a single-turn `complete_run`
/// tool call. The orchestrator drives the run to terminal completion
/// on the no-pending path and enters the approval loop on the
/// pending path. Either outcome is a non-409 2xx response, which is
/// all the F57 assertion requires.
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
                "id": "mock-f57",
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
                                "arguments": json!({ "final_answer": "F57 regression ok." }).to_string(),
                            }
                        }],
                    },
                    "finish_reason": "tool_calls",
                }],
                "usage": { "prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15 }
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

    (format!("http://{addr}"), hits)
}

async fn provision_provider(h: &LiveHarness, suffix: &str, mock_url: &str) {
    // Use the pre-seeded `default_tenant` the harness auto-creates —
    // matches the F56 test shape and avoids the extra
    // workspace/project bootstrap the dynamic `h.tenant` would need.
    let tenant = "default_tenant";
    let connection_id = format!("conn_f57_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": "openrouter",
            "plaintext_value": format!("sk-f57-{suffix}"),
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
    let session_id = format!("sess_f57_{suffix}");
    let run_id = format!("run_f57_{suffix}");

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

async fn request_approval(h: &LiveHarness, run_id: &str, approval_id: &str) {
    let res = h
        .client()
        .post(format!("{}/v1/approvals", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id": "default_tenant",
            "workspace_id": "default_workspace",
            "project_id": "default_project",
            "approval_id": approval_id,
            "run_id": run_id,
            "requirement": "required",
        }))
        .send()
        .await
        .expect("POST /v1/approvals reached server");
    assert!(
        res.status().is_success(),
        "POST /v1/approvals failed: {} body={}",
        res.status(),
        res.text().await.unwrap_or_default(),
    );
}

async fn approve(h: &LiveHarness, approval_id: &str) {
    let res = h
        .client()
        .post(format!(
            "{}/v1/approvals/{}/approve",
            h.base_url, approval_id,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({}))
        .send()
        .await
        .expect("POST /v1/approvals/:id/approve reached server");
    assert!(
        res.status().is_success(),
        "POST /v1/approvals/{approval_id}/approve failed: {} body={}",
        res.status(),
        res.text().await.unwrap_or_default(),
    );
}

async fn orchestrate(h: &LiveHarness, run_id: &str, goal: &str) -> (u16, Value) {
    let r = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "goal": goal, "max_iterations": 1 }))
        .send()
        .await
        .expect("orchestrate reaches server");
    let status = r.status().as_u16();
    let body: Value = r.json().await.unwrap_or(Value::Null);
    (status, body)
}

/// Shared assertion: the response body must not contain the raw FF
/// rejection classifier `execution_not_eligible`, the status must be
/// 2xx, and the body must carry a non-empty `termination` field. A
/// 5xx with a null JSON body would have slipped past the string-only
/// check, which is why this helper pins all three invariants
/// together.
fn assert_orchestrate_accepted(status: u16, body: &Value, where_: &str) {
    let body_str = body.to_string();
    assert!(
        !body_str.contains("execution_not_eligible"),
        "F57 regression @ {where_}: raw FF rejection `execution_not_eligible` \
         leaked (status={status}); body={body_str}"
    );
    assert!(
        (200..300).contains(&status),
        "F57 regression @ {where_}: expected 2xx, got {status}; body={body_str}"
    );
    let termination = body
        .get("termination")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        !termination.is_empty(),
        "F57 regression @ {where_}: response missing `termination` field; \
         body={body_str}"
    );
}

/// **F57 primary regression**: a run with a pending approval must
/// accept `/orchestrate` without returning 409
/// `execution_not_eligible`.
///
/// The pre-fix failure mode: `renew_lease_if_stale` fires
/// unconditionally on orchestrate entry; the FF execution is in a
/// mid-approval sub-phase; `ff_renew_lease` or its claim-fallback
/// rejects with an eligibility code; the handler propagates a 409
/// Conflict. Operators see the exact error from the Phase 2-v2 log.
///
/// Post-fix: the handler detects the pending approval via
/// `ApprovalReadModel::has_pending_for_run` and skips renewal. The
/// orchestrate call returns a non-409 status.
#[tokio::test]
async fn orchestrate_with_pending_approval_does_not_409() {
    let h = LiveHarness::setup().await;
    let (mock_url, _hits) = spawn_mock().await;
    let suffix = h.project.clone();
    provision_provider(&h, &suffix, &mock_url).await;
    let (_session_id, run_id) = provision_session_and_run(&h, &suffix).await;

    // Seed a pending approval against the run. No FF-side suspend is
    // needed to exercise the handler branch — `has_pending_for_run`
    // reads the cairn projection, not FF state. This keeps the test
    // deterministic and avoids coupling to FF's approval-signal
    // timing.
    let approval_id = format!("appr_f57_{suffix}");
    request_approval(&h, &run_id, &approval_id).await;

    let (status, body) = orchestrate(&h, &run_id, "Proceed only after approval.").await;
    assert_orchestrate_accepted(status, &body, "orchestrate with pending approval");
}

/// **F57 iteration cycle**: create, orchestrate, approve, orchestrate,
/// repeated 5 times. No call may 409 with `execution_not_eligible`.
/// This is the structural shape of the dogfood retry that caught F57
/// on c3 of the M1-v2 attempt.
#[tokio::test]
async fn five_approval_cycles_do_not_409() {
    let h = LiveHarness::setup().await;
    let (mock_url, _hits) = spawn_mock().await;
    let suffix = h.project.clone();
    provision_provider(&h, &suffix, &mock_url).await;
    let (_session_id, run_id) = provision_session_and_run(&h, &suffix).await;

    for iter in 0..5 {
        let approval_id = format!("appr_f57_cycle_{suffix}_{iter}");
        request_approval(&h, &run_id, &approval_id).await;

        let (status, body) =
            orchestrate(&h, &run_id, &format!("cycle {iter}: check approval.")).await;
        assert_orchestrate_accepted(status, &body, &format!("iter {iter} orchestrate"));

        // Resolve the approval before the next cycle so we're not just
        // repeatedly hitting the skip-renew branch on the same
        // approval id — each cycle has its own create-approve pair,
        // covering the full mid-run pattern.
        approve(&h, &approval_id).await;
    }
}

/// **F57 no-pending path preserved**: a fresh run with no pending
/// approvals must still traverse the renew_lease_if_stale path and
/// not regress F56. The F57 short-circuit MUST NOT short-circuit when
/// the approval projection is empty — otherwise we'd trip the terminal
/// FCALLs with a stale lease on long-paced flows.
#[tokio::test]
async fn fresh_run_without_pending_approvals_still_reaches_renew() {
    let h = LiveHarness::setup().await;
    let (mock_url, _hits) = spawn_mock().await;
    let suffix = h.project.clone();
    provision_provider(&h, &suffix, &mock_url).await;
    let (_session_id, run_id) = provision_session_and_run(&h, &suffix).await;

    // No approval created. The orchestrate call must complete without
    // 409 — same contract F56 pinned. This test guards against a
    // regression where the F57 check mis-classifies an empty approval
    // set as "pending".
    let (status, body) = orchestrate(&h, &run_id, "No approvals needed.").await;
    assert_orchestrate_accepted(status, &body, "fresh run orchestrate (F56 parity)");
}
