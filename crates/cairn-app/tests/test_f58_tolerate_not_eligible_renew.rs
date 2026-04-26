//! F58 regression: orchestrate must tolerate transient FF phase-conflict
//! codes (`execution_not_eligible` / `execution_not_eligible_for_attempt`)
//! on the entry-time `renew_lease_if_stale` call, falling through into
//! the orchestrate loop with the existing lease instead of returning 409.
//!
//! # Bug (M1-v2 dogfood retry, 2026-04-26, on F57 binary `33836b03`)
//!
//! F56 (#320) and F57 (#321) progressively expanded the conditions under
//! which the orchestrate handler skips lease renewal. F57's fix peeks
//! the approval projection and skips renew when a pending approval
//! exists. That closed the observed c3-cycle 409 on bash-only flows.
//!
//! A dogfood retry under the F57 fix hit a different symptom:
//!
//! ```text
//! c1 orchestrate        → bash mkdir proposed     → 202 waiting_approval
//! c2 approve + orchestrate → bash ls|grep proposed  → 202 waiting_approval
//! c3 approve + orchestrate → bash ls -la proposed   → 202 waiting_approval
//! c4 approve + orchestrate → write Cargo.toml       → 202 waiting_approval
//! c5 approve + orchestrate → 409 execution_not_eligible        ← F58
//! ```
//!
//! The distinguishing variable: c5 followed approval of a `write` tool.
//! Every prior approval was for `bash`. After FF records the tool
//! invocation (especially `write`, which mutates cairn's workspace
//! projection before returning), FF briefly holds the execution's
//! `lifecycle_phase` off `runnable`. The next orchestrate call enters,
//! F57's has-pending check returns false (the approval was just drained
//! or the projection hasn't caught up), the handler calls
//! `renew_lease_if_stale`, and FF rejects with `execution_not_eligible`.
//!
//! # Fix
//!
//! The orchestrate handler now catches `is_transient_phase_conflict()`
//! from `renew_lease_if_stale`, logs at WARN, and proceeds into the
//! orchestrate loop with the existing lease. The loop's own
//! `is_lease_healthy()` gate catches an actually-dead lease. Narrow
//! classifier — only `execution_not_eligible` and
//! `execution_not_eligible_for_attempt` are tolerated; permanent codes
//! (`execution_not_active`, `lease_expired`, `execution_not_found`,
//! `grant_already_exists`, …) still propagate as 409 as before.
//!
//! See also:
//! * `RuntimeError::is_transient_phase_conflict` —
//!   `crates/cairn-runtime/src/error.rs`
//! * FF upstream ask —
//!   `docs/design/ff-upstream/ff-execution-phase-probe.md`

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

const MOCK_MODEL: &str = "openrouter/f58-tolerate-not-eligible";

/// A mock LLM whose response alternates between proposing a `bash` tool
/// call and proposing a `write` tool call. The 5-cycle test alternates
/// so the sequence includes at least one write-after-bash and one
/// bash-after-write — the exact transition shape the dogfood retry hit.
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
        // Cycle, with 5-hit period, so the 5-cycle regression test
        // sees: bash → write → bash → write → complete (matching the
        // M1-v2 dogfood sequence of 4 tool approvals then a
        // terminator). Using `n % 5` keeps the cycle explicit.
        let n = state.hits.fetch_add(1, Ordering::SeqCst);
        let (name, args) = match n % 5 {
            0 | 2 => ("bash", json!({ "command": format!("echo f58 iter {n}") })),
            1 | 3 => (
                "write",
                json!({
                    "path": format!("f58_out_{n}.txt"),
                    "content": format!("iter {n}\n"),
                }),
            ),
            _ => ("complete_run", json!({ "final_answer": "F58 cycle ok." })),
        };
        (
            StatusCode::OK,
            Json(json!({
                "id": format!("mock-f58-{n}"),
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": null,
                        "tool_calls": [{
                            "id": format!("call_f58_{n}"),
                            "type": "function",
                            "function": {
                                "name": name,
                                "arguments": args.to_string(),
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
    let tenant = "default_tenant";
    let connection_id = format!("conn_f58_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": "openrouter",
            "plaintext_value": format!("sk-f58-{suffix}"),
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
    let session_id = format!("sess_f58_{suffix}");
    let run_id = format!("run_f58_{suffix}");

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

/// Shared assertion: the orchestrate response body must not contain the
/// raw FF rejection classifier `execution_not_eligible`, the status must
/// be 2xx, and the body must carry a non-empty `termination` field.
fn assert_orchestrate_accepted(status: u16, body: &Value, where_: &str) {
    let body_str = body.to_string();
    // Both codes F58 tolerates must be absent from the response body —
    // `execution_not_eligible_for_attempt` is the attempt-axis shape
    // of the same class and the regression would leak it identically
    // if the tolerate branch were narrowed.
    for leak in [
        "execution_not_eligible",
        "execution_not_eligible_for_attempt",
    ] {
        assert!(
            !body_str.contains(leak),
            "F58 regression @ {where_}: raw FF rejection `{leak}` \
             leaked (status={status}); body={body_str}"
        );
    }
    assert!(
        (200..300).contains(&status),
        "F58 regression @ {where_}: expected 2xx, got {status}; body={body_str}"
    );
    let termination = body
        .get("termination")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        !termination.is_empty(),
        "F58 regression @ {where_}: response missing `termination` field; \
         body={body_str}"
    );
}

/// **F58 primary regression**: five approval cycles that mix bash
/// and write tool approvals. Critically, each orchestrate call is
/// made with the prior cycle's approval already **resolved** (drained
/// from the pending projection). This means `has_pending_for_run`
/// returns `false` on entry and the handler takes the
/// `renew_lease_if_stale` path — the exact path F58 needs to cover.
/// Seeding a fresh pending approval before every orchestrate (as F57's
/// test does) would deterministically hit F57's fast-path skip and
/// never exercise F58's tolerate branch.
///
/// This is the exact shape of the M1-v2 dogfood retry that caught F58
/// on c5 after a `write` approval — the operator approves, the write
/// drains from the pending set, then the next orchestrate enters with
/// `has_pending=false` and hits the renew path mid-phase-transition.
#[tokio::test]
async fn five_cycle_mixed_bash_write_does_not_409() {
    let h = LiveHarness::setup().await;
    let (mock_url, _hits) = spawn_mock().await;
    let suffix = h.project.clone();
    provision_provider(&h, &suffix, &mock_url).await;
    let (_session_id, run_id) = provision_session_and_run(&h, &suffix).await;

    for iter in 0..5 {
        // Orchestrate FIRST — this is the call that must take the
        // `renew_lease_if_stale` path (no pending approval on entry).
        // On iter 0 there's been no prior approval at all; on later
        // iters the previous cycle's approval was resolved below,
        // leaving the pending set empty again.
        let (status, body) = orchestrate(
            &h,
            &run_id,
            &format!("F58 cycle {iter}: mixed bash+write approvals."),
        )
        .await;
        assert_orchestrate_accepted(status, &body, &format!("iter {iter} orchestrate"));

        // Seed + resolve an approval so the projection returns to
        // empty for the next iteration's orchestrate entry. This
        // mimics the dogfood pattern (propose → approve → repeat)
        // without locking each cycle into the F57 fast path.
        let approval_id = format!("appr_f58_{suffix}_{iter}");
        request_approval(&h, &run_id, &approval_id).await;
        approve(&h, &approval_id).await;
    }
}

/// **F58 non-masking invariant**: a fresh orchestrate on a run that
/// was cancelled under the caller's feet must still return a
/// user-visible error — the tolerate path is narrow (eligibility
/// codes only); permanent 409/404 classes still propagate.
///
/// This guards the "don't swallow real errors" axis. If a future
/// refactor widens `is_transient_phase_conflict` to include terminal
/// codes, this test fires.
#[tokio::test]
async fn orchestrate_on_cancelled_run_still_surfaces_error() {
    let h = LiveHarness::setup().await;
    let (mock_url, _hits) = spawn_mock().await;
    let suffix = h.project.clone();
    provision_provider(&h, &suffix, &mock_url).await;
    let (_session_id, run_id) = provision_session_and_run(&h, &suffix).await;

    // Cancel the run so the next orchestrate must hit the terminal
    // guard OR a permanent FF-state rejection path (both are correct
    // non-masking outcomes; we assert on the negative — the tolerate
    // branch MUST NOT swallow this).
    let r = h
        .client()
        .post(format!("{}/v1/runs/{}/cancel", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({}))
        .send()
        .await
        .expect("cancel reaches server");
    assert!(
        r.status().is_success(),
        "cancel failed: {} body={}",
        r.status(),
        r.text().await.unwrap_or_default(),
    );

    let (status, body) = orchestrate(&h, &run_id, "should not proceed").await;
    let body_str = body.to_string();
    // Either the terminal guard returns 200 with termination=canceled
    // (healthy, operator-friendly outcome) or the FF path returns 4xx.
    // In both cases the body carries a non-empty `termination` or an
    // error code — but crucially the run did NOT enter the orchestrate
    // loop as if nothing happened. A swallowed-cancel would look like a
    // fresh 202 waiting_approval, which we explicitly forbid.
    let termination = body
        .get("termination")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let is_terminal = termination == "canceled"
        || termination == "cancelled"
        || termination == "failed"
        || termination == "completed";
    let is_error_status = !(200..300).contains(&status);
    assert!(
        is_terminal || is_error_status,
        "F58 non-masking: orchestrate on a cancelled run must not silently \
         re-enter the loop. status={status} body={body_str}"
    );
    assert!(
        !(status == 202 && termination == "waiting_approval"),
        "F58 non-masking: cancelled run must not surface as \
         waiting_approval — tolerate branch may be over-broad. \
         status={status} body={body_str}"
    );
}

/// **F57 regression guard**: a pending-approval run still skips renew
/// (fast path). This test re-exercises F57's short-circuit inside the
/// F58 test file so the combined behavior (skip-pending *or*
/// tolerate-eligibility) is pinned in one place.
#[tokio::test]
async fn f57_pending_skip_still_works_under_f58() {
    let h = LiveHarness::setup().await;
    let (mock_url, _hits) = spawn_mock().await;
    let suffix = h.project.clone();
    provision_provider(&h, &suffix, &mock_url).await;
    let (_session_id, run_id) = provision_session_and_run(&h, &suffix).await;

    let approval_id = format!("appr_f58_f57guard_{suffix}");
    request_approval(&h, &run_id, &approval_id).await;

    let (status, body) = orchestrate(&h, &run_id, "F57 guard under F58").await;
    assert_orchestrate_accepted(status, &body, "F57 pending-skip under F58");
}

/// **F56 regression guard**: a freshly-created run with no pending
/// approvals still reaches the renew path and succeeds. F58 only
/// widens the tolerate set on the error-handling side; the happy-path
/// renew must still run and succeed on a healthy cold-start run.
#[tokio::test]
async fn f56_cold_start_still_works_under_f58() {
    let h = LiveHarness::setup().await;
    let (mock_url, _hits) = spawn_mock().await;
    let suffix = h.project.clone();
    provision_provider(&h, &suffix, &mock_url).await;
    let (_session_id, run_id) = provision_session_and_run(&h, &suffix).await;

    let (status, body) = orchestrate(&h, &run_id, "F56 cold-start under F58").await;
    assert_orchestrate_accepted(status, &body, "F56 cold-start under F58");
}
