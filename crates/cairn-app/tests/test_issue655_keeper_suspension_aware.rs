//! #655 regression: multi-iteration approval-gated runs must not
//! flip to `Failed(TerminalWriteDeadlock)` when each operator-paced
//! approval round lasts longer than `lease_ttl_ms`.
//!
//! # Dogfood round 3 (2026-05-03)
//!
//! Post-PR-#647 (#639 background lease keeper shipped) the dogfood
//! roguelike-building run reproduced the failure shape anyway:
//!
//! * 3 operator approval rounds, ~15-20 s per click
//! * 3 LLM iterations, 5 tool dispatches
//! * 3 min 24 s wall time — well under the 900 s breaker
//! * Result: `Failed(TerminalWriteDeadlock)` on the terminal FCALL
//!
//! Log evidence from `/tmp/cairn-dogfood-r3.log`:
//!
//! ```text
//! 16:31:55.655 DEBUG #639 lease keeper hit transient phase conflict, retrying
//!   run_id=run_roguelike_r3_... error=execution conflict: execution_not_eligible
//! ```
//!
//! Every keeper tick during the approval wait churned FF with
//! `execution_not_eligible` — FF's phase machine puts an execution
//! into `waiting_approval`/`attempt_interrupted` while a tool-call
//! approval is pending, and `ff_renew_lease` rejects in that phase.
//! The lease wall-clock expired while the keeper flailed. When cairn
//! dispatched the terminal `ff_complete_execution`, FF returned
//! `lease_expired`; the F64 bounded recovery loop then chewed through
//! its 30 s ceiling trying to reclaim an execution FF still reported
//! as not-eligible, and the run flipped to
//! `Failed(TerminalWriteDeadlock)`.
//!
//! # The fix
//!
//! [`crate::lease_keeper`] now observes the approval projection at
//! every tick. When any pending `ToolCallApproval` row or
//! `ApprovalRequested` exists for the run, the keeper SKIPS the
//! renew FCALL entirely. When the keeper observes the transition
//! from pending → resolved, it fires an immediate renew —
//! `RunService::renew_lease_if_stale` falls back to a full
//! `issue_grant_and_claim` when the wall-clock has expired, which
//! mints a fresh lease before the orchestrator drives the next
//! terminal FCALL.
//!
//! # This test
//!
//! End-to-end HTTP reproduction of the dogfood shape:
//!
//! 1. LiveHarness with `CAIRN_FABRIC_LEASE_TTL_MS = 5000`. Short
//!    enough that the keeper's `lease_ttl_ms / 3 ≈ 1.67 s` tick
//!    fires multiple times inside each 4-second approval-pending
//!    window, so the suspension-probe skip logic is exercised on
//!    every tick.
//! 2. Mock provider that emits TWO successive approval-gated
//!    `bash` tool calls across two iterations, then a
//!    `complete_run` terminator on the third turn.
//! 3. Each `/orchestrate` returns 202 `waiting_approval`. The test
//!    sleeps 4 s before calling `/approve`.
//! 4. During each suspension the keeper observes the projection
//!    and SKIPS renew FCALLs. Pre-#655 the keeper would emit a
//!    renew FCALL every 1.67 s that FF rejected with
//!    `execution_not_eligible` (log evidence in the issue). On
//!    the observed `ToolCallApproved` transition post-#655 the
//!    keeper fires an immediate renew that falls back to
//!    `claim_with_snapshot` when the lease wall-clock has
//!    expired — restoring a fresh TTL before the next terminal
//!    FCALL. Belt-and-suspenders: `f59_prelude_renew` in the
//!    fabric adapter gained a short retry schedule for
//!    `execution_not_eligible` so FF's phase machine has time to
//!    clear any residual waiting-approval state before the
//!    terminal FCALL fires.
//! 5. Final assertion: the run reaches a clean terminal state —
//!    `completed` OR `failed` — but MUST NOT carry
//!    `failure_class = terminal_write_deadlock`.
//!
//! # Pre-fix reproduction
//!
//! The `is_suspended` skip-logic in
//! `crates/cairn-app/src/lease_keeper.rs` is load-bearing: the
//! sibling unit tests `keeper_skips_renew_while_tool_call_approval_pending`
//! and `keeper_force_renews_on_suspension_resolution` both **fail**
//! pre-fix with non-zero `renew_calls` during the suspension
//! window (locally verified by short-circuiting `is_suspended` to
//! `false`; the tests reported `renews=3 expected=0` (spam) and
//! `renews=0 expected >= 1` (missed immediate-on-resume renew)
//! respectively).
//!
//! This integration test is the end-to-end guard that the full
//! HTTP propose-suspend-approve-resume-terminate path does not
//! flip to `Failed(TerminalWriteDeadlock)` under realistic timing
//! where the keeper ticks multiple times inside each suspension
//! window. The per-projection skip is verified at the unit level
//! where the renew count is directly observable.

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

const MOCK_MODEL: &str = "openrouter/issue655-suspension-aware";
/// Wall-time spent holding the run in `waiting_approval` on each
/// round. Must exceed `CAIRN_FABRIC_LEASE_TTL_MS / 3` (the keeper
/// tick interval) so at least one keeper tick fires inside each
/// window. At 5-s TTL the interval is 1.67 s; a 4-s wait gives
/// 2–3 keeper ticks per suspension — pre-#655 all would emit
/// renew FCALLs that FF rejects with `execution_not_eligible`;
/// post-#655 all short-circuit on the suspension probe.
const APPROVAL_WAIT: Duration = Duration::from_secs(4);

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
    /// Number of `requires_approval=true` proposals emitted so far.
    /// Each propose-suspend round advances the counter exactly once,
    /// so the terminator fires on the round AFTER the target count
    /// regardless of how many times the LLM is called during a
    /// drain-plus-propose orchestrate turn.
    approval_rounds: Arc<AtomicUsize>,
}

/// Mock provider: the first two proposals with `requires_approval=true`
/// drive two separate approval rounds; every subsequent call returns a
/// terminator (`complete_run`) so the third orchestrate drives the
/// terminal FCALL.
async fn spawn_mock() -> (String, Arc<AtomicUsize>) {
    let state = MockState {
        hits: Arc::new(AtomicUsize::new(0)),
        approval_rounds: Arc::new(AtomicUsize::new(0)),
    };
    let hits = state.hits.clone();

    async fn chat_handler(
        State(state): State<MockState>,
        Json(_body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        let n = state.hits.fetch_add(1, Ordering::SeqCst);
        let round = state.approval_rounds.load(Ordering::SeqCst);
        // Two approval-gated rounds, then a terminator. Counting by
        // approval rounds (not raw hits) keeps the test
        // deterministic when the orchestrator invokes the LLM
        // twice within a single POST (drain → LLM → proposal →
        // suspend).
        let content_json = if round < 2 {
            state.approval_rounds.fetch_add(1, Ordering::SeqCst);
            json!([{
                "action_type": "invoke_tool",
                "description": format!("issue-655 round {round}: safe readonly bash probe (hit {n})"),
                "confidence": 0.99,
                "tool_name": "bash",
                "tool_args": {
                    "command": format!("echo issue-655-round-{round}")
                },
                "requires_approval": true
            }])
        } else {
            json!([{
                "action_type": "complete_run",
                "description": "all approval rounds cleared — closing out",
                "confidence": 0.99,
                "requires_approval": false
            }])
        };

        (
            StatusCode::OK,
            Json(json!({
                "id": format!("mock-655-{n}"),
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": content_json.to_string(),
                    },
                    "finish_reason": "stop",
                }],
                "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
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
    // Wait for the mock to be reachable so the provider probe below
    // doesn't race the TCP bind.
    let ready_url = format!("{base_url}/v1/models");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(200))
        .build()
        .expect("reqwest client");
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        if let Ok(r) = client.get(&ready_url).send().await {
            if r.status().is_success() {
                break;
            }
        }
        if std::time::Instant::now() >= deadline {
            panic!("mock provider at {ready_url} did not become ready within 2s");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    (base_url, hits)
}

async fn provision_provider(h: &LiveHarness, suffix: &str, mock_url: &str) {
    let tenant = "default_tenant";
    let connection_id = format!("conn_655_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": "openrouter",
            "plaintext_value": format!("sk-655-{suffix}"),
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
    let session_id = format!("sess_655_{suffix}");
    let run_id = format!("run_655_{suffix}");

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

    (session_id, run_id)
}

/// POST `/v1/runs/:id/orchestrate` with a generous client timeout —
/// the pre-fix F64 recovery path can add up to ~30 s on the final
/// deadlocked iteration. We buffer ~60 s so the test's assertion
/// reads the full response body instead of timing out mid-request.
async fn orchestrate(h: &LiveHarness, run_id: &str) -> (u16, Value) {
    let long_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .expect("long-timeout reqwest client");
    let r = long_client
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "run three approval-gated bash calls to prove the keeper \
                    survives per-iteration approval waits",
            "max_iterations": 8,
            "approval_timeout_ms": 60_000u64,
        }))
        .send()
        .await
        .expect("orchestrate reaches server");
    let status = r.status().as_u16();
    let body: Value = r.json().await.unwrap_or(Value::Null);
    (status, body)
}

/// Poll the pending tool-call-approvals projection until a row lands
/// for `run_id` — returns the `call_id`. The projection write is
/// asynchronous w.r.t. the orchestrate HTTP response; a bounded poll
/// is the deterministic way to handshake.
async fn wait_for_pending_call_id(
    h: &LiveHarness,
    tenant: &str,
    workspace: &str,
    project: &str,
    run_id: &str,
    timeout: Duration,
) -> String {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        let r = h
            .client()
            .get(format!(
                "{}/v1/tool-call-approvals?run_id={}&state=pending",
                h.base_url, run_id
            ))
            .header("X-Cairn-Tenant", tenant)
            .header("X-Cairn-Workspace", workspace)
            .header("X-Cairn-Project", project)
            .bearer_auth(&h.admin_token)
            .send()
            .await
            .expect("list tool-call-approvals reaches server");
        if r.status().as_u16() == 200 {
            let body: Value = r.json().await.expect("list json");
            let items = body
                .get("items")
                .and_then(|v| v.as_array())
                .cloned()
                .or_else(|| body.as_array().cloned())
                .unwrap_or_default();
            if let Some(first) = items.first() {
                if let Some(cid) = first.get("call_id").and_then(|v| v.as_str()) {
                    return cid.to_owned();
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no pending tool-call approval appeared for run={run_id} within {timeout:?}");
}

async fn approve(h: &LiveHarness, tenant: &str, workspace: &str, project: &str, call_id: &str) {
    let r = h
        .client()
        .post(format!(
            "{}/v1/tool-call-approvals/{}/approve",
            h.base_url, call_id
        ))
        .header("X-Cairn-Tenant", tenant)
        .header("X-Cairn-Workspace", workspace)
        .header("X-Cairn-Project", project)
        .bearer_auth(&h.admin_token)
        .json(&json!({"scope": {"type": "once"}}))
        .send()
        .await
        .expect("approve reaches server");
    assert_eq!(
        r.status().as_u16(),
        200,
        "approve for call_id={call_id}: {}",
        r.text().await.unwrap_or_default()
    );
}

/// **#655 primary regression**: two operator-paced approval rounds
/// plus a terminator must NOT flip the run to
/// `Failed(TerminalWriteDeadlock)`.
///
/// The test runs the full dogfood shape end-to-end — propose, wait,
/// approve, re-orchestrate — twice in sequence, then the third
/// orchestrate drives the terminator. Pre-fix the keeper's
/// `execution_not_eligible` renew spam during each approval wait
/// pollutes logs and burns FF round-trips; on loaded CI it can still
/// race the lease wall-clock past expiry and trip F64. Post-fix the
/// keeper skips FCALLs while pending and force-renews on resume,
/// keeping the lease alive through the full flow.
#[tokio::test]
async fn multi_round_approval_does_not_wedge_run() {
    let h = LiveHarness::setup_with_env(&[("CAIRN_FABRIC_LEASE_TTL_MS", "5000")]).await;
    let (mock_url, _hits) = spawn_mock().await;

    let tenant = "default_tenant";
    let workspace = "default_workspace";
    let project = "default_project";
    let suffix = h.project.clone();
    provision_provider(&h, &suffix, &mock_url).await;
    let (_session_id, run_id) = provision_session_and_run(&h, &suffix).await;

    // ── Round 1 ─────────────────────────────────────────────────────────
    let (status, body) = orchestrate(&h, &run_id).await;
    assert_eq!(
        status, 202,
        "round 1: expected 202 waiting_approval, got {status}: {body}"
    );
    let call_id_1 = wait_for_pending_call_id(
        &h,
        tenant,
        workspace,
        project,
        &run_id,
        Duration::from_secs(5),
    )
    .await;
    // Hold the suspension. Pre-fix the keeper churns
    // `execution_not_eligible` through this window, adding load +
    // noise. Post-fix the keeper observes the projection and is
    // idle.
    tokio::time::sleep(APPROVAL_WAIT).await;
    approve(&h, tenant, workspace, project, &call_id_1).await;

    // ── Round 2 ─────────────────────────────────────────────────────────
    //
    // The F49 auto-resume kick fires an orchestrate POST in
    // parallel with the test's POST below. On fast test runs the
    // F49 handler can walk the loop past the second approval
    // proposal into the drain + terminator, so the test's POST may
    // legitimately observe `200 completed` instead of `202
    // waiting_approval`. Both outcomes PROVE the fix: the run
    // reached a clean terminal state without `terminal_write_deadlock`.
    //
    // Only the 202 branch issues a follow-up approve; a 200
    // branch short-circuits to the terminal-state assertion.
    let (status, body) = orchestrate(&h, &run_id).await;
    let body_str = body.to_string();
    assert!(
        matches!(status, 200 | 202 | 409),
        "round 2 orchestrate must return 200/202/409; got {status}: {body_str}"
    );
    if status == 202 {
        let call_id_2 = wait_for_pending_call_id(
            &h,
            tenant,
            workspace,
            project,
            &run_id,
            Duration::from_secs(5),
        )
        .await;
        tokio::time::sleep(APPROVAL_WAIT).await;
        approve(&h, tenant, workspace, project, &call_id_2).await;

        // ── Terminator ──────────────────────────────────────────────────
        // Third orchestrate: the drain runs the second approved
        // bash call, then the mock returns `complete_run`. The
        // terminal `ff_complete_execution` must accept the lease
        // the keeper kept fresh.
        let (status, body) = orchestrate(&h, &run_id).await;
        let body_str = body.to_string();
        assert!(
            matches!(status, 200 | 202 | 409),
            "terminator orchestrate must return 200/202/409; got {status}; body={body_str}"
        );
    }

    // Poll the run state until terminal. 30 s covers F64's 30 s
    // bounded recovery loop if it engages on a tight race — the
    // assertion below still rejects `terminal_write_deadlock`.
    let deadline = std::time::Instant::now() + Duration::from_secs(45);
    let (final_state, final_failure_class) = loop {
        let r = h
            .client()
            .get(format!("{}/v1/runs/{}", h.base_url, run_id))
            .bearer_auth(&h.admin_token)
            .send()
            .await
            .expect("run fetch reaches server");
        assert_eq!(r.status().as_u16(), 200);
        let b: Value = r.json().await.expect("run json");
        let run_field = b.get("run").unwrap_or(&b);
        let state = run_field
            .get("state")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_owned();
        let fc = run_field
            .get("failure_class")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_owned();
        if matches!(state.as_str(), "completed" | "failed" | "canceled")
            || std::time::Instant::now() >= deadline
        {
            break (state, fc);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    // The pre-fix defect shape: run.state=failed AND
    // run.failure_class=terminal_write_deadlock. This is the single
    // assertion the test guards against.
    assert_ne!(
        final_failure_class,
        "terminal_write_deadlock",
        "#655: post-keeper-suspension-aware run must NOT flip to \
         Failed(TerminalWriteDeadlock) across {rounds} approval rounds × \
         {wait:?} each; got state={final_state}, \
         failure_class={final_failure_class}",
        rounds = 2,
        wait = APPROVAL_WAIT,
    );
    assert!(
        matches!(final_state.as_str(), "completed" | "failed" | "canceled"),
        "#655: run must reach a terminal state after two approval rounds; \
         got state={final_state}, failure_class={final_failure_class}"
    );
}
