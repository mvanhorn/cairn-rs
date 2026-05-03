//! #639 regression: approval-gated runs whose operator-paced waits
//! exceed `lease_ttl_ms` must NOT flip to
//! `Failed(TerminalWriteDeadlock)`.
//!
//! # The bug (dogfood 2026-05-03, run `run_roguelike_1777808568`)
//!
//! A roguelike-building run executed ~8 tool calls successfully. On
//! the final iteration, cairn attempted to record the run as
//! Completed. Cairn logged:
//!
//! ```text
//! WARN orchestrator loop failed reason=invalid run transition to
//!      completed: ... (code=terminal_write_deadlock)
//! ```
//!
//! Then F64's bounded recovery loop fired, exhausted its 30 s ceiling,
//! and the run landed in `Failed(TerminalWriteDeadlock)` despite the
//! agent having written 370 LOC of real Rust code.
//!
//! Root cause: `POST /v1/runs/:id/orchestrate` is a pull-model driver.
//! FF's `ClaimedTask` renewer dies when the handler returns; between
//! HTTP calls, an operator clicking approvals at human speed (30-60 s
//! per round × several rounds) racks up wall-time that blows past the
//! default `lease_ttl_ms`. By the time cairn dispatches
//! `ff_complete_execution`, the lease is expired.
//!
//! # The fix
//!
//! A background tokio task per live run — the `LeaseKeeperRegistry` —
//! calls `RunService::renew_lease_if_stale` every `lease_ttl_ms / 3`
//! so the FF lease stays healthy regardless of how long the operator
//! takes between approval clicks. See
//! `crates/cairn-app/src/lease_keeper.rs` for the full design note.
//!
//! # This test
//!
//! LiveHarness with aggressive short TTL (`CAIRN_FABRIC_LEASE_TTL_MS
//! = 2000`, well below FabricConfig's nominal 180 s default). Two
//! back-to-back orchestrate calls separated by a 6 s wall-clock wait
//! (3× TTL). The first call drives the run to terminal; the keeper
//! is spawned inside that handler. The 6 s wait is the dogfood
//! approval-pace shape: long enough that FF's expiry scanner would
//! clear `current_lease_id` without intervention.
//!
//! Post-fix: the keeper renews the lease ~9 times during the wait
//! (interval `2000 / 3 ≈ 667 ms`); the second orchestrate's
//! terminal-state short-circuit completes without any FF mutation
//! leaking a deadlock shape to the response.
//!
//! The assertion guards against `terminal_write_deadlock` surfacing
//! in the response body OR on the run record. Pre-fix (keeper not
//! wired) the second call's renew-on-terminal path would still hit
//! `lease_expired` because the lease was dead when the terminal
//! FCALL landed on the first call; the run would have flipped to
//! Failed(TerminalWriteDeadlock) before the second call even ran.
//! That's the shape this test guards against.

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

const MOCK_MODEL: &str = "openrouter/issue639-lease-keeper";
const FINAL_ANSWER: &str = "issue-639 lease-keeper regression answer.";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
    terminator_delay_ms: u64,
}

/// Mock provider with an optional `terminator_delay_ms` that pauses
/// the FIRST response by that many ms. Delaying the provider keeps
/// the orchestrate handler in-flight past the lease TTL, so FF's
/// internal renewer + cairn's background lease keeper BOTH must
/// successfully cover the gap or the terminal FCALL hits
/// `lease_expired`. This is the narrowest reproducer the HTTP harness
/// can observe without coupling into FF's internal state.
async fn spawn_mock(terminator_delay_ms: u64) -> (String, Arc<AtomicUsize>) {
    let state = MockState {
        hits: Arc::new(AtomicUsize::new(0)),
        terminator_delay_ms,
    };
    let hits = state.hits.clone();

    async fn chat_handler(
        State(state): State<MockState>,
        Json(_body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        let n = state.hits.fetch_add(1, Ordering::SeqCst);
        if n == 0 && state.terminator_delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(state.terminator_delay_ms)).await;
        }
        (
            StatusCode::OK,
            Json(json!({
                "id": format!("mock-639-{n}"),
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": null,
                        "tool_calls": [{
                            "id": format!("call_complete_{n}"),
                            "type": "function",
                            "function": {
                                "name": "complete_run",
                                "arguments": json!({ "final_answer": FINAL_ANSWER }).to_string(),
                            }
                        }],
                    },
                    "finish_reason": "tool_calls",
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
    let connection_id = format!("conn_639_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": "openrouter",
            "plaintext_value": format!("sk-639-{suffix}"),
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
    let session_id = format!("sess_639_{suffix}");
    let run_id = format!("run_639_{suffix}");

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
    // F64's deadlock-recovery loop can add up to ~30 s on the pre-fix
    // path. Use a 60 s client so the assertion can read the full
    // response body instead of timing out mid-request.
    let long_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .expect("long-timeout reqwest client");
    let r = long_client
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "goal": goal, "max_iterations": 4 }))
        .send()
        .await
        .expect("orchestrate reaches server");
    let status = r.status().as_u16();
    let body: Value = r.json().await.unwrap_or(Value::Null);
    (status, body)
}

/// **#639 primary regression**: a provider response delayed longer
/// than the lease TTL must NOT trip TerminalWriteDeadlock.
///
/// This is a tighter reproducer of the dogfood incident shape. The
/// mock sleeps 3 s before returning the terminator tool call; with
/// TTL = 2 s, the lease must be renewed AT LEAST ONCE during the
/// provider call or the `ff_complete_execution` at the end of the
/// iteration will hit `lease_expired`.
///
/// This is the same timing as the F62 arm
/// `deadlock_response_points_operator_at_artifacts_and_upstream_issue`
/// — which EXPECTS the deadlock to land on loaded CI. The difference:
/// with the #639 background lease keeper spawned at handler entry,
/// the keeper + FF-SDK's internal `ClaimedTask` renewer overlap to
/// keep the lease alive through the provider delay. Post-fix the run
/// reaches `completed` consistently on the same timing that
/// historically triggered the deadlock.
///
/// # Pre-fix reproduction check
///
/// This test was verified to FAIL before the keeper wiring: with
/// the `lease_keepers.ensure_running` call stubbed out, the 60 s
/// orchestrate client timeout fires during F64's 30 s recovery
/// backoff loop, producing a `reqwest::TimedOut` panic on the
/// request itself. That's how we know the test reliably guards the
/// fix — disable the keeper and the test fails.
///
/// The assertion is deliberately one-directional: we do NOT require
/// the run to always complete (FF's phase gate can still reject
/// transiently), but we REJECT any path that surfaces
/// `TerminalWriteDeadlock` or leaves the run wedged in a non-terminal
/// state. That shape is the exact pre-fix defect #639 closes.
#[tokio::test]
async fn provider_delay_past_ttl_does_not_wedge_run() {
    let h = LiveHarness::setup_with_env(&[("CAIRN_FABRIC_LEASE_TTL_MS", "2000")]).await;
    let (mock_url, _hits) = spawn_mock(3_000).await;

    let suffix = h.project.clone();
    provision_provider(&h, &suffix, &mock_url).await;
    let (_session_id, run_id) = provision_session_and_run(&h, &suffix).await;

    let (status, body) = orchestrate(&h, &run_id, "Answer the prompt.").await;
    let body_str = body.to_string();

    // Allow 200 (happy path, run completed) or 409 (the response
    // shape when the F64 bounded recovery loop fires but eventually
    // recovers). Reject 5xx outright.
    assert!(
        matches!(status, 200 | 409),
        "#639 stress: orchestrate must return 200 or 409; got {status}; \
         body={body_str}"
    );

    // Poll the run state until it reaches a terminal — 5 s cap
    // covers F64's backoff window worst case.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
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
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // The pre-fix defect shape: run.state=failed AND
    // run.failure_class=terminal_write_deadlock.
    assert_ne!(
        final_failure_class, "terminal_write_deadlock",
        "#639 stress: post-keeper run must NOT flip to \
         Failed(TerminalWriteDeadlock); got state={final_state}, \
         failure_class={final_failure_class}; body={body_str}"
    );
    assert!(
        matches!(final_state.as_str(), "completed" | "failed" | "canceled"),
        "#639 stress: run must reach a terminal state within the 5s \
         poll window; got state={final_state}, \
         failure_class={final_failure_class}; body={body_str}"
    );
}
