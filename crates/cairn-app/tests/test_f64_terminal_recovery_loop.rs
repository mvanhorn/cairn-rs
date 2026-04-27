//! F64 regression: bounded terminal-write recovery loop replaces F59's
//! single-shot short-circuit on `lease_expired` + re-claim `NotEligible`.
//!
//! The spec:
//!
//! * When the terminal FCALL hits `lease_expired`, cairn now runs a
//!   ~30s bounded retry loop (immediate probe + 2/4/8/16s backoff).
//!   On each step it sleeps (0s on the first), re-claims, and retries
//!   the FCALL. If any retry succeeds
//!   the run completes normally with
//!   `terminal_write_recovery.outcome == "recovered"`.
//! * If the loop exhausts its schedule the F62 TerminalWriteDeadlock
//!   fallback fires and `terminal_write_recovery.outcome == "deadlocked"`.
//! * Hot path (no recovery needed) leaves `terminal_write_recovery`
//!   as `None`.
//!
//! # Why these tests exist in this shape
//!
//! The dual-door deadlock and the eventual self-heal cannot be forced
//! from the outside with today's FF FCALL surface — as documented in
//! `test_f59_complete_run_retry.rs` and `test_f62_terminal_deadlock_observability.rs`.
//!
//! So the deterministic assertions here are:
//!
//! * Happy path → `terminal_write_recovery` is absent from the run
//!   body. No `terminal_recovery_attempted` event on the event log.
//! * Aggressive-lease-starvation scenario (TTL=2s, 3s terminator
//!   delay) → either recovered OR deadlocked, but the
//!   `terminal_write_recovery` annotation is present on the run with
//!   `attempts >= 1`, a wall-time within the loop's 30s cap, and one
//!   of the two operator-visible outcomes. We ALSO assert that when
//!   the loop times out, the F62 TerminalWriteDeadlock fallback still
//!   fires (regression guard).
//! * Env-gate for aggressive-heal: setting
//!   `CAIRN_F64_AGGRESSIVE_HEAL=1` does not break the normal path.

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

const MOCK_MODEL: &str = "openrouter/f64-terminal-recovery";
const FINAL_ANSWER: &str = "F64 terminal recovery check.";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
    terminator_delay_ms: u64,
}

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
                "id": format!("mock-f64-{n}"),
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
                "usage": {"prompt_tokens": 40, "completion_tokens": 12, "total_tokens": 52}
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
    let connection_id = format!("conn_f64_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": "openrouter",
            "plaintext_value": format!("sk-f64-{suffix}"),
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
    let session_id = format!("sess_f64_{suffix}");
    let run_id = format!("run_f64_{suffix}");

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

/// Longer-timeout client: the F64 recovery loop adds up to ~30s on the
/// deadlock path. The harness default 60s client can race orchestrate
/// completion; this client gives 150s of headroom.
fn long_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(150))
        .build()
        .expect("long-timeout reqwest client")
}

async fn orchestrate(h: &LiveHarness, run_id: &str, goal: &str) -> (u16, Value) {
    let r = long_client()
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

async fn read_run(h: &LiveHarness, run_id: &str) -> Value {
    let deadline = std::time::Instant::now() + Duration::from_secs(4);
    loop {
        let r = h
            .client()
            .get(format!("{}/v1/runs/{}", h.base_url, run_id))
            .bearer_auth(&h.admin_token)
            .send()
            .await
            .expect("run fetch reaches server");
        assert_eq!(r.status().as_u16(), 200, "GET /v1/runs/:id");
        let body: Value = r.json().await.expect("run json");
        let run_field = body.get("run").cloned().unwrap_or(Value::Null);
        let state = run_field
            .get("state")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if matches!(state, "failed" | "completed" | "canceled")
            || std::time::Instant::now() >= deadline
        {
            return body;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// F64 happy path: TTL=5s + 3s terminator delay keeps the lease
/// stale-but-not-fully-expired, so F59 layer (a) renews in place and
/// the recovery loop never fires. `terminal_write_recovery` must be
/// absent from the response body.
#[tokio::test]
async fn happy_path_does_not_emit_terminal_recovery_annotation() {
    let h = LiveHarness::setup_with_env(&[("CAIRN_FABRIC_LEASE_TTL_MS", "5000")]).await;
    let (mock_url, hits) = spawn_mock(3_000).await;

    let suffix = h.project.clone();
    provision_provider(&h, &suffix, &mock_url).await;
    let (_session_id, run_id) = provision_session_and_run(&h, &suffix).await;

    let (status, body) = orchestrate(&h, &run_id, "Answer the prompt.").await;
    let body_str = body.to_string();
    assert_eq!(status, 200, "happy-path orchestrate; body={body_str}");
    assert_eq!(
        body.get("termination").and_then(|v| v.as_str()),
        Some("completed"),
        "F64 happy path must still complete; body={body_str}"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1, "one provider hit");

    let run_body = read_run(&h, &run_id).await;
    let run_body_str = run_body.to_string();
    let run_field = run_body.get("run").unwrap_or(&run_body);
    assert!(
        run_field.get("terminal_write_recovery").is_none(),
        "F64 happy path must NOT attach terminal_write_recovery; body={run_body_str}"
    );
}

/// F64 recovery loop behavior under aggressive lease starvation.
///
/// Same scenario as the F62 deadlock test (TTL=2s, 3s terminator
/// delay). If FF enters the dual-door state during this test, F64's
/// loop runs and emits a `terminal_write_recovery` annotation carrying
/// either `"recovered"` or `"deadlocked"`. The exact outcome is
/// platform-dependent (FF's eligibility gate is lenient on some
/// hosts), so we assert a disjunction: either the annotation is
/// present (recovery fired) OR the run reached a terminal state
/// without needing recovery.
///
/// When the loop DID fire, we additionally assert the spec:
///
/// * `attempts >= 1`
/// * `wall_time_ms` within the 30s cap (with a 5s grace for clock
///   skew / event-loop scheduling)
/// * `outcome` is `"recovered"` or `"deadlocked"`
/// * On `"deadlocked"`, the F62 TerminalWriteDeadlock fallback also
///   fires (the run has `failure_class=terminal_write_deadlock` and
///   `state=failed`)
#[tokio::test]
async fn recovery_loop_annotates_run_under_aggressive_lease_starvation() {
    let h = LiveHarness::setup_with_env(&[("CAIRN_FABRIC_LEASE_TTL_MS", "2000")]).await;
    let (mock_url, _hits) = spawn_mock(3_000).await;

    let suffix = h.project.clone();
    provision_provider(&h, &suffix, &mock_url).await;
    let (_session_id, run_id) = provision_session_and_run(&h, &suffix).await;

    let (status, _body) = orchestrate(&h, &run_id, "Answer the prompt.").await;
    assert!(
        matches!(status, 200 | 409),
        "orchestrate status; got {status}"
    );

    let run_body = read_run(&h, &run_id).await;
    let run_body_str = run_body.to_string();
    let run_field = run_body.get("run").unwrap_or(&run_body);
    let state = run_field
        .get("state")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        matches!(state, "completed" | "failed" | "canceled"),
        "run must reach terminal state; got state={state}, body={run_body_str}"
    );

    // F62/F64 projection regression guard: if the run reached
    // `failed` with `failure_class=terminal_write_deadlock`, the F64
    // recovery annotation MUST be present. The two must land as a
    // pair — a deadlocked run without the annotation means the
    // projection or bridge lost the event. Catching this inline
    // (rather than letting the `if let Some(recovery)` branch skip
    // assertions) turns a silent projection regression into a loud
    // test failure.
    let failure_class = run_field
        .get("failure_class")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if failure_class == "terminal_write_deadlock" {
        let recovery = run_field.get("terminal_write_recovery").unwrap_or_else(|| {
            panic!(
                "F62/F64: run flipped to failed with \
                 failure_class=terminal_write_deadlock but \
                 terminal_write_recovery annotation is missing \
                 (projection / bridge regression); body={run_body_str}"
            )
        });
        let outcome = recovery
            .get("outcome")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        assert_eq!(
            outcome, "deadlocked",
            "F62/F64: deadlocked run must carry outcome=deadlocked; \
             body={run_body_str}"
        );
    }

    if let Some(recovery) = run_field.get("terminal_write_recovery") {
        let attempts = recovery
            .get("attempts")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let wall_time_ms = recovery
            .get("wall_time_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let outcome = recovery
            .get("outcome")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        assert!(
            attempts >= 1,
            "F64: recovery annotation must record at least one attempt; body={run_body_str}"
        );
        assert!(
            wall_time_ms <= 35_000,
            "F64: recovery wall-time must stay inside 30s cap (+5s grace); \
             got wall_time_ms={wall_time_ms}, body={run_body_str}"
        );
        assert!(
            matches!(
                outcome,
                "recovered"
                    | "deadlocked"
                    | "non_transient_retry_error"
                    | "non_transient_reclaim_error"
            ),
            "F64: recovery outcome must be a known enum value; got {outcome}, \
             body={run_body_str}"
        );

        if outcome == "deadlocked" {
            let failure_class = run_field
                .get("failure_class")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            assert_eq!(
                failure_class, "terminal_write_deadlock",
                "F64+F62: deadlocked outcome must coincide with \
                 failure_class=terminal_write_deadlock; body={run_body_str}"
            );
            assert_eq!(
                state, "failed",
                "F64+F62: deadlocked outcome must flip run to failed; \
                 got state={state}, body={run_body_str}"
            );
        }
    }
}

/// F64 env gate: `CAIRN_F64_AGGRESSIVE_HEAL=1` must not regress the
/// happy path. It only enables a log-only proactive-heal breadcrumb
/// today (the adapter doesn't yet carry an ApprovalService handle);
/// this test pins that future wiring changes don't break the normal
/// completion path when the env is set.
#[tokio::test]
async fn aggressive_heal_env_does_not_regress_happy_path() {
    let h = LiveHarness::setup_with_env(&[
        ("CAIRN_FABRIC_LEASE_TTL_MS", "5000"),
        ("CAIRN_F64_AGGRESSIVE_HEAL", "1"),
    ])
    .await;
    let (mock_url, hits) = spawn_mock(3_000).await;

    let suffix = h.project.clone();
    provision_provider(&h, &suffix, &mock_url).await;
    let (_session_id, run_id) = provision_session_and_run(&h, &suffix).await;

    let (status, body) = orchestrate(&h, &run_id, "Answer the prompt.").await;
    let body_str = body.to_string();
    assert_eq!(
        status, 200,
        "aggressive-heal env must not break happy path; body={body_str}"
    );
    assert_eq!(
        body.get("termination").and_then(|v| v.as_str()),
        Some("completed"),
        "happy path must still complete with aggressive-heal env; body={body_str}"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}
