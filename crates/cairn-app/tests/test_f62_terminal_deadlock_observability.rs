//! F62 regression: when the F59 short-circuit fires (terminal FCALL
//! `lease_expired` AND re-claim `execution_not_eligible`), cairn must
//!
//!   1. Flip the run to `Failed(TerminalWriteDeadlock)` so operators
//!      see a terminal state instead of a zombie `running` row.
//!   2. Surface an operator-actionable message that mentions the
//!      artifact-preservation hint and links the tracked FF upstream
//!      issue (https://github.com/avifenesh/FlowFabric/issues/371).
//!   3. Preserve the F59 happy-path behavior (regression guard).
//!
//! # Why this test is shaped the way it is
//!
//! The exact dual-door deadlock cannot be forced from the outside with
//! today's FF FCALL surface — see the note in
//! `test_f59_complete_run_retry.rs` around
//! `complete_run_survives_expired_lease_mid_orchestrate`, which
//! explicitly states that the fully-expired-lease case is not
//! reproducible in the harness.
//!
//! So this test has two arms:
//!
//! * `happy_path_still_completes_when_lease_does_not_fully_expire` —
//!   regression guard for F59 layer (a) + (b) under the scenario the
//!   harness can reliably reproduce.
//! * `deadlock_response_points_operator_at_artifacts_and_upstream_issue` —
//!   aggressive lease starvation (TTL=2s + 3s terminator delay). If
//!   FF ever enters the dual-door state during the test (observed
//!   intermittently on loaded CI boxes), the response body MUST carry
//!   the FF upstream URL and the run state MUST be `failed` with
//!   `failure_class=terminal_write_deadlock`. If FF does not enter the
//!   deadlock (because its phase gate is lenient enough under the
//!   scenario), the test still asserts the response body is
//!   operator-actionable (no raw FF strings leaked).
//!
//! The rendered-error invariants that DO hold deterministically are
//! covered as unit tests on `RuntimeError::InvalidTransition` in
//! `cairn_runtime::error` — those pin the exact prose downstream.

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

const MOCK_MODEL: &str = "openrouter/f62-terminal-deadlock";
const FINAL_ANSWER: &str = "F62 deadlock observability check.";

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
                "id": format!("mock-f62-{n}"),
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
    let connection_id = format!("conn_f62_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": "openrouter",
            "plaintext_value": format!("sk-f62-{suffix}"),
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
    let session_id = format!("sess_f62_{suffix}");
    let run_id = format!("run_f62_{suffix}");

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

/// F59 layer (a)+(b) regression guard: TTL=5s + 3s terminator delay
/// means the lease is stale-but-not-fully-expired when the terminal
/// FCALL fires, so layer (a) renews and the FCALL succeeds first try.
/// The F62 deadlock short-circuit must NOT fire; the response must NOT
/// mention the F62 upstream link. This pins that the new F62 wiring
/// does not regress the hot path.
#[tokio::test]
async fn happy_path_still_completes_when_lease_does_not_fully_expire() {
    let h = LiveHarness::setup_with_env(&[("CAIRN_FABRIC_LEASE_TTL_MS", "5000")]).await;
    let (mock_url, hits) = spawn_mock(3_000).await;

    let suffix = h.project.clone();
    provision_provider(&h, &suffix, &mock_url).await;
    let (_session_id, run_id) = provision_session_and_run(&h, &suffix).await;

    let (status, body) = orchestrate(&h, &run_id, "Answer the prompt.").await;
    let body_str = body.to_string();
    assert_eq!(status, 200, "orchestrate status; body={body_str}");
    assert_eq!(
        body.get("termination").and_then(|v| v.as_str()),
        Some("completed"),
        "F59 happy path must still complete; body={body_str}"
    );
    assert!(
        !body_str.contains("FlowFabric/issues/371"),
        "F62 deadlock banner must NOT leak into the happy path; body={body_str}"
    );
    assert!(
        !body_str.contains("terminal_write_deadlock"),
        "F62 sentinel must NOT appear on the happy path; body={body_str}"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1, "one provider hit");
}

/// F62 primary assertion: when the orchestrator's terminal FCALL
/// enters the dual-door deadlock (lease fully expired + phase still
/// non-runnable), the HTTP response and the run record must be
/// operator-actionable:
///
/// * The response body must reference the FF upstream issue URL so
///   operators can correlate against known history.
/// * The run's `failure_class` must be `terminal_write_deadlock` (not
///   a raw FF string or a generic `execution_error`).
/// * The run must NOT sit at `running` indefinitely — it transitions
///   to `failed` via the `BridgeEvent::ExecutionFailed` emitted from
///   the short-circuit branch.
///
/// Scenario: TTL=2s, 3s terminator delay. On many platforms this
/// triggers the deadlock; on others FF's gate is lenient enough that
/// the FCALL lands. Either way we verify the response is shaped
/// correctly — the test fails only when the response leaks raw FF
/// jargon without the F62 pointer AND the run is still stuck at
/// `running` (the pre-F62 defect shape).
#[tokio::test]
async fn deadlock_response_points_operator_at_artifacts_and_upstream_issue() {
    let h = LiveHarness::setup_with_env(&[("CAIRN_FABRIC_LEASE_TTL_MS", "2000")]).await;
    let (mock_url, _hits) = spawn_mock(3_000).await;

    let suffix = h.project.clone();
    provision_provider(&h, &suffix, &mock_url).await;
    let (_session_id, run_id) = provision_session_and_run(&h, &suffix).await;

    let (status, body) = orchestrate(&h, &run_id, "Answer the prompt.").await;
    let body_str = body.to_string();
    // Deadlock path surfaces as 409 (InvalidTransition); the lenient
    // non-deadlock path surfaces as 200 (termination=completed).
    // Any other status (e.g. 5xx, 400) is a regression — the test
    // must fail loudly rather than probing a nonsense body.
    assert!(
        matches!(status, 200 | 409),
        "F62: orchestrate returned unexpected status {status}; \
         expected 200 (happy) or 409 (deadlock); body={body_str}"
    );

    // Poll the run until the projection has observed the
    // `ExecutionFailed` bridge event emitted by the F62 short-circuit.
    // The bridge → event-log → projection path is async, so a bare
    // GET immediately after the orchestrate return can race. Cap at 3s
    // to match other F5x probe tests.
    let mut state;
    let mut failure_class;
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
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
        // GET /v1/runs/:id returns RunDetailResponse with a top-level
        // `run` field alongside `tasks` and `completion`. Walk into
        // the RunRecord when present (fall back to the body root for
        // safety if the shape ever changes back to the bare record).
        let run_field = body.get("run").unwrap_or(&body);
        state = run_field
            .get("state")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_owned();
        failure_class = run_field
            .get("failure_class")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_owned();
        if matches!(state.as_str(), "failed" | "completed" | "canceled")
            || std::time::Instant::now() >= deadline
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let deadlock_hit = failure_class == "terminal_write_deadlock"
        || body_str.contains("terminal_write_deadlock")
        || body_str.contains("FlowFabric/issues/371");

    if deadlock_hit {
        // F62 deadlock path: both invariants apply.
        assert!(
            body_str.contains("FlowFabric/issues/371"),
            "F62: deadlock response must link the upstream FF issue for \
             operator correlation; body={body_str}"
        );
        assert!(
            body_str.contains("artifact") || body_str.contains("filesystem"),
            "F62: deadlock response must mention artifacts / filesystem so \
             operators know earlier tool-call work is still on disk; \
             body={body_str}"
        );
        assert_eq!(
            state, "failed",
            "F62: deadlock must flip the run to `failed` (not leave it as \
             `running`); got state={state}, failure_class={failure_class}, \
             body={body_str}"
        );
        assert_eq!(
            failure_class, "terminal_write_deadlock",
            "F62: deadlock must carry failure_class=terminal_write_deadlock; \
             got failure_class={failure_class}, body={body_str}"
        );
    } else {
        // FF was lenient enough this time; no deadlock fired. The run
        // must have still reached a terminal state inside the polling
        // window — leaving it at `running` would be the pre-F62 defect
        // shape (work done, no terminal state written). Fail loudly
        // rather than passing silently on that shape.
        assert!(
            matches!(state.as_str(), "failed" | "completed" | "canceled"),
            "F62: non-deadlock path must still reach a terminal state \
             inside the polling window; got state={state}, \
             failure_class={failure_class}, body={body_str}"
        );
        // And the response must not leak the pre-F62 raw `lease_expired`
        // prose without the operator-actionable wrapping.
        let leaks_raw_lease_expired = body_str.contains("lease expired before cairn could write")
            && !body_str.contains("FlowFabric/issues/371");
        assert!(
            !leaks_raw_lease_expired,
            "F62: response leaks the raw lease_expired classifier without \
             the F62 operator pointer; body={body_str}"
        );
    }
}
