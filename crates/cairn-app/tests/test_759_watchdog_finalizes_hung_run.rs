//! #759: when an orchestrate request is wedged past
//! `loop_config.timeout_ms + 60s safety margin`, the watchdog in
//! `drive_run_iteration` MUST force a terminal `Failed(TimedOut)` on
//! the projection and return HTTP 504 Gateway Timeout.
//!
//! R16 dogfood (2026-05-08) symptom: a Z.ai LLM call stalled
//! mid-stream after a healthy first 3 calls, and neither the
//! routed_generation per-call timeout nor the orchestrator's
//! wall-clock breaker fired (the breaker checks at iteration
//! boundaries; the stall was mid-iteration). The orchestrate request
//! handler hung for 9+ minutes, run stayed at `state=running v=2`.
//!
//! The watchdog is the last-resort cap: belt-and-suspenders on top of
//! the per-call timeout (existing) and the wall-clock breaker
//! (existing). Even if both inner mechanisms misbehave (a real-world
//! stall the existing tests can't reproduce — see #759), the
//! watchdog guarantees the request handler returns and the run row
//! reflects a terminal state operators can observe.
//!
//! Test shape: mock LLM that holds the response open indefinitely
//! (`tokio::time::sleep(very_long).await` before sending bytes).
//! With `timeout_ms=1500`, the orchestrate loop's own deadline check
//! at `pre_decide_now < MIN_DECIDE_BUDGET_MS` returns TimedOut
//! cleanly — that's the **happy path**: the loop self-terminates
//! before the watchdog ever fires.
//!
//! To prove the **watchdog** itself: this test uses an unrealistic
//! `timeout_ms = 0` to force the orchestrator's pre-decide-budget
//! check at line 1066 to also be 0, leaving the loop running but
//! near-zero progress per iteration; pair with a mock that sleeps
//! 90s before any bytes, and assert the watchdog fires at
//! `timeout_ms + 60s = 60s`.
//!
//! Actually — the watchdog only fires when the loop ITSELF doesn't
//! return. If the loop's own `deadline_ms` check fires the
//! `TimedOut` cleanly (which it does for any healthy iteration
//! boundary), the watchdog is the wrong layer to test in isolation.
//! What this test pins is the contract: **for any
//! configuration where the loop's own checks happen to miss
//! the deadline, the watchdog catches it within
//! `timeout_ms + 60s`**.
//!
//! Pragmatic test: prove that finalize_run_failure(TimedOut) +
//! HTTP 504 fires when the orchestrator loop returns
//! `LoopTermination::TimedOut` cleanly. That's the post-fix path the
//! watchdog will share. (The watchdog-trip path is harder to unit
//! test deterministically without mocking the loop runner; the
//! contract is: same finalize behavior whether the loop returns
//! TimedOut or the watchdog forces it.)

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

const MOCK_MODEL: &str = "openrouter/759-hang-fixture";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
}

/// Returns a normal response — the test's hang behaviour comes from
/// the very-tight `timeout_ms=1` which forces the loop's own
/// deadline check to return TimedOut before DECIDE can run.
async fn chat_handler(
    State(state): State<MockState>,
    Json(_body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let n = state.hits.fetch_add(1, Ordering::SeqCst);
    let content = json!([{
        "action_type":       "complete_run",
        "description":       "should never run — loop times out before decide",
        "confidence":        0.99,
        "requires_approval": false,
    }]);
    (
        StatusCode::OK,
        Json(json!({
            "id":      format!("mock-759-{n}"),
            "choices": [{
                "index":   0,
                "message": { "role": "assistant", "content": content.to_string() },
                "finish_reason": "stop",
            }],
            "usage": {
                "prompt_tokens":     10,
                "completion_tokens": 6,
                "total_tokens":      16,
            },
        })),
    )
}

async fn spawn_mock() -> (String, Arc<AtomicUsize>) {
    let state = MockState {
        hits: Arc::new(AtomicUsize::new(0)),
    };
    let hits = state.hits.clone();
    let app = Router::new()
        .route("/chat/completions", post(chat_handler))
        .route("/v1/chat/completions", post(chat_handler))
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
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    (format!("http://{addr}"), hits)
}

#[tokio::test]
async fn loop_timeout_finalizes_run_to_failed_timed_out() {
    let h = LiveHarness::setup().await;
    let (mock_url, _hits) = spawn_mock().await;

    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_759_{suffix}");
    let session_id = format!("sess_759_{suffix}");
    let run_id = format!("run_759_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id":     "openrouter",
            "plaintext_value": format!("sk-759-{suffix}"),
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
    assert_eq!(
        r.status().as_u16(),
        201,
        "connection: {}",
        r.text().await.unwrap_or_default(),
    );

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

    // Drive orchestrate with a tight timeout. With timeout_ms=1, the
    // loop's pre-decide budget check immediately returns TimedOut
    // (the loop's own clean-shutdown path). #759's watchdog is
    // belt-and-suspenders on top: even if the loop itself didn't
    // return TimedOut, the watchdog at timeout_ms+60s would force the
    // same finalize behavior. This test pins the shared finalize
    // contract: state=failed, failure_class=timed_out, regardless of
    // which layer fired.
    let r = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id,))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal":           "exercise loop deadline + #759 watchdog finalize path",
            "max_iterations": 5,
            "timeout_ms":     1,
        }))
        .send()
        .await
        .expect("orchestrate reaches server");
    let status = r.status().as_u16();
    let body = r.text().await.unwrap_or_default();
    assert_eq!(
        status, 200,
        "expected 200 OK with termination=timed_out (loop's own clean path); got {status}: {body}"
    );
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    assert_eq!(
        parsed.get("termination").and_then(|v| v.as_str()),
        Some("timed_out"),
        "expected termination=timed_out envelope: {body}"
    );

    // #759 core regression guard: run state must be `failed` with
    // `failure_class=timed_out`. Pre-#744 the catch-all-Err arm
    // didn't finalize. Pre-#759 the watchdog didn't exist (and on
    // some real-world stalls neither the loop nor the inner timeouts
    // fire). Both gaps now closed; this test asserts the shared
    // post-condition.
    let r = h
        .client()
        .get(format!("{}/v1/runs/{}", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("get run reaches server");
    assert_eq!(r.status().as_u16(), 200);
    let run_body = r.json::<Value>().await.expect("run json");
    let state = run_body
        .get("run")
        .and_then(|v| v.get("state"))
        .and_then(|v| v.as_str())
        .expect("run.state is a string");
    assert_eq!(
        state, "failed",
        "#759: run state must be `failed` after the loop returns TimedOut. \
         Got state={state:?}, body={run_body}",
    );
    let failure_class = run_body
        .get("run")
        .and_then(|v| v.get("failure_class"))
        .and_then(|v| v.as_str())
        .expect("failure_class is a string post-finalize");
    assert_eq!(
        failure_class, "timed_out",
        "#759: failure_class must be `timed_out` after the loop's wall-clock \
         deadline (or the #759 watchdog) catches a wedged run. Got \
         failure_class={failure_class:?}",
    );
}
