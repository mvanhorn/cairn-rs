//! Issue #351 regression: orchestrate must refuse when the selected
//! provider does not report usage AND the configured `token_cap` is
//! below the 50_000 sentinel.
//!
//! # Context
//!
//! F65 PR-3 (merged `ba8adc6e`) added the token-cap circuit breaker.
//! `BreakerState::after_decide` treats absent `DecideOutput.input_tokens`
//! / `output_tokens` as zero, so a provider that never populates
//! `usage` on its chat responses silently disables the token-cap path:
//! the cap never trips and the only way the loop terminates is via the
//! Round or WallClock breakers. For tight budgets that is
//! indistinguishable from the token budget not existing.
//!
//! Copilot flagged this during PR #348 review. The defensive fix is
//! layered in the orchestrate handler: when the resolved backend's
//! `Backend::reports_usage()` returns `false` and
//! `breakers.token_cap < 50_000`, the handler returns HTTP 422 with
//! `code: "provider_does_not_report_usage"` so the operator either
//! raises the cap (explicit opt-in to unbounded spend on that
//! provider) or switches backends.
//!
//! # What this file covers
//!
//! Three behaviours of the defensive check:
//!
//!   1. `refuses_when_provider_no_usage_and_tight_token_cap` — the
//!      selected connection registers as `openai-compatible` (the
//!      sole `reports_usage = false` backend today) with
//!      `orchestrator_token_cap = 1000`. Orchestrate must return 422
//!      with the sentinel message before any provider round-trip.
//!   2. `accepts_when_provider_no_usage_and_loose_token_cap` — same
//!      non-reporting backend, but `token_cap = 100_000` (above the
//!      sentinel). Orchestrate must proceed normally; the log-once
//!      `DECIDE response carried no token usage` WARN fires inside
//!      the loop but doesn't surface in the handler response.
//!   3. `accepts_when_provider_reports_usage_regardless_of_cap` —
//!      `openrouter` connection (reports_usage = true) with a
//!      deliberately small `token_cap` (100). Orchestrate must
//!      proceed; the token-cap breaker will subsequently trip inside
//!      the loop once real usage accumulates past the cap.
//!
//! The tests stand up a mock OpenAI-compatible upstream (identical
//! shape across all three scenarios); the only lever is the
//! connection's `provider_family` / `adapter_type` and the
//! orchestrator's token_cap default.

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

const MOCK_MODEL: &str = "mock-usage-defensive";
const FINAL_ANSWER: &str = "done";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
    include_usage: bool,
}

/// Spawn a mock OpenAI-compatible chat-completions upstream. Responds
/// with a native `complete_run` tool_call on every request so the run
/// terminates in one iteration in the loose-cap / reports-usage
/// scenarios. `include_usage = false` mirrors a misbehaving
/// compat-layer or local plugin that omits the `usage` block.
async fn spawn_mock(include_usage: bool) -> (String, Arc<AtomicUsize>) {
    let state = MockState {
        hits: Arc::new(AtomicUsize::new(0)),
        include_usage,
    };
    let hits = state.hits.clone();

    async fn chat_handler(
        State(state): State<MockState>,
        Json(_body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        state.hits.fetch_add(1, Ordering::SeqCst);

        let mut resp = json!({
            "id": "mock-351",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [{
                        "id": "call_done_1",
                        "type": "function",
                        "function": {
                            "name": "complete_run",
                            "arguments": json!({ "final_answer": FINAL_ANSWER }).to_string(),
                        }
                    }],
                },
                "finish_reason": "tool_calls",
            }],
        });
        if state.include_usage {
            resp["usage"] = json!({
                "prompt_tokens": 120,
                "completion_tokens": 12,
                "total_tokens": 132,
            });
        }
        (StatusCode::OK, Json(resp))
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
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    (format!("http://{addr}"), hits)
}

/// Provision a credential + provider connection + system defaults +
/// session + run on the harness. `provider_family` controls which
/// backend the orchestrate handler resolves, and therefore which
/// `reports_usage` branch the defensive check takes.
async fn provision(
    h: &LiveHarness,
    mock_url: &str,
    provider_family: &str,
    adapter_type: &str,
    scenario_tag: &str,
) -> String {
    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_{scenario_tag}_{suffix}");
    let session_id = format!("sess_{scenario_tag}_{suffix}");
    let run_id = format!("run_{scenario_tag}_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": provider_family,
            "plaintext_value": format!("sk-{scenario_tag}-{suffix}"),
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
            "provider_family": provider_family,
            "adapter_type": adapter_type,
            "supported_models": [MOCK_MODEL],
            "credential_id": credential_id,
            "endpoint_url": mock_url,
        }))
        .send()
        .await
        .expect("connection reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "connection create failed: {}",
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

    run_id
}

/// Override the operator-level `orchestrator_token_cap` default. The
/// orchestrate handler reads this on entry via `RuntimeConfig`; a
/// subsequent request will see the updated cap.
async fn set_token_cap(h: &LiveHarness, cap: u64) {
    let r = h
        .client()
        .put(format!(
            "{}/v1/settings/defaults/system/system/orchestrator_token_cap",
            h.base_url,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "value": cap }))
        .send()
        .await
        .expect("token-cap default reaches server");
    assert_eq!(
        r.status().as_u16(),
        200,
        "token_cap setting failed: {}",
        r.text().await.unwrap_or_default()
    );
}

async fn orchestrate(h: &LiveHarness, run_id: &str) -> (u16, Value) {
    let r = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id,))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "Say done.",
            "max_iterations": 4,
        }))
        .send()
        .await
        .expect("orchestrate reaches server");
    let status = r.status().as_u16();
    let body: Value = r.json().await.unwrap_or(Value::Null);
    (status, body)
}

/// Primary refusal regression. A `openai-compatible` connection
/// (the only `reports_usage = false` backend today) with the system
/// `token_cap = 1000` must cause orchestrate to return 422 with
/// `code: provider_does_not_report_usage`, and the mock provider
/// must not receive ANY chat request — the refusal happens before
/// the first DECIDE round.
#[tokio::test]
async fn orchestrate_refuses_when_provider_no_usage_and_tight_token_cap() {
    let h = LiveHarness::setup().await;
    let (mock_url, hits) = spawn_mock(/* include_usage */ false).await;
    let run_id = provision(&h, &mock_url, "openai_compat", "openai_compat", "refuse").await;
    set_token_cap(&h, 1_000).await;

    let (status, body) = orchestrate(&h, &run_id).await;

    assert_eq!(
        status, 422,
        "#351: tight token_cap on non-reporting provider must refuse; \
         got status={status}, body={body}",
    );
    assert_eq!(
        body.get("code").and_then(|v| v.as_str()),
        Some("provider_does_not_report_usage"),
        "#351: error code mismatch; body={body}",
    );
    assert_eq!(
        body.get("status_code").and_then(|v| v.as_u64()),
        Some(422),
        "#351: status_code field must mirror HTTP status; body={body}",
    );
    let message = body
        .get("message")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(
        message.contains("50000") || message.contains("50_000"),
        "#351: message must cite the 50_000 sentinel so the operator \
         can correlate; got message={message:?}",
    );
    assert!(
        message.contains("token_cap"),
        "#351: message must reference token_cap; got message={message:?}",
    );
    // Zero hits: the refusal is a pre-dispatch guard, not a
    // post-dispatch kill. A provider round-trip at this point would
    // mean the check ran too late.
    let n = hits.load(Ordering::SeqCst);
    assert_eq!(
        n, 0,
        "#351: provider must not be contacted when the defensive check fires; got {n} hits",
    );
}

/// A non-reporting provider with a loose token_cap (above the
/// sentinel) must NOT be refused. The token-cap breaker will
/// under-count, but the operator has explicitly opted in by raising
/// the cap past the sentinel — that is the escape hatch the check
/// is designed to leave open.
#[tokio::test]
async fn orchestrate_accepts_when_provider_no_usage_and_loose_token_cap() {
    let h = LiveHarness::setup().await;
    let (mock_url, hits) = spawn_mock(/* include_usage */ false).await;
    let run_id = provision(
        &h,
        &mock_url,
        "openai_compat",
        "openai_compat",
        "accept_loose",
    )
    .await;
    set_token_cap(&h, 100_000).await;

    let (status, body) = orchestrate(&h, &run_id).await;

    assert_eq!(
        status, 200,
        "#351: loose token_cap must bypass the defensive refusal \
         even on a non-reporting provider; got status={status}, body={body}",
    );
    assert_ne!(
        body.get("code").and_then(|v| v.as_str()),
        Some("provider_does_not_report_usage"),
        "#351: must not return the refusal code on a loose cap; body={body}",
    );
    // One hit proves the handler actually drove DECIDE. We don't
    // assert on termination state — the in-memory fabric rejects
    // some terminal FCALLs in LiveHarness, which is orthogonal to
    // #351. What matters is the refusal did NOT fire.
    let n = hits.load(Ordering::SeqCst);
    assert!(
        n >= 1,
        "#351: expected at least 1 provider round-trip when the \
         defensive refusal is bypassed; got {n}",
    );
}

/// A `reports_usage = true` provider (openrouter) must proceed even
/// with a deliberately low token_cap. The defensive check gates on
/// the backend's `reports_usage` flag, not the cap alone — a
/// reporting provider's token-cap breaker is real and will trip
/// inside the loop, so refusing at the handler would be a false
/// positive.
#[tokio::test]
async fn orchestrate_accepts_when_provider_reports_usage_regardless_of_cap() {
    let h = LiveHarness::setup().await;
    let (mock_url, hits) = spawn_mock(/* include_usage */ true).await;
    let run_id = provision(
        &h,
        &mock_url,
        "openrouter",
        "openrouter",
        "accept_reporting",
    )
    .await;
    set_token_cap(&h, 100).await;

    let (status, body) = orchestrate(&h, &run_id).await;

    // The run may terminate with either 200 (complete_run accepted)
    // or 200 with termination=token_cap_reached once cumulative
    // usage crosses the tight cap. Either way, the refusal code
    // must NOT surface — that is the load-bearing #351 invariant
    // for this scenario.
    assert_eq!(
        status, 200,
        "#351: reporting provider must never surface the defensive \
         refusal regardless of cap; got status={status}, body={body}",
    );
    assert_ne!(
        body.get("code").and_then(|v| v.as_str()),
        Some("provider_does_not_report_usage"),
        "#351: reporting provider incorrectly flagged as non-reporting; \
         body={body}",
    );
    let n = hits.load(Ordering::SeqCst);
    assert!(
        n >= 1,
        "#351: expected at least 1 provider round-trip when the \
         defensive refusal is not applicable; got {n}",
    );
}
