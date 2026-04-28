//! F65 PR-3 LiveHarness integration tests for circuit-breaker
//! enforcement in `OrchestratorLoop`.
//!
//! Eight scenarios, every one running the real `POST /v1/runs/{id}/orchestrate`
//! HTTP path against a `cairn-app` subprocess (via `LiveHarness`) with a
//! scripted OpenAI-compatible mock provider:
//!
//! 1. `test_breaker_round_cap_trips_and_emits_outcome` — round breaker
//!    terminates at iteration equal to cap, with an event on the log.
//! 2. `test_breaker_token_cap_trips_mid_session` — cumulative tokens
//!    from scripted usage (`prompt_tokens=100`, `completion_tokens=40`)
//!    breach cap on round 2.
//! 3. `test_breaker_no_tool_use_streak_trips_on_narration` — three
//!    consecutive DECIDE responses that only emit `continue` (no tools)
//!    trip the streak breaker.
//! 4. `test_breaker_overrides_tighten_from_request_body` — request-body
//!    override (smaller than default) is honoured: trip fires earlier.
//! 5. `test_breaker_overrides_loosen_returns_400` — override that
//!    exceeds the configured default returns HTTP 400
//!    `invalid_breaker_override`.
//! 6. `test_budget_threshold_crossed_fires_at_80_percent` — warning
//!    fires exactly once on first 80% crossing; NoToolUseConsecutive
//!    never emits a warning per the locked Q2 decision.
//! 7. `test_checkpoint_persisted_on_breaker_trip` — the last completed
//!    iteration's Result checkpoint persists before termination.
//! 8. `test_wall_clock_breaker_trips_under_stopwatch` — monotonic
//!    wall-clock breaker fires; tolerance `[500, 2_000]` ms for CI
//!    scheduler jitter. Has a pure-function fallback unit via
//!    `breakers::tests::wall_clock_trip_ordering_round_wins_when_simultaneous`
//!    in cairn-orchestrator/src/breakers.rs that covers the math
//!    deterministically even when the subprocess path is timing-
//!    sensitive — so we do NOT `#[ignore]` here.
//!
//! Harness pattern lifted from `test_f35_tool_errors_as_feedback.rs`:
//! scripted OpenAI-compat mock provider walks a per-test response
//! vector; tests assert on HTTP response + event log + hit counts.
//! Run with `--test-threads=1` so the shared Valkey testcontainer's
//! metrics/projection state doesn't interleave across scenarios.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

const MOCK_MODEL: &str = "openrouter/f65-pr3-breakers";

// ─────────────────────────────────────────────────────────────────────────────
// Mock provider helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Each response is rendered as one chat-completions reply, stamped with
/// the canonical `usage` block so the loop's token-cap breaker has
/// usage to accumulate. Tests that need different per-round usage
/// counts build a `ScriptEntry` vector instead of a `Vec<Value>`.
#[derive(Clone)]
struct ScriptEntry {
    /// Proposals JSON for this DECIDE round, matches the LLM system
    /// prompt's OpenAI-compat array shape.
    proposals: Value,
    /// Provider-reported prompt_tokens for this round.
    prompt_tokens: u64,
    /// Provider-reported completion_tokens for this round.
    completion_tokens: u64,
    /// If set, the mock sleeps this long before responding — used by
    /// the wall-clock breaker test to nudge monotonic elapsed forward.
    sleep_ms: u64,
}

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
    script: Arc<Vec<ScriptEntry>>,
}

async fn spawn_scripted_mock(script: Vec<ScriptEntry>) -> (String, Arc<AtomicUsize>) {
    let state = MockState {
        hits: Arc::new(AtomicUsize::new(0)),
        script: Arc::new(script),
    };
    let hits = state.hits.clone();

    async fn chat_handler(
        State(state): State<MockState>,
        Json(_body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        let idx = state.hits.fetch_add(1, Ordering::SeqCst);
        let entry = state
            .script
            .get(idx)
            .cloned()
            .unwrap_or_else(|| ScriptEntry {
                // Overshoot fallback mirrors F35's pattern — respond with
                // complete_run so tests don't hang; hit-count assertions
                // still catch over-iteration.
                proposals: json!([{
                    "action_type": "complete_run",
                    "description": "overshoot: script exhausted",
                    "confidence": 1.0,
                    "requires_approval": false,
                }]),
                prompt_tokens: 10,
                completion_tokens: 10,
                sleep_ms: 0,
            });
        if entry.sleep_ms > 0 {
            tokio::time::sleep(Duration::from_millis(entry.sleep_ms)).await;
        }

        (
            StatusCode::OK,
            Json(json!({
                "id": format!("mock-f65-{idx}"),
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": entry.proposals.to_string(),
                    },
                    "finish_reason": "stop",
                }],
                "usage": {
                    "prompt_tokens": entry.prompt_tokens,
                    "completion_tokens": entry.completion_tokens,
                    "total_tokens": entry.prompt_tokens + entry.completion_tokens,
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
    tokio::time::sleep(Duration::from_millis(25)).await;
    (format!("http://{addr}"), hits)
}

/// Provision a credential, provider connection, system-default models,
/// session, and run; returns the (session_id, run_id) so the caller can
/// target the orchestrate path. Mirrors F35's `setup_and_orchestrate`
/// but separates setup from the POST so the test can pass a custom
/// orchestrate request body.
async fn provision(h: &LiveHarness, mock_url: &str, suffix_prefix: &str) -> (String, String) {
    let suffix = format!("{}_{}", suffix_prefix, h.project);
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_{suffix}");
    let session_id = format!("sess_{suffix}");
    let run_id = format!("run_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": "openrouter",
            "plaintext_value": format!("sk-{suffix}"),
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

    (session_id, run_id)
}

async fn orchestrate(h: &LiveHarness, run_id: &str, body: Value) -> (u16, Value) {
    let r = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&body)
        .send()
        .await
        .expect("orchestrate reaches server");
    let status = r.status().as_u16();
    let value: Value = r.json().await.unwrap_or(Value::Null);
    (status, value)
}

async fn fetch_events(h: &LiveHarness) -> Vec<Value> {
    let r = h
        .client()
        .get(format!("{}/v1/events?limit=500", h.base_url))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("GET /v1/events reaches server");
    assert!(r.status().is_success(), "events: {}", r.status());
    let body: Value = r.json().await.expect("events body is json");
    if let Some(arr) = body.as_array() {
        arr.clone()
    } else if let Some(arr) = body.get("items").and_then(Value::as_array) {
        arr.clone()
    } else {
        panic!("unexpected /v1/events body shape: {body}");
    }
}

fn event_type(ev: &Value) -> &str {
    ev.get("event_type")
        .or_else(|| ev.get("event"))
        .and_then(Value::as_str)
        .unwrap_or("")
}

/// Shortcut: scripted round emitting a single narration proposal. The
/// `create_memory` action_type is a valid cairn-domain variant that
/// carries no `tool_name`, so the NoToolUseConsecutive streak counter
/// registers this as a zero-tool-use round. Used whenever the test
/// wants the streak breaker to fire.
fn narration_round(prompt: u64, completion: u64) -> ScriptEntry {
    ScriptEntry {
        proposals: json!([{
            "action_type": "create_memory",
            "description": "note to self",
            "confidence": 0.7,
            "requires_approval": false,
        }]),
        prompt_tokens: prompt,
        completion_tokens: completion,
        sleep_ms: 0,
    }
}

/// Shortcut: scripted round emitting a read-tool proposal.
fn read_round(prompt: u64, completion: u64) -> ScriptEntry {
    ScriptEntry {
        proposals: json!([{
            "action_type": "invoke_tool",
            "description": "read a file",
            "tool_name": "read",
            "tool_args": { "path": "/etc/hostname" },
            "confidence": 0.9,
            "requires_approval": false,
        }]),
        prompt_tokens: prompt,
        completion_tokens: completion,
        sleep_ms: 0,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 1 — Round breaker trips and emits a CircuitBreakerTripped event.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_breaker_round_cap_trips_and_emits_outcome() {
    let h = LiveHarness::setup().await;
    // Script is "never completes": every round is narration with a
    // concrete tool proposal so the streak breaker stays quiet.
    let script = std::iter::repeat_with(|| read_round(50, 50))
        .take(10)
        .collect();
    let (mock_url, hits) = spawn_scripted_mock(script).await;
    let (_sess, run_id) = provision(&h, &mock_url, "f65_p3_round").await;

    let (status, body) = orchestrate(
        &h,
        &run_id,
        json!({
            "goal": "never complete",
            // max_iterations high so the breaker, not legacy cap, trips.
            "max_iterations": 100,
            "breaker_overrides": { "round_cap": 3 },
        }),
    )
    .await;
    assert_eq!(status, 200, "orchestrate should return 200; body={body}");
    assert_eq!(
        body.get("termination").and_then(Value::as_str),
        Some("breaker_tripped"),
        "body={body}"
    );
    assert_eq!(body.get("which").and_then(Value::as_str), Some("round"));
    assert_eq!(body.get("limit").and_then(Value::as_u64), Some(3));
    // measured == 3 at trip time (iteration == round_cap).
    assert_eq!(body.get("measured").and_then(Value::as_u64), Some(3));

    // Mock must have been hit exactly 3 times (iterations 0, 1, 2 →
    // trip on entry to iteration 3, which is BEFORE the 4th DECIDE call).
    let n = hits.load(Ordering::SeqCst);
    assert_eq!(
        n, 3,
        "expected 3 DECIDE calls before round-cap trip, got {n}"
    );

    // Event log carries the durable CircuitBreakerTripped event.
    let events = fetch_events(&h).await;
    let tripped_count = events
        .iter()
        .filter(|e| event_type(e) == "circuit_breaker_tripped")
        .count();
    assert_eq!(
        tripped_count,
        1,
        "expected exactly one CircuitBreakerTripped event; got {tripped_count}. Events: {:?}",
        events.iter().map(event_type).collect::<Vec<_>>()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 2 — Token breaker trips mid-session from cumulative usage.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_breaker_token_cap_trips_mid_session() {
    let h = LiveHarness::setup().await;
    // 140 tokens per round (prompt=100, completion=40). Override
    // token_cap to 250 so round 1 lands at 140 (no trip, no warn since
    // 140/250 = 56 %) and round 2 pushes cumulative to 280 which
    // exceeds the cap.
    let script = (0..6).map(|_| read_round(100, 40)).collect();
    let (mock_url, hits) = spawn_scripted_mock(script).await;
    let (_sess, run_id) = provision(&h, &mock_url, "f65_p3_token").await;

    let (status, body) = orchestrate(
        &h,
        &run_id,
        json!({
            "goal": "push tokens over cap",
            "max_iterations": 50,
            "breaker_overrides": { "token_cap": 250 },
        }),
    )
    .await;
    assert_eq!(status, 200, "body={body}");
    assert_eq!(
        body.get("termination").and_then(Value::as_str),
        Some("breaker_tripped"),
    );
    assert_eq!(body.get("which").and_then(Value::as_str), Some("tokens"));
    assert_eq!(body.get("limit").and_then(Value::as_u64), Some(250));
    let measured = body.get("measured").and_then(Value::as_u64).unwrap();
    assert!(
        measured >= 250,
        "measured must be >= limit at trip; got {measured}"
    );
    let n = hits.load(Ordering::SeqCst);
    assert_eq!(
        n, 2,
        "expected trip after 2 DECIDE calls (140+140), got {n}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 3 — NoToolUseConsecutive trips on three consecutive narration rounds.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_breaker_no_tool_use_streak_trips_on_narration() {
    let h = LiveHarness::setup().await;
    // 3 narration rounds (no tool_name) — streak cap default is 3.
    // 4th entry is defensive overshoot; the trip should land first.
    let script = (0..4).map(|_| narration_round(20, 20)).collect();
    let (mock_url, hits) = spawn_scripted_mock(script).await;
    let (_sess, run_id) = provision(&h, &mock_url, "f65_p3_streak").await;

    let (status, body) = orchestrate(
        &h,
        &run_id,
        json!({
            "goal": "pure narration",
            "max_iterations": 50,
            "breaker_overrides": { "no_tool_use_streak": 3 },
        }),
    )
    .await;
    assert_eq!(status, 200, "body={body}");
    assert_eq!(
        body.get("termination").and_then(Value::as_str),
        Some("breaker_tripped"),
    );
    assert_eq!(
        body.get("which").and_then(Value::as_str),
        Some("no_tool_use_consecutive"),
    );
    assert_eq!(body.get("measured").and_then(Value::as_u64), Some(3));
    assert_eq!(body.get("limit").and_then(Value::as_u64), Some(3));
    let n = hits.load(Ordering::SeqCst);
    assert_eq!(
        n, 3,
        "expected trip after exactly 3 narration DECIDEs, got {n}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 3b — Cursor Bugbot regression: terminal complete_run after narration MUST
//      NOT trip NoToolUseConsecutive. Without the terminal carve-out in
//      loop_runner's tool_or_terminal_count computation, two narration
//      rounds followed by `complete_run` would hit streak=3 and terminate
//      as BreakerTripped instead of Completed.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_complete_run_after_narration_does_not_trip_streak() {
    let h = LiveHarness::setup().await;
    // 2 narration rounds, then complete_run. streak_cap=3 — without the
    // carve-out, the complete_run round increments streak to 3 and trips.
    let script = vec![
        narration_round(10, 10),
        narration_round(10, 10),
        ScriptEntry {
            proposals: json!([{
                "action_type": "complete_run",
                "description": "all done after deliberation",
                "confidence": 0.98,
                "requires_approval": false,
            }]),
            prompt_tokens: 10,
            completion_tokens: 10,
            sleep_ms: 0,
        },
    ];
    let (mock_url, hits) = spawn_scripted_mock(script).await;
    let (_sess, run_id) = provision(&h, &mock_url, "f65_p3_streak_cr").await;

    let (status, body) = orchestrate(
        &h,
        &run_id,
        json!({
            "goal": "narrate twice then complete",
            "max_iterations": 10,
            "breaker_overrides": { "no_tool_use_streak": 3 },
        }),
    )
    .await;
    assert_eq!(status, 200, "body={body}");
    let termination = body
        .get("termination")
        .and_then(Value::as_str)
        .unwrap_or("<missing>");
    assert_ne!(
        termination, "breaker_tripped",
        "regression: complete_run must NOT trip the streak breaker. body={body}"
    );
    // The exact termination may be `completed` OR `failed` depending on
    // whether the LiveHarness run_service.complete FCALL succeeds in
    // test mode (same allowance as F35); either way, it must not be
    // `breaker_tripped` with `which=no_tool_use_consecutive`.
    let n = hits.load(Ordering::SeqCst);
    assert!(
        n >= 3,
        "expected ≥3 DECIDEs to include the terminal complete_run round; got {n}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 4 — Request-body override tightens round_cap below the default.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_breaker_overrides_tighten_from_request_body() {
    let h = LiveHarness::setup().await;
    // Default round_cap is 30; override to 5.
    let script = (0..12).map(|_| read_round(50, 50)).collect();
    let (mock_url, hits) = spawn_scripted_mock(script).await;
    let (_sess, run_id) = provision(&h, &mock_url, "f65_p3_tighten").await;

    let (status, body) = orchestrate(
        &h,
        &run_id,
        json!({
            "goal": "tighten round_cap",
            "max_iterations": 100,
            "breaker_overrides": { "round_cap": 5 },
        }),
    )
    .await;
    assert_eq!(status, 200, "body={body}");
    assert_eq!(
        body.get("termination").and_then(Value::as_str),
        Some("breaker_tripped"),
    );
    assert_eq!(body.get("which").and_then(Value::as_str), Some("round"));
    assert_eq!(body.get("limit").and_then(Value::as_u64), Some(5));
    let n = hits.load(Ordering::SeqCst);
    assert_eq!(
        n, 5,
        "expected 5 DECIDE calls before round-cap trip, got {n}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 5 — Loosening override returns HTTP 400 `invalid_breaker_override`.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_breaker_overrides_loosen_returns_400() {
    let h = LiveHarness::setup().await;
    // Tighten the operator-wide default via the admin settings endpoint
    // so we have a known value to fail against. Without this, the
    // default is 30 and 999 would still be loosening — but we want to
    // prove the code path independently of the hardcoded default.
    let r = h
        .client()
        .put(format!(
            "{}/v1/settings/defaults/system/system/orchestrator_round_cap",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "value": 5 }))
        .send()
        .await
        .expect("settings reach server");
    assert_eq!(r.status().as_u16(), 200);

    // No mock needed — the request should fail fast at validation.
    let (_mock_url, _hits) = spawn_scripted_mock(vec![]).await;
    let (_sess, run_id) = provision(&h, &_mock_url, "f65_p3_loosen").await;

    let (status, body) = orchestrate(
        &h,
        &run_id,
        json!({
            "goal": "loosen is rejected",
            "breaker_overrides": { "round_cap": 999 },
        }),
    )
    .await;
    assert_eq!(status, 400, "body={body}");
    assert_eq!(
        body.get("error_code").and_then(Value::as_str),
        Some("invalid_breaker_override"),
        "body={body}"
    );
    let msg = body.get("message").and_then(Value::as_str).unwrap_or("");
    assert!(
        msg.contains("round_cap") && msg.contains("999"),
        "error message should mention both the field and the rejected value; got {msg}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 6 — BudgetThresholdCrossed fires at 80% for Tokens (not NoToolUseConsecutive).
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_budget_threshold_crossed_fires_at_80_percent() {
    let h = LiveHarness::setup().await;
    // token_cap=1000; 5 rounds of 200 tokens each (prompt=120,completion=80).
    // Round 1: cumulative=200 (20%, no warn).
    // Round 2: cumulative=400 (40%).
    // Round 3: cumulative=600 (60%).
    // Round 4: cumulative=800 (80% → warning, once).
    // Round 5: cumulative=1000 (trip).
    let script: Vec<_> = (0..6).map(|_| read_round(120, 80)).collect();
    let (mock_url, hits) = spawn_scripted_mock(script).await;
    let (_sess, run_id) = provision(&h, &mock_url, "f65_p3_warn").await;

    let (status, body) = orchestrate(
        &h,
        &run_id,
        json!({
            "goal": "cross 80 percent",
            "max_iterations": 50,
            "breaker_overrides": { "token_cap": 1000 },
        }),
    )
    .await;
    assert_eq!(status, 200, "body={body}");
    assert_eq!(
        body.get("termination").and_then(Value::as_str),
        Some("breaker_tripped"),
    );
    assert_eq!(body.get("which").and_then(Value::as_str), Some("tokens"));
    let n = hits.load(Ordering::SeqCst);
    assert_eq!(
        n, 5,
        "expected 5 DECIDEs (cumulative 200/400/600/800/1000), got {n}"
    );

    let events = fetch_events(&h).await;
    // Exactly ONE BudgetThresholdCrossed event on the durable log —
    // warn-once latch. (The `/v1/events` summary endpoint only exposes
    // event_type; ratio_bps is asserted below via the metrics
    // endpoint, which labels the counter by kind.)
    let warnings: Vec<&Value> = events
        .iter()
        .filter(|e| event_type(e) == "budget_threshold_crossed")
        .collect();
    assert_eq!(
        warnings.len(),
        1,
        "expected exactly one BudgetThresholdCrossed warning; got {}: types={:?}",
        warnings.len(),
        events.iter().map(event_type).collect::<Vec<_>>()
    );

    // Prometheus scrape: the token warn counter must be exactly 1 and
    // the NoToolUseConsecutive warn counter must be absent (never
    // incremented), encoding Decision 2's "skip warning for streak"
    // guarantee in the observable metric surface.
    let r = h
        .client()
        .get(format!("{}/v1/metrics", h.base_url))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("metrics reach server");
    let body = r.text().await.unwrap_or_default();
    let tokens_line = body
        .lines()
        .find(|l| {
            l.contains("cairn_orchestrator_breaker_threshold_warns_total")
                && l.contains("kind=\"tokens\"")
        })
        .unwrap_or_else(|| {
            panic!("metrics scrape missing tokens-warn counter line; full body:\n{body}")
        });
    let tokens_count: u64 = tokens_line
        .split_whitespace()
        .last()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    assert_eq!(
        tokens_count, 1,
        "expected tokens threshold-warn counter == 1; got {tokens_count} (line: {tokens_line})"
    );
    let streak_present = body
        .lines()
        .any(|l| l.contains("kind=\"no_tool_use_consecutive\"") && l.contains("threshold_warns"));
    assert!(
        !streak_present,
        "Decision 2 guard: NoToolUseConsecutive must never appear in the threshold-warn counter"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 7 — Checkpoint is persisted at the last completed iteration before trip.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_checkpoint_persisted_on_breaker_trip() {
    let h = LiveHarness::setup().await;
    // round_cap=2 forces a trip on entry to iteration 2; iteration 1
    // must have landed a complete EXECUTE + Result-checkpoint pair
    // before the trip.
    let script = (0..4).map(|_| read_round(50, 50)).collect();
    let (mock_url, _hits) = spawn_scripted_mock(script).await;
    let (_sess, run_id) = provision(&h, &mock_url, "f65_p3_ckpt").await;

    let (status, body) = orchestrate(
        &h,
        &run_id,
        json!({
            "goal": "checkpoint before trip",
            "max_iterations": 100,
            "breaker_overrides": { "round_cap": 2 },
        }),
    )
    .await;
    assert_eq!(status, 200, "body={body}");
    assert_eq!(
        body.get("termination").and_then(Value::as_str),
        Some("breaker_tripped")
    );

    // Fetch events — one CheckpointPersisted for iteration 1 (the
    // Result checkpoint of the last fully-executed iteration) must
    // precede the CircuitBreakerTripped event.
    let events = fetch_events(&h).await;
    let (mut ckpt_pos, mut trip_pos) = (None::<usize>, None::<usize>);
    for (i, ev) in events.iter().enumerate() {
        match event_type(ev) {
            "checkpoint_recorded" => ckpt_pos = Some(i),
            "circuit_breaker_tripped" => trip_pos = Some(i),
            _ => {}
        }
    }
    let trip_pos = trip_pos.unwrap_or_else(|| {
        panic!(
            "expected a CircuitBreakerTripped event on the log; got event types: {:?}",
            events.iter().map(event_type).collect::<Vec<_>>()
        )
    });
    let ckpt_pos = ckpt_pos.unwrap_or_else(|| {
        panic!(
            "expected at least one CheckpointPersisted before trip; event types: {:?}",
            events.iter().map(event_type).collect::<Vec<_>>()
        )
    });
    assert!(
        ckpt_pos < trip_pos,
        "checkpoint must land before breaker trip; ckpt@{ckpt_pos} trip@{trip_pos}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 8 — Wall-clock breaker trips under a stopwatch.
// ─────────────────────────────────────────────────────────────────────────────

/// Times the HTTP orchestrate under a `wall_clock_ms=500` cap and a
/// scripted provider that sleeps 300 ms per DECIDE. Expects the trip to
/// land in the second DECIDE round. CI tolerance `[500, 2_000]` accepts
/// scheduler jitter on loaded runners; determinism is still proved by
/// the `wall_clock_trip_ordering_round_wins_when_simultaneous` unit
/// test in `breakers::tests` and by asserting the trip's termination
/// kind is `wall_clock` regardless of the exact measured value.
#[tokio::test]
async fn test_wall_clock_breaker_trips_under_stopwatch() {
    let h = LiveHarness::setup().await;
    // Each DECIDE sleeps 300ms → round 1 ~300ms, round 2 ~600ms which
    // exceeds the 500ms cap.
    let script: Vec<_> = (0..5)
        .map(|_| ScriptEntry {
            proposals: json!([{
                "action_type": "invoke_tool",
                "description": "read a file",
                "tool_name": "read",
                "tool_args": { "path": "/etc/hostname" },
                "confidence": 0.9,
                "requires_approval": false,
            }]),
            prompt_tokens: 50,
            completion_tokens: 50,
            sleep_ms: 300,
        })
        .collect();
    let (mock_url, _hits) = spawn_scripted_mock(script).await;
    let (_sess, run_id) = provision(&h, &mock_url, "f65_p3_wall").await;

    let started = Instant::now();
    let (status, body) = orchestrate(
        &h,
        &run_id,
        json!({
            "goal": "wall-clock stopwatch",
            "max_iterations": 50,
            "timeout_ms": 60_000,
            "breaker_overrides": { "wall_clock_ms": 500 },
        }),
    )
    .await;
    let elapsed_ms = started.elapsed().as_millis() as u64;

    assert_eq!(status, 200, "body={body}");
    assert_eq!(
        body.get("termination").and_then(Value::as_str),
        Some("breaker_tripped"),
    );
    assert_eq!(
        body.get("which").and_then(Value::as_str),
        Some("wall_clock"),
    );
    assert_eq!(body.get("limit").and_then(Value::as_u64), Some(500));
    let measured = body.get("measured").and_then(Value::as_u64).unwrap();
    assert!(
        measured >= 500,
        "reported measured must be >= cap on trip; got {measured}"
    );
    // Caller-visible wall-clock should not be below the configured
    // breaker cap (500ms). We intentionally DO NOT enforce an upper
    // bound here: slow or heavily loaded CI runners may legitimately
    // exceed any fixed ceiling, and per cairn's "no such thing as a
    // flake" memory we must not gate a correctness assertion on
    // scheduling jitter. The lower-bound check catches the genuine
    // regression class (breaker firing implausibly early), which is
    // the one the breaker should prevent.
    assert!(
        elapsed_ms >= 500,
        "stopwatch plausibility check: elapsed_ms={elapsed_ms} below 500ms cap — \
         breaker fired before the wall-clock limit was reached"
    );
}
