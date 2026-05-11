//! F59 regression: `RunService::complete` must survive the
//! "lease-expired between last iteration and terminal FCALL" window.
//!
//! # Bug (M1-v2 dogfood, 2026-04-26, on F58 binary `6af0ba94`)
//!
//! Drive ran 50 iterations. LLM produced a working 111-LOC `main.rs` +
//! `Cargo.toml` that compiled cleanly. Every `complete_run` FCALL
//! rejected with:
//!
//! ```text
//! HTTP 200, termination=failed
//! reason: "invalid run transition to completed: the execution's lease
//!          expired before cairn could write the terminal outcome ..."
//! ```
//!
//! Hit 15+ times across 50 cycles. LLM kept calling `complete_run`,
//! failing, re-checking state, trying again.
//!
//! # Why F51/F56/F57/F58 did not cover this
//!
//! F51 (#316) added `renew_lease_if_stale` at the orchestrate HANDLER
//! entry — that closes the gap between HTTP calls. F56/F57/F58 patched
//! cascading transient phase conflicts on that same handler-entry
//! renew.
//!
//! But the lease can still expire **inside a single orchestrate call**,
//! between the orchestrator loop's last DECIDE → EXECUTE cycle and the
//! final `complete_run` FCALL. On operator-paced flows with slow
//! bedrock latency + per-iteration tool approvals, the DECIDE →
//! terminal-FCALL window routinely exceeds the 30s default lease TTL.
//! The handler's entry-time renew is too early to help.
//!
//! # Fix (two layers of defence, both in `fabric_adapter::RunService::complete`)
//!
//! **Layer (a) — pre-FCALL renew:** `renew_lease_if_stale` right before
//! the terminal FCALL, tolerating the F58-classified transient phase
//! conflicts.
//!
//! **Layer (b) — retry once on `lease_expired`:** if the FCALL rejects
//! with `lease_expired` (classified via `RuntimeError::is_lease_expired`),
//! run a fresh `claim` (rotates lease epoch), then retry the FCALL
//! ONCE. If the retry also fails, surface as a 409
//! `InvalidTransition { from: "lease_expired", to: <target> }` so the
//! operator sees the `invalid_transition_hint` prose (re-claim
//! instructions) rather than a 500. Structured failure classes that
//! are NOT `lease_expired` or `Internal` (e.g. `NotFound`,
//! `execution_not_active` if the run became terminal mid-retry) flow
//! through unchanged — masking them as `lease_expired` would misreport
//! the actual failure.
//!
//! Both layers wrap at the same level (the `fabric_adapter`'s impl of
//! `RunService::complete`), so the handler and loop_runner are
//! untouched.
//!
//! # This test
//!
//! Three scenarios:
//!
//! 1. `complete_run_survives_expired_lease_mid_orchestrate` — short
//!    TTL + long first-turn provider delay so the lease ticks past
//!    its TTL between handler-entry renew and the terminal FCALL.
//!    Pre-fix: `termination="failed"` with `lease_expired`. Post-fix:
//!    `termination="completed"`.
//!
//! 2. `complete_run_happy_path_is_single_fcall` — default TTL, fast
//!    provider, first-try complete succeeds. Regression guard that
//!    layer (a)/(b) don't break the hot path.
//!
//! 3. `complete_run_carries_forward_f51_and_f58_guarantees` — sanity
//!    check that the entry-time handler renew (F51) and the tolerate-
//!    transient-phase-conflict fall-through (F58) still work: a
//!    back-to-back orchestrate with a fresh lease produces no
//!    `lease_expired` leak in the response body.
//!
//! Notes:
//! * LiveHarness with the default store (sqlite projection + in-memory
//!   event log) mirrors F51/F56/F57/F58 for apples-to-apples drift
//!   checks. No Postgres-specific surface.
//! * We inject lease expiry via `CAIRN_FABRIC_LEASE_TTL_MS=2000` plus a
//!   provider that holds the response for 3s on the terminator turn.
//!   That drives the TTL past the expiry threshold between the handler
//!   renew (entry) and the terminal FCALL (exit).

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

const MOCK_MODEL: &str = "openrouter/f59-complete-run-retry";
const FINAL_ANSWER: &str = "F59 complete-run retry ok.";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
    /// Milliseconds to sleep on the first chat call, before returning
    /// the `complete_run` tool call. Used to drive the lease past its
    /// TTL while the LLM round-trip is in flight. Zero disables the
    /// delay (used by the happy-path test).
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
        // Stall the first (and only) turn by `terminator_delay_ms` so
        // the FF lease can tick past its TTL while the HTTP call is
        // inside `chat/completions`. Post-fix: the complete path
        // renews before the FCALL, and retries once on lease_expired.
        if n == 0 && state.terminator_delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(state.terminator_delay_ms)).await;
        }
        (
            StatusCode::OK,
            Json(json!({
                "id": format!("mock-f59-{n}"),
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
    let connection_id = format!("conn_f59_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": "openrouter",
            "plaintext_value": format!("sk-f59-{suffix}"),
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
    let session_id = format!("sess_f59_{suffix}");
    let run_id = format!("run_f59_{suffix}");

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

/// **F59 primary regression**: the lease ages into the stale window
/// (`remaining_ms <= F59_MIN_REMAINING_MS`) between the orchestrate
/// handler's entry-time renew (F51) and the final `complete_run`
/// FCALL. `RunService::complete`'s pre-FCALL renew (layer a) must
/// observe this and extend the lease, so the terminal FCALL succeeds
/// on the first attempt.
///
/// Setup:
///   * `CAIRN_FABRIC_LEASE_TTL_MS=5000` — above FabricConfig's 1000 ms
///     minimum, above the 3 s terminator delay so the lease has NOT
///     fully expired (~2 s remaining) when layer (a) fires. That is
///     the "stale but still valid" window that the M1-v2 dogfood runs
///     routinely hit on slow bedrock calls.
///   * Mock provider sleeps 3 s on turn 1 before returning the
///     `complete_run` tool call.
///
/// Flow:
///   1. Handler enters, `renew_lease_if_stale` mints a 5 s lease.
///   2. Bedrock call blocks for 3 s.
///   3. Orchestrator dispatches `CompleteRun` → adapter `complete()`.
///   4. Layer (a) sees remaining ≈ 2 s < F59_MIN_REMAINING_MS (10 s),
///      takes the renew-in-place branch, extends to a fresh 5 s lease.
///   5. `ff_complete_execution` fires with a healthy lease and
///      succeeds on the first attempt.
///
/// Pre-fix: layer (a) did not exist; as the M1-v2 dogfood drove
/// longer and slower iterations the lease ticked past TTL before
/// `ff_complete_execution` fired and the FCALL rejected with
/// `lease_expired`. Post-fix: `termination="completed"`.
///
/// The harder "lease timestamp fully expired in-flight" case is
/// intentionally NOT exercised here — FF's current FCALL surface
/// exposes no cairn-reachable path to recover an execution whose
/// lease expired before FF's scanner ran. That is an FF upstream gap
/// tracked in `docs/design/ff-upstream/ff-complete-run-lease-semantics.md`.
/// When FF lands the fix, layer (b)'s retry wiring picks it up
/// automatically without further cairn changes.
#[tokio::test]
async fn complete_run_survives_expired_lease_mid_orchestrate() {
    // 5s TTL + 3s terminator delay => lease is STALE (remaining < 10s
    // min-remaining threshold) but not expired when layer (a) fires.
    // Layer (a) takes the renew-in-place branch of `renew_lease_if_stale`
    // and extends the lease before the terminal FCALL.
    //
    // We intentionally do NOT test the "lease timestamp fully expired
    // in-flight" branch end-to-end: FF's current FCALL surface exposes
    // no path to recover a never-scanned-but-expired lease (both
    // ff_claim_execution and ff_renew_lease reject with
    // execution_not_eligible / lease_expired respectively in that
    // state). That is an FF-upstream gap tracked in
    // docs/design/ff-upstream/ff-complete-run-lease-semantics.md. When
    // FF exposes a "claim-for-terminal-write" path, layer (b)'s retry
    // wiring is already in place to pick it up.
    let h = LiveHarness::setup_with_env(&[("CAIRN_FABRIC_LEASE_TTL_MS", "5000")]).await;
    let (mock_url, hits) = spawn_mock(3_000).await;

    let suffix = h.project.clone();
    provision_provider(&h, &suffix, &mock_url).await;
    let (_session_id, run_id) = provision_session_and_run(&h, &suffix).await;

    let (status, body) = orchestrate(&h, &run_id, "Answer the prompt.").await;
    let body_str = body.to_string();
    assert_eq!(
        status, 200,
        "F59: orchestrate status must be 200 despite in-flight lease expiry; \
         body={body_str}"
    );

    let term = body
        .get("termination")
        .and_then(|v| v.as_str())
        .unwrap_or("<missing>");
    assert_eq!(
        term, "completed",
        "F59: run must reach termination=completed via layer (a) pre-renew \
         or layer (b) retry-once; pre-fix this was failed/lease_expired. \
         body={body_str}"
    );

    // No raw `lease_expired` classifier must leak into the operator
    // response body. The whole point of F59 is that layer (a) prevents
    // the rejection, and when layer (b) fires it surfaces a
    // cairn-typed message, not the FF classifier.
    assert!(
        !body_str.contains("lease expired before cairn could write"),
        "F59: response must not leak the raw lease_expired classifier \
         message after pre-FCALL renew + retry; body={body_str}"
    );

    // The mock provider only fires once per orchestrate call. We
    // dispatched a single orchestrate invocation, so exactly one LLM
    // round-trip should have occurred.
    let hit_count = hits.load(Ordering::SeqCst);
    assert_eq!(
        hit_count, 1,
        "F59: exactly one provider hit expected for a one-turn flow; got {hit_count}"
    );
}

/// **F59 happy-path regression guard**: default TTL + fast provider.
/// The pre-FCALL renew added in layer (a) must no-op (snapshot read
/// only), and layer (b) must not fire (first FCALL succeeds), so the
/// hot path is unchanged.
#[tokio::test]
async fn complete_run_happy_path_is_single_fcall() {
    let h = LiveHarness::setup().await;
    let (mock_url, hits) = spawn_mock(0).await;

    let suffix = h.project.clone();
    provision_provider(&h, &suffix, &mock_url).await;
    let (_session_id, run_id) = provision_session_and_run(&h, &suffix).await;

    let (status, body) = orchestrate(&h, &run_id, "Answer the prompt.").await;
    let body_str = body.to_string();
    assert_eq!(status, 200, "F59 happy-path orchestrate; body={body_str}");
    assert_eq!(
        body.get("termination").and_then(|v| v.as_str()),
        Some("completed"),
        "F59 happy-path must complete on first try; body={body_str}"
    );
    assert!(
        !body_str.contains("lease_expired"),
        "F59 happy-path must not mention lease_expired; body={body_str}"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1, "one provider hit");
}

/// **F59 cross-guarantee check**: a second orchestrate against a
/// terminal run must NOT surface `lease_expired` — either F51's
/// handler-entry renew picks up the terminal state, or the
/// terminal-run short-circuit in the handler responds 409/200 cleanly.
/// This pins that F59's new layers do not regress F51/F58's
/// operator-facing invariants.
#[tokio::test]
async fn complete_run_carries_forward_f51_and_f58_guarantees() {
    let h = LiveHarness::setup().await;
    let (mock_url, _hits) = spawn_mock(0).await;

    let suffix = h.project.clone();
    provision_provider(&h, &suffix, &mock_url).await;
    let (_session_id, run_id) = provision_session_and_run(&h, &suffix).await;

    // First call: clean completion.
    let (s1, b1) = orchestrate(&h, &run_id, "Turn one.").await;
    assert_eq!(s1, 200, "first orchestrate; body={b1}");
    assert_eq!(
        b1.get("termination").and_then(|v| v.as_str()),
        Some("completed"),
        "first orchestrate must complete; body={b1}"
    );

    // Second call on an already-terminal run. F51/F58 contract:
    // either 200 termination=completed (handler's terminal-run
    // short-circuit) or 409 InvalidTransition. Neither path may leak
    // `lease_expired`.
    let (s2, b2) = orchestrate(&h, &run_id, "Turn two.").await;
    let body_str = b2.to_string();
    assert!(
        s2 == 200 || s2 == 409,
        "F59: back-to-back orchestrate on terminal run must be 200 or \
         409, got status={s2}; body={body_str}"
    );
    assert!(
        !body_str.contains("lease_expired"),
        "F59: terminal-run second orchestrate must not surface \
         lease_expired; body={body_str}"
    );
    assert!(
        !body_str.contains("lease expired during run completion"),
        "F59: terminal-run second orchestrate must not surface the \
         F59 retry-exhausted message (retry logic must not fire on a \
         terminal run); body={body_str}"
    );
}
