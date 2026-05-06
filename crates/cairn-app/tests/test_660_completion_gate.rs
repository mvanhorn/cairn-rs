//! Issue #660 regression — the orchestrator's strict completion gate must
//! refuse `complete_run` when the F47 verification accumulator still has
//! errors, and must terminate the run in `FailureClass::VerificationRejected`
//! after three consecutive refusals.
//!
//! **Bug (dogfood R4, 2026-05-03).** The LLM called `complete_run` while the
//! previous `bash` tool_result contained five cargo borrow-checker errors
//! and exit code 101. The orchestrator accepted the call unconditionally
//! and flipped the run's state to `completed`. Operators who keyed their
//! CI automation on `state == "completed"` saw "success" while the run's
//! own summary started with "## Status: Compilation Failed".
//!
//! **Fix (PR for #660).** With `orchestrator_strict_completion_gate = true`
//! (the default), the loop now inspects the incremental
//! `VerificationAccumulator` before dispatching a `CompleteRun` proposal.
//! If `errors` is non-empty the proposal is stripped in place, a rejection
//! `StepSummary` is pushed into the decide window, and the loop re-enters
//! GATHER → DECIDE. A three-strike cap terminates the run in
//! `Failed(VerificationRejected)` so a non-converging model cannot burn the
//! full iteration budget ping-ponging against the gate.
//!
//! This file drives a full HTTP run with a scripted provider:
//!   * turn 0 → `invoke_tool bash` that surfaces two `error:` lines.
//!   * turn 1+ → `complete_run` on every subsequent turn.
//!
//! Two tests:
//!
//! 1. `strict_gate_blocks_complete_run_and_fails_after_three_rejects` —
//!    default flag value (ON). Run MUST NOT transition to `Completed`.
//!    The handler response body's `termination` must be `"failed"`, and
//!    `GET /v1/runs/:id` must report `state = "failed"` with
//!    `failure_class = "verification_rejected"`. Before the gate landed,
//!    this same fixture terminated as `completed` with a red build in its
//!    summary — the fixture is therefore the "would fail before the
//!    gate, passes after" signal issue #660 asked for.
//!
//! 2. `strict_gate_disabled_per_run_accepts_complete_run_with_errors` —
//!    the inverse. After toggling
//!    `run:<id>:orchestrator_strict_completion_gate` to `false` via the
//!    defaults-projection PUT, the same scripted provider completes the
//!    run as before. Proves the flag is load-bearing.

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

const MODEL_ID: &str = "openrouter/test-660-gate-model";

/// Two cargo-like error lines the bash turn surfaces in stdout. The
/// F47 extractor buckets them into `errors[]`; the gate reads
/// `error_count() > 0` and refuses `complete_run`.
const CARGO_ERROR_MARKER_A: &str = "error[E0382]: borrow of moved value: `x`";
const CARGO_ERROR_MARKER_B: &str = "error: could not compile `demo` due to previous error";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
}

/// Scripted provider:
///   * turn 0 -> `invoke_tool bash` that prints cargo-like error lines
///     to stdout. The cairn bash adapter surfaces stdout on the
///     `ActionResult.tool_output` which the F47 accumulator scans in
///     place.
///   * turn 1+ -> `complete_run`, forever. The gate's job is to keep
///     refusing these proposals until the accumulator is clean or the
///     3-strike cap trips.
async fn spawn_mock() -> (String, Arc<AtomicUsize>) {
    let state = MockState {
        hits: Arc::new(AtomicUsize::new(0)),
    };
    let hits = state.hits.clone();

    async fn chat_handler(
        State(state): State<MockState>,
        Json(_body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        let n = state.hits.fetch_add(1, Ordering::SeqCst);
        let content = if n == 0 {
            // `printf` lets us emit multi-line stdout that the bash
            // tool adapter will relay verbatim. The `\n` escapes are
            // inside the single-quoted shell arg so the shell sees
            // them and the tool_result carries two separate lines the
            // F47 extractor can match against its `error:` regex.
            let cmd = format!(
                "printf '%s\\n%s\\n' '{}' '{}'",
                CARGO_ERROR_MARKER_A, CARGO_ERROR_MARKER_B,
            );
            json!([{
                "action_type":       "invoke_tool",
                "description":       "#660: run cargo build (simulated failure)",
                "tool_name":         "bash",
                "tool_args":         { "command": cmd },
                "confidence":        0.95,
                "requires_approval": false,
            }])
        } else {
            // Every subsequent turn proposes `complete_run`. Without the
            // gate the first call flips the run to `completed`; with
            // the gate all three get stripped and the loop falls into
            // `LoopTermination::Failed { reason: "verification_rejected: …" }`.
            json!([{
                "action_type":       "complete_run",
                "description":       "#660: all done (LLM lying about the build)",
                "confidence":        0.99,
                "requires_approval": false,
            }])
        };
        (
            StatusCode::OK,
            Json(json!({
                "id":      format!("mock-660-{n}"),
                "choices": [{
                    "index":   0,
                    "message": {
                        "role":    "assistant",
                        "content": content.to_string(),
                    },
                    "finish_reason": "stop",
                }],
                "usage": {
                    "prompt_tokens":     12,
                    "completion_tokens": 8,
                    "total_tokens":      20,
                },
            })),
        )
    }

    let app = Router::new()
        .route("/chat/completions", post(chat_handler))
        .route("/v1/chat/completions", post(chat_handler))
        .route(
            "/v1/models",
            get(|| async { Json(json!({ "data": [{ "id": MODEL_ID }] })) }),
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

/// Boilerplate: provision credential + provider connection + defaults +
/// session + run. Shared between the two tests so any drift in the
/// provisioning surface fails both.
///
/// Returns the (tenant, session_id, run_id) triple so the caller can
/// drive orchestrate + read back the final run state.
async fn provision_run(h: &LiveHarness, mock_url: &str) -> (String, String, String) {
    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_660_{suffix}");
    let session_id = format!("sess_660_{suffix}");
    let run_id = format!("run_660_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id":     "openrouter",
            "plaintext_value": format!("sk-660-{suffix}"),
        }))
        .send()
        .await
        .expect("credential");
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
            "supported_models":       [MODEL_ID],
            "credential_id":          credential_id,
            "endpoint_url":           mock_url,
        }))
        .send()
        .await
        .expect("connection");
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
            .json(&json!({ "value": MODEL_ID }))
            .send()
            .await
            .expect("defaults");
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
        .expect("session");
    assert_eq!(r.status().as_u16(), 201);

    // #702 follow-up: this test's stub LLM emits a bash call with
    // `printf` (a mutation-adjacent verb not on the orchestrator
    // allowlist). The test's semantic is the #660 completion gate,
    // orthogonal to orchestrator doctrine. Pin the run's role to
    // `executor` via project defaults so the orchestrator bash
    // policy doesn't fire.
    let r = h
        .client()
        .put(format!(
            "{}/v1/settings/defaults/project/{project}/run:{run_id}:agent_role",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "value": "executor" }))
        .send()
        .await
        .expect("set agent_role default");
    assert_eq!(r.status().as_u16(), 200);

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
        .expect("run");
    assert_eq!(r.status().as_u16(), 201);

    (tenant, session_id, run_id)
}

/// Read back the run record so tests can assert the terminal
/// `state` + `failure_class` honestly reflects what the gate did.
async fn fetch_run_state(h: &LiveHarness, run_id: &str) -> Value {
    let r = h
        .client()
        .get(format!("{}/v1/runs/{}", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("get run");
    assert_eq!(r.status().as_u16(), 200, "get run: {run_id}");
    r.json::<Value>().await.expect("run json")
}

/// #660 (1) — strict gate ON (default). The LLM's `complete_run` MUST
/// NOT flip the run to `completed` while verification errors are live.
///
/// Negative-before-after: delete the `orchestrator_strict_completion_gate`
/// flag from `LoopConfig` and this test will return `completed` with a
/// failing build in the summary (reproducing the dogfood R4 bug
/// verbatim); the assertion below then fires. After the gate landed, the
/// test passes with `termination=failed` and
/// `failure_class=verification_rejected`.
#[tokio::test]
async fn strict_gate_blocks_complete_run_and_fails_after_three_rejects() {
    let h = LiveHarness::setup().await;
    let (mock_url, hits) = spawn_mock().await;
    let (_tenant, _session, run_id) = provision_run(&h, &mock_url).await;

    // Generous enough max_iterations that the gate's 3-strike cap is
    // what fires, not the iteration budget. The dogfood R4 bug
    // triggered at iteration 3 of an 8-iteration run, so 10 is well
    // above the observed window.
    let orch = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal":           "#660: pretend to fix the borrow errors, then call complete_run",
            "max_iterations": 10,
        }))
        .timeout(Duration::from_secs(60))
        .send()
        .await
        .expect("orchestrate");
    let orch_status = orch.status().as_u16();
    let orch_body: Value = orch.json().await.unwrap_or(Value::Null);

    // The handler's response body uses `termination` as the canonical
    // shape (the same field F38 / F47 test against). `failed` is the
    // wire label for `LoopTermination::Failed`.
    assert_eq!(
        orch_status, 200,
        "orchestrate must return 200 even on gate-induced failure: body={orch_body}",
    );
    let termination = orch_body
        .get("termination")
        .and_then(Value::as_str)
        .unwrap_or("<missing>");
    assert_ne!(
        termination, "completed",
        "#660 regression: strict gate must NOT let `complete_run` through \
         while verification has errors. Full body: {orch_body}",
    );
    assert_eq!(
        termination, "failed",
        "expected `failed` (three rejects exhausted); got {termination}. body={orch_body}",
    );

    // The response's `reason` field must carry the contract prefix
    // the handler's `classify_failed_reason` keys on.
    let reason = orch_body
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or("");
    assert!(
        reason.starts_with("verification_rejected:"),
        "failure reason must use the `verification_rejected:` prefix; got: {reason:?}"
    );

    // The run projection must reflect the terminal failure with the new
    // `VerificationRejected` class so operator dashboards and filter
    // queries don't have to re-parse the reason string.
    let run = fetch_run_state(&h, &run_id).await;
    let state = run
        .get("run")
        .and_then(|r| r.get("state"))
        .and_then(Value::as_str)
        .or_else(|| run.get("state").and_then(Value::as_str))
        .unwrap_or("<missing>");
    assert_eq!(
        state, "failed",
        "#660: run projection must reflect the gate-induced failure; got {state}. full={run}",
    );
    let failure_class = run
        .get("run")
        .and_then(|r| r.get("failure_class"))
        .and_then(Value::as_str)
        .or_else(|| run.get("failure_class").and_then(Value::as_str))
        .unwrap_or("<missing>");
    assert_eq!(
        failure_class, "verification_rejected",
        "#660: failure_class must be `verification_rejected`; got {failure_class}. full={run}",
    );

    // Sanity: the mock must have been called AT LEAST four times —
    // one bash + at least three rejected complete_run proposals.
    // The gate strips `complete_run` silently, so the loop keeps
    // calling DECIDE which keeps calling the provider.
    assert!(
        hits.load(Ordering::SeqCst) >= 4,
        "#660: scripted provider must see >=4 calls (1 bash + 3 complete_run rejections); \
         got {}",
        hits.load(Ordering::SeqCst),
    );
}

/// #660 (2) — operator flips
/// `run:<id>:orchestrator_strict_completion_gate = false` via the
/// defaults-projection PUT BEFORE `/orchestrate` fires, and the same
/// scripted provider completes normally (pre-#660 behaviour). Proves
/// the flag is the on/off switch the handler advertises.
#[tokio::test]
async fn strict_gate_disabled_per_run_accepts_complete_run_with_errors() {
    let h = LiveHarness::setup().await;
    let (mock_url, _hits) = spawn_mock().await;
    let (_tenant, _session, run_id) = provision_run(&h, &mock_url).await;

    // Persist the per-run default BEFORE orchestrate so the handler
    // sees it on the first lookup and does not back-fill `true`.
    // Scope: Project. Key: `run:<id>:orchestrator_strict_completion_gate`.
    // Value: native JSON `false` — `resolve_run_bool_default` will pick
    // up either the bool or the string form, but the bool form is the
    // canonical shape the persist helper writes.
    let key = format!("run:{run_id}:orchestrator_strict_completion_gate");
    let r = h
        .client()
        .put(format!(
            "{}/v1/settings/defaults/project/default_project/{key}",
            h.base_url,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "value": false }))
        .send()
        .await
        .expect("defaults PUT");
    assert!(
        (200..300).contains(&r.status().as_u16()),
        "setting the per-run flag must succeed: {} {}",
        r.status(),
        r.text().await.unwrap_or_default(),
    );

    let orch = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal":           "#660 inverse: gate OFF → complete_run is accepted",
            "max_iterations": 10,
        }))
        .timeout(Duration::from_secs(60))
        .send()
        .await
        .expect("orchestrate");
    let orch_status = orch.status().as_u16();
    let orch_body: Value = orch.json().await.unwrap_or(Value::Null);
    assert_eq!(
        orch_status, 200,
        "orchestrate: {} body={orch_body}",
        orch_status,
    );

    let termination = orch_body
        .get("termination")
        .and_then(Value::as_str)
        .unwrap_or("<missing>");
    assert_eq!(
        termination, "completed",
        "#660 inverse: with the gate OFF the LLM's `complete_run` must flow \
         through exactly as it did pre-#660. body={orch_body}",
    );

    // The run projection must match. The dogfood R4 bug surfaced as
    // `state=completed` + failing build; the inverse test deliberately
    // reproduces that shape to prove the flag actually flips behaviour.
    let run = fetch_run_state(&h, &run_id).await;
    let state = run
        .get("run")
        .and_then(|r| r.get("state"))
        .and_then(Value::as_str)
        .or_else(|| run.get("state").and_then(Value::as_str))
        .unwrap_or("<missing>");
    assert_eq!(
        state, "completed",
        "#660 inverse: gate OFF → run projection must settle on `completed`; \
         got {state}. full={run}",
    );
}
