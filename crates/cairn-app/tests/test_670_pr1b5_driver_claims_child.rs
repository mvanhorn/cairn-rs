//! #670 G4 PR-1b-5: `ChildRunDriver` claim-path end-to-end smoke.
//!
//! PR-1b-3 shipped the driver scaffolding gated off; the claim path
//! was a `tracing::debug!` observe-only stub. PR-1b-5 flipped the
//! gate default-on and wired `drive_run_iteration` as the per-tick
//! dispatch target. This test pins the end-to-end contract: a
//! parent's `spawn_subagent` produces a `Pending` child row; the
//! driver's tick picks it up; the child runs an orchestrator
//! iteration against its own mock LLM; the child's terminal state
//! fires the projection's descendant-counter decrement; the parent's
//! `in_flight_descendants` returns to zero.
//!
//! The assertion needs patience: the driver ticks every 500 ms, the
//! child's orchestrator loop then runs async against the mock, and
//! the terminal event's decrement is a projection write. Polling up
//! to ~8s covers the whole path on CI (typical local runs complete
//! inside 1.5s).
//!
//! ## Why this test is necessary
//!
//! Without it, the driver's claim wiring could silently regress to
//! the observe-only scaffolding shape — the type-level plumbing
//! would still compile, the unit tests would still pass, but child
//! runs would stall forever in `Pending`. This is the single
//! dynamic-behaviour test that proves the end-to-end pipeline works.

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

const MOCK_MODEL: &str = "openrouter/670-pr1b5-driver-claim";
const DELEGATED_ROLE: &str = "researcher";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
}

/// Mock chat completions endpoint. Emits a spawn_subagent proposal
/// on iteration 1 (parent), complete_run on iterations 2+ (both
/// parent and child fall through to complete on their first real
/// decide call). The hits counter is shared so this mock serves
/// both the parent's and the child's orchestrator calls from the
/// same server — keeps the test small. Both runs run once through
/// the loop, which is enough for the claim-path smoke.
async fn chat_handler(
    State(state): State<MockState>,
    Json(_body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let n = state.hits.fetch_add(1, Ordering::SeqCst);
    let content = if n == 0 {
        // First call: parent orchestrate proposes a spawn_subagent.
        json!([{
            "action_type":       "spawn_subagent",
            "description":       "PR-1b-5 parent delegates to researcher",
            "tool_name":         DELEGATED_ROLE,
            "tool_args":         { "goal": "PR-1b-5 child goal" },
            "confidence":        0.95,
            "requires_approval": false,
        }])
    } else {
        // Every subsequent call (parent's post-spawn continuation +
        // the child's first+only iteration): complete.
        json!([{
            "action_type":       "complete_run",
            "description":       "PR-1b-5 done",
            "confidence":        0.99,
            "requires_approval": false,
        }])
    };
    (
        StatusCode::OK,
        Json(json!({
            "id":      format!("mock-670-pr1b5-{n}"),
            "choices": [{
                "index":   0,
                "message": {
                    "role":    "assistant",
                    "content": content.to_string(),
                },
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
    (format!("http://{addr}"), hits)
}

/// Provision credential + provider connection + default models +
/// session + root run. Mirrors the shape used by
/// `test_670_g3_child_run_created_on_spawn.rs::provision_run` —
/// the provisioning surface is not the code under test.
async fn provision_run(h: &LiveHarness, mock_url: &str) -> String {
    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_pr1b5_{suffix}");
    let session_id = format!("sess_pr1b5_{suffix}");
    let run_id = format!("run_pr1b5_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id":     "openrouter",
            "plaintext_value": format!("sk-pr1b5-{suffix}"),
        }))
        .send()
        .await
        .expect("credential reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "credential: {}",
        r.text().await.unwrap_or_default()
    );
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
            .expect("defaults reaches server");
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

    run_id
}

/// Poll `/v1/runs/<child>` every 250ms up to `deadline` waiting for
/// the run to reach a terminal state. Returns the final `run` JSON
/// object. Panics if the deadline elapses without terminal.
async fn poll_child_terminal(h: &LiveHarness, child_run_id: &str, deadline: Duration) -> Value {
    let start = Instant::now();
    loop {
        let r = h
            .client()
            .get(format!("{}/v1/runs/{}", h.base_url, child_run_id))
            .bearer_auth(&h.admin_token)
            .send()
            .await
            .expect("get child reaches server");
        let status = r.status().as_u16();
        if status == 200 {
            let body: Value = r.json().await.unwrap();
            if let Some(run) = body.get("run") {
                let state = run.get("state").and_then(|s| s.as_str()).unwrap_or("");
                if matches!(state, "completed" | "failed" | "canceled") {
                    return run.clone();
                }
            }
        }
        if start.elapsed() > deadline {
            panic!(
                "child {child_run_id} did not reach terminal state within {deadline:?}; \
                 last GET /v1/runs/:id status={status} — driver scan should fire every \
                 500ms, so a terminal state should land within ~3s. If this hits \
                 repeatedly, the driver's claim path has regressed to observe-only.",
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// PR-1b-5 smoke: parent proposes spawn_subagent → child lands Pending
/// → driver tick claims it → child completes → parent's
/// in_flight_descendants returns to 0.
#[tokio::test]
async fn driver_claims_pending_child_and_drives_to_terminal() {
    // Explicitly leave the flag unset (default-on); earlier tests in
    // this file suite may have exported the opt-out.
    std::env::remove_var("CAIRN_CHILD_RUN_DRIVER_ENABLED");

    let h = LiveHarness::setup().await;
    let (mock_url, _hits) = spawn_mock().await;
    let parent_run_id = provision_run(&h, &mock_url).await;

    // Drive ONE parent orchestrate iteration. Mock returns
    // spawn_subagent → FabricTaskServiceAdapter::spawn_subagent runs
    // Phase-1 (child RunRecord created, root_run_id inherited) +
    // counter increment + Phase-2 (task submitted) + Phase-3
    // (SubagentSpawned emit). Parent loop yields
    // ActionStatus::SubagentSpawned which (as of this PR) keeps the
    // parent in a continue-after-delegate shape — the next mock call
    // returns complete_run and the parent terminates.
    let r = h
        .client()
        .post(format!(
            "{}/v1/runs/{}/orchestrate",
            h.base_url, parent_run_id
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "PR-1b-5 parent goal",
            "max_iterations": 2,
        }))
        .send()
        .await
        .expect("parent orchestrate reaches server");
    let status = r.status().as_u16();
    assert!(
        status == 200 || status == 202,
        "parent orchestrate must succeed; status={status} body={}",
        r.text().await.unwrap_or_default(),
    );

    // Find the child via the parent's /children endpoint. The spawn
    // audit row carries the real child_run_id (G3 contract).
    let r = h
        .client()
        .get(format!("{}/v1/runs/{}/children", h.base_url, parent_run_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("children reaches server");
    assert_eq!(r.status().as_u16(), 200);
    let children: Value = r.json().await.unwrap();
    let items: &Vec<Value> = children
        .get("items")
        .and_then(|v| v.as_array())
        .expect("children endpoint returns {items: [...]}");
    assert_eq!(
        items.len(),
        1,
        "spawn_subagent must produce exactly one child row; body={children}",
    );
    let child_run_id = items[0]
        .get("run_id")
        .and_then(|v| v.as_str())
        .expect("child has run_id")
        .to_owned();

    // Wait for the driver to claim + drive the child to terminal.
    // 8s covers a cold-path CI run + one or two driver ticks + the
    // mock round-trip + the projection write propagation.
    let child = poll_child_terminal(&h, &child_run_id, Duration::from_secs(8)).await;
    let child_state = child
        .get("state")
        .and_then(|s| s.as_str())
        .unwrap_or("<missing>");
    // The mock returns complete_run, so "completed" is the expected
    // terminal. A "failed" terminal would still prove the driver
    // ran the child (vs PR-1b-3's observe-only shape where the
    // child sat Pending forever), but it indicates the child
    // orchestrator hit a real error — flag it loudly. The most
    // operator-useful thing is the message + failure_class so we
    // surface them in the panic.
    assert_eq!(
        child_state,
        "completed",
        "driver claimed the child but the child terminated with \
         state={child_state} — expected 'completed' (mock returns \
         complete_run). failure_class={:?}, full body={child}. \
         Anything other than 'completed' indicates either the mock \
         is returning something unexpected or the child's \
         orchestrator loop hit a configuration issue unrelated to \
         the driver's claim path.",
        child.get("failure_class"),
    );

    // Parent's descendant counter must return to 0 after the child's
    // terminal event fires the projection's decrement (RFC 027 §97).
    // `RunRecord.in_flight_descendants` is `#[serde(skip_serializing_if
    // = "is_zero_i64")]`, so the counter returning to 0 manifests as
    // the field being absent from the serialized body. Any present
    // value other than 0 is a counter leak; a missing field is the
    // success signal.
    //
    // Also poll because the child's terminal projection write may
    // propagate a handful of ms after `poll_child_terminal` returns
    // (the detail endpoint reads from the durable backend; the
    // terminal transition raced here).
    let parent_counter_deadline = Duration::from_secs(2);
    let parent_start = Instant::now();
    loop {
        let r = h
            .client()
            .get(format!("{}/v1/runs/{}", h.base_url, parent_run_id))
            .bearer_auth(&h.admin_token)
            .send()
            .await
            .expect("parent get reaches server");
        let body: Value = r.json().await.unwrap();
        let in_flight = body
            .get("run")
            .and_then(|r| r.get("in_flight_descendants"))
            .and_then(|v| v.as_i64());
        // Field absent (None) OR explicitly 0 both mean "counter is 0".
        if in_flight.is_none() || in_flight == Some(0) {
            break;
        }
        if parent_start.elapsed() > parent_counter_deadline {
            panic!(
                "parent's in_flight_descendants did not return to 0 after \
                 child terminal; observed={in_flight:?}, parent body={body}. \
                 RFC 027 §97 contract: child terminal transition decrements \
                 the root's counter via the RunStateChanged projection.",
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
