//! #765 regression: `POST /v1/runs/:id/orchestrate` must finalize the
//! run even when the HTTP client disconnects mid-loop. Pre-fix, axum
//! cancelled the request task on client disconnect, dropping the
//! orchestrator-loop future and every `tokio::time::timeout` inside
//! it; `finalize_run_failure` never fired; runs stuck in
//! `state=running` forever.
//!
//! Test shape:
//!   1. Mock LLM that sleeps 4s before responding — slow enough that
//!      the test client can disconnect mid-loop.
//!   2. POST /orchestrate with a tight reqwest timeout (300ms).
//!   3. The POST errors out client-side with a timeout — axum sees
//!      the connection drop and cancels the request handler. Pre-fix
//!      this would orphan the run.
//!   4. Wait long enough for the spawned task to run to terminal.
//!   5. GET /v1/runs/:id and assert the run reached terminal state.
//!
//! Pre-fix this test would fail at step 5 with `state=running`.
//! Post-fix the spawned `tokio::spawn` task survives request-task
//! cancellation; the loop runs to terminal; the run is finalized
//! `Failed(execution_error)` (mock returns 401 once it wakes).

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

const MOCK_MODEL: &str = "openrouter/765-slow-fail";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
}

/// Sleeps 4 seconds, then returns 401. The 4s is long enough that a
/// client-side timeout of 300ms will fire WAY before the response is
/// emitted — proving the cancellation case. After it wakes, returning
/// 401 produces a deterministic `OrchestratorError::ProviderAuthFailed`
/// that finalizes the run as `Failed`.
async fn slow_chat_handler(
    State(state): State<MockState>,
    Json(_body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    state.hits.fetch_add(1, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(4)).await;
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "error": {
                "message": "forced upstream 401 for #765 regression test",
                "code": 401,
                "type": "invalid_api_key",
            }
        })),
    )
}

async fn spawn_mock() -> (String, Arc<AtomicUsize>) {
    let state = MockState {
        hits: Arc::new(AtomicUsize::new(0)),
    };
    let hits = state.hits.clone();
    let app = Router::new()
        .route("/chat/completions", post(slow_chat_handler))
        .route("/v1/chat/completions", post(slow_chat_handler))
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
    tokio::time::sleep(Duration::from_millis(25)).await;
    (format!("http://{addr}"), hits)
}

#[tokio::test]
async fn orchestrate_survives_client_disconnect() {
    let h = LiveHarness::setup().await;
    let (mock_url, hits) = spawn_mock().await;

    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_765_{suffix}");
    let session_id = format!("sess_765_{suffix}");
    let run_id = format!("run_765_{suffix}");

    // Provision credential, connection, defaults, session, run.
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id":     "openrouter",
            "plaintext_value": format!("sk-765-{suffix}"),
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

    // ── Core #765 reproducer: POST with a tight client timeout. ───────
    // The mock's chat endpoint sleeps 4s. With a 300ms client timeout,
    // reqwest aborts the request long before the LLM responds. axum
    // sees the TCP connection drop and cancels the request task.
    //
    // Pre-fix: cancellation drops the orchestrator-loop future
    // mid-iteration; `finalize_run_failure` never fires; the run
    // sits at `state=running` indefinitely.
    //
    // Post-fix: the loop runs in a `tokio::spawn` task that the
    // runtime owns, NOT the request task. Cancellation only severs
    // the response delivery — the spawned task continues.
    let cancelling_client = reqwest::Client::builder()
        .timeout(Duration::from_millis(300))
        .build()
        .expect("client");
    let cancelled_send = cancelling_client
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal":           "exercise client-disconnect path for #765 regression",
            "max_iterations": 1,
        }))
        .send()
        .await;
    assert!(
        cancelled_send.is_err(),
        "request should have timed out client-side; got: {:?}",
        cancelled_send.map(|r| r.status())
    );

    // Wait long enough for the spawned task to finish and finalize.
    // Mock sleeps 4s, the loop's HTTP-401-classification path is
    // synchronous after that, finalize is two event-store appends.
    // 10s is generous slack for CI.
    let mut final_state: Option<String> = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let r = h
            .client()
            .get(format!("{}/v1/runs/{}", h.base_url, run_id))
            .bearer_auth(&h.admin_token)
            .send()
            .await
            .expect("get run reaches server");
        if r.status().as_u16() != 200 {
            continue;
        }
        let body = r.json::<Value>().await.unwrap_or(Value::Null);
        if let Some(state) = body
            .get("run")
            .and_then(|v| v.get("state"))
            .and_then(|v| v.as_str())
        {
            if state == "failed" || state == "completed" || state == "canceled" {
                final_state = Some(state.to_owned());
                break;
            }
        }
    }

    let final_state = final_state.unwrap_or_else(|| {
        panic!(
            "#765: run never reached terminal state after client disconnect — \
             pre-fix this is the wedge symptom (orphaned run stuck at \
             state=running). Mock hits: {}. The spawned orchestrator-loop \
             task should survive client cancellation and finalize the run \
             via the same code path it uses on a normal POST.",
            hits.load(Ordering::SeqCst),
        )
    });

    assert!(
        matches!(final_state.as_str(), "failed" | "completed" | "canceled"),
        "#765: run must be terminal post-disconnect; got state={final_state:?}",
    );
    // The mock returns 401 → ProviderAuthFailed → execution_error.
    // (Other terminal classes are also acceptable as long as we're
    // terminal — the bug being prevented is "stuck running forever",
    // not "didn't pick the exact failure_class".)
    assert!(
        hits.load(Ordering::SeqCst) >= 1,
        "mock provider should have been called at least once",
    );
}
