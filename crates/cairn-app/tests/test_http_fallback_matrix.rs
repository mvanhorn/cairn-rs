//! End-to-end HTTP regression test for the full provider-fallback matrix:
//! per-model retry (#693 R3-A), cross-model within a connection,
//! cross-connection across the tenant, and the AllProvidersExhausted →
//! WaitingApproval state transition (#693 R3-B).
//!
//! Complements `test_http_dogfood_fallback.rs`, which exercises the
//! 2-model single-connection case. This file covers the full
//! production shape: multiple connections × multiple models per
//! connection, with the error distributed across the matrix so each
//! axis gets an independent assertion.
//!
//! # Scenarios
//!
//! | Test                                       | What it pins                         |
//! |--------------------------------------------|--------------------------------------|
//! | `cross_connection_fallback_advances`       | Connection-A exhausts → Connection-B |
//! |                                            | takes over with a successful model   |
//! | `same_model_retry_recovers_from_transient` | Preferred returns 503 once then ok — |
//! |                                            | retry + backoff keeps it on preferred|
//! |                                            | without advancing to the next model  |
//! | `full_matrix_exhaust_flips_to_waiting_approval` | Every cell fails → run state       |
//! |                                            | transitions to waiting_approval per  |
//! |                                            | R3-B + escalate_to_operator approval |
//! |                                            | card is submitted                    |

mod support;

use std::sync::{Arc, Mutex};

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

/// One axis-1 response script: how the mock handles a particular
/// `model_id` on each successive call. Exhausting the script = panic
/// (the test didn't account for how many calls its fallback made).
#[derive(Clone)]
enum Step {
    /// Return HTTP 503. Classifies as `upstream_5xx`, advances the
    /// chain (or retries same-model per R3-A retry budget).
    ServerError,
    /// Return HTTP 429. Classifies as `rate_limited`, records
    /// cooldown, advances *without* consuming the same-model retry
    /// budget.
    RateLimited,
    /// Return a valid `complete_run` action so the run terminates
    /// cleanly. This must be used at least once on every
    /// non-exhaustion-test path — otherwise the fabric loop will keep
    /// polling.
    Ok,
}

#[derive(Clone)]
struct MockState {
    /// Per-model scripts (queued deque). `None` slot means "this model
    /// is unknown, return a 400 so the mock fails loudly — don't
    /// silently accept an unexpected model_id."
    scripts: Arc<Mutex<std::collections::HashMap<String, Vec<Step>>>>,
    /// Per-model hit counter. Assertion surface.
    hits: Arc<Mutex<std::collections::HashMap<String, usize>>>,
}

impl MockState {
    fn new(scripts: Vec<(&str, Vec<Step>)>) -> Self {
        let mut map = std::collections::HashMap::new();
        for (model, script) in scripts {
            map.insert(model.to_owned(), script);
        }
        Self {
            scripts: Arc::new(Mutex::new(map)),
            hits: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }

    fn hit(&self, model: &str) {
        let mut hits = self.hits.lock().unwrap();
        *hits.entry(model.to_owned()).or_default() += 1;
    }

    fn hits_for(&self, model: &str) -> usize {
        self.hits.lock().unwrap().get(model).copied().unwrap_or(0)
    }
}

async fn chat_handler(
    State(state): State<MockState>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let model = body
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_owned();
    state.hit(&model);

    let mut scripts = state.scripts.lock().unwrap();
    let queue = match scripts.get_mut(&model) {
        Some(q) => q,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": {"message": format!("mock: unknown model {model}")}})),
            );
        }
    };
    if queue.is_empty() {
        // Panic (not 500) because an exhausted script is a test
        // mis-configuration — the assertion surface should surface
        // it loudly. Returning 500 would turn the test panic into a
        // cryptic orchestrate failure down the call chain.
        panic!("mock: script exhausted for model {model} (test needs more Steps)");
    }
    let step = queue.remove(0);
    match step {
        Step::ServerError => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": {"message": "simulated 503"}})),
        ),
        Step::RateLimited => (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error": {"message": "simulated 429", "code": 429}})),
        ),
        Step::Ok => (
            StatusCode::OK,
            Json(json!({
                "id": format!("mock-ok-{model}"),
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": json!([{
                            "action_type": "complete_run",
                            "description": "done via fallback",
                            "confidence": 0.95,
                            "requires_approval": false,
                        }]).to_string(),
                    },
                    "finish_reason": "stop",
                }],
                "usage": {"prompt_tokens": 10, "completion_tokens": 6, "total_tokens": 16},
            })),
        ),
    }
}

/// Spawn a mock provider serving a fixed model list and a scripted
/// per-model response queue.
async fn spawn_mock(models: &[&str], state: MockState) -> String {
    let models_payload: Vec<Value> = models.iter().map(|m| json!({"id": *m})).collect();
    let list_response = json!({"data": models_payload});

    let app = Router::new()
        .route("/chat/completions", post(chat_handler))
        .route("/v1/chat/completions", post(chat_handler))
        .route(
            "/v1/models",
            get(move || {
                let body = list_response.clone();
                async move { Json(body) }
            }),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    format!("http://{addr}")
}

/// Boilerplate: credential + session + run for a tenant.
async fn provision_run(h: &LiveHarness, tenant: &str, session_id: &str, run_id: &str) -> String {
    let suffix = h.project.clone();
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": "openrouter",
            "plaintext_value": format!("sk-fallback-{suffix}"),
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
        .post(format!("{}/v1/sessions", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id": tenant,
            "workspace_id": "default_workspace",
            "project_id": "default_project",
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
            "workspace_id": "default_workspace",
            "project_id": "default_project",
            "session_id": session_id,
            "run_id": run_id,
        }))
        .send()
        .await
        .expect("run reaches server");
    assert_eq!(r.status().as_u16(), 201);

    credential_id
}

async fn register_connection(
    h: &LiveHarness,
    tenant: &str,
    connection_id: &str,
    models: &[&str],
    credential_id: &str,
    endpoint_url: &str,
) {
    let r = h
        .client()
        .post(format!("{}/v1/providers/connections", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id": tenant,
            "provider_connection_id": connection_id,
            "provider_family": "openrouter",
            "adapter_type": "openrouter",
            "supported_models": models,
            "credential_id": credential_id,
            "endpoint_url": endpoint_url,
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
}

async fn set_system_default_model(h: &LiveHarness, model: &str) {
    for key in ["generate_model", "brain_model"] {
        let r = h
            .client()
            .put(format!(
                "{}/v1/settings/defaults/system/system/{}",
                h.base_url, key,
            ))
            .bearer_auth(&h.admin_token)
            .json(&json!({ "value": model }))
            .send()
            .await
            .expect("defaults reaches server");
        assert_eq!(r.status().as_u16(), 200);
    }
}

// ── Scenario 1: cross-connection fallback ───────────────────────────────────

/// Connection-A has both its models fail with 5xx. Connection-B's first
/// model succeeds. The run must complete via Connection-B without
/// exhausting. Proves axis-2 (cross-binding) composition.
#[tokio::test]
async fn cross_connection_fallback_advances() {
    let h = LiveHarness::setup().await;
    let tenant = "default_tenant".to_owned();
    let suffix = h.project.clone();
    let session_id = format!("sess_xconn_{suffix}");
    let run_id = format!("run_xconn_{suffix}");

    // Connection-A: both models return 5xx on every attempt. Scripts
    // cover initial + 2 retries for each = 3 responses per model (R3-A
    // same-model retry budget).
    let state_a = MockState::new(vec![
        (
            "primary/a-model-1",
            vec![Step::ServerError, Step::ServerError, Step::ServerError],
        ),
        (
            "primary/a-model-2",
            vec![Step::ServerError, Step::ServerError, Step::ServerError],
        ),
    ]);
    // Connection-B: first model succeeds on the first attempt.
    let state_b = MockState::new(vec![("secondary/b-model-1", vec![Step::Ok])]);

    let mock_a = spawn_mock(&["primary/a-model-1", "primary/a-model-2"], state_a.clone()).await;
    let mock_b = spawn_mock(&["secondary/b-model-1"], state_b.clone()).await;

    let credential_id = provision_run(&h, &tenant, &session_id, &run_id).await;

    // Register Connection-A FIRST so it's tried first in the
    // cross-connection chain.
    register_connection(
        &h,
        &tenant,
        &format!("conn_a_{suffix}"),
        &["primary/a-model-1", "primary/a-model-2"],
        &credential_id,
        &mock_a,
    )
    .await;
    register_connection(
        &h,
        &tenant,
        &format!("conn_b_{suffix}"),
        &["secondary/b-model-1"],
        &credential_id,
        &mock_b,
    )
    .await;

    set_system_default_model(&h, "primary/a-model-1").await;

    let r = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({"goal": "succeed via axis-2 fallback", "max_iterations": 1}))
        .send()
        .await
        .expect("orchestrate reaches server");
    let status = r.status().as_u16();
    let body = r.text().await.unwrap_or_default();
    assert!(
        status == 200,
        "orchestrate must succeed via Connection-B; status={status} body={body}",
    );

    // Connection-A's both models must have been attempted (with retries —
    // so ≥3 per model). Connection-B's first model must have succeeded
    // on attempt 1. Nothing from Connection-B except the successful model
    // should have been touched.
    // Exact count: 1 initial attempt + 2 retries = 3 per model.
    // Using `assert_eq!` (not `>=`) because we know the precise
    // number — a looser bound would hide a retry-loop bug that
    // over-counts attempts.
    assert_eq!(
        state_a.hits_for("primary/a-model-1"),
        3,
        "primary/a-model-1 must see exactly 3 attempts (1 initial + 2 retries)",
    );
    assert_eq!(
        state_a.hits_for("primary/a-model-2"),
        3,
        "primary/a-model-2 must see exactly 3 attempts after axis-1 advance",
    );
    assert_eq!(
        state_b.hits_for("secondary/b-model-1"),
        1,
        "secondary/b-model-1 must be the first successful call (exactly 1 hit)",
    );
}

// ── Scenario 2: same-model retry on transient ───────────────────────────────

/// Preferred model returns one 5xx then succeeds. R3-A's retry-with-
/// backoff must absorb the transient without advancing the chain.
/// Proves per-model retry is doing its job — a transient blip no
/// longer consumes a chain cell.
#[tokio::test]
async fn same_model_retry_recovers_from_transient() {
    let h = LiveHarness::setup().await;
    let tenant = "default_tenant".to_owned();
    let suffix = h.project.clone();
    let session_id = format!("sess_retry_{suffix}");
    let run_id = format!("run_retry_{suffix}");

    // Preferred model: 5xx once, then Ok on retry. Fallback model
    // scripted too just in case — but if it's ever hit, the test
    // fails (the retry should absorb the transient).
    let state = MockState::new(vec![
        ("preferred/flappy", vec![Step::ServerError, Step::Ok]),
        ("backup/never-hit", vec![Step::Ok]),
    ]);
    let mock_url = spawn_mock(&["preferred/flappy", "backup/never-hit"], state.clone()).await;

    let credential_id = provision_run(&h, &tenant, &session_id, &run_id).await;
    register_connection(
        &h,
        &tenant,
        &format!("conn_{suffix}"),
        &["preferred/flappy", "backup/never-hit"],
        &credential_id,
        &mock_url,
    )
    .await;
    set_system_default_model(&h, "preferred/flappy").await;

    let r = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({"goal": "retry absorbs transient", "max_iterations": 1}))
        .send()
        .await
        .expect("orchestrate reaches server");
    assert_eq!(r.status().as_u16(), 200);

    // Preferred model must have seen exactly 2 hits: the initial 503
    // + the retry success. Fallback must have been untouched.
    assert_eq!(
        state.hits_for("preferred/flappy"),
        2,
        "preferred must see 1 failure + 1 retry-success; got {}",
        state.hits_for("preferred/flappy"),
    );
    assert_eq!(
        state.hits_for("backup/never-hit"),
        0,
        "fallback must be untouched when retry absorbs the transient",
    );
}

// ── Scenario 3: full matrix exhaust → WaitingApproval ──────────────────────

/// Every cell in the 2×2 matrix fails with a fallback-eligible error.
/// The run transitions to `waiting_approval` (R3-B) and the
/// `escalate_to_operator` approval card is visible via the approvals
/// endpoint. Proves both the exhaustion path and the state-transition
/// fix ship together.
#[tokio::test]
async fn full_matrix_exhaust_flips_to_waiting_approval() {
    let h = LiveHarness::setup().await;
    let tenant = "default_tenant".to_owned();
    let suffix = h.project.clone();
    let session_id = format!("sess_exhaust_{suffix}");
    let run_id = format!("run_exhaust_{suffix}");

    // Both connections: every model fails with rate-limited (skips retry,
    // advances immediately via cooldown). This keeps the test fast
    // (no 1-3s backoff per cell) while still fully exhausting the
    // matrix.
    let state_a = MockState::new(vec![
        ("a/exhaust-1", vec![Step::RateLimited]),
        ("a/exhaust-2", vec![Step::RateLimited]),
    ]);
    let state_b = MockState::new(vec![
        ("b/exhaust-1", vec![Step::RateLimited]),
        ("b/exhaust-2", vec![Step::RateLimited]),
    ]);

    let mock_a = spawn_mock(&["a/exhaust-1", "a/exhaust-2"], state_a.clone()).await;
    let mock_b = spawn_mock(&["b/exhaust-1", "b/exhaust-2"], state_b.clone()).await;

    let credential_id = provision_run(&h, &tenant, &session_id, &run_id).await;
    register_connection(
        &h,
        &tenant,
        &format!("conn_a_{suffix}"),
        &["a/exhaust-1", "a/exhaust-2"],
        &credential_id,
        &mock_a,
    )
    .await;
    register_connection(
        &h,
        &tenant,
        &format!("conn_b_{suffix}"),
        &["b/exhaust-1", "b/exhaust-2"],
        &credential_id,
        &mock_b,
    )
    .await;
    set_system_default_model(&h, "a/exhaust-1").await;

    // Orchestrate expects 502 with `all_providers_exhausted`.
    let r = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({"goal": "exhaust everything", "max_iterations": 1}))
        .send()
        .await
        .expect("orchestrate reaches server");
    let status = r.status().as_u16();
    let body = r.text().await.unwrap_or_default();
    assert_eq!(
        status, 502,
        "exhaustion must return 502; got={status} body={body}",
    );
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    assert_eq!(
        parsed.get("code").and_then(|v| v.as_str()),
        Some("all_providers_exhausted"),
        "exhaustion must surface as `all_providers_exhausted`: {body}",
    );

    // Every cell must have been touched exactly once (rate_limited
    // advances without retry).
    for model in &["a/exhaust-1", "a/exhaust-2", "b/exhaust-1", "b/exhaust-2"] {
        let state = if model.starts_with("a/") {
            &state_a
        } else {
            &state_b
        };
        assert_eq!(
            state.hits_for(model),
            1,
            "every cell must be tried exactly once; {model} had {}",
            state.hits_for(model),
        );
    }

    // R3-B contract: run is now `waiting_approval`, not `running`.
    // Poll up to 2s — the state transition is best-effort after the
    // 502 response so a tiny propagation window is expected.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let mut observed_state = String::new();
    while std::time::Instant::now() < deadline {
        let r = h
            .client()
            .get(format!("{}/v1/runs/{}", h.base_url, run_id))
            .bearer_auth(&h.admin_token)
            .send()
            .await
            .expect("run get reaches server");
        let body: Value = r.json().await.unwrap_or(Value::Null);
        observed_state = body
            .get("run")
            .and_then(|r| r.get("state"))
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .to_owned();
        if observed_state == "waiting_approval" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(
        observed_state, "waiting_approval",
        "#693 R3-B: run must flip to waiting_approval after exhaustion (observed: {observed_state:?})",
    );

    // The escalate_to_operator card must be visible via the approvals
    // endpoint. Filter pending ones on this project and look for the
    // matching run_id.
    let r = h
        .client()
        .get(format!(
            "{}/v1/approvals?tenant_id={tenant}&workspace_id=default_workspace&project_id=default_project&state=pending",
            h.base_url,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("approvals list reaches server");
    let body: Value = r.json().await.unwrap_or(Value::Null);
    let items = body
        .get("items")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let escalate_card = items.iter().find(|it| {
        it.get("run_id").and_then(|v| v.as_str()) == Some(&run_id)
            && it.get("tool_name").and_then(|v| v.as_str()) == Some("escalate_to_operator")
    });
    assert!(
        escalate_card.is_some(),
        "escalate_to_operator card must be present after exhaustion. pending approvals: {items:?}",
    );

    // Silence unused warning on the hit counters — they're captured
    // via `.clone()` above and asserted in-place, but also provide
    // a handy debug surface if the test starts flaking.
    let _ = (Arc::clone(&state_a.hits), Arc::clone(&state_b.hits));
}
