//! Issue #656 — `brain_model` PUT order-dependency.
//!
//! Dogfood round 3 reported that `PUT
//! /v1/settings/defaults/system/system/brain_model` with `{"value":
//! "glm-4.7"}` returned 422 `unknown_model` when no provider
//! connection advertised the model yet, but returned 200 for the
//! identical body ~30 s later (after the operator registered the
//! connection). Operator setup scripts naturally order "set primary
//! model default" before "register provider connection" — so the very
//! first call in the happy-path setup flow failed.
//!
//! The fix (PR for #656): relax `validate_setting_value` to accept
//! any non-empty, length-capped string for model-id keys. The
//! authoritative "is this model routable right now" check stays at
//! orchestrate time in `handlers/runs/orchestrate.rs`, where the
//! 503 `preferred_model_unavailable` response already names the
//! problem and includes the full connection inventory in one body.
//!
//! This test asserts BOTH halves of the contract:
//!
//! 1. PUT with no provider registered → 200 (forward reference
//!    persists). Then register a matching provider connection,
//!    orchestrate a run, and confirm the orchestrate loop picks up
//!    the model through the provider. Proves the happy path the
//!    dogfood script was trying to run.
//!
//! 2. PUT with no provider registered → 200 (same as above). Then
//!    orchestrate a run WITHOUT registering a provider — assert the
//!    response carries the typed `preferred_model_unavailable`
//!    classifier so the operator sees the real problem at the right
//!    layer.

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

/// Operator-namespaced ID that is NOT in the bundled LiteLLM catalog.
/// This is the key property for the #656 repro — a catalog-unknown ID
/// (private Z.ai model) is exactly what the dogfood script used.
const FORWARD_REF_MODEL: &str = "glm-4.7";
const FINAL_ANSWER: &str = "#656 forward-reference ok.";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
}

async fn spawn_mock() -> (String, Arc<AtomicUsize>) {
    let state = MockState {
        hits: Arc::new(AtomicUsize::new(0)),
    };
    let hits = state.hits.clone();

    async fn chat_handler(
        State(state): State<MockState>,
        Json(_body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        state.hits.fetch_add(1, Ordering::SeqCst);
        (
            StatusCode::OK,
            Json(json!({
                "id": "mock-656",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": null,
                        "tool_calls": [{
                            "id": "call_complete_1",
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
            get(|| async { Json(json!({ "data": [{"id": FORWARD_REF_MODEL}] })) }),
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

async fn put_brain_model(h: &LiveHarness, value: &str) -> reqwest::Response {
    h.client()
        .put(format!(
            "{}/v1/settings/defaults/system/system/brain_model",
            h.base_url,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "value": value }))
        .send()
        .await
        .expect("PUT brain_model reaches server")
}

async fn put_generate_model(h: &LiveHarness, value: &str) -> reqwest::Response {
    h.client()
        .put(format!(
            "{}/v1/settings/defaults/system/system/generate_model",
            h.base_url,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "value": value }))
        .send()
        .await
        .expect("PUT generate_model reaches server")
}

async fn register_provider(h: &LiveHarness, suffix: &str, mock_url: &str) {
    let tenant = "default_tenant";
    let connection_id = format!("conn_656_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": "openrouter",
            "plaintext_value": format!("sk-656-{suffix}"),
        }))
        .send()
        .await
        .expect("credential create reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "credential create: {}",
        r.text().await.unwrap_or_default(),
    );
    let credential_id = r
        .json::<Value>()
        .await
        .expect("credential json")
        .get("id")
        .and_then(|v| v.as_str())
        .expect("credential id")
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
            "supported_models": [FORWARD_REF_MODEL],
            "credential_id": credential_id,
            "endpoint_url": mock_url,
        }))
        .send()
        .await
        .expect("connection create reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "connection create: {}",
        r.text().await.unwrap_or_default(),
    );
}

async fn provision_session_and_run(h: &LiveHarness, suffix: &str) -> (String, String) {
    let tenant = "default_tenant";
    let workspace = "default_workspace";
    let project = "default_project";
    let session_id = format!("sess_656_{suffix}");
    let run_id = format!("run_656_{suffix}");

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
        .expect("session create reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "session create: {}",
        r.text().await.unwrap_or_default(),
    );

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
        .expect("run create reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "run create: {}",
        r.text().await.unwrap_or_default(),
    );

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

/// **#656 primary regression.** The dogfood happy path:
///   1. PUT brain_model=glm-4.7 with NO provider registered → 200.
///   2. Register a provider connection that advertises the model.
///   3. Orchestrate a run → succeeds (model resolves through provider).
#[tokio::test]
async fn brain_model_forward_reference_then_register_provider_orchestrates() {
    let h = LiveHarness::setup().await;
    let suffix = h.project.clone();

    // Step 1. PUT before any provider exists — used to 422.
    let r = put_brain_model(&h, FORWARD_REF_MODEL).await;
    let status = r.status().as_u16();
    let body = r.text().await.unwrap_or_default();
    assert_eq!(
        status, 200,
        "#656 regression: PUT brain_model={FORWARD_REF_MODEL} with no provider \
         registered must 200 (forward reference accepted). status={status} body={body}"
    );

    // generate_model also — the setup script hits both keys.
    let r = put_generate_model(&h, FORWARD_REF_MODEL).await;
    assert_eq!(
        r.status().as_u16(),
        200,
        "PUT generate_model={FORWARD_REF_MODEL} as forward reference must 200: {}",
        r.text().await.unwrap_or_default(),
    );

    // Step 2. Register the provider the operator would register next.
    let (mock_url, hits) = spawn_mock().await;
    register_provider(&h, &suffix, &mock_url).await;

    // Step 3. Orchestrate a run. The system default brain_model now
    // resolves through the just-registered connection.
    let (_, run_id) = provision_session_and_run(&h, &suffix).await;
    let (status, body) = orchestrate(&h, &run_id, "test #656 end-to-end").await;

    // Accept either 200 or 202 — orchestrate's success contract has
    // evolved across F56/F57/F47. The critical property is "not 5xx /
    // not 422 preferred_model_unavailable".
    assert!(
        (200..300).contains(&status),
        "orchestrate after provider registration must succeed; got {status} body={body}"
    );

    // And the mock provider actually got called, proving the
    // resolution path picked up the model through the connection.
    assert!(
        hits.load(Ordering::SeqCst) > 0,
        "mock provider never received a chat-completions request; \
         status={status} body={body}"
    );
}

/// **#656 safety net.** Forward reference accepted at PUT — but
/// orchestrate surfaces the typed error when no provider backs it.
///
/// This is the "you configured a model that nothing routes to" path.
/// We want the error to land at orchestrate time, not PUT time,
/// because orchestrate is the layer where the operator actually
/// observes the consequence. The error body must name the
/// `preferred_model_unavailable` classifier so callers can key
/// automation off it.
#[tokio::test]
async fn brain_model_forward_reference_without_provider_fails_at_orchestrate() {
    let h = LiveHarness::setup().await;
    let suffix = h.project.clone();

    // PUT is accepted.
    let r = put_brain_model(&h, FORWARD_REF_MODEL).await;
    assert_eq!(
        r.status().as_u16(),
        200,
        "PUT brain_model={FORWARD_REF_MODEL} as forward reference must 200: {}",
        r.text().await.unwrap_or_default(),
    );
    let r = put_generate_model(&h, FORWARD_REF_MODEL).await;
    assert_eq!(
        r.status().as_u16(),
        200,
        "PUT generate_model={FORWARD_REF_MODEL} as forward reference must 200: {}",
        r.text().await.unwrap_or_default(),
    );

    // Register a DIFFERENT provider connection so the orchestrate
    // handler has non-empty `summaries` but none advertise
    // FORWARD_REF_MODEL. That is the precise branch that returns
    // 503 `preferred_model_unavailable`; the empty-summaries branch
    // falls through to the startup env fallback and is a distinct
    // code path not covered by #656.
    let tenant = "default_tenant";
    let other_connection_id = format!("conn_other_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": "openrouter",
            "plaintext_value": format!("sk-other-{suffix}"),
        }))
        .send()
        .await
        .expect("credential reaches server");
    assert_eq!(r.status().as_u16(), 201, "credential create");
    let credential_id = r
        .json::<Value>()
        .await
        .expect("credential json")
        .get("id")
        .and_then(|v| v.as_str())
        .expect("credential id")
        .to_owned();

    let r = h
        .client()
        .post(format!("{}/v1/providers/connections", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id": tenant,
            "provider_connection_id": other_connection_id,
            "provider_family": "openrouter",
            "adapter_type": "openrouter",
            // Advertises a DIFFERENT model than the one configured as
            // brain_model / generate_model. This is the exact failure
            // mode the orchestrate-time check exists for.
            "supported_models": ["openrouter/unrelated-model:free"],
            "credential_id": credential_id,
            "endpoint_url": "https://openrouter.ai/api/v1",
        }))
        .send()
        .await
        .expect("connection create reaches server");
    assert_eq!(r.status().as_u16(), 201, "connection create");

    let (_, run_id) = provision_session_and_run(&h, &suffix).await;
    let (status, body) = orchestrate(&h, &run_id, "test #656 no-backing-provider").await;

    // The orchestrate-time check surfaces a typed 503. Exact
    // classifier depends on whether the tenant's connection lookup
    // resolves SOMETHING (even a mismatched model) before the
    // summaries-based verification fires:
    //
    //   * `preferred_model_unavailable` — the summaries path at
    //     `orchestrate.rs:~1174`. Fires when the tenant has one or
    //     more active connections and `resolve_generation_for_model`
    //     returned an adapter (typically via startup-env fallback
    //     when `CAIRN_BRAIN_URL` is set). Body carries the connection
    //     inventory.
    //
    //   * `no_brain_provider` — the earlier path at
    //     `orchestrate.rs:~899`. Fires when
    //     `resolve_generation_for_model` returned None AND no
    //     `state.brain_provider` startup fallback is configured
    //     (fresh CI boot with no env brain URL). Body tells the
    //     operator which env vars or POST to use.
    //
    // Both are acceptable: in BOTH cases the error is typed, 503,
    // and the body names the remediation path. The non-acceptable
    // outcomes this test is guarding against are (a) 200, (b) an
    // untyped 500, (c) an error body that doesn't name the
    // provider-connection remediation. We assert all three
    // negatively and the classifier positively.
    assert_eq!(
        status, 503,
        "orchestrate with no provider advertising brain_model must 503; \
         got {status} body={body}"
    );
    let body_str = body.to_string();
    let is_preferred_model_unavailable = body_str.contains("preferred_model_unavailable");
    let is_no_brain_provider = body_str.contains("no_brain_provider");
    assert!(
        is_preferred_model_unavailable || is_no_brain_provider,
        "orchestrate error body must carry a typed classifier \
         (`preferred_model_unavailable` or `no_brain_provider`) so \
         operators can distinguish this from untyped 503s; body={body_str}"
    );
    // The body must point the operator at the provider-connection
    // remediation either way — that's the point of the typed error.
    assert!(
        body_str.contains("/v1/providers/connections"),
        "orchestrate error body must name the provider-connection \
         remediation endpoint; body={body_str}"
    );
    // If the `preferred_model_unavailable` branch fired, the message
    // also names the model. `no_brain_provider` intentionally does
    // not — it fires before the model identity matters.
    if is_preferred_model_unavailable {
        assert!(
            body_str.contains(FORWARD_REF_MODEL),
            "`preferred_model_unavailable` body must name the \
             configured model; body={body_str}"
        );
    }
}
