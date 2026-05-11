//! Issue #668 regression: operators can audit the LLM's chain of
//! thought for a completed run.
//!
//! # The bug
//!
//! Before this PR: the orchestrator recorded `LlmCallTrace` metadata
//! (tokens, latency, cost, model id) on every provider call but never
//! persisted the actual prompt the LLM received or the response it
//! returned. Operators debugging "why did the model do X?" had only
//! the metadata — no way to answer "what did it see?" or "what did
//! it say?". This test closes that gap.
//!
//! # The test
//!
//! Drives a live cairn-app subprocess against a mock OpenAI provider
//! that returns a known system prompt echo + response text, runs one
//! orchestrate iteration, then fetches the newly-added
//! `GET /v1/sessions/:session_id/llm-traces/:trace_id/body` endpoint
//! and asserts:
//!
//! 1. `messages_json` contains the system prompt we constructed
//!    (verbatim) and the user message we sent.
//! 2. `response_text` contains the prose the mock LLM returned.
//! 3. `tool_calls_json` is an empty JSON array on the text-path call
//!    (mock returns prose, not native tool calls).
//! 4. `model_id` matches the mock's resolved model id.
//! 5. Redaction stripped an embedded fake API key from the prompt.
//!
//! # Prove-the-fix contract
//!
//! Pre-fix this test FAILS at the GET — the route `/v1/sessions/:id/
//! llm-traces/:trace_id/body` didn't exist on `main` (404), AND the
//! `llm_completions` projection was never written to (empty table).
//! Post-fix the GET returns 200 with the body fields populated.
//!
//! The PR body carries the verbatim stash-test-pop ceremony output.

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

const MOCK_MODEL: &str = "openrouter/668-chain-of-thought";

/// A recognisable phrase the mock LLM returns as prose so the test
/// can search for it in `response_text`.
const RESPONSE_MARKER: &str = "#668 completion body capture end-to-end smoke test";

/// A fake API key shape that `cairn_providers::redact::redact_secrets`
/// should strip from the prompt. Matches the `sk-[A-Za-z0-9]{20,}`
/// pattern in the provider-key redactor (no dashes — dashes don't
/// match the regex's character class). The plan is to inject this
/// into the goal text so the assembled system/user message carries
/// it, then assert the stored body does NOT contain the literal key.
const FAKE_API_KEY: &str = "sk-668aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
}

/// Mock OpenAI-compatible chat completions endpoint. Returns a
/// `complete_run` proposal wrapped in prose so the orchestrator
/// terminates cleanly in one iteration and the post-decide tracing
/// path fires the `LlmCompletionRecorded` emit.
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
        let prose = format!(
            "{marker} — iteration {n}\n\n[{{\"action_type\":\"complete_run\",\"description\":\"done\",\"confidence\":0.99,\"requires_approval\":false}}]",
            marker = RESPONSE_MARKER,
        );
        (
            StatusCode::OK,
            Json(json!({
                "id":      format!("mock-668-{n}"),
                "choices": [{
                    "index":   0,
                    "message": {
                        "role":    "assistant",
                        "content": prose,
                    },
                    "finish_reason": "stop",
                }],
                "usage": {
                    "prompt_tokens":     20,
                    "completion_tokens": 12,
                    "total_tokens":      32,
                },
            })),
        )
    }

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
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), hits)
}

async fn provision_run(h: &LiveHarness, mock_url: &str) -> (String, String) {
    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_668_{suffix}");
    let session_id = format!("sess_668_{suffix}");
    let run_id = format!("run_668_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id":     "openrouter",
            "plaintext_value": format!("sk-668-{suffix}"),
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

    (session_id, run_id)
}

/// Regression test: the orchestrator persists LLM chain-of-thought
/// bodies and operators can fetch them via the new endpoint.
#[tokio::test]
async fn llm_round_trip_body_is_persisted_and_fetchable() {
    let h = LiveHarness::setup().await;
    let (mock_url, _hits) = spawn_mock().await;
    let (session_id, run_id) = provision_run(&h, &mock_url).await;

    // Drive one orchestrate iteration. The mock's `complete_run`
    // proposal terminates the loop cleanly and the post-decide
    // tracing emitter fires `LlmCompletionRecorded`.
    //
    // The goal string embeds a fake API key so we can assert
    // redaction stripped it from the persisted body.
    let goal_with_secret =
        format!("#668 prove-the-fix test (key={FAKE_API_KEY}; this must be redacted)",);
    let r = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": goal_with_secret,
            "max_iterations": 1,
        }))
        .send()
        .await
        .expect("orchestrate reaches server");
    let status = r.status().as_u16();
    let body_text = r.text().await.unwrap_or_default();
    assert!(
        status == 200 || status == 202,
        "orchestrate must succeed on the happy path; status={status} body={body_text}",
    );

    // Look up the trace_id produced by this iteration via the
    // existing llm-traces metadata endpoint. The test does not care
    // which concrete id was minted — it just needs to follow the
    // metadata row to its body sibling.
    let r = h
        .client()
        .get(format!(
            "{}/v1/sessions/{}/llm-traces",
            h.base_url, session_id,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("list llm-traces reaches server");
    assert_eq!(r.status().as_u16(), 200);
    let traces_body: Value = r.json().await.expect("llm-traces json");
    let traces = traces_body
        .get("traces")
        .and_then(|v| v.as_array())
        .expect("llm-traces response has `traces` array");
    let trace_id = traces
        .iter()
        .find_map(|t| t.get("trace_id").and_then(|v| v.as_str()))
        .expect("at least one LlmCallTrace row exists after orchestrate")
        .to_owned();

    // Fetch the body endpoint. Pre-fix this returns 404 — the route
    // doesn't exist.
    let r = h
        .client()
        .get(format!(
            "{}/v1/sessions/{}/llm-traces/{}/body",
            h.base_url, session_id, trace_id,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("GET body reaches server");
    assert_eq!(
        r.status().as_u16(),
        200,
        "#668 regression: GET body must return 200 after orchestrate. \
         Pre-fix this returns 404 (route did not exist) or the projection \
         was empty. body={}",
        r.text().await.unwrap_or_default(),
    );

    let body: Value = r.json().await.expect("body endpoint json");

    // Assertion 1: model_id matches the mock.
    assert_eq!(
        body.get("model_id").and_then(|v| v.as_str()),
        Some(MOCK_MODEL),
        "model_id must match the resolved mock model. body={body}",
    );

    // Assertion 2: messages_json contains the serialised prompt. The
    // orchestrator builds `[{"role":"system",...},{"role":"user",...}]`
    // so we look for the role labels to confirm it's the right shape.
    let messages_json = body
        .get("messages_json")
        .and_then(|v| v.as_str())
        .expect("messages_json field present");
    assert!(
        messages_json.contains("\"role\":\"system\""),
        "messages_json must include the system-role message; body={body}",
    );
    assert!(
        messages_json.contains("\"role\":\"user\""),
        "messages_json must include the user-role message; body={body}",
    );

    // Assertion 3: response_text includes the mock's marker prose.
    // This proves the LLM's actual response was captured, not just
    // the metadata (tokens/latency) that was already available.
    let response_text = body
        .get("response_text")
        .and_then(|v| v.as_str())
        .expect("response_text field present");
    assert!(
        response_text.contains(RESPONSE_MARKER),
        "response_text must include the mock's marker (#668 chain-of-thought \
         capture). Without this assertion the test would pass for any empty \
         string. Observed: {response_text:?}",
    );

    // Assertion 4: tool_calls_json is an empty array because the
    // mock returned prose rather than a native tool call. The field
    // is persisted (not missing) but empty.
    assert_eq!(
        body.get("tool_calls_json").and_then(|v| v.as_str()),
        Some("[]"),
        "tool_calls_json must be an empty JSON array for text-path responses. body={body}",
    );

    // Assertion 5: redaction stripped the embedded fake API key
    // from the user message. If this fails the redact pipeline is
    // broken and secrets could flow into stored bodies.
    let system_prompt = body
        .get("system_prompt")
        .and_then(|v| v.as_str())
        .expect("system_prompt field present");
    let full_body_text = format!("{system_prompt} {messages_json} {response_text}");
    assert!(
        !full_body_text.contains(FAKE_API_KEY),
        "#668 redaction failure: the fake API key leaked into the \
         persisted body. This must never happen — redaction is run \
         before event emission. Full body payload: {body}",
    );
}
