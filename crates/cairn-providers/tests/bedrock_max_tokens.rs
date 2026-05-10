//! Bedrock Converse `inferenceConfig.maxTokens` wiring.
//!
//! Converse applies a model-specific default (4096 for Claude) when
//! `inferenceConfig` is absent — long reviewer prompts with multi-
//! tool-call responses hit that ceiling and truncate mid-tool-call.
//! These tests lock the three ways to override the ceiling:
//!
//! 1. Default: field + env both unset → request body has no
//!    `inferenceConfig` key (preserves behavior for every existing
//!    caller; Converse's model default still applies on the server).
//! 2. Per-instance builder: [`Bedrock::with_max_tokens`] wins over
//!    the env, so library callers can bypass operator config.
//! 3. Env fallback: `BEDROCK_MAX_TOKENS` is parsed into `maxTokens`
//!    when the field is unset — the "rescue a running binary" path.
//!
//! Uses httpmock's body-matcher closures (same pattern as
//! `bedrock_converse_tools.rs`) so assertions live in the match
//! predicate — a missed expectation shows up as an unmatched mock,
//! distinct from a test-assertion failure.

use std::sync::{Arc, OnceLock};

use cairn_providers::{
    ChatMessage, ChatProvider,
    backends::bedrock::Bedrock,
    completion::{CompletionProvider, CompletionRequest},
    signer::BearerAuth,
};
use httpmock::prelude::*;
use serde_json::{Value, json};
use tokio::sync::Mutex as AsyncMutex;

const MODEL: &str = "us.anthropic.claude-opus-4-7";

/// Serializes env-var tests so concurrent test threads can't poison
/// each other's view of `BEDROCK_MAX_TOKENS`. Each test body acquires
/// this for its entire duration. Uses `tokio::sync::Mutex` so the
/// guard can safely cross the `.await` in `chat_with_tools` without
/// tripping clippy's `await_holding_lock`.
fn env_lock() -> &'static AsyncMutex<()> {
    static LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| AsyncMutex::new(()))
}

fn ok_response() -> Value {
    json!({
        "output": {
            "message": {
                "role": "assistant",
                "content": [{"text": "ok"}]
            }
        },
        "stopReason": "end_turn",
        "usage": {"inputTokens": 5, "outputTokens": 1}
    })
}

fn parse_body(req: &httpmock::HttpMockRequest) -> Option<Value> {
    let raw = req.body().as_ref();
    serde_json::from_slice(raw).ok()
}

#[tokio::test]
async fn default_omits_inference_config() {
    let _lock = env_lock().lock().await;
    let _guard = EnvGuard::unset("BEDROCK_MAX_TOKENS");

    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST)
            .path(format!("/model/{MODEL}/converse"))
            .is_true(|req| match parse_body(req) {
                Some(body) => body.get("inferenceConfig").is_none(),
                None => false,
            });
        then.status(200).json_body(ok_response());
    });

    let signer = Arc::new(BearerAuth::new("test-token"));
    let bedrock = Bedrock::with_signer(MODEL, "us-west-2", signer)
        .expect("provider builds")
        .with_endpoint(server.base_url());

    bedrock
        .chat(&[ChatMessage::user("hi")], None)
        .await
        .expect("chat ok");

    mock.assert();
}

#[tokio::test]
async fn with_max_tokens_sets_inference_config() {
    let _lock = env_lock().lock().await;
    let _guard = EnvGuard::unset("BEDROCK_MAX_TOKENS");

    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST)
            .path(format!("/model/{MODEL}/converse"))
            .is_true(|req| match parse_body(req) {
                Some(body) => body["inferenceConfig"]["maxTokens"] == 16_384,
                None => false,
            });
        then.status(200).json_body(ok_response());
    });

    let signer = Arc::new(BearerAuth::new("test-token"));
    let bedrock = Bedrock::with_signer(MODEL, "us-west-2", signer)
        .expect("provider builds")
        .with_endpoint(server.base_url())
        .with_max_tokens(16_384);

    bedrock
        .chat(&[ChatMessage::user("hi")], None)
        .await
        .expect("chat ok");

    mock.assert();
}

#[tokio::test]
async fn env_var_sets_inference_config_when_field_unset() {
    let _lock = env_lock().lock().await;
    let _guard = EnvGuard::set("BEDROCK_MAX_TOKENS", "24000");

    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST)
            .path(format!("/model/{MODEL}/converse"))
            .is_true(|req| match parse_body(req) {
                Some(body) => body["inferenceConfig"]["maxTokens"] == 24_000,
                None => false,
            });
        then.status(200).json_body(ok_response());
    });

    let signer = Arc::new(BearerAuth::new("test-token"));
    let bedrock = Bedrock::with_signer(MODEL, "us-west-2", signer)
        .expect("provider builds")
        .with_endpoint(server.base_url());

    bedrock
        .chat(&[ChatMessage::user("hi")], None)
        .await
        .expect("chat ok");

    mock.assert();
}

#[tokio::test]
async fn field_wins_over_env() {
    // Operator set env to 8k; library caller builder-set to 32k. The
    // builder wins so a library's explicit policy isn't silently
    // capped by operator config.
    let _lock = env_lock().lock().await;
    let _guard = EnvGuard::set("BEDROCK_MAX_TOKENS", "8000");

    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST)
            .path(format!("/model/{MODEL}/converse"))
            .is_true(|req| match parse_body(req) {
                Some(body) => body["inferenceConfig"]["maxTokens"] == 32_000,
                None => false,
            });
        then.status(200).json_body(ok_response());
    });

    let signer = Arc::new(BearerAuth::new("test-token"));
    let bedrock = Bedrock::with_signer(MODEL, "us-west-2", signer)
        .expect("provider builds")
        .with_endpoint(server.base_url())
        .with_max_tokens(32_000);

    bedrock
        .chat(&[ChatMessage::user("hi")], None)
        .await
        .expect("chat ok");

    mock.assert();
}

#[tokio::test]
async fn completion_path_also_applies_inference_config() {
    // The `complete()` path uses `converse_text_only`, which is a
    // different code path from `chat_with_tools`. Without this, the
    // completion surface silently capped at Converse's 4096 default
    // even when the operator raised `BEDROCK_MAX_TOKENS`. Regression
    // guard: both paths must emit `inferenceConfig.maxTokens`.
    let _lock = env_lock().lock().await;
    let _guard = EnvGuard::unset("BEDROCK_MAX_TOKENS");

    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST)
            .path(format!("/model/{MODEL}/converse"))
            .is_true(|req| match parse_body(req) {
                Some(body) => body["inferenceConfig"]["maxTokens"] == 12_000,
                None => false,
            });
        then.status(200).json_body(ok_response());
    });

    let signer = Arc::new(BearerAuth::new("test-token"));
    let bedrock = Bedrock::with_signer(MODEL, "us-west-2", signer)
        .expect("provider builds")
        .with_endpoint(server.base_url())
        .with_max_tokens(12_000);

    let req = CompletionRequest {
        prompt: "summarize".into(),
        max_tokens: None,
        temperature: None,
    };
    bedrock.complete(&req).await.expect("complete ok");
    mock.assert();
}

#[tokio::test]
async fn invalid_env_is_ignored_silently() {
    // A garbled env var shouldn't break the provider; it should fall
    // back to Converse's default (i.e. omit inferenceConfig). The
    // alternative (panic on parse) would turn a typo into a cairn-app
    // outage.
    let _lock = env_lock().lock().await;
    let _guard = EnvGuard::set("BEDROCK_MAX_TOKENS", "not-a-number");

    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST)
            .path(format!("/model/{MODEL}/converse"))
            .is_true(|req| match parse_body(req) {
                Some(body) => body.get("inferenceConfig").is_none(),
                None => false,
            });
        then.status(200).json_body(ok_response());
    });

    let signer = Arc::new(BearerAuth::new("test-token"));
    let bedrock = Bedrock::with_signer(MODEL, "us-west-2", signer)
        .expect("provider builds")
        .with_endpoint(server.base_url());

    bedrock
        .chat(&[ChatMessage::user("hi")], None)
        .await
        .expect("chat ok");

    mock.assert();
}

// ── Env guard ────────────────────────────────────────────────────────
//
// `std::env::set_var` + `remove_var` are `unsafe` in Rust 2024. The
// guard restores the prior value on drop so concurrent tests in the
// same process don't poison each other. Each test keeps the guard
// live for the duration of the chat() call. cargo runs test-binary
// tests sequentially by default in 2024? No — it still parallelizes;
// we serialize via the standard test-threads=1 invocation or by
// accepting that each test fully guards its own view of the var.
// Since the env read happens once per converse call inside the
// tokio::spawn that owns the runtime, and each test owns its guard
// across the single `.await`, this is safe in practice.

struct EnvGuard {
    key: &'static str,
    prior: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let prior = std::env::var(key).ok();
        // SAFETY: Rust 2024 marked env mutation `unsafe` because it's
        // racy with threads that read env concurrently. The tests in
        // this binary each own an EnvGuard for the duration of their
        // single Bedrock call; no other thread here reads the var.
        unsafe { std::env::set_var(key, value) };
        Self { key, prior }
    }
    fn unset(key: &'static str) -> Self {
        let prior = std::env::var(key).ok();
        // SAFETY: see comment on `set`.
        unsafe { std::env::remove_var(key) };
        Self { key, prior }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prior {
            Some(v) => {
                // SAFETY: see comment on `set`.
                unsafe { std::env::set_var(self.key, v) };
            }
            None => {
                // SAFETY: see comment on `set`.
                unsafe { std::env::remove_var(self.key) };
            }
        }
    }
}
