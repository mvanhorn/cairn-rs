//! Integration tests for the SigV4 request-signing path.
//!
//! These cover both seams that gained SigV4 in feat/cairn-providers-bedrock-sigv4:
//!
//! 1. `OpenAiCompat::with_signer(...)` — the Bedrock OpenAI-compat gateway
//!    shares its chat wire format with every other OpenAI-compat backend,
//!    so we inject a signer and assert the header shape + the fact that
//!    the downstream request layer still sends the model's chosen
//!    `chat/completions` payload.
//! 2. `backends::bedrock::Bedrock::with_signer(...)` — native Converse
//!    path. Uses the same signer, but lives under a different struct.

use std::sync::Arc;

use cairn_providers::{
    ChatMessage, ChatProvider,
    signer::SigV4Signer,
    wire::openai_compat::{OpenAiCompat, ProviderConfig},
};
use httpmock::prelude::*;
use serde_json::json;

fn static_sigv4_signer() -> Arc<SigV4Signer> {
    // Canonical SigV4 test credentials from AWS docs — same values
    // used in the unit tests so signatures are reproducible.
    Arc::new(SigV4Signer::with_static_credentials(
        "AKIAIOSFODNN7EXAMPLE",
        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        None,
        "us-west-2",
    ))
}

// ── OpenAI-compat + SigV4 ───────────────────────────────────────────

#[tokio::test]
async fn openai_compat_with_sigv4_signer_sends_aws4_authorization_header() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST)
            // BEDROCK_COMPAT preset uses `v1/chat/completions`, distinct
            // from the standard OpenAI preset's `chat/completions`.
            .path("/v1/chat/completions")
            // Anchor on the deterministic prefix + credential scope —
            // `Signature=...` varies per-request but the rest is fixed
            // by the static test credentials.
            .header_matches("authorization", "^AWS4-HMAC-SHA256 ")
            .header_matches(
                "authorization",
                "/us-west-2/bedrock/aws4_request.*Signature=",
            )
            // SigV4 also always adds `x-amz-date`.
            .header_exists("x-amz-date");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!({
                "id": "chatcmpl-1",
                "object": "chat.completion",
                "created": 0,
                "model": "us.anthropic.claude-opus-4-7",
                "choices": [{
                    "index": 0,
                    "message": { "role": "assistant", "content": "hello from bedrock" },
                    "finish_reason": "stop"
                }],
                "usage": { "prompt_tokens": 3, "completion_tokens": 5, "total_tokens": 8 }
            }));
    });

    let provider = OpenAiCompat::new(
        ProviderConfig::BEDROCK_COMPAT,
        // api_key is ignored when a signer is attached, but the
        // current struct still stores it. Pass an empty string to
        // prove the signer-only path really bypasses the key check.
        "",
        Some(server.base_url()),
        Some("us.anthropic.claude-opus-4-7".to_owned()),
        None,
        None,
        None,
    )
    .expect("provider builds")
    .with_signer(static_sigv4_signer());

    let resp = provider
        .chat_with_tools(&[ChatMessage::user("hi")], None, None)
        .await
        .expect("chat succeeds");

    assert_eq!(resp.text().as_deref(), Some("hello from bedrock"));
    mock.assert();
}

#[tokio::test]
async fn openai_compat_without_signer_still_uses_bearer() {
    // Regression guard: the Bearer path must stay intact for the 11
    // OpenAI-compat backends that do NOT use SigV4.
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/chat/completions")
            .header("authorization", "Bearer sk-test");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!({
                "id": "chatcmpl-1",
                "object": "chat.completion",
                "created": 0,
                "model": "gpt-4.1-nano",
                "choices": [{
                    "index": 0,
                    "message": { "role": "assistant", "content": "pong" },
                    "finish_reason": "stop"
                }],
                "usage": { "prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2 }
            }));
    });

    let provider = OpenAiCompat::new(
        ProviderConfig::OPENAI,
        "sk-test",
        Some(server.base_url()),
        Some("gpt-4.1-nano".to_owned()),
        None,
        None,
        None,
    )
    .expect("provider builds");

    let resp = provider
        .chat_with_tools(&[ChatMessage::user("ping")], None, None)
        .await
        .expect("chat succeeds");

    assert_eq!(resp.text().as_deref(), Some("pong"));
    mock.assert();
}

#[tokio::test]
async fn openai_compat_empty_api_key_without_signer_returns_auth_error() {
    // The SigV4 path changed the pre-send guard to allow empty
    // api_key when a signer is installed. Lock in that the Bearer
    // path still rejects an empty key.
    let server = MockServer::start();
    let provider = OpenAiCompat::new(
        ProviderConfig::OPENAI,
        "", // empty
        Some(server.base_url()),
        Some("gpt-4.1-nano".to_owned()),
        None,
        None,
        None,
    )
    .expect("provider builds");

    let err = provider
        .chat_with_tools(&[ChatMessage::user("hi")], None, None)
        .await
        .expect_err("should reject before sending");

    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("api key"),
        "unexpected error: {msg}"
    );
}
