//! Integration tests for Bedrock Converse tool-call support.
//!
//! Verifies the full tool-call round-trip:
//! 1. Tools + messages translate into the correct `toolConfig` +
//!    content-block shape on the wire.
//! 2. A `toolUse` response deserializes into `ChatResponse::tool_calls()`.
//! 3. Multi-turn with a `toolResult` message serializes correctly.
//!
//! Uses httpmock to capture what cairn sends, so we don't depend on a
//! live Bedrock account to assert wire shape.

use std::sync::Arc;

use cairn_providers::{
    ChatMessage, ChatProvider,
    backends::bedrock::Bedrock,
    chat::{FunctionDef, Tool},
    signer::BearerAuth,
};
use httpmock::prelude::*;
use serde_json::{Value, json};

fn provider(server: &MockServer, model: &str) -> Bedrock {
    // Bearer signer with a fixed token so the test doesn't need AWS
    // credentials. BedRock only sees the mock server, so auth is
    // irrelevant to the mock's matchers.
    let signer = Arc::new(BearerAuth::new("test-token"));
    Bedrock::with_signer(model, "us-west-2", signer)
        .expect("provider builds")
        .with_endpoint(server.base_url())
}

fn add_tool() -> Tool {
    Tool {
        tool_type: "function".to_owned(),
        function: FunctionDef {
            name: "add_numbers".to_owned(),
            description: "add two integers".to_owned(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "a": {"type": "integer"},
                    "b": {"type": "integer"}
                },
                "required": ["a", "b"]
            }),
        },
    }
}

// ── Request wire shape ───────────────────────────────────────────────

#[tokio::test]
async fn converse_request_includes_tool_config_and_text_message() {
    let server = MockServer::start();
    // Match on body shape — we care that the exact Converse JSON
    // gets sent, not just that a request happens.
    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/model/us.anthropic.claude-opus-4-7/converse")
            .is_true(|req| {
                let body_str = String::from_utf8_lossy(req.body().as_ref());
                let body: Value = match serde_json::from_str(&body_str) {
                    Ok(v) => v,
                    Err(_) => return false,
                };
                // Message text
                body["messages"][0]["role"] == "user"
                    && body["messages"][0]["content"][0]["text"] == "what is 3+4?"
                    // toolConfig shape
                    && body["toolConfig"]["tools"][0]["toolSpec"]["name"] == "add_numbers"
                    && body["toolConfig"]["tools"][0]["toolSpec"]["inputSchema"]["json"]["type"]
                        == "object"
                    && body["toolConfig"]["tools"][0]["toolSpec"]["inputSchema"]["json"]
                        ["required"][0] == "a"
            });
        then.status(200).json_body(json!({
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [{
                        "toolUse": {
                            "toolUseId": "tu_1",
                            "name": "add_numbers",
                            "input": {"a": 3, "b": 4}
                        }
                    }]
                }
            },
            "stopReason": "tool_use",
            "usage": {"inputTokens": 100, "outputTokens": 20, "totalTokens": 120}
        }));
    });

    let resp = provider(&server, "us.anthropic.claude-opus-4-7")
        .chat_with_tools(
            &[ChatMessage::user("what is 3+4?")],
            Some(&[add_tool()]),
            None,
        )
        .await
        .expect("chat succeeds");

    // Tool call parsed correctly.
    let calls = resp.tool_calls().expect("tool_calls");
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].function.name, "add_numbers");
    assert_eq!(calls[0].id, "tu_1");
    let args: Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
    assert_eq!(args["a"], 3);
    assert_eq!(args["b"], 4);
    assert_eq!(resp.finish_reason().as_deref(), Some("tool_use"));
    mock.assert();
}

#[tokio::test]
async fn converse_request_omits_tool_config_when_no_tools() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/model/us.anthropic.claude-opus-4-7/converse")
            .is_true(|req| {
                let body_str = String::from_utf8_lossy(req.body().as_ref());
                let body: Value = match serde_json::from_str(&body_str) {
                    Ok(v) => v,
                    Err(_) => return false,
                };
                // No toolConfig when tools = None.
                body.get("toolConfig").is_none()
            });
        then.status(200).json_body(json!({
            "output": {"message": {"role": "assistant", "content": [{"text": "ok"}]}},
            "stopReason": "end_turn",
            "usage": {"inputTokens": 5, "outputTokens": 1}
        }));
    });

    let resp = provider(&server, "us.anthropic.claude-opus-4-7")
        .chat_with_tools(&[ChatMessage::user("hi")], None, None)
        .await
        .expect("chat succeeds");
    assert_eq!(resp.text().as_deref(), Some("ok"));
    mock.assert();
}

#[tokio::test]
async fn converse_request_routes_system_message_to_system_field() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST).is_true(|req| {
            let body_str = String::from_utf8_lossy(req.body().as_ref());
            let body: Value = match serde_json::from_str(&body_str) {
                Ok(v) => v,
                Err(_) => return false,
            };
            // System lives in `system[]`, not `messages[]`.
            body["system"][0]["text"] == "you are terse"
                && body["messages"][0]["role"] == "user"
                && body["messages"][0]["content"][0]["text"] == "hi"
        });
        then.status(200).json_body(json!({
            "output": {"message": {"role": "assistant", "content": [{"text": "hi."}]}},
            "stopReason": "end_turn",
            "usage": {"inputTokens": 5, "outputTokens": 1}
        }));
    });

    let _ = provider(&server, "us.anthropic.claude-opus-4-7")
        .chat_with_tools(
            &[
                ChatMessage::system("you are terse"),
                ChatMessage::user("hi"),
            ],
            None,
            None,
        )
        .await
        .expect("chat succeeds");
    mock.assert();
}

#[tokio::test]
async fn converse_multi_turn_with_tool_result_serializes_correctly() {
    // Reproduces the sequence the orchestrator would run:
    //   1. user: question
    //   2. assistant: toolUse
    //   3. user: toolResult (sent as Tool role from cairn's side,
    //       translated to user on the wire)
    //   4. assistant: final text
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST).is_true(|req| {
            let body_str = String::from_utf8_lossy(req.body().as_ref());
            let body: Value = match serde_json::from_str(&body_str) {
                Ok(v) => v,
                Err(_) => return false,
            };
            body["messages"][0]["role"] == "user"
                && body["messages"][1]["role"] == "assistant"
                && body["messages"][1]["content"][0]["toolUse"]["toolUseId"] == "tu_1"
                && body["messages"][1]["content"][0]["toolUse"]["input"]["a"] == 3
                // Tool result arrives on the user side (Converse convention).
                && body["messages"][2]["role"] == "user"
                && body["messages"][2]["content"][0]["toolResult"]["toolUseId"] == "tu_1"
                && body["messages"][2]["content"][0]["toolResult"]["content"][0]["text"] == "7"
        });
        then.status(200).json_body(json!({
            "output": {"message": {"role": "assistant", "content": [{"text": "3 + 4 = 7."}]}},
            "stopReason": "end_turn",
            "usage": {"inputTokens": 50, "outputTokens": 10}
        }));
    });

    use cairn_providers::{FunctionCall, ToolCall, chat::MessageContent};
    let assistant_tool_use = ChatMessage {
        role: cairn_providers::ChatRole::Assistant,
        content_type: MessageContent::ToolUse(vec![ToolCall {
            id: "tu_1".to_owned(),
            call_type: "function".to_owned(),
            function: FunctionCall {
                name: "add_numbers".to_owned(),
                arguments: r#"{"a":3,"b":4}"#.to_owned(),
            },
        }]),
        content: String::new(),
    };
    let tool_result =
        ChatMessage::tool_result("tu_1".to_owned(), "add_numbers".to_owned(), "7".to_owned());

    let resp = provider(&server, "us.anthropic.claude-opus-4-7")
        .chat_with_tools(
            &[
                ChatMessage::user("what is 3+4?"),
                assistant_tool_use,
                tool_result,
            ],
            Some(&[add_tool()]),
            None,
        )
        .await
        .expect("chat succeeds");
    assert_eq!(resp.text().as_deref(), Some("3 + 4 = 7."));
    mock.assert();
}

// ── Error paths ──────────────────────────────────────────────────────

#[tokio::test]
async fn converse_http_error_surfaces_as_provider_error() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(POST);
        then.status(500)
            .json_body(json!({"message": "backend exploded"}));
    });

    let err = provider(&server, "us.anthropic.claude-opus-4-7")
        .chat_with_tools(&[ChatMessage::user("x")], None, None)
        .await
        .expect_err("should fail");
    let msg = err.to_string();
    assert!(
        msg.contains("Bedrock") && msg.contains("500"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn converse_429_surfaces_as_rate_limited() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(POST);
        then.status(429).json_body(json!({"message": "slow down"}));
    });

    let err = provider(&server, "us.anthropic.claude-opus-4-7")
        .chat_with_tools(&[ChatMessage::user("x")], None, None)
        .await
        .expect_err("should fail");
    assert!(matches!(err, cairn_providers::ProviderError::RateLimited));
}
