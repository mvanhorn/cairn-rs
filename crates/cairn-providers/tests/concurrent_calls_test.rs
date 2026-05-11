//! #762 reproducer: 2 concurrent calls to the same `OpenAiCompat`
//! instance against a mock server. If the bug is in cairn's HTTP path,
//! this test will hang or fail. If the bug is Z.ai-specific, this
//! test will pass and we look elsewhere.

use std::sync::Arc;
use std::time::Duration;

use cairn_providers::{
    ChatMessage, ChatProvider,
    wire::openai_compat::{OpenAiCompat, ProviderConfig},
};
use httpmock::prelude::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_concurrent_calls_complete_within_30s() {
    let server = MockServer::start();

    // Mock: every request returns a valid OpenAI-compat response
    // after 200ms (simulates Z.ai latency).
    let _mock = server.mock(|when, then| {
        when.method(POST).path("/chat/completions");
        then.status(200)
            .delay(Duration::from_millis(200))
            .header("content-type", "application/json")
            .body(
                r#"{
                "id": "test-123",
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": "ok"},
                    "finish_reason": "stop"
                }],
                "usage": {"prompt_tokens": 5, "completion_tokens": 1, "total_tokens": 6}
            }"#,
            );
    });

    // Build ONE OpenAiCompat shared across both concurrent calls
    // (mirrors cairn-app's `Arc<dyn GenerationProvider>` pattern).
    let provider = Arc::new(
        OpenAiCompat::new(
            ProviderConfig::default(),
            "test-key",
            Some(server.base_url()),
            Some("test-model".to_owned()),
            None,
            None,
            Some(10), // 10s timeout — plenty for a 200ms-latency mock
        )
        .expect("OpenAiCompat::new"),
    );

    let messages = vec![ChatMessage::user("hello")];

    // Fire 2 concurrent calls. Both should complete within ~1s.
    let p1 = provider.clone();
    let m1 = messages.clone();
    let p2 = provider.clone();
    let m2 = messages.clone();

    let start = std::time::Instant::now();

    let h1 = tokio::spawn(async move { p1.chat_with_tools(&m1, None, None).await });
    let h2 = tokio::spawn(async move { p2.chat_with_tools(&m2, None, None).await });

    let r1 = tokio::time::timeout(Duration::from_secs(30), h1)
        .await
        .expect("call 1 must complete within 30s")
        .expect("call 1 join")
        .expect("call 1 result");
    let r2 = tokio::time::timeout(Duration::from_secs(30), h2)
        .await
        .expect("call 2 must complete within 30s")
        .expect("call 2 join")
        .expect("call 2 result");

    let elapsed = start.elapsed();

    assert_eq!(r1.text(), Some("ok".to_owned()), "call 1 content");
    assert_eq!(r2.text(), Some("ok".to_owned()), "call 2 content");
    assert!(
        elapsed < Duration::from_secs(5),
        "2 concurrent calls (each 200ms latency) should complete in well under 5s; got {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_concurrent_calls_with_large_prompt_complete_within_30s() {
    // R17 dogfood evidence: smoke probe (tiny prompt) worked at N=1. The 8-issue
    // dispatch (large prompt, ~5kchar with full issue body) wedged at N=2.
    // This test reproduces the size+concurrency combo against a mock.
    let server = MockServer::start();
    let _mock = server.mock(|when, then| {
        when.method(POST).path("/chat/completions");
        then.status(200)
            .delay(Duration::from_millis(500))
            .header("content-type", "application/json")
            .body(
                r#"{
                "id": "test-123",
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": "ok"},
                    "finish_reason": "stop"
                }],
                "usage": {"prompt_tokens": 5000, "completion_tokens": 1, "total_tokens": 5001}
            }"#,
            );
    });

    let provider = Arc::new(
        OpenAiCompat::new(
            ProviderConfig::default(),
            "test-key",
            Some(server.base_url()),
            Some("test-model".to_owned()),
            None,
            None,
            Some(30),
        )
        .expect("OpenAiCompat::new"),
    );

    // ~5kchar message body, mimicking a large GitHub issue prompt.
    let big_content = "x".repeat(5000);
    let messages = vec![ChatMessage::user(big_content)];

    let p1 = provider.clone();
    let m1 = messages.clone();
    let p2 = provider.clone();
    let m2 = messages.clone();

    let start = std::time::Instant::now();
    let h1 = tokio::spawn(async move { p1.chat_with_tools(&m1, None, None).await });
    let h2 = tokio::spawn(async move { p2.chat_with_tools(&m2, None, None).await });

    let r1 = tokio::time::timeout(Duration::from_secs(30), h1)
        .await
        .expect("call 1 must complete within 30s")
        .expect("join 1")
        .expect("result 1");
    let r2 = tokio::time::timeout(Duration::from_secs(30), h2)
        .await
        .expect("call 2 must complete within 30s")
        .expect("join 2")
        .expect("result 2");
    let elapsed = start.elapsed();

    assert_eq!(r1.text(), Some("ok".to_owned()));
    assert_eq!(r2.text(), Some("ok".to_owned()));
    assert!(
        elapsed < Duration::from_secs(5),
        "2 concurrent calls with 5kchar prompts should complete well under 5s; got {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn eight_concurrent_calls_complete_within_30s() {
    let server = MockServer::start();

    let _mock = server.mock(|when, then| {
        when.method(POST).path("/chat/completions");
        then.status(200)
            .delay(Duration::from_millis(200))
            .header("content-type", "application/json")
            .body(
                r#"{
                "id": "test-123",
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": "ok"},
                    "finish_reason": "stop"
                }],
                "usage": {"prompt_tokens": 5, "completion_tokens": 1, "total_tokens": 6}
            }"#,
            );
    });

    let provider = Arc::new(
        OpenAiCompat::new(
            ProviderConfig::default(),
            "test-key",
            Some(server.base_url()),
            Some("test-model".to_owned()),
            None,
            None,
            Some(10),
        )
        .expect("OpenAiCompat::new"),
    );

    let messages = vec![ChatMessage::user("hello")];

    let start = std::time::Instant::now();
    let mut handles = Vec::new();
    for _ in 0..8 {
        let p = provider.clone();
        let m = messages.clone();
        handles.push(tokio::spawn(async move {
            p.chat_with_tools(&m, None, None).await
        }));
    }
    for (i, h) in handles.into_iter().enumerate() {
        let r = tokio::time::timeout(Duration::from_secs(30), h)
            .await
            .unwrap_or_else(|_| panic!("call {i} did not complete within 30s"))
            .expect("join")
            .expect("result");
        assert_eq!(r.text(), Some("ok".to_owned()), "call {i} content");
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(10),
        "8 concurrent calls (each 200ms latency) should complete in well under 10s; got {elapsed:?}"
    );
}
