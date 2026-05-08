//! #762 regression: per-binding concurrency cap.
//!
//! `RoutedBinding::concurrency_limit` is a `Semaphore` that bounds
//! how many `provider.generate` calls can be in flight per binding
//! at the same time. This test verifies:
//!
//!   1. With `cap = 1`, two concurrent `generate` calls serialise
//!      (total wall-clock ≈ 2 × per-call latency).
//!   2. With the default cap (4), eight concurrent calls run in two
//!      batches of 4 (total wall-clock ≈ 2 × per-call latency).
//!
//! Pre-#762 the semaphore didn't exist; both scenarios completed
//! in ~1× per-call latency (no backpressure).
//!
//! These tests use synthetic providers (no HTTP) so they're fast
//! and isolate the semaphore-acquisition behavior from any
//! networking concerns.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cairn_domain::providers::{
    GenerationProvider, GenerationResponse, ProviderAdapterError, ProviderBindingSettings,
};
use cairn_runtime::services::model_chain::ModelChain;
use cairn_runtime::services::routed_generation::{
    RoutedBinding, RoutedGenerationService, DEFAULT_BINDING_CONCURRENCY,
};

const PER_CALL_LATENCY: Duration = Duration::from_millis(300);

struct SlowProvider;

#[async_trait]
impl GenerationProvider for SlowProvider {
    async fn generate(
        &self,
        model_id: &str,
        _messages: Vec<serde_json::Value>,
        _settings: &ProviderBindingSettings,
        _tools: &[serde_json::Value],
    ) -> Result<GenerationResponse, ProviderAdapterError> {
        tokio::time::sleep(PER_CALL_LATENCY).await;
        Ok(GenerationResponse {
            text: "ok".into(),
            input_tokens: Some(1),
            output_tokens: Some(1),
            model_id: model_id.to_owned(),
            tool_calls: vec![],
            finish_reason: Some("stop".into()),
        })
    }
}

#[tokio::test]
async fn cap_of_1_serialises_two_concurrent_calls() {
    let provider = Arc::new(SlowProvider);
    let binding = RoutedBinding::new(
        "test-binding",
        provider,
        ModelChain::single("model-x").with_retry_budget(0, Duration::ZERO),
    )
    .with_concurrency(1);
    let svc = Arc::new(RoutedGenerationService::new(vec![binding]));

    let svc1 = svc.clone();
    let svc2 = svc.clone();

    let start = std::time::Instant::now();
    let h1 = tokio::spawn(async move {
        svc1.generate(vec![], &ProviderBindingSettings::default(), &[])
            .await
    });
    let h2 = tokio::spawn(async move {
        svc2.generate(vec![], &ProviderBindingSettings::default(), &[])
            .await
    });
    let _r1 = h1.await.expect("join 1").expect("ok 1");
    let _r2 = h2.await.expect("join 2").expect("ok 2");
    let elapsed = start.elapsed();

    // With cap=1, the two calls serialise. Total ≈ 2 × PER_CALL_LATENCY.
    // Allow generous slack for CI scheduling jitter.
    assert!(
        elapsed >= PER_CALL_LATENCY * 2 - Duration::from_millis(50),
        "cap=1 must serialise: expected >= {:?}, got {elapsed:?}",
        PER_CALL_LATENCY * 2 - Duration::from_millis(50)
    );
    assert!(
        elapsed < PER_CALL_LATENCY * 4,
        "cap=1 should not be excessively slow: expected < {:?}, got {elapsed:?}",
        PER_CALL_LATENCY * 4
    );
}

/// #762/#764 Gemini-review: per-call timeout must wrap BOTH the
/// permit-acquire AND the dispatch. Without that, a starved-permit
/// case queues indefinitely at acquire_owned().await, bypassing
/// the timeout.
#[tokio::test]
async fn permit_starvation_is_bounded_by_per_call_timeout() {
    struct ForeverProvider;
    #[async_trait]
    impl GenerationProvider for ForeverProvider {
        async fn generate(
            &self,
            model_id: &str,
            _messages: Vec<serde_json::Value>,
            _settings: &ProviderBindingSettings,
            _tools: &[serde_json::Value],
        ) -> Result<GenerationResponse, ProviderAdapterError> {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok(GenerationResponse {
                text: "never".into(),
                input_tokens: None,
                output_tokens: None,
                model_id: model_id.to_owned(),
                tool_calls: vec![],
                finish_reason: None,
            })
        }
    }

    let provider = Arc::new(ForeverProvider);
    let binding = RoutedBinding::new(
        "starve",
        provider,
        ModelChain::single("m").with_retry_budget(0, Duration::ZERO),
    )
    .with_concurrency(1);
    let svc = Arc::new(
        RoutedGenerationService::new(vec![binding])
            .with_per_call_timeout(Duration::from_millis(500)),
    );

    let svc1 = svc.clone();
    let svc2 = svc.clone();

    let start = std::time::Instant::now();
    let h1 = tokio::spawn(async move {
        svc1.generate(vec![], &ProviderBindingSettings::default(), &[])
            .await
    });
    // Stagger so call 2 arrives after call 1 has the only permit.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let h2 = tokio::spawn(async move {
        svc2.generate(vec![], &ProviderBindingSettings::default(), &[])
            .await
    });
    let _ = h1.await;
    let _ = h2.await;
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(5),
        "permit-starvation must be bounded by per_call_timeout (500ms each): got {elapsed:?} \
         (Gemini #764 review: timeout must wrap acquire+dispatch, not just dispatch)"
    );
}

#[tokio::test]
async fn default_cap_runs_8_calls_in_two_batches() {
    assert_eq!(DEFAULT_BINDING_CONCURRENCY, 4, "test pinned to default=4");

    let provider = Arc::new(SlowProvider);
    let binding = RoutedBinding::new(
        "test-binding",
        provider,
        ModelChain::single("model-x").with_retry_budget(0, Duration::ZERO),
    );
    // Default cap (4).
    let svc = Arc::new(RoutedGenerationService::new(vec![binding]));

    let start = std::time::Instant::now();
    let mut handles = Vec::new();
    for _ in 0..8 {
        let s = svc.clone();
        handles.push(tokio::spawn(async move {
            s.generate(vec![], &ProviderBindingSettings::default(), &[])
                .await
        }));
    }
    for (i, h) in handles.into_iter().enumerate() {
        let _r = h
            .await
            .unwrap_or_else(|_| panic!("join {i}"))
            .unwrap_or_else(|_| panic!("ok {i}"));
    }
    let elapsed = start.elapsed();

    // 8 calls / cap 4 = 2 batches × 300ms = ~600ms.
    // Pre-fix (no semaphore): all 8 in parallel → ~300ms.
    // Allow slack for CI.
    assert!(
        elapsed >= PER_CALL_LATENCY * 2 - Duration::from_millis(100),
        "default cap=4 must batch 8 into 2 rounds: expected >= ~{:?}, got {elapsed:?}",
        PER_CALL_LATENCY * 2
    );
    assert!(
        elapsed < PER_CALL_LATENCY * 4,
        "default cap=4 should finish 8 in well under 4× latency: got {elapsed:?}"
    );
}
