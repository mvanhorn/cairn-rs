//! Per-provider-binding model fallback chain.
//!
//! # Problem
//!
//! Dogfood run 2 (2026-04-23) revealed that the orchestrator gave up after a
//! single provider error, even when several other free models were available
//! on the same OpenRouter connection. Three attempts in a row died:
//!
//!   1. `minimax/minimax-m2.5:free`  → empty completion (17 whitespace tokens)
//!   2. `qwen/qwen3-coder:free`      → HTTP 503 via OpenRouter → "OpenInference"
//!   3. `meta-llama/llama-3.3-70b-instruct:free` → 429 rate-limit (daily cap)
//!
//! A production control plane must route around flaky upstreams instead of
//! returning `502 decide_error` on the first hiccup.
//!
//! # Design
//!
//! [`ModelChain`] is the **per-binding** axis of fallback: an ordered list of
//! model IDs served by a single provider binding, walked in order on
//! fallback-eligible errors. It composes with the **cross-binding** axis
//! handled by
//! [`crate::services::routed_generation::RoutedGenerationService`]:
//! that service iterates bindings and invokes each binding's `ModelChain`;
//! if the chain exhausts, it advances to the next binding. The two axes
//! remain orthogonal even though they are composed at the
//! `RoutedGenerationService` layer.
//!
//! | Error class                               | Action                                 |
//! |-------------------------------------------|----------------------------------------|
//! | Provider-layer (A): `RateLimited`,        | mark cooldown if RL, advance model     |
//! | 5xx, network, `EmptyResponse`,            |                                        |
//! | `StructuredOutputInvalid`, timeouts       |                                        |
//! | Non-retryable (C): `Auth`, `InvalidRequest`| escalate immediately, no fallback     |
//!
//! Model-action errors (B) — the model emitted a bad tool_call / hallucinated
//! tool name — are **handled by the orchestrator**, not here. Those get
//! looped back into the conversation as tool_result corrections; they do
//! not trigger a model switch on the first offence.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cairn_domain::providers::ProviderAdapterError;

/// Default cooldown applied after a model returns `RateLimited`.
///
/// Kept short (5 minutes) because free-tier daily caps reset at midnight UTC
/// and because the cooldown map lives in-process — a restart clears it. The
/// operator can adjust via [`ModelChain::with_rate_limit_cooldown`].
pub const DEFAULT_RATE_LIMIT_COOLDOWN: Duration = Duration::from_secs(5 * 60);

/// Default same-model retry budget on transient errors BEFORE advancing to
/// the next model in the chain. Covers R3 dogfood Finding A
/// (issue #693): free-tier providers routinely return `TimedOut` /
/// `ServerError` on a first attempt that succeeds on a retry 1-3 s
/// later. Without this retry, a single transient hiccup exhausts the
/// chain and escalates to the operator.
///
/// `2` retries = up to 3 attempts per model. Picked to cover the
/// common-case transient bucket (provider request-queue jitter) without
/// meaningfully delaying genuine-exhaustion escalation (worst case:
/// ~4 s of backoff sleep before the chain advances — 1 s before
/// retry 1, 3 s before retry 2).
pub const DEFAULT_MAX_RETRIES_PER_MODEL: u32 = 2;

/// Base backoff between same-model retries. Multiplied by `3^n` per
/// attempt (so 1 s before retry 1 and 3 s before retry 2 for the
/// standard 2-retry budget; 9 s is reached only with a 3rd retry).
/// Bounded exponential so we sleep through the provider's queue-
/// jitter window without building a 30 s+ retry storm on a
/// permanently-broken model.
pub const DEFAULT_RETRY_BASE_BACKOFF: Duration = Duration::from_secs(1);

/// A single failed attempt in a model chain.
#[derive(Debug, Clone)]
pub struct FallbackAttempt {
    pub model_id: String,
    pub reason_code: &'static str,
    pub error_message: String,
}

impl FallbackAttempt {
    pub fn new(model_id: &str, err: &ProviderAdapterError) -> Self {
        Self {
            model_id: model_id.to_owned(),
            reason_code: err.reason_code(),
            error_message: err.to_string(),
        }
    }
}

/// Result of walking the fallback chain.
#[derive(Debug)]
pub enum FallbackOutcome<T> {
    /// One of the models succeeded.
    Success {
        value: T,
        model_id: String,
        /// Position in the chain (0 = preferred model).
        fallback_position: usize,
        attempts: Vec<FallbackAttempt>,
    },
    /// Every model in the chain failed with a fallback-eligible error.
    Exhausted { attempts: Vec<FallbackAttempt> },
    /// A model returned an error that MUST NOT be retried on another model
    /// (bad credentials, malformed request).
    NonRetryable {
        model_id: String,
        err: ProviderAdapterError,
        attempts: Vec<FallbackAttempt>,
    },
}

impl<T> FallbackOutcome<T> {
    pub fn attempt_count(&self) -> usize {
        match self {
            FallbackOutcome::Success { attempts, .. }
            | FallbackOutcome::Exhausted { attempts }
            | FallbackOutcome::NonRetryable { attempts, .. } => attempts.len(),
        }
    }
}

/// In-memory cooldown map: model_id → instant until which it should be
/// skipped. Shared across chains so consecutive runs honour the same
/// rate-limit window.
#[derive(Debug, Default, Clone)]
pub struct CooldownMap {
    inner: Arc<Mutex<HashMap<String, Instant>>>,
}

impl CooldownMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self, model_id: &str, duration: Duration) {
        let until = Instant::now() + duration;
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.insert(model_id.to_owned(), until);
    }

    pub fn is_cooling_down(&self, model_id: &str) -> bool {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        match guard.get(model_id).copied() {
            Some(until) if until > now => true,
            Some(_) => {
                guard.remove(model_id);
                false
            }
            None => false,
        }
    }

    pub fn len(&self) -> usize {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        guard.retain(|_, until| *until > now);
        guard.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Per-binding model fallback chain.
///
/// Walks the ordered `models` list on provider-layer errors, skipping
/// cooled-down models. Non-retryable errors (Auth / InvalidRequest)
/// short-circuit with `NonRetryable`.
#[derive(Debug, Clone)]
pub struct ModelChain {
    models: Vec<String>,
    cooldown: CooldownMap,
    rate_limit_cooldown: Duration,
    /// #693 R3-A: same-model retry budget on transient errors. See
    /// [`DEFAULT_MAX_RETRIES_PER_MODEL`].
    max_retries_per_model: u32,
    /// #693 R3-A: base backoff between retries (multiplied by `3^n`
    /// per attempt). See [`DEFAULT_RETRY_BASE_BACKOFF`].
    retry_base_backoff: Duration,
}

impl ModelChain {
    /// Build a chain from an ordered list of model IDs. Empties and
    /// duplicates are dropped. Preserves first-occurrence order.
    pub fn new(models: impl IntoIterator<Item = String>) -> Self {
        let mut deduped: Vec<String> = Vec::new();
        for candidate in models {
            let candidate = candidate.trim();
            if candidate.is_empty() {
                continue;
            }
            if deduped.iter().any(|m| m == candidate) {
                continue;
            }
            deduped.push(candidate.to_owned());
        }
        Self {
            models: deduped,
            cooldown: CooldownMap::new(),
            rate_limit_cooldown: DEFAULT_RATE_LIMIT_COOLDOWN,
            max_retries_per_model: DEFAULT_MAX_RETRIES_PER_MODEL,
            retry_base_backoff: DEFAULT_RETRY_BASE_BACKOFF,
        }
    }

    /// Convenience: chain with a single model and no fallbacks.
    pub fn single(model_id: impl Into<String>) -> Self {
        Self::new(std::iter::once(model_id.into()))
    }

    pub fn with_rate_limit_cooldown(mut self, duration: Duration) -> Self {
        self.rate_limit_cooldown = duration;
        self
    }

    pub fn with_cooldown(mut self, map: CooldownMap) -> Self {
        self.cooldown = map;
        self
    }

    /// #693 R3-A: override the same-model retry budget. `max_retries`
    /// is additional attempts AFTER the first — so `max_retries=0`
    /// disables retry entirely. `base_backoff` is the sleep before
    /// retry 1; retry N sleeps `base * 3^(N-1)`.
    pub fn with_retry_budget(mut self, max_retries: u32, base_backoff: Duration) -> Self {
        self.max_retries_per_model = max_retries;
        self.retry_base_backoff = base_backoff;
        self
    }

    pub fn models(&self) -> &[String] {
        &self.models
    }

    pub fn preferred(&self) -> Option<&str> {
        self.models.first().map(String::as_str)
    }

    pub fn is_empty(&self) -> bool {
        self.models.is_empty()
    }

    pub fn len(&self) -> usize {
        self.models.len()
    }

    pub fn rate_limit_cooldown(&self) -> Duration {
        self.rate_limit_cooldown
    }

    /// Run `attempt` for each model in the chain in order, skipping models
    /// currently under cooldown. Returns the first success or the full list
    /// of attempts once the chain is exhausted.
    pub async fn run<T, F, Fut>(&self, mut attempt: F) -> FallbackOutcome<T>
    where
        F: FnMut(String) -> Fut,
        Fut: std::future::Future<Output = Result<T, ProviderAdapterError>>,
    {
        let mut attempts: Vec<FallbackAttempt> = Vec::with_capacity(self.models.len());

        for (position, model_id) in self.models.iter().enumerate() {
            if self.cooldown.is_cooling_down(model_id) {
                attempts.push(FallbackAttempt {
                    model_id: model_id.clone(),
                    reason_code: "cooldown_skip",
                    error_message: format!(
                        "skipped — model under local rate-limit cooldown for {:?}",
                        self.rate_limit_cooldown
                    ),
                });
                continue;
            }

            // #693 R3-A: same-model retry loop. `max_attempts = 1 +
            // max_retries_per_model`; retry index 0 is the initial
            // attempt, 1..N are retries with `base_backoff * 3^(N-1)`
            // sleeps between them. RateLimited skips the retry loop
            // entirely — its own cooldown mechanism is correct and
            // retrying the same model inside the cooldown window would
            // defeat the purpose.
            let max_attempts = 1 + self.max_retries_per_model;
            for attempt_index in 0..max_attempts {
                match attempt(model_id.clone()).await {
                    Ok(value) => {
                        return FallbackOutcome::Success {
                            value,
                            model_id: model_id.clone(),
                            fallback_position: position,
                            attempts,
                        };
                    }
                    Err(err) => {
                        let attempt_record = FallbackAttempt::new(model_id, &err);

                        // Non-retryable (Auth / InvalidRequest) →
                        // escalate immediately. Same semantic as
                        // pre-R3-A.
                        if !err.is_fallback_eligible() {
                            attempts.push(attempt_record);
                            return FallbackOutcome::NonRetryable {
                                model_id: model_id.clone(),
                                err,
                                attempts,
                            };
                        }

                        // RateLimited → cooldown + advance to next
                        // model. Do NOT retry the same model inside
                        // its own cooldown window.
                        if matches!(err, ProviderAdapterError::RateLimited) {
                            self.cooldown.record(model_id, self.rate_limit_cooldown);
                            attempts.push(attempt_record);
                            break;
                        }

                        attempts.push(attempt_record);

                        // More retries available? Log before sleeping
                        // so operators see "retry initiated" even if
                        // the process is killed mid-backoff. Field
                        // `model_attempt_index` is chain-scoped (0-based,
                        // resets per-model) and deliberately distinct
                        // from `RoutedGenerationService::generate`'s
                        // global `attempt_index` so correlated log
                        // analysis stays unambiguous.
                        let retries_left = max_attempts - attempt_index - 1;
                        if retries_left > 0 {
                            let backoff = self.retry_backoff_for(attempt_index);
                            tracing::warn!(
                                model_id = %model_id,
                                model_attempt_index = attempt_index,
                                max_attempts = max_attempts,
                                backoff_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX),
                                "model_chain: retrying after transient provider error (#693 R3-A)",
                            );
                            tokio::time::sleep(backoff).await;
                        }
                    }
                }
            }
        }

        FallbackOutcome::Exhausted { attempts }
    }

    /// Compute the backoff before retry `attempt_index` (0-based —
    /// so retry 1 uses `base`, retry 2 uses `base * 3`, retry 3 uses
    /// `base * 9`). Kept as a pure helper for deterministic test
    /// coverage of the schedule.
    fn retry_backoff_for(&self, attempt_index: u32) -> Duration {
        // `3u32.pow(attempt_index)` can overflow if the caller ever
        // configures a very large retry budget; clamp to 9 (= 3^2,
        // the standard 3-attempt schedule). Saturating behaviour
        // means a badly-configured chain worst-case sleeps 9×base
        // between retries instead of panicking.
        let multiplier = 3u32.saturating_pow(attempt_index).min(9);
        self.retry_base_backoff.saturating_mul(multiplier)
    }
}

/// Format an attempt list into a single human-readable paragraph the
/// app layer can surface via `ToolCallApprovalService::submit_proposal`
/// for operator intervention.
pub fn format_attempt_summary(attempts: &[FallbackAttempt]) -> String {
    if attempts.is_empty() {
        return "no models attempted (fallback chain was empty)".to_owned();
    }
    let mut out = String::with_capacity(attempts.len() * 80);
    out.push_str("all providers failed:\n");
    for (i, a) in attempts.iter().enumerate() {
        out.push_str(&format!(
            "  {}. [{}] {} — {}\n",
            i + 1,
            a.reason_code,
            a.model_id,
            a.error_message
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err_rate_limited() -> ProviderAdapterError {
        ProviderAdapterError::RateLimited
    }
    fn err_5xx() -> ProviderAdapterError {
        ProviderAdapterError::ServerError {
            status: 503,
            message: "upstream connect error".to_owned(),
        }
    }
    fn err_empty() -> ProviderAdapterError {
        ProviderAdapterError::EmptyResponse {
            model_id: "m".to_owned(),
            prompt_tokens: Some(500),
            completion_tokens: Some(0),
        }
    }
    fn err_auth() -> ProviderAdapterError {
        ProviderAdapterError::Auth("bad api key".to_owned())
    }
    fn err_invalid() -> ProviderAdapterError {
        ProviderAdapterError::InvalidRequest("unknown field".to_owned())
    }

    #[test]
    fn fallback_eligibility_matrix() {
        assert!(err_rate_limited().is_fallback_eligible());
        assert!(err_5xx().is_fallback_eligible());
        assert!(err_empty().is_fallback_eligible());
        assert!(ProviderAdapterError::StructuredOutputInvalid("x".into()).is_fallback_eligible());
        assert!(!err_auth().is_fallback_eligible());
        assert!(!err_invalid().is_fallback_eligible());
    }

    #[test]
    fn chain_dedupes_and_trims() {
        let chain = ModelChain::new(vec![
            "a".to_owned(),
            "b".to_owned(),
            "a".to_owned(),
            "  ".to_owned(),
            "c".to_owned(),
        ]);
        assert_eq!(chain.models(), &["a", "b", "c"]);
    }

    #[test]
    fn single_model_chain_has_len_one() {
        let chain = ModelChain::single("solo");
        assert_eq!(chain.len(), 1);
        assert_eq!(chain.preferred(), Some("solo"));
    }

    #[test]
    fn empty_chain_is_empty() {
        let empty = ModelChain::new(Vec::<String>::new());
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn chain_succeeds_on_first_model() {
        let chain = ModelChain::new(vec!["m1".to_owned(), "m2".to_owned()]);
        let outcome: FallbackOutcome<String> = chain
            .run(|m| async move { Ok::<_, ProviderAdapterError>(format!("ok-{m}")) })
            .await;
        match outcome {
            FallbackOutcome::Success {
                value,
                model_id,
                fallback_position,
                attempts,
            } => {
                assert_eq!(value, "ok-m1");
                assert_eq!(model_id, "m1");
                assert_eq!(fallback_position, 0);
                assert!(attempts.is_empty());
            }
            other => panic!("expected Success, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn chain_advances_on_rate_limit_then_succeeds() {
        let chain = ModelChain::new(vec!["m1".to_owned(), "m2".to_owned(), "m3".to_owned()]);
        let mut first = true;
        let outcome = chain
            .run(|m| {
                let is_first = first;
                first = false;
                async move {
                    if is_first {
                        Err(err_rate_limited())
                    } else {
                        Ok(format!("ok-{m}"))
                    }
                }
            })
            .await;
        match outcome {
            FallbackOutcome::Success {
                value,
                fallback_position,
                attempts,
                ..
            } => {
                assert_eq!(value, "ok-m2");
                assert_eq!(fallback_position, 1);
                assert_eq!(attempts.len(), 1);
                assert_eq!(attempts[0].reason_code, "rate_limited");
            }
            other => panic!("expected Success, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn chain_advances_on_5xx_then_succeeds() {
        // Disable retries so this test isolates the advance-on-5xx
        // semantic. With retries enabled, m1's second attempt would
        // succeed and the fallback would never fire — verified by the
        // dedicated retry-budget tests below.
        let chain = ModelChain::new(vec!["m1".to_owned(), "m2".to_owned()])
            .with_retry_budget(0, Duration::ZERO);
        let mut first = true;
        let outcome = chain
            .run(|m| {
                let is_first = first;
                first = false;
                async move {
                    if is_first {
                        Err(err_5xx())
                    } else {
                        Ok(format!("ok-{m}"))
                    }
                }
            })
            .await;
        assert!(matches!(outcome, FallbackOutcome::Success { .. }));
    }

    #[tokio::test]
    async fn chain_advances_on_empty_response() {
        // Disable retries so this test isolates the advance-on-empty
        // semantic. #693 R3-A's retry would otherwise consume the
        // `Err(err_empty())` branch multiple times before advancing.
        let chain = ModelChain::new(vec!["m1".to_owned(), "m2".to_owned()])
            .with_retry_budget(0, Duration::ZERO);
        let mut first = true;
        let outcome = chain
            .run(|m| {
                let is_first = first;
                first = false;
                async move {
                    if is_first {
                        Err(err_empty())
                    } else {
                        Ok(m)
                    }
                }
            })
            .await;
        match outcome {
            FallbackOutcome::Success {
                fallback_position,
                attempts,
                ..
            } => {
                assert_eq!(fallback_position, 1);
                assert_eq!(attempts[0].reason_code, "empty_response");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn chain_escalates_on_auth_without_trying_next() {
        let chain = ModelChain::new(vec!["m1".to_owned(), "m2".to_owned()]);
        let mut calls = 0;
        let outcome: FallbackOutcome<()> = chain
            .run(|_m| {
                calls += 1;
                async move { Err(err_auth()) }
            })
            .await;
        assert_eq!(calls, 1, "must not try m2 after auth error");
        match outcome {
            FallbackOutcome::NonRetryable { model_id, .. } => assert_eq!(model_id, "m1"),
            other => panic!("expected NonRetryable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn chain_escalates_on_invalid_request_without_trying_next() {
        let chain = ModelChain::new(vec!["m1".to_owned(), "m2".to_owned()]);
        let mut calls = 0;
        let outcome: FallbackOutcome<()> = chain
            .run(|_m| {
                calls += 1;
                async move { Err(err_invalid()) }
            })
            .await;
        assert_eq!(calls, 1);
        assert!(matches!(outcome, FallbackOutcome::NonRetryable { .. }));
    }

    #[tokio::test]
    async fn chain_exhausts_when_all_fail_retryably() {
        // Disable retries so this test isolates the per-model advance
        // invariant (3 models → 3 attempts). See the dedicated
        // retry-budget tests for the retries-per-model dimension.
        let chain = ModelChain::new(vec!["m1".to_owned(), "m2".to_owned(), "m3".to_owned()])
            .with_retry_budget(0, Duration::ZERO);
        let outcome: FallbackOutcome<()> = chain.run(|_m| async move { Err(err_5xx()) }).await;
        match outcome {
            FallbackOutcome::Exhausted { attempts } => {
                assert_eq!(attempts.len(), 3);
                for a in attempts {
                    assert_eq!(a.reason_code, "upstream_5xx");
                }
            }
            other => panic!("expected Exhausted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn rate_limited_model_skipped_on_next_call() {
        let chain = ModelChain::new(vec!["m1".to_owned(), "m2".to_owned()])
            .with_rate_limit_cooldown(Duration::from_secs(60));

        let mut first = true;
        let _ = chain
            .run(|m| {
                let is_first = first;
                first = false;
                async move {
                    if is_first {
                        Err(err_rate_limited())
                    } else {
                        Ok(m)
                    }
                }
            })
            .await;

        let mut calls: Vec<String> = Vec::new();
        let outcome = chain
            .run(|m| {
                calls.push(m.clone());
                async move { Ok::<_, ProviderAdapterError>(m) }
            })
            .await;
        assert_eq!(calls, vec!["m2".to_owned()], "m1 must be skipped");
        match outcome {
            FallbackOutcome::Success {
                model_id,
                fallback_position,
                attempts,
                ..
            } => {
                assert_eq!(model_id, "m2");
                assert_eq!(fallback_position, 1);
                assert_eq!(attempts.len(), 1);
                assert_eq!(attempts[0].reason_code, "cooldown_skip");
            }
            other => panic!("expected Success, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cooldown_entry_expires() {
        let chain = ModelChain::new(vec!["m1".to_owned(), "m2".to_owned()])
            .with_rate_limit_cooldown(Duration::from_millis(20));
        let _: FallbackOutcome<()> = chain.run(|_m| async move { Err(err_rate_limited()) }).await;
        assert!(chain.cooldown.is_cooling_down("m1"));
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(!chain.cooldown.is_cooling_down("m1"));
    }

    #[test]
    fn format_attempt_summary_lists_each_failure() {
        let attempts = vec![
            FallbackAttempt {
                model_id: "a".to_owned(),
                reason_code: "empty_response",
                error_message: "empty".to_owned(),
            },
            FallbackAttempt {
                model_id: "b".to_owned(),
                reason_code: "upstream_5xx",
                error_message: "503".to_owned(),
            },
        ];
        let s = format_attempt_summary(&attempts);
        assert!(s.contains("empty_response"));
        assert!(s.contains("upstream_5xx"));
        assert!(s.contains("a"));
        assert!(s.contains("b"));
    }

    #[test]
    fn format_attempt_summary_empty_is_graceful() {
        let s = format_attempt_summary(&[]);
        assert!(s.contains("no models"));
    }

    // ── #693 R3-A: same-model retry budget tests ──────────────────────────

    /// Transient 5xx on the first attempt recovers on retry. The
    /// chain must NOT advance to the next model — exhaustion was
    /// never tripped.
    #[tokio::test]
    async fn retry_recovers_after_transient_5xx() {
        // `retry_base_backoff` shrunk to 1 ms so the test stays
        // sub-millisecond on real-time tokio (this crate's test
        // harness doesn't enable tokio-test-util's `start_paused`).
        let chain = ModelChain::new(vec!["m1".to_owned(), "m2".to_owned()])
            .with_retry_budget(2, Duration::from_millis(1));
        // Budget = 2 retries (3 attempts). Fail once, recover.
        let mut calls = 0;
        let outcome = chain
            .run(|m| {
                calls += 1;
                let call = calls;
                async move {
                    if call == 1 {
                        Err(err_5xx())
                    } else {
                        Ok(format!("ok-{m}"))
                    }
                }
            })
            .await;
        assert_eq!(calls, 2, "expected 1 fail + 1 retry success");
        match outcome {
            FallbackOutcome::Success {
                value,
                model_id,
                fallback_position,
                attempts,
            } => {
                assert_eq!(value, "ok-m1");
                assert_eq!(model_id, "m1", "must recover on same model, not advance");
                assert_eq!(fallback_position, 0);
                assert_eq!(attempts.len(), 1, "pre-recover failure recorded");
                assert_eq!(attempts[0].reason_code, "upstream_5xx");
            }
            other => panic!("expected Success via retry, got {other:?}"),
        }
    }

    /// Retry budget exhausted → chain advances to next model. Each
    /// attempt against m1 records a separate `FallbackAttempt`, so
    /// `attempts` carries `max_retries + 1` entries for m1 before
    /// m2 is tried.
    #[tokio::test]
    async fn retry_exhausts_then_advances_to_next_model() {
        let chain = ModelChain::new(vec!["m1".to_owned(), "m2".to_owned()])
            .with_retry_budget(2, Duration::from_millis(1));
        // m1 fails all 3 attempts; m2 succeeds on first.
        let mut calls: Vec<String> = Vec::new();
        let outcome = chain
            .run(|m| {
                calls.push(m.clone());
                async move {
                    if m == "m1" {
                        Err(err_5xx())
                    } else {
                        Ok(m)
                    }
                }
            })
            .await;
        assert_eq!(
            calls,
            vec!["m1", "m1", "m1", "m2"],
            "3 attempts at m1 (1 initial + 2 retries) then advance to m2",
        );
        match outcome {
            FallbackOutcome::Success {
                model_id,
                fallback_position,
                attempts,
                ..
            } => {
                assert_eq!(model_id, "m2");
                assert_eq!(fallback_position, 1);
                assert_eq!(attempts.len(), 3, "all 3 m1 attempts recorded");
                for a in &attempts {
                    assert_eq!(a.model_id, "m1");
                    assert_eq!(a.reason_code, "upstream_5xx");
                }
            }
            other => panic!("expected Success on m2, got {other:?}"),
        }
    }

    /// Non-retryable error (Auth) must NOT retry — escalates
    /// immediately on the first attempt. Protects the pre-R3-A
    /// invariant.
    #[tokio::test]
    async fn non_retryable_short_circuits_without_retry() {
        let chain = ModelChain::new(vec!["m1".to_owned(), "m2".to_owned()]);
        let mut calls = 0;
        let outcome: FallbackOutcome<()> = chain
            .run(|_m| {
                calls += 1;
                async move { Err(err_auth()) }
            })
            .await;
        assert_eq!(calls, 1, "Auth must not retry");
        match outcome {
            FallbackOutcome::NonRetryable { model_id, .. } => assert_eq!(model_id, "m1"),
            other => panic!("expected NonRetryable, got {other:?}"),
        }
    }

    /// RateLimited advances to the next model immediately — do NOT
    /// retry the same model inside its cooldown window. Preserves
    /// the existing cooldown-vs-retry separation.
    #[tokio::test]
    async fn rate_limited_advances_without_same_model_retry() {
        let chain = ModelChain::new(vec!["m1".to_owned(), "m2".to_owned()]);
        let mut calls: Vec<String> = Vec::new();
        let outcome = chain
            .run(|m| {
                calls.push(m.clone());
                async move {
                    if m == "m1" {
                        Err(err_rate_limited())
                    } else {
                        Ok(m)
                    }
                }
            })
            .await;
        assert_eq!(
            calls,
            vec!["m1", "m2"],
            "rate-limited m1 advances to m2 immediately; NO same-model retry inside cooldown",
        );
        assert!(matches!(outcome, FallbackOutcome::Success { .. }));
    }

    /// `with_retry_budget(0, _)` disables retry — exhaustion fires
    /// on the first attempt per model, matching pre-R3-A behaviour.
    /// Used by pre-existing tests that isolate advance semantics.
    #[tokio::test]
    async fn retry_budget_zero_disables_retry() {
        let chain = ModelChain::new(vec!["m1".to_owned(), "m2".to_owned()])
            .with_retry_budget(0, Duration::ZERO);
        let mut calls = 0;
        let outcome: FallbackOutcome<()> = chain
            .run(|_m| {
                calls += 1;
                async move { Err(err_5xx()) }
            })
            .await;
        assert_eq!(calls, 2, "1 attempt per model × 2 models");
        assert!(matches!(outcome, FallbackOutcome::Exhausted { .. }));
    }

    /// Backoff schedule: `base * 3^n` clamped to `base * 9`. Verifies
    /// the pure helper without involving the runtime.
    #[test]
    fn retry_backoff_schedule_is_base_times_three_to_n() {
        let chain =
            ModelChain::new(vec!["m1".to_owned()]).with_retry_budget(5, Duration::from_secs(1));
        assert_eq!(chain.retry_backoff_for(0), Duration::from_secs(1));
        assert_eq!(chain.retry_backoff_for(1), Duration::from_secs(3));
        assert_eq!(chain.retry_backoff_for(2), Duration::from_secs(9));
        // Clamped at 9× so a misconfigured budget never generates a
        // 30-minute sleep.
        assert_eq!(chain.retry_backoff_for(3), Duration::from_secs(9));
        assert_eq!(chain.retry_backoff_for(10), Duration::from_secs(9));
    }
}
