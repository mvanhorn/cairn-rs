//! Hot-reloadable runtime configuration via DefaultsService.
//!
//! `RuntimeConfig` wraps [`DefaultsService`] and provides typed accessors for
//! model settings and operational knobs.  Every accessor follows this priority
//! chain:
//!
//! 1. **DefaultsService** (store-backed, changeable via
//!    `PUT /v1/settings/defaults/:scope/:scope_id/:key` without a server
//!    restart)
//! 2. **Environment variable** (set at process startup)
//! 3. **Hardcoded default** (compile-time constant)
//!
//! The key names used for DefaultsService are intentionally short
//! (e.g. `"generate_model"`) so they can be set via the API. The scope /
//! scope_id segments identify which layer a default applies to — system,
//! tenant, workspace, or project:
//!
//! ```text
//! PUT /v1/settings/defaults/system/system/generate_model
//! { "value": "llama3.2:3b" }
//!
//! PUT /v1/settings/defaults/tenant/acme/generate_model
//! { "value": "gpt-4o" }
//! ```

use std::sync::Arc;

use cairn_domain::Scope;
use cairn_store::projections::DefaultsReadModel;

// ── Setting keys ──────────────────────────────────────────────────────────────

/// DefaultsService key for the primary generation model (worker/everyday path).
pub const KEY_GENERATE_MODEL: &str = "generate_model";
/// DefaultsService key for the brain model (compute-heavy / reasoning path).
pub const KEY_BRAIN_MODEL: &str = "brain_model";
/// DefaultsService key for the SSE streaming model.
pub const KEY_STREAM_MODEL: &str = "stream_model";
/// DefaultsService key for the embedding model (OpenAI-compat path).
pub const KEY_EMBED_MODEL: &str = "embed_model";
/// DefaultsService key for the embedding model when Ollama is active.
pub const KEY_OLLAMA_EMBED_MODEL: &str = "ollama_embed_model";
/// DefaultsService key for max output tokens.
pub const KEY_MAX_TOKENS: &str = "max_tokens";
/// DefaultsService key for comma-separated thinking-mode model prefixes.
pub const KEY_THINKING_MODEL_PREFIXES: &str = "thinking_model_prefixes";
/// DefaultsService key for the brain inference endpoint URL.
pub const KEY_BRAIN_URL: &str = "brain_url";
/// DefaultsService key for the worker inference endpoint URL.
pub const KEY_WORKER_URL: &str = "worker_url";

// ── F65 PR-3: orchestrator circuit-breaker defaults ─────────────────────────
/// DefaultsService key for the default orchestrator Round-cap breaker.
pub const KEY_ORCHESTRATOR_ROUND_CAP: &str = "orchestrator_round_cap";
/// DefaultsService key for the default orchestrator Tokens-cap breaker.
pub const KEY_ORCHESTRATOR_TOKEN_CAP: &str = "orchestrator_token_cap";
/// DefaultsService key for the default orchestrator NoToolUseStreak-cap breaker.
pub const KEY_ORCHESTRATOR_NO_TOOL_USE_STREAK: &str = "orchestrator_no_tool_use_streak";
/// DefaultsService key for the default orchestrator WallClock-cap breaker (ms).
pub const KEY_ORCHESTRATOR_WALL_CLOCK_MS: &str = "orchestrator_wall_clock_ms";
/// DefaultsService key for the default orchestrator warning-threshold ratio
/// in basis points (10_000 = 100 %). See issue #479.
pub const KEY_ORCHESTRATOR_WARN_RATIO_BPS: &str = "orchestrator_warn_ratio_bps";

/// Hardcoded default orchestrator Round cap — matches `BreakerConfig::default()`.
pub const DEFAULT_ORCHESTRATOR_ROUND_CAP: u32 = 30;
/// Hardcoded default orchestrator Tokens cap — matches `BreakerConfig::default()`.
pub const DEFAULT_ORCHESTRATOR_TOKEN_CAP: u64 = 200_000;
/// Hardcoded default orchestrator NoToolUseStreak cap — matches `BreakerConfig::default()`.
pub const DEFAULT_ORCHESTRATOR_NO_TOOL_USE_STREAK: u32 = 3;
/// Hardcoded default orchestrator WallClock cap (ms) — matches `BreakerConfig::default()`.
pub const DEFAULT_ORCHESTRATOR_WALL_CLOCK_MS: u64 = 15 * 60 * 1_000;
/// Hardcoded default orchestrator warning-threshold ratio, in basis points
/// (10_000 = 100 %). `8_000` = 80 %, matching the historical const in
/// `cairn-orchestrator::breakers` before #479 promoted it to a runtime-tunable.
pub const DEFAULT_ORCHESTRATOR_WARN_RATIO_BPS: u32 = 8_000;

// ── RuntimeConfig ─────────────────────────────────────────────────────────────

/// Hot-reloadable runtime configuration.
///
/// Wraps any type that implements [`DefaultsReadModel`] (e.g. `InMemoryStore`)
/// to read system-scoped settings. The `Arc<dyn …>` is type-erased so
/// `RuntimeConfig` can be stored in `AppState` and `InMemoryServices` without
/// generic parameters.
pub struct RuntimeConfig {
    store: Arc<dyn DefaultsReadModel + Send + Sync>,
}

impl RuntimeConfig {
    /// Create a config backed by the given store.
    ///
    /// Pass `runtime.store.clone()` from `InMemoryServices`.
    pub fn new(store: Arc<dyn DefaultsReadModel + Send + Sync>) -> Self {
        Self { store }
    }

    /// Read a string setting using the three-layer fallback.
    async fn get_string(&self, key: &str, env_var: &str, fallback: &str) -> String {
        // 1. Store (hot-reloadable via DefaultsService::set)
        if let Ok(Some(setting)) = self.store.get(Scope::System, "system", key).await {
            if let Some(s) = setting.value.as_str() {
                return s.to_owned();
            }
        }
        // 2. Environment variable
        if let Ok(s) = std::env::var(env_var) {
            if !s.is_empty() {
                return s;
            }
        }
        // 3. Hardcoded default
        fallback.to_owned()
    }

    /// F65 PR-3: read a `u32` setting using the three-layer fallback.
    ///
    /// Store values may be encoded as JSON numbers or numeric strings —
    /// both are accepted. Any other JSON shape (float, bool, array,
    /// object, null) emits an operator-facing warn log and falls
    /// through to the env var; parse failures on numeric strings fall
    /// through similarly; malformed env vars fall through to the
    /// hardcoded default. Every fall-through path logs so mis-typed
    /// settings are visible rather than silently ignored.
    async fn get_u32(&self, key: &str, env_var: &str, fallback: u32) -> u32 {
        if let Ok(Some(setting)) = self.store.get(Scope::System, "system", key).await {
            if let Some(n) = setting.value.as_u64() {
                if let Ok(v) = u32::try_from(n) {
                    return v;
                }
                tracing::warn!(
                    key,
                    value = n,
                    "RuntimeConfig: store value exceeds u32; falling back to env/default"
                );
            } else if let Some(s) = setting.value.as_str() {
                match s.parse::<u32>() {
                    Ok(v) => return v,
                    Err(e) => tracing::warn!(
                        key,
                        value = s,
                        error = %e,
                        "RuntimeConfig: store value is not a valid u32; falling back to env/default"
                    ),
                }
            } else {
                // Copilot review on #348: emit a warn when the store
                // carries an unexpected JSON shape (float / bool /
                // array / object / null) so a mis-typed setting can't
                // silently fall through to env/default.
                tracing::warn!(
                    key,
                    value = %setting.value,
                    "RuntimeConfig: store value is not a JSON number or numeric string; falling back to env/default"
                );
            }
        }
        if let Ok(s) = std::env::var(env_var) {
            if !s.is_empty() {
                match s.parse::<u32>() {
                    Ok(v) => return v,
                    Err(e) => tracing::warn!(
                        env_var,
                        value = s,
                        error = %e,
                        "RuntimeConfig: env var is not a valid u32; falling back to hardcoded default"
                    ),
                }
            }
        }
        fallback
    }

    /// F65 PR-3: read a `u64` setting using the three-layer fallback.
    /// Same semantics as [`Self::get_u32`] but widened to `u64`.
    async fn get_u64(&self, key: &str, env_var: &str, fallback: u64) -> u64 {
        if let Ok(Some(setting)) = self.store.get(Scope::System, "system", key).await {
            if let Some(n) = setting.value.as_u64() {
                return n;
            }
            if let Some(s) = setting.value.as_str() {
                match s.parse::<u64>() {
                    Ok(v) => return v,
                    Err(e) => tracing::warn!(
                        key,
                        value = s,
                        error = %e,
                        "RuntimeConfig: store value is not a valid u64; falling back to env/default"
                    ),
                }
            } else {
                tracing::warn!(
                    key,
                    value = %setting.value,
                    "RuntimeConfig: store value is not a JSON number or numeric string; falling back to env/default"
                );
            }
        }
        if let Ok(s) = std::env::var(env_var) {
            if !s.is_empty() {
                match s.parse::<u64>() {
                    Ok(v) => return v,
                    Err(e) => tracing::warn!(
                        env_var,
                        value = s,
                        error = %e,
                        "RuntimeConfig: env var is not a valid u64; falling back to hardcoded default"
                    ),
                }
            }
        }
        fallback
    }

    // ── Typed accessors ───────────────────────────────────────────────────────

    /// Default model for generation requests (worker/everyday path, openai-compat).
    ///
    /// Key: `generate_model` · Env: `CAIRN_DEFAULT_GENERATE_MODEL` · Default: empty (user must configure)
    pub async fn default_generate_model(&self) -> String {
        self.get_string(KEY_GENERATE_MODEL, "CAIRN_DEFAULT_GENERATE_MODEL", "")
            .await
    }

    /// Default model for the brain (compute-heavy / reasoning) path.
    ///
    /// Key: `brain_model` · Env: `CAIRN_BRAIN_MODEL` · Default: empty (user must configure)
    ///
    /// Set via `CAIRN_BRAIN_MODEL`, the settings API, or the Providers page in the dashboard.
    pub async fn default_brain_model(&self) -> String {
        self.get_string(KEY_BRAIN_MODEL, "CAIRN_BRAIN_MODEL", "")
            .await
    }

    /// Base URL for the brain inference endpoint.
    ///
    /// Key: `brain_url` · Env: `CAIRN_BRAIN_URL` · Default: empty (user must configure)
    pub async fn brain_url(&self) -> String {
        self.get_string(KEY_BRAIN_URL, "CAIRN_BRAIN_URL", "").await
    }

    /// Base URL for the worker inference endpoint (everyday generation + embeddings).
    ///
    /// Key: `worker_url` · Env: `CAIRN_WORKER_URL` · Default: empty (user must configure)
    pub async fn worker_url(&self) -> String {
        self.get_string(KEY_WORKER_URL, "CAIRN_WORKER_URL", "")
            .await
    }

    /// API key for OpenRouter (https://openrouter.ai).
    ///
    /// Returns `None` when `OPENROUTER_API_KEY` is not set, which is the signal
    /// to skip OpenRouter provider construction at startup.
    ///
    /// Not hot-reloadable — process restart required to pick up a new key.
    pub fn openrouter_api_key() -> Option<String> {
        std::env::var("OPENROUTER_API_KEY")
            .ok()
            .filter(|k| !k.is_empty())
    }

    /// Default model for SSE token-streaming.
    ///
    /// Key: `stream_model` · Env: `CAIRN_DEFAULT_STREAM_MODEL` · Default: empty (user must configure)
    pub async fn default_stream_model(&self) -> String {
        self.get_string(KEY_STREAM_MODEL, "CAIRN_DEFAULT_STREAM_MODEL", "")
            .await
    }

    /// Default embedding model (OpenAI-compat provider path).
    ///
    /// Key: `embed_model` · Env: `CAIRN_DEFAULT_EMBED_MODEL` · Default: empty (user must configure)
    pub async fn default_embed_model(&self) -> String {
        self.get_string(KEY_EMBED_MODEL, "CAIRN_DEFAULT_EMBED_MODEL", "")
            .await
    }

    /// Default embedding model when the Ollama provider is active.
    ///
    /// Key: `ollama_embed_model` · Env: `CAIRN_DEFAULT_OLLAMA_EMBED` · Default: `nomic-embed-text`
    pub async fn default_ollama_embed_model(&self) -> String {
        self.get_string(
            KEY_OLLAMA_EMBED_MODEL,
            "CAIRN_DEFAULT_OLLAMA_EMBED",
            "nomic-embed-text",
        )
        .await
    }

    /// Default max output tokens for generation calls.
    ///
    /// Key: `max_tokens` · Env: `CAIRN_DEFAULT_MAX_TOKENS` · Default: `4096`
    pub async fn default_max_tokens(&self) -> u32 {
        let s = self
            .get_string(KEY_MAX_TOKENS, "CAIRN_DEFAULT_MAX_TOKENS", "4096")
            .await;
        s.parse().unwrap_or(4096)
    }

    /// Comma-separated model-name prefixes that require `think: false` to
    /// suppress chain-of-thought reasoning (e.g. Qwen3 models).
    ///
    /// Key: `thinking_model_prefixes` · Env: `CAIRN_THINKING_MODELS` · Default: `qwen3`
    pub async fn thinking_model_prefixes(&self) -> Vec<String> {
        let s = self
            .get_string(
                KEY_THINKING_MODEL_PREFIXES,
                "CAIRN_THINKING_MODELS",
                "qwen3",
            )
            .await;
        s.split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect()
    }

    /// Return `true` when `model_id` starts with any thinking-mode prefix.
    pub async fn supports_thinking_mode(&self, model_id: &str) -> bool {
        self.thinking_model_prefixes()
            .await
            .iter()
            .any(|prefix| model_id.contains(prefix.as_str()))
    }

    // ── F65 PR-3: orchestrator circuit-breaker defaults ─────────────────────

    /// Default orchestrator Round-cap breaker.
    ///
    /// Key: `orchestrator_round_cap` · Env: `CAIRN_ORCHESTRATOR_ROUND_CAP`
    /// · Default: `30`.
    pub async fn orchestrator_round_cap(&self) -> u32 {
        self.get_u32(
            KEY_ORCHESTRATOR_ROUND_CAP,
            "CAIRN_ORCHESTRATOR_ROUND_CAP",
            DEFAULT_ORCHESTRATOR_ROUND_CAP,
        )
        .await
    }

    /// Default orchestrator Tokens-cap breaker (cumulative input + output).
    ///
    /// Key: `orchestrator_token_cap` · Env: `CAIRN_ORCHESTRATOR_TOKEN_CAP`
    /// · Default: `200_000`.
    pub async fn orchestrator_token_cap(&self) -> u64 {
        self.get_u64(
            KEY_ORCHESTRATOR_TOKEN_CAP,
            "CAIRN_ORCHESTRATOR_TOKEN_CAP",
            DEFAULT_ORCHESTRATOR_TOKEN_CAP,
        )
        .await
    }

    /// Default orchestrator NoToolUseStreak-cap breaker.
    ///
    /// Key: `orchestrator_no_tool_use_streak` · Env:
    /// `CAIRN_ORCHESTRATOR_NO_TOOL_USE_STREAK` · Default: `3`.
    pub async fn orchestrator_no_tool_use_streak(&self) -> u32 {
        self.get_u32(
            KEY_ORCHESTRATOR_NO_TOOL_USE_STREAK,
            "CAIRN_ORCHESTRATOR_NO_TOOL_USE_STREAK",
            DEFAULT_ORCHESTRATOR_NO_TOOL_USE_STREAK,
        )
        .await
    }

    /// Default orchestrator WallClock-cap breaker (milliseconds).
    ///
    /// Key: `orchestrator_wall_clock_ms` · Env: `CAIRN_ORCHESTRATOR_WALL_CLOCK_MS`
    /// · Default: `900_000` (15 minutes).
    pub async fn orchestrator_wall_clock_ms(&self) -> u64 {
        self.get_u64(
            KEY_ORCHESTRATOR_WALL_CLOCK_MS,
            "CAIRN_ORCHESTRATOR_WALL_CLOCK_MS",
            DEFAULT_ORCHESTRATOR_WALL_CLOCK_MS,
        )
        .await
    }

    /// Default orchestrator warning-threshold ratio in basis points
    /// (10_000 = 100 %). `8_000` means the 80 %
    /// `BudgetThresholdCrossed` warning fires when a breaker's measured
    /// value reaches 80 % of its cap. Tunable so operators can run
    /// quieter tracks (e.g. `9_000` for 90 %) or disable the warning
    /// entirely (`10_000` — equal to the trip threshold). Per-run
    /// overrides land via `breaker_overrides.warn_ratio_bps` on the
    /// orchestrate request body.
    ///
    /// Key: `orchestrator_warn_ratio_bps` · Env:
    /// `CAIRN_ORCHESTRATOR_WARN_RATIO_BPS` · Default: `8_000` (80 %).
    pub async fn orchestrator_warn_ratio_bps(&self) -> u32 {
        self.get_u32(
            KEY_ORCHESTRATOR_WARN_RATIO_BPS,
            "CAIRN_ORCHESTRATOR_WARN_RATIO_BPS",
            DEFAULT_ORCHESTRATOR_WARN_RATIO_BPS,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_store::InMemoryStore;

    fn make_config() -> RuntimeConfig {
        let store = Arc::new(InMemoryStore::new());
        RuntimeConfig::new(store)
    }

    /// Without any store value or env var, model defaults are empty (user must configure).
    /// Non-model settings (max_tokens, thinking prefixes, ollama embed) have sensible defaults.
    #[tokio::test]
    async fn hardcoded_fallback_when_no_store_or_env() {
        let cfg = make_config();
        // Provider-agnostic: model defaults are empty — user brings their own.
        assert_eq!(cfg.default_generate_model().await, "");
        assert_eq!(cfg.default_brain_model().await, "");
        assert_eq!(cfg.default_stream_model().await, "");
        assert_eq!(cfg.default_embed_model().await, "");
        assert_eq!(cfg.brain_url().await, "");
        assert_eq!(cfg.worker_url().await, "");
        // Non-model settings have sensible defaults.
        assert_eq!(cfg.default_ollama_embed_model().await, "nomic-embed-text");
        assert_eq!(cfg.default_max_tokens().await, 4096);
        assert_eq!(cfg.thinking_model_prefixes().await, vec!["qwen3"]);
    }

    /// Store-backed value takes precedence over env and hardcoded default.
    #[tokio::test]
    async fn store_value_wins_over_env_and_default() {
        use cairn_domain::{DefaultSettingSet, RuntimeEvent, Scope};
        use cairn_store::EventLog;

        let store = Arc::new(InMemoryStore::new());
        // Write a system-scoped default setting directly to the store.
        store
            .append(&[cairn_domain::EventEnvelope::for_runtime_event(
                cairn_domain::EventId::new("evt_cfg_test"),
                cairn_domain::EventSource::System,
                RuntimeEvent::DefaultSettingSet(DefaultSettingSet {
                    scope: Scope::System,
                    scope_id: "system".to_owned(),
                    key: KEY_GENERATE_MODEL.to_owned(),
                    value: serde_json::json!("llama3.2:3b"),
                }),
            )])
            .await
            .unwrap();

        let cfg = RuntimeConfig::new(store);
        assert_eq!(
            cfg.default_generate_model().await,
            "llama3.2:3b",
            "store value must override hardcoded default"
        );
    }

    /// supports_thinking_mode uses the thinking_model_prefixes list.
    #[tokio::test]
    async fn supports_thinking_mode_matches_prefix() {
        let cfg = make_config();
        assert!(cfg.supports_thinking_mode("qwen3.5:9b").await);
        assert!(cfg.supports_thinking_mode("qwen3:8b").await);
        assert!(!cfg.supports_thinking_mode("llama3.2:3b").await);
        assert!(!cfg.supports_thinking_mode("nomic-embed-text").await);
    }
}
