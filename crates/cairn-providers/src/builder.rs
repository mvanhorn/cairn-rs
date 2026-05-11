//! Runtime provider construction.
//!
//! [`ProviderBuilder`] constructs any backend from a [`Backend`] enum and
//! runtime configuration.  Operators add providers through the API with
//! their own endpoint URL, API key, and model.

use crate::chat::ChatProvider;
use crate::error::ProviderError;
use crate::wire::openai_compat::{OpenAiCompat, ProviderConfig};
use crate::wire::zai::{ZaiConfig, ZaiProvider};

/// Supported LLM backend families.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Backend {
    OpenAI,
    Anthropic,
    Ollama,
    DeepSeek,
    Xai,
    Google,
    Groq,
    AzureOpenAI,
    OpenRouter,
    MiniMax,
    /// AWS Bedrock Converse API — full-featured (guardrails, documents, tool_config).
    Bedrock,
    /// AWS Bedrock OpenAI-compatible gateway — simpler, standard wire format.
    BedrockCompat,
    /// Any OpenAI-compatible endpoint — operator supplies URL + key.
    OpenAiCompatible,
    /// Native Z.ai adapter (coding plan + general paas).  OpenAI-shaped but
    /// lives in its own wire module to isolate GLM-specific quirks
    /// (thinking mode, `prompt_tokens_details.cached_tokens`, 1305 error
    /// envelope).  See `wire::zai`.
    Zai,
    /// Same as [`Backend::Zai`] but defaulting to the GLM Coding Plan tier.
    ZaiCoding,
}

impl std::str::FromStr for Backend {
    type Err = ProviderError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "openai" => Ok(Self::OpenAI),
            "anthropic" => Ok(Self::Anthropic),
            "ollama" => Ok(Self::Ollama),
            "deepseek" => Ok(Self::DeepSeek),
            "xai" => Ok(Self::Xai),
            "google" | "gemini" => Ok(Self::Google),
            "groq" => Ok(Self::Groq),
            "azure-openai" | "azure_openai" | "azureopenai" => Ok(Self::AzureOpenAI),
            "openrouter" => Ok(Self::OpenRouter),
            "minimax" => Ok(Self::MiniMax),
            "bedrock" | "aws-bedrock" | "bedrock-converse" => Ok(Self::Bedrock),
            "bedrock-compat" | "bedrock-openai" | "bedrock_compat" => Ok(Self::BedrockCompat),
            "openai-compatible" | "openai_compatible" | "generic" | "custom" => {
                Ok(Self::OpenAiCompatible)
            }
            "zai" | "z_ai" | "z-ai" | "z.ai" => Ok(Self::Zai),
            "zai-coding" | "z_ai_coding" | "z-ai-coding" | "zai_coding" | "glm-coding" => {
                Ok(Self::ZaiCoding)
            }
            _ => Err(ProviderError::InvalidRequest(format!(
                "unknown backend: {s}"
            ))),
        }
    }
}

impl std::fmt::Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::OpenAI => "openai",
            Self::Anthropic => "anthropic",
            Self::Ollama => "ollama",
            Self::DeepSeek => "deepseek",
            Self::Xai => "xai",
            Self::Google => "google",
            Self::Groq => "groq",
            Self::AzureOpenAI => "azure-openai",
            Self::OpenRouter => "openrouter",
            Self::MiniMax => "minimax",
            Self::Bedrock => "bedrock",
            Self::BedrockCompat => "bedrock-compat",
            Self::OpenAiCompatible => "openai-compatible",
            Self::Zai => "zai",
            Self::ZaiCoding => "zai-coding",
        })
    }
}

impl Backend {
    /// Get the preset [`ProviderConfig`] for this backend.
    pub fn config(&self) -> ProviderConfig {
        match self {
            Self::OpenAI => ProviderConfig::OPENAI,
            Self::Anthropic => ProviderConfig::ANTHROPIC,
            Self::Ollama => ProviderConfig::OLLAMA,
            Self::DeepSeek => ProviderConfig::DEEPSEEK,
            Self::Xai => ProviderConfig::XAI,
            Self::Google => ProviderConfig::GOOGLE,
            Self::Groq => ProviderConfig::GROQ,
            Self::AzureOpenAI => ProviderConfig::AZURE_OPENAI,
            Self::OpenRouter => ProviderConfig::OPENROUTER,
            Self::MiniMax => ProviderConfig::MINIMAX,
            Self::BedrockCompat => ProviderConfig::BEDROCK_COMPAT,
            // Zai/ZaiCoding have their own wire module; the returned config
            // here is a harmless placeholder since build_chat() short-circuits
            // before using it.
            Self::Bedrock | Self::OpenAiCompatible | Self::Zai | Self::ZaiCoding => {
                ProviderConfig::default()
            }
        }
    }

    /// Whether this backend is known to populate `Usage` (prompt +
    /// completion token counts) on every chat response.
    ///
    /// The orchestrator's token-cap circuit breaker
    /// (`cairn_orchestrator::BreakerState::after_decide`) can only enforce
    /// a budget when each DECIDE round reports usage. When both
    /// `input_tokens` and `output_tokens` are `None` every round, the
    /// breaker under-counts to zero and the operator's configured
    /// `token_cap` is silently bypassed — the run only terminates when a
    /// different breaker (Round, WallClock) trips. For tight budgets that
    /// is indistinguishable from the budget not existing.
    ///
    /// The orchestrate handler uses this to refuse to start a run when
    /// the selected provider does not report usage AND the operator's
    /// `token_cap` is below a sentinel threshold (see
    /// `crates/cairn-app/src/handlers/runs.rs`). The operator can either
    /// raise `token_cap` (explicit opt-in to unbounded spend) or switch
    /// provider — the defensive refusal is the loud, actionable path.
    ///
    /// # Coverage
    ///
    /// All mainstream cloud backends populate the OpenAI-shaped `usage`
    /// block or their native equivalent (`usage.inputTokens` on Bedrock
    /// Converse, `prompt_tokens_details` on Z.ai). Modern Ollama (0.3+)
    /// also populates `usage` on its `/v1/chat/completions` endpoint.
    ///
    /// `OpenAiCompatible` is the single `false` case: it is the
    /// catch-all for operator-supplied URLs where we have no wire
    /// contract — a locally-built provider plugin or a stripped-down
    /// proxy may legitimately omit `usage`, so we cannot assume.
    pub fn reports_usage(&self) -> bool {
        match self {
            Self::OpenAI
            | Self::Anthropic
            | Self::Ollama
            | Self::DeepSeek
            | Self::Xai
            | Self::Google
            | Self::Groq
            | Self::AzureOpenAI
            | Self::OpenRouter
            | Self::MiniMax
            | Self::Bedrock
            | Self::BedrockCompat
            | Self::Zai
            | Self::ZaiCoding => true,
            // Unknown / operator-supplied OpenAI-compatible endpoint.
            // Could be a local plugin or reduced-feature proxy that
            // omits `usage`. Conservative default: treat as no-usage so
            // the orchestrate handler's defensive check kicks in on
            // tight token budgets. Operators with a known-good compat
            // endpoint can raise `token_cap` past the sentinel to
            // bypass.
            Self::OpenAiCompatible => false,
        }
    }
}

/// Builder for constructing any LLM provider at runtime.
pub struct ProviderBuilder {
    backend: Backend,
    api_key: Option<String>,
    base_url: Option<String>,
    model: Option<String>,
    max_tokens: Option<u32>,
    temperature: Option<f32>,
    timeout_secs: Option<u64>,
    region: Option<String>,
}

impl ProviderBuilder {
    pub fn new(backend: Backend) -> Self {
        Self {
            backend,
            api_key: None,
            base_url: None,
            model: None,
            max_tokens: None,
            temperature: None,
            timeout_secs: None,
            region: None,
        }
    }

    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }
    pub fn base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = Some(url.into());
        self
    }
    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }
    pub fn max_tokens(mut self, n: u32) -> Self {
        self.max_tokens = Some(n);
        self
    }
    pub fn temperature(mut self, t: f32) -> Self {
        self.temperature = Some(t);
        self
    }
    pub fn timeout_secs(mut self, s: u64) -> Self {
        self.timeout_secs = Some(s);
        self
    }
    pub fn region(mut self, r: impl Into<String>) -> Self {
        self.region = Some(r.into());
        self
    }

    /// Build a boxed [`ChatProvider`].
    pub fn build_chat(self) -> Result<Box<dyn ChatProvider>, ProviderError> {
        if self.backend == Backend::OpenAiCompatible && self.base_url.is_none() {
            return Err(ProviderError::InvalidRequest(
                "endpoint URL required for generic backend".to_owned(),
            ));
        }
        let key = self.api_key.unwrap_or_default();

        // Z.ai has its own wire module — separate struct, separate file, so
        // GLM quirks can evolve without touching OpenAI/DeepSeek/Groq.
        if matches!(self.backend, Backend::Zai | Backend::ZaiCoding) {
            let config = match self.backend {
                Backend::ZaiCoding => ZaiConfig::CODING,
                Backend::Zai => ZaiConfig::GENERAL,
                _ => unreachable!(),
            };
            return Ok(Box::new(ZaiProvider::new(
                config,
                key,
                self.base_url,
                self.model,
                self.max_tokens,
                self.temperature,
                self.timeout_secs,
            )?));
        }

        // Bedrock has its own wire format.
        if self.backend == Backend::Bedrock {
            let region = self.region.unwrap_or_else(|| "us-west-2".to_owned());
            let model = self
                .model
                .unwrap_or_else(|| "minimax.minimax-m2.5".to_owned());
            return Ok(Box::new(crate::backends::bedrock::Bedrock::new(
                model, region, key,
            )?));
        }

        // Everything else is OpenAI-compatible — one struct, different config.
        let config = self.backend.config();
        Ok(Box::new(OpenAiCompat::new(
            config,
            key,
            self.base_url,
            self.model,
            self.max_tokens,
            self.temperature,
            self.timeout_secs,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::Backend;

    /// Every `Backend` variant with a known wire contract (i.e. every
    /// typed backend except `OpenAiCompatible`) must report `true`, so
    /// the orchestrate handler's defensive check does not spuriously
    /// refuse runs on mainstream providers. Adding a new `Backend`
    /// variant without updating `reports_usage()` will fall through
    /// the match and fail to compile; this test double-guards the
    /// coverage at the semantic level. Keeping the enumeration
    /// explicit (rather than deriving a count) avoids a brittle
    /// off-by-one whenever a new variant lands.
    #[test]
    fn mainstream_backends_report_usage() {
        for backend in [
            Backend::OpenAI,
            Backend::Anthropic,
            Backend::Ollama,
            Backend::DeepSeek,
            Backend::Xai,
            Backend::Google,
            Backend::Groq,
            Backend::AzureOpenAI,
            Backend::OpenRouter,
            Backend::MiniMax,
            Backend::Bedrock,
            Backend::BedrockCompat,
            Backend::Zai,
            Backend::ZaiCoding,
        ] {
            assert!(
                backend.reports_usage(),
                "mainstream backend {backend} must report usage — \
                 the orchestrator's token-cap breaker relies on it",
            );
        }
    }

    /// The generic OpenAI-compat slot is operator-supplied and has no
    /// wire contract we can enforce. Left as `false` so the
    /// orchestrate handler refuses to start a run on a tight
    /// `token_cap` until the operator raises the cap or switches to a
    /// typed backend. See `crates/cairn-app/src/handlers/runs.rs` for
    /// the refusal site.
    #[test]
    fn openai_compatible_does_not_report_usage_by_default() {
        assert!(!Backend::OpenAiCompatible.reports_usage());
    }
}
