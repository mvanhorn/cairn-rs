//! AWS Bedrock backend — calls the Converse API.
//!
//! Native cairn backend (not based on OpenAI wire format). Supports any
//! Bedrock model accessible via the Converse API (Anthropic Claude,
//! MiniMax, Meta Llama, Mistral, etc.).
//!
//! Two authentication paths:
//! * **Bearer token** — `BEDROCK_API_KEY` / `AWS_BEARER_TOKEN_BEDROCK`.
//!   Legacy / opt-in. Works against Bedrock's API-key preview.
//! * **AWS SigV4** — default credential chain (env → shared config →
//!   IMDS → container role → SSO). This is the canonical path for
//!   production deployments and the only path that supports IMDS-issued
//!   short-lived credentials on EC2.
//!
//! [`Bedrock::from_env`] picks Bearer when `BEDROCK_API_KEY` /
//! `AWS_BEARER_TOKEN_BEDROCK` is set; otherwise it falls back to SigV4.
//! Callers that want to force one or the other can use
//! [`Bedrock::with_bearer`] or [`Bedrock::with_sigv4`] directly.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::chat::{ChatMessage, ChatProvider, ChatResponse, ChatRole, StructuredOutput, Tool};
use crate::completion::{CompletionProvider, CompletionRequest, CompletionResponse};
use crate::embedding::EmbeddingProvider;
use crate::error::{ProviderError, safe_raw_response};
use crate::models::ModelsProvider;
use crate::redact::redact_secrets;
use crate::signer::{BearerAuth, RequestSigner, SigV4Signer};
use crate::{CairnProvider, ToolCall, Usage};

/// Default HTTP client timeout for Bedrock (seconds).
///
/// Bounded on purpose: a hung upstream can never stall the orchestrator for
/// longer than one call's worth of latency.
pub const DEFAULT_BEDROCK_TIMEOUT_SECS: u64 = 120;

pub struct Bedrock {
    model_id: String,
    region: String,
    signer: Arc<dyn RequestSigner>,
    client: reqwest::Client,
}

impl Bedrock {
    /// Construct a Bedrock client with Bearer-token auth.
    ///
    /// Back-compat shim for operators on the API-key path. New callers
    /// should prefer [`Bedrock::with_sigv4`] on EC2/ECS/EKS where the
    /// instance / task / pod already has an IAM identity — SigV4 avoids
    /// the key rotation burden and works with IMDS-issued short-lived
    /// credentials out of the box.
    ///
    /// Returns a `ProviderError::InvalidRequest` if the underlying
    /// reqwest client cannot be built. Previously returned `Self` and
    /// silently fell back to `Client::default()` on failure — which
    /// discarded the timeout we just set and reintroduced F27. Copilot
    /// review on PR #287 asked for a proper `Result` return so
    /// misconfiguration / TLS init failures surface through the same
    /// error channel as every other provider adapter.
    pub fn new(
        model_id: impl Into<String>,
        region: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Result<Self, ProviderError> {
        Self::with_bearer(model_id, region, api_key)
    }

    /// Alias for [`Bedrock::new`]. Explicit Bearer-token constructor.
    pub fn with_bearer(
        model_id: impl Into<String>,
        region: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Result<Self, ProviderError> {
        let client = build_http_client()?;
        Ok(Self {
            model_id: model_id.into(),
            region: region.into(),
            signer: Arc::new(BearerAuth::new(api_key.into())),
            client,
        })
    }

    /// Construct a Bedrock client that signs every request with AWS
    /// SigV4 using the default credential chain.
    ///
    /// The default chain covers: `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`
    /// (+ optional `AWS_SESSION_TOKEN`) → `~/.aws/credentials` with
    /// optional `AWS_PROFILE` → IMDS (EC2) → ECS / EKS container role →
    /// SSO. Short-lived credentials refresh automatically per request.
    ///
    /// This is an async constructor because the chain needs a tokio
    /// runtime for IMDS probes; build the client once at startup and
    /// reuse it for the lifetime of the process.
    pub async fn with_sigv4(
        model_id: impl Into<String>,
        region: impl Into<String>,
    ) -> Result<Self, ProviderError> {
        let region = region.into();
        let signer = SigV4Signer::from_default_chain(region.clone()).await?;
        let client = build_http_client()?;
        Ok(Self {
            model_id: model_id.into(),
            region,
            signer: Arc::new(signer),
            client,
        })
    }

    /// Construct from an explicit signer. Lets operators inject static
    /// STS credentials, a test signer, or a custom service name
    /// (AgentCore Gateway wants `"bedrock-agentcore"`).
    pub fn with_signer(
        model_id: impl Into<String>,
        region: impl Into<String>,
        signer: Arc<dyn RequestSigner>,
    ) -> Result<Self, ProviderError> {
        let client = build_http_client()?;
        Ok(Self {
            model_id: model_id.into(),
            region: region.into(),
            signer,
            client,
        })
    }

    /// Construct from environment variables — Bearer-only, sync.
    ///
    /// - `BEDROCK_API_KEY` or `AWS_BEARER_TOKEN_BEDROCK` (required).
    /// - `BEDROCK_MODEL_ID` (default: `minimax.minimax-m2.5`)
    /// - `AWS_REGION` (default: `us-west-2`)
    ///
    /// Returns `None` when neither Bearer env var is set. Callers that
    /// want automatic SigV4 fallback (IMDS on EC2, shared config, SSO)
    /// should use [`Bedrock::from_env_async`] instead — SigV4 needs an
    /// async runtime for IMDS probes, so it cannot ride the sync path.
    pub fn from_env() -> Option<Result<Self, ProviderError>> {
        let api_key = std::env::var("BEDROCK_API_KEY")
            .or_else(|_| std::env::var("AWS_BEARER_TOKEN_BEDROCK"))
            .ok()
            .filter(|k| !k.is_empty())?;
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".to_owned());
        let model_id =
            std::env::var("BEDROCK_MODEL_ID").unwrap_or_else(|_| "minimax.minimax-m2.5".to_owned());
        Some(Self::with_bearer(model_id, region, api_key))
    }

    /// Construct from environment variables with SigV4 fallback.
    ///
    /// Same env-var contract as [`Bedrock::from_env`], but when no
    /// Bearer key is set, this falls back to the AWS default credential
    /// chain (env → shared config → IMDS → container role → SSO).
    ///
    /// Returns `None` only when `CAIRN_DISABLE_BEDROCK_ENV_FALLBACK=1`
    /// is set and no Bearer key is configured.
    pub async fn from_env_async() -> Option<Result<Self, ProviderError>> {
        if let Some(bearer) = Self::from_env() {
            return Some(bearer);
        }
        if std::env::var("CAIRN_DISABLE_BEDROCK_ENV_FALLBACK")
            .ok()
            .as_deref()
            == Some("1")
        {
            return None;
        }
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".to_owned());
        let model_id =
            std::env::var("BEDROCK_MODEL_ID").unwrap_or_else(|_| "minimax.minimax-m2.5".to_owned());
        Some(Self::with_sigv4(model_id, region).await)
    }

    pub fn model_id(&self) -> &str {
        &self.model_id
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    /// Authentication scheme in use (`"bearer"` or `"sigv4"`).
    pub fn auth_scheme(&self) -> &'static str {
        self.signer.scheme()
    }

    pub(crate) async fn chat_with_tools_for_model(
        &self,
        model: Option<&str>,
        messages: &[ChatMessage],
        tools: Option<&[Tool]>,
        schema: Option<StructuredOutput>,
    ) -> Result<Box<dyn ChatResponse>, ProviderError> {
        if tools.is_some_and(|tools| !tools.is_empty()) || schema.is_some() {
            return Err(ProviderError::Unsupported(
                "Bedrock chat_with_tools does not support tools or structured output yet"
                    .to_owned(),
            ));
        }

        let model = model
            .filter(|model| !model.trim().is_empty())
            .unwrap_or(&self.model_id);
        let wire_msgs: Vec<Value> = messages
            .iter()
            .map(|m| {
                serde_json::json!({
                    "role": m.role.to_string(),
                    "content": m.content,
                })
            })
            .collect();
        let system = messages
            .iter()
            .find(|m| m.role == ChatRole::System)
            .map(|m| m.content.clone());
        let (text, input_tokens, output_tokens) = self.converse(model, wire_msgs, system).await?;
        let usage = match (input_tokens, output_tokens) {
            (Some(i), Some(o)) => Some(Usage {
                prompt_tokens: i,
                completion_tokens: o,
                total_tokens: i + o,
                cached_tokens: None,
            }),
            _ => None,
        };
        Ok(Box::new(BedrockChatResponse { text, usage }))
    }

    async fn converse(
        &self,
        model: &str,
        messages: Vec<Value>,
        system: Option<String>,
    ) -> Result<(String, Option<u32>, Option<u32>), ProviderError> {
        let url = format!(
            "https://bedrock-runtime.{}.amazonaws.com/model/{}/converse",
            self.region, model
        );
        let bedrock_msgs: Vec<Value> = messages
            .iter()
            .filter(|m| m["role"].as_str() != Some("system"))
            .map(|m| {
                let role = m["role"].as_str().unwrap_or("user");
                let content = m["content"].as_str().unwrap_or("");
                serde_json::json!({
                    "role": role,
                    "content": [{"text": content}]
                })
            })
            .collect();
        let mut body_json = serde_json::json!({ "messages": bedrock_msgs });
        if let Some(sys) = &system {
            body_json["system"] = serde_json::json!([{"text": sys}]);
        }
        // Serialize once so we can both hash the exact bytes in the
        // SigV4 canonical request AND send them on the wire. If we let
        // `reqwest::RequestBuilder::json` re-serialize we'd risk a
        // signature mismatch when aws-sigv4 hashes different bytes than
        // reqwest sends (whitespace, key ordering, etc.).
        let body_bytes: bytes::Bytes = serde_json::to_vec(&body_json)
            .map_err(|e| ProviderError::InvalidRequest(format!("encode converse body: {e}")))?
            .into();
        let req = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            // `Bytes::clone` is a cheap ref-count bump — no payload copy.
            .body(body_bytes.clone());
        let signed = self.signer.sign(req, "POST", &url, &body_bytes).await?;
        let resp = signed
            .send()
            .await
            // Go through `ProviderError::from(reqwest::Error)` so a
            // client-side timeout surfaces as `ProviderError::TimedOut`
            // (and hence `ProviderAdapterError::TimedOut` → fallback-
            // eligible with a `timed_out` reason code) rather than a
            // generic `Http` transport failure. Copilot review on #287.
            .map_err(ProviderError::from)?;
        if !resp.status().is_success() {
            let status = resp.status();
            if status.as_u16() == 429 {
                return Err(ProviderError::RateLimited);
            }
            let body = safe_raw_response(&resp.text().await.unwrap_or_default());
            return Err(ProviderError::Provider(format!("Bedrock {status}: {body}")));
        }
        let resp_body: Value = resp
            .json()
            .await
            .map_err(|e| ProviderError::Http(redact_secrets(&format!("parse: {e}"))))?;
        let text = resp_body["output"]["message"]["content"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|c| c["text"].as_str())
                    .collect::<Vec<_>>()
                    .join("")
            })
            .unwrap_or_default();
        let input_tokens = resp_body["usage"]["inputTokens"].as_u64().map(|n| n as u32);
        let output_tokens = resp_body["usage"]["outputTokens"]
            .as_u64()
            .map(|n| n as u32);
        Ok((text, input_tokens, output_tokens))
    }
}

// ── ChatProvider ─────────────────────────────────────────────────────────────

struct BedrockChatResponse {
    text: String,
    usage: Option<Usage>,
}

impl ChatResponse for BedrockChatResponse {
    fn text(&self) -> Option<String> {
        Some(self.text.clone())
    }
    fn tool_calls(&self) -> Option<Vec<ToolCall>> {
        None
    }
    fn usage(&self) -> Option<Usage> {
        self.usage.clone()
    }
}

impl std::fmt::Debug for BedrockChatResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BedrockChatResponse")
            .field("text", &self.text)
            .finish()
    }
}

impl std::fmt::Display for BedrockChatResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

#[async_trait]
impl ChatProvider for Bedrock {
    async fn chat_with_tools(
        &self,
        messages: &[ChatMessage],
        tools: Option<&[Tool]>,
        schema: Option<StructuredOutput>,
    ) -> Result<Box<dyn ChatResponse>, ProviderError> {
        self.chat_with_tools_for_model(None, messages, tools, schema)
            .await
    }
}

// ── CompletionProvider ───────────────────────────────────────────────────────

#[async_trait]
impl CompletionProvider for Bedrock {
    async fn complete(&self, req: &CompletionRequest) -> Result<CompletionResponse, ProviderError> {
        let messages = vec![serde_json::json!({
            "role": "user",
            "content": req.prompt,
        })];
        let (text, _, _) = self.converse(&self.model_id, messages, None).await?;
        Ok(CompletionResponse { text })
    }
}

// ── EmbeddingProvider ────────────────────────────────────────────────────────

#[async_trait]
impl EmbeddingProvider for Bedrock {
    async fn embed(&self, _input: Vec<String>) -> Result<Vec<Vec<f32>>, ProviderError> {
        Err(ProviderError::Unsupported(
            "Bedrock embedding not yet implemented".into(),
        ))
    }
}

#[async_trait]
impl ModelsProvider for Bedrock {}

impl CairnProvider for Bedrock {}

/// Build the shared reqwest client used by every construction path.
///
/// 120s bounded timeout — never unbounded — so the orchestrator can
/// always escape a hung upstream within one call's worth of latency
/// (F27 dogfood blocker). Applied at the client level *and* per request
/// so the stricter bound wins.
fn build_http_client() -> Result<reqwest::Client, ProviderError> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(DEFAULT_BEDROCK_TIMEOUT_SECS))
        .build()
        .map_err(|err| {
            ProviderError::InvalidRequest(format!(
                "failed to build Bedrock HTTP client with \
                 {DEFAULT_BEDROCK_TIMEOUT_SECS}s timeout: {err}"
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn with_bearer_reports_bearer_scheme() {
        let b = Bedrock::with_bearer("us.anthropic.claude-opus-4-7", "us-west-2", "sk-x").unwrap();
        assert_eq!(b.auth_scheme(), "bearer");
        assert_eq!(b.region(), "us-west-2");
    }

    #[test]
    fn with_signer_accepts_sigv4_static_creds() {
        let signer = Arc::new(SigV4Signer::with_static_credentials(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            None,
            "us-west-2",
        ));
        let b = Bedrock::with_signer("us.anthropic.claude-opus-4-7", "us-west-2", signer).unwrap();
        assert_eq!(b.auth_scheme(), "sigv4");
    }
}
