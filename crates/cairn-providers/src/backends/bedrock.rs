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

use crate::chat::{
    ChatMessage, ChatProvider, ChatResponse, ChatRole, MessageContent, StructuredOutput, Tool,
    ToolChoice,
};
use crate::completion::{CompletionProvider, CompletionRequest, CompletionResponse};
use crate::embedding::EmbeddingProvider;
use crate::error::{ProviderError, safe_raw_response};
use crate::models::ModelsProvider;
use crate::redact::redact_secrets;
use crate::signer::{BearerAuth, RequestSigner, SigV4Signer};
use crate::{CairnProvider, FunctionCall, ToolCall, Usage};

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
    /// Override for the default `https://bedrock-runtime.{region}.amazonaws.com`
    /// endpoint. Intended for tests only — points the converse calls at
    /// an httpmock server without changing production behaviour.
    endpoint_override: Option<String>,
    /// Per-request completion-token ceiling included in Converse's
    /// `inferenceConfig.maxTokens`. When `None` *and* the
    /// `BEDROCK_MAX_TOKENS` env var is unset, Converse applies its
    /// model-specific default (4096 for Claude). Tests set this
    /// directly via [`Bedrock::with_max_tokens`]; production operators
    /// override via the env var, which is read once per request in
    /// `chat_with_tools_for_model`.
    max_tokens: Option<u32>,
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
            endpoint_override: None,
            max_tokens: None,
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
            endpoint_override: None,
            max_tokens: None,
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
            endpoint_override: None,
            max_tokens: None,
        })
    }

    /// Override the API endpoint. Intended for tests only — redirects
    /// converse calls to an httpmock server. Production callers should
    /// not touch this.
    #[doc(hidden)]
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint_override = Some(endpoint.into());
        self
    }

    /// Set the per-request completion-token ceiling. The value is
    /// forwarded to Converse as `inferenceConfig.maxTokens`. When
    /// unset and no `BEDROCK_MAX_TOKENS` env var is present, Converse
    /// applies its model-specific default (4096 for Claude on tool-
    /// calling paths) — long-response workloads (large reviewer
    /// prompts, multi-tool-call turns) routinely hit that ceiling,
    /// truncating the completion mid-tool-call.
    pub fn with_max_tokens(mut self, max_tokens: u32) -> Self {
        self.max_tokens = Some(max_tokens);
        self
    }

    /// Resolve the effective max-tokens cap. Field wins over env.
    /// Whitespace-only / unparseable env values are treated as unset
    /// (silent ignore) so a typo can't turn a running binary into an
    /// outage.
    ///
    /// Why `std::env::var` on every call and not a `OnceLock` cache:
    /// Bedrock requests are the slow path (hundreds of milliseconds
    /// on Converse). A single env lookup is ~100 ns — vanishing
    /// against the request cost. The integration tests (and future
    /// on-the-fly operator config reloads) also need the env to be
    /// re-read, which a `OnceLock` cache would permanently freeze to
    /// whatever the first caller observed.
    fn resolve_max_tokens(&self) -> Option<u32> {
        self.max_tokens.or_else(|| {
            std::env::var("BEDROCK_MAX_TOKENS")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .and_then(|s| s.trim().parse::<u32>().ok())
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
        // Structured-output / JSON-schema enforcement still needs
        // Converse-specific wiring. Drop the hard guard so plain tool
        // calls go through, but reject `schema` until we add
        // `additionalModelRequestFields` mapping for Claude's
        // `response_format` shim.
        if schema.is_some() {
            return Err(ProviderError::Unsupported(
                "Bedrock structured output not yet implemented".to_owned(),
            ));
        }

        let model = model
            .filter(|model| !model.trim().is_empty())
            .unwrap_or(&self.model_id);

        // Route system messages into Converse's dedicated `system` field,
        // and everything else into `messages[]` with content-block
        // translation. Only the *first* system message is preserved,
        // matching Converse's single-shot system-prompt contract; any
        // subsequent system messages are appended so long prompts split
        // across several entries still work.
        let system_blocks: Vec<Value> = messages
            .iter()
            .filter(|m| m.role == ChatRole::System)
            .map(|m| serde_json::json!({ "text": m.content }))
            .collect();

        let bedrock_msgs: Vec<Value> = messages
            .iter()
            .filter(|m| m.role != ChatRole::System)
            .map(chat_message_to_converse)
            .collect::<Result<Vec<_>, _>>()?;

        let mut body_json = serde_json::json!({ "messages": bedrock_msgs });
        if !system_blocks.is_empty() {
            body_json["system"] = Value::Array(system_blocks);
        }
        // Capture the sanitized→original tool-name map so the response
        // parser can restore cairn's original ids on any `toolUse`
        // blocks. Empty when `tools` is absent/empty; lookups return
        // `None` and fall through to the raw name unchanged.
        let tool_name_map = if let Some(tools) = tools.filter(|t| !t.is_empty()) {
            let (cfg, rename) = build_tool_config(tools, None)?;
            body_json["toolConfig"] = cfg;
            rename
        } else {
            std::collections::HashMap::new()
        };

        // Converse defaults `maxTokens` to 4096 for Claude when
        // `inferenceConfig` is absent — long reviews with many tool
        // calls hit that ceiling and truncate. Per-instance
        // `Bedrock::with_max_tokens` wins; `BEDROCK_MAX_TOKENS` env var
        // is the operator-facing fallback so existing binaries can be
        // rescued without a rebuild. When neither is set, omit the
        // block and let Converse apply its model default (preserves
        // behavior for every existing caller). Applied in both the
        // tool-calling and completion paths via `resolve_max_tokens`.
        if let Some(n) = self.resolve_max_tokens() {
            body_json["inferenceConfig"] = serde_json::json!({ "maxTokens": n });
        }

        self.converse_raw(model, body_json, &tool_name_map).await
    }

    async fn converse_raw(
        &self,
        model: &str,
        body_json: Value,
        tool_name_map: &std::collections::HashMap<String, String>,
    ) -> Result<Box<dyn ChatResponse>, ProviderError> {
        let url = match self.endpoint_override.as_deref() {
            Some(base) => format!("{}/model/{}/converse", base.trim_end_matches('/'), model),
            None => format!(
                "https://bedrock-runtime.{}.amazonaws.com/model/{}/converse",
                self.region, model
            ),
        };
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
        parse_converse_response(resp_body, tool_name_map)
    }

    /// Thin wrapper for the completion path. Plain text-only converse
    /// without tools or schema — preserves the legacy sync contract.
    async fn converse_text_only(
        &self,
        model: &str,
        messages: Vec<Value>,
        system: Option<String>,
    ) -> Result<String, ProviderError> {
        let mut body_json = serde_json::json!({ "messages": messages });
        if let Some(sys) = system {
            body_json["system"] = serde_json::json!([{ "text": sys }]);
        }
        // Same ceiling policy as the tool-calling path; without this
        // the completion surface (used by every non-chat prompt) is
        // silently capped at Converse's default even when the caller
        // or operator raised `max_tokens`.
        if let Some(n) = self.resolve_max_tokens() {
            body_json["inferenceConfig"] = serde_json::json!({ "maxTokens": n });
        }
        // No tools on the completion path → empty rename map.
        let empty = std::collections::HashMap::new();
        let resp = self.converse_raw(model, body_json, &empty).await?;
        Ok(resp.text().unwrap_or_default())
    }
}

// ── Request mapping: cairn types → Converse wire ─────────────────────

/// Translate one cairn `ChatMessage` into a Converse message object.
///
/// Rules:
/// * `System` is filtered upstream (routes to the request-level `system`
///   field instead of `messages`).
/// * `User` / `Assistant` text becomes `content: [{text}]`.
/// * `Assistant` with `MessageContent::ToolUse(calls)` becomes one
///   `toolUse` block per call, each carrying the JSON-parsed arguments.
/// * `Tool` role with `MessageContent::ToolResult(calls)` becomes a
///   single `user` message (Converse requires tool results to arrive
///   from the user side) holding one `toolResult` block per call; the
///   call's `arguments` string is surfaced as the tool-result body.
fn chat_message_to_converse(m: &ChatMessage) -> Result<Value, ProviderError> {
    match (&m.role, &m.content_type) {
        (ChatRole::System, _) => Err(ProviderError::InvalidRequest(
            "system messages are routed to the Converse `system` field; \
             chat_message_to_converse should not receive them"
                .to_owned(),
        )),
        (ChatRole::Assistant, MessageContent::ToolUse(calls)) => {
            let mut content = Vec::with_capacity(calls.len() + usize::from(!m.content.is_empty()));
            if !m.content.is_empty() {
                content.push(serde_json::json!({ "text": m.content }));
            }
            for call in calls {
                // Bedrock's Converse API requires `toolUse.input` to
                // be a JSON *object*. Accept only if parsing yields an
                // object; everything else (invalid JSON, a string, a
                // number, an array) falls back to the `_raw` wrapper
                // so one ill-formed turn can't poison the whole run.
                let input: Value = serde_json::from_str(&call.function.arguments)
                    .ok()
                    .filter(Value::is_object)
                    .unwrap_or_else(|| serde_json::json!({ "_raw": call.function.arguments }));
                content.push(serde_json::json!({
                    "toolUse": {
                        "toolUseId": call.id,
                        // Sanitize here for the same reason we sanitize
                        // `toolSpec.name` in `build_tool_config`:
                        // Bedrock requires every `toolUse.name` in the
                        // message history to match `[a-zA-Z0-9_-]+` and
                        // to be consistent with the ids in `toolConfig`.
                        // The cairn `ToolCall` always carries the
                        // original id (the response parser reverse-
                        // mapped it on the prior turn), so we sanitize
                        // on the way back out. Round-trip stays clean
                        // because the model receives the sanitized
                        // name, emits it back, and the parser rewrites
                        // it to the original — the id on the
                        // orchestrator side never changes.
                        "name": sanitize_bedrock_tool_name(&call.function.name),
                        "input": input,
                    }
                }));
            }
            Ok(serde_json::json!({ "role": "assistant", "content": content }))
        }
        (ChatRole::Tool, MessageContent::ToolResult(calls)) => {
            // Converse carries tool results on a `user` turn. Each
            // call gets its own `toolResult` block; the arguments
            // string on the `ToolCall` is the tool's raw output.
            let content: Vec<Value> = calls
                .iter()
                .map(|call| {
                    serde_json::json!({
                        "toolResult": {
                            "toolUseId": call.id,
                            "content": [{"text": call.function.arguments}],
                        }
                    })
                })
                .collect();
            Ok(serde_json::json!({ "role": "user", "content": content }))
        }
        (role, _) => {
            // Plain text path for everything else (`User` / `Assistant`
            // without tool content). `Tool` role without the expected
            // `ToolResult` content is silently downgraded to a user-
            // text block — Converse has no other legal representation
            // for it, and refusing the whole request here would
            // regress callers that pre-flatten tool output into text.
            let wire_role = match role {
                ChatRole::Assistant => "assistant",
                _ => "user",
            };
            Ok(serde_json::json!({
                "role": wire_role,
                "content": [{"text": m.content}],
            }))
        }
    }
}

/// Sanitize a cairn tool name into Bedrock's `toolSpec.name` constraint
/// (`[a-zA-Z0-9_-]+`). Any character outside that set — most commonly the
/// `.` that cairn uses as a namespace separator (`github_api.review_pr`,
/// `memory.search`, etc.) — collapses to `_`. The reverse mapping is
/// stored in the caller's `HashMap<String, String>` so an inbound
/// `toolUse.name` from Bedrock can be rewritten back to cairn's
/// original id.
fn sanitize_bedrock_tool_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Build the Converse `toolConfig` value from a cairn tool slice.
///
/// Maps 1:1 onto Bedrock's schema:
/// `toolConfig.tools[].toolSpec = {name, description, inputSchema.json}`.
///
/// Bedrock's Converse API enforces `toolSpec.name` ∈ `[a-zA-Z0-9_-]+`.
/// Cairn tool ids routinely include `.` (e.g. `github_api.review_pr`),
/// which fails that regex — observed on the dogfood as:
///   `Bedrock 400: Value 'github_api.review_pr' at
///   'toolConfig.tools.6.member.toolSpec.name' failed to satisfy constraint`.
/// We sanitize on the way out via `sanitize_bedrock_tool_name` and hand
/// back a `HashMap<sanitized → original>` so the caller can reverse-map
/// the name on any inbound `toolUse` block before passing it back to
/// the orchestrator. Collisions after sanitization are a hard error —
/// silently dropping one tool would make the agent think it's available
/// but route every invocation to its namesake.
///
/// `ToolChoice` mapping:
/// * `Auto` → `{auto: {}}` (also the Converse default; we omit the field
///   to stay compatible with models that reject explicit `auto`).
/// * `Any` → `{any: {}}`
/// * `Specific` → `{tool: {name}}` (sanitized on the way out).
/// * `None` → no `toolConfig` at all (we're called only when `tools` is
///   non-empty; suppressing `toolConfig` lets the model answer in plain
///   text as the caller asked).
fn build_tool_config(
    tools: &[Tool],
    tool_choice: Option<&ToolChoice>,
) -> Result<(Value, std::collections::HashMap<String, String>), ProviderError> {
    let mut rename: std::collections::HashMap<String, String> =
        std::collections::HashMap::with_capacity(tools.len());
    let specs: Vec<Value> = tools
        .iter()
        .map(|t| {
            if t.function.name.trim().is_empty() {
                return Err(ProviderError::InvalidRequest(
                    "Bedrock toolSpec.name must not be empty".to_owned(),
                ));
            }
            let original = &t.function.name;
            let safe = sanitize_bedrock_tool_name(original);
            if let Some(prev) = rename.insert(safe.clone(), original.clone())
                && prev != *original
            {
                return Err(ProviderError::InvalidRequest(format!(
                    "Bedrock tool-name sanitization collision: '{}' and '{}' \
                     both map to '{}'; rename one to avoid ambiguity",
                    prev, original, safe
                )));
            }
            let mut tool_spec = serde_json::json!({
                "name": safe,
                "description": t.function.description,
                "inputSchema": { "json": t.function.parameters.clone() },
            });
            // Converse accepts an empty description; dropping an empty
            // string is cheaper on the wire than sending it. No-op when
            // the caller already omitted it.
            if t.function.description.is_empty() {
                tool_spec.as_object_mut().unwrap().remove("description");
            }
            Ok(serde_json::json!({ "toolSpec": tool_spec }))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut cfg = serde_json::json!({ "tools": specs });
    match tool_choice {
        None | Some(ToolChoice::Auto) => {}
        Some(ToolChoice::Any) => {
            cfg["toolChoice"] = serde_json::json!({ "any": {} });
        }
        Some(ToolChoice::Specific(name)) => {
            cfg["toolChoice"] =
                serde_json::json!({ "tool": { "name": sanitize_bedrock_tool_name(name) } });
        }
        Some(ToolChoice::None) => {
            // Caller asked for no tool use even though tools were
            // supplied. Converse has no "tools available but forbid
            // invocation" knob, so we drop toolConfig entirely — the
            // model will answer with text. This matches how the
            // OpenAI-compat backend treats `tool_choice: "none"`.
            return Ok((Value::Null, rename));
        }
    }
    Ok((cfg, rename))
}

// ── Response mapping: Converse wire → cairn types ────────────────────

/// Parse a Converse response into a cairn [`ChatResponse`].
///
/// Content blocks are split into:
/// * `{text}` blocks → concatenated into the response's `text()`.
/// * `{toolUse}` blocks → appended to `tool_calls()` with the input
///   object re-encoded as a JSON string (cairn's `FunctionCall::arguments`
///   is a `String`, not a `Value`, for OpenAI-wire parity).
///
/// `rename` is the outbound sanitization map from
/// [`build_tool_config`]; we reverse it here so the cairn orchestrator
/// sees the original tool id (e.g. `github_api.review_pr`), not the
/// Bedrock-sanitized name (e.g. `github_api_review_pr`). Unknown names
/// pass through unchanged — the model can (and rarely does) mint a
/// name we didn't register, and the downstream `ToolCall` handler will
/// reject it.
///
/// `stopReason` surfaces as `finish_reason()`. `usage.inputTokens` /
/// `usage.outputTokens` / `usage.cacheReadInputTokens` become `Usage`.
fn parse_converse_response(
    mut resp: Value,
    rename: &std::collections::HashMap<String, String>,
) -> Result<Box<dyn ChatResponse>, ProviderError> {
    // Move the content array out of the response rather than cloning
    // — a long tool call can carry several KB of input JSON and the
    // caller doesn't need the original array afterwards.
    let content: Vec<Value> = match resp.pointer_mut("/output/message/content") {
        Some(Value::Array(arr)) => std::mem::take(arr),
        _ => Vec::new(),
    };

    let mut text = String::new();
    let mut tool_calls: Vec<ToolCall> = Vec::new();
    for mut block in content {
        if let Some(t) = block.get("text").and_then(Value::as_str) {
            text.push_str(t);
            continue;
        }
        // Take ownership of the toolUse sub-object so we can move
        // `input` out of it without cloning. `Value::take` leaves
        // `Null` in place and hands us the original tree.
        let tu = match block.pointer_mut("/toolUse") {
            Some(v) => v.take(),
            None => continue,
        };
        let Value::Object(mut tu_obj) = tu else {
            // `toolUse` present but not an object — the upstream is
            // malformed. Surface as a Provider error so the operator
            // can see the breakage instead of silently dropping it.
            return Err(ProviderError::Provider(
                "Bedrock toolUse block is not an object".to_owned(),
            ));
        };
        let id = tu_obj
            .get("toolUseId")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ProviderError::Provider("Bedrock toolUse block missing toolUseId".to_owned())
            })?
            .to_owned();
        let name_raw = tu_obj.get("name").and_then(Value::as_str).ok_or_else(|| {
            ProviderError::Provider("Bedrock toolUse block missing name".to_owned())
        })?;
        // Reverse the outbound sanitization so the orchestrator sees
        // the original cairn tool id, not the Bedrock-sanitized name.
        let name = rename
            .get(name_raw)
            .cloned()
            .unwrap_or_else(|| name_raw.to_owned());
        // `input` is required per the Converse spec. Treat its
        // absence as a protocol error rather than silently minting
        // `null` arguments — downstream orchestrators use this to
        // drive the next turn and a silent `null` would corrupt it.
        let input_val = tu_obj.remove("input").ok_or_else(|| {
            ProviderError::Provider("Bedrock toolUse block missing input".to_owned())
        })?;
        let arguments = serde_json::to_string(&input_val)
            .map_err(|e| ProviderError::Provider(format!("encode toolUse.input: {e}")))?;
        tool_calls.push(ToolCall {
            id,
            call_type: "function".to_owned(),
            function: FunctionCall { name, arguments },
        });
    }

    let stop_reason = resp
        .get("stopReason")
        .and_then(Value::as_str)
        .map(String::from);

    let input_tokens = resp["usage"]["inputTokens"].as_u64().map(|n| n as u32);
    let output_tokens = resp["usage"]["outputTokens"].as_u64().map(|n| n as u32);
    let cache_read = resp["usage"]["cacheReadInputTokens"]
        .as_u64()
        .map(|n| n as u32)
        .filter(|n| *n > 0);
    let usage = match (input_tokens, output_tokens) {
        (Some(i), Some(o)) => Some(Usage {
            prompt_tokens: i,
            completion_tokens: o,
            total_tokens: i + o,
            cached_tokens: cache_read,
        }),
        _ => None,
    };

    Ok(Box::new(BedrockChatResponse {
        text,
        tool_calls: if tool_calls.is_empty() {
            None
        } else {
            Some(tool_calls)
        },
        usage,
        finish_reason: stop_reason,
    }))
}

// ── ChatProvider ─────────────────────────────────────────────────────────────

struct BedrockChatResponse {
    text: String,
    tool_calls: Option<Vec<ToolCall>>,
    usage: Option<Usage>,
    finish_reason: Option<String>,
}

impl ChatResponse for BedrockChatResponse {
    fn text(&self) -> Option<String> {
        if self.text.is_empty() {
            None
        } else {
            Some(self.text.clone())
        }
    }
    fn tool_calls(&self) -> Option<Vec<ToolCall>> {
        self.tool_calls.clone()
    }
    fn usage(&self) -> Option<Usage> {
        self.usage.clone()
    }
    fn finish_reason(&self) -> Option<String> {
        self.finish_reason.clone()
    }
}

impl std::fmt::Debug for BedrockChatResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BedrockChatResponse")
            .field("text", &self.text)
            .field("tool_calls", &self.tool_calls)
            .field("finish_reason", &self.finish_reason)
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
            "content": [{ "text": req.prompt }],
        })];
        let text = self
            .converse_text_only(&self.model_id, messages, None)
            .await?;
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
    use crate::FunctionCall;
    use crate::chat::FunctionDef;
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

    // ── Request mapping ──────────────────────────────────────────

    fn sample_tool() -> Tool {
        Tool {
            tool_type: "function".to_owned(),
            function: FunctionDef {
                name: "add_numbers".to_owned(),
                description: "add two integers".to_owned(),
                parameters: serde_json::json!({
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

    /// Regression guard: Bedrock's toolSpec.name regex is
    /// `[a-zA-Z0-9_-]+`, but cairn ids routinely contain `.`
    /// (`github_api.review_pr`, `memory.search`). Sanitize on the
    /// way out and round-trip the name back on the way in.
    #[test]
    fn build_tool_config_sanitizes_dot_in_tool_name() {
        let t = Tool {
            tool_type: "function".to_owned(),
            function: FunctionDef {
                name: "github_api.review_pr".to_owned(),
                description: "post PR review".to_owned(),
                parameters: serde_json::json!({"type":"object","properties":{}}),
            },
        };
        let (cfg, rename) = build_tool_config(&[t], None).unwrap();
        assert_eq!(
            cfg["tools"][0]["toolSpec"]["name"], "github_api_review_pr",
            "dot must be sanitized to underscore for Converse toolSpec.name regex"
        );
        assert_eq!(
            rename.get("github_api_review_pr"),
            Some(&"github_api.review_pr".to_owned()),
            "rename map carries the sanitized→original reverse lookup"
        );
    }

    #[test]
    fn build_tool_config_rejects_sanitization_collision() {
        // Two tools that sanitize to the same Bedrock name — silently
        // dropping one would make the agent think the tool is available
        // but route every invocation to its namesake. Surface as an
        // InvalidRequest instead.
        let a = Tool {
            tool_type: "function".to_owned(),
            function: FunctionDef {
                name: "a.b".to_owned(),
                description: String::new(),
                parameters: serde_json::json!({}),
            },
        };
        let b = Tool {
            tool_type: "function".to_owned(),
            function: FunctionDef {
                name: "a_b".to_owned(),
                description: String::new(),
                parameters: serde_json::json!({}),
            },
        };
        let err = build_tool_config(&[a, b], None).unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("sanitization collision"), "got: {msg}");
    }

    #[test]
    fn parse_converse_response_restores_original_tool_name() {
        let resp = serde_json::json!({
            "output": {
                "message": {
                    "content": [{
                        "toolUse": {
                            "toolUseId": "tu_1",
                            "name": "github_api_review_pr",
                            "input": {"repo": "o/r", "pr_number": 1}
                        }
                    }]
                }
            }
        });
        let mut rename = std::collections::HashMap::new();
        rename.insert(
            "github_api_review_pr".to_owned(),
            "github_api.review_pr".to_owned(),
        );
        let parsed = parse_converse_response(resp, &rename).unwrap();
        let calls = parsed.tool_calls().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].function.name, "github_api.review_pr",
            "inbound tool_use.name must be reverse-mapped to the original cairn id"
        );
    }

    #[test]
    fn build_tool_config_maps_toolspec_shape() {
        let tools = vec![sample_tool()];
        let cfg = build_tool_config(&tools, None).unwrap().0;
        let spec = &cfg["tools"][0]["toolSpec"];
        assert_eq!(spec["name"], "add_numbers");
        assert_eq!(spec["description"], "add two integers");
        // inputSchema.json carries the full JSON schema as Converse expects.
        assert_eq!(spec["inputSchema"]["json"]["type"], "object");
        assert_eq!(
            spec["inputSchema"]["json"]["properties"]["a"]["type"],
            "integer"
        );
        // Auto toolChoice is implicit — Converse defaults to auto when
        // the field is absent.
        assert!(cfg.get("toolChoice").is_none());
    }

    #[test]
    fn build_tool_config_omits_empty_description() {
        let mut t = sample_tool();
        t.function.description.clear();
        let cfg = build_tool_config(&[t], None).unwrap().0;
        assert!(cfg["tools"][0]["toolSpec"].get("description").is_none());
    }

    #[test]
    fn build_tool_config_rejects_empty_tool_name() {
        let mut t = sample_tool();
        t.function.name.clear();
        let err = build_tool_config(&[t], None).unwrap_err();
        assert!(matches!(err, ProviderError::InvalidRequest(_)));
    }

    #[test]
    fn build_tool_config_maps_any_choice() {
        let cfg = build_tool_config(&[sample_tool()], Some(&ToolChoice::Any))
            .unwrap()
            .0;
        assert_eq!(cfg["toolChoice"]["any"], serde_json::json!({}));
    }

    #[test]
    fn build_tool_config_maps_specific_choice() {
        let cfg = build_tool_config(
            &[sample_tool()],
            Some(&ToolChoice::Specific("add_numbers".to_owned())),
        )
        .unwrap()
        .0;
        assert_eq!(cfg["toolChoice"]["tool"]["name"], "add_numbers");
    }

    #[test]
    fn build_tool_config_none_choice_returns_null() {
        // `ToolChoice::None` suppresses toolConfig entirely — Converse
        // has no "tools available but forbidden" knob.
        let cfg = build_tool_config(&[sample_tool()], Some(&ToolChoice::None))
            .unwrap()
            .0;
        assert!(cfg.is_null());
    }

    #[test]
    fn chat_message_to_converse_text_roundtrip() {
        let wire = chat_message_to_converse(&ChatMessage::user("hi")).unwrap();
        assert_eq!(wire["role"], "user");
        assert_eq!(wire["content"][0]["text"], "hi");
    }

    /// Regression: Bedrock rejects a multi-turn conversation if any
    /// `toolUse.name` in the message history contains `.`, even though
    /// the response parser restored the cairn id to carry through the
    /// orchestrator's `ToolCall` history. Sanitize on the way back out.
    #[test]
    fn chat_message_to_converse_sanitizes_tool_use_name_in_history() {
        let msg = ChatMessage {
            role: ChatRole::Assistant,
            content_type: MessageContent::ToolUse(vec![ToolCall {
                id: "tc1".to_owned(),
                call_type: "function".to_owned(),
                function: FunctionCall {
                    name: "github_api.review_pr".to_owned(),
                    arguments: r#"{"repo":"o/r","pr_number":1}"#.to_owned(),
                },
            }]),
            content: String::new(),
        };
        let wire = chat_message_to_converse(&msg).unwrap();
        let tu = &wire["content"][0]["toolUse"];
        assert_eq!(
            tu["name"], "github_api_review_pr",
            "toolUse.name must be sanitized to match the configured toolSpec.name"
        );
    }

    #[test]
    fn chat_message_to_converse_assistant_tool_use() {
        // Assistant ToolUse should serialise as a `toolUse` content
        // block with the parsed input object, not a stringified one.
        let msg = ChatMessage {
            role: ChatRole::Assistant,
            content_type: MessageContent::ToolUse(vec![ToolCall {
                id: "tc1".to_owned(),
                call_type: "function".to_owned(),
                function: FunctionCall {
                    name: "add_numbers".to_owned(),
                    arguments: r#"{"a":3,"b":4}"#.to_owned(),
                },
            }]),
            content: String::new(),
        };
        let wire = chat_message_to_converse(&msg).unwrap();
        assert_eq!(wire["role"], "assistant");
        let tu = &wire["content"][0]["toolUse"];
        assert_eq!(tu["toolUseId"], "tc1");
        assert_eq!(tu["name"], "add_numbers");
        assert_eq!(tu["input"]["a"], 3);
        assert_eq!(tu["input"]["b"], 4);
    }

    #[test]
    fn chat_message_to_converse_assistant_tool_use_with_text() {
        // Assistant can both emit text and call a tool in the same
        // turn — the text block must lead.
        let msg = ChatMessage {
            role: ChatRole::Assistant,
            content_type: MessageContent::ToolUse(vec![ToolCall {
                id: "tc1".to_owned(),
                call_type: "function".to_owned(),
                function: FunctionCall {
                    name: "f".to_owned(),
                    arguments: "{}".to_owned(),
                },
            }]),
            content: "Thinking about it...".to_owned(),
        };
        let wire = chat_message_to_converse(&msg).unwrap();
        assert_eq!(wire["content"][0]["text"], "Thinking about it...");
        assert_eq!(wire["content"][1]["toolUse"]["toolUseId"], "tc1");
    }

    #[test]
    fn chat_message_to_converse_tool_result_becomes_user_turn() {
        // Converse carries tool results on a user turn, not a tool
        // turn (unlike OpenAI). Assert we translate correctly.
        let msg =
            ChatMessage::tool_result("tc1".to_owned(), "add_numbers".to_owned(), "7".to_owned());
        let wire = chat_message_to_converse(&msg).unwrap();
        assert_eq!(wire["role"], "user");
        let tr = &wire["content"][0]["toolResult"];
        assert_eq!(tr["toolUseId"], "tc1");
        assert_eq!(tr["content"][0]["text"], "7");
    }

    #[test]
    fn chat_message_to_converse_tolerates_invalid_json_tool_args() {
        // If the model produced invalid JSON tool args, wrap in `_raw`
        // rather than erroring the whole turn.
        let msg = ChatMessage {
            role: ChatRole::Assistant,
            content_type: MessageContent::ToolUse(vec![ToolCall {
                id: "tc1".to_owned(),
                call_type: "function".to_owned(),
                function: FunctionCall {
                    name: "f".to_owned(),
                    arguments: "not json".to_owned(),
                },
            }]),
            content: String::new(),
        };
        let wire = chat_message_to_converse(&msg).unwrap();
        assert_eq!(wire["content"][0]["toolUse"]["input"]["_raw"], "not json");
    }

    // ── Response parsing ─────────────────────────────────────────

    #[test]
    fn parse_converse_response_text_only() {
        let resp = serde_json::json!({
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [{"text": "hello"}]
                }
            },
            "stopReason": "end_turn",
            "usage": {"inputTokens": 10, "outputTokens": 2, "totalTokens": 12}
        });
        let parsed = parse_converse_response(resp, &std::collections::HashMap::new()).unwrap();
        assert_eq!(parsed.text().as_deref(), Some("hello"));
        assert!(parsed.tool_calls().is_none());
        assert_eq!(parsed.finish_reason().as_deref(), Some("end_turn"));
        let u = parsed.usage().unwrap();
        assert_eq!(u.prompt_tokens, 10);
        assert_eq!(u.completion_tokens, 2);
    }

    #[test]
    fn parse_converse_response_tool_use() {
        // Real-shape fixture captured from `aws bedrock-runtime converse`
        // against us.anthropic.claude-opus-4-7.
        let resp = serde_json::json!({
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [{
                        "toolUse": {
                            "toolUseId": "tooluse_Z9sULYOSOASUR3SNOjygiX",
                            "name": "add_numbers",
                            "input": {"a": 3, "b": 4},
                            "type": "tool_use"
                        }
                    }]
                }
            },
            "stopReason": "tool_use",
            "usage": {"inputTokens": 789, "outputTokens": 86, "totalTokens": 875, "cacheReadInputTokens": 0}
        });
        let parsed = parse_converse_response(resp, &std::collections::HashMap::new()).unwrap();
        // Text is None (empty is normalised to None in `text()`).
        assert!(parsed.text().is_none());
        let calls = parsed.tool_calls().expect("tool_calls");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, "tooluse_Z9sULYOSOASUR3SNOjygiX");
        assert_eq!(calls[0].function.name, "add_numbers");
        // Arguments serialised back to canonical JSON string.
        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(args["a"], 3);
        assert_eq!(args["b"], 4);
        assert_eq!(parsed.finish_reason().as_deref(), Some("tool_use"));
        // Cache tokens = 0 normalise to None so metrics don't report noise.
        assert!(parsed.usage().unwrap().cached_tokens.is_none());
    }

    #[test]
    fn parse_converse_response_mixed_text_and_tool_use() {
        let resp = serde_json::json!({
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [
                        {"text": "Let me compute that."},
                        {"toolUse": {"toolUseId": "tc1", "name": "f", "input": {}}}
                    ]
                }
            },
            "stopReason": "tool_use",
            "usage": {"inputTokens": 1, "outputTokens": 1}
        });
        let parsed = parse_converse_response(resp, &std::collections::HashMap::new()).unwrap();
        assert_eq!(parsed.text().as_deref(), Some("Let me compute that."));
        assert_eq!(parsed.tool_calls().unwrap().len(), 1);
    }

    #[test]
    fn parse_converse_response_populates_cache_read_tokens() {
        let resp = serde_json::json!({
            "output": {"message": {"role": "assistant", "content": [{"text": "ok"}]}},
            "stopReason": "end_turn",
            "usage": {"inputTokens": 10, "outputTokens": 2, "cacheReadInputTokens": 7}
        });
        let u = parse_converse_response(resp, &std::collections::HashMap::new())
            .unwrap()
            .usage()
            .unwrap();
        assert_eq!(u.cached_tokens, Some(7));
    }

    #[test]
    fn parse_converse_response_rejects_tool_use_missing_id() {
        let resp = serde_json::json!({
            "output": {"message": {"role": "assistant", "content": [{"toolUse": {"name": "f", "input": {}}}]}},
            "stopReason": "tool_use",
            "usage": {"inputTokens": 1, "outputTokens": 1}
        });
        let err = parse_converse_response(resp, &std::collections::HashMap::new()).unwrap_err();
        assert!(matches!(err, ProviderError::Provider(_)));
    }

    #[test]
    fn parse_converse_response_rejects_tool_use_missing_input() {
        // The Converse spec requires `input` on every `toolUse` block.
        // A silent `null` would corrupt the next turn's orchestration,
        // so reject explicitly.
        let resp = serde_json::json!({
            "output": {"message": {"role": "assistant", "content": [{"toolUse": {"toolUseId": "t", "name": "f"}}]}},
            "stopReason": "tool_use",
            "usage": {"inputTokens": 1, "outputTokens": 1}
        });
        let err = parse_converse_response(resp, &std::collections::HashMap::new()).unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(err, ProviderError::Provider(_)) && msg.contains("input"),
            "expected missing-input rejection, got: {msg}"
        );
    }

    #[test]
    fn parse_converse_response_rejects_tool_use_non_object() {
        // Defensive: a malformed upstream sending `toolUse: "oops"`
        // shouldn't silently drop the block.
        let resp = serde_json::json!({
            "output": {"message": {"role": "assistant", "content": [{"toolUse": "oops"}]}},
            "stopReason": "tool_use",
            "usage": {"inputTokens": 1, "outputTokens": 1}
        });
        let err = parse_converse_response(resp, &std::collections::HashMap::new()).unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(err, ProviderError::Provider(_)) && msg.contains("not an object"),
            "expected non-object rejection, got: {msg}"
        );
    }

    #[test]
    fn chat_message_to_converse_rejects_non_object_valid_json_tool_args() {
        // Bedrock requires toolUse.input to be a JSON *object*. Valid
        // JSON that isn't an object (string, number, array) must fall
        // back to the `_raw` wrapper so we don't send a request the
        // service will reject.
        for non_object in ["\"foo\"", "42", "[1,2,3]", "null", "true"] {
            let msg = ChatMessage {
                role: ChatRole::Assistant,
                content_type: MessageContent::ToolUse(vec![ToolCall {
                    id: "tc1".to_owned(),
                    call_type: "function".to_owned(),
                    function: FunctionCall {
                        name: "f".to_owned(),
                        arguments: non_object.to_owned(),
                    },
                }]),
                content: String::new(),
            };
            let wire = chat_message_to_converse(&msg).unwrap();
            let input = &wire["content"][0]["toolUse"]["input"];
            assert!(
                input.is_object() && input["_raw"] == non_object,
                "non-object JSON {non_object:?} should wrap as _raw; got {input:?}"
            );
        }
    }
}
