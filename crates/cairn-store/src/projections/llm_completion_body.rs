//! Issue #668: LLM chain-of-thought body projection.
//!
//! `ProviderCallCompleted` + `LlmCallTrace` capture metadata (tokens,
//! latency, cost, model id). This projection is the sibling that
//! captures the actual round-trip *body* — the system prompt the LLM
//! received, the user messages, the response text, and the proposed
//! tool calls. Operators use the stored bodies to debug why the LLM
//! made a specific decision and to audit what prompt context it
//! actually saw.
//!
//! Written from `RuntimeEvent::LlmCompletionRecorded` events, which
//! are emitted by the orchestrator alongside `ProviderCallCompleted`
//! after every successful LLM call. The `trace_id` on each row
//! matches the sibling `provider_call_id` so operators can join the
//! body to the metadata row.
//!
//! # Redaction
//!
//! The emit site runs `cairn_providers::redact::redact_secrets` on
//! every text field BEFORE constructing the event. This projection
//! does NOT re-redact; consumers should treat stored bodies as
//! already-redacted-of-known-secret-patterns.
//!
//! # Size + truncation
//!
//! Individual text fields can be tens of kilobytes for long reasoning
//! chains. The emit site truncates any field that exceeds
//! `CAIRN_LLM_TRACE_MAX_FIELD_BYTES` (default 256 KiB) with a
//! `[TRUNCATED]` marker appended. This keeps the event log bounded
//! without losing the most useful prefix.

use async_trait::async_trait;
use cairn_domain::tenancy::ProjectKey;
use cairn_domain::{RunId, SessionId};

use crate::error::StoreError;

/// One row per `LlmCompletionRecorded` event — the full body of a
/// single LLM round-trip, post-redaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LlmCompletionBodyRecord {
    /// Matches the sibling `ProviderCallCompleted.provider_call_id`
    /// so the body can be joined to the metadata row.
    pub trace_id: String,
    pub project: ProjectKey,
    pub session_id: SessionId,
    pub run_id: Option<RunId>,
    pub model_id: String,
    /// Post-redaction system prompt the LLM received.
    pub system_prompt: String,
    /// JSON-serialised `Vec<Message>` (role + content), post-redaction.
    pub messages_json: String,
    /// Post-redaction free-text response from the provider.
    pub response_text: String,
    /// JSON-serialised `Vec<ToolCall>` the LLM proposed, post-redaction.
    pub tool_calls_json: String,
    pub recorded_at_ms: u64,
}

/// Read API for the `llm_completions` projection.
#[async_trait]
pub trait LlmCompletionBodyReadModel: Send + Sync {
    /// Fetch a single body row by trace id. Returns `None` when the
    /// row doesn't exist (either the LLM call predates G1 of #668,
    /// the tenant has opted out via `CAIRN_LLM_TRACE_BODIES_ENABLED=false`,
    /// or the retention sweeper has reaped the row).
    async fn get_by_trace_id(
        &self,
        trace_id: &str,
    ) -> Result<Option<LlmCompletionBodyRecord>, StoreError>;

    /// List all body rows for a session, ordered by `recorded_at_ms`
    /// ascending. Used by the UI Reasoning tab to render every LLM
    /// round-trip for a given session in order.
    async fn list_by_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<LlmCompletionBodyRecord>, StoreError>;
}
