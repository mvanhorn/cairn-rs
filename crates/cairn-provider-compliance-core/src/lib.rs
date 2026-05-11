//! RFC 030 PR-H: family-neutral compliance checks.
//!
//! Shared across [`cairn_knowledge_compliance`] (RFC 029 PR-C) and
//! [`cairn_memory_compliance`] (RFC 030 PR-H). Only checks that apply
//! identically to both families live here; each family's suite owns
//! the checks that speak in its wire types.
//!
//! Contents:
//!
//! - [`check_no_dual_family_capabilities`] — the capabilities array in
//!   an initialize response must not declare both `memory_provider`
//!   and `knowledge_provider`. Mirrors the handshake validator from
//!   RFC 030 PR-A so compliance suites can assert a mock adapter's
//!   behaviour without running a real plugin host.
//! - [`check_error_shape_stability`] — `RetrievalError` + `IngestError`
//!   Display strings carry the context the operator UI depends on.
//! - [`ComplianceError`] + [`ComplianceResult`] — the shared failure
//!   shape used by both family-specific suites.

use cairn_memory::ingest::{IngestError, SourceType};
use cairn_memory::retrieval::RetrievalError;
use cairn_plugin_proto::CapabilityFamily;
use serde_json::Value;

/// Compliance failure shape. Both family-specific suites re-export this
/// under their own `compliance` alias so fixture test code stays brief.
#[derive(Debug, PartialEq, Eq)]
pub struct ComplianceError {
    pub reason: String,
}

impl std::fmt::Display for ComplianceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "compliance failure: {}", self.reason)
    }
}

impl std::error::Error for ComplianceError {}

pub type ComplianceResult<T = ()> = Result<T, ComplianceError>;

fn fail(reason: impl Into<String>) -> ComplianceError {
    ComplianceError {
        reason: reason.into(),
    }
}

/// RFC 030 §Decisions D4: a plugin MUST NOT advertise both
/// `memory_provider` and `knowledge_provider` in the same initialize
/// response. The runtime's handshake validator
/// ([`cairn_tools::handshake_validator::reject_dual_provider_families`])
/// rejects this at boot; the compliance suite replays the same rule so
/// adapter authors can verify their `initialize` shape without running
/// a real plugin host.
///
/// Accepts `Vec<serde_json::Value>` matching the shape on the wire
/// (`InitializeResult.capabilities`). Each entry must carry a `type`
/// string; entries without one are ignored (unrelated bug, not a
/// dual-family violation).
pub fn check_no_dual_family_capabilities(capabilities: &[Value]) -> ComplianceResult {
    let mut memory_seen = false;
    let mut knowledge_seen = false;
    for cap in capabilities {
        let Some(ty) = cap.get("type").and_then(Value::as_str) else {
            continue;
        };
        if ty == CapabilityFamily::MemoryProvider.as_str() {
            memory_seen = true;
        } else if ty == CapabilityFamily::KnowledgeProvider.as_str() {
            knowledge_seen = true;
        }
    }
    if memory_seen && knowledge_seen {
        return Err(fail(
            "initialize response declares both `memory_provider` and \
             `knowledge_provider` in capabilities[]; RFC 030 §Decisions \
             D4 forbids a single plugin from straddling both families",
        ));
    }
    Ok(())
}

/// Error Display text is part of the wire contract (it flows into
/// operator UI + structured logs). The shape must stay stable so
/// operators' alerts / diagnostics don't drift.
///
/// Covers `RetrievalError` + `IngestError` — both are shared between
/// the memory and knowledge paths because RFC 030 did not split the
/// in-process error types.
pub fn check_error_shape_stability() -> ComplianceResult {
    let retrieval_unavailable = RetrievalError::ProviderUnavailable {
        provider: "plugin:x".to_owned(),
        reason: "no handshake".to_owned(),
    }
    .to_string();
    if !retrieval_unavailable.contains("plugin:x")
        || !retrieval_unavailable.contains("no handshake")
    {
        return Err(fail(format!(
            "RetrievalError::ProviderUnavailable display drift: {retrieval_unavailable}"
        )));
    }

    let retrieval_internal = RetrievalError::Internal("boom".to_owned()).to_string();
    if !retrieval_internal.contains("boom") {
        return Err(fail(format!(
            "RetrievalError::Internal display drift: {retrieval_internal}"
        )));
    }

    let ingest_rejected = IngestError::ProviderRejected {
        provider: "plugin:x".to_owned(),
        reason: "read-only".to_owned(),
    }
    .to_string();
    if !ingest_rejected.contains("plugin:x") || !ingest_rejected.contains("read-only") {
        return Err(fail(format!(
            "IngestError::ProviderRejected display drift: {ingest_rejected}"
        )));
    }

    let ingest_unavailable = IngestError::ProviderUnavailable {
        provider: "plugin:x".to_owned(),
        reason: "plugin crashed".to_owned(),
    }
    .to_string();
    if !ingest_unavailable.contains("plugin:x") || !ingest_unavailable.contains("plugin crashed") {
        return Err(fail(format!(
            "IngestError::ProviderUnavailable display drift: {ingest_unavailable}"
        )));
    }

    let ingest_unsupported = IngestError::UnsupportedSource(SourceType::KnowledgePack).to_string();
    if !ingest_unsupported.contains("unsupported source") {
        return Err(fail(format!(
            "IngestError::UnsupportedSource display drift: {ingest_unsupported}"
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn empty_capabilities_pass_dual_family_check() {
        assert!(check_no_dual_family_capabilities(&[]).is_ok());
    }

    #[test]
    fn single_family_passes() {
        assert!(
            check_no_dual_family_capabilities(&[json!({"type": "knowledge_provider"})]).is_ok()
        );
        assert!(check_no_dual_family_capabilities(&[json!({"type": "memory_provider"})]).is_ok());
    }

    #[test]
    fn multi_non_provider_families_pass() {
        let caps = vec![
            json!({"type": "tool_provider"}),
            json!({"type": "signal_source"}),
            json!({"type": "knowledge_provider"}),
        ];
        assert!(check_no_dual_family_capabilities(&caps).is_ok());
    }

    #[test]
    fn dual_provider_families_fail() {
        let caps = vec![
            json!({"type": "memory_provider"}),
            json!({"type": "knowledge_provider"}),
        ];
        let err = check_no_dual_family_capabilities(&caps).unwrap_err();
        assert!(err.reason.contains("memory_provider"));
        assert!(err.reason.contains("knowledge_provider"));
        assert!(err.reason.contains("RFC 030"));
    }

    #[test]
    fn capabilities_without_type_are_skipped() {
        let caps = vec![
            json!({"no_type_field": "knowledge_provider"}),
            json!({"type": "memory_provider"}),
        ];
        assert!(check_no_dual_family_capabilities(&caps).is_ok());
    }

    #[test]
    fn error_shape_stability_passes() {
        check_error_shape_stability().unwrap();
    }
}
