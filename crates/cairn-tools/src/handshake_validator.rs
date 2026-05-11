//! RFC 030 handshake-time validators.
//!
//! Called by [`crate::plugin_host::StdioPluginHost::handshake`] after the
//! plugin's `InitializeResult` is parsed but before state transitions to
//! `Ready`. Each validator returns `Err(String)` with an operator-legible
//! explanation of the rejection; the host converts that into
//! `PluginHostError::HandshakeFailed` and marks the plugin `Failed`.
//!
//! These live in a dedicated module so that both the stdio host and the
//! compliance suite can call them against the same
//! `Vec<serde_json::Value>` capability wire shape.

use cairn_plugin_proto::CapabilityFamily;
use serde_json::Value;

/// Reject handshake responses that declare both `memory_provider` and
/// `knowledge_provider` in `capabilities[]`.
///
/// RFC 030 §Decisions locks each plugin to a single provider family.
/// Dual-declaration is an operator/plugin-author mistake, not a feature:
/// the two families have divergent post-hoc rescorer paths, tool-surface
/// rules (`auto_extract`), and scoring-policy keys. Routing on family works
/// only if one plugin means exactly one family.
///
/// Other multi-capability combinations (e.g. `tool_provider` +
/// `signal_source` + `knowledge_provider`) are legal and untouched by this
/// check.
pub fn reject_dual_provider_families(capabilities: &[Value]) -> Result<(), String> {
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
        Err("initialize response declares both `memory_provider` and \
             `knowledge_provider` in capabilities[]; RFC 030 forbids a single \
             plugin from straddling both families. Split into two plugins or \
             drop one family."
            .to_owned())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_capabilities_pass() {
        assert!(reject_dual_provider_families(&[]).is_ok());
    }

    #[test]
    fn single_knowledge_family_passes() {
        let caps = vec![serde_json::json!({
            "type": "knowledge_provider",
            "detail": { "retrievalModes": ["hybrid"] }
        })];
        assert!(reject_dual_provider_families(&caps).is_ok());
    }

    #[test]
    fn single_memory_family_passes() {
        let caps = vec![serde_json::json!({
            "type": "memory_provider",
            "detail": { "autoExtract": true }
        })];
        assert!(reject_dual_provider_families(&caps).is_ok());
    }

    #[test]
    fn memory_plus_tool_provider_is_legal() {
        let caps = vec![
            serde_json::json!({"type": "memory_provider", "detail": {}}),
            serde_json::json!({"type": "tool_provider"}),
        ];
        assert!(reject_dual_provider_families(&caps).is_ok());
    }

    #[test]
    fn knowledge_plus_signal_source_is_legal() {
        let caps = vec![
            serde_json::json!({"type": "knowledge_provider", "detail": {}}),
            serde_json::json!({"type": "signal_source"}),
        ];
        assert!(reject_dual_provider_families(&caps).is_ok());
    }

    #[test]
    fn dual_family_is_rejected() {
        let caps = vec![
            serde_json::json!({"type": "memory_provider", "detail": {}}),
            serde_json::json!({"type": "knowledge_provider", "detail": {}}),
        ];
        let err = reject_dual_provider_families(&caps).unwrap_err();
        assert!(
            err.contains("memory_provider") && err.contains("knowledge_provider"),
            "error message should cite both families: {err}"
        );
        assert!(err.contains("RFC 030"), "error should cite RFC 030");
    }

    #[test]
    fn capabilities_without_type_field_are_skipped() {
        // Pathological shape that might arrive from a broken plugin. The
        // validator should not panic on it; other validators will fail the
        // handshake for its own reasons.
        let caps = vec![
            serde_json::json!({"not_type": "memory_provider"}),
            serde_json::json!({"type": "tool_provider"}),
        ];
        assert!(reject_dual_provider_families(&caps).is_ok());
    }
}
