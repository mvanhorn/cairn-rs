use serde::{Deserialize, Serialize};

/// Canonical plugin capability family names per RFC 007.
///
/// `KnowledgeProvider` added by RFC 029. `MemoryProvider` added by RFC 030
/// (split from the overloaded RFC 029 family). Unlike the other families,
/// both provider families negotiate their effective capability detail
/// (retrieval modes, ingest capability, per-dimension scoring support,
/// plus `auto_extract` for memory) at the `initialize` handshake rather
/// than declaring it in the manifest. See
/// [`KnowledgeProviderCapability`](crate::knowledge::KnowledgeProviderCapability)
/// and [`MemoryProviderCapability`](crate::memory::MemoryProviderCapability).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityFamily {
    ToolProvider,
    SignalSource,
    ChannelProvider,
    PostTurnHook,
    PolicyHook,
    EvalScorer,
    KnowledgeProvider,
    MemoryProvider,
}

impl CapabilityFamily {
    pub fn as_str(self) -> &'static str {
        match self {
            CapabilityFamily::ToolProvider => "tool_provider",
            CapabilityFamily::SignalSource => "signal_source",
            CapabilityFamily::ChannelProvider => "channel_provider",
            CapabilityFamily::PostTurnHook => "post_turn_hook",
            CapabilityFamily::PolicyHook => "policy_hook",
            CapabilityFamily::EvalScorer => "eval_scorer",
            CapabilityFamily::KnowledgeProvider => "knowledge_provider",
            CapabilityFamily::MemoryProvider => "memory_provider",
        }
    }

    /// RFC 030: the two provider families (memory / knowledge) are mutually
    /// exclusive on a single plugin. The handshake validator uses this to
    /// reject manifests or initialize responses that declare both.
    pub fn is_provider_family(self) -> bool {
        matches!(
            self,
            CapabilityFamily::KnowledgeProvider | CapabilityFamily::MemoryProvider
        )
    }
}

/// Canonical plugin invocation outcome statuses per RFC 007.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvocationStatus {
    Success,
    RetryableFailure,
    PermanentFailure,
    Timeout,
    Canceled,
    ProtocolViolation,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_family_str_matches_rfc() {
        assert_eq!(CapabilityFamily::ToolProvider.as_str(), "tool_provider");
        assert_eq!(CapabilityFamily::EvalScorer.as_str(), "eval_scorer");
        assert_eq!(
            CapabilityFamily::KnowledgeProvider.as_str(),
            "knowledge_provider"
        );
    }

    #[test]
    fn knowledge_provider_roundtrip() {
        let json = serde_json::to_string(&CapabilityFamily::KnowledgeProvider).unwrap();
        assert_eq!(json, "\"knowledge_provider\"");
        let back: CapabilityFamily = serde_json::from_str(&json).unwrap();
        assert_eq!(back, CapabilityFamily::KnowledgeProvider);
    }

    #[test]
    fn memory_provider_roundtrip() {
        let json = serde_json::to_string(&CapabilityFamily::MemoryProvider).unwrap();
        assert_eq!(json, "\"memory_provider\"");
        let back: CapabilityFamily = serde_json::from_str(&json).unwrap();
        assert_eq!(back, CapabilityFamily::MemoryProvider);
    }

    #[test]
    fn provider_family_flag_identifies_memory_and_knowledge() {
        assert!(CapabilityFamily::MemoryProvider.is_provider_family());
        assert!(CapabilityFamily::KnowledgeProvider.is_provider_family());
        assert!(!CapabilityFamily::ToolProvider.is_provider_family());
        assert!(!CapabilityFamily::EvalScorer.is_provider_family());
    }

    #[test]
    fn invocation_status_serde() {
        let json = serde_json::to_string(&InvocationStatus::Success).unwrap();
        assert_eq!(json, "\"success\"");
    }
}
