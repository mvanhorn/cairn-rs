//! Language-neutral plugin protocol boundaries and shared types.
//!
//! Defines the JSON-RPC 2.0 wire format, manifest schema, and capability
//! declarations per RFC 007. The crate describes what goes on the wire; it
//! depends on `cairn-domain` only for shared ID newtypes (`ProjectKey`,
//! `ChunkId`, `DocumentId`, `SourceId`) that are intentionally stable
//! across the plugin boundary (RFC 029 Decided list; RFC 030 renamed
//! `KnowledgeDocumentId` to the family-neutral `DocumentId` with a
//! back-compat alias).

pub mod capabilities;
pub mod knowledge;
pub mod manifest;
pub mod memory;
pub mod wire;

pub use capabilities::{CapabilityFamily, InvocationStatus};
pub use knowledge::{
    ChunkRecordWire, DimensionSupport, KnowledgeIngestAck, KnowledgeIngestParams,
    KnowledgeIngestStatus, KnowledgeIngestStatusParams, KnowledgeIngestStatusResult,
    KnowledgeListSourcesParams, KnowledgeListSourcesResult, KnowledgeProviderCapability,
    KnowledgeQueryParams, KnowledgeQueryResult, KnowledgeSource, KnowledgeSourcesChangedParams,
    MetadataFilterWire, RetrievalModeWire, RetrievalResultWire, ScoringBreakdownWire,
    ScoringDimensionSet, SourceTypeWire,
};
pub use manifest::{CapabilityWire, LimitsWire, PluginManifestWire};
pub use memory::{
    MemoryChunkRecordWire, MemoryIngestAck, MemoryIngestParams, MemoryIngestStatus,
    MemoryIngestStatusParams, MemoryIngestStatusResult, MemoryListSourcesParams,
    MemoryListSourcesResult, MemoryProviderCapability, MemoryQueryDiagnostics, MemoryQueryParams,
    MemoryQueryResult, MemoryRetrievalResultWire, MemorySource, MemorySourcesChangedParams,
};
pub use wire::{
    ActorWire, CancelParams, CancelResult, ChannelsDeliverParams, ChannelsDeliverResult,
    EvalScoreParams, EvalScoreResult, EventEmitParams, HooksPostTurnParams, HooksPostTurnResult,
    HostInfo, InitializeParams, InitializeResult, JsonRpcError, JsonRpcErrorBody,
    JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, LogEmitParams, PluginInfo,
    PluginNotification, PolicyEvaluateParams, PolicyEvaluateResult, ProgressUpdateParams,
    RuntimeLinkageWire, ScopeWire, SignalsPollParams, SignalsPollResult, ToolDescriptorWire,
    ToolsInvokeParams, ToolsInvokeResult, ToolsListResult,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_version_is_one_zero() {
        let params = InitializeParams {
            protocol_version: "1.0".to_owned(),
            host: HostInfo {
                name: "cairn".to_owned(),
                version: "0.1.0".to_owned(),
            },
        };
        assert_eq!(params.protocol_version, "1.0");
    }
}
