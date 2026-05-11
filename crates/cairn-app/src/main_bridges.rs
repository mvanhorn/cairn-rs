//! Thin trait bridges that glue cairn-app's runtime state to the
//! plugin-host surfaces. Extracted from `main.rs` so multiple call
//! sites (agent-tool wiring, memory-ingest dispatcher, deep-search
//! future hook) can share the same implementations without
//! duplicating them as inline `struct`s.

use async_trait::async_trait;

use cairn_memory::multi_provider::KnowledgePluginDispatcher;
use cairn_memory::multi_provider_memory::{MemoryPluginDispatcher, MemoryPluginError};
use cairn_plugin_proto::memory::{
    MemoryIngestAck, MemoryIngestParams, MemoryIngestStatusParams, MemoryIngestStatusResult,
    MemoryQueryParams, MemoryQueryResult,
};

/// Adapter that lets a knowledge-family stdio dispatcher answer
/// memory-family calls during the RFC 030 rollout window.
///
/// The memory + knowledge wire types are structurally identical
/// (same `ScoringBreakdownWire`, `RetrievalModeWire`, etc.), so the
/// conversion is a field-move per struct via the `From` impls in
/// `cairn_plugin_proto::memory`. Errors are translated via
/// `From<KnowledgePluginError> for MemoryPluginError` in cairn-memory.
/// No serde_json round-trip.
///
/// Replaced by a dedicated memory-family dispatcher
/// (cairn-memory-mem0's stdio host) once the plugin-host cache
/// exposes a per-family channel. Until then this bridge keeps a
/// single stdio-knowledge subprocess serving both families via the
/// shared `StdioKnowledgeDispatcher` and its `Arc<AtomicU64>`
/// request-id counter.
pub struct KnowledgeDispatcherAsMemory<T>(pub T);

#[async_trait]
impl<T> MemoryPluginDispatcher for KnowledgeDispatcherAsMemory<T>
where
    T: KnowledgePluginDispatcher + Send + Sync,
{
    async fn query(
        &self,
        plugin_id: &str,
        params: MemoryQueryParams,
    ) -> Result<MemoryQueryResult, MemoryPluginError> {
        let k_res = self.0.query(plugin_id, params.into()).await?;
        Ok(k_res.into())
    }

    async fn ingest(
        &self,
        plugin_id: &str,
        params: MemoryIngestParams,
    ) -> Result<MemoryIngestAck, MemoryPluginError> {
        let ack = self.0.ingest(plugin_id, params.into()).await?;
        Ok(ack.into())
    }

    async fn ingest_status(
        &self,
        plugin_id: &str,
        params: MemoryIngestStatusParams,
    ) -> Result<MemoryIngestStatusResult, MemoryPluginError> {
        let res = self.0.ingest_status(plugin_id, params.into()).await?;
        Ok(res.into())
    }
}
