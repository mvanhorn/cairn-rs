//! RFC 030 PR-C: `MultiProviderMemory` — runtime dispatch layer for the
//! memory-provider capability family.
//!
//! Structural twin of [`crate::multi_provider::MultiProviderRetrieval`] +
//! [`crate::multi_provider::MultiProviderIngest`] but routing `memory.*`
//! wire calls rather than `knowledge.*`. Decisions locked by the RFC:
//!
//! - `cairn-default` → call through to the in-process
//!   [`RetrievalService`] / [`IngestService`] (the same default used by
//!   the knowledge family; per RFC 030 it serves both families today,
//!   marked as TODO for a dedicated default knowledge context).
//! - `plugin:<id>` → marshal to wire params via the memory-family bridge,
//!   dispatch to a [`MemoryPluginDispatcher`], lift the wire response back
//!   into the in-process shape. The memory wire types
//!   (`MemoryQueryParams`, etc.) are structurally identical to the
//!   knowledge-family types but are distinct on the Rust side so code
//!   routing on [`cairn_plugin_proto::CapabilityFamily`] cannot
//!   accidentally feed a knowledge payload into a memory path.
//!
//! `MemoryPluginDispatcher` is independent from `KnowledgePluginDispatcher`
//! on purpose — each transport implementation (stdio, SSE, etc.) will
//! provide one impl per family so the compliance suite's family-mismatch
//! guards can treat them separately.
//!
//! This module is transport- and registry-agnostic. It takes trait objects
//! for both "which memory provider is configured" and "how to dispatch a
//! `memory.*` call", mirroring the knowledge module. App-layer composition
//! picks concrete impls (project projection + stdio plugin host).

use async_trait::async_trait;
use cairn_domain::{DocumentId, ProjectKey};
use cairn_plugin_proto::memory::{
    MemoryIngestAck, MemoryIngestParams, MemoryIngestStatusParams, MemoryIngestStatusResult,
    MemoryQueryParams, MemoryQueryResult,
};

use crate::ingest::{IngestError, IngestPackRequest, IngestRequest, IngestService, IngestStatus};
use crate::multi_provider::{
    parse_provider_ref, NoOpResponseHook, ProviderResolver, ProviderRoute, ResponseHook,
};
use crate::retrieval::{RetrievalError, RetrievalQuery, RetrievalResponse, RetrievalService};

/// Dispatches a `memory.*` call to a plugin subprocess. Mirrors
/// [`crate::multi_provider::KnowledgePluginDispatcher`]; kept as a distinct
/// trait so host implementations implement one per family and the
/// handshake-validator can assert the family-match invariant.
#[async_trait]
pub trait MemoryPluginDispatcher: Send + Sync {
    async fn query(
        &self,
        plugin_id: &str,
        params: MemoryQueryParams,
    ) -> Result<MemoryQueryResult, MemoryPluginError>;

    async fn ingest(
        &self,
        plugin_id: &str,
        params: MemoryIngestParams,
    ) -> Result<MemoryIngestAck, MemoryPluginError>;

    async fn ingest_status(
        &self,
        plugin_id: &str,
        params: MemoryIngestStatusParams,
    ) -> Result<MemoryIngestStatusResult, MemoryPluginError>;
}

#[async_trait]
impl<T: MemoryPluginDispatcher + ?Sized> MemoryPluginDispatcher for std::sync::Arc<T> {
    async fn query(
        &self,
        plugin_id: &str,
        params: MemoryQueryParams,
    ) -> Result<MemoryQueryResult, MemoryPluginError> {
        (**self).query(plugin_id, params).await
    }

    async fn ingest(
        &self,
        plugin_id: &str,
        params: MemoryIngestParams,
    ) -> Result<MemoryIngestAck, MemoryPluginError> {
        (**self).ingest(plugin_id, params).await
    }

    async fn ingest_status(
        &self,
        plugin_id: &str,
        params: MemoryIngestStatusParams,
    ) -> Result<MemoryIngestStatusResult, MemoryPluginError> {
        (**self).ingest_status(plugin_id, params).await
    }
}

/// Errors from the memory plugin dispatcher. Shape mirrors
/// [`crate::multi_provider::KnowledgePluginError`] — the runtime translates
/// each variant to the right `RetrievalError` / `IngestError` at the call
/// site.
#[derive(Debug)]
pub enum MemoryPluginError {
    Unavailable(String),
    PluginError(String),
    Internal(String),
}

impl std::fmt::Display for MemoryPluginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable(msg) => write!(f, "memory plugin unavailable: {msg}"),
            Self::PluginError(msg) => write!(f, "memory plugin error: {msg}"),
            Self::Internal(msg) => write!(f, "internal memory dispatch error: {msg}"),
        }
    }
}

impl std::error::Error for MemoryPluginError {}

/// RFC 030 PR-F rollout: lets bridge adapters that invoke the
/// knowledge-family dispatcher on behalf of a memory-family call
/// propagate errors without hand-rolling a match on every site.
impl From<crate::multi_provider::KnowledgePluginError> for MemoryPluginError {
    fn from(e: crate::multi_provider::KnowledgePluginError) -> Self {
        use crate::multi_provider::KnowledgePluginError as K;
        match e {
            K::Unavailable(m) => Self::Unavailable(m),
            K::PluginError(m) => Self::PluginError(m),
            K::Internal(m) => Self::Internal(m),
        }
    }
}

// ─── Wire conversions (memory family) ─────────────────────────────────────
//
// Structural twins of the knowledge-family `From` impls in
// `plugin_bridge.rs`. Kept here rather than in `plugin_bridge.rs` so the
// memory wire types live beside the dispatcher that marshals them — moving
// both is a small diff if we ever split the crate.

impl From<RetrievalQuery> for MemoryQueryParams {
    fn from(q: RetrievalQuery) -> Self {
        Self {
            project: q.project,
            query_text: q.query_text,
            mode: q.mode.into(),
            limit: u32::try_from(q.limit).unwrap_or(u32::MAX),
            metadata_filters: q.metadata_filters.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<MemoryQueryResult> for RetrievalResponse {
    fn from(r: MemoryQueryResult) -> Self {
        // The in-process `RetrievalResponse` shape is family-neutral; we
        // lift via the knowledge-family equivalent by copying the
        // diagnostics and results field-wise so both families share one
        // downstream rescorer path. Mirrors `KnowledgeQueryResult` →
        // `RetrievalResponse` byte-for-byte except for the Rust type
        // identities.
        use cairn_plugin_proto::knowledge::{
            ChunkRecordWire, KnowledgeQueryDiagnostics, KnowledgeQueryResult, RetrievalResultWire,
        };
        let proxy = KnowledgeQueryResult {
            results: r
                .results
                .into_iter()
                .map(|m| RetrievalResultWire {
                    chunk: ChunkRecordWire {
                        chunk_id: m.chunk.chunk_id,
                        document_id: m.chunk.document_id,
                        source_id: m.chunk.source_id,
                        source_type: m.chunk.source_type,
                        project: m.chunk.project,
                        text: m.chunk.text,
                        position: m.chunk.position,
                        created_at: m.chunk.created_at,
                        updated_at: m.chunk.updated_at,
                        provenance_metadata: m.chunk.provenance_metadata,
                        credibility_score: m.chunk.credibility_score,
                        graph_linkage: m.chunk.graph_linkage,
                        content_hash: m.chunk.content_hash,
                        entities: m.chunk.entities,
                    },
                    score: m.score,
                    breakdown: m.breakdown,
                })
                .collect(),
            diagnostics: KnowledgeQueryDiagnostics {
                mode_used: r.diagnostics.mode_used,
                stages_used: r.diagnostics.stages_used,
                reranker_used: r.diagnostics.reranker_used,
                scoring_dimensions_used: r.diagnostics.scoring_dimensions_used,
                results_returned: r.diagnostics.results_returned,
                latency_ms: r.diagnostics.latency_ms,
            },
        };
        proxy.into()
    }
}

impl From<IngestRequest> for MemoryIngestParams {
    fn from(r: IngestRequest) -> Self {
        Self {
            document_id: r.document_id,
            source_id: r.source_id,
            source_type: r.source_type.into(),
            project: r.project,
            content: r.content,
            import_id: r.import_id,
            corpus_id: r.corpus_id,
            tags: r.tags,
        }
    }
}

// ─── MultiProviderMemory (retrieval side) ─────────────────────────────────

/// Dispatching [`RetrievalService`] that routes each memory query to the
/// project's configured memory provider.
///
/// Mirrors [`crate::multi_provider::MultiProviderRetrieval`] but dispatches
/// through a [`MemoryPluginDispatcher`] for the plugin route. The
/// in-process default provider is shared with the knowledge family today —
/// per RFC 030, cairn-default serves both families until a dedicated
/// default memory backend lands.
pub struct MultiProviderMemory<R, P, D, H = NoOpResponseHook> {
    default: R,
    resolver: P,
    dispatcher: D,
    hook: H,
}

impl<R, P, D> MultiProviderMemory<R, P, D, NoOpResponseHook> {
    pub fn new(default: R, resolver: P, dispatcher: D) -> Self {
        Self {
            default,
            resolver,
            dispatcher,
            hook: NoOpResponseHook,
        }
    }
}

impl<R, P, D, H> MultiProviderMemory<R, P, D, H> {
    /// Wire a response hook (typically the post-hoc rescorer parameterised
    /// on the memory family — PR-F). Every response flows through the hook
    /// regardless of route so the runtime-owned dimension contract holds
    /// for both cairn-default and plugin paths.
    pub fn with_response_hook<H2>(self, hook: H2) -> MultiProviderMemory<R, P, D, H2> {
        MultiProviderMemory {
            default: self.default,
            resolver: self.resolver,
            dispatcher: self.dispatcher,
            hook,
        }
    }
}

#[async_trait]
impl<R, P, D, H> RetrievalService for MultiProviderMemory<R, P, D, H>
where
    R: std::ops::Deref + Send + Sync,
    R::Target: RetrievalService,
    P: ProviderResolver,
    D: MemoryPluginDispatcher,
    H: ResponseHook,
{
    async fn query(&self, query: RetrievalQuery) -> Result<RetrievalResponse, RetrievalError> {
        let pref = self
            .resolver
            .resolve(&query.project)
            .await
            .map_err(|e| RetrievalError::Internal(e.to_string()))?;

        let response = match parse_provider_ref(&pref) {
            ProviderRoute::CairnDefault => self.default.query(query).await?,
            ProviderRoute::Plugin(plugin_id) => {
                let plugin_id_owned = plugin_id.to_owned();
                let params: MemoryQueryParams = query.into();
                let wire_result = self
                    .dispatcher
                    .query(&plugin_id_owned, params)
                    .await
                    .map_err(|e| memory_plugin_error_to_retrieval_error(&plugin_id_owned, e))?;
                wire_result.into()
            }
            ProviderRoute::Unknown(raw) => {
                return Err(RetrievalError::ProviderUnavailable {
                    provider: raw.to_owned(),
                    reason: "unrecognised provider_ref shape".to_owned(),
                });
            }
        };

        self.hook.apply(response).await
    }
}

// ─── MultiProviderMemoryIngest ────────────────────────────────────────────

/// Dispatching [`IngestService`] for the memory family. Symmetric to
/// [`crate::multi_provider::MultiProviderIngest`]. Same operational
/// tradeoffs: `submit_pack` (RFC 013 bundles) is cairn-specific, so plugin
/// memory providers receive [`IngestError::ProviderRejected`].
pub struct MultiProviderMemoryIngest<I, P, D> {
    default: I,
    resolver: P,
    dispatcher: D,
}

impl<I, P, D> MultiProviderMemoryIngest<I, P, D> {
    pub fn new(default: I, resolver: P, dispatcher: D) -> Self {
        Self {
            default,
            resolver,
            dispatcher,
        }
    }
}

#[async_trait]
impl<I, P, D> IngestService for MultiProviderMemoryIngest<I, P, D>
where
    I: std::ops::Deref + Send + Sync,
    I::Target: IngestService,
    P: ProviderResolver,
    D: MemoryPluginDispatcher,
{
    async fn submit(&self, request: IngestRequest) -> Result<(), IngestError> {
        let pref = self
            .resolver
            .resolve(&request.project)
            .await
            .map_err(|e| IngestError::Internal(e.to_string()))?;

        match parse_provider_ref(&pref) {
            ProviderRoute::CairnDefault => self.default.submit(request).await,
            ProviderRoute::Plugin(plugin_id) => {
                let plugin_id_owned = plugin_id.to_owned();
                let document_id = request.document_id.clone();
                let params: MemoryIngestParams = request.into();
                let ack = self
                    .dispatcher
                    .ingest(&plugin_id_owned, params)
                    .await
                    .map_err(|e| memory_plugin_error_to_ingest_error(&plugin_id_owned, e))?;
                if ack.accepted {
                    let _ = document_id;
                    Ok(())
                } else {
                    Err(IngestError::ProviderRejected {
                        provider: format!("plugin:{plugin_id_owned}"),
                        reason: ack
                            .reason
                            .unwrap_or_else(|| "provider declined memory ingest".to_owned()),
                    })
                }
            }
            ProviderRoute::Unknown(raw) => Err(IngestError::ProviderUnavailable {
                provider: raw.to_owned(),
                reason: "unrecognised provider_ref shape".to_owned(),
            }),
        }
    }

    async fn submit_pack(&self, request: IngestPackRequest) -> Result<(), IngestError> {
        let pref = self
            .resolver
            .resolve(&request.project)
            .await
            .map_err(|e| IngestError::Internal(e.to_string()))?;

        match parse_provider_ref(&pref) {
            ProviderRoute::CairnDefault => self.default.submit_pack(request).await,
            ProviderRoute::Plugin(plugin_id) => Err(IngestError::ProviderRejected {
                provider: format!("plugin:{plugin_id}"),
                reason: "knowledge-pack ingest (RFC 013 bundles) is a cairn-default pipeline; \
                    plugin memory providers own their own ingest surface"
                    .to_owned(),
            }),
            ProviderRoute::Unknown(raw) => Err(IngestError::ProviderUnavailable {
                provider: raw.to_owned(),
                reason: "unrecognised provider_ref shape".to_owned(),
            }),
        }
    }

    async fn status(&self, document_id: &DocumentId) -> Result<Option<IngestStatus>, IngestError> {
        self.default.status(document_id).await
    }
}

impl<I, P, D> MultiProviderMemoryIngest<I, P, D>
where
    I: std::ops::Deref + Send + Sync,
    I::Target: IngestService,
    P: ProviderResolver,
    D: MemoryPluginDispatcher,
{
    /// Project-scoped ingest-status lookup for the memory family. Required
    /// for plugin providers because `memory.ingest_status` is keyed per
    /// `(project, document_id)` via the dispatcher.
    pub async fn plugin_ingest_status(
        &self,
        project: &ProjectKey,
        document_id: &DocumentId,
    ) -> Result<Option<IngestStatus>, IngestError> {
        let pref = self
            .resolver
            .resolve(project)
            .await
            .map_err(|e| IngestError::Internal(e.to_string()))?;

        match parse_provider_ref(&pref) {
            ProviderRoute::CairnDefault => self.default.status(document_id).await,
            ProviderRoute::Plugin(plugin_id) => {
                let plugin_id_owned = plugin_id.to_owned();
                let result = self
                    .dispatcher
                    .ingest_status(
                        &plugin_id_owned,
                        MemoryIngestStatusParams {
                            document_id: document_id.clone(),
                        },
                    )
                    .await
                    .map_err(|e| memory_plugin_error_to_ingest_error(&plugin_id_owned, e))?;
                Ok(result.status.map(memory_status_to_ingest_status))
            }
            ProviderRoute::Unknown(raw) => Err(IngestError::ProviderUnavailable {
                provider: raw.to_owned(),
                reason: "unrecognised provider_ref shape".to_owned(),
            }),
        }
    }
}

fn memory_plugin_error_to_retrieval_error(plugin_id: &str, e: MemoryPluginError) -> RetrievalError {
    match e {
        MemoryPluginError::Unavailable(reason) => RetrievalError::ProviderUnavailable {
            provider: format!("plugin:{plugin_id}"),
            reason,
        },
        MemoryPluginError::PluginError(msg) => RetrievalError::Internal(msg),
        MemoryPluginError::Internal(msg) => RetrievalError::Internal(msg),
    }
}

fn memory_plugin_error_to_ingest_error(plugin_id: &str, e: MemoryPluginError) -> IngestError {
    match e {
        MemoryPluginError::Unavailable(reason) => IngestError::ProviderUnavailable {
            provider: format!("plugin:{plugin_id}"),
            reason,
        },
        MemoryPluginError::PluginError(msg) => IngestError::Internal(msg),
        MemoryPluginError::Internal(msg) => IngestError::Internal(msg),
    }
}

fn memory_status_to_ingest_status(
    s: cairn_plugin_proto::memory::MemoryIngestStatus,
) -> IngestStatus {
    use cairn_plugin_proto::memory::MemoryIngestStatus as M;
    match s {
        M::Pending => IngestStatus::Pending,
        M::Parsing => IngestStatus::Parsing,
        M::Chunking => IngestStatus::Chunking,
        M::Embedding => IngestStatus::Embedding,
        M::Indexing => IngestStatus::Indexing,
        M::Completed => IngestStatus::Completed,
        M::Failed => IngestStatus::Failed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::SourceType;
    use crate::multi_provider::{ProviderResolverError, CAIRN_DEFAULT_PROVIDER_REF};
    use crate::retrieval::{CandidateStage, RerankerStrategy, RetrievalDiagnostics, RetrievalMode};
    use cairn_domain::{ChunkId, ProviderRef, SourceId};
    use cairn_plugin_proto::knowledge::{RetrievalModeWire, ScoringBreakdownWire, SourceTypeWire};
    use cairn_plugin_proto::memory::{
        MemoryChunkRecordWire, MemoryIngestStatus, MemoryQueryDiagnostics,
        MemoryRetrievalResultWire,
    };
    use std::sync::Mutex;

    fn proj() -> ProjectKey {
        ProjectKey::new("t", "w", "p")
    }

    fn sample_query() -> RetrievalQuery {
        RetrievalQuery {
            project: proj(),
            query_text: "q".to_owned(),
            mode: RetrievalMode::Hybrid,
            reranker: RerankerStrategy::None,
            limit: 5,
            metadata_filters: vec![],
            scoring_policy: None,
        }
    }

    fn sample_memory_wire_result() -> MemoryQueryResult {
        MemoryQueryResult {
            results: vec![MemoryRetrievalResultWire {
                chunk: MemoryChunkRecordWire {
                    chunk_id: ChunkId::new("m1"),
                    document_id: DocumentId::new("mem-42"),
                    source_id: SourceId::new("session:abc"),
                    source_type: SourceTypeWire::PlainText,
                    project: proj(),
                    text: "alice prefers oolong".to_owned(),
                    position: 0,
                    created_at: 1_700_000_000_000,
                    updated_at: None,
                    provenance_metadata: None,
                    credibility_score: None,
                    graph_linkage: None,
                    content_hash: None,
                    entities: vec![],
                },
                score: 0.91,
                breakdown: ScoringBreakdownWire {
                    semantic_relevance: Some(0.91),
                    ..Default::default()
                },
            }],
            diagnostics: MemoryQueryDiagnostics {
                mode_used: RetrievalModeWire::VectorOnly,
                stages_used: Some(vec!["vector".to_owned()]),
                reranker_used: None,
                scoring_dimensions_used: vec!["semantic_relevance".to_owned()],
                results_returned: 1,
                latency_ms: Some(8),
            },
        }
    }

    struct FixedResolver(ProviderRef);

    #[async_trait]
    impl ProviderResolver for FixedResolver {
        async fn resolve(
            &self,
            _project: &ProjectKey,
        ) -> Result<ProviderRef, ProviderResolverError> {
            Ok(self.0.clone())
        }
    }

    struct DefaultOnlyRetrieval {
        calls: Mutex<usize>,
    }

    #[async_trait]
    impl RetrievalService for DefaultOnlyRetrieval {
        async fn query(&self, _q: RetrievalQuery) -> Result<RetrievalResponse, RetrievalError> {
            *self.calls.lock().unwrap() += 1;
            Ok(RetrievalResponse {
                results: vec![],
                diagnostics: RetrievalDiagnostics {
                    mode_used: RetrievalMode::Hybrid,
                    reranker_used: RerankerStrategy::None,
                    candidates_generated: 0,
                    results_returned: 0,
                    latency_ms: 0,
                    stages_used: vec![CandidateStage::Lexical],
                    scoring_dimensions_used: vec![],
                    effective_policy: None,
                    family: None,
                },
            })
        }
    }

    struct DefaultOnlyIngest {
        submit_calls: Mutex<usize>,
    }

    #[async_trait]
    impl IngestService for DefaultOnlyIngest {
        async fn submit(&self, _r: IngestRequest) -> Result<(), IngestError> {
            *self.submit_calls.lock().unwrap() += 1;
            Ok(())
        }
        async fn submit_pack(&self, _r: IngestPackRequest) -> Result<(), IngestError> {
            Ok(())
        }
        async fn status(&self, _d: &DocumentId) -> Result<Option<IngestStatus>, IngestError> {
            Ok(None)
        }
    }

    #[derive(Default)]
    struct RecordingMemoryDispatcher {
        query_calls: Mutex<Vec<(String, MemoryQueryParams)>>,
        ingest_calls: Mutex<Vec<(String, MemoryIngestParams)>>,
        status_calls: Mutex<Vec<(String, MemoryIngestStatusParams)>>,
        next_query_result: Mutex<Option<MemoryQueryResult>>,
        next_ingest_ack: Mutex<Option<MemoryIngestAck>>,
        next_status_result: Mutex<Option<MemoryIngestStatusResult>>,
        fail_unavailable: Mutex<Option<String>>,
    }

    #[async_trait]
    impl MemoryPluginDispatcher for RecordingMemoryDispatcher {
        async fn query(
            &self,
            plugin_id: &str,
            params: MemoryQueryParams,
        ) -> Result<MemoryQueryResult, MemoryPluginError> {
            if let Some(reason) = self.fail_unavailable.lock().unwrap().clone() {
                return Err(MemoryPluginError::Unavailable(reason));
            }
            self.query_calls
                .lock()
                .unwrap()
                .push((plugin_id.to_owned(), params));
            Ok(self
                .next_query_result
                .lock()
                .unwrap()
                .clone()
                .expect("next_query_result must be set"))
        }

        async fn ingest(
            &self,
            plugin_id: &str,
            params: MemoryIngestParams,
        ) -> Result<MemoryIngestAck, MemoryPluginError> {
            if let Some(reason) = self.fail_unavailable.lock().unwrap().clone() {
                return Err(MemoryPluginError::Unavailable(reason));
            }
            self.ingest_calls
                .lock()
                .unwrap()
                .push((plugin_id.to_owned(), params));
            Ok(self
                .next_ingest_ack
                .lock()
                .unwrap()
                .clone()
                .expect("next_ingest_ack must be set"))
        }

        async fn ingest_status(
            &self,
            plugin_id: &str,
            params: MemoryIngestStatusParams,
        ) -> Result<MemoryIngestStatusResult, MemoryPluginError> {
            if let Some(reason) = self.fail_unavailable.lock().unwrap().clone() {
                return Err(MemoryPluginError::Unavailable(reason));
            }
            self.status_calls
                .lock()
                .unwrap()
                .push((plugin_id.to_owned(), params));
            Ok(self
                .next_status_result
                .lock()
                .unwrap()
                .clone()
                .expect("next_status_result must be set"))
        }
    }

    #[tokio::test]
    async fn cairn_default_memory_routes_to_in_process() {
        let default = DefaultOnlyRetrieval {
            calls: Mutex::new(0),
        };
        let resolver = FixedResolver(ProviderRef::new(CAIRN_DEFAULT_PROVIDER_REF));
        let dispatcher = RecordingMemoryDispatcher::default();
        let mp = MultiProviderMemory::new(std::sync::Arc::new(default), resolver, dispatcher);
        mp.query(sample_query()).await.unwrap();
        assert_eq!(*mp.default.calls.lock().unwrap(), 1);
        assert!(mp.dispatcher.query_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn memory_plugin_route_dispatches_and_lifts_response() {
        let default = DefaultOnlyRetrieval {
            calls: Mutex::new(0),
        };
        let resolver = FixedResolver(ProviderRef::new("plugin:mem0"));
        let dispatcher = RecordingMemoryDispatcher::default();
        *dispatcher.next_query_result.lock().unwrap() = Some(sample_memory_wire_result());
        let mp = MultiProviderMemory::new(std::sync::Arc::new(default), resolver, dispatcher);
        let resp = mp.query(sample_query()).await.unwrap();
        assert_eq!(resp.results.len(), 1);
        assert_eq!(*mp.default.calls.lock().unwrap(), 0);
        let calls = mp.dispatcher.query_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "mem0");
    }

    #[tokio::test]
    async fn memory_plugin_unavailable_surfaces_provider_unavailable() {
        let default = DefaultOnlyRetrieval {
            calls: Mutex::new(0),
        };
        let resolver = FixedResolver(ProviderRef::new("plugin:mem0"));
        let dispatcher = RecordingMemoryDispatcher::default();
        *dispatcher.fail_unavailable.lock().unwrap() = Some("handshake timeout".to_owned());
        let mp = MultiProviderMemory::new(std::sync::Arc::new(default), resolver, dispatcher);
        match mp.query(sample_query()).await {
            Err(RetrievalError::ProviderUnavailable { provider, reason }) => {
                assert_eq!(provider, "plugin:mem0");
                assert_eq!(reason, "handshake timeout");
            }
            other => panic!("expected ProviderUnavailable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn memory_unknown_provider_ref_errors() {
        let default = DefaultOnlyRetrieval {
            calls: Mutex::new(0),
        };
        let resolver = FixedResolver(ProviderRef::new("bedrock-kb"));
        let dispatcher = RecordingMemoryDispatcher::default();
        let mp = MultiProviderMemory::new(std::sync::Arc::new(default), resolver, dispatcher);
        assert!(matches!(
            mp.query(sample_query()).await,
            Err(RetrievalError::ProviderUnavailable { .. })
        ));
    }

    fn sample_ingest_request() -> IngestRequest {
        IngestRequest {
            document_id: DocumentId::new("mem-42"),
            source_id: SourceId::new("session:abc"),
            source_type: SourceType::PlainText,
            project: proj(),
            content: "alice prefers oolong".to_owned(),
            import_id: None,
            corpus_id: None,
            bundle_source_id: None,
            tags: vec![],
        }
    }

    #[tokio::test]
    async fn memory_ingest_cairn_default_routes_to_in_process() {
        let default = DefaultOnlyIngest {
            submit_calls: Mutex::new(0),
        };
        let resolver = FixedResolver(ProviderRef::new(CAIRN_DEFAULT_PROVIDER_REF));
        let dispatcher = RecordingMemoryDispatcher::default();
        let mp = MultiProviderMemoryIngest::new(std::sync::Arc::new(default), resolver, dispatcher);
        mp.submit(sample_ingest_request()).await.unwrap();
        assert_eq!(*mp.default.submit_calls.lock().unwrap(), 1);
        assert!(mp.dispatcher.ingest_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn memory_ingest_plugin_accepted() {
        let default = DefaultOnlyIngest {
            submit_calls: Mutex::new(0),
        };
        let resolver = FixedResolver(ProviderRef::new("plugin:mem0"));
        let dispatcher = RecordingMemoryDispatcher::default();
        *dispatcher.next_ingest_ack.lock().unwrap() = Some(MemoryIngestAck {
            document_id: DocumentId::new("mem-42"),
            accepted: true,
            reason: None,
        });
        let mp = MultiProviderMemoryIngest::new(std::sync::Arc::new(default), resolver, dispatcher);
        mp.submit(sample_ingest_request()).await.unwrap();
        assert_eq!(mp.dispatcher.ingest_calls.lock().unwrap().len(), 1);
        assert_eq!(*mp.default.submit_calls.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn memory_ingest_plugin_rejected_surfaces_provider_rejected() {
        // Typical mem0 response when the adapter declares auto_extract:
        // explicit memory.ingest returns accepted=false with a
        // "memory_store suppressed" reason.
        let default = DefaultOnlyIngest {
            submit_calls: Mutex::new(0),
        };
        let resolver = FixedResolver(ProviderRef::new("plugin:mem0"));
        let dispatcher = RecordingMemoryDispatcher::default();
        *dispatcher.next_ingest_ack.lock().unwrap() = Some(MemoryIngestAck {
            document_id: DocumentId::new("mem-42"),
            accepted: false,
            reason: Some("auto_extract provider".to_owned()),
        });
        let mp = MultiProviderMemoryIngest::new(std::sync::Arc::new(default), resolver, dispatcher);
        match mp.submit(sample_ingest_request()).await {
            Err(IngestError::ProviderRejected { provider, reason }) => {
                assert_eq!(provider, "plugin:mem0");
                assert_eq!(reason, "auto_extract provider");
            }
            other => panic!("expected ProviderRejected, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn memory_plugin_ingest_status_routes_through_dispatcher() {
        let default = DefaultOnlyIngest {
            submit_calls: Mutex::new(0),
        };
        let resolver = FixedResolver(ProviderRef::new("plugin:mem0"));
        let dispatcher = RecordingMemoryDispatcher::default();
        *dispatcher.next_status_result.lock().unwrap() = Some(MemoryIngestStatusResult {
            status: Some(MemoryIngestStatus::Completed),
        });
        let mp = MultiProviderMemoryIngest::new(std::sync::Arc::new(default), resolver, dispatcher);
        let out = mp
            .plugin_ingest_status(&proj(), &DocumentId::new("mem-42"))
            .await
            .unwrap();
        assert_eq!(out, Some(IngestStatus::Completed));
    }
}
