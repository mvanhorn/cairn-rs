//! Fixtures reused across the suite's individual checks.

use async_trait::async_trait;
use cairn_domain::{ChunkId, KnowledgeDocumentId, ProjectKey, SourceId};
use cairn_memory::multi_provider::{KnowledgePluginDispatcher, KnowledgePluginError};
use cairn_plugin_proto::knowledge::{
    ChunkRecordWire, DimensionSupport, KnowledgeIngestAck, KnowledgeIngestParams,
    KnowledgeIngestStatusParams, KnowledgeIngestStatusResult, KnowledgeProviderCapability,
    KnowledgeQueryDiagnostics, KnowledgeQueryParams, KnowledgeQueryResult, RetrievalModeWire,
    RetrievalResultWire, ScoringBreakdownWire, ScoringDimensionSet, SourceTypeWire,
};
use std::sync::Mutex;

/// Sample project key reused across the suite.
pub fn sample_project() -> ProjectKey {
    ProjectKey::new("t_compliance", "w_compliance", "p_compliance")
}

/// Every-dim-surfaced capability snapshot. Represents the cairn-default
/// shape — a provider that populates every provider-required scoring
/// dimension. Used as the baseline for compliance checks.
pub fn full_capability() -> KnowledgeProviderCapability {
    KnowledgeProviderCapability {
        retrieval_modes: vec![
            RetrievalModeWire::LexicalOnly,
            RetrievalModeWire::VectorOnly,
            RetrievalModeWire::Hybrid,
        ],
        ingest_capable: true,
        ingest_source_types: vec![
            SourceTypeWire::PlainText,
            SourceTypeWire::Markdown,
            SourceTypeWire::Html,
        ],
        scoring_dimensions: ScoringDimensionSet {
            semantic_relevance: DimensionSupport::Surfaced,
            lexical_relevance: DimensionSupport::Surfaced,
            freshness_decay: DimensionSupport::Surfaced,
            staleness_penalty: DimensionSupport::Surfaced,
            recency_of_use: DimensionSupport::Surfaced,
        },
    }
}

/// Read-only / minimal capability snapshot — Bedrock-KB-style. Declares
/// only `semantic_relevance = Surfaced`; every other provider-required
/// dim is `NotSupported`. Used to drive the tri-state check.
pub fn read_only_capability() -> KnowledgeProviderCapability {
    KnowledgeProviderCapability {
        retrieval_modes: vec![RetrievalModeWire::Hybrid],
        ingest_capable: false,
        ingest_source_types: vec![],
        scoring_dimensions: ScoringDimensionSet {
            semantic_relevance: DimensionSupport::Surfaced,
            lexical_relevance: DimensionSupport::NotSupported,
            freshness_decay: DimensionSupport::NotSupported,
            staleness_penalty: DimensionSupport::NotSupported,
            recency_of_use: DimensionSupport::NotSupported,
        },
    }
}

/// A minimal valid `KnowledgeQueryResult` the suite uses as the input
/// for round-trip + field-presence checks. Populates every RFC 029
/// required field; individual checks that need to elide a field build
/// their own variant.
pub fn sample_query_result() -> KnowledgeQueryResult {
    KnowledgeQueryResult {
        results: vec![RetrievalResultWire {
            chunk: ChunkRecordWire {
                chunk_id: ChunkId::new("c1"),
                document_id: KnowledgeDocumentId::new("d1"),
                source_id: SourceId::new("s1"),
                source_type: SourceTypeWire::Markdown,
                project: sample_project(),
                text: "compliance sample".to_owned(),
                position: 0,
                created_at: 1_000,
                updated_at: Some(2_000),
                provenance_metadata: None,
                credibility_score: Some(0.5),
                graph_linkage: None,
                content_hash: Some("hash".to_owned()),
                entities: vec!["acme".to_owned()],
            },
            score: 0.8,
            breakdown: ScoringBreakdownWire {
                semantic_relevance: Some(0.8),
                lexical_relevance: Some(0.4),
                freshness_decay: Some(0.6),
                staleness_penalty: Some(0.0),
                recency_of_use: None,
                // Runtime-owned fields intentionally populated on the
                // wire so the "overwrite" check can observe them being
                // discarded.
                graph_proximity: Some(0.99),
                source_credibility: Some(0.99),
                corroboration: Some(0.99),
            },
        }],
        diagnostics: KnowledgeQueryDiagnostics {
            mode_used: RetrievalModeWire::Hybrid,
            stages_used: Some(vec!["lexical".to_owned(), "vector".to_owned()]),
            reranker_used: Some("mmr".to_owned()),
            scoring_dimensions_used: vec![
                "semantic_relevance".to_owned(),
                "lexical_relevance".to_owned(),
            ],
            results_returned: 1,
            latency_ms: Some(5),
        },
    }
}

/// A minimal in-process mock `KnowledgePluginDispatcher` for the
/// plugin-path fixtures. Records every call + returns canned
/// responses. Tests reset the canned response per case.
#[derive(Default)]
pub struct MockDispatcher {
    pub next_query_result: Mutex<Option<KnowledgeQueryResult>>,
    pub next_ingest_ack: Mutex<Option<KnowledgeIngestAck>>,
    pub next_status: Mutex<Option<KnowledgeIngestStatusResult>>,
    pub query_calls: Mutex<Vec<(String, KnowledgeQueryParams)>>,
    pub ingest_calls: Mutex<Vec<(String, KnowledgeIngestParams)>>,
    pub status_calls: Mutex<Vec<(String, KnowledgeIngestStatusParams)>>,
}

#[async_trait]
impl KnowledgePluginDispatcher for MockDispatcher {
    async fn query(
        &self,
        plugin_id: &str,
        params: KnowledgeQueryParams,
    ) -> Result<KnowledgeQueryResult, KnowledgePluginError> {
        self.query_calls
            .lock()
            .unwrap()
            .push((plugin_id.to_owned(), params));
        self.next_query_result
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| KnowledgePluginError::Internal("no query response staged".into()))
    }

    async fn ingest(
        &self,
        plugin_id: &str,
        params: KnowledgeIngestParams,
    ) -> Result<KnowledgeIngestAck, KnowledgePluginError> {
        self.ingest_calls
            .lock()
            .unwrap()
            .push((plugin_id.to_owned(), params));
        self.next_ingest_ack
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| KnowledgePluginError::Internal("no ingest ack staged".into()))
    }

    async fn ingest_status(
        &self,
        plugin_id: &str,
        params: KnowledgeIngestStatusParams,
    ) -> Result<KnowledgeIngestStatusResult, KnowledgePluginError> {
        self.status_calls
            .lock()
            .unwrap()
            .push((plugin_id.to_owned(), params));
        self.next_status
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| KnowledgePluginError::Internal("no status staged".into()))
    }
}
