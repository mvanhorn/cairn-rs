//! RFC 029 Knowledge-provider wire types.
//!
//! The `KnowledgeProvider` capability is unusual among RFC 007 capability
//! families: its effective detail (retrieval modes, ingest capability,
//! per-dimension scoring support) is negotiated at the `initialize`
//! handshake rather than declared in the manifest, because it legitimately
//! depends on runtime state (credentials, backend reachability) that is not
//! available at manifest-parse time. See RFC 029 §"Capability declaration".
//!
//! The four host-to-plugin calls added by RFC 029 are:
//!
//! | Method | Params | Result |
//! |---|---|---|
//! | `knowledge.query` | [`KnowledgeQueryParams`] | [`KnowledgeQueryResult`] |
//! | `knowledge.ingest` | [`KnowledgeIngestParams`] | [`KnowledgeIngestAck`] |
//! | `knowledge.ingest_status` | [`KnowledgeIngestStatusParams`] | [`KnowledgeIngestStatusResult`] |
//! | `knowledge.list_sources` | [`KnowledgeListSourcesParams`] | [`KnowledgeListSourcesResult`] |
//!
//! Plus the plugin-to-host notification `knowledge.sources.changed` carrying
//! [`KnowledgeSourcesChangedParams`].
//!
//! Every type derives `serde::Serialize` + `serde::Deserialize`. Field names
//! follow camelCase on the wire to match the existing RFC 007 conventions in
//! [`crate::wire`] (see the `#[serde(rename_all = "camelCase")]` directives).

use cairn_domain::{ChunkId, KnowledgeDocumentId, ProjectKey, SourceId};
use serde::{Deserialize, Serialize};

// ─── Capability snapshot negotiated at `initialize` ────────────────────────

/// Layer 2 handshake snapshot for the `knowledge_provider` capability family.
///
/// This is the entry that appears inside `InitializeResult.capabilities[]`
/// when a plugin advertises `type = "knowledge_provider"`. The manifest entry
/// is empty — this struct carries the full effective capability detail.
///
/// Tri-state scoring-dimension declaration is enforced at handshake, not at
/// manifest parse: every provider-required dimension MUST be present in
/// [`scoring_dimensions`] as either `Surfaced` or `NotSupported`. The three
/// runtime-owned dimensions (graph_proximity, source_credibility,
/// corroboration) MUST NOT appear — they are always computed post-hoc by the
/// runtime regardless of provider. Validation of both rules lives in
/// `cairn-tools` (the plugin host).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnowledgeProviderCapability {
    /// Which retrieval modes the provider supports at runtime. Never empty.
    pub retrieval_modes: Vec<RetrievalModeWire>,
    /// Whether the provider can accept `knowledge.ingest` calls. When `false`,
    /// the runtime hides `memory_store` from agent prompts for any project
    /// configured with this provider (RFC 029 §Tool Surface).
    pub ingest_capable: bool,
    /// Source types the provider accepts for ingest. Meaningful only when
    /// [`ingest_capable`] is `true`; otherwise for operator UI display.
    #[serde(default)]
    pub ingest_source_types: Vec<SourceTypeWire>,
    /// Tri-state declaration for each of the 5 provider-required scoring
    /// dimensions. See [`ScoringDimensionSet`] for the fields.
    pub scoring_dimensions: ScoringDimensionSet,
}

/// Tri-state per-dimension declaration. Every field MUST be set explicitly.
///
/// Absence of any field is rejected at handshake — implicit omission is not
/// a valid state. A provider that has no signal for a dimension declares
/// [`DimensionSupport::NotSupported`]; a provider that surfaces the
/// dimension's score in [`ScoringBreakdownWire`] declares
/// [`DimensionSupport::Surfaced`].
///
/// The three runtime-owned dimensions (graph_proximity, source_credibility,
/// corroboration) are NOT represented here — they are computed post-hoc by
/// the runtime on every result regardless of provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScoringDimensionSet {
    pub semantic_relevance: DimensionSupport,
    pub lexical_relevance: DimensionSupport,
    pub freshness_decay: DimensionSupport,
    pub staleness_penalty: DimensionSupport,
    pub recency_of_use: DimensionSupport,
}

/// Whether a provider surfaces a scoring dimension in its query results.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DimensionSupport {
    Surfaced,
    NotSupported,
}

// ─── Retrieval mode + source type on the wire ──────────────────────────────

/// Retrieval mode selection on the wire. Mirrors
/// `cairn_memory::retrieval::RetrievalMode` with matching serde.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetrievalModeWire {
    LexicalOnly,
    VectorOnly,
    Hybrid,
}

/// Supported source type on the wire. Mirrors
/// `cairn_memory::ingest::SourceType` with matching serde.
///
/// RFC 030 collapsed the earlier `JsonStructured` duplicate variant into
/// [`SourceTypeWire::StructuredJson`]; the `"json_structured"` wire alias is
/// still accepted on deserialisation via serde alias for back-compat with
/// messages emitted by pre-RFC-030 adapters.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceTypeWire {
    PlainText,
    Markdown,
    Html,
    #[serde(alias = "json_structured")]
    StructuredJson,
    KnowledgePack,
}

// ─── knowledge.query ───────────────────────────────────────────────────────

/// Host → plugin: retrieve chunks relevant to a query.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnowledgeQueryParams {
    pub project: ProjectKey,
    pub query_text: String,
    pub mode: RetrievalModeWire,
    /// Max results. The plugin MAY return fewer; MUST NOT return more.
    pub limit: u32,
    /// Metadata filters applied before scoring. Empty means no filter.
    #[serde(default)]
    pub metadata_filters: Vec<MetadataFilterWire>,
}

/// Plugin → host: scored retrieval results with provider-surfaced scoring.
///
/// The runtime-owned scoring dimensions (graph_proximity, source_credibility,
/// corroboration) on each result's breakdown are **always overwritten** by
/// the host before the agent sees the response; plugins that populate them
/// have their values discarded. This guards against a buggy or malicious
/// plugin influencing dimensions it is not allowed to compute.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnowledgeQueryResult {
    pub results: Vec<RetrievalResultWire>,
    pub diagnostics: KnowledgeQueryDiagnostics,
}

/// Per-query diagnostics. RFC 003 requires retrieval mode used, candidate
/// stages, contributing dimensions, effective policy, and reranker path; a
/// provider that cannot surface a field returns `"not_surfaced_by_provider"`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnowledgeQueryDiagnostics {
    pub mode_used: RetrievalModeWire,
    /// Candidate-generation stages. May be omitted by providers that cannot
    /// surface internal stage information.
    #[serde(default)]
    pub stages_used: Option<Vec<String>>,
    /// Reranker path. May be omitted.
    #[serde(default)]
    pub reranker_used: Option<String>,
    /// Scoring dimensions that contributed nontrivially to the final score,
    /// by name. Runtime-owned dimensions are appended by the host post-hoc.
    pub scoring_dimensions_used: Vec<String>,
    pub results_returned: u32,
    #[serde(default)]
    pub latency_ms: Option<u64>,
}

/// A single scored result on the wire.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetrievalResultWire {
    pub chunk: ChunkRecordWire,
    pub score: f64,
    pub breakdown: ScoringBreakdownWire,
}

/// Canonical scoring dimensions on the wire (RFC 003).
///
/// All eight fields are present. Provider-required dimensions the provider
/// declared `NotSupported` serialize as `null`. Runtime-owned dimensions
/// (graph_proximity, source_credibility, corroboration) are serialized as
/// `null` by providers and populated by the host post-hoc.
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScoringBreakdownWire {
    #[serde(default)]
    pub semantic_relevance: Option<f64>,
    #[serde(default)]
    pub lexical_relevance: Option<f64>,
    #[serde(default)]
    pub freshness_decay: Option<f64>,
    #[serde(default)]
    pub staleness_penalty: Option<f64>,
    #[serde(default)]
    pub recency_of_use: Option<f64>,
    /// Runtime-owned. Host overwrites any provider-supplied value.
    #[serde(default)]
    pub graph_proximity: Option<f64>,
    /// Runtime-owned.
    #[serde(default)]
    pub source_credibility: Option<f64>,
    /// Runtime-owned.
    #[serde(default)]
    pub corroboration: Option<f64>,
}

/// Metadata filter clause on the wire. Simple key=value equality for v1.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetadataFilterWire {
    pub key: String,
    pub value: String,
}

/// A chunk on the wire. Wire-carried subset of the in-process
/// `cairn_memory::ingest::ChunkRecord` — excludes the embedding vector
/// (not needed across the plugin boundary) and the re-embed bookkeeping
/// flags (runtime-internal).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChunkRecordWire {
    pub chunk_id: ChunkId,
    pub document_id: KnowledgeDocumentId,
    pub source_id: SourceId,
    pub source_type: SourceTypeWire,
    pub project: ProjectKey,
    pub text: String,
    pub position: u32,
    pub created_at: u64,
    #[serde(default)]
    pub updated_at: Option<u64>,
    #[serde(default)]
    pub provenance_metadata: Option<serde_json::Value>,
    #[serde(default)]
    pub credibility_score: Option<f64>,
    #[serde(default)]
    pub graph_linkage: Option<String>,
    #[serde(default)]
    pub content_hash: Option<String>,
    #[serde(default)]
    pub entities: Vec<String>,
}

// ─── knowledge.ingest ──────────────────────────────────────────────────────

/// Host → plugin: submit a document for ingestion.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnowledgeIngestParams {
    pub document_id: KnowledgeDocumentId,
    pub source_id: SourceId,
    pub source_type: SourceTypeWire,
    pub project: ProjectKey,
    pub content: String,
    #[serde(default)]
    pub import_id: Option<String>,
    #[serde(default)]
    pub corpus_id: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
}

/// Plugin → host: ingest acknowledgement. Ingest is async; the final status
/// comes via `knowledge.ingest_status`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnowledgeIngestAck {
    pub document_id: KnowledgeDocumentId,
    pub accepted: bool,
    #[serde(default)]
    pub reason: Option<String>,
}

// ─── knowledge.ingest_status ───────────────────────────────────────────────

/// Host → plugin: query the status of a previously-submitted ingest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnowledgeIngestStatusParams {
    pub document_id: KnowledgeDocumentId,
}

/// Plugin → host: current ingest status or `None` if the document is unknown.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnowledgeIngestStatusResult {
    #[serde(default)]
    pub status: Option<KnowledgeIngestStatus>,
}

/// Ingest status on the wire. Mirrors `cairn_memory::ingest::IngestStatus`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeIngestStatus {
    Pending,
    Parsing,
    Chunking,
    Embedding,
    Indexing,
    Completed,
    Failed,
}

// ─── knowledge.list_sources ────────────────────────────────────────────────

/// Host → plugin: enumerate corpora/indices the provider currently serves.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnowledgeListSourcesParams {
    /// Per-project scoping — the provider may serve different source sets per
    /// project. When `None`, the provider returns its full tenant-visible
    /// source set.
    #[serde(default)]
    pub project: Option<ProjectKey>,
}

/// Plugin → host: current-reality source list. Not static; may change across
/// calls as the backend changes. Layer 3 enumeration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnowledgeListSourcesResult {
    pub sources: Vec<KnowledgeSource>,
}

/// Metadata for one corpus/index the provider makes available.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnowledgeSource {
    /// Provider-scoped source id. Opaque to cairn; the plugin round-trips it
    /// in subsequent queries if needed.
    pub source_id: String,
    pub display_name: String,
    /// Hint only; MAY be stale by the time cairn consumes it.
    #[serde(default)]
    pub estimated_chunks: Option<u64>,
    /// Optional free-form description shown in operator UI.
    #[serde(default)]
    pub description: Option<String>,
}

// ─── knowledge.sources.changed notification ────────────────────────────────

/// Plugin → host notification body. Carried inside `event.emit` with
/// `type = "knowledge.sources.changed"`. Receipt invalidates cairn's cached
/// source list for `provider_id`; cairn refetches via
/// `knowledge.list_sources` on next UI refresh.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnowledgeSourcesChangedParams {
    pub provider_id: String,
    #[serde(default)]
    pub project: Option<ProjectKey>,
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_domain::{ProjectId, TenantId, WorkspaceId};

    fn sample_project() -> ProjectKey {
        ProjectKey {
            tenant_id: TenantId::new("t1"),
            workspace_id: WorkspaceId::new("w1"),
            project_id: ProjectId::new("p1"),
        }
    }

    fn sample_scoring_dimensions() -> ScoringDimensionSet {
        ScoringDimensionSet {
            semantic_relevance: DimensionSupport::Surfaced,
            lexical_relevance: DimensionSupport::Surfaced,
            freshness_decay: DimensionSupport::NotSupported,
            staleness_penalty: DimensionSupport::NotSupported,
            recency_of_use: DimensionSupport::NotSupported,
        }
    }

    #[test]
    fn capability_roundtrips() {
        let cap = KnowledgeProviderCapability {
            retrieval_modes: vec![RetrievalModeWire::Hybrid, RetrievalModeWire::VectorOnly],
            ingest_capable: false,
            ingest_source_types: vec![],
            scoring_dimensions: sample_scoring_dimensions(),
        };
        let json = serde_json::to_value(&cap).unwrap();
        // camelCase field names
        assert_eq!(json["ingestCapable"], false);
        assert!(json["retrievalModes"].is_array());
        assert_eq!(json["scoringDimensions"]["semanticRelevance"], "surfaced");
        assert_eq!(json["scoringDimensions"]["freshnessDecay"], "not_supported");

        let back: KnowledgeProviderCapability = serde_json::from_value(json).unwrap();
        assert_eq!(back, cap);
    }

    #[test]
    fn capability_rejects_missing_dimension_field() {
        // A manifest that leaves out freshness_decay must fail to deserialize.
        let bad = serde_json::json!({
            "retrievalModes": ["hybrid"],
            "ingestCapable": false,
            "scoringDimensions": {
                "semanticRelevance": "surfaced",
                "lexicalRelevance": "surfaced",
                "stalenessPenalty": "not_supported",
                "recencyOfUse": "not_supported"
                // freshnessDecay intentionally missing
            }
        });
        let parsed: Result<KnowledgeProviderCapability, _> = serde_json::from_value(bad);
        assert!(parsed.is_err(), "missing dimension field must reject");
    }

    #[test]
    fn query_params_roundtrip() {
        let p = KnowledgeQueryParams {
            project: sample_project(),
            query_text: "rehash cursor direction".to_owned(),
            mode: RetrievalModeWire::Hybrid,
            limit: 5,
            metadata_filters: vec![MetadataFilterWire {
                key: "subsystem".to_owned(),
                value: "hashtable".to_owned(),
            }],
        };
        let json = serde_json::to_value(&p).unwrap();
        assert_eq!(json["queryText"], "rehash cursor direction");
        assert_eq!(json["mode"], "hybrid");
        assert_eq!(json["metadataFilters"][0]["key"], "subsystem");
        let back: KnowledgeQueryParams = serde_json::from_value(json).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn query_result_runtime_owned_dimensions_serialize_as_null_when_absent() {
        // A provider-emitted result: runtime-owned dimensions untouched by
        // provider (null), plus surfaced dimensions present.
        let r = KnowledgeQueryResult {
            results: vec![RetrievalResultWire {
                chunk: ChunkRecordWire {
                    chunk_id: ChunkId::new("c1"),
                    document_id: KnowledgeDocumentId::new("d1"),
                    source_id: SourceId::new("s1"),
                    source_type: SourceTypeWire::Markdown,
                    project: sample_project(),
                    text: "…".to_owned(),
                    position: 0,
                    created_at: 1_700_000_000_000,
                    updated_at: None,
                    provenance_metadata: None,
                    credibility_score: None,
                    graph_linkage: None,
                    content_hash: None,
                    entities: vec![],
                },
                score: 0.82,
                breakdown: ScoringBreakdownWire {
                    semantic_relevance: Some(0.9),
                    lexical_relevance: Some(0.7),
                    freshness_decay: None,
                    staleness_penalty: None,
                    recency_of_use: None,
                    // Provider MUST leave these null. Compliance-suite check
                    // in a later PR validates the runtime enforces this.
                    graph_proximity: None,
                    source_credibility: None,
                    corroboration: None,
                },
            }],
            diagnostics: KnowledgeQueryDiagnostics {
                mode_used: RetrievalModeWire::Hybrid,
                stages_used: None,
                reranker_used: None,
                scoring_dimensions_used: vec![
                    "semantic_relevance".to_owned(),
                    "lexical_relevance".to_owned(),
                ],
                results_returned: 1,
                latency_ms: Some(23),
            },
        };
        let json = serde_json::to_value(&r).unwrap();
        let b = &json["results"][0]["breakdown"];
        assert_eq!(b["semanticRelevance"], 0.9);
        assert!(b["graphProximity"].is_null());
        assert!(b["sourceCredibility"].is_null());
        assert!(b["corroboration"].is_null());

        let back: KnowledgeQueryResult = serde_json::from_value(json).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn ingest_params_roundtrip() {
        let p = KnowledgeIngestParams {
            document_id: KnowledgeDocumentId::new("d2"),
            source_id: SourceId::new("s2"),
            source_type: SourceTypeWire::PlainText,
            project: sample_project(),
            content: "hello".to_owned(),
            import_id: Some("imp-1".to_owned()),
            corpus_id: None,
            tags: vec!["valkey".to_owned()],
        };
        let json = serde_json::to_value(&p).unwrap();
        assert_eq!(json["documentId"], "d2");
        assert_eq!(json["sourceType"], "plain_text");
        let back: KnowledgeIngestParams = serde_json::from_value(json).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn ingest_ack_roundtrip() {
        let a = KnowledgeIngestAck {
            document_id: KnowledgeDocumentId::new("d3"),
            accepted: false,
            reason: Some("read-only provider".to_owned()),
        };
        let json = serde_json::to_value(&a).unwrap();
        assert_eq!(json["accepted"], false);
        let back: KnowledgeIngestAck = serde_json::from_value(json).unwrap();
        assert_eq!(back, a);
    }

    #[test]
    fn ingest_status_roundtrip() {
        let r = KnowledgeIngestStatusResult {
            status: Some(KnowledgeIngestStatus::Embedding),
        };
        let json = serde_json::to_value(&r).unwrap();
        assert_eq!(json["status"], "embedding");
        let back: KnowledgeIngestStatusResult = serde_json::from_value(json).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn list_sources_roundtrip() {
        let r = KnowledgeListSourcesResult {
            sources: vec![KnowledgeSource {
                source_id: "bedrock-kb:ABC".to_owned(),
                display_name: "valkey corpus".to_owned(),
                estimated_chunks: Some(1553),
                description: None,
            }],
        };
        let json = serde_json::to_value(&r).unwrap();
        assert_eq!(json["sources"][0]["displayName"], "valkey corpus");
        assert_eq!(json["sources"][0]["estimatedChunks"], 1553);
        let back: KnowledgeListSourcesResult = serde_json::from_value(json).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn sources_changed_notification_roundtrip() {
        let p = KnowledgeSourcesChangedParams {
            provider_id: "plugin:bedrock-kb".to_owned(),
            project: Some(sample_project()),
        };
        let json = serde_json::to_value(&p).unwrap();
        assert_eq!(json["providerId"], "plugin:bedrock-kb");
        let back: KnowledgeSourcesChangedParams = serde_json::from_value(json).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn dimension_support_serializes_as_snake_case() {
        let s = DimensionSupport::NotSupported;
        let json = serde_json::to_value(s).unwrap();
        assert_eq!(json, "not_supported");
    }
}
