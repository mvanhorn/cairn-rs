//! RFC 030 Memory-provider wire types.
//!
//! The `MemoryProvider` capability family was split from the overloaded
//! RFC 029 `KnowledgeProvider` family after RFC 030 identified two distinct
//! domain models: **memory** (episodic, agent-written, turn-by-turn) and
//! **knowledge** (curated, operator-ingested, corpus-scale). The two surfaces
//! share most of their shape but differ in:
//!
//! - **`auto_extract: bool`** on [`MemoryProviderCapability`] — memory
//!   backends like mem0 extract facts from conversation turns themselves
//!   (post-turn hook style); the agent does not call `memory_store`
//!   explicitly. When `auto_extract = true`, the runtime suppresses
//!   `memory_store` from the tool surface.
//! - Canonical method names: `memory.query`, `memory.ingest`,
//!   `memory.ingest_status`, `memory.list_sources`. (Mirror of the
//!   `knowledge.*` suite — the two methods do not alias on the wire.)
//! - Source types are typically episodic (`plain_text` or `structured_json`
//!   for tool-call traces), not corpus artefacts.
//!
//! The wire types mirror [`crate::knowledge`] intentionally: same field
//! names + shapes so the host-side dispatcher code can be generic over the
//! two families. The separation lives at the type level so that a
//! handshake-time validator can reject plugins that try to straddle both
//! families (see [`crate::capabilities::CapabilityFamily::is_provider_family`]).
//!
//! The four host-to-plugin calls added by RFC 030 are:
//!
//! | Method | Params | Result |
//! |---|---|---|
//! | `memory.query` | [`MemoryQueryParams`] | [`MemoryQueryResult`] |
//! | `memory.ingest` | [`MemoryIngestParams`] | [`MemoryIngestAck`] |
//! | `memory.ingest_status` | [`MemoryIngestStatusParams`] | [`MemoryIngestStatusResult`] |
//! | `memory.list_sources` | [`MemoryListSourcesParams`] | [`MemoryListSourcesResult`] |
//!
//! Plus the plugin-to-host notification `memory.sources.changed` carrying
//! [`MemorySourcesChangedParams`].

use cairn_domain::{ChunkId, DocumentId, ProjectKey, SourceId};
use serde::{Deserialize, Serialize};

use crate::knowledge::{
    MetadataFilterWire, RetrievalModeWire, ScoringBreakdownWire, ScoringDimensionSet,
    SourceTypeWire,
};

// ─── Capability snapshot negotiated at `initialize` ────────────────────────

/// Layer 2 handshake snapshot for the `memory_provider` capability family.
///
/// Mirrors [`crate::knowledge::KnowledgeProviderCapability`] with one addition:
/// [`auto_extract`](Self::auto_extract). When `true`, the provider extracts
/// memories from conversation turns itself (mem0's post-turn hook style) and
/// the runtime suppresses `memory_store` from the tool surface.
///
/// Tri-state scoring-dimension declaration is enforced at handshake, not at
/// manifest parse: every provider-required dimension MUST be present in
/// [`scoring_dimensions`](Self::scoring_dimensions) as either `Surfaced` or
/// `NotSupported`. The three runtime-owned dimensions (graph_proximity,
/// source_credibility, corroboration) MUST NOT appear — they are always
/// computed post-hoc by the runtime regardless of provider.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryProviderCapability {
    /// Which retrieval modes the provider supports at runtime. Never empty.
    pub retrieval_modes: Vec<RetrievalModeWire>,
    /// Whether the provider can accept `memory.ingest` calls from the agent
    /// (i.e. the `memory_store` tool path). See also
    /// [`auto_extract`](Self::auto_extract): an auto-extract provider may
    /// still set `ingest_capable = true` if it accepts explicit stores as a
    /// supplement, but the tool-surface suppression is driven by
    /// `auto_extract`, not by this flag.
    pub ingest_capable: bool,
    /// Source types the provider accepts for ingest. Meaningful only when
    /// [`ingest_capable`](Self::ingest_capable) is `true`; otherwise for
    /// operator UI display.
    #[serde(default)]
    pub ingest_source_types: Vec<SourceTypeWire>,
    /// Whether the provider auto-extracts memories from conversation turns
    /// (mem0's post-turn hook style). When `true`, the runtime suppresses
    /// `memory_store` from the tool surface — the agent does not call the
    /// provider explicitly. The provider still accepts `memory.query` calls
    /// from the runtime on the agent's behalf.
    pub auto_extract: bool,
    /// Tri-state declaration for each of the 5 provider-required scoring
    /// dimensions. See [`ScoringDimensionSet`] for the fields.
    pub scoring_dimensions: ScoringDimensionSet,
}

// ─── memory.query ──────────────────────────────────────────────────────────

/// Host → plugin: retrieve memory chunks relevant to a query.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryQueryParams {
    pub project: ProjectKey,
    pub query_text: String,
    pub mode: RetrievalModeWire,
    /// Max results. The plugin MAY return fewer; MUST NOT return more.
    pub limit: u32,
    /// Metadata filters applied before scoring. Empty means no filter.
    #[serde(default)]
    pub metadata_filters: Vec<MetadataFilterWire>,
}

/// Plugin → host: scored memory retrieval results with provider-surfaced
/// scoring. Runtime-owned dimensions on each result's breakdown
/// (`graph_proximity`, `source_credibility`, `corroboration`) are
/// **always overwritten** by the host before the agent sees the response;
/// plugins that populate them have their values discarded.
///
/// RFC 030 PR-F note: the memory-family post-hoc rescorer skips
/// `multi_neighbors` (memory is episodic — there is no graph to traverse),
/// so `graph_proximity` stays at its default zero for memory-family results.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryQueryResult {
    pub results: Vec<MemoryRetrievalResultWire>,
    pub diagnostics: MemoryQueryDiagnostics,
}

/// Per-query diagnostics. Mirrors the knowledge-family shape; the host
/// overwrites the `family` field on the return path with
/// `CapabilityFamily::MemoryProvider.as_str()` so downstream observers (eval
/// scorers, audit) can filter by family without trusting plugin output.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryQueryDiagnostics {
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

/// A single scored memory result on the wire.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryRetrievalResultWire {
    pub chunk: MemoryChunkRecordWire,
    pub score: f64,
    pub breakdown: ScoringBreakdownWire,
}

/// A memory chunk on the wire. Wire-carried subset of the in-process
/// `cairn_memory::ingest::ChunkRecord`, sharing the shape of
/// `ChunkRecordWire` but using the family-neutral [`DocumentId`] so
/// that memory and knowledge flows share a single record type in the
/// hot path. The structural twin of [`crate::knowledge::ChunkRecordWire`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryChunkRecordWire {
    pub chunk_id: ChunkId,
    pub document_id: DocumentId,
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

// ─── memory.ingest ─────────────────────────────────────────────────────────

/// Host → plugin: submit a memory for ingestion.
///
/// Only called when the provider's [`MemoryProviderCapability::auto_extract`]
/// is `false`. Auto-extract providers (mem0) receive memory through the
/// `post_turn_hook` protocol, not this method.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryIngestParams {
    pub document_id: DocumentId,
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

/// Plugin → host: memory-ingest acknowledgement. Ingest is async; the final
/// status comes via `memory.ingest_status`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryIngestAck {
    pub document_id: DocumentId,
    pub accepted: bool,
    #[serde(default)]
    pub reason: Option<String>,
}

// ─── memory.ingest_status ──────────────────────────────────────────────────

/// Host → plugin: query the status of a previously-submitted memory ingest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryIngestStatusParams {
    pub document_id: DocumentId,
}

/// Plugin → host: current memory-ingest status or `None` if the document is
/// unknown.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryIngestStatusResult {
    #[serde(default)]
    pub status: Option<MemoryIngestStatus>,
}

/// Memory-ingest status on the wire. Structural twin of
/// [`crate::knowledge::KnowledgeIngestStatus`]; kept as a distinct type so
/// code that routes on [`crate::capabilities::CapabilityFamily`] cannot
/// accidentally feed a knowledge status into a memory path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryIngestStatus {
    Pending,
    Parsing,
    Chunking,
    Embedding,
    Indexing,
    Completed,
    Failed,
}

// ─── memory.list_sources ───────────────────────────────────────────────────

/// Host → plugin: enumerate memory sources the provider currently serves.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryListSourcesParams {
    /// Per-project scoping — the provider may serve different memory sets
    /// per project. When `None`, the provider returns its full
    /// tenant-visible memory source set.
    #[serde(default)]
    pub project: Option<ProjectKey>,
}

/// Plugin → host: current-reality memory-source list. Not static; may change
/// across calls as the backend changes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryListSourcesResult {
    pub sources: Vec<MemorySource>,
}

/// Metadata for one memory source the provider makes available. Structural
/// twin of [`crate::knowledge::KnowledgeSource`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemorySource {
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

// ─── memory.sources.changed notification ───────────────────────────────────

/// Plugin → host notification body. Carried inside `event.emit` with
/// `type = "memory.sources.changed"`. Receipt invalidates cairn's cached
/// memory-source list for `provider_id`; cairn refetches via
/// `memory.list_sources` on next UI refresh.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemorySourcesChangedParams {
    pub provider_id: String,
    #[serde(default)]
    pub project: Option<ProjectKey>,
}

// ─── Canonical method names ────────────────────────────────────────────────

/// Canonical JSON-RPC method names for the `memory_provider` family.
///
/// These are locked by RFC 030; plugins must answer to these exact strings.
/// Structural twin of the `knowledge.*` suite used by RFC 029.
pub mod methods {
    pub const MEMORY_QUERY: &str = "memory.query";
    pub const MEMORY_INGEST: &str = "memory.ingest";
    pub const MEMORY_INGEST_STATUS: &str = "memory.ingest_status";
    pub const MEMORY_LIST_SOURCES: &str = "memory.list_sources";
    pub const MEMORY_SOURCES_CHANGED: &str = "memory.sources.changed";
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::knowledge::DimensionSupport;
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
            lexical_relevance: DimensionSupport::NotSupported,
            freshness_decay: DimensionSupport::Surfaced,
            staleness_penalty: DimensionSupport::NotSupported,
            recency_of_use: DimensionSupport::Surfaced,
        }
    }

    #[test]
    fn capability_roundtrips_with_auto_extract_true() {
        let cap = MemoryProviderCapability {
            retrieval_modes: vec![RetrievalModeWire::VectorOnly],
            ingest_capable: false,
            ingest_source_types: vec![],
            auto_extract: true,
            scoring_dimensions: sample_scoring_dimensions(),
        };
        let json = serde_json::to_value(&cap).unwrap();
        assert_eq!(json["autoExtract"], true);
        assert_eq!(json["ingestCapable"], false);
        assert_eq!(json["scoringDimensions"]["freshnessDecay"], "surfaced");
        assert_eq!(json["scoringDimensions"]["recencyOfUse"], "surfaced");
        let back: MemoryProviderCapability = serde_json::from_value(json).unwrap();
        assert_eq!(back, cap);
    }

    #[test]
    fn capability_rejects_missing_auto_extract_field() {
        // `auto_extract` has no default — a plugin that leaves it out must
        // fail deserialisation. The runtime needs a deliberate declaration
        // to decide whether to suppress the `memory_store` tool.
        let bad = serde_json::json!({
            "retrievalModes": ["vector_only"],
            "ingestCapable": true,
            "ingestSourceTypes": ["plain_text"],
            "scoringDimensions": {
                "semanticRelevance": "surfaced",
                "lexicalRelevance": "not_supported",
                "freshnessDecay": "surfaced",
                "stalenessPenalty": "not_supported",
                "recencyOfUse": "surfaced"
            }
            // autoExtract intentionally missing
        });
        let parsed: Result<MemoryProviderCapability, _> = serde_json::from_value(bad);
        assert!(parsed.is_err(), "missing autoExtract field must reject");
    }

    #[test]
    fn query_params_roundtrip() {
        let p = MemoryQueryParams {
            project: sample_project(),
            query_text: "what did alice say about tea?".to_owned(),
            mode: RetrievalModeWire::VectorOnly,
            limit: 5,
            metadata_filters: vec![],
        };
        let json = serde_json::to_value(&p).unwrap();
        assert_eq!(json["queryText"], "what did alice say about tea?");
        assert_eq!(json["mode"], "vector_only");
        let back: MemoryQueryParams = serde_json::from_value(json).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn query_result_runtime_owned_dimensions_serialise_null_for_memory() {
        let r = MemoryQueryResult {
            results: vec![MemoryRetrievalResultWire {
                chunk: MemoryChunkRecordWire {
                    chunk_id: ChunkId::new("m1"),
                    document_id: DocumentId::new("mem-turn-42"),
                    source_id: SourceId::new("session:abc"),
                    source_type: SourceTypeWire::PlainText,
                    project: sample_project(),
                    text: "alice said she wanted oolong".to_owned(),
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
                    lexical_relevance: None,
                    freshness_decay: None,
                    staleness_penalty: None,
                    recency_of_use: None,
                    graph_proximity: None,
                    source_credibility: None,
                    corroboration: None,
                },
            }],
            diagnostics: MemoryQueryDiagnostics {
                mode_used: RetrievalModeWire::VectorOnly,
                stages_used: None,
                reranker_used: None,
                scoring_dimensions_used: vec!["semantic_relevance".to_owned()],
                results_returned: 1,
                latency_ms: Some(8),
            },
        };
        let json = serde_json::to_value(&r).unwrap();
        let b = &json["results"][0]["breakdown"];
        assert_eq!(b["semanticRelevance"], 0.91);
        assert!(b["graphProximity"].is_null());
        assert!(b["sourceCredibility"].is_null());
        assert!(b["corroboration"].is_null());
        let back: MemoryQueryResult = serde_json::from_value(json).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn ingest_params_roundtrip() {
        let p = MemoryIngestParams {
            document_id: DocumentId::new("mem-42"),
            source_id: SourceId::new("session:abc"),
            source_type: SourceTypeWire::PlainText,
            project: sample_project(),
            content: "user prefers oolong".to_owned(),
            import_id: None,
            corpus_id: None,
            tags: vec!["preference".to_owned()],
        };
        let json = serde_json::to_value(&p).unwrap();
        assert_eq!(json["documentId"], "mem-42");
        assert_eq!(json["sourceType"], "plain_text");
        let back: MemoryIngestParams = serde_json::from_value(json).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn ingest_ack_roundtrip() {
        let a = MemoryIngestAck {
            document_id: DocumentId::new("mem-42"),
            accepted: false,
            reason: Some("auto-extract provider — memory_store suppressed".to_owned()),
        };
        let json = serde_json::to_value(&a).unwrap();
        assert_eq!(json["accepted"], false);
        let back: MemoryIngestAck = serde_json::from_value(json).unwrap();
        assert_eq!(back, a);
    }

    #[test]
    fn ingest_status_roundtrip() {
        let r = MemoryIngestStatusResult {
            status: Some(MemoryIngestStatus::Completed),
        };
        let json = serde_json::to_value(&r).unwrap();
        assert_eq!(json["status"], "completed");
        let back: MemoryIngestStatusResult = serde_json::from_value(json).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn list_sources_roundtrip() {
        let r = MemoryListSourcesResult {
            sources: vec![MemorySource {
                source_id: "mem0:project-42".to_owned(),
                display_name: "Session memory".to_owned(),
                estimated_chunks: Some(128),
                description: None,
            }],
        };
        let json = serde_json::to_value(&r).unwrap();
        assert_eq!(json["sources"][0]["displayName"], "Session memory");
        let back: MemoryListSourcesResult = serde_json::from_value(json).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn sources_changed_notification_roundtrip() {
        let p = MemorySourcesChangedParams {
            provider_id: "plugin:mem0".to_owned(),
            project: Some(sample_project()),
        };
        let json = serde_json::to_value(&p).unwrap();
        assert_eq!(json["providerId"], "plugin:mem0");
        let back: MemorySourcesChangedParams = serde_json::from_value(json).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn method_names_are_canonical() {
        assert_eq!(methods::MEMORY_QUERY, "memory.query");
        assert_eq!(methods::MEMORY_INGEST, "memory.ingest");
        assert_eq!(methods::MEMORY_INGEST_STATUS, "memory.ingest_status");
        assert_eq!(methods::MEMORY_LIST_SOURCES, "memory.list_sources");
        assert_eq!(methods::MEMORY_SOURCES_CHANGED, "memory.sources.changed");
    }
}
