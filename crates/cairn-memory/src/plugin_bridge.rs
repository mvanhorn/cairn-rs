//! RFC 029: From/TryFrom bridges between cairn-plugin-proto wire types
//! and the in-process retrieval/ingest types owned by this crate.
//!
//! The host runtime keeps an in-process `RetrievalService` trait regardless
//! of whether retrieval ultimately dispatches to `InMemoryRetrieval`
//! (cairn-default) or to a plugin subprocess via RFC 007. When the plugin
//! path is taken, the MultiProvider dispatcher marshals a `RetrievalQuery`
//! into wire-level `KnowledgeQueryParams`, awaits the plugin response, and
//! lifts the wire `KnowledgeQueryResult` back into the in-process
//! `RetrievalResponse` shape.
//!
//! Invariants enforced here:
//!
//! 1. Runtime-owned scoring dimensions carried by the plugin response
//!    (`graph_proximity`, `source_credibility`, `corroboration`) are dropped
//!    on the way in. The runtime-owned post-hoc rescorer (PR B2) is the
//!    authoritative source for those dimensions. Guards against a misbehaving
//!    plugin trying to influence scoring it is not allowed to compute.
//! 2. Provider-required dimensions the plugin left as `None` on the wire
//!    surface as `0.0` in the breakdown (the numeric identity for the
//!    downstream weighted-sum scorer).
//! 3. `SourceType` / `RetrievalMode` / `IngestStatus` conversions match the
//!    string representations in the `rename_all = "snake_case"` serde
//!    contract on both sides; the enums are kept 1:1 on purpose.

use cairn_plugin_proto::knowledge::{
    ChunkRecordWire, KnowledgeIngestParams, KnowledgeIngestStatus, KnowledgeQueryParams,
    KnowledgeQueryResult, MetadataFilterWire, RetrievalModeWire, RetrievalResultWire,
    ScoringBreakdownWire, SourceTypeWire,
};

use crate::ingest::{ChunkRecord, IngestRequest, IngestStatus, SourceType};
use crate::retrieval::{
    CandidateStage, MetadataFilter, RerankerStrategy, RetrievalDiagnostics, RetrievalMode,
    RetrievalQuery, RetrievalResponse, RetrievalResult, ScoringBreakdown,
};

// ─── RetrievalMode ⇄ RetrievalModeWire ────────────────────────────────────

impl From<RetrievalMode> for RetrievalModeWire {
    fn from(mode: RetrievalMode) -> Self {
        match mode {
            RetrievalMode::LexicalOnly => Self::LexicalOnly,
            RetrievalMode::VectorOnly => Self::VectorOnly,
            RetrievalMode::Hybrid => Self::Hybrid,
        }
    }
}

impl From<RetrievalModeWire> for RetrievalMode {
    fn from(mode: RetrievalModeWire) -> Self {
        match mode {
            RetrievalModeWire::LexicalOnly => Self::LexicalOnly,
            RetrievalModeWire::VectorOnly => Self::VectorOnly,
            RetrievalModeWire::Hybrid => Self::Hybrid,
        }
    }
}

// ─── SourceType ⇄ SourceTypeWire ──────────────────────────────────────────

impl From<SourceType> for SourceTypeWire {
    fn from(t: SourceType) -> Self {
        match t {
            SourceType::PlainText => Self::PlainText,
            SourceType::Markdown => Self::Markdown,
            SourceType::Html => Self::Html,
            // RFC 030 collapsed the wire-side duplicate; the in-process
            // `SourceType::JsonStructured` and `::StructuredJson` both map
            // to the single `SourceTypeWire::StructuredJson`. The distinction
            // between them is a parser-stage detail that never needed to cross
            // the plugin boundary. Back-compat: wire messages carrying
            // `"json_structured"` deserialise via serde alias (see
            // `SourceTypeWire` in cairn-plugin-proto).
            SourceType::StructuredJson | SourceType::JsonStructured => Self::StructuredJson,
            SourceType::KnowledgePack => Self::KnowledgePack,
        }
    }
}

impl From<SourceTypeWire> for SourceType {
    fn from(t: SourceTypeWire) -> Self {
        match t {
            SourceTypeWire::PlainText => Self::PlainText,
            SourceTypeWire::Markdown => Self::Markdown,
            SourceTypeWire::Html => Self::Html,
            SourceTypeWire::StructuredJson => Self::StructuredJson,
            SourceTypeWire::KnowledgePack => Self::KnowledgePack,
        }
    }
}

// ─── IngestStatus ⇄ KnowledgeIngestStatus ─────────────────────────────────

impl From<IngestStatus> for KnowledgeIngestStatus {
    fn from(s: IngestStatus) -> Self {
        match s {
            IngestStatus::Pending => Self::Pending,
            IngestStatus::Parsing => Self::Parsing,
            IngestStatus::Chunking => Self::Chunking,
            IngestStatus::Embedding => Self::Embedding,
            IngestStatus::Indexing => Self::Indexing,
            IngestStatus::Completed => Self::Completed,
            IngestStatus::Failed => Self::Failed,
        }
    }
}

impl From<KnowledgeIngestStatus> for IngestStatus {
    fn from(s: KnowledgeIngestStatus) -> Self {
        match s {
            KnowledgeIngestStatus::Pending => Self::Pending,
            KnowledgeIngestStatus::Parsing => Self::Parsing,
            KnowledgeIngestStatus::Chunking => Self::Chunking,
            KnowledgeIngestStatus::Embedding => Self::Embedding,
            KnowledgeIngestStatus::Indexing => Self::Indexing,
            KnowledgeIngestStatus::Completed => Self::Completed,
            KnowledgeIngestStatus::Failed => Self::Failed,
        }
    }
}

// ─── MetadataFilter ⇄ MetadataFilterWire ──────────────────────────────────

impl From<MetadataFilter> for MetadataFilterWire {
    fn from(f: MetadataFilter) -> Self {
        Self {
            key: f.key,
            value: f.value,
        }
    }
}

impl From<MetadataFilterWire> for MetadataFilter {
    fn from(f: MetadataFilterWire) -> Self {
        Self {
            key: f.key,
            value: f.value,
        }
    }
}

// ─── ChunkRecord ⇄ ChunkRecordWire ────────────────────────────────────────
//
// `embedding`, `embedding_model_id`, and `needs_reembed` are runtime-internal
// bookkeeping — they never cross the plugin boundary. Wire → in-process
// lifts with those fields defaulted; in-process → wire drops them.

impl From<ChunkRecord> for ChunkRecordWire {
    fn from(c: ChunkRecord) -> Self {
        Self {
            chunk_id: c.chunk_id,
            document_id: c.document_id,
            source_id: c.source_id,
            source_type: c.source_type.into(),
            project: c.project,
            text: c.text,
            position: c.position,
            created_at: c.created_at,
            updated_at: c.updated_at,
            provenance_metadata: c.provenance_metadata,
            credibility_score: c.credibility_score,
            graph_linkage: c.graph_linkage,
            content_hash: c.content_hash,
            entities: c.entities,
        }
    }
}

impl From<ChunkRecordWire> for ChunkRecord {
    fn from(c: ChunkRecordWire) -> Self {
        Self {
            chunk_id: c.chunk_id,
            document_id: c.document_id,
            source_id: c.source_id,
            source_type: c.source_type.into(),
            project: c.project,
            text: c.text,
            position: c.position,
            created_at: c.created_at,
            updated_at: c.updated_at,
            provenance_metadata: c.provenance_metadata,
            credibility_score: c.credibility_score,
            graph_linkage: c.graph_linkage,
            embedding: None,
            content_hash: c.content_hash,
            entities: c.entities,
            embedding_model_id: None,
            needs_reembed: false,
        }
    }
}

// ─── ScoringBreakdown ⇄ ScoringBreakdownWire ──────────────────────────────
//
// Runtime-owned dimensions: on the way OUT to a plugin we send `None` for
// graph_proximity / source_credibility / corroboration (the plugin must not
// be told a score it is not allowed to compute). On the way IN we drop any
// values the plugin populated — the runtime post-hoc rescorer (B2) fills
// those fields before the agent sees the response.

impl From<ScoringBreakdown> for ScoringBreakdownWire {
    fn from(b: ScoringBreakdown) -> Self {
        Self {
            semantic_relevance: Some(b.semantic_relevance),
            lexical_relevance: Some(b.lexical_relevance),
            freshness_decay: Some(b.freshness_decay),
            staleness_penalty: Some(b.staleness_penalty),
            recency_of_use: b.recency_of_use,
            graph_proximity: None,
            source_credibility: None,
            corroboration: None,
        }
    }
}

impl From<ScoringBreakdownWire> for ScoringBreakdown {
    fn from(b: ScoringBreakdownWire) -> Self {
        Self {
            semantic_relevance: b.semantic_relevance.unwrap_or(0.0),
            lexical_relevance: b.lexical_relevance.unwrap_or(0.0),
            freshness_decay: b.freshness_decay.unwrap_or(0.0),
            staleness_penalty: b.staleness_penalty.unwrap_or(0.0),
            source_credibility: 0.0,
            corroboration: 0.0,
            graph_proximity: 0.0,
            recency_of_use: b.recency_of_use,
        }
    }
}

// ─── RetrievalResult ⇄ RetrievalResultWire ────────────────────────────────

impl From<RetrievalResult> for RetrievalResultWire {
    fn from(r: RetrievalResult) -> Self {
        Self {
            chunk: r.chunk.into(),
            score: r.score,
            breakdown: r.breakdown.into(),
        }
    }
}

impl From<RetrievalResultWire> for RetrievalResult {
    fn from(r: RetrievalResultWire) -> Self {
        Self {
            chunk: r.chunk.into(),
            score: r.score,
            breakdown: r.breakdown.into(),
        }
    }
}

// ─── RetrievalQuery → KnowledgeQueryParams ────────────────────────────────
//
// One-way on purpose: the plugin side never needs to reconstruct an
// in-process `RetrievalQuery` (the host owns that type). Scoring policy
// is host-side; plugins don't receive it.

impl From<RetrievalQuery> for KnowledgeQueryParams {
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

// ─── KnowledgeQueryResult → RetrievalResponse ─────────────────────────────
//
// One-way on purpose: the host lifts the wire response back into the
// in-process shape. Diagnostics reconstruction is best-effort — fields the
// plugin omitted are synthesized from what the runtime observed.

impl From<KnowledgeQueryResult> for RetrievalResponse {
    fn from(r: KnowledgeQueryResult) -> Self {
        let mode_used: RetrievalMode = r.diagnostics.mode_used.into();
        let results_returned = r.diagnostics.results_returned as usize;
        let candidates_generated = results_returned;
        let latency_ms = r.diagnostics.latency_ms.unwrap_or(0);
        let stages_used = r
            .diagnostics
            .stages_used
            .unwrap_or_default()
            .into_iter()
            .filter_map(parse_candidate_stage)
            .collect();
        let scoring_dimensions_used = r.diagnostics.scoring_dimensions_used;
        let effective_policy = None;
        // Provider-declared reranker path. Plugin providers that can't
        // surface a structured reranker leave this `None`; we map known
        // wire strings onto our in-process enum and fall back to `None`
        // for unknown values (provider-specific rerankers are still
        // opaque to cairn's tunable policy per RFC 029).
        let reranker_used = r
            .diagnostics
            .reranker_used
            .as_deref()
            .and_then(parse_reranker_strategy)
            .unwrap_or(RerankerStrategy::None);
        let results = r.results.into_iter().map(Into::into).collect();
        Self {
            results,
            diagnostics: RetrievalDiagnostics {
                mode_used,
                reranker_used,
                candidates_generated,
                results_returned,
                latency_ms,
                stages_used,
                scoring_dimensions_used,
                effective_policy,
                // RFC 030: the rescorer stamps the correct family on
                // the return path; leave `None` here since the wire
                // bridge is family-neutral.
                family: None,
            },
        }
    }
}

fn parse_candidate_stage(s: String) -> Option<CandidateStage> {
    match s.as_str() {
        "lexical" => Some(CandidateStage::Lexical),
        "vector" => Some(CandidateStage::Vector),
        "merged" => Some(CandidateStage::Merged),
        "reranked" => Some(CandidateStage::Reranked),
        _ => None,
    }
}

fn parse_reranker_strategy(s: &str) -> Option<RerankerStrategy> {
    match s {
        "none" => Some(RerankerStrategy::None),
        "mmr" => Some(RerankerStrategy::Mmr),
        "provider_reranker" | "provider-reranker" => Some(RerankerStrategy::ProviderReranker),
        _ => None,
    }
}

// ─── IngestRequest → KnowledgeIngestParams ────────────────────────────────
//
// `bundle_source_id` is a host-side field (used by the ingest-pipeline's
// bundle import flow) and has no wire counterpart — it stays behind.

impl From<IngestRequest> for KnowledgeIngestParams {
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

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_domain::{ChunkId, KnowledgeDocumentId, ProjectKey, SourceId};

    fn proj() -> ProjectKey {
        ProjectKey::new("t", "w", "p")
    }

    #[test]
    fn retrieval_mode_round_trips() {
        for mode in [
            RetrievalMode::LexicalOnly,
            RetrievalMode::VectorOnly,
            RetrievalMode::Hybrid,
        ] {
            let wire: RetrievalModeWire = mode.into();
            let back: RetrievalMode = wire.into();
            assert_eq!(mode, back);
        }
    }

    #[test]
    fn source_type_round_trips() {
        for t in [
            SourceType::PlainText,
            SourceType::Markdown,
            SourceType::Html,
            SourceType::StructuredJson,
            SourceType::KnowledgePack,
        ] {
            let wire: SourceTypeWire = t.into();
            let back: SourceType = wire.into();
            assert_eq!(t, back);
        }
    }

    #[test]
    fn json_structured_collapses_into_structured_json_on_the_wire() {
        // RFC 030: `SourceType::JsonStructured` (in-process parser stage) and
        // `SourceType::StructuredJson` both serialize to
        // `SourceTypeWire::StructuredJson`. The distinction is a parser-stage
        // detail that never needed to cross the plugin boundary.
        let wire: SourceTypeWire = SourceType::JsonStructured.into();
        assert_eq!(wire, SourceTypeWire::StructuredJson);
    }

    #[test]
    fn legacy_json_structured_wire_literal_still_deserialises() {
        // Back-compat: pre-RFC-030 adapters that emit `"json_structured"`
        // on the wire must still deserialize cleanly via serde alias.
        let v: SourceTypeWire = serde_json::from_str("\"json_structured\"").unwrap();
        assert_eq!(v, SourceTypeWire::StructuredJson);
    }

    #[test]
    fn ingest_status_round_trips() {
        for s in [
            IngestStatus::Pending,
            IngestStatus::Parsing,
            IngestStatus::Chunking,
            IngestStatus::Embedding,
            IngestStatus::Indexing,
            IngestStatus::Completed,
            IngestStatus::Failed,
        ] {
            let wire: KnowledgeIngestStatus = s.into();
            let back: IngestStatus = wire.into();
            assert_eq!(s, back);
        }
    }

    #[test]
    fn chunk_record_round_trip_drops_runtime_fields() {
        let original = ChunkRecord {
            chunk_id: ChunkId::new("c1"),
            document_id: KnowledgeDocumentId::new("d1"),
            source_id: SourceId::new("s1"),
            source_type: SourceType::Markdown,
            project: proj(),
            text: "hello".to_owned(),
            position: 3,
            created_at: 1_000,
            updated_at: Some(2_000),
            provenance_metadata: None,
            credibility_score: Some(0.5),
            graph_linkage: Some("link".to_owned()),
            embedding: Some(vec![0.1, 0.2]),
            content_hash: Some("abc".to_owned()),
            entities: vec!["acme".to_owned()],
            embedding_model_id: Some("text-embedding-3".to_owned()),
            needs_reembed: true,
        };
        let wire: ChunkRecordWire = original.clone().into();
        let back: ChunkRecord = wire.into();
        // Runtime-internal fields are defaulted on the way back.
        assert!(back.embedding.is_none());
        assert!(back.embedding_model_id.is_none());
        assert!(!back.needs_reembed);
        // Business fields survive.
        assert_eq!(back.chunk_id, original.chunk_id);
        assert_eq!(back.text, original.text);
        assert_eq!(back.position, original.position);
        assert_eq!(back.entities, original.entities);
    }

    #[test]
    fn scoring_breakdown_drops_runtime_owned_dims_from_wire() {
        // A plugin populates all 8 fields; the conversion discards the
        // three runtime-owned dimensions regardless.
        let wire = ScoringBreakdownWire {
            semantic_relevance: Some(0.9),
            lexical_relevance: Some(0.8),
            freshness_decay: Some(0.7),
            staleness_penalty: Some(0.1),
            recency_of_use: Some(0.3),
            graph_proximity: Some(0.99),
            source_credibility: Some(0.99),
            corroboration: Some(0.99),
        };
        let in_proc: ScoringBreakdown = wire.into();
        assert_eq!(in_proc.semantic_relevance, 0.9);
        assert_eq!(in_proc.freshness_decay, 0.7);
        assert_eq!(in_proc.graph_proximity, 0.0);
        assert_eq!(in_proc.source_credibility, 0.0);
        assert_eq!(in_proc.corroboration, 0.0);
    }

    #[test]
    fn scoring_breakdown_none_fields_default_to_zero() {
        let wire = ScoringBreakdownWire {
            semantic_relevance: None,
            lexical_relevance: Some(0.4),
            freshness_decay: None,
            staleness_penalty: None,
            recency_of_use: None,
            graph_proximity: None,
            source_credibility: None,
            corroboration: None,
        };
        let in_proc: ScoringBreakdown = wire.into();
        assert_eq!(in_proc.semantic_relevance, 0.0);
        assert_eq!(in_proc.lexical_relevance, 0.4);
        assert_eq!(in_proc.freshness_decay, 0.0);
        assert!(in_proc.recency_of_use.is_none());
    }

    #[test]
    fn out_bound_breakdown_leaves_runtime_dims_none() {
        let in_proc = ScoringBreakdown {
            semantic_relevance: 0.9,
            lexical_relevance: 0.8,
            freshness_decay: 0.7,
            staleness_penalty: 0.1,
            source_credibility: 0.99,
            corroboration: 0.99,
            graph_proximity: 0.99,
            recency_of_use: Some(0.3),
        };
        let wire: ScoringBreakdownWire = in_proc.into();
        assert!(wire.graph_proximity.is_none());
        assert!(wire.source_credibility.is_none());
        assert!(wire.corroboration.is_none());
        assert_eq!(wire.semantic_relevance, Some(0.9));
    }

    #[test]
    fn retrieval_query_to_params_shape() {
        let q = RetrievalQuery {
            project: proj(),
            query_text: "hello".to_owned(),
            mode: RetrievalMode::Hybrid,
            reranker: RerankerStrategy::None,
            limit: 10,
            metadata_filters: vec![MetadataFilter {
                key: "k".to_owned(),
                value: "v".to_owned(),
            }],
            scoring_policy: None,
        };
        let params: KnowledgeQueryParams = q.into();
        assert_eq!(params.limit, 10);
        assert_eq!(params.metadata_filters.len(), 1);
        assert!(matches!(params.mode, RetrievalModeWire::Hybrid));
    }

    #[test]
    fn retrieval_query_limit_saturates_on_overflow() {
        let q = RetrievalQuery {
            project: proj(),
            query_text: String::new(),
            mode: RetrievalMode::LexicalOnly,
            reranker: RerankerStrategy::None,
            // Larger than u32::MAX on platforms where usize > 32 bits.
            limit: (u32::MAX as usize).saturating_add(10),
            metadata_filters: vec![],
            scoring_policy: None,
        };
        let params: KnowledgeQueryParams = q.into();
        assert_eq!(params.limit, u32::MAX);
    }
}
