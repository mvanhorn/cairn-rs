//! Individual shape-only compliance checks.
//!
//! Every check returns a `ComplianceResult`: `Ok(())` on pass,
//! `Err(ComplianceError { reason })` on fail with a human-readable
//! reason naming the offending field. Tests in `tests/` call each
//! check against a fixture and assert `Ok` — so a failure surfaces as
//! a test panic with the reason attached.

use cairn_domain::{ChunkId, KnowledgeDocumentId, ProjectKey, SourceId};
use cairn_memory::ingest::{IngestError, SourceType};
use cairn_memory::retrieval::{RetrievalError, RetrievalResponse};
use cairn_plugin_proto::knowledge::{
    ChunkRecordWire, DimensionSupport, KnowledgeIngestAck, KnowledgeIngestParams,
    KnowledgeIngestStatus, KnowledgeIngestStatusParams, KnowledgeIngestStatusResult,
    KnowledgeListSourcesParams, KnowledgeListSourcesResult, KnowledgeProviderCapability,
    KnowledgeQueryDiagnostics, KnowledgeQueryParams, KnowledgeQueryResult, KnowledgeSource,
    KnowledgeSourcesChangedParams, MetadataFilterWire, RetrievalModeWire, RetrievalResultWire,
    ScoringBreakdownWire, ScoringDimensionSet, SourceTypeWire,
};

/// Compliance failure shape.
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

// ─── 1. Wire-type round-trips ─────────────────────────────────────────────

/// Every wire type in `cairn-plugin-proto::knowledge` must serialize to
/// JSON and deserialize back without losing fields. This is the core
/// of the "shape stable" contract: a new field landing on either end
/// without a corresponding opt-in default would break this check.
pub fn check_wire_type_round_trips() -> ComplianceResult {
    fn round_trip<T>(name: &str, value: T) -> ComplianceResult
    where
        T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let json = serde_json::to_string(&value)
            .map_err(|e| fail(format!("{name} failed to serialize: {e}")))?;
        let back: T = serde_json::from_str(&json)
            .map_err(|e| fail(format!("{name} failed to deserialize: {e}")))?;
        if value != back {
            return Err(fail(format!(
                "{name} round-trip changed the value: {value:?} != {back:?}"
            )));
        }
        Ok(())
    }

    let project = ProjectKey::new("t", "w", "p");

    round_trip(
        "KnowledgeProviderCapability",
        KnowledgeProviderCapability {
            retrieval_modes: vec![RetrievalModeWire::Hybrid, RetrievalModeWire::VectorOnly],
            ingest_capable: true,
            ingest_source_types: vec![SourceTypeWire::Markdown],
            scoring_dimensions: ScoringDimensionSet {
                semantic_relevance: DimensionSupport::Surfaced,
                lexical_relevance: DimensionSupport::Surfaced,
                freshness_decay: DimensionSupport::Surfaced,
                staleness_penalty: DimensionSupport::NotSupported,
                recency_of_use: DimensionSupport::NotSupported,
            },
        },
    )?;

    round_trip(
        "KnowledgeQueryParams",
        KnowledgeQueryParams {
            project: project.clone(),
            query_text: "hello".to_owned(),
            mode: RetrievalModeWire::Hybrid,
            limit: 10,
            metadata_filters: vec![MetadataFilterWire {
                key: "source".to_owned(),
                value: "local".to_owned(),
            }],
        },
    )?;

    let chunk = ChunkRecordWire {
        chunk_id: ChunkId::new("c1"),
        document_id: KnowledgeDocumentId::new("d1"),
        source_id: SourceId::new("s1"),
        source_type: SourceTypeWire::Markdown,
        project: project.clone(),
        text: "chunk".to_owned(),
        position: 0,
        created_at: 1_000,
        updated_at: Some(2_000),
        provenance_metadata: None,
        credibility_score: Some(0.5),
        graph_linkage: None,
        content_hash: Some("hash".to_owned()),
        entities: vec!["acme".to_owned()],
    };

    round_trip("ChunkRecordWire", chunk.clone())?;

    round_trip(
        "KnowledgeQueryResult",
        KnowledgeQueryResult {
            results: vec![RetrievalResultWire {
                chunk: chunk.clone(),
                score: 0.8,
                breakdown: ScoringBreakdownWire {
                    semantic_relevance: Some(0.8),
                    lexical_relevance: Some(0.4),
                    freshness_decay: Some(0.3),
                    staleness_penalty: Some(0.1),
                    recency_of_use: None,
                    graph_proximity: None,
                    source_credibility: None,
                    corroboration: None,
                },
            }],
            diagnostics: KnowledgeQueryDiagnostics {
                mode_used: RetrievalModeWire::Hybrid,
                stages_used: Some(vec!["lexical".to_owned()]),
                reranker_used: None,
                scoring_dimensions_used: vec!["semantic_relevance".to_owned()],
                results_returned: 1,
                latency_ms: Some(5),
            },
        },
    )?;

    round_trip(
        "KnowledgeIngestParams",
        KnowledgeIngestParams {
            document_id: KnowledgeDocumentId::new("d1"),
            source_id: SourceId::new("s1"),
            source_type: SourceTypeWire::Markdown,
            project: project.clone(),
            content: "payload".to_owned(),
            import_id: Some("imp".to_owned()),
            corpus_id: Some("corpus".to_owned()),
            tags: vec!["tag".to_owned()],
        },
    )?;

    round_trip(
        "KnowledgeIngestAck",
        KnowledgeIngestAck {
            document_id: KnowledgeDocumentId::new("d1"),
            accepted: true,
            reason: None,
        },
    )?;

    round_trip(
        "KnowledgeIngestStatusParams",
        KnowledgeIngestStatusParams {
            document_id: KnowledgeDocumentId::new("d1"),
        },
    )?;

    round_trip(
        "KnowledgeIngestStatusResult",
        KnowledgeIngestStatusResult {
            status: Some(KnowledgeIngestStatus::Completed),
        },
    )?;

    round_trip(
        "KnowledgeListSourcesParams",
        KnowledgeListSourcesParams {
            project: Some(project.clone()),
        },
    )?;

    round_trip(
        "KnowledgeListSourcesResult",
        KnowledgeListSourcesResult {
            sources: vec![KnowledgeSource {
                source_id: "s1".to_owned(),
                display_name: "Source 1".to_owned(),
                estimated_chunks: Some(100),
                description: Some("a sample source".to_owned()),
            }],
        },
    )?;

    round_trip(
        "KnowledgeSourcesChangedParams",
        KnowledgeSourcesChangedParams {
            provider_id: "mem0".to_owned(),
            project: Some(project),
        },
    )?;

    Ok(())
}

// ─── 2. Required-field presence in KnowledgeQueryResult ───────────────────

/// A well-formed `KnowledgeQueryResult` carries every required
/// diagnostics field (`mode_used`, `results_returned`,
/// `scoring_dimensions_used`) and at least one result with a valid
/// chunk.
pub fn check_required_field_presence(result: &KnowledgeQueryResult) -> ComplianceResult {
    // `results_returned` must match the actual results length — this
    // catches plugins that populate `results[]` but forget to update
    // the summary counter (or vice versa).
    let reported = usize::try_from(result.diagnostics.results_returned)
        .map_err(|_| fail("results_returned exceeds usize::MAX"))?;
    if reported != result.results.len() {
        return Err(fail(format!(
            "diagnostics.results_returned ({}) != results.len() ({})",
            reported,
            result.results.len()
        )));
    }

    // `scoring_dimensions_used` MUST NOT be empty on a non-empty
    // result set — the plugin must state which dims contributed.
    if !result.results.is_empty() && result.diagnostics.scoring_dimensions_used.is_empty() {
        return Err(fail(
            "scoring_dimensions_used is empty on a non-empty result set",
        ));
    }

    // Every `RetrievalResultWire` must carry a `chunk.document_id` and
    // `chunk.source_id` — these are the identity anchors cairn's
    // post-hoc rescorer keys its batched lookups on.
    for (i, r) in result.results.iter().enumerate() {
        if r.chunk.document_id.as_str().is_empty() {
            return Err(fail(format!(
                "result[{i}].chunk.document_id is empty — every chunk must carry a document id"
            )));
        }
        if r.chunk.source_id.as_str().is_empty() {
            return Err(fail(format!(
                "result[{i}].chunk.source_id is empty — every chunk must carry a source id"
            )));
        }
    }

    Ok(())
}

// ─── 3. Tri-state matches actually-surfaced ───────────────────────────────

/// A provider declaring `SurfaceDim = Surfaced` at handshake must
/// populate that dim on at least one result in the wire response.
/// Conversely, `NotSupported` dims must come through as `None` on
/// every result.
pub fn check_tri_state_matches_surfaced(
    capability: &KnowledgeProviderCapability,
    result: &KnowledgeQueryResult,
) -> ComplianceResult {
    if result.results.is_empty() {
        // No results → no way to verify positive surfacing; nothing to
        // contradict the negative surfacing either. Vacuously OK.
        return Ok(());
    }

    let dims = &capability.scoring_dimensions;

    check_dim(
        "semantic_relevance",
        dims.semantic_relevance,
        result
            .results
            .iter()
            .map(|r| r.breakdown.semantic_relevance),
    )?;
    check_dim(
        "lexical_relevance",
        dims.lexical_relevance,
        result.results.iter().map(|r| r.breakdown.lexical_relevance),
    )?;
    check_dim(
        "freshness_decay",
        dims.freshness_decay,
        result.results.iter().map(|r| r.breakdown.freshness_decay),
    )?;
    check_dim(
        "staleness_penalty",
        dims.staleness_penalty,
        result.results.iter().map(|r| r.breakdown.staleness_penalty),
    )?;
    check_dim(
        "recency_of_use",
        dims.recency_of_use,
        result.results.iter().map(|r| r.breakdown.recency_of_use),
    )?;

    Ok(())
}

fn check_dim(
    name: &str,
    declared: DimensionSupport,
    mut values: impl Iterator<Item = Option<f64>>,
) -> ComplianceResult {
    // Single-pass short-circuit: `.any(|v| v.is_some())` stops at the
    // first populated value instead of collecting the full iterator.
    let has_any_value = values.any(|v| v.is_some());
    match (declared, has_any_value) {
        (DimensionSupport::Surfaced, false) => Err(fail(format!(
            "{name} declared Surfaced but every result has it as null"
        ))),
        (DimensionSupport::NotSupported, true) => Err(fail(format!(
            "{name} declared NotSupported but at least one result has a value"
        ))),
        _ => Ok(()),
    }
}

// ─── 4. Runtime-owned injection is overwritten ────────────────────────────

/// After cairn's post-hoc rescoring, no runtime-owned dimension on any
/// result should carry the plugin-injected sentinel value. This check
/// takes the in-process `RetrievalResponse` that the rescorer produced
/// and a sentinel the caller-side fixture knew the plugin had
/// populated on the wire. If any result still carries the sentinel,
/// that means the rescorer failed to overwrite it.
pub fn check_runtime_owned_overwritten(
    response: &RetrievalResponse,
    provider_sentinel: f64,
) -> ComplianceResult {
    for (i, r) in response.results.iter().enumerate() {
        if (r.breakdown.graph_proximity - provider_sentinel).abs() < f64::EPSILON {
            return Err(fail(format!(
                "result[{i}].breakdown.graph_proximity still carries the provider sentinel \
                 ({provider_sentinel}) — runtime must have overwritten it"
            )));
        }
        if (r.breakdown.source_credibility - provider_sentinel).abs() < f64::EPSILON {
            return Err(fail(format!(
                "result[{i}].breakdown.source_credibility still carries the provider sentinel \
                 ({provider_sentinel}) — runtime must have overwritten it"
            )));
        }
        if (r.breakdown.corroboration - provider_sentinel).abs() < f64::EPSILON {
            return Err(fail(format!(
                "result[{i}].breakdown.corroboration still carries the provider sentinel \
                 ({provider_sentinel}) — runtime must have overwritten it"
            )));
        }
    }
    Ok(())
}

// ─── 5. Error-shape stability ─────────────────────────────────────────────

/// Error Display text is part of the wire contract (it flows into
/// operator UI + structured logs). The shape must stay stable so
/// operators' alerts / diagnostics don't drift.
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

// ─── 6. Diagnostics computed_by markers ──────────────────────────────────

/// After the post-hoc rescorer runs, `diagnostics.scoring_dimensions_used`
/// must carry the three runtime-computed markers. Provider-surfaced
/// dimension names stay as bare strings; runtime-computed ones carry a
/// `:runtime_post_hoc` suffix so operator UI can render per-dimension
/// provenance.
pub fn check_diagnostics_computed_by_markers(response: &RetrievalResponse) -> ComplianceResult {
    if response.results.is_empty() {
        // Rescorer short-circuits on empty responses and does not
        // append markers. Vacuously OK.
        return Ok(());
    }
    for marker in [
        "graph_proximity:runtime_post_hoc",
        "source_credibility:runtime_post_hoc",
        "corroboration:runtime_post_hoc",
    ] {
        if !response
            .diagnostics
            .scoring_dimensions_used
            .iter()
            .any(|d| d == marker)
        {
            return Err(fail(format!(
                "diagnostics missing expected runtime-computed marker `{marker}`; saw {:?}",
                response.diagnostics.scoring_dimensions_used
            )));
        }
    }
    Ok(())
}
