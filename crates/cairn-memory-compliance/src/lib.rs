//! RFC 030 PR-H: shape-only compliance suite for the `memory_provider`
//! plugin contract.
//!
//! Mirror of [`cairn_knowledge_compliance`] speaking in memory-family
//! types. Each check returns a `ComplianceResult`: `Ok(())` on pass,
//! `Err(ComplianceError { reason })` on fail. The check set:
//!
//! 1. [`check_wire_type_round_trips`] — every memory wire type in
//!    `cairn-plugin-proto::memory` round-trips through serde.
//! 2. [`check_required_field_presence`] — a well-formed
//!    [`MemoryQueryResult`] carries the required diagnostics fields.
//! 3. [`check_tri_state_matches_surfaced`] — tri-state scoring-dimension
//!    declarations match what the plugin surfaces on results.
//! 4. [`check_runtime_owned_overwritten`] — runtime-owned dimensions
//!    (graph_proximity, source_credibility, corroboration) populated
//!    by a provider are unconditionally overwritten by cairn's
//!    post-hoc rescorer. For memory-family responses `graph_proximity`
//!    stays at zero because the rescorer skips `multi_neighbors` (RFC
//!    030 PR-F); this check is correspondingly relaxed on that dim.
//! 5. [`check_diagnostics_computed_by_markers`] — after rescoring,
//!    `diagnostics.scoring_dimensions_used` carries the runtime-
//!    computed markers. For memory-family responses only
//!    `source_credibility` + `corroboration` markers are expected —
//!    `graph_proximity` is not marked because the rescorer did not
//!    touch it (RFC 030 PR-F).
//! 6. [`check_auto_extract_memory_store_contract`] — a provider
//!    declaring `auto_extract = true` on its [`MemoryProviderCapability`]
//!    must not claim ingest acceptance for `memory.ingest` calls when
//!    the runtime would have suppressed the tool.
//!
//! Family-neutral checks (error shape stability, dual-family rejection)
//! are re-exported from [`cairn_provider_compliance_core`] so call
//! sites can reach everything through one import.

pub use cairn_provider_compliance_core::{
    check_error_shape_stability, check_no_dual_family_capabilities, ComplianceError,
    ComplianceResult,
};

use cairn_domain::{ChunkId, DocumentId, ProjectKey, SourceId};
use cairn_memory::retrieval::RetrievalResponse;
use cairn_plugin_proto::knowledge::{
    DimensionSupport, MetadataFilterWire, RetrievalModeWire, ScoringBreakdownWire,
    ScoringDimensionSet, SourceTypeWire,
};
use cairn_plugin_proto::memory::{
    MemoryChunkRecordWire, MemoryIngestAck, MemoryIngestParams, MemoryIngestStatus,
    MemoryIngestStatusParams, MemoryIngestStatusResult, MemoryListSourcesParams,
    MemoryListSourcesResult, MemoryProviderCapability, MemoryQueryDiagnostics, MemoryQueryParams,
    MemoryQueryResult, MemoryRetrievalResultWire, MemorySource, MemorySourcesChangedParams,
};

fn fail(reason: impl Into<String>) -> ComplianceError {
    ComplianceError {
        reason: reason.into(),
    }
}

/// Round-trip every memory-family wire type through serde. A new field
/// landing on either end without a `#[serde(default)]` opt-in would
/// break this.
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
        "MemoryProviderCapability",
        MemoryProviderCapability {
            retrieval_modes: vec![RetrievalModeWire::VectorOnly],
            ingest_capable: false,
            ingest_source_types: vec![],
            auto_extract: true,
            scoring_dimensions: ScoringDimensionSet {
                semantic_relevance: DimensionSupport::Surfaced,
                lexical_relevance: DimensionSupport::NotSupported,
                freshness_decay: DimensionSupport::Surfaced,
                staleness_penalty: DimensionSupport::NotSupported,
                recency_of_use: DimensionSupport::Surfaced,
            },
        },
    )?;

    round_trip(
        "MemoryQueryParams",
        MemoryQueryParams {
            project: project.clone(),
            query_text: "what did alice say?".to_owned(),
            mode: RetrievalModeWire::VectorOnly,
            limit: 10,
            metadata_filters: vec![MetadataFilterWire {
                key: "session".to_owned(),
                value: "abc".to_owned(),
            }],
        },
    )?;

    let chunk = MemoryChunkRecordWire {
        chunk_id: ChunkId::new("m1"),
        document_id: DocumentId::new("mem-42"),
        source_id: SourceId::new("session:abc"),
        source_type: SourceTypeWire::PlainText,
        project: project.clone(),
        text: "alice prefers oolong".to_owned(),
        position: 0,
        created_at: 1_000,
        updated_at: Some(2_000),
        provenance_metadata: None,
        credibility_score: Some(0.5),
        graph_linkage: None,
        content_hash: Some("hash".to_owned()),
        entities: vec!["alice".to_owned()],
    };

    round_trip("MemoryChunkRecordWire", chunk.clone())?;

    round_trip(
        "MemoryQueryResult",
        MemoryQueryResult {
            results: vec![MemoryRetrievalResultWire {
                chunk: chunk.clone(),
                score: 0.91,
                breakdown: ScoringBreakdownWire {
                    semantic_relevance: Some(0.91),
                    lexical_relevance: None,
                    freshness_decay: Some(0.3),
                    staleness_penalty: None,
                    recency_of_use: Some(0.6),
                    // Runtime-owned — must round-trip as None.
                    graph_proximity: None,
                    source_credibility: None,
                    corroboration: None,
                },
            }],
            diagnostics: MemoryQueryDiagnostics {
                mode_used: RetrievalModeWire::VectorOnly,
                stages_used: Some(vec!["vector".to_owned()]),
                reranker_used: None,
                scoring_dimensions_used: vec!["semantic_relevance".to_owned()],
                results_returned: 1,
                latency_ms: Some(5),
            },
        },
    )?;

    round_trip(
        "MemoryIngestParams",
        MemoryIngestParams {
            document_id: DocumentId::new("mem-42"),
            source_id: SourceId::new("session:abc"),
            source_type: SourceTypeWire::PlainText,
            project: project.clone(),
            content: "user prefers oolong".to_owned(),
            import_id: None,
            corpus_id: None,
            tags: vec!["preference".to_owned()],
        },
    )?;

    round_trip(
        "MemoryIngestAck",
        MemoryIngestAck {
            document_id: DocumentId::new("mem-42"),
            accepted: true,
            reason: None,
        },
    )?;

    round_trip(
        "MemoryIngestStatusParams",
        MemoryIngestStatusParams {
            document_id: DocumentId::new("mem-42"),
        },
    )?;

    round_trip(
        "MemoryIngestStatusResult",
        MemoryIngestStatusResult {
            status: Some(MemoryIngestStatus::Completed),
        },
    )?;

    round_trip(
        "MemoryListSourcesParams",
        MemoryListSourcesParams {
            project: Some(project.clone()),
        },
    )?;

    round_trip(
        "MemoryListSourcesResult",
        MemoryListSourcesResult {
            sources: vec![MemorySource {
                source_id: "mem0:project-42".to_owned(),
                display_name: "Session memory".to_owned(),
                estimated_chunks: Some(128),
                description: None,
            }],
        },
    )?;

    round_trip(
        "MemorySourcesChangedParams",
        MemorySourcesChangedParams {
            provider_id: "plugin:mem0".to_owned(),
            project: Some(project),
        },
    )?;

    Ok(())
}

/// `MemoryQueryResult` must carry every required diagnostics field + at
/// least one result with non-empty chunk identifiers.
pub fn check_required_field_presence(result: &MemoryQueryResult) -> ComplianceResult {
    let reported = usize::try_from(result.diagnostics.results_returned)
        .map_err(|_| fail("results_returned exceeds usize::MAX"))?;
    if reported != result.results.len() {
        return Err(fail(format!(
            "diagnostics.results_returned ({}) != results.len() ({})",
            reported,
            result.results.len()
        )));
    }

    if !result.results.is_empty() && result.diagnostics.scoring_dimensions_used.is_empty() {
        return Err(fail(
            "scoring_dimensions_used is empty on a non-empty result set",
        ));
    }

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

/// Tri-state scoring-dimension declarations must match what the plugin
/// surfaces: `Surfaced` must populate at least one result, `NotSupported`
/// must be `None` on every result.
pub fn check_tri_state_matches_surfaced(
    capability: &MemoryProviderCapability,
    result: &MemoryQueryResult,
) -> ComplianceResult {
    if result.results.is_empty() {
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

/// Tolerance used for sentinel-equality comparisons against
/// `provider_sentinel`. `f64::EPSILON` is ~2.22e-16 which is too tight
/// for `[0, 1]`-range scores (any arithmetic the rescorer does around
/// zero can push below that threshold), and absolute-epsilon around
/// `0.99` sentinels is dominated by operation error. A fixed 1e-9
/// tolerance is both tighter than any legitimate rescorer
/// transformation and looser than spurious rounding.
const SENTINEL_TOLERANCE: f64 = 1e-9;

/// True when `x` equals the sentinel to within `SENTINEL_TOLERANCE`.
/// Handles `NaN` sentinels (`f64::NAN != f64::NAN` — `(NAN - NAN).abs()
/// < _` is always `false`). If the provider returned NaN on a
/// runtime-owned dim and the rescorer failed to overwrite, we still
/// catch it via the `is_nan()` branch.
fn matches_sentinel(x: f64, sentinel: f64) -> bool {
    if sentinel.is_nan() {
        return x.is_nan();
    }
    (x - sentinel).abs() < SENTINEL_TOLERANCE
}

/// After cairn's post-hoc rescoring, no runtime-owned dimension should
/// carry the plugin-injected sentinel. Memory-family response: the
/// rescorer zeros `graph_proximity` in Step 1 and skips the
/// `multi_neighbors` compute (RFC 030 PR-F), so this check still
/// verifies overwrite for `source_credibility` + `corroboration`; the
/// `graph_proximity` check is relaxed to "stays exactly zero", which
/// is the stronger guarantee the memory rescorer provides.
pub fn check_runtime_owned_overwritten(
    response: &RetrievalResponse,
    provider_sentinel: f64,
) -> ComplianceResult {
    for (i, r) in response.results.iter().enumerate() {
        // Memory-family: graph_proximity must be exactly 0.0 — the
        // rescorer skipped the multi_neighbors compute and left the
        // zeroing from step 1 in place.
        if r.breakdown.graph_proximity != 0.0 {
            return Err(fail(format!(
                "result[{i}].breakdown.graph_proximity = {} — memory-family \
                 rescorer must leave it at 0.0",
                r.breakdown.graph_proximity
            )));
        }
        if matches_sentinel(r.breakdown.source_credibility, provider_sentinel) {
            return Err(fail(format!(
                "result[{i}].breakdown.source_credibility still carries the provider sentinel \
                 ({provider_sentinel}) — runtime must have overwritten it"
            )));
        }
        if matches_sentinel(r.breakdown.corroboration, provider_sentinel) {
            return Err(fail(format!(
                "result[{i}].breakdown.corroboration still carries the provider sentinel \
                 ({provider_sentinel}) — runtime must have overwritten it"
            )));
        }
    }
    Ok(())
}

/// After memory-family rescoring, `diagnostics.scoring_dimensions_used`
/// must carry the two runtime-computed markers the memory rescorer
/// does populate (`source_credibility` + `corroboration`). The
/// `graph_proximity:runtime_post_hoc` marker MUST NOT be present —
/// the rescorer skipped that step, so claiming it would misrepresent
/// provenance. This is the inverse of the knowledge-family contract
/// where all three markers must be present.
pub fn check_diagnostics_computed_by_markers(response: &RetrievalResponse) -> ComplianceResult {
    if response.results.is_empty() {
        return Ok(());
    }
    let dims = &response.diagnostics.scoring_dimensions_used;
    for marker in [
        "source_credibility:runtime_post_hoc",
        "corroboration:runtime_post_hoc",
    ] {
        if !dims.iter().any(|d| d == marker) {
            return Err(fail(format!(
                "diagnostics missing expected runtime-computed marker `{marker}`; saw {dims:?}"
            )));
        }
    }
    if dims.iter().any(|d| d == "graph_proximity:runtime_post_hoc") {
        return Err(fail(
            "memory-family diagnostics must NOT carry `graph_proximity:runtime_post_hoc` — \
             the rescorer skipped multi_neighbors and did not compute the dim",
        ));
    }
    Ok(())
}

/// RFC 030 §Decisions D6: a memory provider that declares
/// `auto_extract = true` gets its `memory_store` tool hidden from the
/// agent prompt (PR-C's visibility gate + PR-D's invocation-time
/// rejection). If an operator still dispatches a `memory.ingest` call
/// against such a provider, the provider MUST respond with
/// `accepted = false` + a reason that cites the auto_extract mode.
/// This check captures that contract on a mock provider response.
pub fn check_auto_extract_memory_store_contract(
    capability: &MemoryProviderCapability,
    ingest_ack: &MemoryIngestAck,
) -> ComplianceResult {
    if !capability.auto_extract {
        // Not an auto-extract provider — contract doesn't apply.
        return Ok(());
    }
    if ingest_ack.accepted {
        return Err(fail(
            "MemoryProviderCapability.auto_extract = true but the provider accepted a \
             memory.ingest call — RFC 030 D6 requires auto-extract providers to reject \
             explicit ingests with a reason",
        ));
    }
    match ingest_ack.reason.as_deref() {
        Some(r) if r.to_lowercase().contains("auto") || r.to_lowercase().contains("extract") => {
            Ok(())
        }
        Some(other) => Err(fail(format!(
            "auto_extract provider rejection reason must cite the auto_extract mode; saw {other:?}"
        ))),
        None => Err(fail(
            "auto_extract provider rejected ingest but provided no reason — operator UI needs one",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_type_round_trips_pass_on_canonical_fixtures() {
        check_wire_type_round_trips().unwrap();
    }

    #[test]
    fn error_shape_stability_reexport_works() {
        check_error_shape_stability().unwrap();
    }

    #[test]
    fn dual_family_reexport_rejects_both_families() {
        let caps = vec![
            serde_json::json!({"type": "memory_provider"}),
            serde_json::json!({"type": "knowledge_provider"}),
        ];
        assert!(check_no_dual_family_capabilities(&caps).is_err());
    }

    #[test]
    fn auto_extract_provider_rejecting_ingest_with_cited_reason_passes() {
        let cap = MemoryProviderCapability {
            retrieval_modes: vec![RetrievalModeWire::VectorOnly],
            ingest_capable: false,
            ingest_source_types: vec![],
            auto_extract: true,
            scoring_dimensions: ScoringDimensionSet {
                semantic_relevance: DimensionSupport::Surfaced,
                lexical_relevance: DimensionSupport::NotSupported,
                freshness_decay: DimensionSupport::NotSupported,
                staleness_penalty: DimensionSupport::NotSupported,
                recency_of_use: DimensionSupport::NotSupported,
            },
        };
        let ack = MemoryIngestAck {
            document_id: DocumentId::new("d"),
            accepted: false,
            reason: Some("auto_extract backend — memory_store is suppressed".into()),
        };
        check_auto_extract_memory_store_contract(&cap, &ack).unwrap();
    }

    #[test]
    fn auto_extract_provider_accepting_ingest_fails() {
        let cap = MemoryProviderCapability {
            retrieval_modes: vec![RetrievalModeWire::VectorOnly],
            ingest_capable: true,
            ingest_source_types: vec![SourceTypeWire::PlainText],
            auto_extract: true,
            scoring_dimensions: ScoringDimensionSet {
                semantic_relevance: DimensionSupport::Surfaced,
                lexical_relevance: DimensionSupport::NotSupported,
                freshness_decay: DimensionSupport::NotSupported,
                staleness_penalty: DimensionSupport::NotSupported,
                recency_of_use: DimensionSupport::NotSupported,
            },
        };
        let ack = MemoryIngestAck {
            document_id: DocumentId::new("d"),
            accepted: true,
            reason: None,
        };
        assert!(check_auto_extract_memory_store_contract(&cap, &ack).is_err());
    }

    #[test]
    fn sentinel_match_handles_nan() {
        // A sentinel of NaN must be detected via is_nan(), not the
        // `(x - y).abs() < tol` path.
        assert!(matches_sentinel(f64::NAN, f64::NAN));
        assert!(!matches_sentinel(0.0, f64::NAN));
        assert!(!matches_sentinel(f64::NAN, 0.0));
    }

    #[test]
    fn sentinel_match_tolerates_tiny_rounding() {
        // Rescorer may transform a 0.99 sentinel through arithmetic;
        // the tolerance must be tight enough to still catch an
        // untouched value but loose enough that a deliberate
        // overwrite to 0.0 or 0.5 is clearly not-sentinel.
        assert!(matches_sentinel(0.99, 0.99));
        assert!(matches_sentinel(0.99 + 1e-12, 0.99));
        assert!(!matches_sentinel(0.0, 0.99));
        assert!(!matches_sentinel(0.5, 0.99));
    }

    #[test]
    fn non_auto_extract_provider_skips_contract_check() {
        let cap = MemoryProviderCapability {
            retrieval_modes: vec![RetrievalModeWire::VectorOnly],
            ingest_capable: true,
            ingest_source_types: vec![SourceTypeWire::PlainText],
            auto_extract: false,
            scoring_dimensions: ScoringDimensionSet {
                semantic_relevance: DimensionSupport::Surfaced,
                lexical_relevance: DimensionSupport::NotSupported,
                freshness_decay: DimensionSupport::NotSupported,
                staleness_penalty: DimensionSupport::NotSupported,
                recency_of_use: DimensionSupport::NotSupported,
            },
        };
        // Accepting is the correct behaviour for a non-auto-extract
        // provider, so the contract check passes (vacuously).
        let ack = MemoryIngestAck {
            document_id: DocumentId::new("d"),
            accepted: true,
            reason: None,
        };
        check_auto_extract_memory_store_contract(&cap, &ack).unwrap();
    }
}
