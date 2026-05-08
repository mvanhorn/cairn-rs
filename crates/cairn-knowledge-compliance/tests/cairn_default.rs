//! RFC 029 PR-C: run the six compliance checks against the
//! in-process cairn-default path (MultiProviderRetrieval wrapping
//! InMemoryRetrieval + PostHocRescorer). Proves the in-tree default
//! satisfies every wire-shape invariant a plugin would have to
//! satisfy.

use std::sync::Arc;

use cairn_domain::{events::ResolvedProviderSnapshot, ChunkId, KnowledgeDocumentId, SourceId};
use cairn_graph::in_memory::InMemoryGraphStore;
use cairn_graph::projections::{GraphNode, GraphProjection, NodeKind};
use cairn_knowledge_compliance::{
    check_diagnostics_computed_by_markers, check_error_shape_stability,
    check_required_field_presence, check_runtime_owned_overwritten,
    check_tri_state_matches_surfaced, check_wire_type_round_trips,
};
use cairn_memory::ingest::{ChunkRecord, SourceType};
use cairn_memory::multi_provider::{
    MultiProviderRetrieval, ProviderResolver, ProviderResolverError,
};
use cairn_memory::post_hoc_rescorer::{NoOpCredibilityLookup, PostHocRescorer};
use cairn_memory::retrieval::{
    RerankerStrategy, RetrievalDiagnostics, RetrievalMode, RetrievalQuery, RetrievalResponse,
    RetrievalResult, RetrievalService, ScoringBreakdown,
};

use cairn_knowledge_compliance::fixtures::{
    full_capability, read_only_capability, sample_project, sample_query_result,
};

// ─── 1. Wire-type round-trips ─────────────────────────────────────────────

#[test]
fn cairn_default_wire_types_round_trip() {
    check_wire_type_round_trips().expect("wire types round-trip cleanly");
}

// ─── 2. Required-field presence ───────────────────────────────────────────

#[test]
fn cairn_default_required_fields_present() {
    check_required_field_presence(&sample_query_result())
        .expect("sample response has every required field");
}

#[test]
fn required_field_check_catches_counter_drift() {
    let mut malformed = sample_query_result();
    malformed.diagnostics.results_returned = 99;
    let err = check_required_field_presence(&malformed).expect_err("counter mismatch must fail");
    assert!(
        err.reason.contains("results_returned"),
        "error must name the drifted field: {}",
        err.reason
    );
}

#[test]
fn required_field_check_catches_empty_scoring_dims() {
    let mut malformed = sample_query_result();
    malformed.diagnostics.scoring_dimensions_used.clear();
    let err = check_required_field_presence(&malformed).expect_err("empty dims must fail");
    assert!(err.reason.contains("scoring_dimensions_used"));
}

// ─── 3. Tri-state matches surfaced ───────────────────────────────────────

#[test]
fn cairn_default_full_capability_matches_full_result() {
    // Our sample result populates every provider-required dim
    // (except recency_of_use which is None); build a capability
    // snapshot that matches.
    let mut cap = full_capability();
    cap.scoring_dimensions.recency_of_use =
        cairn_plugin_proto::knowledge::DimensionSupport::NotSupported;
    check_tri_state_matches_surfaced(&cap, &sample_query_result())
        .expect("full capability matches full result");
}

#[test]
fn tri_state_check_catches_not_supported_with_value() {
    // Provider declares every non-semantic dim = NotSupported but the
    // sample response populates lexical/freshness/staleness → must
    // fail naming one of those. The check short-circuits on the first
    // violation it sees; any of the three unsurfaced dims is a
    // legitimate error message.
    let mut cap = read_only_capability();
    cap.scoring_dimensions.semantic_relevance =
        cairn_plugin_proto::knowledge::DimensionSupport::Surfaced;
    let result = sample_query_result();
    let err = check_tri_state_matches_surfaced(&cap, &result)
        .expect_err("NotSupported with value must fail");
    assert!(
        err.reason.contains("lexical_relevance")
            || err.reason.contains("freshness_decay")
            || err.reason.contains("staleness_penalty"),
        "error must name an offending NotSupported dim: {}",
        err.reason
    );
}

#[test]
fn tri_state_check_catches_surfaced_with_every_result_null() {
    // Provider declares semantic_relevance = Surfaced but every
    // result has it as null → must fail.
    let mut cap = full_capability();
    cap.scoring_dimensions.semantic_relevance =
        cairn_plugin_proto::knowledge::DimensionSupport::Surfaced;
    let mut result = sample_query_result();
    for r in result.results.iter_mut() {
        r.breakdown.semantic_relevance = None;
    }
    let err =
        check_tri_state_matches_surfaced(&cap, &result).expect_err("Surfaced-all-null must fail");
    assert!(err.reason.contains("semantic_relevance"));
}

// ─── 4. Runtime-owned overwritten ─────────────────────────────────────────

struct FixedResolver(cairn_domain::ProviderRef);

#[async_trait::async_trait]
impl ProviderResolver for FixedResolver {
    async fn resolve(
        &self,
        _project: &cairn_domain::ProjectKey,
    ) -> Result<cairn_domain::ProviderRef, ProviderResolverError> {
        Ok(self.0.clone())
    }
}

struct InertDispatcher;

#[async_trait::async_trait]
impl cairn_memory::multi_provider::KnowledgePluginDispatcher for InertDispatcher {
    async fn query(
        &self,
        _plugin_id: &str,
        _params: cairn_plugin_proto::knowledge::KnowledgeQueryParams,
    ) -> Result<
        cairn_plugin_proto::knowledge::KnowledgeQueryResult,
        cairn_memory::multi_provider::KnowledgePluginError,
    > {
        panic!("cairn-default path must not hit the plugin dispatcher")
    }
    async fn ingest(
        &self,
        _plugin_id: &str,
        _params: cairn_plugin_proto::knowledge::KnowledgeIngestParams,
    ) -> Result<
        cairn_plugin_proto::knowledge::KnowledgeIngestAck,
        cairn_memory::multi_provider::KnowledgePluginError,
    > {
        panic!("cairn-default path must not hit the plugin dispatcher")
    }
    async fn ingest_status(
        &self,
        _plugin_id: &str,
        _params: cairn_plugin_proto::knowledge::KnowledgeIngestStatusParams,
    ) -> Result<
        cairn_plugin_proto::knowledge::KnowledgeIngestStatusResult,
        cairn_memory::multi_provider::KnowledgePluginError,
    > {
        panic!("cairn-default path must not hit the plugin dispatcher")
    }
}

/// A fake retrieval service that returns a response with the provider
/// sentinel pre-populated on every runtime-owned dim. After the
/// rescorer runs, none of those sentinels should survive.
struct SentinelRetrieval {
    sentinel: f64,
}

#[async_trait::async_trait]
impl RetrievalService for SentinelRetrieval {
    async fn query(
        &self,
        query: RetrievalQuery,
    ) -> Result<RetrievalResponse, cairn_memory::retrieval::RetrievalError> {
        let chunk = ChunkRecord {
            chunk_id: ChunkId::new("c1"),
            document_id: KnowledgeDocumentId::new("d1"),
            source_id: SourceId::new("s1"),
            source_type: SourceType::Markdown,
            project: query.project,
            text: "x".to_owned(),
            position: 0,
            created_at: 1,
            updated_at: None,
            provenance_metadata: None,
            credibility_score: None,
            graph_linkage: None,
            embedding: None,
            content_hash: None,
            entities: vec!["acme".to_owned()],
            embedding_model_id: None,
            needs_reembed: false,
        };
        Ok(RetrievalResponse {
            results: vec![RetrievalResult {
                chunk,
                score: self.sentinel,
                breakdown: ScoringBreakdown {
                    semantic_relevance: 0.5,
                    lexical_relevance: 0.0,
                    freshness_decay: 0.0,
                    staleness_penalty: 0.0,
                    source_credibility: self.sentinel,
                    corroboration: self.sentinel,
                    graph_proximity: self.sentinel,
                    recency_of_use: None,
                },
            }],
            diagnostics: RetrievalDiagnostics {
                mode_used: RetrievalMode::Hybrid,
                reranker_used: RerankerStrategy::None,
                candidates_generated: 1,
                results_returned: 1,
                latency_ms: 0,
                stages_used: vec![],
                scoring_dimensions_used: vec!["semantic_relevance".to_owned()],
                effective_policy: None,
            },
        })
    }
}

async fn empty_graph() -> Arc<InMemoryGraphStore> {
    let g = Arc::new(InMemoryGraphStore::new());
    g.add_node(GraphNode {
        node_id: "d1".to_owned(),
        kind: NodeKind::Session,
        project: None,
        created_at: 1,
    })
    .await
    .unwrap();
    g
}

#[tokio::test]
async fn cairn_default_rescorer_overwrites_runtime_owned_dims() {
    let sentinel: f64 = 0.99;
    let default = Arc::new(SentinelRetrieval { sentinel });
    let graph = empty_graph().await;
    let rescorer = PostHocRescorer::new(graph, NoOpCredibilityLookup);
    let svc = MultiProviderRetrieval::new(
        default,
        FixedResolver(cairn_domain::ProviderRef::new("cairn-default")),
        InertDispatcher,
    )
    .with_response_hook(rescorer);

    let resp = svc
        .query(RetrievalQuery {
            project: sample_project(),
            query_text: "anything".to_owned(),
            mode: RetrievalMode::Hybrid,
            reranker: RerankerStrategy::None,
            limit: 5,
            metadata_filters: vec![],
            scoring_policy: None,
        })
        .await
        .expect("query succeeds");

    check_runtime_owned_overwritten(&resp, sentinel)
        .expect("runtime-owned dims must not carry the provider sentinel");
}

// ─── 5. Error-shape stability ─────────────────────────────────────────────

#[test]
fn cairn_default_error_shapes_stable() {
    check_error_shape_stability().expect("error Display shapes are stable");
}

// ─── 6. Diagnostics computed_by markers ──────────────────────────────────

#[tokio::test]
async fn cairn_default_rescorer_appends_runtime_computed_markers() {
    let default = Arc::new(SentinelRetrieval { sentinel: 0.5 });
    let graph = empty_graph().await;
    let rescorer = PostHocRescorer::new(graph, NoOpCredibilityLookup);
    let svc = MultiProviderRetrieval::new(
        default,
        FixedResolver(cairn_domain::ProviderRef::new("cairn-default")),
        InertDispatcher,
    )
    .with_response_hook(rescorer);

    let resp = svc
        .query(RetrievalQuery {
            project: sample_project(),
            query_text: "anything".to_owned(),
            mode: RetrievalMode::Hybrid,
            reranker: RerankerStrategy::None,
            limit: 5,
            metadata_filters: vec![],
            scoring_policy: None,
        })
        .await
        .expect("query succeeds");

    check_diagnostics_computed_by_markers(&resp)
        .expect("diagnostics must carry runtime_post_hoc markers");
}

/// Sanity check on the snapshot-for-provider_ref helper — not a
/// compliance check per se, but verifies the suite's fixture
/// assumptions about cairn-default's surfaced dimensions are accurate.
#[test]
fn cairn_default_snapshot_surfaces_expected_dims() {
    let snap = cairn_memory::event_log_resolver::snapshot_for_provider_ref(
        &cairn_domain::ProviderRef::new("cairn-default"),
    )
    .expect("cairn-default must have a snapshot");
    assert!(snap.ingest_capable, "cairn-default must be ingest-capable");
    for dim in [
        "semantic_relevance",
        "lexical_relevance",
        "freshness_decay",
        "staleness_penalty",
        "recency_of_use",
    ] {
        assert!(
            snap.scoring_dimensions_surfaced.iter().any(|d| d == dim),
            "cairn-default must surface {dim}"
        );
    }
    // Build a ResolvedProviderSnapshot to confirm the shape compiles
    // (compile-time sanity for ResolvedProviderSnapshot as constructed
    // here — tests in crates below already verify deserialisation).
    let _ignored = ResolvedProviderSnapshot {
        provider_id: snap.provider_id.clone(),
        ingest_capable: snap.ingest_capable,
        retrieval_modes: snap.retrieval_modes.clone(),
        scoring_dimensions_surfaced: snap.scoring_dimensions_surfaced.clone(),
    };
}
