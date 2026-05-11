//! RFC 029 PR-C: run the six compliance checks against the
//! plugin-dispatch path using an in-process `MockDispatcher`. Proves
//! the plugin path satisfies the same wire-shape contract without
//! needing a real subprocess.

use std::sync::Arc;

use cairn_graph::in_memory::InMemoryGraphStore;
use cairn_graph::projections::{GraphNode, GraphProjection, NodeKind};
use cairn_knowledge_compliance::fixtures::{
    full_capability, read_only_capability, sample_project, sample_query_result, MockDispatcher,
};
use cairn_knowledge_compliance::{
    check_diagnostics_computed_by_markers, check_error_shape_stability,
    check_required_field_presence, check_runtime_owned_overwritten,
    check_tri_state_matches_surfaced, check_wire_type_round_trips,
};
use cairn_memory::in_memory::{InMemoryDocumentStore, InMemoryRetrieval};
use cairn_memory::multi_provider::{
    MultiProviderRetrieval, ProviderResolver, ProviderResolverError,
};
use cairn_memory::post_hoc_rescorer::{NoOpCredibilityLookup, PostHocRescorer};
use cairn_memory::retrieval::{RerankerStrategy, RetrievalMode, RetrievalQuery, RetrievalService};

// ─── 1. Wire-type round-trips (identical to cairn-default path) ──────────

#[test]
fn mock_plugin_wire_types_round_trip() {
    check_wire_type_round_trips().expect("wire types round-trip cleanly");
}

// ─── 2. Required-field presence on mock's canned response ────────────────

#[test]
fn mock_plugin_canned_response_has_required_fields() {
    let canned = sample_query_result();
    check_required_field_presence(&canned).expect("mock response passes shape check");
}

// ─── 3. Tri-state checks on mock ─────────────────────────────────────────

#[test]
fn mock_plugin_full_capability_matches_full_response() {
    let mut cap = full_capability();
    cap.scoring_dimensions.recency_of_use =
        cairn_plugin_proto::knowledge::DimensionSupport::NotSupported;
    check_tri_state_matches_surfaced(&cap, &sample_query_result())
        .expect("mock matches full capability");
}

#[test]
fn mock_plugin_read_only_capability_catches_surfaced_only_in_fixture() {
    // read_only_capability declares only semantic_relevance = Surfaced.
    // The default sample_query_result populates lexical and
    // freshness too — so the tri-state check flags those as
    // NotSupported-but-populated. The check is working as expected.
    let cap = read_only_capability();
    let err = check_tri_state_matches_surfaced(&cap, &sample_query_result())
        .expect_err("read-only cap + full result mismatch must surface");
    assert!(
        err.reason.contains("lexical_relevance")
            || err.reason.contains("freshness_decay")
            || err.reason.contains("staleness_penalty"),
        "error must name the offending dim: {}",
        err.reason
    );
}

// ─── 4. Runtime-owned overwritten — end-to-end through MockDispatcher ───

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
async fn mock_plugin_path_rescores_runtime_owned_dims() {
    // The mock plugin populates graph_proximity/source_credibility/
    // corroboration with the sentinel 0.99 (see sample_query_result).
    // After rescoring, none of those sentinels should survive.
    let dispatcher = Arc::new(MockDispatcher::default());
    *dispatcher.next_query_result.lock().unwrap() = Some(sample_query_result());
    let default = Arc::new(InMemoryRetrieval::new(Arc::new(
        InMemoryDocumentStore::new(),
    )));
    let graph = empty_graph().await;
    let rescorer = PostHocRescorer::new(graph, NoOpCredibilityLookup);
    let svc = MultiProviderRetrieval::new(
        default,
        FixedResolver(cairn_domain::ProviderRef::new("plugin:mock")),
        Arc::clone(&dispatcher),
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
        .expect("query succeeds through mock");

    assert_eq!(
        dispatcher.query_calls.lock().unwrap().len(),
        1,
        "mock plugin dispatcher must have received the query"
    );

    check_runtime_owned_overwritten(&resp, 0.99)
        .expect("runtime-owned dims must not carry the provider sentinel after rescoring");
}

// ─── 5. Error-shape stability (identical) ────────────────────────────────

#[test]
fn mock_plugin_error_shapes_stable() {
    check_error_shape_stability().expect("error Display shapes are stable");
}

// ─── 6. Diagnostics markers on the plugin path ───────────────────────────

#[tokio::test]
async fn mock_plugin_rescorer_appends_runtime_computed_markers() {
    let dispatcher = Arc::new(MockDispatcher::default());
    *dispatcher.next_query_result.lock().unwrap() = Some(sample_query_result());
    let default = Arc::new(InMemoryRetrieval::new(Arc::new(
        InMemoryDocumentStore::new(),
    )));
    let graph = empty_graph().await;
    let rescorer = PostHocRescorer::new(graph, NoOpCredibilityLookup);
    let svc = MultiProviderRetrieval::new(
        default,
        FixedResolver(cairn_domain::ProviderRef::new("plugin:mock")),
        Arc::clone(&dispatcher),
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
        .expect("diagnostics carry runtime_post_hoc markers on the plugin path too");
}
