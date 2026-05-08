//! RFC 029 PR-B2: runtime post-hoc rescorer.
//!
//! Every `KnowledgeQueryResult` flowing through `MultiProviderRetrieval`
//! is passed through a rescorer before reaching the agent. The rescorer
//! replaces the three runtime-owned scoring dimensions —
//! `graph_proximity`, `source_credibility`, `corroboration` — with
//! values cairn computes itself, then recomputes the final `score` per
//! the project's `ScoringPolicy`.
//!
//! Invariants:
//!
//! 1. Runtime-owned dimensions carried by the provider response are
//!    ALWAYS overwritten. A buggy or malicious plugin populating those
//!    fields has no influence on the final score (compliance test 6).
//! 2. Graph neighbor data is fetched in a single batched
//!    `GraphQueryService::multi_neighbors` call — N chunks cost one
//!    round-trip, not N (compliance test 7).
//! 3. `source_credibility` comes from the credibility projection
//!    batched by `source_id`; unknown sources get the default
//!    `source_credibility = chunk.credibility_score.unwrap_or(0.0)`.
//! 4. `corroboration` is intra-response: per chunk, the fraction of
//!    other chunks in the response sharing at least one entity with
//!    it. This is deliberately cheap — cross-query / per-project
//!    history is a follow-up.
//! 5. Final score uses `compute_final_score` with the supplied
//!    `ScoringPolicy`. When no policy is supplied the default is used.
//!
//! `computed_by` markers on diagnostics indicate which dimensions came
//! from the runtime vs. the provider — appended to
//! `scoring_dimensions_used` so operator UI can render per-dimension
//! provenance.

use async_trait::async_trait;
use cairn_graph::GraphQueryService;

use crate::multi_provider::ResponseHook;
use crate::retrieval::{compute_final_score, RetrievalError, RetrievalResponse, ScoringPolicy};

/// Fetches per-source credibility scores in a single batched lookup.
///
/// The trait is generic so the app layer can plug in whatever
/// projection holds `source_quality` / `credibility_score` rows (today
/// cairn-memory's `InMemoryDiagnostics`; a pg-backed projection lands
/// with the memory follow-on).
#[async_trait]
pub trait SourceCredibilityLookup: Send + Sync {
    /// Given a list of source ids, return the credibility score per
    /// source in the 0..=1 range. Sources not found in the projection
    /// are omitted — the rescorer treats absence as "no signal" and
    /// falls back to the per-chunk `credibility_score` on the
    /// `ChunkRecord` itself.
    async fn lookup(&self, source_ids: &[String]) -> Result<Vec<(String, f64)>, RescorerError>;
}

/// Errors from the rescorer path.
#[derive(Debug)]
pub enum RescorerError {
    Graph(String),
    Credibility(String),
    Internal(String),
}

impl std::fmt::Display for RescorerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Graph(m) => write!(f, "graph query failed: {m}"),
            Self::Credibility(m) => write!(f, "credibility lookup failed: {m}"),
            Self::Internal(m) => write!(f, "rescorer internal error: {m}"),
        }
    }
}

impl std::error::Error for RescorerError {}

/// Runtime-owned rescorer. Generic over a `GraphQueryService` (for
/// `multi_neighbors`) and a `SourceCredibilityLookup` so app-layer
/// wiring can pick concrete impls; tests drop in fakes.
pub struct PostHocRescorer<G, C> {
    graph: G,
    credibility: C,
}

impl<G, C> PostHocRescorer<G, C> {
    pub fn new(graph: G, credibility: C) -> Self {
        Self { graph, credibility }
    }
}

impl<G, C> PostHocRescorer<G, C>
where
    G: GraphQueryService,
    C: SourceCredibilityLookup,
{
    /// Replace runtime-owned dimensions + recompute final scores.
    /// `response` is consumed and returned to make the invariant
    /// explicit: no caller sees a half-rescored response. Scoring
    /// policy is supplied by the caller; when `None`, the default
    /// `ScoringPolicy::default()` applies.
    pub async fn rescore(
        &self,
        mut response: RetrievalResponse,
        policy: Option<&ScoringPolicy>,
    ) -> Result<RetrievalResponse, RescorerError> {
        if response.results.is_empty() {
            return Ok(response);
        }

        // Step 1: unconditional discard of provider-returned runtime-
        // owned dimensions. Any value the provider populated is
        // thrown away — cairn is the sole source of truth for these
        // three dims, full stop.
        for r in response.results.iter_mut() {
            r.breakdown.graph_proximity = 0.0;
            r.breakdown.source_credibility = 0.0;
            r.breakdown.corroboration = 0.0;
        }

        // Step 2: batched graph neighbor lookup — one round-trip for
        // every chunk in the response.
        let doc_ids: Vec<String> = response
            .results
            .iter()
            .map(|r| r.chunk.document_id.as_str().to_owned())
            .collect();
        let neighbor_rows = self
            .graph
            .multi_neighbors(&doc_ids)
            .await
            .map_err(|e| RescorerError::Graph(e.to_string()))?;

        let max_neighbors = neighbor_rows
            .iter()
            .map(|(_, edges)| edges.len())
            .max()
            .unwrap_or(0);

        // Step 3: compute graph_proximity per chunk. Simple normalized
        // neighbor-count: chunks with more graph connections are more
        // central to the tenant's knowledge graph, which correlates
        // with being a better retrieval hit. Divide by the batch max
        // so the range stays in [0, 1]. Deliberately cheap — more
        // sophisticated graph-proximity (PageRank-style) is a
        // follow-up that keeps the same invariant (runtime-computed).
        if max_neighbors > 0 {
            for (r, (_id, edges)) in response.results.iter_mut().zip(neighbor_rows.iter()) {
                r.breakdown.graph_proximity = (edges.len() as f64) / (max_neighbors as f64);
            }
        }

        // Step 4: batched source-credibility lookup. One round-trip
        // keyed on the unique source_ids across the response.
        let source_ids: Vec<String> = {
            let mut seen = std::collections::HashSet::new();
            response
                .results
                .iter()
                .map(|r| r.chunk.source_id.as_str().to_owned())
                .filter(|s| seen.insert(s.clone()))
                .collect()
        };
        let credibility_scores: std::collections::HashMap<String, f64> = self
            .credibility
            .lookup(&source_ids)
            .await
            .map_err(|e| RescorerError::Credibility(e.to_string()))?
            .into_iter()
            .collect();

        for r in response.results.iter_mut() {
            let source = r.chunk.source_id.as_str();
            r.breakdown.source_credibility = credibility_scores
                .get(source)
                .copied()
                .or(r.chunk.credibility_score)
                .unwrap_or(0.0)
                .clamp(0.0, 1.0);
        }

        // Step 5: intra-response corroboration. Per chunk, the fraction
        // of other chunks in the response that share at least one
        // entity with it. Single chunks score 0.0 (no peers). Zero-
        // entity chunks score 0.0 (no signal).
        let entity_sets: Vec<std::collections::HashSet<String>> = response
            .results
            .iter()
            .map(|r| r.chunk.entities.iter().cloned().collect())
            .collect();
        if entity_sets.len() > 1 {
            for i in 0..response.results.len() {
                if entity_sets[i].is_empty() {
                    continue;
                }
                let peers = entity_sets.len() - 1;
                let overlaps: usize = entity_sets
                    .iter()
                    .enumerate()
                    .filter(|(j, other)| {
                        *j != i && other.intersection(&entity_sets[i]).next().is_some()
                    })
                    .count();
                response.results[i].breakdown.corroboration = overlaps as f64 / peers as f64;
            }
        }

        // Step 6: final score per scoring policy. The policy weights
        // include both provider-surfaced and runtime-owned dimensions;
        // the scorer treats the breakdown as a whole after our
        // overwrite, so provider values for semantic/lexical/freshness
        // stay authoritative while graph/credibility/corroboration are
        // cairn's.
        let default_policy = ScoringPolicy::default();
        let effective = policy.unwrap_or(&default_policy);
        for r in response.results.iter_mut() {
            r.score = compute_final_score(&r.breakdown, &effective.weights);
        }

        // Resort by final score descending (the provider may have
        // sorted by its own score; runtime-owned dims shift the order).
        response.results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Diagnostics: mark which runtime-owned dimensions the runtime
        // actually populated on this response. `computed_by =
        // runtime_post_hoc` markers let operator UI show per-dimension
        // provenance.
        append_runtime_computed_markers(&mut response.diagnostics.scoring_dimensions_used);

        Ok(response)
    }
}

fn append_runtime_computed_markers(dims: &mut Vec<String>) {
    for dim in [
        "graph_proximity:runtime_post_hoc",
        "source_credibility:runtime_post_hoc",
        "corroboration:runtime_post_hoc",
    ] {
        if !dims.iter().any(|d| d == dim) {
            dims.push(dim.to_owned());
        }
    }
}

#[async_trait]
impl<G, C> ResponseHook for PostHocRescorer<G, C>
where
    G: GraphQueryService,
    C: SourceCredibilityLookup,
{
    async fn apply(
        &self,
        response: RetrievalResponse,
    ) -> Result<RetrievalResponse, RetrievalError> {
        // RFC 029 PR-B2: the rescorer is the only hook wired in
        // production. `policy = None` falls back to the default
        // scoring policy; per-project scoring policies flow in once
        // the scoring-policy endpoint (landing next) can thread a
        // stored policy through the resolver.
        self.rescore(response, None)
            .await
            .map_err(|e| RetrievalError::Internal(e.to_string()))
    }
}

// ─── Helper: a no-op credibility lookup so wiring sites that don't
// ─── yet have a credibility projection can still compose.
pub struct NoOpCredibilityLookup;

#[async_trait]
impl SourceCredibilityLookup for NoOpCredibilityLookup {
    async fn lookup(&self, _source_ids: &[String]) -> Result<Vec<(String, f64)>, RescorerError> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::{ChunkRecord, SourceType};
    use crate::retrieval::{
        RerankerStrategy, RetrievalDiagnostics, RetrievalMode, RetrievalResult, ScoringBreakdown,
    };
    use cairn_domain::{ChunkId, KnowledgeDocumentId, ProjectKey, SourceId};
    use cairn_graph::in_memory::InMemoryGraphStore;
    use cairn_graph::projections::{EdgeKind, GraphEdge, GraphNode, GraphProjection, NodeKind};
    use std::sync::Arc;

    fn proj() -> ProjectKey {
        ProjectKey::new("t", "w", "p")
    }

    fn chunk(doc: &str, source: &str, entities: Vec<&str>) -> ChunkRecord {
        ChunkRecord {
            chunk_id: ChunkId::new(format!("c_{doc}")),
            document_id: KnowledgeDocumentId::new(doc),
            source_id: SourceId::new(source),
            source_type: SourceType::Markdown,
            project: proj(),
            text: format!("text for {doc}"),
            position: 0,
            created_at: 1,
            updated_at: None,
            provenance_metadata: None,
            credibility_score: None,
            graph_linkage: None,
            embedding: None,
            content_hash: None,
            entities: entities.into_iter().map(|s| s.to_owned()).collect(),
            embedding_model_id: None,
            needs_reembed: false,
        }
    }

    fn breakdown_with_provider_populated_runtime_dims() -> ScoringBreakdown {
        // Simulate a malicious/buggy plugin that populated the three
        // runtime-owned dims. The rescorer must throw these values away.
        ScoringBreakdown {
            semantic_relevance: 0.5,
            lexical_relevance: 0.3,
            freshness_decay: 0.7,
            staleness_penalty: 0.0,
            source_credibility: 0.99,
            corroboration: 0.99,
            graph_proximity: 0.99,
            recency_of_use: None,
        }
    }

    fn sample_response(chunks: Vec<ChunkRecord>) -> RetrievalResponse {
        let results = chunks
            .into_iter()
            .map(|c| RetrievalResult {
                chunk: c,
                score: 0.5,
                breakdown: breakdown_with_provider_populated_runtime_dims(),
            })
            .collect();
        RetrievalResponse {
            results,
            diagnostics: RetrievalDiagnostics {
                mode_used: RetrievalMode::Hybrid,
                reranker_used: RerankerStrategy::None,
                candidates_generated: 0,
                results_returned: 0,
                latency_ms: 0,
                stages_used: vec![],
                scoring_dimensions_used: vec!["semantic_relevance".to_owned()],
                effective_policy: None,
            },
        }
    }

    struct CountingGraph(Arc<InMemoryGraphStore>, Arc<std::sync::Mutex<usize>>);

    #[async_trait]
    impl GraphQueryService for CountingGraph {
        async fn query(
            &self,
            q: cairn_graph::queries::GraphQuery,
        ) -> Result<cairn_graph::queries::Subgraph, cairn_graph::queries::GraphQueryError> {
            self.0.query(q).await
        }
        async fn neighbors(
            &self,
            node_id: &str,
            edge_filter: Option<EdgeKind>,
            direction: cairn_graph::queries::TraversalDirection,
            limit: usize,
        ) -> Result<Vec<(GraphEdge, GraphNode)>, cairn_graph::queries::GraphQueryError> {
            self.0
                .neighbors(node_id, edge_filter, direction, limit)
                .await
        }
        async fn find_edges_by_source(
            &self,
            src: &str,
            f: Option<EdgeKind>,
            lim: usize,
        ) -> Result<Vec<GraphEdge>, cairn_graph::queries::GraphQueryError> {
            self.0.find_edges_by_source(src, f, lim).await
        }
        async fn find_edges_by_target(
            &self,
            tgt: &str,
            f: Option<EdgeKind>,
            lim: usize,
        ) -> Result<Vec<GraphEdge>, cairn_graph::queries::GraphQueryError> {
            self.0.find_edges_by_target(tgt, f, lim).await
        }
        async fn shortest_path(
            &self,
            a: &str,
            b: &str,
            f: Option<EdgeKind>,
            d: u32,
        ) -> Result<Option<cairn_graph::queries::Subgraph>, cairn_graph::queries::GraphQueryError>
        {
            self.0.shortest_path(a, b, f, d).await
        }
        async fn multi_neighbors(
            &self,
            node_ids: &[String],
        ) -> Result<Vec<(String, Vec<GraphEdge>)>, cairn_graph::queries::GraphQueryError> {
            *self.1.lock().unwrap() += 1;
            self.0.multi_neighbors(node_ids).await
        }
    }

    async fn build_graph() -> Arc<InMemoryGraphStore> {
        let g = Arc::new(InMemoryGraphStore::new());
        for id in ["d1", "d2", "d3", "x1", "x2"] {
            g.add_node(GraphNode {
                node_id: id.to_owned(),
                kind: NodeKind::Session,
                project: None,
                created_at: 1,
            })
            .await
            .unwrap();
        }
        // d1 — 2 neighbors, d2 — 1 neighbor, d3 — 0 neighbors.
        for (s, t) in [("d1", "x1"), ("d1", "x2"), ("d2", "x1")] {
            g.add_edge(GraphEdge {
                source_node_id: s.to_owned(),
                target_node_id: t.to_owned(),
                kind: EdgeKind::Triggered,
                created_at: 1,
                confidence: None,
            })
            .await
            .unwrap();
        }
        g
    }

    #[tokio::test]
    async fn rescorer_overwrites_provider_runtime_owned_dims() {
        // Compliance test 6: plugin-injected graph_proximity/source_credibility/
        // corroboration are unconditionally replaced.
        let g = build_graph().await;
        let count = Arc::new(std::sync::Mutex::new(0));
        let rescorer = PostHocRescorer::new(CountingGraph(g, count.clone()), NoOpCredibilityLookup);
        let response = sample_response(vec![
            chunk("d1", "s1", vec!["acme"]),
            chunk("d2", "s2", vec!["acme", "beta"]),
            chunk("d3", "s3", vec!["gamma"]),
        ]);
        let rescored = rescorer.rescore(response, None).await.unwrap();
        for r in &rescored.results {
            // graph_proximity: d1 has 2, d2 has 1, d3 has 0 → normalised.
            // Plugin's 0.99 value must have been discarded in all cases.
            assert_ne!(
                r.breakdown.graph_proximity, 0.99,
                "provider value must never survive on any result"
            );
            assert_ne!(r.breakdown.source_credibility, 0.99);
            assert_ne!(r.breakdown.corroboration, 0.99);
        }
    }

    #[tokio::test]
    async fn rescorer_issues_exactly_one_multi_neighbors_call_for_ten_chunks() {
        // Compliance test 7: 10-chunk response → exactly one
        // multi_neighbors round-trip, not 10.
        let g = build_graph().await;
        let count = Arc::new(std::sync::Mutex::new(0));
        let rescorer = PostHocRescorer::new(CountingGraph(g, count.clone()), NoOpCredibilityLookup);
        let chunks: Vec<_> = (0..10)
            .map(|i| chunk(&format!("d_{i}"), &format!("s_{i}"), vec!["acme"]))
            .collect();
        let response = sample_response(chunks);
        rescorer.rescore(response, None).await.unwrap();
        assert_eq!(*count.lock().unwrap(), 1, "must be exactly one call");
    }

    #[tokio::test]
    async fn rescorer_graph_proximity_reflects_neighbor_count() {
        let g = build_graph().await;
        let count = Arc::new(std::sync::Mutex::new(0));
        let rescorer = PostHocRescorer::new(CountingGraph(g, count.clone()), NoOpCredibilityLookup);
        let response = sample_response(vec![
            chunk("d1", "s1", vec![]),
            chunk("d2", "s2", vec![]),
            chunk("d3", "s3", vec![]),
        ]);
        let rescored = rescorer.rescore(response, None).await.unwrap();
        // Find each by document_id since rescorer sorts by final score.
        let by_doc: std::collections::HashMap<String, f64> = rescored
            .results
            .iter()
            .map(|r| {
                (
                    r.chunk.document_id.as_str().to_owned(),
                    r.breakdown.graph_proximity,
                )
            })
            .collect();
        assert_eq!(by_doc["d1"], 1.0, "d1 has max neighbors → 1.0");
        assert_eq!(by_doc["d2"], 0.5, "d2 has half of max → 0.5");
        assert_eq!(by_doc["d3"], 0.0, "d3 has no neighbors → 0.0");
    }

    #[tokio::test]
    async fn rescorer_corroboration_is_fraction_of_entity_overlapping_peers() {
        let g = build_graph().await;
        let count = Arc::new(std::sync::Mutex::new(0));
        let rescorer = PostHocRescorer::new(CountingGraph(g, count.clone()), NoOpCredibilityLookup);
        // 3 chunks: d1 shares "acme" with d2 and "beta" with d3; d2 only
        // with d1; d3 only with d1.
        let response = sample_response(vec![
            chunk("d1", "s1", vec!["acme", "beta"]),
            chunk("d2", "s2", vec!["acme"]),
            chunk("d3", "s3", vec!["beta"]),
        ]);
        let rescored = rescorer.rescore(response, None).await.unwrap();
        let by_doc: std::collections::HashMap<String, f64> = rescored
            .results
            .iter()
            .map(|r| {
                (
                    r.chunk.document_id.as_str().to_owned(),
                    r.breakdown.corroboration,
                )
            })
            .collect();
        assert_eq!(by_doc["d1"], 1.0, "d1 overlaps with both peers");
        assert_eq!(by_doc["d2"], 0.5, "d2 overlaps with one of two peers");
        assert_eq!(by_doc["d3"], 0.5, "d3 overlaps with one of two peers");
    }

    #[tokio::test]
    async fn rescorer_diagnostics_carry_runtime_computed_markers() {
        let g = build_graph().await;
        let count = Arc::new(std::sync::Mutex::new(0));
        let rescorer = PostHocRescorer::new(CountingGraph(g, count.clone()), NoOpCredibilityLookup);
        let response = sample_response(vec![chunk("d1", "s1", vec!["acme"])]);
        let rescored = rescorer.rescore(response, None).await.unwrap();
        let dims = &rescored.diagnostics.scoring_dimensions_used;
        for marker in [
            "graph_proximity:runtime_post_hoc",
            "source_credibility:runtime_post_hoc",
            "corroboration:runtime_post_hoc",
        ] {
            assert!(
                dims.iter().any(|d| d == marker),
                "expected marker {marker} in diagnostics"
            );
        }
    }

    #[tokio::test]
    async fn rescorer_empty_response_is_passthrough() {
        let g = build_graph().await;
        let count = Arc::new(std::sync::Mutex::new(0));
        let rescorer = PostHocRescorer::new(CountingGraph(g, count.clone()), NoOpCredibilityLookup);
        let response = RetrievalResponse {
            results: vec![],
            diagnostics: RetrievalDiagnostics {
                mode_used: RetrievalMode::Hybrid,
                reranker_used: RerankerStrategy::None,
                candidates_generated: 0,
                results_returned: 0,
                latency_ms: 0,
                stages_used: vec![],
                scoring_dimensions_used: vec![],
                effective_policy: None,
            },
        };
        let out = rescorer.rescore(response, None).await.unwrap();
        assert!(out.results.is_empty());
        assert_eq!(*count.lock().unwrap(), 0, "no call on empty input");
    }

    struct FakeCredibility(Vec<(String, f64)>);

    #[async_trait]
    impl SourceCredibilityLookup for FakeCredibility {
        async fn lookup(&self, source_ids: &[String]) -> Result<Vec<(String, f64)>, RescorerError> {
            let wanted: std::collections::HashSet<&str> =
                source_ids.iter().map(String::as_str).collect();
            Ok(self
                .0
                .iter()
                .filter(|(s, _)| wanted.contains(s.as_str()))
                .cloned()
                .collect())
        }
    }

    #[tokio::test]
    async fn rescorer_source_credibility_uses_projection_when_available() {
        let g = build_graph().await;
        let count = Arc::new(std::sync::Mutex::new(0));
        let cred = FakeCredibility(vec![("s1".to_owned(), 0.8), ("s2".to_owned(), 0.4)]);
        let rescorer = PostHocRescorer::new(CountingGraph(g, count.clone()), cred);
        let response = sample_response(vec![
            chunk("d1", "s1", vec![]),
            chunk("d2", "s2", vec![]),
            chunk("d3", "s_missing", vec![]),
        ]);
        let rescored = rescorer.rescore(response, None).await.unwrap();
        let by_doc: std::collections::HashMap<String, f64> = rescored
            .results
            .iter()
            .map(|r| {
                (
                    r.chunk.document_id.as_str().to_owned(),
                    r.breakdown.source_credibility,
                )
            })
            .collect();
        assert_eq!(by_doc["d1"], 0.8);
        assert_eq!(by_doc["d2"], 0.4);
        assert_eq!(
            by_doc["d3"], 0.0,
            "unknown source + no per-chunk credibility → 0.0"
        );
    }
}
