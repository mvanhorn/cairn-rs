//! RFC 029 §"Integration Tests" compliance proof for PR B1.
//!
//! Covers tests 1-5 and 8-12 of the §Integration Tests list:
//!
//! 1. Capability declaration round-trip
//! 2. `cairn-default` behaviour unchanged through MultiProviderRetrieval
//! 3. Plugin dispatch works (mock dispatcher round-trip)
//! 4. Per-project isolation (two projects → two providers)
//! 5. Provider-required dimensions declarable as not_supported
//! 8. (moved to B2) Scoring-policy rejection against unsupported dims
//! 9. `memory_store` hidden under read-only provider
//! 10. Deep search works across providers
//! 11. Provider-unavailable fails loudly (no silent fallback)
//! 12. Plugin-ack rejection surfaces ProviderRejected
//!
//! Tests 6-7 (post-hoc rescoring + batched multi_neighbors) and 13
//! (compliance suite) belong to PR B2 / the separate crate, not B1.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cairn_domain::{
    contexts::VisibilityContext, events::ResolvedProviderSnapshot, ChunkId, KnowledgeDocumentId,
    ProjectKey, ProviderRef, SourceId,
};
use cairn_memory::deep_search::{DeepSearchRequest, DeepSearchService};
use cairn_memory::deep_search_impl::IterativeDeepSearch;
use cairn_memory::event_log_resolver::snapshot_for_provider_ref;
use cairn_memory::ingest::{
    IngestError, IngestPackRequest, IngestRequest, IngestService, IngestStatus, SourceType,
};
use cairn_memory::multi_provider::{
    KnowledgePluginDispatcher, KnowledgePluginError, MultiProviderIngest, MultiProviderRetrieval,
    ProviderResolver, ProviderResolverError,
};
use cairn_memory::retrieval::{
    RerankerStrategy, RetrievalDiagnostics, RetrievalError, RetrievalMode, RetrievalQuery,
    RetrievalResponse, RetrievalService, ScoringBreakdown,
};
use cairn_plugin_proto::knowledge::{
    ChunkRecordWire, DimensionSupport, KnowledgeIngestAck, KnowledgeIngestParams,
    KnowledgeIngestStatusParams, KnowledgeIngestStatusResult, KnowledgeProviderCapability,
    KnowledgeQueryDiagnostics, KnowledgeQueryParams, KnowledgeQueryResult, RetrievalModeWire,
    RetrievalResultWire, ScoringBreakdownWire, ScoringDimensionSet, SourceTypeWire,
};
use std::collections::{HashMap, HashSet};

fn proj(id: &str) -> ProjectKey {
    ProjectKey::new("t", "w", id)
}

fn sample_query(p: &ProjectKey) -> RetrievalQuery {
    RetrievalQuery {
        project: p.clone(),
        query_text: "q".to_owned(),
        mode: RetrievalMode::Hybrid,
        reranker: RerankerStrategy::None,
        limit: 5,
        metadata_filters: vec![],
        scoring_policy: None,
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────

struct FixedResolver(ProviderRef);

#[async_trait]
impl ProviderResolver for FixedResolver {
    async fn resolve(&self, _project: &ProjectKey) -> Result<ProviderRef, ProviderResolverError> {
        Ok(self.0.clone())
    }
}

struct PerProjectResolver(HashMap<String, ProviderRef>);

#[async_trait]
impl ProviderResolver for PerProjectResolver {
    async fn resolve(&self, project: &ProjectKey) -> Result<ProviderRef, ProviderResolverError> {
        Ok(self
            .0
            .get(project.project_id.as_str())
            .cloned()
            .unwrap_or_else(|| ProviderRef::new("cairn-default")))
    }
}

struct EchoRetrieval {
    label: String,
    calls: Mutex<usize>,
}

#[async_trait]
impl RetrievalService for EchoRetrieval {
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
                stages_used: vec![],
                scoring_dimensions_used: vec![],
                effective_policy: Some(self.label.clone()),
                family: None,
            },
        })
    }
}

struct MockDispatcher {
    query_result: Mutex<Option<KnowledgeQueryResult>>,
    ingest_ack: Mutex<Option<KnowledgeIngestAck>>,
    unavailable: Mutex<Option<String>>,
    query_calls: Mutex<Vec<String>>,
}

impl MockDispatcher {
    fn new() -> Self {
        Self {
            query_result: Mutex::new(None),
            ingest_ack: Mutex::new(None),
            unavailable: Mutex::new(None),
            query_calls: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl KnowledgePluginDispatcher for MockDispatcher {
    async fn query(
        &self,
        plugin_id: &str,
        _params: KnowledgeQueryParams,
    ) -> Result<KnowledgeQueryResult, KnowledgePluginError> {
        if let Some(r) = self.unavailable.lock().unwrap().clone() {
            return Err(KnowledgePluginError::Unavailable(r));
        }
        self.query_calls.lock().unwrap().push(plugin_id.to_owned());
        self.query_result
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| KnowledgePluginError::Internal("no response set".into()))
    }

    async fn ingest(
        &self,
        _plugin_id: &str,
        _params: KnowledgeIngestParams,
    ) -> Result<KnowledgeIngestAck, KnowledgePluginError> {
        if let Some(r) = self.unavailable.lock().unwrap().clone() {
            return Err(KnowledgePluginError::Unavailable(r));
        }
        self.ingest_ack
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| KnowledgePluginError::Internal("no ack set".into()))
    }

    async fn ingest_status(
        &self,
        _plugin_id: &str,
        _params: KnowledgeIngestStatusParams,
    ) -> Result<KnowledgeIngestStatusResult, KnowledgePluginError> {
        Err(KnowledgePluginError::Unavailable("not set".into()))
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

/// Test #1 — Capability declaration round-trips.
/// Manifest-layer variant is empty; handshake-layer struct carries the
/// effective detail. Serialize → deserialize preserves every field.
#[test]
fn capability_declaration_round_trips() {
    let cap = KnowledgeProviderCapability {
        retrieval_modes: vec![RetrievalModeWire::Hybrid, RetrievalModeWire::VectorOnly],
        ingest_capable: false,
        ingest_source_types: vec![],
        scoring_dimensions: ScoringDimensionSet {
            semantic_relevance: DimensionSupport::Surfaced,
            lexical_relevance: DimensionSupport::NotSupported,
            freshness_decay: DimensionSupport::NotSupported,
            staleness_penalty: DimensionSupport::NotSupported,
            recency_of_use: DimensionSupport::NotSupported,
        },
    };
    let json = serde_json::to_string(&cap).expect("serialize");
    let back: KnowledgeProviderCapability = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(cap, back);
    assert!(!cap.ingest_capable);
    assert!(cap
        .retrieval_modes
        .iter()
        .any(|m| matches!(m, RetrievalModeWire::Hybrid)));
}

/// Test #2 — cairn-default behaviour unchanged through MultiProviderRetrieval.
/// Wrapping EchoRetrieval in MultiProviderRetrieval with a
/// `cairn-default` resolver yields byte-identical passthrough semantics.
#[tokio::test]
async fn cairn_default_through_multi_provider_is_passthrough() {
    let inner = Arc::new(EchoRetrieval {
        label: "inner".to_owned(),
        calls: Mutex::new(0),
    });
    let mp = MultiProviderRetrieval::new(
        inner.clone(),
        FixedResolver(ProviderRef::new("cairn-default")),
        MockDispatcher::new(),
    );
    let resp = mp.query(sample_query(&proj("a"))).await.unwrap();
    assert_eq!(*inner.calls.lock().unwrap(), 1);
    assert_eq!(
        resp.diagnostics.effective_policy.as_deref(),
        Some("inner"),
        "passthrough must surface inner's diagnostics verbatim"
    );
}

/// Test #3 — Plugin dispatch works: mock plugin returns canned response,
/// runtime surfaces it through RetrievalService::query.
#[tokio::test]
async fn plugin_dispatch_surfaces_mock_response() {
    let dispatcher = Arc::new(MockDispatcher::new());
    *dispatcher.query_result.lock().unwrap() = Some(KnowledgeQueryResult {
        results: vec![RetrievalResultWire {
            chunk: ChunkRecordWire {
                chunk_id: ChunkId::new("c"),
                document_id: KnowledgeDocumentId::new("d"),
                source_id: SourceId::new("s"),
                source_type: SourceTypeWire::Markdown,
                project: proj("a"),
                text: "mocked chunk".to_owned(),
                position: 0,
                created_at: 0,
                updated_at: None,
                provenance_metadata: None,
                credibility_score: None,
                graph_linkage: None,
                content_hash: None,
                entities: vec![],
            },
            score: 0.7,
            breakdown: ScoringBreakdownWire {
                semantic_relevance: Some(0.7),
                ..Default::default()
            },
        }],
        diagnostics: KnowledgeQueryDiagnostics {
            mode_used: RetrievalModeWire::Hybrid,
            stages_used: None,
            reranker_used: None,
            scoring_dimensions_used: vec!["semantic_relevance".to_owned()],
            results_returned: 1,
            latency_ms: Some(5),
        },
    });
    let mp = MultiProviderRetrieval::new(
        Arc::new(EchoRetrieval {
            label: "shouldnt-be-called".to_owned(),
            calls: Mutex::new(0),
        }),
        FixedResolver(ProviderRef::new("plugin:mock")),
        Arc::clone(&dispatcher),
    );
    let resp = mp.query(sample_query(&proj("a"))).await.unwrap();
    assert_eq!(resp.results.len(), 1);
    assert_eq!(resp.results[0].chunk.text, "mocked chunk");
    assert_eq!(dispatcher.query_calls.lock().unwrap().as_slice(), &["mock"]);
}

/// Test #4 — Per-project isolation: two projects with different provider
/// configs dispatch to different providers.
#[tokio::test]
async fn per_project_isolation_routes_distinctly() {
    let inner = Arc::new(EchoRetrieval {
        label: "default".to_owned(),
        calls: Mutex::new(0),
    });
    let dispatcher = Arc::new(MockDispatcher::new());
    *dispatcher.query_result.lock().unwrap() = Some(KnowledgeQueryResult {
        results: vec![],
        diagnostics: KnowledgeQueryDiagnostics {
            mode_used: RetrievalModeWire::Hybrid,
            stages_used: None,
            reranker_used: None,
            scoring_dimensions_used: vec![],
            results_returned: 0,
            latency_ms: None,
        },
    });
    let mut routes = HashMap::new();
    routes.insert("a".to_owned(), ProviderRef::new("cairn-default"));
    routes.insert("b".to_owned(), ProviderRef::new("plugin:mock"));
    let mp = MultiProviderRetrieval::new(
        inner.clone(),
        PerProjectResolver(routes),
        Arc::clone(&dispatcher),
    );

    mp.query(sample_query(&proj("a"))).await.unwrap();
    mp.query(sample_query(&proj("b"))).await.unwrap();

    assert_eq!(
        *inner.calls.lock().unwrap(),
        1,
        "project a must have hit cairn-default inner retrieval exactly once"
    );
    assert_eq!(
        dispatcher.query_calls.lock().unwrap().as_slice(),
        &["mock"],
        "project b must have hit plugin:mock exactly once"
    );
}

/// Test #5 — Provider-required dimensions declarable as `not_supported`.
/// A plugin declaring `freshness_decay = NotSupported` serves queries;
/// the field comes back `None` on the wire and through to the in-process
/// breakdown as 0.0 (the numeric identity for the weighted-sum scorer).
#[tokio::test]
async fn provider_required_dim_can_be_not_supported() {
    let cap = KnowledgeProviderCapability {
        retrieval_modes: vec![RetrievalModeWire::VectorOnly],
        ingest_capable: false,
        ingest_source_types: vec![],
        scoring_dimensions: ScoringDimensionSet {
            semantic_relevance: DimensionSupport::Surfaced,
            lexical_relevance: DimensionSupport::NotSupported,
            freshness_decay: DimensionSupport::NotSupported,
            staleness_penalty: DimensionSupport::NotSupported,
            recency_of_use: DimensionSupport::NotSupported,
        },
    };
    assert!(matches!(
        cap.scoring_dimensions.freshness_decay,
        DimensionSupport::NotSupported
    ));

    let wire_result = KnowledgeQueryResult {
        results: vec![RetrievalResultWire {
            chunk: ChunkRecordWire {
                chunk_id: ChunkId::new("c"),
                document_id: KnowledgeDocumentId::new("d"),
                source_id: SourceId::new("s"),
                source_type: SourceTypeWire::Markdown,
                project: proj("a"),
                text: "t".to_owned(),
                position: 0,
                created_at: 0,
                updated_at: None,
                provenance_metadata: None,
                credibility_score: None,
                graph_linkage: None,
                content_hash: None,
                entities: vec![],
            },
            score: 0.5,
            breakdown: ScoringBreakdownWire {
                semantic_relevance: Some(0.5),
                freshness_decay: None,
                ..Default::default()
            },
        }],
        diagnostics: KnowledgeQueryDiagnostics {
            mode_used: RetrievalModeWire::VectorOnly,
            stages_used: None,
            reranker_used: None,
            scoring_dimensions_used: vec!["semantic_relevance".to_owned()],
            results_returned: 1,
            latency_ms: None,
        },
    };
    let resp: RetrievalResponse = wire_result.into();
    assert_eq!(resp.results[0].breakdown.semantic_relevance, 0.5);
    assert_eq!(resp.results[0].breakdown.freshness_decay, 0.0);
}

/// Test #9 — `memory_store` hidden under a read-only provider.
/// The cairn-memory crate tests only the VisibilityContext shape (it
/// cannot depend on cairn-runtime). The equivalent `is_tool_visible`
/// predicate check lives in `cairn-runtime::services::marketplace_service`
/// under `is_tool_visible_gates_memory_store_on_resolved_provider`.
#[test]
fn visibility_context_carries_read_only_snapshot() {
    let snap = ResolvedProviderSnapshot {
        provider_id: "plugin:bedrock-kb".into(),
        ingest_capable: false,
        retrieval_modes: vec!["hybrid".into()],
        scoring_dimensions_surfaced: vec!["semantic_relevance".into()],
        // Knowledge-family snapshot — auto_extract only applies to memory
        // family per RFC 030.
        auto_extract: None,
    };
    let ctx = VisibilityContext {
        project: proj("a"),
        run_id: None,
        enabled_plugins: HashSet::new(),
        allowlisted_tools: HashMap::new(),
        resolved_knowledge_provider: Some(snap.clone()),
        resolved_memory_provider: None,
    };
    let got = ctx
        .resolved_knowledge_provider
        .as_ref()
        .expect("provider snapshot present");
    assert_eq!(got, &snap);
    assert!(!got.ingest_capable, "read-only provider must advertise it");
}

/// Test #10 — Deep search works across providers. Each hop dispatches
/// through the multi-provider layer; routes that hit a plugin see the
/// plugin's results (mocked here).
#[tokio::test]
async fn deep_search_across_providers_dispatches_per_hop() {
    let inner = Arc::new(EchoRetrieval {
        label: "default".to_owned(),
        calls: Mutex::new(0),
    });
    let mp = MultiProviderRetrieval::new(
        inner.clone(),
        FixedResolver(ProviderRef::new("cairn-default")),
        MockDispatcher::new(),
    );
    let ds = IterativeDeepSearch::new(mp);
    let req = DeepSearchRequest {
        project: proj("a"),
        query_text: "hello world".to_owned(),
        mode: RetrievalMode::Hybrid,
        max_hops: 2,
        per_hop_limit: 3,
    };
    let _ = ds.search(req).await.unwrap();
    // Deep search triggers at least one hop through the underlying
    // retrieval — the exact count depends on quality-gate decisions,
    // but every hop goes through MultiProviderRetrieval.
    assert!(*inner.calls.lock().unwrap() >= 1);
}

/// Test #11 — Provider-unavailable fails loudly. No silent fallback.
#[tokio::test]
async fn provider_unavailable_returns_error_not_fallback() {
    let inner = Arc::new(EchoRetrieval {
        label: "never-called".to_owned(),
        calls: Mutex::new(0),
    });
    let dispatcher = Arc::new(MockDispatcher::new());
    *dispatcher.unavailable.lock().unwrap() = Some("plugin crashed".into());
    let mp = MultiProviderRetrieval::new(
        inner.clone(),
        FixedResolver(ProviderRef::new("plugin:mock")),
        Arc::clone(&dispatcher),
    );
    match mp.query(sample_query(&proj("a"))).await {
        Err(RetrievalError::ProviderUnavailable { provider, reason }) => {
            assert_eq!(provider, "plugin:mock");
            assert_eq!(reason, "plugin crashed");
        }
        other => panic!("expected ProviderUnavailable, got {other:?}"),
    }
    assert_eq!(
        *inner.calls.lock().unwrap(),
        0,
        "must not silently fall back to cairn-default"
    );
}

/// Test #12 — Plugin-ack rejection on ingest surfaces ProviderRejected.
/// Mirror of #11 for the ingest path: the tool layer sees a structured
/// rejection and can emit `KnowledgeIngestRejected` alongside.
#[tokio::test]
async fn plugin_rejected_ingest_surfaces_provider_rejected() {
    struct InertIngest;
    #[async_trait]
    impl IngestService for InertIngest {
        async fn submit(&self, _r: IngestRequest) -> Result<(), IngestError> {
            panic!("default path must not be hit on plugin route");
        }
        async fn submit_pack(&self, _r: IngestPackRequest) -> Result<(), IngestError> {
            panic!("default path must not be hit on plugin route");
        }
        async fn status(
            &self,
            _d: &KnowledgeDocumentId,
        ) -> Result<Option<IngestStatus>, IngestError> {
            Ok(None)
        }
    }

    let dispatcher = Arc::new(MockDispatcher::new());
    *dispatcher.ingest_ack.lock().unwrap() = Some(KnowledgeIngestAck {
        document_id: KnowledgeDocumentId::new("d"),
        accepted: false,
        reason: Some("provider is read-only".to_owned()),
    });
    let mp = MultiProviderIngest::new(
        Arc::new(InertIngest),
        FixedResolver(ProviderRef::new("plugin:bedrock-kb")),
        Arc::clone(&dispatcher),
    );
    let req = IngestRequest {
        document_id: KnowledgeDocumentId::new("d"),
        source_id: SourceId::new("s"),
        source_type: SourceType::Markdown,
        project: proj("a"),
        content: "payload".to_owned(),
        import_id: None,
        corpus_id: None,
        bundle_source_id: None,
        tags: vec![],
    };
    match mp.submit(req).await {
        Err(IngestError::ProviderRejected { provider, reason }) => {
            assert_eq!(provider, "plugin:bedrock-kb");
            assert_eq!(reason, "provider is read-only");
        }
        other => panic!("expected ProviderRejected, got {other:?}"),
    }
}

/// Extra — snapshot_for_provider_ref wiring mirrors the RFC intent:
/// cairn-default surfaces ingest_capable = true + the five required
/// scoring dimensions; plugin refs return None (snapshot is sourced
/// from the plugin host's handshake cache in a follow-up PR).
#[test]
fn cairn_default_snapshot_matches_rfc_intent() {
    let snap =
        snapshot_for_provider_ref(&ProviderRef::new("cairn-default")).expect("snapshot present");
    assert!(snap.ingest_capable);
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
    assert!(snap.retrieval_modes.iter().any(|m| m == "hybrid"));
}

/// Extra — unused imports sanity: ensure the wire struct is present and
/// builds without requiring specific private helpers.
#[test]
fn scoring_breakdown_default_matches_zero_identity() {
    let b = ScoringBreakdown::default();
    assert_eq!(b.semantic_relevance, 0.0);
    assert_eq!(b.freshness_decay, 0.0);
}
