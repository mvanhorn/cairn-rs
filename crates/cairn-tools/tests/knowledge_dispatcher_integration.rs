//! RFC 029: integration test for the `StdioKnowledgeDispatcher`
//! behind `MultiProviderRetrieval`. Confirms the dispatcher plugs in
//! at the right seam and surfaces unregistered-plugin errors as
//! `RetrievalError::ProviderUnavailable` — the contract the runtime
//! relies on to emit `KnowledgeProviderUnavailable` for operator
//! audit.
//!
//! Runs end-to-end through `MultiProviderRetrieval::query` so the
//! error type mapping (plugin `Unavailable` → retrieval
//! `ProviderUnavailable`) is actually exercised.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cairn_domain::{ProjectKey, ProviderRef};
use cairn_memory::multi_provider::{
    MultiProviderRetrieval, ProviderResolver, ProviderResolverError,
};
use cairn_memory::retrieval::{
    RerankerStrategy, RetrievalError, RetrievalMode, RetrievalQuery, RetrievalResponse,
    RetrievalService,
};
use cairn_tools::knowledge_dispatcher::StdioKnowledgeDispatcher;
use cairn_tools::plugin_host::StdioPluginHost;

struct FixedResolver(ProviderRef);

#[async_trait]
impl ProviderResolver for FixedResolver {
    async fn resolve(&self, _project: &ProjectKey) -> Result<ProviderRef, ProviderResolverError> {
        Ok(self.0.clone())
    }
}

struct InertRetrieval;

#[async_trait]
impl RetrievalService for InertRetrieval {
    async fn query(&self, _q: RetrievalQuery) -> Result<RetrievalResponse, RetrievalError> {
        panic!("cairn-default path must not be hit on a plugin route")
    }
}

#[tokio::test]
async fn plugin_route_against_unregistered_plugin_surfaces_provider_unavailable() {
    let host = Arc::new(Mutex::new(StdioPluginHost::new()));
    let dispatcher = StdioKnowledgeDispatcher::new(host);
    let svc = MultiProviderRetrieval::new(
        Arc::new(InertRetrieval),
        FixedResolver(ProviderRef::new("plugin:not_installed")),
        dispatcher,
    );

    let err = svc
        .query(RetrievalQuery {
            project: ProjectKey::new("t", "w", "p"),
            query_text: "anything".into(),
            mode: RetrievalMode::Hybrid,
            reranker: RerankerStrategy::None,
            limit: 5,
            metadata_filters: vec![],
            scoring_policy: None,
        })
        .await
        .expect_err("unregistered plugin must error");

    match err {
        RetrievalError::ProviderUnavailable { provider, reason } => {
            assert_eq!(
                provider, "plugin:not_installed",
                "provider must name the ref verbatim"
            );
            assert!(
                reason.contains("not registered"),
                "reason must name the cause (got: {reason})"
            );
        }
        other => panic!("expected ProviderUnavailable, got {other:?}"),
    }
}
