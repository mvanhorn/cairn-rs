//! RFC 029: `MultiProviderRetrieval` / `MultiProviderIngest` — the runtime
//! dispatch layer that sits in front of the existing in-process
//! `InMemoryRetrieval` and plugin-subprocess providers.
//!
//! Dispatch rule per RFC 029 §"Runtime resolution":
//!
//! - `cairn-default` → call through to the in-process default
//!   ([`RetrievalService`] / [`IngestService`]).
//! - `plugin:<id>` → marshal to wire params via
//!   [`crate::plugin_bridge`], dispatch to a [`KnowledgePluginDispatcher`],
//!   lift the wire response back into the in-process shape.
//!
//! There is no ambient fallback: if the configured provider is unavailable
//! (plugin not spawned, handshake failed, credentials missing, transport
//! error) the query fails with [`RetrievalError::ProviderUnavailable`].
//! The runtime is expected to emit `KnowledgeProviderUnavailable` alongside
//! returning the error — wiring of the event emit lives at the service
//! layer in the next step.
//!
//! This module is transport- and registry-agnostic on purpose. It takes
//! trait objects for both "which provider is configured for this project"
//! and "how do I dispatch a knowledge.* call to a plugin". The app-layer
//! composition point picks concrete impls (project projection + stdio
//! plugin host). B1 lands the dispatcher; B2 adds post-hoc scoring on top
//! without touching this module.

use async_trait::async_trait;
use cairn_domain::{KnowledgeDocumentId, ProjectKey, ProviderRef};
use cairn_plugin_proto::knowledge::{
    KnowledgeIngestAck, KnowledgeIngestParams, KnowledgeIngestStatusParams,
    KnowledgeIngestStatusResult, KnowledgeQueryParams, KnowledgeQueryResult,
};

use crate::ingest::{IngestError, IngestPackRequest, IngestRequest, IngestService, IngestStatus};
use crate::retrieval::{RetrievalError, RetrievalQuery, RetrievalResponse, RetrievalService};

/// The stable string identifying the in-process default provider (RFC 029).
pub const CAIRN_DEFAULT_PROVIDER_REF: &str = "cairn-default";

/// Plugin reference prefix: `plugin:<plugin_id>`.
pub const PLUGIN_REF_PREFIX: &str = "plugin:";

/// Resolves the configured knowledge provider for a given project.
///
/// Implementations project from the `project_knowledge_providers` read
/// model: the current-configuration row (kind = "configured"). When no
/// row exists for a project, implementations MUST return
/// `ProviderRef("cairn-default")` — the project-creation default per RFC
/// 029 §"Operating floor".
#[async_trait]
pub trait ProviderResolver: Send + Sync {
    async fn resolve(&self, project: &ProjectKey) -> Result<ProviderRef, ProviderResolverError>;
}

#[async_trait]
impl<T: ProviderResolver + ?Sized> ProviderResolver for std::sync::Arc<T> {
    async fn resolve(&self, project: &ProjectKey) -> Result<ProviderRef, ProviderResolverError> {
        (**self).resolve(project).await
    }
}

/// Errors from provider resolution.
#[derive(Debug)]
pub enum ProviderResolverError {
    Internal(String),
}

impl std::fmt::Display for ProviderResolverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Internal(msg) => write!(f, "provider resolver error: {msg}"),
        }
    }
}

impl std::error::Error for ProviderResolverError {}

/// Dispatches a knowledge.* call to a plugin subprocess.
///
/// Implementations own the plugin host (RFC 007 stdio transport in the
/// in-tree case; future SSE / websocket transports for remote adapters)
/// and perform the full JSON-RPC round-trip. Transport-level failure
/// surfaces as `Unavailable`; a JSON-RPC error response surfaces as
/// `PluginError`.
#[async_trait]
pub trait KnowledgePluginDispatcher: Send + Sync {
    async fn query(
        &self,
        plugin_id: &str,
        params: KnowledgeQueryParams,
    ) -> Result<KnowledgeQueryResult, KnowledgePluginError>;

    async fn ingest(
        &self,
        plugin_id: &str,
        params: KnowledgeIngestParams,
    ) -> Result<KnowledgeIngestAck, KnowledgePluginError>;

    async fn ingest_status(
        &self,
        plugin_id: &str,
        params: KnowledgeIngestStatusParams,
    ) -> Result<KnowledgeIngestStatusResult, KnowledgePluginError>;
}

// Blanket impl so callers can share a dispatcher behind an `Arc` without
// wrapping. Mirrors the same pattern the retrieval / ingest traits would
// get via `async_trait`'s auto-impl for `Box<dyn _>`.
#[async_trait]
impl<T: KnowledgePluginDispatcher + ?Sized> KnowledgePluginDispatcher for std::sync::Arc<T> {
    async fn query(
        &self,
        plugin_id: &str,
        params: KnowledgeQueryParams,
    ) -> Result<KnowledgeQueryResult, KnowledgePluginError> {
        (**self).query(plugin_id, params).await
    }

    async fn ingest(
        &self,
        plugin_id: &str,
        params: KnowledgeIngestParams,
    ) -> Result<KnowledgeIngestAck, KnowledgePluginError> {
        (**self).ingest(plugin_id, params).await
    }

    async fn ingest_status(
        &self,
        plugin_id: &str,
        params: KnowledgeIngestStatusParams,
    ) -> Result<KnowledgeIngestStatusResult, KnowledgePluginError> {
        (**self).ingest_status(plugin_id, params).await
    }
}

/// Errors from the plugin dispatcher.
#[derive(Debug)]
pub enum KnowledgePluginError {
    Unavailable(String),
    PluginError(String),
    Internal(String),
}

impl std::fmt::Display for KnowledgePluginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable(msg) => write!(f, "plugin unavailable: {msg}"),
            Self::PluginError(msg) => write!(f, "plugin error: {msg}"),
            Self::Internal(msg) => write!(f, "internal plugin dispatch error: {msg}"),
        }
    }
}

impl std::error::Error for KnowledgePluginError {}

/// Split a [`ProviderRef`] into its target: either the in-process default
/// or a plugin id. Returns [`ProviderRoute::Unknown`] for any value that
/// is neither `"cairn-default"` nor prefixed with `"plugin:"`.
pub fn parse_provider_ref(pref: &ProviderRef) -> ProviderRoute<'_> {
    let s = pref.as_str();
    if s == CAIRN_DEFAULT_PROVIDER_REF {
        ProviderRoute::CairnDefault
    } else if let Some(rest) = s.strip_prefix(PLUGIN_REF_PREFIX) {
        if rest.is_empty() {
            ProviderRoute::Unknown(s)
        } else {
            ProviderRoute::Plugin(rest)
        }
    } else {
        ProviderRoute::Unknown(s)
    }
}

/// Target of a `ProviderRef`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderRoute<'a> {
    CairnDefault,
    Plugin(&'a str),
    Unknown(&'a str),
}

// ─── MultiProviderRetrieval ───────────────────────────────────────────────

/// Hook called after a query response is materialized but before it
/// reaches the caller. RFC 029 PR-B2 uses this to inject the post-hoc
/// rescorer (overwriting runtime-owned scoring dimensions + recomputing
/// the final score). Left as a trait rather than a concrete type so
/// test sites and future B3+ hooks (e.g. cross-query corroboration)
/// can compose without reshaping MultiProviderRetrieval.
#[async_trait]
pub trait ResponseHook: Send + Sync {
    async fn apply(&self, response: RetrievalResponse)
        -> Result<RetrievalResponse, RetrievalError>;
}

#[async_trait]
impl<T: ResponseHook + ?Sized> ResponseHook for std::sync::Arc<T> {
    async fn apply(
        &self,
        response: RetrievalResponse,
    ) -> Result<RetrievalResponse, RetrievalError> {
        (**self).apply(response).await
    }
}

/// No-op hook — leaves the response unchanged. Default for
/// `MultiProviderRetrieval::new`; callers wire a real hook via
/// [`MultiProviderRetrieval::with_response_hook`].
pub struct NoOpResponseHook;

#[async_trait]
impl ResponseHook for NoOpResponseHook {
    async fn apply(
        &self,
        response: RetrievalResponse,
    ) -> Result<RetrievalResponse, RetrievalError> {
        Ok(response)
    }
}

/// Dispatching [`RetrievalService`] that routes each query to the project's
/// configured provider.
///
/// Generic over the in-process default implementation type (`R`) where `R:
/// Deref<Target: RetrievalService>`, the resolver (`P: ProviderResolver`),
/// the dispatcher (`D: KnowledgePluginDispatcher`), and an optional
/// response hook (`H: ResponseHook`). The Deref bound accepts both
/// `Arc<ConcreteRetrieval>` and `Arc<dyn RetrievalService>` directly
/// without an extra indirection — call sites pass the same `Arc<…>` they
/// already hold.
pub struct MultiProviderRetrieval<R, P, D, H = NoOpResponseHook> {
    default: R,
    resolver: P,
    dispatcher: D,
    hook: H,
}

impl<R, P, D> MultiProviderRetrieval<R, P, D, NoOpResponseHook> {
    pub fn new(default: R, resolver: P, dispatcher: D) -> Self {
        Self {
            default,
            resolver,
            dispatcher,
            hook: NoOpResponseHook,
        }
    }
}

impl<R, P, D, H> MultiProviderRetrieval<R, P, D, H> {
    /// RFC 029 PR-B2: wire the post-hoc rescorer (or any other
    /// response-shaping hook) so every response — cairn-default or
    /// plugin — flows through it before reaching the caller. The
    /// runtime-owned dimension contract depends on this being applied
    /// unconditionally to every route; there is no "skip the rescorer
    /// on cairn-default" path.
    pub fn with_response_hook<H2>(self, hook: H2) -> MultiProviderRetrieval<R, P, D, H2> {
        MultiProviderRetrieval {
            default: self.default,
            resolver: self.resolver,
            dispatcher: self.dispatcher,
            hook,
        }
    }
}

#[async_trait]
impl<R, P, D, H> RetrievalService for MultiProviderRetrieval<R, P, D, H>
where
    R: std::ops::Deref + Send + Sync,
    R::Target: RetrievalService,
    P: ProviderResolver,
    D: KnowledgePluginDispatcher,
    H: ResponseHook,
{
    async fn query(&self, query: RetrievalQuery) -> Result<RetrievalResponse, RetrievalError> {
        let pref = self
            .resolver
            .resolve(&query.project)
            .await
            .map_err(|e| RetrievalError::Internal(e.to_string()))?;

        let response = match parse_provider_ref(&pref) {
            ProviderRoute::CairnDefault => self.default.query(query).await?,
            ProviderRoute::Plugin(plugin_id) => {
                let plugin_id_owned = plugin_id.to_owned();
                let params: KnowledgeQueryParams = query.into();
                let wire_result = self
                    .dispatcher
                    .query(&plugin_id_owned, params)
                    .await
                    .map_err(|e| match e {
                        KnowledgePluginError::Unavailable(reason) => {
                            RetrievalError::ProviderUnavailable {
                                provider: format!("plugin:{plugin_id_owned}"),
                                reason,
                            }
                        }
                        KnowledgePluginError::PluginError(msg) => RetrievalError::Internal(msg),
                        KnowledgePluginError::Internal(msg) => RetrievalError::Internal(msg),
                    })?;
                wire_result.into()
            }
            ProviderRoute::Unknown(raw) => {
                return Err(RetrievalError::ProviderUnavailable {
                    provider: raw.to_owned(),
                    reason: "unrecognised provider_ref shape".to_owned(),
                });
            }
        };

        self.hook.apply(response).await
    }
}

// ─── MultiProviderIngest ──────────────────────────────────────────────────

/// Dispatching [`IngestService`] that routes each submission to the
/// project's configured provider.
///
/// Symmetric to [`MultiProviderRetrieval`]. One operational difference:
/// `submit_pack` (RFC 013 bundles) is a cairn-specific pipeline — plugins
/// do not receive bundle JSON, so a project configured with a plugin
/// provider sees `submit_pack` fail with
/// [`IngestError::ProviderRejected`]. This matches the RFC's "plugin
/// providers implement whatever pipeline their backend uses" rule.
pub struct MultiProviderIngest<I, P, D> {
    default: I,
    resolver: P,
    dispatcher: D,
}

impl<I, P, D> MultiProviderIngest<I, P, D> {
    pub fn new(default: I, resolver: P, dispatcher: D) -> Self {
        Self {
            default,
            resolver,
            dispatcher,
        }
    }
}

#[async_trait]
impl<I, P, D> IngestService for MultiProviderIngest<I, P, D>
where
    I: std::ops::Deref + Send + Sync,
    I::Target: IngestService,
    P: ProviderResolver,
    D: KnowledgePluginDispatcher,
{
    async fn submit(&self, request: IngestRequest) -> Result<(), IngestError> {
        let pref = self
            .resolver
            .resolve(&request.project)
            .await
            .map_err(|e| IngestError::Internal(e.to_string()))?;

        match parse_provider_ref(&pref) {
            ProviderRoute::CairnDefault => self.default.submit(request).await,
            ProviderRoute::Plugin(plugin_id) => {
                let plugin_id_owned = plugin_id.to_owned();
                let document_id = request.document_id.clone();
                let params: KnowledgeIngestParams = request.into();
                let ack = self
                    .dispatcher
                    .ingest(&plugin_id_owned, params)
                    .await
                    .map_err(|e| plugin_error_to_ingest_error(&plugin_id_owned, e))?;
                if ack.accepted {
                    let _ = document_id;
                    Ok(())
                } else {
                    Err(IngestError::ProviderRejected {
                        provider: format!("plugin:{plugin_id_owned}"),
                        reason: ack
                            .reason
                            .unwrap_or_else(|| "provider declined ingest".to_owned()),
                    })
                }
            }
            ProviderRoute::Unknown(raw) => Err(IngestError::ProviderUnavailable {
                provider: raw.to_owned(),
                reason: "unrecognised provider_ref shape".to_owned(),
            }),
        }
    }

    async fn submit_pack(&self, request: IngestPackRequest) -> Result<(), IngestError> {
        let pref = self
            .resolver
            .resolve(&request.project)
            .await
            .map_err(|e| IngestError::Internal(e.to_string()))?;

        match parse_provider_ref(&pref) {
            ProviderRoute::CairnDefault => self.default.submit_pack(request).await,
            ProviderRoute::Plugin(plugin_id) => Err(IngestError::ProviderRejected {
                provider: format!("plugin:{plugin_id}"),
                reason: "knowledge-pack ingest (RFC 013 bundles) is a cairn-default pipeline; \
                    plugin providers own their own ingest surface"
                    .to_owned(),
            }),
            ProviderRoute::Unknown(raw) => Err(IngestError::ProviderUnavailable {
                provider: raw.to_owned(),
                reason: "unrecognised provider_ref shape".to_owned(),
            }),
        }
    }

    async fn status(
        &self,
        document_id: &KnowledgeDocumentId,
    ) -> Result<Option<IngestStatus>, IngestError> {
        // Status resolution needs a project scope (the configured provider
        // may differ per project) but the trait signature only carries
        // `document_id`. Pre-RFC-029 callers work unchanged for cairn-default
        // (the pre-existing in-process path owns this lookup). For plugin
        // providers, the caller wanting plugin-reported status should hit a
        // project-scoped HTTP endpoint in cairn-app that goes through
        // `plugin_ingest_status` on this type directly.
        self.default.status(document_id).await
    }
}

impl<I, P, D> MultiProviderIngest<I, P, D>
where
    I: std::ops::Deref + Send + Sync,
    I::Target: IngestService,
    P: ProviderResolver,
    D: KnowledgePluginDispatcher,
{
    /// Project-scoped ingest-status lookup. Required for plugin providers
    /// because RFC 007's `knowledge.ingest_status` is keyed per
    /// `(project, document_id)` via the dispatcher — `IngestService::status`
    /// alone does not carry the project scope.
    pub async fn plugin_ingest_status(
        &self,
        project: &ProjectKey,
        document_id: &KnowledgeDocumentId,
    ) -> Result<Option<IngestStatus>, IngestError> {
        let pref = self
            .resolver
            .resolve(project)
            .await
            .map_err(|e| IngestError::Internal(e.to_string()))?;

        match parse_provider_ref(&pref) {
            ProviderRoute::CairnDefault => self.default.status(document_id).await,
            ProviderRoute::Plugin(plugin_id) => {
                let plugin_id_owned = plugin_id.to_owned();
                let result = self
                    .dispatcher
                    .ingest_status(
                        &plugin_id_owned,
                        KnowledgeIngestStatusParams {
                            document_id: document_id.clone(),
                        },
                    )
                    .await
                    .map_err(|e| plugin_error_to_ingest_error(&plugin_id_owned, e))?;
                Ok(result.status.map(Into::into))
            }
            ProviderRoute::Unknown(raw) => Err(IngestError::ProviderUnavailable {
                provider: raw.to_owned(),
                reason: "unrecognised provider_ref shape".to_owned(),
            }),
        }
    }
}

fn plugin_error_to_ingest_error(plugin_id: &str, e: KnowledgePluginError) -> IngestError {
    match e {
        KnowledgePluginError::Unavailable(reason) => IngestError::ProviderUnavailable {
            provider: format!("plugin:{plugin_id}"),
            reason,
        },
        KnowledgePluginError::PluginError(msg) => IngestError::Internal(msg),
        KnowledgePluginError::Internal(msg) => IngestError::Internal(msg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::SourceType;
    use crate::retrieval::{CandidateStage, RerankerStrategy, RetrievalDiagnostics, RetrievalMode};
    use cairn_domain::{ChunkId, SourceId};
    use cairn_plugin_proto::knowledge::{
        ChunkRecordWire, KnowledgeIngestStatus, KnowledgeQueryDiagnostics, RetrievalModeWire,
        RetrievalResultWire, ScoringBreakdownWire, SourceTypeWire,
    };
    use std::sync::Mutex;

    fn proj() -> ProjectKey {
        ProjectKey::new("t", "w", "p")
    }

    fn sample_query() -> RetrievalQuery {
        RetrievalQuery {
            project: proj(),
            query_text: "q".to_owned(),
            mode: RetrievalMode::Hybrid,
            reranker: RerankerStrategy::None,
            limit: 5,
            metadata_filters: vec![],
            scoring_policy: None,
        }
    }

    struct FixedResolver(ProviderRef);

    #[async_trait]
    impl ProviderResolver for FixedResolver {
        async fn resolve(
            &self,
            _project: &ProjectKey,
        ) -> Result<ProviderRef, ProviderResolverError> {
            Ok(self.0.clone())
        }
    }

    struct DefaultOnlyRetrieval {
        response: Mutex<Option<RetrievalResponse>>,
        calls: Mutex<usize>,
    }

    #[async_trait]
    impl RetrievalService for DefaultOnlyRetrieval {
        async fn query(&self, _q: RetrievalQuery) -> Result<RetrievalResponse, RetrievalError> {
            *self.calls.lock().unwrap() += 1;
            let resp = self
                .response
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| RetrievalResponse {
                    results: vec![],
                    diagnostics: RetrievalDiagnostics {
                        mode_used: RetrievalMode::Hybrid,
                        reranker_used: RerankerStrategy::None,
                        candidates_generated: 0,
                        results_returned: 0,
                        latency_ms: 0,
                        stages_used: vec![CandidateStage::Lexical],
                        scoring_dimensions_used: vec![],
                        effective_policy: None,
                        family: None,
                    },
                });
            Ok(resp)
        }
    }

    struct DefaultOnlyIngest {
        submit_calls: Mutex<usize>,
        submit_pack_calls: Mutex<usize>,
    }

    #[async_trait]
    impl IngestService for DefaultOnlyIngest {
        async fn submit(&self, _r: IngestRequest) -> Result<(), IngestError> {
            *self.submit_calls.lock().unwrap() += 1;
            Ok(())
        }
        async fn submit_pack(&self, _r: IngestPackRequest) -> Result<(), IngestError> {
            *self.submit_pack_calls.lock().unwrap() += 1;
            Ok(())
        }
        async fn status(
            &self,
            _d: &KnowledgeDocumentId,
        ) -> Result<Option<IngestStatus>, IngestError> {
            Ok(None)
        }
    }

    #[derive(Default)]
    struct RecordingDispatcher {
        query_calls: Mutex<Vec<(String, KnowledgeQueryParams)>>,
        ingest_calls: Mutex<Vec<(String, KnowledgeIngestParams)>>,
        status_calls: Mutex<Vec<(String, KnowledgeIngestStatusParams)>>,
        next_query_result: Mutex<Option<KnowledgeQueryResult>>,
        next_ingest_ack: Mutex<Option<KnowledgeIngestAck>>,
        next_status_result: Mutex<Option<KnowledgeIngestStatusResult>>,
        fail_unavailable: Mutex<Option<String>>,
    }

    #[async_trait]
    impl KnowledgePluginDispatcher for RecordingDispatcher {
        async fn query(
            &self,
            plugin_id: &str,
            params: KnowledgeQueryParams,
        ) -> Result<KnowledgeQueryResult, KnowledgePluginError> {
            if let Some(reason) = self.fail_unavailable.lock().unwrap().clone() {
                return Err(KnowledgePluginError::Unavailable(reason));
            }
            self.query_calls
                .lock()
                .unwrap()
                .push((plugin_id.to_owned(), params));
            Ok(self
                .next_query_result
                .lock()
                .unwrap()
                .clone()
                .expect("next_query_result must be set"))
        }

        async fn ingest(
            &self,
            plugin_id: &str,
            params: KnowledgeIngestParams,
        ) -> Result<KnowledgeIngestAck, KnowledgePluginError> {
            if let Some(reason) = self.fail_unavailable.lock().unwrap().clone() {
                return Err(KnowledgePluginError::Unavailable(reason));
            }
            self.ingest_calls
                .lock()
                .unwrap()
                .push((plugin_id.to_owned(), params));
            Ok(self
                .next_ingest_ack
                .lock()
                .unwrap()
                .clone()
                .expect("next_ingest_ack must be set"))
        }

        async fn ingest_status(
            &self,
            plugin_id: &str,
            params: KnowledgeIngestStatusParams,
        ) -> Result<KnowledgeIngestStatusResult, KnowledgePluginError> {
            if let Some(reason) = self.fail_unavailable.lock().unwrap().clone() {
                return Err(KnowledgePluginError::Unavailable(reason));
            }
            self.status_calls
                .lock()
                .unwrap()
                .push((plugin_id.to_owned(), params));
            Ok(self
                .next_status_result
                .lock()
                .unwrap()
                .clone()
                .expect("next_status_result must be set"))
        }
    }

    #[test]
    fn provider_route_parses_known_shapes() {
        let def = ProviderRef::new("cairn-default");
        let plug = ProviderRef::new("plugin:mem0");
        let empty_plug = ProviderRef::new("plugin:");
        let junk = ProviderRef::new("mem0");
        assert!(matches!(
            parse_provider_ref(&def),
            ProviderRoute::CairnDefault
        ));
        assert!(matches!(
            parse_provider_ref(&plug),
            ProviderRoute::Plugin("mem0")
        ));
        assert!(matches!(
            parse_provider_ref(&empty_plug),
            ProviderRoute::Unknown("plugin:")
        ));
        assert!(matches!(
            parse_provider_ref(&junk),
            ProviderRoute::Unknown("mem0")
        ));
    }

    fn sample_wire_query_result() -> KnowledgeQueryResult {
        KnowledgeQueryResult {
            results: vec![RetrievalResultWire {
                chunk: ChunkRecordWire {
                    chunk_id: ChunkId::new("c"),
                    document_id: KnowledgeDocumentId::new("d"),
                    source_id: SourceId::new("s"),
                    source_type: SourceTypeWire::Markdown,
                    project: proj(),
                    text: "chunk".to_owned(),
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
                    ..Default::default()
                },
            }],
            diagnostics: KnowledgeQueryDiagnostics {
                mode_used: RetrievalModeWire::Hybrid,
                stages_used: Some(vec!["lexical".to_owned()]),
                reranker_used: None,
                scoring_dimensions_used: vec!["semantic_relevance".to_owned()],
                results_returned: 1,
                latency_ms: Some(3),
            },
        }
    }

    #[tokio::test]
    async fn cairn_default_routes_to_in_process() {
        let default = DefaultOnlyRetrieval {
            response: Mutex::new(None),
            calls: Mutex::new(0),
        };
        let resolver = FixedResolver(ProviderRef::new(CAIRN_DEFAULT_PROVIDER_REF));
        let dispatcher = RecordingDispatcher::default();
        let mp = MultiProviderRetrieval::new(std::sync::Arc::new(default), resolver, dispatcher);
        mp.query(sample_query()).await.unwrap();
        assert_eq!(*mp.default.calls.lock().unwrap(), 1);
        assert!(mp.dispatcher.query_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn plugin_route_dispatches_and_lifts_response() {
        let default = DefaultOnlyRetrieval {
            response: Mutex::new(None),
            calls: Mutex::new(0),
        };
        let resolver = FixedResolver(ProviderRef::new("plugin:mem0"));
        let dispatcher = RecordingDispatcher::default();
        *dispatcher.next_query_result.lock().unwrap() = Some(sample_wire_query_result());
        let mp = MultiProviderRetrieval::new(std::sync::Arc::new(default), resolver, dispatcher);
        let resp = mp.query(sample_query()).await.unwrap();
        assert_eq!(resp.results.len(), 1);
        assert_eq!(*mp.default.calls.lock().unwrap(), 0);
        let calls = mp.dispatcher.query_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "mem0");
    }

    #[tokio::test]
    async fn plugin_unavailable_surfaces_provider_unavailable_error() {
        let default = DefaultOnlyRetrieval {
            response: Mutex::new(None),
            calls: Mutex::new(0),
        };
        let resolver = FixedResolver(ProviderRef::new("plugin:mem0"));
        let dispatcher = RecordingDispatcher::default();
        *dispatcher.fail_unavailable.lock().unwrap() = Some("handshake timeout".to_owned());
        let mp = MultiProviderRetrieval::new(std::sync::Arc::new(default), resolver, dispatcher);
        match mp.query(sample_query()).await {
            Err(RetrievalError::ProviderUnavailable { provider, reason }) => {
                assert_eq!(provider, "plugin:mem0");
                assert_eq!(reason, "handshake timeout");
            }
            other => panic!("expected ProviderUnavailable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unknown_provider_ref_errors() {
        let default = DefaultOnlyRetrieval {
            response: Mutex::new(None),
            calls: Mutex::new(0),
        };
        let resolver = FixedResolver(ProviderRef::new("bedrock-kb"));
        let dispatcher = RecordingDispatcher::default();
        let mp = MultiProviderRetrieval::new(std::sync::Arc::new(default), resolver, dispatcher);
        assert!(matches!(
            mp.query(sample_query()).await,
            Err(RetrievalError::ProviderUnavailable { .. })
        ));
    }

    fn sample_ingest_request() -> IngestRequest {
        IngestRequest {
            document_id: KnowledgeDocumentId::new("d"),
            source_id: SourceId::new("s"),
            source_type: SourceType::Markdown,
            project: proj(),
            content: "content".to_owned(),
            import_id: None,
            corpus_id: None,
            bundle_source_id: None,
            tags: vec![],
        }
    }

    #[tokio::test]
    async fn ingest_cairn_default_routes_to_in_process() {
        let default = DefaultOnlyIngest {
            submit_calls: Mutex::new(0),
            submit_pack_calls: Mutex::new(0),
        };
        let resolver = FixedResolver(ProviderRef::new(CAIRN_DEFAULT_PROVIDER_REF));
        let dispatcher = RecordingDispatcher::default();
        let mp = MultiProviderIngest::new(std::sync::Arc::new(default), resolver, dispatcher);
        mp.submit(sample_ingest_request()).await.unwrap();
        assert_eq!(*mp.default.submit_calls.lock().unwrap(), 1);
        assert!(mp.dispatcher.ingest_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn ingest_plugin_route_accepted() {
        let default = DefaultOnlyIngest {
            submit_calls: Mutex::new(0),
            submit_pack_calls: Mutex::new(0),
        };
        let resolver = FixedResolver(ProviderRef::new("plugin:mem0"));
        let dispatcher = RecordingDispatcher::default();
        *dispatcher.next_ingest_ack.lock().unwrap() = Some(KnowledgeIngestAck {
            document_id: KnowledgeDocumentId::new("d"),
            accepted: true,
            reason: None,
        });
        let mp = MultiProviderIngest::new(std::sync::Arc::new(default), resolver, dispatcher);
        mp.submit(sample_ingest_request()).await.unwrap();
        assert_eq!(mp.dispatcher.ingest_calls.lock().unwrap().len(), 1);
        assert_eq!(*mp.default.submit_calls.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn ingest_plugin_rejected_surfaces_provider_rejected() {
        let default = DefaultOnlyIngest {
            submit_calls: Mutex::new(0),
            submit_pack_calls: Mutex::new(0),
        };
        let resolver = FixedResolver(ProviderRef::new("plugin:bedrock-kb"));
        let dispatcher = RecordingDispatcher::default();
        *dispatcher.next_ingest_ack.lock().unwrap() = Some(KnowledgeIngestAck {
            document_id: KnowledgeDocumentId::new("d"),
            accepted: false,
            reason: Some("read-only".to_owned()),
        });
        let mp = MultiProviderIngest::new(std::sync::Arc::new(default), resolver, dispatcher);
        match mp.submit(sample_ingest_request()).await {
            Err(IngestError::ProviderRejected { provider, reason }) => {
                assert_eq!(provider, "plugin:bedrock-kb");
                assert_eq!(reason, "read-only");
            }
            other => panic!("expected ProviderRejected, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn submit_pack_on_plugin_is_rejected() {
        let default = DefaultOnlyIngest {
            submit_calls: Mutex::new(0),
            submit_pack_calls: Mutex::new(0),
        };
        let resolver = FixedResolver(ProviderRef::new("plugin:mem0"));
        let dispatcher = RecordingDispatcher::default();
        let mp = MultiProviderIngest::new(std::sync::Arc::new(default), resolver, dispatcher);
        let req = IngestPackRequest {
            pack_id: cairn_domain::KnowledgePackId::new("pk"),
            project: proj(),
            bundle_json: "{}".to_owned(),
        };
        assert!(matches!(
            mp.submit_pack(req).await,
            Err(IngestError::ProviderRejected { .. })
        ));
        assert_eq!(*mp.default.submit_pack_calls.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn plugin_ingest_status_routes_through_dispatcher() {
        let default = DefaultOnlyIngest {
            submit_calls: Mutex::new(0),
            submit_pack_calls: Mutex::new(0),
        };
        let resolver = FixedResolver(ProviderRef::new("plugin:mem0"));
        let dispatcher = RecordingDispatcher::default();
        *dispatcher.next_status_result.lock().unwrap() = Some(KnowledgeIngestStatusResult {
            status: Some(KnowledgeIngestStatus::Completed),
        });
        let mp = MultiProviderIngest::new(std::sync::Arc::new(default), resolver, dispatcher);
        let out = mp
            .plugin_ingest_status(&proj(), &KnowledgeDocumentId::new("d"))
            .await
            .unwrap();
        assert_eq!(out, Some(IngestStatus::Completed));
    }
}
