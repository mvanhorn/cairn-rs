//! RFC 029: event-log-backed [`ProviderResolver`] plus a placeholder
//! [`KnowledgePluginDispatcher`] used in the B1 wiring.
//!
//! The resolver walks the event log (via `cairn_store::EventLog::read_stream`)
//! looking for the latest `KnowledgeProviderConfigured` event for a project
//! and returns that `ProviderRef`. Event-log order (stream position) is the
//! authoritative ordering — `at_ms` wall-clock is only surfaced for
//! operator UI, not consulted here. When no configuration event exists the
//! resolver returns `"cairn-default"` — the project-creation default per
//! RFC 029 §"Operating floor".
//!
//! Performance: the first resolution after process start is O(event log);
//! subsequent resolutions for the same project are O(1) via an in-process
//! cache (HashMap keyed by `ProjectKey`). The cache is invalidated
//! implicitly on process restart — since configuration events are rare
//! (operators typically configure their provider once per project) this
//! is adequate. A projection-backed read model with live invalidation
//! via the event broadcast is a tracked follow-up.
//!
//! The [`UnavailablePluginDispatcher`] always returns `Unavailable` with
//! a clear reason. Productised plugin retrieval ships in a separate PR
//! once the adapter binaries (`cairn-knowledge-bedrock-kb`,
//! `cairn-knowledge-mem0`) are ready. Until then, any
//! `provider_ref = "plugin:<id>"` write is accepted (operator intent is
//! captured in the event log) but query-time dispatch fails loudly — no
//! silent fallback to cairn-default.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cairn_domain::events::ResolvedProviderSnapshot;
use cairn_domain::{ProjectKey, ProviderRef, RuntimeEvent};
use cairn_plugin_proto::knowledge::{
    KnowledgeIngestAck, KnowledgeIngestParams, KnowledgeIngestStatusParams,
    KnowledgeIngestStatusResult, KnowledgeQueryParams, KnowledgeQueryResult,
};
use cairn_store::EventLog;

use crate::multi_provider::{
    KnowledgePluginDispatcher, KnowledgePluginError, ProviderResolver, ProviderResolverError,
    CAIRN_DEFAULT_PROVIDER_REF,
};

/// Page size used when walking the event log during resolution. Tuned to
/// match cairn-store's typical `read_stream` chunks; the resolver keeps
/// pulling pages until the head is reached or the project's configuration
/// is found.
const PAGE_SIZE: usize = 1_000;

pub struct EventLogProviderResolver<S> {
    store: Arc<S>,
    /// Cached resolutions keyed by `ProjectKey`. First resolution per
    /// project is O(event log); subsequent calls within this process
    /// lifetime are O(1). No invalidation today — a reconfiguration
    /// requires a process restart to take effect on in-flight resolver
    /// lookups. Acceptable for B1 because (a) configuration is rare and
    /// (b) the full orchestrate request path re-runs the resolver every
    /// time, so a config change takes effect on the next request for
    /// projects whose cache entry hasn't been populated. A live
    /// broadcast-driven invalidation lands alongside the projection-
    /// backed resolver follow-up.
    cache: Mutex<HashMap<ProjectKey, ProviderRef>>,
}

impl<S> EventLogProviderResolver<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            cache: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl<S> ProviderResolver for EventLogProviderResolver<S>
where
    S: EventLog + 'static,
{
    async fn resolve(&self, project: &ProjectKey) -> Result<ProviderRef, ProviderResolverError> {
        resolve_latest_provider(
            self.store.as_ref(),
            &self.cache,
            project,
            extract_knowledge_provider_ref,
        )
        .await
    }
}

/// RFC 030 PR-G: memory-family twin of [`EventLogProviderResolver`].
/// Scans the event log for `MemoryProviderConfigured` events. Separate
/// resolver so the two families can be resolved independently (the
/// knowledge slot might be `plugin:bedrock-kb` while the memory slot
/// stays on `cairn-default`, for instance).
pub struct EventLogMemoryProviderResolver<S> {
    store: Arc<S>,
    cache: Mutex<HashMap<ProjectKey, ProviderRef>>,
}

impl<S> EventLogMemoryProviderResolver<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            cache: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl<S> ProviderResolver for EventLogMemoryProviderResolver<S>
where
    S: EventLog + 'static,
{
    async fn resolve(&self, project: &ProjectKey) -> Result<ProviderRef, ProviderResolverError> {
        resolve_latest_provider(
            self.store.as_ref(),
            &self.cache,
            project,
            extract_memory_provider_ref,
        )
        .await
    }
}

/// Shared resolver core: walks the event log looking for the latest
/// provider-configuration event for a given project, falling back to
/// `cairn-default` when none is found. The per-event extraction closure
/// narrows to one of the two provider families.
///
/// Position-ordered (not wall-clock): the pg/sqlite projection upserts
/// rows in the same transaction as the append, so stream order matches
/// the projection's upsert order. `at_ms` is operator-UI wall-clock
/// and can drift under a skewed system clock, which is why we consult
/// position instead.
async fn resolve_latest_provider<S, F>(
    store: &S,
    cache: &Mutex<HashMap<ProjectKey, ProviderRef>>,
    project: &ProjectKey,
    extract: F,
) -> Result<ProviderRef, ProviderResolverError>
where
    S: EventLog + ?Sized,
    F: Fn(&RuntimeEvent) -> Option<(&ProjectKey, &ProviderRef)>,
{
    if let Some(cached) = cache.lock().ok().and_then(|m| m.get(project).cloned()) {
        return Ok(cached);
    }

    let mut latest: Option<ProviderRef> = None;
    let mut after = None;
    loop {
        let page = store
            .read_stream(after, PAGE_SIZE)
            .await
            .map_err(|e| ProviderResolverError::Internal(e.to_string()))?;
        if page.is_empty() {
            break;
        }
        for stored in &page {
            if let Some((ev_project, ev_ref)) = extract(&stored.envelope.payload) {
                if ev_project == project {
                    latest = Some(ev_ref.clone());
                }
            }
        }
        after = page.last().map(|s| s.position);
    }

    let resolved = latest.unwrap_or_else(|| ProviderRef::new(CAIRN_DEFAULT_PROVIDER_REF));
    if let Ok(mut map) = cache.lock() {
        map.insert(project.clone(), resolved.clone());
    }
    Ok(resolved)
}

fn extract_knowledge_provider_ref(ev: &RuntimeEvent) -> Option<(&ProjectKey, &ProviderRef)> {
    if let RuntimeEvent::KnowledgeProviderConfigured(e) = ev {
        Some((&e.project, &e.provider_ref))
    } else {
        None
    }
}

fn extract_memory_provider_ref(ev: &RuntimeEvent) -> Option<(&ProjectKey, &ProviderRef)> {
    if let RuntimeEvent::MemoryProviderConfigured(e) = ev {
        Some((&e.project, &e.provider_ref))
    } else {
        None
    }
}

/// RFC 030: memory-family variant of [`snapshot_for_provider_ref`].
/// Returns a memory-family [`ResolvedProviderSnapshot`] — identical to
/// the knowledge-family one for cairn-default except `auto_extract` is
/// `Some(false)` (cairn-default ingests explicitly). Plugin refs
/// surface `None` until the plugin host's handshake cache threads in.
pub fn memory_snapshot_for_provider_ref(pref: &ProviderRef) -> Option<ResolvedProviderSnapshot> {
    if pref.as_str() == CAIRN_DEFAULT_PROVIDER_REF {
        Some(ResolvedProviderSnapshot {
            provider_id: CAIRN_DEFAULT_PROVIDER_REF.to_owned(),
            ingest_capable: true,
            retrieval_modes: vec![
                "lexical_only".to_owned(),
                "vector_only".to_owned(),
                "hybrid".to_owned(),
            ],
            scoring_dimensions_surfaced: vec![
                "semantic_relevance".to_owned(),
                "lexical_relevance".to_owned(),
                "freshness_decay".to_owned(),
                "staleness_penalty".to_owned(),
                "recency_of_use".to_owned(),
            ],
            // Cairn-default accepts explicit `memory_store` calls — the
            // agent is responsible for stashing memories itself, not a
            // post-turn auto-extractor. Mem0-style backends return
            // `Some(true)` here instead.
            auto_extract: Some(false),
        })
    } else {
        None
    }
}

/// Derive a `ResolvedProviderSnapshot` for a `ProviderRef` that will be
/// attached to `VisibilityContext` at run start.
///
/// - `"cairn-default"` → snapshot describing the in-process default:
///   ingest_capable = true, all four retrieval modes plus `memory_filtered`,
///   semantic + lexical + freshness + staleness + recency surfaced. These
///   match the capabilities InMemoryRetrieval + InMemoryIngest actually
///   implement today.
/// - `"plugin:<id>"` → `None` in B1. The plugin host's handshake cache is
///   the real source of truth; until that cache threads into this
///   resolver, leave the snapshot absent. `is_tool_visible` treats `None`
///   as "memory_store visible" — consistent with cairn-default's posture.
/// - Anything else → `None`. The `MultiProvider` dispatch layer will
///   error at query time with `ProviderUnavailable`.
pub fn snapshot_for_provider_ref(pref: &ProviderRef) -> Option<ResolvedProviderSnapshot> {
    if pref.as_str() == CAIRN_DEFAULT_PROVIDER_REF {
        Some(ResolvedProviderSnapshot {
            provider_id: CAIRN_DEFAULT_PROVIDER_REF.to_owned(),
            ingest_capable: true,
            retrieval_modes: vec![
                "lexical_only".to_owned(),
                "vector_only".to_owned(),
                "hybrid".to_owned(),
            ],
            scoring_dimensions_surfaced: vec![
                "semantic_relevance".to_owned(),
                "lexical_relevance".to_owned(),
                "freshness_decay".to_owned(),
                "staleness_penalty".to_owned(),
                "recency_of_use".to_owned(),
            ],
            // RFC 030: cairn-default is the knowledge-family snapshot here —
            // auto_extract is a memory-family concept. PR-G introduces a
            // parallel memory-family snapshot for the memory slot.
            auto_extract: None,
        })
    } else {
        None
    }
}

/// Placeholder dispatcher returning `Unavailable` for every call.
///
/// Used in the B1 wiring so the `MultiProviderRetrieval` / `…Ingest`
/// types compose and the cairn-default path remains fully functional,
/// while plugin-path dispatch surfaces a clear error until productised
/// plugin retrieval ships.
pub struct UnavailablePluginDispatcher;

#[async_trait]
impl KnowledgePluginDispatcher for UnavailablePluginDispatcher {
    async fn query(
        &self,
        plugin_id: &str,
        _params: KnowledgeQueryParams,
    ) -> Result<KnowledgeQueryResult, KnowledgePluginError> {
        Err(KnowledgePluginError::Unavailable(format!(
            "plugin:{plugin_id} knowledge retrieval not wired in this build"
        )))
    }

    async fn ingest(
        &self,
        plugin_id: &str,
        _params: KnowledgeIngestParams,
    ) -> Result<KnowledgeIngestAck, KnowledgePluginError> {
        Err(KnowledgePluginError::Unavailable(format!(
            "plugin:{plugin_id} knowledge ingest not wired in this build"
        )))
    }

    async fn ingest_status(
        &self,
        plugin_id: &str,
        _params: KnowledgeIngestStatusParams,
    ) -> Result<KnowledgeIngestStatusResult, KnowledgePluginError> {
        Err(KnowledgePluginError::Unavailable(format!(
            "plugin:{plugin_id} knowledge ingest-status not wired in this build"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_domain::{
        events::KnowledgeProviderConfigured, EventEnvelope, EventId, EventSource, OperatorId,
    };
    use cairn_store::InMemoryStore;

    fn proj(id: &str) -> ProjectKey {
        ProjectKey::new("t", "w", id)
    }

    fn envelope_for(event: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
        EventEnvelope::for_runtime_event(
            EventId::new(format!("ev_{}", rand_suffix())),
            EventSource::System,
            event,
        )
    }

    fn rand_suffix() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        COUNTER.fetch_add(1, Ordering::Relaxed).to_string()
    }

    #[tokio::test]
    async fn empty_log_resolves_to_cairn_default() {
        let store = Arc::new(InMemoryStore::new());
        let resolver = EventLogProviderResolver::new(store);
        let pref = resolver.resolve(&proj("a")).await.unwrap();
        assert_eq!(pref.as_str(), "cairn-default");
    }

    #[tokio::test]
    async fn latest_configuration_event_wins() {
        let store = Arc::new(InMemoryStore::new());
        store
            .append(&[
                envelope_for(RuntimeEvent::KnowledgeProviderConfigured(
                    KnowledgeProviderConfigured {
                        project: proj("a"),
                        provider_ref: ProviderRef::new("plugin:bedrock-kb"),
                        configured_by: OperatorId::new("op"),
                        is_bootstrap: false,
                        at_ms: 100,
                    },
                )),
                envelope_for(RuntimeEvent::KnowledgeProviderConfigured(
                    KnowledgeProviderConfigured {
                        project: proj("a"),
                        provider_ref: ProviderRef::new("plugin:mem0"),
                        configured_by: OperatorId::new("op"),
                        is_bootstrap: false,
                        at_ms: 200,
                    },
                )),
            ])
            .await
            .unwrap();

        let resolver = EventLogProviderResolver::new(store);
        let pref = resolver.resolve(&proj("a")).await.unwrap();
        assert_eq!(pref.as_str(), "plugin:mem0");
    }

    #[tokio::test]
    async fn resolver_is_project_scoped() {
        let store = Arc::new(InMemoryStore::new());
        store
            .append(&[envelope_for(RuntimeEvent::KnowledgeProviderConfigured(
                KnowledgeProviderConfigured {
                    project: proj("a"),
                    provider_ref: ProviderRef::new("plugin:mem0"),
                    configured_by: OperatorId::new("op"),
                    is_bootstrap: false,
                    at_ms: 100,
                },
            ))])
            .await
            .unwrap();

        let resolver = EventLogProviderResolver::new(store);
        let a = resolver.resolve(&proj("a")).await.unwrap();
        let b = resolver.resolve(&proj("b")).await.unwrap();
        assert_eq!(a.as_str(), "plugin:mem0");
        assert_eq!(b.as_str(), "cairn-default");
    }

    #[tokio::test]
    async fn snapshot_for_cairn_default_has_ingest_capable() {
        let snap = snapshot_for_provider_ref(&ProviderRef::new("cairn-default")).unwrap();
        assert_eq!(snap.provider_id, "cairn-default");
        assert!(snap.ingest_capable);
        assert!(snap
            .scoring_dimensions_surfaced
            .contains(&"semantic_relevance".to_owned()));
    }

    #[tokio::test]
    async fn snapshot_for_plugin_refs_returns_none() {
        assert!(snapshot_for_provider_ref(&ProviderRef::new("plugin:mem0")).is_none());
        assert!(snapshot_for_provider_ref(&ProviderRef::new("unknown")).is_none());
    }

    // ── RFC 030 PR-G memory-family resolver tests ─────────────────────

    #[tokio::test]
    async fn memory_resolver_empty_log_falls_back_to_cairn_default() {
        let store = Arc::new(InMemoryStore::new());
        let resolver = EventLogMemoryProviderResolver::new(store);
        let pref = resolver.resolve(&proj("a")).await.unwrap();
        assert_eq!(pref.as_str(), "cairn-default");
    }

    #[tokio::test]
    async fn memory_resolver_ignores_knowledge_provider_events() {
        // If only a `KnowledgeProviderConfigured` landed, the memory
        // resolver must not pick it up — falls back to cairn-default.
        use cairn_domain::events::MemoryProviderConfigured;
        let store = Arc::new(InMemoryStore::new());
        store
            .append(&[envelope_for(RuntimeEvent::KnowledgeProviderConfigured(
                KnowledgeProviderConfigured {
                    project: proj("a"),
                    provider_ref: ProviderRef::new("plugin:bedrock-kb"),
                    configured_by: OperatorId::new("op"),
                    is_bootstrap: false,
                    at_ms: 100,
                },
            ))])
            .await
            .unwrap();
        let resolver = EventLogMemoryProviderResolver::new(store.clone());
        let pref = resolver.resolve(&proj("a")).await.unwrap();
        assert_eq!(
            pref.as_str(),
            "cairn-default",
            "knowledge-family event must not affect memory resolution"
        );

        // Now append the memory event and reconfirm.
        store
            .append(&[envelope_for(RuntimeEvent::MemoryProviderConfigured(
                MemoryProviderConfigured {
                    project: proj("a"),
                    provider_ref: ProviderRef::new("plugin:mem0"),
                    configured_by: OperatorId::new("op"),
                    is_bootstrap: false,
                    at_ms: 200,
                },
            ))])
            .await
            .unwrap();
        // Need a fresh resolver because the old one cached.
        let resolver2 = EventLogMemoryProviderResolver::new(store);
        let pref2 = resolver2.resolve(&proj("a")).await.unwrap();
        assert_eq!(pref2.as_str(), "plugin:mem0");
    }

    #[tokio::test]
    async fn memory_and_knowledge_resolvers_are_independent() {
        use cairn_domain::events::MemoryProviderConfigured;
        let store = Arc::new(InMemoryStore::new());
        store
            .append(&[
                envelope_for(RuntimeEvent::KnowledgeProviderConfigured(
                    KnowledgeProviderConfigured {
                        project: proj("a"),
                        provider_ref: ProviderRef::new("plugin:bedrock-kb"),
                        configured_by: OperatorId::new("op"),
                        is_bootstrap: false,
                        at_ms: 100,
                    },
                )),
                envelope_for(RuntimeEvent::MemoryProviderConfigured(
                    MemoryProviderConfigured {
                        project: proj("a"),
                        provider_ref: ProviderRef::new("plugin:mem0"),
                        configured_by: OperatorId::new("op"),
                        is_bootstrap: false,
                        at_ms: 110,
                    },
                )),
            ])
            .await
            .unwrap();
        let k = EventLogProviderResolver::new(store.clone())
            .resolve(&proj("a"))
            .await
            .unwrap();
        let m = EventLogMemoryProviderResolver::new(store)
            .resolve(&proj("a"))
            .await
            .unwrap();
        assert_eq!(k.as_str(), "plugin:bedrock-kb");
        assert_eq!(m.as_str(), "plugin:mem0");
    }

    #[tokio::test]
    async fn memory_snapshot_for_cairn_default_reports_not_auto_extract() {
        let snap = memory_snapshot_for_provider_ref(&ProviderRef::new("cairn-default")).unwrap();
        assert_eq!(snap.auto_extract, Some(false));
        assert!(snap.ingest_capable);
        assert_eq!(snap.provider_id, "cairn-default");
    }

    #[tokio::test]
    async fn memory_snapshot_for_plugin_refs_returns_none() {
        assert!(memory_snapshot_for_provider_ref(&ProviderRef::new("plugin:mem0")).is_none());
    }

    #[tokio::test]
    async fn unavailable_dispatcher_errors_for_every_method() {
        let d = UnavailablePluginDispatcher;
        let err = d
            .query(
                "mem0",
                KnowledgeQueryParams {
                    project: proj("a"),
                    query_text: String::new(),
                    mode: cairn_plugin_proto::knowledge::RetrievalModeWire::Hybrid,
                    limit: 1,
                    metadata_filters: vec![],
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, KnowledgePluginError::Unavailable(_)));
    }
}
