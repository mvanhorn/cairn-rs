//! Application state and startup replay.
//!
//! The GitHub-specific plugin state moved to
//! `cairn_integrations::github::GitHubPlugin` as part of #557; cairn-app
//! handlers recover the concrete plugin via
//! `state.integrations.get_typed::<GitHubPlugin>("github")`.

use async_trait::async_trait;
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::sync::broadcast;

// ── cairn crates ─────────────────────────────────────────────────────────────

use cairn_api::auth::ServiceTokenRegistry;
use cairn_api::bootstrap::BootstrapConfig;
use cairn_api::onboarding::StarterTemplateRegistry;
use cairn_api::sse::SseFrame;

use cairn_domain::{
    KnowledgeDocumentId, ProjectKey, PromptTemplateVar, SourceId, TaskId, TenantId, WorkspaceId,
};

use cairn_evals::services::eval_service::{MemoryDiagnosticsSource, SourceQualitySnapshot};
use cairn_evals::{
    EvalBaselineServiceImpl, EvalDatasetServiceImpl, EvalRubricServiceImpl,
    EvalRunService as ProductEvalRunService, GraphIntegration as EvalGraphIntegration,
    ModelComparisonServiceImpl, PluginDimensionScore, PluginRubricScorer,
};

use cairn_graph::in_memory::InMemoryGraphStore;

use cairn_memory::api_impl::MemoryApiImpl;
use cairn_memory::deep_search_impl::{IterativeDeepSearch, KeywordDecomposer};
use cairn_memory::diagnostics::DiagnosticsService;
use cairn_memory::diagnostics_impl::InMemoryDiagnostics;
use cairn_memory::event_log_resolver::{EventLogProviderResolver, UnavailablePluginDispatcher};
use cairn_memory::export_service_impl::InMemoryExportService;
use cairn_memory::feed_impl::FeedStore;
use cairn_memory::graph_expansion::GraphBackedExpansion;
use cairn_memory::import_service_impl::InMemoryImportService;
use cairn_memory::in_memory::{InMemoryDocumentStore, InMemoryRetrieval};
use cairn_memory::ingest::SourceType;
use cairn_memory::multi_provider::MultiProviderRetrieval;
use cairn_memory::pipeline::{IngestPipeline, ParagraphChunker};
use cairn_memory::post_hoc_rescorer::{NoOpCredibilityLookup, PostHocRescorer};

use cairn_runtime::startup::ReadinessState;
use cairn_runtime::{
    LicenseService, MarketplaceService, ModelRegistry, ProjectService, RuntimeServices,
    TenantService, TriggerService, WorkspaceService,
};

use cairn_tools::{execute_eval_score, InMemoryPluginRegistry, StdioPluginHost};

// ── crate-internal ───────────────────────────────────────────────────────────

use crate::metrics::AppMetrics;
use crate::tokens::{OperatorTokenStore, RequestLogBuffer};
/// Default tenant ID used when no scope is supplied.
///
/// Exported as `pub` so the binary crate (`main.rs` + `bin_*` modules)
/// can reuse the same literal without redefining it. Pre-#185 these
/// lived as `pub(crate)` and the binary crate carried a duplicate
/// `const` to avoid reaching into the lib's private namespace.
pub const DEFAULT_TENANT_ID: &str = "default_tenant";
pub const DEFAULT_WORKSPACE_ID: &str = "default_workspace";
pub const DEFAULT_PROJECT_ID: &str = "default_project";

// ── Type aliases ─────────────────────────────────────────────────────────────

/// RFC 029 PR-B1: deep-search dispatches retrieval through the
/// MultiProvider layer so each hop respects the project's configured
/// knowledge provider. `cairn_store::InMemoryStore` is the event-log
/// backend hold the resolver reads from; the placeholder dispatcher
/// surfaces `ProviderUnavailable` for `plugin:<id>` routes until the
/// adapter binaries productise plugin retrieval.
///
/// RFC 029 PR-B2: every hop goes through `PostHocRescorer` so the
/// quality gate sees runtime-owned scoring dimensions (not the
/// provider's), matching the contract the agent-level memory tool
/// sees through the same rescorer.
pub(crate) type AppDeepRetrieval = MultiProviderRetrieval<
    Arc<InMemoryRetrieval>,
    EventLogProviderResolver<cairn_store::InMemoryStore>,
    UnavailablePluginDispatcher,
    PostHocRescorer<Arc<InMemoryGraphStore>, NoOpCredibilityLookup>,
>;

pub(crate) type AppDeepSearch = IterativeDeepSearch<
    AppDeepRetrieval,
    KeywordDecomposer,
    GraphBackedExpansion<Arc<InMemoryGraphStore>>,
>;

pub(crate) type AppIngestPipeline = IngestPipeline<Arc<InMemoryDocumentStore>, ParagraphChunker>;

// Constants are defined in lib.rs and re-exported via crate::DEFAULT_*

// ── Adapter: MemoryDiagnosticsSource ─────────────────────────────────────────

/// Adapts `InMemoryDiagnostics` to `cairn_evals::MemoryDiagnosticsSource`, breaking the
/// circular dependency by not requiring `cairn-evals` to depend on `cairn-memory`.
pub(crate) struct DiagnosticsAdapter(pub(crate) Arc<InMemoryDiagnostics>);

#[async_trait]
impl MemoryDiagnosticsSource for DiagnosticsAdapter {
    async fn list_source_quality(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
    ) -> Result<Vec<SourceQualitySnapshot>, String> {
        let records = DiagnosticsService::list_source_quality(self.0.as_ref(), project, limit)
            .await
            .map_err(|e| e.to_string())?;
        Ok(records
            .into_iter()
            .map(|r| SourceQualitySnapshot {
                source_id: r.source_id.clone(),
                total_chunks: r.total_chunks,
                credibility_score: Some(r.credibility_score),
                retrieval_count: r.retrieval_count,
                query_hit_rate: r.query_hit_rate,
                error_rate: r.error_rate,
                last_ingested_at: Some(r.last_ingested_at),
            })
            .collect())
    }
}

// ── Adapter: PluginRubricScorer ──────────────────────────────────────────────

pub(crate) struct AppPluginRubricScorer {
    pub(crate) plugin_registry: Arc<InMemoryPluginRegistry>,
}

#[async_trait]
impl PluginRubricScorer for AppPluginRubricScorer {
    async fn score(
        &self,
        plugin_id: &str,
        input: &serde_json::Value,
        expected_output: Option<&serde_json::Value>,
        actual_output: &serde_json::Value,
    ) -> Result<PluginDimensionScore, cairn_evals::services::rubric_impl::EvalRubricError> {
        let result = execute_eval_score(
            self.plugin_registry.as_ref(),
            plugin_id,
            input.clone(),
            expected_output.cloned(),
            actual_output.clone(),
        )
        .await
        .map_err(|err| {
            cairn_evals::services::rubric_impl::EvalRubricError::PluginScoreFailed(err.to_string())
        })?;
        Ok(PluginDimensionScore {
            score: result.score,
            passed: result.passed,
            feedback: result.reasoning,
        })
    }
}

// ── Binding / view structs ───────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub(crate) struct SqEqSessionBinding {
    pub(crate) project: ProjectKey,
}

#[derive(Clone, Debug)]
pub(crate) struct A2aTaskBinding {
    pub(crate) task_id: TaskId,
    pub(crate) project: ProjectKey,
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MailboxMessageView {
    pub(crate) message_id: String,
    pub(crate) run_id: Option<String>,
    pub(crate) task_id: Option<String>,
    pub(crate) sender_id: Option<String>,
    pub(crate) body: Option<String>,
    pub(crate) delivered: bool,
    pub(crate) created_at: u64,
}

#[derive(Clone, Debug)]
pub struct AppMailboxMessage {
    pub(crate) sender_id: Option<String>,
    pub(crate) body: Option<String>,
    pub(crate) delivered: bool,
}

#[derive(Clone, Debug, Default)]
pub struct AppSourceMetadata {
    pub(crate) name: Option<String>,
    pub(crate) description: Option<String>,
}

/// Cached prompt version content and template vars (not in event payload).
#[derive(Clone, Debug, Default)]
pub struct AppVersionContent {
    pub(crate) content: String,
    pub(crate) template_vars: Vec<PromptTemplateVar>,
}

#[derive(Clone, Debug)]
pub struct PendingIngestJobPayload {
    pub(crate) project: ProjectKey,
    pub(crate) source_id: SourceId,
    pub(crate) document_id: KnowledgeDocumentId,
    pub(crate) content: String,
    pub(crate) source_type: SourceType,
}

#[derive(Clone, Copy, Debug)]
pub struct RateLimitBucket {
    pub(crate) count: u32,
    pub(crate) window_started_ms: u64,
}

// ── AppState ─────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct AppState {
    pub config: BootstrapConfig,
    pub runtime: Arc<RuntimeServices>,
    pub evals: Arc<ProductEvalRunService>,
    pub eval_baselines: Arc<EvalBaselineServiceImpl>,
    pub eval_datasets: Arc<EvalDatasetServiceImpl>,
    #[allow(dead_code)]
    pub model_comparisons: Arc<ModelComparisonServiceImpl>,
    pub eval_rubrics: Arc<EvalRubricServiceImpl>,
    pub runtime_sse_tx: broadcast::Sender<SseFrame>,
    /// Ring buffer of the last 10,000 SSE frames with monotonic sequence IDs.
    /// Clients use Last-Event-ID to replay missed events after reconnect (RFC 002).
    pub sse_event_buffer: Arc<std::sync::RwLock<VecDeque<(u64, SseFrame)>>>,
    /// Monotonic counter for SSE frame sequence IDs.
    pub sse_seq: Arc<std::sync::atomic::AtomicU64>,
    pub graph: Arc<InMemoryGraphStore>,
    pub document_store: Arc<InMemoryDocumentStore>,
    pub retrieval: Arc<InMemoryRetrieval>,
    pub deep_search: Arc<AppDeepSearch>,
    pub ingest: Arc<AppIngestPipeline>,
    pub diagnostics: Arc<InMemoryDiagnostics>,
    pub feed: Arc<FeedStore>,
    pub bundle_import: Arc<InMemoryImportService>,
    pub bundle_export: Arc<InMemoryExportService>,
    pub source_metadata: Arc<Mutex<HashMap<String, AppSourceMetadata>>>,
    /// Cache of prompt version content + template vars, keyed by version_id.
    pub version_content: Arc<Mutex<HashMap<String, AppVersionContent>>>,
    pub pending_ingest_jobs: Arc<Mutex<HashMap<String, PendingIngestJobPayload>>>,
    pub mailbox_messages: Arc<Mutex<HashMap<String, AppMailboxMessage>>>,
    pub templates: Arc<StarterTemplateRegistry>,
    pub service_tokens: Arc<ServiceTokenRegistry>,
    /// Per-operator API token metadata store (token_id -> record + raw token).
    /// Separate from `service_tokens` which only holds the auth lookup map.
    pub operator_tokens: Arc<OperatorTokenStore>,
    pub plugin_registry: Arc<InMemoryPluginRegistry>,
    pub plugin_host: Arc<Mutex<StdioPluginHost>>,
    /// RFC 015: plugin marketplace service -- manages discover/install/enable lifecycle.
    pub marketplace: Arc<Mutex<MarketplaceService<cairn_store::InMemoryStore>>>,
    /// RFC 022: trigger service -- manages triggers and run templates.
    ///
    /// RFC-025 Phase 1.5a: the service is projection-backed rather
    /// than rebuilt from the event log at boot. The struct holds an
    /// `Arc<InMemoryStore>` internally and issues async projection
    /// reads / event log writes; no in-memory HashMaps are held here
    /// and no outer `Mutex` is needed (the service is stateless +
    /// clone-cheap). The concrete `InMemoryStore` type is the same
    /// `Arc` that `runtime.store` holds, so the projection reads hit
    /// the same rows as the rest of the runtime.
    ///
    /// The concrete `InMemoryStore` binding matches the rest of
    /// `AppState` — every service-layer field on `AppState` is bound
    /// to `InMemoryStore` today because `runtime.store` is an
    /// `Arc<InMemoryStore>`. Persistent backends (pg/sqlite) are wired
    /// via the dual-write `set_secondary_log` path on the primary
    /// in-memory store; the primary projections + event log are still
    /// the source of truth that every service reads from. Migrating
    /// `AppState` to a generic `<S: Store>` or `Arc<dyn Store>` shape
    /// is RFC-025 Phase 4 scope; flipping just the trigger service
    /// here would create a lopsided seam.
    pub triggers: Arc<TriggerService<cairn_store::InMemoryStore>>,
    pub repo_clone_cache: Arc<cairn_workspace::RepoCloneCache>,
    pub project_repo_access: Arc<cairn_workspace::ProjectRepoAccessService>,
    /// Per-project set of local-filesystem paths attached via
    /// `host=local_fs` on `POST /v1/projects/:project/repos`. Parallel to
    /// `project_repo_access` (which enforces the RFC 016 `owner/repo`
    /// shape); this map stores arbitrary directory paths so operators
    /// can point cairn at a local checkout as a pseudo-repo.
    pub project_local_paths: Arc<crate::repo_routes::ProjectLocalPaths>,
    /// Sandbox service accessed via the `SandboxServiceApi` trait (issue #443).
    /// Holding the trait object keeps cairn-app from binding to the concrete
    /// shape of `cairn_workspace::SandboxService` and enables mock-based
    /// tests. The concrete instance is constructed in `AppState::new` and
    /// widened to `Arc<dyn SandboxServiceApi>` at the assignment site.
    pub sandbox_service: Arc<dyn cairn_workspace::SandboxServiceApi>,
    pub(crate) sqeq_sessions: Arc<Mutex<HashMap<String, SqEqSessionBinding>>>,
    pub(crate) a2a_tasks: Arc<Mutex<HashMap<String, A2aTaskBinding>>>,
    pub rate_limits: Arc<Mutex<HashMap<String, RateLimitBucket>>>,
    /// T6b-C6: timestamp of the most recent eviction sweep over
    /// `rate_limits`. Used to amortize the O(N) retain pass so an
    /// attacker keeping the bucket map at the threshold can't force
    /// a sweep on every request.
    pub rate_limit_last_sweep_ms: Arc<std::sync::atomic::AtomicU64>,
    pub metrics: Arc<AppMetrics>,
    pub memory_api: Arc<MemoryApiImpl<InMemoryRetrieval>>,
    #[allow(dead_code)]
    pub memory_proposal_hook: Arc<crate::sse_hooks::SseMemoryProposalHook>,
    pub started_at: Instant,
    /// OTLP span exporter (RFC 021). Disabled by default.
    pub otlp_exporter: Arc<cairn_runtime::telemetry::OtlpExporter>,
    /// Brain LLM provider for orchestration -- set post-construction by main.rs
    /// once the concrete provider (Ollama or OpenAI-compat) is configured.
    /// `None` means orchestration is unavailable until a provider is configured.
    pub brain_provider: Option<Arc<dyn cairn_domain::providers::GenerationProvider>>,
    /// Bedrock provider -- used when the model_id is a Bedrock model (e.g. minimax.minimax-m2.5).
    pub bedrock_provider: Option<Arc<dyn cairn_domain::providers::GenerationProvider>>,
    /// Built-in tool registry wired by main.rs with real memory backends.
    /// `None` until set -- orchestrate handler falls back to stub dispatcher.
    pub tool_registry: Option<Arc<cairn_tools::BuiltinToolRegistry>>,
    /// Ring buffer of the last 2,000 structured request log entries, populated
    /// by the observability middleware.  Consumed by `GET /v1/admin/logs`.
    pub request_log: Arc<std::sync::RwLock<RequestLogBuffer>>,
    /// Integration plugin registry -- holds all configured integrations (GitHub, Linear, etc.).
    pub integrations: Arc<cairn_integrations::IntegrationRegistry>,
    /// Model catalog — per-model metadata including cost rates and capabilities.
    /// Operators can override entries at runtime via the admin API.
    pub model_registry: ModelRegistry,
    /// FlowFabric services aggregate — `Some` in production (when
    /// `build_runtime_with_optional_fabric` successfully boots
    /// `FabricServices`). `None` only when a test fixture has injected a
    /// read-only runtime (see `crates/cairn-app/tests/support/fake_fabric.rs`).
    /// When `Some`, `runtime.runs / tasks / sessions` are the Fabric
    /// adapters (see `crate::fabric_adapter`); handlers call through the
    /// trait unchanged.
    ///
    /// Rare direct-access handlers (e.g. admin inspect endpoints) may reach
    /// through this field to `FabricServices::budgets`, `quotas`,
    /// `scheduler`, `signals` which aren't on the core trait surface.
    pub fabric: Option<Arc<cairn_fabric::FabricServices>>,
    /// RFC 020 §"Startup order": shared readiness state that the startup
    /// sequence mutates and the `/health/ready` handler + readiness
    /// middleware read. Starts with all branches `Pending`; flipped to
    /// `ready` once the startup graph completes (see `main.rs`).
    pub readiness: ReadinessState,
    /// RFC 020 Track 3: shared tool-call result cache, consulted by
    /// `RuntimeExecutePhase` before every tool dispatch. Populated at
    /// startup from the event log (every prior `ToolInvocationCompleted`)
    /// and incrementally on each completion thereafter. Wrapped in
    /// `Arc<Mutex<_>>` because both the orchestrator (hot path on each
    /// tool call) and the startup replay share ownership.
    pub tool_result_cache: Arc<std::sync::Mutex<cairn_runtime::startup::ToolCallResultCache>>,
    /// Skills catalog (GAP-012 / issue #147): in-process registry of
    /// capability bundles. Read by `GET /v1/skills` and `/v1/skills/:id`;
    /// populated by workers/operators via the skills registration API.
    /// Starts empty — UI shows an empty-state prompt until a worker
    /// registers a skill.
    pub skill_catalog: Arc<tokio::sync::RwLock<cairn_domain::skills::SkillCatalog>>,
    /// Cached providers-with-counts summary for `GET /v1/models/catalog/providers`.
    /// Populated on first request; immutable thereafter for the process
    /// lifetime. Runtime overrides via the admin CRUD API do NOT invalidate
    /// this cache — see module doc on `handlers::model_catalog`.
    pub model_catalog_providers_cache:
        Arc<std::sync::OnceLock<Vec<crate::handlers::model_catalog::ProviderCount>>>,
    /// Background task that derives lifecycle metrics from the event
    /// log broadcast. Kept on `AppState` so its lifetime tracks the
    /// process; drop/cancel is managed by shutdown paths.
    #[cfg(any(feature = "metrics-core", feature = "metrics-providers"))]
    pub metrics_tap: Option<crate::metrics_tap::MetricsTap>,
    /// Background task exporting RuntimeEvents as OTLP spans over
    /// HTTP/protobuf. Enabled when the `metrics-otel` feature is on
    /// AND `CAIRN_OTLP_ENABLED` is truthy at boot. `None` otherwise.
    #[cfg(feature = "metrics-otel")]
    pub otlp_tap: Option<crate::metrics_otel::OtlpTap>,
    /// Handle to the batching sink wrapping the OTLP transport.
    /// Kept separately so `shutdown_telemetry()` can flush any
    /// buffered spans before process exit. `None` when the OTLP
    /// export is not wired.
    #[cfg(feature = "metrics-otel")]
    pub otlp_batch: Option<Arc<crate::metrics_otel::BatchingSink>>,
    /// Provider-fallback cooldown storage partitioned by
    /// `(tenant_id, binding_id)` so a rate-limit on one tenant/connection
    /// does NOT cool down the same `model_id` for unrelated tenants or
    /// sibling connections with different credentials. Keyed internally
    /// by `model_id` inside each scoped `CooldownMap`. Populated when
    /// `provider.generate()` returns `ProviderAdapterError::RateLimited`
    /// so subsequent orchestrate calls skip the cooled-down model for
    /// `DEFAULT_RATE_LIMIT_COOLDOWN` (5 min). Cleared on process
    /// restart by design (in-memory; event-sourced cooldown is a
    /// follow-up).
    pub provider_fallback_cooldown: Arc<ScopedProviderFallbackCooldown>,
    /// F49: queue for auto-resume orchestrate kicks. Always present on
    /// AppState — the inner channel is an `OnceLock` inside the sender
    /// that `main.rs` installs after the HTTP listener binds. When an
    /// approval resolves AND the run has no other pending approvals
    /// AND the run is state=running, the SSE publish loop calls
    /// `OrchestrateKickSender::kick(run_id)`, which is a no-op before
    /// install (early startup / non-http roles). The worker spawned
    /// from `main.rs` drains the channel and POSTs
    /// `/v1/runs/:id/orchestrate` via reqwest on the local listener.
    pub orchestrate_kick_tx: Arc<OrchestrateKickSender>,
    /// F50: operator-visible notification sink. Previously only the
    /// admin `/v1/events/append` path pushed notifications, which meant
    /// service-layer approvals/run-failures never reached the bell icon
    /// or sidebar badge. The sink is a trait object so the concrete
    /// buffer stays in the binary (preserving the crate boundary) while
    /// the lib-level SSE publish loop can still push into it.
    pub notification_sink: Arc<NotificationSink>,
    /// #433: per-tenant per-endpoint Idempotency-Key cache for the
    /// orchestrate handler (the only endpoint wired in this PR — see
    /// `idempotency.rs` module doc for the rationale and follow-up
    /// wiring plan for create-run / create-tool-invocation). Entries
    /// expire after 5 min; cap is 5_000 entries with amortized
    /// eviction. In-process only — multi-node team mode still lets a
    /// retry hit a different node; that gap closes with a future
    /// FF-backed shared cache.
    pub idempotency_cache: Arc<crate::idempotency::IdempotencyCache>,
    /// #639: background lease-keeper registry.
    ///
    /// One `tokio::spawn`'d keeper per live run; each keeper calls
    /// `RunService::renew_lease_if_stale` every `lease_ttl_ms / 3` so
    /// long approval-paced flows can't let FF's lease expire between
    /// orchestrate HTTP calls. The registry is atomic on
    /// `ensure_running` (concurrent orchestrate handlers don't spawn
    /// duplicates) and the keeper self-exits on terminal state or
    /// fatal renew error. See `crate::lease_keeper` for the full
    /// contract.
    pub lease_keepers: Arc<crate::lease_keeper::LeaseKeeperRegistry>,
}

/// F50: dynamic-dispatch wrapper so the lib crate can push
/// `OperatorNotification` values without depending on the binary's
/// concrete `NotificationBuffer` type.
#[derive(Debug, Default)]
pub struct NotificationSink {
    inner: std::sync::OnceLock<Arc<dyn OperatorNotificationSink>>,
}

impl NotificationSink {
    pub fn new() -> Self {
        Self::default()
    }

    /// Install the sink once the binary has built its `NotificationBuffer`.
    /// Idempotent — first install wins.
    pub fn install(&self, sink: Arc<dyn OperatorNotificationSink>) {
        let _ = self.inner.set(sink);
    }

    /// Push a notification. No-op when the sink hasn't been installed
    /// yet (early startup).
    pub fn push(&self, n: OperatorNotification) {
        if let Some(sink) = self.inner.get() {
            sink.push(n);
        }
    }
}

/// F50: a single operator notification destined for the in-memory bell
/// buffer. Mirrors the fields of the binary's private `Notification`
/// type but stays crate-public so the lib can construct them.
///
/// `tenant_id` is recorded on every notification so the `/v1/
/// notifications` handler can filter cross-tenant leakage in a future
/// pass (v1 currently returns the global buffer unfiltered for
/// backwards compatibility with the bell UI). Multi-tenant deployments
/// should upgrade the handler to drop rows whose `tenant_id` does not
/// match the caller principal's tenant.
#[derive(Clone, Debug)]
pub struct OperatorNotification {
    pub id: String,
    pub notif_type: OperatorNotificationType,
    pub message: String,
    pub entity_id: Option<String>,
    pub href: String,
    pub created_at_ms: u64,
    /// Originating event's tenant, extracted from the envelope's
    /// `ownership` field. `None` for system-scoped events that are
    /// visible to every tenant (same semantics as SSE fan-out).
    pub tenant_id: Option<String>,
}

/// F50: notification categories the bell icon distinguishes.
#[derive(Clone, Copy, Debug)]
pub enum OperatorNotificationType {
    ApprovalRequested,
    ApprovalResolved,
    RunCompleted,
    RunFailed,
    TaskStuck,
}

/// F50: thin trait the binary implements on its concrete buffer. Keeps
/// the bin-level `NotificationBuffer` free of lib dependencies.
pub trait OperatorNotificationSink: Send + Sync + std::fmt::Debug {
    fn push(&self, notification: OperatorNotification);
}

/// F49: thin wrapper so the channel sender can be stored in a
/// pre-existing AppState with zero placeholders.
///
/// The inner `OnceLock<Sender>` is set by `main.rs` once the HTTP
/// listener has bound a port and the worker task has started.
/// `send(run_id)` is a no-op when the worker is not running — the
/// approval handler still succeeds, operators just lose the
/// auto-resume affordance (same behaviour as pre-F49).
#[derive(Debug, Default)]
pub struct OrchestrateKickSender {
    inner: std::sync::OnceLock<tokio::sync::mpsc::UnboundedSender<cairn_domain::RunId>>,
}

impl OrchestrateKickSender {
    pub fn new() -> Self {
        Self::default()
    }

    /// Install the sender end once the background worker is running.
    /// Idempotent — subsequent calls are dropped (the first sender wins).
    pub fn install(&self, tx: tokio::sync::mpsc::UnboundedSender<cairn_domain::RunId>) {
        let _ = self.inner.set(tx);
    }

    /// Best-effort enqueue of a run_id for auto-resume. Returns `true`
    /// when the channel is installed and the send succeeded.
    pub fn kick(&self, run_id: cairn_domain::RunId) -> bool {
        match self.inner.get() {
            Some(tx) => tx.send(run_id).is_ok(),
            None => false,
        }
    }
}

/// In-memory provider-fallback cooldown storage partitioned by
/// `(tenant_id, binding_id)`. Each partition holds its own
/// [`cairn_orchestrator::CooldownMap`] so rate-limit events are isolated.
///
/// See [`AppState::provider_fallback_cooldown`] for semantics.
#[derive(Debug, Default)]
pub struct ScopedProviderFallbackCooldown {
    inner: std::sync::Mutex<
        std::collections::HashMap<(String, String), cairn_orchestrator::CooldownMap>,
    >,
}

impl ScopedProviderFallbackCooldown {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fetch (or lazily create) the `CooldownMap` for the given
    /// `(tenant_id, binding_id)` scope.
    ///
    /// Prunes scopes whose inner `CooldownMap` is empty before returning —
    /// this keeps the outer map bounded under tenant/connection churn
    /// since `CooldownMap` entries already self-expire after their window.
    /// Without this sweep the outer map could grow unboundedly even
    /// though every inner entry had long since expired.
    pub fn get_or_create(
        &self,
        tenant_id: &str,
        binding_id: &str,
    ) -> cairn_orchestrator::CooldownMap {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());

        // Light amortised sweep: drop scopes whose CooldownMap is empty.
        // Cheap because `CooldownMap::is_empty` is a single mutex
        // acquisition + HashMap::retain. At steady state the outer map
        // only keeps scopes that are currently cooling down something,
        // so size is bounded by the number of simultaneously-throttled
        // (tenant, binding) pairs.
        guard.retain(|_, cooldown| !cooldown.is_empty());

        guard
            .entry((tenant_id.to_owned(), binding_id.to_owned()))
            .or_default()
            .clone()
    }

    /// Number of tracked scopes (after pruning). Primarily for
    /// observability / tests.
    pub fn scope_count(&self) -> usize {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.retain(|_, cooldown| !cooldown.is_empty());
        guard.len()
    }
}

// ── AppState impl ────────────────────────────────────────────────────────────

impl AppState {
    // RFC-025 Phase 1.5b (2026-04-28): `replay_graph` removed.
    //
    // The graph read-model is declared Ephemeral. `AppState.graph` is
    // `Arc<InMemoryGraphStore>` -- a process-scoped derived index over
    // the event log. Every event-log append routed through
    // `publish_runtime_frames_since` (`crates/cairn-app/src/handlers/sse.rs`)
    // already projects the event into the graph on the write path, so
    // the boot walker was duplicating that logic O(N) times per restart.
    //
    // Post-removal semantics: on persistent backends (pg/sqlite), graph
    // queries against pre-restart node IDs return empty subgraphs until
    // those entities participate in new events. This matches the
    // Ephemeral contract already applied to TaskDependencyAdded /
    // TaskDependencyResolved in `crates/cairn-store/src/projection_registry.rs`
    // ("graph projection owns the read model, not cairn-store").
    //
    // If provenance traversal across restarts becomes a product
    // requirement, a later RFC-025 phase wires `PgGraphStore`
    // (`crates/cairn-graph/src/pg/store.rs`) + a matching SQLite store
    // behind `GraphProjection` on `AppState.graph`. That migration
    // drops any boot walker too (the store survives restart under its
    // own backend) -- it does not reintroduce the replay path.
    //
    // See docs/design/rfcs/RFC-025-runtime-aggregate-backend-abstraction.md
    // §"Phase 1.5b" for the full rationale.

    // RFC-025 Phase 1 (milestone 6): `replay_evals` removed.
    //
    // Before Phase 1, `state.evals` was a standalone in-memory service that
    // did NOT read from the event log on its own, so boot had to walk the
    // full log (O(N) per process restart per domain) to rebuild it. That
    // walker was #437: a hand-rolled projection outside the SyncProjection
    // framework, with two projection paths for the same domain and only
    // one (the in-memory one) actually backed by the event log.
    //
    // Milestones 3/4 land real `eval_runs` projection tables on pg +
    // sqlite (V034 migration), wired inside `PgSyncProjection::apply_async`
    // / `SqliteSyncProjection::apply_async`. The in-memory projection
    // applier in `cairn-store::in_memory` builds the same read model for
    // `--db memory`. All three backends now expose `EvalRunReadModel` and
    // are byte-equal per the projection_parity harness in
    // `crates/cairn-store/tests/projection_parity.rs`.
    //
    // `state.evals` (the standalone in-memory service) continues to exist
    // as a lazy hot-path cache for the handler response bodies that still
    // return the richer `EvalRun` shape (includes plugin_metrics etc.)
    // rather than the `EvalRunRecord` projection shape. It is populated on
    // the write path inside each handler; a process restart drops the cache
    // but every durable field reads back from the projection.

    // RFC-025 Phase 1.5a: `replay_triggers` deleted (2026-04-29).
    //
    // Durable trigger / run_template / trigger-fire state lives in the
    // `triggers` / `run_templates` / `trigger_fires` projection tables
    // created by pg V035 + sqlite/schema.rs. The service reads them
    // directly on every query; no boot-time event-log walk is needed.
    // See `crates/cairn-runtime/src/services/trigger_service.rs`.

    pub async fn new(config: BootstrapConfig) -> Result<Self, String> {
        // Load the credential master key BEFORE any runtime construction so
        // a misconfigured team-mode deployment fails fast with a
        // single, unambiguous error line in boot logs rather than partway
        // through a lengthy Fabric connect. See `load_master_key` for the
        // source-priority and fail-loud semantics.
        let master_key = load_master_key(&config)?;

        // Construct FabricServices + install the FabricAdapter trio for
        // runs/tasks/sessions. Any boot failure on the Fabric path
        // (unreachable Valkey, HMAC validation, …) surfaces here before
        // cairn-app starts serving traffic — no silent fall-back.
        let (runtime, fabric) = build_runtime_with_optional_fabric(master_key).await?;
        Self::new_with_runtime(config, runtime, fabric).await
    }

    /// Build an `AppState` around a caller-provided runtime.
    ///
    /// Used by integration tests that need to inject a test fixture
    /// (e.g. `FakeFabric` under `tests/support/`) in place of live Fabric —
    /// the ancillary state (graph, eval services, metrics, tokens) wires up
    /// identically either way.
    ///
    /// Production callers use [`Self::new`], which builds the runtime via
    /// `build_runtime_with_optional_fabric` and then delegates here.
    pub async fn new_with_runtime(
        config: BootstrapConfig,
        runtime: Arc<RuntimeServices>,
        fabric: Option<Arc<cairn_fabric::FabricServices>>,
    ) -> Result<Self, String> {
        // Surface any `Stubbed` RuntimeEvent variants at boot so pg/sqlite
        // operators see the silent-no-op inventory instead of discovering
        // it via empty API reads. In-memory skips the stubbed check (it
        // materialises projections through a separate code path that
        // cannot leak empty reads), but still runs
        // `assert_no_stubs_for_in_memory` so the uniform call path is
        // exercised. See
        // `docs/design/rfcs/RFC-025-runtime-aggregate-backend-abstraction.md`
        // for the registry contract.
        let projection_check: Result<(), cairn_store::RegistryError> = match &config.storage {
            cairn_api::bootstrap::StorageBackend::InMemory => {
                cairn_store::assert_no_stubs_for_in_memory()
            }
            cairn_api::bootstrap::StorageBackend::Sqlite { .. } => {
                cairn_store::assert_no_stubs_for_persistent_backend(cairn_store::Backend::Sqlite)
            }
            cairn_api::bootstrap::StorageBackend::Postgres { .. } => {
                cairn_store::assert_no_stubs_for_persistent_backend(cairn_store::Backend::Postgres)
            }
        };
        if let Err(err) = projection_check {
            tracing::warn!(
                %err,
                backend = crate::errors::storage_backend_label(&config.storage),
                "projection registry carries Stubbed variants"
            );
        }

        let graph = Arc::new(InMemoryGraphStore::new());
        let plugin_registry = Arc::new(InMemoryPluginRegistry::new());
        let document_store = Arc::new(InMemoryDocumentStore::new());
        let diagnostics = Arc::new(InMemoryDiagnostics::new());
        let evals = Arc::new(
            ProductEvalRunService::with_graph_and_event_log(
                Arc::new(EvalGraphIntegration::new(graph.clone())),
                runtime.store.clone(),
            )
            .with_memory_diagnostics(Arc::new(DiagnosticsAdapter(diagnostics.clone()))),
        );
        let eval_baselines = Arc::new(EvalBaselineServiceImpl::new(evals.clone()));
        let eval_datasets = Arc::new(EvalDatasetServiceImpl::new());
        let model_comparisons = Arc::new(ModelComparisonServiceImpl::new());
        let eval_rubrics = Arc::new(EvalRubricServiceImpl::with_plugin_scorer(
            evals.clone(),
            eval_datasets.clone(),
            Arc::new(AppPluginRubricScorer {
                plugin_registry: plugin_registry.clone(),
            }),
        ));
        // NOTE(F34b): retrieval here is wired over the unconditionally in-memory
        // `InMemoryDocumentStore` created above, with no embedding provider.
        // This entire retrieval stack is placeholder — the external memory
        // crate (embedder + reranker + persistent store) replaces it in a
        // future PR. The surviving crash surface — `memory_search` with
        // `mode=vector` hitting the missing-embedder branch — is guarded at
        // the tool layer in `tool_impls::ConcreteMemorySearchTool` (F34a).
        // Backend-selection cleanup (swapping `InMemoryDocumentStore` for a
        // persistent store and threading an embedder) is tracked as F34b and
        // will likely be superseded by the external memory crate integration.
        let retrieval = Arc::new(
            InMemoryRetrieval::with_diagnostics(document_store.clone(), diagnostics.clone())
                .with_graph(graph.clone()),
        );
        // RFC 029 PR-B1: wrap the inner retrieval in MultiProviderRetrieval
        // so every deep-search hop dispatches through the same provider
        // resolver as the agent's memory_search tool.
        // RFC 029 PR-B2: attach PostHocRescorer so hops see runtime-owned
        // scoring dimensions; the deep-search quality gate's threshold
        // compares the rescored `score`, not the provider's raw score.
        let deep_search_inner = Arc::new(InMemoryRetrieval::new(document_store.clone()));
        let deep_search_rescorer = PostHocRescorer::new(graph.clone(), NoOpCredibilityLookup);
        let deep_search = Arc::new(
            IterativeDeepSearch::new(
                MultiProviderRetrieval::new(
                    deep_search_inner,
                    EventLogProviderResolver::new(runtime.store.clone()),
                    UnavailablePluginDispatcher,
                )
                .with_response_hook(deep_search_rescorer),
            )
            .with_graph_hook(GraphBackedExpansion::new(graph.clone())),
        );
        let ingest = Arc::new(IngestPipeline::new(
            document_store.clone(),
            ParagraphChunker::default(),
        ));
        let feed = Arc::new(FeedStore::new());
        let bundle_import = Arc::new(InMemoryImportService::new(document_store.clone()));
        let bundle_export = Arc::new(InMemoryExportService::new(
            document_store.clone(),
            runtime.store.clone(),
            "cairn-app",
        ));
        let source_metadata = Arc::new(Mutex::new(HashMap::new()));
        let version_content: Arc<Mutex<HashMap<String, AppVersionContent>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let pending_ingest_jobs = Arc::new(Mutex::new(HashMap::new()));
        let mailbox_messages = Arc::new(Mutex::new(HashMap::new()));
        let service_tokens = Arc::new(ServiceTokenRegistry::new());
        let plugin_host = Arc::new(Mutex::new(StdioPluginHost::new()));
        let repo_clone_cache = Arc::new(cairn_workspace::RepoCloneCache::default());
        let project_repo_access = Arc::new(cairn_workspace::ProjectRepoAccessService::new());
        let project_local_paths = Arc::new(crate::repo_routes::ProjectLocalPaths::default());
        let sqeq_sessions = Arc::new(Mutex::new(HashMap::new()));
        let a2a_tasks = Arc::new(Mutex::new(HashMap::new()));
        let sandbox_repo_source = Arc::new(cairn_workspace::providers::RepoCloneCacheSource::new(
            repo_clone_cache.clone(),
        ));
        let sandbox_event_sink = Arc::new(crate::telemetry_routes::UsageSandboxEventSink::new(
            runtime.store.clone(),
            Arc::new(cairn_workspace::BufferedSandboxEventSink::default()),
        ));
        // F65 PR-5: bridge the cairn-workspace F65 event sink to the
        // real cairn-store event log. Fire-and-forget tokio::spawn
        // delivery per `sandbox_f65_bridges::StoreF65EventSink`.
        let f65_event_sink: Arc<dyn cairn_workspace::sandbox::f65::F65SandboxEventSink> = Arc::new(
            crate::sandbox_f65_bridges::StoreF65EventSink::new(runtime.store.clone()),
        );
        // F65 PR-5: bridge the workspace-local WorkspaceSnapshotWriter
        // trait to the cairn-store impl on InMemoryStore.
        let f65_snapshot_writer: Arc<dyn cairn_workspace::sandbox::f65::WorkspaceSnapshotWriter> =
            Arc::new(crate::sandbox_f65_bridges::StoreSnapshotWriter::new(
                runtime.store.clone(),
            ));
        let sandbox_service = Arc::new(
            cairn_workspace::SandboxService::new(
                HashMap::from([
                    (
                        cairn_workspace::SandboxStrategy::Overlay,
                        Box::new(cairn_workspace::OverlayProvider::with_repo_source(
                            default_sandbox_base_dir(),
                            sandbox_repo_source.clone(),
                        )) as Box<dyn cairn_workspace::SandboxProvider>,
                    ),
                    (
                        cairn_workspace::SandboxStrategy::Reflink,
                        Box::new(cairn_workspace::ReflinkProvider::with_repo_source(
                            default_sandbox_base_dir(),
                            sandbox_repo_source,
                        )) as Box<dyn cairn_workspace::SandboxProvider>,
                    ),
                ]),
                sandbox_event_sink,
                default_sandbox_base_dir(),
                Arc::new(cairn_workspace::SystemClock),
            )
            // RFC 020 §"Run recovery matrix" — `AllowlistRevoked` row. Wire
            // the project-scoped repo allowlist so the recovery sweep can
            // detect repo bindings that are no longer authorised and emit
            // `SandboxAllowlistRevoked` for the run-level recovery service
            // to synthesize an operator approval against.
            .with_allowlist(project_repo_access.clone())
            // RFC 020 §"Run recovery matrix" — `BaseRevisionDrift` row.
            // Wire the repo clone cache so the recovery sweep can diff a
            // sandbox's stored `base_revision` against the live clone HEAD
            // and emit `SandboxBaseRevisionDrift` when the clone moved
            // between provisioning and recovery. Overlay-only; reflink
            // sandboxes are exempt per RFC 016 (physically independent).
            .with_clone_cache(repo_clone_cache.clone())
            // F65 PR-5: wire snapshot root + F65 event sink + snapshot
            // writer. The snapshot root is configurable via
            // `CAIRN_SNAPSHOT_DIR` (plan §2.1) with a sensible default
            // under the process temp dir for dev/CI; production sets
            // it to `~/.cairn/snapshots`.
            .with_snapshot_dir(default_snapshot_dir())
            .with_f65_event_sink(f65_event_sink)
            .with_snapshot_writer(f65_snapshot_writer),
        );
        // Widen to the trait object for storage on `AppState`. Every
        // consumer inside cairn-app only needs `SandboxServiceApi`; the
        // concrete type stays live for the GC-sweeper wiring below (which
        // takes `Arc<dyn SandboxServiceApi>` too, so no downcast needed).
        let sandbox_service: Arc<dyn cairn_workspace::SandboxServiceApi> = sandbox_service;
        // RFC 015: marketplace service wrapping the plugin host.
        let marketplace = {
            let mut svc = MarketplaceService::new(runtime.store.clone());
            svc.load_bundled_catalog();
            Arc::new(Mutex::new(svc))
        };
        let rate_limits = Arc::new(Mutex::new(HashMap::new()));
        let rate_limit_last_sweep_ms = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let metrics = Arc::new(AppMetrics::default());
        let (runtime_sse_tx, _) = broadcast::channel(256);
        let sse_event_buffer = Arc::new(std::sync::RwLock::new(
            VecDeque::<(u64, SseFrame)>::with_capacity(10_000),
        ));
        let sse_seq = Arc::new(std::sync::atomic::AtomicU64::new(1));
        let memory_proposal_hook =
            Arc::new(crate::sse_hooks::SseMemoryProposalHook::with_sse_channel(
                runtime_sse_tx.clone(),
                sse_event_buffer.clone(),
                sse_seq.clone(),
            ));
        let memory_api = Arc::new(
            MemoryApiImpl::new(
                InMemoryRetrieval::with_diagnostics(document_store.clone(), diagnostics.clone())
                    .with_graph(graph.clone()),
                document_store.clone(),
            )
            .with_proposal_hook(Box::new(crate::sse_hooks::SharedMemoryProposalHook(
                memory_proposal_hook.clone(),
            ))),
        );

        runtime
            .tenants
            .create(
                TenantId::new(DEFAULT_TENANT_ID),
                "Default Tenant".to_owned(),
            )
            .await
            .map_err(|err| format!("failed to seed default tenant: {err}"))?;
        runtime
            .workspaces
            .create(
                TenantId::new(DEFAULT_TENANT_ID),
                WorkspaceId::new(DEFAULT_WORKSPACE_ID),
                "Default Workspace".to_owned(),
            )
            .await
            .map_err(|err| format!("failed to seed default workspace: {err}"))?;
        runtime
            .projects
            .create(
                ProjectKey::new(DEFAULT_TENANT_ID, DEFAULT_WORKSPACE_ID, DEFAULT_PROJECT_ID),
                "Default Project".to_owned(),
            )
            .await
            .map_err(|err| format!("failed to seed default project: {err}"))?;
        runtime
            .licenses
            .activate(
                TenantId::new(DEFAULT_TENANT_ID),
                crate::deployment_mode_tier(config.mode),
                None,
            )
            .await
            .map_err(|err| format!("failed to seed default license: {err}"))?;

        #[allow(unused_mut)]
        let mut state = Self {
            config,
            document_store,
            retrieval,
            deep_search,
            ingest,
            diagnostics,
            feed,
            bundle_import,
            bundle_export,
            source_metadata,
            version_content,
            pending_ingest_jobs,
            mailbox_messages,
            templates: Arc::new(StarterTemplateRegistry::v1_defaults()),
            service_tokens,
            operator_tokens: Arc::new(OperatorTokenStore::new()),
            plugin_registry,
            plugin_host,
            marketplace,
            triggers: Arc::new(TriggerService::new(runtime.store.clone())),
            repo_clone_cache,
            project_repo_access,
            project_local_paths,
            sandbox_service,
            sqeq_sessions,
            a2a_tasks,
            rate_limits,
            rate_limit_last_sweep_ms,
            metrics,
            memory_api,
            memory_proposal_hook,
            started_at: Instant::now(),
            // Placeholder — swapped out below if metrics-otel is
            // enabled AND `CAIRN_OTLP_ENABLED` is set at boot.
            otlp_exporter: Arc::new(cairn_runtime::telemetry::OtlpExporter::disabled()),
            #[cfg(feature = "metrics-otel")]
            otlp_tap: None,
            #[cfg(feature = "metrics-otel")]
            otlp_batch: None,
            runtime_sse_tx,
            sse_event_buffer,
            sse_seq,
            runtime,
            evals,
            eval_baselines,
            eval_datasets,
            model_comparisons,
            eval_rubrics,
            graph,
            brain_provider: None,
            bedrock_provider: None,
            tool_registry: None,
            request_log: Arc::new(std::sync::RwLock::new(RequestLogBuffer::new())),
            integrations: Arc::new(cairn_integrations::IntegrationRegistry::new()),
            model_registry: ModelRegistry::with_bundled()
                .unwrap_or_else(|_| ModelRegistry::empty()),
            fabric,
            readiness: ReadinessState::new(),
            tool_result_cache: Arc::new(std::sync::Mutex::new(
                cairn_runtime::startup::ToolCallResultCache::new(),
            )),
            skill_catalog: Arc::new(tokio::sync::RwLock::new(
                cairn_domain::skills::SkillCatalog::new(),
            )),
            model_catalog_providers_cache: Arc::new(std::sync::OnceLock::new()),
            #[cfg(any(feature = "metrics-core", feature = "metrics-providers"))]
            metrics_tap: None,
            provider_fallback_cooldown: Arc::new(ScopedProviderFallbackCooldown::new()),
            orchestrate_kick_tx: Arc::new(OrchestrateKickSender::new()),
            notification_sink: Arc::new(NotificationSink::new()),
            idempotency_cache: Arc::new(crate::idempotency::IdempotencyCache::new()),
            lease_keepers: Arc::new(crate::lease_keeper::LeaseKeeperRegistry::new()),
        };
        state.runtime.store.reset_usage_counters();

        #[cfg(any(feature = "metrics-core", feature = "metrics-providers"))]
        {
            let tap = crate::metrics_tap::MetricsTap::spawn(
                state.runtime.store.clone(),
                state.metrics.clone(),
            );
            state.metrics_tap = Some(tap);
        }

        #[cfg(feature = "metrics-otel")]
        {
            use cairn_runtime::telemetry::OtlpExporter;
            let cfg = crate::metrics_otel::otlp_config_from_env();
            if cfg.enabled {
                // Only HTTP/protobuf is wired today. Warn loudly if
                // the operator asked for a transport we can't
                // deliver so the mismatch is visible in boot logs
                // instead of silently falling back.
                if !matches!(
                    cfg.protocol,
                    cairn_domain::protocols::OtlpProtocol::HttpBinary
                ) {
                    tracing::warn!(
                        requested = ?cfg.protocol,
                        fallback = "http/protobuf",
                        "CAIRN_OTLP_PROTOCOL requested an unsupported transport; \
                         falling back to http/protobuf. gRPC + http/json are \
                         tracked as follow-up work."
                    );
                }
                // Wrap HttpProtoSink in BatchingSink so bursty event
                // streams don't blast the collector with one POST per
                // event. 64 spans or 2 s, whichever fires first —
                // matches the defaults in RFC 021's exporter sketch.
                let transport = std::sync::Arc::new(crate::metrics_otel::HttpProtoSink::new(
                    &cfg.endpoint,
                    &cfg.service_name,
                ));
                let batched = std::sync::Arc::new(crate::metrics_otel::BatchingSink::new(
                    transport,
                    64,
                    std::time::Duration::from_millis(2_000),
                ));
                // The OtlpExporter holds the sink in a Box; wrap the
                // batcher in a shim that defers to the shared Arc so
                // AppState can retain the Arc for graceful
                // shutdown-flush while the exporter owns its Box.
                struct SinkArc(std::sync::Arc<dyn cairn_runtime::telemetry::SpanExportSink>);
                #[async_trait::async_trait]
                impl cairn_runtime::telemetry::SpanExportSink for SinkArc {
                    async fn export(
                        &self,
                        spans: &[cairn_runtime::telemetry::ExportableSpan],
                    ) -> Result<(), String> {
                        self.0.export(spans).await
                    }
                }
                let sink_for_exporter: std::sync::Arc<
                    dyn cairn_runtime::telemetry::SpanExportSink,
                > = batched.clone();
                let exporter = Arc::new(OtlpExporter::new(
                    cfg.clone(),
                    Box::new(SinkArc(sink_for_exporter)),
                ));
                state.otlp_exporter = exporter.clone();
                state.otlp_tap = Some(crate::metrics_otel::OtlpTap::spawn(
                    state.runtime.store.clone(),
                    exporter,
                ));
                // Retain the batcher so shutdown_telemetry() can
                // flush buffered spans before process exit.
                state.otlp_batch = Some(batched);
                tracing::info!(
                    endpoint = %cfg.endpoint,
                    redact_content = cfg.redact_content,
                    "OTLP export enabled"
                );
            }
        }

        Ok(state)
    }

    /// Graceful shutdown of background telemetry tasks. Call from
    /// the process-exit path so any buffered OTLP spans are flushed
    /// to the collector before the process ends; without this call
    /// the BatchingSink's timer task is cancelled asynchronously
    /// and the final batch is dropped.
    ///
    /// Idempotent — safe to call multiple times; second call is a
    /// no-op because both shutdown hooks `take()` their handles.
    pub async fn shutdown_telemetry(&self) {
        #[cfg(feature = "metrics-otel")]
        {
            // Stop the tap first so no new spans enter the
            // batcher's buffer after we've started draining.
            if let Some(tap) = &self.otlp_tap {
                tap.shutdown().await;
            }
            if let Some(batch) = &self.otlp_batch {
                batch.shutdown().await;
            }
        }
        #[cfg(any(feature = "metrics-core", feature = "metrics-providers"))]
        {
            if let Some(tap) = &self.metrics_tap {
                tap.shutdown().await;
            }
        }
    }
}

// ── Helpers (local copies of private lib.rs fns used by new()) ───────────────

/// Resolve the sandbox base directory.
///
/// Production default: `$TMPDIR/cairn-workspace-sandboxes`.
///
/// Honors the `CAIRN_SANDBOX_BASE_DIR` env var override when set. This
/// exists primarily for per-harness test isolation: the default
/// `/tmp/cairn-workspace-sandboxes` is a process-global directory, and
/// parallel integration tests that seed entries into its
/// `recovery_registry/` subdirectory can race one another during
/// `SandboxService::recover_all`'s drift sweep. Test harnesses should
/// set this env var to a unique per-harness path before spawning the
/// cairn-app subprocess. Production deployments leave it unset.
fn default_sandbox_base_dir() -> PathBuf {
    if let Ok(override_path) = std::env::var("CAIRN_SANDBOX_BASE_DIR") {
        if !override_path.is_empty() {
            return PathBuf::from(override_path);
        }
    }
    std::env::temp_dir().join("cairn-workspace-sandboxes")
}

/// Resolve the plugin-state root under which each integration plugin
/// persists its own per-process state (closes #556).
///
/// Production default: `~/.cairn/plugins`. Override via
/// `CAIRN_PLUGIN_STATE_DIR` for dev / integration tests. When the
/// home directory cannot be detected (neither `HOME` on Unix nor
/// `USERPROFILE` on Windows is set), falls back to
/// `$TMPDIR/cairn-plugins`.
///
/// The home-detection order mirrors `cairn_runtime::FileConfigStore::
/// open_default` so operator-state directories (`config.toml`,
/// `models.toml`, plugin allowlists) all resolve to the same root.
///
/// Each plugin owns a subdirectory under this root (e.g.
/// `<root>/github/allowlist.json`) — the root itself is shared across
/// plugins, but filenames never collide because the subdir is plugin-
/// specific. See `cairn_integrations::github::GitHubPlugin::STATE_SUBDIR`.
pub fn default_plugin_state_dir() -> PathBuf {
    if let Ok(override_path) = std::env::var("CAIRN_PLUGIN_STATE_DIR") {
        if !override_path.is_empty() {
            return PathBuf::from(override_path);
        }
    }
    // Unix: HOME. Windows: USERPROFILE (the convention every other
    // cross-platform cairn path helper uses). Falling through to
    // `$TMPDIR` under a misconfigured Windows box would silently
    // persist the allowlist to a temp directory that may be wiped on
    // reboot — a subtle restart-durability regression.
    for var in ["HOME", "USERPROFILE"] {
        if let Ok(home) = std::env::var(var) {
            if !home.is_empty() {
                return PathBuf::from(home).join(".cairn").join("plugins");
            }
        }
    }
    std::env::temp_dir().join("cairn-plugins")
}

/// F65 PR-5: resolve the snapshot root for durable workspace snapshots.
/// Production default: `~/.cairn/snapshots`. Override via
/// `CAIRN_SNAPSHOT_DIR` for dev / integration tests. Falls back to
/// temp_dir()/cairn-snapshots when HOME is unset.
pub(crate) fn default_snapshot_dir() -> PathBuf {
    if let Ok(override_path) = std::env::var("CAIRN_SNAPSHOT_DIR") {
        if !override_path.is_empty() {
            return PathBuf::from(override_path);
        }
    }
    if let Ok(home) = std::env::var("HOME") {
        if !home.is_empty() {
            return PathBuf::from(home).join(".cairn").join("snapshots");
        }
    }
    std::env::temp_dir().join("cairn-snapshots")
}

/// Build the runtime aggregate.
///
/// Constructs `FabricServices` from env config, wires the
/// `FabricRunServiceAdapter` / `Task` / `Session` trio on top of a shared
/// `InMemoryStore`, and installs them via
/// `RuntimeServices::with_store_and_core`. Boot failure on the Fabric
/// path (unreachable Valkey, HMAC validation, …) surfaces here before
/// cairn-app serves traffic — no silent fall-back.
///
/// Integration tests that need to stand up an AppState without a live
/// Valkey build a `FakeFabric`-backed `RuntimeServices` (see
/// `crates/cairn-app/tests/support/fake_fabric.rs`) and call
/// [`AppBootstrap::router_with_injected_runtime`] directly, bypassing
/// this constructor.
async fn build_runtime_with_optional_fabric(
    master_key: Arc<cairn_runtime::MasterKey>,
) -> Result<
    (
        Arc<RuntimeServices>,
        Option<Arc<cairn_fabric::FabricServices>>,
    ),
    String,
> {
    tracing::info!("constructing FabricServices (production runtime)");

    let fabric_config = cairn_fabric::FabricConfig::from_env()
        .map_err(|e| format!("FabricConfig::from_env failed: {e}"))?;

    // FabricServices::start needs a shared EventLog handle. We use the same
    // InMemoryStore that backs the runtime's projections so fabric's
    // EventBridge writes land on the same read model that cairn-app
    // handlers query.
    let store = Arc::new(cairn_store::InMemoryStore::new());
    let event_log: Arc<dyn cairn_store::event_log::EventLog + Send + Sync> = store.clone();

    // Wire the in-memory store under the cursor-store trait so the
    // lease-history subscriber can persist its XREAD cursors across
    // restarts against the same backing the rest of the projections use.
    let cursor_store: Arc<dyn cairn_store::projections::FfLeaseHistoryCursorStore> = store.clone();
    let fabric = cairn_fabric::FabricServices::start_with_lease_history(
        fabric_config,
        event_log,
        cursor_store,
    )
    .await
    .map_err(|e| format!("FabricServices::start failed: {e}"))?;
    let fabric = Arc::new(fabric);

    // One-shot cross-instance tag backfill. Gated on
    // `CAIRN_BACKFILL_INSTANCE_TAG=1` so a fresh deploy pays no cost.
    // Only applies when an operator does an in-place binary swap with
    // pre-existing `Running`/`WaitingApproval` executions that predate
    // the `cairn.instance_id` tag filter. Idempotent — running twice
    // is a no-op on the second pass because the HSET only fires on
    // hashes that lack the tag. Logs once at completion.
    //
    // PR-C4c: the backfill utility walks `ff:exec:*:tags` via
    // `ferriskey::Client::scan` + `HSET` — Valkey-only. Skip
    // automatically on the PG boot path (where
    // `fabric.valkey_runtime` is `None`). An operator who sets
    // the env var on a PG deploy gets a one-line warning instead
    // of a boot-halting panic.
    if std::env::var("CAIRN_BACKFILL_INSTANCE_TAG").as_deref() == Ok("1") {
        if let Some(valkey_runtime) = fabric.valkey_runtime.as_ref() {
            match cairn_fabric::instance_tag_backfill::backfill_instance_tag(
                &valkey_runtime.client,
                valkey_runtime.config.worker_instance_id.as_str(),
            )
            .await
            {
                Ok(outcome) => {
                    tracing::info!(
                        scanned = outcome.scanned,
                        tagged = outcome.tagged,
                        skipped_tagged = outcome.skipped_tagged,
                        skipped_foreign = outcome.skipped_foreign,
                        "backfilled cairn.instance_id on {} executions",
                        outcome.tagged,
                    );
                    eprintln!(
                        "backfilled cairn.instance_id on {} executions (scanned={} skipped_tagged={} skipped_foreign={})",
                        outcome.tagged,
                        outcome.scanned,
                        outcome.skipped_tagged,
                        outcome.skipped_foreign,
                    );
                }
                Err(e) => {
                    // Do not halt startup: the backfill is advisory. A
                    // failed pass leaves existing foreign behavior intact
                    // (lease expiries on untagged execs are dropped). An
                    // operator can re-run by restarting with the env var
                    // still set.
                    tracing::error!(error = %e, "cairn.instance_id backfill failed");
                    eprintln!("warning: cairn.instance_id backfill failed: {e}");
                }
            }
        } else {
            tracing::warn!(
                "CAIRN_BACKFILL_INSTANCE_TAG=1 set but the fabric is running on a \
                 non-Valkey backend (Postgres) — the backfill walks ff:exec:*:tags \
                 on Valkey only. Ignoring the env var."
            );
        }
    }

    // Build the adapters that implement the cairn-runtime traits but
    // route mutations to Fabric. Each shares the same store for projection
    // reads (the resolvers look up project from bare ids).
    let runs: Arc<dyn cairn_runtime::runs::RunService> = Arc::new(
        crate::fabric_adapter::FabricRunServiceAdapter::new(fabric.clone(), store.clone()),
    );
    let tasks: Arc<dyn cairn_runtime::tasks::TaskService> = Arc::new(
        crate::fabric_adapter::FabricTaskServiceAdapter::new(fabric.clone(), store.clone()),
    );
    let sessions: Arc<dyn cairn_runtime::sessions::SessionService> = Arc::new(
        crate::fabric_adapter::FabricSessionServiceAdapter::new(fabric.clone(), store.clone()),
    );

    let mut services =
        RuntimeServices::with_store_core_and_key(store, runs, tasks, sessions, master_key);
    // Also expose the raw fabric via the type-erased slot on
    // RuntimeServices so non-trait surfaces (budgets, quotas, signals)
    // remain reachable from runtime-scoped code. Cast the Arc to Any here
    // because cairn-runtime does not name cairn-fabric types.
    services.fabric = Some(fabric.clone() as Arc<dyn std::any::Any + Send + Sync>);

    tracing::info!("fabric runtime installed; adapters active on runs/tasks/sessions");

    Ok((Arc::new(services), Some(fabric)))
}

/// Resolve the credential master key at boot.
///
/// Priority (same shape as `CAIRN_ADMIN_TOKEN` resolution in `main.rs`):
///   1. `CAIRN_CREDENTIAL_KEY_FILE` — path to a file containing the key.
///   2. `CAIRN_CREDENTIAL_KEY` — the key directly (hex or base64).
///
/// Semantics by deployment mode:
///   - **SelfHostedTeam**: unset OR malformed → hard error with `Err(_)`. The
///     caller fails startup rather than booting with a silently-default key.
///   - **Local**: unset → log a loud warning and fall back to a
///     hard-coded dev-only key. This is intentionally insecure and exists
///     only to keep local development booting without extra setup. A real
///     deployment must set the env var.
///
/// The fallback path exists because local-mode operators use `--db memory`,
/// which discards credentials on restart anyway — there is no persistence
/// contract to break. The fallback key is repository-visible and must not
/// be treated as secret; operators who need confidentiality must set
/// `CAIRN_CREDENTIAL_KEY` or `CAIRN_CREDENTIAL_KEY_FILE`. The previous
/// pre-fix default (`"cairn-local-test-key"`) is deliberately NOT reused
/// so old ciphertexts encrypted under it cannot be decrypted by the new
/// binary (that would silently re-validate the regression the cluster
/// closed).
fn load_master_key(config: &BootstrapConfig) -> Result<Arc<cairn_runtime::MasterKey>, String> {
    use cairn_api::bootstrap::DeploymentMode;

    match cairn_runtime::MasterKey::from_env() {
        Ok(Some(key)) => {
            tracing::info!(
                fingerprint = %key.fingerprint(),
                "credential master key loaded from environment"
            );
            eprintln!(
                "credentials: master key loaded (fingerprint={})",
                key.fingerprint()
            );
            Ok(Arc::new(key))
        }
        Ok(None) => {
            if config.mode == DeploymentMode::SelfHostedTeam {
                Err(
                    "FATAL: CAIRN_CREDENTIAL_KEY (or CAIRN_CREDENTIAL_KEY_FILE) is \
                     required in self-hosted team mode. Generate a 32-byte key with \
                     `openssl rand -hex 32` and set it in the process environment \
                     before starting cairn-app. Credentials encrypted with the \
                     pre-fix default key must be rotated."
                        .to_owned(),
                )
            } else {
                eprintln!(
                    "warning: credentials: CAIRN_CREDENTIAL_KEY is not set — \
                     using a dev-only deterministic key. This is ACCEPTED only \
                     for local --db memory runs and the credentials encrypted \
                     here CANNOT be carried over to a production deployment. \
                     Set CAIRN_CREDENTIAL_KEY to a 32-byte hex or base64 value \
                     to encrypt credentials with an operator-chosen key."
                );
                // Derive a deterministic 32-byte dev key from a short literal
                // so the key is reproducible for local dev and we don't
                // accidentally ship "cairn-local-test-key" as the real key
                // again. The key is still hardcoded — a local operator who
                // cares about secrecy sets CAIRN_CREDENTIAL_KEY.
                let dev_bytes = *b"cairn-dev-local-INSECURE-32byte!";
                Ok(Arc::new(cairn_runtime::MasterKey::from_bytes(dev_bytes)))
            }
        }
        Err(e) => Err(format!("FATAL: credential master key invalid: {e}")),
    }
}
