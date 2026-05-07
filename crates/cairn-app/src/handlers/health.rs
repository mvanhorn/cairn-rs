//! Health, system status, settings, license, and onboarding handlers.
//!
//! Extracted from `lib.rs` — contains the health-check endpoints, readiness
//! probes, metrics, version, dashboard, settings CRUD, license management,
//! onboarding/template handlers, and the catch-all not-found / not-implemented
//! fallback handlers.

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::IntoResponse,
    Json,
};
use utoipa::ToSchema;
use x509_parser::parse_x509_certificate;

use cairn_api::bootstrap::BootstrapConfig;
use cairn_api::onboarding::{create_onboarding_checklist, materialize_template};
use cairn_api::settings_api::SettingsSummary;
use cairn_api::{CriticalEventSummary, DashboardOverview};
use cairn_domain::{
    ProjectId, ProjectKey, RunId, RunState, RuntimeEvent, SessionId, TenantId, WorkspaceId,
};
use cairn_runtime::{DefaultsService, LicenseService, ProviderBindingService};
use cairn_store::{EventLog, StoredEvent};
use cairn_tools::{PluginHost, PluginRegistry};

use crate::errors::{
    deployment_mode_label, now_ms, runtime_error_response, storage_backend_label,
    store_error_response, validation_error_response, AppApiError,
};
use crate::helpers::{parse_project_scope, parse_scope_name};
use crate::middleware::refresh_activity_metrics;
use crate::state::AppState;

const DEFAULT_TENANT_ID: &str = "default_tenant";
const DEFAULT_WORKSPACE_ID: &str = "default_workspace";
const DEFAULT_PROJECT_ID: &str = "default_project";

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct DashboardActivityItem {
    pub event_type: String,
    pub message: String,
    pub occurred_at_ms: u64,
    pub run_id: Option<String>,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub(crate) struct OnboardingStatusQuery {
    pub project_id: Option<String>,
    pub template_id: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct MaterializeTemplateRequest {
    pub template_id: String,
    pub tenant_id: Option<String>,
    pub workspace_id: Option<String>,
    pub project_id: Option<String>,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub(crate) struct TenantQuery {
    pub tenant_id: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct SetDefaultSettingRequest {
    pub value: serde_json::Value,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct ResolveDefaultQuery {
    pub project: String,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct TlsSettingsResponse {
    pub enabled: bool,
    pub cert_subject: Option<String>,
    pub expires_at: Option<String>,
}

// ── Health DTOs ──────────────────────────────────────────────────────────────

#[derive(Clone, Debug, serde::Serialize, ToSchema)]
pub(crate) struct HealthCheck {
    name: String,
    status: String,
    latency_ms: u64,
}

#[derive(Clone, Debug, serde::Serialize, ToSchema)]
pub(crate) struct HealthReport {
    status: String,
    version: String,
    uptime_secs: u64,
    store_ok: bool,
    plugin_registry_count: u32,
    checks: Vec<HealthCheck>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct VersionReport {
    version: String,
    git_sha: String,
    build_date: String,
}

/// RFC 010: per-component status entry.
#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct ComponentStatus {
    name: String,
    status: String, // "ok" | "degraded" | "down"
    message: Option<String>,
}

/// RFC 010: system-level status view returned by GET /v1/status.
#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct SystemStatus {
    status: String, // "ok" | "degraded" | "incident"
    version: String,
    uptime_secs: u64,
    components: Vec<ComponentStatus>,
}

// ── Agent templates ──────────────────────────────────────────────────────────

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct AgentTemplate {
    id: String,
    name: String,
    description: String,
    icon: String,
    default_prompt: String,
    default_tools: Vec<String>,
    approval_policy: String,
    agent_role: String,
}

#[derive(serde::Deserialize)]
pub(crate) struct InstantiateTemplateRequest {
    goal: String,
    #[serde(default)]
    tenant_id: Option<String>,
    #[serde(default)]
    workspace_id: Option<String>,
    #[serde(default)]
    project_id: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct LicenseOverrideRequest {
    tenant_id: Option<String>,
    feature: String,
    allowed: bool,
    reason: Option<String>,
}

// ── Health / readiness ───────────────────────────────────────────────────────

pub(crate) async fn build_health_report(state: &AppState) -> HealthReport {
    let store_start = Instant::now();
    let store_ok = state.runtime.store.head_position().await.is_ok();
    let store_latency_ms = store_start.elapsed().as_millis() as u64;

    let plugin_start = Instant::now();
    let plugin_registry = state.plugin_registry.list_all();
    let plugin_registry_count = plugin_registry.len() as u32;
    let plugin_latency_ms = plugin_start.elapsed().as_millis() as u64;

    let checks = vec![
        HealthCheck {
            name: "store_connectivity".to_owned(),
            status: if store_ok {
                "healthy".to_owned()
            } else {
                "unhealthy".to_owned()
            },
            latency_ms: store_latency_ms,
        },
        HealthCheck {
            name: "plugin_registry".to_owned(),
            status: "healthy".to_owned(),
            latency_ms: plugin_latency_ms,
        },
    ];

    let status = if !store_ok {
        "unhealthy"
    } else if checks.iter().any(|check| check.status != "healthy") {
        "degraded"
    } else {
        "healthy"
    };

    HealthReport {
        status: status.to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        uptime_secs: state.started_at.elapsed().as_secs(),
        store_ok,
        plugin_registry_count,
        checks,
    }
}

#[utoipa::path(
    get,
    path = "/health",
    tag = "health",
    responses(
        (status = 200, description = "Service is healthy or degraded", body = HealthReport),
        (status = 503, description = "Service is unavailable", body = HealthReport)
    )
)]
pub(crate) async fn health_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let report = build_health_report(state.as_ref()).await;
    let status = match report.status.as_str() {
        "healthy" | "degraded" => StatusCode::OK,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    (status, Json(report))
}

/// GET /health/ready — readiness probe (RFC 020 §"Startup order").
///
/// Returns 503 with startup progress JSON during recovery.
/// Returns 200 with the same progress JSON shape (all branches `complete`)
/// once the startup graph completes.
///
/// Unlike `/health` (liveness, always 200 once HTTP is bound), this endpoint
/// gates readiness on the `ReadinessState` in `AppState`, which the startup
/// sequence in `main.rs` flips branch-by-branch until all are `Complete`.
pub(crate) async fn health_ready_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let progress = state.readiness.progress();
    let status = if state.readiness.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(progress)).into_response()
}

pub(crate) async fn system_status_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut components = Vec::new();

    // event_store: ping head_position
    let store_ok = state.runtime.store.head_position().await.is_ok();
    components.push(ComponentStatus {
        name: "event_store".to_owned(),
        status: if store_ok { "ok" } else { "down" }.to_owned(),
        message: if store_ok {
            None
        } else {
            Some("event store unreachable".to_owned())
        },
    });

    // plugin_registry: degraded if any plugin is in a degraded lifecycle state
    let plugins = state.plugin_registry.list_all();
    let any_plugin_degraded = state
        .plugin_host
        .lock()
        .map(|h| {
            plugins
                .iter()
                .any(|m| matches!(h.state(&m.id), Some(cairn_tools::PluginState::Failed)))
        })
        .unwrap_or(false);
    components.push(ComponentStatus {
        name: "plugin_registry".to_owned(),
        status: if any_plugin_degraded {
            "degraded"
        } else {
            "ok"
        }
        .to_owned(),
        message: if any_plugin_degraded {
            Some(format!("{} plugin(s) degraded", plugins.len()))
        } else {
            None
        },
    });

    // provider_routing: degraded if any provider health record shows degraded status.
    let any_provider_degraded = state.runtime.store.any_provider_degraded().await;
    components.push(ComponentStatus {
        name: "provider_routing".to_owned(),
        status: if any_provider_degraded {
            "degraded"
        } else {
            "ok"
        }
        .to_owned(),
        message: if any_provider_degraded {
            Some("one or more providers degraded".to_owned())
        } else {
            None
        },
    });

    // memory_index: ok regardless; degraded if doc store has no documents at all
    let doc_count = state.retrieval.all_current_chunks().len();
    components.push(ComponentStatus {
        name: "memory_index".to_owned(),
        status: "ok".to_owned(),
        message: Some(format!("{doc_count} indexed chunks")),
    });

    // auth: always ok for InMemory
    components.push(ComponentStatus {
        name: "auth".to_owned(),
        status: "ok".to_owned(),
        message: None,
    });

    let overall = if components.iter().any(|c| c.status == "down") {
        "incident"
    } else if components.iter().any(|c| c.status == "degraded") {
        "degraded"
    } else {
        "ok"
    };

    (
        StatusCode::OK,
        Json(SystemStatus {
            status: overall.to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            uptime_secs: state.started_at.elapsed().as_secs(),
            components,
        }),
    )
        .into_response()
}

pub(crate) async fn ready_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if !state.metrics.is_started() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "ready": false,
                "reason": "startup_incomplete"
            })),
        )
            .into_response();
    }

    match state.runtime.store.probe_write().await {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "ready": true
            })),
        )
            .into_response(),
        Err(err) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "ready": false,
                "reason": err.to_string()
            })),
        )
            .into_response(),
    }
}

pub(crate) async fn metrics_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    refresh_activity_metrics(state.as_ref()).await;
    #[cfg(feature = "metrics-core")]
    crate::middleware::refresh_scrape_metrics(state.as_ref()).await;

    // Base text: cairn's own metrics.
    let mut body = state.metrics.render_prometheus();

    // Append FF's metrics when FabricServices is wired. FF exposes its
    // own `prometheus::Registry` via `ff_observability::Metrics::render`,
    // which returns Prometheus text-exposition format — identical wire
    // shape to cairn's own renderer, so concatenation is safe. Metric
    // names are `ff_*`-prefixed (see ff-observability 0.3.2 `real.rs`
    // `mod name`) so there is no collision with cairn's namespace. When
    // `state.fabric` is `None` (e.g. in-memory dev mode, unit tests),
    // there is no FF runtime and nothing to render.
    //
    // PR-C4c: `ff_metrics` only exists on the Valkey runtime (the
    // `ff_observability::Metrics` registry is constructed inside
    // `FabricRuntime::start` and shared with FF's ff-engine). The
    // Postgres runtime has no equivalent registry today — skip the
    // render on the PG boot path.
    if let Some(fabric) = state.fabric.as_ref() {
        if let Some(valkey_runtime) = fabric.valkey_runtime.as_ref() {
            let ff_text = valkey_runtime.ff_metrics.render();
            if !ff_text.is_empty() {
                if !body.ends_with('\n') {
                    body.push('\n');
                }
                body.push_str(&ff_text);
            }
        }
    }

    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}

pub(crate) async fn version_handler() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(VersionReport {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            git_sha: option_env!("GIT_SHA").unwrap_or("unknown").to_owned(),
            build_date: option_env!("BUILD_DATE").unwrap_or("unknown").to_owned(),
        }),
    )
}

// ── Dashboard ────────────────────────────────────────────────────────────────

pub(crate) async fn dashboard_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: crate::extractors::TenantScope,
) -> impl IntoResponse {
    let tenant_id = tenant_scope.tenant_id();
    let active_runs = state
        .runtime
        .store
        .count_active_runs_for_tenant(tenant_id)
        .await as u32;
    let active_tasks = state
        .runtime
        .store
        .count_active_tasks_for_tenant(tenant_id)
        .await as u32;
    let pending_approvals = state
        .runtime
        .store
        .count_pending_approvals_for_tenant(tenant_id)
        .await as u32;
    let memory_doc_count = state.diagnostics.total_documents_for_tenant(tenant_id);
    let active_plugins = state.plugin_registry.list_all().len() as u32;
    let active_providers = match state
        .runtime
        .provider_bindings
        .list(tenant_id, usize::MAX, 0)
        .await
    {
        Ok(bindings) => bindings
            .into_iter()
            .filter(|binding| binding.active)
            .count() as u32,
        Err(err) => return runtime_error_response(err),
    };

    let now = now_ms();
    let day_start_ms = now - (now % 86_400_000);
    let eval_runs_today = state
        .runtime
        .store
        .count_eval_runs_since_for_tenant(tenant_id, day_start_ms)
        .await as u32;

    let tenant_events = match tenant_events(state.as_ref(), tenant_id, 10_000).await {
        Ok(events) => events,
        Err(err) => return store_error_response(err),
    };

    let failed_runs_24h = tenant_events
        .iter()
        .filter(|event| {
            event.stored_at >= now.saturating_sub(24 * 60 * 60 * 1000)
                && matches!(
                    &event.envelope.payload,
                    RuntimeEvent::RunStateChanged(change) if change.transition.to == RunState::Failed
                )
        })
        .count() as u32;

    let recent_critical_events: Vec<CriticalEventSummary> = tenant_events
        .iter()
        .rev()
        .filter_map(critical_event_summary)
        .filter(|summary| summary.occurred_at_ms >= now.saturating_sub(60 * 60 * 1000))
        .take(20)
        .collect();

    let mut degraded_components = Vec::new();
    if state.runtime.store.head_position().await.is_err() {
        degraded_components.push("store".to_owned());
    }
    if !recent_critical_events.is_empty() {
        degraded_components.push("runtime".to_owned());
    }

    (
        StatusCode::OK,
        Json(DashboardOverview {
            active_runs,
            active_tasks,
            pending_approvals,
            failed_runs_24h,
            degraded_components: degraded_components.clone(),
            recent_critical_events,
            active_providers,
            active_plugins,
            memory_doc_count: memory_doc_count.into(),
            eval_runs_today,
            system_healthy: degraded_components.is_empty(),
            error_rate_24h: state.metrics.error_rate(),
            latency_p50_ms: state.metrics.latency_percentile(50.0),
            latency_p95_ms: state.metrics.latency_percentile(95.0),
        }),
    )
        .into_response()
}

pub(crate) async fn dashboard_activity_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: crate::extractors::TenantScope,
) -> impl IntoResponse {
    match tenant_events(state.as_ref(), tenant_scope.tenant_id(), 10_000).await {
        Ok(events) => {
            let items: Vec<DashboardActivityItem> =
                events.iter().rev().take(20).map(activity_item).collect();
            (StatusCode::OK, Json(items)).into_response()
        }
        Err(err) => store_error_response(err),
    }
}

pub(crate) async fn tenant_events(
    state: &AppState,
    tenant_id: &TenantId,
    limit: usize,
) -> Result<Vec<StoredEvent>, cairn_store::StoreError> {
    Ok(state
        .runtime
        .store
        .read_stream(None, limit)
        .await?
        .into_iter()
        .filter(|event| event_belongs_to_tenant(event, tenant_id))
        .collect())
}

pub(crate) fn event_belongs_to_tenant(event: &StoredEvent, tenant_id: &TenantId) -> bool {
    use cairn_domain::OwnershipKey;
    match &event.envelope.ownership {
        OwnershipKey::Tenant(key) => key.tenant_id == *tenant_id,
        OwnershipKey::Workspace(key) => key.tenant_id == *tenant_id,
        OwnershipKey::Project(key) => key.tenant_id == *tenant_id,
        OwnershipKey::System => false,
    }
}

pub(crate) fn activity_item(event: &StoredEvent) -> DashboardActivityItem {
    DashboardActivityItem {
        event_type: crate::event_type_name(&event.envelope.payload).to_owned(),
        message: crate::event_message(&event.envelope.payload),
        occurred_at_ms: event.stored_at,
        run_id: crate::run_id_for_event(&event.envelope.payload),
    }
}

pub(crate) fn critical_event_summary(event: &StoredEvent) -> Option<CriticalEventSummary> {
    match &event.envelope.payload {
        RuntimeEvent::RunStateChanged(change) if change.transition.to == RunState::Failed => {
            Some(CriticalEventSummary {
                event_type: "run_failed".to_owned(),
                message: format!("Run {} failed", change.run_id),
                occurred_at_ms: event.stored_at,
                run_id: Some(change.run_id.to_string()),
            })
        }
        RuntimeEvent::RecoveryCompleted(recovery) => Some(CriticalEventSummary {
            event_type: "recovery_completed".to_owned(),
            message: recovery
                .run_id
                .as_ref()
                .map(|run_id| format!("Recovery completed for run {run_id}"))
                .or_else(|| {
                    recovery
                        .task_id
                        .as_ref()
                        .map(|task_id| format!("Recovery completed for task {task_id}"))
                })
                .unwrap_or_else(|| "Recovery completed".to_owned()),
            occurred_at_ms: event.stored_at,
            run_id: recovery.run_id.as_ref().map(ToString::to_string),
        }),
        _ => None,
    }
}

// ── Onboarding ───────────────────────────────────────────────────────────────

pub(crate) async fn get_onboarding_status_handler(
    Query(query): Query<OnboardingStatusQuery>,
) -> impl IntoResponse {
    let checklist = create_onboarding_checklist(
        &ProjectId::new(
            query
                .project_id
                .unwrap_or_else(|| DEFAULT_PROJECT_ID.to_owned()),
        ),
        query.template_id.as_deref(),
    );
    (StatusCode::OK, Json(checklist))
}

pub(crate) fn agent_template_catalog() -> Vec<AgentTemplate> {
    vec![
        AgentTemplate {
            id: "knowledge-assistant".to_owned(),
            name: "Knowledge Assistant".to_owned(),
            description: "Retrieval-aware agent that searches memory, stores new knowledge, \
                          and fetches web pages to answer questions with cited sources."
                .to_owned(),
            icon: "BookOpen".to_owned(),
            default_prompt: "You are a knowledgeable research assistant. When given a question:\n\
                             1. Search memory for relevant context before answering.\n\
                             2. If memory is insufficient, use web_fetch to find current information.\n\
                             3. Synthesise findings into a clear, evidence-based answer.\n\
                             4. Store key discoveries in memory for future reference.\n\
                             5. Cite your sources — include where each fact came from.\n\n\
                             Be thorough: check multiple sources before concluding. If information \
                             is uncertain or conflicting, say so."
                .to_owned(),
            default_tools: vec![
                "memory_search".to_owned(),
                "memory_store".to_owned(),
                "web_fetch".to_owned(),
            ],
            approval_policy: "none".to_owned(),
            agent_role: "researcher".to_owned(),
        },
        AgentTemplate {
            id: "code-reviewer".to_owned(),
            name: "Code Reviewer".to_owned(),
            description: "Reads files, searches for patterns, inspects git history, and \
                          scores code quality. Requires approval before posting comments."
                .to_owned(),
            icon: "Code2".to_owned(),
            default_prompt: "You are an expert code reviewer. When given code to review:\n\
                             1. Read all relevant files — the changed files and their context.\n\
                             2. Search for anti-patterns, security issues, and performance concerns.\n\
                             3. Inspect recent git changes to understand the intent.\n\
                             4. Produce a structured review with severity ratings:\n\
                                - **critical**: bugs, security vulnerabilities, data loss risks\n\
                                - **warning**: performance issues, maintainability concerns\n\
                                - **suggestion**: style improvements, better alternatives\n\
                             5. Be constructive — explain why something is a problem and \
                                suggest a concrete fix.\n\n\
                             Reference specific files and line numbers in every finding."
                .to_owned(),
            default_tools: vec![
                "file_read".to_owned(),
                "grep_search".to_owned(),
                "bash".to_owned(),
                "eval_score".to_owned(),
            ],
            approval_policy: "sensitive".to_owned(),
            agent_role: "reviewer".to_owned(),
        },
        AgentTemplate {
            id: "data-analyst".to_owned(),
            name: "Data Analyst".to_owned(),
            description: "Fetches data from HTTP APIs, extracts fields with JSONPath, \
                          performs calculations, and reads reference files."
                .to_owned(),
            icon: "BarChart3".to_owned(),
            default_prompt: "You are a data analyst. When given an analysis task:\n\
                             1. Fetch data from the provided endpoints using http_request.\n\
                             2. Extract relevant fields with json_extract.\n\
                             3. Perform calculations — averages, trends, comparisons.\n\
                             4. Read any reference files for context or baselines.\n\
                             5. Summarise findings clearly with concrete numbers.\n\n\
                             Always validate data before drawing conclusions. Note any \
                             anomalies, missing data, or limitations in your analysis."
                .to_owned(),
            default_tools: vec![
                "http_request".to_owned(),
                "json_extract".to_owned(),
                "calculate".to_owned(),
                "file_read".to_owned(),
            ],
            approval_policy: "none".to_owned(),
            agent_role: "executor".to_owned(),
        },
    ]
}

pub(crate) async fn list_agent_templates_handler() -> impl IntoResponse {
    (StatusCode::OK, Json(agent_template_catalog()))
}

pub(crate) async fn instantiate_agent_template_handler(
    State(state): State<Arc<AppState>>,
    Path(template_id): Path<String>,
    Json(body): Json<InstantiateTemplateRequest>,
) -> impl IntoResponse {
    let catalog = agent_template_catalog();
    let Some(template) = catalog.iter().find(|t| t.id == template_id) else {
        return AppApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("agent template '{template_id}' not found"),
        )
        .into_response();
    };

    if body.goal.trim().is_empty() {
        return AppApiError::new(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "goal must not be empty",
        )
        .into_response();
    }

    let t_id = TenantId::new(body.tenant_id.as_deref().unwrap_or(DEFAULT_TENANT_ID));
    let w_id = WorkspaceId::new(body.workspace_id.as_deref().unwrap_or(DEFAULT_WORKSPACE_ID));
    let p_id = ProjectId::new(body.project_id.as_deref().unwrap_or(DEFAULT_PROJECT_ID));
    let project = ProjectKey::new(t_id.as_str(), w_id.as_str(), p_id.as_str());

    let suffix = &now_ms().to_string()[8..]; // last 6 digits of epoch-ms for uniqueness
    let sess_id = SessionId::new(format!("sess_tmpl_{}_{}", template.id, suffix));
    let run_id = RunId::new(format!("run_tmpl_{}_{}", template.id, suffix));

    // Create session
    let session = match state
        .runtime
        .sessions
        .create(&project, sess_id.clone())
        .await
    {
        Ok(s) => s,
        Err(e) => {
            return AppApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "session_error",
                e.to_string(),
            )
            .into_response();
        }
    };

    // Create run (use plain start; agent_role stored via defaults below)
    let run = match state
        .runtime
        .runs
        .start(&project, &sess_id, run_id, None)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return AppApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "run_error",
                e.to_string(),
            )
            .into_response();
        }
    };

    // Store goal and template config as tenant-scoped defaults for this run
    let run_key = run.run_id.as_str().to_owned();
    let _ = state
        .runtime
        .defaults
        .set(
            cairn_domain::tenancy::Scope::Tenant,
            t_id.as_str().to_owned(),
            format!("run:{run_key}:goal"),
            serde_json::json!(body.goal.trim()),
        )
        .await;
    let _ = state
        .runtime
        .defaults
        .set(
            cairn_domain::tenancy::Scope::Tenant,
            t_id.as_str().to_owned(),
            format!("run:{run_key}:agent_role"),
            serde_json::json!(template.agent_role),
        )
        .await;

    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "template_id":    template.id,
            "template_name":  template.name,
            "session_id":     session.session_id.as_str(),
            "run_id":         run.run_id.as_str(),
            "goal":           body.goal.trim(),
            "default_tools":  template.default_tools,
            "agent_role":     template.agent_role,
            "approval_policy": template.approval_policy,
        })),
    )
        .into_response()
}

pub(crate) async fn list_onboarding_templates_handler(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    (StatusCode::OK, Json(state.templates.list().to_vec()))
}

pub(crate) async fn materialize_onboarding_template_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<MaterializeTemplateRequest>,
) -> impl IntoResponse {
    let Some(template) = state.templates.get(&body.template_id).cloned() else {
        return AppApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "starter template not found",
        )
        .into_response();
    };

    let tenant_id = TenantId::new(
        body.tenant_id
            .unwrap_or_else(|| DEFAULT_TENANT_ID.to_owned()),
    );
    let workspace_id = WorkspaceId::new(
        body.workspace_id
            .unwrap_or_else(|| DEFAULT_WORKSPACE_ID.to_owned()),
    );
    let project_id = ProjectId::new(
        body.project_id
            .unwrap_or_else(|| DEFAULT_PROJECT_ID.to_owned()),
    );

    let provenance =
        materialize_template(&template, &tenant_id, &workspace_id, &project_id, now_ms());
    (StatusCode::OK, Json(provenance)).into_response()
}

// ── Settings ─────────────────────────────────────────────────────────────────

pub(crate) async fn get_settings_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let settings = SettingsSummary {
        deployment_mode: deployment_mode_label(state.config.mode).to_owned(),
        store_backend: storage_backend_label(&state.config.storage).to_owned(),
        plugin_count: u32::from(
            state
                .config
                .has_role(cairn_api::bootstrap::ServerRole::PluginHost),
        ),
    };
    (StatusCode::OK, Json(settings))
}

pub(crate) fn tls_settings_summary(
    config: &BootstrapConfig,
) -> Result<TlsSettingsResponse, String> {
    if !config.tls_enabled {
        return Ok(TlsSettingsResponse {
            enabled: false,
            cert_subject: None,
            expires_at: None,
        });
    }

    let cert_path = config
        .tls_cert_path
        .as_deref()
        .ok_or_else(|| "TLS enabled without cert path".to_owned())?;
    let key_path = config
        .tls_key_path
        .as_deref()
        .ok_or_else(|| "TLS enabled without key path".to_owned())?;

    let cert_file = File::open(cert_path)
        .map_err(|err| format!("failed to open TLS cert {}: {}", cert_path, err))?;
    let mut cert_reader = BufReader::new(cert_file);
    let first_cert = rustls_pemfile::certs(&mut cert_reader)
        .next()
        .transpose()
        .map_err(|err| format!("failed to parse TLS cert {}: {}", cert_path, err))?
        .ok_or_else(|| format!("no TLS certificates found in {}", cert_path))?;
    let (_, cert) = parse_x509_certificate(first_cert.as_ref())
        .map_err(|err| format!("failed to inspect TLS cert {}: {}", cert_path, err))?;

    // Also verify the key path is readable so the status endpoint reflects active config.
    File::open(key_path).map_err(|err| format!("failed to open TLS key {}: {}", key_path, err))?;

    Ok(TlsSettingsResponse {
        enabled: true,
        cert_subject: Some(cert.subject().to_string()),
        expires_at: Some(cert.validity().not_after.to_string()),
    })
}

pub(crate) async fn get_tls_settings_handler(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    match tls_settings_summary(&state.config) {
        Ok(summary) => (StatusCode::OK, Json(summary)).into_response(),
        Err(err) => AppApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", err)
            .into_response(),
    }
}

/// Known model-id keys. Values for these keys are validated as
/// non-empty, length-capped strings. Crucially we do NOT reject based
/// on "is this model present in the catalog or on a provider connection
/// right now": operator setup scripts commonly do `PUT brain_model`
/// first (it's the "primary" setting) and then `POST
/// /v1/providers/connections` second, so PUT-time existence was a
/// foot-gun (#656). The authoritative "is this model routable"
/// check runs at orchestrate time in
/// `crates/cairn-app/src/handlers/runs/orchestrate.rs` and returns a
/// typed 503 `preferred_model_unavailable` with the full connection
/// inventory — that's the layer where the operator actually cares.
const MODEL_ID_KEYS: &[&str] = &[
    "brain_model",
    "generate_model",
    "stream_model",
    "embed_model",
];

/// Numeric keys: parsed and range-checked. Keys outside this list and
/// `MODEL_ID_KEYS` fall through to the generic string-length cap.
const NUMERIC_KEYS: &[(&str, f64, f64)] = &[
    ("max_tokens", 1.0, 1_000_000.0),
    ("timeout_ms", 1.0, 3_600_000.0),
    ("temperature", 0.0, 2.0),
    // F29 CD: operator-tunable stalled-run threshold, in milliseconds.
    // Floor of 1 ms is intentional — integration tests need to force
    // runs to appear stuck on short timelines. Ceiling of 24 h keeps
    // the range typed as finite u64.
    ("stuck_run_threshold_ms", 1.0, 86_400_000.0),
];

/// Numeric keys whose value is read as a `u64` at the request site and
/// therefore must be persisted as a whole number. Fractional floats are
/// rejected at PUT so the read site never silently drops a value.
const INTEGER_ONLY_KEYS: &[&str] = &["stuck_run_threshold_ms", "timeout_ms", "max_tokens"];

/// Per-key cap on JSON-string length. Model ids are short; free-form
/// prompt-like keys get a wider budget.
const MODEL_ID_MAX_LEN: usize = 256;
const PROMPT_LIKE_MAX_LEN: usize = 4096;

/// Typed rejection from `validate_setting_value`. The handler
/// converts this into an HTTP response AND logs at INFO so dogfood
/// and production operator scripts that only see access-log lines
/// (`status=422 latency=0ms`) can see *why* the PUT was rejected
/// without rerunning with `-v`. Motivated by #656 — the missing log
/// line misled triage into filing the case as a boot-readiness race.
struct SettingValidationError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl SettingValidationError {
    fn validation(message: String) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            code: "validation_error",
            message,
        }
    }
}

fn validate_setting_value(
    key: &str,
    value: &serde_json::Value,
) -> Result<(), SettingValidationError> {
    // Numeric keys: reject non-numbers, enforce range.
    if let Some((_, min, max)) = NUMERIC_KEYS.iter().find(|(k, _, _)| *k == key) {
        let n = value
            .as_f64()
            .ok_or_else(|| SettingValidationError::validation(format!("{key} must be a number")))?;
        if !n.is_finite() || n < *min || n > *max {
            return Err(SettingValidationError::validation(format!(
                "{key} must be within [{min}, {max}]"
            )));
        }
        // Integer-only duration keys — `as_u64()` at the read site would
        // silently drop a fractional value, which then falls back to the
        // hard-coded default and misleads the operator. Reject at PUT.
        if INTEGER_ONLY_KEYS.contains(&key) && n.fract() != 0.0 {
            return Err(SettingValidationError::validation(format!(
                "{key} must be a whole number"
            )));
        }
        return Ok(());
    }

    // Model-id keys: must be a non-empty, length-capped string. We
    // deliberately DO NOT verify the model exists in any catalog or on
    // any provider connection at PUT time. Setup flows naturally order
    // `PUT brain_model` (the primary setting) before `POST
    // /v1/providers/connections` — rejecting forward references made
    // the onboarding script fail its first call and misled operators
    // (#656). The authoritative "is this model routable right now"
    // check lives in `handlers/runs/orchestrate.rs` and returns a typed
    // 503 `preferred_model_unavailable` with the full connection
    // inventory when a configured default has no backing connection at
    // orchestrate time. That's the right layer for the check: it only
    // fires when a run actually tries to route, and it gives the
    // operator the full inventory + a one-liner fix in the same body.
    if MODEL_ID_KEYS.contains(&key) {
        let model_id = value
            .as_str()
            .ok_or_else(|| SettingValidationError::validation(format!("{key} must be a string")))?;
        if model_id.is_empty() {
            return Err(SettingValidationError::validation(format!(
                "{key} must not be empty"
            )));
        }
        if model_id.len() > MODEL_ID_MAX_LEN {
            return Err(SettingValidationError::validation(format!(
                "{key} exceeds max length {MODEL_ID_MAX_LEN}"
            )));
        }
        return Ok(());
    }

    // Generic string-length cap for everything else.
    if let Some(s) = value.as_str() {
        if s.len() > PROMPT_LIKE_MAX_LEN {
            return Err(SettingValidationError::validation(format!(
                "{key} exceeds max length {PROMPT_LIKE_MAX_LEN}"
            )));
        }
    }
    Ok(())
}

/// Tenant-scope gate for the `/v1/settings/defaults/:scope/:scope_id/:key`
/// CRUD surface (GET/PUT/DELETE) and resolve.
///
/// Returns `Some(error_response)` when the caller lacks access for the
/// requested scope; `None` when the call should proceed.
///
/// Policy (mirrors codex's read-side fix from PR #733, applied uniformly
/// across the surface so the same access rule governs disclosure AND
/// tampering):
///
/// * `is_admin` — passes through unconditionally.
/// * `Scope::System` — non-admin → 403 `forbidden`. System defaults
///   are operator-controlled (model IDs, budget caps); not visible or
///   mutable per-tenant.
/// * `Scope::Tenant` — non-admin → only when `scope_id` matches the
///   caller's `tenant_id`. Mismatch returns 404 (not 403) so the
///   handler does not leak whether the tenant exists.
/// * `Scope::Workspace` / `Scope::Project` — non-admin → 403. The
///   compound `scope_id` (`tenant/workspace/project`) format does not
///   carry the tenant prefix on `Workspace` so a per-tenant check
///   would race against caller-supplied scope_id parsing; admin-only
///   is the safer posture and matches codex's read fix.
fn require_default_scope_access(
    tenant_scope: &crate::extractors::TenantScope,
    scope: cairn_domain::Scope,
    scope_id: &str,
) -> Option<axum::response::Response> {
    if tenant_scope.is_admin {
        return None;
    }
    match scope {
        cairn_domain::Scope::System => Some(
            AppApiError::new(
                StatusCode::FORBIDDEN,
                "forbidden",
                "system scope requires admin",
            )
            .into_response(),
        ),
        cairn_domain::Scope::Tenant => {
            if scope_id == tenant_scope.tenant_id().as_str() {
                None
            } else {
                Some(
                    AppApiError::new(StatusCode::NOT_FOUND, "not_found", "tenant not found")
                        .into_response(),
                )
            }
        }
        cairn_domain::Scope::Workspace | cairn_domain::Scope::Project => Some(
            AppApiError::new(
                StatusCode::FORBIDDEN,
                "forbidden",
                "workspace/project defaults require admin",
            )
            .into_response(),
        ),
    }
}

pub(crate) async fn set_default_setting_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: crate::extractors::TenantScope,
    Path((scope_name, scope_id, key)): Path<(String, String, String)>,
    Json(body): Json<SetDefaultSettingRequest>,
) -> impl IntoResponse {
    let Some(scope) = parse_scope_name(&scope_name) else {
        // Log before returning — the default access-log line
        // (`status=422 latency=0ms`) does not include the rejection
        // reason, and operator scripts that see only that line had no
        // way to diagnose setup failures (#656).
        tracing::info!(
            scope = %scope_name,
            scope_id = %scope_id,
            key = %key,
            code = "invalid_scope",
            "settings PUT rejected: unknown scope name",
        );
        return validation_error_response("invalid scope");
    };

    // Mirror the read-side gate from `get_default_setting_handler`:
    // non-admin callers can only write to their own tenant's
    // tenant-scoped defaults. System / workspace / project writes
    // require admin. Without this, any authenticated caller could
    // overwrite ANY tenant's persisted run goals / model defaults /
    // budget overrides — a strictly worse vulnerability than the
    // disclosure path codex's PR closed on the GET handler.
    if let Some(err) = require_default_scope_access(&tenant_scope, scope, &scope_id) {
        return err;
    }

    // Per-key validation (closes #228). Empty / oversized / non-numeric
    // values for numeric keys now 422 instead of silently persisting.
    // Model-id values are accepted as forward references and verified
    // at orchestrate time (#656).
    if let Err(err) = validate_setting_value(&key, &body.value) {
        tracing::info!(
            scope = %scope_name,
            scope_id = %scope_id,
            key = %key,
            code = %err.code,
            status = err.status.as_u16(),
            reason = %err.message,
            "settings PUT rejected: validation failed",
        );
        return AppApiError::new(err.status, err.code, err.message).into_response();
    }

    match state
        .runtime
        .defaults
        .set(scope, scope_id.clone(), key.clone(), body.value)
        .await
    {
        Ok(setting) => (StatusCode::OK, Json(setting)).into_response(),
        Err(err) => {
            // 5xx — a real store / runtime failure, not an operator
            // input mistake. Log at WARN so production log pipelines
            // that filter out INFO still surface it.
            tracing::warn!(
                scope = %scope_name,
                scope_id = %scope_id,
                key = %key,
                code = "runtime_error",
                reason = %err,
                "settings PUT failed: runtime error persisting default",
            );
            runtime_error_response(err)
        }
    }
}

/// `GET /v1/settings/defaults/:scope/:scope_id/:key` — fetch a single stored
/// default setting by exact scope coordinates.
///
/// Returns `{scope, scope_id, key, value, source}` on hit, or 404 when the
/// triple has not been explicitly persisted. `source` always equals the
/// requested `scope` (this endpoint is exact-lookup — for fallback
/// resolution use `GET /v1/settings/defaults/resolve/:key?project=…`).
pub(crate) async fn get_default_setting_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: crate::extractors::TenantScope,
    Path((scope, scope_id, key)): Path<(String, String, String)>,
) -> impl IntoResponse {
    use cairn_store::projections::DefaultsReadModel;

    let Some(scope_enum) = parse_scope_name(&scope) else {
        return validation_error_response("invalid scope");
    };

    if let Some(err) = require_default_scope_access(&tenant_scope, scope_enum, &scope_id) {
        return err;
    }

    match DefaultsReadModel::get(state.runtime.store.as_ref(), scope_enum, &scope_id, &key).await {
        Ok(Some(setting)) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "scope": scope,
                "scope_id": scope_id,
                "key": key,
                "value": setting.value,
                "source": scope,
            })),
        )
            .into_response(),
        Ok(None) => AppApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("default setting '{scope}/{scope_id}/{key}' not set"),
        )
        .into_response(),
        Err(err) => store_error_response(err),
    }
}

pub(crate) async fn clear_default_setting_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: crate::extractors::TenantScope,
    Path((scope, scope_id, key)): Path<(String, String, String)>,
) -> impl IntoResponse {
    let Some(scope_enum) = parse_scope_name(&scope) else {
        return validation_error_response("invalid scope");
    };

    // Same access rule as PUT/GET: non-admin can only act on its own
    // tenant's tenant-scoped defaults. Without the gate, any caller
    // could clear another tenant's persisted defaults — silent
    // tampering, not just disclosure.
    if let Some(err) = require_default_scope_access(&tenant_scope, scope_enum, &scope_id) {
        return err;
    }

    match state
        .runtime
        .defaults
        .clear(scope_enum, scope_id, key)
        .await
    {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

/// `GET /v1/settings/defaults/all` — flat list of stored default settings
/// scoped to the caller's authorization.
///
/// **Admin callers**: receive every setting across all four scopes
/// (System, Tenant, Workspace, Project). This is the "operator
/// dashboard" view — admins see everything they can manage.
///
/// **Non-admin (tenant) callers**: receive ONLY tenant-scoped
/// settings whose `scope_id` matches the caller's `tenant_id`.
/// System / workspace / project rows are excluded entirely (matches
/// the per-key GET handler's policy: those scopes are admin-only).
/// Other tenants' rows are excluded — exposing them was the
/// vulnerability codex's PR #733 closed on the per-key route; this
/// closes the equivalent disclosure on the bulk-list route.
///
/// Unset keys are omitted. For the effective value of a specific
/// key with fallback resolution, use
/// `GET /v1/settings/defaults/resolve/{key}?project=...`.
///
/// Response shape:
/// ```json
/// {
///   "settings": [
///     { "scope": "system", "scope_id": "system", "key": "generate_model", "value": "llama3.2:3b" },
///     { "scope": "tenant", "scope_id": "acme", "key": "max_tokens", "value": 8192 }
///   ],
///   "total": 2
/// }
/// ```
pub(crate) async fn list_all_defaults_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: crate::extractors::TenantScope,
) -> impl IntoResponse {
    use cairn_domain::Scope;
    use cairn_store::projections::DefaultsReadModel;

    let store = state.runtime.store.as_ref();
    let mut all_settings: Vec<serde_json::Value> = Vec::new();

    if tenant_scope.is_admin {
        // Admin: return every setting across all four scopes.
        if let Ok(sys_settings) =
            DefaultsReadModel::list_by_scope(store, Scope::System, "system").await
        {
            for s in sys_settings {
                all_settings.push(serde_json::json!({
                    "scope":    "system",
                    "scope_id": "system",
                    "key":      s.key,
                    "value":    s.value,
                }));
            }
        }

        if let Ok(tenants) = cairn_store::projections::TenantReadModel::list(store, 200, 0).await {
            for tenant in &tenants {
                let tid = tenant.tenant_id.as_str();
                if let Ok(settings) =
                    DefaultsReadModel::list_by_scope(store, Scope::Tenant, tid).await
                {
                    for s in settings {
                        all_settings.push(serde_json::json!({
                            "scope":    "tenant",
                            "scope_id": tid,
                            "key":      s.key,
                            "value":    s.value,
                        }));
                    }
                }
            }
        }

        // Default-workspace settings — full multi-workspace iteration
        // would need a `list_all` method on WorkspaceReadModel.
        if let Ok(settings) =
            DefaultsReadModel::list_by_scope(store, Scope::Workspace, "default").await
        {
            for s in settings {
                all_settings.push(serde_json::json!({
                    "scope":    "workspace",
                    "scope_id": "default",
                    "key":      s.key,
                    "value":    s.value,
                }));
            }
        }
    } else {
        // Non-admin: only the caller's own tenant rows. Mirrors the
        // per-key GET handler's `Scope::Tenant` + `scope_id == caller.tenant_id`
        // gate. System/workspace/project rows are admin-only there too.
        let tid = tenant_scope.tenant_id().as_str();
        if let Ok(settings) = DefaultsReadModel::list_by_scope(store, Scope::Tenant, tid).await {
            for s in settings {
                all_settings.push(serde_json::json!({
                    "scope":    "tenant",
                    "scope_id": tid,
                    "key":      s.key,
                    "value":    s.value,
                }));
            }
        }
    }

    let total = all_settings.len();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "settings": all_settings,
            "total": total,
        })),
    )
}

pub(crate) async fn resolve_default_setting_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: crate::extractors::TenantScope,
    Path(key): Path<String>,
    Query(query): Query<ResolveDefaultQuery>,
) -> impl IntoResponse {
    let Some((tenant_id, workspace_id, project_id)) = parse_project_scope(&query.project) else {
        return validation_error_response("project must use tenant/workspace/project");
    };
    if !tenant_scope.is_admin && tenant_scope.tenant_id().as_str() != tenant_id {
        return AppApiError::new(StatusCode::NOT_FOUND, "not_found", "project not found")
            .into_response();
    }

    let project = ProjectKey::new(tenant_id, workspace_id, project_id);

    match state.runtime.defaults.resolve(&project, &key).await {
        Ok(Some(value)) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "project": format!("{tenant_id}/{workspace_id}/{project_id}"),
                "key": key,
                "value": value,
            })),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "key": key,
                "value": null,
            })),
        )
            .into_response(),
        Err(err) => runtime_error_response(err),
    }
}

// ── License ──────────────────────────────────────────────────────────────────

pub(crate) async fn get_license_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<TenantQuery>,
) -> impl IntoResponse {
    let tenant_id = TenantId::new(
        query
            .tenant_id
            .unwrap_or_else(|| DEFAULT_TENANT_ID.to_owned()),
    );
    match state.runtime.licenses.get_active(&tenant_id).await {
        Ok(Some(license)) => (StatusCode::OK, Json(license)).into_response(),
        Ok(None) => AppApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "active license not found",
        )
        .into_response(),
        Err(err) => AppApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            err.to_string(),
        )
        .into_response(),
    }
}

pub(crate) async fn set_license_override_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<LicenseOverrideRequest>,
) -> impl IntoResponse {
    let tenant_id = TenantId::new(
        body.tenant_id
            .unwrap_or_else(|| DEFAULT_TENANT_ID.to_owned()),
    );
    match state
        .runtime
        .licenses
        .set_override(tenant_id, body.feature, body.allowed, body.reason)
        .await
    {
        Ok(record) => (StatusCode::OK, Json(record)).into_response(),
        Err(err) => {
            tracing::error!("set_license_override failed: {err}");
            AppApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                err.to_string(),
            )
            .into_response()
        }
    }
}

// ── Catch-all fallbacks ──────────────────────────────────────────────────────

pub(crate) async fn not_implemented_handler() -> impl IntoResponse {
    AppApiError::new(
        StatusCode::NOT_IMPLEMENTED,
        "not_implemented",
        "route preserved but not implemented yet",
    )
}

pub(crate) async fn not_found_handler() -> impl IntoResponse {
    AppApiError::new(StatusCode::NOT_FOUND, "not_found", "route not found")
}
