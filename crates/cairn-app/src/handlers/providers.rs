//! Provider health, budget, binding, pool, connection, route policy,
//! and guardrail handlers.
//!
//! Extracted from `lib.rs` — contains all provider-related CRUD and
//! operational endpoints.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use utoipa::ToSchema;

use cairn_api::http::{ApiError, ListResponse};
use cairn_domain::policy::{GuardrailRule, GuardrailSubjectType};
use cairn_domain::providers::{
    OperationKind, ProviderBudget, ProviderBudgetPeriod, ProviderConnectionRecord,
    ProviderHealthRecord, RoutePolicyRule,
};
use cairn_domain::{
    ProjectKey, ProviderBindingId, ProviderConnectionId, ProviderModelId, TenantId,
};
use cairn_runtime::{
    BudgetService, CredentialService, DefaultsService, GuardrailService, ProviderBindingService,
    ProviderConnectionService, ProviderHealthService, RoutePolicyService,
};
use cairn_store::projections::RoutePolicyReadModel;
use cairn_store::EventLog;

use crate::errors::{
    now_ms, require_feature, runtime_error_response, store_error_response, AppApiError,
};
use crate::extractors::TenantScope;
use crate::state::AppState;

const DEFAULT_TENANT_ID: &str = "default_tenant";

// ── DTOs ────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct TenantScopedQuery {
    pub tenant_id: String,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl TenantScopedQuery {
    pub(crate) fn limit(&self) -> usize {
        self.limit.unwrap_or(100)
    }

    pub(crate) fn offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub(crate) struct OptionalTenantScopedQuery {
    pub tenant_id: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl OptionalTenantScopedQuery {
    pub(crate) fn tenant_id(&self) -> &str {
        self.tenant_id.as_deref().unwrap_or(DEFAULT_TENANT_ID)
    }

    pub(crate) fn limit(&self) -> usize {
        self.limit.unwrap_or(100)
    }

    pub(crate) fn offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }
}

#[derive(Clone, Debug, serde::Deserialize, ToSchema)]
pub(crate) struct CreateProviderConnectionRequest {
    pub tenant_id: String,
    pub provider_connection_id: String,
    pub provider_family: String,
    pub adapter_type: String,
    #[serde(default)]
    pub supported_models: Vec<String>,
    #[serde(default)]
    pub credential_id: Option<String>,
    #[serde(default)]
    pub endpoint_url: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct ManualProviderHealthCheckRequest {
    pub latency_ms: Option<u64>,
    pub success: bool,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct SetProviderHealthScheduleRequest {
    pub interval_ms: u64,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct SetProviderBudgetRequest {
    pub tenant_id: String,
    pub period: ProviderBudgetPeriod,
    pub limit_micros: u64,
    pub alert_threshold_percent: u32,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct SetProviderRetryPolicyRequest {
    pub max_attempts: u32,
    pub backoff_ms: u64,
    pub retryable_error_classes: Vec<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct CreateProviderPoolRequest {
    pub pool_id: String,
    pub max_connections: u32,
    pub tenant_id: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct AddPoolConnectionRequest {
    pub connection_id: String,
}

#[derive(serde::Deserialize)]
pub(crate) struct UpdateProviderConnectionRequest {
    pub provider_family: String,
    pub adapter_type: String,
    pub supported_models: Vec<String>,
    pub endpoint_url: Option<String>,
    pub credential_id: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct CostRankingQuery {
    pub tenant_id: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl CostRankingQuery {
    pub(crate) fn limit(&self) -> usize {
        self.limit.unwrap_or(100)
    }

    pub(crate) fn offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }
}

#[derive(Clone, Debug, serde::Deserialize, ToSchema)]
pub(crate) struct CreateProviderBindingRequest {
    pub tenant_id: String,
    pub workspace_id: String,
    pub project_id: String,
    pub provider_connection_id: String,
    #[schema(value_type = String)]
    pub operation_kind: OperationKind,
    pub provider_model_id: String,
    pub estimated_cost_micros: Option<u64>,
}

impl CreateProviderBindingRequest {
    pub(crate) fn project(&self) -> ProjectKey {
        ProjectKey::new(
            self.tenant_id.as_str(),
            self.workspace_id.as_str(),
            self.project_id.as_str(),
        )
    }
}

/// Wire format for a single rule inside a `POST /v1/providers/route-policies`
/// create request. Four fields (`preferred_model_ids`, `fallback_model_ids`,
/// `max_cost_micros`, `require_provider_ids`) are accepted and deserialised
/// to keep the request schema forward-compatible with the richer policy
/// shape the routing engine will consume, but the current `From` impl
/// below only threads `rule_id`, `policy_id`, `priority`, `description`,
/// `capability` into `RoutePolicyRule`. The remaining fields are retained
/// on the wire (operators can send them today without getting a 400) and
/// will be wired in when RFC-013 lands those routing axes — see #487.
#[derive(Clone, Debug, serde::Deserialize)]
#[allow(dead_code)]
pub(crate) struct CreateRoutePolicyRuleRequest {
    #[serde(default)]
    pub rule_id: String,
    #[serde(default)]
    pub policy_id: String,
    #[serde(default)]
    pub priority: u32,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub capability: Option<String>,
    #[serde(default)]
    pub preferred_model_ids: Vec<String>,
    #[serde(default)]
    pub fallback_model_ids: Vec<String>,
    #[serde(default)]
    pub max_cost_micros: Option<u64>,
    #[serde(default)]
    pub require_provider_ids: Vec<String>,
}

impl From<CreateRoutePolicyRuleRequest> for RoutePolicyRule {
    fn from(r: CreateRoutePolicyRuleRequest) -> Self {
        Self {
            rule_id: if r.rule_id.is_empty() {
                r.capability.clone().unwrap_or_else(|| "rule".to_owned())
            } else {
                r.rule_id
            },
            policy_id: r.policy_id,
            priority: r.priority,
            description: r.description.or(r.capability),
        }
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct CreateRoutePolicyRequest {
    pub tenant_id: String,
    pub name: String,
    pub rules: Vec<CreateRoutePolicyRuleRequest>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct CreateGuardrailPolicyRequest {
    pub tenant_id: String,
    pub name: String,
    pub rules: Vec<GuardrailRule>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct EvaluateGuardrailPolicyRequest {
    pub tenant_id: String,
    pub subject_type: GuardrailSubjectType,
    pub subject_id: Option<String>,
    pub action: String,
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Returns true when the named adapter needs an operator-supplied API key
/// to successfully authenticate against its upstream endpoint.
///
/// The exceptions — adapters that authenticate without a cairn-stored
/// credential — are:
/// - `ollama`: unauthenticated; the adapter talks to a local daemon.
/// - `bedrock` / `bedrock-compat`: SigV4 using AWS environment or
///   instance-profile credentials (`AWS_ACCESS_KEY_ID`,
///   `AWS_SECRET_ACCESS_KEY`, `AWS_PROFILE`, IMDS, etc.).
///
/// Matching is case-insensitive and accepts the common `_` and `-`
/// spellings (`bedrock_compat`, `bedrock-compat`) so the handler behaves
/// identically regardless of which spelling the client sent.
///
/// Unknown / operator-supplied adapter strings are conservatively treated
/// as "requires credential" — a typo or a generic OpenAI-compatible
/// endpoint still needs a key in practice, and returning 422 with a
/// pointer to the credentials API is strictly better than silently
/// registering a connection that 401s on the first call.
fn adapter_requires_credential(adapter_type: &str) -> bool {
    let normalized = adapter_type.trim().to_lowercase().replace('_', "-");
    !matches!(
        normalized.as_str(),
        "ollama" | "bedrock" | "bedrock-compat" | "bedrock-converse" | "bedrock-openai",
    )
}

/// Load a provider connection and assert the caller's [`TenantScope`]
/// owns it (or the caller is admin).
///
/// Returns **404 not_found** when the id does not exist OR exists but
/// belongs to a different tenant. The response body is identical in
/// both cases — Gemini SEC-007 (PR #717): a 403
/// "wrong tenant" leaks the existence of another tenant's connection
/// id, letting an operator probe for valid IDs across the keyspace.
/// We collapse both into the same 404 so a non-admin caller cannot
/// distinguish "doesn't exist anywhere" from "exists but not yours".
///
/// Returns the [`ProviderConnectionRecord`] on the success path so
/// callers don't need to re-fetch (avoids the redundant `get` Gemini
/// flagged on the update handler).
///
/// # Failure-shape contract
/// The error path returns a fully-built `axum::response::Response` so
/// every handler renders the same envelope without each having to
/// import [`AppApiError`]. Returning `Result<_, Response>` lets
/// handlers `?`-propagate or `match`-translate at the call site.
pub(crate) async fn load_connection_owned_by_scope(
    state: &AppState,
    conn_id: &ProviderConnectionId,
    scope: &TenantScope,
) -> Result<ProviderConnectionRecord, axum::response::Response> {
    match state.runtime.provider_connections.get(conn_id).await {
        Ok(Some(record)) => {
            // Compare typed `TenantId` values directly — a previous
            // iteration of this check coerced to `&str` via `as_str()`
            // (Gemini review on #717), which both fails to type-check
            // (`TenantId: PartialEq<TenantId>` only) and bypasses
            // `TenantId`'s Eq contract if it ever grows beyond a
            // newtype around String.
            if !scope.is_admin && record.tenant_id != *scope.tenant_id() {
                Err(AppApiError::new(
                    StatusCode::NOT_FOUND,
                    "not_found",
                    "provider connection not found",
                )
                .into_response())
            } else {
                Ok(record)
            }
        }
        Ok(None) => Err(AppApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "provider connection not found",
        )
        .into_response()),
        Err(err) => Err(runtime_error_response(err)),
    }
}

// ── Handlers ────────────────────────────────────────────────────────────────

pub(crate) async fn list_provider_health_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<TenantScopedQuery>,
) -> impl IntoResponse {
    // #422: honest pagination — fetch `limit + 1`, derive `has_more`.
    let limit = query.limit();
    let offset = query.offset();
    match state
        .runtime
        .provider_health
        .list(&TenantId::new(query.tenant_id), limit + 1, offset)
        .await
    {
        Ok(mut items) => {
            let has_more = items.len() > limit;
            items.truncate(limit);
            (
                StatusCode::OK,
                Json(ListResponse::<ProviderHealthRecord> { items, has_more }),
            )
                .into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn list_provider_budgets_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<TenantScopedQuery>,
) -> impl IntoResponse {
    // #422: budgets per tenant are naturally bounded (one row per
    // period), but the service returns them all in one shot. Paginate
    // in-memory and compute `has_more` honestly.
    let offset = query.offset();
    let limit = query.limit();
    match state
        .runtime
        .budgets
        .list_budgets(&TenantId::new(query.tenant_id))
        .await
    {
        Ok(all) => {
            let total = all.len();
            let items: Vec<_> = all.into_iter().skip(offset).take(limit).collect();
            let has_more = offset.saturating_add(items.len()) < total;
            (
                StatusCode::OK,
                Json(ListResponse::<ProviderBudget> { items, has_more }),
            )
                .into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn set_provider_budget_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<SetProviderBudgetRequest>,
) -> impl IntoResponse {
    match state
        .runtime
        .budgets
        .set_budget(
            TenantId::new(body.tenant_id),
            body.period,
            body.limit_micros,
            body.alert_threshold_percent,
        )
        .await
    {
        Ok(record) => (StatusCode::CREATED, Json(record)).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn manual_provider_health_check_handler(
    State(state): State<Arc<AppState>>,
    _role: crate::extractors::AdminRoleGuard,
    Path(connection_id): Path<String>,
    Json(body): Json<ManualProviderHealthCheckRequest>,
) -> impl IntoResponse {
    // T6a-H12: provider health is tenant-shared state. A hostile tenant
    // could otherwise poison the shared provider's health record,
    // triggering fallback routing for every other tenant. Gate on
    // AdminRoleGuard until the service gains a principal-aware
    // `record_check_for_tenant` variant.
    match state
        .runtime
        .provider_health
        .record_check(
            &ProviderConnectionId::new(connection_id),
            body.latency_ms.unwrap_or(0),
            body.success,
        )
        .await
    {
        Ok(record) => (StatusCode::OK, Json(record)).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn recover_provider_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(connection_id): Path<String>,
) -> impl IntoResponse {
    // SEC #717: previously unauthenticated against the URL-path id —
    // any operator could mark another tenant's connection "recovered",
    // forcing health flaps and re-enabling provider bindings on a
    // foreign tenant's behalf. Cross-tenant hits collapse to 404 (not
    // 403) so an attacker can't enumerate connection ids.
    let conn_id = ProviderConnectionId::new(connection_id);
    if let Err(resp) = load_connection_owned_by_scope(&state, &conn_id, &tenant_scope).await {
        return resp;
    }
    match state.runtime.provider_health.mark_recovered(&conn_id).await {
        Ok(record) => (StatusCode::OK, Json(record)).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn set_provider_health_schedule_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(connection_id): Path<String>,
    Json(body): Json<SetProviderHealthScheduleRequest>,
) -> impl IntoResponse {
    // SEC #717: tenant-scope the schedule mutation. Previously
    // unauthenticated against the URL-path id — a foreign operator
    // could schedule arbitrary-cadence probes on another tenant's
    // connection, both noisy upstream and a vector for tampering with
    // the shared provider-health record.
    let conn_id = ProviderConnectionId::new(connection_id);
    if let Err(resp) = load_connection_owned_by_scope(&state, &conn_id, &tenant_scope).await {
        return resp;
    }
    match state
        .runtime
        .provider_health
        .schedule_health_check(&conn_id, body.interval_ms)
        .await
    {
        Ok(schedule) => (StatusCode::OK, Json(schedule)).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn get_provider_health_schedule_handler(
    State(state): State<Arc<AppState>>,
    Path(connection_id): Path<String>,
) -> impl IntoResponse {
    use cairn_store::projections::ProviderHealthScheduleReadModel;
    match ProviderHealthScheduleReadModel::get_schedule(
        state.runtime.store.as_ref(),
        &connection_id,
    )
    .await
    {
        Ok(Some(schedule)) => (StatusCode::OK, Json(schedule)).into_response(),
        Ok(None) => AppApiError::new(StatusCode::NOT_FOUND, "not_found", "schedule not found")
            .into_response(),
        Err(err) => AppApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "store_error",
            err.to_string(),
        )
        .into_response(),
    }
}

pub(crate) async fn set_provider_retry_policy_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(connection_id): Path<String>,
    Json(body): Json<SetProviderRetryPolicyRequest>,
) -> impl IntoResponse {
    use cairn_domain::{providers::RetryPolicy, ProviderRetryPolicySet, RuntimeEvent};
    // SEC #717: the existing handler took `TenantScope` but trusted it
    // blindly — it stamped the caller's tenant_id onto the event
    // envelope without verifying the conn_id belonged to that tenant.
    // Cross-tenant operators could pollute the event log with phantom
    // retry-policy events targeting another tenant's connection. Pin
    // the conn_id to the caller's tenant via the helper; cross-tenant
    // hits 404 so the conn_id space stays opaque.
    let conn_id = ProviderConnectionId::new(connection_id);
    if let Err(resp) = load_connection_owned_by_scope(&state, &conn_id, &tenant_scope).await {
        return resp;
    }
    let event = cairn_runtime::make_envelope(RuntimeEvent::ProviderRetryPolicySet(
        ProviderRetryPolicySet {
            connection_id: conn_id,
            tenant_id: tenant_scope.tenant_id().clone(),
            policy: RetryPolicy {
                max_attempts: body.max_attempts,
                backoff_ms: body.backoff_ms,
                retryable_error_classes: body.retryable_error_classes,
            },
            set_at_ms: now_ms(),
        },
    ));
    match state.runtime.store.append(&[event]).await {
        Ok(_) => (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response(),
        Err(err) => store_error_response(err),
    }
}

pub(crate) async fn run_provider_health_checks_handler(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    // NOTE: this is a batch *action* whose response happens to be
    // shaped like a list (every record that just ran). It is not a
    // paginated read — there is no further page to fetch — so
    // `has_more: false` here is literally true (no next page), not
    // the pagination-lie from #422.
    match state.runtime.provider_health.run_due_health_checks().await {
        Ok(records) => (
            StatusCode::OK,
            Json(ListResponse::<ProviderHealthRecord> {
                items: records,
                has_more: false,
            }),
        )
            .into_response(),
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn create_provider_pool_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateProviderPoolRequest>,
) -> impl IntoResponse {
    use cairn_runtime::ProviderConnectionPoolService;
    let tenant_id = TenantId::new(body.tenant_id.as_deref().unwrap_or(DEFAULT_TENANT_ID));
    match state
        .runtime
        .provider_pools
        .create_pool(tenant_id, body.pool_id, body.max_connections)
        .await
    {
        Ok(pool) => (StatusCode::CREATED, Json(pool)).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn list_provider_pools_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<TenantScopedQuery>,
) -> impl IntoResponse {
    use cairn_runtime::ProviderConnectionPoolService;
    // #422: pools per tenant are admin-curated and bounded, but we
    // still honour limit/offset honestly.
    let offset = query.offset();
    let limit = query.limit();
    match state
        .runtime
        .provider_pools
        .list_pools(&TenantId::new(query.tenant_id))
        .await
    {
        Ok(all) => {
            let total = all.len();
            let items: Vec<_> = all.into_iter().skip(offset).take(limit).collect();
            let has_more = offset.saturating_add(items.len()) < total;
            (StatusCode::OK, Json(ListResponse { items, has_more })).into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn add_pool_connection_handler(
    State(state): State<Arc<AppState>>,
    Path(pool_id): Path<String>,
    Json(body): Json<AddPoolConnectionRequest>,
) -> impl IntoResponse {
    use cairn_runtime::ProviderConnectionPoolService;
    match state
        .runtime
        .provider_pools
        .add_connection(&pool_id, ProviderConnectionId::new(body.connection_id))
        .await
    {
        Ok(pool) => (StatusCode::CREATED, Json(pool)).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn remove_pool_connection_handler(
    State(state): State<Arc<AppState>>,
    Path((pool_id, conn_id)): Path<(String, String)>,
) -> impl IntoResponse {
    use cairn_runtime::ProviderConnectionPoolService;
    match state
        .runtime
        .provider_pools
        .remove_connection(&pool_id, &ProviderConnectionId::new(conn_id))
        .await
    {
        Ok(pool) => (StatusCode::OK, Json(pool)).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn list_provider_connections_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<TenantScopedQuery>,
) -> impl IntoResponse {
    // #422: honest pagination — fetch `limit + 1`, derive `has_more`.
    let limit = query.limit();
    let offset = query.offset();
    match state
        .runtime
        .provider_connections
        .list(&TenantId::new(query.tenant_id), limit + 1, offset)
        .await
    {
        Ok(mut items) => {
            let has_more = items.len() > limit;
            items.truncate(limit);
            (StatusCode::OK, Json(ListResponse { items, has_more })).into_response()
        }
        Err(err) => AppApiError::new(StatusCode::BAD_REQUEST, "bad_request", err.to_string())
            .into_response(),
    }
}

/// `GET /v1/providers/registry` — admin-only cross-tenant snapshot of
/// every provider connection cached in this process.
///
/// **#428:** response includes `connection_id`, `backend`, and `model`
/// for every tenant's provider bindings — leaking those to a
/// per-tenant operator would disclose which providers another tenant
/// has configured. Gated with `AdminRoleGuard` (admin principal or
/// workspace-admin role).
pub(crate) async fn provider_registry_handler(
    State(state): State<Arc<AppState>>,
    _admin: crate::extractors::AdminRoleGuard,
) -> impl IntoResponse {
    let snapshot = state.runtime.provider_registry.snapshot();
    let catalog = static_provider_registry_catalog();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "connections": snapshot.connections,
            "fallbacks": snapshot.fallbacks,
            "catalog": catalog,
        })),
    )
        .into_response()
}

pub(crate) fn static_provider_registry_catalog() -> Vec<serde_json::Value> {
    cairn_domain::provider_registry::all()
        .iter()
        .map(|provider| {
            serde_json::json!({
                "id": provider.id,
                "name": provider.name,
                "api_base": provider.api_base,
                "api_format": format!("{:?}", provider.api_format).to_lowercase(),
                "default_model": provider.default_model,
                "available": provider.is_available(),
                "requires_key": provider.requires_key(),
                "env_keys": provider.env_keys,
                "models": provider.models.iter().map(|model| serde_json::json!({
                    "id": model.id,
                    "context_window": model.context_window,
                    "capabilities": {
                        "streaming": model.capabilities.streaming,
                        "tool_use": model.capabilities.tool_use,
                        "vision": model.capabilities.vision,
                        "thinking": model.capabilities.thinking,
                    },
                    "input_cost_per_1m": model.input_cost_per_1m,
                    "output_cost_per_1m": model.output_cost_per_1m,
                })).collect::<Vec<_>>(),
            })
        })
        .collect()
}

#[utoipa::path(
    post,
    path = "/v1/providers/connections",
    tag = "providers",
    request_body = CreateProviderConnectionRequest,
    responses(
        (status = 201, description = "Provider connection created", body = crate::ProviderConnectionRecordDoc),
        (status = 401, description = "Unauthorized", body = ApiError),
        (status = 404, description = "Tenant not found", body = ApiError),
        (status = 409, description = "Provider connection ID already exists", body = ApiError),
        (status = 422, description = "Unprocessable entity", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
pub(crate) async fn create_provider_connection_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: crate::extractors::TenantScope,
    Json(body): Json<CreateProviderConnectionRequest>,
) -> impl IntoResponse {
    use cairn_domain::MULTI_PROVIDER;
    use cairn_runtime::ProviderConnectionConfig;

    if let Some(denied) = require_feature(&state.config, MULTI_PROVIDER) {
        return denied;
    }

    // Tenant boundary: a non-admin caller MUST register the connection
    // under their own tenant — `body.tenant_id` is operator-supplied
    // and cannot be trusted on its own. Without this check, operator B
    // could pass `body.tenant_id="tenant_a"` + a tenant-A credential
    // and the credential-ownership validator below would happily
    // accept the link (matching tenant-id-as-claimed against
    // tenant-id-on-credential), giving operator B a backdoor to
    // exfiltrate tenant A's API key via the /test probe.
    //
    // Admin tokens bypass: cross-tenant provider provisioning is a
    // legitimate admin workflow, so the operator-claim check only
    // applies to non-admin scopes.
    if !tenant_scope.is_admin && body.tenant_id != tenant_scope.tenant_id().as_str() {
        return AppApiError::new(
            StatusCode::FORBIDDEN,
            "tenant_scope_mismatch",
            "tenant_id in request body must match the caller's tenant scope",
        )
        .into_response();
    }

    // #634: refuse credential-less registration for adapters that require
    // a key at runtime. Previously the handler silently accepted the
    // payload, stored a connection record with no credential binding, and
    // the operator only discovered the missing key on the first chat or
    // /test call (401 from upstream). That made the UI wizard's
    // "Register Provider" path look successful while producing a
    // provider that could never route.
    //
    // Adapters exempt from this check:
    //   - ollama: runs unauthenticated against a local endpoint.
    //   - bedrock / bedrock-compat: authenticate via AWS SigV4 using
    //     environment / instance-profile credentials, not a cairn-stored
    //     API key.
    //
    // Operators who intentionally want a credential-less shell can still
    // achieve it by pre-creating the `provider_credential_<conn_id>`
    // default via /v1/settings/defaults before POSTing here. That gives
    // them an explicit escape hatch, while blocking the silent-breakage
    // path that bit the dogfood run.
    if body.credential_id.is_none() && adapter_requires_credential(&body.adapter_type) {
        let key = format!("provider_credential_{}", body.provider_connection_id);
        let system_project = cairn_domain::ProjectKey::system();
        let already_bound = matches!(
            state.runtime.defaults.resolve(&system_project, &key).await,
            Ok(Some(_)),
        );
        if !already_bound {
            return AppApiError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "credential_required",
                format!(
                    "adapter \"{}\" requires an API key: either pass `credential_id` in the request body (create one via POST /v1/admin/tenants/:id/credentials) or pre-bind one via PUT /v1/settings/defaults/system/system/provider_credential_{}",
                    body.adapter_type, body.provider_connection_id,
                ),
            )
            .into_response();
        }
    }

    let before = crate::handlers::sse::current_event_head(&state).await;
    let conn_id = body.provider_connection_id.clone();
    let credential_id = body.credential_id.clone();
    let endpoint_url = body.endpoint_url.clone();
    let tenant_id = TenantId::new(body.tenant_id.clone());

    if let Some(err) =
        validate_credential_belongs_to_tenant(state.as_ref(), &tenant_id, credential_id.as_deref())
            .await
    {
        return err;
    }

    match state
        .runtime
        .provider_connections
        .create(
            tenant_id,
            ProviderConnectionId::new(body.provider_connection_id),
            ProviderConnectionConfig {
                provider_family: body.provider_family,
                adapter_type: body.adapter_type,
                supported_models: body.supported_models,
            },
        )
        .await
    {
        Ok(record) => {
            if let Some(cred_id) = credential_id {
                let key = format!("provider_credential_{conn_id}");
                let _ = state
                    .runtime
                    .defaults
                    .set(
                        cairn_domain::Scope::System,
                        "system".to_owned(),
                        key,
                        serde_json::json!(cred_id),
                    )
                    .await;
            }
            if let Some(url) = endpoint_url {
                let key = format!("provider_endpoint_{conn_id}");
                let _ = state
                    .runtime
                    .defaults
                    .set(
                        cairn_domain::Scope::System,
                        "system".to_owned(),
                        key,
                        serde_json::json!(url),
                    )
                    .await;
            }
            crate::handlers::sse::publish_runtime_frames_since(&state, before).await;
            (StatusCode::CREATED, Json(record)).into_response()
        }
        // Route through `runtime_error_response` so Conflict maps to 409
        // (entity-collision) and NotFound/Validation keep their correct
        // status codes instead of being flattened to 400. F40.
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn resolve_provider_key_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(connection_id): Path<String>,
) -> impl IntoResponse {
    // SEC #717: this handler was previously unauthenticated against
    // the conn_id — a foreign operator could probe whether another
    // tenant has a credential bound to a given connection (the
    // distinct 200/404/410 status codes leak the bind state). With
    // the scope check in place, every cross-tenant probe collapses
    // into the same 404 the helper produces for non-existent ids.
    let conn_id = ProviderConnectionId::new(&connection_id);
    if let Err(resp) = load_connection_owned_by_scope(&state, &conn_id, &tenant_scope).await {
        return resp;
    }

    let credential_id_str = connection_id.as_str();
    let cred_key = format!("provider_credential_{credential_id_str}");
    let system_project = cairn_domain::ProjectKey::system();
    match state.runtime.defaults.resolve(&system_project, &cred_key).await {
        Ok(Some(setting)) => {
            if let Some(cred_id) = setting.as_str() {
                let credential_id = cairn_domain::CredentialId::new(cred_id);
                match state.runtime.credentials.get(&credential_id).await {
                    Ok(Some(record)) if record.active => {
                        (StatusCode::OK, Json(serde_json::json!({
                            "connection_id": connection_id,
                            "credential_id": cred_id,
                            "has_key": true,
                            "provider_id": record.provider_id,
                        }))).into_response()
                    }
                    Ok(Some(_)) => {
                        AppApiError::new(StatusCode::GONE, "credential_revoked", "linked credential has been revoked")
                            .into_response()
                    }
                    Ok(None) => {
                        AppApiError::new(StatusCode::NOT_FOUND, "credential_not_found", "linked credential not found")
                            .into_response()
                    }
                    Err(err) => runtime_error_response(err),
                }
            } else {
                AppApiError::new(StatusCode::NOT_FOUND, "no_credential", "no credential linked to this connection")
                    .into_response()
            }
        }
        _ => {
            AppApiError::new(StatusCode::NOT_FOUND, "no_credential", "no credential linked to this connection — store API key via POST /v1/admin/tenants/:id/credentials then link it")
                .into_response()
        }
    }
}

pub(crate) async fn update_provider_connection_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
    Json(body): Json<UpdateProviderConnectionRequest>,
) -> impl IntoResponse {
    use cairn_runtime::ProviderConnectionConfig;

    // SEC #717: route through `load_connection_owned_by_scope` so the
    // 6-handler sweep behaves identically. Gemini SEC-007: the prior
    // PR returned 403 on cross-tenant — that leaks existence of the
    // foreign id. Collapse to 404. Helper also fixes the typed
    // `TenantId` comparison Gemini called out (the prior code
    // compared `TenantId != &str` and didn't compile against
    // `cairn-domain` HEAD; only sibling-PR drift kept the workspace
    // green up to this point). The redundant `get` Gemini noted is
    // gone too — the helper returns the loaded record so the handler
    // doesn't fetch twice.
    let conn_id = ProviderConnectionId::new(&id);
    let before = crate::handlers::sse::current_event_head(&state).await;
    let _existing = match load_connection_owned_by_scope(&state, &conn_id, &tenant_scope).await {
        Ok(record) => record,
        Err(resp) => return resp,
    };

    let config = ProviderConnectionConfig {
        provider_family: body.provider_family,
        adapter_type: body.adapter_type,
        supported_models: body.supported_models,
    };
    if let Some(cred_id) = body.credential_id.as_deref() {
        // Pre-fetch the existing record so the credential validator
        // can compare ownership against the connection's *real*
        // tenant (not body-supplied data — body has no tenant field
        // on update). This is a small redundancy with the read
        // inside `provider_connections.update` below, but in the hot
        // path both reads hit the in-memory projection rather than a
        // store roundtrip; the cost is dominated by the credential
        // ownership check itself. Map both error branches through
        // `runtime_error_response` so a missing connection produces
        // the same 404 envelope shape as a missing connection on the
        // update call below (Gemini consistency feedback).
        let tenant_id = match state.runtime.provider_connections.get(&conn_id).await {
            Ok(Some(record)) => record.tenant_id,
            Ok(None) => {
                return runtime_error_response(cairn_runtime::RuntimeError::NotFound {
                    entity: "provider_connection",
                    id: conn_id.to_string(),
                });
            }
            Err(err) => return runtime_error_response(err),
        };
        if let Some(err) =
            validate_credential_belongs_to_tenant(state.as_ref(), &tenant_id, Some(cred_id)).await
        {
            return err;
        }
    }

    match state
        .runtime
        .provider_connections
        .update(&conn_id, config)
        .await
    {
        Ok(record) => {
            if let Some(url) = body.endpoint_url {
                let key = format!("provider_endpoint_{id}");
                let _ = state
                    .runtime
                    .defaults
                    .set(
                        cairn_domain::Scope::System,
                        "system".to_owned(),
                        key,
                        serde_json::json!(url),
                    )
                    .await;
            }
            if let Some(cred_id) = body.credential_id {
                let key = format!("provider_credential_{id}");
                let _ = state
                    .runtime
                    .defaults
                    .set(
                        cairn_domain::Scope::System,
                        "system".to_owned(),
                        key,
                        serde_json::json!(cred_id),
                    )
                    .await;
            }
            crate::handlers::sse::publish_runtime_frames_since(&state, before).await;
            (StatusCode::OK, Json(record)).into_response()
        }
        Err(err) => AppApiError::new(StatusCode::BAD_REQUEST, "bad_request", err.to_string())
            .into_response(),
    }
}

async fn validate_credential_belongs_to_tenant(
    state: &AppState,
    tenant_id: &TenantId,
    credential_id: Option<&str>,
) -> Option<axum::response::Response> {
    let credential_id = credential_id?;
    let credential = match state
        .runtime
        .credentials
        .get(&cairn_domain::CredentialId::new(credential_id))
        .await
    {
        Ok(Some(record)) => record,
        Ok(None) => {
            return Some(
                AppApiError::new(
                    StatusCode::NOT_FOUND,
                    "credential_not_found",
                    "credential not found",
                )
                .into_response(),
            );
        }
        Err(err) => return Some(runtime_error_response(err)),
    };

    if credential.tenant_id != *tenant_id {
        return Some(
            AppApiError::new(
                StatusCode::FORBIDDEN,
                "credential_tenant_mismatch",
                "credential must belong to the same tenant as the provider connection",
            )
            .into_response(),
        );
    }

    None
}

pub(crate) async fn delete_provider_connection_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
) -> impl IntoResponse {
    // F40: Previously this handler appended a `ProviderConnectionRegistered`
    // event with empty fields and `Disabled` status — which overwrote the
    // projection row with bogus data instead of removing it. Re-creating
    // with the same ID then failed with 400 "provider_connection conflict".
    // We now emit a real `ProviderConnectionDeleted` event; the projection
    // hard-removes the row so the ID is free to re-use. The full history
    // (Registered -> Deleted -> Registered) remains in the event log.
    //
    // SEC #717: the original delete handler had no auth at all against
    // the URL-path id — any operator could wipe another tenant's
    // provider connection. Cross-tenant hits collapse to 404 via the
    // shared helper (Gemini SEC-007 enumeration safety) instead of a
    // distinct 403 that would let an attacker probe valid ids.
    let conn_id = ProviderConnectionId::new(&id);
    if let Err(resp) = load_connection_owned_by_scope(&state, &conn_id, &tenant_scope).await {
        return resp;
    }
    let before = crate::handlers::sse::current_event_head(&state).await;
    match state.runtime.provider_connections.delete(&conn_id).await {
        Ok(()) => {
            crate::handlers::sse::publish_runtime_frames_since(&state, before).await;
            (
                StatusCode::OK,
                Json(serde_json::json!({ "deleted": true, "connection_id": id })),
            )
                .into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

#[utoipa::path(
    get,
    path = "/v1/providers/bindings",
    tag = "providers",
    responses(
        (status = 200, description = "Provider bindings listed", body = crate::ProviderBindingListResponseDoc),
        (status = 400, description = "Invalid request", body = ApiError),
        (status = 401, description = "Unauthorized", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
pub(crate) async fn list_provider_bindings_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<OptionalTenantScopedQuery>,
) -> impl IntoResponse {
    // #422: honest pagination — fetch `limit + 1`, derive `has_more`.
    let limit = query.limit();
    match state
        .runtime
        .provider_bindings
        .list(&TenantId::new(query.tenant_id()), limit + 1, query.offset())
        .await
    {
        Ok(mut items) => {
            let has_more = items.len() > limit;
            items.truncate(limit);
            (StatusCode::OK, Json(ListResponse { items, has_more })).into_response()
        }
        Err(err) => AppApiError::new(StatusCode::BAD_REQUEST, "bad_request", err.to_string())
            .into_response(),
    }
}

pub(crate) async fn get_binding_cost_stats_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    use cairn_store::projections::ProviderBindingCostStatsReadModel;
    match ProviderBindingCostStatsReadModel::get(
        state.runtime.store.as_ref(),
        &ProviderBindingId::new(id),
    )
    .await
    {
        Ok(Some(stats)) => (StatusCode::OK, Json(stats)).into_response(),
        Ok(None) => AppApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no cost stats for binding",
        )
        .into_response(),
        Err(err) => store_error_response(err),
    }
}

pub(crate) async fn list_binding_cost_ranking_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<CostRankingQuery>,
) -> impl IntoResponse {
    use cairn_store::projections::ProviderBindingCostStatsReadModel;
    // #422: the read model returns every binding's stats for the
    // tenant. Apply limit/offset in-memory and compute `has_more` so
    // operator UI can page through ranked bindings.
    let tenant_id = TenantId::new(query.tenant_id.as_deref().unwrap_or(DEFAULT_TENANT_ID));
    let offset = query.offset();
    let limit = query.limit();
    match ProviderBindingCostStatsReadModel::list_by_tenant(
        state.runtime.store.as_ref(),
        &tenant_id,
    )
    .await
    {
        Ok(all) => {
            let total = all.len();
            let items: Vec<_> = all.into_iter().skip(offset).take(limit).collect();
            let has_more = offset.saturating_add(items.len()) < total;
            (StatusCode::OK, Json(ListResponse { items, has_more })).into_response()
        }
        Err(err) => store_error_response(err),
    }
}

pub(crate) async fn create_provider_binding_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateProviderBindingRequest>,
) -> impl IntoResponse {
    match state
        .runtime
        .provider_bindings
        .create(
            body.project(),
            ProviderConnectionId::new(body.provider_connection_id),
            body.operation_kind,
            ProviderModelId::new(body.provider_model_id),
            body.estimated_cost_micros,
        )
        .await
    {
        Ok(record) => (StatusCode::CREATED, Json(record)).into_response(),
        Err(err) => AppApiError::new(StatusCode::BAD_REQUEST, "bad_request", err.to_string())
            .into_response(),
    }
}

pub(crate) async fn list_route_policies_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<TenantScopedQuery>,
) -> impl IntoResponse {
    // #422: honest pagination — fetch `limit + 1`, derive `has_more`.
    let limit = query.limit();
    let offset = query.offset();
    match RoutePolicyReadModel::list_by_tenant(
        state.runtime.store.as_ref(),
        &TenantId::new(query.tenant_id),
        limit + 1,
        offset,
    )
    .await
    {
        Ok(mut items) => {
            let has_more = items.len() > limit;
            items.truncate(limit);
            (StatusCode::OK, Json(ListResponse { items, has_more })).into_response()
        }
        Err(err) => {
            tracing::error!("list_route_policies failed: {err}");
            AppApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                err.to_string(),
            )
            .into_response()
        }
    }
}

pub(crate) async fn create_route_policy_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateRoutePolicyRequest>,
) -> impl IntoResponse {
    let domain_rules: Vec<RoutePolicyRule> = body.rules.into_iter().map(Into::into).collect();
    match state
        .runtime
        .route_policies
        .create(TenantId::new(body.tenant_id), body.name, domain_rules)
        .await
    {
        Ok(record) => (StatusCode::CREATED, Json(record)).into_response(),
        Err(err) => AppApiError::new(StatusCode::BAD_REQUEST, "bad_request", err.to_string())
            .into_response(),
    }
}

pub(crate) async fn create_guardrail_policy_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateGuardrailPolicyRequest>,
) -> impl IntoResponse {
    match state
        .runtime
        .guardrails
        .create_policy(TenantId::new(body.tenant_id), body.name, body.rules)
        .await
    {
        Ok(record) => (StatusCode::CREATED, Json(record)).into_response(),
        Err(err) => AppApiError::new(StatusCode::BAD_REQUEST, "bad_request", err.to_string())
            .into_response(),
    }
}

pub(crate) async fn evaluate_guardrail_policy_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<EvaluateGuardrailPolicyRequest>,
) -> impl IntoResponse {
    match state
        .runtime
        .guardrails
        .evaluate(
            TenantId::new(body.tenant_id),
            body.subject_type,
            body.subject_id,
            body.action,
        )
        .await
    {
        Ok(decision) => (StatusCode::OK, Json(decision)).into_response(),
        Err(err) => AppApiError::new(StatusCode::BAD_REQUEST, "bad_request", err.to_string())
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::adapter_requires_credential;

    #[test]
    fn ollama_exempt() {
        assert!(!adapter_requires_credential("ollama"));
        assert!(!adapter_requires_credential("Ollama"));
        assert!(!adapter_requires_credential("  OLLAMA  "));
    }

    #[test]
    fn bedrock_exempt_all_spellings() {
        // Bedrock family authenticates via AWS SigV4 (env / instance
        // profile), not a cairn-stored API key.
        assert!(!adapter_requires_credential("bedrock"));
        assert!(!adapter_requires_credential("bedrock-compat"));
        assert!(!adapter_requires_credential("bedrock_compat"));
        assert!(!adapter_requires_credential("bedrock-converse"));
        assert!(!adapter_requires_credential("bedrock-openai"));
    }

    #[test]
    fn everything_else_requires_credential() {
        // Representative sample across families — openai-shaped, native
        // Z.ai, Google Gemini, azure — every one needs a key.
        for adapter in [
            "openai",
            "anthropic",
            "zai",
            "zai-coding",
            "google",
            "deepseek",
            "xai",
            "groq",
            "azure-openai",
            "openrouter",
            "minimax",
            "openai-compatible",
        ] {
            assert!(
                adapter_requires_credential(adapter),
                "{adapter} should require a credential",
            );
        }
    }

    #[test]
    fn unknown_adapters_fail_closed() {
        // A typo or brand-new adapter must refuse the write rather than
        // silently accept — 422 with `credential_required` is strictly
        // better than a registered-but-broken connection.
        assert!(adapter_requires_credential("typo-adapter"));
        assert!(adapter_requires_credential("my-custom-proxy"));
    }
}
