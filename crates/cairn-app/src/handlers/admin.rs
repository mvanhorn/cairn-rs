//! Admin, tenant, workspace, credential, and notification handlers.
//!
//! Extracted from `lib.rs` — contains tenant CRUD, workspace/project
//! management, workspace membership, resource sharing, credential
//! store, operator profiles, notification preferences, audit logs,
//! request logs, quota/retention policies, event-log compaction, and
//! snapshot management.

use std::sync::Arc;

use axum::{
    extract::{rejection::JsonRejection, Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use utoipa::ToSchema;

use cairn_api::auth::AuthPrincipal;
use cairn_api::http::{ApiError, ListResponse};
use cairn_domain::credentials::CredentialRecord;
use cairn_domain::{
    AuditLogEntry, AuditOutcome, CredentialId, ProjectKey, TenantId, WorkspaceId, WorkspaceKey,
    WorkspaceRole, CREDENTIAL_MANAGEMENT,
};
use cairn_runtime::{
    AuditService, CredentialService, NotificationService, OperatorProfileService, ProjectService,
    QuotaService, RetentionService, TenantService, WorkspaceMembershipService, WorkspaceService,
};
use cairn_store::projections::{AuditLogReadModel, QuotaReadModel, RetentionPolicyReadModel};

use crate::errors::{
    api_error_with_details, json_rejection_response, require_feature, runtime_error_response,
    store_error_response, validation_error_response, AppApiError,
};
use crate::extractors::{AdminRoleGuard, TenantScope};
use crate::state::AppState;
use crate::tokens::RequestLogEntry;
use crate::webhook_validation::{validate_channels, WebhookValidationPolicy};
#[allow(unused_imports)]
use crate::{ProjectRecordDoc, RunListResponseDoc, TenantRecordDoc, WorkspaceRecordDoc};

const DEFAULT_TENANT_ID: &str = "default_tenant";

pub(crate) fn audit_actor_id(principal: &AuthPrincipal) -> String {
    match principal {
        AuthPrincipal::Operator { operator_id, .. } => operator_id.to_string(),
        AuthPrincipal::ServiceAccount { name, .. } => name.clone(),
        AuthPrincipal::System => "system".to_owned(),
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct AuditLogQuery {
    /// Inclusive lower bound on `occurred_at_ms`.
    pub since_ms: Option<u64>,
    /// Exclusive upper bound on `occurred_at_ms` — used for
    /// "older than X" cursor pagination from the UI.
    pub before_ms: Option<u64>,
    /// Max entries to return (capped at [`MAX_AUDIT_LIMIT`],
    /// default [`DEFAULT_AUDIT_LIMIT`]).
    pub limit: Option<usize>,
}

pub(crate) const DEFAULT_AUDIT_LIMIT: usize = 100;
pub(crate) const MAX_AUDIT_LIMIT: usize = 1000;

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct CreateOperatorProfileRequest {
    pub display_name: String,
    pub email: String,
    pub role: WorkspaceRole,
}

#[derive(Clone, Debug, serde::Deserialize, ToSchema)]
pub(crate) struct CreateProjectRequest {
    pub project_id: String,
    pub name: String,
}

#[derive(Clone, Debug, serde::Deserialize, ToSchema)]
pub(crate) struct CreateTenantRequest {
    pub tenant_id: String,
    pub name: String,
}

#[derive(Clone, Debug, serde::Deserialize, ToSchema)]
pub(crate) struct CreateWorkspaceRequest {
    pub workspace_id: String,
    pub name: String,
}

#[derive(Clone, Debug, serde::Serialize, ToSchema)]
pub(crate) struct CredentialSummary {
    #[schema(value_type = String)]
    pub id: CredentialId,
    #[schema(value_type = String)]
    pub tenant_id: TenantId,
    pub provider_id: String,
    pub name: String,
    pub credential_type: String,
    pub key_version: Option<String>,
    pub key_id: Option<String>,
    pub encrypted_at_ms: Option<u64>,
    pub active: bool,
    pub revoked_at_ms: Option<u64>,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub(crate) struct PaginationQuery {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl PaginationQuery {
    /// Default per-page cap when the client doesn't supply one.
    pub const DEFAULT_LIMIT: usize = 100;

    /// Maximum per-page cap (Copilot review on #589). Requests with
    /// `limit` above this are clamped silently so `limit + 1` overflow
    /// fetches can't wrap `usize` or force unbounded reads. Matches
    /// the cap already applied to `TenantCostQuery::MAX_LIMIT`.
    pub const MAX_LIMIT: usize = 1_000;

    pub fn limit(&self) -> usize {
        self.limit
            .unwrap_or(Self::DEFAULT_LIMIT)
            .min(Self::MAX_LIMIT)
    }

    pub fn offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct RotateCredentialKeyRequest {
    pub old_key_id: String,
    pub new_key_id: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct SetRetentionPolicyRequest {
    pub full_history_days: u32,
    pub current_state_days: u32,
    pub max_events_per_entity: u32,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct SetTenantQuotaRequest {
    pub max_concurrent_runs: u32,
    pub max_sessions_per_hour: u32,
    pub max_tasks_per_run: u32,
}

/// Closes #492: the plaintext value must never survive in logs, `Debug`,
/// or the OpenAPI example.
///
/// The struct itself is crate-private (`pub(crate)`) and `plaintext_value`
/// is exposed through a narrow accessor (`into_plaintext_value`) so the
/// handler can move it into the credential service call without exposing
/// the field to unrelated code. The manual `Debug` impl prints
/// `[redacted]` so future `tracing::debug!("{body:?}")` additions cannot
/// leak the secret.
///
/// Per Copilot review on PR #535: the field was previously `pub` with a
/// comment claiming it was private; tightening now so the comment and
/// the access pattern match.
#[derive(Clone, serde::Deserialize, ToSchema)]
pub(crate) struct StoreCredentialRequest {
    pub provider_id: String,
    plaintext_value: String,
    pub key_id: Option<String>,
}

impl StoreCredentialRequest {
    /// Move the plaintext value out for the encrypt call. The caller is
    /// expected to hand this straight to the credential service, which
    /// wraps it in `Zeroizing<String>` for the remainder of its lifetime.
    pub(crate) fn into_plaintext_value(self) -> String {
        self.plaintext_value
    }
}

impl std::fmt::Debug for StoreCredentialRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreCredentialRequest")
            .field("provider_id", &self.provider_id)
            .field("plaintext_value", &"[redacted]")
            .field("key_id", &self.key_id)
            .finish()
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
#[allow(dead_code)]
pub(crate) struct TenantScopedQuery {
    pub tenant_id: String,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl TenantScopedQuery {
    pub fn limit(&self) -> usize {
        self.limit.unwrap_or(100)
    }

    pub fn offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }
}

// ── Admin DTOs ───────────────────────────────────────────────────────────────

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct CompactEventLogRequest {
    retain_last_n: u32,
}

/// RFC 008 tenant overview: per-workspace summary entry.
#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct WorkspaceSummary {
    workspace_id: String,
    name: String,
    member_count: u32,
    project_count: u32,
    active_runs: u32,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct TenantOverview {
    tenant_id: String,
    workspace_count: u32,
    total_members: u32,
    active_runs: u32,
    workspaces: Vec<WorkspaceSummary>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct RequestLogsQuery {
    #[serde(default = "default_logs_limit")]
    limit: usize,
    level: Option<String>,
    /// Lower bound on `start_time_unix_ns` (millis × 1_000_000). Entries older
    /// than this are excluded — used by the UI "last hour / last 24h" filter.
    since_ms: Option<u64>,
}

fn default_logs_limit() -> usize {
    200
}

const MAX_REQUEST_LOGS_LIMIT: usize = 500;

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct SetNotificationPreferencesRequest {
    tenant_id: Option<String>,
    event_types: Vec<String>,
    channels: Vec<cairn_domain::notification_prefs::NotificationChannel>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct CreateShareRequest {
    target_workspace_id: String,
    resource_type: String,
    resource_id: String,
    #[serde(default)]
    permissions: Vec<String>,
    tenant_id: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct AddWorkspaceMemberRequest {
    member_id: String,
    role: WorkspaceRole,
}

// ── Helpers (used only by admin handlers) ────────────────────────────────────

pub(crate) fn credential_summary(record: CredentialRecord) -> CredentialSummary {
    CredentialSummary {
        id: record.id,
        tenant_id: record.tenant_id,
        provider_id: record.provider_id,
        name: record.name,
        credential_type: record.credential_type,
        key_version: record.key_version,
        key_id: record.key_id,
        encrypted_at_ms: record.encrypted_at_ms,
        active: record.active,
        revoked_at_ms: record.revoked_at_ms,
        created_at: record.created_at,
        updated_at: record.updated_at,
    }
}

pub(crate) async fn workspace_key_for_id(
    state: &Arc<AppState>,
    workspace_id: &WorkspaceId,
) -> Result<WorkspaceKey, cairn_runtime::RuntimeError> {
    let workspace = state
        .runtime
        .workspaces
        .get(workspace_id)
        .await?
        .ok_or_else(|| cairn_runtime::RuntimeError::NotFound {
            entity: "workspace",
            id: workspace_id.to_string(),
        })?;
    Ok(WorkspaceKey::new(
        workspace.tenant_id,
        workspace.workspace_id,
    ))
}

// ── Tenant handlers ──────────────────────────────────────────────────────────

pub(crate) async fn list_tenants_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Query(query): Query<PaginationQuery>,
) -> impl IntoResponse {
    // T6a-H3 + T6a-C4: listing every tenant is an admin-only operation.
    // Non-admins enumerating the tenant topology is a cross-tenant
    // metadata leak.
    //
    // #422: honest pagination — fetch `limit + 1`, compute `has_more`,
    // truncate. The previous unconditional `has_more: false` silently
    // hid extra rows past the first page.
    let limit = query.limit();
    match state.runtime.tenants.list(limit + 1, query.offset()).await {
        Ok(mut items) => {
            let has_more = items.len() > limit;
            items.truncate(limit);
            (StatusCode::OK, Json(ListResponse { items, has_more })).into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

#[utoipa::path(
    post,
    path = "/v1/admin/tenants",
    tag = "admin",
    request_body = CreateTenantRequest,
    responses(
        (status = 201, description = "Tenant created", body = TenantRecordDoc),
        (status = 400, description = "Invalid request", body = ApiError),
        (status = 401, description = "Unauthorized", body = ApiError),
        (status = 422, description = "Unprocessable entity", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
pub(crate) async fn create_tenant_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Extension(principal): Extension<AuthPrincipal>,
    Json(body): Json<CreateTenantRequest>,
) -> impl IntoResponse {
    match state
        .runtime
        .tenants
        .create(TenantId::new(body.tenant_id), body.name)
        .await
    {
        Ok(record) => match state
            .runtime
            .audits
            .record(
                record.tenant_id.clone(),
                audit_actor_id(&principal),
                "create_tenant".to_owned(),
                "tenant".to_owned(),
                record.tenant_id.to_string(),
                AuditOutcome::Success,
                serde_json::json!({ "name": record.name }),
            )
            .await
        {
            Ok(_) => (StatusCode::CREATED, Json(record)).into_response(),
            Err(err) => runtime_error_response(err),
        },
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn get_tenant_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.runtime.tenants.get(&TenantId::new(id)).await {
        Ok(Some(record)) => (StatusCode::OK, Json(record)).into_response(),
        Ok(None) => {
            AppApiError::new(StatusCode::NOT_FOUND, "not_found", "tenant not found").into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn get_tenant_overview_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let tenant_id = TenantId::new(id);

    match state.runtime.tenants.get(&tenant_id).await {
        Ok(None) => {
            return AppApiError::new(StatusCode::NOT_FOUND, "not_found", "tenant not found")
                .into_response()
        }
        Err(err) => return runtime_error_response(err),
        Ok(Some(_)) => {}
    }

    let workspaces = match state
        .runtime
        .workspaces
        .list_by_tenant(&tenant_id, usize::MAX, 0, false)
        .await
    {
        Ok(ws) => ws,
        Err(err) => return runtime_error_response(err),
    };

    let tenant_active_runs = state
        .runtime
        .store
        .count_active_runs_for_tenant(&tenant_id)
        .await as u32;

    let mut workspace_summaries = Vec::with_capacity(workspaces.len());
    let mut total_members: u32 = 0;

    for workspace in &workspaces {
        let workspace_key =
            WorkspaceKey::new(workspace.tenant_id.clone(), workspace.workspace_id.clone());

        let members = match state
            .runtime
            .workspace_memberships
            .list_members(&workspace_key)
            .await
        {
            Ok(m) => m,
            Err(err) => return runtime_error_response(err),
        };

        let projects = match state
            .runtime
            .projects
            .list_by_workspace(&workspace.tenant_id, &workspace.workspace_id, usize::MAX, 0)
            .await
        {
            Ok(p) => p,
            Err(err) => return runtime_error_response(err),
        };

        let ws_active_runs = state
            .runtime
            .store
            .count_active_runs_for_workspace(&workspace_key)
            .await as u32;

        let member_count = members.len() as u32;
        total_members += member_count;

        workspace_summaries.push(WorkspaceSummary {
            workspace_id: workspace.workspace_id.to_string(),
            name: workspace.name.clone(),
            member_count,
            project_count: projects.len() as u32,
            active_runs: ws_active_runs,
        });
    }

    (
        StatusCode::OK,
        Json(TenantOverview {
            tenant_id: tenant_id.to_string(),
            workspace_count: workspaces.len() as u32,
            total_members,
            active_runs: tenant_active_runs,
            workspaces: workspace_summaries,
        }),
    )
        .into_response()
}

// ── Quota / retention ────────────────────────────────────────────────────────

pub(crate) async fn get_tenant_quota_handler(
    State(state): State<Arc<AppState>>,
    Path(tenant_id): Path<String>,
) -> impl IntoResponse {
    match QuotaReadModel::get_quota(state.runtime.store.as_ref(), &TenantId::new(tenant_id)).await {
        Ok(Some(quota)) => (StatusCode::OK, Json(quota)).into_response(),
        Ok(None) => AppApiError::new(StatusCode::NOT_FOUND, "not_found", "tenant quota not found")
            .into_response(),
        Err(err) => store_error_response(err),
    }
}

pub(crate) async fn set_tenant_quota_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Path(tenant_id): Path<String>,
    Json(body): Json<SetTenantQuotaRequest>,
) -> impl IntoResponse {
    match state
        .runtime
        .quotas
        .set_quota(
            TenantId::new(tenant_id),
            body.max_concurrent_runs,
            body.max_sessions_per_hour,
            body.max_tasks_per_run,
        )
        .await
    {
        Ok(quota) => (StatusCode::OK, Json(quota)).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn get_retention_policy_handler(
    State(state): State<Arc<AppState>>,
    Path(tenant_id): Path<String>,
) -> impl IntoResponse {
    match RetentionPolicyReadModel::get_by_tenant(
        state.runtime.store.as_ref(),
        &TenantId::new(tenant_id),
    )
    .await
    {
        Ok(Some(policy)) => (StatusCode::OK, Json(policy)).into_response(),
        Ok(None) => AppApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "tenant retention policy not found",
        )
        .into_response(),
        Err(err) => store_error_response(err),
    }
}

pub(crate) async fn set_retention_policy_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Path(tenant_id): Path<String>,
    Json(body): Json<SetRetentionPolicyRequest>,
) -> impl IntoResponse {
    match state
        .runtime
        .retention
        .set_policy(
            TenantId::new(tenant_id),
            body.full_history_days,
            body.current_state_days,
            body.max_events_per_entity,
        )
        .await
    {
        Ok(policy) => (StatusCode::OK, Json(policy)).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn apply_retention_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Path(tenant_id): Path<String>,
) -> impl IntoResponse {
    match state
        .runtime
        .retention
        .apply_retention(&TenantId::new(tenant_id))
        .await
    {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

// ── Audit log ────────────────────────────────────────────────────────────────

pub(crate) async fn list_audit_log_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Query(query): Query<AuditLogQuery>,
) -> impl IntoResponse {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_AUDIT_LIMIT)
        .clamp(1, MAX_AUDIT_LIMIT);
    // Fetch one extra so we can accurately report `has_more` without needing
    // a separate count query.
    let fetch_limit = limit.saturating_add(1);
    // Admin users see all audit entries (scan all known tenants).
    // Non-admin users only see their own tenant's entries.
    if tenant_scope.is_admin {
        // Collect audit entries across all tenants for admin visibility.
        let tenants = match state.runtime.tenants.list(100, 0).await {
            Ok(t) => t,
            Err(err) => return runtime_error_response(err),
        };
        let mut all_items = Vec::new();
        for tenant in &tenants {
            match AuditLogReadModel::list_by_tenant(
                state.runtime.store.as_ref(),
                &tenant.tenant_id,
                query.since_ms,
                query.before_ms,
                fetch_limit,
            )
            .await
            {
                Ok(mut items) => all_items.append(&mut items),
                Err(err) => return runtime_error_response(err.into()),
            }
        }
        // Sort by occurred_at_ms descending (most recent first).
        all_items.sort_by_key(|r| std::cmp::Reverse(r.occurred_at_ms));
        let has_more = all_items.len() > limit;
        all_items.truncate(limit);
        (
            StatusCode::OK,
            Json(ListResponse {
                has_more,
                items: all_items,
            }),
        )
            .into_response()
    } else {
        match AuditLogReadModel::list_by_tenant(
            state.runtime.store.as_ref(),
            tenant_scope.tenant_id(),
            query.since_ms,
            query.before_ms,
            fetch_limit,
        )
        .await
        {
            Ok(mut items) => {
                let has_more = items.len() > limit;
                items.truncate(limit);
                (StatusCode::OK, Json(ListResponse { has_more, items })).into_response()
            }
            Err(err) => runtime_error_response(err.into()),
        }
    }
}

pub(crate) async fn list_audit_log_for_resource_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path((resource_type, resource_id)): Path<(String, String)>,
    Query(query): Query<PaginationQuery>,
) -> impl IntoResponse {
    // #422: honest pagination. `list_by_resource` does not push
    // limit/offset into the store today (the backing data is naturally
    // bounded per resource-id — audit entries per run are single-digit
    // on average, three-digit for pathological flows). Apply the page
    // in-memory after the tenant filter, and compute `has_more` against
    // the filtered total so callers see honest flags even when the
    // page ends exactly at the list tail.
    match AuditLogReadModel::list_by_resource(
        state.runtime.store.as_ref(),
        &resource_type,
        &resource_id,
    )
    .await
    {
        Ok(items) => {
            let filtered: Vec<AuditLogEntry> = items
                .into_iter()
                .filter(|entry| entry.tenant_id == *tenant_scope.tenant_id())
                .collect();
            let total = filtered.len();
            let offset = query.offset();
            let limit = query.limit();
            let items: Vec<AuditLogEntry> = filtered.into_iter().skip(offset).take(limit).collect();
            let has_more = offset.saturating_add(items.len()) < total;
            (StatusCode::OK, Json(ListResponse { has_more, items })).into_response()
        }
        Err(err) => runtime_error_response(err.into()),
    }
}

// ── Request logs ─────────────────────────────────────────────────────────────

/// `GET /v1/admin/logs?limit=200&level=info,warn,error` — structured request
/// log tail from the in-memory ring buffer populated by observability middleware.
pub(crate) async fn list_request_logs_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Query(q): Query<RequestLogsQuery>,
) -> impl IntoResponse {
    // T6a-H3: request logs span every tenant — admin-only.
    let limit = q.limit.clamp(1, MAX_REQUEST_LOGS_LIMIT);
    let level_filter: Vec<&'static str> = q
        .level
        .as_deref()
        .map(|s| {
            s.split(',')
                .filter_map(|l| match l.trim() {
                    "info" => Some("info"),
                    "warn" => Some("warn"),
                    "error" => Some("error"),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    // UI sends millis since epoch; buffer stores nanos.
    let since_ns = q.since_ms.map(|ms| ms.saturating_mul(1_000_000));

    let (entries, buffered): (Vec<RequestLogEntry>, usize) = match state.request_log.read() {
        Ok(log) => (
            log.tail(limit, &level_filter, since_ns)
                .into_iter()
                .cloned()
                .collect(),
            log.len(),
        ),
        Err(poisoned) => {
            let log = poisoned.into_inner();
            (
                log.tail(limit, &level_filter, since_ns)
                    .into_iter()
                    .cloned()
                    .collect(),
                log.len(),
            )
        }
    };

    // `total` is the total number of entries currently held in the ring
    // buffer (useful for the "Showing N of M buffered" footer). `limit`
    // echoes the applied page size so clients can derive whether they
    // hit the pagination ceiling (entries.len() == limit && limit < total).
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "entries": entries,
            "limit":   limit,
            "total":   buffered,
        })),
    )
}

// ── Snapshot / compaction ────────────────────────────────────────────────────

pub(crate) async fn compact_event_log_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Path(id): Path<String>,
    Json(body): Json<CompactEventLogRequest>,
) -> impl IntoResponse {
    let tenant_id = TenantId::new(id);
    let report = state
        .runtime
        .store
        .compact_event_log(&tenant_id, Some(body.retain_last_n as u64));
    (StatusCode::OK, Json(report)).into_response()
}

pub(crate) async fn create_snapshot_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Path(id): Path<String>,
) -> axum::response::Response {
    let tenant_id = TenantId::new(id);
    let snapshot = match state.runtime.store.create_snapshot(&tenant_id) {
        Ok(s) => s,
        Err(e) => {
            // Closes #418 + SEC-007: the snapshot backend surfaces raw
            // driver strings (connection paths, SQL fragments, tenant
            // row data in some arms). Log the full chain server-side
            // and return a stable opaque message — operators correlate
            // via `x-request-id`. The envelope is the canonical
            // `AppApiError` shape so UI parsers keyed on `code`/
            // `message` no longer see `undefined`.
            tracing::error!(
                tenant_id = %tenant_id.as_str(),
                error = %e,
                "snapshot_failed: create_snapshot backend error"
            );
            return AppApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "snapshot_failed",
                "failed to create tenant snapshot",
            )
            .into_response();
        }
    };
    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "snapshot_id": snapshot.snapshot_id,
            "tenant_id": snapshot.tenant_id.as_str(),
            "event_position": snapshot.event_position,
            "state_hash": snapshot.state_hash,
            "created_at_ms": snapshot.created_at_ms,
        })),
    )
        .into_response()
}

pub(crate) async fn list_snapshots_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(query): Query<PaginationQuery>,
) -> impl IntoResponse {
    use cairn_store::projections::SnapshotReadModel;
    let tenant_id = TenantId::new(id);
    match SnapshotReadModel::list_by_tenant(state.runtime.store.as_ref(), &tenant_id).await {
        Ok(snapshots) => {
            // #422: honest pagination. Snapshots per tenant are bounded
            // (typically <30), but callers still get a truthful
            // `has_more` so the UI "load more" control works.
            let total = snapshots.len();
            let offset = query.offset();
            let limit = query.limit();
            let items: Vec<_> = snapshots
                .iter()
                .skip(offset)
                .take(limit)
                .map(|s| {
                    serde_json::json!({
                        "snapshot_id": s.snapshot_id,
                        "tenant_id": s.tenant_id.as_str(),
                        "event_position": s.event_position,
                        "state_hash": s.state_hash,
                        "created_at_ms": s.created_at_ms,
                    })
                })
                .collect();
            let has_more = offset.saturating_add(items.len()) < total;
            (StatusCode::OK, Json(ListResponse { items, has_more })).into_response()
        }
        Err(err) => store_error_response(err),
    }
}

pub(crate) async fn restore_from_snapshot_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Path(id): Path<String>,
) -> impl IntoResponse {
    use cairn_store::projections::SnapshotReadModel;
    let tenant_id = TenantId::new(id);
    let latest = match SnapshotReadModel::get_latest(state.runtime.store.as_ref(), &tenant_id).await
    {
        Ok(Some(s)) => s,
        Ok(None) => {
            return AppApiError::new(
                StatusCode::NOT_FOUND,
                "not_found",
                "no snapshot found for tenant",
            )
            .into_response();
        }
        Err(err) => return store_error_response(err),
    };
    let report = state.runtime.store.restore_from_snapshot(&latest);
    (StatusCode::OK, Json(report)).into_response()
}

// ── Workspace / project handlers ─────────────────────────────────────────────

#[utoipa::path(
    post,
    path = "/v1/admin/tenants/{tenant_id}/workspaces",
    tag = "admin",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier")
    ),
    request_body = CreateWorkspaceRequest,
    responses(
        (status = 201, description = "Workspace created", body = WorkspaceRecordDoc),
        (status = 400, description = "Invalid request", body = ApiError),
        (status = 401, description = "Unauthorized", body = ApiError),
        (status = 404, description = "Tenant not found", body = ApiError),
        (status = 422, description = "Unprocessable entity", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
pub(crate) async fn create_workspace_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Path(tenant_id): Path<String>,
    Json(body): Json<CreateWorkspaceRequest>,
) -> impl IntoResponse {
    match state
        .runtime
        .workspaces
        .create(
            TenantId::new(tenant_id),
            WorkspaceId::new(body.workspace_id),
            body.name,
        )
        .await
    {
        Ok(record) => (StatusCode::CREATED, Json(record)).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub(crate) struct ListWorkspacesQuery {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    /// When true, soft-deleted (archived) workspaces are included in the
    /// response. Defaults to false so operator lists show only active
    /// workspaces.
    #[serde(default)]
    pub include_archived: bool,
}

impl ListWorkspacesQuery {
    pub fn limit(&self) -> usize {
        self.limit.unwrap_or(100)
    }
    pub fn offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }
}

pub(crate) async fn list_workspaces_handler(
    State(state): State<Arc<AppState>>,
    Path(tenant_id): Path<String>,
    Query(query): Query<ListWorkspacesQuery>,
) -> impl IntoResponse {
    // #422: honest pagination — fetch `limit + 1`, derive `has_more`.
    let limit = query.limit();
    match state
        .runtime
        .workspaces
        .list_by_tenant(
            &TenantId::new(tenant_id),
            limit + 1,
            query.offset(),
            query.include_archived,
        )
        .await
    {
        Ok(mut items) => {
            let has_more = items.len() > limit;
            items.truncate(limit);
            (StatusCode::OK, Json(ListResponse { items, has_more })).into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

#[utoipa::path(
    delete,
    path = "/v1/admin/tenants/{tenant_id}/workspaces/{workspace_id}",
    tag = "admin",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("workspace_id" = String, Path, description = "Workspace identifier")
    ),
    responses(
        (status = 204, description = "Workspace archived"),
        (status = 401, description = "Unauthorized", body = ApiError),
        (status = 404, description = "Workspace not found for tenant", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
pub(crate) async fn delete_workspace_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Path((tenant_id, workspace_id)): Path<(String, String)>,
) -> impl IntoResponse {
    match state
        .runtime
        .workspaces
        .archive(&TenantId::new(tenant_id), &WorkspaceId::new(workspace_id))
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => runtime_error_response(err),
    }
}

#[utoipa::path(
    post,
    path = "/v1/admin/workspaces/{workspace_id}/projects",
    tag = "admin",
    params(
        ("workspace_id" = String, Path, description = "Workspace identifier")
    ),
    request_body = CreateProjectRequest,
    responses(
        (status = 201, description = "Project created", body = ProjectRecordDoc),
        (status = 400, description = "Invalid request", body = ApiError),
        (status = 401, description = "Unauthorized", body = ApiError),
        (status = 404, description = "Workspace not found", body = ApiError),
        (status = 422, description = "Unprocessable entity", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
pub(crate) async fn create_project_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Path(workspace_id): Path<String>,
    Json(body): Json<CreateProjectRequest>,
) -> impl IntoResponse {
    let workspace_key = match workspace_key_for_id(&state, &WorkspaceId::new(workspace_id)).await {
        Ok(workspace_key) => workspace_key,
        Err(err) => return runtime_error_response(err),
    };

    match state
        .runtime
        .projects
        .create(
            ProjectKey::new(
                workspace_key.tenant_id,
                workspace_key.workspace_id,
                body.project_id,
            ),
            body.name,
        )
        .await
    {
        Ok(record) => (StatusCode::CREATED, Json(record)).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn list_projects_handler(
    State(state): State<Arc<AppState>>,
    Path(workspace_id): Path<String>,
    Query(query): Query<PaginationQuery>,
) -> impl IntoResponse {
    let workspace_id = WorkspaceId::new(workspace_id);
    let workspace = match state.runtime.workspaces.get(&workspace_id).await {
        Ok(Some(workspace)) => workspace,
        Ok(None) => {
            return AppApiError::new(StatusCode::NOT_FOUND, "not_found", "workspace not found")
                .into_response()
        }
        Err(err) => return runtime_error_response(err),
    };

    // #422: honest pagination — fetch `limit + 1`, derive `has_more`.
    let limit = query.limit();
    match state
        .runtime
        .projects
        .list_by_workspace(
            &workspace.tenant_id,
            &workspace.workspace_id,
            limit + 1,
            query.offset(),
        )
        .await
    {
        Ok(mut items) => {
            let has_more = items.len() > limit;
            items.truncate(limit);
            (StatusCode::OK, Json(ListResponse { items, has_more })).into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

// ── Workspace members ────────────────────────────────────────────────────────

pub(crate) async fn add_workspace_member_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Path(workspace_id): Path<String>,
    Json(body): Json<AddWorkspaceMemberRequest>,
) -> impl IntoResponse {
    let workspace_key = match workspace_key_for_id(&state, &WorkspaceId::new(workspace_id)).await {
        Ok(workspace_key) => workspace_key,
        Err(err) => return runtime_error_response(err),
    };

    match state
        .runtime
        .workspace_memberships
        .add_member(workspace_key, body.member_id, body.role)
        .await
    {
        Ok(record) => (StatusCode::CREATED, Json(record)).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn list_workspace_members_handler(
    State(state): State<Arc<AppState>>,
    Path(workspace_id): Path<String>,
    Query(query): Query<PaginationQuery>,
) -> impl IntoResponse {
    let workspace_key = match workspace_key_for_id(&state, &WorkspaceId::new(workspace_id)).await {
        Ok(workspace_key) => workspace_key,
        Err(err) => return runtime_error_response(err),
    };

    // #422: the service returns every member in one shot. Membership
    // per workspace is bounded (typically <100), so pagination happens
    // in-memory after the service call.
    match state
        .runtime
        .workspace_memberships
        .list_members(&workspace_key)
        .await
    {
        Ok(all) => {
            let total = all.len();
            let offset = query.offset();
            let limit = query.limit();
            let items: Vec<_> = all.into_iter().skip(offset).take(limit).collect();
            let has_more = offset.saturating_add(items.len()) < total;
            (StatusCode::OK, Json(ListResponse { items, has_more })).into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn remove_workspace_member_handler(
    State(state): State<Arc<AppState>>,
    Path((workspace_id, member_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let workspace_key = match workspace_key_for_id(&state, &WorkspaceId::new(workspace_id)).await {
        Ok(workspace_key) => workspace_key,
        Err(err) => return runtime_error_response(err),
    };

    match state
        .runtime
        .workspace_memberships
        .remove_member(workspace_key, member_id)
        .await
    {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

// ── Workspace shares ─────────────────────────────────────────────────────────

pub(crate) async fn create_workspace_share_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Path(workspace_id): Path<String>,
    Json(body): Json<CreateShareRequest>,
) -> impl IntoResponse {
    use cairn_runtime::ResourceSharingService;
    let tenant_id = TenantId::new(body.tenant_id.as_deref().unwrap_or(DEFAULT_TENANT_ID));
    match state
        .runtime
        .resource_sharing
        .share(
            tenant_id,
            WorkspaceId::new(workspace_id),
            WorkspaceId::new(body.target_workspace_id),
            body.resource_type,
            body.resource_id,
            body.permissions,
        )
        .await
    {
        Ok(share) => (StatusCode::CREATED, Json(share)).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn list_workspace_shares_handler(
    State(state): State<Arc<AppState>>,
    Path(workspace_id): Path<String>,
    Query(query): Query<TenantScopedQuery>,
) -> impl IntoResponse {
    use cairn_runtime::ResourceSharingService;
    // #422: shares per workspace are bounded (admin-curated), but the
    // service returns the full list. Apply limit/offset in-memory and
    // compute `has_more` against the filtered total.
    let offset = query.offset();
    let limit = query.limit();
    match state
        .runtime
        .resource_sharing
        .list_shares(
            &TenantId::new(query.tenant_id),
            &WorkspaceId::new(workspace_id),
        )
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

pub(crate) async fn revoke_workspace_share_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Path((_workspace_id, share_id)): Path<(String, String)>,
) -> impl IntoResponse {
    use cairn_runtime::ResourceSharingService;
    match state.runtime.resource_sharing.revoke(&share_id).await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

// ── Credentials ──────────────────────────────────────────────────────────────

/// Upper bound for `plaintext_value`. API keys/secrets across
/// supported providers (OpenAI, Anthropic, Bedrock signed tokens,
/// Vertex service accounts JSON) sit well under 4 KiB. A caller that
/// submits more has almost certainly mis-pasted the value — reject
/// early rather than encrypt a 4 KiB garbage string and burn storage.
const MAX_PLAINTEXT_VALUE_LEN: usize = 4096;

pub(crate) async fn store_credential_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Path(tenant_id): Path<String>,
    Json(body): Json<StoreCredentialRequest>,
) -> impl IntoResponse {
    if let Some(denied) = require_feature(&state.config, CREDENTIAL_MANAGEMENT) {
        return denied;
    }
    // Closes #403: validation before any credential work. Empty
    // provider_id / plaintext_value and oversized plaintext_value
    // were previously 201-accepted, leading to a tenant accumulating
    // unusable or suspicious credential rows.
    if body.provider_id.trim().is_empty() {
        return validation_error_response("provider_id must not be empty");
    }
    if body.plaintext_value.is_empty() {
        return validation_error_response("plaintext_value must not be empty");
    }
    if body.plaintext_value.len() > MAX_PLAINTEXT_VALUE_LEN {
        return validation_error_response(format!(
            "plaintext_value exceeds {MAX_PLAINTEXT_VALUE_LEN} bytes (got {})",
            body.plaintext_value.len()
        ));
    }
    let tenant = TenantId::new(tenant_id);
    let provider_id = body.provider_id.clone();
    let key_id = body.key_id.clone();
    // `into_plaintext_value` moves the secret out so it does NOT stay
    // accessible on the `body` stack frame after this call. The service
    // wraps it in `Zeroizing<String>` for the rest of its lifetime.
    let plaintext_value = body.into_plaintext_value();
    match state
        .runtime
        .credentials
        .store(tenant.clone(), provider_id.clone(), plaintext_value, key_id)
        .await
    {
        Ok(record) => (StatusCode::CREATED, Json(credential_summary(record))).into_response(),
        // Closes #217: duplicate `(tenant_id, provider_id)` is a
        // typed conflict from the credential service. Re-shape the
        // generic `Conflict` 409 into the `credential_exists` code so
        // clients can dispatch on it without string-sniffing the
        // message.
        Err(cairn_runtime::RuntimeError::Conflict {
            entity: "credential",
            ..
        }) => AppApiError::new(
            StatusCode::CONFLICT,
            "credential_exists",
            format!(
                "credential for provider '{provider_id}' already exists for tenant '{}'",
                tenant.as_str()
            ),
        )
        .into_response(),
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn list_credentials_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(tenant_id): Path<String>,
    Query(query): Query<PaginationQuery>,
) -> impl IntoResponse {
    // Closes #447: enforce the same tenant/admin scoping that every other
    // tenant-scoped list endpoint uses. A non-admin caller asking for a
    // tenant that is not their own gets a 404 so the endpoint does not
    // leak tenant-id existence.
    let target = TenantId::new(tenant_id);
    if !tenant_scope.is_admin && tenant_scope.tenant_id() != &target {
        return AppApiError::new(StatusCode::NOT_FOUND, "not_found", "credential not found")
            .into_response();
    }
    // #422: honest pagination — fetch `limit + 1`, derive `has_more`.
    let limit = query.limit();
    match state
        .runtime
        .credentials
        .list(&target, limit + 1, query.offset())
        .await
    {
        Ok(records) => {
            let has_more = records.len() > limit;
            let items = records
                .into_iter()
                .take(limit)
                .map(credential_summary)
                .collect::<Vec<_>>();
            (StatusCode::OK, Json(ListResponse { items, has_more })).into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn revoke_credential_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    tenant_scope: TenantScope,
    Path((tenant_id, id)): Path<(String, String)>,
) -> impl IntoResponse {
    // Closes #494: align with the canonical tenant-check shape used by
    // `list_credentials_handler`, `get_session_handler`, `delete_session_admin_handler`,
    // etc. Two layers, both return 404 (NEVER 403) to avoid the
    // id-enumeration oracle documented on PR #337 / PR #537:
    //
    //   1. Path-tenant vs caller-tenant — a caller that crafted a URL
    //      for a foreign tenant gets 404 unless `scope.is_admin` (which
    //      is true only for the admin service account / System
    //      principal; see `is_admin_principal`). Workspace-admin-role
    //      operators PASS `AdminRoleGuard` but do NOT set
    //      `tenant_scope.is_admin`, so they remain tenant-scoped here —
    //      they can revoke only within their own tenant. Only the
    //      admin service account can cross tenants.
    //   2. Record-tenant vs path-tenant — the record's own tenant_id
    //      must match the `:tenant_id` path segment. Protects against
    //      typos in admin tooling and keeps the URL contract honest.
    //
    // Non-admin operators without workspace-admin role are rejected
    // earlier by `AdminRoleGuard` with 403 (role-level refusal).
    let target = TenantId::new(tenant_id);
    if !tenant_scope.is_admin && tenant_scope.tenant_id() != &target {
        return AppApiError::new(StatusCode::NOT_FOUND, "not_found", "credential not found")
            .into_response();
    }

    let credential_id = CredentialId::new(id);
    let existing = match state.runtime.credentials.get(&credential_id).await {
        Ok(Some(record)) => record,
        Ok(None) => {
            return AppApiError::new(StatusCode::NOT_FOUND, "not_found", "credential not found")
                .into_response()
        }
        Err(err) => return runtime_error_response(err),
    };

    if existing.tenant_id != target {
        return AppApiError::new(StatusCode::NOT_FOUND, "not_found", "credential not found")
            .into_response();
    }

    match state.runtime.credentials.revoke(&credential_id).await {
        Ok(record) => (StatusCode::OK, Json(credential_summary(record))).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn rotate_credential_key_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Path(tenant_id): Path<String>,
    Json(body): Json<RotateCredentialKeyRequest>,
) -> impl IntoResponse {
    match state
        .runtime
        .credentials
        .rotate_key(TenantId::new(tenant_id), body.old_key_id, body.new_key_id)
        .await
    {
        Ok(record) => (StatusCode::OK, Json(record)).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

// ── Operator profiles ────────────────────────────────────────────────────────

pub(crate) async fn create_operator_profile_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Path(tenant_id): Path<String>,
    Json(body): Json<CreateOperatorProfileRequest>,
) -> impl IntoResponse {
    match state
        .runtime
        .operator_profiles
        .create(
            TenantId::new(tenant_id),
            body.display_name,
            body.email,
            body.role,
        )
        .await
    {
        Ok(record) => (StatusCode::CREATED, Json(record)).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn list_operator_profiles_handler(
    State(state): State<Arc<AppState>>,
    // Closes #404 negative-path: the /v1/admin/* prefix signals
    // admin-only. Operator profiles are roster-sensitive (operator
    // ids, display names, permissions) — non-admins, including
    // same-tenant operators, must not list.
    _role: AdminRoleGuard,
    Path(tenant_id): Path<String>,
    Query(query): Query<PaginationQuery>,
) -> impl IntoResponse {
    // #422: honest pagination — fetch `limit + 1`, derive `has_more`.
    let limit = query.limit();
    match state
        .runtime
        .operator_profiles
        .list(&TenantId::new(tenant_id), limit + 1, query.offset())
        .await
    {
        Ok(mut items) => {
            let has_more = items.len() > limit;
            items.truncate(limit);
            (StatusCode::OK, Json(ListResponse { items, has_more })).into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

// ── Notifications ────────────────────────────────────────────────────────────

pub(crate) async fn set_operator_notifications_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Path(operator_id): Path<String>,
    Json(body): Json<SetNotificationPreferencesRequest>,
) -> impl IntoResponse {
    // Validate channel targets up front so typos (`not-a-url`) round-trip as
    // an actionable 422, not a latent delivery failure (#235). The dispatcher
    // layer would otherwise happily persist the preference and silently drop
    // every future delivery attempt.
    //
    // #451 / #452: the policy now also governs SSRF — webhooks must not be
    // allowed to target IMDS, RFC 1918, or loopback unless the operator
    // explicitly opts in via `CAIRN_ALLOW_INTERNAL_WEBHOOKS=1` (or Local
    // mode).
    let policy = WebhookValidationPolicy::from_config(&state.config);
    if let Err(msg) = validate_channels(&body.channels, policy).await {
        return validation_error_response(msg);
    }
    let tenant_id = TenantId::new(body.tenant_id.as_deref().unwrap_or(DEFAULT_TENANT_ID));
    match state
        .runtime
        .notifications
        .set_preferences(tenant_id, operator_id, body.event_types, body.channels)
        .await
    {
        Ok(()) => (StatusCode::CREATED, Json(serde_json::json!({ "ok": true }))).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn get_operator_notifications_handler(
    State(state): State<Arc<AppState>>,
    Path(operator_id): Path<String>,
    Query(query): Query<TenantScopedQuery>,
) -> impl IntoResponse {
    let tenant_id = TenantId::new(query.tenant_id);
    match state
        .runtime
        .notifications
        .get_preferences(&tenant_id, &operator_id)
        .await
    {
        Ok(Some(prefs)) => (StatusCode::OK, Json(prefs)).into_response(),
        Ok(None) => {
            // Return an empty preference object instead of 404 so the UI
            // renders an empty-state rather than an error.
            let empty = cairn_domain::notification_prefs::NotificationPreference {
                pref_id: String::new(),
                tenant_id: tenant_id.clone(),
                operator_id: operator_id.clone(),
                event_types: vec![],
                channels: vec![],
            };
            (StatusCode::OK, Json(empty)).into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn list_failed_notifications_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Query(query): Query<PaginationQuery>,
) -> impl IntoResponse {
    // #422: service returns every failed record for the tenant. Apply
    // limit/offset in-memory; this list is naturally small (most
    // tenants have 0-10 failed notifications at a time).
    match state
        .runtime
        .notifications
        .list_failed(tenant_scope.tenant_id())
        .await
    {
        Ok(all) => {
            let total = all.len();
            let offset = query.offset();
            let limit = query.limit();
            let items: Vec<_> = all.into_iter().skip(offset).take(limit).collect();
            let has_more = offset.saturating_add(items.len()) < total;
            (StatusCode::OK, Json(ListResponse { items, has_more })).into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn retry_notification_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(record_id): Path<String>,
) -> impl IntoResponse {
    match state
        .runtime
        .notifications
        .retry(tenant_scope.tenant_id(), &record_id)
        .await
    {
        Ok(record) => (StatusCode::OK, Json(record)).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

// ── Model pricing CRUD ──────────────────────────────────────────────────────

/// `GET /v1/admin/models` — List all model entries in the registry.
pub(crate) async fn list_models_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let entries = state.model_registry.all();
    (StatusCode::OK, Json(entries)).into_response()
}

/// `GET /v1/admin/models/:id` — Get a specific model entry by ID.
pub(crate) async fn get_model_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.model_registry.get(&id) {
        Some(entry) => (StatusCode::OK, Json(entry)).into_response(),
        None => {
            AppApiError::new(StatusCode::NOT_FOUND, "not_found", "model not found").into_response()
        }
    }
}

/// `PUT /v1/admin/models/:id` — Create or update a model entry (operator override).
pub(crate) async fn set_model_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Path(id): Path<String>,
    Json(mut entry): Json<cairn_domain::model_catalog::ModelEntry>,
) -> impl IntoResponse {
    // Ensure the body ID matches the path parameter.
    entry.id = id;
    state.model_registry.register(entry.clone());
    (StatusCode::OK, Json(entry)).into_response()
}

/// `DELETE /v1/admin/models/:id` — Remove a model entry.
pub(crate) async fn delete_model_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.model_registry.unregister(&id) {
        Some(removed) => (StatusCode::OK, Json(removed)).into_response(),
        None => {
            AppApiError::new(StatusCode::NOT_FOUND, "not_found", "model not found").into_response()
        }
    }
}

/// `POST /v1/admin/models/import-litellm` — Import models from LiteLLM JSON body.
///
/// Error contract:
/// - Extractor-level JSON failures return the extractor's native 4xx
///   status (e.g. 400 for syntactically invalid JSON, 415 for a missing
///   / invalid `Content-Type` header) with `{code:"validation_error"}`.
///   Routed through the shared `json_rejection_response` helper so the
///   response shape matches the rest of the admin surface.
/// - 400 (with `{code:"invalid_json"}`) when the payload is syntactically
///   valid JSON but not a top-level object.
/// - 413 Payload Too Large (with `{code:"payload_too_large"}`) when the
///   body exceeds the per-route 1,000,000-byte cap. Same
///   `json_rejection_response` path — `BytesRejection::LengthLimitError`'s
///   native status is `PAYLOAD_TOO_LARGE`.
///
/// #493: the route layers a `DefaultBodyLimit::max(1_000_000)` (1 MB
/// decimal, ≈ 0.95 MiB) overriding the global 10 MB default. LiteLLM's
/// real catalog is O(100 KB); anything near the cap is attacker-crafted.
/// The handler parses the body exactly once via `Json<serde_json::Value>`
/// and passes the inner
/// `Map<String, Value>` straight to `import_litellm_map` — no re-parse
/// and no `HashMap` intermediate.
pub(crate) async fn import_litellm_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    body: Result<Json<serde_json::Value>, JsonRejection>,
) -> impl IntoResponse {
    let Json(value) = match body {
        Ok(v) => v,
        Err(rej) => return json_rejection_response(rej),
    };
    // The LiteLLM format is a top-level JSON object keyed by model id.
    let serde_json::Value::Object(obj) = value else {
        return AppApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_json",
            "request body is not valid LiteLLM JSON (expected a JSON object)",
        )
        .into_response();
    };
    let count = state.model_registry.import_litellm_map(&obj);
    (
        StatusCode::OK,
        Json(serde_json::json!({ "imported": count })),
    )
        .into_response()
}

// ── Waitpoint HMAC rotation ──────────────────────────────────────────────────

/// Request body for `POST /v1/admin/rotate-waitpoint-hmac`.
///
/// `new_kid` must be non-empty and must not contain `:` (FF uses `:` as
/// the separator in the on-disk secret hash).
///
/// `new_secret_hex` must be non-empty, even-length hex. Operators
/// generate it with e.g. `openssl rand -hex 32`.
///
/// `grace_ms` is how long the previously-installed kid stays accepted
/// for verification after rotation. Defaults to 60_000 (1 minute) when
/// unset — enough for in-flight waitpoints to resume without
/// fabricating a verification failure. Zero means instant cutover
/// (can fail in-flight waitpoints signed with the old key).
#[derive(Clone, Debug, serde::Deserialize, ToSchema)]
pub(crate) struct RotateWaitpointHmacRequest {
    pub new_kid: String,
    pub new_secret_hex: String,
    #[serde(default)]
    pub grace_ms: Option<u64>,
}

/// Response body for `POST /v1/admin/rotate-waitpoint-hmac`.
///
/// Returned with HTTP 200 when at least one partition accepted the
/// rotation (or replied `noop` on exact replay). Partial failures list
/// their partition indices + typed FF error code in `failed`; the
/// rotation is idempotent on the same `(new_kid, new_secret_hex)` so
/// a retry after the underlying fault clears converges.
///
/// Returned with HTTP 400 when every partition rejected with the same
/// input-validation code (`invalid_kid`, `invalid_secret_hex`,
/// `invalid_grace_ms`, `rotation_conflict`) — the rotation never had
/// a chance.
///
/// Returned with HTTP 500 when no partition responded (transport
/// failure fan-out).
#[derive(Clone, Debug, serde::Serialize, ToSchema)]
pub(crate) struct RotateWaitpointHmacResponse {
    /// Count of partitions that accepted a fresh rotation.
    pub rotated: u16,
    /// Count of partitions that replied `noop` (exact replay of the
    /// same kid + secret). Idempotent retry path.
    pub noop: u16,
    /// Per-partition failures. Empty on full success.
    pub failed: Vec<RotateWaitpointHmacFailure>,
    /// Echoed back for operator confirmation.
    pub new_kid: String,
}

#[derive(Clone, Debug, serde::Serialize, ToSchema)]
pub(crate) struct RotateWaitpointHmacFailure {
    pub partition_index: u16,
    /// FF typed error code when the Lua envelope returned one;
    /// `null` for transport / timeout failures.
    pub code: Option<String>,
    pub detail: String,
}

/// Default grace window for rotation when the caller omits `grace_ms`.
/// 60 seconds is long enough for a typical in-flight waitpoint to
/// resume under normal load and short enough that an operator can
/// redeploy a new kid and expect full cutover within a few minutes.
const DEFAULT_ROTATE_GRACE_MS: u64 = 60_000;

/// `POST /v1/admin/rotate-waitpoint-hmac` — rotate the waitpoint HMAC
/// signing kid across every execution partition.
///
/// Admin-only. Delegates to `FabricRotationService::rotate_waitpoint_hmac`
/// which fans out the FCALL across all partitions. See the service's
/// rustdoc for idempotency and partial-success semantics.
///
/// Returns 503 when the fabric runtime is not installed (read-only
/// fixture deployments). Production deployments always have `fabric`.
pub(crate) async fn rotate_waitpoint_hmac_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Json(body): Json<RotateWaitpointHmacRequest>,
) -> impl IntoResponse {
    let Some(fabric) = state.fabric.as_ref() else {
        return AppApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "fabric_unavailable",
            "waitpoint HMAC rotation requires the fabric runtime; this deployment has none",
        )
        .into_response();
    };

    let grace_ms = body.grace_ms.unwrap_or(DEFAULT_ROTATE_GRACE_MS);
    let outcome = fabric
        .rotation
        .rotate_waitpoint_hmac(&body.new_kid, &body.new_secret_hex, grace_ms)
        .await;

    // SEC-007 defense-in-depth (Gemini review on PR #540): the fabric
    // `RotationFailure::detail` contract today is an opaque
    // classification hint (`"lua_rejected"`, `"transport_error"`,
    // `"unparseable_envelope"`) and `RotationFailure::code` is a typed
    // FF sentinel. But if the engine-level implementation later drifts
    // and stuffs a raw driver string into `detail`, that would leak
    // through this handler. Allowlist `detail` values to the known
    // classifiers here so the HTTP body cannot surface anything
    // unexpected even if the engine regresses. Raw failure strings
    // are already logged at `debug` level in the engine.
    let failed: Vec<RotateWaitpointHmacFailure> = outcome
        .failed
        .iter()
        .map(|f| RotateWaitpointHmacFailure {
            partition_index: f.partition_index,
            code: f.code.clone(),
            detail: sanitize_rotation_detail(&f.detail),
        })
        .collect();

    let resp = RotateWaitpointHmacResponse {
        rotated: outcome.rotated,
        noop: outcome.noop,
        failed,
        new_kid: outcome.new_kid.clone(),
    };

    // Closes #418: canonical envelope (`status_code`, `code`,
    // `message`, `request_id`) on both failure paths. The per-
    // partition breakdown (rotated/noop/failed counts + per-failure
    // codes) lives under `details` so operators keep the diagnostic
    // richness without the envelope drifting from the shape every SDK
    // parser keys on.
    //
    // All partitions failed with the same Lua-level input-validation
    // code → 400. This is the "operator typo" path (empty kid, bad
    // hex, etc.) and the rotation never did anything useful anywhere.
    if outcome.rotated == 0 && outcome.noop == 0 {
        // Fallback to an empty object (not `null`) when serialization
        // fails — keeps the OpenAPI contract clean for clients that
        // treat `details: null` ambiguously. Serialization of a
        // `RotateWaitpointHmacResponse` cannot actually fail given
        // the derived `Serialize` impl, but the fallback is cheap
        // defence against future refactors.
        let details = serde_json::to_value(&resp).unwrap_or_else(|_| serde_json::json!({}));
        if let Some(code) = unanimous_input_error_code(&outcome.failed) {
            return api_error_with_details(
                StatusCode::BAD_REQUEST,
                code,
                "rotation rejected by every partition",
                details,
            );
        }
        // Every partition failed but not with a unanimous input code →
        // transport or mixed failure. 500 so the operator sees this
        // as a service fault rather than a validation issue.
        return api_error_with_details(
            StatusCode::INTERNAL_SERVER_ERROR,
            "rotation_failed",
            "rotation failed on every partition",
            details,
        );
    }

    // Any success (rotated or noop) → 200 with the full outcome
    // breakdown. Partial failure is visible in `failed[]` but does
    // not demote the status — the rotation is idempotent and
    // operators retry with the same (new_kid, new_secret_hex) to
    // converge.
    (StatusCode::OK, Json(resp)).into_response()
}

/// SEC-007 defense-in-depth: map a fabric `RotationFailure::detail`
/// string to the allowlisted classification sentinel the HTTP body
/// may carry. Unknown values collapse to `"unspecified"` so any
/// future engine regression that stuffs a raw driver string into
/// `detail` cannot leak through the response body.
///
/// The allowlist mirrors the `ROTATION_DETAIL_*` constants the
/// `valkey_control_plane_impl` engine emits today. Any new classifier
/// added upstream must be added here explicitly — a deliberate
/// breakage surface so review catches the envelope-drift.
fn sanitize_rotation_detail(raw: &str) -> String {
    const ALLOWED: &[&str] = &["lua_rejected", "transport_error", "unparseable_envelope"];
    if ALLOWED.contains(&raw) {
        raw.to_owned()
    } else {
        // Log the drift so operators see the upstream contract has
        // changed; return a safe sentinel to the caller.
        tracing::warn!(
            raw_detail = %raw,
            "unexpected rotation failure detail — falling back to `unspecified` for SEC-007"
        );
        "unspecified".to_owned()
    }
}

/// If every partition failed with the same FF input-validation code,
/// return it. These are Lua-level rejects that mean the rotation
/// never had a chance and the operator needs to fix the input before
/// retry. `rotation_conflict` is included because it blocks the
/// rotation completely (operator must pick a fresh kid).
fn unanimous_input_error_code(
    failures: &[cairn_fabric::services::RotationFailure],
) -> Option<String> {
    const INPUT_CODES: &[&str] = &[
        "invalid_kid",
        "invalid_secret_hex",
        "invalid_grace_ms",
        "rotation_conflict",
    ];
    let first = failures.first()?.code.as_deref()?;
    if !INPUT_CODES.contains(&first) {
        return None;
    }
    if failures.iter().all(|f| f.code.as_deref() == Some(first)) {
        Some(first.to_owned())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{sanitize_rotation_detail, StoreCredentialRequest};

    /// SEC-007 defense-in-depth (Gemini review on PR #540):
    /// `sanitize_rotation_detail` must pass through only the allowlist
    /// of classification sentinels and collapse anything else to the
    /// opaque `unspecified` sentinel — including what would be a raw
    /// driver string if the upstream engine ever regressed.
    #[test]
    fn rotation_detail_sanitizer_passes_known_classifiers_and_redacts_unknown() {
        // Known allowlist — must pass through verbatim.
        for ok in ["lua_rejected", "transport_error", "unparseable_envelope"] {
            assert_eq!(
                sanitize_rotation_detail(ok),
                ok,
                "allowlisted classifier must pass through: {ok}"
            );
        }

        // Simulated engine-regression values — must collapse to
        // `unspecified` so the caller's body never surfaces raw driver
        // content.
        for leaky in [
            "connection refused: host=internal-db.prod.example.com port=5432",
            "NOSCRIPT No matching script. SHA1=abcdef...",
            "FCALL args=['cairn.lease_fence=0xDEADBEEF']",
            "", // empty detail still collapses — contract says allowlist-only.
            "timeout waiting for cluster-reply from cairn-valkey-prod:7001",
        ] {
            assert_eq!(
                sanitize_rotation_detail(leaky),
                "unspecified",
                "non-allowlist input must collapse to `unspecified`: {leaky}"
            );
        }
    }

    /// Closes #492: `{body:?}` must not leak the plaintext. The handler
    /// does not print the body today, but `StoreCredentialRequest` is
    /// derived-Clone and public within the crate, so any future
    /// `tracing::debug!("body = {body:?}")` MUST redact. This pins the
    /// Debug shape so the invariant can't silently regress.
    #[test]
    fn store_credential_request_debug_redacts_plaintext() {
        let body = StoreCredentialRequest {
            provider_id: "openai".to_owned(),
            plaintext_value: "sk-SHOULD-BE-REDACTED-9f8e7d".to_owned(),
            key_id: Some("primary".to_owned()),
        };
        let formatted = format!("{body:?}");
        assert!(
            !formatted.contains("sk-SHOULD-BE-REDACTED"),
            "Debug leaked plaintext: {formatted}"
        );
        assert!(
            formatted.contains("[redacted]"),
            "Debug must mark plaintext_value as [redacted]; got: {formatted}"
        );
        assert!(
            formatted.contains("openai"),
            "non-secret fields should still be visible: {formatted}"
        );
        assert!(
            formatted.contains("primary"),
            "key_id should still be visible: {formatted}"
        );
    }
}
