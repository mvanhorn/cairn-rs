//! HTTP request extractors and scope guards.

use axum::extract::{FromRequest, FromRequestParts, Query, Request};
use axum::http::request::Parts;
use axum::response::IntoResponse;
use axum::Json;
use serde::de::DeserializeOwned;

use cairn_api::auth::AuthPrincipal;
use cairn_api::endpoints::ListQuery;
use cairn_api::memory_api::MemorySearchQuery;
use cairn_domain::tenancy::TenantRole;
use cairn_domain::{ProjectKey, TenantId, WorkspaceRole};

use crate::errors::{
    forbidden_api_error, query_rejection_error, tenant_role_missing_response,
    tenant_scope_mismatch_error, unauthorized_api_error, AppApiError,
};
use crate::{DEFAULT_PROJECT_ID, DEFAULT_TENANT_ID, DEFAULT_WORKSPACE_ID};

// ── Query structs ──────────────────────────────────────────────────────────

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct ProjectScopedQuery {
    pub(crate) tenant_id: String,
    pub(crate) workspace_id: String,
    pub(crate) project_id: String,
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub(crate) struct OptionalProjectScopedQuery {
    pub(crate) tenant_id: Option<String>,
    pub(crate) workspace_id: Option<String>,
    pub(crate) project_id: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
}

impl OptionalProjectScopedQuery {
    pub(crate) fn project(&self) -> ProjectKey {
        ProjectKey::new(
            self.tenant_id.as_deref().unwrap_or(DEFAULT_TENANT_ID),
            self.workspace_id.as_deref().unwrap_or(DEFAULT_WORKSPACE_ID),
            self.project_id.as_deref().unwrap_or(DEFAULT_PROJECT_ID),
        )
    }

    pub(crate) fn limit(&self) -> usize {
        self.limit.unwrap_or(100)
    }

    pub(crate) fn offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub(crate) struct PreservedMemoryListQuery {
    pub(crate) tenant_id: Option<String>,
    pub(crate) workspace_id: Option<String>,
    pub(crate) project_id: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
    pub(crate) status: Option<String>,
    pub(crate) category: Option<String>,
}

impl PreservedMemoryListQuery {
    pub(crate) fn project(&self) -> ProjectKey {
        ProjectKey::new(
            self.tenant_id.as_deref().unwrap_or(DEFAULT_TENANT_ID),
            self.workspace_id.as_deref().unwrap_or(DEFAULT_WORKSPACE_ID),
            self.project_id.as_deref().unwrap_or(DEFAULT_PROJECT_ID),
        )
    }

    pub(crate) fn list_query(&self) -> ListQuery {
        ListQuery {
            limit: self.limit,
            offset: self.offset,
            status: self.status.clone(),
            category: self.category.clone(),
        }
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct PreservedMemorySearchParams {
    pub(crate) q: String,
    pub(crate) limit: Option<usize>,
    pub(crate) tenant_id: Option<String>,
    pub(crate) workspace_id: Option<String>,
    pub(crate) project_id: Option<String>,
}

impl PreservedMemorySearchParams {
    pub(crate) fn project(&self) -> ProjectKey {
        ProjectKey::new(
            self.tenant_id.as_deref().unwrap_or(DEFAULT_TENANT_ID),
            self.workspace_id.as_deref().unwrap_or(DEFAULT_WORKSPACE_ID),
            self.project_id.as_deref().unwrap_or(DEFAULT_PROJECT_ID),
        )
    }

    pub(crate) fn search_query(&self) -> MemorySearchQuery {
        MemorySearchQuery {
            q: self.q.clone(),
            limit: self.limit,
        }
    }
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub(crate) struct TenantCostQuery {
    pub(crate) since_ms: Option<u64>,
    /// Per-page row cap. Defaults to 200; capped at 1 000 to bound
    /// worst-case response size (#423). Previously the handler returned
    /// every session cost row for the tenant in one payload — tenants
    /// with long run histories could produce 100k+-row responses, an
    /// OOM and latency hazard the store's own READ path couldn't
    /// protect against.
    pub(crate) limit: Option<usize>,
    /// Zero-based offset into the tenant's session-cost list, newest
    /// first.
    pub(crate) offset: Option<usize>,
}

impl TenantCostQuery {
    /// Default page size. Matches the pattern used by other list
    /// endpoints that truly need bulk reads (audit logs, traces).
    pub(crate) const DEFAULT_LIMIT: usize = 200;

    /// Maximum per-page cap (#423). Requests exceeding this are
    /// clamped silently — the response's `has_more` flag is still
    /// truthful so clients can keep paging.
    pub(crate) const MAX_LIMIT: usize = 1_000;

    pub(crate) fn limit(&self) -> usize {
        self.limit
            .unwrap_or(Self::DEFAULT_LIMIT)
            .min(Self::MAX_LIMIT)
    }

    pub(crate) fn offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }
}

impl ProjectScopedQuery {
    pub(crate) fn project(&self) -> ProjectKey {
        ProjectKey::new(
            self.tenant_id.as_str(),
            self.workspace_id.as_str(),
            self.project_id.as_str(),
        )
    }

    pub(crate) fn limit(&self) -> usize {
        self.limit.unwrap_or(100)
    }

    pub(crate) fn offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }
}

// ── HasProjectScope trait ──────────────────────────────────────────────────

pub(crate) trait HasProjectScope {
    fn project(&self) -> ProjectKey;
}

impl HasProjectScope for OptionalProjectScopedQuery {
    fn project(&self) -> ProjectKey {
        Self::project(self)
    }
}

impl HasProjectScope for PreservedMemoryListQuery {
    fn project(&self) -> ProjectKey {
        Self::project(self)
    }
}

impl HasProjectScope for PreservedMemorySearchParams {
    fn project(&self) -> ProjectKey {
        Self::project(self)
    }
}

// ── Scope types ────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct TenantScope {
    pub tenant_id: TenantId,
    /// `true` when the request was authenticated with the admin service account.
    /// Admin tokens bypass per-tenant scope checks so they can access any tenant.
    pub is_admin: bool,
}

impl TenantScope {
    pub fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }
}

pub struct WorkspaceRoleGuard<const MIN_ROLE: u8>;
#[allow(dead_code)]
pub(crate) type MemberRoleGuard = WorkspaceRoleGuard<1>;
pub(crate) type ReviewerRoleGuard = WorkspaceRoleGuard<2>;
pub type AdminRoleGuard = WorkspaceRoleGuard<3>;

#[derive(Clone, Debug)]
pub(crate) struct ProjectScope<T> {
    pub(crate) tenant: TenantScope,
    #[allow(dead_code)]
    pub(crate) project: ProjectKey,
    pub(crate) value: T,
}

impl<T> ProjectScope<T> {
    #[allow(dead_code)]
    pub(crate) fn project(&self) -> &ProjectKey {
        &self.project
    }

    pub(crate) fn into_inner(self) -> T {
        self.value
    }

    #[allow(dead_code)]
    pub(crate) fn tenant_scope(&self) -> &TenantScope {
        &self.tenant
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ProjectJson<T> {
    pub(crate) tenant: TenantScope,
    #[allow(dead_code)]
    pub(crate) project: ProjectKey,
    pub(crate) value: T,
}

impl<T> ProjectJson<T> {
    #[allow(dead_code)]
    pub(crate) fn project(&self) -> &ProjectKey {
        &self.project
    }

    pub(crate) fn into_inner(self) -> T {
        self.value
    }

    #[allow(dead_code)]
    pub(crate) fn tenant_scope(&self) -> &TenantScope {
        &self.tenant
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

pub(crate) fn validate_project_scope<T: HasProjectScope>(
    tenant: TenantScope,
    value: T,
) -> Result<(TenantScope, ProjectKey, T), AppApiError> {
    let project = value.project();
    // Admin tokens have cross-tenant access — skip the scope check.
    if !tenant.is_admin && project.tenant_id != *tenant.tenant_id() {
        return Err(tenant_scope_mismatch_error());
    }

    Ok((tenant, project, value))
}

/// `true` for the bootstrap admin service account or the System principal.
/// T6b-C5 (shared helper): returns true when the path's project belongs
/// to the caller's tenant OR the caller is an admin. Used by
/// `marketplace_routes` and `repo_routes` to refuse cross-tenant
/// mutations; hoisted here to avoid divergent copies.
pub(crate) fn enforce_project_tenant(
    principal: &AuthPrincipal,
    project: &cairn_domain::tenancy::ProjectKey,
) -> bool {
    if is_admin_principal(principal) {
        return true;
    }
    principal
        .tenant()
        .map(|t| t.tenant_id == project.tenant_id)
        .unwrap_or(false)
}

pub fn is_admin_principal(principal: &AuthPrincipal) -> bool {
    match principal {
        AuthPrincipal::System => true,
        AuthPrincipal::ServiceAccount { name, .. } => name == "admin",
        AuthPrincipal::Operator { .. } => false,
    }
}

// ── FromRequestParts / FromRequest impls ───────────────────────────────────

#[axum::async_trait]
impl<S> FromRequestParts<S> for TenantScope
where
    S: Send + Sync,
{
    type Rejection = AppApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let tenant_id = parts
            .extensions
            .get::<TenantId>()
            .cloned()
            .ok_or_else(unauthorized_api_error)?;
        // Admin service account bypasses per-tenant scope checks.
        let is_admin = parts
            .extensions
            .get::<AuthPrincipal>()
            .map(is_admin_principal)
            .unwrap_or(false);
        Ok(Self {
            tenant_id,
            is_admin,
        })
    }
}

#[axum::async_trait]
impl<S, T> FromRequestParts<S> for ProjectScope<T>
where
    S: Send + Sync,
    T: HasProjectScope + DeserializeOwned + Send,
{
    type Rejection = AppApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let tenant = TenantScope::from_request_parts(parts, state).await?;
        let Query(value) = Query::<T>::from_request_parts(parts, state)
            .await
            .map_err(|err| query_rejection_error(err.to_string()))?;
        let (tenant, project, value) = validate_project_scope(tenant, value)?;
        Ok(Self {
            tenant,
            project,
            value,
        })
    }
}

#[axum::async_trait]
impl<S, T> FromRequest<S> for ProjectJson<T>
where
    S: Send + Sync,
    T: HasProjectScope + DeserializeOwned + Send,
{
    type Rejection = AppApiError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let is_admin = request
            .extensions()
            .get::<AuthPrincipal>()
            .map(is_admin_principal)
            .unwrap_or(false);
        let tenant = request
            .extensions()
            .get::<TenantId>()
            .cloned()
            .map(|tenant_id| TenantScope {
                tenant_id,
                is_admin,
            })
            .ok_or_else(unauthorized_api_error)?;
        let Json(value) = Json::<T>::from_request(request, state)
            .await
            .map_err(|err| query_rejection_error(err.body_text()))?;
        let (tenant, project, value) = validate_project_scope(tenant, value)?;
        Ok(Self {
            tenant,
            project,
            value,
        })
    }
}

#[axum::async_trait]
impl<S, const MIN_ROLE: u8> FromRequestParts<S> for WorkspaceRoleGuard<MIN_ROLE>
where
    S: Send + Sync,
{
    type Rejection = AppApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // T6b-C4 fail-closed: the prior "no workspace role attached →
        // treat as unrestricted" branch was a blanket bypass. Admin
        // endpoints must refuse requests from principals that haven't
        // had a role attached, UNLESS the principal is System / the
        // admin service account (these carry `is_admin_principal ==
        // true` and don't need a workspace-role binding to pass).
        if let Some(principal) = parts.extensions.get::<AuthPrincipal>() {
            if is_admin_principal(principal) {
                return Ok(Self);
            }
        }
        let Some(role) = parts.extensions.get::<WorkspaceRole>().copied() else {
            return Err(forbidden_api_error(
                "workspace role not attached; refusing privileged request",
            ));
        };
        if (role as u8) < MIN_ROLE {
            return Err(forbidden_api_error("insufficient workspace role"));
        }
        Ok(Self)
    }
}

// ── TenantAdminGuard (RFC 026 PR-A0) ───────────────────────────────────────

/// The request's TARGET tenant id — parsed from the URL path by
/// `crate::middleware::attach_tenant_role`. Distinct from the
/// authenticated principal's home `TenantId` extension so the two
/// don't clobber each other on admin-path requests.
///
/// Populated only for paths matching `/v1/admin/tenants/:tenant_id/*`
/// or `/v1/admin/operators/:id/tenant-roles/:tenant/*`.
#[derive(Clone, Debug)]
pub struct TargetTenantId(pub TenantId);

/// Extractor that admits god-token principals **or** operators holding
/// `TenantRole::Admin` on the target tenant.
///
/// Attached earlier in the pipeline by
/// [`crate::middleware::attach_tenant_role`], which parses the path's
/// target tenant id and inserts the operator's active TenantRole into
/// request extensions. System / admin-service-account principals
/// short-circuit via [`is_admin_principal`] — they keep cross-tenant
/// access for backward compatibility with existing `AdminRoleGuard`
/// usage sites.
///
/// Rejection body on a real-operator 403 is intentionally non-canonical
/// (see [`tenant_role_missing_response`]): the UI `<AdminGate>` wrapper
/// parses `error_code == "tenant_role_missing"` to distinguish an
/// upgrade-regression from a deliberate role gap.
pub struct TenantAdminGuard;

#[axum::async_trait]
impl<S> FromRequestParts<S> for TenantAdminGuard
where
    S: Send + Sync,
{
    type Rejection = axum::response::Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let principal = parts
            .extensions
            .get::<AuthPrincipal>()
            .cloned()
            .ok_or_else(|| unauthorized_api_error().into_response())?;

        // God-token backward compat — deployment-global admin service
        // account and System principals pass without a tenant-role
        // check. This preserves every pre-PR-A0 admin workflow.
        if is_admin_principal(&principal) {
            return Ok(Self);
        }

        // Real operator: middleware attached a `TenantRole` extension
        // only when the operator has an active grant on the target
        // tenant. Absence of the extension means no grant; Admin is
        // the only role that clears this guard.
        match parts.extensions.get::<TenantRole>().copied() {
            Some(role) if role.is_admin() => Ok(Self),
            Some(_) | None => {
                // Produce the structured `tenant_role_missing` body so
                // operators upgrading from pre-A0 main get an actionable
                // hint instead of a bare 403.
                //
                // Prefer the path's target tenant id (`TargetTenantId`,
                // attached by `middleware::attach_tenant_role`) over
                // the authenticated principal's home tenant — the body
                // must reflect what the operator REQUESTED, not their
                // home. Fall back to the principal's tenant when the
                // path didn't carry a target (e.g. a non-admin path
                // that somehow reached this guard).
                let tenant_id = parts
                    .extensions
                    .get::<TargetTenantId>()
                    .map(|t| t.0.as_str().to_owned())
                    .or_else(|| {
                        parts
                            .extensions
                            .get::<TenantId>()
                            .map(|t| t.as_str().to_owned())
                    })
                    .unwrap_or_default();
                let operator_id = match &principal {
                    AuthPrincipal::Operator { operator_id, .. } => operator_id.as_str().to_owned(),
                    AuthPrincipal::ServiceAccount { name, .. } => name.clone(),
                    AuthPrincipal::System => String::new(),
                };
                Err(tenant_role_missing_response(&tenant_id, &operator_id))
            }
        }
    }
}
