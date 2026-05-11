//! RFC 031 PR-B: `/v1/projects/:project/agent-roles` HTTP surface.
//!
//! Five endpoints:
//!   GET    /v1/projects/:project/agent-roles[?source=…]
//!   GET    /v1/projects/:project/agent-roles/:id
//!   POST   /v1/projects/:project/agent-roles
//!   PATCH  /v1/projects/:project/agent-roles/:id
//!   DELETE /v1/projects/:project/agent-roles/:id
//!
//! Writes require `AdminRoleGuard`; reads only require an
//! authenticated tenant-scoped principal. Body-size cap is 128 KiB
//! (§D4 total-body ceiling) — enforced via `DefaultBodyLimit` on the
//! router routes. Structural validation (§Prompt Engineering Contract)
//! runs pre-persist; a validation failure returns 422 with the full
//! failure list.
//!
//! §D13 prompt normalisation (BOM strip / CRLF→LF / final-line
//! trailing-whitespace trim) runs **pre-validation** so the validator
//! sees the same bytes the projection eventually stores.

#![allow(clippy::result_large_err)] // axum Response is the natural error shape for helpers

use std::sync::Arc;

use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

use cairn_api::auth::AuthPrincipal;
use cairn_domain::agent_roles::{AgentRole, AgentRoleTier, ResponseShape};
use cairn_domain::agent_roles_validation::{
    validate_prompt_structure, FailureCode, ValidationFailure, DESCRIPTION_MAX_BYTES,
    NAME_MAX_CHARS, SYSTEM_PROMPT_MAX_BYTES, TOOLS_MAX_ENTRIES,
};
use cairn_domain::ProjectKey;
use cairn_runtime::services::{AgentRoleService, ResolvedRole, RoleSource, SourceFilter};
use cairn_store::projections::{AgentRoleReadModel, AgentRoleRecord};
use cairn_store::EventLog;

use crate::errors::{
    api_error_with_details, json_rejection_response, runtime_error_response, store_error_response,
    AppApiError,
};
use crate::extractors::{enforce_project_tenant, AdminRoleGuard};
use crate::marketplace_routes::{operator_id_from_principal, project_key_from_path};
use crate::AppState;

// ── Wire shapes ─────────────────────────────────────────────────────────────

/// Same shape as `ResolvedRole` but Deserialize too, so tests can round-trip
/// the envelope. We can't derive Deserialize on the runtime type because its
/// `RoleSource` variant serialisation is closed.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct ResolvedRoleWire<'a> {
    pub(crate) role: &'a AgentRole,
    pub(crate) source: &'static str,
    pub(crate) shadows_builtin: Option<&'a str>,
    pub(crate) defined_at: Option<u64>,
    pub(crate) defined_by: Option<&'a str>,
}

fn role_source_str(src: RoleSource) -> &'static str {
    match src {
        RoleSource::Builtin => "builtin",
        RoleSource::Custom => "custom",
        RoleSource::CustomShadow => "custom_shadow",
    }
}

fn resolved_to_wire(r: &ResolvedRole) -> ResolvedRoleWire<'_> {
    ResolvedRoleWire {
        role: &r.role,
        source: role_source_str(r.source),
        shadows_builtin: r.shadows_builtin.as_deref(),
        defined_at: r.defined_at,
        defined_by: r.defined_by.as_ref().map(|o| o.as_str()),
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ListResponse<'a> {
    pub(crate) items: Vec<ResolvedRoleWire<'a>>,
    pub(crate) total: usize,
    pub(crate) has_more: bool,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct Advisory {
    pub(crate) code: String,
    pub(crate) message: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct DefineResponse<'a> {
    pub(crate) role: &'a AgentRole,
    pub(crate) source: &'static str,
    pub(crate) shadows_builtin: Option<&'a str>,
    pub(crate) defined_at: u64,
    pub(crate) defined_by: &'a str,
    pub(crate) warnings: Vec<Advisory>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct RetractResponse {
    pub(crate) role_id: String,
    pub(crate) retracted_at: u64,
    pub(crate) retracted_by: String,
    pub(crate) warnings: Vec<Advisory>,
}

// ── Request bodies ──────────────────────────────────────────────────────────

/// POST body (§HTTP Surface Delta).
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct CreateAgentRoleRequest {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) tier: AgentRoleTier,
    #[serde(default)]
    pub(crate) description: String,
    pub(crate) system_prompt: String,
    #[serde(default)]
    pub(crate) tools: Vec<String>,
    #[serde(default)]
    pub(crate) forbid_all_tools: bool,
    #[serde(default)]
    pub(crate) max_context_tokens: Option<u32>,
    #[serde(default)]
    pub(crate) response_shape: Option<ResponseShape>,
}

impl CreateAgentRoleRequest {
    fn into_role(self) -> AgentRole {
        AgentRole {
            role_id: self.id,
            display_name: self.name,
            description: self.description,
            system_prompt: Some(self.system_prompt),
            tools: self.tools,
            forbid_all_tools: self.forbid_all_tools,
            max_context_tokens: self.max_context_tokens,
            tier: self.tier,
            response_shape: self.response_shape.unwrap_or_default(),
        }
    }
}

/// PATCH body — JSON Merge Patch over `AgentRole` fields.
///
/// `id` and `tier` are immutable per §PATCH semantics. When present,
/// the handler returns 422 `ImmutableField` regardless of their value
/// (this covers both "different id" and "same id as path" cases — the
/// rule is "don't put `id`/`tier` in the PATCH body at all").
#[derive(Clone, Debug, Deserialize, Default)]
pub(crate) struct PatchAgentRoleRequest {
    #[serde(default)]
    pub(crate) id: Option<String>,
    #[serde(default)]
    pub(crate) tier: Option<AgentRoleTier>,
    #[serde(default)]
    pub(crate) name: Option<String>,
    #[serde(default)]
    pub(crate) description: Option<String>,
    #[serde(default)]
    pub(crate) system_prompt: Option<String>,
    #[serde(default)]
    pub(crate) tools: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) forbid_all_tools: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_optional_field")]
    pub(crate) max_context_tokens: Option<Option<u32>>,
    #[serde(default)]
    pub(crate) response_shape: Option<ResponseShape>,
}

/// Distinguish "field missing" from "field present and null" — the
/// PATCH body may explicitly null out `max_context_tokens`.
fn deserialize_optional_field<'de, T, D>(d: D) -> Result<Option<Option<T>>, D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Deserialize::deserialize(d).map(Some)
}

#[derive(Clone, Debug, Deserialize, Default)]
pub(crate) struct ListQuery {
    #[serde(default)]
    pub(crate) source: Option<String>,
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn bad_request(message: impl Into<String>) -> Response {
    AppApiError::new(StatusCode::BAD_REQUEST, "invalid_request", message).into_response()
}

fn not_found(message: impl Into<String>) -> Response {
    AppApiError::new(StatusCode::NOT_FOUND, "not_found", message).into_response()
}

fn payload_too_large(message: impl Into<String>) -> Response {
    AppApiError::new(StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large", message).into_response()
}

fn precondition_failed(message: impl Into<String>) -> Response {
    AppApiError::new(
        StatusCode::PRECONDITION_FAILED,
        "precondition_failed",
        message,
    )
    .into_response()
}

fn conflict(message: impl Into<String>) -> Response {
    AppApiError::new(StatusCode::CONFLICT, "conflict", message).into_response()
}

/// Structural-validation failures → 422 body with `details.failures[]`.
fn validation_failure_response(failures: Vec<ValidationFailure>) -> Response {
    api_error_with_details(
        StatusCode::UNPROCESSABLE_ENTITY,
        "validation_failed",
        "agent role failed structural validation",
        serde_json::json!({ "failures": failures }),
    )
}

/// Resolve the project path segment to a `ProjectKey` and enforce the
/// tenant scope on the principal. Returns either the key or an error
/// response ready to return.
fn resolve_project(principal: &AuthPrincipal, project: &str) -> Result<ProjectKey, Response> {
    let project = project_key_from_path(project).map_err(bad_request)?;
    if !enforce_project_tenant(principal, &project) {
        return Err(crate::errors::tenant_scope_mismatch_error().into_response());
    }
    Ok(project)
}

/// §D13 prompt normalisation: BOM strip + CRLF→LF + per-line
/// trailing-whitespace trim.
///
/// The RFC specifies "final-line trailing-whitespace trim"; in practice
/// per-line stripping is strictly a superset (includes the final line)
/// and gives consistent tokenisation for multi-line prompts. The
/// validator sees the normalised bytes and the projection stores them.
fn normalise_prompt(raw: &str) -> String {
    let without_bom = raw.strip_prefix('\u{FEFF}').unwrap_or(raw);
    let converted = without_bom.replace("\r\n", "\n");
    let mut out = converted
        .lines()
        .map(|line| line.trim_end())
        .collect::<Vec<_>>()
        .join("\n");
    // `lines()` drops a trailing newline; preserve it when the raw
    // prompt ended with one so POST→GET round-trips are stable.
    if converted.ends_with('\n') {
        out.push('\n');
    }
    out
}

/// §D4 per-field caps. Returns 413 body text on overflow.
fn check_field_caps(role: &AgentRole) -> Result<(), String> {
    if role.display_name.chars().count() > NAME_MAX_CHARS {
        return Err(format!("name exceeds {NAME_MAX_CHARS} char limit"));
    }
    if role.description.len() > DESCRIPTION_MAX_BYTES {
        return Err(format!(
            "description exceeds {DESCRIPTION_MAX_BYTES} byte limit"
        ));
    }
    if role.tools.len() > TOOLS_MAX_ENTRIES {
        return Err(format!("tools[] exceeds {TOOLS_MAX_ENTRIES} entry limit"));
    }
    if let Some(prompt) = role.system_prompt.as_deref() {
        if prompt.len() > SYSTEM_PROMPT_MAX_BYTES {
            return Err(format!(
                "system_prompt exceeds {SYSTEM_PROMPT_MAX_BYTES} byte limit"
            ));
        }
    }
    Ok(())
}

fn shadow_warning(role_id: &str) -> Option<Advisory> {
    let (code, message) = match role_id {
        "orchestrator" => (
            "shadow_warn_orchestrator",
            "shadowing the built-in orchestrator — verify response_shape and base-prompt exemption",
        ),
        "reviewer" => (
            "shadow_warn_reviewer",
            "shadowing the built-in reviewer — ensure citation-backed review mandate is preserved",
        ),
        "executor" => (
            "shadow_warn_executor",
            "shadowing the built-in executor — verify the 5-phase workflow is present",
        ),
        "researcher" => (
            "shadow_warn_researcher",
            "shadowing the built-in researcher — verify evidence-citation mandate",
        ),
        "generic" => (
            "shadow_warn_generic",
            "shadowing the built-in generic role — this is the fallback for unknown role ids; verify deliberate intent",
        ),
        _ => return None,
    };
    Some(Advisory {
        code: code.to_owned(),
        message: message.to_owned(),
    })
}

fn etag_header(defined_at: u64) -> HeaderMap {
    let mut headers = HeaderMap::new();
    // Quoted opaque-tag per RFC 7232 §2.3.
    if let Ok(v) = HeaderValue::try_from(format!("\"{defined_at}\"")) {
        headers.insert(header::ETAG, v);
    }
    headers
}

fn parse_source_filter(raw: Option<&str>) -> Result<SourceFilter, Response> {
    match raw {
        None | Some("") | Some("all") => Ok(SourceFilter::All),
        Some("builtin") => Ok(SourceFilter::Builtin),
        Some("custom") => Ok(SourceFilter::AnyCustom),
        Some("custom_shadow") => Ok(SourceFilter::CustomShadow),
        Some(other) => Err(bad_request(format!(
            "source must be one of all|builtin|custom|custom_shadow, got `{other}`"
        ))),
    }
}

/// Extract and parse the `If-Match` ETag value; `None` means the
/// header was absent. `Some(Err(…))` means the header was present but
/// malformed (caller treats that as 412 per RFC 7232 §3.1 — "not
/// matching").
fn parse_if_match(headers: &HeaderMap) -> Option<Result<u64, ()>> {
    let raw = headers.get(header::IF_MATCH)?;
    let s = raw.to_str().ok()?.trim();
    // Accept both quoted (`"1730…"`) and bare forms for leniency.
    let unquoted = s.trim_start_matches('"').trim_end_matches('"');
    match unquoted.parse::<u64>() {
        Ok(v) => Some(Ok(v)),
        Err(_) => Some(Err(())),
    }
}

// ── Handlers ─────────────────────────────────────────────────────────────────

/// `GET /v1/projects/:project/agent-roles[?source=…]`.
///
/// Reader role — any authenticated operator with tenant-scope access.
/// Returns the merged list of built-ins + project customs, filtered
/// by `?source=`. No pagination (bounded set per project per §GET
/// list response).
pub(crate) async fn list_agent_roles_handler(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Path(project_raw): Path<String>,
    Query(query): Query<ListQuery>,
) -> Response {
    let project = match resolve_project(&principal, &project_raw) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    let filter = match parse_source_filter(query.source.as_deref()) {
        Ok(f) => f,
        Err(resp) => return resp,
    };

    let items = match state.runtime.agent_roles.list(&project, filter).await {
        Ok(items) => items,
        Err(err) => return runtime_error_response(err),
    };
    let wire_items: Vec<ResolvedRoleWire<'_>> = items.iter().map(resolved_to_wire).collect();
    let total = wire_items.len();
    let body = ListResponse {
        items: wire_items,
        total,
        has_more: false,
    };
    (StatusCode::OK, Json(body)).into_response()
}

/// `GET /v1/projects/:project/agent-roles/:id` — single role fetch.
///
/// 200 + ETag when an active row or a matching built-in exists; 404
/// when the id is unknown both in the projection and in
/// `default_roles()`. The response body is the same envelope the
/// list endpoint emits per item.
pub(crate) async fn get_agent_role_handler(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Path((project_raw, role_id)): Path<(String, String)>,
) -> Response {
    let project = match resolve_project(&principal, &project_raw) {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    // Active custom first; fall back to built-in.
    match AgentRoleReadModel::get_active(state.runtime.store.as_ref(), &project, &role_id).await {
        Ok(Some(row)) => {
            let defined_at = row.defined_at;
            let wire = ResolvedRoleWire {
                role: &row.role,
                source: if row.shadows_builtin.is_some() {
                    "custom_shadow"
                } else {
                    "custom"
                },
                shadows_builtin: row.shadows_builtin.as_deref(),
                defined_at: Some(defined_at),
                defined_by: Some(row.defined_by.as_str()),
            };
            let mut resp = (StatusCode::OK, Json(wire)).into_response();
            resp.headers_mut().extend(etag_header(defined_at));
            resp
        }
        Ok(None) => {
            let builtins = cairn_domain::agent_roles::default_roles();
            if let Some(builtin) = builtins.into_iter().find(|r| r.role_id == role_id) {
                let wire = ResolvedRoleWire {
                    role: &builtin,
                    source: "builtin",
                    shadows_builtin: None,
                    defined_at: None,
                    defined_by: None,
                };
                (StatusCode::OK, Json(wire)).into_response()
            } else {
                not_found(format!("agent role `{role_id}` not found for project"))
            }
        }
        Err(err) => store_error_response(err),
    }
}

/// `POST /v1/projects/:project/agent-roles` — create a role (or
/// re-activate a previously-retracted one per §D6).
///
/// Returns 201 with `ETag: "<defined_at>"` on success. 409 when an
/// active row already exists for `(project, role_id)`. 413 on any
/// per-field size overflow. 422 on structural validation failure.
pub(crate) async fn create_agent_role_handler(
    State(state): State<Arc<AppState>>,
    _guard: AdminRoleGuard,
    Extension(principal): Extension<AuthPrincipal>,
    Path(project_raw): Path<String>,
    body: Result<Json<CreateAgentRoleRequest>, JsonRejection>,
) -> Response {
    let project = match resolve_project(&principal, &project_raw) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    let Json(req) = match body {
        Ok(v) => v,
        Err(rej) => return json_rejection_response(rej),
    };

    let mut role = req.into_role();
    if let Some(prompt) = role.system_prompt.as_ref() {
        role.system_prompt = Some(normalise_prompt(prompt));
    }

    if let Err(msg) = check_field_caps(&role) {
        return payload_too_large(msg);
    }

    let report = validate_prompt_structure(&role);
    if !report.passed {
        return validation_failure_response(report.failures);
    }

    // §D6 create-only uniqueness: reject 409 if an active row already
    // exists. We use `get_any` and check `is_active()` — a
    // previously-retracted row is fine (service's `define` upserts
    // and the projection clears `retracted_at` atomically).
    match AgentRoleReadModel::get_any(state.runtime.store.as_ref(), &project, &role.role_id).await {
        Ok(Some(existing)) if existing.is_active() => {
            return conflict(format!(
                "agent role `{}` already has an active definition — use PATCH to update",
                role.role_id
            ));
        }
        Ok(_) => {}
        Err(err) => return store_error_response(err),
    }

    let operator = operator_id_from_principal(&principal);
    let role_id = role.role_id.clone();
    let resolved = match state
        .runtime
        .agent_roles
        .define(&project, role, operator.clone())
        .await
    {
        Ok(r) => r,
        Err(err) => return runtime_error_response(err),
    };

    let warnings = shadow_warning(&role_id).into_iter().collect();
    let defined_at = resolved.defined_at.unwrap_or(0);
    let operator_str = operator.as_str().to_owned();
    let body = DefineResponse {
        role: &resolved.role,
        source: role_source_str(resolved.source),
        shadows_builtin: resolved.shadows_builtin.as_deref(),
        defined_at,
        defined_by: &operator_str,
        warnings,
    };
    let mut resp = (StatusCode::CREATED, Json(body)).into_response();
    resp.headers_mut().extend(etag_header(defined_at));
    resp
}

/// `PATCH /v1/projects/:project/agent-roles/:id` — update mutable
/// fields. Immutable fields (`id`, `tier`) trigger 422 `ImmutableField`.
///
/// `If-Match: "<defined_at>"` is optional; if present, a stale value
/// returns 412. On success, the response is identical to POST plus
/// the refreshed `ETag`.
pub(crate) async fn patch_agent_role_handler(
    State(state): State<Arc<AppState>>,
    _guard: AdminRoleGuard,
    Extension(principal): Extension<AuthPrincipal>,
    Path((project_raw, role_id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Result<Json<PatchAgentRoleRequest>, JsonRejection>,
) -> Response {
    let project = match resolve_project(&principal, &project_raw) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    let Json(patch) = match body {
        Ok(v) => v,
        Err(rej) => return json_rejection_response(rej),
    };

    // Load the existing row. 404 when absent (PATCH only updates
    // existing rows — there's no create-on-patch semantic).
    let existing: AgentRoleRecord = match AgentRoleReadModel::get_active(
        state.runtime.store.as_ref(),
        &project,
        &role_id,
    )
    .await
    {
        Ok(Some(r)) => r,
        Ok(None) => {
            return not_found(format!("agent role `{role_id}` not found"));
        }
        Err(err) => return store_error_response(err),
    };

    // §PATCH check order: ImmutableField first, before merge.
    let mut immutable_failures: Vec<ValidationFailure> = Vec::new();
    if patch.id.is_some() {
        immutable_failures.push(ValidationFailure {
            code: FailureCode::ImmutableField,
            field: "id".to_owned(),
            message: "role id is immutable; POST a new role with a different id instead".to_owned(),
            span: None,
            suggested_insert_offset: None,
        });
    }
    if patch.tier.is_some() {
        immutable_failures.push(ValidationFailure {
            code: FailureCode::ImmutableField,
            field: "tier".to_owned(),
            message: "tier is immutable on PATCH".to_owned(),
            span: None,
            suggested_insert_offset: None,
        });
    }
    if !immutable_failures.is_empty() {
        return validation_failure_response(immutable_failures);
    }

    // If-Match check — absent header is a pass (opt-in per §PATCH
    // semantics); present-but-mismatched or malformed → 412.
    if let Some(parsed) = parse_if_match(&headers) {
        match parsed {
            Ok(v) if v == existing.defined_at => {}
            _ => {
                return precondition_failed(format!(
                    "If-Match mismatch; current ETag is \"{}\"",
                    existing.defined_at
                ));
            }
        }
    }

    // Merge patch into existing role.
    let mut merged = existing.role.clone();
    if let Some(v) = patch.name {
        merged.display_name = v;
    }
    if let Some(v) = patch.description {
        merged.description = v;
    }
    if let Some(v) = patch.system_prompt {
        merged.system_prompt = Some(normalise_prompt(&v));
    }
    if let Some(v) = patch.tools {
        merged.tools = v;
    }
    if let Some(v) = patch.forbid_all_tools {
        merged.forbid_all_tools = v;
    }
    if let Some(v) = patch.max_context_tokens {
        merged.max_context_tokens = v;
    }
    if let Some(v) = patch.response_shape {
        merged.response_shape = v;
    }

    if let Err(msg) = check_field_caps(&merged) {
        return payload_too_large(msg);
    }
    let report = validate_prompt_structure(&merged);
    if !report.passed {
        return validation_failure_response(report.failures);
    }

    let operator = operator_id_from_principal(&principal);
    let resolved = match state
        .runtime
        .agent_roles
        .define(&project, merged, operator.clone())
        .await
    {
        Ok(r) => r,
        Err(err) => return runtime_error_response(err),
    };

    let warnings = shadow_warning(&role_id).into_iter().collect();
    let defined_at = resolved.defined_at.unwrap_or(0);
    let operator_str = operator.as_str().to_owned();
    let body = DefineResponse {
        role: &resolved.role,
        source: role_source_str(resolved.source),
        shadows_builtin: resolved.shadows_builtin.as_deref(),
        defined_at,
        defined_by: &operator_str,
        warnings,
    };
    let mut resp = (StatusCode::OK, Json(body)).into_response();
    resp.headers_mut().extend(etag_header(defined_at));
    resp
}

/// `DELETE /v1/projects/:project/agent-roles/:id` — retract a custom
/// role. 200 on success (or idempotent repeat). 404 when the id has
/// never been defined for this project.
///
/// §D7 idempotency: a repeat DELETE on an already-retracted role
/// emits **no** new event, returns the stored `retracted_at` /
/// `retracted_by` verbatim. Clients observing the same timestamp on
/// successive DELETEs is the intended signal.
pub(crate) async fn delete_agent_role_handler(
    State(state): State<Arc<AppState>>,
    _guard: AdminRoleGuard,
    Extension(principal): Extension<AuthPrincipal>,
    Path((project_raw, role_id)): Path<(String, String)>,
) -> Response {
    let project = match resolve_project(&principal, &project_raw) {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    match AgentRoleReadModel::get_any(state.runtime.store.as_ref(), &project, &role_id).await {
        Ok(None) => not_found(format!("agent role `{role_id}` not found for project")),
        Ok(Some(row)) if !row.is_active() => {
            // Idempotent repeat — existing retracted row.
            let body = RetractResponse {
                role_id: row.role_id,
                retracted_at: row.retracted_at.unwrap_or(0),
                retracted_by: row
                    .retracted_by
                    .map(|o| o.as_str().to_owned())
                    .unwrap_or_default(),
                warnings: Vec::new(),
            };
            (StatusCode::OK, Json(body)).into_response()
        }
        Ok(Some(row)) => {
            let operator = operator_id_from_principal(&principal);
            if let Err(err) = state
                .runtime
                .agent_roles
                .retract(&project, &row.role_id, operator)
                .await
            {
                return runtime_error_response(err);
            }
            // Re-read so the body reflects the event timestamp the
            // projection just wrote.
            let retracted =
                match AgentRoleReadModel::get_any(state.runtime.store.as_ref(), &project, &role_id)
                    .await
                {
                    Ok(Some(r)) => r,
                    Ok(None) => {
                        // Invariant violation: retract succeeded but the
                        // projection has no row. Log server-side, return a
                        // generic 500 to the caller.
                        tracing::error!(
                            role_id = %role_id,
                            project = ?project,
                            "retract succeeded but projection read returned no row"
                        );
                        return AppApiError::new(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "internal_error",
                            "internal runtime error",
                        )
                        .into_response();
                    }
                    Err(err) => return store_error_response(err),
                };
            let body = RetractResponse {
                role_id: retracted.role_id,
                retracted_at: retracted.retracted_at.unwrap_or(0),
                retracted_by: retracted
                    .retracted_by
                    .map(|o| o.as_str().to_owned())
                    .unwrap_or_default(),
                warnings: Vec::new(),
            };
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(err) => store_error_response(err),
    }
}

// ── RFC 031 PR-D3 §History panel ─────────────────────────────────────────────

/// One entry in the agent-role history response. The shape is
/// deliberately narrow — the UI only needs the kind, actor, timestamp,
/// and (for `defined` events) enough of the role snapshot to render a
/// diff between consecutive entries.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct AgentRoleHistoryEntry {
    /// `"defined"` or `"retracted"`.
    pub(crate) kind: &'static str,
    pub(crate) at_ms: u64,
    pub(crate) actor: String,
    /// `Some(role)` on `defined` entries so the UI can diff the
    /// prompt / tools / shape between successive snapshots. `None`
    /// on `retracted` entries.
    pub(crate) role: Option<cairn_domain::agent_roles::AgentRole>,
    /// `Some("reviewer")` etc. on `defined` entries when the id
    /// matches a built-in. `None` otherwise + on retract entries.
    pub(crate) shadows_builtin: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct AgentRoleHistoryResponse {
    pub(crate) items: Vec<AgentRoleHistoryEntry>,
    pub(crate) total: usize,
}

/// `GET /v1/projects/:project/agent-roles/:id/history` — RFC 031 PR-D3
/// §History panel.
///
/// Returns every `AgentRoleDefined` / `AgentRoleRetracted` event on
/// the global event log that matches `(project, role_id)`, oldest
/// first. The UI renders the list with a prompt diff between
/// consecutive `defined` entries.
///
/// Reads the full event stream in chunks of 10 000; filter-in-memory
/// is fine because a single project's role-mutation events are
/// bounded by human iteration cadence (dozens per role over the
/// project's lifetime, not millions). No pagination in v1 — the UI
/// renders the full history inline.
pub(crate) async fn get_agent_role_history_handler(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Path((project_raw, role_id)): Path<(String, String)>,
) -> Response {
    let project = match resolve_project(&principal, &project_raw) {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    // Walk the event log in chunks until exhausted. The per-role
    // history is bounded; reading more than one page is rare in
    // practice.
    const CHUNK: usize = 10_000;
    let mut cursor: Option<cairn_store::EventPosition> = None;
    let mut items: Vec<AgentRoleHistoryEntry> = Vec::new();
    let store = state.runtime.store.as_ref();
    loop {
        let batch = match store.read_stream(cursor, CHUNK).await {
            Ok(v) => v,
            Err(e) => return store_error_response(e),
        };
        if batch.is_empty() {
            break;
        }
        let last = batch.last().map(|e| e.position);
        for stored in &batch {
            match &stored.envelope.payload {
                cairn_domain::RuntimeEvent::AgentRoleDefined(e)
                    if e.project == project && e.role.role_id == role_id =>
                {
                    items.push(AgentRoleHistoryEntry {
                        kind: "defined",
                        at_ms: e.at_ms,
                        actor: e.defined_by.as_str().to_owned(),
                        role: Some(e.role.clone()),
                        shadows_builtin: e.shadows_builtin.clone(),
                    });
                }
                cairn_domain::RuntimeEvent::AgentRoleRetracted(e)
                    if e.project == project && e.role_id == role_id =>
                {
                    items.push(AgentRoleHistoryEntry {
                        kind: "retracted",
                        at_ms: e.at_ms,
                        actor: e.retracted_by.as_str().to_owned(),
                        role: None,
                        shadows_builtin: None,
                    });
                }
                _ => {}
            }
        }
        cursor = last;
        if batch.len() < CHUNK {
            break;
        }
    }

    // Append order is stream order == timestamp order modulo clock
    // skew. Keep it explicit for the UI so older → newer rendering
    // doesn't need a secondary sort.
    items.sort_by_key(|i| i.at_ms);
    let total = items.len();
    (
        StatusCode::OK,
        Json(AgentRoleHistoryResponse { items, total }),
    )
        .into_response()
}
