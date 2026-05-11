//! HTTP route handlers for triggers and run templates — RFC 022.
//!
//! All routes are project-scoped:
//!   /v1/projects/:project/triggers/*
//!   /v1/projects/:project/run-templates/*
//!
//! RFC-025 Phase 1.5a migration notes: the handlers below previously
//! took a `Mutex<TriggerService>` lock, mutated in-memory HashMaps, then
//! separately appended a `RuntimeEvent` through the event log. Post-
//! refactor the `TriggerService` is projection-backed and each async
//! method already appends the durable event inside the same call, so the
//! handlers are thinner — no lock, no second append, one `.await` per
//! CRUD call. The cross-tenant guard + request validation + response
//! shape are unchanged so the HTTP contract is preserved.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};

use cairn_domain::decisions::RunMode;
use cairn_domain::ids::{OperatorId, RunTemplateId, TriggerId};
use cairn_domain::tenancy::ProjectKey;
use cairn_runtime::{
    RateLimitConfig, RunTemplate, SignalPattern, TemplateBudget, Trigger, TriggerCondition,
    TriggerError, TriggerEvent, TriggerState,
};

use crate::AppState;

// ── Request DTOs ────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct CreateTriggerRequest {
    pub name: String,
    pub description: Option<String>,
    pub signal_type: String,
    pub plugin_id: Option<String>,
    pub conditions: Vec<TriggerCondition>,
    pub run_template_id: String,
    #[serde(default = "default_max_chain_depth")]
    pub max_chain_depth: u8,
    pub rate_limit: Option<RateLimitConfig>,
}

fn default_max_chain_depth() -> u8 {
    5
}

#[derive(Deserialize)]
pub struct DisableRequest {
    pub reason: Option<String>,
}

#[derive(Deserialize)]
pub struct CreateRunTemplateRequest {
    pub name: String,
    pub description: Option<String>,
    #[serde(default)]
    pub default_mode: RunMode,
    pub system_prompt: String,
    pub initial_user_message: Option<String>,
    pub plugin_allowlist: Option<Vec<String>>,
    pub tool_allowlist: Option<Vec<String>>,
    #[serde(default)]
    pub budget: TemplateBudget,
    pub sandbox_hint: Option<String>,
    #[serde(default)]
    pub required_fields: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ListQuery {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl ListQuery {
    fn limit(&self) -> usize {
        self.limit.unwrap_or(100).min(100)
    }

    fn offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }
}

// ── Response DTOs ───────────────────────────────────────────────────────────

#[derive(Serialize)]
struct TriggerEventResponse {
    events: Vec<TriggerEvent>,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn validate_project_segment(value: &str, field: &'static str) -> Result<(), String> {
    let is_valid = !value.is_empty()
        && value != "."
        && value != ".."
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'));

    if is_valid {
        Ok(())
    } else {
        Err(format!("{field} contains unsupported path characters"))
    }
}

fn project_key(project_id: &str) -> Result<ProjectKey, String> {
    if let Some((tenant_id, workspace_id, scoped_project_id)) =
        crate::parse_project_scope(project_id)
    {
        validate_project_segment(tenant_id, "tenant_id")?;
        validate_project_segment(workspace_id, "workspace_id")?;
        validate_project_segment(scoped_project_id, "project_id")?;
        return Ok(ProjectKey::new(tenant_id, workspace_id, scoped_project_id));
    }

    validate_project_segment(project_id, "project_id")?;
    Ok(ProjectKey::new(
        crate::DEFAULT_TENANT_ID,
        crate::DEFAULT_WORKSPACE_ID,
        project_id,
    ))
}

/// T6c-C3: derive the operator id from the authenticated principal so
/// trigger writes land with the real actor in the event log.
fn operator_id_from_principal(principal: &cairn_api::auth::AuthPrincipal) -> OperatorId {
    OperatorId::new(crate::handlers::admin::audit_actor_id(principal))
}

/// T6c-C3: cross-tenant guard. Returns a response when the path-scoped
/// project doesn't belong to the caller's tenant; caller returns it
/// directly.
fn check_tenant(
    principal: &cairn_api::auth::AuthPrincipal,
    project: &ProjectKey,
) -> Option<axum::response::Response> {
    if crate::extractors::enforce_project_tenant(principal, project) {
        None
    } else {
        Some(crate::errors::tenant_scope_mismatch_error().into_response())
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn bad_request_response(message: impl Into<String>) -> axum::response::Response {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            error: message.into(),
        }),
    )
        .into_response()
}

fn not_found_response(entity: &str, id: &str) -> axum::response::Response {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: format!("{entity} not found: {id}"),
        }),
    )
        .into_response()
}

/// RFC-025 Phase 1.5a: surface a `TriggerError` from the projection-backed
/// service as the right HTTP status. Keeps the response body shape
/// identical to the pre-refactor handler so clients don't see a behaviour
/// change.
///
/// `treat_template_not_found_as_bad_request` = true lets create handlers
/// preserve the pre-refactor 400 response when a caller posts a trigger
/// referencing a missing template (vs. 404 used by GET/DELETE endpoints
/// where the entity itself is what's missing). See PR #569 Copilot
/// review for the compatibility rationale.
fn trigger_error_response_with(
    err: TriggerError,
    treat_template_not_found_as_bad_request: bool,
) -> axum::response::Response {
    let status = match &err {
        TriggerError::TemplateNotFound(_) if treat_template_not_found_as_bad_request => {
            StatusCode::BAD_REQUEST
        }
        TriggerError::TriggerNotFound(_) | TriggerError::TemplateNotFound(_) => {
            StatusCode::NOT_FOUND
        }
        TriggerError::TemplateInUse { .. } => StatusCode::CONFLICT,
        TriggerError::NotSuspended(_) => StatusCode::BAD_REQUEST,
        TriggerError::Store(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (
        status,
        Json(ErrorResponse {
            error: err.to_string(),
        }),
    )
        .into_response()
}

/// Thin wrapper for endpoints where a missing template/trigger should
/// be 404 (reads + deletes). See `trigger_error_response_with` for
/// endpoints that need the 400-for-missing-template legacy semantics.
fn trigger_error_response(err: TriggerError) -> axum::response::Response {
    trigger_error_response_with(err, false)
}

// ── Trigger Handlers ────────────────────────────────────────────────────────

/// GET /v1/projects/:project/triggers
pub async fn list_triggers_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::Extension(principal): axum::extract::Extension<cairn_api::auth::AuthPrincipal>,
    Query(query): Query<ListQuery>,
    Path(project_id): Path<String>,
) -> impl IntoResponse {
    let project = match project_key(&project_id) {
        Ok(project) => project,
        Err(message) => return bad_request_response(message),
    };
    // T6c-C3: refuse cross-tenant reads so list endpoints don't leak.
    if let Some(resp) = check_tenant(&principal, &project) {
        return resp;
    }
    let mut list = match state.triggers.list_triggers_for_project(&project).await {
        Ok(list) => list,
        Err(err) => return trigger_error_response(err),
    };
    list.sort_by_key(|r| r.id.clone());
    let list: Vec<Trigger> = list
        .into_iter()
        .skip(query.offset())
        .take(query.limit())
        .collect();
    Json(serde_json::to_value(&list).expect("trigger/template list serialization")).into_response()
}

/// POST /v1/projects/:project/triggers
pub async fn create_trigger_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::Extension(principal): axum::extract::Extension<cairn_api::auth::AuthPrincipal>,
    Path(project_id): Path<String>,
    Json(body): Json<CreateTriggerRequest>,
) -> impl IntoResponse {
    let project = match project_key(&project_id) {
        Ok(project) => project,
        Err(message) => return bad_request_response(message),
    };
    if let Some(resp) = check_tenant(&principal, &project) {
        return resp;
    }

    let now = now_ms();
    // RFC-025 Phase 1.5a review: UUID-based id rather than
    // millisecond-timestamp so concurrent trigger creations in the same
    // ms can't collide with the projection's PRIMARY KEY on
    // `trigger_id` + `ON CONFLICT DO NOTHING` (which would silently
    // drop the second row).
    let trigger = Trigger {
        id: TriggerId::new(format!("trigger_{}", uuid::Uuid::new_v4())),
        project,
        name: body.name,
        description: body.description,
        signal_pattern: SignalPattern {
            signal_type: body.signal_type,
            plugin_id: body.plugin_id,
        },
        conditions: body.conditions,
        run_template_id: RunTemplateId::new(body.run_template_id),
        state: TriggerState::Enabled,
        rate_limit: body.rate_limit.unwrap_or_default(),
        max_chain_depth: body.max_chain_depth,
        created_by: operator_id_from_principal(&principal),
        created_at: now,
        updated_at: now,
    };

    match state.triggers.create_trigger(trigger).await {
        Ok(event) => (
            StatusCode::CREATED,
            Json(TriggerEventResponse {
                events: vec![event],
            }),
        )
            .into_response(),
        // Pre-refactor: create-trigger returned 400 when the referenced
        // template was missing. Preserve that (PR #569 review) rather
        // than switching to 404 — the caller's referenced entity is not
        // missing from the URL; the body payload is invalid.
        Err(err) => trigger_error_response_with(err, true),
    }
}

/// GET /v1/projects/:project/triggers/:trigger_id
pub async fn get_trigger_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::Extension(principal): axum::extract::Extension<cairn_api::auth::AuthPrincipal>,
    Path((project_id, trigger_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let project = match project_key(&project_id) {
        Ok(project) => project,
        Err(message) => return bad_request_response(message),
    };
    if let Some(resp) = check_tenant(&principal, &project) {
        return resp;
    }
    match state
        .triggers
        .get_trigger(&TriggerId::new(&trigger_id))
        .await
    {
        Ok(Some(trigger)) if trigger.project == project => {
            Json(serde_json::to_value(&trigger).expect("trigger serialization")).into_response()
        }
        Ok(_) => not_found_response("trigger", &trigger_id),
        Err(err) => trigger_error_response(err),
    }
}

/// DELETE /v1/projects/:project/triggers/:trigger_id
pub async fn delete_trigger_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::Extension(principal): axum::extract::Extension<cairn_api::auth::AuthPrincipal>,
    Path((project_id, trigger_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let project = match project_key(&project_id) {
        Ok(project) => project,
        Err(message) => return bad_request_response(message),
    };
    if let Some(resp) = check_tenant(&principal, &project) {
        return resp;
    }
    let trigger_id = TriggerId::new(&trigger_id);
    // Cross-project guard: reject if the trigger exists but in a
    // different project than the URL-scoped one. Matches the
    // pre-refactor behaviour (404 rather than 403 so we don't leak
    // cross-tenant existence).
    match state.triggers.get_trigger(&trigger_id).await {
        Ok(Some(trigger)) if trigger.project == project => {}
        Ok(_) => return not_found_response("trigger", trigger_id.as_str()),
        Err(err) => return trigger_error_response(err),
    }
    match state
        .triggers
        .delete_trigger(&trigger_id, operator_id_from_principal(&principal))
        .await
    {
        Ok(event) => (
            StatusCode::OK,
            Json(TriggerEventResponse {
                events: vec![event],
            }),
        )
            .into_response(),
        Err(err) => trigger_error_response(err),
    }
}

/// POST /v1/projects/:project/triggers/:trigger_id/enable
pub async fn enable_trigger_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::Extension(principal): axum::extract::Extension<cairn_api::auth::AuthPrincipal>,
    Path((project_id, trigger_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let project = match project_key(&project_id) {
        Ok(project) => project,
        Err(message) => return bad_request_response(message),
    };
    if let Some(resp) = check_tenant(&principal, &project) {
        return resp;
    }
    let trigger_id = TriggerId::new(&trigger_id);
    match state.triggers.get_trigger(&trigger_id).await {
        Ok(Some(trigger)) if trigger.project == project => {}
        Ok(_) => return not_found_response("trigger", trigger_id.as_str()),
        Err(err) => return trigger_error_response(err),
    }
    match state
        .triggers
        .enable_trigger(&trigger_id, operator_id_from_principal(&principal))
        .await
    {
        Ok(event) => (
            StatusCode::OK,
            Json(TriggerEventResponse {
                events: vec![event],
            }),
        )
            .into_response(),
        Err(err) => trigger_error_response(err),
    }
}

/// POST /v1/projects/:project/triggers/:trigger_id/disable
pub async fn disable_trigger_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::Extension(principal): axum::extract::Extension<cairn_api::auth::AuthPrincipal>,
    Path((project_id, trigger_id)): Path<(String, String)>,
    body: Option<Json<DisableRequest>>,
) -> impl IntoResponse {
    let reason = body.and_then(|Json(b)| b.reason);
    let project = match project_key(&project_id) {
        Ok(project) => project,
        Err(message) => return bad_request_response(message),
    };
    if let Some(resp) = check_tenant(&principal, &project) {
        return resp;
    }
    let trigger_id = TriggerId::new(&trigger_id);
    match state.triggers.get_trigger(&trigger_id).await {
        Ok(Some(trigger)) if trigger.project == project => {}
        Ok(_) => return not_found_response("trigger", trigger_id.as_str()),
        Err(err) => return trigger_error_response(err),
    }
    match state
        .triggers
        .disable_trigger(&trigger_id, operator_id_from_principal(&principal), reason)
        .await
    {
        Ok(event) => (
            StatusCode::OK,
            Json(TriggerEventResponse {
                events: vec![event],
            }),
        )
            .into_response(),
        Err(err) => trigger_error_response(err),
    }
}

/// POST /v1/projects/:project/triggers/:trigger_id/resume
pub async fn resume_trigger_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::Extension(principal): axum::extract::Extension<cairn_api::auth::AuthPrincipal>,
    Path((project_id, trigger_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let project = match project_key(&project_id) {
        Ok(project) => project,
        Err(message) => return bad_request_response(message),
    };
    if let Some(resp) = check_tenant(&principal, &project) {
        return resp;
    }
    let trigger_id = TriggerId::new(&trigger_id);
    match state.triggers.get_trigger(&trigger_id).await {
        Ok(Some(trigger)) if trigger.project == project => {}
        Ok(_) => return not_found_response("trigger", trigger_id.as_str()),
        Err(err) => return trigger_error_response(err),
    }
    match state
        .triggers
        .resume_trigger(&trigger_id, operator_id_from_principal(&principal))
        .await
    {
        Ok(event) => (
            StatusCode::OK,
            Json(TriggerEventResponse {
                events: vec![event],
            }),
        )
            .into_response(),
        Err(err) => trigger_error_response(err),
    }
}

// ── Run Template Handlers ───────────────────────────────────────────────────

/// GET /v1/projects/:project/run-templates
pub async fn list_run_templates_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::Extension(principal): axum::extract::Extension<cairn_api::auth::AuthPrincipal>,
    Query(query): Query<ListQuery>,
    Path(project_id): Path<String>,
) -> impl IntoResponse {
    let project = match project_key(&project_id) {
        Ok(project) => project,
        Err(message) => return bad_request_response(message),
    };
    if let Some(resp) = check_tenant(&principal, &project) {
        return resp;
    }
    let mut list = match state.triggers.list_templates_for_project(&project).await {
        Ok(list) => list,
        Err(err) => return trigger_error_response(err),
    };
    list.sort_by_key(|r| r.id.clone());
    let list: Vec<RunTemplate> = list
        .into_iter()
        .skip(query.offset())
        .take(query.limit())
        .collect();
    Json(serde_json::to_value(&list).expect("trigger/template list serialization")).into_response()
}

/// POST /v1/projects/:project/run-templates
pub async fn create_run_template_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::Extension(principal): axum::extract::Extension<cairn_api::auth::AuthPrincipal>,
    Path(project_id): Path<String>,
    Json(body): Json<CreateRunTemplateRequest>,
) -> impl IntoResponse {
    let project = match project_key(&project_id) {
        Ok(project) => project,
        Err(message) => return bad_request_response(message),
    };
    if let Some(resp) = check_tenant(&principal, &project) {
        return resp;
    }
    let now = now_ms();
    // RFC-025 Phase 1.5a review: same UUID-based-id rationale as
    // `create_trigger_handler` above.
    let template = RunTemplate {
        id: RunTemplateId::new(format!("tmpl_{}", uuid::Uuid::new_v4())),
        project,
        name: body.name,
        description: body.description,
        default_mode: body.default_mode,
        system_prompt: body.system_prompt,
        initial_user_message: body.initial_user_message,
        plugin_allowlist: body.plugin_allowlist,
        tool_allowlist: body.tool_allowlist,
        budget: body.budget,
        sandbox_hint: body.sandbox_hint,
        required_fields: body.required_fields,
        created_by: operator_id_from_principal(&principal),
        created_at: now,
        updated_at: now,
    };
    match state.triggers.create_template(template).await {
        Ok(event) => (
            StatusCode::CREATED,
            Json(TriggerEventResponse {
                events: vec![event],
            }),
        )
            .into_response(),
        Err(err) => trigger_error_response(err),
    }
}

/// GET /v1/projects/:project/run-templates/:template_id
pub async fn get_run_template_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::Extension(principal): axum::extract::Extension<cairn_api::auth::AuthPrincipal>,
    Path((project_id, template_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let project = match project_key(&project_id) {
        Ok(project) => project,
        Err(message) => return bad_request_response(message),
    };
    if let Some(resp) = check_tenant(&principal, &project) {
        return resp;
    }
    match state
        .triggers
        .get_template(&RunTemplateId::new(&template_id))
        .await
    {
        Ok(Some(template)) if template.project == project => {
            Json(serde_json::to_value(&template).expect("template serialization")).into_response()
        }
        Ok(_) => not_found_response("run template", &template_id),
        Err(err) => trigger_error_response(err),
    }
}

/// DELETE /v1/projects/:project/run-templates/:template_id
/// Returns 409 if any trigger references it.
pub async fn delete_run_template_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::Extension(principal): axum::extract::Extension<cairn_api::auth::AuthPrincipal>,
    Path((project_id, template_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let project = match project_key(&project_id) {
        Ok(project) => project,
        Err(message) => return bad_request_response(message),
    };
    if let Some(resp) = check_tenant(&principal, &project) {
        return resp;
    }
    let template_id = RunTemplateId::new(&template_id);
    match state.triggers.get_template(&template_id).await {
        Ok(Some(template)) if template.project == project => {}
        Ok(_) => return not_found_response("run template", template_id.as_str()),
        Err(err) => return trigger_error_response(err),
    }
    match state
        .triggers
        .delete_template(&template_id, operator_id_from_principal(&principal))
        .await
    {
        Ok(event) => (
            StatusCode::OK,
            Json(TriggerEventResponse {
                events: vec![event],
            }),
        )
            .into_response(),
        Err(err) => trigger_error_response(err),
    }
}
