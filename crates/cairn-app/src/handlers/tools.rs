//! Tool invocation and checkpoint HTTP handlers.
//!
//! Tenant isolation contract
//! -------------------------
//!
//! Every handler in this module enforces tenant isolation, either via
//! a direct `TenantScope` extractor on the handler signature or via a
//! body extractor such as `ProjectJson<T>` that validates the body
//! tenant against the caller's scope at extraction time. Before
//! reading or mutating any record, handlers enforce
//! `tenant_scope.is_admin || record.project.tenant_id ==
//! *tenant_scope.tenant_id()`. Cross-tenant access returns 404 (not
//! 403) to avoid an id-enumeration oracle.
//!
//! See META #372 for the audit trail — these handlers previously took
//! only `State<Arc<AppState>>` + a path/query/body extractor and leaked
//! cross-tenant reads, writes, and cancellations.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};

use cairn_api::http::ListResponse;
use cairn_domain::tool_invocation::ToolInvocationTarget;
use cairn_domain::{
    CheckpointId, CheckpointStrategy, CheckpointStrategySet, ExecutionClass, RunId, RuntimeEvent,
    SessionId, TaskId, ToolInvocationId,
};
use cairn_runtime::{CheckpointService, ToolInvocationService};
use cairn_store::projections::{
    CheckpointReadModel, CheckpointStrategyReadModel, ToolInvocationProgressReadModel,
    ToolInvocationReadModel,
};
use cairn_store::EventLog;

use crate::errors::{
    bad_request_response, now_ms, operator_event_envelope, run_not_found_response,
    runtime_error_response, store_error_response, validation_error_response, AppApiError,
};
use crate::extractors::TenantScope;
use crate::helpers::{build_run_replay_result, checkpoint_recorded_position};
use crate::state::AppState;
use crate::{
    cancel_plugin_invocation, current_event_head, parse_tool_invocation_state,
    publish_runtime_frames_since,
};

// ── Shared helpers ──────────────────────────────────────────────────────────

/// Response shared by every "this id is either not yours or doesn't exist"
/// exit path in this module. A single string keeps the behavior uniform so
/// a cross-tenant probe cannot distinguish a real miss from a scope miss by
/// the error body.
fn tool_invocation_not_found_response() -> axum::response::Response {
    AppApiError::new(
        StatusCode::NOT_FOUND,
        "not_found",
        "tool invocation not found",
    )
    .into_response()
}

fn checkpoint_not_found_response() -> axum::response::Response {
    AppApiError::new(StatusCode::NOT_FOUND, "not_found", "checkpoint not found").into_response()
}

fn tool_invocation_progress_not_found_response() -> axum::response::Response {
    AppApiError::new(
        StatusCode::NOT_FOUND,
        "not_found",
        "tool invocation progress not found",
    )
    .into_response()
}

/// True when the caller is allowed to see a record rooted at the given
/// `ProjectKey`. Admin tokens see every tenant (matches the existing
/// `load_run_visible_to_tenant` / `load_task_visible_to_tenant`
/// contract); non-admin tokens must match tenant exactly.
fn tenant_visible(scope: &TenantScope, project: &cairn_domain::ProjectKey) -> bool {
    scope.is_admin || project.tenant_id == *scope.tenant_id()
}

// ── DTOs ─────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub(crate) struct ToolInvocationListQuery {
    pub(crate) run_id: Option<String>,
    pub(crate) state: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
}

impl ToolInvocationListQuery {
    pub(crate) fn limit(&self) -> usize {
        self.limit.unwrap_or(100).min(500)
    }

    pub(crate) fn offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }
}

/// Body shape for `POST /v1/tool-invocations`.
///
/// #365: a body-supplied `tenant_id` is now validated against the
/// `TenantScope` via `ProjectJson<CreateToolInvocationRequest>`.
/// Non-admin callers whose body `tenant_id` disagrees with the
/// bearer-token tenant are refused with 403 before the handler even
/// runs; admin tokens pass through (matches the `CreateRunRequest` /
/// `CreateTaskRequest` shape). The old handler accepted the body
/// tenant verbatim, which let any authenticated caller plant records
/// into an arbitrary tenant.
#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct CreateToolInvocationRequest {
    pub(crate) tenant_id: String,
    pub(crate) workspace_id: String,
    pub(crate) project_id: String,
    pub(crate) invocation_id: String,
    pub(crate) session_id: Option<String>,
    pub(crate) run_id: Option<String>,
    pub(crate) task_id: Option<String>,
    pub(crate) target: ToolInvocationTarget,
    pub(crate) execution_class: ExecutionClass,
    /// F55: structured tool args persisted on the invocation projection.
    /// Optional — legacy clients that only log lifecycle metadata omit it.
    #[serde(default)]
    pub(crate) args: Option<serde_json::Value>,
}

impl CreateToolInvocationRequest {
    pub(crate) fn project(&self) -> cairn_domain::ProjectKey {
        cairn_domain::ProjectKey::new(
            self.tenant_id.as_str(),
            self.workspace_id.as_str(),
            self.project_id.as_str(),
        )
    }
}

impl crate::extractors::HasProjectScope for CreateToolInvocationRequest {
    fn project(&self) -> cairn_domain::ProjectKey {
        Self::project(self)
    }
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub(crate) struct CheckpointListQuery {
    pub(crate) run_id: Option<String>,
    pub(crate) limit: Option<usize>,
}

impl CheckpointListQuery {
    pub(crate) fn limit(&self) -> usize {
        self.limit.unwrap_or(100).min(500)
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct SaveCheckpointRequest {
    pub(crate) checkpoint_id: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct SetCheckpointStrategyRequest {
    pub(crate) strategy_id: String,
    pub(crate) interval_ms: u64,
    pub(crate) max_checkpoints: u32,
    pub(crate) trigger_on_task_complete: bool,
}

// ── Handlers: Tool Invocations ──────────────────────────────────────────────

/// F55: operator-facing view of a tool invocation. Explicitly names every
/// field the operator dashboards (and `GET /v1/tool-invocations`) need —
/// no `#[serde(flatten)]` over `ToolInvocationRecord` because
/// `ToolInvocationTarget::Builtin` / `::Plugin` both serialize a nested
/// `tool_name` that would collide with the flat `tool_name` below and
/// produce a duplicate-key JSON object. Instead we carry the durable
/// record under a stable `record` namespace so clients that want the
/// full projection shape still have it.
#[derive(Debug, serde::Serialize)]
pub(crate) struct ToolInvocationView {
    /// Flattened tool name (from `target.tool_name`). Primary operator
    /// field — the dogfood bug was that this was null in the response.
    tool_name: String,
    /// Alias of the durable `state` enum. Preserved under this name so
    /// dashboards that were written against the earlier `status`
    /// convention keep working.
    status: cairn_domain::tool_invocation::ToolInvocationState,
    /// Structured tool arguments captured at dispatch time.
    /// Always serialized (as JSON `null` when absent) so operator
    /// dashboards see a stable key shape regardless of whether the
    /// invocation is pre-F55 or a legacy non-orchestrator caller.
    args: Option<serde_json::Value>,
    /// Truncated UTF-8 preview of the captured tool output. Always
    /// serialized — see `args`.
    output: Option<String>,
    /// True when `output` was truncated at the backend cap.
    output_truncated: bool,
    /// The durable projection record, unchanged, under an explicit key
    /// so the response doesn't collide with the flat fields above.
    record: cairn_domain::tool_invocation::ToolInvocationRecord,
}

impl ToolInvocationView {
    fn from_record(record: cairn_domain::tool_invocation::ToolInvocationRecord) -> Self {
        let tool_name = match &record.target {
            ToolInvocationTarget::Builtin { tool_name } => tool_name.clone(),
            ToolInvocationTarget::Plugin { tool_name, .. } => tool_name.clone(),
        };
        let status = record.state;
        let args = record.args_json.clone();
        // F55 review: strip the sentinel suffix from `output` so clients
        // that render both `output` AND `output_truncated` don't show
        // truncation twice. The durable record on `record.output_preview`
        // keeps the raw projection value, suffix and all.
        let raw_preview = record.output_preview.as_deref();
        let output_truncated = raw_preview
            .map(|p| {
                p.ends_with(cairn_domain::tool_invocation::TOOL_OUTPUT_PREVIEW_TRUNCATED_SUFFIX)
            })
            .unwrap_or(false);
        let output = raw_preview.map(|p| {
            if output_truncated {
                p.strip_suffix(cairn_domain::tool_invocation::TOOL_OUTPUT_PREVIEW_TRUNCATED_SUFFIX)
                    .unwrap_or(p)
                    .to_owned()
            } else {
                p.to_owned()
            }
        });
        Self {
            tool_name,
            status,
            args,
            output,
            output_truncated,
            record,
        }
    }
}

/// `GET /v1/tool-invocations?run_id=…`
///
/// #362: tenant-scoped. The run is resolved first and its project scope
/// is compared against the caller. A non-admin reading a run from
/// another tenant sees an empty list (same shape as "no invocations")
/// — we do not 404 because this endpoint is list-shaped, and returning
/// empty matches the behavior of a run whose invocation table happens
/// to be empty. Admins see all tenants.
pub(crate) async fn list_tool_invocations_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Query(query): Query<ToolInvocationListQuery>,
) -> impl IntoResponse {
    let Some(run_id) = query.run_id.as_deref() else {
        return (
            StatusCode::OK,
            Json(ListResponse::<ToolInvocationView> {
                items: Vec::new(),
                has_more: false,
            }),
        )
            .into_response();
    };

    // Resolve the run through the shared `load_run_visible_to_tenant`
    // helper so the tenant gate stays in lockstep with every other run
    // read in cairn-app. A missing run returns the same empty shape as
    // "run with no invocations" — cross-tenant probing cannot
    // distinguish the two cases. (Cursor bugbot #537.)
    let run_id = RunId::new(run_id);
    match crate::helpers::load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return (
                StatusCode::OK,
                Json(ListResponse::<ToolInvocationView> {
                    items: Vec::new(),
                    has_more: false,
                }),
            )
                .into_response();
        }
        Err(resp) => return resp,
    }

    // F55 review: filter BEFORE pagination. The state filter is applied
    // in-memory (the read model doesn't push it down), so fetching only
    // `limit + offset` pre-filter would silently drop matching rows that
    // sit past the raw-fetch window. Fetch the run's full invocation
    // list (bounded by a safety cap), filter, THEN offset+limit.
    //
    // Fetch `MAX + 1` so we can detect when a run exceeds the cap and
    // force `has_more=true` instead of silently dropping trailing rows.
    const MAX_TOOL_INVOCATIONS_PER_RUN: usize = 10_000;

    let parsed_state = match query.state.as_deref() {
        Some(s) => match parse_tool_invocation_state(s) {
            Ok(parsed) => Some(parsed),
            Err(message) => return bad_request_response(message),
        },
        None => None,
    };

    let all = match ToolInvocationReadModel::list_by_run(
        state.runtime.store.as_ref(),
        &run_id,
        MAX_TOOL_INVOCATIONS_PER_RUN + 1,
        0,
    )
    .await
    {
        Ok(items) => items,
        Err(err) => return store_error_response(err),
    };
    let cap_exceeded = all.len() > MAX_TOOL_INVOCATIONS_PER_RUN;
    let all: Vec<_> = all.into_iter().take(MAX_TOOL_INVOCATIONS_PER_RUN).collect();

    let filtered: Vec<_> = match parsed_state {
        Some(s) => all.into_iter().filter(|i| i.state == s).collect(),
        None => all,
    };

    let offset = query.offset();
    let limit = query.limit();
    let total = filtered.len();
    let items: Vec<ToolInvocationView> = filtered
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(ToolInvocationView::from_record)
        .collect();

    // F55 review: has_more accurately reflects whether matching rows
    // exist past this page, so cursor-style paging works with the
    // state filter active. `cap_exceeded` forces has_more=true so
    // operators hitting the safety cap know more data exists even
    // though this endpoint declines to walk past it.
    let has_more = cap_exceeded || offset.saturating_add(items.len()) < total;

    (StatusCode::OK, Json(ListResponse { items, has_more })).into_response()
}

/// `GET /v1/tool-invocations/:id`
///
/// #363: tenant-scoped. Cross-tenant reads return 404 (same response
/// as unknown id) so the endpoint does not leak id existence across
/// tenants.
pub(crate) async fn get_tool_invocation_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match ToolInvocationReadModel::get(state.runtime.store.as_ref(), &ToolInvocationId::new(id))
        .await
    {
        Ok(Some(record)) if tenant_visible(&tenant_scope, &record.project) => (
            StatusCode::OK,
            // F55: return the flattened operator view so single-item
            // GETs match the shape of the list endpoint.
            Json(ToolInvocationView::from_record(record)),
        )
            .into_response(),
        Ok(Some(_)) | Ok(None) => tool_invocation_not_found_response(),
        Err(err) => store_error_response(err),
    }
}

/// `GET /v1/tool-invocations/:id/progress`
///
/// #364: tenant-scoped AND O(1). The previous implementation scanned
/// up to 10k events from the log and reverse-searched for a matching
/// `ToolInvocationProgressUpdated`, which was both a DoS (the scan
/// runs on every request, and 10k is neither enough for busy runs
/// nor bounded by tenant) and a cross-tenant read oracle. The fix
/// queries the new `tool_invocation_progress` projection — one row
/// per invocation with the project scope already materialized — and
/// returns 404 when the caller is not allowed to see it.
pub(crate) async fn get_tool_invocation_progress_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let invocation_id = ToolInvocationId::new(id);
    match ToolInvocationProgressReadModel::get(state.runtime.store.as_ref(), &invocation_id).await {
        Ok(Some(record)) if tenant_visible(&tenant_scope, &record.project) => (
            StatusCode::OK,
            Json(serde_json::json!({
                // Preserve the off-by-half ceiling used by the earlier
                // handler — operator dashboards render a float and
                // the +0.5 prevents a 99/100 jitter at completion.
                "percent": record.progress_pct as f64 + 0.5,
                "message": record.message,
                "updated_at_ms": record.updated_at_ms,
            })),
        )
            .into_response(),
        Ok(Some(_)) | Ok(None) => tool_invocation_progress_not_found_response(),
        Err(err) => store_error_response(err),
    }
}

/// `POST /v1/tool-invocations`
///
/// #365: the body-supplied tenant is validated against the caller's
/// `TenantScope` via `ProjectJson<T>`. Non-admin callers whose body
/// `tenant_id` disagrees with the bearer-token tenant are refused
/// before this handler runs (403 from the extractor); admin tokens
/// pass through. Matches `CreateRunRequest` /
/// `CreateTaskRequest` — the pre-fix handler bypassed the check and
/// accepted any tenant from the body, so a caller could plant a
/// record into any tenant simply by spelling it in JSON.
pub(crate) async fn create_tool_invocation_handler(
    State(state): State<Arc<AppState>>,
    project_scope: crate::extractors::ProjectJson<CreateToolInvocationRequest>,
) -> impl IntoResponse {
    let body = project_scope.into_inner();
    let before = current_event_head(&state).await;
    let project = body.project();
    let invocation_id = ToolInvocationId::new(body.invocation_id);
    match state
        .runtime
        .tool_invocations
        .record_start(
            &project,
            invocation_id.clone(),
            body.session_id.map(SessionId::new),
            body.run_id.map(RunId::new),
            body.task_id.map(TaskId::new),
            body.target,
            body.execution_class,
            body.args,
        )
        .await
    {
        Ok(()) => {
            publish_runtime_frames_since(&state, before).await;
            match ToolInvocationReadModel::get(state.runtime.store.as_ref(), &invocation_id).await {
                Ok(Some(record)) => (StatusCode::CREATED, Json(record)).into_response(),
                Ok(None) => {
                    tracing::error!("tool invocation not found after create: {invocation_id}");
                    AppApiError::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal_error",
                        "tool invocation not found after create",
                    )
                    .into_response()
                }
                Err(err) => {
                    tracing::error!("tool invocation read after create failed: {err}");
                    AppApiError::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal_error",
                        err.to_string(),
                    )
                    .into_response()
                }
            }
        }
        Err(err) => runtime_error_response(err),
    }
}

/// `POST /v1/tool-invocations/:id/complete`
///
/// #366: tenant-scoped. Cross-tenant callers (and unknown ids) get
/// 404 — the 404 comes first so we don't leak the target's existence
/// by running the state machine on a record we're not allowed to
/// touch.
pub(crate) async fn complete_tool_invocation_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let before = current_event_head(&state).await;
    let invocation_id = ToolInvocationId::new(id);
    let record =
        match ToolInvocationReadModel::get(state.runtime.store.as_ref(), &invocation_id).await {
            Ok(Some(record)) if tenant_visible(&tenant_scope, &record.project) => record,
            Ok(Some(_)) | Ok(None) => return tool_invocation_not_found_response(),
            Err(err) => return store_error_response(err),
        };

    let tool_name = match &record.target {
        ToolInvocationTarget::Builtin { tool_name } => tool_name.clone(),
        ToolInvocationTarget::Plugin { tool_name, .. } => tool_name.clone(),
    };

    match state
        .runtime
        .tool_invocations
        .record_completed(
            &record.project,
            invocation_id.clone(),
            record.task_id.clone(),
            tool_name,
            &[],
            None,
            None,
        )
        .await
    {
        Ok(()) => {
            publish_runtime_frames_since(&state, before).await;
            match ToolInvocationReadModel::get(state.runtime.store.as_ref(), &invocation_id).await {
                Ok(Some(updated)) => (StatusCode::OK, Json(updated)).into_response(),
                Ok(None) => {
                    tracing::error!("tool invocation not found after completion: {invocation_id}");
                    AppApiError::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal_error",
                        "tool invocation not found after completion",
                    )
                    .into_response()
                }
                Err(err) => store_error_response(err),
            }
        }
        Err(err) => runtime_error_response(err),
    }
}

/// `POST /v1/tool-invocations/:id/cancel`
///
/// #367: tenant-scoped. The plugin-cancel RPC only fires once we
/// have confirmed the caller owns the invocation — otherwise any
/// tenant could DoS another tenant's plugin host by flooding cancel
/// calls against ids they learned from a timing side channel.
pub(crate) async fn cancel_tool_invocation_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let invocation_id = ToolInvocationId::new(id);

    let record =
        match ToolInvocationReadModel::get(state.runtime.store.as_ref(), &invocation_id).await {
            Ok(Some(record)) if tenant_visible(&tenant_scope, &record.project) => record,
            Ok(Some(_)) | Ok(None) => return tool_invocation_not_found_response(),
            Err(err) => return store_error_response(err),
        };

    let tool_name = match &record.target {
        ToolInvocationTarget::Builtin { tool_name } => tool_name.clone(),
        ToolInvocationTarget::Plugin { tool_name, .. } => tool_name.clone(),
    };

    // Best-effort: send cancel RPC to the plugin if one is handling this
    // invocation. Only runs AFTER the tenant check above so a cross-tenant
    // caller cannot use this endpoint to reach another tenant's plugin
    // host.
    if let ToolInvocationTarget::Plugin { plugin_id, .. } = &record.target {
        if let Ok(mut host) = state.plugin_host.lock() {
            cancel_plugin_invocation(&mut host, plugin_id, invocation_id.as_str());
        }
    }

    let before = current_event_head(&state).await;
    match state
        .runtime
        .tool_invocations
        .record_failed(
            &record.project,
            invocation_id.clone(),
            record.task_id.clone(),
            tool_name,
            cairn_domain::tool_invocation::ToolInvocationOutcomeKind::Canceled,
            Some("cancelled_by_operator".to_owned()),
            None,
        )
        .await
    {
        Ok(()) => {
            publish_runtime_frames_since(&state, before).await;
            (
                StatusCode::OK,
                Json(serde_json::json!({ "cancelled": true })),
            )
                .into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

// ── Handlers: Checkpoints ───────────────────────────────────────────────────

/// `GET /v1/checkpoints?run_id=…`
///
/// #368: tenant-scoped. The run is resolved first and its project is
/// compared against the caller; a non-admin reading another tenant's
/// run sees an empty list (same shape as a run with no checkpoints),
/// matching `list_tool_invocations_handler`.
pub(crate) async fn list_checkpoints_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Query(query): Query<CheckpointListQuery>,
) -> impl IntoResponse {
    let Some(run_id) = query.run_id.as_deref() else {
        return validation_error_response("run_id is required");
    };

    // Shared `load_run_visible_to_tenant` helper keeps the gate in
    // sync with every other run read in cairn-app. Cross-tenant or
    // missing → same empty shape as "run with no checkpoints".
    // (Cursor bugbot #537.)
    let run_id = RunId::new(run_id);
    match crate::helpers::load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return (
                StatusCode::OK,
                Json(ListResponse::<cairn_domain::Checkpoint> {
                    items: Vec::new(),
                    has_more: false,
                }),
            )
                .into_response();
        }
        Err(resp) => return resp,
    }

    // #422: the service accepts a limit but not an offset — fetch
    // `limit + 1` rows so `has_more` reflects whether the run has
    // unreturned checkpoints. Operators can re-request with a larger
    // `limit` if they need more; offset-based paging through
    // checkpoints is not required by the UI today.
    let limit = query.limit();
    match state
        .runtime
        .checkpoints
        .list_by_run(&run_id, limit + 1)
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

/// `GET /v1/checkpoints/:id`
///
/// Tenant-scoped read — cross-tenant callers get 404.
pub(crate) async fn get_checkpoint_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match CheckpointReadModel::get(state.runtime.store.as_ref(), &CheckpointId::new(id)).await {
        Ok(Some(record)) if tenant_visible(&tenant_scope, &record.project) => {
            (StatusCode::OK, Json(record)).into_response()
        }
        Ok(Some(_)) | Ok(None) => checkpoint_not_found_response(),
        Err(err) => store_error_response(err),
    }
}

/// `POST /v1/checkpoints/:id/restore` — restore a run to a specific
/// checkpoint.
///
/// #369: tenant-scoped. This is the most dangerous endpoint in the
/// module — restoring a checkpoint rewinds the run and re-fires side
/// effects, so a cross-tenant restore could destroy inflight work in
/// another tenant. The check is applied BEFORE any event-log read so
/// a probe cannot learn which checkpoint ids exist via timing.
///
/// Alias for `POST /v1/runs/:run_id/replay-to-checkpoint?checkpoint_id=<id>`.
/// Looks up the checkpoint by ID to resolve the owning run, then
/// replays the run's event log up to the position where the
/// checkpoint was recorded.
pub(crate) async fn restore_checkpoint_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(checkpoint_id_str): Path<String>,
) -> impl IntoResponse {
    let checkpoint_id = CheckpointId::new(&checkpoint_id_str);

    // Resolve the checkpoint -> run_id + tenant-scope gate.
    let checkpoint =
        match CheckpointReadModel::get(state.runtime.store.as_ref(), &checkpoint_id).await {
            Ok(Some(c)) if tenant_visible(&tenant_scope, &c.project) => c,
            Ok(Some(_)) | Ok(None) => return checkpoint_not_found_response(),
            Err(err) => return store_error_response(err),
        };

    // Find the event-log position at which the checkpoint was recorded.
    let position = match checkpoint_recorded_position(
        state.runtime.store.as_ref(),
        &checkpoint.checkpoint_id,
        &checkpoint.run_id,
    )
    .await
    {
        Ok(Some(p)) => p,
        Ok(None) => {
            return AppApiError::new(
                StatusCode::NOT_FOUND,
                "not_found",
                "checkpoint event not found in event log",
            )
            .into_response();
        }
        Err(err) => return store_error_response(err),
    };

    match build_run_replay_result(state.as_ref(), &checkpoint.run_id, None, Some(position.0)).await
    {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(err) => store_error_response(err),
    }
}

/// `POST /v1/runs/:id/checkpoint` — save a checkpoint for a run.
///
/// #370: tenant-scoped. Non-admin callers cannot plant a checkpoint
/// on another tenant's run; the run lookup returns 404 when the caller
/// is out of scope.
pub(crate) async fn save_checkpoint_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(run_id): Path<String>,
    Json(body): Json<SaveCheckpointRequest>,
) -> impl IntoResponse {
    let run_id = RunId::new(run_id);
    // Shared helper — stays in lockstep with every other run mutation
    // in cairn-app. Missing / cross-tenant → 404. (Cursor bugbot #537.)
    let run =
        match crate::helpers::load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id)
            .await
        {
            Ok(Some(run)) => run,
            Ok(None) => return run_not_found_response(),
            Err(resp) => return resp,
        };

    let before = current_event_head(&state).await;
    match state
        .runtime
        .checkpoints
        .save(&run.project, &run_id, CheckpointId::new(body.checkpoint_id))
        .await
    {
        Ok(record) => {
            publish_runtime_frames_since(&state, before).await;
            (StatusCode::CREATED, Json(record)).into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

/// `GET /v1/runs/:id/checkpoint-strategy`
///
/// #371: tenant-scoped with admin bypass — the pre-fix shape missed
/// `is_admin`, so the admin token would 404 on any tenant other than
/// its own. Same class of bug as PR #337; aligned here to close the
/// gap.
pub(crate) async fn get_checkpoint_strategy_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(run_id): Path<String>,
) -> impl IntoResponse {
    let run_id = RunId::new(run_id);
    let run = match state.runtime.runs.get(&run_id).await {
        Ok(Some(run)) if tenant_visible(&tenant_scope, &run.project) => run,
        Ok(Some(_)) | Ok(None) => return run_not_found_response(),
        Err(err) => return runtime_error_response(err),
    };

    match CheckpointStrategyReadModel::get_by_run(state.runtime.store.as_ref(), &run.run_id).await {
        Ok(Some(strategy)) => (StatusCode::OK, Json(strategy)).into_response(),
        Ok(None) => AppApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "checkpoint strategy not found",
        )
        .into_response(),
        Err(err) => store_error_response(err),
    }
}

/// `POST /v1/runs/:id/checkpoint-strategy`
///
/// #371: tenant-scoped with admin bypass. Same shape change as the
/// companion GET above.
pub(crate) async fn set_checkpoint_strategy_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(run_id): Path<String>,
    Json(body): Json<SetCheckpointStrategyRequest>,
) -> impl IntoResponse {
    let run_id = RunId::new(run_id);
    let run = match state.runtime.runs.get(&run_id).await {
        Ok(Some(run)) if tenant_visible(&tenant_scope, &run.project) => run,
        Ok(Some(_)) | Ok(None) => return run_not_found_response(),
        Err(err) => return runtime_error_response(err),
    };

    let strategy = CheckpointStrategy {
        strategy_id: body.strategy_id.clone(),
        project: run.project.clone(),
        run_id: run.run_id.clone(),
        interval_ms: body.interval_ms,
        max_checkpoints: body.max_checkpoints,
        trigger_on_task_complete: body.trigger_on_task_complete,
    };

    // Emit the CheckpointStrategySet event with full fields so the projection
    // can restore them on query.
    let event =
        operator_event_envelope(RuntimeEvent::CheckpointStrategySet(CheckpointStrategySet {
            strategy_id: strategy.strategy_id.clone(),
            description: String::new(),
            set_at_ms: now_ms(),
            run_id: Some(run_id.clone()),
            interval_ms: body.interval_ms,
            max_checkpoints: body.max_checkpoints,
            trigger_on_task_complete: body.trigger_on_task_complete,
        }));

    let before = current_event_head(&state).await;
    match state.runtime.store.append(&[event]).await {
        Ok(_) => {
            publish_runtime_frames_since(&state, before).await;
            (StatusCode::OK, Json(strategy)).into_response()
        }
        Err(err) => store_error_response(err),
    }
}
