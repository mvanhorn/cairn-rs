//! Run lifecycle CRUD + transition handlers.
//!
//! Covers the core run state machine:
//! - `GET /v1/runs` — list runs scoped to a project
//! - `GET /v1/runs/:id` — per-run detail view with tasks + completion annotation
//! - `POST /v1/runs` — create a new run under a session
//! - `POST /v1/runs/:id/cancel` — cancel a run mid-execution
//! - `POST /v1/runs/:id/claim` — activate the run's FF execution lease
//! - `POST /v1/runs/:id/pause` / `resume` — operator-driven state transitions
//! - `POST /v1/runs/:id/spawn-subagent` — nest a child run under a parent
//! - `GET /v1/runs/:id/children` — list child runs
//! - `GET /v1/runs/due-resumes` / `POST /v1/runs/process-scheduled-resumes` —
//!   scheduler-driven resume fan-out
//! - `POST /v1/runs/:id/recover` — deprecated no-op stub (kept for v1 UI compat)

use std::sync::Arc;

use axum::{
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
    Json,
};

use cairn_api::auth::AuthPrincipal;
use cairn_api::http::{ApiError, ListResponse};
use cairn_domain::{
    PauseReason, PauseReasonKind, ProjectKey, ResumeTrigger, RunId, RunResumeTarget, RunState,
    SessionId, TaskId, WorkspaceRole,
};
use cairn_runtime::RuntimeError;
use cairn_store::projections::{PauseScheduleReadModel, TaskReadModel};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::current_event_head;
use crate::errors::{
    now_ms, parse_run_state, run_not_found_response, runtime_error_response, store_error_response,
    validation_error_response, AppApiError,
};
use crate::extractors::{HasProjectScope, ProjectJson, ProjectScope, TenantScope};
use crate::helpers::{build_run_record_view, load_run_visible_to_tenant};
use crate::middleware::ensure_workspace_role_for_project;
use crate::persist_run_mode_default;
use crate::persist_run_string_default;
use crate::publish_runtime_frames_since;
use crate::state::AppState;
use crate::{
    PaginationQuery, RunRecordView, DEFAULT_PROJECT_ID, DEFAULT_TENANT_ID, DEFAULT_WORKSPACE_ID,
};
#[allow(unused_imports)]
use crate::{RunListResponseDoc, RunRecordDoc};
use cairn_store::projections::TaskRecord;

// ── DTOs ─────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct SpawnSubagentRunResponse {
    pub(crate) parent_run_id: String,
    pub(crate) child_run_id: String,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct RunDetailResponse {
    pub(crate) run: RunRecordView,
    pub(crate) tasks: Vec<TaskRecord>,
    /// F47 PR2: completion annotation, if the run has terminated via
    /// `LoopTermination::Completed` and the `RunCompletionAnnotated`
    /// event has been projected onto the RunRecord. Absent for running
    /// / failed / canceled runs and for runs completed before F47 PR2
    /// shipped (no annotation ever landed on the event log).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) completion: Option<RunCompletion>,
}

/// F47 PR2: operator-visible shape of the run completion annotation on
/// `GET /v1/runs/:id`. Mirrors the fields on `RunCompletionAnnotated`
/// minus the ProjectKey / SessionId / RunId (already carried by the
/// parent `run` field).
#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct RunCompletion {
    pub(crate) summary: String,
    pub(crate) verification: cairn_domain::CompletionVerification,
    /// Wall-clock ms at which the orchestrator emitted the annotation.
    pub(crate) completed_at: u64,
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)] // T6a-H10: response shape widened to include `failures: Vec`.
pub(crate) struct ScheduledResumeProcessResponse {
    pub(crate) resumed_count: usize,
}

#[derive(Clone, Debug, Default, serde::Deserialize, ToSchema)]
pub(crate) struct RunListQuery {
    // Scope fields are optional at the HTTP boundary: bare calls
    // (e.g. first-load UI without localStorage scope) fall back to
    // the default tenant/workspace/project rather than 422-ing on
    // missing query params.
    #[serde(default)]
    pub(crate) tenant_id: Option<String>,
    #[serde(default)]
    pub(crate) workspace_id: Option<String>,
    #[serde(default)]
    pub(crate) project_id: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
}

impl RunListQuery {
    pub(crate) fn project(&self) -> ProjectKey {
        ProjectKey::new(
            self.tenant_id
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or(DEFAULT_TENANT_ID),
            self.workspace_id
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or(DEFAULT_WORKSPACE_ID),
            self.project_id
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or(DEFAULT_PROJECT_ID),
        )
    }

    pub(crate) fn limit(&self) -> usize {
        self.limit.unwrap_or(50).min(200)
    }

    pub(crate) fn offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }
}

impl HasProjectScope for RunListQuery {
    fn project(&self) -> ProjectKey {
        RunListQuery::project(self)
    }
}

#[derive(Clone, Debug, serde::Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateRunRequest {
    pub(crate) tenant_id: String,
    pub(crate) workspace_id: String,
    pub(crate) project_id: String,
    pub(crate) session_id: String,
    pub(crate) run_id: String,
    #[serde(default)]
    pub(crate) parent_run_id: Option<String>,
    /// RFC 018: execution mode (direct/plan/execute).
    #[serde(default)]
    #[schema(value_type = Option<String>)]
    pub(crate) mode: Option<cairn_domain::decisions::RunMode>,
    /// F42: operator-supplied natural-language objective for this run.
    ///
    /// When present, it is persisted as the run's "goal" default so the
    /// orchestrator's decide phase ships it to the LLM as the user
    /// message `## Goal` section. Without it, the orchestrator falls
    /// back to the generic "Execute the run objective." placeholder —
    /// which dogfood v8 proved is useless: the model has nothing to act
    /// on and produces a meta-reply ("no specific objective was
    /// provided").
    ///
    /// The field stays optional for back-compat with callers that still
    /// pass the goal on `POST /v1/runs/:id/orchestrate` instead. When
    /// both are supplied, the orchestrate-body goal wins (explicit
    /// per-invocation override beats the run default).
    #[serde(default)]
    pub(crate) prompt: Option<String>,
}

impl CreateRunRequest {
    pub(crate) fn project(&self) -> ProjectKey {
        ProjectKey::new(
            self.tenant_id.as_str(),
            self.workspace_id.as_str(),
            self.project_id.as_str(),
        )
    }

    /// SEC-002: reject control-character / empty / oversized inputs at
    /// the HTTP boundary before any id flows into FF's key builders
    /// (where a null-byte is a delimiter — see id_map.rs F02 fix). Must
    /// be called explicitly by every handler that consumes this struct;
    /// the `project()` accessor intentionally stays infallible so it can
    /// continue serving as the `HasProjectScope` impl.
    pub(crate) fn validate(&self) -> Result<(), String> {
        // F42: prompts are bounded at the HTTP boundary — an unbounded
        // string would bloat the defaults store and blow up the user
        // message sent to the LLM. Matches the ceiling used by the
        // prompt-authoring endpoint (`bin_providers.rs`).
        let prompt_len_check = self
            .prompt
            .as_deref()
            .map(|p| crate::validate::max_len_str("prompt", p, crate::validate::MAX_PROMPT_LEN))
            .unwrap_or(Ok(()));
        crate::validate::check_all(&[
            crate::validate::require_id("tenant_id", &self.tenant_id),
            crate::validate::require_id("workspace_id", &self.workspace_id),
            crate::validate::require_id("project_id", &self.project_id),
            crate::validate::require_id("session_id", &self.session_id),
            crate::validate::require_id("run_id", &self.run_id),
            crate::validate::valid_id("parent_run_id", &self.parent_run_id),
            prompt_len_check,
        ])
    }
}

impl HasProjectScope for CreateRunRequest {
    fn project(&self) -> ProjectKey {
        CreateRunRequest::project(self)
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
#[allow(dead_code)]
pub(crate) struct SpawnSubagentRunRequest {
    pub(crate) session_id: String,
    pub(crate) parent_task_id: Option<String>,
    pub(crate) child_task_id: Option<String>,
    pub(crate) child_run_id: Option<String>,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub(crate) struct PauseRunRequest {
    #[serde(alias = "kind")]
    pub(crate) reason_kind: Option<PauseReasonKind>,
    pub(crate) detail: Option<String>,
    pub(crate) actor: Option<String>,
    pub(crate) resume_after_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub(crate) struct ResumeRunRequest {
    pub(crate) trigger: Option<ResumeTrigger>,
    pub(crate) target: Option<RunResumeTarget>,
}

// ── Handlers ─────────────────────────────────────────────────────────────────

#[utoipa::path(
    get,
    path = "/v1/runs",
    tag = "runtime",
    responses(
        (status = 200, description = "Runs listed", body = RunListResponseDoc),
        (status = 400, description = "Invalid request", body = ApiError),
        (status = 401, description = "Unauthorized", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
pub(crate) async fn list_runs_handler(
    State(state): State<Arc<AppState>>,
    project_scope: ProjectScope<RunListQuery>,
) -> impl IntoResponse {
    let query = project_scope.into_inner();
    let status_filter = match query.status.as_deref().map(parse_run_state).transpose() {
        Ok(status_filter) => status_filter,
        Err(err) => return validation_error_response(err),
    };
    let session_id = query.session_id.as_deref().map(SessionId::new);
    let limit = query.limit();
    match state
        .runtime
        .store
        .list_runs_filtered(
            &RunListQuery::project(&query),
            session_id.as_ref(),
            status_filter,
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
        Err(err) => store_error_response(err),
    }
}

pub(crate) async fn get_run_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let run_id = RunId::new(id);
    match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(run)) => {
            // F47 PR2: pull completion annotation fields off the raw
            // RunRecord BEFORE wrapping in RunRecordView. The fields
            // survive on the projection regardless of view wrapping,
            // but resolving them here keeps the response shape explicit
            // (one `completion` object vs three scattered fields).
            let completion = match (
                run.completion_summary.clone(),
                run.completion_verification.clone(),
                run.completion_annotated_at_ms,
            ) {
                (Some(summary), Some(verification), Some(completed_at)) => Some(RunCompletion {
                    summary,
                    verification,
                    completed_at,
                }),
                // Partial annotation is a projection bug (all three
                // fields are written atomically by the applier). Fall
                // back to `None` rather than surfacing an inconsistent
                // half-shape to operators.
                _ => None,
            };
            let run = build_run_record_view(state.as_ref(), run).await;
            match TaskReadModel::list_by_parent_run(
                state.runtime.store.as_ref(),
                &run.run.run_id,
                200,
            )
            .await
            {
                Ok(tasks) => (
                    StatusCode::OK,
                    Json(RunDetailResponse {
                        run,
                        tasks,
                        completion,
                    }),
                )
                    .into_response(),
                Err(err) => store_error_response(err),
            }
        }
        Ok(None) => run_not_found_response(),
        Err(response) => response,
    }
}

#[utoipa::path(
    post,
    path = "/v1/runs",
    tag = "runtime",
    request_body = CreateRunRequest,
    responses(
        (status = 201, description = "Run created", body = RunRecordDoc),
        (status = 400, description = "Invalid request", body = ApiError),
        (status = 401, description = "Unauthorized", body = ApiError),
        (status = 404, description = "Session not found", body = ApiError),
        (status = 422, description = "Unprocessable entity", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
pub(crate) async fn create_run_handler(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<AuthPrincipal>,
    trace_id: Option<Extension<crate::middleware::TraceId>>,
    project_scope: ProjectJson<CreateRunRequest>,
) -> impl IntoResponse {
    let body = project_scope.into_inner();
    // SEC-002: validate tenant / workspace / project / session / run ids
    // before they flow through FF — null bytes, newlines, and oversized
    // fields must return 422, not propagate into Valkey key builders.
    if let Err(msg) = body.validate() {
        return validation_error_response(msg);
    }
    let project = CreateRunRequest::project(&body);
    if let Err(response) = ensure_workspace_role_for_project(
        state.as_ref(),
        &principal,
        &project,
        WorkspaceRole::Member,
    )
    .await
    {
        return response;
    }
    let session_id = SessionId::new(body.session_id.clone());
    // Scoped get (#439): `project` is already known from the request
    // body's HasProjectScope extractor, so route the lookup through the
    // service-layer scope check rather than the admin-only unchecked
    // path. A mismatched project returns `None` indistinguishable from
    // "unknown id", so no explicit post-check is needed.
    match state.runtime.sessions.get(&project, &session_id).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return AppApiError::new(StatusCode::NOT_FOUND, "not_found", "session not found")
                .into_response();
        }
        Err(err) => return runtime_error_response(err),
    }
    if let Some(parent_run_id) = body.parent_run_id.as_ref().map(RunId::new) {
        match state.runtime.runs.get(&parent_run_id).await {
            Ok(Some(parent_run)) if parent_run.project == project => {}
            Ok(Some(_)) | Ok(None) => {
                return AppApiError::new(
                    StatusCode::NOT_FOUND,
                    "not_found",
                    "parent run not found",
                )
                .into_response();
            }
            Err(err) => return runtime_error_response(err),
        }
    }
    let before = current_event_head(&state).await;
    // RFC 011: if the request arrived with an `x-trace-id` header the
    // middleware put it on extensions as a `TraceId`. Thread it through
    // to Fabric so the emitted `RunCreated` envelope's correlation_id
    // matches the trace id, making `GET /v1/trace/:id` non-empty.
    let correlation_id = trace_id.map(|Extension(t)| t.as_str().to_owned());
    let start_result = match correlation_id.as_deref() {
        Some(corr) if !corr.is_empty() => {
            state
                .runtime
                .runs
                .start_with_correlation(
                    &project,
                    &session_id,
                    RunId::new(body.run_id),
                    body.parent_run_id.map(RunId::new),
                    corr,
                )
                .await
        }
        _ => {
            state
                .runtime
                .runs
                .start(
                    &project,
                    &session_id,
                    RunId::new(body.run_id),
                    body.parent_run_id.map(RunId::new),
                )
                .await
        }
    };
    match start_result {
        Ok(run) => {
            if let Some(mode) = body.mode.as_ref() {
                if let Err(err) =
                    persist_run_mode_default(state.as_ref(), &project, &run.run_id, mode).await
                {
                    return runtime_error_response(err);
                }
            }
            // F42: persist the operator-supplied prompt as the run's
            // "goal" default so `POST /v1/runs/:id/orchestrate` reads it
            // back via `resolve_run_string_default(..., "goal")`. Empty
            // strings are rejected up-front: an empty prompt is almost
            // certainly a client bug, and routing "" through as the
            // goal would silently reproduce the dogfood-v8 symptom
            // (LLM sees no objective) rather than surface the mistake.
            if let Some(prompt) = body.prompt.as_ref() {
                if prompt.trim().is_empty() {
                    return validation_error_response(
                        "prompt: must be non-empty when present; omit the field to skip",
                    );
                }
                if let Err(err) = persist_run_string_default(
                    state.as_ref(),
                    &project,
                    &run.run_id,
                    "goal",
                    prompt,
                )
                .await
                {
                    return runtime_error_response(err);
                }
            }
            publish_runtime_frames_since(&state, before).await;
            let view = build_run_record_view(state.as_ref(), run).await;
            (StatusCode::CREATED, Json(view)).into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

/// `POST /v1/runs/:id/cancel` -- cancel a run mid-execution.
///
/// Transitions the run to `Canceled` state and updates the parent session.
pub(crate) async fn cancel_run_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let run_id = RunId::new(&id);

    // T6a-C2: verify tenant scope before any mutation.
    let run = match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(run)) => run,
        Ok(None) => {
            return run_not_found_response();
        }
        Err(response) => return response,
    };

    let before = current_event_head(&state).await;
    match state.runtime.runs.cancel(&run.session_id, &run_id).await {
        Ok(record) => {
            publish_runtime_frames_since(&state, before).await;
            (StatusCode::OK, Json(record)).into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

/// `POST /v1/runs/:id/claim` — activate the run's FF execution lease.
///
/// Required before `enter_waiting_approval`, `pause`, or any other
/// FCALL that rejects non-active executions (see
/// `RunService::claim` docstring for the full semantics). On the
/// Fabric path this walks `ff_issue_claim_grant` +
/// `ff_claim_execution` (with `ff_claim_resumed_execution` dispatch
/// when the execution is resuming from a prior suspension). On the
/// in-memory courtesy path this is a no-op that returns the current
/// record — there's no lease to activate.
///
/// **NOT idempotent.** Re-claiming an already-active run fails at
/// FF's grant gate with `execution_not_eligible` and surfaces as a
/// 500 here. Callers must claim once per lifecycle. See
/// `RunService::claim` docstring.
///
/// Get-first is belt-and-suspenders against projection staleness —
/// `FabricRunServiceAdapter::claim` already delegates through
/// `resolve_run_project`, which maps missing-in-store to
/// `RuntimeError::NotFound` → 404. Keeping the explicit lookup here
/// avoids relying on that transitive mapping and isolates the 404
/// response from any future change in the adapter layer.
///
/// No request body: runs are not worker-pulled, so the caller never
/// advertises worker identity through this endpoint (unlike
/// `POST /v1/tasks/:id/claim`, which takes `worker_id` +
/// `lease_duration_ms`). Fabric uses
/// `FabricConfig::worker_instance_id` + `lease_ttl_ms` from the
/// process config.
pub(crate) async fn claim_run_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let run_id = RunId::new(&id);

    // T6a-C2: tenant scope + explicit 404 before the adapter call so the
    // 404 path is isolated from future adapter changes.
    let run = match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(run)) => run,
        Ok(None) => {
            return run_not_found_response();
        }
        Err(response) => return response,
    };

    let before = current_event_head(&state).await;
    match state.runtime.runs.claim(&run.session_id, &run_id).await {
        Ok(record) => {
            publish_runtime_frames_since(&state, before).await;
            (StatusCode::OK, Json(record)).into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn pause_run_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
    Json(body): Json<PauseRunRequest>,
) -> impl IntoResponse {
    let run_id = RunId::new(id);

    // T6a-C2: tenant scope check before any mutation.
    let run = match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(run)) => run,
        Ok(None) => {
            return run_not_found_response();
        }
        Err(response) => return response,
    };

    let before = current_event_head(&state).await;
    let reason = PauseReason {
        kind: body.reason_kind.unwrap_or(PauseReasonKind::OperatorPause),
        detail: body.detail,
        resume_after_ms: body.resume_after_ms,
        actor: body.actor,
    };

    match state
        .runtime
        .runs
        .pause(&run.session_id, &run_id, reason)
        .await
    {
        Ok(run) => {
            publish_runtime_frames_since(&state, before).await;
            (StatusCode::OK, Json(run)).into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn resume_run_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
    Json(body): Json<ResumeRunRequest>,
) -> impl IntoResponse {
    let run_id = RunId::new(id);

    // T6a-C2: tenant scope check before any mutation.
    let run = match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(run)) => run,
        Ok(None) => {
            return run_not_found_response();
        }
        Err(response) => return response,
    };

    let before = current_event_head(&state).await;
    match state
        .runtime
        .runs
        .resume(
            &run.session_id,
            &run_id,
            body.trigger.unwrap_or(ResumeTrigger::OperatorResume),
            body.target.unwrap_or(RunResumeTarget::Running),
        )
        .await
    {
        Ok(run) => {
            publish_runtime_frames_since(&state, before).await;
            (StatusCode::OK, Json(run)).into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn list_due_run_resumes_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Query(query): Query<PaginationQuery>,
) -> impl IntoResponse {
    match PauseScheduleReadModel::list_due(state.runtime.store.as_ref(), now_ms()).await {
        Ok(due) => {
            let mut all = Vec::new();
            for record in due {
                if record.project.tenant_id != *tenant_scope.tenant_id() {
                    continue;
                }
                match state.runtime.runs.get(&record.run_id).await {
                    Ok(Some(run)) if run.state == RunState::Paused => all.push(run),
                    Ok(_) => {}
                    Err(err) => return runtime_error_response(err),
                }
            }
            // #422: honest pagination against the filtered total.
            let total = all.len();
            let offset = query.offset();
            let limit = query.limit();
            let items: Vec<_> = all.into_iter().skip(offset).take(limit).collect();
            let has_more = offset.saturating_add(items.len()) < total;
            (StatusCode::OK, Json(ListResponse { items, has_more })).into_response()
        }
        Err(err) => store_error_response(err),
    }
}

pub(crate) async fn process_scheduled_run_resumes_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
) -> impl IntoResponse {
    let due = match PauseScheduleReadModel::list_due(state.runtime.store.as_ref(), now_ms()).await {
        Ok(due) => due,
        Err(err) => return store_error_response(err),
    };

    let before = current_event_head(&state).await;
    let mut resumed_count = 0usize;
    // T6a-H10: aggregate per-run failures rather than short-circuiting.
    // Bailing mid-loop leaves already-resumed runs without a published
    // SSE frame and leaves the caller guessing about partial success.
    let mut failures: Vec<serde_json::Value> = Vec::new();
    for record in due {
        if record.project.tenant_id != *tenant_scope.tenant_id() {
            continue;
        }
        let session_id = match state.runtime.runs.get(&record.run_id).await {
            Ok(Some(run)) => run.session_id,
            Ok(None) => continue,
            Err(err) => {
                tracing::warn!(
                    run_id = %record.run_id,
                    error = %err,
                    "failed to load run for scheduled resume; skipping",
                );
                continue;
            }
        };
        match state
            .runtime
            .runs
            .resume(
                &session_id,
                &record.run_id,
                ResumeTrigger::ResumeAfterTimer,
                RunResumeTarget::Running,
            )
            .await
        {
            Ok(_) => resumed_count += 1,
            Err(RuntimeError::InvalidTransition { .. }) | Err(RuntimeError::NotFound { .. }) => {
                // Non-fatal per-run skip: run moved to terminal state
                // or disappeared between list_due and resume. Ignored
                // silently to match the prior contract.
            }
            Err(err) => {
                tracing::warn!(
                    run_id = %record.run_id,
                    error = %err,
                    "scheduled resume failed — continuing with remaining runs"
                );
                failures.push(serde_json::json!({
                    "run_id": record.run_id.to_string(),
                    "error": err.to_string(),
                }));
            }
        }
    }
    // Always publish whatever did succeed, even on partial failure.
    if resumed_count > 0 {
        publish_runtime_frames_since(&state, before).await;
    }
    // Keep camelCase `resumedCount` for backward compat with the existing
    // UI + integration test. Add `failures` as a new field so callers can
    // opt in to per-run error visibility without breaking old parsers.
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "resumedCount": resumed_count,
            "failures": failures,
        })),
    )
        .into_response()
}

pub(crate) async fn spawn_subagent_run_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
    Json(body): Json<SpawnSubagentRunRequest>,
) -> impl IntoResponse {
    let parent_run_id = RunId::new(id);
    let parent_run = match state.runtime.runs.get(&parent_run_id).await {
        Ok(Some(run))
            if tenant_scope.is_admin || run.project.tenant_id == *tenant_scope.tenant_id() =>
        {
            run
        }
        Ok(Some(_)) | Ok(None) => {
            return run_not_found_response();
        }
        Err(err) => return runtime_error_response(err),
    };

    let child_session_id = SessionId::new(body.session_id);
    // Scoped get (#439): the parent run's project is authoritative
    // here, so the child session must live in the same scope. The
    // service-layer scope check returns `None` for a cross-project
    // id, so no explicit post-check is needed.
    match state
        .runtime
        .sessions
        .get(&parent_run.project, &child_session_id)
        .await
    {
        Ok(Some(_)) => {}
        Ok(None) => {
            return AppApiError::new(StatusCode::NOT_FOUND, "not_found", "session not found")
                .into_response();
        }
        Err(err) => return runtime_error_response(err),
    }

    let _child_task_id = body
        .child_task_id
        .map(TaskId::new)
        .unwrap_or_else(|| TaskId::new(format!("task_subagent_{}", Uuid::new_v4())));
    let child_run_id = body
        .child_run_id
        .map(RunId::new)
        .unwrap_or_else(|| RunId::new(format!("run_subagent_{}", Uuid::new_v4())));
    let before = current_event_head(&state).await;
    match state
        .runtime
        .runs
        .spawn_subagent(
            &parent_run.project,
            parent_run_id.clone(),
            &child_session_id,
            Some(child_run_id),
        )
        .await
    {
        Ok(child_run) => {
            publish_runtime_frames_since(&state, before).await;
            (
                StatusCode::CREATED,
                Json(SpawnSubagentRunResponse {
                    parent_run_id: parent_run_id.to_string(),
                    child_run_id: child_run.run_id.to_string(),
                }),
            )
                .into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn list_child_runs_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
    Query(query): Query<PaginationQuery>,
) -> impl IntoResponse {
    let parent_run_id = RunId::new(id);
    let parent_run =
        match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &parent_run_id).await {
            Ok(Some(run)) => run,
            Ok(None) => {
                return run_not_found_response();
            }
            Err(response) => return response,
        };

    // #422: the service accepts a limit but not an offset — fetch
    // `limit + 1` rows and flip `has_more` on overflow. Offset-based
    // deep paging through children is not supported yet; operators
    // usually want the first N anyway.
    let limit = query.limit();
    match state
        .runtime
        .runs
        .list_child_runs(&parent_run.run_id, limit + 1)
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

/// Deprecated stub. Manual recovery used to drive cairn-side
/// `RecoveryServiceImpl::recover_interrupted_runs`, but recovery now lives
/// unconditionally in FlowFabric's background scanners
/// (`LeaseExpiryScanner`, `AttemptTimeoutScanner`,
/// `ExecutionDeadlineScanner`, `SuspensionTimeoutScanner`,
/// `DependencyReconciler`, `UnblockScanner`, etc. — 14 total). Calling this
/// endpoint no longer does anything beyond confirming the run exists.
///
/// Kept as a 202 stub so operator dashboards hitting `/v1/runs/:id/recover`
/// don't break. Scheduled for removal in v2.
///
/// #430: deprecation is signalled via RFC 8594 HTTP response headers
/// (`Deprecation` + `Sunset` + `Link`) rather than a `deprecated: true`
/// field in the response body. Header-based markers are what SDK
/// generators, API gateways, and proxies inspect for lifecycle
/// management; body markers leak into UI renders and are invisible to
/// tooling.
pub(crate) async fn recover_run_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let run_id = RunId::new(id);
    match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return run_not_found_response(),
        Err(response) => return response,
    }

    // RFC 8594 headers. `Deprecation: <http-date>` points to the date
    // the endpoint was deprecated — PAST tense (RFC 020 milestone
    // 2026-04-21 when the 14 FF background scanners took over
    // recovery; see project_rfc020_milestone_tracks_1_4_shipped.md).
    // `Sunset` points to the planned removal date (v1.0 cut, approx
    // 2027-04-28). `Link: rel="deprecation"` points SDK consumers at
    // human docs. Cursor review caught the prior future-dated
    // `Deprecation` header — RFC 8594 semantics require past-tense.
    let mut headers = HeaderMap::new();
    headers.insert(
        "deprecation",
        HeaderValue::from_static("Tue, 21 Apr 2026 00:00:00 GMT"),
    );
    headers.insert(
        "sunset",
        HeaderValue::from_static("Wed, 28 Apr 2027 00:00:00 GMT"),
    );
    headers.insert(
        "link",
        HeaderValue::from_static(
            "<https://github.com/avifenesh/cairn-rs/blob/main/docs/design/rfcs/\
             recovery-retirement.md>; rel=\"deprecation\"; type=\"text/html\"",
        ),
    );

    (
        StatusCode::ACCEPTED,
        headers,
        Json(serde_json::json!({
            "status": "accepted",
            "note": "recovery is handled by FlowFabric background scanners \
                     (lease_expiry, attempt_timeout, execution_deadline, \
                     suspension_timeout, dependency_reconciler, unblock_scanner, \
                     and 8 others); this endpoint is a no-op kept for \
                     backwards-compatibility and will be removed in v2. \
                     Inspect the `Deprecation` and `Sunset` response headers \
                     for the authoritative lifecycle signal (RFC 8594).",
        })),
    )
        .into_response()
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn req(
        tenant: &str,
        workspace: &str,
        project: &str,
        session: &str,
        run: &str,
    ) -> CreateRunRequest {
        CreateRunRequest {
            tenant_id: tenant.into(),
            workspace_id: workspace.into(),
            project_id: project.into(),
            session_id: session.into(),
            run_id: run.into(),
            parent_run_id: None,
            mode: None,
            prompt: None,
        }
    }

    #[test]
    fn validate_accepts_normal_ids() {
        assert!(req("t1", "w1", "p1", "s1", "r1").validate().is_ok());
    }

    /// SEC-002: embedded NUL bytes are FF delimiters under RFC-011; rejecting
    /// them at the HTTP boundary prevents tenant-scope collapse via id_map.
    #[test]
    fn validate_rejects_tenant_id_with_null_byte() {
        let r = req("tenant\0bad", "w1", "p1", "s1", "r1");
        let err = r.validate().unwrap_err();
        assert!(err.contains("tenant_id"));
        assert!(err.contains("control characters"));
    }

    #[test]
    fn validate_rejects_workspace_id_with_soh() {
        let r = req("t1", "ws\x01bad", "p1", "s1", "r1");
        let err = r.validate().unwrap_err();
        assert!(err.contains("workspace_id"));
        assert!(err.contains("control characters"));
    }

    #[test]
    fn validate_rejects_project_id_with_newline() {
        let r = req("t1", "w1", "proj\nbad", "s1", "r1");
        let err = r.validate().unwrap_err();
        assert!(err.contains("project_id"));
        assert!(err.contains("control characters"));
    }

    #[test]
    fn validate_rejects_empty_run_id() {
        let r = req("t1", "w1", "p1", "s1", "");
        let err = r.validate().unwrap_err();
        assert!(err.contains("run_id"));
        assert!(err.contains("required"));
    }

    #[test]
    fn validate_rejects_empty_session_id() {
        let r = req("t1", "w1", "p1", "", "r1");
        let err = r.validate().unwrap_err();
        assert!(err.contains("session_id"));
        assert!(err.contains("required"));
    }

    #[test]
    fn validate_rejects_oversized_tenant_id() {
        let r = req(
            &"x".repeat(crate::validate::MAX_ID_LEN + 1),
            "w1",
            "p1",
            "s1",
            "r1",
        );
        let err = r.validate().unwrap_err();
        assert!(err.contains("tenant_id"));
        assert!(err.contains("maximum length"));
    }

    /// parent_run_id is optional — absent or empty is ok, control-chars are not.
    #[test]
    fn validate_parent_run_id_optional_but_checked() {
        let mut r = req("t1", "w1", "p1", "s1", "r1");
        assert!(r.validate().is_ok());
        r.parent_run_id = Some("parent\x07id".into());
        assert!(r.validate().is_err());
    }

    /// F42: prompt field has the standard `MAX_PROMPT_LEN` bound. An
    /// unbounded value would bloat the per-run defaults store and blow
    /// up the LLM user message — the handler-side 422 makes the limit
    /// actionable.
    #[test]
    fn validate_rejects_oversized_prompt() {
        let mut r = req("t1", "w1", "p1", "s1", "r1");
        r.prompt = Some("x".repeat(crate::validate::MAX_PROMPT_LEN + 1));
        let err = r.validate().unwrap_err();
        assert!(err.contains("prompt"));
        assert!(err.contains("exceeds maximum length"));
    }

    /// F42: a prompt exactly at the limit must pass — off-by-one
    /// guard.
    #[test]
    fn validate_accepts_prompt_at_limit() {
        let mut r = req("t1", "w1", "p1", "s1", "r1");
        r.prompt = Some("x".repeat(crate::validate::MAX_PROMPT_LEN));
        assert!(r.validate().is_ok());
    }
}
