//! Run HTTP handlers and request/response DTOs.

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
    CheckpointId, MailboxMessageId, PauseReason, PauseReasonKind, ProjectKey, ResumeTrigger, RunId,
    RunResumeTarget, RunState, RunStateChanged, RuntimeEvent, SessionId, StateTransition, TaskId,
    WorkspaceRole,
};
use cairn_runtime::{
    MailboxService, NotificationService, RunCostAlertService, RunSlaService, RuntimeError,
};
use cairn_store::projections::{
    ApprovalReadModel, AuditLogReadModel, CheckpointReadModel, OperatorInterventionReadModel,
    PauseScheduleReadModel, RecoveryEscalationReadModel, RunCostReadModel, RunReadModel,
    SessionCostReadModel, TaskReadModel,
};
use cairn_store::{EntityRef, EventLog, EventPosition, StoredEvent};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::errors::{
    api_error_with_details, bad_request_response, now_ms, operator_event_envelope, parse_run_state,
    run_not_found_response, runtime_error_response, store_error_response,
    validation_error_response, AppApiError,
};
use crate::extractors::{HasProjectScope, ProjectJson, ProjectScope, TenantCostQuery, TenantScope};
use crate::helpers::{
    build_diagnosis_report, build_run_record_view, build_run_replay_result,
    checkpoint_recorded_position, load_run_visible_to_tenant, working_dir_for_run,
};
use crate::middleware::ensure_workspace_role_for_project;
use crate::sandbox::workspace_error_response;
use crate::state::AppState;
use crate::{
    append_run_intervention_event, current_event_head, event_message, event_type_name,
    persist_run_mode_default, persist_run_string_default, publish_runtime_frames_since,
    resolve_run_mode_default, resolve_run_string_default, PaginationQuery, RunRecordView,
    DEFAULT_PROJECT_ID, DEFAULT_TENANT_ID, DEFAULT_WORKSPACE_ID,
};
#[allow(unused_imports)]
use crate::{RunListResponseDoc, RunRecordDoc};
use cairn_store::projections::{RunRecord, TaskRecord};

// ── Constants ────────────────────────────────────────────────────────────────

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
pub(crate) struct AuditEntry {
    #[serde(rename = "type")]
    pub(crate) entry_type: String,
    pub(crate) timestamp_ms: u64,
    pub(crate) description: String,
    pub(crate) actor: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct AuditTrail {
    pub(crate) run_id: String,
    pub(crate) entries: Vec<AuditEntry>,
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RunInterventionResponse {
    pub(crate) ok: bool,
    pub(crate) run: Option<RunRecord>,
    pub(crate) message_id: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)] // T6a-H10: response shape widened to include `failures: Vec`.
pub(crate) struct ScheduledResumeProcessResponse {
    pub(crate) resumed_count: usize,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct RunReplayQuery {
    pub(crate) from_position: Option<u64>,
    pub(crate) to_position: Option<u64>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct StalledRunsQuery {
    pub(crate) minutes: Option<u64>,
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
}

impl StalledRunsQuery {
    pub(crate) fn stale_after_ms(&self) -> u64 {
        self.minutes.unwrap_or(30).saturating_mul(60_000)
    }

    pub(crate) fn limit(&self) -> usize {
        self.limit.unwrap_or(100)
    }

    pub(crate) fn offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }
}

/// Settings-defaults key for the stuck-run threshold (system scope, milliseconds).
pub(crate) const STUCK_RUN_THRESHOLD_KEY: &str = "stuck_run_threshold_ms";

/// Read the system-scope `stuck_run_threshold_ms` default, if set.
///
/// Returns `None` when unset, when the stored value is not a non-negative
/// whole number, OR when the projection read fails — in the last case the
/// error is logged at `warn` and callers fall back to the hard-coded
/// default so a transient store outage never turns `/v1/runs/stalled` into
/// a 500. An operator-visible store outage is already surfaced by the
/// dedicated store-health surface (`GET /v1/status`).
pub(crate) async fn resolve_stuck_run_threshold_ms(state: &AppState) -> Option<u64> {
    use cairn_domain::Scope;
    use cairn_store::projections::DefaultsReadModel;

    let record = match DefaultsReadModel::get(
        state.runtime.store.as_ref(),
        Scope::System,
        "system",
        STUCK_RUN_THRESHOLD_KEY,
    )
    .await
    {
        Ok(Some(r)) => r,
        Ok(None) => return None,
        Err(err) => {
            tracing::warn!(
                error = %err,
                key = STUCK_RUN_THRESHOLD_KEY,
                "failed to read stuck-run threshold default; falling back to hard-coded value"
            );
            return None;
        }
    };
    // Accept both JSON integer and JSON float forms — validation
    // guarantees the stored number is whole and within u64 range.
    record.value.as_u64().or_else(|| {
        record.value.as_f64().and_then(|n| {
            if n.is_finite() && n.fract() == 0.0 && n >= 0.0 && n <= u64::MAX as f64 {
                Some(n as u64)
            } else {
                None
            }
        })
    })
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct ReplayToCheckpointQuery {
    pub(crate) checkpoint_id: String,
}

/// Paginated event query params: cursor (exclusive lower bound) + limit.
#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct EventsPageQuery {
    pub(crate) cursor: Option<u64>,
    /// Alias for cursor (legacy/test compatibility): return events as a plain array.
    pub(crate) from: Option<u64>,
    pub(crate) limit: Option<usize>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct EventSummary {
    pub(crate) position: u64,
    pub(crate) event_type: String,
    pub(crate) occurred_at_ms: u64,
    pub(crate) description: String,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct EventsPage {
    pub(crate) events: Vec<EventSummary>,
    pub(crate) next_cursor: Option<u64>,
    pub(crate) has_more: bool,
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

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RunInterventionAction {
    ForceComplete,
    ForceFail,
    ForceRestart,
    InjectMessage,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct RunInterventionRequest {
    pub(crate) action: RunInterventionAction,
    pub(crate) reason: String,
    pub(crate) message_body: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct SetRunCostAlertRequest {
    /// T6a-C2: tenant_id is accepted in the body for schema compat but
    /// ignored — the handler uses the resolved run's tenant_id instead.
    #[serde(default, rename = "tenant_id")]
    pub(crate) _tenant_id_deprecated: Option<String>,
    pub(crate) threshold_micros: u64,
}

/// Response returned on POST /v1/runs/:id/cost-alert so callers can
/// render the configured threshold without a follow-up GET. Closes #431
/// (also noted: the sister save-checkpoint endpoint already returns the
/// created record).
#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct RunCostAlertResponse {
    pub(crate) run_id: String,
    pub(crate) tenant_id: String,
    pub(crate) threshold_micros: u64,
}

// ── Plan review request DTOs (RFC 018) — #427 ────────────────────────────────
//
// Before #427 these three endpoints accepted `serde_json::Value` and
// plucked fields by name. Consequences logged in the audit: unknown
// fields silently accepted (typos in UI code went through audit as
// "plan approved with no comment"), wrong types silently ignored,
// OpenAPI could not describe the shape, no input size cap.
//
// Typed structs with `deny_unknown_fields` turn typos into 422s and
// let utoipa derive a ToSchema so the OpenAPI spec is accurate.

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApprovePlanRequest {
    #[serde(default)]
    pub(crate) reviewer_comments: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RejectPlanRequest {
    /// Optional operator-provided reason. When absent the handler
    /// substitutes "rejected by operator" (pre-#427 behaviour).
    #[serde(default)]
    pub(crate) reason: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RevisePlanRequest {
    /// Required — a revise without reviewer_comments is a no-op from
    /// the plan-author's perspective.
    pub(crate) reviewer_comments: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct SetRunSlaRequest {
    /// T6a-C2: tenant_id is accepted in the body for schema compat but
    /// ignored — the handler uses the resolved run's tenant_id instead.
    #[serde(default, rename = "tenant_id")]
    pub(crate) _tenant_id_deprecated: Option<String>,
    pub(crate) target_completion_ms: u64,
    #[serde(default = "default_alert_pct")]
    pub(crate) alert_at_percent: u8,
}

fn default_alert_pct() -> u8 {
    80
}

/// Body for `POST /v1/runs/:id/orchestrate`.
///
/// Cairn owns model selection: the caller describes the task (goal, mode,
/// iteration/timeout/approval budgets) and the control plane picks the
/// model from the tenant's configured provider bindings. Legacy clients
/// that still send a `model_id` field are tolerated because this struct
/// does NOT use `serde(deny_unknown_fields)` — by default Serde ignores
/// unknown fields on Deserialize, so the legacy payload is accepted
/// without error and the field is discarded.
#[derive(serde::Deserialize)]
pub(crate) struct OrchestrateRequest {
    #[serde(default)]
    pub(crate) goal: Option<String>,
    #[serde(default)]
    pub(crate) max_iterations: Option<u32>,
    #[serde(default)]
    pub(crate) timeout_ms: Option<u64>,
    /// RFC 018: execution mode override for this orchestration.
    #[serde(default)]
    pub(crate) mode: Option<cairn_domain::decisions::RunMode>,
    /// BP-v2: maximum milliseconds the execute phase will block waiting on
    /// operator approval for each in-flight tool call before auto-rejecting
    /// with a "timeout" reason. Defaults to 24h (86_400_000 ms) when unset.
    ///
    /// The HTTP client can shorten this for automated flows — e.g. a
    /// GitHub-webhook-driven run that should expire after 15 minutes if
    /// no reviewer shows up. Operator-approved amendments to args are
    /// still honoured as long as the decision arrives inside the window.
    #[serde(default)]
    pub(crate) approval_timeout_ms: Option<u64>,
    /// F65 PR-3: per-run circuit-breaker overrides. Any subset of the
    /// four breakers may be tightened below the operator-configured
    /// defaults resolved via `RuntimeConfig` (store → env → default).
    ///
    /// Tighten-only: overrides that exceed the configured defaults are
    /// rejected with HTTP 400 `invalid_breaker_override`. This keeps
    /// per-run requests from bypassing operator-wide safety caps.
    #[serde(default)]
    pub(crate) breaker_overrides: Option<BreakerOverrides>,
}

/// F65 PR-3: subset of breaker caps a caller may tighten on a single run.
///
/// All fields are optional. An unset field inherits the operator-configured
/// default resolved via `RuntimeConfig`. Present fields MUST be tighter
/// (lower) than the corresponding default — HTTP 400 otherwise.
#[derive(serde::Deserialize, Debug, Clone, Default, utoipa::ToSchema)]
pub(crate) struct BreakerOverrides {
    #[serde(default)]
    pub(crate) round_cap: Option<u32>,
    #[serde(default)]
    pub(crate) token_cap: Option<u64>,
    #[serde(default)]
    pub(crate) no_tool_use_streak: Option<u32>,
    #[serde(default)]
    pub(crate) wall_clock_ms: Option<u64>,
    /// #479: per-run override for the warning-threshold ratio. Basis
    /// points, 10_000 = 100 %. Tighten-only — the override MUST be less
    /// than or equal to the configured default so callers can only make
    /// warnings fire earlier, never later (lower bps = earlier warning).
    #[serde(default)]
    pub(crate) warn_ratio_bps: Option<u32>,
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
        Err(err) => return bad_request_response(err),
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

pub(crate) async fn list_stalled_runs_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Query(query): Query<StalledRunsQuery>,
) -> impl IntoResponse {
    // Resolve stale threshold: explicit `?minutes=` query wins; otherwise read
    // the system-scope `stuck_run_threshold_ms` default; finally fall back to
    // the hard-coded 30-minute default baked into `StalledRunsQuery`.
    let stale_after_ms = if query.minutes.is_some() {
        query.stale_after_ms()
    } else {
        resolve_stuck_run_threshold_ms(state.as_ref())
            .await
            .unwrap_or_else(|| query.stale_after_ms())
    };

    // Cover both Running and Pending so zombie pending runs (that never
    // started) surface as stalled — see F29 CD dogfood blocker.
    let mut candidate_runs = Vec::new();
    for target_state in [RunState::Running, RunState::Pending] {
        match RunReadModel::list_by_state(state.runtime.store.as_ref(), target_state, 10_000).await
        {
            Ok(runs) => candidate_runs.extend(runs),
            Err(err) => return store_error_response(err),
        }
    }

    let mut all = Vec::new();
    for run in candidate_runs {
        // Admin service account sees all tenants; operator tenants are
        // restricted to their own runs.
        if !tenant_scope.is_admin && run.project.tenant_id != *tenant_scope.tenant_id() {
            continue;
        }

        match build_diagnosis_report(state.as_ref(), &run, stale_after_ms).await {
            Ok((report, true)) => all.push(report),
            Ok((_report, false)) => {}
            Err(err) => return store_error_response(err),
        }
    }

    // #422: honest pagination. Stalled-run diagnosis is assembled in
    // memory from two state scans (Running + Pending) filtered by
    // staleness. Apply limit/offset against the filtered total so the
    // UI load-more works for large backlogs.
    let total = all.len();
    let offset = query.offset();
    let limit = query.limit();
    let items: Vec<_> = all.into_iter().skip(offset).take(limit).collect();
    let has_more = offset.saturating_add(items.len()) < total;

    (StatusCode::OK, Json(ListResponse { items, has_more })).into_response()
}

/// `GET /v1/runs/:id/telemetry` — live-aggregated per-run telemetry.
///
/// Aggregates the run state, all provider calls, and all tool invocations
/// into a single JSON payload suitable for the operator observability panel.
/// See F29 CD.
pub(crate) async fn get_run_telemetry_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let run_id = id;
    use cairn_store::projections::{ProviderCallReadModel, ToolInvocationReadModel};

    let run_id = RunId::new(run_id);
    let store = state.runtime.store.as_ref();

    let run = match RunReadModel::get(store, &run_id).await {
        Ok(Some(r)) => r,
        Ok(None) => return run_not_found_response(),
        Err(err) => return store_error_response(err),
    };

    // Tenant scoping. Admin service account + System principals bypass —
    // operator tenants are restricted to their own runs.
    if !tenant_scope.is_admin && run.project.tenant_id != *tenant_scope.tenant_id() {
        return run_not_found_response();
    }

    // Stuck-flag derivation (reuse the stuck-threshold default).
    let stale_after_ms = resolve_stuck_run_threshold_ms(state.as_ref())
        .await
        .unwrap_or(30 * 60_000);
    let now = now_ms();
    let non_terminal = matches!(run.state, RunState::Pending | RunState::Running);
    let stuck = non_terminal && now.saturating_sub(run.updated_at) > stale_after_ms;
    let stuck_since_ms = if stuck { Some(run.updated_at) } else { None };

    let provider_calls = match ProviderCallReadModel::list_by_run(store, &run_id, 1000).await {
        Ok(calls) => calls,
        Err(err) => return store_error_response(err),
    };
    let tool_invocations = match ToolInvocationReadModel::list_by_run(store, &run_id, 1000, 0).await
    {
        Ok(invs) => invs,
        Err(err) => return store_error_response(err),
    };

    // Totals + row serialization.
    let mut total_cost_micros: u128 = 0;
    let mut total_input_tokens: u128 = 0;
    let mut total_output_tokens: u128 = 0;
    let mut total_errors: u64 = 0;
    let mut wall_min: Option<u64> = None;
    let mut wall_max: Option<u64> = None;

    let provider_rows: Vec<serde_json::Value> = provider_calls
        .iter()
        .map(|c| {
            total_cost_micros =
                total_cost_micros.saturating_add(c.cost_micros.unwrap_or(0) as u128);
            total_input_tokens =
                total_input_tokens.saturating_add(c.input_tokens.unwrap_or(0) as u128);
            total_output_tokens =
                total_output_tokens.saturating_add(c.output_tokens.unwrap_or(0) as u128);
            if c.status != cairn_domain::providers::ProviderCallStatus::Succeeded {
                total_errors = total_errors.saturating_add(1);
            }
            if c.started_at_ms > 0 {
                wall_min = Some(wall_min.map_or(c.started_at_ms, |m| m.min(c.started_at_ms)));
            }
            if c.finished_at_ms > 0 {
                wall_max = Some(wall_max.map_or(c.finished_at_ms, |m| m.max(c.finished_at_ms)));
            }
            let latency_ms = c.latency_ms.unwrap_or_else(|| {
                if c.finished_at_ms >= c.started_at_ms && c.started_at_ms > 0 {
                    c.finished_at_ms - c.started_at_ms
                } else {
                    0
                }
            });
            // error_class is Serialize via its enum derive; render as snake_case string.
            let error_class = c
                .error_class
                .as_ref()
                .map(|ec| serde_json::to_value(ec).unwrap_or(serde_json::Value::Null));
            serde_json::json!({
                "provider_call_id": c.provider_call_id.as_str(),
                "model": c.provider_model_id.as_str(),
                "status": c.status,
                "input_tokens": c.input_tokens.unwrap_or(0),
                "output_tokens": c.output_tokens.unwrap_or(0),
                "cost_micros": c.cost_micros.unwrap_or(0),
                "latency_ms": latency_ms,
                "started_at_ms": c.started_at_ms,
                "finished_at_ms": c.finished_at_ms,
                "error_class": error_class,
                "error_message": redact_provider_error(c.raw_error_message.as_deref()),
            })
        })
        .collect();

    let tool_rows: Vec<serde_json::Value> = tool_invocations
        .iter()
        .map(|t| {
            // Count Failed / Canceled tool invocations toward totals.errors
            // so operator dashboards don't under-report when a run's
            // errors all live on the tool side.
            if matches!(
                t.state,
                cairn_domain::tool_invocation::ToolInvocationState::Failed
                    | cairn_domain::tool_invocation::ToolInvocationState::Canceled
            ) {
                total_errors = total_errors.saturating_add(1);
            }

            // Wall bounds:
            // - lower bound = start_at_ms when present (any state).
            // - upper bound = finish_at_ms when present; for in-flight
            //   invocations (started, not terminal, no finish_at), treat
            //   `now` as the effective end so wall_ms reflects ongoing
            //   work instead of 0.
            if let Some(start) = t.started_at_ms {
                if start > 0 {
                    wall_min = Some(wall_min.map_or(start, |m| m.min(start)));
                }
            }
            if let Some(end) = t.finished_at_ms {
                if end > 0 {
                    wall_max = Some(wall_max.map_or(end, |m| m.max(end)));
                }
            } else if let Some(start) = t.started_at_ms {
                // In-flight invocation — extend wall to `now`.
                if start > 0 && !t.state.is_terminal() {
                    wall_max = Some(wall_max.map_or(now, |m| m.max(now)));
                }
            }
            let duration_ms = match (t.started_at_ms, t.finished_at_ms) {
                (Some(s), Some(f)) if f >= s => f - s,
                (Some(s), None) if s > 0 && !t.state.is_terminal() && now >= s => now - s,
                _ => 0,
            };
            let tool_name = match &t.target {
                cairn_domain::tool_invocation::ToolInvocationTarget::Builtin { tool_name } => {
                    tool_name.as_str()
                }
                cairn_domain::tool_invocation::ToolInvocationTarget::Plugin {
                    tool_name, ..
                } => tool_name.as_str(),
            };
            // F55: thread args + output preview into the telemetry
            // payload so RunDetailPage can show "what cairn ran" and
            // "what cairn got back" inline on the tool-invocation row.
            //
            // Strip the truncation marker from `output_preview` here too
            // so the UI can rely on `output_truncated` as the sole source
            // of truth for "(truncated)" badges.
            let raw_preview = t.output_preview.as_deref();
            let output_truncated = raw_preview
                .map(|p| {
                    p.ends_with(cairn_domain::tool_invocation::TOOL_OUTPUT_PREVIEW_TRUNCATED_SUFFIX)
                })
                .unwrap_or(false);
            let output_preview = raw_preview.map(|p| {
                if output_truncated {
                    p.strip_suffix(
                        cairn_domain::tool_invocation::TOOL_OUTPUT_PREVIEW_TRUNCATED_SUFFIX,
                    )
                    .unwrap_or(p)
                    .to_owned()
                } else {
                    p.to_owned()
                }
            });
            serde_json::json!({
                "invocation_id": t.invocation_id.as_str(),
                "tool_name": tool_name,
                "status": t.state,
                "started_at_ms": t.started_at_ms.unwrap_or(0),
                "finished_at_ms": t.finished_at_ms.unwrap_or(0),
                "duration_ms": duration_ms,
                "args": t.args_json.as_ref(),
                "output_preview": output_preview,
                "output_truncated": output_truncated,
                "error_message": t.error_message.as_deref(),
            })
        })
        .collect();

    let wall_ms = match (wall_min, wall_max) {
        (Some(a), Some(b)) if b >= a => b - a,
        _ => 0,
    };

    let body = serde_json::json!({
        "run_id": run.run_id.to_string(),
        "state": run.state,
        "stuck": stuck,
        "stuck_since_ms": stuck_since_ms,
        "provider_calls": provider_rows,
        "tool_invocations": tool_rows,
        "totals": {
            // Clamp u128 → u64 to avoid modular wrap on pathological runs.
            "cost_micros": u128::min(total_cost_micros, u64::MAX as u128) as u64,
            "input_tokens": u128::min(total_input_tokens, u64::MAX as u128) as u64,
            "output_tokens": u128::min(total_output_tokens, u64::MAX as u128) as u64,
            "provider_calls": provider_calls.len() as u64,
            "tool_calls": tool_invocations.len() as u64,
            "errors": total_errors,
            "wall_ms": wall_ms,
        },
        "phase_timings": {},
    });

    (StatusCode::OK, Json(body)).into_response()
}

/// Redact unsafe provider error strings for telemetry responses.
///
/// Provider-layer errors already flow through `cairn_providers::redact` —
/// this is the belt-and-suspenders guard at the API boundary:
///
/// - `None` input → `None` output.
/// - String with a live-looking `bearer ... sk-...` pattern → replaced
///   with the fixed marker `"<redacted: leaked credential pattern>"`.
///   The returned `Option` is always `Some` here — downstream JSON
///   callers see the marker, not `null`, so the UI can distinguish
///   "redacted" from "no error at all".
/// - Otherwise → pass through; if longer than 1024 chars, truncate
///   with a trailing ellipsis.
fn redact_provider_error(raw: Option<&str>) -> Option<String> {
    let raw = raw?;
    let lower = raw.to_ascii_lowercase();
    if lower.contains("bearer ") && lower.contains("sk-") {
        // Likely still carries a live key. Drop rather than leak.
        return Some("<redacted: leaked credential pattern>".to_owned());
    }
    const MAX: usize = 1024;
    if raw.len() > MAX {
        let mut s = raw.chars().take(MAX).collect::<String>();
        s.push_str("...");
        Some(s)
    } else {
        Some(raw.to_owned())
    }
}

pub(crate) async fn list_escalated_runs_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Query(query): Query<PaginationQuery>,
) -> impl IntoResponse {
    // #422: escalations per tenant are small (typically <50), but the
    // read model returns them all in one call. Apply limit/offset
    // in-memory and emit an honest `has_more`.
    match RecoveryEscalationReadModel::list_by_tenant(
        state.runtime.store.as_ref(),
        tenant_scope.tenant_id(),
    )
    .await
    {
        Ok(all) => {
            let total = all.len();
            let offset = query.offset();
            let limit = query.limit();
            let items: Vec<_> = all.into_iter().skip(offset).take(limit).collect();
            let has_more = offset.saturating_add(items.len()) < total;
            (
                StatusCode::OK,
                Json(ListResponse::<cairn_domain::recovery::RecoveryEscalation> {
                    items,
                    has_more,
                }),
            )
                .into_response()
        }
        Err(err) => store_error_response(err),
    }
}

pub(crate) async fn diagnose_run_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let run_id = RunId::new(id);
    let run = match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(run)) => run,
        Ok(None) => {
            return run_not_found_response();
        }
        Err(response) => return response,
    };

    match build_diagnosis_report(state.as_ref(), &run, 30 * 60_000).await {
        Ok((report, _)) => (StatusCode::OK, Json(report)).into_response(),
        Err(err) => store_error_response(err),
    }
}

pub(crate) async fn get_run_audit_trail_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let run_id = RunId::new(id.clone());

    // Validate run exists and belongs to tenant — same projection path
    // the rest of the run-read handlers use (F31 fix).
    match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return run_not_found_response();
        }
        Err(response) => return response,
    }

    // Read all events for this run from the event log
    let stored_events = match state
        .runtime
        .store
        .read_by_entity(&EntityRef::Run(run_id.clone()), None, 1000)
        .await
    {
        Ok(events) => events,
        Err(err) => return store_error_response(err),
    };

    let mut entries: Vec<AuditEntry> = Vec::new();
    for stored in &stored_events {
        entries.push(AuditEntry {
            entry_type: "event".to_owned(),
            timestamp_ms: stored.stored_at,
            description: event_message(&stored.envelope.payload),
            actor: None,
        });
        // Synthesize an initial-state entry right after RunCreated
        if matches!(&stored.envelope.payload, RuntimeEvent::RunCreated(_)) {
            entries.push(AuditEntry {
                entry_type: "event".to_owned(),
                timestamp_ms: stored.stored_at,
                description: format!("Run {} entered state Pending", run_id.as_str()),
                actor: None,
            });
        }
    }

    // Read audit log entries for this run
    let audit_logs = match AuditLogReadModel::list_by_resource(
        state.runtime.store.as_ref(),
        "run",
        run_id.as_str(),
    )
    .await
    {
        Ok(logs) => logs,
        Err(err) => return store_error_response(err),
    };

    entries.extend(audit_logs.into_iter().map(|entry| AuditEntry {
        entry_type: "audit".to_owned(),
        timestamp_ms: entry.occurred_at_ms,
        description: entry.action.clone(),
        actor: Some(entry.actor_id.clone()),
    }));

    entries.sort_by_key(|e| e.timestamp_ms);

    (
        StatusCode::OK,
        Json(AuditTrail {
            run_id: id,
            entries,
        }),
    )
        .into_response()
}

pub(crate) async fn list_run_events_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
    Query(query): Query<EventsPageQuery>,
) -> impl IntoResponse {
    let run_id = RunId::new(id);
    let run = match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(run)) => run,
        Ok(None) => {
            return run_not_found_response();
        }
        Err(response) => return response,
    };

    let limit = query.limit.unwrap_or(50).clamp(1, 500);
    // #429: response shape is ALWAYS `EventsPage { events, next_cursor,
    // has_more }`. The earlier dual-shape branch (plain array when
    // `from=N` was passed) violated the 'pick one' rule — OpenAPI
    // could only describe one shape and SDK generators choked. The
    // legacy `from=N` query param is still honoured as an alias for
    // `cursor=N`, but the response wrapper is unconditional.
    let cursor = query.cursor.or(query.from).map(EventPosition);

    // Fetch one extra to detect whether more pages exist
    let fetched = match state
        .runtime
        .store
        .read_by_entity(&EntityRef::Run(run.run_id.clone()), cursor, limit + 1)
        .await
    {
        Ok(events) => events,
        Err(err) => return store_error_response(err),
    };

    let has_more = fetched.len() > limit;
    let page: Vec<StoredEvent> = fetched.into_iter().take(limit).collect();
    let next_cursor = if has_more {
        page.last().map(|e| e.position.0)
    } else {
        None
    };

    let events: Vec<EventSummary> = page
        .into_iter()
        .map(|e| EventSummary {
            position: e.position.0,
            event_type: event_type_name(&e.envelope.payload).to_owned(),
            occurred_at_ms: e.stored_at,
            description: event_message(&e.envelope.payload),
        })
        .collect();

    (
        StatusCode::OK,
        Json(EventsPage {
            events,
            next_cursor,
            has_more,
        }),
    )
        .into_response()
}

pub(crate) async fn replay_run_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
    Query(query): Query<RunReplayQuery>,
) -> impl IntoResponse {
    let run_id = RunId::new(id);
    let run = match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(run)) => run,
        Ok(None) => {
            return run_not_found_response();
        }
        Err(response) => return response,
    };

    if let (Some(from), Some(to)) = (query.from_position, query.to_position) {
        if to < from {
            return validation_error_response("to_position must be >= from_position");
        }
    }

    match build_run_replay_result(
        state.as_ref(),
        &run.run_id,
        query.from_position,
        query.to_position,
    )
    .await
    {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(err) => store_error_response(err),
    }
}

pub(crate) async fn replay_run_to_checkpoint_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
    Query(query): Query<ReplayToCheckpointQuery>,
) -> impl IntoResponse {
    let run_id = RunId::new(id);
    let run = match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(run)) => run,
        Ok(None) => {
            return run_not_found_response();
        }
        Err(response) => return response,
    };

    let checkpoint_id = CheckpointId::new(query.checkpoint_id);
    let checkpoint =
        match CheckpointReadModel::get(state.runtime.store.as_ref(), &checkpoint_id).await {
            Ok(Some(checkpoint)) if checkpoint.run_id == run.run_id => checkpoint,
            Ok(Some(_)) | Ok(None) => {
                return AppApiError::new(
                    StatusCode::NOT_FOUND,
                    "not_found",
                    "checkpoint not found for run",
                )
                .into_response();
            }
            Err(err) => return store_error_response(err),
        };

    let checkpoint_position = match checkpoint_recorded_position(
        state.runtime.store.as_ref(),
        &checkpoint.checkpoint_id,
        &run.run_id,
    )
    .await
    {
        Ok(Some(position)) => position,
        Ok(None) => {
            return AppApiError::new(
                StatusCode::NOT_FOUND,
                "not_found",
                "checkpoint event not found",
            )
            .into_response();
        }
        Err(err) => return store_error_response(err),
    };

    match build_run_replay_result(
        state.as_ref(),
        &run.run_id,
        None,
        Some(checkpoint_position.0),
    )
    .await
    {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(err) => store_error_response(err),
    }
}

pub(crate) async fn list_run_interventions_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
    Query(query): Query<PaginationQuery>,
) -> impl IntoResponse {
    let run_id = RunId::new(id);
    match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return run_not_found_response();
        }
        Err(response) => return response,
    }

    // #422: honest pagination — fetch `limit + 1`, derive `has_more`.
    let limit = query.limit();
    match OperatorInterventionReadModel::list_by_run(
        state.runtime.store.as_ref(),
        &run_id,
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

pub(crate) async fn intervene_run_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
    Json(body): Json<RunInterventionRequest>,
) -> impl IntoResponse {
    let run_id = RunId::new(id);
    let run = match state.runtime.runs.get(&run_id).await {
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
    // Tenant to stamp on intervention events / notifications: the run's
    // real tenant, not the request principal's. Admin can cross tenants
    // past the guard above, so using the principal's tenant here would
    // mislabel events and misroute SSE/notifications.
    let event_tenant_id = run.project.tenant_id.clone();

    let before = current_event_head(&state).await;
    match body.action {
        RunInterventionAction::ForceComplete => {
            match state.runtime.runs.complete(&run.session_id, &run_id).await {
                Ok(updated_run) => {
                    if let Err(err) = append_run_intervention_event(
                        &state,
                        &run_id,
                        &event_tenant_id,
                        "force_complete",
                        &body.reason,
                    )
                    .await
                    {
                        return store_error_response(err);
                    }
                    publish_runtime_frames_since(&state, before).await;
                    (
                        StatusCode::OK,
                        Json(RunInterventionResponse {
                            ok: true,
                            run: Some(updated_run),
                            message_id: None,
                        }),
                    )
                        .into_response()
                }
                Err(err) => runtime_error_response(err),
            }
        }
        RunInterventionAction::ForceFail => {
            let events = vec![
                operator_event_envelope(RuntimeEvent::RunStateChanged(RunStateChanged {
                    project: run.project.clone(),
                    run_id: run_id.clone(),
                    transition: StateTransition {
                        from: Some(run.state),
                        to: RunState::Failed,
                    },
                    failure_class: Some(cairn_domain::FailureClass::ExecutionError),
                    pause_reason: None,
                    resume_trigger: None,
                })),
                operator_event_envelope(RuntimeEvent::OperatorIntervention(
                    cairn_domain::OperatorIntervention {
                        run_id: Some(run_id.clone()),
                        tenant_id: event_tenant_id.clone(),
                        action: "force_fail".to_owned(),
                        reason: body.reason,
                        intervened_at_ms: now_ms(),
                    },
                )),
            ];
            match state.runtime.store.append(&events).await {
                Ok(_) => {
                    // RFC 008: notify any operators subscribed to run.failed.
                    let _ = state
                        .runtime
                        .notifications
                        .notify_if_applicable(
                            &event_tenant_id,
                            "run.failed",
                            serde_json::json!({ "run_id": run_id.as_str() }),
                        )
                        .await;
                    match state.runtime.runs.get(&run_id).await {
                        Ok(Some(updated_run)) => {
                            publish_runtime_frames_since(&state, before).await;
                            (
                                StatusCode::OK,
                                Json(RunInterventionResponse {
                                    ok: true,
                                    run: Some(updated_run),
                                    message_id: None,
                                }),
                            )
                                .into_response()
                        }
                        Ok(None) => AppApiError::new(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "internal_error",
                            "run not found after intervention",
                        )
                        .into_response(),
                        Err(err) => runtime_error_response(err),
                    }
                }
                Err(err) => store_error_response(err),
            }
        }
        RunInterventionAction::ForceRestart => {
            if !run.state.is_terminal() {
                return validation_error_response("force_restart requires a terminal run state");
            }

            let events = vec![
                operator_event_envelope(RuntimeEvent::RunStateChanged(RunStateChanged {
                    project: run.project.clone(),
                    run_id: run_id.clone(),
                    transition: StateTransition {
                        from: Some(run.state),
                        to: RunState::Running,
                    },
                    failure_class: None,
                    pause_reason: None,
                    resume_trigger: Some(ResumeTrigger::OperatorResume),
                })),
                operator_event_envelope(RuntimeEvent::OperatorIntervention(
                    cairn_domain::OperatorIntervention {
                        run_id: Some(run_id.clone()),
                        tenant_id: event_tenant_id.clone(),
                        action: "force_restart".to_owned(),
                        reason: body.reason,
                        intervened_at_ms: now_ms(),
                    },
                )),
            ];
            match state.runtime.store.append(&events).await {
                Ok(_) => match state.runtime.runs.get(&run_id).await {
                    Ok(Some(updated_run)) => {
                        publish_runtime_frames_since(&state, before).await;
                        (
                            StatusCode::OK,
                            Json(RunInterventionResponse {
                                ok: true,
                                run: Some(updated_run),
                                message_id: None,
                            }),
                        )
                            .into_response()
                    }
                    Ok(None) => AppApiError::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal_error",
                        "run not found after intervention",
                    )
                    .into_response(),
                    Err(err) => runtime_error_response(err),
                },
                Err(err) => store_error_response(err),
            }
        }
        RunInterventionAction::InjectMessage => {
            let Some(message_body) = body.message_body else {
                return validation_error_response("inject_message requires message_body");
            };

            // T6a-H11: reject message injection into a terminal run. A
            // Completed/Failed/Canceled run has no consumer for the
            // mailbox row, so the write would dangle forever.
            if run.state.is_terminal() {
                return AppApiError::new(
                    StatusCode::CONFLICT,
                    "run_terminal",
                    format!(
                        "cannot inject message into run in terminal state {:?}",
                        run.state
                    ),
                )
                .into_response();
            }

            let message_id = MailboxMessageId::new(format!("msg_intervention_{}", Uuid::new_v4()));
            match state
                .runtime
                .mailbox
                .append(
                    &run.project,
                    message_id.clone(),
                    Some(run_id.clone()),
                    None,
                    message_body,
                    None,
                    0,
                )
                .await
            {
                Ok(_) => {
                    if let Err(err) = append_run_intervention_event(
                        &state,
                        &run_id,
                        &event_tenant_id,
                        "inject_message",
                        &body.reason,
                    )
                    .await
                    {
                        return store_error_response(err);
                    }
                    publish_runtime_frames_since(&state, before).await;
                    (
                        StatusCode::OK,
                        Json(RunInterventionResponse {
                            ok: true,
                            run: None,
                            message_id: Some(message_id.to_string()),
                        }),
                    )
                        .into_response()
                }
                Err(err) => runtime_error_response(err),
            }
        }
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

pub(crate) async fn get_run_cost_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let run_id = RunId::new(id.clone());
    match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(_run)) => {
            match RunCostReadModel::get_run_cost(state.runtime.store.as_ref(), &run_id).await {
                Ok(Some(record)) => (StatusCode::OK, Json(record)).into_response(),
                Ok(None) => {
                    // Return a zero-valued cost record instead of 404 when no cost data exists.
                    (
                        StatusCode::OK,
                        Json(cairn_domain::providers::RunCostRecord {
                            run_id: RunId::new(id),
                            total_cost_micros: 0,
                            total_tokens_in: 0,
                            total_tokens_out: 0,
                            provider_calls: 0,
                            token_in: 0,
                            token_out: 0,
                        }),
                    )
                        .into_response()
                }
                Err(err) => store_error_response(err),
            }
        }
        Ok(None) => run_not_found_response(),
        Err(response) => response,
    }
}

pub(crate) async fn set_run_cost_alert_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
    Json(body): Json<SetRunCostAlertRequest>,
) -> impl IntoResponse {
    let run_id = RunId::new(id);

    // T6a-C2: resolve the run under tenant scope and use the run's
    // actual tenant_id — not a body-supplied one, which lets callers
    // forge cross-tenant alerts.
    let run = match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(r)) => r,
        Ok(None) => {
            return run_not_found_response();
        }
        Err(response) => return response,
    };

    match state
        .runtime
        .run_cost_alerts
        .set_alert(
            run_id.clone(),
            run.project.tenant_id.clone(),
            body.threshold_micros,
        )
        .await
    {
        // #431: return the created alert record instead of `{ok: true}`.
        // The UI was forced to re-list alerts to discover the value it
        // just set. Callers that only care about success can still
        // check the HTTP 201 status.
        Ok(()) => (
            StatusCode::CREATED,
            Json(RunCostAlertResponse {
                run_id: run_id.as_str().to_owned(),
                tenant_id: run.project.tenant_id.as_str().to_owned(),
                threshold_micros: body.threshold_micros,
            }),
        )
            .into_response(),
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn list_run_cost_alerts_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Query(query): Query<PaginationQuery>,
) -> impl IntoResponse {
    // #422: the service returns every triggered alert for the tenant.
    // Paginate in-memory and emit an honest `has_more`.
    match state
        .runtime
        .run_cost_alerts
        .list_triggered_by_tenant(tenant_scope.tenant_id())
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

pub(crate) async fn set_run_sla_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
    Json(body): Json<SetRunSlaRequest>,
) -> impl IntoResponse {
    let run_id = RunId::new(id);

    // T6a-C2: tenant scope + use run's actual tenant_id.
    let run = match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(r)) => r,
        Ok(None) => {
            return run_not_found_response();
        }
        Err(response) => return response,
    };

    match state
        .runtime
        .run_sla
        .set_sla(
            run_id,
            run.project.tenant_id.clone(),
            body.target_completion_ms,
            body.alert_at_percent,
        )
        .await
    {
        Ok(config) => (StatusCode::CREATED, Json(config)).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn get_run_sla_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let run_id = RunId::new(id);

    // T6a-C2: tenant scope before the read.
    match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return run_not_found_response();
        }
        Err(response) => return response,
    }

    match state.runtime.run_sla.check_sla(&run_id).await {
        Ok(status) => (StatusCode::OK, Json(status)).into_response(),
        Err(RuntimeError::NotFound { .. }) => AppApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "SLA not configured for run",
        )
        .into_response(),
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn list_sla_breached_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Query(query): Query<PaginationQuery>,
) -> impl IntoResponse {
    // #422: service returns every breach for the tenant. Paginate in
    // memory and emit an honest `has_more`.
    match state
        .runtime
        .run_sla
        .list_breached_by_tenant(tenant_scope.tenant_id())
        .await
    {
        Ok(all) => {
            let total = all.len();
            let offset = query.offset();
            let limit = query.limit();
            let items: Vec<_> = all.into_iter().skip(offset).take(limit).collect();
            let has_more = offset.saturating_add(items.len()) < total;
            (
                StatusCode::OK,
                Json(ListResponse::<cairn_domain::sla::SlaBreach> { items, has_more }),
            )
                .into_response()
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

// ── F53: terminal-failure state flip helpers ────────────────────────────────

/// F53: heuristically map a `LoopTermination::Failed { reason }` string to
/// a `FailureClass`. The reason is free-form from the orchestrator loop
/// (provider errors, lease-expiry diagnostics, tool failures, …), so we
/// substring-match the obvious cases and fall back to `ExecutionError`
/// so operators can still filter failed runs by class.
/// F65 PR-3: merge per-run `BreakerOverrides` into the operator-configured
/// default `BreakerConfig`. Overrides are tighten-only — every explicit
/// override field must be less than or equal to the corresponding default.
/// Any loosening is rejected with a specific error message suitable for
/// HTTP 400 `invalid_breaker_override` (operator-readable: mentions the
/// exact field and both values).
fn resolve_breaker_overrides(
    default_cfg: &cairn_orchestrator::BreakerConfig,
    overrides: Option<&BreakerOverrides>,
) -> Result<cairn_orchestrator::BreakerConfig, String> {
    let Some(over) = overrides else {
        return Ok(default_cfg.clone());
    };
    let mut out = default_cfg.clone();
    if let Some(v) = over.round_cap {
        if v > default_cfg.round_cap {
            return Err(format!(
                "round_cap override {v} exceeds configured default {}; \
                 breaker overrides must tighten (lower) caps, never loosen them",
                default_cfg.round_cap
            ));
        }
        out.round_cap = v;
    }
    if let Some(v) = over.token_cap {
        if v > default_cfg.token_cap {
            return Err(format!(
                "token_cap override {v} exceeds configured default {}; \
                 breaker overrides must tighten (lower) caps, never loosen them",
                default_cfg.token_cap
            ));
        }
        out.token_cap = v;
    }
    if let Some(v) = over.no_tool_use_streak {
        if v > default_cfg.no_tool_use_streak {
            return Err(format!(
                "no_tool_use_streak override {v} exceeds configured default {}; \
                 breaker overrides must tighten (lower) caps, never loosen them",
                default_cfg.no_tool_use_streak
            ));
        }
        out.no_tool_use_streak = v;
    }
    if let Some(v) = over.wall_clock_ms {
        if v > default_cfg.wall_clock_ms {
            return Err(format!(
                "wall_clock_ms override {v} exceeds configured default {}; \
                 breaker overrides must tighten (lower) caps, never loosen them",
                default_cfg.wall_clock_ms
            ));
        }
        out.wall_clock_ms = v;
    }
    if let Some(v) = over.warn_ratio_bps {
        // #479: tighten-only is "lower bps", which fires the warning
        // earlier — the exact opposite of round_cap/token_cap (which
        // tighten downward for the same safety reason: smaller cap =
        // trips sooner). A request that RAISES the ratio (warning fires
        // closer to the trip line) loosens the safety posture and is
        // rejected.
        if v > default_cfg.warn_ratio_bps {
            return Err(format!(
                "warn_ratio_bps override {v} exceeds configured default {}; \
                 breaker overrides must tighten (lower) the warning threshold, \
                 never raise it",
                default_cfg.warn_ratio_bps
            ));
        }
        out.warn_ratio_bps = v;
    }
    Ok(out)
}

fn classify_failed_reason(reason: &str) -> cairn_domain::FailureClass {
    let lower = reason.to_ascii_lowercase();
    if lower.contains("lease") && (lower.contains("expir") || lower.contains("lost")) {
        cairn_domain::FailureClass::LeaseExpired
    } else if lower.contains("timed out") || lower.contains("timeout") {
        cairn_domain::FailureClass::TimedOut
    } else if lower.contains("approval") && lower.contains("reject") {
        cairn_domain::FailureClass::ApprovalRejected
    } else if lower.contains("policy") && lower.contains("denied") {
        cairn_domain::FailureClass::PolicyDenied
    } else {
        cairn_domain::FailureClass::ExecutionError
    }
}

/// F53: flip the run to the terminal `Failed` state via
/// `RunService::fail`. Called from every non-success
/// `LoopTermination` branch (Failed / MaxIterationsReached / TimedOut)
/// so the run projection catches up to the orchestrate response body
/// (previously runs stayed `Running` forever, forcing operators to
/// manually cancel).
///
/// Swallows errors: a transient store blip or an already-terminal run
/// (e.g. `runs.complete` already fired inside the loop) must not turn
/// an orchestrate response into a 5xx for the operator. All failure
/// paths are logged so ops can spot the drift.
async fn finalize_run_failure(
    state: &AppState,
    session_id: &cairn_domain::SessionId,
    run_id: &cairn_domain::RunId,
    failure_class: cairn_domain::FailureClass,
) {
    if let Err(e) = state
        .runtime
        .runs
        .fail(session_id, run_id, failure_class)
        .await
    {
        // `InvalidTransition` is the common benign case: the loop already
        // reached a terminal state (Completed / Canceled) before we got
        // here. Log at debug. Anything else is operator-visible.
        match &e {
            cairn_runtime::error::RuntimeError::InvalidTransition { .. } => {
                tracing::debug!(
                    run_id = %run_id,
                    failure_class = ?failure_class,
                    error = %e,
                    "F53: run already in terminal state; skip fail-flip"
                );
            }
            _ => {
                tracing::warn!(
                    run_id = %run_id,
                    failure_class = ?failure_class,
                    error = %e,
                    "F53: failed to flip run to Failed after terminal orchestrate; \
                     GET /v1/runs/:id may report stale state=running"
                );
            }
        }
    }
}

// ── Orchestrator entry point ──────────────────────────────────────────────

/// POST /v1/runs/:id/orchestrate -- trigger the GATHER -> DECIDE -> EXECUTE loop.
///
/// #433: this is the single most expensive POST in the API (LLM tokens,
/// provider cost, breaker-state writes). A double-submit after a 502
/// gateway timeout used to burn real budget. This wrapper honors the
/// `Idempotency-Key` HTTP header: a retry with the same key + same body
/// replays the first response; a retry with the same key + different
/// body returns 409. See `crate::idempotency` for the full contract.
pub(crate) async fn orchestrate_run_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(run_id_str): Path<String>,
    headers: axum::http::HeaderMap,
    // #433 review — Gemini high-severity: use `Json<Value>` as a thin
    // gate so axum's default 2MB body cap applies. Going through
    // `serde_json::Value` instead of `OrchestrateRequest` directly
    // keeps the raw wire bytes available for the idempotency
    // body-hash (`serde_json::to_vec(&v)` is stable for
    // round-tripped Value). Pre-fix we used `Bytes` which has no cap.
    Json(body_value): Json<serde_json::Value>,
) -> axum::response::Response {
    use crate::idempotency::{claim, IdempotencyEndpoint};

    // Serialize back to bytes for the idempotency body-hash. `Value`
    // round-trips deterministically under serde_json's canonical form
    // (object-key order preserved via IndexMap when the
    // `preserve_order` feature is on, otherwise alphabetic). Both
    // orderings are stable WITHIN a given crates.io build, which is
    // all the hash needs — retries from the same client serialise
    // the same way.
    let body_bytes = match serde_json::to_vec(&body_value) {
        Ok(b) => b,
        Err(err) => {
            return AppApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid_request",
                format!("failed to re-serialize body for idempotency hash: {err}"),
            )
            .into_response();
        }
    };

    // Typed re-parse. Keeps the same 422 semantics the project-wide
    // `json_rejection_response` maps to — the prior `Bytes` path was
    // returning 400 on malformed bodies, drifting from the rest of
    // the API. Copilot review flagged the status-code regression.
    let body: OrchestrateRequest = match serde_json::from_value(body_value) {
        Ok(v) => v,
        Err(err) => {
            return AppApiError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_error",
                format!("invalid orchestrate request: {err}"),
            )
            .into_response();
        }
    };

    // Claim the idempotency slot. Three outcomes:
    //   - no header               => guard = None, proceed as normal.
    //   - claimed                 => guard = Some, run + publish.
    //   - replay / conflict / 400 => short-circuit with that Response.
    //
    // The cache handle is cloned so the borrow doesn't overlap with
    // the `State(state)` move into the inner handler. `Arc::clone` is
    // ~2 instructions; fine for a once-per-request path.
    let cache = state.idempotency_cache.clone();
    let tenant_id_str = tenant_scope.tenant_id().as_str().to_owned();
    let guard = match claim(
        &cache,
        &headers,
        &body_bytes,
        &tenant_id_str,
        IdempotencyEndpoint::Orchestrate,
    ) {
        Ok(g) => g,
        Err(response) => return response,
    };

    let response =
        orchestrate_run_handler_inner(State(state), tenant_scope, Path(run_id_str), Json(body))
            .await;

    // Cache the response bytes so retries replay verbatim. Axum
    // responses are streaming by nature; we buffer here to capture the
    // body. Gemini/Copilot review flagged `to_bytes(..., usize::MAX)`
    // as an unbounded-buffer DoS vector. Cap at 1MB — far larger than
    // every observed orchestrate response (tens of KB) but well
    // below the `DefaultBodyLimit::max(10 * 1024 * 1024)` the router
    // already enforces on requests. If a future response ever
    // legitimately exceeds this, we'll see the 500 below and can
    // revisit.
    const MAX_RESPONSE_BUFFER_BYTES: usize = 1024 * 1024;
    if let Some(guard) = guard {
        let (parts, stream_body) = response.into_parts();
        let status = parts.status;
        let collected = match axum::body::to_bytes(stream_body, MAX_RESPONSE_BUFFER_BYTES).await {
            Ok(b) => b,
            Err(err) => {
                // Either the body stream errored or it exceeded the
                // 1MB cap. Release the claim by dropping the guard
                // (happens automatically) and return 500 so the
                // client retries without replaying a broken response.
                drop(guard);
                tracing::warn!(
                    error = %err,
                    cap_bytes = MAX_RESPONSE_BUFFER_BYTES,
                    "idempotency: failed to buffer response body (likely exceeded cap)"
                );
                return AppApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "idempotency_buffer_failed",
                    "failed to buffer response body for idempotent replay \
                     (cap 1MB — increase MAX_RESPONSE_BUFFER_BYTES if legitimate)",
                )
                .into_response();
            }
        };
        guard.publish(status, collected.clone());
        return axum::response::Response::from_parts(parts, axum::body::Body::from(collected));
    }

    response
}

async fn orchestrate_run_handler_inner(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(run_id_str): Path<String>,
    Json(body): Json<OrchestrateRequest>,
) -> axum::response::Response {
    use cairn_domain::RunId;
    use cairn_harness_tools::{
        HarnessBash, HarnessBashKill, HarnessBashOutput, HarnessBuiltin, HarnessEdit, HarnessGlob,
        HarnessGrep, HarnessLsp, HarnessMultiEdit, HarnessRead, HarnessWebFetch, HarnessWrite,
    };
    use cairn_orchestrator::{
        LlmDecidePhase, LoopConfig, LoopTermination, OrchestrationContext, OrchestratorLoop,
        RuntimeExecutePhase, StandardGatherPhase,
    };
    use cairn_runtime::services::{
        ApprovalServiceImpl, CheckpointServiceImpl, MailboxServiceImpl, ToolInvocationServiceImpl,
    };
    // F30 (2026-04-24): introspection tools (`GetRunTool`, `GetTaskTool`,
    // `GetApprovalsTool`, `ListRunsTool`, `SearchEventsTool`,
    // `WaitForTaskTool`) are kept registered — they serve legitimate
    // agent-task goals like "list every failed run from today" or "check
    // the status of task I created earlier." Removing them outright
    // would cripple system-aware prompts.
    //
    // The loop-exhaustion bug (dogfood run 1 evidence: 15 introspection
    // calls per trivial prose prompt) is instead closed at the prompt
    // layer: the system prompt now explicitly forbids calling
    // introspection tools on THIS run itself (the goal, iteration
    // number, and step history are already in the prompt), while still
    // allowing them for genuinely system-aware tasks where the user's
    // goal is to look at OTHER runs / tasks / approvals. See the F30
    // design note in
    // `crates/cairn-orchestrator/src/decide_impl.rs::build_system_prompt`.
    use cairn_tools::{
        BuiltinToolRegistry, CalculateTool, CancelTaskTool, CreateTaskTool, GetApprovalsTool,
        GetRunTool, GetTaskTool, GraphQueryTool, HttpRequestTool, JsonExtractTool, ListRunsTool,
        MemorySearchTool, MemoryStoreTool, NotificationSink, NotifyOperatorTool,
        ResolveApprovalTool, ScheduleTaskTool, ScratchPadTool, SearchEventsTool, SummarizeTextTool,
        ToolSearchTool, WaitForTaskTool,
    };

    let run_id = RunId::new(run_id_str);

    // T6a-C2: tenant scope MUST gate orchestration — this kicks off LLM calls
    // and burns provider budget. Cross-tenant orchestrate is a budget DoS.
    let run = match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(r)) => r,
        Ok(None) => {
            return run_not_found_response();
        }
        Err(response) => return response,
    };

    // Terminal runs (Completed / Failed / Canceled) must short-circuit
    // before the orchestration loop. Re-entering the loop on a finalized
    // FF execution would dispatch a second terminal FCALL and FF would
    // reject it as `execution_not_active` — surfacing a false
    // `termination=failed` for a run that had actually succeeded.
    //
    // The authoritative FF snapshot state is re-verified downstream by
    // `renew_lease_if_stale` via its terminal branch; this projection
    // read is a fast-path that lets the handler skip the workspace +
    // defaults plumbing entirely when the run is already terminal.
    if run.state.is_terminal() {
        // `is_terminal()` covers exactly the three arms below today. A future
        // variant that gets added to `RunState::is_terminal` but missed here
        // must NOT panic the request path — `unreachable!` on an HTTP handler
        // is a DoS surface (#455). Log + return 500 so the ops team sees a
        // loud stack trace, not a dropped connection.
        let termination = match run.state {
            cairn_domain::RunState::Completed => "completed",
            cairn_domain::RunState::Failed => "failed",
            cairn_domain::RunState::Canceled => "canceled",
            other => {
                tracing::error!(
                    run_id = %run.run_id,
                    run_state = ?other,
                    "RunState::is_terminal returned true for unhandled variant — update orchestrate_run_handler",
                );
                return AppApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "run is in an unexpected terminal state",
                )
                .into_response();
            }
        };
        return (
            StatusCode::OK,
            Json(serde_json::json!({
                "termination": termination,
                "run_state": run.state,
            })),
        )
            .into_response();
    }

    // Transition run to Running if it's still Pending
    if run.state == cairn_domain::RunState::Pending {
        use cairn_domain::{RunState, RunStateChanged, RuntimeEvent, StateTransition};
        use cairn_runtime::make_envelope;
        let evt = make_envelope(RuntimeEvent::RunStateChanged(RunStateChanged {
            project: run.project.clone(),
            run_id: run.run_id.clone(),
            transition: StateTransition {
                from: Some(RunState::Pending),
                to: RunState::Running,
            },
            failure_class: None,
            pause_reason: None,
            resume_trigger: None,
        }));
        if let Err(e) = state.runtime.store.append(&[evt]).await {
            tracing::warn!("failed to transition run to running: {e}");
        }
    }

    // F41 (2026-04-24) + F51 (2026-04-26): activate the run's FF
    // execution before the orchestrator loop dispatches any terminal
    // FCALL, AND keep the lease fresh across the pull-model HTTP gap
    // between invocations.
    //
    // `POST /v1/runs` calls `runs.start` which calls FF's
    // `ff_create_execution` — that leaves the execution in
    // `lifecycle_phase = "runnable"`. FF's terminal FCALLs
    // (`ff_complete_execution` et al.) gate on
    // `lifecycle_phase == "active"` via `validate_lease_and_mark_expired`,
    // so the loop's final `CompleteRun` step was being rejected with
    // `execution_not_active -> completed` pre-F41. The transition to
    // `active` is owned by `ff_claim_execution` via
    // `issue_grant_and_claim`, and the F51 helper re-uses that exact
    // path in its no-lease branch.
    //
    // `POST /v1/runs/:id/orchestrate` is a pull-model driver — each
    // call runs one GATHER → DECIDE → EXECUTE iteration and returns;
    // no long-lived worker renews the lease between invocations.
    // Operator-paced flows (human approvals, slow tool-call confirms)
    // routinely exceed the 30s default TTL, and the next call's
    // terminal FCALL (`ff_complete_execution`) would then reject with
    // `lease_expired`. Prior workaround was to crank
    // `CAIRN_FABRIC_LEASE_TTL_MS` up to 600_000, which paid 20× recovery
    // latency on stuck runs and `worker_leases` index bloat.
    //
    // `renew_lease_if_stale` is the single entry-time call that
    // handles all three cases:
    //
    //   * **First call on a freshly-created run** (no lease yet): the
    //     snapshot has `current_lease = None` and we fall back to a
    //     full `issue_grant_and_claim`. This supersedes the F41
    //     `ensure_active` call that used to live here — the no-lease
    //     branch of `renew_lease_if_stale` walks the exact same
    //     sequence `ensure_active` did, without the extra snapshot
    //     round-trip two calls would have cost.
    //   * **Expired-but-not-cleared lease** (`remaining_ms <= 0`): FF's
    //     scanner hasn't rolled the exec forward yet. We can't
    //     `ff_renew_lease` (would reject `lease_expired`), so we
    //     full-reclaim.
    //   * **Near-expiry** (remaining <= `F51_MIN_REMAINING_MS`): renew
    //     in place via `ff_renew_lease` — no epoch rotation, no
    //     lease-history write, preserves the existing fence triple.
    //   * **Healthy**: snapshot read only, no FF mutation. Back-to-back
    //     orchestrate calls (<1s) hit this path.
    //
    // Threshold rationale: 10s gives one iteration's worth of headroom
    // under the default 30s TTL. Calls arriving with ≥20s of TTL
    // already burned are rare but always worth renewing early so the
    // iteration's terminal FCALL doesn't spill past expiry.
    //
    // A failure here (runtime-level 5xx, tenant visibility issue)
    // aborts orchestration before we burn provider budget on a run
    // that can't terminate.
    //
    // F57 (2026-04-26): skip `renew_lease_if_stale` entirely when the
    // run has unresolved pending approvals. Mid-approval executions
    // sit in FF's suspended/waiting-approval sub-phases where
    // `ff_renew_lease` rejects with `execution_not_active` (from
    // lease-gate) or the claim fallback rejects with
    // `execution_not_eligible` (from the grant gate's
    // `lifecycle_phase == "runnable"` requirement — a suspended
    // execution fails this). The orchestrator's approval-drain path
    // inside the loop doesn't need a renewed lease to answer the
    // caller: it reads the pending approvals from the projection,
    // returns `termination=waiting_approval`, and leaves the FF
    // execution alone. Renewing the lease on a mid-approval
    // execution is not just wasteful — it's the exact FCALL that was
    // 409-ing callers mid-run on F56's dbd52180.
    //
    // `has_pending_for_run` is a projection read (single SQL row on
    // pg/sqlite, HashMap scan in-memory); it's safe to do on every
    // orchestrate call. If the projection read itself fails, we fall
    // back to the original renew path — masking a pending-approvals
    // miss would be worse than an extra FF round-trip.
    let has_pending =
        ApprovalReadModel::has_pending_for_run(state.runtime.store.as_ref(), &run.run_id)
            .await
            .unwrap_or_else(|err| {
                tracing::warn!(
                    run_id = %run.run_id,
                    error = %err,
                    "F57: pending-approvals projection read failed; \
                     proceeding with renew_lease_if_stale"
                );
                false
            });

    const F51_MIN_REMAINING_MS: u64 = 10_000;
    let refreshed = if has_pending {
        tracing::debug!(
            run_id = %run.run_id,
            "F57: pending approvals present; skipping renew_lease_if_stale \
             to avoid FF mid-approval-phase rejection"
        );
        // Skip the renew/claim FCALLs (they 409 on mid-approval
        // executions) but STILL do a read-only FF snapshot so the
        // terminal-state guard below stays authoritative. The
        // projection record from `load_run_visible_to_tenant` is
        // fast but may lag FF if a scheduler cron or sibling
        // handler finalized the run between its read and here —
        // that's the exact race the F51 "second terminal-state
        // check" was added to close, and we don't want to regress
        // it just because we're skipping the lease renewal.
        //
        // `runtime.runs.get` walks `read_run_record -> describe_execution`,
        // which is a pure FF read (no FCALL, no lifecycle mutation).
        // A `NotFound` return at this point means the run was
        // deleted mid-flight; propagate via the projection record
        // so the subsequent terminal-state guard still fires on a
        // finalized local state.
        match state.runtime.runs.get(&run.run_id).await {
            Ok(Some(ff_record)) => ff_record,
            Ok(None) => run.clone(),
            Err(err) => {
                tracing::warn!(
                    run_id = %run.run_id,
                    error = %err,
                    "F57: FF snapshot read failed during pending-approvals skip; \
                     falling back to projection record"
                );
                run.clone()
            }
        }
    } else {
        match state
            .runtime
            .runs
            .renew_lease_if_stale(&run.session_id, &run.run_id, F51_MIN_REMAINING_MS)
            .await
        {
            Ok(record) => record,
            // F58 (2026-04-26): tolerate FF's transient phase-conflict
            // codes. After a tool invocation lands (esp. `write`), FF
            // moves the execution's `lifecycle_phase` off `runnable`
            // briefly while the invocation is recorded; the next
            // orchestrate call catches the phase mid-flip and the
            // grant gate rejects with `execution_not_eligible`. The
            // existing lease is still valid, and the orchestrate loop
            // has its own `is_lease_healthy()` gate that will fail
            // cleanly if the lease is actually dead. Log at WARN with
            // structured fields so we retain visibility, then fall
            // through with the projection record. Narrow by design —
            // `is_transient_phase_conflict` excludes permanent
            // failures (terminal, deleted, revoked) so real 409s
            // still propagate. See
            // `docs/design/ff-upstream/ff-execution-phase-probe.md`
            // for the root-cause discussion and the FF-side probe we
            // need to retire this tolerate path.
            Err(err) if err.is_transient_phase_conflict() => {
                tracing::warn!(
                    run_id = %run.run_id,
                    session_id = %run.session_id,
                    error = %err,
                    f58 = "tolerate_transient_renew_conflict",
                    "F58: renew_lease_if_stale rejected with a transient FF \
                     phase-conflict code; proceeding into orchestrate loop \
                     with the existing lease. The loop's is_lease_healthy() \
                     gate will catch an actually-dead lease."
                );
                // Mirror F57's pending-approvals branch: do a read-only FF
                // snapshot so the subsequent terminal-state guard stays
                // authoritative. Falling back to `run.clone()` directly
                // (the projection record) would reintroduce the F51 race
                // where FF finalizes the run between `load_run_visible_to_tenant`
                // and here. `runtime.runs.get` walks
                // `read_run_record -> describe_execution` (a pure FF read,
                // no FCALL, no lifecycle mutation) so it's safe even though
                // the renew just failed.
                match state.runtime.runs.get(&run.run_id).await {
                    Ok(Some(ff_record)) => ff_record,
                    Ok(None) => run.clone(),
                    Err(snap_err) => {
                        tracing::warn!(
                            run_id = %run.run_id,
                            error = %snap_err,
                            "F58: FF snapshot read failed during \
                             tolerate-transient branch; falling back to \
                             projection record"
                        );
                        run.clone()
                    }
                }
            }
            Err(err) => {
                tracing::error!(
                    run_id = %run.run_id,
                    error = %err,
                    "F51: failed to refresh run lease before orchestrate loop"
                );
                return runtime_error_response(err);
            }
        }
    };

    // Second terminal-state check against the authoritative FF snapshot.
    // The projection-backed check above can be stale if another process
    // (scheduler cron, sibling handler, resume path) finalized this run
    // between the projection read and here; `renew_lease_if_stale`
    // returns the fresh FF record, which is the definitive source of
    // truth for lifecycle state.
    if refreshed.state.is_terminal() {
        let termination = match refreshed.state {
            cairn_domain::RunState::Completed => "completed",
            cairn_domain::RunState::Failed => "failed",
            cairn_domain::RunState::Canceled => "canceled",
            other => unreachable!(
                "RunState::is_terminal returned true for non-terminal variant {other:?}"
            ),
        };
        return (
            StatusCode::OK,
            Json(serde_json::json!({
                "termination": termination,
                "run_state": refreshed.state,
            })),
        )
            .into_response();
    }

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let working_dir = match working_dir_for_run(state.as_ref(), &run).await {
        Ok(path) => path,
        Err(err) => return workspace_error_response(err),
    };
    let default_goal =
        resolve_run_string_default(state.as_ref(), &run.project, &run.run_id, "goal").await;
    let default_agent_role =
        resolve_run_string_default(state.as_ref(), &run.project, &run.run_id, "agent_role").await;
    let default_run_mode =
        resolve_run_mode_default(state.as_ref(), &run.project, &run.run_id).await;

    let ctx = OrchestrationContext {
        project: run.project.clone(),
        session_id: run.session_id.clone(),
        run_id: run.run_id.clone(),
        task_id: None,
        iteration: 0,
        goal: body
            .goal
            .or(default_goal)
            .unwrap_or_else(|| "Execute the run objective.".to_owned()),
        agent_type: run
            .agent_role_id
            .clone()
            .or(default_agent_role)
            .unwrap_or_else(|| "orchestrator".to_owned()),
        run_started_at_ms: now_ms,
        working_dir: working_dir.clone(),
        run_mode: body.mode.clone().or(default_run_mode).unwrap_or_default(),
        discovered_tool_names: vec![],
        step_history: vec![],
        is_recovery: false,
        approval_timeout: body
            .approval_timeout_ms
            .map(std::time::Duration::from_millis),
    };

    // Cairn picks the model. The caller describes the task; the control
    // plane resolves the preferred model from system defaults and derives
    // the full per-binding model chain below.
    let model_id = {
        let brain_model = state.runtime.runtime_config.default_brain_model().await;
        let model = if brain_model.trim().is_empty() || brain_model == "default" {
            state.runtime.runtime_config.default_generate_model().await
        } else {
            brain_model
        };
        model.trim().to_owned()
    };
    if model_id.is_empty() || model_id == "default" {
        return AppApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "no_brain_provider",
            "No default LLM model configured. Set brain_model or generate_model on the system scope, or add a provider connection via POST /v1/providers/connections.",
        )
        .into_response();
    }

    let is_bedrock_model = model_id.contains('.') && !model_id.contains('/');
    let brain = match state
        .runtime
        .provider_registry
        .resolve_generation_for_model(
            &run.project.tenant_id,
            &model_id,
            cairn_runtime::ProviderResolutionPurpose::Brain,
        )
        .await
    {
        Ok(Some(provider)) => provider,
        Ok(None) => {
            if is_bedrock_model {
                match &state.bedrock_provider {
                    Some(provider) => provider.clone(),
                    None => {
                        return AppApiError::new(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "no_bedrock_provider",
                            "Bedrock model requested but AWS credentials not configured.",
                        )
                        .into_response();
                    }
                }
            } else {
                match &state.brain_provider {
                    Some(provider) => provider.clone(),
                    None => {
                        return AppApiError::new(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "no_brain_provider",
                            "No LLM provider configured. Add one via POST /v1/providers/connections, or set CAIRN_BRAIN_URL / OPENROUTER_API_KEY / OLLAMA_HOST.",
                        )
                        .into_response()
                    }
                }
            }
        }
        Err(err) => return runtime_error_response(err),
    };

    let gather = StandardGatherPhase::builder(state.runtime.store.clone())
        .with_retrieval(state.retrieval.clone())
        .with_graph(state.graph.clone())
        .with_defaults(state.runtime.store.clone())
        .with_checkpoints(state.runtime.store.clone())
        .build();

    // ── SSE notification sink for notify_operator ───────────────────────────
    // Wraps the broadcast channel so notify_operator can push realtime events.
    struct SseSink {
        tx: tokio::sync::broadcast::Sender<cairn_api::sse::SseFrame>,
        seq: std::sync::Arc<std::sync::atomic::AtomicU64>,
        buf: std::sync::Arc<
            std::sync::RwLock<std::collections::VecDeque<(u64, cairn_api::sse::SseFrame)>>,
        >,
    }
    #[async_trait::async_trait]
    impl NotificationSink for SseSink {
        async fn emit(&self, channel: &str, severity: &str, message: &str) {
            let frame = cairn_api::sse::SseFrame {
                event: cairn_api::sse::SseEventName::OperatorNotification,
                data: serde_json::json!({
                    "channel":  channel,
                    "severity": severity,
                    "message":  message,
                }),
                id: None,
                // NotificationSink has no scope context; keep tenant-agnostic.
                // Filtering happens in the SSE handler.
                tenant_id: None,
            };
            let seq = self.seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut frame_with_id = frame.clone();
            frame_with_id.id = Some(seq.to_string());
            {
                let mut buf = self.buf.write().unwrap_or_else(|e| e.into_inner());
                if buf.len() >= 10_000 {
                    buf.pop_front();
                }
                buf.push_back((seq, frame_with_id));
            }
            let _ = self.tx.send(frame);
        }
    }
    let sse_sink: std::sync::Arc<dyn NotificationSink> = std::sync::Arc::new(SseSink {
        tx: state.runtime_sse_tx.clone(),
        seq: state.sse_seq.clone(),
        buf: state.sse_event_buffer.clone(),
    });
    let mailbox_svc: std::sync::Arc<dyn cairn_runtime::MailboxService> = std::sync::Arc::new(
        cairn_runtime::services::MailboxServiceImpl::new(state.runtime.store.clone()),
    );

    // ── Build BuiltinToolRegistry ────────────────────────────────────────────
    // Wire all ~30 built-in tools (RFC 018 prerequisite).
    // Prefer real memory tool implementations (wired at startup with live
    // RetrievalService + IngestPipeline).  Fall back to stubs otherwise.
    let registry = {
        // Concrete memory tools: use real impl when state.tool_registry is set,
        // otherwise fall back to stubs (schema-correct but no backing service).
        let (search_tool, store_tool, register_repo_tool): (
            std::sync::Arc<dyn cairn_tools::ToolHandler>,
            std::sync::Arc<dyn cairn_tools::ToolHandler>,
            std::sync::Arc<dyn cairn_tools::ToolHandler>,
        ) = if let Some(ref real) = state.tool_registry {
            let search: std::sync::Arc<dyn cairn_tools::ToolHandler> = real
                .get("memory_search")
                .unwrap_or_else(|| std::sync::Arc::new(MemorySearchTool::new()));
            let store: std::sync::Arc<dyn cairn_tools::ToolHandler> = real
                .get("memory_store")
                .unwrap_or_else(|| std::sync::Arc::new(MemoryStoreTool::new()));
            let register_repo: std::sync::Arc<dyn cairn_tools::ToolHandler> =
                real.get("cairn.registerRepo").unwrap_or_else(|| {
                    std::sync::Arc::new(crate::tool_impls::ConcreteRegisterRepoTool::new(
                        state.project_repo_access.clone(),
                        state.repo_clone_cache.clone(),
                    ))
                });
            (search, store, register_repo)
        } else {
            (
                std::sync::Arc::new(MemorySearchTool::new()),
                std::sync::Arc::new(MemoryStoreTool::new()),
                std::sync::Arc::new(crate::tool_impls::ConcreteRegisterRepoTool::new(
                    state.project_repo_access.clone(),
                    state.repo_clone_cache.clone(),
                )),
            )
        };

        // Shared services needed by tool constructors
        let store_ref = state.runtime.store.clone();
        let workspace_root = working_dir.clone();
        let task_svc: Arc<dyn cairn_runtime::tasks::TaskService> = state.runtime.tasks.clone();
        let approval_svc: Arc<dyn cairn_runtime::ApprovalService> =
            Arc::new(ApprovalServiceImpl::new(store_ref.clone()));

        // ── Observational tools ─────────────────────────────────────────────
        let _ = &workspace_root; // harness tools use ToolContext.working_dir at exec time.
        let web_fetch: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(HarnessBuiltin::<HarnessWebFetch>::new());
        let grep_search: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(HarnessBuiltin::<HarnessGrep>::new());
        let file_read: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(HarnessBuiltin::<HarnessRead>::new());
        let glob_find: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(HarnessBuiltin::<HarnessGlob>::new());
        let lsp_tool: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(HarnessBuiltin::<HarnessLsp>::new());
        let json_extract: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(JsonExtractTool);
        let calculate: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(CalculateTool);
        let graph_query: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(GraphQueryTool::new(state.graph.clone()));
        let get_run: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(GetRunTool::new(store_ref.clone()));
        let get_task: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(GetTaskTool::new(store_ref.clone()));
        let get_approvals: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(GetApprovalsTool::new(store_ref.clone()));
        let list_runs: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(ListRunsTool::new(store_ref.clone()));
        let search_events: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(SearchEventsTool::new(store_ref.clone()));
        let wait_for_task: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(WaitForTaskTool::new(store_ref.clone()));

        // ── Internal tools ──────────────────────────────────────────────────
        let scratch_pad: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(ScratchPadTool::new());
        // Skills — agentskills.io activation via published harness-skill.
        let skill_tool: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(HarnessBuiltin::<cairn_skills::HarnessSkill>::new());
        let file_write: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(HarnessBuiltin::<HarnessWrite>::new());
        let edit_tool: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(HarnessBuiltin::<HarnessEdit>::new());
        let multi_edit_tool: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(HarnessBuiltin::<HarnessMultiEdit>::new());
        let create_task: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(CreateTaskTool::new(task_svc.clone()));
        let cancel_task: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(CancelTaskTool::new(task_svc));
        let summarize_text: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(SummarizeTextTool::new(brain.clone(), model_id.clone()));

        // ── External tools ──────────────────────────────────────────────────
        let bash: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(HarnessBuiltin::<HarnessBash>::new());
        let bash_output: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(HarnessBuiltin::<HarnessBashOutput>::new());
        let bash_kill: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(HarnessBuiltin::<HarnessBashKill>::new());
        let http_request: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(HttpRequestTool);
        // git / gh CLI access goes through `bash` — no dedicated wrappers.
        let resolve_approval: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(ResolveApprovalTool::new(approval_svc));
        let schedule_task: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(ScheduleTaskTool::new(store_ref.clone()));
        let score_tool: std::sync::Arc<dyn cairn_tools::ToolHandler> =
            std::sync::Arc::new(cairn_tools::EvalScoreTool::new(store_ref));

        // Helper: register all tools in a registry builder.
        let register_all = |reg: BuiltinToolRegistry| -> BuiltinToolRegistry {
            reg // Core / Observational
                .register(search_tool.clone())
                .register(store_tool.clone())
                .register(register_repo_tool.clone())
                .register(web_fetch.clone())
                .register(grep_search.clone())
                .register(file_read.clone())
                .register(glob_find.clone())
                .register(lsp_tool.clone())
                .register(json_extract.clone())
                .register(calculate.clone())
                .register(graph_query.clone())
                .register(get_run.clone())
                .register(get_task.clone())
                .register(get_approvals.clone())
                .register(list_runs.clone())
                .register(search_events.clone())
                .register(wait_for_task.clone())
                // Internal
                .register(scratch_pad.clone())
                .register(skill_tool.clone())
                .register(file_write.clone())
                .register(edit_tool.clone())
                .register(multi_edit_tool.clone())
                .register(create_task.clone())
                .register(cancel_task.clone())
                .register(summarize_text.clone())
                // External
                .register(bash.clone())
                .register(bash_output.clone())
                .register(bash_kill.clone())
                .register(std::sync::Arc::new(NotifyOperatorTool::new(
                    Some(mailbox_svc.clone()),
                    sse_sink.clone(),
                )))
                .register(http_request.clone())
                .register(resolve_approval.clone())
                .register(schedule_task.clone())
                .register(score_tool.clone())
        };

        // Build inner registry for ToolSearchTool.
        let inner = std::sync::Arc::new(register_all(BuiltinToolRegistry::new()));

        // Full registry with ToolSearchTool that can search the deferred tier.
        std::sync::Arc::new(
            register_all(BuiltinToolRegistry::new())
                .register(std::sync::Arc::new(ToolSearchTool::new(inner))),
        )
    };

    // ── Compose the RoutedGenerationService (F17) ────────────────────────
    // Cross-binding axis: every active provider connection for this tenant
    // becomes a `RoutedBinding`, with the binding that supports `model_id`
    // taking preference. Per-binding axis: each binding's ModelChain is
    // that connection's `supported_models` list (preferred model first on
    // the preferred binding). Cooldowns are scoped per
    // `(tenant_id, binding_id)` so a rate-limited model is only skipped
    // within that tenant's binding — other tenants or sibling connections
    // with independent credentials are unaffected.
    //
    // Dogfood run 2 motivated this: MiniMax empty → Qwen 503 → Llama 429.
    // A single-model hard-fail is a budget DoS on the operator's day.
    // Track which tenant-registered connection (if any) serves the
    // preferred `model_id`. Surfaced back out of the routing block so
    // the post-loop `token_cap` defensive check (issue #351) can
    // look up the selected backend's `reports_usage` flag without
    // re-walking the active-connection list.
    let mut preferred_connection_id: Option<String> = None;
    let routed = {
        let scoped_cooldowns = state.provider_fallback_cooldown.clone();
        let tenant_key = run.project.tenant_id.as_str().to_owned();
        // Surface store/registry failures as an operator-facing 5xx rather
        // than silently treating them as "no connections" — that would
        // route requests to the startup fallback and hide an outage.
        let summaries = match state
            .runtime
            .provider_registry
            .active_connection_summaries(&run.project.tenant_id)
            .await
        {
            Ok(s) => s,
            Err(err) => return runtime_error_response(err),
        };

        let mut bindings: Vec<cairn_runtime::RoutedBinding> = Vec::new();

        // Identify the binding that serves `model_id` so it leads the chain.
        let preferred_idx = summaries
            .iter()
            .position(|(_, sup)| sup.iter().any(|m| m.trim() == model_id));

        // If the configured system-default model isn't advertised by any
        // active connection, fail loudly instead of silently degrading to
        // "first model on first connection". Operators get an actionable
        // error with the full list of tenant connections + their models
        // so they can fix the mismatch (update the default, update the
        // connection's supported_models, or add a new connection).
        if preferred_idx.is_none() && !summaries.is_empty() {
            let inventory = summaries
                .iter()
                .map(|(conn, models)| format!("{conn}=[{}]", models.join(",")))
                .collect::<Vec<_>>()
                .join("; ");
            // Return 503 (not 422) because the tenant's configured system
            // default is genuinely unserviceable right now — the operator
            // needs to know immediately that routing is gone, not discover
            // it later via a delayed approval card. 503 also tells well-
            // behaved callers to back off + retry after operator fix.
            return AppApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "preferred_model_unavailable",
                format!(
                    "System-default model '{model_id}' is not advertised by any active provider connection for this tenant. Active connections + supported_models: {inventory}. Fix by updating `brain_model`/`generate_model` at PUT /v1/settings/defaults/system/<key>, adding the model to a connection's `supported_models`, or creating a new connection via POST /v1/providers/connections.",
                ),
            )
            .into_response();
        }

        let order: Vec<usize> = match preferred_idx {
            Some(pref) => {
                let mut v = vec![pref];
                v.extend((0..summaries.len()).filter(|i| *i != pref));
                v
            }
            None => (0..summaries.len()).collect(),
        };

        for idx in order {
            let (conn_id, supported) = &summaries[idx];
            // Resolve the adapter by EXACT connection ID. Previously we
            // probed by model ID, but when multiple connections share a
            // model slug (e.g. two proxies both exposing `gpt-4o-mini`)
            // `resolve_generation_for_model` returns the same adapter
            // for both bindings — conflating their credentials, quotas,
            // and route records.
            let probe_model = if Some(idx) == preferred_idx {
                model_id.clone()
            } else {
                supported
                    .first()
                    .cloned()
                    .unwrap_or_else(|| model_id.clone())
            };
            let connection_id = cairn_domain::ProviderConnectionId::new(conn_id.as_str());
            let adapter = match state
                .runtime
                .provider_registry
                .resolve_generation_for_connection(
                    &run.project.tenant_id,
                    &connection_id,
                    &probe_model,
                )
                .await
            {
                Ok(Some(a)) => a,
                _ => continue,
            };

            // Build the per-binding model chain.
            let mut models: Vec<String> = Vec::new();
            if Some(idx) == preferred_idx {
                models.push(model_id.clone());
            }
            for m in supported {
                let m = m.trim();
                if !m.is_empty() && !models.iter().any(|x| x == m) {
                    models.push(m.to_owned());
                }
            }
            if models.is_empty() {
                continue;
            }

            // Scope cooldown by (tenant, binding) so one tenant's 429 on
            // model X does not suppress the same model X for another
            // tenant or for a sibling connection that has its own quota.
            let cooldown = scoped_cooldowns.get_or_create(&tenant_key, conn_id);
            // Track the first successfully-constructed binding that
            // serves `model_id` so the #351 defensive check classifies
            // the backend that will actually run DECIDE, not merely a
            // connection that advertised the model but failed to build
            // (Copilot review on #354).
            if Some(idx) == preferred_idx && preferred_connection_id.is_none() {
                preferred_connection_id = Some(conn_id.clone());
            }
            bindings.push(cairn_runtime::RoutedBinding {
                binding_id: conn_id.clone(),
                provider: adapter,
                chain: cairn_runtime::ModelChain::new(models).with_cooldown(cooldown),
            });
        }

        // Fallback for self-hosted dev with no active-connection records
        // (CAIRN_BRAIN_URL / OPENROUTER_API_KEY env-only mode): wrap the
        // startup-resolved adapter as a single-binding chain.
        if bindings.is_empty() {
            let cooldown = scoped_cooldowns.get_or_create(&tenant_key, "startup");
            bindings.push(cairn_runtime::RoutedBinding {
                binding_id: "startup".to_owned(),
                provider: brain.clone(),
                chain: cairn_runtime::ModelChain::single(model_id.clone()).with_cooldown(cooldown),
            });
        }

        cairn_runtime::RoutedGenerationService::new(bindings)
    };

    let decide = LlmDecidePhase::from_routed(routed).with_tools(registry.clone());

    // Build loop config first so checkpoint policy is available for execute.
    let mut cfg = LoopConfig::default();
    if let Some(m) = body.max_iterations {
        cfg.max_iterations = m;
    }
    if let Some(t) = body.timeout_ms {
        cfg.timeout_ms = t;
    }

    // F65 PR-3: resolve the default breaker caps via the RuntimeConfig
    // 3-layer fallback (store → env → hardcoded default), then apply
    // per-run overrides from the request body. Overrides are
    // tighten-only — any loosening request is rejected with HTTP 400
    // so a caller can never bypass operator-configured safety caps.
    let default_breakers = cairn_orchestrator::BreakerConfig {
        round_cap: state.runtime.runtime_config.orchestrator_round_cap().await,
        token_cap: state.runtime.runtime_config.orchestrator_token_cap().await,
        no_tool_use_streak: state
            .runtime
            .runtime_config
            .orchestrator_no_tool_use_streak()
            .await,
        wall_clock_ms: state
            .runtime
            .runtime_config
            .orchestrator_wall_clock_ms()
            .await,
        // #479: the warning ratio defaults to 80 % via RuntimeConfig (and
        // env/CAIRN_ORCHESTRATOR_WARN_RATIO_BPS). Per-run override lands
        // via `breaker_overrides.warn_ratio_bps` — `resolve_breaker_overrides`
        // enforces tighten-only on this axis (lower bps = warning fires
        // earlier, which is strictly safer).
        warn_ratio_bps: state
            .runtime
            .runtime_config
            .orchestrator_warn_ratio_bps()
            .await,
    };
    let breakers =
        match resolve_breaker_overrides(&default_breakers, body.breaker_overrides.as_ref()) {
            Ok(b) => b,
            Err(e) => {
                // Closes #415: use the canonical `AppApiError` envelope
                // (`status_code`, `code`, `message`, `request_id`) — every
                // other validation error in runs.rs uses it, and clients
                // that parse `code`/`message` previously saw `undefined`
                // because this hand-rolled `json!` omitted them.
                return AppApiError::new(StatusCode::BAD_REQUEST, "invalid_breaker_override", e)
                    .into_response();
            }
        };

    // Issue #351 — defensive check: a provider that does not populate
    // `Usage` on every chat response silently disables the token-cap
    // circuit breaker (see `BreakerState::after_decide` — absent tokens
    // add 0, so the cap can never trip). Operators who set a tight cap
    // on such a provider are relying on a budget that does not exist;
    // only the Round or WallClock breakers will ever terminate the run.
    //
    // We refuse to start the run when BOTH conditions hold:
    //   * The selected backend's `reports_usage()` is `false`, AND
    //   * The resolved `token_cap` is below `PROVIDER_USAGE_SENTINEL`.
    //
    // Sentinel rationale: a minimum-useful agent turn is ~5–10k tokens
    // (system prompt + tool schemas + step history). 50_000 gives an
    // operator roughly 5× a minimum turn of headroom — enough that a
    // non-reporting provider misconfigured at (say) token_cap=1000
    // trips this check loudly instead of burning hours of budget on
    // runs that never terminate via the token path.
    //
    // Flagged by Copilot on PR #348 (F65 PR-3). Coverage: only the
    // `Backend::OpenAiCompatible` slot (operator-supplied generic
    // endpoint) returns `false` today — every other typed backend
    // (OpenAI, Anthropic, Bedrock*, Ollama, OpenRouter, Groq, DeepSeek,
    // Google, xAI, Azure, MiniMax, Zai*) populates `usage`.
    //
    // Two resolution paths:
    //   1. Tenant-registered connection matches `model_id`
    //      (`preferred_connection_id` is `Some`): classify via
    //      `backend_for_connection_id`.
    //   2. Env-only startup fallback (`preferred_connection_id` is
    //      `None`, i.e. no tenant connections served the model): the
    //      orchestrate flow falls through to `state.brain_provider`
    //      which itself may be built from `CAIRN_BRAIN_URL` /
    //      `CAIRN_WORKER_URL` — the exact case that produces an
    //      `openai-compatible` (non-reporting) backend. Classify via
    //      `brain_fallback_backend`, which mirrors the priority order
    //      `select_fallback_generation(Brain)` uses. Copilot on #354
    //      flagged that skipping this path left the token-cap bypass
    //      in place for env-only OpenAI-compat deployments.
    const PROVIDER_USAGE_SENTINEL: u64 = 50_000;
    let selected_backend: Option<cairn_providers::Backend> =
        if let Some(connection_id) = preferred_connection_id.as_deref() {
            let conn_id_typed = cairn_domain::ProviderConnectionId::new(connection_id);
            state
                .runtime
                .provider_registry
                .backend_for_connection_id(&run.project.tenant_id, &conn_id_typed)
                .await
                .ok()
                .flatten()
        } else {
            state.runtime.provider_registry.brain_fallback_backend()
        };

    if let Some(backend) = selected_backend {
        if !backend.reports_usage() && breakers.token_cap < PROVIDER_USAGE_SENTINEL {
            return AppApiError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "provider_does_not_report_usage",
                format!(
                    "Provider {backend} does not report token usage; token_cap={} is below the \
                     {PROVIDER_USAGE_SENTINEL} sentinel. The token-cap circuit breaker cannot \
                     enforce a budget this tight on a provider that omits `usage` from chat \
                     responses — raise token_cap (explicit opt-in to unbounded token spend \
                     on this provider) via PUT /v1/settings/defaults/system/system/orchestrator_token_cap \
                     or via `breaker_overrides.token_cap` in the orchestrate request body, \
                     OR switch to a provider connection whose backend populates usage \
                     (OpenAI, Anthropic, Bedrock, Ollama, OpenRouter, Groq, DeepSeek, \
                     Google, xAI, Azure, MiniMax, Z.ai). See issue #351.",
                    breakers.token_cap,
                ),
            )
            .into_response();
        }
    }
    // `None` means the registry couldn't classify the active backend
    // (registry lookup error, or stored metadata doesn't parse to a
    // known variant, or startup fallback entry is missing the
    // `with_metadata` label). Proceed without refusal — the status
    // quo pre-#351 is preserved (log-once WARN inside the loop).

    cfg.breakers = breakers;

    // Build RuntimeExecutePhase from the shared runtime store.
    // All service impls share the same Arc<InMemoryStore> so writes from one
    // service are immediately visible to reads from another.
    let store = state.runtime.store.clone();

    // BP-v2 (research doc `docs/research/llm-agent-approval-systems.md`):
    // wire the tool-call approval service so the execute phase drives
    // the propose-then-await flow. The reader adapter bridges the store
    // projection (`ToolCallApprovalReadModel`) to the runtime-facing
    // `ToolCallApprovalReader` trait so cache misses re-hydrate from the
    // persistent projection (restart, eviction, cross-process resume).
    // F25: use the shared `tool_call_approvals` from AppState so the
    // `await_decision` park (inside this handler's execute phase) and
    // the operator `approve` path (`/v1/tool-call-approvals/:id/approve`
    // → `state.runtime.tool_call_approvals.approve`) target the same
    // service instance + PendingMap. Previously each orchestrate call
    // constructed a fresh `ToolCallApprovalServiceImpl`, so the
    // operator's approve fired a oneshot in an instance nobody was
    // parked on — every run timed out after `approval_timeout_ms`.
    let tool_call_approval_service: Arc<dyn cairn_runtime::ToolCallApprovalService> =
        state.runtime.tool_call_approvals.clone();
    let tool_call_approval_reader_for_drain: Arc<
        dyn cairn_runtime::tool_call_approvals::ToolCallApprovalReader,
    > = Arc::new(cairn_runtime::services::ToolCallApprovalReaderAdapter::new(
        store.clone(),
    ));

    let execute = RuntimeExecutePhase::builder()
        .tool_registry(registry)
        .run_service(state.runtime.runs.clone())
        .task_service(state.runtime.tasks.clone())
        .approval_service(Arc::new(ApprovalServiceImpl::new(store.clone())))
        .checkpoint_service(Arc::new(CheckpointServiceImpl::new(store.clone())))
        .mailbox_service(Arc::new(MailboxServiceImpl::new(store.clone())))
        .tool_invocation_service(Arc::new(ToolInvocationServiceImpl::new(store)))
        .tool_call_approval_service(tool_call_approval_service)
        .decision_service(Arc::new(
            crate::telemetry_routes::UsageMeteredDecisionService::new(
                state.runtime.decision_service.clone(),
                state.runtime.store.clone(),
            ),
        ))
        .checkpoint_every_n_tool_calls(cfg.checkpoint_every_n_tool_calls)
        .tool_result_cache(state.tool_result_cache.clone())
        .build();

    let sse_emitter = std::sync::Arc::new(crate::sse_hooks::SseOrchestratorEmitter::new(
        state.runtime_sse_tx.clone(),
        state.sse_event_buffer.clone(),
        state.sse_seq.clone(),
    ));

    // Composite emitter: SSE events + ProviderCallCompleted trace recording.
    struct TracingEmitter {
        inner: std::sync::Arc<crate::sse_hooks::SseOrchestratorEmitter>,
        store: std::sync::Arc<cairn_store::InMemoryStore>,
        exporter: std::sync::Arc<cairn_runtime::telemetry::OtlpExporter>,
        fatal_error: std::sync::Mutex<Option<String>>,
        /// F65 PR-3: metrics sink for breaker trip / warning counters +
        /// per-kind measured-at-trip histograms.
        metrics: std::sync::Arc<crate::metrics::AppMetrics>,
    }
    #[async_trait::async_trait]
    impl cairn_orchestrator::OrchestratorEventEmitter for TracingEmitter {
        async fn on_started(&self, ctx: &cairn_orchestrator::OrchestrationContext) {
            self.inner.on_started(ctx).await;
        }
        async fn on_gather_completed(
            &self,
            ctx: &cairn_orchestrator::OrchestrationContext,
            g: &cairn_orchestrator::GatherOutput,
        ) {
            self.inner.on_gather_completed(ctx, g).await;
        }
        async fn on_decide_completed(
            &self,
            ctx: &cairn_orchestrator::OrchestrationContext,
            d: &cairn_orchestrator::DecideOutput,
        ) {
            self.inner.on_decide_completed(ctx, d).await;
            crate::tracing_emitter::record_decide_trace(
                ctx,
                d,
                &self.store,
                &self.exporter,
                &self.fatal_error,
            )
            .await;
        }
        async fn on_tool_called(
            &self,
            ctx: &cairn_orchestrator::OrchestrationContext,
            name: &str,
            args: Option<&serde_json::Value>,
        ) {
            self.inner.on_tool_called(ctx, name, args).await;
        }
        async fn on_tool_result(
            &self,
            ctx: &cairn_orchestrator::OrchestrationContext,
            name: &str,
            ok: bool,
            out: Option<&serde_json::Value>,
            err: Option<&str>,
            duration_ms: u64,
        ) {
            self.inner
                .on_tool_result(ctx, name, ok, out, err, duration_ms)
                .await;
        }
        async fn on_step_completed(
            &self,
            ctx: &cairn_orchestrator::OrchestrationContext,
            d: &cairn_orchestrator::DecideOutput,
            e: &cairn_orchestrator::ExecuteOutcome,
        ) {
            self.inner.on_step_completed(ctx, d, e).await;
        }
        async fn on_breaker_tripped(
            &self,
            ctx: &cairn_orchestrator::OrchestrationContext,
            trip: &cairn_domain::session_orchestration::CircuitBreakerTrip,
        ) {
            // Forward to SSE for live dashboards.
            self.inner.on_breaker_tripped(ctx, trip).await;
            // F65 PR-3: append RuntimeEvent::CircuitBreakerTripped to the
            // durable event log so projections (session_outcome, UI) see
            // the trip alongside the SessionOutcomeEmitted that PR-4+ will
            // wire. Failures here are logged; the loop still returns
            // `LoopTermination::BreakerTripped` so the operator-visible
            // HTTP response is unaffected.
            use cairn_domain::{CircuitBreakerTripped, EventEnvelope, EventId, EventSource};
            use cairn_store::EventLog;
            let at_ms = crate::errors::now_ms();
            let envelope = EventEnvelope::for_runtime_event(
                EventId::new(format!("evt_breaker_tripped_{}", uuid::Uuid::new_v4())),
                EventSource::System,
                cairn_domain::RuntimeEvent::CircuitBreakerTripped(CircuitBreakerTripped {
                    project: ctx.project.clone(),
                    session_id: ctx.session_id.clone(),
                    run_id: ctx.run_id.clone(),
                    trip: trip.clone(),
                    at_ms,
                }),
            );
            if let Err(e) = self.store.append(&[envelope]).await {
                tracing::warn!(
                    run_id = %ctx.run_id,
                    error = %e,
                    "F65 PR-3: failed to append CircuitBreakerTripped event — \
                     SSE/dashboard still reflect the trip, durable event log missed it"
                );
            }
            // Observability: increment trip counter + measured-at-trip
            // histogram so dashboards can track how often each breaker
            // fires and how far past the threshold runs are actually
            // landing. `NoToolUseConsecutive` skips the histogram (no
            // distribution to learn from — it always trips at exactly
            // the cap).
            crate::metrics::record_breaker_trip(self.metrics.as_ref(), trip.which, trip.measured);
        }
        async fn on_budget_threshold_crossed(
            &self,
            ctx: &cairn_orchestrator::OrchestrationContext,
            which: cairn_domain::session_orchestration::BreakerKind,
            measured: u64,
            limit: u64,
            ratio_bps: u32,
        ) {
            self.inner
                .on_budget_threshold_crossed(ctx, which, measured, limit, ratio_bps)
                .await;
            use cairn_domain::{BudgetThresholdCrossed, EventEnvelope, EventId, EventSource};
            use cairn_store::EventLog;
            let at_ms = crate::errors::now_ms();
            let envelope = EventEnvelope::for_runtime_event(
                EventId::new(format!("evt_budget_crossed_{}", uuid::Uuid::new_v4())),
                EventSource::System,
                cairn_domain::RuntimeEvent::BudgetThresholdCrossed(BudgetThresholdCrossed {
                    project: ctx.project.clone(),
                    session_id: ctx.session_id.clone(),
                    run_id: ctx.run_id.clone(),
                    which_breaker: which,
                    measured,
                    limit,
                    ratio_bps,
                    at_ms,
                }),
            );
            if let Err(e) = self.store.append(&[envelope]).await {
                tracing::warn!(
                    run_id = %ctx.run_id,
                    error = %e,
                    "F65 PR-3: failed to append BudgetThresholdCrossed event — \
                     warning surfaced on SSE but not persisted"
                );
            }
            crate::metrics::record_breaker_threshold_warn(self.metrics.as_ref(), which);
        }
        async fn on_finished(
            &self,
            ctx: &cairn_orchestrator::OrchestrationContext,
            t: &cairn_orchestrator::LoopTermination,
        ) {
            self.inner.on_finished(ctx, t).await;
        }
        fn take_fatal_error(&self) -> Option<String> {
            let mut slot = self.fatal_error.lock().unwrap_or_else(|p| p.into_inner());
            slot.take()
        }
    }
    let emitter: std::sync::Arc<dyn cairn_orchestrator::OrchestratorEventEmitter> =
        std::sync::Arc::new(TracingEmitter {
            inner: sse_emitter,
            store: state.runtime.store.clone(),
            exporter: state.otlp_exporter.clone(),
            fatal_error: std::sync::Mutex::new(None),
            metrics: state.metrics.clone(),
        });

    // RFC 020 Track 4 — dual checkpoint hook. Wires the orchestrator loop
    // to `CheckpointService::save_dual` so each iteration emits an Intent
    // checkpoint (post-decide, pre-execute) and a Result checkpoint
    // (post-execute), closing invariant #5 end-to-end.
    let dual_ckpt_hook: std::sync::Arc<dyn cairn_orchestrator::CheckpointHook> =
        std::sync::Arc::new(cairn_orchestrator::DualCheckpointHook::new(
            ctx.project.clone(),
            std::sync::Arc::new(CheckpointServiceImpl::new(state.runtime.store.clone())),
        ));

    match OrchestratorLoop::new(gather, decide, execute, cfg)
        .with_emitter(emitter)
        .with_checkpoint_hook(dual_ckpt_hook)
        .with_approval_reader(tool_call_approval_reader_for_drain)
        .run(ctx)
        .await
    {
        Ok(LoopTermination::Completed {
            summary,
            verification,
        }) => {
            // F47 PR2: persist the completion annotation via an
            // event-sourced `RunCompletionAnnotated`. Emitted AFTER
            // `runs.complete` has flipped the run to the terminal state
            // (the CompleteRun ActionType inside the loop already fired
            // that FCALL) — this event only annotates the terminal
            // run with the LLM summary + extractor-produced evidence.
            //
            // Failures to append are logged + swallowed so a transient
            // store outage never turns a successful run into an HTTP
            // error. The operator still has the summary in the HTTP
            // response body (and on the SSE `orchestrate_finished`
            // frame); the annotation is best-effort durability on top.
            use cairn_domain::{RunCompletionAnnotated, RuntimeEvent};
            use cairn_runtime::make_envelope;
            // Checked conversion (Copilot review on #313): `as_millis()`
            // returns `u128` and `as u64` would silently truncate past
            // year 584 million. `unwrap_or_default()` mapped pre-epoch
            // clocks to `0`. Clamp to `i64::MAX` ms (year 292 million)
            // rather than `u64::MAX` because the pg / sqlite
            // projection appliers `try_from::<i64>` the value — a
            // `u64::MAX` clamp would trip the projection error path
            // and block the annotation from persisting at all. An
            // `i64::MAX` clamp still produces an obviously-broken
            // timestamp operators can spot AND lands in the durable
            // projection. Pre-epoch (`duration_since` error) explicitly
            // clamps to `0`.
            let occurred_at_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| u64::try_from(d.as_millis().min(i64::MAX as u128)).unwrap_or(0))
                .unwrap_or(0);
            let annotation = make_envelope(RuntimeEvent::RunCompletionAnnotated(
                RunCompletionAnnotated {
                    project: run.project.clone(),
                    session_id: run.session_id.clone(),
                    run_id: run.run_id.clone(),
                    summary: summary.clone(),
                    verification: verification.clone(),
                    occurred_at_ms,
                },
            ));
            if let Err(e) = state.runtime.store.append(&[annotation]).await {
                tracing::warn!(
                    run_id = %run.run_id,
                    error = %e,
                    "F47 PR2: failed to persist RunCompletionAnnotated — \
                     completion summary + verification will not survive \
                     past the SSE stream on this run"
                );
            }
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "termination": "completed", "summary": summary, "model_id": model_id,
                })),
            )
                .into_response()
        }
        Ok(LoopTermination::Failed { reason }) => {
            // F53: flip run.state from Running to Failed. Without this,
            // GET /v1/runs/:id keeps reporting state=running forever and
            // the operator has no durable record of the terminal failure.
            // The "reason" string may mention lease expiry, provider error,
            // etc.; we map to a FailureClass heuristically and otherwise
            // default to ExecutionError.
            let failure_class = classify_failed_reason(&reason);
            finalize_run_failure(state.as_ref(), &run.session_id, &run.run_id, failure_class).await;
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "termination": "failed", "reason": reason,
                })),
            )
                .into_response()
        }
        Ok(LoopTermination::MaxIterationsReached) => {
            // F53: same treatment as Failed — max iterations is a terminal
            // failure and the run must not stay in state=running.
            finalize_run_failure(
                state.as_ref(),
                &run.session_id,
                &run.run_id,
                cairn_domain::FailureClass::ExecutionError,
            )
            .await;
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "termination": "max_iterations_reached",
                })),
            )
                .into_response()
        }
        Ok(LoopTermination::TimedOut) => {
            // F53: same treatment — wall-clock timeout is terminal.
            finalize_run_failure(
                state.as_ref(),
                &run.session_id,
                &run.run_id,
                cairn_domain::FailureClass::TimedOut,
            )
            .await;
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "termination": "timed_out",
                })),
            )
                .into_response()
        }
        Ok(LoopTermination::WaitingApproval { approval_id }) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({
                "termination": "waiting_approval", "approval_id": approval_id.as_str(),
            })),
        )
            .into_response(),
        Ok(LoopTermination::WaitingSubagent { child_task_id }) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({
                "termination": "waiting_subagent", "child_task_id": child_task_id.as_str(),
            })),
        )
            .into_response(),
        Ok(LoopTermination::PlanProposed { plan_markdown }) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "termination": "plan_proposed",
                "outcome": "plan_proposed",
                "plan_markdown": plan_markdown,
            })),
        )
            .into_response(),
        Ok(LoopTermination::BreakerTripped { trip }) => {
            // F65 PR-3: a circuit breaker tripped mid-run. The loop
            // already emitted `RuntimeEvent::CircuitBreakerTripped` via
            // the emitter hook (and appended a `Checkpoint` at the last
            // completed iteration); here we flip the run to the terminal
            // failure state so `GET /v1/runs/:id` stops reporting
            // `state=running`. Classification is `ExecutionError` —
            // breaker trips are operator-facing policy enforcement, not
            // timeout classes (the wall-clock breaker is distinct from
            // the legacy `timeout_ms` path which still maps to
            // `FailureClass::TimedOut`).
            finalize_run_failure(
                state.as_ref(),
                &run.session_id,
                &run.run_id,
                cairn_domain::FailureClass::ExecutionError,
            )
            .await;
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "termination": "breaker_tripped",
                    "which": trip.which,
                    "measured": trip.measured,
                    "limit": trip.limit,
                    "at_iteration": trip.at_iteration,
                })),
            )
                .into_response()
        }
        Err(e) => {
            // T6a-H9: log the full error details (for ops) but send a
            // sanitized stable message to the client. The full Display
            // may embed provider URLs, model names, partial LLM output,
            // credential fragments, etc. — none of which belong in a 5xx
            // body. User-caused errors (NotFound, InvalidTransition)
            // still surface a friendly code + short message.
            tracing::warn!(run_id = %run_id, error = %e, "orchestration failed");
            let (status, code, msg): (_, &'static str, String) = match &e {
                cairn_orchestrator::OrchestratorError::Runtime(
                    cairn_runtime::error::RuntimeError::NotFound { .. },
                ) => (StatusCode::NOT_FOUND, "not_found", e.to_string()),
                cairn_orchestrator::OrchestratorError::Runtime(
                    cairn_runtime::error::RuntimeError::InvalidTransition { .. },
                ) => (StatusCode::CONFLICT, "invalid_transition", e.to_string()),
                cairn_orchestrator::OrchestratorError::Gather(_) => (
                    StatusCode::BAD_GATEWAY,
                    "gather_error",
                    "upstream gather phase failed".to_owned(),
                ),
                cairn_orchestrator::OrchestratorError::Decide(_) => (
                    StatusCode::BAD_GATEWAY,
                    "decide_error",
                    "upstream decide phase failed".to_owned(),
                ),
                cairn_orchestrator::OrchestratorError::AllProvidersExhausted { attempts } => {
                    // F15 + F17: every binding × model in the routed chain
                    // failed with fallback-eligible errors. Surface a
                    // single ToolCallApprovalService proposal with the
                    // full summary so the operator can rotate credentials,
                    // add a provider, or abort.
                    // SEC-007: redact summary before handing it to logs or
                    // to the operator-facing approval card. `summary` is
                    // built from `ProviderAdapterError::to_string()` which
                    // may embed upstream response bodies; those can echo
                    // bearer tokens if a misconfigured provider rejected
                    // the request with the auth header included.
                    let summary = cairn_providers::redact_secrets(
                        &cairn_orchestrator::format_attempt_summary(attempts),
                    );
                    // Best-effort: if the approval submission itself fails
                    // (store-append error, cache issue) we still return 502
                    // with the inline summary, but we MUST log the drop so
                    // operators have a trace that no card appeared in the
                    // tool-call-approvals UI. Never silently discard a
                    // `store.append`-backed Result.
                    if let Err(err) = submit_all_providers_exhausted_proposal(
                        state.as_ref(),
                        &run,
                        &model_id,
                        attempts,
                        &summary,
                    )
                    .await
                    {
                        tracing::error!(
                            run_id = %run.run_id,
                            error = %err,
                            "failed to submit providers-exhausted tool-call approval; operator will not see the card in the UI (HTTP 502 body still carries the summary)"
                        );
                    }
                    // SEC-007: `summary` + `a.error_message` are built from
                    // `ProviderAdapterError::to_string()` which for
                    // `ServerError` / `StructuredOutputInvalid` carries the
                    // upstream response body (truncated + redacted, but
                    // still provider-internal). Log the full detail and
                    // return only classification to the caller. The
                    // operator can correlate via `run_id` in the logs or
                    // view the full summary in the tool-call-approval
                    // card submitted above.
                    tracing::warn!(
                        run_id = %run_id,
                        attempt_count = attempts.len(),
                        full_summary = %summary,
                        "all providers exhausted during orchestration"
                    );
                    // Closes #416: canonical envelope (`status_code`,
                    // `code`, `message`, `request_id`) with per-attempt
                    // diagnostics and termination sentinel folded under
                    // `details`. SDK parsers keyed on `code`/`message`
                    // previously saw `null` because the outer object used
                    // `error_code`/`remediation` as peer fields.
                    let remediation = "One or more of: rotate credentials, top up provider credits, add a provider connection via POST /v1/providers/connections, update system defaults via PUT /v1/settings/defaults/system/brain_model (or generate_model), or edit a connection's `supported_models`. Full per-model failure summary is available in the tool-call-approvals UI.";
                    let details = serde_json::json!({
                        "termination": "providers_exhausted",
                        "attempts": attempts.iter().map(|a| serde_json::json!({
                            "model_id": a.model_id,
                            "reason_code": a.reason_code,
                        })).collect::<Vec<_>>(),
                    });
                    return api_error_with_details(
                        StatusCode::BAD_GATEWAY,
                        "all_providers_exhausted",
                        remediation,
                        details,
                    );
                }
                cairn_orchestrator::OrchestratorError::ProviderAuthFailed {
                    binding_id,
                    model_id: m,
                    detail,
                } => {
                    // SEC-007: `detail` is built by openai_compat from the
                    // upstream response body + the provider's internal
                    // config name. Never forward that to the API caller —
                    // it can carry credential-adjacent fragments or
                    // proprietary internals. Run through `redact_secrets`
                    // even in server-side logs so any bearer tokens / keys
                    // that happened to echo back in the upstream body get
                    // scrubbed before hitting log aggregators. Response
                    // body carries only a stable opaque classification.
                    let detail_safe = cairn_providers::redact_secrets(detail);
                    tracing::warn!(
                        run_id = %run_id,
                        binding_id = %binding_id,
                        model_id = %m,
                        detail = %detail_safe,
                        "provider auth failed during orchestration"
                    );
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "provider_auth_failed",
                        "Provider authentication failed. Rotate the credential via POST /v1/admin/credentials/rotate or update the provider connection.".to_owned(),
                    )
                }
                cairn_orchestrator::OrchestratorError::ProviderInvalidRequest {
                    binding_id,
                    model_id: m,
                    detail,
                } => {
                    // Same SEC-007 redaction rationale as ProviderAuthFailed.
                    let detail_safe = cairn_providers::redact_secrets(detail);
                    tracing::warn!(
                        run_id = %run_id,
                        binding_id = %binding_id,
                        model_id = %m,
                        detail = %detail_safe,
                        "provider rejected request during orchestration"
                    );
                    (
                        StatusCode::BAD_GATEWAY,
                        "provider_invalid_request",
                        "Provider rejected the request. This indicates a bug in cairn's prompt construction — please file an issue.".to_owned(),
                    )
                }
                _ => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "orchestration_error",
                    "orchestration failed — see server logs".to_owned(),
                ),
            };
            AppApiError::new(status, code, msg).into_response()
        }
    }
}

/// Submit a `ToolCallApprovalService::submit_proposal` proposal summarising
/// the fallback-chain exhaustion so the operator gets an actionable card in
/// the new tool-call-approvals UI.
///
/// Best-effort: failures are logged but don't re-fail the caller since the
/// caller is already returning an HTTP 502 with the summary inline.
async fn submit_all_providers_exhausted_proposal(
    state: &AppState,
    run: &cairn_store::projections::RunRecord,
    preferred_model: &str,
    attempts: &[cairn_orchestrator::FallbackAttempt],
    summary: &str,
) -> Result<(), String> {
    use cairn_runtime::ToolCallApprovalService as _;

    let tool_call_approval_reader = Arc::new(
        cairn_runtime::services::ToolCallApprovalReaderAdapter::new(state.runtime.store.clone()),
    );
    let svc = cairn_runtime::services::ToolCallApprovalServiceImpl::new(
        state.runtime.store.clone(),
        tool_call_approval_reader,
    );

    let call_id = cairn_domain::ToolCallId::new(format!(
        "tc_providers_exhausted_{}_{}",
        run.run_id.as_str(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    ));
    let tcp = cairn_runtime::ToolCallProposal {
        call_id,
        project: run.project.clone(),
        session_id: run.session_id.clone(),
        run_id: run.run_id.clone(),
        tool_name: "escalate_to_operator".to_owned(),
        tool_args: serde_json::json!({
            "reason": "all_providers_exhausted",
            "preferred_model": preferred_model,
            "summary": summary,
            "attempts": attempts.iter().map(|a| serde_json::json!({
                "model_id": a.model_id,
                "reason_code": a.reason_code,
                "error": a.error_message,
            })).collect::<Vec<_>>(),
            "suggested_actions": [
                "Update system defaults `brain_model` / `generate_model` to a model you have credits for",
                "Edit a provider connection's `supported_models` list to include a working model",
                "Top up free-tier credits on your configured provider (OpenRouter, etc.)",
                "Add a new provider connection via POST /v1/providers/connections",
                "Abort the run",
            ],
        }),
        // Build a descriptive one-liner that distinguishes this card
        // from other providers-exhausted events at a glance: include
        // run_id + the first attempt's reason so the operator can see
        // the dominant failure mode without expanding the card.
        display_summary: Some(format!(
            "providers exhausted on run {} ({} models tried; first failure: {})",
            run.run_id.as_str(),
            attempts.len(),
            attempts.first().map(|a| a.reason_code).unwrap_or("unknown"),
        )),
        match_policy: cairn_domain::approvals::ApprovalMatchPolicy::Exact,
    };

    svc.submit_proposal(tcp).await.map(|_| ()).map_err(|e| {
        tracing::warn!(error = %e, "failed to submit providers-exhausted tool-call approval");
        e.to_string()
    })
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

pub(crate) async fn list_tenant_costs_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Query(query): Query<TenantCostQuery>,
) -> impl IntoResponse {
    // #423: honest pagination. The previous shape returned every
    // session-cost row for the tenant in one payload; a tenant with
    // six months of activity could produce 100k+ rows, an OOM and
    // latency hazard the store's own read path did not bound. Fetch
    // `limit + 1` at the store layer and flip `has_more` on overflow.
    let limit = query.limit();
    let offset = query.offset();
    match SessionCostReadModel::list_by_tenant(
        state.runtime.store.as_ref(),
        tenant_scope.tenant_id(),
        query.since_ms.unwrap_or(0),
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
        Err(err) => store_error_response(err),
    }
}

// ── Plan review handlers (RFC 018) ───────────────────────────────────────────

/// POST /v1/runs/:plan_run_id/approve
pub(crate) async fn approve_plan_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Extension(principal): Extension<AuthPrincipal>,
    Path(plan_run_id): Path<String>,
    // PR #555 review (Copilot): surface `deny_unknown_fields` / wrong-
    // type rejections through the canonical `Error` envelope rather
    // than axum's default body. Matches the OpenAPI 422 contract.
    body: Result<Json<ApprovePlanRequest>, axum::extract::rejection::JsonRejection>,
) -> impl IntoResponse {
    use cairn_domain::events::PlanApproved;
    use cairn_runtime::make_envelope;

    let Json(body) = match body {
        Ok(b) => b,
        Err(err) => return crate::errors::json_rejection_response(err),
    };
    let run_id = RunId::new(&plan_run_id);

    // T6a-C2: tenant scope check.
    let run = match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(r)) => r,
        Ok(None) => {
            return run_not_found_response();
        }
        Err(response) => return response,
    };

    // #427: reviewer_comments is now a typed Option<String>. Normalise
    // empty strings to None so audit rows carry `null` rather than `""`.
    //
    // Copilot review: this IS a semantic change from the pre-typing
    // behaviour (not a preservation). The old `Value::as_str()` would
    // have returned `Some("")` for `""`, and `.map(str::to_owned)`
    // produced `Some("".to_owned())`. We intentionally collapse empty
    // to None now because an empty `reviewer_comments` audit row is
    // operator noise, not a signal worth preserving.
    let reviewer_comments = body.reviewer_comments.filter(|s| !s.is_empty());
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // T6a-H7: use the authenticated principal as the actor rather than a
    // hardcoded "operator" literal. Pre-fix, every plan approval was
    // attributed to a fake user — breaking audit integrity in team
    // deployments where multiple operators review plans.
    let evt = make_envelope(cairn_domain::RuntimeEvent::PlanApproved(PlanApproved {
        project: run.project.clone(),
        plan_run_id: run_id,
        approved_by: cairn_domain::OperatorId::new(crate::handlers::admin::audit_actor_id(
            &principal,
        )),
        reviewer_comments,
        approved_at: now_ms,
    }));

    if let Err(e) = state.runtime.store.append(&[evt]).await {
        return AppApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "store_error",
            e.to_string(),
        )
        .into_response();
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "plan_run_id": plan_run_id,
            "status": "approved",
            "next_step": "create_execute_run",
        })),
    )
        .into_response()
}

/// POST /v1/runs/:plan_run_id/reject
pub(crate) async fn reject_plan_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Extension(principal): Extension<AuthPrincipal>,
    Path(plan_run_id): Path<String>,
    // PR #555 review (Copilot): canonical Error envelope on rejections.
    body: Result<Json<RejectPlanRequest>, axum::extract::rejection::JsonRejection>,
) -> impl IntoResponse {
    use cairn_domain::events::PlanRejected;
    use cairn_runtime::make_envelope;

    let Json(body) = match body {
        Ok(b) => b,
        Err(err) => return crate::errors::json_rejection_response(err),
    };
    let run_id = RunId::new(&plan_run_id);

    // T6a-C2: tenant scope check.
    let run = match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(r)) => r,
        Ok(None) => {
            return run_not_found_response();
        }
        Err(response) => return response,
    };

    // #427: reason is now an Option<String>; default to "rejected by
    // operator" when the caller omits it OR sends an empty string.
    //
    // Copilot review: this IS a semantic change from the pre-typing
    // path. The old `.as_str().unwrap_or("rejected by operator")`
    // returned the literal `""` when the client sent an empty
    // string — it only defaulted when the field was absent. We
    // intentionally collapse empty-to-default here because an empty
    // `reason` audit row is operator noise, not a meaningful signal.
    let reason = body
        .reason
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "rejected by operator".to_owned());
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // T6a-H7: audit with the real principal, not a hardcoded "operator".
    let evt = make_envelope(cairn_domain::RuntimeEvent::PlanRejected(PlanRejected {
        project: run.project.clone(),
        plan_run_id: run_id,
        rejected_by: cairn_domain::OperatorId::new(crate::handlers::admin::audit_actor_id(
            &principal,
        )),
        reason,
        rejected_at: now_ms,
    }));

    if let Err(e) = state.runtime.store.append(&[evt]).await {
        return AppApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "store_error",
            e.to_string(),
        )
        .into_response();
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "plan_run_id": plan_run_id,
            "status": "rejected",
        })),
    )
        .into_response()
}

/// POST /v1/runs/:plan_run_id/revise
pub(crate) async fn revise_plan_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(plan_run_id): Path<String>,
    // PR #555 review (Copilot): canonical Error envelope on rejections.
    body: Result<Json<RevisePlanRequest>, axum::extract::rejection::JsonRejection>,
) -> impl IntoResponse {
    use cairn_domain::events::PlanRevisionRequested;
    use cairn_runtime::make_envelope;

    let Json(body) = match body {
        Ok(b) => b,
        Err(err) => return crate::errors::json_rejection_response(err),
    };
    let original_run_id = RunId::new(&plan_run_id);

    // T6a-C2: tenant scope check.
    let original_run =
        match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &original_run_id).await {
            Ok(Some(r)) => r,
            Ok(None) => {
                return run_not_found_response();
            }
            Err(response) => return response,
        };

    // #427: reviewer_comments is now a typed String. An empty string
    // is still a client error (a revise without comments is the same
    // as not reviewing).
    let reviewer_comments = body.reviewer_comments;
    if reviewer_comments.is_empty() {
        return bad_request_response("reviewer_comments is required for revise");
    }

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // Create a new Plan-mode run for the revision.
    let new_run_id = RunId::new(format!("run_{now_ms}_rev"));
    let before = current_event_head(&state).await;
    match state
        .runtime
        .runs
        .start(
            &original_run.project,
            &original_run.session_id,
            new_run_id.clone(),
            Some(original_run_id.clone()),
        )
        .await
    {
        Ok(_) => {}
        Err(err) => return runtime_error_response(err),
    }

    // Emit PlanRevisionRequested event.
    let evt = make_envelope(cairn_domain::RuntimeEvent::PlanRevisionRequested(
        PlanRevisionRequested {
            project: original_run.project.clone(),
            original_plan_run_id: original_run_id,
            new_plan_run_id: new_run_id.clone(),
            reviewer_comments,
            requested_at: now_ms,
        },
    ));

    if let Err(e) = state.runtime.store.append(&[evt]).await {
        return AppApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "store_error",
            e.to_string(),
        )
        .into_response();
    }

    publish_runtime_frames_since(&state, before).await;

    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "plan_run_id": plan_run_id,
            "new_plan_run_id": new_run_id.as_str(),
            "status": "revision_requested",
        })),
    )
        .into_response()
}

// ── Record checkpoint ────────────────────────────────────────────────────────

/// `POST /v1/runs/:id/checkpoint` -- record a checkpoint for a run.
///
/// Alias for `save_checkpoint_handler`; provides the `record_checkpoint_handler`
/// name expected by the preserved route catalog and audit tests.
///
/// #370: tenant-scoped — forwards the `TenantScope` extractor into the
/// underlying handler so the alias path is not a bypass.
#[allow(dead_code)]
pub(crate) async fn record_checkpoint_handler(
    state: State<Arc<AppState>>,
    tenant_scope: crate::extractors::TenantScope,
    path: Path<String>,
    body: Json<crate::SaveCheckpointRequest>,
) -> impl IntoResponse {
    crate::save_checkpoint_handler(state, tenant_scope, path, body).await
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

    // ── F29 CD: redact_provider_error ───────────────────────────────
    //
    // Security-sensitive: this function is the last line of defence
    // against an upstream provider echoing a live Authorization header
    // into an error payload. The matrix below locks in the three cases
    // the telemetry handler relies on.

    #[test]
    fn redact_provider_error_drops_leaked_bearer_key() {
        // Build the leaked marker at runtime so GitGuardian static scans
        // don't flag this literal as a real credential.
        let marker = format!("sk-{}", "fake-test-only-".to_owned() + &"x".repeat(24));
        let leaked = format!("upstream returned: Authorization: Bearer {marker} denied");
        let out = redact_provider_error(Some(&leaked)).expect("some");
        assert!(
            !out.contains(&marker),
            "leaked credential pattern must not survive: {out}"
        );
        assert!(
            out.contains("<redacted"),
            "expected explicit redaction marker, got: {out}"
        );
    }

    #[test]
    fn redact_provider_error_truncates_oversize_payload() {
        let huge = "x".repeat(4096);
        let out = redact_provider_error(Some(&huge)).expect("some");
        // Truncation cap is 1024 chars + 3-char ellipsis.
        assert!(out.len() <= 1024 + 3, "length cap broken: {}", out.len());
        assert!(out.ends_with("..."), "ellipsis marker missing: {out}");
    }

    #[test]
    fn redact_provider_error_passes_clean_message_through() {
        let msg = "provider returned 500: upstream timeout";
        assert_eq!(
            redact_provider_error(Some(msg)).as_deref(),
            Some(msg),
            "clean message must pass through verbatim"
        );
    }

    #[test]
    fn redact_provider_error_none_stays_none() {
        assert_eq!(redact_provider_error(None), None);
    }
}
