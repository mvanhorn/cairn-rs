//! Session HTTP handlers and request/response DTOs.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};

use cairn_api::http::{ApiError, ListResponse};
use cairn_domain::{ProjectKey, SessionId};
use cairn_store::projections::{
    LlmCallTraceReadModel, RunCostReadModel, RunReadModel, RunRecord, SessionCostReadModel,
    SessionRecord, TaskReadModel,
};
use cairn_store::{EntityRef, EventLog, EventPosition, StoredEvent};
use utoipa::ToSchema;

use crate::errors::{
    parse_session_state, runtime_error_response, store_error_response, validation_error_response,
    AppApiError,
};
use crate::extractors::{AdminRoleGuard, HasProjectScope, ProjectJson, ProjectScope, TenantScope};
use crate::state::AppState;
use crate::{
    event_message, event_type_name, runtime_event_to_activity_entry, ActivityEntry, EventSummary,
    EventsPage, EventsPageQuery, DEFAULT_PROJECT_ID, DEFAULT_TENANT_ID, DEFAULT_WORKSPACE_ID,
};
#[allow(unused_imports)]
use crate::{SessionListResponseDoc, SessionRecordDoc};

// ── DTOs ─────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct SessionDetailResponse {
    pub(crate) session: SessionRecord,
    pub(crate) runs: Vec<RunRecord>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct SessionActivity {
    pub(crate) session_id: String,
    pub(crate) entries: Vec<ActivityEntry>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct SessionCostResponse {
    #[serde(flatten)]
    pub(crate) summary: cairn_domain::providers::SessionCostRecord,
    pub(crate) run_breakdown: Vec<cairn_domain::providers::RunCostRecord>,
}

#[derive(Clone, Debug, serde::Deserialize, ToSchema)]
pub(crate) struct CreateSessionRequest {
    pub(crate) tenant_id: String,
    pub(crate) workspace_id: String,
    pub(crate) project_id: String,
    pub(crate) session_id: String,
}

impl CreateSessionRequest {
    pub(crate) fn project(&self) -> ProjectKey {
        ProjectKey::new(
            self.tenant_id.as_str(),
            self.workspace_id.as_str(),
            self.project_id.as_str(),
        )
    }
}

impl HasProjectScope for CreateSessionRequest {
    fn project(&self) -> ProjectKey {
        CreateSessionRequest::project(self)
    }
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub(crate) struct SessionListQuery {
    // Scope fields are optional at the HTTP boundary: bare calls
    // (e.g. first-load UI without localStorage scope) fall back to
    // the default tenant/workspace/project rather than 422-ing on
    // missing query params. Non-default scopes still flow through
    // `validate_project_scope` for tenant-mismatch rejection.
    #[serde(default)]
    pub(crate) tenant_id: Option<String>,
    #[serde(default)]
    pub(crate) workspace_id: Option<String>,
    #[serde(default)]
    pub(crate) project_id: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
}

impl SessionListQuery {
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

impl HasProjectScope for SessionListQuery {
    fn project(&self) -> ProjectKey {
        SessionListQuery::project(self)
    }
}

// ── Handlers ─────────────────────────────────────────────────────────────────

#[utoipa::path(
    get,
    path = "/v1/sessions",
    tag = "runtime",
    responses(
        (status = 200, description = "Sessions listed", body = SessionListResponseDoc),
        (status = 400, description = "Invalid request", body = ApiError),
        (status = 401, description = "Unauthorized", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
pub(crate) async fn list_sessions_handler(
    State(state): State<Arc<AppState>>,
    project_scope: ProjectScope<SessionListQuery>,
) -> impl IntoResponse {
    let query = project_scope.into_inner();
    let status_filter = match query.status.as_deref().map(parse_session_state).transpose() {
        Ok(status_filter) => status_filter,
        Err(err) => return validation_error_response(err),
    };
    let limit = query.limit();

    match state
        .runtime
        .sessions
        .list(
            &SessionListQuery::project(&query),
            query.offset() + limit + 1,
            0,
        )
        .await
    {
        Ok(items) => {
            let mut items: Vec<SessionRecord> = items
                .into_iter()
                .filter(|session| {
                    status_filter.is_none_or(|status_filter| session.state == status_filter)
                })
                .skip(query.offset())
                .take(limit + 1)
                .collect();
            let has_more = items.len() > limit;
            items.truncate(limit);
            (StatusCode::OK, Json(ListResponse { items, has_more })).into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn get_session_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state
        .runtime
        .sessions
        .lookup_any_admin(&SessionId::new(id))
        .await
    {
        // Admin tokens bypass the per-tenant scope check so they can
        // view sessions across any tenant (mirrors the pattern in
        // `tasks.rs`/`runs.rs`/`approvals.rs`). Without this, admin
        // callers get spurious 404s on non-default-tenant sessions.
        Ok(Some(session))
            if tenant_scope.is_admin || session.project.tenant_id == *tenant_scope.tenant_id() =>
        {
            match RunReadModel::list_by_session(
                state.runtime.store.as_ref(),
                &session.session_id,
                200,
                0,
            )
            .await
            {
                Ok(runs) => (
                    StatusCode::OK,
                    Json(SessionDetailResponse { session, runs }),
                )
                    .into_response(),
                Err(err) => store_error_response(err),
            }
        }
        Ok(Some(_)) | Ok(None) => {
            AppApiError::new(StatusCode::NOT_FOUND, "not_found", "session not found")
                .into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn get_session_activity_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let session_id = SessionId::new(id.clone());

    match state.runtime.sessions.lookup_any_admin(&session_id).await {
        Ok(Some(s))
            if tenant_scope.is_admin || s.project.tenant_id == *tenant_scope.tenant_id() => {}
        Ok(Some(_)) | Ok(None) => {
            return AppApiError::new(StatusCode::NOT_FOUND, "not_found", "session not found")
                .into_response();
        }
        Err(err) => return runtime_error_response(err),
    }

    let runs = match RunReadModel::list_by_session(
        state.runtime.store.as_ref(),
        &session_id,
        200,
        0,
    )
    .await
    {
        Ok(r) => r,
        Err(err) => return store_error_response(err),
    };

    let mut entries: Vec<ActivityEntry> = Vec::new();

    for run in &runs {
        // Read run-scoped events
        match state
            .runtime
            .store
            .read_by_entity(&EntityRef::Run(run.run_id.clone()), None, 200)
            .await
        {
            Ok(events) => {
                for stored in events {
                    if let Some(entry) =
                        runtime_event_to_activity_entry(&stored.envelope.payload, stored.stored_at)
                    {
                        entries.push(entry);
                    }
                }
            }
            Err(err) => return store_error_response(err),
        }

        // Read task-scoped events for each task in this run
        let tasks =
            match TaskReadModel::list_by_parent_run(state.runtime.store.as_ref(), &run.run_id, 200)
                .await
            {
                Ok(t) => t,
                Err(err) => return store_error_response(err),
            };

        for task in &tasks {
            match state
                .runtime
                .store
                .read_by_entity(&EntityRef::Task(task.task_id.clone()), None, 200)
                .await
            {
                Ok(events) => {
                    for stored in events {
                        if let Some(entry) = runtime_event_to_activity_entry(
                            &stored.envelope.payload,
                            stored.stored_at,
                        ) {
                            entries.push(entry);
                        }
                    }
                }
                Err(err) => return store_error_response(err),
            }
        }
    }

    entries.sort_by_key(|e| e.timestamp_ms);
    // Return last 100 entries
    let len = entries.len();
    if len > 100 {
        entries.drain(0..len - 100);
    }

    (
        StatusCode::OK,
        Json(SessionActivity {
            session_id: id,
            entries,
        }),
    )
        .into_response()
}

pub(crate) async fn get_session_active_runs_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
    Query(query): Query<crate::handlers::admin::PaginationQuery>,
) -> impl IntoResponse {
    let session_id = SessionId::new(id.clone());

    match state.runtime.sessions.lookup_any_admin(&session_id).await {
        Ok(Some(s))
            if tenant_scope.is_admin || s.project.tenant_id == *tenant_scope.tenant_id() => {}
        Ok(Some(_)) | Ok(None) => {
            return AppApiError::new(StatusCode::NOT_FOUND, "not_found", "session not found")
                .into_response();
        }
        Err(err) => return runtime_error_response(err),
    }

    // #422: the store's `list_by_session` sorts by `created_at` ASC
    // and does not push the state filter down, so a session with
    // thousands of historical terminal runs would hide active runs
    // past the scan window (cursor / gemini / copilot review). Fix:
    // iterate in chunks, filter to active per chunk, stop when either
    //   (a) we have enough rows to satisfy offset + limit + 1 active
    //       rows (we then know `has_more` honestly), OR
    //   (b) the store returns a short chunk (end of session reached).
    // A total-runs ceiling bounds worst-case CPU: we do at most
    // MAX_SCAN_CHUNKS × CHUNK_SIZE read ops, and the filter pass is
    // O(rows) so the full walk is a known quantity. Sessions that
    // legitimately have more than MAX_SCAN_CHUNKS × CHUNK_SIZE runs
    // are pathological and the endpoint surfaces `has_more=true`
    // with whatever active rows we did find.
    const CHUNK_SIZE: usize = 500;
    const MAX_SCAN_CHUNKS: usize = 40; // 40 × 500 = 20 000 runs scanned

    let offset = query.offset();
    let limit = query.limit();
    let needed = offset.saturating_add(limit).saturating_add(1);

    let mut active: Vec<RunRecord> = Vec::new();
    let mut scanned_chunks = 0usize;
    let mut scan_offset = 0usize;
    let mut reached_end = false;
    loop {
        let chunk = match RunReadModel::list_by_session(
            state.runtime.store.as_ref(),
            &session_id,
            CHUNK_SIZE,
            scan_offset,
        )
        .await
        {
            Ok(r) => r,
            Err(err) => return store_error_response(err),
        };
        let chunk_len = chunk.len();
        active.extend(chunk.into_iter().filter(|r| !r.state.is_terminal()));
        scanned_chunks += 1;
        if chunk_len < CHUNK_SIZE {
            reached_end = true;
            break;
        }
        if active.len() >= needed {
            break;
        }
        if scanned_chunks >= MAX_SCAN_CHUNKS {
            break;
        }
        scan_offset = scan_offset.saturating_add(CHUNK_SIZE);
    }

    let cap_exceeded = !reached_end && active.len() < needed;
    let total = active.len();
    let items: Vec<RunRecord> = active.into_iter().skip(offset).take(limit).collect();
    let has_more = cap_exceeded || offset.saturating_add(items.len()) < total;

    (
        StatusCode::OK,
        Json(ListResponse::<RunRecord> { items, has_more }),
    )
        .into_response()
}

pub(crate) async fn get_session_cost_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let session_id = SessionId::new(id);
    match state.runtime.sessions.lookup_any_admin(&session_id).await {
        Ok(Some(session))
            if tenant_scope.is_admin || session.project.tenant_id == *tenant_scope.tenant_id() =>
        {
            match SessionCostReadModel::get_session_cost(state.runtime.store.as_ref(), &session_id)
                .await
            {
                Ok(Some(record)) => {
                    match RunCostReadModel::list_by_session(
                        state.runtime.store.as_ref(),
                        &session_id,
                    )
                    .await
                    {
                        Ok(run_breakdown) => (
                            StatusCode::OK,
                            Json(SessionCostResponse {
                                summary: record,
                                run_breakdown,
                            }),
                        )
                            .into_response(),
                        Err(err) => store_error_response(err),
                    }
                }
                Ok(None) => {
                    AppApiError::new(StatusCode::NOT_FOUND, "not_found", "session cost not found")
                        .into_response()
                }
                Err(err) => store_error_response(err),
            }
        }
        Ok(Some(_)) | Ok(None) => {
            AppApiError::new(StatusCode::NOT_FOUND, "not_found", "session not found")
                .into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

/// `GET /v1/sessions/:id/llm-traces` — per-session LLM call trace history (GAP-010).
///
/// Returns up to 200 traces for the session, most-recent first.
/// Each trace records model, tokens, latency, and cost for one provider call.
pub(crate) async fn get_session_llm_traces_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let session_id = SessionId::new(id);

    // Verify the session exists and belongs to the requesting tenant
    // (admin tokens bypass the scope check).
    match state.runtime.sessions.lookup_any_admin(&session_id).await {
        Ok(Some(s))
            if tenant_scope.is_admin || s.project.tenant_id == *tenant_scope.tenant_id() => {}
        Ok(Some(_)) | Ok(None) => {
            return AppApiError::new(StatusCode::NOT_FOUND, "not_found", "session not found")
                .into_response();
        }
        Err(err) => return runtime_error_response(err),
    }

    match LlmCallTraceReadModel::list_by_session(state.runtime.store.as_ref(), &session_id, 200)
        .await
    {
        Ok(traces) => (
            StatusCode::OK,
            Json(serde_json::json!({ "traces": traces })),
        )
            .into_response(),
        Err(err) => store_error_response(err),
    }
}

pub(crate) async fn list_session_events_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
    Query(query): Query<EventsPageQuery>,
) -> impl IntoResponse {
    let session_id = SessionId::new(id);
    let session = match state.runtime.sessions.lookup_any_admin(&session_id).await {
        Ok(Some(session))
            if tenant_scope.is_admin || session.project.tenant_id == *tenant_scope.tenant_id() =>
        {
            session
        }
        Ok(Some(_)) | Ok(None) => {
            return AppApiError::new(StatusCode::NOT_FOUND, "not_found", "session not found")
                .into_response();
        }
        Err(err) => return runtime_error_response(err),
    };

    let limit = query.limit.unwrap_or(50).clamp(1, 500);
    let cursor = query.cursor.map(EventPosition);

    let fetched = match state
        .runtime
        .store
        .read_by_entity(
            &EntityRef::Session(session.session_id.clone()),
            cursor,
            limit + 1,
        )
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

    let events = page
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

/// Max length for a caller-supplied `session_id`. Keeps the HTTP body
/// bounded and prevents unbounded growth in projections / logs.
const SESSION_ID_MAX_LEN: usize = 256;

#[utoipa::path(
    post,
    path = "/v1/sessions",
    tag = "runtime",
    request_body = CreateSessionRequest,
    responses(
        (status = 201, description = "Session created", body = SessionRecordDoc),
        (status = 401, description = "Unauthorized", body = ApiError),
        (status = 409, description = "Session already exists", body = ApiError),
        (status = 422, description = "Unprocessable entity", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
pub(crate) async fn create_session_handler(
    State(state): State<Arc<AppState>>,
    project_scope: ProjectJson<CreateSessionRequest>,
) -> impl IntoResponse {
    let body = project_scope.into_inner();

    // Validate session_id shape (closes #229 — previously empty and
    // 10k-char ids both returned 201). We trim first, then validate
    // and persist the trimmed value so `"  sess-abc  "` stores as
    // `sess-abc` instead of leaking whitespace into the projection /
    // any downstream lookup.
    let trimmed = body.session_id.trim().to_owned();
    if trimmed.is_empty() {
        return validation_error_response("session_id must not be empty");
    }
    if trimmed.len() > SESSION_ID_MAX_LEN {
        return validation_error_response(format!(
            "session_id exceeds max length {SESSION_ID_MAX_LEN}"
        ));
    }

    let session_id = SessionId::new(trimmed);
    let project = CreateSessionRequest::project(&body);

    // Reject duplicates with 409 instead of silently returning 201
    // (closes #229). Mirrors `CredentialServiceImpl::store`.
    //
    // Scoped get (#439, Gemini review on #554): use the project-scoped
    // service method rather than `lookup_any_admin`. A session that
    // exists in a different tenant MUST NOT surface as a 409 here — a
    // cross-tenant 409 would leak the existence of foreign session
    // ids to unauthenticated probers (SEC-007 pattern). The scoped
    // `get` returns `None` for foreign ids so the create path
    // proceeds, FabricSessionService then fails atomically if the
    // fabric-side id really collides.
    match state.runtime.sessions.get(&project, &session_id).await {
        Ok(Some(_)) => {
            return AppApiError::new(
                StatusCode::CONFLICT,
                "conflict",
                format!("session already exists: {}", session_id.as_str()),
            )
            .into_response();
        }
        Ok(None) => {}
        Err(err) => return runtime_error_response(err),
    }

    match state.runtime.sessions.create(&project, session_id).await {
        Ok(session) => (StatusCode::CREATED, Json(session)).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

/// `DELETE /v1/sessions/:id/snapshots` — F65 PR-5 admin endpoint that
/// immediately reaps every workspace snapshot belonging to a session.
/// Admin-only per Q4 locked decision. Returns
/// `{"reaped": <n>, "at_ms": <now>}` on success.
#[derive(Clone, Debug, serde::Serialize, ToSchema)]
pub(crate) struct SnapshotReapResponse {
    pub reaped: u32,
    pub at_ms: u64,
}

pub(crate) async fn delete_session_snapshots_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Path(session_id): Path<String>,
) -> impl IntoResponse {
    let trimmed = session_id.trim().to_owned();
    if trimmed.is_empty() {
        return validation_error_response("session_id must not be empty");
    }
    if trimmed.len() > SESSION_ID_MAX_LEN {
        return validation_error_response(format!(
            "session_id exceeds max length {SESSION_ID_MAX_LEN}"
        ));
    }
    let session_id = SessionId::new(trimmed);

    // Resolve the session's project scope first. The
    // `WorkspaceSnapshotReadModel::list_by_session` trait now requires
    // a `&ProjectKey` per issue #438 (defence-in-depth tenant guard at
    // the query layer). Admin-only endpoint, so returning NOT_FOUND
    // on a missing session is correct — admins who can reach this
    // handler can still target any tenant by supplying its SessionId.
    let session = match state.runtime.sessions.lookup_any_admin(&session_id).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            return AppApiError::new(StatusCode::NOT_FOUND, "not_found", "session not found")
                .into_response();
        }
        Err(err) => return runtime_error_response(err),
    };

    // Enumerate via the read model so we only reap rows that actually
    // exist + are still live (skipping already-reaped rows is idempotent).
    let snapshots = match <cairn_store::InMemoryStore as cairn_store::projections::WorkspaceSnapshotReadModel>::list_by_session(
        state.runtime.store.as_ref(),
        &session.project,
        &session_id,
    )
    .await
    {
        Ok(rows) => rows,
        Err(err) => return store_error_response(err),
    };

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default();

    let mut reaped = 0u32;
    for snap in snapshots {
        if snap.reaped_at.is_some() {
            continue;
        }
        // Reap the on-disk directory via SandboxService (honours in-
        // flight restore leases).
        match state.sandbox_service.reap_snapshot_dir(&snap.snapshot_id) {
            Ok(_) => {}
            Err(err) => {
                return crate::errors::AppApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    format!("reap snapshot dir failed: {err}"),
                )
                .into_response();
            }
        }
        // Emit the reap event via the store append path. Operators see
        // `reason="operator_cleared"` on metrics + telemetry; the domain
        // event shape intentionally doesn't carry the reason (plan §5).
        let envelope = cairn_domain::EventEnvelope::for_runtime_event(
            cairn_domain::EventId::new(format!(
                "evt_snap_reap_{}_{now_ms}",
                snap.snapshot_id.as_str()
            )),
            cairn_domain::EventSource::Runtime,
            cairn_domain::RuntimeEvent::WorkspaceSnapshotReaped(
                cairn_domain::WorkspaceSnapshotReaped {
                    project: snap.project.clone(),
                    snapshot_id: snap.snapshot_id.clone(),
                    at_ms: now_ms,
                },
            ),
        );
        if let Err(err) = state
            .runtime
            .store
            .append(std::slice::from_ref(&envelope))
            .await
        {
            return store_error_response(err);
        }
        reaped += 1;
    }

    (
        StatusCode::OK,
        Json(SnapshotReapResponse {
            reaped,
            at_ms: now_ms,
        }),
    )
        .into_response()
}

/// `DELETE /v1/admin/tenants/:tenant_id/sessions/:session_id` — admin
/// soft-delete. Mirrors PR BB's workspace pattern exactly: archive the
/// session via `SessionService::archive`, which issues the fabric-side
/// cancel + `cairn.archived` tag write and emits a `SessionArchived`
/// bridge event. Returns 204 on success, 404 when the session doesn't
/// belong to the supplied tenant (admin bypasses only the scope read
/// check; cross-tenant DELETE is still refused by id).
pub(crate) async fn delete_session_admin_handler(
    State(state): State<Arc<AppState>>,
    _role: AdminRoleGuard,
    Path((tenant_id, session_id)): Path<(String, String)>,
) -> impl IntoResponse {
    // Apply the same shape rules as `create_session_handler` — a URL
    // segment is still operator input, so an oversized or whitespace-
    // only `:session_id` should surface as 422 instead of hitting the
    // runtime with a 10k-char lookup key.
    let trimmed = session_id.trim().to_owned();
    if trimmed.is_empty() {
        return validation_error_response("session_id must not be empty");
    }
    if trimmed.len() > SESSION_ID_MAX_LEN {
        return validation_error_response(format!(
            "session_id exceeds max length {SESSION_ID_MAX_LEN}"
        ));
    }
    let session_id = SessionId::new(trimmed);

    // Enforce tenant-ownership: admin token may address any tenant, but
    // the URL's :tenant_id must actually own the session. Prevents a
    // mistyped path from silently archiving the wrong tenant's session.
    //
    // Issue #439: resolve the session's full project scope via
    // `lookup_any_admin` (admin cross-tenant intent is explicit in the
    // method name) and pass the authoritative scope through to
    // `archive`. The service layer re-checks that the stored project
    // equals the one we just observed, so a race that moves the
    // session to a new project between the two calls would be caught.
    let session_record = match state.runtime.sessions.lookup_any_admin(&session_id).await {
        Ok(Some(record)) if record.project.tenant_id.as_str() == tenant_id => record,
        Ok(Some(_)) | Ok(None) => {
            return AppApiError::new(StatusCode::NOT_FOUND, "not_found", "session not found")
                .into_response();
        }
        Err(err) => return runtime_error_response(err),
    };

    match state
        .runtime
        .sessions
        .archive(&session_record.project, &session_id)
        .await
    {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => runtime_error_response(err),
    }
}
