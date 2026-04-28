//! Feed items, mailbox, recent events, and stats handlers.
//!
//! Extracted from `lib.rs` — contains feed listing, mark-read, mailbox
//! CRUD, recent-events polling, and stats overview.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};

use cairn_api::feed::{FeedEndpoints, FeedQuery};
use cairn_api::http::ListResponse;
use cairn_domain::{ProjectKey, RunId, SessionId, TaskId};
use cairn_runtime::MailboxService;
use cairn_store::EventLog;

use crate::errors::{
    bad_request_response, now_ms, runtime_error_response, store_error_response, AppApiError,
};
use crate::extractors::OptionalProjectScopedQuery;
use crate::helpers::{event_message, event_type_name, mailbox_message_view, run_id_for_event};
use crate::state::{AppMailboxMessage, AppState, MailboxMessageView};

const DEFAULT_TENANT_ID: &str = "default_tenant";
const DEFAULT_WORKSPACE_ID: &str = "default_workspace";
const DEFAULT_PROJECT_ID: &str = "default_project";

// ── DTOs ────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Default, serde::Deserialize)]
#[allow(dead_code)]
pub(crate) struct FeedListQuery {
    pub tenant_id: Option<String>,
    pub workspace_id: Option<String>,
    pub project_id: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub before: Option<String>,
    pub source: Option<String>,
    pub unread: Option<bool>,
}

impl FeedListQuery {
    pub(crate) fn project(&self) -> ProjectKey {
        ProjectKey::new(
            self.tenant_id.as_deref().unwrap_or(DEFAULT_TENANT_ID),
            self.workspace_id.as_deref().unwrap_or(DEFAULT_WORKSPACE_ID),
            self.project_id.as_deref().unwrap_or(DEFAULT_PROJECT_ID),
        )
    }

    pub(crate) fn to_feed_query(&self) -> FeedQuery {
        FeedQuery {
            limit: self.limit,
            before: self.before.clone(),
            source: self.source.clone(),
            unread: self.unread,
        }
    }
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub(crate) struct MailboxListQuery {
    pub run_id: Option<String>,
    pub session_id: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl MailboxListQuery {
    pub(crate) fn limit(&self) -> usize {
        self.limit.unwrap_or(100).min(500)
    }

    pub(crate) fn offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct AppendMailboxRequest {
    pub tenant_id: String,
    pub workspace_id: String,
    pub project_id: String,
    pub message_id: Option<String>,
    pub run_id: Option<String>,
    pub task_id: Option<String>,
    pub sender_id: Option<String>,
    pub body: Option<String>,
}

impl AppendMailboxRequest {
    pub(crate) fn project(&self) -> ProjectKey {
        ProjectKey::new(
            self.tenant_id.as_str(),
            self.workspace_id.as_str(),
            self.project_id.as_str(),
        )
    }
}

// PaginationQuery is defined in admin.rs and re-exported via crate::*
use crate::handlers::admin::PaginationQuery;

// ── Handlers ────────────────────────────────────────────────────────────────

pub(crate) async fn list_feed_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<FeedListQuery>,
) -> impl IntoResponse {
    match state
        .feed
        .list(&query.project(), &query.to_feed_query())
        .await
    {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(err) => {
            tracing::error!("list_feed failed: {err}");
            AppApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                err.to_string(),
            )
            .into_response()
        }
    }
}

pub(crate) async fn mark_feed_item_read_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.feed.mark_read(&id).await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response(),
        Err(err) => AppApiError::new(StatusCode::NOT_FOUND, "not_found", err).into_response(),
    }
}

pub(crate) async fn mark_all_feed_items_read_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<OptionalProjectScopedQuery>,
) -> impl IntoResponse {
    match state.feed.read_all(&query.project()).await {
        Ok(changed) => (
            StatusCode::OK,
            Json(serde_json::json!({ "changed": changed })),
        )
            .into_response(),
        Err(err) => {
            tracing::error!("mark_all_feed_items_read failed: {err}");
            AppApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                err.to_string(),
            )
            .into_response()
        }
    }
}

pub(crate) async fn list_mailbox_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<MailboxListQuery>,
) -> impl IntoResponse {
    // #422: honest pagination. Two query paths:
    //   - by-run: push limit/offset into the service (`list_by_run`
    //     accepts both). Fetch `limit + 1`, derive `has_more`, truncate.
    //     Post-sort by `created_at` ASC: cursor-bugbot review flagged
    //     that removing the original unconditional post-sort left the
    //     by-run branch returning whatever order the store happened to
    //     produce.
    //   - by-session: naïvely merging every run's full mailbox across
    //     an offset=K page is O(runs × (offset + limit)), a 2M-row hot
    //     path at deep offsets (cursor+gemini+copilot flagged). Cap
    //     each run's fetch at `MAX_PER_RUN` so callers that deep-page
    //     into a multi-megabyte session still terminate. When a run
    //     hits the cap — i.e. it has more mailbox messages than we
    //     scanned — we conservatively flip `has_more=true` since we
    //     can no longer compute the accurate total. Operators that
    //     need deep paging through a single session's mailbox should
    //     switch to the by-run endpoint where pagination is O(1).
    let limit = query.limit();
    let offset = query.offset();

    let (raw_records, has_more) = if let Some(run_id) = query.run_id.as_deref() {
        match state
            .runtime
            .mailbox
            .list_by_run(&RunId::new(run_id), limit + 1, offset)
            .await
        {
            Ok(mut records) => {
                let more = records.len() > limit;
                records.truncate(limit);
                // Preserve the historical created_at-ASC ordering the
                // UI's feed view depends on (cursor bugbot review).
                records.sort_by_key(|record| record.created_at);
                (records, more)
            }
            Err(err) => return runtime_error_response(err),
        }
    } else if let Some(session_id) = query.session_id.as_deref() {
        // Bound the cross-run merge: this path is intentionally not
        // optimized for deep paging (operators that need it should
        // move to the by-run endpoint). We still emit a truthful
        // `has_more` by flipping the flag defensively whenever any
        // run's fetch hits the cap — the true per-session mailbox
        // count is unknown without a k-way merge against every
        // mailbox projection.
        const MAX_RUNS_PER_SESSION_SCAN: usize = 500;
        const MAX_MAILBOX_PER_RUN: usize = 1_000;
        let runs = match state
            .runtime
            .runs
            .list_by_session(&SessionId::new(session_id), MAX_RUNS_PER_SESSION_SCAN, 0)
            .await
        {
            Ok(runs) => runs,
            Err(err) => return runtime_error_response(err),
        };
        // If we had more runs than the scan cap, we cannot produce a
        // truthful cross-run total — surface has_more=true so the UI
        // shows a load-more / "deep-paging not supported here" hint.
        let mut cap_exceeded = runs.len() >= MAX_RUNS_PER_SESSION_SCAN;
        let mut records = Vec::new();
        for run in runs {
            // Fetch `MAX_MAILBOX_PER_RUN + 1` so we can tell when a
            // single run's mailbox outgrew the cap (otherwise the
            // cross-run sort silently drops rows).
            match state
                .runtime
                .mailbox
                .list_by_run(&run.run_id, MAX_MAILBOX_PER_RUN + 1, 0)
                .await
            {
                Ok(mut run_records) => {
                    if run_records.len() > MAX_MAILBOX_PER_RUN {
                        cap_exceeded = true;
                        run_records.truncate(MAX_MAILBOX_PER_RUN);
                    }
                    records.append(&mut run_records);
                }
                Err(err) => return runtime_error_response(err),
            }
        }
        records.sort_by_key(|record| record.created_at);
        let total = records.len();
        let slice: Vec<_> = records.into_iter().skip(offset).take(limit).collect();
        let more = cap_exceeded || offset.saturating_add(slice.len()) < total;
        (slice, more)
    } else {
        return bad_request_response("run_id or session_id is required");
    };

    let items: Vec<MailboxMessageView> = raw_records
        .into_iter()
        .filter_map(|record| mailbox_message_view(&state, record))
        .collect();
    (StatusCode::OK, Json(ListResponse { items, has_more })).into_response()
}

pub(crate) async fn append_mailbox_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<AppendMailboxRequest>,
) -> impl IntoResponse {
    let message_id = body
        .message_id
        .clone()
        .unwrap_or_else(|| format!("mailbox_{}", now_ms()));
    match state
        .runtime
        .mailbox
        .append(
            &body.project(),
            message_id.clone().into(),
            body.run_id.clone().map(RunId::new),
            body.task_id.clone().map(TaskId::new),
            body.body.clone().unwrap_or_default(),
            None,
            0,
        )
        .await
    {
        Ok(record) => {
            state
                .mailbox_messages
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(
                    message_id.clone(),
                    AppMailboxMessage {
                        sender_id: body.sender_id,
                        body: body.body,
                        delivered: false,
                    },
                );
            // #456: `mailbox_message_view` looks up the overlay we just
            // inserted above, so the `Some` arm is the happy path. If a
            // tombstone interleaves (concurrent `mark_mailbox_delivered_handler`
            // evicting the entry) the lookup returns `None` — refuse to
            // panic the request path; log + 500 instead.
            match mailbox_message_view(&state, record) {
                Some(item) => (StatusCode::CREATED, Json(item)).into_response(),
                None => {
                    tracing::error!(
                        message_id = %message_id,
                        "mailbox overlay vanished between insert and view",
                    );
                    AppApiError::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal_error",
                        "mailbox message disappeared after insert",
                    )
                    .into_response()
                }
            }
        }
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn mark_mailbox_delivered_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let mut mailbox = state
        .mailbox_messages
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    match mailbox.get_mut(&id) {
        Some(message) => {
            message.delivered = true;
            (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response()
        }
        None => AppApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "mailbox message not found",
        )
        .into_response(),
    }
}

/// `GET /v1/events` — recent events without SSE.
///
/// No SSE connection needed — suitable for initial page load.
/// Returns at most `limit` events (default 50, capped at 500).
pub(crate) async fn recent_events_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<PaginationQuery>,
) -> impl IntoResponse {
    let limit: usize = query.limit().min(500);
    let events = match state.runtime.store.read_stream(None, limit).await {
        Ok(v) => v,
        Err(e) => return store_error_response(e),
    };

    let items: Vec<serde_json::Value> = events
        .iter()
        .rev()
        .take(limit)
        .map(|ev| {
            serde_json::json!({
                "position":   ev.position.0,
                "event_type": event_type_name(&ev.envelope.payload),
                "message":    event_message(&ev.envelope.payload),
                "run_id":     run_id_for_event(&ev.envelope.payload),
                "stored_at":  ev.stored_at,
            })
        })
        .collect();

    let count = items.len();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "items": items,
            "count": count,
            "limit": limit,
        })),
    )
        .into_response()
}

/// `GET /v1/stats` — lightweight aggregate counts for the deployment.
pub(crate) async fn stats_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let store = state.runtime.store.as_ref();

    let total_events: u64 = store
        .head_position()
        .await
        .ok()
        .flatten()
        .map(|p| p.0 + 1)
        .unwrap_or(0);

    let active_runs: u64 = store.count_active_runs().await;
    let active_tasks: u64 = store.count_active_tasks().await;

    use cairn_store::projections::SessionReadModel;
    let total_sessions: u64 = SessionReadModel::list_active(store, usize::MAX)
        .await
        .unwrap_or_default()
        .len() as u64;

    let total_runs: u64 = if total_events > 0 {
        match store.read_stream(None, usize::MAX).await {
            Ok(events) => events
                .iter()
                .filter(|e| {
                    matches!(
                        e.envelope.payload,
                        cairn_domain::RuntimeEvent::RunCreated(_)
                    )
                })
                .count() as u64,
            Err(_) => 0,
        }
    } else {
        0
    };

    let pending_approvals: u64 = {
        let dummy = cairn_domain::ProjectKey::new("", "", "");
        use cairn_store::projections::ApprovalReadModel;
        ApprovalReadModel::list_pending(store, &dummy, usize::MAX, 0)
            .await
            .unwrap_or_default()
            .len() as u64
    };

    let uptime_seconds = state.started_at.elapsed().as_secs();

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "total_events":      total_events,
            "total_sessions":    total_sessions,
            "total_runs":        total_runs,
            "total_tasks":       active_tasks,
            "active_runs":       active_runs,
            "pending_approvals": pending_approvals,
            "uptime_seconds":    uptime_seconds,
        })),
    )
        .into_response()
}
