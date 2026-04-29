//! Run event-log handlers + DTOs.
//!
//! Covers `GET /v1/runs/:id/events` (cursor-paginated stream of the
//! durable event log for a single run).

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};

use cairn_domain::RunId;
use cairn_store::{EntityRef, EventLog, EventPosition, StoredEvent};

use crate::errors::{run_not_found_response, store_error_response};
use crate::extractors::TenantScope;
use crate::helpers::load_run_visible_to_tenant;
use crate::state::AppState;
use crate::{event_message, event_type_name};

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
