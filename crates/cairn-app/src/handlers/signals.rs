//! Signal ingest and subscription handlers.
//!
//! Extracted from `lib.rs` — contains signal ingestion, listing,
//! subscription CRUD, and signal-to-trigger routing.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};

use cairn_api::http::ListResponse;
use cairn_domain::{ProjectKey, RunId, SignalId};
use cairn_runtime::{SignalRouterService, SignalService};
use cairn_store::EventLog;

use crate::errors::{now_ms, runtime_error_response, AppApiError};
use crate::extractors::OptionalProjectScopedQuery;
use crate::helpers::feed_item_from_signal;
use crate::state::{AppMailboxMessage, AppState};
use crate::triggers::{
    materialize_triggered_run, runtime_event_for_trigger_service_event,
    trigger_decision_outcomes_for_signal, unavailable_trigger_decision, PendingTriggeredRun,
};

// ── DTOs ────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct IngestSignalRequest {
    pub tenant_id: String,
    pub workspace_id: String,
    pub project_id: String,
    pub signal_id: String,
    pub source: String,
    pub payload: serde_json::Value,
    pub timestamp_ms: Option<u64>,
}

impl IngestSignalRequest {
    pub(crate) fn project(&self) -> ProjectKey {
        ProjectKey::new(
            self.tenant_id.as_str(),
            self.workspace_id.as_str(),
            self.project_id.as_str(),
        )
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct CreateSignalSubscriptionRequest {
    pub tenant_id: String,
    pub workspace_id: String,
    pub project_id: String,
    pub signal_kind: String,
    pub target_run_id: Option<String>,
    pub target_mailbox_id: Option<String>,
    pub filter_expression: Option<String>,
}

impl CreateSignalSubscriptionRequest {
    pub(crate) fn project(&self) -> ProjectKey {
        ProjectKey::new(
            self.tenant_id.as_str(),
            self.workspace_id.as_str(),
            self.project_id.as_str(),
        )
    }
}

// ── Handlers ────────────────────────────────────────────────────────────────

pub(crate) async fn ingest_signal_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<IngestSignalRequest>,
) -> impl IntoResponse {
    let project = body.project();
    let timestamp_ms = body.timestamp_ms.unwrap_or_else(now_ms);
    let before = crate::handlers::sse::current_event_head(&state).await;
    match state
        .runtime
        .signals
        .ingest(
            &project,
            SignalId::new(body.signal_id.clone()),
            body.source.clone(),
            body.payload.clone(),
            timestamp_ms,
        )
        .await
    {
        Ok(record) => {
            state.feed.push_item(feed_item_from_signal(&record));
            // Route signal to subscribers
            if let Ok(routed) = state.runtime.signal_router.route_signal(&record.id).await {
                if !routed.mailbox_message_ids.is_empty() {
                    let mut mailbox_messages = state
                        .mailbox_messages
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    for message_id in routed.mailbox_message_ids {
                        mailbox_messages.insert(
                            message_id.to_string(),
                            AppMailboxMessage {
                                sender_id: Some(format!("signal:{}", record.source)),
                                body: Some(record.payload.to_string()),
                                delivered: true,
                            },
                        );
                    }
                }
            }

            // RFC 022: evaluate triggers for this signal.
            //
            // RFC-025 Phase 1.5a: the trigger service is projection-backed
            // and async. The two-stage flow (preview candidates → consult
            // decision layer → evaluate against prepared set) is preserved
            // so the window between candidate enumeration and decision
            // submission still honours the "no newly-enabled trigger can
            // slip past without a matching decision" invariant. All
            // durable TriggerSkipped/Fired/Denied/RateLimited/
            // PendingApproval events are persisted below through a single
            // event-log append (not inside the trigger service) so the
            // telemetry + SSE plumbing that depends on the append ordering
            // keeps its current shape.
            let decision_candidates = match state
                .triggers
                .decision_candidates_for_signal(
                    &project,
                    &record.id,
                    &record.source,
                    "", // plugin_id — empty for direct API signals
                    &record.payload,
                    None, // source_run_chain_depth
                )
                .await
            {
                Ok(ids) => ids,
                Err(err) => {
                    tracing::warn!(
                        project = ?project,
                        error = %err,
                        "trigger candidate preview failed"
                    );
                    Vec::new()
                }
            };
            let trigger_decision_outcomes: HashMap<
                cairn_domain::TriggerId,
                cairn_runtime::services::trigger_service::TriggerDecisionOutcome,
            > = trigger_decision_outcomes_for_signal(
                state.as_ref(),
                &project,
                &record.id,
                &record.source,
                decision_candidates,
            )
            .await;
            let prepared_trigger_ids: HashSet<_> =
                trigger_decision_outcomes.keys().cloned().collect();
            let decision_fn = {
                let trigger_decision_outcomes = trigger_decision_outcomes.clone();
                let project = project.clone();
                let record_id = record.id.clone();
                move |trigger_id: &cairn_domain::TriggerId, signal_type: &str| {
                    trigger_decision_outcomes.get(trigger_id).cloned().unwrap_or_else(|| {
                        tracing::warn!(
                            project = ?project,
                            trigger_id = %trigger_id,
                            signal_id = %record_id,
                            signal_type,
                            "trigger evaluation reached decision phase without a prepared decision"
                        );
                        unavailable_trigger_decision(
                            trigger_id,
                            format!(
                                "decision_unavailable_for_trigger_fire:{signal_type}"
                            ),
                        )
                    })
                }
            };
            let trigger_events = match state
                .triggers
                .evaluate_signal_for_candidates(
                    &project,
                    &record.id,
                    &record.source,
                    "", // plugin_id — empty for direct API signals
                    &record.payload,
                    None, // source_run_chain_depth
                    &prepared_trigger_ids,
                    &decision_fn,
                )
                .await
            {
                Ok(events) => events,
                Err(err) => {
                    tracing::warn!(
                        project = ?project,
                        error = %err,
                        "trigger evaluation failed"
                    );
                    Vec::new()
                }
            };

            let persisted_trigger_events: Vec<cairn_domain::RuntimeEvent> = trigger_events
                .iter()
                .filter_map(|event| runtime_event_for_trigger_service_event(&project, event))
                .collect();
            crate::telemetry_routes::record_trigger_fire_usage(
                state.runtime.store.as_ref(),
                &project,
                &trigger_events,
            );

            // Collect the pending-run materialisation set. Rather than
            // issuing two projection reads per fired trigger (PR #569
            // review: N+1 avoidance), we fetch all triggers + templates
            // for the project once and build two HashMaps; lookups per
            // fire event are then O(1). The matching set is bounded by
            // the number of triggers in the project — typically single
            // digits, occasionally tens; the two list reads are cheaper
            // than 2*N individual reads when N > 1 and indistinguishable
            // from them when N == 0 or 1.
            let mut pending_runs: Vec<PendingTriggeredRun> = Vec::new();
            let fire_count = trigger_events
                .iter()
                .filter(|e| {
                    matches!(
                        e,
                        cairn_runtime::services::trigger_service::TriggerEvent::TriggerFired { .. }
                    )
                })
                .count();
            if fire_count > 0 {
                let triggers_in_project = state
                    .triggers
                    .list_triggers_for_project(&project)
                    .await
                    .unwrap_or_default();
                let templates_in_project = state
                    .triggers
                    .list_templates_for_project(&project)
                    .await
                    .unwrap_or_default();
                let triggers_by_id: HashMap<_, _> = triggers_in_project
                    .into_iter()
                    .map(|t| (t.id.clone(), t))
                    .collect();
                let templates_by_id: HashMap<_, _> = templates_in_project
                    .into_iter()
                    .map(|t| (t.id.clone(), t))
                    .collect();
                for event in &trigger_events {
                    if let cairn_runtime::services::trigger_service::TriggerEvent::TriggerFired {
                        trigger_id,
                        run_id,
                        ..
                    } = event
                    {
                        tracing::info!(
                            trigger_id = %trigger_id,
                            run_id = %run_id,
                            "trigger fired — run created from signal"
                        );
                        let Some(trigger) = triggers_by_id.get(trigger_id) else {
                            tracing::warn!(
                                trigger_id = %trigger_id,
                                "trigger fired but trigger definition was unavailable during materialization"
                            );
                            continue;
                        };
                        let Some(template) = templates_by_id.get(&trigger.run_template_id) else {
                            tracing::warn!(
                                trigger_id = %trigger_id,
                                run_template_id = %trigger.run_template_id,
                                "trigger fired but run template was unavailable during materialization"
                            );
                            continue;
                        };
                        pending_runs.push(PendingTriggeredRun {
                            trigger_id: trigger_id.clone(),
                            run_id: run_id.clone(),
                            template: template.clone(),
                        });
                    }
                }
            }

            if !persisted_trigger_events.is_empty() {
                let envelopes: Vec<_> = persisted_trigger_events
                    .into_iter()
                    .map(cairn_runtime::make_envelope)
                    .collect();
                if let Err(error) = state.runtime.store.append(&envelopes).await {
                    tracing::warn!(
                        project = ?project,
                        error = %error,
                        "failed to persist trigger events during signal ingest"
                    );
                }
            }

            for pending_run in pending_runs {
                if let Err(err) =
                    materialize_triggered_run(state.as_ref(), &project, pending_run).await
                {
                    tracing::warn!(
                        project = ?project,
                        error = %err,
                        "failed to materialize trigger-fired run"
                    );
                }
            }

            crate::handlers::sse::publish_runtime_frames_since(&state, before).await;
            (StatusCode::CREATED, Json(record)).into_response()
        }
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn list_signals_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<OptionalProjectScopedQuery>,
) -> impl IntoResponse {
    // #422: honest pagination — fetch `limit + 1`, derive `has_more`.
    let limit = query.limit();
    match state
        .runtime
        .signals
        .list_by_project(&query.project(), limit + 1, query.offset())
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

pub(crate) async fn create_signal_subscription_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateSignalSubscriptionRequest>,
) -> impl IntoResponse {
    match state
        .runtime
        .signal_router
        .subscribe(
            body.project(),
            body.signal_kind,
            body.target_run_id.map(RunId::new),
            body.target_mailbox_id,
            body.filter_expression,
        )
        .await
    {
        Ok(subscription) => (StatusCode::CREATED, Json(subscription)).into_response(),
        Err(err) => runtime_error_response(err),
    }
}

pub(crate) async fn list_signal_subscriptions_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<OptionalProjectScopedQuery>,
) -> impl IntoResponse {
    // #422: honest pagination — fetch `limit + 1`, derive `has_more`.
    let limit = query.limit();
    match state
        .runtime
        .signal_router
        .list_by_project(&query.project(), limit + 1, query.offset())
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

pub(crate) async fn delete_signal_subscription_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.runtime.store.delete_signal_subscription(&id).await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response(),
        Err(err) => {
            tracing::error!("delete_signal_subscription failed: {err}");
            AppApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                err.to_string(),
            )
            .into_response()
        }
    }
}
