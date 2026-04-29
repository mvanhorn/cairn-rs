//! Operator intervention endpoints.
//!
//! Covers:
//! - `GET /v1/runs/:id/interventions` — paginated per-run intervention log
//! - `POST /v1/runs/:id/intervene` — force-complete / fail / restart /
//!   inject-message on a run (operator-triggered state transitions)

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};

use cairn_api::http::ListResponse;
use cairn_domain::{
    MailboxMessageId, ResumeTrigger, RunId, RunState, RunStateChanged, RuntimeEvent,
    StateTransition,
};
use cairn_runtime::MailboxService;
use cairn_store::projections::{OperatorInterventionReadModel, RunRecord};
use cairn_store::EventLog;
use uuid::Uuid;

use crate::append_run_intervention_event;
use crate::current_event_head;
use crate::errors::{
    now_ms, operator_event_envelope, run_not_found_response, runtime_error_response,
    store_error_response, validation_error_response, AppApiError,
};
use crate::extractors::TenantScope;
use crate::helpers::load_run_visible_to_tenant;
use crate::publish_runtime_frames_since;
use crate::state::AppState;
use crate::PaginationQuery;

use cairn_runtime::NotificationService;

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RunInterventionResponse {
    pub(crate) ok: bool,
    pub(crate) run: Option<RunRecord>,
    pub(crate) message_id: Option<String>,
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
