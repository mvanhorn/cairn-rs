//! Checkpoint / replay handlers + DTOs.
//!
//! Covers:
//! - `GET /v1/runs/:id/replay` — replay the run's event window
//! - `GET /v1/runs/:id/replay-to-checkpoint` — replay up to a saved checkpoint
//! - `POST /v1/runs/:id/checkpoint` — alias for `save_checkpoint_handler`

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};

use cairn_domain::{CheckpointId, RunId};
use cairn_store::projections::CheckpointReadModel;

use crate::errors::{
    run_not_found_response, store_error_response, validation_error_response, AppApiError,
};
use crate::extractors::TenantScope;
use crate::helpers::{
    build_run_replay_result, checkpoint_recorded_position, load_run_visible_to_tenant,
};
use crate::state::AppState;

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct RunReplayQuery {
    pub(crate) from_position: Option<u64>,
    pub(crate) to_position: Option<u64>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct ReplayToCheckpointQuery {
    pub(crate) checkpoint_id: String,
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
