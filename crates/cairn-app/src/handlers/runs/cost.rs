//! Cost + SLA handlers for a run.
//!
//! Covers:
//! - `GET /v1/runs/:id/cost` — per-run cost record (zero-valued when empty)
//! - `POST /v1/runs/:id/cost-alert` — set a per-run cost alert threshold
//! - `GET /v1/runs/cost-alerts` — list tenant-wide triggered cost alerts
//! - `POST /v1/runs/:id/sla` — configure per-run SLA
//! - `GET /v1/runs/:id/sla` — query SLA status for a run
//! - `GET /v1/runs/sla-breached` — list tenant-wide SLA breaches
//! - `GET /v1/tenants/:id/costs` — aggregate session-cost rows for a tenant

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};

use cairn_api::http::ListResponse;
use cairn_domain::RunId;
use cairn_runtime::{RunCostAlertService, RunSlaService, RuntimeError};
use cairn_store::projections::{RunCostReadModel, SessionCostReadModel};

use crate::errors::{
    run_not_found_response, runtime_error_response, store_error_response, AppApiError,
};
use crate::extractors::{TenantCostQuery, TenantScope};
use crate::handlers::runs::helpers;
use crate::helpers::load_run_visible_to_tenant;
use crate::state::AppState;
use crate::PaginationQuery;

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

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct SetRunSlaRequest {
    /// T6a-C2: tenant_id is accepted in the body for schema compat but
    /// ignored — the handler uses the resolved run's tenant_id instead.
    #[serde(default, rename = "tenant_id")]
    pub(crate) _tenant_id_deprecated: Option<String>,
    pub(crate) target_completion_ms: u64,
    #[serde(default = "helpers::default_alert_pct")]
    pub(crate) alert_at_percent: u8,
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
