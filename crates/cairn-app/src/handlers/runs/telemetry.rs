//! Run-diagnosis and telemetry endpoints.
//!
//! Covers:
//! - `GET /v1/runs/stalled` — list runs over the stuck-threshold
//! - `GET /v1/runs/:id/telemetry` — aggregated provider-calls + tool-invocations
//! - `GET /v1/runs/escalated` — tenant-wide recovery escalations
//! - `POST /v1/runs/:id/diagnose` — single-run diagnosis
//! - `GET /v1/runs/:id/audit` — per-run event + audit log timeline

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};

use cairn_api::http::ListResponse;
use cairn_domain::{RunId, RunState, RuntimeEvent};
use cairn_store::projections::{AuditLogReadModel, RecoveryEscalationReadModel, RunReadModel};
use cairn_store::{EntityRef, EventLog};

use crate::errors::{now_ms, run_not_found_response, store_error_response};
use crate::event_message;
use crate::extractors::TenantScope;
use crate::handlers::runs::helpers::{redact_provider_error, resolve_stuck_run_threshold_ms};
use crate::helpers::{build_diagnosis_report, load_run_visible_to_tenant};
use crate::state::AppState;
use crate::PaginationQuery;

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

    // Response to Copilot review (PR #567): `slice::sort_by_key` is
    // documented stable (std guarantees `O(n log n)` stable sort), so
    // equal-timestamp rows preserve insertion order. RunCreated
    // synthesises a second entry with the exact same `stored_at`; we
    // push RunCreated then Pending, so stable sort keeps that order
    // in the rendered timeline.
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
