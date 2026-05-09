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

    // #570: storage-layer candidate fetch. `RunReadModel::list_stalled`
    // composes state (Running ∪ Pending) + staleness + tenant at the
    // projection surface, so the handler stops fetching 20 000 rows
    // (10k Running + 10k Pending) and filtering in memory.
    //
    // Admin operators with no tenant-scope still need cross-tenant
    // visibility; their bearer identity never resolves to a concrete
    // tenant_id via `tenant_scope.tenant_id()`, so when `is_admin`
    // the handler falls back to the old dual-state scan.
    //
    // `build_diagnosis_report` refines the stalled predicate with
    // per-run task activity (a run whose tasks are heartbeating is
    // NOT stalled even if its own RunRecord.updated_at is cold —
    // Copilot review on #589). The post-fetch filter can therefore
    // discard projection rows. To keep `has_more` honest we fetch a
    // larger candidate window than `limit + 1` and paginate the
    // diagnosed-stalled set in memory. CANDIDATE_SCAN_CAP bounds the
    // per-request scan so a pathological tenant can't stall the
    // dashboard on a 100k-row fan-out.
    const CANDIDATE_SCAN_CAP: usize = 10_000;
    let limit = query.limit();
    let offset = query.offset();
    let now = now_ms();

    let candidate_runs = if tenant_scope.is_admin {
        // Admin: cross-tenant. `list_stalled` is tenant-scoped by
        // design (per-tenant indexes are correctness + performance),
        // so admin goes through the dual-state walk.
        let mut all = Vec::new();
        for target_state in [RunState::Running, RunState::Pending] {
            match RunReadModel::list_by_state(
                state.runtime.store.as_ref(),
                target_state,
                CANDIDATE_SCAN_CAP,
            )
            .await
            {
                Ok(runs) => all.extend(runs),
                Err(err) => return store_error_response(err),
            }
        }
        all.into_iter()
            .filter(|r| now.saturating_sub(r.updated_at) > stale_after_ms)
            .collect::<Vec<_>>()
    } else {
        match RunReadModel::list_stalled(
            state.runtime.store.as_ref(),
            tenant_scope.tenant_id(),
            now,
            stale_after_ms,
            CANDIDATE_SCAN_CAP,
            0,
        )
        .await
        {
            Ok(runs) => runs,
            Err(err) => return store_error_response(err),
        }
    };

    // Per-run diagnosis (tasks + events) still assembled in memory —
    // that cost is per-result, not per-candidate. Apply the refined
    // stalled predicate BEFORE pagination so `has_more` reflects the
    // true diagnosed-stalled total.
    let mut reports = Vec::with_capacity(candidate_runs.len());
    for run in candidate_runs {
        match build_diagnosis_report(state.as_ref(), &run, stale_after_ms).await {
            Ok((report, true)) => reports.push(report),
            Ok((_report, false)) => {}
            Err(err) => return store_error_response(err),
        }
    }

    let total = reports.len();
    let items: Vec<_> = reports.into_iter().skip(offset).take(limit).collect();
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

    // Issue #689 R2-B: live-aggregated "prose-playing" signal.
    // Walks the ordered tool_invocations list for this run and looks
    // for >= 2 consecutive `bash`-target invocations whose `command`
    // JSON field matches the bare-`echo` heuristic. This derives
    // from persisted state rather than the orchestrator loop's
    // in-memory counter so it survives cairn-app restarts and
    // matches whatever the operator actually sees in the event log.
    let prose_playing_detected = detect_prose_playing_in_invocations(&tool_invocations);

    let body = serde_json::json!({
        "run_id": run.run_id.to_string(),
        "state": run.state,
        "stuck": stuck,
        "stuck_since_ms": stuck_since_ms,
        "prose_playing_detected": prose_playing_detected,
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

/// Issue #689 R2-B: scan an ordered list of tool invocations for the
/// "echo-via-bash prose-playing" pattern.
///
/// Returns `true` when at least `ECHO_BASH_DETECTION_THRESHOLD`
/// consecutive bash invocations match the bare-echo heuristic. The
/// invocations list is expected in store order (most recently
/// persisted first OR oldest first — either works, since the
/// detector only looks at consecutive runs).
///
/// Implementation reuses the pure classifier from
/// `cairn_orchestrator::echo_detector` so there's exactly one
/// source of truth for the heuristic. If the orchestrator
/// classifier evolves (e.g. adds shell-prefix forms), telemetry
/// picks up the improvement automatically.
fn detect_prose_playing_in_invocations(
    invocations: &[cairn_domain::tool_invocation::ToolInvocationRecord],
) -> bool {
    use cairn_domain::tool_invocation::ToolInvocationTarget;
    let mut consecutive = 0u32;
    for inv in invocations {
        let is_bash = matches!(
            &inv.target,
            ToolInvocationTarget::Builtin { tool_name } if tool_name.as_str() == "bash"
        );
        if !is_bash {
            consecutive = 0;
            continue;
        }
        let command_is_bare_echo = inv
            .args_json
            .as_ref()
            .and_then(|v| v.get("command"))
            .and_then(|v| v.as_str())
            .map(is_bare_echo_command_for_telemetry)
            .unwrap_or(false);
        if command_is_bare_echo {
            consecutive = consecutive.saturating_add(1);
            if consecutive >= cairn_orchestrator::ECHO_BASH_DETECTION_THRESHOLD {
                return true;
            }
        } else {
            consecutive = 0;
        }
    }
    false
}

/// Thin wrapper around the orchestrator's pure classifier to keep
/// the import site shallow. The check is identical to what the
/// loop runner's `EchoDetectorState` applies live, so telemetry and
/// the in-process detector agree on what counts as prose-play.
fn is_bare_echo_command_for_telemetry(cmd: &str) -> bool {
    // Build a minimal ActionProposal to reuse the orchestrator's
    // canonical classifier. This is a few bytes of transient allocation
    // per bash invocation scanned — negligible vs the 1000-row query
    // bound already in place for `list_by_run`.
    use cairn_domain::ActionProposal;
    let proposal = ActionProposal::invoke_tool(
        "bash",
        serde_json::json!({ "command": cmd }),
        "telemetry-classify",
        0.0,
        false,
    );
    cairn_orchestrator::is_echo_bash_proposal(&proposal)
}

pub(crate) async fn list_escalated_runs_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Query(query): Query<PaginationQuery>,
) -> impl IntoResponse {
    // #570: storage-layer pagination. `RecoveryEscalationReadModel::
    // list_by_tenant` now accepts `limit + 1` and `offset`; even when
    // the InMemory impl returns a stub empty list today, the wire
    // contract is future-proofed for the pg/sqlite implementations
    // that will ingest RecoveryEscalation events.
    let limit = query.limit();
    let offset = query.offset();
    match RecoveryEscalationReadModel::list_by_tenant(
        state.runtime.store.as_ref(),
        tenant_scope.tenant_id(),
        limit.saturating_add(1),
        offset,
    )
    .await
    {
        Ok(mut items) => {
            let has_more = items.len() > limit;
            items.truncate(limit);
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

/// #789: per-run reasoning trajectory for post-mortem replay.
///
/// `GET /v1/runs/:id/trajectory` returns the run's compacted
/// per-iteration reasoning steps in chronological order. Each step
/// carries the model's chain-of-thought, the top-1 proposed action,
/// the user-message delta vs the prior iteration, and the calibrated
/// confidence — enough to read the run like a story.
///
/// On `--db memory` the projection serves up to
/// `REASONING_STEP_CAP_PER_RUN` steps per run (FIFO eviction past
/// that). pg/sqlite parity is a follow-up — those backends currently
/// return an empty trajectory.
pub(crate) async fn get_run_trajectory_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(id): Path<String>,
    Query(pagination): Query<PaginationQuery>,
) -> impl IntoResponse {
    let run_id = RunId::new(id);
    // Tenant gate before exposing any trajectory data.
    let _run = match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(r)) => r,
        Ok(None) => return run_not_found_response(),
        Err(response) => return response,
    };

    let limit = pagination.limit.unwrap_or(200).min(200);
    let offset = pagination.offset.unwrap_or(0);
    use cairn_store::projections::reasoning_step::ReasoningStepReadModel;
    let total =
        match ReasoningStepReadModel::count_for_run(state.runtime.store.as_ref(), &run_id).await {
            Ok(n) => n,
            Err(err) => return store_error_response(err),
        };
    match ReasoningStepReadModel::list_by_run(state.runtime.store.as_ref(), &run_id, limit, offset)
        .await
    {
        Ok(items) => {
            // `total` is the full count for the run (Gemini PR #794
            // review): clients can compute `has_more` as
            // `offset + items.len() < total`. `items.len()` is the
            // returned-page size — kept on the response for
            // convenience.
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "run_id": run_id.as_str(),
                    "items": items,
                    "count": items.len(),
                    "total": total,
                    "limit": limit,
                    "offset": offset,
                })),
            )
                .into_response()
        }
        Err(err) => store_error_response(err),
    }
}

/// #789: live fleet view — every active agent + its current action.
///
/// `GET /v1/admin/agents/live` returns a snapshot of every run in a
/// non-terminal state for the caller's tenant, joined with the most
/// recent reasoning step (current action + reasoning preview). Used
/// by the operator dashboard to answer "what's in the box right now,
/// and what is each agent thinking?".
///
/// Tenant-scoped: admin gets all tenants, regular operators see only
/// their tenant's runs.
pub(crate) async fn get_live_agents_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
) -> impl IntoResponse {
    use cairn_store::projections::reasoning_step::ReasoningStepReadModel;
    use cairn_store::projections::RunReadModel;
    // Aggregate runs across the non-terminal states. RunReadModel
    // exposes `list_by_state` indexed on the projection's state
    // column; iterate the active set and concat. Tenant filter
    // happens client-side because the trait doesn't expose a
    // `list_active_by_tenant` helper today (see follow-up: add one
    // and replace this loop with a single index-friendly call).
    let active_states = [
        cairn_domain::RunState::Pending,
        cairn_domain::RunState::Running,
        cairn_domain::RunState::WaitingApproval,
        cairn_domain::RunState::WaitingDependency,
        cairn_domain::RunState::Paused,
    ];
    let mut runs: Vec<cairn_store::projections::RunRecord> = Vec::new();
    for run_state in active_states {
        match RunReadModel::list_by_state(state.runtime.store.as_ref(), run_state, 500).await {
            Ok(rs) => runs.extend(rs.into_iter().filter(|r| {
                tenant_scope.is_admin || r.project.tenant_id == *tenant_scope.tenant_id()
            })),
            Err(err) => return store_error_response(err),
        }
    }
    let mut agents: Vec<serde_json::Value> = Vec::new();
    for run in runs {
        // N+1 caveat (Gemini PR #794 review): each iteration takes
        // the in-memory state lock once. With the InMemoryStore
        // that's a single Mutex acquisition per active run —
        // measured microseconds even with 500 active runs. A future
        // bulk-fetch helper on the trait would amortize this (and
        // let pg/sqlite serve from a single round-trip when parity
        // lands); tracked as a follow-up.
        //
        // Errors propagate now (vs the previous `unwrap_or(None)`
        // which silently swallowed projection failures — Gemini
        // same review).
        let latest =
            match ReasoningStepReadModel::latest_for_run(state.runtime.store.as_ref(), &run.run_id)
                .await
            {
                Ok(opt) => opt,
                Err(err) => return store_error_response(err),
            };
        agents.push(serde_json::json!({
            "run_id": run.run_id.as_str(),
            "session_id": run.session_id.as_str(),
            "agent_role_id": run.agent_role_id,
            "state": format!("{:?}", run.state).to_lowercase(),
            "iteration": run.iteration,
            "started_at_ms": run.created_at,
            "updated_at_ms": run.updated_at,
            "current_action": latest.as_ref().map(|s| serde_json::to_value(&s.proposed_action).unwrap_or(serde_json::Value::Null)),
            "current_reasoning_compact": latest.as_ref().map(|s| s.reasoning_compact.clone()),
            "confidence": latest.as_ref().map(|s| s.confidence),
            "last_step_at_ms": latest.as_ref().map(|s| s.recorded_at_ms),
        }));
    }
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "tenant_id": tenant_scope.tenant_id().as_str(),
            "agents": agents,
            "count": agents.len(),
        })),
    )
        .into_response()
}
