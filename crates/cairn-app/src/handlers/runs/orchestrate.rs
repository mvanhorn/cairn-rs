//! Orchestrator entry point + request DTOs.
//!
//! Covers `POST /v1/runs/:id/orchestrate` — the idempotent GATHER →
//! DECIDE → EXECUTE iteration driver — plus the breaker-override DTOs
//! and the supporting helpers:
//!
//! - [`resolve_breaker_overrides`] — merge per-run tighten-only overrides
//!   into the operator-configured default breaker config
//! - [`submit_all_providers_exhausted_proposal`] — best-effort operator
//!   approval card emitted when the routed provider chain runs dry

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};

use cairn_store::projections::ApprovalReadModel;

use crate::errors::{
    api_error_with_details, run_not_found_response, runtime_error_response, AppApiError,
};
use crate::extractors::TenantScope;
use crate::handlers::runs::helpers::{classify_failed_reason, finalize_run_failure};
use crate::helpers::{load_run_visible_to_tenant, working_dir_for_run};
use crate::sandbox::workspace_error_response;
use crate::state::AppState;
use crate::{
    resolve_run_bool_default, resolve_run_mode_default, resolve_run_string_default,
    resolve_run_u32_default,
};

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

// ── F53: terminal-failure state flip helpers ────────────────────────────────

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
    //
    // PR #567 review (Copilot): wrap the extractor in `Result<...,
    // JsonRejection>` so malformed body / wrong content-type / payload
    // too large surface through the canonical `Error` envelope
    // (`status_code`, `code`, `message`, `request_id`) via
    // `json_rejection_response`, matching every other typed handler
    // (plan approve/reject/revise, create_run, etc.).
    body: Result<Json<serde_json::Value>, axum::extract::rejection::JsonRejection>,
) -> axum::response::Response {
    use crate::idempotency::{claim, IdempotencyEndpoint};

    let Json(body_value) = match body {
        Ok(b) => b,
        Err(err) => return crate::errors::json_rejection_response(err),
    };

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
    use cairn_store::EventLog;
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
        // Copilot review (PR #567): mirror the earlier terminal-guard
        // above (#455 DoS surface) — never `unreachable!` on an HTTP
        // handler. A new `is_terminal` variant added without an arm
        // here must surface as a 500, not panic the request task.
        let termination = match refreshed.state {
            cairn_domain::RunState::Completed => "completed",
            cairn_domain::RunState::Failed => "failed",
            cairn_domain::RunState::Canceled => "canceled",
            other => {
                tracing::error!(
                    run_id = %refreshed.run_id,
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
                "run_state": refreshed.state,
            })),
        )
            .into_response();
    }

    // #639: spawn a background lease-keeper for this run so long
    // approval-paced flows can't let the FF lease expire between
    // orchestrate HTTP calls. The keeper calls
    // `RunService::renew_lease_if_stale` every `lease_ttl_ms / 3` and
    // self-exits when it observes a terminal state or a non-transient
    // renew error. Idempotent: `ensure_running` atomically checks
    // under a mutex and no-ops if a keeper is already alive for this
    // run_id, so concurrent orchestrate calls never spawn duplicates.
    //
    // Only runs when the Fabric services aggregate is installed —
    // pure in-memory test harnesses (`AppState.fabric = None`) don't
    // have a lease to renew. The lease TTL comes from FabricConfig
    // (default 180 s; operator-overridable via
    // `CAIRN_FABRIC_LEASE_TTL_MS`).
    if let Some(fabric) = state.fabric.as_ref() {
        // `lease_ttl_ms` is a `FabricRuntimeHandle` trait method; the
        // method is reachable via the `Arc<dyn FabricRuntimeHandle>`
        // field on `FabricServices.runtime` without a `use` import
        // because the trait is already auto-in-scope at the vtable
        // call site.
        let lease_ttl_ms = fabric.runtime.lease_ttl_ms();
        // #655: pass the cairn-store projection handle so the keeper's
        // suspension probe can read `ApprovalReadModel` +
        // `ToolCallApprovalReadModel` on each tick and skip renew
        // FCALLs that FF would 409 with `execution_not_eligible`.
        state
            .lease_keepers
            .ensure_running(
                refreshed.run_id.clone(),
                refreshed.session_id.clone(),
                state.runtime.runs.clone(),
                state.runtime.store.clone(),
                lease_ttl_ms,
            )
            .await;
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
    let default_max_iterations =
        resolve_run_u32_default(state.as_ref(), &run.project, &run.run_id, "max_iterations").await;
    // #660: strict completion gate flag. Absent key → None → default
    // `true` flows in via `LoopConfig::default()` below. Operators who
    // want the legacy "LLM calls it done no matter what" flow flip the
    // key via `PUT /v1/settings/defaults/project/<proj>/run:<id>:orchestrator_strict_completion_gate`
    // with body `{"value": false}`.
    let default_strict_completion_gate = resolve_run_bool_default(
        state.as_ref(),
        &run.project,
        &run.run_id,
        "orchestrator_strict_completion_gate",
    )
    .await;

    // #651: capture presence flags BEFORE the `Option::or` chain moves the
    // fields out of `body`. An operator-supplied `goal` on the first
    // `/orchestrate` POST MUST be persisted into the run's "goal" default
    // so the empty-body auto-resume kick from the F49 worker
    // (`main.rs::auto-resume orchestrate worker firing POST`) recovers
    // the objective on every subsequent iteration. Without this, the LLM
    // sees the fallback "Execute the run objective." string and completes
    // the run with a "no objective" summary (issue #651 root cause).
    let body_has_goal = body.goal.is_some();
    let body_has_max_iterations = body.max_iterations.is_some();
    let goal_value = body
        .goal
        .or(default_goal.clone())
        .unwrap_or_else(|| "Execute the run objective.".to_owned());

    let ctx = OrchestrationContext {
        project: run.project.clone(),
        session_id: run.session_id.clone(),
        run_id: run.run_id.clone(),
        task_id: None,
        iteration: 0,
        goal: goal_value.clone(),
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

    // #651: persist the resolved `goal` into the run's per-run defaults
    // projection so the empty-body auto-resume POST from the F49 worker
    // recovers the objective on every follow-up iteration. Two cases
    // cover every call path:
    //
    //   1. Body supplied `goal` (explicit operator request): persist —
    //      refreshes the default so the latest operator intent wins.
    //   2. Body omitted `goal` AND no default is stored yet (legacy run
    //      that pre-dates the persistence patch in lifecycle.rs):
    //      back-fill from the fallback string. This is cheap and keeps
    //      the invariant "once orchestrate has resolved a goal, the
    //      default holds it".
    //
    // Best-effort: a defaults-projection write failure logs at WARN and
    // the in-flight iteration still uses `ctx.goal`. The next auto-resume
    // kick would fall through to the fallback — but that is strictly no
    // worse than the pre-fix behaviour and is surfaced in logs.
    if body_has_goal || default_goal.is_none() {
        if let Err(err) = crate::persist_run_string_default(
            state.as_ref(),
            &run.project,
            &run.run_id,
            "goal",
            &goal_value,
        )
        .await
        {
            tracing::warn!(
                run_id = %run.run_id,
                error = %err,
                "#651: failed to persist run goal default; auto-resume may lose objective"
            );
        }
    }

    // #651: same shape for `max_iterations`. An operator who specifies a
    // 50-iteration cap on the first POST expects that cap to hold across
    // every auto-resume; without persistence the empty-body kick falls
    // back to `LoopConfig::default().max_iterations` (20) which can
    // terminate long multi-tool runs early.
    //
    // Three cases:
    //   1. Body carries a value → persist it (fresh operator intent).
    //   2. Body omits, no default stored yet → back-fill with
    //      `cairn_orchestrator::LoopConfig::default().max_iterations`
    //      so a follow-up auto-resume reads a concrete value instead of
    //      falling through to the same default via the None path. This
    //      keeps the defaults projection authoritative once orchestrate
    //      has touched a run.
    //   3. Body omits, default already stored → nothing to do.
    let persist_iter: Option<u32> = if body_has_max_iterations {
        body.max_iterations
    } else if default_max_iterations.is_none() {
        Some(cairn_orchestrator::LoopConfig::default().max_iterations)
    } else {
        None
    };
    if let Some(v) = persist_iter {
        if let Err(err) = crate::persist_run_u32_default(
            state.as_ref(),
            &run.project,
            &run.run_id,
            "max_iterations",
            v,
        )
        .await
        {
            tracing::warn!(
                run_id = %run.run_id,
                error = %err,
                "#651: failed to persist run max_iterations default; auto-resume may fall back to LoopConfig default"
            );
        }
    }

    // #660: persist the strict completion gate flag once per run so every
    // auto-resume kick reads a stable value instead of racing the
    // (currently unset → default true) None path. Mirrors the shape used
    // for `max_iterations` above — back-fill on first contact, leave
    // alone after. A body-level override is NOT part of this PR: the
    // per-run flip channel is the defaults-projection PUT documented in
    // the LoopConfig rustdoc + the OpenAPI note on the orchestrate
    // handler. Keeping the request body out of it limits the surface.
    //
    // Resolve the concrete value we want to persist here (before `cfg`
    // is built below) so we do not have to cross-reference `cfg` from
    // above it.
    if default_strict_completion_gate.is_none() {
        let backfill_value =
            cairn_orchestrator::LoopConfig::default().orchestrator_strict_completion_gate;
        if let Err(err) = crate::persist_run_bool_default(
            state.as_ref(),
            &run.project,
            &run.run_id,
            "orchestrator_strict_completion_gate",
            backfill_value,
        )
        .await
        {
            tracing::warn!(
                run_id = %run.run_id,
                error = %err,
                "#660: failed to persist run orchestrator_strict_completion_gate default; \
                 auto-resume iterations will fall back to LoopConfig default"
            );
        }
    }

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

    // Bedrock IDs follow `<vendor>.<model>[-<version>][:<suffix>]` — the
    // leading segment before the first `.` is a known AWS Bedrock vendor.
    // Any other `.`-containing id (e.g. `glm-4.7`, `gpt-4.1`, `llama-3.2`)
    // is a version number on a non-Bedrock model and must NOT route
    // through the Bedrock provider. Previous heuristic
    // (`contains('.') && !contains('/')`) mis-routed every dotted
    // version string, blocking every OpenAI-compat brain path.
    const BEDROCK_VENDORS: &[&str] = &[
        "anthropic",
        "meta",
        "amazon",
        "minimax",
        "cohere",
        "ai21",
        "stability",
        "mistral",
        // AWS cross-region inference prefixes (e.g. `us.anthropic.claude-...`).
        "us",
        "eu",
        "apac",
    ];
    let is_bedrock_model = model_id
        .split('.')
        .next()
        .map(|vendor| BEDROCK_VENDORS.contains(&vendor))
        .unwrap_or(false);
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
        Err(cairn_runtime::error::RuntimeError::CredentialMissing { connection_id }) => {
            // #353 + Gemini review on #587: the default
            // `runtime_error_response` path renders `CredentialMissing`
            // with the generic `:tenant` placeholder (the variant does
            // not carry the tenant_id). Here we know it — interpolate
            // the concrete tenant into the remediation URL so
            // operators can copy-paste without substitution.
            let tenant = run.project.tenant_id.as_str();
            return AppApiError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "provider_credential_missing",
                format!(
                    "provider connection {connection_id} has no credential bound. \
                     (1) Store an API key via POST /v1/admin/tenants/{tenant}/credentials, \
                     then (2) link it by re-sending the full connection update to \
                     PUT /v1/providers/connections/{connection_id} — include \
                     provider_family, adapter_type, supported_models (required by \
                     UpdateProviderConnectionRequest) alongside credential_id=<credential id>.",
                ),
            )
            .into_response();
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
    // Adapts the shared runtime SSE broadcast channel to `NotificationSink`
    // so `NotifyOperatorTool` can push realtime frames. Extracted struct +
    // impl live in `super::orchestrate_emitter::SseSink`.
    let sse_sink: std::sync::Arc<dyn NotificationSink> =
        std::sync::Arc::new(super::orchestrate_emitter::SseSink {
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
    // #353: lifted out of the routed-service closure so we can fail-fast
    // with 422 `provider_credential_missing` if every registered
    // connection for this tenant was rejected at build time for the
    // same reason. See the block further down.
    let mut credential_missing_connections: Vec<String> = Vec::new();
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

        // #353: `credential_missing_connections` lives in the outer
        // scope so the post-closure check can surface a 422 when no
        // binding builds successfully AND at least one failed for
        // `CredentialMissing`. Keeping the collection here (closure
        // side) captures errors from `resolve_generation_for_connection`
        // without propagating them through `RoutedGenerationService`.
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
                Err(cairn_runtime::error::RuntimeError::CredentialMissing { .. }) => {
                    credential_missing_connections.push(conn_id.clone());
                    continue;
                }
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
        //
        // #353: we deliberately do NOT fall through to the startup
        // fallback when at least one tenant-registered connection
        // failed for `CredentialMissing` — the operator explicitly
        // registered a connection; they want it to work. Surfacing
        // startup-env fallback here would hide the configuration
        // error. The post-closure branch below returns 422 in that case.
        if bindings.is_empty() && credential_missing_connections.is_empty() {
            let cooldown = scoped_cooldowns.get_or_create(&tenant_key, "startup");
            bindings.push(cairn_runtime::RoutedBinding {
                binding_id: "startup".to_owned(),
                provider: brain.clone(),
                chain: cairn_runtime::ModelChain::single(model_id.clone()).with_cooldown(cooldown),
            });
        }

        cairn_runtime::RoutedGenerationService::new(bindings)
    };

    // #353: every tenant-registered connection rejected the build with
    // `CredentialMissing`. Return the dedicated 422 with the list of
    // affected connections so the operator knows exactly which rows
    // need a credential linked. Keeps the SDK retry-on-5xx loop from
    // pinning on a configuration bug that only a human can fix.
    if routed.is_empty() && !credential_missing_connections.is_empty() {
        let conns = credential_missing_connections.join(", ");
        let tenant = run.project.tenant_id.as_str();
        return AppApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "provider_credential_missing",
            format!(
                "No provider connection for this tenant has a credential bound. \
                 Affected connections: [{conns}]. \
                 (1) Store an API key via POST /v1/admin/tenants/{tenant}/credentials, \
                 then (2) link it by re-sending the full connection update to \
                 PUT /v1/providers/connections/<id> — include provider_family, \
                 adapter_type, supported_models (required by \
                 UpdateProviderConnectionRequest) alongside credential_id=<credential id>.",
            ),
        )
        .into_response();
    }

    let decide = LlmDecidePhase::from_routed(routed).with_tools(registry.clone());

    // Build loop config first so checkpoint policy is available for execute.
    // #651: fall back to the persisted `max_iterations` default when the
    // body omits the field. The F49 auto-resume worker POSTs `{}` so
    // without this lookup every follow-up iteration drops back to
    // `LoopConfig::default().max_iterations` (20) and overrides the
    // operator-chosen cap from the first POST.
    let mut cfg = LoopConfig::default();
    if let Some(m) = body.max_iterations.or(default_max_iterations) {
        cfg.max_iterations = m;
    }
    if let Some(t) = body.timeout_ms {
        cfg.timeout_ms = t;
    }
    // #660: route the per-run default into LoopConfig. Absent → inherit
    // LoopConfig::default() (strict gate ON). Present → trust the
    // persisted value. The first orchestrate POST for a run back-fills
    // the default below so subsequent auto-resume kicks observe a
    // stable value rather than falling through to the default on
    // every iteration.
    if let Some(gate) = default_strict_completion_gate {
        cfg.orchestrator_strict_completion_gate = gate;
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
        .build()
        // All six required services are supplied above — any missing
        // setter here is a compile-time regression, not a runtime
        // configuration gap, so `.expect` is the right shape.
        .expect("RuntimeExecutePhase builder misconfigured");

    let sse_emitter = std::sync::Arc::new(crate::sse_hooks::SseOrchestratorEmitter::new(
        state.runtime_sse_tx.clone(),
        state.sse_event_buffer.clone(),
        state.sse_seq.clone(),
    ));

    // Composite emitter: SSE events + ProviderCallCompleted trace recording.
    // Struct + impl live in `super::orchestrate_emitter::TracingEmitter`.
    let emitter: std::sync::Arc<dyn cairn_orchestrator::OrchestratorEventEmitter> =
        std::sync::Arc::new(super::orchestrate_emitter::TracingEmitter {
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
                    if let Err(err) =
                        super::orchestrate_exhaustion::submit_all_providers_exhausted_proposal(
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
