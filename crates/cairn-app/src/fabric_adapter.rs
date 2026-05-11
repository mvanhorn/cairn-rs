//! Bridges trait-based handlers to FabricServices.
//!
//! Reads go to store projection; writes go to Fabric. Installed on
//! `state.runtime.{runs,tasks,sessions}` by `AppState::new`.
//!
//! This module exists so that HTTP handlers can continue to call
//! `state.runtime.runs.get(...)` (a trait method with bare IDs) while the
//! underlying work is routed to [`cairn_fabric::FabricServices`], which
//! requires a `ProjectKey` for every operation. The adapter resolves the
//! missing project context by reading the cairn-store projection first, then
//! delegates to the Fabric service.
//!
//! Scope per service (see `docs/design/notes/cairn-fabric-handler-wiring.md`):
//!
//! | Method kind     | Routing      | Notes                                         |
//! |-----------------|--------------|-----------------------------------------------|
//! | Mutations       | Fabric       | `start`, `complete`, `fail`, `cancel`, …       |
//! | Bare-ID reads   | Projection   | `get(run_id)` — resolve project then delegate |
//! | Batch/list      | Projection   | FF doesn't index by cairn scope               |
//! | Dependencies    | Fabric (T1)  | FF flow-edge fcalls (not store)               |

use std::sync::Arc;

use async_trait::async_trait;
use cairn_domain::TaskDependencyRecord;
use cairn_domain::{
    ApprovalDecision, FailureClass, PauseReason, ProjectKey, ResumeTrigger, RunId, RunResumeTarget,
    SessionId, TaskId, TaskResumeTarget, TaskState,
};
use cairn_fabric::event_bridge::BridgeEvent;
use cairn_fabric::services::ReclaimForTerminalWriteOutcome;
use cairn_fabric::{FabricError, FabricServices};
use cairn_runtime::error::RuntimeError;
use cairn_runtime::runs::RunService;
use cairn_runtime::sessions::SessionService;
use cairn_runtime::tasks::TaskService;
use cairn_store::projections::{
    DescendantsCapOutcome, RunDescendantsCounter, RunReadModel, RunRecord, SessionReadModel,
    SessionRecord, TaskReadModel, TaskRecord,
};
use cairn_store::InMemoryStore;

// ── Project resolvers ────────────────────────────────────────────────────────
//
// The store projections already key records by ID (`HashMap<String, RunRecord>`
// et al.) and each record carries `project: ProjectKey`. No new index is
// required — the resolvers just do the standard `RunReadModel::get(id)` /
// `TaskReadModel::get(id)` / `SessionReadModel::get(id)` lookup and project
// the `project` field out of the returned record.
//
// The projections' `get` methods are `async` (the `RunReadModel` trait
// requires it for Postgres/SQLite backends), so the resolvers are async too.
// Each call is O(1) for InMemoryStore (single mutex-guarded HashMap lookup)
// and a single indexed SELECT for Postgres/SQLite — no I/O amplification.

/// Resolve the owning project for a run from the store projection.
///
/// Returns `Ok(None)` when the run is not in the projection yet (race during
/// create) or when the store has no record of it. Returns `Err` only for
/// store-level failures (e.g. Postgres connection loss).
pub async fn resolve_project_from_run_id(
    store: &Arc<InMemoryStore>,
    run_id: &RunId,
) -> Result<Option<ProjectKey>, RuntimeError> {
    match RunReadModel::get(store.as_ref(), run_id).await? {
        Some(record) => Ok(Some(record.project)),
        None => Ok(None),
    }
}

/// Resolve the owning project for a task from the store projection.
pub async fn resolve_project_from_task_id(
    store: &Arc<InMemoryStore>,
    task_id: &TaskId,
) -> Result<Option<ProjectKey>, RuntimeError> {
    match TaskReadModel::get(store.as_ref(), task_id).await? {
        Some(record) => Ok(Some(record.project)),
        None => Ok(None),
    }
}

/// Resolve the owning project for a session from the store projection.
pub async fn resolve_project_from_session_id(
    store: &Arc<InMemoryStore>,
    session_id: &SessionId,
) -> Result<Option<ProjectKey>, RuntimeError> {
    match SessionReadModel::get(store.as_ref(), session_id).await? {
        Some(record) => Ok(Some(record.project)),
        None => Ok(None),
    }
}

/// Poll the `InMemoryStore` `SessionReadModel` until `session_id` is
/// visible (or 2s elapse). Used by `FabricSessionServiceAdapter::create`
/// as a read-after-write barrier so callers that `POST /v1/sessions`
/// followed immediately by `POST /v1/runs` don't race the
/// `EventBridge` consumer that populates the projection. See the
/// detailed call-site comment for the failure mode.
///
/// Returns `Err(RuntimeError::Internal)` on timeout — strictly safer
/// than returning OK on a session whose projection the very next
/// request will 404 on. The 2s ceiling matches `wait_for_run_projected`
/// in the RFC 020 integration-test harness (same race shape, same
/// budget). Typical resolution is a single poll at 0ms.
async fn wait_for_session_projection(
    store: &Arc<InMemoryStore>,
    session_id: &SessionId,
) -> Result<(), RuntimeError> {
    use std::time::Duration;
    // `tokio::time::Instant` (not `std::time::Instant`) so the deadline
    // and `tokio::time::sleep` track the same clock — matters under
    // `tokio::time::pause()` in unit tests, and is idiomatic for an
    // async polling loop.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut backoff_us: u64 = 100;
    loop {
        if SessionReadModel::get(store.as_ref(), session_id)
            .await?
            .is_some()
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            // SEC-007: the error body MUST NOT leak internal details
            // (bridge consumer state, timing budgets, projection
            // internals) — `RuntimeError::Internal`'s message flows
            // unredacted into HTTP 500 response bodies via
            // `runtime_error_response`. Log the full diagnostic
            // context for operators, surface an opaque message to the
            // caller. Same pattern as `fabric_err_to_runtime`'s
            // `Internal`/`Valkey` arms below.
            tracing::error!(
                session_id = %session_id,
                "session projection not populated within 2s \
                 (EventBridge consumer may be stalled)"
            );
            return Err(RuntimeError::Internal("fabric layer error".into()));
        }
        tokio::time::sleep(Duration::from_micros(backoff_us)).await;
        // Cap at 5 ms so tail-latency samples stay bounded; we're
        // looking at a sub-ms channel drain in the happy path.
        backoff_us = (backoff_us * 2).min(5_000);
    }
}

/// Translate a `FabricError` into the handler-facing `RuntimeError`.
///
/// Both types already carry structured NotFound / Validation / Internal
/// variants, so the mapping is direct. We keep this as a private helper
/// (rather than a `From` impl in cairn-fabric) so cairn-fabric does not
/// depend on cairn-runtime — bridge goes one way, same shape both ends.
fn fabric_err_to_runtime(err: FabricError) -> RuntimeError {
    match err {
        FabricError::NotFound { entity, id } => RuntimeError::NotFound { entity, id },
        FabricError::Validation { reason } => RuntimeError::Validation { reason },
        FabricError::DependencyConflict(detail) => RuntimeError::DependencyConflict(Box::new(
            cairn_runtime::error::DependencyConflictDetail {
                dependent_task_id: detail.dependent_task_id,
                prerequisite_task_id: detail.prerequisite_task_id,
                existing_kind: detail.existing_kind,
                existing_data_passing_ref: detail.existing_data_passing_ref,
                requested_kind: detail.requested_kind,
                requested_data_passing_ref: detail.requested_data_passing_ref,
            },
        )),
        // FF FCALL contention codes are caller-retriable, not operator 5xx:
        // they fire when two workers race for the same lease, when a grant
        // TTL expires mid-claim, or when a scheduler-routed eligible set
        // changes under the caller's feet. Surface them as 409 Conflict so
        // clients can back off + retry instead of triggering ops alerts.
        //
        // The rejection is packed as `FabricError::Internal("<fcall> rejected:
        // <code>")` by `check_fcall_success`; pattern-match on the `: <code>`
        // suffix. Keep the list tight — only FF-documented contention codes
        // belong here; anything else stays Internal so legitimate bugs don't
        // get hidden behind a 409.
        // FF FCALL rejections that mean "resource is not in a state that
        // accepts this operation" — e.g. pause on a pending run with no
        // lease (`fence_required`), pause on a terminal run
        // (`execution_not_active`), or pause on a run whose lease has
        // moved on (`stale_lease`, `invalid_lease_for_suspend`,
        // `already_suspended`). These are not server faults; they are
        // operator-visible state conflicts that the HTTP layer surfaces
        // as 409 Conflict via `RuntimeError::InvalidTransition`. Closes
        // #216 — previously these collapsed into a 500.
        //
        // Ordered BEFORE `is_claim_contention` because `execution_not_active`
        // appears in both shapes: it is a retriable race from the claim
        // path's perspective (someone completed the execution out from
        // under us) but a permanent state-conflict from the suspend/resume
        // path's perspective (you're asking to pause a terminal run).
        // Disambiguate on the FCALL name prefix so suspend/resume
        // rejections can't be misclassified as retriable claim contention.
        // F37: terminal FCALL (`ff_complete_execution` /
        // `ff_fail_execution`) state conflicts. Ordered BEFORE
        // `is_claim_contention` because `execution_not_active` appears
        // in both — fence on the FCALL name prefix to disambiguate.
        FabricError::Internal(ref msg) if is_terminal_state_conflict(msg) => {
            let code = msg
                .rsplit_once(": ")
                .map(|(_, c)| c.trim().to_owned())
                .unwrap_or_else(|| "invalid_state".to_owned());
            // Derive the transition target from the FCALL name so the
            // 409 body reads correctly for each terminal op: complete
            // → "completed", fail → "failed", cancel → "cancelled".
            // Mirrors the suspend/resume handler below which picks
            // "active" vs "suspended" the same way.
            let to = if msg.starts_with("ff_fail_execution") {
                "failed"
            } else if msg.starts_with("ff_cancel_execution") {
                "cancelled"
            } else {
                "completed"
            };
            tracing::debug!(
                fabric_err = %msg,
                code = %code,
                to = %to,
                "fabric terminal-FCALL state conflict (409 to caller)"
            );
            RuntimeError::InvalidTransition {
                entity: "run",
                from: code,
                to: to.to_owned(),
            }
        }
        FabricError::Internal(ref msg) if is_suspend_state_conflict(msg) => {
            let code = msg
                .rsplit_once(": ")
                .map(|(_, c)| c.trim().to_owned())
                .unwrap_or_else(|| "invalid_state".to_owned());
            let to = if msg.starts_with("ff_resume_execution") {
                "active"
            } else {
                "suspended"
            };
            tracing::debug!(
                fabric_err = %msg,
                code = %code,
                to = %to,
                "fabric suspend/resume state conflict (409 to caller)"
            );
            RuntimeError::InvalidTransition {
                entity: "run",
                from: code,
                to: to.to_owned(),
            }
        }
        FabricError::Internal(ref msg) if is_claim_contention(msg) => {
            tracing::debug!(fabric_err = %msg, "fabric claim contention (409 to caller)");
            // SEC-007: the 409 body must not leak the FF FCALL name.
            // `is_claim_contention` already verified the `"<fcall> rejected:
            // <code>"` format; surface only the documented contention code
            // so callers can dispatch without seeing the FCALL internals.
            let code = msg
                .rsplit_once(": ")
                .map(|(_, c)| c.trim().to_owned())
                .unwrap_or_else(|| "claim_contention".to_owned());
            RuntimeError::Conflict {
                entity: "execution",
                id: code,
            }
        }
        // PR-C2 (#599): FF 0.13 typed `EngineError` now surfaces through
        // `FabricError::Engine` for the migrated trait-routed methods
        // (`record_spend`, `release_budget`, `deliver_approval_signal`,
        // `issue_grant_and_claim`, `read_waitpoint_token`). Before this
        // arm, those errors collapsed into the catchall 500. Dispatch
        // on the typed variants so they surface as 409/422 at the HTTP
        // boundary — preserves the F37 invariant (no 5xx leak on
        // expected state conflicts / caller-retriable races).
        FabricError::Engine(boxed) => fabric_engine_err_to_runtime(*boxed),
        // SEC-007: Valkey / script / bridge / config / internal variants
        // carry FCALL names, key names, and occasionally secret-hash
        // references — none of which should reach the 500 response body.
        // Log the detail for operators (journald / CloudWatch) and return
        // an opaque message to the caller.
        other => {
            tracing::error!(fabric_err = %other, "fabric layer error");
            RuntimeError::Internal("fabric layer error".into())
        }
    }
}

/// Map FF 0.13's typed [`EngineError`] variants onto cairn's
/// [`RuntimeError`] so the HTTP handler stack keeps its pre-PR-C2
/// 4xx / 5xx contract (F37).
///
/// Classification axes follow the engine_error docstrings
/// (`ff-core-0.13.0/src/engine_error.rs:40`-`:485`):
/// - `NotFound` → 404 via `RuntimeError::NotFound`
/// - `Validation` → 422 via `RuntimeError::Validation`
///   (cairn renders validation errors as HTTP 422 in
///   `validation_error_response`)
/// - `Contention(_)` → 409 via `RuntimeError::Conflict` (retryable race)
/// - `Conflict(_)` → 409 via `RuntimeError::Conflict` (permanent)
/// - `State(_)` → 409 via either `RuntimeError::Conflict` (signal /
///   waitpoint variants — not on a lifecycle-transition path) or
///   `RuntimeError::InvalidTransition` (lifecycle variants — carrying
///   a kebab-case `from` label clients can branch on)
/// - `Unavailable` / `Transport` / `Bug` / `ResourceExhausted` / `Timeout`
///   → 500 (opaque, audit via tracing — these really are fabric faults)
fn fabric_engine_err_to_runtime(err: cairn_fabric::engine_error::EngineError) -> RuntimeError {
    use cairn_fabric::engine_error::{ConflictKind, ContentionKind, EngineError, StateKind};

    match err {
        EngineError::NotFound { entity } => RuntimeError::NotFound {
            entity,
            id: String::new(),
        },
        EngineError::Validation { kind, detail } => RuntimeError::Validation {
            reason: format!("{kind:?}: {detail}"),
        },
        EngineError::Contention(kind) => {
            let code = match kind {
                ContentionKind::ExecutionNotActive { .. } => "execution_not_active",
                ContentionKind::ExecutionNotEligible => "execution_not_eligible",
                ContentionKind::ExecutionNotLeaseable => "execution_not_leaseable",
                ContentionKind::ExecutionNotReclaimable => "execution_not_reclaimable",
                ContentionKind::ExecutionNotInEligibleSet => "execution_not_in_eligible_set",
                ContentionKind::LeaseConflict => "lease_conflict",
                ContentionKind::InvalidClaimGrant => "invalid_claim_grant",
                ContentionKind::ClaimGrantExpired => "claim_grant_expired",
                ContentionKind::NoEligibleExecution => "no_eligible_execution",
                ContentionKind::NoActiveLease => "no_active_lease",
                ContentionKind::WaitpointNotFound => "waitpoint_not_found",
                ContentionKind::WaitpointPendingUseBufferScript => {
                    "waitpoint_pending_use_buffer_script"
                }
                ContentionKind::StaleGraphRevision => "stale_graph_revision",
                ContentionKind::UseClaimResumedExecution => "use_claim_resumed_execution",
                ContentionKind::NotAResumedExecution => "not_a_resumed_execution",
                ContentionKind::RateLimitExceeded => "rate_limit_exceeded",
                ContentionKind::ConcurrencyLimitExceeded => "concurrency_limit_exceeded",
                ContentionKind::RetryExhausted => "retry_exhausted",
                _ => "contention",
            };
            tracing::debug!(fabric_err = ?kind, code = %code, "fabric engine contention (409 to caller)");
            RuntimeError::Conflict {
                entity: "execution",
                id: code.to_owned(),
            }
        }
        EngineError::State(kind) => {
            // Two-axis mapping (Copilot #599 review):
            // 1. Signal / waitpoint variants do NOT represent a
            //    lifecycle transition — the caller was delivering or
            //    reading a signal, not driving the run to a terminal
            //    state. Return `RuntimeError::Conflict { entity: "signal" }`
            //    so the 409 body doesn't falsely advertise a
            //    "from: X / to: completed" lifecycle transition.
            // 2. Everything else IS a lifecycle transition attempt
            //    (complete / fail / cancel / resume / claim). Return
            //    `RuntimeError::InvalidTransition` with the kebab-case
            //    "from" label so clients can branch without re-parsing
            //    a message.
            let code = match kind {
                StateKind::StaleLease => "stale_lease",
                StateKind::LeaseExpired => "lease_expired",
                StateKind::LeaseRevoked => "lease_revoked",
                StateKind::ExecutionNotSuspended => "execution_not_suspended",
                StateKind::AlreadySuspended => "already_suspended",
                StateKind::WaitpointClosed => "waitpoint_closed",
                StateKind::TargetNotSignalable => "target_not_signalable",
                StateKind::DuplicateSignal => "duplicate_signal",
                StateKind::ResumeConditionNotMet => "resume_condition_not_met",
                StateKind::WaitpointNotPending => "waitpoint_not_pending",
                StateKind::PendingWaitpointExpired => "pending_waitpoint_expired",
                StateKind::WaitpointNotOpen => "waitpoint_not_open",
                StateKind::ExecutionNotTerminal => "execution_not_terminal",
                StateKind::FlowAlreadyTerminal => "flow_already_terminal",
                StateKind::DepsNotSatisfied => "deps_not_satisfied",
                StateKind::NotBlockedByDeps => "not_blocked_by_deps",
                StateKind::NotRunnable => "not_runnable",
                StateKind::Terminal => "terminal",
                StateKind::BudgetExceeded => "budget_exceeded",
                StateKind::BudgetSoftExceeded => "budget_soft_exceeded",
                StateKind::OkAlreadyApplied => "ok_already_applied",
                _ => "invalid_state",
            };

            // Signal/waitpoint-bucket: NOT a lifecycle transition. Avoid
            // the misleading `InvalidTransition { to: "completed" }` body.
            let is_signal_bucket = matches!(
                kind,
                StateKind::WaitpointClosed
                    | StateKind::TargetNotSignalable
                    | StateKind::DuplicateSignal
                    | StateKind::ResumeConditionNotMet
                    | StateKind::WaitpointNotPending
                    | StateKind::PendingWaitpointExpired
                    | StateKind::WaitpointNotOpen
            );

            if is_signal_bucket {
                tracing::debug!(fabric_err = ?kind, code = %code, "fabric engine signal-path state conflict (409 to caller)");
                RuntimeError::Conflict {
                    entity: "signal",
                    id: code.to_owned(),
                }
            } else {
                tracing::debug!(fabric_err = ?kind, from = %code, "fabric engine lifecycle state conflict (409 to caller)");
                RuntimeError::InvalidTransition {
                    entity: "run",
                    from: code.to_owned(),
                    to: "completed".to_owned(),
                }
            }
        }
        EngineError::Conflict(kind) => {
            // Copilot #599 review: return a stable kebab-case code
            // rather than `format!("{kind:?}")` — the upstream debug
            // repr may include struct fields (e.g.
            // `DependencyAlreadyExists { existing }`) that leak internal
            // shape and can change across FF versions. Fixed
            // kebab-case tokens give HTTP clients a stable id to branch
            // on and keep SEC-007 in force.
            let code = match kind {
                ConflictKind::DependencyAlreadyExists { .. } => "dependency_already_exists",
                ConflictKind::CycleDetected => "cycle_detected",
                ConflictKind::SelfReferencingEdge => "self_referencing_edge",
                ConflictKind::ExecutionAlreadyInFlow => "execution_already_in_flow",
                ConflictKind::WaitpointAlreadyExists => "waitpoint_already_exists",
                ConflictKind::BudgetAttachConflict => "budget_attach_conflict",
                ConflictKind::QuotaAttachConflict => "quota_attach_conflict",
                ConflictKind::RotationConflict(_) => "rotation_conflict",
                ConflictKind::ActiveAttemptExists => "active_attempt_exists",
                _ => "conflict",
            };
            tracing::debug!(fabric_err = ?kind, code = %code, "fabric engine conflict (409 to caller)");
            RuntimeError::Conflict {
                entity: "execution",
                id: code.to_owned(),
            }
        }
        // Everything else (Unavailable / Transport / Bug /
        // ResourceExhausted / Timeout / StreamDisconnected / StreamLag)
        // is a genuine fabric fault — log for operators, opaque 500 to
        // caller. Preserves SEC-007 (no FCALL names / key names in the
        // response body).
        other => {
            tracing::error!(fabric_err = %other, "fabric engine layer error");
            RuntimeError::Internal("fabric layer error".into())
        }
    }
}

/// FF typed error codes emitted by `ff_suspend_execution` and
/// `ff_resume_execution` when the request is rejected because the
/// execution is not in a state that can accept the transition. Unlike
/// [`is_claim_contention`], these are operator-visible state conflicts
/// (rather than caller-retriable races) and map to
/// `RuntimeError::InvalidTransition` → HTTP 409 via the app's
/// `runtime_error_response`.
///
/// The codes come from `ff-script::flowfabric.lua` — see
/// `ff_suspend_execution` and `validate_lease_and_mark_expired`.
/// FF typed error codes emitted by the terminal FCALLs
/// (`ff_complete_execution`, `ff_fail_execution`, `ff_cancel_execution`)
/// when the transition is rejected because the execution is not in a
/// state that can accept it. These are operator-visible state
/// conflicts, not server faults — map them to
/// [`RuntimeError::InvalidTransition`] → HTTP 409 so the caller gets a
/// structured response instead of an opaque 500 "fabric layer error"
/// leaking FF internals (F37).
///
/// Codes come from `ff-script::flowfabric.lua` —
/// `validate_lease_and_mark_expired` + `resolve_lease_fence`.
fn is_terminal_state_conflict(msg: &str) -> bool {
    const STATE_CODES: &[&str] = &[
        // Already terminal / non-active: the scanner moved the exec
        // before the orchestrator's terminal FCALL landed.
        "execution_not_active",
        // Lease expired while we were computing the terminal write.
        // validate_lease_and_mark_expired gates on lease_expires_at
        // before accepting even the operator_override path.
        "lease_expired",
        // Lease was revoked by an operator before complete landed.
        "lease_revoked",
        // Caller passed a fence but the stored lease moved on.
        "stale_lease",
        // Unfenced path rejected because source != "operator_override".
        // In cairn's current code this is a programming bug (we always
        // pass operator_override on unfenced) — surface as 409 for
        // debuggability rather than collapsing to 500.
        "fence_required",
        // Partial fence triple is the F37 bug itself. It should be
        // unreachable post-fix; classifying it here means a regression
        // reintroducing it surfaces as 409 with a specific code,
        // instead of disappearing into the generic "fabric layer error"
        // 500 that hid it for weeks.
        "partial_fence_triple",
    ];
    if !msg.starts_with("ff_complete_execution")
        && !msg.starts_with("ff_fail_execution")
        && !msg.starts_with("ff_cancel_execution")
    {
        return false;
    }
    let Some((_, code)) = msg.rsplit_once(": ") else {
        return false;
    };
    STATE_CODES.contains(&code.trim())
}

fn is_suspend_state_conflict(msg: &str) -> bool {
    const STATE_CODES: &[&str] = &[
        // No lease / partial fence triple — run has not been claimed
        // yet (typical for a pending run the operator tries to pause).
        "fence_required",
        "partial_fence_triple",
        // Run is in a terminal phase (completed, failed, cancelled) or
        // otherwise not in `active` lifecycle_phase.
        "execution_not_active",
        // Lease was revoked before suspend landed.
        "lease_revoked",
        // Lease moved on — stale epoch / id / attempt_id.
        "stale_lease",
        "invalid_lease_for_suspend",
        // A suspension is already open for this execution.
        "already_suspended",
        // Waitpoint exists but is in an unexpected shape (pending
        // record without a minted HMAC token, etc.).
        "waitpoint_not_token_bound",
    ];
    // Gate on the FCALL name so we don't swallow a shared code
    // (`execution_not_active`) emitted from the claim path.
    if !msg.starts_with("ff_suspend_execution") && !msg.starts_with("ff_resume_execution") {
        return false;
    }
    let Some((_, code)) = msg.rsplit_once(": ") else {
        return false;
    };
    STATE_CODES.contains(&code.trim())
}

/// FF typed error codes that represent caller-retriable contention rather
/// than an operator-alert system fault. See `ff-script::ScriptError` for
/// the canonical list and `claim_common::issue_grant_and_claim` for the
/// call sites that produce them.
fn is_claim_contention(msg: &str) -> bool {
    const CONTENTION_CODES: &[&str] = &[
        "lease_conflict",
        "invalid_claim_grant",
        "claim_grant_expired",
        "execution_not_leaseable",
        "execution_not_eligible",
        "execution_not_eligible_for_attempt",
        // Scheduler-routed contention: another scheduler already pulled
        // the execution out of the eligible set before this caller's
        // grant-issue FCALL landed. Caller-retriable (wait for the
        // winner to finish or for a new execution to become eligible).
        "execution_not_in_eligible_set",
        // Grant-step contention: another worker's grant was still
        // active (within grant_ttl_ms) when this caller tried to
        // issue its own. This is the dominant shape of the
        // concurrent-claim race: N callers hit ff_issue_claim_grant
        // simultaneously, the first wins, the others see this.
        "grant_already_exists",
        "execution_not_found",
        // Replay/terminal contention: a terminal path (cancellation,
        // completion) mutated state between eligibility and claim.
        "execution_not_active",
        "no_active_lease",
        "no_eligible_execution",
    ];
    // Format (from check_fcall_success): "<fcall> rejected: <code>".
    let Some((_, code)) = msg.rsplit_once(": ") else {
        return false;
    };
    CONTENTION_CODES.contains(&code.trim())
}

/// F64 backoff schedule (milliseconds) for the bounded terminal-write
/// recovery loop. Each entry is the sleep BEFORE the corresponding
/// re-claim + FCALL retry probe. The first probe runs immediately
/// (0ms) so a system that self-heals between the initial lease_expired
/// and this loop entry doesn't pay a mandatory 2s latency tax. The
/// subsequent entries follow the 2/4/8/16s geometric backoff —
/// total wall-clock cap is ~30s (0 + 2 + 4 + 8 + 16) before the F62
/// `TerminalWriteDeadlock` fallback fires.
const F64_BACKOFF_MS: [u64; 5] = [0, 2_000, 4_000, 8_000, 16_000];

/// F64 aggressive healing env-var. Currently enables a log-only
/// breadcrumb inside the recovery loop's deadlock branch — useful for
/// confirming the hook fires against real incidents before committing
/// to mutating side-effects. The target end-state is a proactive
/// approval-cancel (FF's approval-resolution handlers can push the
/// execution phase forward), but wiring that requires an
/// ApprovalService handle on the adapter and is intentionally
/// deferred.
///
/// Default OFF — set to `1` / `true` / `yes` / `on` (case-insensitive)
/// to enable the current placeholder branch; the truthy set matches
/// the convention used by `otlp_config_from_env` et al. Enable
/// selectively (e.g. M1-v2 dogfood re-runs) when you need the
/// breadcrumb; leave off in production until the cancel path ships.
const F64_AGGRESSIVE_HEAL_ENV: &str = "CAIRN_F64_AGGRESSIVE_HEAL";

fn f64_aggressive_heal_enabled() -> bool {
    std::env::var(F64_AGGRESSIVE_HEAL_ENV)
        .ok()
        .map(|v| v.trim().to_ascii_lowercase())
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

/// Minimum-remaining lease budget for the pre-terminal renew.
///
/// Long enough that the terminal FCALL will not trip
/// `validate_lease_and_mark_expired` on a clock-skew hair; short
/// enough that `renew_lease_if_stale`'s in-place extension branch
/// dominates the hot path (we want to extend, not reclaim).
const F59_MIN_REMAINING_MS: u64 = 10_000;

/// Pre-terminal-FCALL lease renew.
///
/// Idempotent on a fresh lease (snapshot read only). Tolerates the
/// transient phase conflicts classified by
/// `RuntimeError::is_transient_phase_conflict` — they indicate the
/// execution is briefly in a non-`runnable` sub-phase (sibling FCALL
/// mid-tool or mid-write), the existing lease is still valid, and the
/// terminal FCALL will accept it. Hard errors (NotFound, permanent
/// Conflicts, Store / Internal) propagate unchanged.
///
/// # Performance note
///
/// This issues its own `describe_execution` via
/// `renew_lease_if_stale`; the terminal FCALL methods
/// (`FabricRunService::{complete,fail,cancel}`) also do their own
/// `describe_execution` to resolve the lease context. On the common
/// hot path (lease still fresh, layer (a) no-ops) that is two
/// snapshot reads per terminal FCALL. Consolidating them requires
/// refactoring the FabricRunService method surface to accept a
/// pre-resolved snapshot — an fabric-layer change outside this
/// adapter's scope. The snapshot read is a single HGETALL on the
/// execution hash (typical sub-ms on loopback Valkey); the
/// orthogonal correctness win from renewing before the FCALL
/// dominates the extra round-trip.
/// #655 pre-terminal renew retry schedule.
///
/// When the pre-terminal renew hits `execution_not_eligible` after a
/// suspension resume, FF's phase machine is still catching up to
/// cairn's approval-resolution event. Tolerating the first failure
/// (the pre-#655 F59 behaviour) hands the terminal FCALL an already-
/// dead lease because `renew_lease_if_stale` never reached its
/// `claim_with_snapshot` fallback. Retrying on a short backoff gives
/// FF time to clear the residual phase state without spraying the
/// lease-keeper's projection-driven skip logic into this tight path.
///
/// Five steps, ~3 s total: the overwhelming majority of
/// `execution_not_eligible` windows clear within one FF scanner
/// cycle (~1.5 s). The schedule's final arm covers back-to-back
/// approval resumes where the scanner is still advancing the prior
/// iteration. Anything longer than this still falls through to F64's
/// 30 s bounded recovery loop — same ceiling as pre-#655.
const F59_PHASE_RETRY_BACKOFF_MS: [u64; 5] = [100, 250, 500, 1_000, 1_500];

async fn f59_prelude_renew(
    fabric: &Arc<FabricServices>,
    project: &ProjectKey,
    session_id: &SessionId,
    run_id: &RunId,
    fcall: &'static str,
) -> Result<(), RuntimeError> {
    // Extract the renewal invocation into a closure so the initial
    // attempt and retry attempts share one call site — keeps the
    // error classification logic consistent and avoids the "did we
    // remember to update both arms?" maintenance hazard. Gemini
    // review on #658 flagged the duplication.
    let renew = || async {
        fabric
            .runs
            .renew_lease_if_stale(project, session_id, run_id, F59_MIN_REMAINING_MS)
            .await
            .map_err(fabric_err_to_runtime)
    };

    // First attempt: hot path. If the lease is fresh or can be
    // extended/reclaimed in a single hop, we're done in one FCALL.
    let mut last_err = match renew().await {
        Ok(_) => return Ok(()),
        Err(err) if err.is_transient_phase_conflict() => err,
        Err(err) => return Err(err),
    };

    // #655 retry loop: FF's phase is momentarily non-eligible. This
    // is the post-suspension-resume race where the `ToolCallApproved`
    // event has landed in cairn's projection but FF's execution
    // hasn't been nudged back to `runnable`/`active` yet. A short
    // backoff lets FF's scanners advance without ever falling
    // through to the coarser F64 loop.
    //
    // Cancellation: this fn is called from the axum request handler
    // (inside `FabricServiceRunAdapter::complete / fail / cancel`).
    // No explicit CancellationToken is available on this path, but
    // the reqwest client aborts the entire future tree on disconnect
    // and the 30 s F64 ceiling dominates this 3 s retry window, so
    // extending this helper with a token would be cosmetic — the
    // HTTP transport already provides the escape hatch. Gemini
    // review on #658 suggested wrapping `sleep` in `tokio::select!`
    // for cancellation symmetry with the lease-keeper's
    // long-running loop; declining because the shapes are
    // different (keeper is a background task with an owned token;
    // this is a bounded async call chain) and adding a unused
    // token plumb-through on every call site would regress the
    // surface without operator-visible benefit.
    for backoff_ms in F59_PHASE_RETRY_BACKOFF_MS {
        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
        match renew().await {
            Ok(_) => {
                tracing::debug!(
                    run_id = %run_id,
                    fcall,
                    backoff_ms,
                    "#655 F59: pre-terminal renew cleared after short-backoff retry"
                );
                return Ok(());
            }
            Err(err) if err.is_transient_phase_conflict() => {
                last_err = err;
            }
            Err(err) => return Err(err),
        }
    }

    // Still transient after the full schedule. Fall through with
    // the pre-#655 behaviour: log + tolerate so the terminal FCALL
    // either succeeds against the existing lease (happy path when
    // only the PHASE was wrong, not the lease) or triggers F64's
    // bounded recovery (happy path when the lease itself died).
    tracing::debug!(
        run_id = %run_id,
        fcall,
        error = %last_err,
        "F59: pre-terminal renew still transient after retry schedule; \
         continuing with existing lease (F64 recovery may engage)"
    );
    Ok(())
}

/// F64: bounded terminal-write recovery loop.
///
/// Replaces F59's single-shot short-circuit on
/// `lease_expired` + re-claim `NotEligible` with a backoff-driven retry.
/// FF often self-heals the not-eligible phase within a few seconds
/// (scanner cycles advance execution state; lease reaper clears stale
/// claims), so giving up after one re-claim attempt throws away runs
/// that would have completed with a brief wait. This helper keeps
/// retrying (re-claim + terminal-FCALL retry) on each backoff step
/// until:
///
/// * a retry succeeds (`outcome = "recovered"`), OR
/// * the backoff schedule is exhausted (`outcome = "deadlocked"`), at
///   which point F62's TerminalWriteDeadlock fallback fires — the run
///   flips `Failed(TerminalWriteDeadlock)` with the upstream-link
///   hint, and the operator-facing response becomes
///   `InvalidTransition { terminal_write_deadlock → <to> }`.
///
/// Regardless of the outcome, a `TerminalRecoveryAttempted` bridge
/// event is emitted at the end of the loop with the attempt count,
/// wall-clock duration, and outcome, so operators see on
/// `GET /v1/runs/:id` whether recovery fired and whether it saved the
/// run.
///
/// Removable once FF#371 lands upstream.
///
/// Error shaping on non-transient failures mirrors the previous F59
/// contract (SEC-007 / F37): `RuntimeError::Internal` is re-shaped to
/// `InvalidTransition { lease_expired → <to> }`; structured variants
/// (`NotFound`, permanent `Conflict`, `Validation`, …) propagate
/// unchanged.
#[allow(clippy::too_many_arguments)]
async fn f64_terminal_recovery_loop<F, Fut>(
    fabric: &Arc<FabricServices>,
    store: &Arc<InMemoryStore>,
    project: &ProjectKey,
    session_id: &SessionId,
    run_id: &RunId,
    fcall: &'static str,
    to: &'static str,
    original_err: &RuntimeError,
    mut attempt_fcall: F,
) -> Result<RunRecord, RuntimeError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<RunRecord, RuntimeError>>,
{
    tracing::warn!(
        run_id = %run_id,
        fcall,
        error = %original_err,
        "F64: terminal FCALL rejected with lease_expired; entering bounded \
         recovery loop (immediate probe + 2s/4s/8s/16s backoff, ~30s cap)"
    );

    if f64_aggressive_heal_enabled() {
        // Placeholder: when aggressive-heal is enabled, operators opt
        // into proactive healing side-effects. Current shipped surface
        // is a structured log breadcrumb so the hook is observable +
        // ready to be wired to an approval-cancel path once the
        // adapter grows an ApprovalService handle. Keep this as a
        // named line rather than a noop so grep against real logs
        // shows whether the env var reached a subprocess.
        tracing::warn!(
            run_id = %run_id,
            fcall,
            env = F64_AGGRESSIVE_HEAL_ENV,
            "F64: aggressive-heal enabled; proactive approval-cancel \
             would fire here (shipped: log-only hook, wire via \
             AppState ApprovalService handle when needed)"
        );
    }

    let start = std::time::Instant::now();
    let mut attempts: u32 = 0;
    let mut last_err: RuntimeError = clone_runtime_error(original_err);

    for (step, backoff_ms) in F64_BACKOFF_MS.iter().enumerate() {
        attempts = attempts.saturating_add(1);
        // Log the attempt BEFORE the backoff sleep so an operator
        // tailing the log sees the loop progressing even if the
        // subprocess is SIGKILL'd mid-sleep — without this, the
        // audit trail jumps from the entry warn straight to the
        // exhaustion warn with no per-iter breadcrumbs.
        tracing::info!(
            run_id = %run_id,
            fcall,
            attempt = attempts,
            step = step + 1,
            backoff_ms,
            "F64: starting reclaim attempt after backoff"
        );
        tokio::time::sleep(std::time::Duration::from_millis(*backoff_ms)).await;

        // Reclaim via FF 0.15 issue_reclaim_grant + reclaim_execution
        // (#710). Three outcomes — Reclaimed (retry FCALL),
        // NotReclaimable (phase moved on; continue backoff),
        // CapExceeded (terminal_failed; no recovery).
        let reclaim_start = std::time::Instant::now();
        let reclaim_result = fabric
            .runs
            .reclaim_for_terminal_write(project, session_id, run_id)
            .await;
        let reclaim_ms = u64::try_from(reclaim_start.elapsed().as_millis()).unwrap_or(u64::MAX);

        let outcome_label: &'static str = match &reclaim_result {
            Ok(ReclaimForTerminalWriteOutcome::Reclaimed(_)) => "reclaimed",
            Ok(ReclaimForTerminalWriteOutcome::NotReclaimable { .. }) => "not_reclaimable",
            Ok(ReclaimForTerminalWriteOutcome::CapExceeded { .. }) => "cap_exceeded",
            Err(_) => "error",
        };
        tracing::debug!(
            run_id = %run_id,
            fcall,
            attempt = attempts,
            outcome = outcome_label,
            reclaim_ms,
            "F64: reclaim_for_terminal_write returned"
        );

        match reclaim_result {
            Ok(ReclaimForTerminalWriteOutcome::Reclaimed(_record)) => {
                // Fresh attempt minted; retry the terminal FCALL.
                tracing::info!(
                    run_id = %run_id,
                    fcall,
                    attempt = attempts,
                    step = step + 1,
                    backoff_ms,
                    reclaim_ms,
                    "F64: reclaim succeeded; retrying terminal FCALL"
                );
                match attempt_fcall().await {
                    Ok(record) => {
                        let wall_ms =
                            u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
                        tracing::info!(
                            run_id = %run_id,
                            fcall,
                            attempts,
                            wall_time_ms = wall_ms,
                            "F64: terminal FCALL recovered after reclaim"
                        );
                        emit_recovery_attempt(
                            fabric,
                            project,
                            run_id,
                            fcall,
                            attempts,
                            wall_ms,
                            "recovered",
                        )
                        .await;
                        return Ok(record);
                    }
                    Err(retry_err) => {
                        if retry_err.is_lease_expired() {
                            // Fresh lease expired inside the FCALL
                            // (the TTL window we just minted still
                            // lost the race). Keep looping.
                            tracing::warn!(
                                run_id = %run_id,
                                fcall,
                                attempt = attempts,
                                error = %retry_err,
                                "F64: terminal FCALL retry hit lease_expired again; \
                                 continuing recovery loop"
                            );
                            last_err = retry_err;
                            continue;
                        }
                        // Non-transient retry error — operator sees
                        // accurate failure class. Still emit the
                        // recovery-attempted event so the attempt is
                        // visible.
                        let wall_ms =
                            u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
                        emit_recovery_attempt(
                            fabric,
                            project,
                            run_id,
                            fcall,
                            attempts,
                            wall_ms,
                            "non_transient_retry_error",
                        )
                        .await;
                        return Err(shape_terminal_retry_error(retry_err, to));
                    }
                }
            }
            Ok(ReclaimForTerminalWriteOutcome::NotReclaimable { detail }) => {
                // Execution moved out of `lease_expired_reclaimable`
                // between cairn's lease-expired detection and the
                // grant attempt. Per the helper doc, the right
                // response is "retry the original FCALL without
                // reclaim" — but the FCALL already failed once
                // with lease_expired, so the lease likely is still
                // gone. Fold this into the existing transient-
                // phase-conflict path: keep looping; FF may clear
                // the not-eligible phase on the next scanner cycle.
                tracing::info!(
                    run_id = %run_id,
                    fcall,
                    attempt = attempts,
                    step = step + 1,
                    backoff_ms,
                    detail = %detail,
                    "F64: reclaim returned NotReclaimable; continuing backoff"
                );
                // Preserve the "phase still transient" failure
                // class on `last_err` so the exhaustion path
                // surfaces an accurate cause. Reusing
                // `original_err`'s shape (lease_expired) keeps the
                // log filter on `last_error` predictable.
                last_err = clone_runtime_error(original_err);
                continue;
            }
            Ok(ReclaimForTerminalWriteOutcome::CapExceeded { reclaim_count }) => {
                // FF moved the execution to terminal_failed —
                // `max_reclaim_count` was hit. No further reclaims
                // are possible; surface to the operator with a
                // concrete failure class so the incident is
                // visible.
                tracing::error!(
                    run_id = %run_id,
                    fcall,
                    attempt = attempts,
                    reclaim_count,
                    "F64: reclaim cap exceeded; execution moved to terminal_failed; \
                     exiting loop"
                );
                let wall_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
                emit_recovery_attempt(
                    fabric,
                    project,
                    run_id,
                    fcall,
                    attempts,
                    wall_ms,
                    "reclaim_cap_exceeded",
                )
                .await;
                return Err(shape_terminal_retry_error(
                    clone_runtime_error(original_err),
                    to,
                ));
            }
            Err(rc_err) => {
                let rc_err_runtime = fabric_err_to_runtime(rc_err);
                if rc_err_runtime.is_transient_phase_conflict() {
                    tracing::info!(
                        run_id = %run_id,
                        fcall,
                        attempt = attempts,
                        step = step + 1,
                        backoff_ms,
                        error = %rc_err_runtime,
                        "F64: reclaim still rejected with transient phase conflict; \
                         continuing backoff"
                    );
                    last_err = rc_err_runtime;
                    continue;
                }
                // Non-transient reclaim error — operator sees the
                // accurate failure class. Emit recovery-attempted
                // so the incident is auditable.
                tracing::error!(
                    run_id = %run_id,
                    fcall,
                    attempt = attempts,
                    error = %rc_err_runtime,
                    "F64: reclaim failed with non-transient error; exiting loop"
                );
                let wall_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
                emit_recovery_attempt(
                    fabric,
                    project,
                    run_id,
                    fcall,
                    attempts,
                    wall_ms,
                    "non_transient_reclaim_error",
                )
                .await;
                return Err(shape_terminal_retry_error(rc_err_runtime, to));
            }
        }
    }

    // Backoff schedule exhausted — F62 TerminalWriteDeadlock fallback.
    // #710: URL retargeted from the (now-closed) FF#371 to cairn's
    // own consumer-migration tracker. FF#371 closed 2026-04-28 (PR
    // 407 in FF 0.15.0 shipped `issue_reclaim_grant` / `claim_from_reclaim_grant`);
    // the remaining actionable work is the cairn-side consumer
    // migration from the pre-0.15 `issue_grant_and_claim` recovery
    // path to the new reclaim-grant APIs. See #710 for the
    // migration plan.
    //
    // The tracing field name stays `upstream_issue` to preserve
    // log-aggregation filters + alerts that depend on the key. Only
    // the URL value flips. "Upstream" is used loosely here — this
    // run is blocked by a pre-FF-0.15 consumer path we control, not
    // by FF itself — but the field identity is the public contract.
    let wall_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
    tracing::warn!(
        run_id = %run_id,
        fcall,
        attempts,
        wall_time_ms = wall_ms,
        upstream_issue = "https://github.com/avifenesh/cairn-rs/issues/710",
        last_error = %last_err,
        "F62/F64: recovery loop exhausted — dual-door deadlock. Flipping \
         run to Failed(TerminalWriteDeadlock). Cairn's pre-FF-0.15 \
         recovery path cannot clear this; migration to FF 0.15 \
         issue_reclaim_grant tracked at cairn-rs#710"
    );
    let prev_state = match RunReadModel::get(store.as_ref(), run_id).await {
        Ok(Some(record)) => Some(record.state),
        Ok(None) => {
            tracing::warn!(
                run_id = %run_id,
                fcall,
                "F62: run not found in projection while emitting \
                 ExecutionFailed(TerminalWriteDeadlock); prev_state will be None"
            );
            None
        }
        Err(store_err) => {
            tracing::warn!(
                run_id = %run_id,
                fcall,
                error = %store_err,
                "F62: store error reading prev_state for \
                 ExecutionFailed(TerminalWriteDeadlock); emitting with prev_state=None"
            );
            None
        }
    };
    fabric
        .bridge
        .emit(BridgeEvent::ExecutionFailed {
            run_id: run_id.clone(),
            project: project.clone(),
            failure_class: FailureClass::TerminalWriteDeadlock,
            prev_state,
        })
        .await;
    emit_recovery_attempt(
        fabric,
        project,
        run_id,
        fcall,
        attempts,
        wall_ms,
        "deadlocked",
    )
    .await;
    Err(RuntimeError::InvalidTransition {
        entity: "run",
        from: "terminal_write_deadlock".to_owned(),
        to: to.to_owned(),
    })
}

/// F64: re-shape a retry error that came back from the terminal FCALL
/// after a successful re-claim. Retains the SEC-007 / F37 invariants
/// (no Internal leak; no 500 on terminal-state conflicts).
fn shape_terminal_retry_error(retry_err: RuntimeError, to: &'static str) -> RuntimeError {
    if retry_err.is_lease_expired() {
        retry_err
    } else if matches!(retry_err, RuntimeError::Internal(_)) {
        RuntimeError::InvalidTransition {
            entity: "run",
            from: "lease_expired".to_owned(),
            to: to.to_owned(),
        }
    } else {
        retry_err
    }
}

/// F64: emit the `TerminalRecoveryAttempted` bridge event. Centralised
/// so the three call sites (complete/fail/cancel via
/// `f64_terminal_recovery_loop`) share one write. `outcome` is an
/// open-enum string; the `"recovered"` and `"deadlocked"` values
/// appear on the operator surface, and the extra strings
/// (`"non_transient_retry_error"`, `"non_transient_reclaim_error"`)
/// are auditable via event-log replay.
async fn emit_recovery_attempt(
    fabric: &Arc<FabricServices>,
    project: &ProjectKey,
    run_id: &RunId,
    fcall: &'static str,
    attempts: u32,
    wall_time_ms: u64,
    outcome: &str,
) {
    // Fail loudly if the system clock reports a time before UNIX_EPOCH
    // or past u64::MAX ms. These are hardware/schema-drift shapes, not
    // recoverable states — log with full context and skip emission
    // rather than silently falling back to `0` (which would make the
    // audit row look like it occurred in 1970 and mislead operators).
    let occurred_at_ms = match std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_millis()).ok())
    {
        Some(ms) => ms,
        None => {
            tracing::error!(
                run_id = %run_id,
                fcall,
                outcome,
                attempts,
                wall_time_ms,
                "F64: system clock before UNIX_EPOCH or past u64::MAX ms; \
                 skipping TerminalRecoveryAttempted emission (hardware/clock fault)"
            );
            return;
        }
    };
    fabric
        .bridge
        .emit(BridgeEvent::TerminalRecoveryAttempted {
            run_id: run_id.clone(),
            project: project.clone(),
            fcall: fcall.to_owned(),
            attempts,
            wall_time_ms,
            outcome: outcome.to_owned(),
            occurred_at_ms,
        })
        .await;
}

/// F64: shallow clone of a `RuntimeError` by value. `RuntimeError`
/// doesn't derive `Clone` across all variants (the Internal/Store
/// wrappers own non-Clone payloads), so we reconstruct the variants we
/// care about preserving and fall back to the Display string for the
/// rest. Used to keep the original `lease_expired` error as the
/// `last_err` for logging after we consume it by-ref in the loop.
fn clone_runtime_error(err: &RuntimeError) -> RuntimeError {
    match err {
        RuntimeError::InvalidTransition { entity, from, to } => RuntimeError::InvalidTransition {
            entity,
            from: from.clone(),
            to: to.clone(),
        },
        RuntimeError::LeaseExpired { task_id } => RuntimeError::LeaseExpired {
            task_id: task_id.clone(),
        },
        other => RuntimeError::Internal(format!("{other}")),
    }
}

// ── RunService adapter ───────────────────────────────────────────────────────

/// Adapter routing [`RunService`] calls to [`FabricServices::runs`].
pub struct FabricRunServiceAdapter {
    pub fabric: Arc<FabricServices>,
    pub store: Arc<InMemoryStore>,
}

impl FabricRunServiceAdapter {
    pub fn new(fabric: Arc<FabricServices>, store: Arc<InMemoryStore>) -> Self {
        Self { fabric, store }
    }
}

#[async_trait]
impl RunService for FabricRunServiceAdapter {
    async fn start(
        &self,
        project: &ProjectKey,
        session_id: &SessionId,
        run_id: RunId,
        parent_run_id: Option<RunId>,
    ) -> Result<RunRecord, RuntimeError> {
        // Caller already supplies a project — straight delegation, no
        // projection lookup needed.
        self.fabric
            .runs
            .start(project, session_id, run_id, parent_run_id)
            .await
            .map_err(fabric_err_to_runtime)
    }

    /// Override the default trait impl (which drops the correlation_id and
    /// falls through to `start`). Fabric path threads the correlation onto
    /// the FF `cairn.correlation_id` exec_core tag AND onto the emitted
    /// `BridgeEvent::ExecutionCreated` so the cairn-store envelope's
    /// `correlation_id` field is populated for SSE / audit consumers. Sqeq
    /// ingress is the primary caller.
    async fn start_with_correlation(
        &self,
        project: &ProjectKey,
        session_id: &SessionId,
        run_id: RunId,
        parent_run_id: Option<RunId>,
        correlation_id: &str,
    ) -> Result<RunRecord, RuntimeError> {
        self.fabric
            .runs
            .start_with_correlation(
                project,
                session_id,
                run_id,
                parent_run_id,
                Some(correlation_id),
            )
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn get(&self, run_id: &RunId) -> Result<Option<RunRecord>, RuntimeError> {
        let record = match resolve_run_scope(&self.store, run_id).await? {
            Some(r) => r,
            None => return Ok(None),
        };
        self.fabric
            .runs
            .get(&record.project, &record.session_id, run_id)
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn list_by_session(
        &self,
        session_id: &SessionId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<RunRecord>, RuntimeError> {
        // Projection path: FF does not index runs by cairn SessionId.
        // cairn-store's RunReadModel serves this view from the event log;
        // `FabricRunService::list_by_session` itself returns an empty Vec by
        // design (see run_service.rs:398-402).
        list_runs_by_session_from_projection(&self.store, session_id, limit, offset).await
    }

    async fn complete(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
    ) -> Result<RunRecord, RuntimeError> {
        // Cross-check the caller's session_id against the projection.
        // Mismatch is an operator error (the run is keyed to a
        // different session in the projection); we fail loud rather
        // than silently minting a different ExecutionId.
        let project = resolve_run_project_checking_session(&self.store, run_id, session_id).await?;
        f59_prelude_renew(&self.fabric, &project, session_id, run_id, "complete").await?;
        let first = self
            .fabric
            .runs
            .complete(&project, session_id, run_id)
            .await
            .map_err(fabric_err_to_runtime);
        match first {
            Ok(record) => Ok(record),
            Err(err) if err.is_lease_expired() => {
                f64_terminal_recovery_loop(
                    &self.fabric,
                    &self.store,
                    &project,
                    session_id,
                    run_id,
                    "complete",
                    "completed",
                    &err,
                    || async {
                        self.fabric
                            .runs
                            .complete(&project, session_id, run_id)
                            .await
                            .map_err(fabric_err_to_runtime)
                    },
                )
                .await
            }
            Err(err) => Err(err),
        }
    }

    async fn fail(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
        failure_class: FailureClass,
    ) -> Result<RunRecord, RuntimeError> {
        let project = resolve_run_project_checking_session(&self.store, run_id, session_id).await?;
        f59_prelude_renew(&self.fabric, &project, session_id, run_id, "fail").await?;
        let first = self
            .fabric
            .runs
            .fail(&project, session_id, run_id, failure_class)
            .await
            .map_err(fabric_err_to_runtime);
        match first {
            Ok(record) => Ok(record),
            Err(err) if err.is_lease_expired() => {
                f64_terminal_recovery_loop(
                    &self.fabric,
                    &self.store,
                    &project,
                    session_id,
                    run_id,
                    "fail",
                    "failed",
                    &err,
                    || async {
                        self.fabric
                            .runs
                            .fail(&project, session_id, run_id, failure_class)
                            .await
                            .map_err(fabric_err_to_runtime)
                    },
                )
                .await
            }
            Err(err) => Err(err),
        }
    }

    async fn cancel(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
    ) -> Result<RunRecord, RuntimeError> {
        let project = resolve_run_project_checking_session(&self.store, run_id, session_id).await?;
        f59_prelude_renew(&self.fabric, &project, session_id, run_id, "cancel").await?;
        let first = self
            .fabric
            .runs
            .cancel(&project, session_id, run_id)
            .await
            .map_err(fabric_err_to_runtime);
        match first {
            Ok(record) => Ok(record),
            Err(err) if err.is_lease_expired() => {
                f64_terminal_recovery_loop(
                    &self.fabric,
                    &self.store,
                    &project,
                    session_id,
                    run_id,
                    "cancel",
                    "cancelled",
                    &err,
                    || async {
                        self.fabric
                            .runs
                            .cancel(&project, session_id, run_id)
                            .await
                            .map_err(fabric_err_to_runtime)
                    },
                )
                .await
            }
            Err(err) => Err(err),
        }
    }

    async fn pause(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
        reason: PauseReason,
    ) -> Result<RunRecord, RuntimeError> {
        let project = resolve_run_project_checking_session(&self.store, run_id, session_id).await?;
        self.fabric
            .runs
            .pause(&project, session_id, run_id, reason)
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn resume(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
        trigger: ResumeTrigger,
        target: RunResumeTarget,
    ) -> Result<RunRecord, RuntimeError> {
        let project = resolve_run_project_checking_session(&self.store, run_id, session_id).await?;
        self.fabric
            .runs
            .resume(&project, session_id, run_id, trigger, target)
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn claim(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
    ) -> Result<RunRecord, RuntimeError> {
        // Active-lease activation for the run's FF execution so the
        // approval-gate / signal-delivery FCALLs accept it downstream.
        // `FabricRunService::claim` handles the
        // ff_issue_claim_grant + ff_claim_execution sequence (and the
        // `use_claim_resumed_execution` dispatch for resumed
        // executions) via `claim_common::issue_grant_and_claim`.
        let project = resolve_run_project_checking_session(&self.store, run_id, session_id).await?;
        self.fabric
            .runs
            .claim(&project, session_id, run_id)
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn ensure_active(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
    ) -> Result<RunRecord, RuntimeError> {
        // F41: idempotent on-ramp used by `POST /v1/runs/:id/orchestrate`.
        // The FabricRunService-side check inspects the FF snapshot once
        // and either short-circuits (already active) or walks the normal
        // grant-and-claim sequence (runnable). Either way, the caller
        // gets a run that is guaranteed to accept terminal FCALLs
        // (`ff_complete_execution`, `ff_fail_execution`,
        // `ff_cancel_execution`).
        let project = resolve_run_project_checking_session(&self.store, run_id, session_id).await?;
        self.fabric
            .runs
            .ensure_active(&project, session_id, run_id)
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn renew_lease_if_stale(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
        min_remaining_ms: u64,
    ) -> Result<RunRecord, RuntimeError> {
        // F51: keep the run's FF lease fresh across orchestrate
        // handler invocations. `FabricRunService::renew_lease_if_stale`
        // snapshot-reads the lease, no-ops if plenty of TTL remains,
        // renews in place if stale, or falls back to a full claim if
        // the lease is already gone.
        let project = resolve_run_project_checking_session(&self.store, run_id, session_id).await?;
        self.fabric
            .runs
            .renew_lease_if_stale(&project, session_id, run_id, min_remaining_ms)
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn enter_waiting_approval(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
    ) -> Result<RunRecord, RuntimeError> {
        let project = resolve_run_project_checking_session(&self.store, run_id, session_id).await?;
        self.fabric
            .runs
            .enter_waiting_approval(&project, session_id, run_id)
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn enter_waiting_subagent(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
        child_task_id: &TaskId,
    ) -> Result<RunRecord, RuntimeError> {
        let project = resolve_run_project_checking_session(&self.store, run_id, session_id).await?;
        self.fabric
            .runs
            .enter_waiting_subagent(&project, session_id, run_id, child_task_id)
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn resolve_approval(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
        decision: ApprovalDecision,
    ) -> Result<RunRecord, RuntimeError> {
        let project = resolve_run_project_checking_session(&self.store, run_id, session_id).await?;
        self.fabric
            .runs
            .resolve_approval(&project, session_id, run_id, decision)
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn list_child_runs(
        &self,
        parent_run_id: &RunId,
        limit: usize,
    ) -> Result<Vec<RunRecord>, RuntimeError> {
        // Parent→child linkage is indexed in the projection (Postgres
        // / SQLite `idx_runs_parent`, InMemory HashMap filter). FF has
        // no native parent-run index (child runs get distinct
        // execution ids via id_map), so the store projection is the
        // authoritative read.
        use cairn_store::projections::RunReadModel;
        Ok(RunReadModel::list_by_parent_run(self.store.as_ref(), parent_run_id, limit).await?)
    }
}

// ── TaskService adapter ──────────────────────────────────────────────────────

/// Adapter routing [`TaskService`] calls to [`FabricServices::tasks`].
pub struct FabricTaskServiceAdapter {
    pub fabric: Arc<FabricServices>,
    pub store: Arc<InMemoryStore>,
}

impl FabricTaskServiceAdapter {
    pub fn new(fabric: Arc<FabricServices>, store: Arc<InMemoryStore>) -> Self {
        Self { fabric, store }
    }
}

// Error type for the bare-ID path: either the projection failed to find the
// record (returns NotFound) or the resolver hit a real store error.
#[allow(dead_code)]
async fn resolve_task_project(
    store: &Arc<InMemoryStore>,
    task_id: &TaskId,
) -> Result<ProjectKey, RuntimeError> {
    resolve_project_from_task_id(store, task_id)
        .await?
        .ok_or_else(|| RuntimeError::NotFound {
            entity: "task",
            id: task_id.to_string(),
        })
}

async fn resolve_session_project(
    store: &Arc<InMemoryStore>,
    session_id: &SessionId,
) -> Result<ProjectKey, RuntimeError> {
    resolve_project_from_session_id(store, session_id)
        .await?
        .ok_or_else(|| RuntimeError::NotFound {
            entity: "session",
            id: session_id.to_string(),
        })
}

/// Fetch a task's projection record.
///
/// Returns `Ok(None)` for unknown task ids (projection-lag race or never
/// created). Callers on the task-mutation path treat `None` as
/// NotFound; the `get` path returns `Ok(None)` to the HTTP layer.
async fn resolve_task_scope(
    store: &Arc<InMemoryStore>,
    task_id: &TaskId,
) -> Result<Option<TaskRecord>, RuntimeError> {
    TaskReadModel::get(store.as_ref(), task_id)
        .await
        .map_err(RuntimeError::from)
}

/// Resolve the task's `(project, session_id_option)` from the
/// projection. `session_id` is derived by following
/// `TaskRecord.parent_run_id → RunRecord.session_id` — cairn does not
/// store session scope directly on the task projection.
///
/// Returns `Err(NotFound)` when the task itself is missing from the
/// projection. When the task has no parent run (bare submission),
/// returns `Ok((project, None))` matching the `task_to_execution_id`
/// (solo) mint path used at submit time.
///
/// When `caller_session_id` is supplied (the adapter threads it through
/// from the trait method), we cross-check that it matches the
/// projection-derived value. Mismatch ⇒ typed Validation error, same
/// contract as `resolve_run_project_checking_session` — no silent
/// fallbacks.
/// Read-only variant: returns `None` when the task is unknown (so the
/// `get` handler can return 404) instead of erroring. Derives
/// `session_id` from the parent run; does not cross-check.
async fn resolve_task_project_and_session_opt(
    store: &Arc<InMemoryStore>,
    task_id: &TaskId,
) -> Result<Option<(ProjectKey, Option<SessionId>)>, RuntimeError> {
    let task = match resolve_task_scope(store, task_id).await? {
        Some(t) => t,
        None => return Ok(None),
    };
    let session = match &task.parent_run_id {
        Some(prid) => RunReadModel::get(store.as_ref(), prid)
            .await?
            .map(|r| r.session_id),
        None => None,
    };
    Ok(Some((task.project, session)))
}

async fn resolve_task_project_and_session(
    store: &Arc<InMemoryStore>,
    task_id: &TaskId,
    caller_session_id: Option<&SessionId>,
) -> Result<(ProjectKey, Option<SessionId>), RuntimeError> {
    let task = resolve_task_scope(store, task_id)
        .await?
        .ok_or_else(|| RuntimeError::NotFound {
            entity: "task",
            id: task_id.to_string(),
        })?;

    // Use the session binding already on the task record when present.
    // Bare tasks carry no binding and route via the solo mint path.
    // If the task row has no session_id, walk parent_run_id → run.session_id.
    let derived_session_id = if let Some(sid) = task.session_id.clone() {
        Some(sid)
    } else {
        match &task.parent_run_id {
            Some(parent_run_id) => {
                let run = RunReadModel::get(store.as_ref(), parent_run_id)
                    .await?
                    .ok_or_else(|| RuntimeError::NotFound {
                        entity: "run",
                        id: parent_run_id.to_string(),
                    })?;
                Some(run.session_id)
            }
            None => None,
        }
    };

    if let Some(caller) = caller_session_id {
        match &derived_session_id {
            Some(derived) if derived.as_str() == caller.as_str() => {}
            Some(derived) => {
                return Err(RuntimeError::Validation {
                    reason: format!(
                        "task {} belongs to session {}, but the request specified {}",
                        task_id.as_str(),
                        derived.as_str(),
                        caller.as_str()
                    ),
                });
            }
            None => {
                return Err(RuntimeError::Validation {
                    reason: format!(
                        "task {} was submitted without a session binding, \
                         but the request specified session {}",
                        task_id.as_str(),
                        caller.as_str()
                    ),
                });
            }
        }
    }

    Ok((task.project, derived_session_id))
}

#[allow(dead_code)]
async fn resolve_run_project(
    store: &Arc<InMemoryStore>,
    run_id: &RunId,
) -> Result<ProjectKey, RuntimeError> {
    resolve_project_from_run_id(store, run_id)
        .await?
        .ok_or_else(|| RuntimeError::NotFound {
            entity: "run",
            id: run_id.to_string(),
        })
}

/// Fetch the run's full projection record (project + session_id).
///
/// Returns `Ok(None)` when the projection has not yet observed this run
/// (create/mutate race) or the run was never created. Surfacing this as
/// `None` lets the read-only `get` path return `Ok(None)` to the caller
/// rather than silently falling back.
async fn resolve_run_scope(
    store: &Arc<InMemoryStore>,
    run_id: &RunId,
) -> Result<Option<RunRecord>, RuntimeError> {
    RunReadModel::get(store.as_ref(), run_id)
        .await
        .map_err(RuntimeError::from)
}

/// Resolve `project` from the projection AND cross-check that the
/// caller's `session_id` matches what the projection holds.
///
/// The FF `ExecutionId` is minted from
/// `(project, session_id, run_id)`; a mismatched `session_id` mints a
/// different ID and the FCALL targets a non-existent execution — which
/// FF reports as a generic not-found and the operator sees as an
/// unexplained 404. Per the "no silent fallbacks" rule, we fail loud
/// here with a typed Validation error instead.
///
/// Returns `NotFound` when the projection hasn't observed the run yet
/// (projection-lag race on a very recently started run) — the operator
/// should retry after the projection catches up.
async fn resolve_run_project_checking_session(
    store: &Arc<InMemoryStore>,
    run_id: &RunId,
    session_id: &SessionId,
) -> Result<ProjectKey, RuntimeError> {
    let record = RunReadModel::get(store.as_ref(), run_id)
        .await?
        .ok_or_else(|| RuntimeError::NotFound {
            entity: "run",
            id: run_id.to_string(),
        })?;
    if record.session_id.as_str() != session_id.as_str() {
        return Err(RuntimeError::Validation {
            reason: format!(
                "run {} is bound to session {}, but the request specified {}",
                run_id.as_str(),
                record.session_id.as_str(),
                session_id.as_str()
            ),
        });
    }
    Ok(record.project)
}

/// Projection-backed runs-by-session lookup, extracted so unit tests can
/// exercise the `list_by_session` path without constructing a Valkey-backed
/// `FabricServices`.
async fn list_runs_by_session_from_projection(
    store: &Arc<InMemoryStore>,
    session_id: &SessionId,
    limit: usize,
    offset: usize,
) -> Result<Vec<RunRecord>, RuntimeError> {
    RunReadModel::list_by_session(store.as_ref(), session_id, limit, offset)
        .await
        .map_err(RuntimeError::from)
}

#[async_trait]
impl TaskService for FabricTaskServiceAdapter {
    async fn submit(
        &self,
        project: &ProjectKey,
        session_id: Option<&SessionId>,
        task_id: TaskId,
        parent_run_id: Option<RunId>,
        parent_task_id: Option<TaskId>,
        priority: u32,
    ) -> Result<TaskRecord, RuntimeError> {
        // session_id is supplied by the caller (None for bare tasks).
        // If caller omitted it but the task has a parent run
        // already in the projection, we could derive it — but at submit
        // time the parent run's session is the authoritative source, so
        // we fall back to the parent_run_id lookup.
        let resolved_session = match session_id {
            Some(sid) => Some(sid.clone()),
            None => match &parent_run_id {
                Some(prid) => {
                    // A task with a parent run must resolve its session.
                    // Silently returning None would route to the solo-mint path
                    // and land on a different Valkey partition than the parent run.
                    let run = RunReadModel::get(self.store.as_ref(), prid)
                        .await?
                        .ok_or_else(|| RuntimeError::NotFound {
                            entity: "run",
                            id: prid.to_string(),
                        })?;
                    Some(run.session_id)
                }
                None => None,
            },
        };
        self.fabric
            .tasks
            .submit(
                project,
                task_id,
                parent_run_id,
                parent_task_id,
                priority,
                resolved_session.as_ref(),
            )
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn declare_dependency(
        &self,
        dependent_task_id: &TaskId,
        prerequisite_task_id: &TaskId,
        dependency_kind: cairn_domain::DependencyKind,
        data_passing_ref: Option<String>,
    ) -> Result<TaskDependencyRecord, RuntimeError> {
        // Resolve project + session for both tasks from the
        // projection; FF flow edges can only connect members of the
        // same flow. Reject cross-session / bare-task declares here
        // with a Validation error rather than letting FF surface a
        // less-useful opaque FCALL error.
        let (dep_project, dep_session) =
            resolve_task_project_and_session(&self.store, dependent_task_id, None).await?;
        let (pre_project, pre_session) =
            resolve_task_project_and_session(&self.store, prerequisite_task_id, None).await?;

        if dep_project != pre_project {
            return Err(RuntimeError::Validation {
                reason: format!(
                    "task dependencies must share a project: {} → {} cross project boundary",
                    dependent_task_id.as_str(),
                    prerequisite_task_id.as_str()
                ),
            });
        }

        let session_id = match (&dep_session, &pre_session) {
            (Some(a), Some(b)) if a == b => a.clone(),
            (Some(_), Some(_)) => {
                return Err(RuntimeError::Validation {
                    reason: format!(
                        "task dependencies must share a session; {} and {} \
                         belong to different sessions",
                        dependent_task_id.as_str(),
                        prerequisite_task_id.as_str()
                    ),
                });
            }
            _ => {
                return Err(RuntimeError::Validation {
                    reason: format!(
                        "task dependencies require both tasks to be session-\
                         bound; {} or {} was submitted without a session",
                        dependent_task_id.as_str(),
                        prerequisite_task_id.as_str()
                    ),
                });
            }
        };

        self.fabric
            .tasks
            .declare_dependency(
                &dep_project,
                &session_id,
                dependent_task_id,
                prerequisite_task_id,
                dependency_kind,
                data_passing_ref.as_deref(),
            )
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn check_dependencies(
        &self,
        task_id: &TaskId,
    ) -> Result<Vec<TaskDependencyRecord>, RuntimeError> {
        // Bare tasks (no session) can't have dependencies — there's
        // no flow for them to live in. Return empty rather than
        // surfacing a less-useful error.
        let (project, session_id) =
            resolve_task_project_and_session(&self.store, task_id, None).await?;
        let Some(sid) = session_id else {
            return Ok(Vec::new());
        };
        self.fabric
            .tasks
            .check_dependencies(&project, &sid, task_id)
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn get(&self, task_id: &TaskId) -> Result<Option<TaskRecord>, RuntimeError> {
        // Read-only: derive (project, session_id) from projection; None
        // caller_session means no cross-check.
        let (project, session) =
            match resolve_task_project_and_session_opt(&self.store, task_id).await? {
                Some(v) => v,
                None => return Ok(None),
            };
        self.fabric
            .tasks
            .get(&project, session.as_ref(), task_id)
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn claim(
        &self,
        session_id: Option<&SessionId>,
        task_id: &TaskId,
        lease_owner: String,
        lease_duration_ms: u64,
    ) -> Result<TaskRecord, RuntimeError> {
        let (project, session) =
            resolve_task_project_and_session(&self.store, task_id, session_id).await?;
        self.fabric
            .tasks
            .claim(
                &project,
                session.as_ref(),
                task_id,
                lease_owner,
                lease_duration_ms,
            )
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn heartbeat(
        &self,
        session_id: Option<&SessionId>,
        task_id: &TaskId,
        lease_extension_ms: u64,
    ) -> Result<TaskRecord, RuntimeError> {
        let (project, session) =
            resolve_task_project_and_session(&self.store, task_id, session_id).await?;
        self.fabric
            .tasks
            .heartbeat(&project, session.as_ref(), task_id, lease_extension_ms)
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn start(
        &self,
        session_id: Option<&SessionId>,
        task_id: &TaskId,
    ) -> Result<TaskRecord, RuntimeError> {
        let (project, session) =
            resolve_task_project_and_session(&self.store, task_id, session_id).await?;
        self.fabric
            .tasks
            .start(&project, session.as_ref(), task_id)
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn complete(
        &self,
        session_id: Option<&SessionId>,
        task_id: &TaskId,
    ) -> Result<TaskRecord, RuntimeError> {
        let (project, session) =
            resolve_task_project_and_session(&self.store, task_id, session_id).await?;
        self.fabric
            .tasks
            .complete(&project, session.as_ref(), task_id)
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn fail(
        &self,
        session_id: Option<&SessionId>,
        task_id: &TaskId,
        failure_class: FailureClass,
    ) -> Result<TaskRecord, RuntimeError> {
        let (project, session) =
            resolve_task_project_and_session(&self.store, task_id, session_id).await?;
        self.fabric
            .tasks
            .fail(&project, session.as_ref(), task_id, failure_class)
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn cancel(
        &self,
        session_id: Option<&SessionId>,
        task_id: &TaskId,
    ) -> Result<TaskRecord, RuntimeError> {
        let (project, session) =
            resolve_task_project_and_session(&self.store, task_id, session_id).await?;
        self.fabric
            .tasks
            .cancel(&project, session.as_ref(), task_id)
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn dead_letter(
        &self,
        session_id: Option<&SessionId>,
        task_id: &TaskId,
    ) -> Result<TaskRecord, RuntimeError> {
        let (project, session) =
            resolve_task_project_and_session(&self.store, task_id, session_id).await?;
        self.fabric
            .tasks
            .dead_letter(&project, session.as_ref(), task_id)
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn list_dead_lettered(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<TaskRecord>, RuntimeError> {
        // Projection path: FF's terminal_failed set is not indexed by cairn
        // scope. The handler-wiring map explicitly routes list_* queries to
        // the store projection to preserve the cairn scope filter.
        TaskReadModel::list_by_state(self.store.as_ref(), project, TaskState::DeadLettered, limit)
            .await
            .map(|mut v| {
                if offset >= v.len() {
                    Vec::new()
                } else {
                    v.drain(offset..).collect()
                }
            })
            .map_err(RuntimeError::from)
    }

    async fn pause(
        &self,
        session_id: Option<&SessionId>,
        task_id: &TaskId,
        reason: PauseReason,
    ) -> Result<TaskRecord, RuntimeError> {
        let (project, session) =
            resolve_task_project_and_session(&self.store, task_id, session_id).await?;
        self.fabric
            .tasks
            .pause(&project, session.as_ref(), task_id, reason)
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn resume(
        &self,
        session_id: Option<&SessionId>,
        task_id: &TaskId,
        trigger: ResumeTrigger,
        target: TaskResumeTarget,
    ) -> Result<TaskRecord, RuntimeError> {
        let (project, session) =
            resolve_task_project_and_session(&self.store, task_id, session_id).await?;
        self.fabric
            .tasks
            .resume(&project, session.as_ref(), task_id, trigger, target)
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn list_by_state(
        &self,
        project: &ProjectKey,
        state: TaskState,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, RuntimeError> {
        // Projection path — same rationale as list_dead_lettered.
        TaskReadModel::list_by_state(self.store.as_ref(), project, state, limit)
            .await
            .map_err(RuntimeError::from)
    }

    async fn list_expired_leases(
        &self,
        now: u64,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, RuntimeError> {
        // Projection path. FF has its own lease-expiry scanner running
        // in-process; surfacing expired leases to cairn handlers is a
        // read-only query on the event log projection.
        TaskReadModel::list_expired_leases(self.store.as_ref(), now, limit)
            .await
            .map_err(RuntimeError::from)
    }

    async fn release_lease(
        &self,
        session_id: Option<&SessionId>,
        task_id: &TaskId,
    ) -> Result<TaskRecord, RuntimeError> {
        let (project, session) =
            resolve_task_project_and_session(&self.store, task_id, session_id).await?;
        self.fabric
            .tasks
            .release_lease(&project, session.as_ref(), task_id)
            .await
            .map_err(fabric_err_to_runtime)
    }

    /// Issue #670 G1+G2+G3 override: create a child `RunRecord`,
    /// submit the child task, and emit `RuntimeEvent::SubagentSpawned`
    /// carrying the LLM's delegation context (goal + role) verbatim.
    ///
    /// G1 (shipped #671): emit `RuntimeEvent::SubagentSpawned` so the
    /// `subagent_spawns` projection captures the parent→child linkage.
    /// G2 (shipped #671): carry `goal` + `role` through verbatim from
    /// the LLM's `ActionProposal`.
    /// **G3 (this PR):** create a real child `RunRecord` at spawn time
    /// — previously `child_run_id` was always `None` and the child
    /// showed up nowhere in the UI. After G3, `GET /v1/runs/:id/children`
    /// returns the child row, `RunReadModel::list_by_parent_run` resolves,
    /// and the `subagent_spawns.child_run_id` column is populated.
    ///
    /// Phase ordering (all via the same `EventBridge`, FIFO):
    /// 1. `fabric.runs.start` → emits `BridgeEvent::ExecutionCreated`
    ///    → `RunCreated` on the cairn-domain side. Creates the row the
    ///    rest of this method will reference.
    /// 2. `fabric.tasks.submit` → emits `BridgeEvent::TaskCreated`.
    /// 3. `fabric.bridge.emit(SubagentSpawned)` → writes the audit row
    ///    with `child_run_id = Some(child_run_id)` so the row points
    ///    at the real child run created in Phase 1.
    ///
    /// Failure modes: each phase propagates its error and short-
    /// circuits the downstream ones. If the run creation in Phase 1
    /// succeeds but the task submit in Phase 2 fails, the child
    /// `RunRecord` is orphaned — a follow-up increment (G5) wires
    /// recovery on orphan child runs. Acceptable for G3 because the
    /// caller returns an execute-layer `Failed` that the orchestrator
    /// surfaces to the operator.
    async fn spawn_subagent(
        &self,
        parent_run_id: RunId,
        parent_task_id: Option<TaskId>,
        child_task_id: TaskId,
        child_session_id: SessionId,
        child_run_id: Option<RunId>,
        goal: String,
        role: String,
        parent_context: Option<String>,
        reuse_sandbox_from: Option<RunId>,
    ) -> Result<TaskRecord, RuntimeError> {
        // #670 G4 PR-1a cross-tenant contract: derive the child's
        // project from the parent run row. No caller-supplied project
        // argument exists; the signature prevents tenancy override.
        //
        // StoreError flows through `?` directly so the structured
        // `RuntimeError::Store` classification is preserved — per
        // SEC-007, we don't `to_string()` the raw driver text into
        // `RuntimeError::Internal` (which would leak constraint
        // names / schema fragments into the public error surface).
        let parent = RunReadModel::get(self.store.as_ref(), &parent_run_id)
            .await?
            .ok_or_else(|| RuntimeError::NotFound {
                entity: "run",
                id: parent_run_id.as_str().to_owned(),
            })?;
        let parent_project = parent.project.clone();
        let parent_session_id = parent.session_id.clone();
        let parent_tenant = parent_project.tenant_id.clone();

        // #844 PR-2: validate `reuse_sandbox_from`. The explicit opt-in
        // escape hatch the parent LLM uses to let a replacement sub-
        // agent continue from a dead sibling's on-disk work. Must
        // reference a **sibling under the same root** and the same
        // project — any other value is rejected at this boundary so
        // the LLM cannot point the child at an unrelated run's sandbox
        // (cross-root / cross-project reads), and there is no silent
        // fallback to a fresh sandbox on a bad id (the LLM would keep
        // emitting the wrong id without feedback). Validation-class
        // errors surface into `execute_impl`'s `ActionStatus::Failed`
        // reason → step_history → the next DECIDE sees the rejection.
        if let Some(reuse_id) = reuse_sandbox_from.as_ref() {
            // Parent's root is its own id when parent IS the root
            // (parent.root_run_id == None pre-V069, or == parent_run_id
            // when projected). Siblings under the same root must match
            // this anchor.
            let parent_root = parent
                .root_run_id
                .clone()
                .unwrap_or_else(|| parent_run_id.clone());

            let reuse_run = RunReadModel::get(self.store.as_ref(), reuse_id)
                .await?
                .ok_or_else(|| RuntimeError::Validation {
                    reason: format!(
                        "spawn_subagent.reuse_sandbox_from={reuse}: referenced \
                         run does not exist — pass the run_id of a prior \
                         sibling under the same root, or omit the field for \
                         a fresh sandbox",
                        reuse = reuse_id.as_str(),
                    ),
                })?;

            let reuse_root = reuse_run
                .root_run_id
                .clone()
                .unwrap_or_else(|| reuse_run.run_id.clone());

            if reuse_root != parent_root {
                return Err(RuntimeError::Validation {
                    reason: format!(
                        "spawn_subagent.reuse_sandbox_from={reuse}: not a sibling \
                         under the parent's root (referenced run's root={reuse_root}, \
                         parent's root={parent_root}). Only prior siblings under the \
                         same root may share a sandbox — cross-root sandbox sharing \
                         leaks work across unrelated goals.",
                        reuse = reuse_id.as_str(),
                        reuse_root = reuse_root.as_str(),
                        parent_root = parent_root.as_str(),
                    ),
                });
            }

            if reuse_run.project != parent_project {
                return Err(RuntimeError::Validation {
                    reason: format!(
                        "spawn_subagent.reuse_sandbox_from={reuse}: project \
                         mismatch (referenced run's project differs from the \
                         parent's). Sandbox sharing across projects is rejected.",
                        reuse = reuse_id.as_str(),
                    ),
                });
            }
        }

        // G3: child_run_id should be Some(id) on the LLM-initiated
        // path (execute_impl mints one). If the caller passed None
        // (test fakes, pre-G3 callers), fall back to the convention
        // `subagent_<parent>` so we always create a concrete child
        // RunRecord. Matches the default impl of
        // `RunService::spawn_subagent` behaviour.
        let child_run_id =
            child_run_id.unwrap_or_else(|| RunId::new_subagent_for_parent(&parent_run_id));

        // Phase 1: create the child RunRecord with parent linkage via
        // the real fabric path. This emits `BridgeEvent::ExecutionCreated`
        // which the bridge translates to `RuntimeEvent::RunCreated`,
        // threading `parent_run_id` + (#670 G6) `agent_role_id` onto
        // the domain event so operator queries like
        // `RunReadModel::list_by_parent_run` see the child and the
        // child's orchestrator loop picks the delegated role's
        // system prompt on its first iteration. The projection
        // (PR-1b-3 change) inherits the parent's `root_run_id` onto
        // the child's row inside the same INSERT.
        //
        // Role plumbing (G6): `role` is the `tool_name` field of the
        // LLM's `spawn_subagent` `ActionProposal` (e.g. `"researcher"`,
        // `"executor"`). Empty string treated as "no role" — the
        // child's orchestrator loop falls back to `"orchestrator"`
        // via `orchestrate_run_handler_inner`'s default. The fabric
        // layer does not validate the id; unknown ids surface
        // observably at prompt-selection time (the orchestrator's
        // prompt picker falls through to the default prompt).
        let child_role = (!role.is_empty()).then(|| role.clone());
        self.fabric
            .runs
            .start_with_role(
                &parent_project,
                &child_session_id,
                child_run_id.clone(),
                Some(parent_run_id.clone()),
                child_role,
            )
            .await
            .map_err(fabric_err_to_runtime)?;

        // #700: persist the spawn goal onto the child run's defaults so
        // `ChildRunDriver` + F49 auto-resume both read the real goal
        // through `resolve_run_string_default(... "goal")` instead of
        // falling through to the `"Execute the run objective."`
        // placeholder.
        //
        // The HTTP `/orchestrate` path already does this write when an
        // operator POSTs with `{ "goal": "..." }` (see
        // `handlers/runs/orchestrate.rs` — #651). A subagent has no
        // prior operator POST; the spawn adapter is the only place
        // that knows both the goal and the child run id, so the write
        // must land here.
        //
        // Key format `run:<child_run_id>:goal` matches
        // `helpers::run_default_key(run_id, "goal")` so the HTTP
        // resolver finds it on the next orchestrate call.
        //
        // Best-effort: a defaults write failure logs at WARN and the
        // spawn still succeeds. The child's first iteration would
        // then see the fallback goal — no worse than pre-#700 and
        // the warn is operator-visible.
        {
            use cairn_runtime::services::DefaultsServiceImpl;
            use cairn_runtime::DefaultsService;
            let defaults = DefaultsServiceImpl::new(self.store.clone());
            let key = format!("run:{}:goal", child_run_id.as_str());
            if let Err(err) = defaults
                .set(
                    cairn_domain::tenancy::Scope::Project,
                    parent_project.project_id.to_string(),
                    key,
                    serde_json::Value::String(goal.clone()),
                )
                .await
            {
                tracing::warn!(
                    error = %err,
                    parent_run_id = %parent_run_id,
                    child_run_id = %child_run_id,
                    "#700: failed to persist child run goal default; child's \
                     first iteration will fall back to `Execute the run objective.` \
                     placeholder and the subagent may produce empty output",
                );
            }

            // #775: same persistence pattern for parent_context. Stored
            // under `run:<child_run_id>:parent_context`; resolved on
            // every child orchestrate iteration via
            // `resolve_run_string_default(... "parent_context")` and
            // populated into `OrchestrationContext.parent_context` so
            // the `## Parent context` section renders in the child's
            // user message. Skipped when the parent did not provide a
            // context — None means no row, no fallback, no rendered
            // section.
            if let Some(ref pc) = parent_context {
                let pc_key = format!("run:{}:parent_context", child_run_id.as_str());
                if let Err(err) = defaults
                    .set(
                        cairn_domain::tenancy::Scope::Project,
                        parent_project.project_id.to_string(),
                        pc_key,
                        serde_json::Value::String(pc.clone()),
                    )
                    .await
                {
                    tracing::warn!(
                        error = %err,
                        parent_run_id = %parent_run_id,
                        child_run_id = %child_run_id,
                        "#775: failed to persist child run parent_context default; \
                         child's first iteration will not see the `## Parent context` \
                         section and the parent's binding direction is silently lost",
                    );
                }
            }

            // #844 PR-2: persist the opt-in `reuse_sandbox_from` id on
            // the child's defaults. Read back by the orchestrate
            // handler via `resolve_run_string_default(..., "reuse_sandbox_from")`
            // before calling `working_dir_for_run` — when set, the
            // child's working_dir resolves to the referenced run's
            // sandbox instead of a fresh one. Validated above against
            // the authoritative projection, so the row written here is
            // known-good at write time. `None` → no row, no default,
            // fresh sandbox (today's behaviour, byte-identical).
            if let Some(ref reuse_id) = reuse_sandbox_from {
                let reuse_key = format!("run:{}:reuse_sandbox_from", child_run_id.as_str());
                if let Err(err) = defaults
                    .set(
                        cairn_domain::tenancy::Scope::Project,
                        parent_project.project_id.to_string(),
                        reuse_key,
                        serde_json::Value::String(reuse_id.as_str().to_owned()),
                    )
                    .await
                {
                    tracing::warn!(
                        error = %err,
                        parent_run_id = %parent_run_id,
                        child_run_id = %child_run_id,
                        reuse_sandbox_from = %reuse_id,
                        "#844 PR-2: failed to persist child run reuse_sandbox_from \
                         default; child's working_dir will resolve to a fresh \
                         sandbox instead of the requested prior sibling's — \
                         partial on-disk work from the predecessor will not be \
                         visible to the child",
                    );
                }
            }
        }

        // #670 G4 / RFC 027 §79-99: descendant-counter fan-out gate.
        // After Phase-1 lands the child row (with its `root_run_id`
        // inherited from parent), we issue the atomic compare-and-
        // increment on the ROOT row. Per RFC 027 §89 the durable
        // backend's `UPDATE ... WHERE counter < :cap RETURNING` is
        // the authoritative arbiter; two concurrent spawns against
        // the same root cannot both admit above the cap.
        //
        // Resolving the root: prefer the parent's own `root_run_id`
        // (set by the projection during the parent's own spawn or
        // by the V069 backfill for a legacy root). If the parent
        // has `None` here — meaning it was created pre-PR-1b-3 or
        // the backfill hasn't reached this chain — fall back to
        // charging the PARENT itself as the root. That's correct
        // for the common case where the parent IS the root; for a
        // legacy-chain mid-depth case the cap still gates the
        // subtree, just at the nearest ancestor we can identify
        // without a O(depth) chain walk on the hot path.
        let root_for_cap = parent
            .root_run_id
            .clone()
            .unwrap_or_else(|| parent_run_id.clone());
        let cap = spawn_cap();
        match self
            .store
            .try_increment_descendants(&root_for_cap, cap)
            .await?
        {
            DescendantsCapOutcome::Admitted { .. } => {
                // Counter slot reserved. Proceed with Phase-2.
            }
            DescendantsCapOutcome::CapReached => {
                // Roll back Phase-1 — cancel the child run we just
                // created so the Pending row doesn't leak. The
                // projection emits `RunStateChanged → Canceled`,
                // which has `parent_run_id.is_some()` and a
                // `root_run_id` (just inherited), so the projection's
                // terminal decrement fires against the root. BUT we
                // did NOT increment (we're here because the increment
                // rejected), so that decrement will produce an
                // auditable underflow of -1 — the RFC-027 §93 signal
                // operators see on
                // `child_run_driver_descendant_underflow_total`.
                //
                // Acceptable trade-off: guaranteed row cleanup beats
                // a clean counter. A truly clean rollback would
                // require event-schema surgery (a RunCreated variant
                // that doesn't inherit the root, or a direct DELETE
                // on the projection that bypasses the event log —
                // both larger than this RFC's scope).
                //
                // Gemini #678: if `runs.cancel` fails here, the child
                // run leaks in Pending. Once PR-1b-5 wires the driver
                // claim path, a leaked Pending child with a
                // parent_run_id would be picked up on the next tick
                // and executed — bypassing the quota. Log ERROR so
                // operators see the leak; the run's state is the
                // authoritative signal they'd grep for.
                if let Err(err) = self
                    .fabric
                    .runs
                    .cancel(&parent_project, &child_session_id, &child_run_id)
                    .await
                {
                    tracing::error!(
                        error = %err,
                        parent_run_id = %parent_run_id,
                        child_run_id = %child_run_id,
                        root_for_cap = %root_for_cap,
                        "RFC-027 §89: cap-rollback cancel failed — child \
                         run leaks in Pending. Once the ChildRunDriver \
                         claim path (PR-1b-5) is enabled, this row will \
                         be executed and the quota bypassed. Operator \
                         recovery: POST \
                         /v1/admin/tenants/:tenant/runs/:id/cancel-orphan",
                    );
                }
                return Err(RuntimeError::QuotaExceeded {
                    tenant_id: parent_tenant.to_string(),
                    quota_type: "concurrent_descendants".to_owned(),
                    current: cap as u32,
                    limit: cap as u32,
                });
            }
            DescendantsCapOutcome::RootNotFound => {
                // Root row missing. Shouldn't happen (we just read
                // the parent and it points at a real root), but if
                // it does, surface as internal — not something the
                // operator can fix. Roll back Phase-1 anyway; if
                // that fails, log (Gemini #678) — a leaked Pending
                // child with a parent_run_id would be picked up by
                // the driver once PR-1b-5 enables it.
                if let Err(err) = self
                    .fabric
                    .runs
                    .cancel(&parent_project, &child_session_id, &child_run_id)
                    .await
                {
                    tracing::error!(
                        error = %err,
                        parent_run_id = %parent_run_id,
                        child_run_id = %child_run_id,
                        root_for_cap = %root_for_cap,
                        "RFC-027: RootNotFound rollback cancel failed — \
                         child run leaks in Pending. Operator recovery: \
                         POST /v1/admin/tenants/:tenant/runs/:id/cancel-orphan",
                    );
                }
                return Err(RuntimeError::Internal(format!(
                    "descendant counter root row not found: {root_for_cap}",
                )));
            }
        }

        // Phase 2: submit the child task via the real fabric path.
        // This also emits `BridgeEvent::TaskCreated`, so the child
        // row lands on the `tasks` projection before `SubagentSpawned`
        // tries to patch its parent linkage.
        let record = match self
            .fabric
            .tasks
            .submit(
                &parent_project,
                child_task_id.clone(),
                Some(parent_run_id.clone()),
                parent_task_id.clone(),
                /* priority */ 0,
                Some(&child_session_id),
            )
            .await
            .map_err(fabric_err_to_runtime)
        {
            Ok(record) => record,
            Err(phase2_err) => {
                // RFC 027 §107: Phase-2 failed after Phase-1 landed
                // and the counter was incremented. Synchronously
                // fail the child as `OrphanChild`; the `Failed`
                // terminal fires the projection's decrement path
                // against the root, releasing the cap slot in the
                // same write. We do NOT re-attempt Phase-2 — the
                // orphan state is the correct terminal for a child
                // whose task row never materialised.
                if let Err(fail_err) = self
                    .fabric
                    .runs
                    .fail(
                        &parent_project,
                        &child_session_id,
                        &child_run_id,
                        FailureClass::OrphanChild,
                    )
                    .await
                {
                    // RFC 027 §109-111: the compensating fail itself
                    // failed. Log ERROR and fire a compensating
                    // direct-decrement so the counter slot is still
                    // released even without the `Failed` terminal
                    // event firing. Counter drift under compounded
                    // failure is the tolerated worst-case (§111).
                    tracing::error!(
                        error = %fail_err,
                        parent_run_id = %parent_run_id,
                        child_run_id = %child_run_id,
                        root_for_cap = %root_for_cap,
                        "RFC-027 §109: compensating fail(OrphanChild) failed after \
                         Phase-2 error; issuing direct decrement on descendant counter \
                         (child_run_driver_orphan_fail_failed_total)",
                    );
                    if let Err(dec_err) = self.store.decrement_descendants(&root_for_cap).await {
                        tracing::error!(
                            error = %dec_err,
                            root_for_cap = %root_for_cap,
                            "RFC-027 §111: compensating decrement failed after fail() \
                             also failed — counter leak. Operators see this on \
                             child_run_driver_orphan_counter_leak_total; the underflow-\
                             on-next-legit-decrement path is tolerated per §93.",
                        );
                    }
                }
                return Err(phase2_err);
            }
        };

        // #670 G5: suspend the parent on `child_completed:<child_task_id>`
        // BEFORE emitting the `SubagentSpawned` fact. Closes the
        // signal-before-suspend race — by the time this method returns
        // and execute_impl yields `ActionStatus::SubagentSpawned`, the
        // parent's FF execution is already suspended on the waitpoint
        // that the child's terminal will signal against.
        //
        // Best-effort: a suspend failure logs WARN but does NOT block
        // the spawn (the operator can still manually re-POST
        // /orchestrate as the pre-G5 fallback). The spawn itself
        // already committed Phase-1/2 side effects; tearing those down
        // on a suspend failure would leak more than it prevents.
        if let Err(err) = self
            .fabric
            .runs
            .enter_waiting_subagent(
                &parent_project,
                &parent_session_id,
                &parent_run_id,
                &child_task_id,
            )
            .await
        {
            tracing::warn!(
                error = %err,
                parent_run_id = %parent_run_id,
                child_task_id = %child_task_id,
                "G5: enter_waiting_subagent failed; parent stays Running. \
                 Auto-resume will not fire on child completion; operator \
                 must manually re-POST /v1/runs/:id/orchestrate.",
            );
        }

        // Phase 3: emit the spawn fact. Carries the real child_run_id
        // now (G3); was always None in G1+G2.
        self.fabric
            .bridge
            .emit(BridgeEvent::SubagentSpawned {
                parent_run_id,
                parent_task_id,
                child_task_id,
                child_session_id,
                child_run_id: Some(child_run_id),
                project: parent_project,
                goal,
                role,
                // #775: parent_context threading lands in step 3
                // (spawn_subagent trait+impl signature change). For
                // now keep parity with pre-#775 callers — None means
                // "no parent context provided", which is the same
                // default any pre-existing caller would have seen.
                parent_context,
            })
            .await;

        Ok(record)
    }
}

/// #670 G4 / RFC 027 §99: per-root cap on concurrent descendants.
/// Default 16, clamp [1, 256]. Override via
/// `CAIRN_MAX_CONCURRENT_DESCENDANTS`.
fn spawn_cap() -> i64 {
    const DEFAULT: i64 = 16;
    const MIN: i64 = 1;
    const MAX: i64 = 256;
    std::env::var("CAIRN_MAX_CONCURRENT_DESCENDANTS")
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .map(|n| n.clamp(MIN, MAX))
        .unwrap_or(DEFAULT)
}

// ── SessionService adapter ───────────────────────────────────────────────────

/// Adapter routing [`SessionService`] calls to [`FabricServices::sessions`].
pub struct FabricSessionServiceAdapter {
    pub fabric: Arc<FabricServices>,
    pub store: Arc<InMemoryStore>,
}

impl FabricSessionServiceAdapter {
    pub fn new(fabric: Arc<FabricServices>, store: Arc<InMemoryStore>) -> Self {
        Self { fabric, store }
    }
}

#[async_trait]
impl SessionService for FabricSessionServiceAdapter {
    async fn create(
        &self,
        project: &ProjectKey,
        session_id: SessionId,
    ) -> Result<SessionRecord, RuntimeError> {
        let record = self
            .fabric
            .sessions
            .create(project, session_id.clone())
            .await
            .map_err(fabric_err_to_runtime)?;

        // Read-after-write barrier: wait for the `SessionCreated` bridge
        // event to be drained by the `EventBridge` consumer and applied
        // to the `InMemoryStore` session projection before returning.
        //
        // Why this matters — task #178 regression: the bridge is an
        // async mpsc pipeline (see `cairn-fabric::event_bridge`). A
        // caller that creates a session and immediately creates a run
        // (e.g. `POST /v1/sessions` → `POST /v1/runs`) can outrun the
        // consumer: `FabricSessionService::create` returns as soon as
        // the FF `ff_create_flow` FCALL + `cairn.*` HSETs commit; the
        // `BridgeEvent::SessionCreated` is still queued in the
        // consumer channel. The immediately-following run handler then
        // calls `state.runtime.sessions.get(&session_id)`, which goes
        // through this adapter's `get()` → `resolve_project_from_session_id`
        // → `SessionReadModel::get` on the InMemoryStore and finds
        // `None`, so the run handler returns 404 "session not found"
        // even though the session's FF flow exists and the caller just
        // received a 201 for it. That failure mode broke the three
        // RFC 020 integration tests (#1, #6, #11) on `origin/main`
        // the moment the shared Valkey test container got busy enough
        // to serialise consumer wake-ups behind other writers.
        //
        // Strategy: bounded poll with exponential-ish backoff. 2s budget
        // matches the wider-than-expected tail observed on CI under
        // concurrent test load; the typical path resolves in the first
        // 1ms poll. Timing out is strictly better than returning
        // without the projection — the alternative is a silent race
        // that only surfaces as a 404 on an immediately-following
        // lookup.
        wait_for_session_projection(&self.store, &session_id).await?;
        Ok(record)
    }

    async fn get(
        &self,
        project: &ProjectKey,
        session_id: &SessionId,
    ) -> Result<Option<SessionRecord>, RuntimeError> {
        // Scope check at the service layer (issue #439): previously this
        // adapter resolved the session's project via the store and
        // trusted the caller to tenant-match after the fetch. That left
        // every new handler one forgotten comparison away from a
        // #185-shape cross-tenant leak. Now the caller must assert the
        // expected project up front; if the stored row's scope does not
        // match, we return `None` (indistinguishable from "unknown id"
        // on purpose — leaking the distinction reveals ids that exist
        // in other tenants).
        let stored_project = match resolve_project_from_session_id(&self.store, session_id).await? {
            Some(p) => p,
            None => return Ok(None),
        };
        if stored_project != *project {
            return Ok(None);
        }
        self.fabric
            .sessions
            .get(project, session_id)
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn lookup_any_admin(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<SessionRecord>, RuntimeError> {
        // Admin-only cross-tenant lookup (issue #439). The caller is
        // responsible for guarding this with an AdminRoleGuard /
        // TenantScope::is_admin check — the service layer cannot
        // authenticate; it only owns the shape of "return the record
        // regardless of project."
        let project = match resolve_project_from_session_id(&self.store, session_id).await? {
            Some(p) => p,
            None => return Ok(None),
        };
        self.fabric
            .sessions
            .get(&project, session_id)
            .await
            .map_err(fabric_err_to_runtime)
    }

    async fn list(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<SessionRecord>, RuntimeError> {
        // Projection path: FF flows are partitioned by flow_id, not indexed
        // by project. cairn-store's SessionReadModel lists by project from
        // the event log — the only source with the cairn scope view.
        SessionReadModel::list_by_project(self.store.as_ref(), project, limit, offset)
            .await
            .map_err(RuntimeError::from)
    }

    async fn archive(
        &self,
        project: &ProjectKey,
        session_id: &SessionId,
    ) -> Result<SessionRecord, RuntimeError> {
        // Scope check at the service layer (issue #439): mirror the
        // `get` guard — an archive call with a mismatched project is
        // treated as "session not found" rather than routing the
        // tenant-A id through to the fabric under tenant-B's scope.
        let stored_project = resolve_session_project(&self.store, session_id).await?;
        if stored_project != *project {
            return Err(RuntimeError::NotFound {
                entity: "session",
                id: session_id.as_str().to_owned(),
            });
        }
        self.fabric
            .sessions
            .archive(project, session_id)
            .await
            .map_err(fabric_err_to_runtime)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_domain::{
        EventEnvelope, EventId, EventSource, RunCreated, RuntimeEvent, SessionCreated, TaskCreated,
    };
    use cairn_store::event_log::EventLog;

    /// The adapter types should be `Send + Sync` so they can live inside
    /// `Arc<dyn RunService>` / `Arc<dyn TaskService>` / `Arc<dyn SessionService>`
    /// alongside the existing `*ServiceImpl` variants.
    #[test]
    fn adapters_are_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FabricRunServiceAdapter>();
        assert_send_sync::<FabricTaskServiceAdapter>();
        assert_send_sync::<FabricSessionServiceAdapter>();
    }

    /// SEC-007: Valkey error messages (FCALL names, key names, occasionally
    /// secret-hash references) MUST NOT flow into the 500 response body.
    /// The adapter's mapper genericizes the Internal variant to a fixed
    /// string; operators still see the detail in `tracing::error!`. Any
    /// regression that surfaces `other.to_string()` to the caller fires
    /// this test.
    #[test]
    fn fabric_err_valkey_detail_does_not_leak_to_runtime_internal() {
        let leaky = FabricError::Valkey(
            "HGET waitpoint_hmac_secrets:{p:7} secret:abc123 — \
             connection refused at 10.0.0.1:6379"
                .into(),
        );
        match fabric_err_to_runtime(leaky) {
            RuntimeError::Internal(msg) => {
                assert_eq!(
                    msg, "fabric layer error",
                    "Internal message must be opaque; got leaky detail: {msg:?}"
                );
                assert!(!msg.contains("secret:"), "secret hash field leaked");
                assert!(!msg.contains("HGET"), "FCALL detail leaked");
                assert!(!msg.contains("10.0.0.1"), "connection endpoint leaked");
            }
            other => panic!("expected RuntimeError::Internal, got {other:?}"),
        }
    }

    /// NotFound and Validation variants are user-facing and MUST keep
    /// their detail (404 / 422 responses). Pin the pass-through so a
    /// future refactor doesn't accidentally genericize these too.
    #[test]
    fn fabric_err_not_found_and_validation_pass_through() {
        let nf = FabricError::NotFound {
            entity: "run",
            id: "run_abc".into(),
        };
        match fabric_err_to_runtime(nf) {
            RuntimeError::NotFound { entity, id } => {
                assert_eq!(entity, "run");
                assert_eq!(id, "run_abc");
            }
            other => panic!("expected NotFound pass-through, got {other:?}"),
        }
        let val = FabricError::Validation {
            reason: "limit must be positive".into(),
        };
        match fabric_err_to_runtime(val) {
            RuntimeError::Validation { reason } => {
                assert_eq!(reason, "limit must be positive");
            }
            other => panic!("expected Validation pass-through, got {other:?}"),
        }
    }

    /// F37: terminal FCALL state conflicts must map to
    /// `RuntimeError::InvalidTransition` with a target derived from the
    /// FCALL name — not the opaque 500 "fabric layer error" that hid
    /// `partial_fence_triple` in production for weeks.
    #[test]
    fn terminal_state_conflict_maps_to_invalid_transition_per_fcall() {
        for (msg, expected_from, expected_to) in [
            (
                "ff_complete_execution rejected: partial_fence_triple",
                "partial_fence_triple",
                "completed",
            ),
            (
                "ff_complete_execution rejected: lease_expired",
                "lease_expired",
                "completed",
            ),
            (
                "ff_complete_execution rejected: execution_not_active",
                "execution_not_active",
                "completed",
            ),
            (
                "ff_fail_execution rejected: stale_lease",
                "stale_lease",
                "failed",
            ),
            (
                "ff_cancel_execution rejected: lease_revoked",
                "lease_revoked",
                "cancelled",
            ),
        ] {
            let err = FabricError::Internal(msg.to_owned());
            match fabric_err_to_runtime(err) {
                RuntimeError::InvalidTransition { entity, from, to } => {
                    assert_eq!(entity, "run", "msg={msg}");
                    assert_eq!(from, expected_from, "msg={msg}");
                    assert_eq!(to, expected_to, "msg={msg}");
                }
                other => {
                    panic!("expected InvalidTransition for {msg:?}, got {other:?}")
                }
            }
        }
    }

    /// Terminal state codes must ONLY classify when prefixed by a
    /// recognized terminal FCALL. A code that leaked from an unrelated
    /// call (e.g. a generic "valkey: lease_expired" string) must keep
    /// falling through to the generic internal-error path.
    #[test]
    fn terminal_state_conflict_gates_on_fcall_name_prefix() {
        assert!(!is_terminal_state_conflict("valkey: lease_expired"));
        assert!(!is_terminal_state_conflict(
            "ff_suspend_execution rejected: lease_expired"
        ));
        assert!(!is_terminal_state_conflict(
            "ff_renew_lease rejected: stale_lease"
        ));
    }

    fn test_project() -> ProjectKey {
        ProjectKey::new("tenant-a", "workspace-a", "project-a")
    }

    fn envelope(event: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
        EventEnvelope::for_runtime_event(EventId::new("evt_test"), EventSource::Runtime, event)
    }

    async fn seed_session(
        store: &Arc<InMemoryStore>,
        project: &ProjectKey,
        session_id: &SessionId,
    ) {
        store
            .append(&[envelope(RuntimeEvent::SessionCreated(SessionCreated {
                project: project.clone(),
                session_id: session_id.clone(),
            }))])
            .await
            .unwrap();
    }

    async fn seed_run(
        store: &Arc<InMemoryStore>,
        project: &ProjectKey,
        session_id: &SessionId,
        run_id: &RunId,
    ) {
        store
            .append(&[envelope(RuntimeEvent::RunCreated(RunCreated {
                project: project.clone(),
                session_id: session_id.clone(),
                run_id: run_id.clone(),
                parent_run_id: None,
                agent_role_id: None,
                prompt_release_id: None,
            }))])
            .await
            .unwrap();
    }

    async fn seed_task(
        store: &Arc<InMemoryStore>,
        project: &ProjectKey,
        task_id: &TaskId,
        parent_run_id: Option<&RunId>,
    ) {
        store
            .append(&[envelope(RuntimeEvent::TaskCreated(TaskCreated {
                project: project.clone(),
                task_id: task_id.clone(),
                parent_run_id: parent_run_id.cloned(),
                parent_task_id: None,
                prompt_release_id: None,
                session_id: None,
            }))])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn resolve_run_returns_none_for_unknown_id() {
        let store = Arc::new(InMemoryStore::new());
        let result = resolve_project_from_run_id(&store, &RunId::new("run_missing"))
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn resolve_task_returns_none_for_unknown_id() {
        let store = Arc::new(InMemoryStore::new());
        let result = resolve_project_from_task_id(&store, &TaskId::new("task_missing"))
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn resolve_session_returns_none_for_unknown_id() {
        let store = Arc::new(InMemoryStore::new());
        let result = resolve_project_from_session_id(&store, &SessionId::new("sess_missing"))
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn resolve_run_returns_project_after_insert() {
        let store = Arc::new(InMemoryStore::new());
        let project = test_project();
        let session_id = SessionId::new("sess_1");
        let run_id = RunId::new("run_1");

        seed_session(&store, &project, &session_id).await;
        seed_run(&store, &project, &session_id, &run_id).await;

        let resolved = resolve_project_from_run_id(&store, &run_id)
            .await
            .unwrap()
            .expect("run is seeded, resolver must return Some");
        assert_eq!(resolved, project);
    }

    #[tokio::test]
    async fn resolve_task_returns_project_after_insert() {
        let store = Arc::new(InMemoryStore::new());
        let project = test_project();
        let task_id = TaskId::new("task_1");

        seed_task(&store, &project, &task_id, None).await;

        let resolved = resolve_project_from_task_id(&store, &task_id)
            .await
            .unwrap()
            .expect("task is seeded, resolver must return Some");
        assert_eq!(resolved, project);
    }

    #[tokio::test]
    async fn resolve_session_returns_project_after_insert() {
        let store = Arc::new(InMemoryStore::new());
        let project = test_project();
        let session_id = SessionId::new("sess_1");

        seed_session(&store, &project, &session_id).await;

        let resolved = resolve_project_from_session_id(&store, &session_id)
            .await
            .unwrap()
            .expect("session is seeded, resolver must return Some");
        assert_eq!(resolved, project);
    }

    /// `resolve_run_project` surfaces a typed NotFound with
    /// `entity: "run"`. Handlers map this straight to HTTP 404; any drift to
    /// a generic Internal error would mask a legitimate missing-resource.
    #[tokio::test]
    async fn resolve_run_project_maps_unknown_id_to_run_not_found() {
        let store = Arc::new(InMemoryStore::new());
        let err = resolve_run_project(&store, &RunId::new("run_missing"))
            .await
            .expect_err("missing run must not resolve");
        match err {
            RuntimeError::NotFound { entity, id } => {
                assert_eq!(entity, "run");
                assert_eq!(id, "run_missing");
            }
            other => panic!("expected NotFound {{ entity: \"run\", .. }}, got {other:?}"),
        }
    }

    /// Same invariant as above but on the task-side helper — guards against
    /// the resolver being silently re-aliased (e.g. someone wiring the
    /// task helper through the run one).
    #[tokio::test]
    async fn resolve_task_project_maps_unknown_id_to_task_not_found() {
        let store = Arc::new(InMemoryStore::new());
        let err = resolve_task_project(&store, &TaskId::new("task_missing"))
            .await
            .expect_err("missing task must not resolve");
        match err {
            RuntimeError::NotFound { entity, id } => {
                assert_eq!(entity, "task");
                assert_eq!(id, "task_missing");
            }
            other => panic!("expected NotFound {{ entity: \"task\", .. }}, got {other:?}"),
        }
    }

    /// `FabricRunServiceAdapter::list_by_session` delegates to the cairn-store
    /// projection by design — FF does not index runs by cairn `SessionId`.
    /// This test pins that delegation without needing a live `FabricServices`
    /// by exercising the extracted helper directly.
    #[tokio::test]
    async fn list_runs_by_session_returns_seeded_runs_via_projection() {
        let store = Arc::new(InMemoryStore::new());
        let project = test_project();
        let session_id = SessionId::new("sess_list");
        let run_a = RunId::new("run_a");
        let run_b = RunId::new("run_b");

        seed_session(&store, &project, &session_id).await;
        seed_run(&store, &project, &session_id, &run_a).await;
        seed_run(&store, &project, &session_id, &run_b).await;

        let rows = list_runs_by_session_from_projection(&store, &session_id, 10, 0)
            .await
            .expect("projection read must succeed");
        assert_eq!(rows.len(), 2);
        let ids: std::collections::HashSet<_> =
            rows.iter().map(|r| r.run_id.as_str().to_owned()).collect();
        assert!(ids.contains("run_a"));
        assert!(ids.contains("run_b"));
    }

    /// Unknown session → empty Vec, not an error. Matches the
    /// trait-level contract (list returns an empty collection, not NotFound,
    /// for a session with no runs).
    #[tokio::test]
    async fn list_runs_by_session_returns_empty_for_unknown_session() {
        let store = Arc::new(InMemoryStore::new());
        let rows =
            list_runs_by_session_from_projection(&store, &SessionId::new("sess_empty"), 10, 0)
                .await
                .expect("projection read must succeed");
        assert!(rows.is_empty());
    }

    /// Offset slices before limit — pin the pagination contract so a future
    /// refactor that swaps the two arguments (easy mistake in event-log
    /// projections) fails loudly.
    #[tokio::test]
    async fn list_runs_by_session_respects_offset_and_limit() {
        let store = Arc::new(InMemoryStore::new());
        let project = test_project();
        let session_id = SessionId::new("sess_pag");
        for i in 0..5 {
            let run_id = RunId::new(format!("run_{i}"));
            if i == 0 {
                seed_session(&store, &project, &session_id).await;
            }
            seed_run(&store, &project, &session_id, &run_id).await;
        }
        let page = list_runs_by_session_from_projection(&store, &session_id, 2, 2)
            .await
            .expect("projection read must succeed");
        assert_eq!(page.len(), 2, "expected 2 runs with offset=2 limit=2");
    }

    /// When a run, task, and session all exist for the same scope, every
    /// resolver must return the identical `ProjectKey`. Guards against
    /// accidentally projecting a stale or mismatched scope field (e.g. if
    /// someone ever added a resolver that picked `session.project` for a task
    /// lookup).
    #[tokio::test]
    async fn resolvers_agree_across_run_task_session_for_same_scope() {
        let store = Arc::new(InMemoryStore::new());
        let project = test_project();
        let session_id = SessionId::new("sess_shared");
        let run_id = RunId::new("run_shared");
        let task_id = TaskId::new("task_shared");

        seed_session(&store, &project, &session_id).await;
        seed_run(&store, &project, &session_id, &run_id).await;
        seed_task(&store, &project, &task_id, Some(&run_id)).await;

        let run_proj = resolve_project_from_run_id(&store, &run_id)
            .await
            .unwrap()
            .unwrap();
        let task_proj = resolve_project_from_task_id(&store, &task_id)
            .await
            .unwrap()
            .unwrap();
        let sess_proj = resolve_project_from_session_id(&store, &session_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(run_proj, project);
        assert_eq!(task_proj, project);
        assert_eq!(sess_proj, project);
        assert_eq!(run_proj, task_proj);
        assert_eq!(task_proj, sess_proj);
    }

    /// Compile-time guard that `FabricRunServiceAdapter` overrides
    /// `start_with_correlation` on the `RunService` trait rather than
    /// inheriting the default impl (which drops `correlation_id` at
    /// `cairn-runtime/src/runs.rs:101-110`).
    ///
    /// A live end-to-end assertion needs a running `FabricServices` (Valkey
    /// backed), so it lives in `tests/integration/test_run_lifecycle.rs`.
    /// Here we only prove the override is present: taking a function pointer
    /// to `<FabricRunServiceAdapter as RunService>::start_with_correlation`
    /// and comparing to the default-impl pointer would require Rust method-
    /// resolution tricks, so we fall back to checking that the SOURCE file
    /// contains the explicit override. A regression that deletes the
    /// override would leave the trait default in place and silently drop
    /// correlation on the sqeq ingress path — caught here.
    #[test]
    fn fabric_run_adapter_overrides_start_with_correlation() {
        let src = include_str!("fabric_adapter.rs");
        assert!(
            src.contains("async fn start_with_correlation("),
            "FabricRunServiceAdapter must explicitly override \
             start_with_correlation — default trait impl drops the \
             correlation_id (see cairn-runtime/src/runs.rs:101-110). \
             Sqeq ingress (handlers/sqeq.rs) relies on this for audit \
             trail preservation on the Fabric path.",
        );
        // Belt-and-braces: verify the override threads correlation_id
        // through to the fabric layer, not into the void.
        assert!(
            src.contains(".start_with_correlation(") && src.contains("Some(correlation_id)"),
            "override must pass the correlation_id down to \
             fabric.runs.start_with_correlation — delegating to plain \
             `fabric.runs.start()` would still drop it",
        );
    }

    /// Task #178 regression: `wait_for_session_projection` must return
    /// `Ok(())` the first time the projection row appears.
    ///
    /// Pre-fix, `FabricSessionServiceAdapter::create` returned as soon as
    /// `fabric.sessions.create()` committed the FF flow, without waiting
    /// for the async `EventBridge` consumer to drain the
    /// `BridgeEvent::SessionCreated` into the `InMemoryStore` projection.
    /// A tightly-following `POST /v1/runs` then failed with 404 "session
    /// not found" because `FabricSessionServiceAdapter::get` resolves
    /// `(session_id) -> ProjectKey` through that exact projection. The
    /// three RFC 020 integration tests (#1, #6, #11) tripped this.
    ///
    /// This test pins the barrier helper's contract: once a
    /// `SessionCreated` envelope is appended to the store the polling
    /// loop returns immediately (no sleep), and a missing session
    /// stays missing for the full 2s budget before erroring. Both
    /// shapes are needed: "returns on first success" is the happy
    /// path, "errors loudly on timeout" is the safety rail that
    /// keeps a silent-404 from reaching the next handler.
    #[tokio::test]
    async fn wait_for_session_projection_returns_when_row_present() {
        let store = Arc::new(InMemoryStore::new());
        let project = test_project();
        let session_id = SessionId::new("sess_barrier");
        seed_session(&store, &project, &session_id).await;
        // Present from t=0 → loop returns on first probe, well under
        // the 2s ceiling.
        let start = std::time::Instant::now();
        wait_for_session_projection(&store, &session_id)
            .await
            .expect("projection is already populated");
        assert!(
            start.elapsed() < std::time::Duration::from_millis(100),
            "barrier should return immediately when row is present (took {:?})",
            start.elapsed(),
        );
    }

    #[tokio::test]
    async fn wait_for_session_projection_errors_on_missing_row() {
        // An empty store with no producer will never populate the row —
        // the barrier must error out, not hang forever. Two load-bearing
        // assertions:
        //
        // 1. At ~250ms the helper is still polling (didn't wrongly
        //    return `Ok(())` before the deadline, didn't wedge).
        // 2. After the 2s budget elapses the helper returns
        //    `Err(RuntimeError::Internal("fabric layer error"))` — the
        //    hard safety-rail that keeps a silent-404 out of the next
        //    handler, with an opaque SEC-007-compliant message.
        //
        // Real wall-clock — `tokio::time::pause()` / `advance()` need
        // the `test-util` feature which the crate doesn't enable.
        // 2s is cheap enough for a unit test on CI.
        let store = Arc::new(InMemoryStore::new());
        let session_id = SessionId::new("sess_never");

        let fut = wait_for_session_projection(&store, &session_id);
        tokio::pin!(fut);

        // Probe: at t≈250ms the loop must still be polling.
        let probe = tokio::time::timeout(std::time::Duration::from_millis(250), &mut fut).await;
        assert!(
            probe.is_err(),
            "barrier must not return before the 2s deadline on an \
             empty store (got {probe:?})",
        );

        // Final: resume awaiting the future — it will observe its own
        // deadline crossing (~1.75s from now) and return Err.
        match (&mut fut).await {
            Err(RuntimeError::Internal(msg)) => {
                // SEC-007: surfaced message must be the opaque token,
                // not any internal timing/bridge-state detail.
                assert_eq!(
                    msg, "fabric layer error",
                    "barrier error message must be opaque (SEC-007); got {msg:?}",
                );
            }
            other => {
                panic!("barrier must return RuntimeError::Internal on timeout, got {other:?}",)
            }
        }
    }
}
