//! Providers-exhausted operator approval card.
//!
//! When every binding × model in the routed provider chain has failed
//! with a fallback-eligible error, the orchestrator returns
//! `OrchestratorError::AllProvidersExhausted`. The error handler in
//! `orchestrate::orchestrate_run_handler_inner` returns HTTP 502 with
//! the per-attempt summary inline, and on its way out calls
//! [`submit_all_providers_exhausted_proposal`] to also surface the
//! failure as an operator approval card in the tool-call-approvals UI
//! so ops can rotate credentials / add a provider / abort the run
//! without hunting through logs.
//!
//! Complementing that card, [`suspend_run_for_providers_exhausted`] flips
//! the run's lifecycle state to `WaitingApproval` so `GET /v1/runs/:id`
//! reports the run as blocked-on-operator rather than the stale
//! `Running` state that was the #693 dogfood R3-B finding. Without the
//! flip, operators looking at the runs list see `running` and assume
//! forward progress — but no worker is driving the run; it's waiting
//! on a human to resolve the `escalate_to_operator` card emitted above.

use std::sync::Arc;

use crate::state::AppState;

/// Submit a `ToolCallApprovalService::submit_proposal` proposal summarising
/// the fallback-chain exhaustion so the operator gets an actionable card in
/// the new tool-call-approvals UI.
///
/// Best-effort: failures are logged but don't re-fail the caller since the
/// caller is already returning an HTTP 502 with the summary inline.
pub(super) async fn submit_all_providers_exhausted_proposal(
    state: &AppState,
    run: &cairn_store::projections::RunRecord,
    preferred_model: &str,
    attempts: &[cairn_orchestrator::FallbackAttempt],
    summary: &str,
) -> Result<(), String> {
    use cairn_runtime::ToolCallApprovalService as _;

    let tool_call_approval_reader = Arc::new(
        cairn_runtime::services::ToolCallApprovalReaderAdapter::new(state.runtime.store.clone()),
    );
    let svc = cairn_runtime::services::ToolCallApprovalServiceImpl::new(
        state.runtime.store.clone(),
        tool_call_approval_reader,
    );

    // Copilot review (PR #567): ms-resolution suffix can collide on
    // concurrent retries against the same run. UUID v4 is collision-
    // proof and keeps the `tc_providers_exhausted_<run>_` prefix
    // operators scan for in log search.
    let call_id = cairn_domain::ToolCallId::new(format!(
        "tc_providers_exhausted_{}_{}",
        run.run_id.as_str(),
        uuid::Uuid::new_v4()
    ));
    let tcp = cairn_runtime::ToolCallProposal {
        call_id,
        project: run.project.clone(),
        session_id: run.session_id.clone(),
        run_id: run.run_id.clone(),
        tool_name: "escalate_to_operator".to_owned(),
        tool_args: serde_json::json!({
            "reason": "all_providers_exhausted",
            "preferred_model": preferred_model,
            "summary": summary,
            // SEC-007 (Copilot review, PR #567): `error_message` can
            // contain upstream provider bodies that in turn echo bearer
            // tokens or proprietary internals. `summary` above is
            // already redacted by the caller. Mirror that here so the
            // per-attempt field doesn't bypass the same gate — run
            // `redact_secrets` on every error before it lands in the
            // durable approval card.
            "attempts": attempts.iter().map(|a| serde_json::json!({
                "model_id": a.model_id,
                "reason_code": a.reason_code,
                "error": cairn_providers::redact_secrets(&a.error_message),
            })).collect::<Vec<_>>(),
            "suggested_actions": [
                "Update system defaults `brain_model` / `generate_model` to a model you have credits for",
                "Edit a provider connection's `supported_models` list to include a working model",
                "Top up free-tier credits on your configured provider (OpenRouter, etc.)",
                "Add a new provider connection via POST /v1/providers/connections",
                "Abort the run",
            ],
        }),
        // Build a descriptive one-liner that distinguishes this card
        // from other providers-exhausted events at a glance: include
        // run_id + the first attempt's reason so the operator can see
        // the dominant failure mode without expanding the card.
        display_summary: Some(format!(
            "providers exhausted on run {} ({} models tried; first failure: {})",
            run.run_id.as_str(),
            attempts.len(),
            attempts.first().map(|a| a.reason_code).unwrap_or("unknown"),
        )),
        match_policy: cairn_domain::approvals::ApprovalMatchPolicy::Exact,
    };

    svc.submit_proposal(tcp).await.map(|_| ()).map_err(|e| {
        tracing::warn!(error = %e, "failed to submit providers-exhausted tool-call approval");
        e.to_string()
    })
}

/// Flip the run to `WaitingApproval` after the providers-exhausted
/// escalation card has been submitted so `GET /v1/runs/:id` no longer
/// reports the run as `running` while it's actually blocked on operator
/// action (#693 R3-B).
///
/// Reuses the existing `RunService::enter_waiting_approval` primitive
/// — the same one the regular tool-call approval path relies on — so
/// that the approve/reject round trip on the escalate_to_operator card
/// resumes the run through the standard `resolve_approval` path without
/// bespoke wiring. No new `RunState` variant was introduced: the
/// operator-visible semantics ("blocked on operator decision") match
/// `WaitingApproval` exactly, and reusing it keeps dashboards,
/// lease-keeper suppression (#666 skip-renew-while-pending), and
/// resume plumbing coherent.
///
/// Best-effort like [`submit_all_providers_exhausted_proposal`]: the
/// caller is already returning HTTP 502 with the inline summary.
/// Failures here are logged at `error` (not swallowed at `warn`) because
/// they result in operator-visible state drift: the approval card
/// appears but the run still reports `state=running`. Surfacing at
/// `error` gives ops a grep-able marker (`r3b_state_transition_failed`)
/// that distinguishes this drift from benign `InvalidTransition`
/// already-terminal races, which log at `debug`.
pub(super) async fn suspend_run_for_providers_exhausted(
    state: &AppState,
    run: &cairn_store::projections::RunRecord,
) {
    match state
        .runtime
        .runs
        .enter_waiting_approval(&run.session_id, &run.run_id)
        .await
    {
        Ok(_) => {
            tracing::info!(
                run_id = %run.run_id,
                session_id = %run.session_id,
                "#693 R3-B: run transitioned to waiting_approval after providers-exhausted escalation"
            );
        }
        Err(err) => {
            // `InvalidTransition` is the benign already-terminal race:
            // the loop's terminal handler finalized the run before we
            // got here, so the state is already past `Running`. Log at
            // debug and move on.
            match &err {
                cairn_runtime::error::RuntimeError::InvalidTransition { .. } => {
                    tracing::debug!(
                        run_id = %run.run_id,
                        error = %err,
                        "#693 R3-B: run already in a terminal or suspended state; skip waiting_approval flip"
                    );
                }
                _ => {
                    tracing::error!(
                        run_id = %run.run_id,
                        session_id = %run.session_id,
                        error = %err,
                        r3b_state_transition_failed = true,
                        "#693 R3-B: failed to flip run to waiting_approval after providers-exhausted escalation; \
                         GET /v1/runs/:id may report stale state=running while the escalate_to_operator card is pending"
                    );
                }
            }
        }
    }
}
