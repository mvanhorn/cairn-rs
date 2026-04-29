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
