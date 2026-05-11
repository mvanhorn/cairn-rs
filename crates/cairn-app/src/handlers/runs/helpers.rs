//! Shared internals for the runs handler sub-modules.
//!
//! Anything that two or more domain sub-modules need to reach — the
//! stuck-run threshold default, the provider-error redactor, the
//! failure-class classifier, and the run-fail finalizer — lives here.

use crate::state::AppState;

// ── Stuck-run threshold ─────────────────────────────────────────────────────

/// Settings-defaults key for the stuck-run threshold (system scope, milliseconds).
pub(crate) const STUCK_RUN_THRESHOLD_KEY: &str = "stuck_run_threshold_ms";

/// Read the system-scope `stuck_run_threshold_ms` default, if set.
///
/// Returns `None` when unset, when the stored value is not a non-negative
/// whole number, OR when the projection read fails — in the last case the
/// error is logged at `warn` and callers fall back to the hard-coded
/// default so a transient store outage never turns `/v1/runs/stalled` into
/// a 500. An operator-visible store outage is already surfaced by the
/// dedicated store-health surface (`GET /v1/status`).
pub(crate) async fn resolve_stuck_run_threshold_ms(state: &AppState) -> Option<u64> {
    use cairn_domain::Scope;
    use cairn_store::projections::DefaultsReadModel;

    let record = match DefaultsReadModel::get(
        state.runtime.store.as_ref(),
        Scope::System,
        "system",
        STUCK_RUN_THRESHOLD_KEY,
    )
    .await
    {
        Ok(Some(r)) => r,
        Ok(None) => return None,
        Err(err) => {
            tracing::warn!(
                error = %err,
                key = STUCK_RUN_THRESHOLD_KEY,
                "failed to read stuck-run threshold default; falling back to hard-coded value"
            );
            return None;
        }
    };
    // Accept both JSON integer and JSON float forms — validation
    // guarantees the stored number is whole and within u64 range.
    record.value.as_u64().or_else(|| {
        record.value.as_f64().and_then(|n| {
            if n.is_finite() && n.fract() == 0.0 && n >= 0.0 && n <= u64::MAX as f64 {
                Some(n as u64)
            } else {
                None
            }
        })
    })
}

// ── Telemetry redactor ──────────────────────────────────────────────────────

/// Redact unsafe provider error strings for telemetry responses.
///
/// Provider-layer errors already flow through `cairn_providers::redact` —
/// this is the belt-and-suspenders guard at the API boundary:
///
/// - `None` input → `None` output.
/// - String with a live-looking `bearer ... sk-...` pattern → replaced
///   with the fixed marker `"<redacted: leaked credential pattern>"`.
///   The returned `Option` is always `Some` here — downstream JSON
///   callers see the marker, not `null`, so the UI can distinguish
///   "redacted" from "no error at all".
/// - Otherwise → pass through; if longer than 1024 chars, truncate
///   with a trailing ellipsis.
pub(super) fn redact_provider_error(raw: Option<&str>) -> Option<String> {
    let raw = raw?;
    let lower = raw.to_ascii_lowercase();
    if lower.contains("bearer ") && lower.contains("sk-") {
        // Likely still carries a live key. Drop rather than leak.
        return Some("<redacted: leaked credential pattern>".to_owned());
    }
    const MAX: usize = 1024;
    if raw.len() > MAX {
        let mut s = raw.chars().take(MAX).collect::<String>();
        s.push_str("...");
        Some(s)
    } else {
        Some(raw.to_owned())
    }
}

// ── Failure classification + finalizer ──────────────────────────────────────

/// F53: heuristically map a `LoopTermination::Failed { reason }` string to
/// a `FailureClass`. The reason is free-form from the orchestrator loop
/// (provider errors, lease-expiry diagnostics, tool failures, …), so we
/// substring-match the obvious cases and fall back to `ExecutionError`
/// so operators can still filter failed runs by class.
pub(super) fn classify_failed_reason(reason: &str) -> cairn_domain::FailureClass {
    let lower = reason.to_ascii_lowercase();
    // #825: execute_impl::derive_signal emits a reason prefixed with
    // `model_reported_failure:` when the agent emits FailRun. Match
    // BEFORE the verification_rejected branch — both are agent-
    // terminated failures but #660/#821/#823's gate rejection (which
    // fires on `error:` lines in tool output) uses the
    // `verification_rejected:` prefix, while #825's explicit
    // self-reported failure uses this one. Literal prefix is a
    // contract with cairn-orchestrator::execute_impl::derive_signal.
    if lower.starts_with("model_reported_failure") {
        cairn_domain::FailureClass::ModelReportedFailure
    }
    // RFC 032 PR-4: the completion-contract verifier rejected the
    // claimed deliverable `MAX_COMPLETION_GATE_REJECTIONS` times in
    // a row. Prefix is a wire contract with
    // `cairn-orchestrator::loop_runner::OrchestratorLoop::run_inner`
    // — a rename there breaks the classification here. Match BEFORE
    // `verification_rejected` so the contract-specific class wins
    // for reasons carrying the contract prefix even if a future
    // refactor concatenates both strings.
    else if lower.starts_with("contract_not_met") {
        cairn_domain::FailureClass::ContractNotMet
    }
    // #660: the strict completion gate emits a reason string that
    // starts with the literal `verification_rejected:` prefix (see
    // `crates/cairn-orchestrator/src/loop_runner.rs` — the LoopConfig
    // rustdoc documents the contract). Match the prefix so a future
    // change to the error list / attempt count suffix can't silently
    // drop the classification back to `ExecutionError`.
    else if lower.starts_with("verification_rejected") {
        cairn_domain::FailureClass::VerificationRejected
    } else if lower.contains("lease") && (lower.contains("expir") || lower.contains("lost")) {
        cairn_domain::FailureClass::LeaseExpired
    } else if lower.contains("timed out") || lower.contains("timeout") {
        cairn_domain::FailureClass::TimedOut
    } else if lower.contains("approval") && lower.contains("reject") {
        cairn_domain::FailureClass::ApprovalRejected
    } else if lower.contains("policy") && lower.contains("denied") {
        cairn_domain::FailureClass::PolicyDenied
    } else {
        cairn_domain::FailureClass::ExecutionError
    }
}

/// F53: flip the run to the terminal `Failed` state via
/// `RunService::fail`. Called from every non-success
/// `LoopTermination` branch (Failed / MaxIterationsReached / TimedOut)
/// so the run projection catches up to the orchestrate response body
/// (previously runs stayed `Running` forever, forcing operators to
/// manually cancel).
///
/// Swallows errors: a transient store blip or an already-terminal run
/// (e.g. `runs.complete` already fired inside the loop) must not turn
/// an orchestrate response into a 5xx for the operator. All failure
/// paths are logged so ops can spot the drift.
pub(super) async fn finalize_run_failure(
    state: &AppState,
    session_id: &cairn_domain::SessionId,
    run_id: &cairn_domain::RunId,
    failure_class: cairn_domain::FailureClass,
) {
    if let Err(e) = state
        .runtime
        .runs
        .fail(session_id, run_id, failure_class)
        .await
    {
        // `InvalidTransition` is the common benign case: the loop already
        // reached a terminal state (Completed / Canceled) before we got
        // here. Log at debug. Anything else is operator-visible.
        match &e {
            cairn_runtime::error::RuntimeError::InvalidTransition { .. } => {
                tracing::debug!(
                    run_id = %run_id,
                    failure_class = ?failure_class,
                    error = %e,
                    "F53: run already in terminal state; skip fail-flip"
                );
            }
            _ => {
                tracing::warn!(
                    run_id = %run_id,
                    failure_class = ?failure_class,
                    error = %e,
                    "F53: failed to flip run to Failed after terminal orchestrate; \
                     GET /v1/runs/:id may report stale state=running"
                );
            }
        }
    }
}

// ── SLA DTO default ─────────────────────────────────────────────────────────

/// Default `alert_at_percent` for [`SetRunSlaRequest`].
pub(super) fn default_alert_pct() -> u8 {
    80
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── F29 CD: redact_provider_error ───────────────────────────────
    //
    // Security-sensitive: this function is the last line of defence
    // against an upstream provider echoing a live Authorization header
    // into an error payload. The matrix below locks in the three cases
    // the telemetry handler relies on.

    #[test]
    fn redact_provider_error_drops_leaked_bearer_key() {
        // Build the leaked marker at runtime so GitGuardian static scans
        // don't flag this literal as a real credential.
        let marker = format!("sk-{}", "fake-test-only-".to_owned() + &"x".repeat(24));
        let leaked = format!("upstream returned: Authorization: Bearer {marker} denied");
        let out = redact_provider_error(Some(&leaked)).expect("some");
        assert!(
            !out.contains(&marker),
            "leaked credential pattern must not survive: {out}"
        );
        assert!(
            out.contains("<redacted"),
            "expected explicit redaction marker, got: {out}"
        );
    }

    #[test]
    fn redact_provider_error_truncates_oversize_payload() {
        let huge = "x".repeat(4096);
        let out = redact_provider_error(Some(&huge)).expect("some");
        // Truncation cap is 1024 chars + 3-char ellipsis.
        assert!(out.len() <= 1024 + 3, "length cap broken: {}", out.len());
        assert!(out.ends_with("..."), "ellipsis marker missing: {out}");
    }

    #[test]
    fn redact_provider_error_passes_clean_message_through() {
        let msg = "provider returned 500: upstream timeout";
        assert_eq!(
            redact_provider_error(Some(msg)).as_deref(),
            Some(msg),
            "clean message must pass through verbatim"
        );
    }

    #[test]
    fn redact_provider_error_none_stays_none() {
        assert_eq!(redact_provider_error(None), None);
    }

    /// #660: the orchestrator's strict completion gate emits a reason
    /// string prefixed `verification_rejected:` when the LLM refuses to
    /// converge away from a failing build. The handler must translate
    /// that into `FailureClass::VerificationRejected` so operators can
    /// filter failed runs by class without re-parsing free-form reason
    /// text.
    /// RFC 032 PR-4: the completion-contract verifier emits a reason
    /// string prefixed `contract_not_met:` when the claimed
    /// deliverable fails verification `MAX_COMPLETION_GATE_REJECTIONS`
    /// times. The handler must translate that into
    /// `FailureClass::ContractNotMet` — distinct from
    /// `VerificationRejected` so operator dashboards can
    /// distinguish "agent lied / admitted" from "agent's claimed
    /// deliverable does not exist".
    #[test]
    fn classify_failed_reason_matches_contract_not_met_prefix() {
        use cairn_domain::FailureClass;
        // Sample reason uses the stable snake_case wire form of
        // `ContractRejectionCode::PrNotFound` per RFC §3.1. The
        // classifier only keys off the `contract_not_met:` prefix;
        // the code payload is for operator logs / LLM diagnostics.
        let reason = "contract_not_met: 3 complete_run attempts rejected by \
                      completion-contract verifier (code: pr_not_found). See \
                      operator logs for details.";
        assert_eq!(
            classify_failed_reason(reason),
            FailureClass::ContractNotMet,
            "RFC 032 PR-4: `contract_not_met:` prefix is the wire contract \
             between the loop runner and this classifier"
        );
    }

    /// RFC 032 PR-4: contract_not_met classification beats
    /// verification_rejected when both prefixes appear — the
    /// earlier prefix in the match chain wins, and contract_not_met
    /// is declared first (RFC §3 order).
    #[test]
    fn classify_failed_reason_contract_not_met_case_insensitive() {
        use cairn_domain::FailureClass;
        assert_eq!(
            classify_failed_reason("Contract_Not_Met: code=pr_not_found"),
            FailureClass::ContractNotMet,
        );
    }

    #[test]
    fn classify_failed_reason_matches_verification_rejected_prefix() {
        use cairn_domain::FailureClass;
        let reason = "verification_rejected: 3 error(s) after 3 complete_run attempts. \
                      First errors: error[E0308]: mismatched types | error: could not \
                      compile `demo` | error: linker failed";
        assert_eq!(
            classify_failed_reason(reason),
            FailureClass::VerificationRejected,
            "#660: the `verification_rejected:` prefix is the wire contract \
             between the loop runner and this classifier; a change here \
             must be reflected in the loop runner's reason format."
        );
    }

    /// #660: lowercase match on the prefix catches case drift from
    /// future log formatters without regressing the classification.
    #[test]
    fn classify_failed_reason_matches_verification_rejected_case_insensitive() {
        use cairn_domain::FailureClass;
        assert_eq!(
            classify_failed_reason("Verification_Rejected: 1 error(s)"),
            FailureClass::VerificationRejected,
        );
    }

    /// Sanity: unrelated reasons still land on `ExecutionError`, so the
    /// `verification_rejected` arm isn't accidentally shadowing another
    /// class.
    #[test]
    fn classify_failed_reason_falls_through_on_unrelated_reasons() {
        use cairn_domain::FailureClass;
        assert_eq!(
            classify_failed_reason("tool error: stdout exceeded 1MB"),
            FailureClass::ExecutionError,
        );
    }

    /// #825: the orchestrator emits `model_reported_failure: <reason>`
    /// when the agent calls `ActionType::FailRun`. The classifier must
    /// route this to `FailureClass::ModelReportedFailure` so operator
    /// dashboards distinguish agent-declared blockage from generic
    /// infrastructure errors.
    #[test]
    fn classify_failed_reason_matches_model_reported_failure_prefix() {
        use cairn_domain::FailureClass;
        let reason = "model_reported_failure: blocked: src/main.rs does not exist; depends on M1-1";
        assert_eq!(
            classify_failed_reason(reason),
            FailureClass::ModelReportedFailure,
            "#825: the `model_reported_failure:` prefix is the wire \
             contract between execute_impl::derive_signal and this \
             classifier; a change here must be reflected in \
             execute_impl::derive_signal."
        );
    }

    /// #825: the model_reported_failure prefix must take priority over
    /// other heuristic substring matches. A fail_run reason may
    /// legitimately mention "timeout" or "approval" in free text
    /// (e.g. "blocked: upstream service timeout preventing us from
    /// proceeding"); the explicit prefix wins so the classification
    /// stays stable.
    #[test]
    fn classify_failed_reason_model_reported_failure_beats_substring_heuristics() {
        use cairn_domain::FailureClass;
        assert_eq!(
            classify_failed_reason(
                "model_reported_failure: blocked: upstream service timeout preventing us \
                 from retrieving the config; operator approval would not change this"
            ),
            FailureClass::ModelReportedFailure,
            "explicit prefix must take priority over `timeout` / \
             `approval` substrings"
        );
    }
}
