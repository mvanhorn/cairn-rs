use std::fmt;

/// Runtime service errors.
#[derive(Debug)]
pub enum RuntimeError {
    /// Entity not found.
    NotFound { entity: &'static str, id: String },
    /// Invalid state transition.
    InvalidTransition {
        entity: &'static str,
        from: String,
        to: String,
    },
    /// Command rejected by policy.
    PolicyDenied { reason: String },
    /// Optimistic concurrency conflict.
    Conflict { entity: &'static str, id: String },
    /// Re-declaring a task dependency with a different edge kind or
    /// `data_passing_ref` than the already-staged edge. Cairn surfaces
    /// this as HTTP 409 with both existing and requested values so
    /// operators can see the divergence.
    ///
    /// Boxed to keep `RuntimeError` at a small `size_of` (clippy
    /// `result_large_err`): this variant is rare compared to the
    /// lifecycle variants, so paying one allocation on the error
    /// path beats inflating every `Result<_, RuntimeError>` return.
    DependencyConflict(Box<DependencyConflictDetail>),
    /// Lease has expired.
    LeaseExpired { task_id: String },
    /// Store error.
    Store(cairn_store::StoreError),
    /// Internal error.
    Internal(String),
    /// Tenant quota exceeded.
    QuotaExceeded {
        tenant_id: String,
        quota_type: String,
        current: u32,
        limit: u32,
    },
    /// Validation failure.
    Validation { reason: String },
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RuntimeError::NotFound { entity, id } => write!(f, "{entity} not found: {id}"),
            RuntimeError::InvalidTransition { entity, from, to } => {
                // F41: when the `from` field carries a known FF state
                // code (injected by `fabric_err_to_runtime`'s terminal /
                // suspend classifiers), surface an operator-actionable
                // summary instead of the raw `execution_not_active ->
                // completed` jargon that the dogfood v6 runs hit.
                //
                // The original code is preserved verbatim at the end
                // (`code={from}`) so the operator can still grep logs
                // and correlate with FF's ScriptError taxonomy, but the
                // leading prose tells them what to do right now.
                if let Some(hint) = invalid_transition_hint(from.as_str(), to.as_str(), entity) {
                    write!(
                        f,
                        "invalid {entity} transition to {to}: {hint} (code={from})"
                    )
                } else {
                    write!(f, "invalid {entity} transition: {from} -> {to}")
                }
            }
            RuntimeError::PolicyDenied { reason } => write!(f, "policy denied: {reason}"),
            RuntimeError::Conflict { entity, id } => {
                write!(f, "{entity} conflict: {id}")
            }
            RuntimeError::LeaseExpired { task_id } => write!(f, "lease expired: {task_id}"),
            RuntimeError::Store(e) => write!(f, "store error: {e}"),
            RuntimeError::Internal(msg) => write!(f, "internal runtime error: {msg}"),
            RuntimeError::QuotaExceeded {
                tenant_id,
                quota_type,
                current,
                limit,
            } => {
                write!(
                    f,
                    "quota exceeded for tenant {tenant_id}: {quota_type} ({current}/{limit})"
                )
            }
            RuntimeError::Validation { reason } => write!(f, "validation error: {reason}"),
            RuntimeError::DependencyConflict(d) => write!(
                f,
                "dependency edge {} <- {} already exists with \
                 kind={} ref={:?}; re-declare requested kind={} ref={:?}",
                d.dependent_task_id,
                d.prerequisite_task_id,
                d.existing_kind,
                d.existing_data_passing_ref,
                d.requested_kind,
                d.requested_data_passing_ref,
            ),
        }
    }
}

impl std::error::Error for RuntimeError {}

impl RuntimeError {
    /// Returns `true` when this error represents a **transient** FF
    /// execution-phase conflict — the execution exists and is not
    /// terminal, but is temporarily in a lifecycle sub-phase (e.g.
    /// mid-approval, resume-in-flight, tool-execution aftermath) that
    /// does not accept the `ff_renew_lease` / grant-fallback FCALL.
    ///
    /// Callers that hold an already-valid lease (the mid-run
    /// orchestrate handler being the canonical case — F58) can treat
    /// this class as "keep going with the existing lease; FF will
    /// flip the phase back on the next cycle" rather than surfacing a
    /// 409 to the operator. The orchestrator loop has its own
    /// `is_lease_healthy()` gate that catches an actually-dead lease.
    ///
    /// # Scope — narrow on purpose
    ///
    /// This matches only the two FF codes known to fire transiently
    /// against an otherwise-live execution during a healthy run:
    ///
    /// * `execution_not_eligible` — FF's grant gate rejects when
    ///   `lifecycle_phase != "runnable"`. Tool invocations (esp.
    ///   `write`) move the phase to `running` transiently; the next
    ///   loop cycle flips it back. (flowfabric.lua lines 3585–3590.)
    /// * `execution_not_eligible_for_attempt` — same class, attempt
    ///   axis. Emitted when the attempt counter moved under us.
    ///
    /// Codes that represent a **permanent** failure (the execution
    /// is terminal, does not exist, or the lease is revoked) are
    /// deliberately excluded — tolerating those would swallow real
    /// errors. In particular `execution_not_active`, `lease_expired`,
    /// `lease_revoked`, `execution_not_found` all stay as hard 409s.
    pub fn is_transient_phase_conflict(&self) -> bool {
        match self {
            RuntimeError::Conflict { entity, id } if *entity == "execution" => matches!(
                id.as_str(),
                "execution_not_eligible" | "execution_not_eligible_for_attempt"
            ),
            _ => false,
        }
    }

    /// Returns `true` when this error represents an FF `lease_expired`
    /// rejection on a terminal FCALL (`ff_complete_execution` /
    /// `ff_fail_execution` / `ff_cancel_execution`).
    ///
    /// F59 (2026-04-26): `POST /v1/runs/:id/orchestrate` is a pull-model
    /// driver. F51 added an entry-time `renew_lease_if_stale` call, but
    /// the lease can still expire **between** the orchestrator's last
    /// iteration and the final `complete_run` FCALL — the DECIDE →
    /// EXECUTE → terminal-FCALL window can span seconds on loaded
    /// workers, and the background renewer that ff-sdk spawns inside
    /// `ClaimedTask` does not cover the cairn-side control plane.
    ///
    /// Callers wrapping the terminal-FCALL service methods
    /// (`RunService::complete` / `fail` / `cancel`) use this
    /// classifier to decide whether to attempt a single re-claim +
    /// retry (F59's retry-once fallback). A permanent `lease_revoked`
    /// would NOT return `true` here — re-claim-then-retry is only
    /// legal when the expiry was a TTL miss, not an operator
    /// revocation.
    ///
    /// The fabric adapter maps `lease_expired` from the FF ScriptError
    /// taxonomy to `InvalidTransition { from: "lease_expired", to }`
    /// via `is_terminal_state_conflict` in
    /// `cairn_app::fabric_adapter::fabric_err_to_runtime`. Match on
    /// that shape exactly.
    pub fn is_lease_expired(&self) -> bool {
        matches!(
            self,
            RuntimeError::InvalidTransition { from, .. } if from == "lease_expired"
        )
    }
}

/// Upstream FF tracker for the `terminal_write_deadlock` wedged state.
///
/// The URL lives as a `macro_rules!` literal (rather than a `const`)
/// because the hint message body is built with `concat!`, and `concat!`
/// only accepts literal-string inputs — not `const` references. When
/// the FF tracker resolves or renumbers the issue, update the literal
/// on the single line below and every grep hit (the hint body + any
/// tests that assert the URL) picks up the new value automatically via
/// the macro expansion. See issue #489.
macro_rules! upstream_ff_terminal_write_deadlock_url {
    () => {
        "https://github.com/avifenesh/FlowFabric/issues/371"
    };
}

/// Map a FF / cairn state-transition rejection code to an operator-
/// actionable prose hint.
///
/// Returns `None` for codes we don't recognise — callers fall back to
/// the raw `{from} -> {to}` format so unknown codes aren't silently
/// swallowed behind a generic message.
///
/// Scope per F41: the terminal and suspend classifiers in
/// `cairn_app::fabric_adapter` are the two known call sites that
/// inject FF codes into this field. Review this table when
/// `is_terminal_state_conflict` or `is_suspend_state_conflict` change,
/// but full coverage is not required — unlisted codes (e.g.
/// `waitpoint_not_token_bound`) fall through to the raw `{from} ->
/// {to}` format, which is strictly safer than a wrong hint. Only add
/// a code here when we can write a genuinely actionable recovery
/// sentence for it.
fn invalid_transition_hint(from: &str, to: &str, entity: &str) -> Option<&'static str> {
    match from {
        "execution_not_active" => Some(match to {
            // Terminal target — run / task never reached `active`
            // lifecycle, or it already transitioned to a terminal
            // state before the caller's request landed.
            "completed" | "failed" | "cancelled" => {
                "the run's execution is no longer in an active lease. \
                 The lease may have expired mid-loop, the run may \
                 already be terminal (check GET /v1/runs/:id), or the \
                 run was never claimed. Re-activate via POST \
                 /v1/runs/:id/claim and retry"
            }
            // Suspend / resume target — run is in a terminal phase
            // so pause / resume cannot apply.
            "suspended" | "active" => {
                "cannot pause or resume: the run is already terminal \
                 or its lease has expired. Check the run's current \
                 state via GET /v1/runs/:id"
            }
            _ => {
                "the execution is not in an active lease (check GET \
                 /v1/runs/:id for current state)"
            }
        }),
        "lease_expired" => Some(
            "the execution's lease expired before cairn could write \
             the terminal outcome. Extend the lease TTL for long-\
             running runs, or retry after re-claiming via POST \
             /v1/runs/:id/claim",
        ),
        // F62: the fabric_adapter F59 short-circuit fires this sentinel
        // when BOTH the terminal FCALL rejected with `lease_expired`
        // AND the recovery `claim` rejected with
        // `execution_not_eligible` / `execution_not_eligible_for_attempt`.
        // FF has no cairn-reachable path out of this state today, so
        // the run is wedged. The hint names the symptom (artifacts may
        // already exist on disk from the tool calls that ran before the
        // lease died) and points at the tracked FF upstream issue so
        // operators can correlate.
        // #489: the URL is the `upstream_ff_terminal_write_deadlock_url!()`
        // macro — one-line change when FF resolves the tracker. `concat!`
        // keeps the return type `&'static str` unchanged.
        "terminal_write_deadlock" => Some(concat!(
            "the orchestrator produced artifacts successfully but the \
             fabric refuses both the terminal write and the lease \
             re-claim. Files written by earlier tool calls may still \
             be visible on the operator's filesystem, but the run \
             cannot be closed without an upstream fabric fix. Tracked \
             at ",
            upstream_ff_terminal_write_deadlock_url!(),
        )),
        "lease_revoked" => Some(
            "the execution's lease was revoked by an operator or \
             scanner before this request completed. Check the run's \
             current state via GET /v1/runs/:id",
        ),
        "stale_lease" | "invalid_lease_for_suspend" => Some(
            "the lease token supplied does not match the execution's \
             current lease. Re-read the run (GET /v1/runs/:id) and \
             retry with the current lease fence",
        ),
        "fence_required" => Some(
            "this operation requires either an active lease fence or \
             an explicit operator override. Claim the run via POST \
             /v1/runs/:id/claim first",
        ),
        "partial_fence_triple" => Some(
            "internal error: cairn sent an inconsistent lease fence to \
             the fabric. This is a cairn bug (F37) — file an issue \
             with the run id",
        ),
        "already_suspended" => Some(
            "a suspension is already open for this run — resume or \
             cancel the existing suspension before starting a new one",
        ),
        _ => {
            // Avoid unused-warning on entity for callers that don't
            // need the generic hint; keep the parameter for future
            // entity-specific messages.
            let _ = entity;
            None
        }
    }
}

/// Payload for [`RuntimeError::DependencyConflict`]. Boxed in the
/// enum so the overall `RuntimeError` size stays small.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependencyConflictDetail {
    pub dependent_task_id: String,
    pub prerequisite_task_id: String,
    pub existing_kind: String,
    pub existing_data_passing_ref: Option<String>,
    pub requested_kind: String,
    pub requested_data_passing_ref: Option<String>,
}

impl From<cairn_store::StoreError> for RuntimeError {
    fn from(e: cairn_store::StoreError) -> Self {
        RuntimeError::Store(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// F41: the operator-facing message for an `execution_not_active`
    /// rejection on a terminal FCALL must NOT be raw FF jargon. It
    /// must name the failure in plain prose and tell the operator how
    /// to recover (re-activate via `/claim`, or check current state).
    #[test]
    fn invalid_transition_execution_not_active_completed_is_operator_actionable() {
        let err = RuntimeError::InvalidTransition {
            entity: "run",
            from: "execution_not_active".to_owned(),
            to: "completed".to_owned(),
        };
        let msg = err.to_string();
        // Must still mention the entity and target state for
        // dashboards / log greps.
        assert!(msg.contains("run"), "missing entity: {msg}");
        assert!(msg.contains("completed"), "missing target: {msg}");
        // Must carry an actionable verb.
        assert!(
            msg.contains("POST /v1/runs/:id/claim") || msg.contains("GET /v1/runs/:id"),
            "missing operator action hint: {msg}"
        );
        // Raw code preserved for correlation.
        assert!(
            msg.contains("execution_not_active"),
            "missing FF code for correlation: {msg}"
        );
        // Must NOT be the raw pre-F41 format.
        assert_ne!(
            msg, "invalid run transition: execution_not_active -> completed",
            "F41 regression: error reverted to raw FF jargon",
        );
    }

    #[test]
    fn invalid_transition_lease_expired_gets_hint() {
        let err = RuntimeError::InvalidTransition {
            entity: "run",
            from: "lease_expired".to_owned(),
            to: "completed".to_owned(),
        };
        let msg = err.to_string();
        assert!(msg.contains("lease"), "missing lease keyword: {msg}");
        assert!(msg.contains("lease_expired"), "missing code: {msg}");
    }

    #[test]
    fn invalid_transition_unknown_code_falls_back_to_raw_format() {
        // Unknown code → preserve the raw `{from} -> {to}` format so
        // operators can still see what happened. The hint table is
        // intentionally closed-world.
        let err = RuntimeError::InvalidTransition {
            entity: "task",
            from: "totally_novel_code".to_owned(),
            to: "completed".to_owned(),
        };
        let msg = err.to_string();
        assert_eq!(
            msg,
            "invalid task transition: totally_novel_code -> completed"
        );
    }

    #[test]
    fn is_transient_phase_conflict_matches_eligibility_codes() {
        // F58: the two codes FF's grant gate emits when an execution
        // is briefly in a non-`runnable` phase (mid-approval, resume,
        // tool-invocation aftermath) must be classified transient.
        for code in [
            "execution_not_eligible",
            "execution_not_eligible_for_attempt",
        ] {
            let err = RuntimeError::Conflict {
                entity: "execution",
                id: code.to_owned(),
            };
            assert!(
                err.is_transient_phase_conflict(),
                "expected `{code}` to be classified transient"
            );
        }
    }

    #[test]
    fn is_transient_phase_conflict_excludes_permanent_codes() {
        // Permanent failures MUST NOT be tolerated — swallowing them
        // would mask real 409s (terminal execution, deleted run,
        // revoked lease).
        for code in [
            "execution_not_active",
            "lease_expired",
            "lease_revoked",
            "execution_not_found",
            "grant_already_exists",
            "stale_lease",
        ] {
            let err = RuntimeError::Conflict {
                entity: "execution",
                id: code.to_owned(),
            };
            assert!(
                !err.is_transient_phase_conflict(),
                "expected `{code}` NOT to be classified transient"
            );
        }
    }

    #[test]
    fn is_transient_phase_conflict_excludes_non_conflict_variants() {
        assert!(!RuntimeError::NotFound {
            entity: "run",
            id: "x".into()
        }
        .is_transient_phase_conflict());
        assert!(!RuntimeError::Internal("something".into()).is_transient_phase_conflict());
        assert!(!RuntimeError::InvalidTransition {
            entity: "run",
            from: "execution_not_eligible".into(),
            to: "active".into(),
        }
        .is_transient_phase_conflict());
        // Conflict on a different entity (not `execution`) is out of
        // scope — only executions have the phase-gated FCALLs.
        assert!(!RuntimeError::Conflict {
            entity: "task",
            id: "execution_not_eligible".into(),
        }
        .is_transient_phase_conflict());
    }

    #[test]
    fn is_lease_expired_matches_terminal_fcall_classifier_shape() {
        // F59: only the `InvalidTransition { from: "lease_expired", .. }`
        // shape the fabric adapter emits for terminal FCALLs must
        // classify as lease-expired. Any other combination must return
        // false so reclaim-retry logic does not fire on unrelated
        // transitions.
        for to in ["completed", "failed", "cancelled"] {
            let err = RuntimeError::InvalidTransition {
                entity: "run",
                from: "lease_expired".to_owned(),
                to: to.to_owned(),
            };
            assert!(
                err.is_lease_expired(),
                "expected is_lease_expired for transition to {to}"
            );
        }
    }

    #[test]
    fn is_lease_expired_excludes_other_codes() {
        for code in [
            "execution_not_active",
            "lease_revoked",
            "stale_lease",
            "fence_required",
            "partial_fence_triple",
            "totally_novel_code",
        ] {
            let err = RuntimeError::InvalidTransition {
                entity: "run",
                from: code.to_owned(),
                to: "completed".to_owned(),
            };
            assert!(
                !err.is_lease_expired(),
                "expected NOT lease_expired for code {code}"
            );
        }
        // Non-InvalidTransition variants never classify as lease-expired.
        assert!(!RuntimeError::Conflict {
            entity: "execution",
            id: "lease_expired".into(),
        }
        .is_lease_expired());
        assert!(!RuntimeError::LeaseExpired {
            task_id: "t".into()
        }
        .is_lease_expired());
        assert!(!RuntimeError::Internal("lease_expired".into()).is_lease_expired());
    }

    /// F62: the `terminal_write_deadlock` sentinel must render an
    /// operator-actionable message that:
    ///   * names the symptom (artifacts may still be on disk from
    ///     earlier tool calls that ran before the lease died)
    ///   * links the tracked FF upstream issue so the operator can
    ///     correlate the deadlock against known history
    ///   * preserves the sentinel in the `code=` suffix for log grep
    #[test]
    fn invalid_transition_terminal_write_deadlock_points_at_artifacts_and_upstream_issue() {
        for to in ["completed", "failed", "cancelled"] {
            let err = RuntimeError::InvalidTransition {
                entity: "run",
                from: "terminal_write_deadlock".to_owned(),
                to: to.to_owned(),
            };
            let msg = err.to_string();
            assert!(
                msg.contains("artifacts") || msg.contains("filesystem"),
                "F62: missing artifact-preservation hint for to={to}: {msg}"
            );
            // #489: pull the URL from the same source-of-truth macro the
            // production code uses, so renumbering the FF tracker flows
            // through both sites in one edit.
            let expected_url = upstream_ff_terminal_write_deadlock_url!();
            assert!(
                msg.contains(expected_url),
                "F62: missing FF upstream issue link ({expected_url}) for to={to}: {msg}"
            );
            assert!(
                msg.contains("code=terminal_write_deadlock"),
                "F62: missing code suffix for log grep (to={to}): {msg}"
            );
        }
    }

    #[test]
    fn invalid_transition_execution_not_active_suspend_distinct_hint() {
        // Suspend target emits a different hint than terminal target
        // (different recovery path).
        let err = RuntimeError::InvalidTransition {
            entity: "run",
            from: "execution_not_active".to_owned(),
            to: "suspended".to_owned(),
        };
        let msg = err.to_string();
        assert!(
            msg.contains("pause or resume") || msg.contains("already terminal"),
            "suspend hint should differ from terminal: {msg}"
        );
    }
}
