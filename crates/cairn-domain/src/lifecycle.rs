use serde::{Deserialize, Serialize};

/// Session-level lifecycle for long-lived execution context.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Open,
    Completed,
    Failed,
    Archived,
}

/// Run lifecycle inside a session.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Pending,
    Running,
    WaitingApproval,
    Paused,
    WaitingDependency,
    Completed,
    Failed,
    Canceled,
}

/// Task lifecycle for leased and schedulable work.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Queued,
    Leased,
    Running,
    WaitingApproval,
    Paused,
    WaitingDependency,
    RetryableFailed,
    Completed,
    Failed,
    Canceled,
    DeadLettered,
}

/// Checkpoint disposition is intentionally narrow in v1.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointDisposition {
    Latest,
    Superseded,
}

/// Failure classes remain explicit even when timeout collapses into failed state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    TimedOut,
    DependencyFailed,
    ApprovalRejected,
    PolicyDenied,
    ExecutionError,
    LeaseExpired,
    CanceledByOperator,
    /// F62: terminal FCALL (`complete` / `fail` / `cancel`) could not be
    /// written because the FF execution was simultaneously out of lease
    /// (`lease_expired` on the FCALL) AND out of `runnable` phase
    /// (`execution_not_eligible` on the retry re-claim). Neither of FF's
    /// documented recovery paths applies in this state; the execution is
    /// wedged until FF lands the upstream fix tracked in
    /// [FlowFabric#371](https://github.com/avifenesh/FlowFabric/issues/371).
    /// Cairn marks the run `Failed` with this class so operators see
    /// the deadlock as a terminal state (rather than a zombie `running`
    /// row) and can correlate against the upstream issue.
    TerminalWriteDeadlock,
    /// #660: the orchestrator's strict completion gate refused the LLM's
    /// `complete_run` action because the F47 `completion_verification`
    /// sidecar reported one or more errors (typically a failing
    /// `cargo build` / `cargo check`). The loop re-enters DECIDE so the
    /// model can fix the diagnostics; after three consecutive rejections
    /// the run terminates with this class so operators can distinguish
    /// "model could not converge past a failing build" from a generic
    /// `ExecutionError`. Flag: `orchestrator_strict_completion_gate`
    /// (default `true`). See RFC-F47 + issue #660.
    VerificationRejected,
    /// #670 G4 / RFC 027 §Orphan-child: a child subagent run whose
    /// spawn failed between Phase-1 (child `RunRecord` created) and
    /// Phase-2 (task submitted), leaving a `Pending` row with no
    /// driver claiming it. The Child Run Driver adapter fails such
    /// runs synchronously on Phase-2 failure (PR-1b-3); the operator
    /// endpoint `POST /v1/admin/tenants/:tenant_id/runs/:id/cancel-orphan`
    /// provides the manual recovery path for runs wedged by a crash
    /// between those two phases (e.g. SIGKILL on cairn-app mid-spawn).
    ///
    /// Only valid for non-root child runs; the operator endpoint
    /// rejects orphan-cancel on root runs (roots have no parent to
    /// have leaked them). The `Failed` terminal fires the standard
    /// descendant-counter decrement path, releasing the cap slot on
    /// the captured `root_run_id`.
    OrphanChild,
    /// #750: every provider binding × model in the routed chain failed
    /// with fallback-eligible errors during a child subagent run. The
    /// child terminates `Failed` (rather than suspending in
    /// `WaitingApproval` per #693 R3-B) so G5's `child_completed`
    /// signal fires with `success=false` and the parent run's
    /// `drive_run_iteration` resumes with the failure visible in
    /// `step_history`. Top-level (operator-initiated) runs continue to
    /// suspend in `WaitingApproval` with an `escalate_to_operator`
    /// approval card so the operator can rotate credentials, add
    /// providers, or abort — only child runs short-circuit to terminal
    /// because no operator is watching their dashboard. See #693
    /// R3-B for the original waiting_approval transition and #750 for
    /// the parent-stuck-in-waiting_dependency bug this resolves.
    AllProvidersExhausted,
    /// #825: the agent emitted `ActionType::FailRun` — a truthful
    /// self-report of "I tried, I cannot proceed." R26 dogfood
    /// surfaced the gap: before `FailRun` existed, a model that
    /// correctly diagnosed its own block (e.g. "M1-7 needs M1-1
    /// first") had only `CompleteRun` available as a terminal verb,
    /// so it would call complete_run with a summary saying "Status:
    /// Blocked" and the run flipped to `state=completed`. This
    /// variant lets operator dashboards distinguish agent-declared
    /// failure (usually: missing precondition, contradictory goal,
    /// dependency not met) from `ExecutionError` (infrastructure
    /// fault), `VerificationRejected` (model lied about a passing
    /// build), and `ApprovalRejected` (operator declined).
    ///
    /// When the agent should prefer which terminal:
    /// * `CompleteRun` — deliverable exists AND verification is clean.
    /// * `FailRun` / `ModelReportedFailure` — the agent cannot produce
    ///   the deliverable and no operator intervention would change
    ///   that outcome.
    /// * `EscalateToOperator` — the agent is paused waiting for
    ///   specific operator input (approval, rotated credential,
    ///   clarification) and resuming IS feasible.
    ModelReportedFailure,
    /// RFC 032: the completion-contract verifier rejected the LLM's
    /// `complete_run` because the claimed deliverable does not exist
    /// (no PR at the declared URL, file missing, prose too short +
    /// under-cited, JSON Schema mismatch, etc). Distinct from
    /// `VerificationRejected` (model lied or admitted) and
    /// `ModelReportedFailure` (model truthfully called `fail_run`).
    ///
    /// The specific rejection reason (a stable snake_case code from
    /// [`cairn_domain::completion_contracts::ContractRejectionCode`])
    /// surfaces via the structured diagnostic threaded into
    /// `step_history` at gate time — not on the `FailureClass`
    /// itself, so the enum stays `Copy` + single-tag snake_case on
    /// the wire like every other variant.
    ///
    /// PR-4 (gate integration) wires emission. PR-2 lands the
    /// variant so projections + operator dashboards are ready.
    ContractNotMet,
}

/// Canonical pause reasons in v1.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PauseReason {
    pub kind: PauseReasonKind,
    pub detail: Option<String>,
    /// Optional timestamp (unix ms) after which the entity should auto-resume.
    pub resume_after_ms: Option<u64>,
    /// Actor (operator or system) who initiated the pause.
    #[serde(default)]
    pub actor: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PauseReasonKind {
    OperatorPause,
    RuntimeSuspension,
    ToolRequestedSuspension,
    PolicyHold,
}

/// Resume trigger sources are deliberately explicit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResumeTrigger {
    OperatorResume,
    ResumeAfterTimer,
    RuntimeSignal,
}

/// Explicit resume targets keep pause/resume semantics narrow.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunResumeTarget {
    Pending,
    Running,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskResumeTarget {
    Queued,
    Running,
}

impl SessionState {
    pub fn is_terminal(self) -> bool {
        !matches!(self, SessionState::Open)
    }
}

impl RunState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            RunState::Completed | RunState::Failed | RunState::Canceled
        )
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        can_transition_run_state(self, next)
    }
}

impl TaskState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TaskState::Completed
                | TaskState::Failed
                | TaskState::Canceled
                | TaskState::DeadLettered
        )
    }

    pub fn is_retryable(self) -> bool {
        matches!(self, TaskState::RetryableFailed)
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        can_transition_task_state(self, next)
    }
}

/// Canonical v1 run-state transition helper from RFC 005.
pub fn can_transition_run_state(from: RunState, to: RunState) -> bool {
    matches!(
        (from, to),
        (RunState::Pending, RunState::Running)
            | (RunState::Pending, RunState::WaitingApproval)
            | (RunState::Pending, RunState::WaitingDependency)
            | (RunState::Pending, RunState::Paused)
            | (RunState::Pending, RunState::Failed)
            | (RunState::Pending, RunState::Canceled)
            | (RunState::Running, RunState::WaitingApproval)
            | (RunState::Running, RunState::WaitingDependency)
            | (RunState::Running, RunState::Paused)
            | (RunState::Running, RunState::Completed)
            | (RunState::Running, RunState::Failed)
            | (RunState::Running, RunState::Canceled)
            | (RunState::WaitingApproval, RunState::Running)
            | (RunState::WaitingApproval, RunState::Paused)
            | (RunState::WaitingApproval, RunState::Failed)
            | (RunState::WaitingApproval, RunState::Canceled)
            | (RunState::Paused, RunState::Pending)
            | (RunState::Paused, RunState::Running)
            | (RunState::Paused, RunState::Failed)
            | (RunState::Paused, RunState::Canceled)
            | (RunState::WaitingDependency, RunState::Running)
            | (RunState::WaitingDependency, RunState::Paused)
            | (RunState::WaitingDependency, RunState::Completed)
            | (RunState::WaitingDependency, RunState::Failed)
            | (RunState::WaitingDependency, RunState::Canceled)
    )
}

/// Canonical v1 task-state transition helper from RFC 005.
pub fn can_transition_task_state(from: TaskState, to: TaskState) -> bool {
    matches!(
        (from, to),
        (TaskState::Queued, TaskState::Leased)
            | (TaskState::Queued, TaskState::WaitingDependency)
            | (TaskState::Queued, TaskState::Paused)
            | (TaskState::Queued, TaskState::Canceled)
            | (TaskState::Queued, TaskState::DeadLettered)
            | (TaskState::WaitingDependency, TaskState::Queued)
            | (TaskState::Leased, TaskState::Running)
            | (TaskState::Leased, TaskState::Queued)
            | (TaskState::Leased, TaskState::Paused)
            | (TaskState::Leased, TaskState::RetryableFailed)
            | (TaskState::Leased, TaskState::Failed)
            | (TaskState::Leased, TaskState::Canceled)
            | (TaskState::Leased, TaskState::DeadLettered)
            | (TaskState::Running, TaskState::WaitingApproval)
            | (TaskState::Running, TaskState::WaitingDependency)
            | (TaskState::Running, TaskState::Paused)
            | (TaskState::Running, TaskState::Completed)
            | (TaskState::Running, TaskState::RetryableFailed)
            | (TaskState::Running, TaskState::Failed)
            | (TaskState::Running, TaskState::Canceled)
            | (TaskState::WaitingApproval, TaskState::Running)
            | (TaskState::WaitingApproval, TaskState::Paused)
            | (TaskState::WaitingApproval, TaskState::RetryableFailed)
            | (TaskState::WaitingApproval, TaskState::Failed)
            | (TaskState::WaitingApproval, TaskState::Canceled)
            | (TaskState::Paused, TaskState::Queued)
            | (TaskState::Paused, TaskState::Running)
            | (TaskState::Paused, TaskState::RetryableFailed)
            | (TaskState::Paused, TaskState::Failed)
            | (TaskState::Paused, TaskState::Canceled)
            | (TaskState::WaitingDependency, TaskState::Running)
            | (TaskState::WaitingDependency, TaskState::Paused)
            | (TaskState::WaitingDependency, TaskState::Completed)
            | (TaskState::WaitingDependency, TaskState::RetryableFailed)
            | (TaskState::WaitingDependency, TaskState::Failed)
            | (TaskState::WaitingDependency, TaskState::Canceled)
            | (TaskState::RetryableFailed, TaskState::Queued)
            | (TaskState::RetryableFailed, TaskState::Failed)
            | (TaskState::RetryableFailed, TaskState::Canceled)
            | (TaskState::RetryableFailed, TaskState::DeadLettered)
    )
}

pub fn can_resume_run_to(target: RunResumeTarget) -> bool {
    matches!(target, RunResumeTarget::Pending | RunResumeTarget::Running)
}

pub fn can_resume_task_to(target: TaskResumeTarget) -> bool {
    matches!(target, TaskResumeTarget::Queued | TaskResumeTarget::Running)
}

/// Operational status of an agent session in the fleet view.
///
/// - `Busy`    — session has a running or claimed task
/// - `Idle`    — session is open but has no active task (queued or paused)
/// - `Offline` — session is terminal (completed / failed / archived)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    Busy,
    Idle,
    Offline,
}

impl AgentStatus {
    /// Derive agent status from the session's current `RunState` (if any).
    pub fn from_run_state(run_state: Option<RunState>) -> Self {
        match run_state {
            Some(RunState::Running) => AgentStatus::Busy,
            Some(RunState::Pending | RunState::WaitingApproval | RunState::Paused) => {
                AgentStatus::Idle
            }
            Some(RunState::Completed | RunState::Failed | RunState::Canceled) | None => {
                AgentStatus::Offline
            }
            Some(RunState::WaitingDependency) => AgentStatus::Idle,
        }
    }
}

/// Derives session state using the v1 rules from RFC 005.
pub fn derive_session_state(
    is_archived: bool,
    any_run_non_terminal: bool,
    latest_root_run_terminal: Option<RunState>,
) -> SessionState {
    if is_archived {
        SessionState::Archived
    } else if any_run_non_terminal {
        SessionState::Open
    } else {
        match latest_root_run_terminal {
            Some(RunState::Failed) => SessionState::Failed,
            Some(RunState::Completed | RunState::Canceled) => SessionState::Completed,
            _ => SessionState::Open,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        can_resume_run_to, can_resume_task_to, derive_session_state, FailureClass, RunResumeTarget,
        RunState, SessionState, TaskResumeTarget, TaskState,
    };

    #[test]
    fn failure_class_model_reported_failure_serialises_snake_case() {
        // #825: the wire shape `"model_reported_failure"` is the
        // contract between the orchestrator's reason-string classifier
        // (classify_failed_reason) and operator dashboards / API
        // consumers. Any rename breaks both.
        assert_eq!(
            serde_json::to_string(&FailureClass::ModelReportedFailure).unwrap(),
            r#""model_reported_failure""#
        );
        let decoded: FailureClass = serde_json::from_str(r#""model_reported_failure""#).unwrap();
        assert_eq!(decoded, FailureClass::ModelReportedFailure);
    }

    #[test]
    fn failure_class_contract_not_met_serialises_snake_case() {
        // RFC 032: `contract_not_met` is the wire shape operator
        // dashboards use to distinguish "claimed deliverable doesn't
        // exist" from `verification_rejected` (model lied) and
        // `model_reported_failure` (model gave up). PR-4 emits this
        // from the gate when the resolved contract rejects.
        assert_eq!(
            serde_json::to_string(&FailureClass::ContractNotMet).unwrap(),
            r#""contract_not_met""#
        );
        let decoded: FailureClass = serde_json::from_str(r#""contract_not_met""#).unwrap();
        assert_eq!(decoded, FailureClass::ContractNotMet);
    }

    #[test]
    fn session_derivation_prefers_archive() {
        let state = derive_session_state(true, true, Some(RunState::Failed));
        assert_eq!(state, SessionState::Archived);
    }

    #[test]
    fn session_stays_open_when_any_run_is_non_terminal() {
        let state = derive_session_state(false, true, Some(RunState::Completed));
        assert_eq!(state, SessionState::Open);
    }

    #[test]
    fn session_follows_latest_root_run_terminal_outcome() {
        assert_eq!(
            derive_session_state(false, false, Some(RunState::Failed)),
            SessionState::Failed
        );
        assert_eq!(
            derive_session_state(false, false, Some(RunState::Completed)),
            SessionState::Completed
        );
        assert_eq!(
            derive_session_state(false, false, Some(RunState::Canceled)),
            SessionState::Completed
        );
    }

    #[test]
    fn task_retryable_failed_is_not_terminal() {
        assert!(TaskState::RetryableFailed.is_retryable());
        assert!(!TaskState::RetryableFailed.is_terminal());
        assert!(TaskState::DeadLettered.is_terminal());
    }

    #[test]
    fn run_state_transitions_follow_v1_contract() {
        assert!(RunState::Pending.can_transition_to(RunState::Running));
        assert!(RunState::Running.can_transition_to(RunState::Paused));
        assert!(RunState::Paused.can_transition_to(RunState::Running));
        assert!(RunState::WaitingDependency.can_transition_to(RunState::Completed));
        assert!(!RunState::Completed.can_transition_to(RunState::Running));
        assert!(!RunState::Canceled.can_transition_to(RunState::Pending));
    }

    #[test]
    fn task_state_transitions_follow_v1_contract() {
        assert!(TaskState::Queued.can_transition_to(TaskState::Leased));
        assert!(TaskState::Leased.can_transition_to(TaskState::Running));
        assert!(TaskState::Running.can_transition_to(TaskState::RetryableFailed));
        assert!(TaskState::RetryableFailed.can_transition_to(TaskState::Queued));
        assert!(TaskState::Paused.can_transition_to(TaskState::Running));
        assert!(!TaskState::Completed.can_transition_to(TaskState::Running));
        assert!(!TaskState::DeadLettered.can_transition_to(TaskState::Queued));
    }

    #[test]
    fn resume_targets_remain_narrow() {
        assert!(can_resume_run_to(RunResumeTarget::Pending));
        assert!(can_resume_run_to(RunResumeTarget::Running));
        assert!(can_resume_task_to(TaskResumeTarget::Queued));
        assert!(can_resume_task_to(TaskResumeTarget::Running));
    }
}
