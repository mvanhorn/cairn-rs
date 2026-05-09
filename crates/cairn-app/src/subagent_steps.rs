//! #670 G7: surface completed subagent runs onto the parent's
//! `step_history` so the parent's next orchestrator decide turn
//! sees what each child actually accomplished.
//!
//! Without this the parent's LLM auto-resumes (G5) with an empty
//! step_history and no idea that its delegation produced anything —
//! so it either re-spawns the same subagent or completes without
//! using the child's output.
//!
//! # Mechanism
//!
//! 1. [`build_subagent_complete_steps`] reads `list_child_runs` on
//!    the parent's run id, filters to terminal children with a
//!    `completion_summary` present, and converts each into a
//!    [`StepSummary`] with `action_kind = "subagent_complete"`.
//! 2. The caller ([`drive_run_iteration`] in
//!    `handlers/runs/orchestrate.rs`) prepends these into the fresh
//!    `OrchestrationContext.step_history` before the first decide
//!    phase runs.
//!
//! The step_history then flows through the orchestrator's existing
//! `GatherPhase → DecidePhase` prompt-assembly path (see
//! `cairn_orchestrator::decide_impl::build_user_prompt`) so the
//! child's summary lands verbatim in the parent's next LLM prompt.
//!
//! # No "already-surfaced" tracking
//!
//! The helper unconditionally surfaces every terminal child with a
//! summary on every parent drive. We accept the duplication:
//!
//! - `step_history` is ephemeral within a single HTTP request —
//!   surfacing a child's summary twice across two auto-resume
//!   kicks is re-injecting the same text the LLM saw once
//!   before, which modern models tolerate as a no-op.
//! - Adding a `surfaced_to_parent_at_ms` column on
//!   `subagent_spawns` would persist state across the request
//!   boundary but also require an UPDATE on every drive, which
//!   trades a read-only surface for a write-per-iteration.
//!
//! If duplicate-summary noise becomes a concern we can revisit with
//! a per-drive iteration-number check (skip children whose
//! completion time predates the parent's previous auto-resume
//! timestamp), but that mechanism is not in scope for G7.
//!
//! # Empty-summary handling
//!
//! Children that terminate without a `completion_summary` (e.g. a
//! child that Failed with no annotation, or a legacy child projected
//! from a pre-F47-PR2 event log) are skipped. The parent is still
//! notified about the child's existence via the usual
//! `/children` surface — G7's job is to bridge the child's LLM
//! output into the parent's prompt, and an empty summary has no
//! output to bridge.

use cairn_domain::{RunId, RunState};
use cairn_orchestrator::context::StepSummary;
use cairn_runtime::RunService;

/// Build `StepSummary` entries for every terminal child of
/// `parent_run_id` whose projection carries a `completion_summary`.
///
/// Errors from the underlying `list_child_runs` call return `Ok(vec![])`
/// (the drive path should not fail just because the step-history seed
/// couldn't be populated — the run will still execute, just without
/// the G7 seed). Failures are logged at WARN by the caller.
///
/// # Ordering
///
/// Returns entries in **chronological** order (oldest-to-newest),
/// matching the `step_history` invariant that new steps are
/// appended at the end during the loop. Because `list_child_runs`
/// returns children newest-first, the helper reverses them before
/// materialising `StepSummary`s. The prompt renderer
/// (`decide_impl::build_user_prompt`) then reverses again under its
/// "most recent first" header — so the end-state is correct both
/// in memory (chronological) and in the prompt (most-recent-first).
///
/// # Iteration stamp
///
/// Each emitted `StepSummary` stamps `iteration = 0` because the
/// child's terminal happened OUTSIDE the parent's current
/// iteration sequence — the step is being retrofit onto the
/// parent's history rather than produced by one of the parent's
/// own iterations. The orchestrator's prompt renderer treats
/// iteration purely as display metadata, so `0` doesn't collide
/// with the parent's real iteration numbers.
pub async fn build_subagent_complete_steps<R: RunService + ?Sized>(
    runs: &R,
    parent_run_id: &RunId,
    limit: usize,
) -> Vec<StepSummary> {
    // Read with a small bounded retry to close the completion-summary
    // race with G5 auto-resume:
    //
    //   1. Child orchestrator loop fires `runs.complete` (inside the
    //      loop). That call emits `BridgeEvent::ExecutionCompleted`
    //      and triggers G5's `fire_parent_resume_if_child` which
    //      spawns a tokio task to re-drive the parent.
    //   2. The loop returns `LoopTermination::Completed`.
    //   3. `drive_run_iteration`'s post-loop match appends
    //      `RunCompletionAnnotated` carrying the child's
    //      `completion_summary`.
    //
    // Steps 1 and 3 happen in the same process and are typically
    // microseconds apart, but the G5 tokio-spawned resume can race
    // step 3. If we read the parent's children projection before
    // the annotation lands, `completion_summary` is `None` and G7
    // seeding is a no-op. Brief retry covers the common case
    // without turning a read into a long-polling storm.
    const MAX_ATTEMPTS: usize = 10;
    const SLEEP_MS: u64 = 50;

    for attempt in 0..MAX_ATTEMPTS {
        let children = match runs.list_child_runs(parent_run_id, limit).await {
            Ok(rows) => rows,
            Err(err) => {
                tracing::warn!(
                    parent_run_id = %parent_run_id,
                    error = %err,
                    "G7: list_child_runs failed — parent will decide without \
                     subagent_complete seeds",
                );
                return Vec::new();
            }
        };

        // Terminal children that already carry a summary are ready
        // to surface. Children still missing a summary are either
        // the annotation-race case (brief wait) OR children that
        // genuinely have no summary (pre-F47 projections, fails
        // without annotation). We distinguish by counting: if at
        // least one terminal child is missing a summary AND at
        // least one terminal child exists, wait a bit; otherwise
        // return what we have.
        let mut terminal_missing_summary = false;
        let mut any_terminal = false;
        let mut steps = Vec::new();
        // Iterate children in reverse: `list_child_runs` returns
        // newest-first, but `step_history` is chronological
        // (oldest-to-newest — new steps are pushed at the end during
        // a run). The prompt renderer (`decide_impl::build_user_prompt`)
        // reverses again when printing, rendering "most recent first"
        // — so if we hand it a newest-first slice it would render
        // backwards under the "most recent first" header. Building
        // chronologically keeps the invariant: the prepended
        // subagent_complete block reads oldest-first in memory,
        // newest-first in the prompt.
        for child in children.iter().rev() {
            if !matches!(
                child.state,
                RunState::Completed | RunState::Failed | RunState::Canceled
            ) {
                continue;
            }
            any_terminal = true;
            match child.completion_summary.as_deref() {
                Some(summary_text) if !summary_text.trim().is_empty() => {
                    let succeeded = matches!(child.state, RunState::Completed);
                    steps.push(StepSummary {
                        iteration: 0,
                        action_kind: "subagent_complete".to_owned(),
                        summary: format!(
                            "child {child_id} ({state}): {body}",
                            child_id = child.run_id.as_str(),
                            state = run_state_label(child.state),
                            body = summary_text,
                        ),
                        succeeded,
                    });
                }
                None if matches!(child.state, RunState::Completed) => {
                    // Annotation-race case: the child hit
                    // `LoopTermination::Completed` but
                    // `drive_run_iteration` hasn't appended
                    // `RunCompletionAnnotated` yet. Brief wait covers
                    // this.
                    terminal_missing_summary = true;
                }
                _ => {
                    // Non-waitable cases:
                    //   - Failed / Canceled without summary: expected
                    //     (finalize_run_failure emits no annotation).
                    //   - Completed with empty-string summary: the
                    //     annotation HAS landed, the model returned
                    //     no content. No amount of retry changes that.
                    // Skip surfacing and skip the retry delay.
                }
            }
        }

        // Done if no race is possible (no terminal children at all,
        // or every terminal child already has a summary, or only
        // Failed/Canceled children remain).
        let race_possible = any_terminal && terminal_missing_summary;
        if !race_possible || attempt == MAX_ATTEMPTS - 1 {
            return steps;
        }

        tokio::time::sleep(std::time::Duration::from_millis(SLEEP_MS)).await;
    }
    // Unreachable: the loop always returns on the last attempt.
    Vec::new()
}

/// Lowercase human-readable label for the terminal run states we
/// surface. Kept tight so the step's `summary` field stays short —
/// the LLM only needs the dominant signal (Completed vs Failed) to
/// decide what to do next.
fn run_state_label(state: RunState) -> &'static str {
    match state {
        RunState::Completed => "completed",
        RunState::Failed => "failed",
        RunState::Canceled => "canceled",
        _ => "non-terminal", // unreachable by construction; keeps `&'static str`
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use cairn_domain::{ProjectKey, RunState, SessionId};
    use cairn_runtime::{RunService, RuntimeError};
    use cairn_store::projections::RunRecord;

    fn mk_child(id: &str, state: RunState, summary: Option<&str>) -> RunRecord {
        RunRecord {
            run_id: RunId::new(id),
            session_id: SessionId::new("sess"),
            parent_run_id: Some(RunId::new("parent")),
            project: ProjectKey::new("t", "w", "p"),
            state,
            prompt_release_id: None,
            agent_role_id: None,
            failure_class: None,
            pause_reason: None,
            resume_trigger: None,
            version: 1,
            created_at: 0,
            updated_at: 0,
            completion_summary: summary.map(str::to_owned),
            completion_verification: None,
            completion_annotated_at_ms: None,
            terminal_write_recovery: None,
            in_flight_descendants: 0,
            root_run_id: Some(RunId::new("parent")),
            iteration: 0,
        }
    }

    /// Minimal RunService fake that only implements the methods the
    /// helper actually calls. Methods not on the helper's call path
    /// `unreachable!` so accidental helper-expansion into the full
    /// trait is caught by test failure rather than silent default
    /// success.
    struct FakeRuns {
        children: Vec<RunRecord>,
    }

    #[async_trait]
    impl RunService for FakeRuns {
        async fn start(
            &self,
            _: &ProjectKey,
            _: &SessionId,
            _: RunId,
            _: Option<RunId>,
        ) -> Result<RunRecord, RuntimeError> {
            unreachable!("start not used by helper test")
        }
        async fn get(&self, _: &RunId) -> Result<Option<RunRecord>, RuntimeError> {
            unreachable!()
        }
        async fn list_by_session(
            &self,
            _: &SessionId,
            _: usize,
            _: usize,
        ) -> Result<Vec<RunRecord>, RuntimeError> {
            unreachable!()
        }
        async fn complete(&self, _: &SessionId, _: &RunId) -> Result<RunRecord, RuntimeError> {
            unreachable!()
        }
        async fn fail(
            &self,
            _: &SessionId,
            _: &RunId,
            _: cairn_domain::FailureClass,
        ) -> Result<RunRecord, RuntimeError> {
            unreachable!()
        }
        async fn cancel(&self, _: &SessionId, _: &RunId) -> Result<RunRecord, RuntimeError> {
            unreachable!()
        }
        async fn pause(
            &self,
            _: &SessionId,
            _: &RunId,
            _: cairn_domain::PauseReason,
        ) -> Result<RunRecord, RuntimeError> {
            unreachable!()
        }
        async fn resume(
            &self,
            _: &SessionId,
            _: &RunId,
            _: cairn_domain::ResumeTrigger,
            _: cairn_domain::RunResumeTarget,
        ) -> Result<RunRecord, RuntimeError> {
            unreachable!()
        }
        async fn claim(&self, _: &SessionId, _: &RunId) -> Result<RunRecord, RuntimeError> {
            unreachable!()
        }
        async fn enter_waiting_approval(
            &self,
            _: &SessionId,
            _: &RunId,
        ) -> Result<RunRecord, RuntimeError> {
            unreachable!()
        }
        async fn resolve_approval(
            &self,
            _: &SessionId,
            _: &RunId,
            _: cairn_domain::ApprovalDecision,
        ) -> Result<RunRecord, RuntimeError> {
            unreachable!()
        }
        async fn list_child_runs(
            &self,
            _: &RunId,
            _: usize,
        ) -> Result<Vec<RunRecord>, RuntimeError> {
            Ok(self.children.clone())
        }
    }

    #[tokio::test]
    async fn empty_when_no_children() {
        let runs = FakeRuns { children: vec![] };
        let steps = build_subagent_complete_steps(&runs, &RunId::new("parent"), 10).await;
        assert!(steps.is_empty());
    }

    #[tokio::test]
    async fn skips_non_terminal_children() {
        let runs = FakeRuns {
            children: vec![
                mk_child("c1", RunState::Running, Some("ignored")),
                mk_child("c2", RunState::Pending, Some("ignored")),
                mk_child("c3", RunState::Paused, Some("ignored")),
                mk_child("c4", RunState::WaitingDependency, Some("ignored")),
            ],
        };
        let steps = build_subagent_complete_steps(&runs, &RunId::new("parent"), 10).await;
        assert!(steps.is_empty(), "got {} steps", steps.len());
    }

    #[tokio::test]
    async fn skips_terminal_without_summary() {
        let runs = FakeRuns {
            children: vec![
                mk_child("c1", RunState::Completed, None),
                mk_child("c2", RunState::Failed, None),
                mk_child("c3", RunState::Canceled, Some("   ")), // whitespace-only counts as empty
            ],
        };
        let steps = build_subagent_complete_steps(&runs, &RunId::new("parent"), 10).await;
        assert!(steps.is_empty(), "got {} steps", steps.len());
    }

    #[tokio::test]
    async fn surfaces_completed_child_with_summary() {
        let runs = FakeRuns {
            children: vec![mk_child(
                "c1",
                RunState::Completed,
                Some("researcher found 3 references"),
            )],
        };
        let steps = build_subagent_complete_steps(&runs, &RunId::new("parent"), 10).await;
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].action_kind, "subagent_complete");
        assert!(steps[0].succeeded);
        assert!(
            steps[0].summary.contains("c1"),
            "summary should include child id: {}",
            steps[0].summary,
        );
        assert!(
            steps[0].summary.contains("completed"),
            "summary should include state label: {}",
            steps[0].summary,
        );
        assert!(
            steps[0].summary.contains("researcher found 3 references"),
            "summary should include child's completion_summary: {}",
            steps[0].summary,
        );
    }

    #[tokio::test]
    async fn failed_child_marks_succeeded_false() {
        let runs = FakeRuns {
            children: vec![mk_child(
                "c_fail",
                RunState::Failed,
                Some("child hit max_iterations"),
            )],
        };
        let steps = build_subagent_complete_steps(&runs, &RunId::new("parent"), 10).await;
        assert_eq!(steps.len(), 1);
        assert!(!steps[0].succeeded);
        assert!(steps[0].summary.contains("failed"));
    }

    #[tokio::test]
    async fn canceled_child_also_succeeded_false() {
        let runs = FakeRuns {
            children: vec![mk_child(
                "c_cancel",
                RunState::Canceled,
                Some("operator cancelled"),
            )],
        };
        let steps = build_subagent_complete_steps(&runs, &RunId::new("parent"), 10).await;
        assert_eq!(steps.len(), 1);
        assert!(!steps[0].succeeded);
        assert!(steps[0].summary.contains("canceled"));
    }

    #[tokio::test]
    async fn multiple_children_reversed_for_chronological_order() {
        // `list_child_runs` returns newest-first. `step_history` is
        // chronological (oldest-to-newest — new steps are appended
        // at the end during a run). The helper reverses the adapter's
        // output to preserve that invariant. Here `c1` is the
        // newest child in the adapter's response and should land
        // LAST in the steps vec so it reads as the most-recent step.
        let runs = FakeRuns {
            children: vec![
                mk_child("c1", RunState::Completed, Some("newest summary")),
                mk_child("c2", RunState::Failed, Some("middle summary")),
                mk_child("c3", RunState::Completed, Some("oldest summary")),
            ],
        };
        let steps = build_subagent_complete_steps(&runs, &RunId::new("parent"), 10).await;
        assert_eq!(steps.len(), 3);
        assert!(
            steps[0].summary.contains("oldest summary"),
            "oldest child should render first in chronological order; got: {}",
            steps[0].summary,
        );
        assert!(
            steps[1].summary.contains("middle summary"),
            "middle child second; got: {}",
            steps[1].summary,
        );
        assert!(
            steps[2].summary.contains("newest summary"),
            "newest child last in chronological order; got: {}",
            steps[2].summary,
        );
    }
}
