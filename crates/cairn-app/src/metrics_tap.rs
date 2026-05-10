//! Event-log subscriber that derives lifecycle metrics from cairn's
//! canonical RuntimeEvent stream.
//!
//! Why a tap instead of direct instrumentation: the services that emit
//! `RunCreated` / `TaskStateChanged` / `ToolInvocationCompleted` live
//! in `cairn-runtime`, `cairn-fabric`, and `cairn-tools` — crates that
//! don't know about `AppMetrics` and shouldn't grow that dependency.
//! The event log is the single bottleneck every mutation funnels
//! through, so tapping it gives us lossless coverage with zero
//! cross-crate wiring.
//!
//! Cadence: driven by the `broadcast::Sender` that cairn-store fans
//! out on every append. Latency from FCALL → metric bump is the
//! event-log transaction + one channel hop (sub-millisecond
//! in-process; the subscriber lives in the same process as the
//! store).
//!
//! Lag: when the broadcast channel overflows (slow consumer), tokio
//! returns `RecvError::Lagged(n)`. We log-and-continue — missing
//! counter bumps are worse than crashing, and the lag count is
//! itself a useful operational signal.

#![cfg(any(feature = "metrics-core", feature = "metrics-providers"))]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[cfg(not(feature = "metrics-core"))]
use cairn_domain::lifecycle::RunState;
#[cfg(feature = "metrics-core")]
use cairn_domain::lifecycle::{RunState, TaskState};
use cairn_domain::{RunId, RuntimeEvent};
use cairn_store::InMemoryStore;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::metrics::AppMetrics;

/// #661: per-run observability state tracked in the tap so we can
/// emit the iteration histogram and inline-run gauge at terminal
/// time. Kept in a single `Mutex<HashMap>` rather than a sharded
/// DashMap: each terminal event takes one lock, the table stays
/// small in steady state (entries evict on terminal), and this
/// crate already standardises on `std::sync::Mutex` for metrics
/// state.
#[derive(Debug, Default)]
struct SubagentRunState {
    /// Highest iteration observed on a `CheckpointPersisted` event
    /// whose `root_run_id` matched this run. `0` when we never saw
    /// the loop progress (operator-cancel before any iteration, or
    /// a run that completed without checkpointing — rare).
    max_iteration: u32,
    /// `true` once at least one `SubagentSpawned` event has named
    /// this run as `parent_run_id`. Drives the `inline_run_ratio`
    /// classification at terminal time.
    spawned_subagent: bool,
}

#[derive(Debug, Default)]
struct SubagentTracker {
    runs: Mutex<HashMap<RunId, SubagentRunState>>,
}

impl SubagentTracker {
    /// Seed a fresh entry on `RunCreated`. Ensures every run the
    /// tap sees has a slot that `take_on_terminal` can remove
    /// exactly once — the predicate "was the entry present at
    /// terminal?" distinguishes a fresh terminal (record metrics)
    /// from a retry/replay of the same transition (skip, don't
    /// double-count the histogram or the inline-run window).
    fn mark_created(&self, run_id: &RunId) {
        let mut runs = self.runs.lock().unwrap_or_else(|e| e.into_inner());
        runs.entry(run_id.clone()).or_default();
    }

    fn mark_iteration(&self, run_id: &RunId, iteration: u32) {
        let mut runs = self.runs.lock().unwrap_or_else(|e| e.into_inner());
        let entry = runs.entry(run_id.clone()).or_default();
        if iteration > entry.max_iteration {
            entry.max_iteration = iteration;
        }
    }

    fn mark_spawn(&self, parent_run_id: &RunId) {
        let mut runs = self.runs.lock().unwrap_or_else(|e| e.into_inner());
        runs.entry(parent_run_id.clone())
            .or_default()
            .spawned_subagent = true;
    }

    /// Remove + return the tracking state on terminal. Returns
    /// `None` for replays / duplicate terminals — the entry was
    /// already consumed on the first transition. Callers gate
    /// metric observations on `Some(state)` so the iteration
    /// histogram and inline-run ratio window each advance exactly
    /// once per run.
    ///
    /// A genuine terminal for a run that made zero progress (e.g.
    /// operator cancels before the first checkpoint) returns
    /// `Some(SubagentRunState::default())` — the entry WAS
    /// present (`mark_created` seeded it) and we still want the
    /// scrape to reflect the run at iteration 0, inline=true.
    fn take_on_terminal(&self, run_id: &RunId) -> Option<SubagentRunState> {
        let mut runs = self.runs.lock().unwrap_or_else(|e| e.into_inner());
        runs.remove(run_id)
    }
}

/// Handle for the tap task. Clone-safe: the JoinHandle and cancel
/// token are shared behind an `Arc` so `AppState` can stay
/// `#[derive(Clone)]`. Cancel the token and `await` via
/// [`Self::shutdown`] for a synchronous stop; otherwise the task is
/// torn down by the runtime when the process exits.
#[derive(Clone)]
pub struct MetricsTap {
    inner: Arc<MetricsTapInner>,
}

struct MetricsTapInner {
    handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    cancel: CancellationToken,
}

impl MetricsTap {
    pub fn spawn(store: Arc<InMemoryStore>, metrics: Arc<AppMetrics>) -> Self {
        let cancel = CancellationToken::new();
        let worker_cancel = cancel.clone();
        // #661: per-run iteration high-water + spawn-seen tracker.
        // Owned by the tap loop; entries evict on terminal
        // `RunStateChanged`. A long-lived unterminated run holds a
        // single small entry — cheaper than scanning the projection
        // on every scrape.
        let tracker = Arc::new(SubagentTracker::default());
        // Subscribe on the caller's task, before spawning — if we
        // subscribed inside the spawned task, the caller could append
        // an event between `spawn` returning and the task actually
        // running, and that event would miss the broadcast.
        let mut rx = store.subscribe();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = worker_cancel.cancelled() => break,
                    msg = rx.recv() => match msg {
                        Ok(ev) => process_event(&metrics, &tracker, &ev.envelope.payload),
                        Err(RecvError::Lagged(n)) => {
                            tracing::warn!(
                                dropped = n,
                                "metrics tap: broadcast lag, {n} events missed"
                            );
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
            }
            tracing::info!("metrics tap stopped");
        });
        tracing::info!("metrics tap started");
        Self {
            inner: Arc::new(MetricsTapInner {
                handle: tokio::sync::Mutex::new(Some(handle)),
                cancel,
            }),
        }
    }

    /// Request a graceful stop and await the worker. Idempotent:
    /// subsequent calls are no-ops.
    pub async fn shutdown(&self) {
        self.inner.cancel.cancel();
        let handle = self.inner.handle.lock().await.take();
        if let Some(h) = handle {
            if let Err(e) = h.await {
                tracing::warn!(error = %e, "metrics tap task panicked");
            }
        }
    }
}

fn process_event(metrics: &AppMetrics, tracker: &SubagentTracker, event: &RuntimeEvent) {
    // #661: subagent-spawn observability runs regardless of the
    // `metrics-core` feature flag — the counter/histogram/gauge are
    // the dogfood-round diagnostic loop for the prompt rewrite in
    // #662 and must be always-on.
    match event {
        RuntimeEvent::RunCreated(e) => {
            // Seeds the tracker so `take_on_terminal` can distinguish
            // "first terminal for this run" (Some) from "retry /
            // replay of the same terminal" (None). Belt + braces:
            // mark_iteration / mark_spawn also `or_default()` insert,
            // so we always have an entry before terminal even if the
            // RunCreated event fell to the floor somehow.
            tracker.mark_created(&e.run_id);
        }
        RuntimeEvent::SubagentSpawned(e) => {
            // The counter (`cairn_orchestrator_subagent_spawn_total`)
            // is incremented at DECIDE time in the TracingEmitter —
            // that measures LLM intent, which is the dogfood signal
            // #661 is after. The tap still observes the domain event
            // so the per-run spawn-seen flag (drives the inline-run
            // ratio classification) reflects spawns that actually
            // landed, not just ones the LLM proposed.
            tracker.mark_spawn(&e.parent_run_id);
        }
        RuntimeEvent::CheckpointPersisted(e) => {
            // `root_run_id` is the top-level run the checkpoint
            // belongs to. Subagent child runs get their own
            // CheckpointPersisted stream with their own
            // root_run_id — each root stands alone for iteration
            // accounting.
            tracker.mark_iteration(&e.root_run_id, e.iteration);
        }
        RuntimeEvent::RunStateChanged(e)
            if matches!(
                e.transition.to,
                RunState::Completed | RunState::Failed | RunState::Canceled
            ) =>
        {
            // `Some(state)` → genuine terminal; observe once. `None`
            // → the entry was already consumed (retry / replay of
            // the same transition, or a terminal for a run whose
            // `RunCreated` landed before the tap started). Skip —
            // observing twice would skew the histogram and inline
            // ratio.
            if let Some(state) = tracker.take_on_terminal(&e.run_id) {
                crate::metrics::observe_run_iterations(metrics, state.max_iteration);
                crate::metrics::record_inline_run_outcome(metrics, !state.spawned_subagent);
            }
        }
        _ => {}
    }

    match event {
        #[cfg(feature = "metrics-core")]
        RuntimeEvent::RunCreated(e) => {
            metrics.record_run_created(
                e.project.tenant_id.as_str(),
                e.project.workspace_id.as_str(),
            );
        }
        #[cfg(feature = "metrics-core")]
        RuntimeEvent::RunStateChanged(e) => {
            let (outcome, failure_class) = match e.transition.to {
                RunState::Completed => (Some("completed"), None),
                RunState::Failed => (Some("failed"), e.failure_class.map(failure_label)),
                RunState::Canceled => (Some("canceled"), None),
                _ => (None, None),
            };
            if let Some(outcome) = outcome {
                metrics.record_run_terminal(
                    e.project.tenant_id.as_str(),
                    e.project.workspace_id.as_str(),
                    outcome,
                    failure_class,
                );
            }
            // Symmetric to the task path: `ExecutionFailed { failure_class:
            // LeaseExpired }` surfaces as a Failed transition on a run. The
            // `cairn_lease_expiries_total{entity="run"}` series would never
            // bump otherwise even though the subscriber emits it.
            if matches!(e.transition.to, RunState::Failed)
                && matches!(
                    e.failure_class,
                    Some(cairn_domain::lifecycle::FailureClass::LeaseExpired)
                )
            {
                metrics.record_lease_expiry("run");
            }
        }
        #[cfg(feature = "metrics-core")]
        RuntimeEvent::TaskCreated(e) => {
            metrics.record_task_created(
                e.project.tenant_id.as_str(),
                e.project.workspace_id.as_str(),
            );
        }
        #[cfg(feature = "metrics-core")]
        RuntimeEvent::TaskStateChanged(e) => {
            let (outcome, failure_class) = match e.transition.to {
                TaskState::Completed => (Some("completed"), None),
                TaskState::Failed => (Some("failed"), e.failure_class.map(failure_label)),
                TaskState::RetryableFailed => {
                    (Some("retryable_failed"), e.failure_class.map(failure_label))
                }
                TaskState::Canceled => (Some("canceled"), None),
                TaskState::DeadLettered => (Some("dead_lettered"), None),
                _ => (None, None),
            };
            if let Some(outcome) = outcome {
                metrics.record_task_terminal(
                    e.project.tenant_id.as_str(),
                    e.project.workspace_id.as_str(),
                    outcome,
                    failure_class,
                );
            }
            // A RetryableFailed with failure_class=LeaseExpired is exactly the
            // signal the lease-history subscriber emits after FF reclaims a
            // dead worker's lease. Counted separately for dashboard visibility
            // without requiring operators to filter on the terminal counter.
            if matches!(e.transition.to, TaskState::RetryableFailed)
                && matches!(
                    e.failure_class,
                    Some(cairn_domain::lifecycle::FailureClass::LeaseExpired)
                )
            {
                metrics.record_lease_expiry("task");
            }
        }
        #[cfg(feature = "metrics-core")]
        RuntimeEvent::ToolInvocationCompleted(e) => {
            use cairn_domain::tool_invocation::ToolInvocationOutcomeKind as K;
            let outcome = match e.outcome {
                K::Success => "success",
                K::RetryableFailure => "retryable_failure",
                K::PermanentFailure => "permanent_failure",
                K::Timeout => "timeout",
                K::Canceled => "canceled",
                K::ProtocolViolation => "protocol_violation",
            };
            metrics.record_tool_invocation(&e.tool_name, outcome);
        }
        #[cfg(feature = "metrics-providers")]
        RuntimeEvent::ProviderCallCompleted(e) => {
            use cairn_domain::providers::{OperationKind, ProviderCallStatus};
            let operation = match e.operation_kind {
                OperationKind::Generate => "generate",
                OperationKind::Embed => "embed",
                OperationKind::Rerank => "rerank",
            };
            let status = match e.status {
                ProviderCallStatus::Succeeded => "succeeded",
                ProviderCallStatus::Failed => "failed",
                ProviderCallStatus::Cancelled => "cancelled",
            };
            metrics.record_provider_call(
                e.provider_connection_id.as_str(),
                e.provider_model_id.as_str(),
                operation,
                status,
                e.latency_ms,
                e.input_tokens,
                e.output_tokens,
            );
        }
        _ => {}
    }
}

#[cfg(feature = "metrics-core")]
fn failure_label(fc: cairn_domain::lifecycle::FailureClass) -> &'static str {
    use cairn_domain::lifecycle::FailureClass;
    match fc {
        FailureClass::TimedOut => "timed_out",
        FailureClass::DependencyFailed => "dependency_failed",
        FailureClass::ApprovalRejected => "approval_rejected",
        FailureClass::PolicyDenied => "policy_denied",
        FailureClass::ExecutionError => "execution_error",
        FailureClass::LeaseExpired => "lease_expired",
        FailureClass::CanceledByOperator => "canceled_by_operator",
        FailureClass::TerminalWriteDeadlock => "terminal_write_deadlock",
        FailureClass::VerificationRejected => "verification_rejected",
        FailureClass::OrphanChild => "orphan_child",
        FailureClass::AllProvidersExhausted => "all_providers_exhausted",
        FailureClass::ModelReportedFailure => "model_reported_failure",
    }
}

#[cfg(test)]
mod tests {
    //! #661 unit tests for `SubagentTracker` — the per-run state
    //! machine that feeds the iterations histogram + inline-run
    //! ratio at terminal time. Full-tap integration (events →
    //! metrics) is covered by the integration test in
    //! `tests/subagent_observability.rs`.
    use super::*;
    use cairn_domain::RunId;

    #[test]
    fn tracker_records_highest_iteration_observed() {
        let tracker = SubagentTracker::default();
        let run = RunId::new("r1");
        tracker.mark_iteration(&run, 1);
        tracker.mark_iteration(&run, 5);
        tracker.mark_iteration(&run, 3); // regression — ignore
        let state = tracker.take_on_terminal(&run).expect("entry exists");
        assert_eq!(state.max_iteration, 5);
        assert!(!state.spawned_subagent);
    }

    #[test]
    fn tracker_marks_spawn_on_matching_parent_run() {
        let tracker = SubagentTracker::default();
        let run = RunId::new("r1");
        tracker.mark_iteration(&run, 4);
        tracker.mark_spawn(&run);
        let state = tracker.take_on_terminal(&run).expect("entry exists");
        assert!(state.spawned_subagent);
        assert_eq!(state.max_iteration, 4);
    }

    #[test]
    fn tracker_evicts_on_terminal_returns_none_on_retry() {
        // A second take (duplicate / retried terminal) returns
        // None so the caller can skip the double-observation. This
        // is the fix for the double-counting race Gemini flagged on
        // #664 review.
        let tracker = SubagentTracker::default();
        let run = RunId::new("r1");
        tracker.mark_iteration(&run, 7);
        tracker.mark_spawn(&run);
        let first = tracker.take_on_terminal(&run).expect("first terminal hits");
        assert_eq!(first.max_iteration, 7);
        assert!(first.spawned_subagent);
        let second = tracker.take_on_terminal(&run);
        assert!(
            second.is_none(),
            "duplicate terminal must return None so metrics aren't double-observed"
        );
    }

    #[test]
    fn tracker_terminal_for_unseen_run_returns_none() {
        // A run whose RunCreated landed before the tap started (or
        // was dropped) will have no tracker entry at all. Terminal
        // for it returns None — we skip the observation rather than
        // invent a zeroed sample.
        let tracker = SubagentTracker::default();
        let state = tracker.take_on_terminal(&RunId::new("r_never_ran"));
        assert!(
            state.is_none(),
            "untracked run must return None to protect metrics from pre-tap-start terminals"
        );
    }

    #[test]
    fn tracker_seeds_on_run_created() {
        // After mark_created, the first terminal returns Some even
        // if no iteration / spawn ever landed. That's the "run made
        // zero progress" sample — we DO want to count it in the
        // inline-ratio window so an operator-cancel doesn't stealth-
        // inflate the delegation ratio.
        let tracker = SubagentTracker::default();
        let run = RunId::new("r_created_only");
        tracker.mark_created(&run);
        let state = tracker
            .take_on_terminal(&run)
            .expect("mark_created seeded the entry");
        assert_eq!(state.max_iteration, 0);
        assert!(!state.spawned_subagent);
    }

    #[test]
    fn process_event_subagent_spawned_marks_parent_without_touching_counter() {
        // The counter is driven by `on_decide_completed` — the tap
        // contributes only to the per-run tracker.
        let metrics = AppMetrics::default();
        let tracker = SubagentTracker::default();
        let parent = RunId::new("r_parent");
        let event = RuntimeEvent::SubagentSpawned(cairn_domain::SubagentSpawned {
            project: cairn_domain::ProjectKey {
                tenant_id: cairn_domain::TenantId::new("t"),
                workspace_id: cairn_domain::WorkspaceId::new("w"),
                project_id: cairn_domain::ProjectId::new("p"),
            },
            parent_run_id: parent.clone(),
            parent_task_id: None,
            child_task_id: cairn_domain::TaskId::new("tk_child"),
            child_session_id: cairn_domain::SessionId::new("s_child"),
            child_run_id: None,
            goal: "tap-test-goal".to_owned(),
            role: "executor".to_owned(),
            parent_context: None,
        });
        process_event(&metrics, &tracker, &event);

        let state = tracker
            .take_on_terminal(&parent)
            .expect("spawn seeded entry");
        assert!(
            state.spawned_subagent,
            "SubagentSpawned must mark the parent as delegating",
        );

        // Counter should still be 0 — the emitter path owns that.
        let rendered = metrics.render_prometheus();
        assert!(
            rendered.contains("cairn_orchestrator_subagent_spawn_total 0"),
            "tap must not double-count the spawn counter;\n{rendered}"
        );
    }

    #[test]
    fn process_event_run_terminal_observes_iterations_and_inline_outcome() {
        let metrics = AppMetrics::default();
        let tracker = SubagentTracker::default();
        let run = RunId::new("r_term");
        let project = cairn_domain::ProjectKey {
            tenant_id: cairn_domain::TenantId::new("t"),
            workspace_id: cairn_domain::WorkspaceId::new("w"),
            project_id: cairn_domain::ProjectId::new("p"),
        };

        // Ingest a checkpoint at iteration 6.
        process_event(
            &metrics,
            &tracker,
            &RuntimeEvent::CheckpointPersisted(cairn_domain::events::CheckpointPersisted {
                project: project.clone(),
                checkpoint_id: cairn_domain::CheckpointId::new("ckpt1"),
                session_id: cairn_domain::SessionId::new("s"),
                root_run_id: run.clone(),
                iteration: 6,
                at_ms: 0,
            }),
        );

        // Ingest the terminal RunStateChanged — Failed to exercise the
        // "fires on non-Completed terminals too" assertion.
        process_event(
            &metrics,
            &tracker,
            &RuntimeEvent::RunStateChanged(cairn_domain::RunStateChanged {
                project,
                run_id: run.clone(),
                transition: cairn_domain::StateTransition {
                    from: Some(RunState::Running),
                    to: RunState::Failed,
                },
                failure_class: None,
                pause_reason: None,
                resume_trigger: None,
            }),
        );

        let rendered = metrics.render_prometheus();
        assert!(
            rendered.contains("cairn_orchestrator_iterations_per_run_sum 6"),
            "histogram must observe 6 at terminal;\n{rendered}"
        );
        // 0 subagents seen → inline=true → ratio 1.0 (1/1).
        assert!(
            rendered.contains("cairn_orchestrator_inline_run_ratio 1.000000"),
            "inline outcome must be true for a run that never spawned a subagent;\n{rendered}"
        );
    }
}
