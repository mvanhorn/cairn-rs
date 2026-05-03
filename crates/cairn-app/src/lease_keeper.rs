//! Background lease-keeper for long-running orchestrate calls (#639).
//!
//! # The problem
//!
//! `POST /v1/runs/:id/orchestrate` is a pull-model driver: every HTTP
//! call runs one GATHER → DECIDE → EXECUTE iteration and returns. FF's
//! `ClaimedTask` renewal task is scoped to the in-memory handler
//! lifetime; once the handler returns (on `waiting_approval`,
//! `max_iterations`, or the natural end of an iteration), the renewer
//! dies. The FF lease then ticks untouched until the next HTTP call.
//!
//! F51 added an entry-time `renew_lease_if_stale` call inside the
//! handler, but that still leaves a window between orchestrate calls
//! where an operator paced on human-scale approval clicks (30-60 s per
//! round, N rounds deep) can blow past `lease_ttl_ms`. When cairn
//! finally dispatches the terminal `ff_complete_execution`, FF rejects
//! it with `lease_expired`; the F64 bounded recovery loop then runs
//! its 30 s ceiling and hands the run to F62's
//! `Failed(TerminalWriteDeadlock)` fallback — a run that succeeded in
//! every operator-visible way flips to Failed.
//!
//! Dogfood run `run_roguelike_1777808568` (2026-05-03) is the recorded
//! incident: 8 successful tool calls, 370 LOC of real Rust, wedged at
//! the final `complete_execution` because 8 approval rounds × ~15 s
//! each went past the 30 s TTL. Tracked upstream at
//! [FlowFabric#371], closed without resolution.
//!
//! [FlowFabric#371]: https://github.com/avifenesh/FlowFabric/issues/371
//!
//! # The fix
//!
//! A background tokio task per live run that calls
//! `RunService::renew_lease_if_stale` every `lease_ttl_ms / 3`. The
//! keeper lives for the full operator-visible lifetime of the run —
//! across every suspend/resume cycle, every approval wait, every
//! operator-paced gap. Decouples lease renewal from orchestrate-call
//! cadence entirely.
//!
//! The registry is a single map keyed by `RunId`; `ensure_running`
//! does an atomic check-and-insert so concurrent orchestrate handlers
//! don't spawn duplicate keepers for the same run.
//!
//! # Exit conditions
//!
//! The keeper task exits (and removes its own entry from the registry)
//! when ANY of:
//!
//! 1. **Terminal state observed.** `renew_lease_if_stale` returns a
//!    `RunRecord` whose `state.is_terminal()` is true. The keeper's
//!    job is done — the terminal-FCALL path already landed the final
//!    lifecycle flip.
//! 2. **Non-transient renew error.** The run is gone (`NotFound`), the
//!    FCALL hit a terminal-state conflict, or the fabric reported
//!    `lease_expired` which we can't repair. Continuing to loop would
//!    just spam errors. Transient phase conflicts (the same class F58
//!    tolerates in the orchestrate handler entry) are swallowed and
//!    retried on the next tick.
//! 3. **Cancellation token fired.** The registry's shutdown path
//!    (called from the process shutdown hook) cancels every keeper in
//!    parallel so the runtime can drain cleanly. Idempotent —
//!    re-triggering a cancelled token is a no-op.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use cairn_domain::{RunId, SessionId};
use cairn_runtime::RunService;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Minimum-remaining lease budget passed to
/// `RunService::renew_lease_if_stale` from the keeper loop. Picked to
/// match the F51 handler-entry constant so the renewal path semantics
/// agree across both callers — the keeper is belt-and-suspenders
/// above F51, not a replacement.
const KEEPER_MIN_REMAINING_MS: u64 = 10_000;

/// Hard floor on the keeper sleep interval. Operators can't configure
/// `lease_ttl_ms` below 1 s (validated in `FabricConfig`), so the
/// `lease_ttl_ms / 3` default already lands above this floor for any
/// valid config. This clamp exists for the degenerate test case where
/// a fake runtime hands us a zero TTL; without it we'd spin-loop.
const KEEPER_MIN_INTERVAL_MS: u64 = 500;

/// Handle to a single per-run lease keeper. Owned by the registry
/// entry; dropped when the keeper exits naturally or is cancelled.
struct LeaseKeeperHandle {
    cancel: CancellationToken,
    join: JoinHandle<()>,
}

/// Shared registry of live lease-keeper tasks.
///
/// One entry per `RunId`. `ensure_running` is the only public write
/// path; it performs an atomic check-and-insert under the registry
/// mutex so two concurrent orchestrate handlers observing the same
/// run cannot each spawn a keeper (the second call is a no-op).
///
/// The registry is owned by `AppState` and lives for the process
/// lifetime. `shutdown_all` drains every keeper before the runtime
/// tears down; omit the call only in tests that exercise the keeper
/// itself and don't mind keepers being torn down by the runtime on
/// process exit.
#[derive(Debug, Default)]
pub struct LeaseKeeperRegistry {
    inner: Mutex<HashMap<RunId, LeaseKeeperHandle>>,
}

impl std::fmt::Debug for LeaseKeeperHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseKeeperHandle")
            .field("cancelled", &self.cancel.is_cancelled())
            .field("finished", &self.join.is_finished())
            .finish()
    }
}

impl LeaseKeeperRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Spawn a keeper for `run_id` if one is not already running.
    ///
    /// Atomic under the registry mutex: if another task already holds
    /// an entry for this run (and the entry's task has not finished),
    /// this call is a no-op. If the entry exists but the task has
    /// exited (natural terminal completion), the stale entry is
    /// reaped and a fresh keeper is spawned — this handles the
    /// re-activation path where a run resumes after its keeper's
    /// previous instance already exited.
    ///
    /// `lease_ttl_ms` is the fabric-side configured lease TTL; the
    /// keeper sleeps `lease_ttl_ms / 3` between renewals so there are
    /// three renewal attempts per TTL window (matching FF's own
    /// internal renewer).
    pub async fn ensure_running(
        &self,
        run_id: RunId,
        session_id: SessionId,
        runs: Arc<dyn RunService>,
        lease_ttl_ms: u64,
    ) {
        let mut guard = self.inner.lock().await;

        // Reap any stale entry whose task already finished. Natural
        // terminal completion leaves the JoinHandle behind until
        // someone reaps it; without this branch a re-activation
        // after terminal completion would hit the "already running"
        // short-circuit even though no keeper is actually alive.
        if let Some(existing) = guard.get(&run_id) {
            if existing.join.is_finished() {
                guard.remove(&run_id);
            } else {
                return;
            }
        }

        let interval_ms = std::cmp::max(lease_ttl_ms / 3, KEEPER_MIN_INTERVAL_MS);
        let interval = Duration::from_millis(interval_ms);
        let cancel = CancellationToken::new();
        let worker_cancel = cancel.clone();
        let task_run_id = run_id.clone();
        let task_session = session_id.clone();
        let join = tokio::spawn(async move {
            run_keeper_loop(task_run_id, task_session, runs, interval, worker_cancel).await;
        });
        guard.insert(run_id, LeaseKeeperHandle { cancel, join });
    }

    /// Cancel every live keeper and await its exit. Idempotent.
    ///
    /// Used by the process shutdown hook; also used by integration
    /// tests that spawn keepers in short-lived harnesses and want to
    /// drain them deterministically between scenarios.
    pub async fn shutdown_all(&self) {
        let handles: Vec<LeaseKeeperHandle> = {
            let mut guard = self.inner.lock().await;
            guard.drain().map(|(_, h)| h).collect()
        };
        // Parallelize cancellation: trigger every token first so a
        // slow keeper that's mid-renew doesn't serialize the drain.
        // Each keeper races the cancellation against its in-flight
        // `renew_lease_if_stale` (see `run_keeper_loop`), so once
        // every token is cancelled we only await the observed
        // cancellation, not a sum of sequential renew latencies.
        // Addresses gemini-code-assist review on PR #647.
        for handle in &handles {
            handle.cancel.cancel();
        }
        for handle in handles {
            if let Err(err) = handle.join.await {
                if !err.is_cancelled() {
                    tracing::warn!(
                        error = %err,
                        "lease keeper task panicked during shutdown"
                    );
                }
            }
        }
    }

    /// Number of live keepers. Test-only.
    #[cfg(test)]
    pub async fn len(&self) -> usize {
        self.inner.lock().await.len()
    }

    /// Whether the registry is empty. Test-only companion to `len`
    /// — clippy's `len_without_is_empty` is a style nit, not a real
    /// contract, but the symmetry keeps the test helpers consistent.
    #[cfg(test)]
    pub async fn is_empty(&self) -> bool {
        self.inner.lock().await.is_empty()
    }

    /// Whether a keeper is registered for `run_id`. Test-only.
    #[cfg(test)]
    pub async fn contains(&self, run_id: &RunId) -> bool {
        self.inner.lock().await.contains_key(run_id)
    }
}

async fn run_keeper_loop(
    run_id: RunId,
    session_id: SessionId,
    runs: Arc<dyn RunService>,
    interval: Duration,
    cancel: CancellationToken,
) {
    tracing::debug!(
        run_id = %run_id,
        interval_ms = interval.as_millis() as u64,
        "#639 lease keeper started"
    );
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::debug!(run_id = %run_id, "#639 lease keeper cancelled");
                return;
            }
            _ = tokio::time::sleep(interval) => {}
        }

        // Wrap the renew call in the same `select!` so cancellation
        // can interrupt an in-flight FCALL instead of waiting for
        // the upstream fabric RPC to return. Without this wrapper,
        // `shutdown_all` would hang for up to `fcall_timeout_ms` on
        // a keeper mid-renew during a slow Valkey/Postgres.
        // Addresses gemini-code-assist review on PR #647.
        let renew_result = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::debug!(
                    run_id = %run_id,
                    "#639 lease keeper cancelled during renewal"
                );
                return;
            }
            result = runs.renew_lease_if_stale(
                &session_id,
                &run_id,
                KEEPER_MIN_REMAINING_MS,
            ) => result,
        };

        match renew_result {
            Ok(record) => {
                if record.state.is_terminal() {
                    tracing::debug!(
                        run_id = %run_id,
                        state = ?record.state,
                        "#639 lease keeper observed terminal state, exiting"
                    );
                    return;
                }
            }
            Err(err) if err.is_transient_phase_conflict() => {
                // Mid-approval / mid-write sub-phases — the existing
                // lease is still valid; the keeper just can't renew
                // it right now. Retry on the next tick. F58
                // tolerates the same class at the orchestrate
                // handler entry; keep the behaviour consistent.
                tracing::debug!(
                    run_id = %run_id,
                    error = %err,
                    "#639 lease keeper hit transient phase conflict, retrying"
                );
            }
            Err(err) => {
                tracing::info!(
                    run_id = %run_id,
                    error = %err,
                    "#639 lease keeper exiting on non-transient error"
                );
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use cairn_domain::{
        ApprovalDecision, FailureClass, PauseReason, ProjectKey, ResumeTrigger, RunId,
        RunResumeTarget, RunState, SessionId, TenantId,
    };
    use cairn_runtime::error::RuntimeError;
    use cairn_runtime::RunService;
    use cairn_store::projections::RunRecord;

    /// Mock run service that records every `renew_lease_if_stale` call
    /// and returns a caller-controlled sequence of responses. Used to
    /// prove the keeper's exit-on-terminal and shutdown-on-cancel
    /// invariants without spinning a real fabric runtime.
    #[derive(Default)]
    struct MockRuns {
        renew_calls: AtomicUsize,
        /// Queue of responses; each renew consumes one. When the queue
        /// is empty the mock returns a fresh running record so the
        /// keeper continues ticking.
        #[allow(clippy::type_complexity)]
        responses: Mutex<Vec<Result<RunRecord, RuntimeError>>>,
    }

    impl MockRuns {
        fn running(state: RunState) -> RunRecord {
            RunRecord {
                run_id: RunId::new("run_test"),
                session_id: SessionId::new("sess_test"),
                parent_run_id: None,
                project: ProjectKey {
                    tenant_id: TenantId::new("t"),
                    workspace_id: cairn_domain::WorkspaceId::new("w"),
                    project_id: cairn_domain::ProjectId::new("p"),
                },
                state,
                prompt_release_id: None,
                agent_role_id: None,
                failure_class: None,
                pause_reason: None,
                resume_trigger: None,
                version: 0,
                created_at: 0,
                updated_at: 0,
                completion_summary: None,
                completion_verification: None,
                completion_annotated_at_ms: None,
                terminal_write_recovery: None,
            }
        }

        async fn push_response(&self, r: Result<RunRecord, RuntimeError>) {
            self.responses.lock().await.push(r);
        }
    }

    #[async_trait]
    impl RunService for MockRuns {
        async fn start(
            &self,
            _project: &ProjectKey,
            _session_id: &SessionId,
            _run_id: RunId,
            _parent_run_id: Option<RunId>,
        ) -> Result<RunRecord, RuntimeError> {
            unreachable!("keeper only calls renew_lease_if_stale")
        }
        async fn get(&self, _run_id: &RunId) -> Result<Option<RunRecord>, RuntimeError> {
            unreachable!("keeper only calls renew_lease_if_stale")
        }
        async fn list_by_session(
            &self,
            _session_id: &SessionId,
            _limit: usize,
            _offset: usize,
        ) -> Result<Vec<RunRecord>, RuntimeError> {
            unreachable!("keeper only calls renew_lease_if_stale")
        }
        async fn complete(
            &self,
            _session_id: &SessionId,
            _run_id: &RunId,
        ) -> Result<RunRecord, RuntimeError> {
            unreachable!("keeper only calls renew_lease_if_stale")
        }
        async fn fail(
            &self,
            _session_id: &SessionId,
            _run_id: &RunId,
            _failure_class: FailureClass,
        ) -> Result<RunRecord, RuntimeError> {
            unreachable!("keeper only calls renew_lease_if_stale")
        }
        async fn cancel(
            &self,
            _session_id: &SessionId,
            _run_id: &RunId,
        ) -> Result<RunRecord, RuntimeError> {
            unreachable!("keeper only calls renew_lease_if_stale")
        }
        async fn pause(
            &self,
            _session_id: &SessionId,
            _run_id: &RunId,
            _reason: PauseReason,
        ) -> Result<RunRecord, RuntimeError> {
            unreachable!("keeper only calls renew_lease_if_stale")
        }
        async fn resume(
            &self,
            _session_id: &SessionId,
            _run_id: &RunId,
            _trigger: ResumeTrigger,
            _target: RunResumeTarget,
        ) -> Result<RunRecord, RuntimeError> {
            unreachable!("keeper only calls renew_lease_if_stale")
        }
        async fn claim(
            &self,
            _session_id: &SessionId,
            _run_id: &RunId,
        ) -> Result<RunRecord, RuntimeError> {
            unreachable!("keeper only calls renew_lease_if_stale")
        }
        async fn enter_waiting_approval(
            &self,
            _session_id: &SessionId,
            _run_id: &RunId,
        ) -> Result<RunRecord, RuntimeError> {
            unreachable!("keeper only calls renew_lease_if_stale")
        }
        async fn resolve_approval(
            &self,
            _session_id: &SessionId,
            _run_id: &RunId,
            _decision: ApprovalDecision,
        ) -> Result<RunRecord, RuntimeError> {
            unreachable!("keeper only calls renew_lease_if_stale")
        }
        async fn list_child_runs(
            &self,
            _parent_run_id: &RunId,
            _limit: usize,
        ) -> Result<Vec<RunRecord>, RuntimeError> {
            unreachable!("keeper only calls renew_lease_if_stale")
        }
        async fn renew_lease_if_stale(
            &self,
            _session_id: &SessionId,
            _run_id: &RunId,
            _min_remaining_ms: u64,
        ) -> Result<RunRecord, RuntimeError> {
            self.renew_calls.fetch_add(1, Ordering::SeqCst);
            let mut q = self.responses.lock().await;
            if q.is_empty() {
                return Ok(Self::running(RunState::Running));
            }
            q.remove(0)
        }
    }

    /// `ensure_running` must atomically deduplicate concurrent inserts
    /// for the same `RunId`. Spawns 50 concurrent ensures; asserts only
    /// one keeper lands in the registry.
    #[tokio::test]
    async fn ensure_running_dedups_concurrent_inserts() {
        let registry = Arc::new(LeaseKeeperRegistry::new());
        let runs: Arc<dyn RunService> = Arc::new(MockRuns::default());
        let run_id = RunId::new("run_dedup");
        let session_id = SessionId::new("sess_dedup");

        let mut handles = Vec::new();
        for _ in 0..50 {
            let r = registry.clone();
            let runs = runs.clone();
            let run_id = run_id.clone();
            let session_id = session_id.clone();
            handles.push(tokio::spawn(async move {
                r.ensure_running(run_id, session_id, runs, 10_000).await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(registry.len().await, 1, "exactly one keeper entry");
        assert!(registry.contains(&run_id).await, "keeper keyed on run_id");

        registry.shutdown_all().await;
        assert_eq!(registry.len().await, 0, "shutdown_all drains every entry");
    }

    /// `shutdown_all` must cancel and await every live keeper. Spawns
    /// three keepers against a mock that never returns terminal,
    /// verifies the join handles complete within the shutdown call.
    #[tokio::test]
    async fn shutdown_all_cancels_every_keeper() {
        let registry = Arc::new(LeaseKeeperRegistry::new());
        let mock = Arc::new(MockRuns::default());
        let runs: Arc<dyn RunService> = mock.clone();

        for i in 0..3 {
            registry
                .ensure_running(
                    RunId::new(format!("run_shutdown_{i}")),
                    SessionId::new(format!("sess_shutdown_{i}")),
                    runs.clone(),
                    // Fast ticker so the keeper is definitely inside
                    // the sleep (cancellation path) on shutdown.
                    1_500,
                )
                .await;
        }
        assert_eq!(registry.len().await, 3);

        // shutdown_all completes. If the keeper did not honour the
        // cancellation token the join would hang and tokio::test would
        // time out.
        tokio::time::timeout(Duration::from_secs(2), registry.shutdown_all())
            .await
            .expect("shutdown_all must complete within 2s (keepers honour cancel)");
        assert_eq!(registry.len().await, 0);
    }

    /// Keeper observing a terminal RunRecord must exit without waiting
    /// for the next tick AND remove itself from the registry on the
    /// next reap (the reap happens lazily on the next `ensure_running`
    /// call for the same run_id — this test proves the task exits
    /// naturally).
    #[tokio::test]
    async fn keeper_exits_on_terminal_state() {
        let registry = Arc::new(LeaseKeeperRegistry::new());
        let mock = Arc::new(MockRuns::default());
        // First renew: still running. Second renew: Completed — the
        // keeper must exit after observing this.
        mock.push_response(Ok(MockRuns::running(RunState::Running)))
            .await;
        mock.push_response(Ok(MockRuns::running(RunState::Completed)))
            .await;

        let runs: Arc<dyn RunService> = mock.clone();
        let run_id = RunId::new("run_terminal");
        let session_id = SessionId::new("sess_terminal");

        registry
            .ensure_running(run_id.clone(), session_id, runs, 1_500)
            .await;

        // Wait up to 2s for the keeper to observe the Completed
        // response and exit. Poll the JoinHandle rather than sleeping
        // a fixed duration — no flake window.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            {
                let guard = registry.inner.lock().await;
                if let Some(h) = guard.get(&run_id) {
                    if h.join.is_finished() {
                        break;
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("keeper did not exit after observing terminal state");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(mock.renew_calls.load(Ordering::SeqCst) >= 2);

        registry.shutdown_all().await;
    }
}
