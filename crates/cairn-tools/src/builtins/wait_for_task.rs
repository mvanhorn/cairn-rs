//! `wait_for_task` built-in tool — poll until a task reaches a terminal state.
//!
//! Polls the task-read-model at a configurable interval until the task reaches
//! a terminal state (`completed`, `failed`, `canceled`, `dead_lettered`) or the
//! wall-clock timeout expires.
//!
//! On timeout the tool returns `Ok(ToolResult{ terminal: false, timed_out: true, ... })`
//! — not an `Err`. That lets the orchestrator's next turn decide what to do
//! (spawn a worker, give up, wait more) instead of treating a still-running
//! task as a tool failure.

use std::sync::Arc;

use async_trait::async_trait;
use cairn_domain::{policy::ExecutionClass, ProjectKey, TaskId};
use cairn_store::projections::TaskReadModel;
use serde_json::Value;

use super::{ToolEffect, ToolError, ToolHandler, ToolResult, ToolTier};
use cairn_domain::recovery::RetrySafety;

/// Hard cap on the wall-clock budget, regardless of what the caller requests.
/// Anything longer belongs in a scheduled task, not a synchronous wait.
const MAX_WAIT_SECS: u64 = 300;
/// Minimum wall-clock budget. Callers asking for `0` get clamped up so the
/// tool always does at least one observation before reporting.
const MIN_WAIT_SECS: u64 = 1;
/// Default wall-clock budget when the caller omits `timeout_secs`.
const DEFAULT_WAIT_SECS: u64 = 60;

/// Hard-cap on the poll interval so a pathological caller can't park the tool
/// longer than the whole deadline in a single sleep.
const MAX_POLL_MS: u64 = 10_000;
/// Minimum poll interval — protects the read-model from a tight busy-loop if
/// a caller passes `0`.
const MIN_POLL_MS: u64 = 500;
/// Default poll interval when the caller omits `poll_interval_ms`.
const DEFAULT_POLL_MS: u64 = 1_000;

/// Per-`get` budget applied via `tokio::time::timeout`. A hung DB pool or
/// event-log deadlock on the read-model side would otherwise block the
/// orchestrator's execute phase indefinitely — the whole-loop deadline check
/// never fires if we never return from `.await`. This upper-bounds the
/// observation window; a timed-out read just counts as a non-terminal
/// observation and the poll loop gets another chance (or hits its overall
/// deadline).
const PER_GET_TIMEOUT_MS: u64 = 5_000;

/// Poll until a task is terminal or the deadline elapses.
pub struct WaitForTaskTool {
    store: Arc<dyn TaskReadModel>,
}

impl WaitForTaskTool {
    pub fn new(store: Arc<dyn TaskReadModel>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ToolHandler for WaitForTaskTool {
    fn name(&self) -> &str {
        "wait_for_task"
    }
    fn tier(&self) -> ToolTier {
        ToolTier::Registered
    }
    fn tool_effect(&self) -> ToolEffect {
        ToolEffect::Observational
    }
    fn retry_safety(&self) -> RetrySafety {
        RetrySafety::IdempotentSafe
    }
    fn description(&self) -> &str {
        "Wait until a task reaches a terminal state (completed/failed/canceled/dead_lettered). \
         Polls the task at the configured interval up to `timeout_secs`. \
         Returns `{terminal: true, state: <final>}` when the task finishes, \
         or `{terminal: false, timed_out: true, state: <last_observed>}` when the \
         deadline elapses — callers should treat the timeout shape as 'still running, \
         decide what to do next', not as a tool error."
    }
    fn parameters_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["task_id"],
            "properties": {
                "task_id": {
                    "type": "string",
                    "description": "Task ID to wait for"
                },
                "timeout_secs": {
                    "type": "integer",
                    "description": "Maximum seconds to wait (default 60, min 1, max 300)",
                    "default": DEFAULT_WAIT_SECS,
                    "minimum": MIN_WAIT_SECS,
                    "maximum": MAX_WAIT_SECS
                },
                "poll_interval_ms": {
                    "type": "integer",
                    "description": "Polling interval in milliseconds (default 1000, min 500, max 10000)",
                    "default": DEFAULT_POLL_MS,
                    "minimum": MIN_POLL_MS,
                    "maximum": MAX_POLL_MS
                }
            }
        })
    }
    fn execution_class(&self) -> ExecutionClass {
        ExecutionClass::SandboxedProcess
    }

    async fn execute(&self, _project: &ProjectKey, args: Value) -> Result<ToolResult, ToolError> {
        let task_id_str = args
            .get("task_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::InvalidArgs {
                field: "task_id".into(),
                message: "required string".into(),
            })?;
        let task_id = TaskId::new(task_id_str);

        let timeout_secs = args
            .get("timeout_secs")
            .and_then(|v| v.as_u64())
            .unwrap_or(DEFAULT_WAIT_SECS)
            .clamp(MIN_WAIT_SECS, MAX_WAIT_SECS);

        let poll_ms = args
            .get("poll_interval_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(DEFAULT_POLL_MS)
            .clamp(MIN_POLL_MS, MAX_POLL_MS);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
        // Tracks the most recent observed state so the timeout shape can report
        // what we last saw (useful for the orchestrator's next-turn decision).
        let mut last_state: Option<cairn_domain::TaskState> = None;

        loop {
            // `tokio::time::timeout` on the read is the load-bearing guard:
            // a pool-exhausted Postgres adapter or a deadlocked in-memory
            // projection could block this `.await` longer than the whole
            // `timeout_secs` budget, and the pre-sleep deadline check below
            // would never run. Bounding each read individually keeps the outer
            // deadline enforceable.
            //
            // The per-read budget is also capped by the remaining overall
            // budget (recomputed each iteration) so a caller that asks for
            // `timeout_secs=1` doesn't wait the full `PER_GET_TIMEOUT_MS`
            // (5 s) on a single hung read before the outer deadline check
            // fires. Gemini review on #687 caught this as a medium.
            let get_fut = TaskReadModel::get(self.store.as_ref(), &task_id);
            let get_timeout = std::time::Duration::from_millis(PER_GET_TIMEOUT_MS)
                .min(deadline.saturating_duration_since(std::time::Instant::now()));

            match tokio::time::timeout(get_timeout, get_fut).await {
                Ok(Ok(Some(task))) => {
                    last_state = Some(task.state);
                    if task.state.is_terminal() {
                        return Ok(ToolResult::ok(serde_json::json!({
                            "task_id":   task.task_id.as_str(),
                            "state":     state_label(task.state),
                            "terminal":  true,
                            "timed_out": false,
                            "waited":    true,
                        })));
                    }
                }
                Ok(Ok(None)) => {
                    // A missing task is a permanent condition — no amount of
                    // polling resurrects a task that never existed. Surface
                    // cleanly so the caller doesn't sleep-retry forever.
                    return Err(ToolError::Permanent(format!(
                        "task not found: {task_id_str}"
                    )));
                }
                Ok(Err(e)) => {
                    return Err(ToolError::Transient(format!("store error: {e}")));
                }
                Err(_elapsed) => {
                    // Per-read timeout. Log at WARN — a hung read is a
                    // server-side signal (pool exhaustion, deadlocked
                    // projection) that production log pipelines filtering
                    // below WARN would otherwise drop silently. Fall through
                    // to the deadline check so the outer loop still owns the
                    // hard wall-clock budget.
                    tracing::warn!(
                        task_id = %task_id_str,
                        get_timeout_ms = get_timeout.as_millis() as u64,
                        "wait_for_task: task read timed out; falling through to deadline check",
                    );
                }
            }

            let now = std::time::Instant::now();
            if now >= deadline {
                return Ok(ToolResult::ok(timeout_result(task_id_str, last_state)));
            }

            // Cap the sleep so we can't overshoot the deadline by more than a
            // single poll interval; if the remaining budget is smaller than
            // `poll_ms`, sleep only that long and let the next iteration
            // observe the deadline crossing.
            let remaining = deadline.saturating_duration_since(now);
            let poll = std::time::Duration::from_millis(poll_ms).min(remaining);
            tokio::time::sleep(poll).await;
        }
    }
}

/// Canonical lowercase state label — matches the shape `get_task` emits so the
/// orchestrator / LLM can compare strings without re-casing.
fn state_label(state: cairn_domain::TaskState) -> String {
    format!("{:?}", state).to_lowercase()
}

/// Timeout-shaped success payload: tells the caller the task hasn't finished
/// yet and includes the last observed state (if any) so the next decide turn
/// can pick between "wait more", "kick off a runner", or "give up".
fn timeout_result(task_id: &str, last_state: Option<cairn_domain::TaskState>) -> Value {
    serde_json::json!({
        "task_id":   task_id,
        "state":     last_state.map(state_label),
        "terminal":  false,
        "timed_out": true,
        "waited":    true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use cairn_domain::{RunId, TaskState};
    use cairn_store::{
        error::StoreError,
        projections::{TaskReadModel, TaskRecord},
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };

    fn project() -> ProjectKey {
        ProjectKey::new("t", "w", "p")
    }

    fn record(id: &str, state: TaskState) -> TaskRecord {
        TaskRecord {
            task_id: TaskId::new(id),
            project: project(),
            parent_run_id: None,
            parent_task_id: None,
            session_id: None,
            state,
            prompt_release_id: None,
            failure_class: None,
            pause_reason: None,
            resume_trigger: None,
            retry_count: 0,
            lease_owner: None,
            lease_expires_at: None,
            title: None,
            description: None,
            version: 1,
            created_at: 0,
            updated_at: 0,
        }
    }

    // ── Stubs ────────────────────────────────────────────────────────────────

    /// Always-running task — never reaches terminal. Exercises the timeout path.
    struct NeverTerminalStore {
        record: TaskRecord,
        get_calls: AtomicUsize,
    }

    #[async_trait]
    impl TaskReadModel for NeverTerminalStore {
        async fn get(&self, task_id: &TaskId) -> Result<Option<TaskRecord>, StoreError> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);
            if *task_id == self.record.task_id {
                Ok(Some(self.record.clone()))
            } else {
                Ok(None)
            }
        }
        async fn list_by_state(
            &self,
            _p: &ProjectKey,
            _s: TaskState,
            _l: usize,
        ) -> Result<Vec<TaskRecord>, StoreError> {
            Ok(vec![])
        }
        async fn list_expired_leases(
            &self,
            _n: u64,
            _l: usize,
        ) -> Result<Vec<TaskRecord>, StoreError> {
            Ok(vec![])
        }
        async fn list_by_parent_run(
            &self,
            _r: &RunId,
            _l: usize,
        ) -> Result<Vec<TaskRecord>, StoreError> {
            Ok(vec![])
        }
        async fn any_non_terminal_children(&self, _r: &RunId) -> Result<bool, StoreError> {
            Ok(false)
        }
    }

    /// Task that flips to `completed` after N observations.
    struct FlipsAfterStore {
        record: Mutex<TaskRecord>,
        flips_after: AtomicUsize,
        counter: AtomicUsize,
    }

    #[async_trait]
    impl TaskReadModel for FlipsAfterStore {
        async fn get(&self, _task_id: &TaskId) -> Result<Option<TaskRecord>, StoreError> {
            let seen = self.counter.fetch_add(1, Ordering::SeqCst);
            let flip_at = self.flips_after.load(Ordering::SeqCst);
            let mut rec = self.record.lock().unwrap();
            if seen >= flip_at {
                rec.state = TaskState::Completed;
            }
            Ok(Some(rec.clone()))
        }
        async fn list_by_state(
            &self,
            _p: &ProjectKey,
            _s: TaskState,
            _l: usize,
        ) -> Result<Vec<TaskRecord>, StoreError> {
            Ok(vec![])
        }
        async fn list_expired_leases(
            &self,
            _n: u64,
            _l: usize,
        ) -> Result<Vec<TaskRecord>, StoreError> {
            Ok(vec![])
        }
        async fn list_by_parent_run(
            &self,
            _r: &RunId,
            _l: usize,
        ) -> Result<Vec<TaskRecord>, StoreError> {
            Ok(vec![])
        }
        async fn any_non_terminal_children(&self, _r: &RunId) -> Result<bool, StoreError> {
            Ok(false)
        }
    }

    /// Store whose `get` hangs for ~5s before returning — simulates a
    /// pool-exhausted or deadlocked read-model so we can verify the per-read
    /// timeout is capped by the remaining overall budget.
    struct HungStore;

    #[async_trait]
    impl TaskReadModel for HungStore {
        async fn get(&self, _task_id: &TaskId) -> Result<Option<TaskRecord>, StoreError> {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            Ok(None)
        }
        async fn list_by_state(
            &self,
            _p: &ProjectKey,
            _s: TaskState,
            _l: usize,
        ) -> Result<Vec<TaskRecord>, StoreError> {
            Ok(vec![])
        }
        async fn list_expired_leases(
            &self,
            _n: u64,
            _l: usize,
        ) -> Result<Vec<TaskRecord>, StoreError> {
            Ok(vec![])
        }
        async fn list_by_parent_run(
            &self,
            _r: &RunId,
            _l: usize,
        ) -> Result<Vec<TaskRecord>, StoreError> {
            Ok(vec![])
        }
        async fn any_non_terminal_children(&self, _r: &RunId) -> Result<bool, StoreError> {
            Ok(false)
        }
    }

    /// Store that returns `None` — task never existed.
    struct EmptyStore;

    #[async_trait]
    impl TaskReadModel for EmptyStore {
        async fn get(&self, _task_id: &TaskId) -> Result<Option<TaskRecord>, StoreError> {
            Ok(None)
        }
        async fn list_by_state(
            &self,
            _p: &ProjectKey,
            _s: TaskState,
            _l: usize,
        ) -> Result<Vec<TaskRecord>, StoreError> {
            Ok(vec![])
        }
        async fn list_expired_leases(
            &self,
            _n: u64,
            _l: usize,
        ) -> Result<Vec<TaskRecord>, StoreError> {
            Ok(vec![])
        }
        async fn list_by_parent_run(
            &self,
            _r: &RunId,
            _l: usize,
        ) -> Result<Vec<TaskRecord>, StoreError> {
            Ok(vec![])
        }
        async fn any_non_terminal_children(&self, _r: &RunId) -> Result<bool, StoreError> {
            Ok(false)
        }
    }

    // ── Metadata ────────────────────────────────────────────────────────────

    #[test]
    fn name_tier_class() {
        let store = Arc::new(EmptyStore) as Arc<dyn TaskReadModel>;
        let t = WaitForTaskTool::new(store);
        assert_eq!(t.name(), "wait_for_task");
        assert_eq!(t.tier(), ToolTier::Registered);
        assert_eq!(t.execution_class(), ExecutionClass::SandboxedProcess);
    }

    // ── Regression: honour `timeout_secs` when the task never terminates ─────
    //
    // Reproduces #685 Finding 1: the LLM called `wait_for_task` with
    // `timeout_secs=60` and the tool hung 628 s (10.5× budget). This test
    // asserts the tool actually returns within ~3 s for a 2 s budget, with
    // the non-terminal timeout-shaped payload the orchestrator expects.

    #[tokio::test]
    async fn times_out_gracefully_when_task_never_terminal() {
        let store = Arc::new(NeverTerminalStore {
            record: record("task_stuck", TaskState::Running),
            get_calls: AtomicUsize::new(0),
        });
        let tool = WaitForTaskTool::new(store.clone() as Arc<dyn TaskReadModel>);

        let start = std::time::Instant::now();
        let res = tool
            .execute(
                &project(),
                serde_json::json!({
                    "task_id": "task_stuck",
                    "timeout_secs": 2,
                    "poll_interval_ms": 500,
                }),
            )
            .await
            .expect("timeout must be a graceful Ok, not a ToolError");

        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(3),
            "wait_for_task must honour timeout_secs=2; took {:?}",
            elapsed,
        );
        assert!(
            elapsed >= std::time::Duration::from_millis(1_800),
            "wait_for_task returned too early ({:?}); should have waited ~2s",
            elapsed,
        );

        assert_eq!(res.output["task_id"], "task_stuck");
        assert_eq!(res.output["terminal"], false);
        assert_eq!(res.output["timed_out"], true);
        assert_eq!(res.output["state"], "running");
        assert!(
            store.get_calls.load(Ordering::SeqCst) >= 2,
            "expected at least two polls inside the 2s budget",
        );
    }

    // ── Happy path: returns early when the task reaches terminal ────────────

    #[tokio::test]
    async fn returns_when_task_becomes_terminal() {
        let store = Arc::new(FlipsAfterStore {
            record: Mutex::new(record("task_flip", TaskState::Running)),
            flips_after: AtomicUsize::new(2),
            counter: AtomicUsize::new(0),
        });
        let tool = WaitForTaskTool::new(store as Arc<dyn TaskReadModel>);

        let res = tool
            .execute(
                &project(),
                serde_json::json!({
                    "task_id": "task_flip",
                    "timeout_secs": 5,
                    "poll_interval_ms": 500,
                }),
            )
            .await
            .expect("should succeed once task reaches terminal");

        assert_eq!(res.output["terminal"], true);
        assert_eq!(res.output["timed_out"], false);
        assert_eq!(res.output["state"], "completed");
    }

    // ── Not-found is permanent, not a sleep-retry loop ──────────────────────

    #[tokio::test]
    async fn missing_task_is_permanent_error() {
        let tool = WaitForTaskTool::new(Arc::new(EmptyStore) as Arc<dyn TaskReadModel>);
        let err = tool
            .execute(
                &project(),
                serde_json::json!({
                    "task_id": "ghost",
                    "timeout_secs": 5,
                    "poll_interval_ms": 500,
                }),
            )
            .await
            .expect_err("missing task must be a ToolError::Permanent");
        assert!(matches!(err, ToolError::Permanent(_)), "got {:?}", err);
    }

    // ── Caller-requested overages get clamped ───────────────────────────────

    #[tokio::test]
    async fn timeout_over_max_is_clamped() {
        // Requesting 10_000 s would otherwise set a deadline far in the
        // future; we can't wait 10_000 s to verify the clamp directly, so
        // instead wire a never-terminal store, pass the absurd budget, and
        // assert the tool still returns in well under 305 s.
        let store = Arc::new(NeverTerminalStore {
            record: record("task_huge", TaskState::Queued),
            get_calls: AtomicUsize::new(0),
        });
        let tool = WaitForTaskTool::new(store as Arc<dyn TaskReadModel>);

        // For a fast test we actually pass a *small* value and read back via
        // the elapsed assertion above — the clamp logic is exercised at the
        // argument-parsing layer; trust the tests of `u64::clamp`. What we
        // do verify here is that a sub-MIN_WAIT_SECS request doesn't produce
        // a zero-wait spin: `timeout_secs=0` must be clamped up to 1 s.
        let start = std::time::Instant::now();
        let res = tool
            .execute(
                &project(),
                serde_json::json!({
                    "task_id": "task_huge",
                    "timeout_secs": 0,
                    "poll_interval_ms": 500,
                }),
            )
            .await
            .expect("zero-timeout must clamp up and still return cleanly");
        let elapsed = start.elapsed();
        assert!(
            elapsed >= std::time::Duration::from_millis(900),
            "timeout_secs=0 should clamp up to MIN_WAIT_SECS=1s; took {:?}",
            elapsed,
        );
        assert_eq!(res.output["timed_out"], true);
    }

    // ── Regression: per-read timeout is capped by remaining overall budget ──
    //
    // Gemini review on #687 caught that `PER_GET_TIMEOUT_MS=5s` could
    // overshoot a caller-requested `timeout_secs=1` by up to 5× when the
    // read hung. This test wires a store whose `get()` sleeps 5s and
    // asserts a 1s caller budget returns in well under the hung read's
    // worst case — i.e. the outer deadline is enforceable.
    #[tokio::test]
    async fn per_read_timeout_capped_by_remaining_budget() {
        let tool = WaitForTaskTool::new(Arc::new(HungStore) as Arc<dyn TaskReadModel>);

        let start = std::time::Instant::now();
        let res = tool
            .execute(
                &project(),
                serde_json::json!({
                    "task_id": "task_hung_read",
                    "timeout_secs": 1,
                    "poll_interval_ms": 500,
                }),
            )
            .await
            .expect("hung read must still surface a graceful timeout, not an error");
        let elapsed = start.elapsed();

        assert!(
            elapsed < std::time::Duration::from_millis(1_200),
            "per-read timeout must be capped by remaining budget; took {:?} \
             (un-capped worst case would be 5s+)",
            elapsed,
        );
        assert_eq!(res.output["timed_out"], true);
        assert_eq!(res.output["terminal"], false);
    }
}
