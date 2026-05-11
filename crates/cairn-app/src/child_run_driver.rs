//! #670 G4 / RFC 027: `ChildRunDriver` — tokio loop that claims and
//! executes child subagent runs via the same orchestrator pipeline
//! the HTTP `/orchestrate` handler uses.
//!
//! ## What this module ships
//!
//! The child-run driver: a tokio-spawned background loop owned by
//! `AppState` that, each tick, scans the runs projection for
//! `Pending` rows with `parent_run_id IS NOT NULL`, dispatches each
//! to `drive_run_iteration` (the shared orchestrator helper extracted
//! from `orchestrate_run_handler_inner`), and respects a concurrency
//! semaphore so one busy tenant doesn't starve HTTP handlers.
//!
//! Child runs drive through the exact same gather → decide → execute
//! pipeline as operator-initiated runs — provider routing,
//! credentials, circuit breakers, dual checkpoints, approvals. The
//! driver is merely the pull-model dispatcher; the engine is shared.
//!
//! ## Feature flag (default-on, explicit opt-out)
//!
//! `CAIRN_CHILD_RUN_DRIVER_ENABLED` defaults to enabled. Set to
//! `"false"` / `"0"` / `"off"` / `"no"` (case-insensitive) to
//! disable. The opt-out list is deliberately forgiving because
//! operators disabling an otherwise-working feature reach for
//! whichever truthy-adjacent value feels natural; strict `"false"`
//! would burn someone mid-incident. The disabled branch is a cheap
//! atomic bump + sleep — zero store reads, zero FF calls.
//!
//! ## Boot ordering (non-negotiable)
//!
//! `main.rs` awaits `RecoveryService::recover_all(...)` to completion
//! **before** calling [`ChildRunDriver::start`]. This is enforced by
//! sequential `.await`s in `main.rs` boot step 4b → 4c; there is no
//! separate tokio task that could race. See RFC 027 §contract 3 for
//! the invariant.
//!
//! ## Shutdown
//!
//! The parent holds a [`ChildRunDriver`] handle and calls
//! [`ChildRunDriver::shutdown`] during graceful teardown (before the
//! tokio runtime drops). The loop observes the cancel token and exits
//! within one tick. On hard shutdown (panic, SIGKILL) the task is
//! dropped by the runtime — acceptable because the driver holds no
//! persistent state of its own; FF's lease machinery reclaims any
//! in-flight child on next boot.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use cairn_domain::RunId;
use cairn_store::projections::{RunReadModel, RunRecord};

use crate::handlers::runs::{drive_run_iteration, OrchestrateRequest};
use crate::state::AppState;

/// Feature flag: default-on. Set to `"false"`/`"0"`/`"off"`/`"no"`
/// (case-insensitive) to disable. Any other value — including unset
/// or the literal `"true"` — leaves the driver enabled. PR-1b-5
/// flipped the default from the PR-1b-3 scaffolding-off posture.
pub const ENABLED_ENV: &str = "CAIRN_CHILD_RUN_DRIVER_ENABLED";

/// Tokio task budget: max concurrent child-run iterations. Clamp
/// [1, 32]; default 4. Env: `CAIRN_CHILD_RUN_DRIVER_CONCURRENCY`.
pub const CONCURRENCY_ENV: &str = "CAIRN_CHILD_RUN_DRIVER_CONCURRENCY";
const CONCURRENCY_DEFAULT: usize = 4;
const CONCURRENCY_MIN: usize = 1;
const CONCURRENCY_MAX: usize = 32;

/// How long the loop sleeps between ticks when no work is available.
/// Short enough that a newly-spawned child is picked up within a
/// few hundred ms; long enough that the loop isn't a hot spin.
const IDLE_TICK: Duration = Duration::from_millis(500);

/// Max child rows the driver observes per tick. The scan goes
/// through `RunReadModel::list_driver_claimable_children` which
/// pushes `state IN ('pending', 'running') AND parent_run_id IS
/// NOT NULL` into the SQL layer (hitting `idx_runs_parent` partial
/// index on pg/sqlite), so non-child runs do NOT inflate the
/// result set — this limit bounds actual child rows.
const SCAN_LIMIT: usize = 128;

/// Metrics surface. Every counter is an atomic so the reader side
/// (Prometheus render) is lock-free. Cardinality is intentionally
/// low: per-driver totals, not per-run labels — child-run labels are
/// already carried by the existing per-run metrics.
#[derive(Debug, Default)]
pub struct ChildRunDriverMetrics {
    /// Ticks where the flag was off, so the driver did nothing.
    pub ticks_disabled: AtomicU64,
    /// Ticks where the flag was on and the driver scanned for work.
    pub ticks_enabled: AtomicU64,
    /// Pending child rows observed across all ticks (monotonic sum).
    pub observed_children_total: AtomicU64,
    /// Child iterations actually dispatched to `drive_run_iteration`.
    /// Distinct from `observed_children_total` because the in-flight
    /// filter may skip a child that's already being processed by an
    /// earlier tick's spawned task.
    pub iterations_dispatched_total: AtomicU64,
    /// Iterations that returned `Ok(response)` (the orchestrator
    /// loop ran to termination; the response's status is the HTTP
    /// shape the HTTP handler would have returned for the same run).
    pub iterations_ok_total: AtomicU64,
    /// Iterations that returned `Err(response)` (pre-loop early
    /// return: lease renewal failed, credential missing, breaker
    /// override invalid, etc.). Counted separately because the
    /// driver should surface these on a distinct dashboard series —
    /// they indicate configuration drift operators need to fix.
    pub iterations_err_total: AtomicU64,
    /// Backpressure hits — semaphore permits exhausted at tick start.
    /// Non-zero sustained value means `CONCURRENCY_MAX` should go up
    /// or the child-run iteration budget should go down.
    pub backpressure_total: AtomicU64,
}

/// Handle to a running driver. `Drop` does NOT stop the loop — call
/// [`Self::shutdown`] explicitly so the await-on-join completes before
/// the tokio runtime tears down.
pub struct ChildRunDriver {
    cancel: CancellationToken,
    join: Option<JoinHandle<()>>,
    metrics: Arc<ChildRunDriverMetrics>,
}

impl ChildRunDriver {
    /// Start the driver tokio task. Returns a handle; the caller is
    /// responsible for calling [`Self::shutdown`] before tearing down
    /// the runtime.
    ///
    /// The driver runs regardless of the feature-flag state — the
    /// flag is checked per-tick so operators can flip it via env
    /// and SIGHUP-style restart without reaching into this code.
    /// When the flag is off, every tick is a cheap atomic bump +
    /// sleep.
    pub fn start(state: Arc<AppState>) -> Self {
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let metrics = Arc::new(ChildRunDriverMetrics::default());
        let metrics_clone = metrics.clone();

        let concurrency = resolve_concurrency();
        let permits = Arc::new(Semaphore::new(concurrency));
        let in_flight: Arc<Mutex<HashSet<RunId>>> = Arc::new(Mutex::new(HashSet::new()));

        let join = tokio::spawn(async move {
            run_loop(state, cancel_clone, metrics_clone, permits, in_flight).await;
        });

        Self {
            cancel,
            join: Some(join),
            metrics,
        }
    }

    /// Metrics snapshot handle. Caller reads atomically — no lock.
    pub fn metrics(&self) -> Arc<ChildRunDriverMetrics> {
        self.metrics.clone()
    }

    /// Signal the loop to exit and await its shutdown. Idempotent —
    /// calling twice is a no-op after the first.
    pub async fn shutdown(&mut self) {
        self.cancel.cancel();
        if let Some(join) = self.join.take() {
            let _ = join.await;
        }
    }
}

impl std::fmt::Debug for ChildRunDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChildRunDriver")
            .field("cancelled", &self.cancel.is_cancelled())
            .field(
                "finished",
                &self.join.as_ref().is_none_or(|j| j.is_finished()),
            )
            .finish()
    }
}

fn resolve_concurrency() -> usize {
    resolve_concurrency_from(std::env::var(CONCURRENCY_ENV).ok().as_deref())
}

/// Pure helper: testable without mutating process env. `None` means
/// the env var is unset → return the default. Some("") is also
/// treated as unset (empty string isn't a valid number anyway).
fn resolve_concurrency_from(raw: Option<&str>) -> usize {
    raw.and_then(|s| s.parse::<usize>().ok())
        .map(|n| n.clamp(CONCURRENCY_MIN, CONCURRENCY_MAX))
        .unwrap_or(CONCURRENCY_DEFAULT)
}

fn driver_enabled() -> bool {
    driver_enabled_from(std::env::var(ENABLED_ENV).ok().as_deref())
}

/// Default-on. Explicit opt-out via `"false"`/`"0"`/`"off"`/`"no"`
/// (case-insensitive, trimmed). Anything else — including unset,
/// `"true"`, `"1"`, or operator typos — leaves the driver enabled.
/// The opt-out list is forgiving because operators disabling an
/// otherwise-working feature during an incident will reach for
/// whichever truthy-adjacent value feels natural; strict `"false"`
/// would burn someone.
fn driver_enabled_from(raw: Option<&str>) -> bool {
    let normalised = raw.map(|v| v.trim().to_ascii_lowercase());
    !matches!(
        normalised.as_deref(),
        Some("false") | Some("0") | Some("off") | Some("no")
    )
}

async fn run_loop(
    state: Arc<AppState>,
    cancel: CancellationToken,
    metrics: Arc<ChildRunDriverMetrics>,
    permits: Arc<Semaphore>,
    in_flight: Arc<Mutex<HashSet<RunId>>>,
) {
    tracing::info!(
        concurrency = permits.available_permits(),
        "child-run driver started (opt-out via {}=false)",
        ENABLED_ENV,
    );

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("child-run driver shutting down");
                return;
            }
            _ = tokio::time::sleep(IDLE_TICK) => {
                tick(&state, &metrics, &permits, &in_flight).await;
            }
        }
    }
}

async fn tick(
    state: &Arc<AppState>,
    metrics: &Arc<ChildRunDriverMetrics>,
    permits: &Arc<Semaphore>,
    in_flight: &Arc<Mutex<HashSet<RunId>>>,
) {
    if !driver_enabled() {
        metrics.ticks_disabled.fetch_add(1, Ordering::Relaxed);
        return;
    }
    metrics.ticks_enabled.fetch_add(1, Ordering::Relaxed);

    if permits.available_permits() == 0 {
        metrics.backpressure_total.fetch_add(1, Ordering::Relaxed);
        return;
    }

    // RFC 027 §contract 4 + PR-1b-4: claim predicate is
    //   state IN (Pending, Running) AND parent_run_id IS NOT NULL
    // Startup ordering guarantees recover_all has already transitioned
    // anything unrecoverable to Failed, so the filter cannot pick up
    // a crashed-and-wedged run that should be reclaimed by recovery.
    // Running is included so post-SIGKILL children (which stay in
    // Running per RFC 020's advisory-marker recovery for non-wedged
    // runs) get re-claimed by the driver — FF's atomic
    // `issue_grant_and_claim` rejects live-lease duplicates, so
    // including them is safe.
    let children = match RunReadModel::list_driver_claimable_children(
        state.runtime.store.as_ref(),
        SCAN_LIMIT,
    )
    .await
    {
        Ok(rows) => rows,
        Err(err) => {
            tracing::warn!(error = %err, "child-run driver scan failed");
            return;
        }
    };

    let count = children.len() as u64;
    if count == 0 {
        return;
    }
    metrics
        .observed_children_total
        .fetch_add(count, Ordering::Relaxed);

    for child in children {
        // Double-claim filter (task #164): two adjacent ticks can see
        // the same child row before the per-spawned-task cleanup
        // drains the in_flight set.
        // FF's `ff_claim_execution` rejects the second claim
        // atomically with `execution_not_eligible` so this is a
        // correctness non-issue, but filtering here avoids wasted
        // work + the `execution_not_eligible` warning noise FF
        // emits. The set is per-process, populated on dispatch and
        // drained when the spawned task returns.
        {
            let mut guard = in_flight.lock().unwrap_or_else(|e| e.into_inner());
            if !guard.insert(child.run_id.clone()) {
                continue;
            }
        }

        let Ok(permit) = permits.clone().try_acquire_owned() else {
            // No permits left — put the child back (we claimed the
            // in-flight slot but aren't going to dispatch it) and
            // break. The tick ends; next tick picks up where this
            // one left off.
            let mut guard = in_flight.lock().unwrap_or_else(|e| e.into_inner());
            guard.remove(&child.run_id);
            metrics.backpressure_total.fetch_add(1, Ordering::Relaxed);
            break;
        };

        let state_for_task = state.clone();
        let metrics_for_task = metrics.clone();
        let in_flight_for_task = in_flight.clone();
        let child_run_id_for_cleanup = child.run_id.clone();

        tokio::spawn(async move {
            let _permit = permit;
            // Panic-safe in-flight cleanup (Gemini review on #679,
            // MEDIUM). Without this Drop guard, a panic inside
            // `dispatch_child_iteration` would leak the RunId in the
            // HashSet forever — that run would never be dispatched
            // again by this process instance. The Drop impl runs even
            // on panic, and we recover from a poisoned lock so the
            // set stays usable if a sibling task already panicked
            // while holding the mutex.
            struct InFlightGuard {
                run_id: RunId,
                set: Arc<Mutex<HashSet<RunId>>>,
            }
            impl Drop for InFlightGuard {
                fn drop(&mut self) {
                    let mut g = self.set.lock().unwrap_or_else(|e| e.into_inner());
                    g.remove(&self.run_id);
                }
            }
            let _guard = InFlightGuard {
                run_id: child_run_id_for_cleanup,
                set: in_flight_for_task,
            };
            dispatch_child_iteration(state_for_task, child, &metrics_for_task).await;
        });
    }
}

/// Run one orchestrator iteration against `child`. The driver calls
/// the same `drive_run_iteration` helper the HTTP `/orchestrate`
/// handler uses so both paths share provider routing, credentials,
/// circuit breakers, dual checkpoints, and approvals.
async fn dispatch_child_iteration(
    state: Arc<AppState>,
    child: RunRecord,
    metrics: &Arc<ChildRunDriverMetrics>,
) {
    metrics
        .iterations_dispatched_total
        .fetch_add(1, Ordering::Relaxed);

    // Drive the child with its persisted per-run defaults — the
    // orchestrator helper's `body.max_iterations.or(persisted)` +
    // `body.goal.or(persisted)` fallback chain picks up whatever the
    // spawn path wrote to the run's defaults. The driver has no
    // operator-level override to impose; it's a system actor driving
    // what's already on the row.
    let body = OrchestrateRequest::default();
    let child_run_id = child.run_id.clone();
    let result = drive_run_iteration(state, child, body).await;

    // Gemini review on #679 (MEDIUM): the `Result<Response, Response>`
    // return shape splits "pre-loop error" vs "loop ran" for the
    // HTTP wrapper's dispatch convenience — but some pre-loop arms
    // return `200 OK` (already-terminal short-circuit) and some
    // post-loop arms return `502 BAD_GATEWAY` (AllProvidersExhausted).
    // Matching on `Ok/Err` would mis-classify both for metrics +
    // logs. Inspect the HTTP status instead: 2xx = iteration did
    // the right thing (succeeded or benignly short-circuited);
    // anything else = the driver should surface it for operator
    // attention.
    let response = match &result {
        Ok(r) => r,
        Err(r) => r,
    };
    let status = response.status();
    if status.is_success() {
        metrics.iterations_ok_total.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            run_id = %child_run_id,
            status = %status,
            "child-run driver: iteration completed",
        );
    } else {
        metrics.iterations_err_total.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            run_id = %child_run_id,
            status = %status,
            "child-run driver: iteration returned non-success status — \
             see drive_run_iteration logs for the classified reason \
             (lease renewal, credential missing, breaker override, \
             providers exhausted, etc.)",
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Default concurrency is 4 when the env var is unset.
    /// Uses the pure `resolve_concurrency_from` helper so the test
    /// doesn't race other parallel tests on the shared process env.
    #[test]
    fn resolve_concurrency_defaults_when_unset() {
        assert_eq!(resolve_concurrency_from(None), CONCURRENCY_DEFAULT);
        assert_eq!(resolve_concurrency_from(Some("")), CONCURRENCY_DEFAULT);
        assert_eq!(
            resolve_concurrency_from(Some("not-a-number")),
            CONCURRENCY_DEFAULT
        );
    }

    /// Valid numeric override is clamped into [MIN, MAX].
    #[test]
    fn resolve_concurrency_clamps_within_bounds() {
        assert_eq!(resolve_concurrency_from(Some("100")), CONCURRENCY_MAX);
        assert_eq!(resolve_concurrency_from(Some("0")), CONCURRENCY_MIN);
        assert_eq!(resolve_concurrency_from(Some("8")), 8);
        assert_eq!(resolve_concurrency_from(Some("1")), CONCURRENCY_MIN);
        assert_eq!(resolve_concurrency_from(Some("32")), CONCURRENCY_MAX);
    }

    /// Default-on: unset, empty, whitespace, and operator typos all
    /// leave the driver enabled. Only the explicit opt-out tokens
    /// disable it.
    #[test]
    fn driver_enabled_default_on_except_for_explicit_opt_out() {
        // Opt-out tokens (disabled).
        for v in [
            "false", "FALSE", "False", "fAlSe", "0", "off", "OFF", "no", "No", " false ", "\tfalse",
        ] {
            assert!(
                !driver_enabled_from(Some(v)),
                "expected disabled for opt-out token {v:?}"
            );
        }
        // Default-on tokens (enabled).
        for v in ["true", "1", "yes", "on", "", "something-weird"] {
            assert!(
                driver_enabled_from(Some(v)),
                "expected enabled for non-opt-out value {v:?}"
            );
        }
        assert!(
            driver_enabled_from(None),
            "expected enabled when env unset (default-on posture)"
        );
    }
}
