//! #670 G4 / RFC 027: `ChildRunDriver` — tokio loop that claims and
//! executes child subagent runs via the same FF primitive the main
//! orchestrator uses.
//!
//! ## What this module ships in PR-1b-3
//!
//! The **scaffolding** only: the tokio loop, cancel token, lifecycle
//! management (start / stop / JoinHandle), the feature-flag gate, the
//! claim predicate (`state == Pending AND parent_run_id IS NOT NULL`),
//! the runtime-isolation semaphores, and the metrics-counter atoms.
//!
//! The driver does **not** actually claim or execute anything yet.
//! Every tick it:
//!
//! 1. Respects the `CAIRN_CHILD_RUN_DRIVER_ENABLED` gate — when off
//!    (default), the loop sleeps and bumps the idle counter; no store
//!    read, no FF call, no work. This is the posture for PR-1b-3 → 1b-4.
//! 2. When on (flipped by PR-1b-5), the loop performs a bounded scan
//!    for pending children, respects the concurrency semaphore, and
//!    logs what it would claim. The actual `issue_grant_and_claim`
//!    call + orchestrator invocation is wired in PR-1b-5 alongside the
//!    flag flip so the enablement and the behaviour change land in the
//!    same PR with dedicated tests.
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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use cairn_runtime::runs::RunService;
use cairn_store::projections::RunReadModel;
use cairn_store::InMemoryStore;

/// Feature flag: when set to `"true"` (case-insensitive), the driver
/// performs its scan loop. Any other value — including unset — leaves
/// the driver in no-op mode: it ticks, sleeps, and does nothing.
/// Default false per RFC 027. PR-1b-5 flips the default to true.
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
/// through `RunReadModel::list_pending_children` which pushes
/// `state = 'pending' AND parent_run_id IS NOT NULL` into the SQL
/// layer (hitting `idx_runs_parent` partial index on pg/sqlite), so
/// pending ROOT runs do NOT inflate the result set — this limit
/// bounds actual child rows, not the full Pending population.
/// (Gemini review on #678, HIGH: earlier design used a generic
/// `list_by_state` scan that could be starved by many pending
/// roots.)
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
    /// Ratio `ticks_enabled / observed_children_total` roughly tracks
    /// queue depth over time.
    pub observed_children_total: AtomicU64,
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
    pub fn start(store: Arc<InMemoryStore>, runs: Arc<dyn RunService>) -> Self {
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let metrics = Arc::new(ChildRunDriverMetrics::default());
        let metrics_clone = metrics.clone();

        let concurrency = resolve_concurrency();
        let permits = Arc::new(Semaphore::new(concurrency));

        let join = tokio::spawn(async move {
            run_loop(store, runs, cancel_clone, metrics_clone, permits).await;
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

/// Pure helper: testable without mutating process env.
fn driver_enabled_from(raw: Option<&str>) -> bool {
    raw.is_some_and(|v| v.eq_ignore_ascii_case("true"))
}

async fn run_loop(
    store: Arc<InMemoryStore>,
    _runs: Arc<dyn RunService>,
    cancel: CancellationToken,
    metrics: Arc<ChildRunDriverMetrics>,
    permits: Arc<Semaphore>,
) {
    tracing::info!(
        concurrency = permits.available_permits(),
        "child-run driver started (gated on {})",
        ENABLED_ENV,
    );

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("child-run driver shutting down");
                return;
            }
            _ = tokio::time::sleep(IDLE_TICK) => {
                tick(&store, &metrics, &permits).await;
            }
        }
    }
}

async fn tick(
    store: &Arc<InMemoryStore>,
    metrics: &Arc<ChildRunDriverMetrics>,
    permits: &Arc<Semaphore>,
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

    // RFC 027 §contract 4: claim predicate is
    //   state == Pending AND parent_run_id IS NOT NULL
    // Startup ordering guarantees recover_all has already transitioned
    // anything unrecoverable to Failed, so the Pending filter cannot
    // pick up a crashed run that should be reclaimed by recovery.
    //
    // `list_pending_children` pushes both clauses into the SQL layer
    // (Gemini review on #678, HIGH): a generic `list_by_state` scan
    // would starve children whenever the Pending population is
    // dominated by pending ROOT runs.
    let children = match RunReadModel::list_pending_children(store.as_ref(), SCAN_LIMIT).await {
        Ok(rows) => rows,
        Err(err) => {
            tracing::warn!(error = %err, "child-run driver scan failed");
            return;
        }
    };

    let count = children.len() as u64;
    if count > 0 {
        metrics
            .observed_children_total
            .fetch_add(count, Ordering::Relaxed);

        // PR-1b-3 scaffolding: observe only. The actual claim +
        // orchestrator-loop-invocation wiring ships in PR-1b-5
        // alongside the feature-flag flip so both land together
        // with a dedicated integration test (RFC 027 §PR-1b-5).
        tracing::debug!(
            pending_children = count,
            "child-run driver observed pending children (scaffolding \
             scan; claim path wires in PR-1b-5)",
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

    /// Feature flag is case-insensitive but strict on value shape —
    /// "true" / "TRUE" yes, everything else (including "1", "yes") no.
    /// Strictness means operators cannot accidentally enable the
    /// driver by setting the var to a truthy-looking but invalid value.
    #[test]
    fn driver_enabled_requires_literal_true() {
        for v in ["true", "TRUE", "True", "tRuE"] {
            assert!(driver_enabled_from(Some(v)), "expected enabled for {v:?}");
        }
        for v in ["false", "FALSE", "0", "1", "yes", "on", ""] {
            assert!(!driver_enabled_from(Some(v)), "expected disabled for {v:?}");
        }
        assert!(
            !driver_enabled_from(None),
            "expected disabled when unset (None)"
        );
    }
}
