//! Background lease-keeper for long-running orchestrate calls (#639, #655).
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
//! # The fix (#639 + #655)
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
//! ## Suspension awareness (#655)
//!
//! #647 shipped the keeper, but dogfood round 3 (2026-05-03) re-
//! reproduced `TerminalWriteDeadlock` on real multi-iteration
//! approval-gated runs. Root cause: FF's phase machine puts an
//! execution into a non-runnable phase
//! (`attempt_interrupted` / `waiting_approval`) while a tool-call
//! approval is pending; `ff_renew_lease` and `ff_claim_execution`
//! both reject with `execution_not_eligible` in that phase. The
//! #647 keeper logged the conflict at DEBUG and retried on the next
//! tick, which accomplished nothing except log churn — the lease's
//! wall-clock expiry still ticked down untouched. When the operator
//! finally resolved the approval and cairn dispatched the terminal
//! FCALL, the lease was dead → F64 30 s recovery exhausted →
//! `Failed(TerminalWriteDeadlock)`.
//!
//! The cairn-side fix (Option 2 from #655): the keeper observes the
//! projection at each tick and SKIPS the renew FCALL when the run is
//! suspended (either `ApprovalReadModel::has_pending_for_run` OR a
//! pending `ToolCallApprovalReadModel` row for this run). Suspended
//! ticks log at TRACE — no FF FCALL, no churn. When the keeper
//! observes the transition from suspended → runnable, it immediately
//! fires a renew (which, via `RunService::renew_lease_if_stale`,
//! falls back to a full `issue_grant_and_claim` when the lease
//! wall-clock has expired) so the wall-clock deadline is reset
//! before the next terminal FCALL leaves the orchestrator.
//!
//! ## Phase-aware classification (#666)
//!
//! Dogfood round 5 (2026-05-03) re-reproduced `TerminalWriteDeadlock`
//! on a rapid-auto-resume cycle. Root cause: cairn's projection is
//! eventually consistent with `RunState`, which is eventually
//! consistent with FF's `lifecycle_phase`. On 19 approvals in 8 min
//! the projection briefly reads "not suspended" during the
//! `runnable → active` transition (FF has delivered the signal but
//! not yet claim_resumed); the keeper fires a renew into that
//! microscopic window, FF rejects `execution_not_eligible`, and the
//! pre-#666 `is_transient_phase_conflict` classifier silently
//! retried — burning wall-clock until the lease expired.
//!
//! #666 replaces the projection probe with a direct read of FF's
//! 7-dimension `StateVector` via `Engine::read_execution_info`
//! (FF 0.15). The keeper now classifies the execution's exact phase
//! each tick ([`PhaseClassification`]) and only fires a renew when
//! FF itself reports the execution is in a renewable shape
//! (`lifecycle_phase = Active`, `attempt_state = RunningAttempt`,
//! `ownership_state = Leased`). Any other classification skips the
//! FCALL. This closes the sample-race deterministically — cairn no
//! longer infers FF's phase from a lagging projection.
//!
//! The `is_transient_phase_conflict` silent-retry branch in the
//! keeper loop is gone. With the probe in place, a phase conflict
//! on renew after a positive classification is a real surprise and
//! logs at WARN so we can forensically reconstruct the race.
//!
//! ## Probe→renew race recovery (#685 Finding 3)
//!
//! Dogfood 2026-05-05 (`run_670_1778021965`) uncovered a residual
//! race under 30 s+ LLM latency: the probe says `RenewableActive`,
//! the renew FCALL arrives ~40 s later (tokio interval slack + the
//! renew's own `describe_execution` round-trip + FF's scanner
//! cadence), and FF rejects `execution_not_eligible` because the
//! state vector has since flipped. The pre-#685 keeper treated that
//! as fatal and exited — which is strictly worse than retrying,
//! because a subsequent `/orchestrate` HTTP call is needed to
//! re-spawn the keeper, and if the operator is waiting on a 2-minute
//! LLM response there is no HTTP call in flight. The lease then
//! expires while the run looks healthy from the operator's side.
//!
//! The fix is a bounded re-probe on phase-conflict rejections:
//!
//!   * Re-probe reports `Terminal` / `ExecutionNotFound`: the run
//!     completed or vanished between probe and renew; exit clean.
//!   * Re-probe reports a non-renewable phase: we raced a legitimate
//!     transition (approval landed, signal delivered, scanner marked
//!     the attempt); log INFO, continue — the next tick observes the
//!     new phase on its own probe.
//!   * Re-probe STILL says `RenewableActive`: cairn-fabric and FF
//!     disagree on the state vector (scanner mid-flight, or FF
//!     reshaped the phase inside the RPC). Log WARN with both
//!     classifications, bump the `probe_renew_classification_mismatch`
//!     observability counter, and continue — bounded by
//!     [`KEEPER_CLASSIFICATION_MISMATCH_CAP`] consecutive mismatches
//!     before we exit and let a later HTTP call re-spawn a fresh
//!     keeper.
//!
//! Non-phase-conflict errors preserve the #666 exit-on-surprise
//! contract: transport failures and operator-revoked leases should
//! not silently retry.
//!
//! # Exit conditions
//!
//! The keeper **task** exits when ANY of:
//!
//! 1. **Terminal state observed.** Either the `StateVector` probe
//!    reports [`LifecyclePhase::Terminal`], or `renew_lease_if_stale`
//!    returns a `RunRecord` whose `state.is_terminal()` is true. The
//!    keeper's job is done — the terminal-FCALL path already landed
//!    the final lifecycle flip.
//! 2. **Non-transient renew error.** The run is gone (`NotFound`), the
//!    FCALL hit a terminal-state conflict, or the fabric reported
//!    `lease_expired` which we can't repair. Continuing to loop would
//!    just spam errors.
//! 3. **Cancellation token fired.** The registry's shutdown path
//!    (called from the process shutdown hook) cancels every keeper in
//!    parallel so the runtime can drain cleanly. Idempotent —
//!    re-triggering a cancelled token is a no-op.
//!
//! On exit conditions 1 and 2 (natural exit — terminal state or
//! non-transient error) the keeper task removes its own entry from
//! the registry before returning. The task holds a `Weak<KeeperMap>`
//! that it upgrades at the natural-exit point; if the upgrade
//! succeeds (registry still alive), it locks the map and removes its
//! run-id entry. This keeps the registry bounded by the set of
//! *currently live* runs rather than growing monotonically with every
//! completed run since process start.
//!
//! The `Weak` side is the keeper loop, so the keeper does not own a
//! strong reference to the registry — no reference cycle between the
//! registry's `JoinHandle` and the task's self-removal path.
//!
//! Exit condition 3 (cancellation via `shutdown_all`) does **not**
//! self-remove. `shutdown_all` has already drained the entire map
//! under the registry mutex before awaiting any join; a cancelled
//! keeper that then tried to re-acquire the lock and remove its
//! (already-drained) entry would be a no-op at best and a subtle
//! race at worst. Keeping the cancellation branch strictly "return
//! immediately" makes the contract between keeper and registry
//! one-directional on shutdown: registry drains, keepers stop.
//!
//! Stale-entry reaping on `ensure_running(same_run_id, ...)` is
//! preserved as a second line of defence — if a keeper task panics
//! or self-removal races with shutdown in an unexpected way, the
//! next `ensure_running` call for the same run still sweeps a
//! finished `JoinHandle` before spawning its replacement.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use cairn_domain::{RunId, SessionId};
use cairn_fabric::engine::{
    AttemptState, Engine, ExecutionId, ExecutionInfo, LifecyclePhase, OwnershipState,
};
use cairn_runtime::RunService;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Map of live keeper handles keyed by `RunId`. Held behind an `Arc`
/// by [`LeaseKeeperRegistry`] so the keeper loop can hold a `Weak`
/// reference and self-remove its entry on natural exit without
/// creating a strong-reference cycle between the registry and the
/// `JoinHandle` it owns. See [`LeaseKeeperRegistry`] docs.
type KeeperMap = Mutex<HashMap<RunId, LeaseKeeperHandle>>;

// The inline-doc reference to the #655 projection-based probe is
// retained above for historical context; production code no longer
// reads `InMemoryStore` / `ApprovalReadModel` /
// `ToolCallApprovalReadModel` from the keeper (issue #666 replaced
// the projection probe with FF's `read_execution_info`). Tests that
// cover the legacy projection path remain in `mod tests` and import
// those types locally.

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

/// Hard ceiling on the keeper sleep interval when operators override
/// it via `CAIRN_LEASE_KEEPER_INTERVAL_MS`. Ticks longer than one
/// minute are effectively "off" for operator-paced approval flows —
/// the whole point of the keeper is sub-TTL wall-clock coverage. A
/// setting above the ceiling is clamped so a misconfigured env var
/// (e.g. `3600000` meant for a scheduler) can't wedge the keeper.
const KEEPER_MAX_INTERVAL_MS: u64 = 60_000;

/// Env var for operator-overridable keeper interval (#666). Clamped
/// into [[`KEEPER_MIN_INTERVAL_MS`], [`KEEPER_MAX_INTERVAL_MS`]]; unset
/// falls back to `lease_ttl_ms / 3` which matches FF's internal
/// renewer cadence.
const KEEPER_INTERVAL_ENV: &str = "CAIRN_LEASE_KEEPER_INTERVAL_MS";

/// Consecutive-tick cap on the #685 classification-mismatch retry
/// path. When `renew_lease_if_stale` returns a phase-conflict
/// rejection immediately after a positive `RenewableActive` probe,
/// and the subsequent re-probe STILL says `RenewableActive`, the
/// keeper is racing FF's internal scanner / expiry cadence in a
/// window the probe cannot close. One or two of these per run is
/// expected under 30 s+ LLM latency (issue #685 Finding 3 repro);
/// three in a row means something is structurally wrong — the lease
/// is probably dead and FF just hasn't rolled the expiry forward
/// yet, OR cairn-fabric and FF disagree on the state vector. In
/// either case we exit so a subsequent `/orchestrate` HTTP call can
/// spawn a fresh keeper against whatever FF state is current.
///
/// The cap is a bound, not a heuristic: the keeper tick interval
/// (500 ms floor, `lease_ttl_ms / 3` default) is the natural rate
/// limit. Three consecutive mismatches at the 500 ms floor is
/// 1.5 s of wall-clock; at the 30 s/3 default it's 30 s. Both are
/// safely below a minute-scale lease TTL, so the keeper never
/// burns through a full lease retrying.
const KEEPER_CLASSIFICATION_MISMATCH_CAP: u32 = 3;

/// Phase classification driving the keeper's per-tick decision
/// whether to issue `ff_renew_lease` (#666).
///
/// Derived from FF's 7-dimension `StateVector` via
/// [`Engine::read_execution_info`]. The keeper probes FF directly
/// rather than inferring phase from cairn's projection, which lags
/// FF on rapid suspend/resume cycles. Variants map 1:1 to the keeper
/// loop's action:
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhaseClassification {
    /// `lifecycle_phase = Active`, `attempt_state = RunningAttempt`,
    /// `ownership_state = Leased`. FF will accept `ff_renew_lease`.
    RenewableActive,
    /// `lifecycle_phase = Suspended`. Waiting for signal/approval/
    /// callback — renew is rejected with `execution_not_active`
    /// (flowfabric.lua:409). Skip.
    Suspended,
    /// `lifecycle_phase = Runnable`. Post-`ff_deliver_signal` window
    /// between cairn's signal-delivery FCALL and FF's next
    /// `ff_claim_resumed_execution`. Transient; renew would reject
    /// `execution_not_eligible`. Skip.
    RunnableUnclaimed,
    /// `lifecycle_phase = Active` but `attempt_state = AttemptInterrupted`.
    /// FF's scanner has marked the lease reclaimable mid-tool-record;
    /// renewing here fails the attempt-state gate. Skip.
    AttemptInterrupted,
    /// `lifecycle_phase = Terminal`. The run has finished (success,
    /// failure, cancel, expire). The keeper exits.
    Terminal,
    /// Any phase the keeper does not explicitly recognise — e.g.
    /// `lifecycle_phase = Submitted`, a future FF variant, or a
    /// state vector where Active meets a non-Leased ownership (lease
    /// expired reclaimable, revoked). Conservative default: skip the
    /// renew, re-probe on the next tick. The keeper never exits on
    /// this variant — only [`Self::Terminal`] and the probe returning
    /// `None` (execution vanished) terminate the loop.
    PhaseInFlight,
    /// [`Engine::read_execution_info`] returned `Ok(None)` — the
    /// execution does not exist in FF (never submitted, or purged).
    /// Keeper exits; there is nothing to renew.
    ExecutionNotFound,
}

impl PhaseClassification {
    /// Classify an [`ExecutionInfo`] into the keeper's action
    /// vocabulary. Pure function — exhaustive unit tests in the
    /// module's `tests` submodule table-drive every relevant
    /// `(LifecyclePhase, AttemptState, OwnershipState)` combination.
    ///
    /// Public so integration tests can stage a real FF execution
    /// through its phase transitions (start → claim →
    /// enter_waiting_approval → resolve_approval) and assert the
    /// classifier returns the expected variant for the resulting
    /// state vector. See `tests/test_666_rapid_auto_resume_keeper.rs`
    /// for the regression test that proves #666's probe path closes
    /// the runnable-unclaimed race.
    pub fn from_info(info: &ExecutionInfo) -> Self {
        let sv = &info.state_vector;
        match sv.lifecycle_phase {
            LifecyclePhase::Terminal => Self::Terminal,
            LifecyclePhase::Suspended => Self::Suspended,
            LifecyclePhase::Runnable => Self::RunnableUnclaimed,
            LifecyclePhase::Active => match (sv.attempt_state, sv.ownership_state) {
                (AttemptState::AttemptInterrupted, _) => Self::AttemptInterrupted,
                (AttemptState::RunningAttempt, OwnershipState::Leased) => Self::RenewableActive,
                // Active + any other combo: lease expired/revoked, or
                // an attempt-state that isn't running (pending retry,
                // pending replay, terminal, none). Treat as in-flight
                // — we can't cleanly renew and we don't own the
                // forward progress, so re-probe next tick.
                _ => Self::PhaseInFlight,
            },
            // Submitted is strictly transient (pre-first-resolution);
            // any future LifecyclePhase variant lands here too.
            LifecyclePhase::Submitted => Self::PhaseInFlight,
        }
    }
}

/// Probe FF for the given execution's current phase classification.
///
/// On transport / validation errors from `read_execution_info`, logs
/// at WARN and returns [`PhaseClassification::PhaseInFlight`] — the
/// loop skips the renew this tick and re-probes next tick. This is
/// an **intentional behaviour change from pre-#666**, where a
/// projection-read failure fell through to "proceed with renew"
/// (accepting the risk of a 409 on a now-suspended execution). The
/// new default is safer for two reasons: (1) the keeper is
/// belt-and-suspenders — the orchestrate handler's F51 entry-time
/// `renew_lease_if_stale` still fires on the next HTTP call, so the
/// existing lease doesn't go unchecked; (2) a bad probe is most
/// likely to happen when FF itself is unhappy, and adding a
/// known-likely-to-409 FCALL on top of that does nothing except
/// churn logs. Sustained probe failures surface at WARN so an
/// operator sees the keeper is flying blind.
///
/// On `Ok(None)` (execution missing) we return
/// [`PhaseClassification::ExecutionNotFound`] so the keeper loop can
/// exit cleanly.
async fn classify_phase(
    engine: &dyn Engine,
    run_id: &RunId,
    execution_id: &ExecutionId,
) -> PhaseClassification {
    match engine.read_execution_info(execution_id).await {
        Ok(Some(info)) => PhaseClassification::from_info(&info),
        Ok(None) => PhaseClassification::ExecutionNotFound,
        Err(err) => {
            tracing::warn!(
                run_id = %run_id,
                execution_id = %execution_id,
                error = %err,
                "#666 lease keeper: read_execution_info failed; skipping renew this tick"
            );
            PhaseClassification::PhaseInFlight
        }
    }
}

/// Test-only observability hook that records keeper-loop behaviour
/// at the two points that matter for the #666 regression:
///
/// * Every time the keeper decides to call
///   `RunService::renew_lease_if_stale`,
///   `renew_attempts` is incremented **before** the FCALL is issued.
/// * Every time that FCALL returns an `execution_not_eligible` /
///   `execution_not_eligible_for_attempt` / `execution_not_active`
///   class error — i.e. the exact FF phase-conflict rejections the
///   pre-#666 keeper's silent-retry classifier papered over —
///   `renew_rejections` is incremented.
/// * Every time a phase-conflict rejection fires after a positive
///   probe AND the re-probe still reports `RenewableActive` (issue
///   #685 Finding 3) — the exact probe→renew race that long LLM
///   calls widen — `probe_renew_classification_mismatch` is
///   incremented. Pre-#685 this symptom exited the keeper; post-#685
///   the keeper re-probes and skips the tick instead, so this
///   counter distinguishes "keeper raced FF and recovered" from
///   "keeper hit a legitimate phase transition".
/// * `tick_completed.notify_waiters()` fires at the end of every
///   keeper-loop iteration (after the phase probe, after any renew
///   result is handled). Test code blocks on
///   `tick_completed.notified().await` to deterministically wait
///   for N ticks without sleeping.
///
/// The hook is public + outside `#[cfg(test)]` because integration
/// tests live under `tests/` — a separate compilation unit that
/// cannot see crate-private items. Production callers never touch
/// the hook: the public `ensure_running` ignores observability,
/// and the `ensure_running_with_observability` entry point is
/// documented test-only.
///
/// The counters are `AtomicUsize` (not `Atomic{u,i}64`) so the hook
/// is platform-agnostic — `AtomicUsize` is word-sized on every
/// Rust target. `Ordering::SeqCst` is used on both reads and writes
/// because correctness beats performance on a test hook that fires
/// at keeper-tick cadence (500 ms floor).
///
/// See `tests/test_666_rapid_auto_resume_keeper.rs` for the
/// deterministic pre-fix / post-fix behavioural contrast built on
/// top of this hook.
#[derive(Debug, Default)]
pub struct KeeperObservability {
    /// Count of `RunService::renew_lease_if_stale` FCALLs the keeper
    /// has decided to issue. Incremented immediately before the
    /// `select!` that awaits the RPC.
    pub renew_attempts: AtomicUsize,
    /// Count of renew FCALLs rejected by FF with a phase-conflict
    /// class error (`execution_not_eligible`,
    /// `execution_not_eligible_for_attempt`, `execution_not_active`,
    /// `lease_expired`). These are the specific rejections the
    /// pre-#666 `is_transient_phase_conflict` classifier silently
    /// retried on, causing the lease-expiry bug.
    pub renew_rejections: AtomicUsize,
    /// Count of classification mismatches observed on the #685
    /// re-probe path: the initial probe said `RenewableActive`, the
    /// renew FCALL rejected with a phase-conflict error, and the
    /// re-probe STILL says `RenewableActive`. This signals the
    /// probe→renew race that long LLM calls widen (issue #685
    /// Finding 3). Operators watching this counter climb know the
    /// keeper is racing FF's scanner rather than hitting a legit
    /// phase transition — which is distinguishable because the
    /// other branch (re-probe flips to non-renewable) is the
    /// expected happy path when approval-wait or suspend lands
    /// between probe and renew.
    pub probe_renew_classification_mismatch: AtomicUsize,
    /// Notified exactly once at the end of every keeper-loop
    /// iteration (i.e. after the phase probe is evaluated and,
    /// where applicable, after a renew FCALL completes). Tests use
    /// `notified().await` in a loop to wait for N ticks without
    /// introducing timing-dependent assertions.
    ///
    /// `Notify` is the right primitive here: it's one-shot per
    /// `notify_waiters()`, it wakes all currently-waiting
    /// `notified()` futures, and it never "drops" a notification
    /// the waiter registered for (unlike `notify_one` which can
    /// race). A test that wants to wait for N ticks arms N
    /// `notified()` futures, then awaits them in sequence — each
    /// future only resolves on a subsequent `notify_waiters()`
    /// call, so N ticks are sampled deterministically.
    pub tick_completed: Notify,
}

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
/// The map is held behind an inner `Arc<KeeperMap>` so the keeper
/// task can be handed a `Weak<KeeperMap>` and self-remove its entry
/// on natural exit (see module-level docs). Without the `Arc`
/// indirection the only self-removal option would be passing the
/// whole `Arc<LeaseKeeperRegistry>` into the task, which creates a
/// reference cycle against the `JoinHandle` the registry owns.
///
/// The registry is owned by `AppState` and lives for the process
/// lifetime. `shutdown_all` drains every keeper before the runtime
/// tears down; omit the call only in tests that exercise the keeper
/// itself and don't mind keepers being torn down by the runtime on
/// process exit.
#[derive(Debug, Default)]
pub struct LeaseKeeperRegistry {
    inner: Arc<KeeperMap>,
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
    ///
    /// `engine` is the cairn-fabric handle the keeper uses to probe
    /// FF's per-tick phase classification (#666); `execution_id` is
    /// the deterministic FF execution identifier the caller minted
    /// via `cairn_fabric::id_map::session_run_to_execution_id`. The
    /// keeper calls [`Engine::read_execution_info`] each tick and
    /// only issues `ff_renew_lease` when the phase classifier reports
    /// `RenewableActive`. Suspended / runnable-unclaimed / attempt-
    /// interrupted phases skip the renew — FF would reject with
    /// `execution_not_eligible` / `execution_not_active` anyway, and
    /// cairn's projection is too lagging to classify them reliably.
    ///
    /// The per-tick sleep defaults to `lease_ttl_ms / 3`, clamped to
    /// [`KEEPER_MIN_INTERVAL_MS`]. Operators can override with the
    /// `CAIRN_LEASE_KEEPER_INTERVAL_MS` env var (clamped to
    /// [`KEEPER_MIN_INTERVAL_MS`, `KEEPER_MAX_INTERVAL_MS`]).
    pub async fn ensure_running(
        &self,
        run_id: RunId,
        session_id: SessionId,
        execution_id: ExecutionId,
        runs: Arc<dyn RunService>,
        engine: Arc<dyn Engine>,
        lease_ttl_ms: u64,
    ) {
        self.ensure_running_inner(
            run_id,
            session_id,
            execution_id,
            runs,
            engine,
            lease_ttl_ms,
            None,
        )
        .await;
    }

    /// Test-only variant of [`Self::ensure_running`] that installs a
    /// [`KeeperObservability`] hook on the spawned keeper. The hook
    /// counts every `renew_lease_if_stale` FCALL the keeper issues
    /// (and every FF phase-conflict rejection that FCALL returns) and
    /// notifies after every loop iteration. Integration tests under
    /// `tests/` use this to deterministically drive and observe the
    /// keeper's behaviour without sleeps or timing-dependent retries.
    ///
    /// Production code MUST NOT call this method — the observability
    /// hook is a test instrumentation surface, not a public API. The
    /// method is public only because Rust integration tests live in a
    /// separate compilation unit and cannot see crate-private items.
    pub async fn ensure_running_with_observability(
        &self,
        run_id: RunId,
        session_id: SessionId,
        execution_id: ExecutionId,
        runs: Arc<dyn RunService>,
        engine: Arc<dyn Engine>,
        lease_ttl_ms: u64,
        observability: Arc<KeeperObservability>,
    ) {
        self.ensure_running_inner(
            run_id,
            session_id,
            execution_id,
            runs,
            engine,
            lease_ttl_ms,
            Some(observability),
        )
        .await;
    }

    async fn ensure_running_inner(
        &self,
        run_id: RunId,
        session_id: SessionId,
        execution_id: ExecutionId,
        runs: Arc<dyn RunService>,
        engine: Arc<dyn Engine>,
        lease_ttl_ms: u64,
        observability: Option<Arc<KeeperObservability>>,
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

        let interval_ms = keeper_interval_ms(lease_ttl_ms);
        let interval = Duration::from_millis(interval_ms);
        let cancel = CancellationToken::new();
        let worker_cancel = cancel.clone();
        let task_run_id = run_id.clone();
        let task_session = session_id.clone();
        // Hand the keeper a `Weak` pointer to the map so it can
        // self-remove its entry on natural exit without creating a
        // strong-reference cycle with the `JoinHandle` stored in that
        // same map. See module-level docs for the lifecycle contract.
        let weak_map = Arc::downgrade(&self.inner);
        let join = tokio::spawn(async move {
            run_keeper_loop(
                task_run_id,
                task_session,
                execution_id,
                runs,
                engine,
                interval,
                worker_cancel,
                observability,
                weak_map,
            )
            .await;
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

/// Pure parse/clamp helper for the keeper-interval env override.
///
/// Returns `Some(clamped_ms)` when the env var is set and parses as a
/// `u64`; returns `None` when the env var is unset OR malformed so the
/// caller falls back to the default. Split out from
/// [`keeper_interval_ms`] so it's reachable from unit tests without
/// depending on the process-wide `OnceLock` cache.
fn parse_env_interval_override(raw: Option<&str>, lease_ttl_ms: u64) -> Option<u64> {
    let raw = raw?;
    match raw.parse::<u64>() {
        Ok(ms) => Some(ms.clamp(KEEPER_MIN_INTERVAL_MS, KEEPER_MAX_INTERVAL_MS)),
        Err(err) => {
            tracing::warn!(
                env = KEEPER_INTERVAL_ENV,
                value = %raw,
                error = %err,
                lease_ttl_ms,
                "#666 lease keeper: ignoring malformed env override; \
                 using lease_ttl_ms / 3 default"
            );
            None
        }
    }
}

/// Resolve the keeper's per-tick sleep interval.
///
/// Default is `lease_ttl_ms / 3` (matches FF's internal renewer
/// cadence), clamped to at least [`KEEPER_MIN_INTERVAL_MS`].
///
/// Operators can override via the [`KEEPER_INTERVAL_ENV`] env var on
/// **all builds** (debug and release). This is a production-supported
/// escape hatch intended for ops: if a run-mode hits a corner case
/// where the phase probe cadence needs tuning, the keeper interval
/// can be adjusted without a binary rebuild. The lookup is cached in
/// a process-wide [`OnceLock`] so subsequent calls never touch the
/// environment — the override reads once at the first keeper spawn
/// and stays fixed for the process lifetime; rolling a new value
/// requires a process restart. The override is clamped to
/// [[`KEEPER_MIN_INTERVAL_MS`], [`KEEPER_MAX_INTERVAL_MS`]]; a
/// malformed value falls back to the default with a WARN.
///
/// When the env var is unset the keeper uses `lease_ttl_ms / 3` so
/// operators who don't set the variable see FF's own internal-renewer
/// cadence mirrored by the keeper.
///
/// [`OnceLock`]: std::sync::OnceLock
fn keeper_interval_ms(lease_ttl_ms: u64) -> u64 {
    /// Cached result of `env::var(KEEPER_INTERVAL_ENV)` parsed into a
    /// clamped `u64`. `None` means "unset or malformed; use default".
    /// Read-once-cached-forever — see function docs.
    static CACHED_OVERRIDE: std::sync::OnceLock<Option<u64>> = std::sync::OnceLock::new();

    let cached = CACHED_OVERRIDE.get_or_init(|| {
        parse_env_interval_override(
            std::env::var(KEEPER_INTERVAL_ENV).ok().as_deref(),
            lease_ttl_ms,
        )
    });
    if let Some(ms) = cached {
        return *ms;
    }
    std::cmp::max(lease_ttl_ms / 3, KEEPER_MIN_INTERVAL_MS)
}

/// Remove this keeper's entry from the registry map on natural exit.
///
/// Upgrading the `Weak` handle is always attempted; when the upgrade
/// fails the whole registry has already been dropped (e.g. the
/// process is shutting down and `AppState` was released) and there
/// is nothing to clean up. When the upgrade succeeds we lock the map
/// briefly and remove the entry keyed on this keeper's `run_id`. The
/// critical section holds only a `remove` call — no awaits — so it
/// cannot deadlock against `ensure_running` or `shutdown_all`.
///
/// Must only be called from the keeper's **natural** exit paths:
/// terminal phase, execution-not-found, renew returned terminal,
/// renew returned error. The cancellation branches return without
/// self-removing because `shutdown_all` has already drained the map
/// under the same mutex before awaiting joins.
async fn remove_self_from_registry(weak_map: &Weak<KeeperMap>, run_id: &RunId) {
    if let Some(map) = weak_map.upgrade() {
        map.lock().await.remove(run_id);
    }
}

async fn run_keeper_loop(
    run_id: RunId,
    session_id: SessionId,
    execution_id: ExecutionId,
    runs: Arc<dyn RunService>,
    engine: Arc<dyn Engine>,
    interval: Duration,
    cancel: CancellationToken,
    observability: Option<Arc<KeeperObservability>>,
    weak_map: Weak<KeeperMap>,
) {
    tracing::debug!(
        run_id = %run_id,
        execution_id = %execution_id,
        interval_ms = interval.as_millis() as u64,
        "#666 lease keeper started (phase-aware)"
    );
    // #666: track the previous tick's classification so we can log a
    // single DEBUG line on the suspended → renewable transition and
    // fire an immediate renew without waiting out another tick — the
    // FF lease's wall-clock has been ticking while we were paused;
    // the orchestrator's next terminal FCALL needs a fresh lease.
    let mut last: Option<PhaseClassification> = None;
    // #685 Finding 3: count consecutive classification mismatches
    // (probe said renewable, renew rejected, re-probe STILL says
    // renewable). Reset on any tick that doesn't hit that exact
    // path. Exit if we hit `KEEPER_CLASSIFICATION_MISMATCH_CAP` in a
    // row — FF and cairn-fabric disagree on the state vector, and
    // continuing to retry buys nothing.
    let mut consecutive_classification_mismatches: u32 = 0;
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::debug!(run_id = %run_id, "#666 lease keeper cancelled");
                return;
            }
            _ = tokio::time::sleep(interval) => {}
        }

        // #666: probe FF directly for the execution's phase. This
        // replaces the projection-based suspension guess that lagged
        // FF on rapid suspend/resume cycles (dogfood R5, 2026-05-03).
        let phase = classify_phase(engine.as_ref(), &run_id, &execution_id).await;

        let mut should_exit = false;
        let mut skip_renew = false;

        match phase {
            PhaseClassification::Terminal => {
                tracing::debug!(
                    run_id = %run_id,
                    execution_id = %execution_id,
                    "#666 lease keeper observed terminal lifecycle_phase, exiting"
                );
                should_exit = true;
            }
            PhaseClassification::ExecutionNotFound => {
                tracing::info!(
                    run_id = %run_id,
                    execution_id = %execution_id,
                    "#666 lease keeper: execution not found in FF, exiting"
                );
                should_exit = true;
            }
            PhaseClassification::Suspended
            | PhaseClassification::RunnableUnclaimed
            | PhaseClassification::AttemptInterrupted
            | PhaseClassification::PhaseInFlight => {
                // Log the transition (runnable → paused) at DEBUG
                // once; stay at TRACE while we remain in the skip
                // state so a long approval wait doesn't flood logs.
                // On the first tick (`last.is_none()`) there is no
                // prior state to transition from — use TRACE so a run
                // that starts in a non-renewable phase (e.g. spawning
                // straight into Suspended during approval wait)
                // doesn't emit a spurious DEBUG "transition" line.
                if last
                    .map(|p| p == PhaseClassification::RenewableActive)
                    .unwrap_or(false)
                {
                    tracing::debug!(
                        run_id = %run_id,
                        execution_id = %execution_id,
                        classification = ?phase,
                        "#666 lease keeper: phase not renewable; skipping renew tick"
                    );
                } else {
                    tracing::trace!(
                        run_id = %run_id,
                        execution_id = %execution_id,
                        classification = ?phase,
                        "#666 lease keeper: phase still not renewable; skipping tick"
                    );
                }
                last = Some(phase);
                skip_renew = true;
                // #685 Finding 3 (Gemini follow-up, PR #688): the
                // mismatch counter's invariant — per the comment
                // at the top of `run_keeper_loop` — is "consecutive
                // mismatches IN A ROW, reset on any tick that
                // doesn't hit that exact path". The initial probe
                // reporting a non-renewable phase is NOT Path C
                // (probe-renewable → renew-rejected → re-probe-
                // renewable); it's a skip-this-tick outcome. In a
                // flap scenario (renewable → Suspended → renewable
                // with the probe/renew race still present) the
                // mismatch ticks must not accumulate across the
                // non-renewable detour, otherwise a pair of races
                // separated by a legitimate Suspended wait would
                // still trip the cap. Reset here.
                consecutive_classification_mismatches = 0;
            }
            PhaseClassification::RenewableActive => {
                if last
                    .map(|p| p != PhaseClassification::RenewableActive)
                    .unwrap_or(false)
                {
                    tracing::debug!(
                        run_id = %run_id,
                        execution_id = %execution_id,
                        "#666 lease keeper: phase flipped back to renewable; \
                         firing immediate renew to reset lease wall-clock"
                    );
                }
                last = Some(phase);
            }
        }

        if should_exit {
            // Natural exit (terminal phase or ExecutionNotFound): drop
            // our registry entry before returning so the `HashMap`
            // doesn't leak a finished `JoinHandle` per completed run.
            remove_self_from_registry(&weak_map, &run_id).await;
            // Final tick notification so a test that armed
            // `tick_completed.notified()` before the terminal
            // transition still observes the exit.
            if let Some(obs) = &observability {
                obs.tick_completed.notify_waiters();
            }
            return;
        }

        if skip_renew {
            if let Some(obs) = &observability {
                obs.tick_completed.notify_waiters();
            }
            continue;
        }

        // Hook point: the keeper has decided to issue a renew FCALL.
        // Increment the attempt counter BEFORE the `select!` so even
        // a cancellation-interrupted FCALL counts as an attempt —
        // test code asserting "pre-fix keeper tried to renew N
        // times" wants to count every intent, not only the
        // round-trips that completed.
        if let Some(obs) = &observability {
            obs.renew_attempts.fetch_add(1, Ordering::SeqCst);
        }

        // Wrap the renew call in the same `select!` so cancellation
        // can interrupt an in-flight FCALL instead of waiting for
        // the upstream fabric RPC to return. Without this wrapper,
        // `shutdown_all` would hang for up to `fcall_timeout_ms` on
        // a keeper mid-renew during a slow Valkey/Postgres.
        let renew_result = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::debug!(
                    run_id = %run_id,
                    "#666 lease keeper cancelled during renewal"
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
                        "#666 lease keeper observed terminal state, exiting"
                    );
                    // Natural exit: drop our registry entry.
                    remove_self_from_registry(&weak_map, &run_id).await;
                    if let Some(obs) = &observability {
                        obs.tick_completed.notify_waiters();
                    }
                    return;
                }
                // A successful renew clears the mismatch streak: we've
                // proved the keeper and FF agree on a renewable state.
                consecutive_classification_mismatches = 0;
            }
            Err(err) => {
                // #685 Finding 3 (dogfood 2026-05-05): the original
                // #666 keeper treated ANY renew error after a positive
                // probe as fatal. Under 30s+ LLM latency the probe →
                // renew gap widens (each tokio interval tick defers to
                // the runtime, and the renew FCALL itself does its
                // own `describe_execution`, giving FF's scanner two
                // sample windows to flip the phase). The dogfood
                // repro shows `last_classification=RenewableActive
                // error=execution conflict: execution_not_eligible`,
                // the keeper exits, and the lease expires while the
                // operator is still waiting on the LLM call to return.
                //
                // Fix: on a phase-conflict rejection, re-probe FF.
                // Three outcomes:
                //
                // 1. Re-probe reports `Terminal` / `ExecutionNotFound`
                //    → exit cleanly, the keeper's job is done. (Would
                //    be incorrect to retry; the run completed or
                //    vanished between the original probe and the
                //    renew FCALL.)
                // 2. Re-probe reports a non-renewable phase
                //    (Suspended, RunnableUnclaimed, AttemptInterrupted,
                //    PhaseInFlight) → we raced a legitimate transition
                //    (approval landed, signal delivered, scanner
                //    marked the attempt). Log at INFO, reset the
                //    mismatch streak, continue: the next tick will
                //    see the non-renewable phase on its own probe
                //    and skip the renew cleanly.
                // 3. Re-probe STILL says `RenewableActive` → cairn-
                //    fabric and FF disagree on the state vector,
                //    OR the scanner is flipping the phase inside the
                //    RPC. Increment the mismatch counter, log WARN
                //    with both classifications, and fall through to
                //    the consecutive-mismatch cap. Below the cap we
                //    continue (skip this tick, re-try on the next);
                //    above the cap we exit.
                //
                // Non-phase-conflict errors preserve the #666
                // exit-on-surprise contract: they are either
                // transport-level (FF down) or logic-level (lease
                // revoked by an operator), and retrying won't
                // improve the situation.
                if !is_phase_conflict_rejection(&err) {
                    tracing::warn!(
                        run_id = %run_id,
                        execution_id = %execution_id,
                        last_classification = ?last,
                        error = %err,
                        "#685 lease keeper: renew_lease_if_stale failed with \
                         non-phase-conflict error; exiting"
                    );
                    remove_self_from_registry(&weak_map, &run_id).await;
                    if let Some(obs) = &observability {
                        obs.tick_completed.notify_waiters();
                    }
                    return;
                }

                // Phase-conflict rejection: always count it.
                if let Some(obs) = &observability {
                    obs.renew_rejections.fetch_add(1, Ordering::SeqCst);
                }

                // Re-probe immediately. The `read_execution_info` RPC
                // is one round-trip against the same backend the
                // renew FCALL just rejected on; if FF is down the
                // re-probe errors out (classify_phase returns
                // PhaseInFlight), which lands us in the non-
                // renewable branch below — the keeper skips the
                // tick and tries again on the next interval.
                let reprobe = classify_phase(engine.as_ref(), &run_id, &execution_id).await;

                match reprobe {
                    PhaseClassification::Terminal => {
                        tracing::info!(
                            run_id = %run_id,
                            execution_id = %execution_id,
                            last_classification = ?last,
                            error = %err,
                            "#685 lease keeper: renew rejected then re-probe \
                             reports terminal; exiting cleanly"
                        );
                        remove_self_from_registry(&weak_map, &run_id).await;
                        if let Some(obs) = &observability {
                            obs.tick_completed.notify_waiters();
                        }
                        return;
                    }
                    PhaseClassification::ExecutionNotFound => {
                        tracing::info!(
                            run_id = %run_id,
                            execution_id = %execution_id,
                            last_classification = ?last,
                            error = %err,
                            "#685 lease keeper: renew rejected then re-probe \
                             reports execution missing; exiting cleanly"
                        );
                        remove_self_from_registry(&weak_map, &run_id).await;
                        if let Some(obs) = &observability {
                            obs.tick_completed.notify_waiters();
                        }
                        return;
                    }
                    PhaseClassification::RenewableActive => {
                        // The mismatch path: both probes say
                        // renewable, FF said otherwise. Bump the
                        // counter; if we hit the cap, exit.
                        consecutive_classification_mismatches =
                            consecutive_classification_mismatches.saturating_add(1);
                        if let Some(obs) = &observability {
                            obs.probe_renew_classification_mismatch
                                .fetch_add(1, Ordering::SeqCst);
                        }

                        if consecutive_classification_mismatches
                            >= KEEPER_CLASSIFICATION_MISMATCH_CAP
                        {
                            tracing::warn!(
                                run_id = %run_id,
                                execution_id = %execution_id,
                                last_classification = ?last,
                                reprobe_classification = ?reprobe,
                                consecutive_mismatches =
                                    consecutive_classification_mismatches,
                                cap = KEEPER_CLASSIFICATION_MISMATCH_CAP,
                                error = %err,
                                "#685 lease keeper: hit classification-mismatch \
                                 cap (probe+re-probe both RenewableActive, FF \
                                 rejected); exiting. Subsequent /orchestrate \
                                 calls will spawn a fresh keeper."
                            );
                            remove_self_from_registry(&weak_map, &run_id).await;
                            if let Some(obs) = &observability {
                                obs.tick_completed.notify_waiters();
                            }
                            return;
                        }

                        tracing::warn!(
                            run_id = %run_id,
                            execution_id = %execution_id,
                            last_classification = ?last,
                            reprobe_classification = ?reprobe,
                            consecutive_mismatches =
                                consecutive_classification_mismatches,
                            cap = KEEPER_CLASSIFICATION_MISMATCH_CAP,
                            error = %err,
                            "#685 lease keeper: classification mismatch \
                             (probe+re-probe both RenewableActive, FF rejected \
                             renew); skipping this tick, retrying next interval"
                        );
                        last = Some(reprobe);
                        // Fall through to tick_completed notification
                        // below; `continue`-semantics by virtue of
                        // being past the `match renew_result`.
                    }
                    PhaseClassification::Suspended
                    | PhaseClassification::RunnableUnclaimed
                    | PhaseClassification::AttemptInterrupted
                    | PhaseClassification::PhaseInFlight => {
                        // Legitimate phase transition between the
                        // probe and the renew. Reset the mismatch
                        // streak — this isn't the race we're guarding
                        // against. Log at INFO (once per transition
                        // class) so operators can see the keeper
                        // recovered, then let the next tick sample
                        // the new phase on its own probe.
                        tracing::info!(
                            run_id = %run_id,
                            execution_id = %execution_id,
                            last_classification = ?last,
                            reprobe_classification = ?reprobe,
                            error = %err,
                            "#685 lease keeper: phase transitioned between \
                             probe and renew; skipping this tick, next tick \
                             will observe the new phase directly"
                        );
                        consecutive_classification_mismatches = 0;
                        last = Some(reprobe);
                    }
                }
            }
        }

        if let Some(obs) = &observability {
            obs.tick_completed.notify_waiters();
        }
    }
}

/// Classify a `RuntimeError` as an FF phase-conflict rejection for
/// observability purposes. Matches the two shapes that `renew_lease_if_stale`
/// can return when FF rejects the FCALL because the execution is in
/// a non-renewable phase:
///
/// * `Conflict { entity: "execution", id: "execution_not_eligible"
///   | "execution_not_eligible_for_attempt" }` — per
///   `RuntimeError::is_transient_phase_conflict`.
/// * `InvalidTransition { from: "execution_not_active" | "lease_expired" }`
///   — per `invalid_transition_hint` in `cairn_runtime::error`.
///
/// Narrower than `is_transient_phase_conflict` on purpose: the pre-#666
/// bug was the *silent retry* on `execution_not_eligible`, and we want
/// the observability counter to reflect that exact behaviour. The
/// hint-style `InvalidTransition { from: "execution_not_active" }` is
/// included because the post-resolve_approval runnable-unclaimed window
/// can surface as either shape depending on which FF FCALL the
/// `renew_lease_if_stale` path fell through to.
fn is_phase_conflict_rejection(err: &cairn_runtime::error::RuntimeError) -> bool {
    use cairn_runtime::error::RuntimeError;
    if err.is_transient_phase_conflict() {
        return true;
    }
    matches!(
        err,
        RuntimeError::InvalidTransition { from, .. }
            if matches!(
                from.as_str(),
                "execution_not_active"
                    | "execution_not_eligible"
                    | "execution_not_eligible_for_attempt"
                    | "lease_expired"
            )
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use cairn_domain::{
        ApprovalDecision, FailureClass, PauseReason, ProjectKey, ResumeTrigger, RunId,
        RunResumeTarget, RunState, SessionId, TenantId,
    };
    use cairn_fabric::engine::snapshots::{EdgeSnapshot, ExecutionSnapshot, FlowSnapshot};
    use cairn_fabric::engine::{
        BlockingReason, EdgeId, EligibilityState, ExpiredLease, FlowId, LaneId, Namespace,
        PublicState, StateVector, TerminalOutcome, WorkerId, WorkerInstanceId, WorkerRegistration,
        WorkerSummary,
    };
    use cairn_fabric::error::FabricError;
    use cairn_runtime::error::RuntimeError;
    use cairn_runtime::RunService;
    use cairn_store::projections::RunRecord;

    /// One scripted renew outcome. The optional `then_engine` closure
    /// lets tests deterministically simulate the race the #685 fix
    /// closes: inject an error response AND flip the engine's probe
    /// view of the state vector in the same transaction, so the
    /// keeper's re-probe sees whatever phase the test is proving the
    /// recovery path handles. Mutates the engine **after** the
    /// renew response is produced; the probe ordering inside the
    /// keeper loop (probe → renew → re-probe) is preserved.
    #[allow(clippy::type_complexity)]
    struct MockRenewAction {
        response: Result<RunRecord, RuntimeError>,
        /// Optional post-response action on the engine. Awaited on
        /// the keeper's task thread, before `renew_lease_if_stale`
        /// returns to the caller — so the re-probe that follows
        /// observes whatever state the closure installed.
        then_engine: Option<
            Box<
                dyn FnOnce(
                        Arc<MockEngine>,
                    )
                        -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
                    + Send
                    + Sync,
            >,
        >,
    }

    impl MockRenewAction {
        fn response(r: Result<RunRecord, RuntimeError>) -> Self {
            Self {
                response: r,
                then_engine: None,
            }
        }
    }

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
        responses: Mutex<Vec<MockRenewAction>>,
        /// Optional engine handle used by `MockRenewAction::then_engine`
        /// closures. Tests that don't use the race-simulation path
        /// leave this `None`; tests that do set it via
        /// [`MockRuns::with_engine`] before spawning the keeper.
        engine_for_actions: Mutex<Option<Arc<MockEngine>>>,
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
                in_flight_descendants: 0,
                root_run_id: None,
                iteration: 0,
            }
        }

        async fn push_response(&self, r: Result<RunRecord, RuntimeError>) {
            self.responses
                .lock()
                .await
                .push(MockRenewAction::response(r));
        }

        /// Queue a scripted renew action: the `response` is returned
        /// to the keeper, THEN (if provided) `then_engine` is awaited
        /// against the engine handle installed via `with_engine`.
        /// The keeper's re-probe on the #685 fix path observes the
        /// mutated engine state.
        async fn push_action(&self, action: MockRenewAction) {
            self.responses.lock().await.push(action);
        }

        /// Associate an engine handle so queued `then_engine` closures
        /// can mutate it. Must be called before the keeper is spawned
        /// or the first renew will fire with `None` and the closure
        /// becomes a no-op (logged via the test's own assertion on
        /// the re-probe classification).
        async fn with_engine(&self, engine: Arc<MockEngine>) {
            *self.engine_for_actions.lock().await = Some(engine);
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
            let action = {
                let mut q = self.responses.lock().await;
                if q.is_empty() {
                    return Ok(Self::running(RunState::Running));
                }
                q.remove(0)
            };
            // Run the post-response mutation BEFORE returning the
            // response so the keeper's re-probe (which follows
            // immediately after `renew_lease_if_stale` returns) sees
            // the mutated engine state. This is the whole point of
            // `MockRenewAction`: deterministically stage the
            // probe → renew-fail → re-probe race without sleeps.
            if let Some(then_engine) = action.then_engine {
                let engine = self.engine_for_actions.lock().await.clone();
                if let Some(engine) = engine {
                    then_engine(engine).await;
                }
            }
            action.response
        }
    }

    /// Minimal Engine mock for keeper tests. Every method except
    /// `read_execution_info` is `unreachable!` — the keeper only
    /// probes this one method. `read_execution_info` returns the
    /// currently-held `ExecutionInfo`, which tests flip mid-run via
    /// [`MockEngine::set_info`] (swap in a new snapshot, e.g. to
    /// promote Suspended → Active) or [`MockEngine::clear_info`]
    /// (simulate "execution vanished from FF" → the probe now
    /// returns `Ok(None)` and the keeper exits on
    /// [`PhaseClassification::ExecutionNotFound`]).
    struct MockEngine {
        info: Mutex<Option<ExecutionInfo>>,
    }

    impl MockEngine {
        fn new(initial: ExecutionInfo) -> Self {
            Self {
                info: Mutex::new(Some(initial)),
            }
        }

        async fn set_info(&self, info: ExecutionInfo) {
            *self.info.lock().await = Some(info);
        }

        async fn clear_info(&self) {
            *self.info.lock().await = None;
        }
    }

    /// Build a minimal `ExecutionInfo` with the 7-dimension state
    /// vector set to the supplied triple and other fields filled
    /// with defensible defaults. Tests only care about the three
    /// classification-relevant dimensions.
    fn make_info(
        execution_id: ExecutionId,
        lifecycle_phase: LifecyclePhase,
        attempt_state: AttemptState,
        ownership_state: OwnershipState,
    ) -> ExecutionInfo {
        let state_vector = StateVector {
            lifecycle_phase,
            ownership_state,
            eligibility_state: EligibilityState::NotApplicable,
            blocking_reason: BlockingReason::None,
            terminal_outcome: TerminalOutcome::None,
            attempt_state,
            public_state: PublicState::Active,
        };
        ExecutionInfo {
            execution_id,
            namespace: "cairn".to_owned(),
            lane_id: "cairn".to_owned(),
            priority: 0,
            execution_kind: "standard".to_owned(),
            state_vector,
            public_state: PublicState::Active,
            created_at: "0".to_owned(),
            started_at: None,
            completed_at: None,
            current_attempt_index: 1,
            flow_id: None,
            blocking_detail: String::new(),
        }
    }

    fn test_execution_id() -> ExecutionId {
        // Any deterministic id; the keeper never interprets it —
        // the mock Engine ignores the argument passed to
        // `read_execution_info`. A zero-uuid + zero-partition string
        // satisfies the public `parse` contract without dragging a
        // PartitionConfig into the tests.
        ExecutionId::parse("{fp:0}:00000000-0000-0000-0000-000000000000")
            .expect("test ExecutionId must parse")
    }

    #[async_trait]
    impl Engine for MockEngine {
        async fn describe_execution(
            &self,
            _id: &ExecutionId,
        ) -> Result<Option<ExecutionSnapshot>, FabricError> {
            unreachable!("keeper only calls read_execution_info")
        }
        async fn describe_flow(&self, _id: &FlowId) -> Result<Option<FlowSnapshot>, FabricError> {
            unreachable!("keeper only calls read_execution_info")
        }
        async fn describe_edge(
            &self,
            _flow_id: &FlowId,
            _edge_id: &EdgeId,
        ) -> Result<Option<EdgeSnapshot>, FabricError> {
            unreachable!("keeper only calls read_execution_info")
        }
        async fn list_incoming_edges(
            &self,
            _execution_id: &ExecutionId,
        ) -> Result<Vec<EdgeSnapshot>, FabricError> {
            unreachable!("keeper only calls read_execution_info")
        }
        async fn get_execution_tag(
            &self,
            _id: &ExecutionId,
            _key: &str,
        ) -> Result<Option<String>, FabricError> {
            unreachable!("keeper only calls read_execution_info")
        }
        async fn get_execution_lane_id(
            &self,
            _id: &ExecutionId,
        ) -> Result<Option<LaneId>, FabricError> {
            unreachable!("keeper only calls read_execution_info")
        }
        async fn set_execution_tag(
            &self,
            _id: &ExecutionId,
            _key: &str,
            _value: &str,
        ) -> Result<(), FabricError> {
            unreachable!("keeper only calls read_execution_info")
        }
        async fn set_flow_tag(
            &self,
            _id: &FlowId,
            _key: &str,
            _value: &str,
        ) -> Result<(), FabricError> {
            unreachable!("keeper only calls read_execution_info")
        }
        async fn set_flow_tags(
            &self,
            _id: &FlowId,
            _tags: &BTreeMap<String, String>,
        ) -> Result<(), FabricError> {
            unreachable!("keeper only calls read_execution_info")
        }
        async fn register_worker(
            &self,
            _worker_id: &WorkerId,
            _instance_id: &WorkerInstanceId,
            _namespace: &Namespace,
            _lanes: &BTreeSet<LaneId>,
            _capabilities: &BTreeSet<String>,
            _liveness_ttl_ms: u64,
        ) -> Result<WorkerRegistration, FabricError> {
            unreachable!("keeper only calls read_execution_info")
        }
        async fn heartbeat_worker(
            &self,
            _instance_id: &WorkerInstanceId,
            _namespace: &Namespace,
        ) -> Result<(), FabricError> {
            unreachable!("keeper only calls read_execution_info")
        }
        async fn mark_worker_dead(
            &self,
            _instance_id: &WorkerInstanceId,
            _namespace: &Namespace,
            _reason: &str,
        ) -> Result<(), FabricError> {
            unreachable!("keeper only calls read_execution_info")
        }
        async fn list_workers(
            &self,
            _namespace: Option<&Namespace>,
        ) -> Result<Vec<WorkerSummary>, FabricError> {
            unreachable!("keeper only calls read_execution_info")
        }
        async fn list_expired_leases(
            &self,
            _now_ms: u64,
            _limit: usize,
        ) -> Result<Vec<ExpiredLease>, FabricError> {
            unreachable!("keeper only calls read_execution_info")
        }
        async fn read_execution_info(
            &self,
            _id: &ExecutionId,
        ) -> Result<Option<ExecutionInfo>, FabricError> {
            Ok(self.info.lock().await.clone())
        }
    }

    /// `ensure_running` must atomically deduplicate concurrent inserts
    /// for the same `RunId`. Spawns 50 concurrent ensures; asserts only
    /// one keeper lands in the registry.
    #[tokio::test]
    async fn ensure_running_dedups_concurrent_inserts() {
        let registry = Arc::new(LeaseKeeperRegistry::new());
        let runs: Arc<dyn RunService> = Arc::new(MockRuns::default());
        let exec_id = test_execution_id();
        let engine: Arc<dyn Engine> = Arc::new(MockEngine::new(make_info(
            exec_id.clone(),
            LifecyclePhase::Active,
            AttemptState::RunningAttempt,
            OwnershipState::Leased,
        )));
        let run_id = RunId::new("run_dedup");
        let session_id = SessionId::new("sess_dedup");

        let mut handles = Vec::new();
        for _ in 0..50 {
            let r = registry.clone();
            let runs = runs.clone();
            let engine = engine.clone();
            let run_id = run_id.clone();
            let session_id = session_id.clone();
            let exec_id = exec_id.clone();
            handles.push(tokio::spawn(async move {
                r.ensure_running(run_id, session_id, exec_id, runs, engine, 10_000)
                    .await;
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
        let exec_id = test_execution_id();
        let engine: Arc<dyn Engine> = Arc::new(MockEngine::new(make_info(
            exec_id.clone(),
            LifecyclePhase::Suspended,
            AttemptState::AttemptInterrupted,
            OwnershipState::Leased,
        )));

        for i in 0..3 {
            registry
                .ensure_running(
                    RunId::new(format!("run_shutdown_{i}")),
                    SessionId::new(format!("sess_shutdown_{i}")),
                    exec_id.clone(),
                    runs.clone(),
                    engine.clone(),
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
    /// for the next tick AND self-remove from the registry on that
    /// natural exit (rather than leaking a finished `JoinHandle`
    /// forever). With the self-removal contract in place the entry
    /// for `run_id` should disappear from the map as the task
    /// returns — the test asserts `!contains(run_id)` as the exit
    /// criterion, not `join.is_finished()` on a still-present entry.
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
        let exec_id = test_execution_id();
        // Renewable phase so the keeper will reach `renew_lease_if_stale`.
        let engine: Arc<dyn Engine> = Arc::new(MockEngine::new(make_info(
            exec_id.clone(),
            LifecyclePhase::Active,
            AttemptState::RunningAttempt,
            OwnershipState::Leased,
        )));
        let run_id = RunId::new("run_terminal");
        let session_id = SessionId::new("sess_terminal");

        registry
            .ensure_running(run_id.clone(), session_id, exec_id, runs, engine, 1_500)
            .await;

        // Wait up to 2s for the keeper to observe the Completed
        // response, self-remove, and exit. Poll `contains` rather
        // than sleeping a fixed duration — no flake window.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while registry.contains(&run_id).await {
            if tokio::time::Instant::now() >= deadline {
                panic!("keeper did not self-remove from registry after observing terminal state");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(mock.renew_calls.load(Ordering::SeqCst) >= 2);
        assert_eq!(
            registry.len().await,
            0,
            "natural-exit self-removal must leave the registry empty"
        );

        registry.shutdown_all().await;
    }

    /// #666 classifier: table-drive every `(LifecyclePhase,
    /// AttemptState, OwnershipState)` combination relevant to the
    /// keeper and assert the mapping. This is the pure-function test
    /// the fix's correctness hinges on — no timing, no async.
    #[test]
    fn phase_classifier_table() {
        use PhaseClassification::*;
        let exec_id = test_execution_id();

        // Terminal: dominates everything else.
        for att in [
            AttemptState::None,
            AttemptState::RunningAttempt,
            AttemptState::AttemptInterrupted,
            AttemptState::AttemptTerminal,
        ] {
            for own in [
                OwnershipState::Unowned,
                OwnershipState::Leased,
                OwnershipState::LeaseExpiredReclaimable,
                OwnershipState::LeaseRevoked,
            ] {
                let info = make_info(exec_id.clone(), LifecyclePhase::Terminal, att, own);
                assert_eq!(
                    PhaseClassification::from_info(&info),
                    Terminal,
                    "terminal lifecycle_phase must dominate (att={att:?}, own={own:?})"
                );
            }
        }

        // Suspended: always skip regardless of attempt/ownership.
        for att in [
            AttemptState::RunningAttempt,
            AttemptState::AttemptInterrupted,
            AttemptState::PendingRetryAttempt,
        ] {
            for own in [OwnershipState::Leased, OwnershipState::Unowned] {
                let info = make_info(exec_id.clone(), LifecyclePhase::Suspended, att, own);
                assert_eq!(PhaseClassification::from_info(&info), Suspended);
            }
        }

        // Runnable: transient post-signal-delivery window.
        for att in [
            AttemptState::PendingFirstAttempt,
            AttemptState::PendingRetryAttempt,
            AttemptState::AttemptInterrupted,
        ] {
            let info = make_info(
                exec_id.clone(),
                LifecyclePhase::Runnable,
                att,
                OwnershipState::Unowned,
            );
            assert_eq!(PhaseClassification::from_info(&info), RunnableUnclaimed);
        }

        // Active + AttemptInterrupted: scanner race — skip.
        for own in [
            OwnershipState::Leased,
            OwnershipState::LeaseExpiredReclaimable,
            OwnershipState::LeaseRevoked,
            OwnershipState::Unowned,
        ] {
            let info = make_info(
                exec_id.clone(),
                LifecyclePhase::Active,
                AttemptState::AttemptInterrupted,
                own,
            );
            assert_eq!(
                PhaseClassification::from_info(&info),
                AttemptInterrupted,
                "Active + AttemptInterrupted must classify as AttemptInterrupted (own={own:?})"
            );
        }

        // Active + RunningAttempt + Leased: the single renewable shape.
        let info = make_info(
            exec_id.clone(),
            LifecyclePhase::Active,
            AttemptState::RunningAttempt,
            OwnershipState::Leased,
        );
        assert_eq!(PhaseClassification::from_info(&info), RenewableActive);

        // Active + RunningAttempt but NOT Leased (lease expired /
        // revoked) — can't renew, re-probe.
        for own in [
            OwnershipState::LeaseExpiredReclaimable,
            OwnershipState::LeaseRevoked,
            OwnershipState::Unowned,
        ] {
            let info = make_info(
                exec_id.clone(),
                LifecyclePhase::Active,
                AttemptState::RunningAttempt,
                own,
            );
            assert_eq!(PhaseClassification::from_info(&info), PhaseInFlight);
        }

        // Active with a non-running attempt state (pending retry,
        // pending replay, terminal, none). We can't renew — re-probe.
        for att in [
            AttemptState::PendingRetryAttempt,
            AttemptState::PendingReplayAttempt,
            AttemptState::PendingFirstAttempt,
            AttemptState::None,
            AttemptState::AttemptTerminal,
        ] {
            let info = make_info(
                exec_id.clone(),
                LifecyclePhase::Active,
                att,
                OwnershipState::Leased,
            );
            assert_eq!(
                PhaseClassification::from_info(&info),
                PhaseInFlight,
                "Active + non-running attempt_state {att:?} must be PhaseInFlight"
            );
        }

        // Submitted: transient pre-resolution.
        let info = make_info(
            exec_id.clone(),
            LifecyclePhase::Submitted,
            AttemptState::None,
            OwnershipState::Unowned,
        );
        assert_eq!(PhaseClassification::from_info(&info), PhaseInFlight);
    }

    /// #666 env-override boundary cases exercised via the pure
    /// [`parse_env_interval_override`] helper. The parser must clamp
    /// to [`KEEPER_MIN_INTERVAL_MS`, `KEEPER_MAX_INTERVAL_MS`];
    /// malformed and unset values must return `None` so the caller
    /// falls back to the `lease_ttl_ms / 3` default.
    ///
    /// We test the pure helper (not `keeper_interval_ms`) because the
    /// latter caches its env lookup in a process-wide `OnceLock`
    /// after the first call — a multi-iteration test calling it with
    /// different env values would see whichever value won the
    /// first-initialization race. The cache is intentional (see
    /// `keeper_interval_ms` docs) and the parse/clamp logic is the
    /// part worth table-driving.
    #[test]
    fn keeper_interval_env_override_clamps_and_parses() {
        // Below the floor: clamp up.
        assert_eq!(
            parse_env_interval_override(Some("100"), 30_000),
            Some(KEEPER_MIN_INTERVAL_MS)
        );

        // Above the ceiling: clamp down.
        assert_eq!(
            parse_env_interval_override(Some("3600000"), 30_000),
            Some(KEEPER_MAX_INTERVAL_MS)
        );

        // In-range: honoured verbatim.
        assert_eq!(
            parse_env_interval_override(Some("2500"), 30_000),
            Some(2_500)
        );

        // Malformed: None → caller falls back to default.
        assert_eq!(
            parse_env_interval_override(Some("not-a-number"), 30_000),
            None
        );

        // Unset: None → caller falls back to default.
        assert_eq!(parse_env_interval_override(None, 30_000), None);
    }

    /// #655 regression, updated for #666: when FF reports the
    /// execution is in an approval-pending suspension (lifecycle_phase
    /// = Suspended), the keeper must skip the renew FCALL every tick.
    /// Pre-#655 the keeper would churn FF with `execution_not_eligible`
    /// rejections; #666 replaces the projection probe with FF's
    /// `read_execution_info` but the skip-while-suspended contract is
    /// preserved.
    #[tokio::test]
    async fn keeper_skips_renew_while_tool_call_approval_pending() {
        let registry = Arc::new(LeaseKeeperRegistry::new());
        let mock = Arc::new(MockRuns::default());
        let runs: Arc<dyn RunService> = mock.clone();
        let exec_id = test_execution_id();
        // FF phase = Suspended throughout the test window.
        let engine_inner = Arc::new(MockEngine::new(make_info(
            exec_id.clone(),
            LifecyclePhase::Suspended,
            AttemptState::AttemptInterrupted,
            OwnershipState::Leased,
        )));
        let engine: Arc<dyn Engine> = engine_inner.clone();

        let run_id = RunId::new("run_666_suspend");
        let session_id = SessionId::new("sess_666_suspend");

        registry
            .ensure_running(run_id.clone(), session_id, exec_id, runs, engine, 1_500)
            .await;

        // Tick interval is 500 ms (1500 / 3). Wait three intervals —
        // pre-fix this would produce ≥2 `renew_lease_if_stale` calls.
        // Post-fix the phase probe short-circuits every tick.
        tokio::time::sleep(Duration::from_millis(1_800)).await;

        let renews = mock.renew_calls.load(Ordering::SeqCst);
        assert_eq!(
            renews, 0,
            "#666: keeper must NOT call renew_lease_if_stale while FF \
             lifecycle_phase is Suspended; observed {renews} renews"
        );

        registry.shutdown_all().await;
    }

    /// #655 force-renew on suspension resolution, updated for #666:
    /// when FF's `lifecycle_phase` transitions back to `Active` (the
    /// signal was delivered, claim_resumed landed), the keeper must
    /// fire a renew on the NEXT tick — not wait for a full interval.
    /// This resets the FF lease's wall-clock deadline so the
    /// orchestrator's terminal FCALL has a fresh lease.
    #[tokio::test]
    async fn keeper_force_renews_on_suspension_resolution() {
        let registry = Arc::new(LeaseKeeperRegistry::new());
        let mock = Arc::new(MockRuns::default());
        let runs: Arc<dyn RunService> = mock.clone();
        let exec_id = test_execution_id();
        // Start suspended.
        let engine_inner = Arc::new(MockEngine::new(make_info(
            exec_id.clone(),
            LifecyclePhase::Suspended,
            AttemptState::AttemptInterrupted,
            OwnershipState::Leased,
        )));
        let engine: Arc<dyn Engine> = engine_inner.clone();

        let run_id = RunId::new("run_666_resolve");
        let session_id = SessionId::new("sess_666_resolve");

        // 1500 ms TTL → 500 ms tick.
        registry
            .ensure_running(
                run_id.clone(),
                session_id,
                exec_id.clone(),
                runs,
                engine,
                1_500,
            )
            .await;

        // Stay suspended for ~1 s (two ticks). No renews must fire.
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        assert_eq!(
            mock.renew_calls.load(Ordering::SeqCst),
            0,
            "#666: no renews should fire while FF phase is Suspended"
        );

        // Resolve by flipping FF's state vector to a renewable shape.
        engine_inner
            .set_info(make_info(
                exec_id,
                LifecyclePhase::Active,
                AttemptState::RunningAttempt,
                OwnershipState::Leased,
            ))
            .await;

        // One tick is 500 ms; two ticks' slack (1100 ms).
        tokio::time::sleep(Duration::from_millis(1_100)).await;

        let renews = mock.renew_calls.load(Ordering::SeqCst);
        assert!(
            renews >= 1,
            "#666: keeper must fire renew after FF phase flips back to \
             RenewableActive; observed {renews} renews"
        );

        registry.shutdown_all().await;
    }

    /// #666: when FF reports the execution no longer exists
    /// (read_execution_info returns Ok(None)), the keeper must exit
    /// cleanly rather than loop forever on a missing execution.
    #[tokio::test]
    async fn keeper_exits_when_execution_vanishes() {
        let registry = Arc::new(LeaseKeeperRegistry::new());
        let mock = Arc::new(MockRuns::default());
        let runs: Arc<dyn RunService> = mock.clone();
        let exec_id = test_execution_id();
        let engine_inner = Arc::new(MockEngine::new(make_info(
            exec_id.clone(),
            LifecyclePhase::Active,
            AttemptState::RunningAttempt,
            OwnershipState::Leased,
        )));
        let engine: Arc<dyn Engine> = engine_inner.clone();

        let run_id = RunId::new("run_666_vanish");
        let session_id = SessionId::new("sess_666_vanish");

        registry
            .ensure_running(run_id.clone(), session_id, exec_id, runs, engine, 1_500)
            .await;

        // After one tick, poison the engine so read_execution_info
        // returns Ok(None). The keeper should classify as
        // ExecutionNotFound and self-remove.
        tokio::time::sleep(Duration::from_millis(700)).await;
        engine_inner.clear_info().await;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while registry.contains(&run_id).await {
            if tokio::time::Instant::now() >= deadline {
                panic!("keeper did not self-remove from registry after execution vanished");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        registry.shutdown_all().await;
    }

    /// Bounded-registry invariant: a keeper that exits on a natural
    /// exit path (terminal-state renew response, in this case) MUST
    /// remove its own entry from the registry map before returning.
    /// Without this contract the registry leaks one
    /// `LeaseKeeperHandle` per completed run since process boot —
    /// for terminal runs the re-activation `ensure_running(same_id)`
    /// reap path never runs because no further orchestrate call is
    /// issued against a completed run.
    ///
    /// Determinism: the test uses the [`KeeperObservability`] hook to
    /// wait for `tick_completed` — that notification fires after
    /// `remove_self_from_registry` and before the keeper's `return`.
    /// So once `notified().await` resolves on the terminal tick, the
    /// self-removal has already happened (single writer → single
    /// reader ordering). We then assert the registry is empty, no
    /// sleeps, no polling.
    #[tokio::test]
    async fn keeper_self_removes_from_registry_on_natural_exit() {
        let registry = Arc::new(LeaseKeeperRegistry::new());
        let mock = Arc::new(MockRuns::default());
        // Immediately return a terminal record so the keeper's very
        // first renew tick triggers the natural-exit self-removal
        // path. Exercises the renew-returned-terminal exit site, the
        // most common in production.
        mock.push_response(Ok(MockRuns::running(RunState::Completed)))
            .await;

        let runs: Arc<dyn RunService> = mock.clone();
        let exec_id = test_execution_id();
        let engine: Arc<dyn Engine> = Arc::new(MockEngine::new(make_info(
            exec_id.clone(),
            LifecyclePhase::Active,
            AttemptState::RunningAttempt,
            OwnershipState::Leased,
        )));
        let run_id = RunId::new("run_selfremove");
        let session_id = SessionId::new("sess_selfremove");
        let obs = Arc::new(KeeperObservability::default());

        // Arm the `notified()` future BEFORE spawning so the keeper's
        // `notify_waiters()` on terminal exit is guaranteed to wake us.
        // `Notify` requires the waiter to register before the notify
        // fires; arming after spawn would be a race.
        let terminal_tick = obs.tick_completed.notified();

        registry
            .ensure_running_with_observability(
                run_id.clone(),
                session_id,
                exec_id,
                runs,
                engine,
                // 1500 ms TTL → 500 ms tick; the first tick's renew
                // returns Completed and the natural-exit path
                // self-removes before returning.
                1_500,
                obs.clone(),
            )
            .await;

        // Bounded deterministic wait: the terminal-tick notification
        // fires after self-removal and the final renew. Two seconds
        // is well above the 500 ms interval + one renew round-trip
        // against the in-process mock.
        tokio::time::timeout(Duration::from_secs(2), terminal_tick)
            .await
            .expect("keeper must emit terminal tick_completed within 2s");

        // Self-removal must have happened before the notification.
        assert!(
            !registry.contains(&run_id).await,
            "keeper must self-remove from registry on natural exit"
        );
        assert_eq!(
            registry.len().await,
            0,
            "bounded-registry invariant: natural exit leaves no leftover entry"
        );
        // And confirm we observed exactly one renew — the terminal one.
        assert_eq!(mock.renew_calls.load(Ordering::SeqCst), 1);
        assert_eq!(obs.renew_attempts.load(Ordering::SeqCst), 1);
    }

    /// Helper: build a phase-conflict `RuntimeError` of the exact
    /// shape the FF adapter surfaces on `ff_renew_lease rejected:
    /// execution_not_eligible`. This is the error the #685 dogfood
    /// incident observed (`execution conflict:
    /// execution_not_eligible` in `Display`). Matches
    /// `is_transient_phase_conflict` so the keeper's observability
    /// and re-probe paths activate.
    fn phase_conflict_err() -> RuntimeError {
        RuntimeError::Conflict {
            entity: "execution",
            id: "execution_not_eligible".to_owned(),
        }
    }

    /// #685 Finding 3, happy-recovery case:
    ///
    /// The probe classifies the execution as `RenewableActive`, the
    /// renew FCALL rejects with `execution_not_eligible`, and the
    /// keeper's re-probe ALSO says `RenewableActive` (cairn-fabric
    /// and FF disagree for one tick, scanner mid-flight). Pre-#685
    /// the keeper exited on the first mismatch; post-#685 it bumps
    /// the `probe_renew_classification_mismatch` counter, skips the
    /// tick, and retries on the NEXT tick — which then succeeds.
    ///
    /// Test shape: queue one rejection action followed by a
    /// successful renew response. Wait for two ticks via the
    /// observability hook. Assert:
    ///
    /// * `renew_attempts == 2` (the rejected tick + the successful
    ///   retry tick).
    /// * `renew_rejections == 1` (the first renew).
    /// * `probe_renew_classification_mismatch == 1` (the re-probe
    ///   agreed with the initial probe, so the mismatch counter
    ///   bumps exactly once).
    /// * The keeper is STILL IN the registry (did not exit).
    ///
    /// Pre-fix contrast: the keeper exited after the first
    /// rejection, so `renew_attempts == 1`,
    /// `probe_renew_classification_mismatch` did not exist, and the
    /// registry was empty within ~500 ms. Verified by temporarily
    /// reverting the `Err(err)` branch to the pre-#685
    /// `WARN + return` shape — the assertion on
    /// `renew_attempts == 2` fails.
    #[tokio::test]
    async fn keeper_685_reprobe_retries_after_transient_race() {
        let registry = Arc::new(LeaseKeeperRegistry::new());
        let mock = Arc::new(MockRuns::default());
        // First renew: inject the probe→renew race. Return a
        // phase-conflict error; leave the engine set to
        // RenewableActive so the re-probe ALSO says renewable —
        // the exact mismatch the #685 fix must recover from.
        mock.push_action(MockRenewAction {
            response: Err(phase_conflict_err()),
            then_engine: None,
        })
        .await;
        // Second renew: success. Proves the keeper survived the
        // first tick and is still ticking.
        mock.push_response(Ok(MockRuns::running(RunState::Running)))
            .await;

        let runs: Arc<dyn RunService> = mock.clone();
        let exec_id = test_execution_id();
        // Engine pinned to RenewableActive for the entire test.
        let engine_inner = Arc::new(MockEngine::new(make_info(
            exec_id.clone(),
            LifecyclePhase::Active,
            AttemptState::RunningAttempt,
            OwnershipState::Leased,
        )));
        mock.with_engine(engine_inner.clone()).await;
        let engine: Arc<dyn Engine> = engine_inner.clone();

        let run_id = RunId::new("run_685_retry");
        let session_id = SessionId::new("sess_685_retry");
        let obs = Arc::new(KeeperObservability::default());

        registry
            .ensure_running_with_observability(
                run_id.clone(),
                session_id,
                exec_id,
                runs,
                engine,
                // 1500 ms TTL → 500 ms tick. Two ticks fit well
                // within a single-digit-second test window.
                1_500,
                obs.clone(),
            )
            .await;

        // Wait for two ticks. Arm futures before checking counters
        // so a `notify_waiters()` race can't drop the signal.
        for tick in 1..=2 {
            let notified = obs.tick_completed.notified();
            tokio::pin!(notified);
            tokio::time::timeout(Duration::from_secs(3), notified)
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "keeper did not signal tick {tick}/2 within 3s — \
                         keeper exited early? \
                         renew_attempts={} renew_rejections={} mismatches={}",
                        obs.renew_attempts.load(Ordering::SeqCst),
                        obs.renew_rejections.load(Ordering::SeqCst),
                        obs.probe_renew_classification_mismatch
                            .load(Ordering::SeqCst),
                    )
                });
        }

        let attempts = obs.renew_attempts.load(Ordering::SeqCst);
        let rejections = obs.renew_rejections.load(Ordering::SeqCst);
        let mismatches = obs
            .probe_renew_classification_mismatch
            .load(Ordering::SeqCst);

        assert_eq!(
            attempts, 2,
            "#685: keeper must retry on the next tick after a phase-\
             conflict rejection whose re-probe agrees with the initial \
             probe. Pre-fix the keeper exited on the first rejection \
             and never issued the second renew.\n\n\
             Observed: renew_attempts={attempts} \
             renew_rejections={rejections} \
             probe_renew_classification_mismatch={mismatches}"
        );
        assert_eq!(
            rejections, 1,
            "#685: exactly one renew should have been rejected \
             (the first). Observed: rejections={rejections}"
        );
        assert_eq!(
            mismatches, 1,
            "#685: the mismatch counter must bump exactly once on \
             the probe+re-probe-both-RenewableActive path. \
             Observed: mismatches={mismatches}"
        );
        assert!(
            registry.contains(&run_id).await,
            "#685: keeper must still be registered after one \
             transient mismatch; pre-fix it had exited"
        );

        registry.shutdown_all().await;
    }

    /// #685 Finding 3, bounded-retry case:
    ///
    /// Three consecutive classification mismatches force the keeper
    /// to exit. This proves the fix is a *bounded* retry — not an
    /// infinite loop that would silently wedge the keeper on a
    /// permanently broken state vector. The cap is
    /// [`KEEPER_CLASSIFICATION_MISMATCH_CAP`]; exceeding it logs
    /// WARN and self-removes from the registry so a subsequent
    /// `/orchestrate` HTTP call re-spawns a fresh keeper against
    /// whatever state FF reports at that time.
    #[tokio::test]
    async fn keeper_685_exits_after_mismatch_cap() {
        let registry = Arc::new(LeaseKeeperRegistry::new());
        let mock = Arc::new(MockRuns::default());
        // Queue exactly KEEPER_CLASSIFICATION_MISMATCH_CAP
        // phase-conflict rejections. Engine stays RenewableActive
        // throughout so every re-probe also says renewable and the
        // mismatch counter climbs each tick. On the CAP-th tick the
        // keeper should exit.
        for _ in 0..KEEPER_CLASSIFICATION_MISMATCH_CAP {
            mock.push_action(MockRenewAction {
                response: Err(phase_conflict_err()),
                then_engine: None,
            })
            .await;
        }

        let runs: Arc<dyn RunService> = mock.clone();
        let exec_id = test_execution_id();
        let engine_inner = Arc::new(MockEngine::new(make_info(
            exec_id.clone(),
            LifecyclePhase::Active,
            AttemptState::RunningAttempt,
            OwnershipState::Leased,
        )));
        mock.with_engine(engine_inner.clone()).await;
        let engine: Arc<dyn Engine> = engine_inner.clone();

        let run_id = RunId::new("run_685_cap");
        let session_id = SessionId::new("sess_685_cap");
        let obs = Arc::new(KeeperObservability::default());

        registry
            .ensure_running_with_observability(
                run_id.clone(),
                session_id,
                exec_id,
                runs,
                engine,
                1_500,
                obs.clone(),
            )
            .await;

        // Poll until the keeper has self-removed from the registry.
        // At 500 ms per tick × CAP ticks + slack, 5 s is a safe
        // ceiling. If the keeper wedged in a retry loop (the
        // anti-pattern this cap guards against) the registry would
        // still contain `run_id` at deadline and the assertion
        // fails with a clear message.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while registry.contains(&run_id).await {
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "#685: keeper did not exit after {} consecutive \
                     classification mismatches (would have retried \
                     forever without the cap). \
                     renew_attempts={} mismatches={}",
                    KEEPER_CLASSIFICATION_MISMATCH_CAP,
                    obs.renew_attempts.load(Ordering::SeqCst),
                    obs.probe_renew_classification_mismatch
                        .load(Ordering::SeqCst)
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let attempts = obs.renew_attempts.load(Ordering::SeqCst);
        let mismatches = obs
            .probe_renew_classification_mismatch
            .load(Ordering::SeqCst);
        assert_eq!(
            attempts as u32, KEEPER_CLASSIFICATION_MISMATCH_CAP,
            "#685: keeper should have issued exactly CAP renew \
             attempts before exiting. Observed: attempts={attempts}"
        );
        assert_eq!(
            mismatches as u32, KEEPER_CLASSIFICATION_MISMATCH_CAP,
            "#685: the mismatch counter should bump once per \
             attempt at the cap. Observed: mismatches={mismatches}"
        );

        registry.shutdown_all().await;
    }

    /// #685 Finding 3, legit-transition recovery case:
    ///
    /// The probe classifies the execution as `RenewableActive`, the
    /// renew FCALL rejects with `execution_not_eligible`, and the
    /// re-probe reports `Suspended` — a legitimate phase transition
    /// between the probe and the renew (approval landed mid-tick).
    /// The keeper should log INFO, reset its mismatch streak, and
    /// continue — the next tick's probe will see the Suspended
    /// phase on its own and skip cleanly.
    ///
    /// Asserts the mismatch counter does NOT bump on this path —
    /// the counter is reserved for the probe+re-probe-both-
    /// RenewableActive case, so operators can distinguish "keeper
    /// raced FF's scanner" from "keeper saw a legitimate
    /// transition".
    #[tokio::test]
    async fn keeper_685_survives_legit_phase_transition() {
        let registry = Arc::new(LeaseKeeperRegistry::new());
        let mock = Arc::new(MockRuns::default());
        let exec_id = test_execution_id();
        let engine_inner = Arc::new(MockEngine::new(make_info(
            exec_id.clone(),
            LifecyclePhase::Active,
            AttemptState::RunningAttempt,
            OwnershipState::Leased,
        )));
        mock.with_engine(engine_inner.clone()).await;

        // First renew: fail, AND flip the engine to Suspended
        // so the keeper's re-probe observes the legitimate
        // transition.
        let flip_exec = exec_id.clone();
        mock.push_action(MockRenewAction {
            response: Err(phase_conflict_err()),
            then_engine: Some(Box::new(move |engine: Arc<MockEngine>| {
                let flip_exec = flip_exec.clone();
                Box::pin(async move {
                    engine
                        .set_info(make_info(
                            flip_exec,
                            LifecyclePhase::Suspended,
                            AttemptState::AttemptInterrupted,
                            OwnershipState::Leased,
                        ))
                        .await;
                })
            })),
        })
        .await;

        let runs: Arc<dyn RunService> = mock.clone();
        let engine: Arc<dyn Engine> = engine_inner.clone();

        let run_id = RunId::new("run_685_legit");
        let session_id = SessionId::new("sess_685_legit");
        let obs = Arc::new(KeeperObservability::default());

        registry
            .ensure_running_with_observability(
                run_id.clone(),
                session_id,
                exec_id,
                runs,
                engine,
                1_500,
                obs.clone(),
            )
            .await;

        // Wait for two ticks. First is the race tick; second is
        // the post-flip tick where the probe should see Suspended
        // and skip the renew entirely.
        for tick in 1..=2 {
            let notified = obs.tick_completed.notified();
            tokio::pin!(notified);
            tokio::time::timeout(Duration::from_secs(3), notified)
                .await
                .unwrap_or_else(|_| panic!("tick {tick}/2 not signalled"));
        }

        let attempts = obs.renew_attempts.load(Ordering::SeqCst);
        let rejections = obs.renew_rejections.load(Ordering::SeqCst);
        let mismatches = obs
            .probe_renew_classification_mismatch
            .load(Ordering::SeqCst);

        assert_eq!(
            attempts, 1,
            "#685: only the first tick should have attempted a \
             renew; the second tick's probe sees Suspended and \
             skips. Observed: attempts={attempts}"
        );
        assert_eq!(
            rejections, 1,
            "#685: the first renew is rejected. \
             Observed: rejections={rejections}"
        );
        assert_eq!(
            mismatches, 0,
            "#685: a legitimate probe → Suspended transition must \
             NOT bump the mismatch counter — that counter is \
             reserved for the probe+re-probe-both-RenewableActive \
             race. Observed: mismatches={mismatches}"
        );
        assert!(
            registry.contains(&run_id).await,
            "#685: keeper must still be registered after a legit \
             phase transition recovery"
        );

        registry.shutdown_all().await;
    }

    /// #685 Finding 3, Gemini MEDIUM follow-up (PR #688): the
    /// `consecutive_classification_mismatches` counter must reset on
    /// every tick that does NOT hit Path C (probe-renewable → renew-
    /// rejected → re-probe-renewable). The initial probe reporting a
    /// non-renewable phase — Path B — is a "skip this tick" outcome,
    /// and must reset the streak just like a successful renew or a
    /// legitimate-transition re-probe.
    ///
    /// Flap scenario the bug allows, pre-fix:
    /// 1. Tick 1: probe RenewableActive, renew rejects, re-probe
    ///    RenewableActive → mismatch counter 1.
    /// 2. Tick 2: probe RenewableActive, renew rejects, re-probe
    ///    RenewableActive → mismatch counter 2.
    /// 3. Tick 3: probe Suspended (approval landed mid-run) →
    ///    skip_renew = true. **Pre-fix: counter STAYS at 2.**
    /// 4. Tick 4: probe RenewableActive (approval resolved), renew
    ///    rejects, re-probe RenewableActive → pre-fix counter hits
    ///    the cap (3) and the keeper EXITS — even though the two
    ///    race windows were separated by a legitimate Suspended
    ///    wait. Post-fix: the Suspended tick reset the counter, so
    ///    tick 4 is a fresh streak of 1 and the keeper continues.
    ///
    /// The test orchestrates the flap deterministically via the
    /// `tick_completed` hook and asserts the keeper is STILL
    /// registered after tick 4. Pre-fix (counter not reset) the
    /// keeper would have self-removed at tick 4; post-fix it stays.
    ///
    /// Pre-fix failure evidence: reverting the new
    /// `consecutive_classification_mismatches = 0` assignment in the
    /// non-renewable Path B branch causes this test to fail at the
    /// `registry.contains(&run_id)` assertion within ~5 s.
    #[tokio::test]
    async fn keeper_685_mismatch_counter_resets_across_non_renewable_detour() {
        let registry = Arc::new(LeaseKeeperRegistry::new());
        let mock = Arc::new(MockRuns::default());
        let exec_id = test_execution_id();

        // Start engine in RenewableActive so the first two ticks
        // enter the renew branch and take the mismatch path.
        let engine_inner = Arc::new(MockEngine::new(make_info(
            exec_id.clone(),
            LifecyclePhase::Active,
            AttemptState::RunningAttempt,
            OwnershipState::Leased,
        )));
        mock.with_engine(engine_inner.clone()).await;

        // Queue three phase-conflict rejections. Engine stays
        // RenewableActive across them (no `then_engine` flip) so
        // each re-probe agrees with the initial probe and the
        // mismatch counter increments.
        //
        // Ticks 1+2 consume two rejections → counter reaches 2.
        // Tick 3 probes Suspended (set from the test coroutine
        // below) → no renew fires, no queued response consumed.
        // Tick 4 probes RenewableActive again (test flips it back)
        // → third queued rejection fires → counter would be 3 pre-
        // fix, keeper exits. Post-fix: counter reset during tick 3,
        // so tick 4 is streak=1 and keeper continues.
        for _ in 0..3 {
            mock.push_action(MockRenewAction {
                response: Err(phase_conflict_err()),
                then_engine: None,
            })
            .await;
        }

        let runs: Arc<dyn RunService> = mock.clone();
        let engine: Arc<dyn Engine> = engine_inner.clone();

        let run_id = RunId::new("run_685_flap_reset");
        let session_id = SessionId::new("sess_685_flap_reset");
        let obs = Arc::new(KeeperObservability::default());

        // 1500 ms TTL → 500 ms tick. Four ticks ≈ 2 s wall-clock.
        registry
            .ensure_running_with_observability(
                run_id.clone(),
                session_id,
                exec_id.clone(),
                runs,
                engine,
                1_500,
                obs.clone(),
            )
            .await;

        // Tick 1: probe+re-probe both RenewableActive → mismatch 1.
        let n1 = obs.tick_completed.notified();
        tokio::pin!(n1);
        tokio::time::timeout(Duration::from_secs(3), n1)
            .await
            .expect("tick 1/4 (first mismatch) not signalled within 3s");
        assert_eq!(
            obs.probe_renew_classification_mismatch
                .load(Ordering::SeqCst),
            1,
            "tick 1 should register the first classification mismatch"
        );

        // Tick 2: same shape → mismatch 2.
        let n2 = obs.tick_completed.notified();
        tokio::pin!(n2);
        tokio::time::timeout(Duration::from_secs(3), n2)
            .await
            .expect("tick 2/4 (second mismatch) not signalled within 3s");
        assert_eq!(
            obs.probe_renew_classification_mismatch
                .load(Ordering::SeqCst),
            2,
            "tick 2 should register the second classification mismatch; \
             one more would hit the cap without a reset"
        );
        assert!(
            registry.contains(&run_id).await,
            "keeper must still be running after two mismatches (cap is 3)"
        );

        // Flip the engine to Suspended. The NEXT tick enters Path B
        // (non-renewable at initial probe) → skip_renew, and the
        // fix resets `consecutive_classification_mismatches`.
        engine_inner
            .set_info(make_info(
                exec_id.clone(),
                LifecyclePhase::Suspended,
                AttemptState::AttemptInterrupted,
                OwnershipState::Leased,
            ))
            .await;

        // Tick 3: probe Suspended → skip_renew = true. No renew
        // fires, no queued response consumed, tick_completed fires.
        let n3 = obs.tick_completed.notified();
        tokio::pin!(n3);
        tokio::time::timeout(Duration::from_secs(3), n3)
            .await
            .expect("tick 3/4 (Suspended skip) not signalled within 3s");
        let attempts_after_suspended = obs.renew_attempts.load(Ordering::SeqCst);
        assert_eq!(
            attempts_after_suspended, 2,
            "tick 3 must NOT have fired a renew (engine is Suspended); \
             observed renew_attempts={attempts_after_suspended}"
        );

        // Flip back to RenewableActive. The next tick's initial
        // probe enters the renew branch again.
        engine_inner
            .set_info(make_info(
                exec_id.clone(),
                LifecyclePhase::Active,
                AttemptState::RunningAttempt,
                OwnershipState::Leased,
            ))
            .await;

        // Tick 4: probe RenewableActive → renew rejects → re-probe
        // RenewableActive → mismatch.
        //
        // Pre-fix: counter was still 2 going into this tick, so it
        // becomes 3, hits the cap, keeper exits.
        // Post-fix: counter was reset to 0 on tick 3 (Suspended),
        // so it becomes 1, well below cap=3, keeper continues.
        let n4 = obs.tick_completed.notified();
        tokio::pin!(n4);
        tokio::time::timeout(Duration::from_secs(3), n4)
            .await
            .expect("tick 4/4 (post-flap mismatch) not signalled within 3s");

        let total_mismatches = obs
            .probe_renew_classification_mismatch
            .load(Ordering::SeqCst);
        assert_eq!(
            total_mismatches, 3,
            "probe_renew_classification_mismatch is a monotonic \
             lifetime counter (not the consecutive streak). Across \
             the 3 renew attempts in this test it must reach 3. \
             Observed: {total_mismatches}"
        );

        // The load-bearing assertion: even though the monotonic
        // counter hit 3, the internal *consecutive* streak was
        // reset by the Suspended tick, so the keeper must still be
        // registered. Pre-fix the keeper exits here.
        assert!(
            registry.contains(&run_id).await,
            "#685 follow-up: the keeper must survive a flap where a \
             non-renewable detour separates two mismatch windows. \
             Pre-fix the consecutive streak persisted across the \
             Suspended tick and the keeper hit the cap on tick 4. \
             renew_attempts={} rejections={} mismatches={}",
            obs.renew_attempts.load(Ordering::SeqCst),
            obs.renew_rejections.load(Ordering::SeqCst),
            total_mismatches,
        );

        registry.shutdown_all().await;
    }

    /// #685 Finding 3, terminal-re-probe case:
    ///
    /// The probe says `RenewableActive`, the renew rejects
    /// phase-conflict, and the re-probe reports `Terminal` — the
    /// run completed between the probe and the renew. Keeper
    /// should exit cleanly (not WARN + retry) because there's
    /// nothing left to renew.
    #[tokio::test]
    async fn keeper_685_exits_clean_when_reprobe_terminal() {
        let registry = Arc::new(LeaseKeeperRegistry::new());
        let mock = Arc::new(MockRuns::default());
        let exec_id = test_execution_id();
        let engine_inner = Arc::new(MockEngine::new(make_info(
            exec_id.clone(),
            LifecyclePhase::Active,
            AttemptState::RunningAttempt,
            OwnershipState::Leased,
        )));
        mock.with_engine(engine_inner.clone()).await;

        let flip_exec = exec_id.clone();
        mock.push_action(MockRenewAction {
            response: Err(phase_conflict_err()),
            then_engine: Some(Box::new(move |engine: Arc<MockEngine>| {
                let flip_exec = flip_exec.clone();
                Box::pin(async move {
                    engine
                        .set_info(make_info(
                            flip_exec,
                            LifecyclePhase::Terminal,
                            AttemptState::AttemptTerminal,
                            OwnershipState::Unowned,
                        ))
                        .await;
                })
            })),
        })
        .await;

        let runs: Arc<dyn RunService> = mock.clone();
        let engine: Arc<dyn Engine> = engine_inner.clone();

        let run_id = RunId::new("run_685_terminal");
        let session_id = SessionId::new("sess_685_terminal");
        let obs = Arc::new(KeeperObservability::default());

        registry
            .ensure_running_with_observability(
                run_id.clone(),
                session_id,
                exec_id,
                runs,
                engine,
                1_500,
                obs.clone(),
            )
            .await;

        // Terminal re-probe: the keeper should exit cleanly on
        // that tick. Poll for registry cleanup — if it stays
        // registered, the exit path didn't fire.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while registry.contains(&run_id).await {
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "#685: keeper did not exit after re-probe observed \
                     Terminal. attempts={} rejections={} mismatches={}",
                    obs.renew_attempts.load(Ordering::SeqCst),
                    obs.renew_rejections.load(Ordering::SeqCst),
                    obs.probe_renew_classification_mismatch
                        .load(Ordering::SeqCst)
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // And the mismatch counter must not have bumped — the
        // terminal exit path is separate from the mismatch loop.
        let mismatches = obs
            .probe_renew_classification_mismatch
            .load(Ordering::SeqCst);
        assert_eq!(
            mismatches, 0,
            "#685: terminal re-probe must NOT count as a \
             classification mismatch. Observed: mismatches={mismatches}"
        );

        registry.shutdown_all().await;
    }

    /// #685 Finding 3, non-phase-conflict preservation case:
    ///
    /// The probe says `RenewableActive`, the renew rejects with a
    /// non-phase-conflict error (e.g. `RuntimeError::Internal` —
    /// transport-level failure, FF crash, malformed response). The
    /// #685 fix must PRESERVE the #666 contract for these errors:
    /// exit immediately without re-probing. Retrying on a
    /// transport failure or a logic-level failure (lease revoked,
    /// store corrupt) just churns the logs.
    ///
    /// Pre-fix contrast: this test passes on BOTH pre-#685 and
    /// post-#685 code — it's a non-regression guardrail that
    /// ensures the new re-probe logic doesn't silently widen the
    /// error-swallowing surface.
    #[tokio::test]
    async fn keeper_685_exits_on_non_phase_conflict_error() {
        let registry = Arc::new(LeaseKeeperRegistry::new());
        let mock = Arc::new(MockRuns::default());
        mock.push_response(Err(RuntimeError::Internal(
            "simulated transport failure".to_owned(),
        )))
        .await;

        let runs: Arc<dyn RunService> = mock.clone();
        let exec_id = test_execution_id();
        let engine: Arc<dyn Engine> = Arc::new(MockEngine::new(make_info(
            exec_id.clone(),
            LifecyclePhase::Active,
            AttemptState::RunningAttempt,
            OwnershipState::Leased,
        )));
        let run_id = RunId::new("run_685_internal");
        let session_id = SessionId::new("sess_685_internal");
        let obs = Arc::new(KeeperObservability::default());

        registry
            .ensure_running_with_observability(
                run_id.clone(),
                session_id,
                exec_id,
                runs,
                engine,
                1_500,
                obs.clone(),
            )
            .await;

        // Wait for the keeper to exit on the first tick.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while registry.contains(&run_id).await {
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "#685 non-regression: keeper must exit on a \
                     non-phase-conflict error (the #666 contract). \
                     attempts={} rejections={} mismatches={}",
                    obs.renew_attempts.load(Ordering::SeqCst),
                    obs.renew_rejections.load(Ordering::SeqCst),
                    obs.probe_renew_classification_mismatch
                        .load(Ordering::SeqCst),
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert_eq!(
            obs.renew_rejections.load(Ordering::SeqCst),
            0,
            "#685: non-phase-conflict errors must NOT count as \
             phase-conflict rejections"
        );
        assert_eq!(
            obs.probe_renew_classification_mismatch
                .load(Ordering::SeqCst),
            0,
            "#685: non-phase-conflict errors must NOT enter the \
             re-probe path"
        );

        registry.shutdown_all().await;
    }

    /// Bounded-registry invariant, cancellation path: a keeper that
    /// exits via `shutdown_all` must still leave the registry empty
    /// (via `shutdown_all`'s drain path). Self-removal is explicitly
    /// skipped on cancellation — this test confirms the drain is
    /// still doing its job after the self-removal rework.
    #[tokio::test]
    async fn shutdown_all_drains_registry_with_self_removal_in_place() {
        let registry = Arc::new(LeaseKeeperRegistry::new());
        let mock = Arc::new(MockRuns::default());
        let runs: Arc<dyn RunService> = mock.clone();
        let exec_id = test_execution_id();
        // Phase = Suspended so the keeper sits in its skip-tick
        // branch and never reaches a self-removal point before
        // `shutdown_all` cancels it.
        let engine: Arc<dyn Engine> = Arc::new(MockEngine::new(make_info(
            exec_id.clone(),
            LifecyclePhase::Suspended,
            AttemptState::AttemptInterrupted,
            OwnershipState::Leased,
        )));

        for i in 0..3 {
            registry
                .ensure_running(
                    RunId::new(format!("run_drain_{i}")),
                    SessionId::new(format!("sess_drain_{i}")),
                    exec_id.clone(),
                    runs.clone(),
                    engine.clone(),
                    1_500,
                )
                .await;
        }
        assert_eq!(registry.len().await, 3);

        tokio::time::timeout(Duration::from_secs(2), registry.shutdown_all())
            .await
            .expect("shutdown_all must complete within 2s");
        assert_eq!(registry.len().await, 0);
    }
}
