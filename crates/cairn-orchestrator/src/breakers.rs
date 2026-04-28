//! F65 PR-3: circuit-breaker runtime state for `OrchestratorLoop`.
//!
//! `BreakerState` is the mutable counter set the loop walks each iteration,
//! derived from the immutable `BreakerConfig` (`LoopConfig.breakers`). It
//! tracks four caps in parallel:
//!
//!   * **Round** — compared against `ctx.iteration` each iteration.
//!   * **Tokens** — cumulative input + output tokens across all DECIDE rounds.
//!   * **NoToolUseConsecutive** — consecutive DECIDE rounds that produced
//!     zero tool-use proposals. Resets on any iteration that dispatches
//!     at least one tool.
//!   * **WallClock** — monotonic `Instant`-based wall-clock relative to
//!     `started_at`.
//!
//! The state also remembers which breakers have already emitted their
//! `BudgetThresholdCrossed` warning, so the 80 % warning fires at most once
//! per run per breaker. `NoToolUseConsecutive` deliberately skips the 80 %
//! warning — see [`crate::context::BreakerConfig`] for the rationale.
//!
//! # Ordering invariant
//!
//! The four checks are consulted in a fixed order
//! (Round → WallClock → Tokens → NoToolUseConsecutive) so that two
//! simultaneously-crossed thresholds produce the same trip kind on every
//! platform. Tests assert this order explicitly.

use std::time::Instant;

use cairn_domain::session_orchestration::{BreakerKind, CircuitBreakerTrip};

use crate::context::BreakerConfig;

/// Basis-point threshold that triggers the once-per-run 80 % warning.
/// 8_000 / 10_000 = 0.80.
const WARN_RATIO_BPS: u32 = 8_000;

/// Decision returned by `BreakerState::after_decide` /
/// `BreakerState::check_pre_gather`.
///
/// The loop interprets each variant:
///
///   * `Continue` — all breakers under their caps; the iteration may proceed.
///   * `Warning { which, measured, limit, ratio_bps }` — a breaker crossed its
///     80 % warning threshold for the first time; emit
///     `RuntimeEvent::BudgetThresholdCrossed`, keep running.
///   * `Tripped(trip)` — a breaker reached its cap; emit
///     `RuntimeEvent::CircuitBreakerTripped` and terminate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum BreakerCheck {
    Continue,
    Warning {
        which: BreakerKind,
        measured: u64,
        limit: u64,
        ratio_bps: u32,
    },
    Tripped(CircuitBreakerTrip),
}

/// Mutable per-run state holding live counters and warning latches.
///
/// Constructed once at the top of `OrchestratorLoop::run_inner` from the
/// immutable `BreakerConfig`. The loop owns the only instance and mutates it
/// via `&mut self` methods; no `Arc<Mutex<…>>` needed — the orchestrator
/// loop is single-threaded.
#[derive(Debug)]
pub(crate) struct BreakerState {
    cfg: BreakerConfig,
    /// Monotonic start-of-loop timestamp; wall-clock comparisons use
    /// `started_at.elapsed()` so forward clock jumps can never cause a
    /// spurious trip.
    started_at: Instant,
    /// Cumulative LLM tokens (input + output) observed across all DECIDE
    /// rounds since the loop started.
    tokens_used: u64,
    /// Consecutive DECIDE rounds with zero tool-use proposals.
    no_tool_use_streak: u32,
    /// Once-per-run latches; flipped `true` when the corresponding
    /// breaker's 80 % warning is emitted to prevent re-fire. Index
    /// matches `BreakerKind as usize` via `kind_index`.
    warn_fired: [bool; 4],
}

impl BreakerState {
    /// Construct a fresh state from `cfg` anchored to `Instant::now()`.
    pub(crate) fn new(cfg: BreakerConfig) -> Self {
        Self {
            cfg,
            started_at: Instant::now(),
            tokens_used: 0,
            no_tool_use_streak: 0,
            warn_fired: [false; 4],
        }
    }

    /// Snapshot the current token counter. Used by tests.
    #[cfg(test)]
    pub(crate) fn tokens_used(&self) -> u64 {
        self.tokens_used
    }

    /// Snapshot the current no-tool-use streak. Used by tests.
    #[cfg(test)]
    pub(crate) fn no_tool_use_streak(&self) -> u32 {
        self.no_tool_use_streak
    }

    /// Check the Round + WallClock breakers at the top of each iteration
    /// BEFORE gather fires. Tokens + no-tool-use streak can only update
    /// after DECIDE, so they are checked in `after_decide`.
    ///
    /// Returns:
    ///   * `Tripped(…)` if Round or WallClock is at-or-above its cap.
    ///   * `Warning { … }` if Round or WallClock just crossed the 80 %
    ///     threshold (first crossing only; latched thereafter).
    ///   * `Continue` otherwise.
    ///
    /// If both Round and WallClock are simultaneously tripped, Round wins
    /// per the module-level ordering invariant.
    pub(crate) fn check_pre_gather(&mut self, iteration: u32) -> BreakerCheck {
        // Round — iteration 0-based, trip when iteration >= round_cap.
        if iteration >= self.cfg.round_cap {
            return BreakerCheck::Tripped(CircuitBreakerTrip {
                which: BreakerKind::Round,
                measured: iteration as u64,
                limit: self.cfg.round_cap as u64,
                at_iteration: iteration,
            });
        }
        // WallClock — monotonic elapsed.
        let elapsed_ms = self.started_at.elapsed().as_millis().min(u64::MAX as u128) as u64;
        if elapsed_ms >= self.cfg.wall_clock_ms {
            return BreakerCheck::Tripped(CircuitBreakerTrip {
                which: BreakerKind::WallClock,
                measured: elapsed_ms,
                limit: self.cfg.wall_clock_ms,
                at_iteration: iteration,
            });
        }

        // 80% warnings (once per breaker). Check in the same order as trips.
        if let Some(check) = self.maybe_warn(
            BreakerKind::Round,
            iteration as u64,
            self.cfg.round_cap as u64,
        ) {
            return check;
        }
        if let Some(check) =
            self.maybe_warn(BreakerKind::WallClock, elapsed_ms, self.cfg.wall_clock_ms)
        {
            return check;
        }
        BreakerCheck::Continue
    }

    /// Update token + no-tool-use counters based on the freshly-completed
    /// DECIDE output, then check for Token / NoToolUseConsecutive trips
    /// and warnings.
    ///
    /// `progress_proposal_count` is the number of proposals this round
    /// that count as "forward progress" for the NoToolUseConsecutive
    /// streak. The caller MUST include both tool proposals
    /// (`tool_name: Some(_)`) AND terminal / operator-gated actions
    /// (`ActionType::CompleteRun`, `EscalateToOperator`, `SpawnSubagent`)
    /// in this count — see `BreakerConfig::no_tool_use_streak` rustdoc
    /// for the complete contract. A zero count increments the streak;
    /// any positive count resets it.
    ///
    /// `input_tokens` + `output_tokens` are typically
    /// `DecideOutput.input_tokens.unwrap_or(0)` etc. The caller MUST
    /// emit a one-time warning when both are `None` (provider didn't
    /// report usage) because this function treats absent tokens as 0
    /// — it cannot distinguish "no tokens" from "no signal."
    pub(crate) fn after_decide(
        &mut self,
        iteration: u32,
        input_tokens: u32,
        output_tokens: u32,
        progress_proposal_count: usize,
    ) -> BreakerCheck {
        let added = (input_tokens as u64).saturating_add(output_tokens as u64);
        self.tokens_used = self.tokens_used.saturating_add(added);
        if progress_proposal_count == 0 {
            self.no_tool_use_streak = self.no_tool_use_streak.saturating_add(1);
        } else {
            self.no_tool_use_streak = 0;
        }

        // Trip checks.
        if self.tokens_used >= self.cfg.token_cap {
            return BreakerCheck::Tripped(CircuitBreakerTrip {
                which: BreakerKind::Tokens,
                measured: self.tokens_used,
                limit: self.cfg.token_cap,
                at_iteration: iteration,
            });
        }
        if self.no_tool_use_streak >= self.cfg.no_tool_use_streak {
            return BreakerCheck::Tripped(CircuitBreakerTrip {
                which: BreakerKind::NoToolUseConsecutive,
                measured: self.no_tool_use_streak as u64,
                limit: self.cfg.no_tool_use_streak as u64,
                at_iteration: iteration,
            });
        }

        // 80% warning for Tokens (once). `NoToolUseConsecutive` deliberately
        // skips the 80 % warning: with a default cap of 3, 80 % rounds to 2
        // — one turn before the trip, which is not actionable for re-prompting.
        // Documented in `BreakerConfig` rustdoc + arch doc §4.1.
        if let Some(check) =
            self.maybe_warn(BreakerKind::Tokens, self.tokens_used, self.cfg.token_cap)
        {
            return check;
        }
        BreakerCheck::Continue
    }

    /// Emit a `Warning` once when `measured / limit >= 80 %`. Returns
    /// `Some(BreakerCheck::Warning)` on the first crossing, `None`
    /// thereafter (latched via `warn_fired`). Zero-limit protects against
    /// divide-by-zero (config validation should reject this but we
    /// defensively skip the warning).
    fn maybe_warn(
        &mut self,
        which: BreakerKind,
        measured: u64,
        limit: u64,
    ) -> Option<BreakerCheck> {
        if limit == 0 {
            return None;
        }
        let idx = kind_index(which);
        if self.warn_fired[idx] {
            return None;
        }
        // Use the overflow-safe `ratio_bps` helper (u128-upcasted) to
        // check the 80% threshold. Avoids the overflow-handling mistake
        // from the earlier `measured * 5 >= limit * 4` form: the original
        // fallback path `measured / 5 >= limit / 4` was NOT mathematically
        // equivalent (e.g. measured=80, limit=100 → 16 >= 25 → false,
        // though 80% of 100 should cross). Gemini-code-assist flagged
        // this on PR #348; the fix routes through the already-safe
        // shared helper instead of hand-rolling the arithmetic twice.
        let ratio = ratio_bps(measured, limit);
        if ratio < WARN_RATIO_BPS {
            return None;
        }
        self.warn_fired[idx] = true;
        Some(BreakerCheck::Warning {
            which,
            measured,
            limit,
            ratio_bps: ratio,
        })
    }
}

/// Map a `BreakerKind` to an index into `warn_fired`. Kept as a free
/// function so tests can assert on array shape without exposing internals.
fn kind_index(which: BreakerKind) -> usize {
    match which {
        BreakerKind::Round => 0,
        BreakerKind::Tokens => 1,
        BreakerKind::NoToolUseConsecutive => 2,
        BreakerKind::WallClock => 3,
    }
}

/// Compute `measured / limit` as basis points (0-10_000). Clamps at
/// 10_000 for the case `measured > limit` (a trip has occurred; we still
/// report the ratio honestly on the downstream event). Uses u128
/// arithmetic so `measured * 10_000` never overflows.
pub(crate) fn ratio_bps(measured: u64, limit: u64) -> u32 {
    if limit == 0 {
        return 0;
    }
    let bps = (measured as u128)
        .saturating_mul(10_000)
        .checked_div(limit as u128)
        .unwrap_or(0);
    bps.min(10_000) as u32
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> BreakerConfig {
        BreakerConfig {
            round_cap: 5,
            token_cap: 1_000,
            no_tool_use_streak: 3,
            wall_clock_ms: 60_000,
        }
    }

    #[test]
    fn ratio_bps_basics() {
        assert_eq!(ratio_bps(0, 100), 0);
        assert_eq!(ratio_bps(50, 100), 5_000);
        assert_eq!(ratio_bps(80, 100), 8_000);
        assert_eq!(ratio_bps(100, 100), 10_000);
        // Clamp at 100% when measured exceeds limit.
        assert_eq!(ratio_bps(200, 100), 10_000);
        assert_eq!(ratio_bps(0, 0), 0);
    }

    #[test]
    fn round_cap_trips_at_iteration_equal_to_cap() {
        let mut s = BreakerState::new(cfg());
        assert_eq!(s.check_pre_gather(0), BreakerCheck::Continue);
        assert_eq!(s.check_pre_gather(3), BreakerCheck::Continue);
        match s.check_pre_gather(5) {
            BreakerCheck::Tripped(t) => {
                assert_eq!(t.which, BreakerKind::Round);
                assert_eq!(t.measured, 5);
                assert_eq!(t.limit, 5);
                assert_eq!(t.at_iteration, 5);
            }
            other => panic!("expected Tripped(Round), got {other:?}"),
        }
    }

    #[test]
    fn round_warning_fires_once_at_80_percent() {
        let mut s = BreakerState::new(cfg());
        // cap=5, 80% of 5 is 4. `measured * 5 >= limit * 4` at iteration 4:
        // 4*5=20, 5*4=20 → >= ✓
        let r = s.check_pre_gather(4);
        match r {
            BreakerCheck::Warning {
                which,
                measured,
                limit,
                ratio_bps,
            } => {
                assert_eq!(which, BreakerKind::Round);
                assert_eq!(measured, 4);
                assert_eq!(limit, 5);
                assert_eq!(ratio_bps, 8_000);
            }
            other => panic!("expected Warning(Round), got {other:?}"),
        }
        // Re-checking at iteration 4 (or lower) must not re-emit the warning.
        assert_eq!(s.check_pre_gather(4), BreakerCheck::Continue);
    }

    #[test]
    fn tokens_cap_trips_on_cumulative_usage() {
        let mut s = BreakerState::new(cfg()); // cap=1000
        assert_eq!(s.after_decide(0, 100, 40, 1), BreakerCheck::Continue);
        assert_eq!(s.tokens_used(), 140);
        // Warning fires when cumulative >= 800.
        let w = s.after_decide(1, 400, 300, 1);
        match w {
            BreakerCheck::Warning { which, .. } => assert_eq!(which, BreakerKind::Tokens),
            other => panic!("expected Warning(Tokens), got {other:?}"),
        }
        // Cap trips at or beyond 1000. Current = 840. Add 200 → 1040.
        match s.after_decide(2, 100, 100, 1) {
            BreakerCheck::Tripped(t) => {
                assert_eq!(t.which, BreakerKind::Tokens);
                assert_eq!(t.measured, 1_040);
                assert_eq!(t.limit, 1_000);
                assert_eq!(t.at_iteration, 2);
            }
            other => panic!("expected Tripped(Tokens), got {other:?}"),
        }
    }

    #[test]
    fn no_tool_use_streak_trips_after_three_zero_tool_turns() {
        let mut s = BreakerState::new(cfg()); // streak cap = 3
                                              // Turn 1: zero tools → streak = 1
        assert_eq!(s.after_decide(0, 10, 10, 0), BreakerCheck::Continue);
        assert_eq!(s.no_tool_use_streak(), 1);
        // Turn 2: zero tools → streak = 2
        assert_eq!(s.after_decide(1, 10, 10, 0), BreakerCheck::Continue);
        assert_eq!(s.no_tool_use_streak(), 2);
        // Turn 3: zero tools → streak = 3 = cap → trip.
        match s.after_decide(2, 10, 10, 0) {
            BreakerCheck::Tripped(t) => {
                assert_eq!(t.which, BreakerKind::NoToolUseConsecutive);
                assert_eq!(t.measured, 3);
                assert_eq!(t.limit, 3);
                assert_eq!(t.at_iteration, 2);
            }
            other => panic!("expected Tripped(NoToolUseConsecutive), got {other:?}"),
        }
    }

    #[test]
    fn streak_resets_on_any_tool_use() {
        let mut s = BreakerState::new(cfg());
        let _ = s.after_decide(0, 10, 10, 0);
        let _ = s.after_decide(1, 10, 10, 0);
        assert_eq!(s.no_tool_use_streak(), 2);
        // Iteration 2 dispatches a tool → reset.
        assert_eq!(s.after_decide(2, 10, 10, 1), BreakerCheck::Continue);
        assert_eq!(s.no_tool_use_streak(), 0);
    }

    #[test]
    fn no_tool_use_never_emits_warning() {
        // Decision 2: NoToolUseConsecutive has no 80% warning.
        let mut s = BreakerState::new(cfg()); // cap=3
                                              // 80 % of 3 is 2.4 → the check `measured*5 >= limit*4` passes at 3 (i.e.
                                              // only at trip). Even so, the code explicitly skips the warning
                                              // emission for this breaker; we assert no `Warning(NoToolUseConsecutive)`
                                              // can be returned no matter the streak value.
        for i in 0..2 {
            assert_eq!(s.after_decide(i, 0, 0, 0), BreakerCheck::Continue);
        }
        // Streak now 2 (i.e. 2/3 = 66 %). Below the threshold by design.
        // Confirming the rationale: at 2 consecutive no-tool turns we'd need
        // 2*5 >= 3*4 → 10 >= 12 → false, so no warning anyway.
        assert!(!matches!(
            s.after_decide(2, 0, 0, 0),
            BreakerCheck::Warning {
                which: BreakerKind::NoToolUseConsecutive,
                ..
            }
        ));
    }

    #[test]
    fn wall_clock_trip_ordering_round_wins_when_simultaneous() {
        // Hand-craft state so both Round and WallClock are at their caps.
        // Round is checked first → wins.
        let mut s = BreakerState::new(BreakerConfig {
            round_cap: 2,
            token_cap: 1_000,
            no_tool_use_streak: 3,
            wall_clock_ms: 0, // Would trip instantly — but round must win.
        });
        match s.check_pre_gather(2) {
            BreakerCheck::Tripped(t) => assert_eq!(t.which, BreakerKind::Round),
            other => panic!("expected Round to win, got {other:?}"),
        }
    }

    #[test]
    fn zero_limit_does_not_panic_in_warning_path() {
        // Defensive: a zero token_cap is pathological (HTTP handler
        // validates tighten-only, but a config built directly could
        // technically set 0). The trip check `measured >= limit` still
        // fires (0 >= 0), which is correct: a zero budget means every
        // round trips. What we guard specifically is the 80% warning
        // math: zero-limit must NOT panic on the integer multiply or
        // produce a spurious warning.
        let mut s = BreakerState::new(BreakerConfig {
            round_cap: 10,
            token_cap: 0,
            no_tool_use_streak: 3,
            wall_clock_ms: 60_000,
        });
        // First call trips immediately on the trip path (measured=0 >= limit=0).
        match s.after_decide(0, 0, 0, 1) {
            BreakerCheck::Tripped(t) => {
                assert_eq!(t.which, BreakerKind::Tokens);
                assert_eq!(t.measured, 0);
                assert_eq!(t.limit, 0);
            }
            other => panic!("expected Tripped(Tokens) for zero-cap, got {other:?}"),
        }
    }

    #[test]
    fn zero_limit_warning_path_does_not_panic() {
        // Exercise `maybe_warn` in isolation via the ratio helper and a
        // hand-built state where only the warning codepath matters.
        // `ratio_bps(0, 0) == 0` covers the divide-by-zero guard, and
        // the state-level path never panics because `maybe_warn` early-
        // returns on `limit == 0`.
        assert_eq!(ratio_bps(0, 0), 0);
        assert_eq!(ratio_bps(42, 0), 0);
    }

    #[test]
    fn kind_index_is_stable() {
        // Documents the warn_fired array layout so adding BreakerKind
        // variants without resizing fails to compile here.
        assert_eq!(kind_index(BreakerKind::Round), 0);
        assert_eq!(kind_index(BreakerKind::Tokens), 1);
        assert_eq!(kind_index(BreakerKind::NoToolUseConsecutive), 2);
        assert_eq!(kind_index(BreakerKind::WallClock), 3);
    }
}
