# FF upstream ask: terminal-FCALL behavior on lease-expired executions

**Status**: DRAFT. Cairn ships a two-layer workaround in
`FabricRunService::complete`'s adapter (F59). When both layers fail
(dual-door deadlock — `lease_expired` on FCALL + `execution_not_eligible`
on re-claim), F62 flips the run to `Failed(TerminalWriteDeadlock)` and
surfaces the operator-actionable message with the upstream link. Filed
upstream as [FlowFabric#371](https://github.com/avifenesh/FlowFabric/issues/371).
This doc captures the underlying contract question for FF maintainers.

**Cairn version**: main @ F62
**FF version**: 0.11

## Problem

Cairn's control-plane loop uses a pull model: `POST /v1/runs/:id/orchestrate`
runs one GATHER → DECIDE → EXECUTE iteration (up to `max_iterations`) and
returns. Within one HTTP call the orchestrator can burn several seconds or
more per iteration (bedrock latency, tool execution, operator approvals).
There is no background lease renewer running inside the per-iteration loop —
ff-sdk's `ClaimedTask` renewer lifetime is scoped to an outer handler we do
not wire here, and cairn's control-plane lease is managed by direct
`ff_issue_grant + ff_claim_execution` + `ff_renew_lease` FCALLs.

Result: on long iterations, the lease can expire between the orchestrator's
last iteration and the final `complete_run` terminal FCALL. M1-v2 dogfood
(2026-04-26) hit this 15+ times across a 50-iteration run. Every
`ff_complete_execution` attempt rejected with:

```
ff_complete_execution rejected: lease_expired
```

## What cairn tries

F59 wires two layers in `cairn-app::fabric_adapter::RunService::{complete,fail,cancel}`:

1. **Pre-FCALL renew**: call `renew_lease_if_stale(min_remaining=10s)`
   immediately before the terminal FCALL. Catches the stale window
   (lease still valid, but aging) and extends in place.
2. **Retry once on `lease_expired`**: if the FCALL rejects, walk a
   fresh `ff_claim_execution` (rotates the lease epoch and mints a
   new lease) and retry the terminal FCALL once against the new lease.

Layer (1) works: it turns "stale lease at FCALL time" into "fresh lease at
FCALL time" deterministically.

Layer (2) hits an FF-shaped gap:

- After `ff_complete_execution` rejects with `lease_expired`,
  `validate_lease_and_mark_expired` has cleared `current_lease_id` and
  marked the lease expired.
- `ff_claim_execution` then rejects with `execution_not_eligible` — the
  `lifecycle_phase` is still `active` (not back to `runnable`), so FF's
  eligibility gate rejects a fresh claim.
- `ff_renew_lease` rejects with `lease_expired` for the same reason.
- `ff_claim_resumed_execution` applies to suspension/resume flows, not
  lease-expired recovery.

There is no cairn-reachable path to recover the execution. Cairn surfaces
a cairn-typed `Internal` error telling the operator the run artifacts are
preserved but the terminal event could not be written.

## What FF could expose

One of (in order of cairn preference):

1. **Auto-transition on lease_expired**. When
   `validate_lease_and_mark_expired` clears `current_lease_id` inside a
   terminal FCALL, also transition `lifecycle_phase` back to `runnable`
   so the NEXT `ff_claim_execution` succeeds. The caller then retries
   the terminal FCALL on the fresh lease. This is the minimal-surface
   fix and matches the existing "lease expired, next caller wins"
   semantics used by the lease-expiry scanner.

2. **New FCALL: `ff_claim_for_terminal_write`**. A dedicated FCALL that
   takes an execution id (no lease fence) and mints a fresh lease
   regardless of `lifecycle_phase`, specifically for the narrow
   single-writer case of writing a terminal event. Cairn is the sole
   authoritative terminal-event writer for its runs, so the
   multi-writer concern doesn't apply. Tighter scope than (1) but more
   surface.

3. **Tolerate lease_expired-but-lease-held-by-us inside the terminal
   FCALL**. When the caller's fence triple matches the last-known lease
   and only the timestamp has expired, accept the terminal write. This
   preserves the guarantee that only the rightful owner writes terminal
   state, while closing the "scanner hasn't cleared yet, but timestamp
   is past" race. Smallest semantic change.

## Why this matters

Cairn is the main FF consumer. The terminal-FCALL happy path is the
most operator-visible path in the product — every successful run closes
through it, and every failure mode operators see traces to this FCALL
landing or not landing. Stretching the cairn-side workaround further
(pre-FCALL renew with ever-larger `min_remaining`, synthetic
checkpoints, etc.) adds bridge traffic proportional to iteration count
without closing the edge.

A first-class "terminal write is always possible for the rightful
owner" contract from FF turns cairn's F59 layer (2) into a genuinely
dead code path, which is the right end-state.

## F64 update (2026-04-26): bounded retry loop replaces F59 short-circuit

F59's single-shot `claim + retry` is now wrapped by F64's bounded retry
loop (immediate probe + 2/4/8/16s backoff, ~30s wall-time cap). The
first probe runs with zero sleep so a system that self-heals between
the initial `lease_expired` and loop entry doesn't pay a mandatory
latency tax. Rationale:

* In practice FF often self-heals the `not-eligible` phase within a few
  seconds — scanner cycles advance execution state, the lease reaper
  clears stale claims, and approval-resolution handlers push the
  lifecycle phase forward.
* F59 giving up after one re-claim attempt threw away runs that would
  have completed with a brief wait.

F64 keeps retrying the `claim + terminal-FCALL` pair on each backoff
step. If any retry succeeds the run completes normally. If the
schedule exhausts, the F62 `TerminalWriteDeadlock` fallback fires —
the run flips `Failed(TerminalWriteDeadlock)` with the upstream link
hint, and the operator response becomes
`InvalidTransition { terminal_write_deadlock → <to> }`.

Regardless of outcome, cairn emits a `TerminalRecoveryAttempted`
event (attempts, wall_time_ms, outcome) that populates a nullable
`terminal_write_recovery` field on `RunRecord`. Operators see this on
`GET /v1/runs/:id` so they can tell whether recovery fired and whether
it saved the run.

**This retry loop will be removed once FF#371 ships the upstream fix.**
The recovery annotation column + event variant stay in the schema for
historical audit, but the active loop code becomes dead and gets
deleted alongside the bridge documentation.

Aggressive-heal hook (proactive approval-cancel on deadlock entry) is
gated on the `CAIRN_F64_AGGRESSIVE_HEAL=1` env var. Shipped as a
log-only breadcrumb today; full wiring will attach an ApprovalService
handle to the adapter when a concrete dogfood incident justifies the
extra surface.

## References

- cairn F64 fix (current): `crates/cairn-app/src/fabric_adapter.rs`
  `f64_terminal_recovery_loop` + call sites in
  `FabricRunServiceAdapter::{complete,fail,cancel}`
- cairn F64 integration test: `crates/cairn-app/tests/test_f64_terminal_recovery_loop.rs`
- cairn F59 predecessor fix: `crates/cairn-app/src/fabric_adapter.rs`
  `RunService::complete` impl (now wrapped by F64)
- cairn repro test: `crates/cairn-app/tests/test_f59_complete_run_retry.rs`
- Related upstream asks:
  - `docs/design/ff-upstream/ff-renew-lease-mid-approval.md`
  - `docs/design/ff-upstream/ff-execution-phase-probe.md`
  - `docs/design/ff-upstream/ff-lease-renewal-pull-mode.md`
