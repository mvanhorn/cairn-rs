# FF upstream ask: renew_lease behavior on mid-approval executions

**Status**: DRAFT. Cairn ships a workaround in the orchestrate handler
(F57, this PR). This doc captures the underlying contract question for
FF maintainers.

**Cairn version**: main @ F57
**FF version**: 0.11

## Problem

Cairn's control-plane loop uses a pull model: `POST /v1/runs/:id/orchestrate`
runs one GATHER → DECIDE → EXECUTE iteration and returns. Between HTTP
calls, there is no long-lived worker holding or renewing the lease.

On the handler entry path cairn calls `FabricRunService::renew_lease_if_stale`
(which wraps `ff_renew_lease`) to keep the lease fresh across operator-paced
workflows. This works cleanly when the execution is in the `active`
lifecycle phase.

When the execution is mid-approval, the surface is ambiguous:

- After cairn calls `ff_suspend_execution` to enter `waiting_approval`:
  `lifecycle_phase = "suspended"`, `ownership_state = "unowned"`,
  `current_lease_id = ""`. `ff_renew_lease` rejects with
  `execution_not_active` (flowfabric.lua:1360-1366). OK — expected.
- After the approval signal auto-resumes the suspension (via
  `composite_deliver_signal` → `ff_deliver_signal`, flowfabric.lua:5417-5459):
  `lifecycle_phase = "runnable"`, `ownership_state = "unowned"`,
  `eligibility_state = "eligible_now"`, no lease.
  Here a fresh `ff_issue_grant + ff_claim_execution` cycle is required to
  reach `active`. But if cairn instead called `ff_renew_lease` on the
  naively-cached lease triple from pre-suspend, it would reject
  `execution_not_active` AND the fallback `ff_issue_grant` would succeed —
  we need to take the claim path, not the renew path.

Empirically (Phase 2-v2 dogfood, 2026-04-26), the third
`/orchestrate` call on a mid-run with two prior approval cycles 409'd
with `execution_not_eligible`. Trace shows `ff_issue_grant` is rejecting
on the `lifecycle_phase != "runnable"` precondition
(flowfabric.lua:3585) — meaning the execution was observed in
`suspended` or transitional phase at the moment of the claim, not
`runnable`. A race window between the SDK-side `deliver_signal` return
and the exec_core HSET completion would explain it, but cairn's
`has_pending_for_run` projection check gives us a deterministic signal
at the handler layer.

## Question for FF maintainers

Is there appetite for `ff_renew_lease` to accept one of these
mid-approval phases as a no-op with a distinguishable return code, so
callers can skip the full claim cycle? Concretely:

1. **Should `ff_renew_lease` tolerate `lifecycle_phase = "suspended"`
   with the historical lease triple?** A suspended execution is
   semantically "still alive, just paused" from cairn's perspective.
   Rejecting as `execution_not_active` forces callers to reclaim
   post-resume, which produces a new lease_epoch and a lease_history
   row cairn doesn't need.

2. **Or**: a new `ff_describe_lease_state` probe FCALL that returns
   `{active | suspended | resume_pending | terminal}` so callers can
   branch without guessing from the snapshot?

3. **Or**: document the resume-transition window precisely —
   exec_core is written atomically (single HSET in the Lua), so
   "transitional phase" should not be visible to another process
   between `ff_deliver_signal`'s HSET and its return. If that's the
   case, the `execution_not_eligible` symptom is a cairn-side stale
   snapshot issue and the upstream fix is a no-op.

## Cairn workaround

Until FF offers a clearer contract, cairn's orchestrate handler peeks
the cairn-side approval projection (`ApprovalReadModel::has_pending_for_run`)
before calling `renew_lease_if_stale`. If any pending approvals exist,
we skip renewal entirely — the orchestrator loop returns
`termination=waiting_approval` from the projection, without making any
FF FCALL on entry.

This closes the operator-visible 409 and adds a single SQL-row (or
HashMap) projection read on the orchestrate hot path.

## Test coverage

- `crates/cairn-app/tests/test_f57_mid_run_orchestrate.rs`:
  - `orchestrate_with_pending_approval_does_not_409`
  - `five_approval_cycles_do_not_409`
  - `fresh_run_without_pending_approvals_still_reaches_renew`

- F56 tests (`test_f56_orchestrate_after_create.rs`) continue to pin
  the cold-start path.

## Related

- F51 (#316): lease renewal for pull-mode orchestrate.
- F56 (#320): ensure_active fold to unblock cold-start 409.
- F57 (this PR): skip renew when pending approvals exist.
- FF flowfabric.lua: `ff_renew_lease` 1315-1423, `ff_issue_grant`
  3550-3650, `ff_deliver_signal` resume branch 5409-5459.
