# FF upstream ask — read-only `execution_phase` probe with valid-operation bitmask

**Filed by:** cairn-rs (F58, 2026-04-26)
**Relationship:** extends the narrower ask in [`ff-renew-lease-mid-approval.md`](ff-renew-lease-mid-approval.md) and is complementary to the pull-mode proposal in [`ff-lease-renewal-pull-mode.md`](ff-lease-renewal-pull-mode.md).

## Use case

Cairn's HTTP `/v1/runs/:id/orchestrate` handler must decide, on every call,
whether to refresh the run's FF lease before entering the orchestrator loop.
`renew_lease_if_stale` is actually two code paths, each with its own phase
precondition — and cairn cannot tell from the outside which one will run:

* **Direct lease refresh (`ff_renew_lease`)**: accepted while the
  execution is `active`; rejects suspended/transitional phases —
  mid-approval (`waiting_approval`), resume-in-flight (`resuming`) —
  with `execution_not_active`. This is the precondition the narrower
  [`ff-renew-lease-mid-approval.md`](ff-renew-lease-mid-approval.md) ask
  targets.
* **Reclaim fallback via the grant gate (`ff_issue_grant` +
  `ff_claim_resumed_execution`)**: stricter — requires
  `lifecycle_phase == "runnable"`. Non-runnable phases reject with
  `execution_not_eligible`. Tool-invocation aftermath (briefly `running`)
  and signal-delivery (briefly `signalling`) land here because the
  lease has already expired by the time cairn re-enters and we skip
  straight to the reclaim branch.

Cairn has no way, today, to check the current `lifecycle_phase` before
calling. The sequence we keep hitting (the `execution_not_eligible`
shape that triggered F58):

1. Operator calls `/orchestrate` → cairn calls `renew_lease_if_stale` →
   lease is present, path is direct renew, FF accepts (execution is
   `active`) → handler enters the loop → loop proposes a tool call → FF
   records the invocation, moves the execution's phase off `runnable`
   briefly → handler returns 202.
2. Operator approves → calls `/orchestrate` again → lease has expired
   meanwhile, so `renew_lease_if_stale` takes the reclaim fallback →
   **phase has not flipped back to `runnable` yet** → FF rejects
   `execution_not_eligible` → cairn surfaces 409 to the operator.

The narrower `execution_not_active` shape (direct-renew mid-approval)
is the companion ask in `ff-renew-lease-mid-approval.md`. Both shapes
land in the same operator-visible defect — and both would be solved by
the probe this doc proposes.

The existing lease is valid; the execution is not terminal; the correct
cairn-side action is to proceed into the loop with the lease we already
have. Cairn currently papers over this with a string-match on the error
code (F58 PR), but that is a workaround, not a solution: we are inferring
phase from a failure mode.

## Observed customer-visible symptom

Dogfood M1-v2 attempt (2026-04-26, binary `33836b03`):

```
c1 orchestrate → bash proposed (mkdir), 202
c2 approve + orchestrate → bash proposed (ls grep), 202
c3 approve + orchestrate → bash proposed (ls -la), 202
c4 approve + orchestrate → write Cargo.toml proposed, 202
c5 approve + orchestrate → 409 execution_not_eligible
```

The operator followed the contract exactly. The 409 is spurious.

## Ask

Expose a read-only FF FCALL (or a field on `describe_execution`) that
reports:

1. **Current `lifecycle_phase`** — the canonical string FF itself uses
   (`runnable`, `waiting_approval`, `resuming`, `running`, `signalling`,
   `terminal`, etc.). One value, no inference needed.
2. **Bitmask or set of valid operations at this phase** — which of
   `{renew, complete, fail, cancel, suspend, resume, signal, …}` FF would
   accept *right now* without a state-conflict rejection. Saves each
   consumer from reconstructing the transition table out-of-band and
   getting it wrong.
3. **Phase-epoch or version** — a monotonic counter or timestamp that
   changes every time `lifecycle_phase` transitions. Lets callers
   implement optimistic "probe + act" without a TOCTOU hole: if the epoch
   moved between probe and action, the action's failure is explicit, not
   a confusing `execution_not_eligible`.

Naming suggestion: `ff_describe_execution_phase` or extend the existing
`describe_execution` return shape with a `phase_gate` sub-struct.

## Why this is the right shape

- **Pull-mode renewal** (previous ask) solves part of this but not all of
  it: it moves the renew from "cairn pushes" to "FF pulls", which
  side-steps the renew-during-wrong-phase race for renew specifically. The
  probe proposal is broader: the same TOCTOU hits `suspend`, `resume`,
  `complete` individually today (cairn has a `is_suspend_state_conflict`
  classifier for exactly this, which is the symptom of needing a probe).
- **Any consumer** of FF (not just cairn) benefits. FF's operational
  contract today is "try the FCALL, pattern-match the error code." That
  couples callers to the flowfabric.lua error taxonomy by necessity.
  A probe decouples them.
- **Error-quality improvement for operators.** A cairn handler that can
  say "this execution is currently in `waiting_approval`, the operations
  you can perform are `{cancel, signal}`" is strictly better operator UX
  than a 409 with a raw FF code.

## Non-goals

- This is not a subscription / push API. A single-shot read-only FCALL is
  sufficient.
- This does not replace FF's internal state gating. FCALLs still validate.
  The probe is advisory — callers may still see a rejection if the phase
  moves between probe and action; the epoch field makes that case
  diagnosable.

## What cairn does in the meantime

PR "F58 — tolerate renew NotEligible" classifies `execution_not_eligible`
and `execution_not_eligible_for_attempt` as transient on the orchestrate
entry path, logs at WARN, and falls through into the loop with the
existing lease. The loop's own `is_lease_healthy()` gate protects against
an actually-dead lease. This is narrow (only two FF codes, only on the
`renew_lease_if_stale` call site, only when cairn already holds a lease
it believes is live) but it is inference-from-failure, not a principled
check. The probe retires this workaround.

## References

- flowfabric.lua lines 3585–3590 — grant gate's `lifecycle_phase ==
  "runnable"` precondition
- cairn-rs `crates/cairn-app/src/fabric_adapter.rs` —
  `is_claim_contention`, `is_suspend_state_conflict`,
  `is_terminal_state_conflict` (three classifiers that exist only because
  there is no probe)
- cairn-rs `crates/cairn-runtime/src/error.rs` —
  `RuntimeError::is_transient_phase_conflict` (F58 workaround)
- cairn-rs `crates/cairn-app/src/handlers/runs.rs` — orchestrate entry's
  renew call site; F57 pending-approvals skip and F58 tolerate branch
