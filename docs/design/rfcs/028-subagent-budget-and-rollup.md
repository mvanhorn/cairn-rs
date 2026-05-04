# RFC 028: Subagent Budget Reservation and Cost Rollup

Status: stub (placeholder — draft pending completion of [RFC 027 G4-G8](./027-child-run-driver.md))
Owner: TBD
Depends on: [RFC 027](./027-child-run-driver.md) (child run driver,
  G4), the subagent back-half landing as a whole.

## Purpose of this stub

RFC 027 defers three concrete subagent-spawning commitments to this
RFC:

1. **Per-child token-budget reservation at spawn time.** Today the
   parent cannot pre-commit a specific token slice to a child —
   child-run cost is bounded only by the tenant's existing
   `QuotaService` limits and the orchestrator iteration cap. Once
   RFC 027 ships, the descendants cap
   (`CAIRN_MAX_CONCURRENT_DESCENDANTS`) provides fan-out bounding,
   but no per-child-run token budget is landed on any track.
   Fine-grained parent→child budget accounting requires a schema
   change on the `runs` projection and a new event pair
   (`TokenBudgetReserved` / `TokenBudgetReleased`) that is out of
   scope for RFC 027.

2. **Per-run cost rollup for delegated spend.** Today
   `RunCostUpdated` events are keyed by `run_id` and do not
   aggregate child-run costs into the parent's per-run total.
   `SessionCostUpdated` is the aggregate surface; delegated spend
   is visible to billing at the session level but not at
   per-run-with-descendants granularity. Operators querying "how
   much did run X and its subagents cost?" have no single number.

3. **Parent→child slice accounting.** With (1) in place, the parent
   can commit a slice of its remaining budget at spawn time;
   unused slice returns on child terminal. This requires both the
   schema change in (1) and an atomicity story across the
   `SubagentSpawned` + `TokenBudgetReserved` dual-event write.

## Disposition

This RFC will be written AFTER RFC 027's G4-G8 land on main. Until
then, this file exists to give RFC 027's forward references a
concrete artifact to point at. The three commitments above are the
scope; the detailed design (event shapes, projection migrations,
query APIs) is intentionally deferred.

If post-G8 the commitments turn out to be addressable without a new
RFC (e.g. existing `SessionCostUpdated` is sufficient for every
operator use case that emerges), this stub may be closed
`wontfix` rather than fleshed out. The stub reserves the number
and records the outstanding work; it does not commit to writing a
full design.

## Non-goals

- Multi-tenant aggregate cost limits: that's session-level
  `QuotaService` territory, not a per-spawn slice concern.
- Billing reconciliation across session restarts: RFC 020 territory.
- Distributed-tracing integration with provider-side token
  accounting: out of scope for both RFC 027 and this one.
