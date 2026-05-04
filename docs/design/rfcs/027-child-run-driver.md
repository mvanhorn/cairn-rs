# RFC 027: Child Run Driver for Subagent Execution

Status: draft
Owner: orchestrator team
Issue: [#670](https://github.com/avifenesh/cairn-rs/issues/670) (G4)
Depends on:
  - [#670 G1+G2](https://github.com/avifenesh/cairn-rs/pull/671) — `SubagentSpawned` event + goal/role
  - [#670 G3](https://github.com/avifenesh/cairn-rs/pull/673) — child `RunRecord` creation at spawn
  - RFC 020 (durable recovery). Track 3 (`ToolCallResultCache` + cache consultation on recovery) is already shipped on main — verified at `crates/cairn-runtime/src/startup.rs:296-480` (`ToolCallResultCache` type + `replay_tool_result_cache` boot hook) and `crates/cairn-app/tests/test_rfc020_tool_idempotency.rs`. This RFC does NOT add a merge gate on Track 3.
  - RFC 008 (tenant/workspace/project scope)
Forward reference: [RFC 028](./028-subagent-budget-and-rollup.md) — stub — covers per-child token-budget reservation and per-run cost rollup, both deferred out of scope below.
Review history: 5 rounds of adversarial challenger review before this draft. The process surfaced multiple factual errors in earlier drafts (fabricated event fields, nonexistent event variants, misattributed code behavior) that were corrected in the final text. Intermediate drafts and per-round critiques are intentionally NOT checked into the repo — they were scratch artifacts, not durable design records. The round-by-round inflection summary near the end of this document is the durable trace of the review.

## Summary

The orchestrator can now propose `spawn_subagent`, create a child `RunRecord`, and emit an audit row (G1-G3) — but **nothing drives the child run**. Child `RunRecord`s land in state `Pending` and sit there. No orchestrator loop picks them up, no LLM call fires, the parent eventually hits its own completion gate with the child still idle.

This RFC picks how the child actually runs, and specifies the durability, safety, tenancy, resource-isolation, and cost contracts the driver must honor to compose correctly with RFC 020 and the one-binary product rule.

## Design space

- **Option A** — in-process tokio spawn per child. Dies on restart; breaks RFC-020 invariants #3/#5/#10. Dropped.
- **Option C** — separate `cairn-worker` binary. Violates the one-product-binary principle enshrined in RFC 001 and RFC 011; see also the CLAUDE.md operator note that codifies it for agents. No operator benefit at current scale. Dropped.
- **Option B** — embedded `ChildRunDriver` worker claiming via the same FF primitive the main orchestrator uses. **Chosen.**

Fuller rationale for Option A's restart-lossage (tokio task dies with the process; no cross-process recovery) and Option C's ops cost (second deploy artifact, no operator benefit at current single-instance scale) informed the choice but isn't carried here — the recommendation section below covers what survives.

## Sequencing dependency

RFC-020 Track 3 (`ToolCallResultCache` + cache consultation on recovery) is **already live on main** — verified at `crates/cairn-runtime/src/startup.rs:296` (the cache type) and `crates/cairn-app/src/main.rs:1478-1551` (the boot replay + recovery path). The sibling integration test at `crates/cairn-app/tests/test_rfc020_tool_idempotency.rs` exercises cache consultation during recovery. Child runs inherit this machinery unchanged; this RFC does NOT add a merge-blocking gate on Track 3.

**Historical note:** earlier RFC drafts added such a gate because the author had not verified Track 3's shipped state. Round-5 review corrected this.

## Contracts

### RFC-020 participation

Child runs driven by `ChildRunDriver` MUST:

1. **Participate in the two-checkpoint contract from iteration 1.** Every child-run orchestrator iteration emits the same `IntentCheckpoint` + `ResultCheckpoint` pair main-orchestrator iterations emit (RFC-020 invariant #5). No carve-out.
2. **Consult `ToolCallResultCache` on post-recovery re-drive.** When the driver re-claims a child after restart, it follows the recovery matrix for the child's last-checkpointed state exactly as the main orchestrator would (RFC-020 invariant #6). Tool calls that committed pre-restart are served from cache, not re-dispatched. Merge-blocked on Track 3.
3. **Respect sandbox-reconciliation startup ordering.** The `ChildRunDriver` tokio loop starts **after** `RecoveryService::recover_all()` completes (cairn-app boot step 4b). `main.rs` wires this as a direct synchronous `await` on `RecoveryService::recover_all(...)` (verified at `crates/cairn-app/src/main.rs:1551`) — no separate join handle, no tokio-spawn-and-await shape. PR-1b-3's boot wiring preserves this direct-await ordering.
4. **Claim filter is `state == Pending AND parent_run_id IS NOT NULL`.** Startup-ordering (#3) guarantees `RecoveryService::recover_all()` has already transitioned anything unrecoverable to `state == Failed`, which the predicate naturally excludes. No new event variants, no failure-class partition. **Context on event naming**: RFC 020's text references `RunRecovered` / `RunRecoveryFailed` events as the recovery-outcome surface, but those event variants never landed in `cairn-domain::events` (verified — grep for the type names returns zero hits in `crates/`). The shipped recovery path uses `RecoveryAttempted` / `RecoveryCompleted { recovered: bool }` (in `cairn-domain/src/events.rs`) plus the terminal `RunRecord.state` as the per-run signal. This RFC leans on `RunRecord.state`; reconciling RFC 020's text with the shipped event shape is a follow-up to RFC 020, not a G4 concern.

### Cross-tenant prevention — **contract change, not pure refactor**

Earlier RFC drafts framed removing the `project` parameter from `spawn_subagent` as a "0 LOC already-the-case" claim. That was false. Every `spawn_subagent` in the tree (`cairn-runtime/src/tasks.rs:235`, `cairn-runtime/src/runs.rs:210`, `cairn-app/src/fabric_adapter.rs:2036`) accepts `project: &ProjectKey` as a caller-controlled parameter today with no parent-row cross-check. The LLM-initiated path has direct argument control over the child's `ProjectKey`.

This is **a contract change**, not a pure refactor:

1. **`TaskService::spawn_subagent` has no default impl** — its trait method is abstract, so removing the `project` parameter rewrites the contract. Every impl + fake updates.

2. **`RunService::spawn_subagent` has a default impl** at `cairn-runtime/src/runs.rs:210-221` that calls `self.start(project, ...)` today. After the change, the default impl must `self.get(&parent_run_id).await?.project` before calling `start`. This changes observable behavior in one case: if `parent_run_id` doesn't exist, callers using the default impl see `RuntimeError::NotFound` from the parent-lookup BEFORE `start` runs, rather than whatever `start` would have returned with a bogus project.

3. **Tripwire test is a contract-proof, not a defense in depth.** PR-1a MUST include an integration test asserting, for every caller of `spawn_subagent`, `child.project == parent.project`. The test is required for PR-1a merge; it is not cosmetic.

### Lease contention

Claim routes through `issue_grant_and_claim` (`crates/cairn-fabric/src/services/run_service.rs:319`) — the same primitive the main orchestrator's HTTP `/orchestrate` handler uses. FF's primitive is atomic and lease-scoped; contenders get a deterministic winner.

**Multi-instance stance**: single cairn-app assumed (RFC-020 Gap B).

### Runtime isolation

1. **Tokio task budget.** `CAIRN_CHILD_RUN_DRIVER_CONCURRENCY` (default 4, clamp [1, 32]) concurrent child-run iterations. Backpressure at the claim path, not the tokio runtime.
2. **Separate DB connection semaphore.** `Arc<Semaphore>` with `ceil(pool_size * 0.25)` permits. HTTP handlers cannot be starved.
3. **Provider pool backpressure.** When provider pool in-use > 80%, sleep `jittered(200ms, 600ms)`, emit `child_run_driver_backpressure_total`.

### Child resource budgets

**Iteration cap.** Child inherits orchestrator default. PR-1b includes extracting `pub const DEFAULT_MAX_ITERATIONS: u32 = 20;` in `cairn-orchestrator::context` so the child-side gate references it by name rather than duplicating the magic number at `crates/cairn-orchestrator/src/context.rs:429`.

**Token budget.** Durable per-child budget reservation is deferred to [RFC 028](./028-subagent-budget-and-rollup.md). In this RFC:

- The child run's cost accrues to its own `RunCostUpdated` events, same as main runs.
- The child is bounded by: the tenant's existing `QuotaService` limits, the iteration cap, the descendants cap below, and the provider-pool backpressure.
- **Known limitation**: parent runs cannot pre-commit a specific token slice to a child. Fine-grained parent→child budget reservation is RFC 028's problem.

**Nesting control: `in_flight_descendants` counter.**

- **Schema (V069 migration on pg + sqlite + InMemory mirror)**: two new columns on the `runs` projection:
  - `in_flight_descendants BIGINT NOT NULL DEFAULT 0` — mandatory `NOT NULL DEFAULT 0` because `NULL + 1 = NULL` and `NULL < :cap` evaluates to NULL; without the default every spawn against pre-migration rows would falsely reject.
  - `root_run_id TEXT` (nullable). Backfilled as: `UPDATE runs SET root_run_id = run_id WHERE parent_run_id IS NULL` (self-reference on existing roots). Pre-migration CHILD rows intentionally leave `root_run_id = NULL` — the decrement-against-NULL path is a no-op, which is correct because pre-migration spawns never incremented any counter.
  - **Spawn-from-legacy-child case**: if a pre-V069 child run (with `root_run_id = NULL`) spawns a subagent post-V069, the spawn path must derive the true root by traversing `parent_run_id` until it finds a row with `parent_run_id IS NULL` (that row is the absolute root, which the backfill has set `root_run_id = run_id` on). The traversed root is then persisted on the new child's `root_run_id` AND backfilled onto every ancestor in the chain that still has `NULL` (one UPDATE per chain per legacy-subtree-spawn — bounded by depth, not fanout). This repairs the chain lazily so the counter works correctly for the new spawn and for every subsequent spawn under that legacy root. Integration tests: (a) pre-V069 child completing post-V069 does not decrement any counter and does not log the underflow WARN; (b) pre-V069 child spawning a new subagent post-V069 correctly attributes the counter to the absolute root, with `root_run_id` backfilled on the chain.

- **Atomic compare-and-increment (authoritative: durable backend)**:
  - pg/sqlite: `UPDATE runs SET in_flight_descendants = in_flight_descendants + 1 WHERE run_id = :root AND in_flight_descendants < :cap RETURNING in_flight_descendants`.
  - InMemory: lock-bound `i64` CAS with the same predicate.
  - If the durable UPDATE returns empty (cap hit), the adapter rejects the spawn with `SubagentFanoutLimitReached` AND rolls back any Phase-1 side effects (the child `RunRecord` row). PR-1b budget includes this rollback path.

- **Durable-vs-in-memory consistency**: the cap check is authoritative on the durable backend. In-memory projection dual-writes per RFC-025's "storage and boot" contract. If the in-memory counter drifts ahead under concurrent spawn (two concurrent spawns see in-memory counter=15 but only one wins the durable UPDATE), the loser's caller sees the rejection and the rollback triggers.

- **Counter typing**: `i64`, not `u64`. Underflow logs a WARN + `child_run_driver_descendant_underflow_total` metric, not a panic.

- **Projection idempotent-on-replay**: `SubagentSpawned` increments deduplicated via the existing `event_log.event_id UNIQUE` constraint.

- **Decrement path**: every non-root descendant's terminal event (`RunCompleted` / `RunFailed` / `RunCanceled`) decrements the root's counter. The root id is read off the terminating child's row (captured at spawn time into `root_run_id`), not re-traversed at completion. Root-cancelled-mid-subtree is handled because the decrement uses the captured root.

- **Cap**: `CAIRN_MAX_CONCURRENT_DESCENDANTS`, default 16, clamp [1, 256]. Per-root, not per-tenant. Per-tenant aggregate is RFC 028.

### Orphan-child on partial-spawn failure

`FabricTaskServiceAdapter::spawn_subagent` Phase-1 (create child `RunRecord`) can succeed while Phase-2 (submit task) fails, leaking an orphan child. Earlier drafts proposed extending `RecoveryService::recover_all()` with a `Pending` sweep — but `recovery_impl.rs:524-526` explicitly excludes `Pending` from recovery by design ("a run that never transitioned out of Pending has no side-effects to reassert"). RFC 020 rejected this by design; this RFC honors that.

**Hot-path defense**:

1. On Phase-2 failure, the adapter synchronously calls `RunService::fail(child_run_id, FailureClass::OrphanChild)` before returning the error. The `Failed` terminal fires the standard descendant-counter decrement path (the terminating child's row carries `root_run_id`, decrement targets the root), so the cap-counter is released along with the run.

2. If that compensating fail itself fails (event-log append race, connection drop), the adapter:
   - logs ERROR + `child_run_driver_orphan_fail_failed_total` metric;
   - **compensating counter-decrement**: directly calls the same atomic-decrement primitive used on the normal terminal path against the captured `root_run_id`. This releases the counter slot even without the `Failed` terminal event firing. If the decrement itself fails (highly unlikely — it's the same SQL used on every child completion), the adapter logs a second ERROR + `child_run_driver_orphan_counter_leak_total` metric. Counter drift accumulates under compounded failure; operators see it in metrics, and the underflow-on-next-legit-decrement path is already tolerated per the `i64`-typed WARN rather than panic.
   - returns the original Phase-2 error. The child stays `Pending` with no task. No task row, no sandbox, no provider call — bounded leak. An operator ticket is expected.

3. **Operator endpoint** `POST /v1/admin/tenants/:tenant_id/runs/:id/cancel-orphan` lets operators manually transition an orphaned `Pending` child to `Failed(OrphanChild)`. The endpoint is **tenant-scoped** per the RFC-026 admin-surface convention (`docs/design/rfcs/026-admin-surface.md:33-42`); middleware extracts `tenant_scope`, handler verifies `run.tenant == scope`. Not automatic, not a sweep — operator intervention only.

4. **`FailureClass::OrphanChild` new variant.** Blast radius ~120 LOC across: `cairn-domain/src/lifecycle.rs` variant; `cairn-store` projection appliers (in-memory + pg + sqlite match-arm exhaustiveness); `cairn-app` handler summaries + telemetry aggregations; `ui/src/lib/types.ts` TypeScript mirror; `openapi_spec.rs` API spec update; parity harness + serde test updates.

### Cost-tracking

1. **Per-child `RunCostUpdated`**: child-run provider calls post costs against the child's `run_id`, same as main runs. Existing mechanism; no change.

2. **No parent-aggregate rollup in this RFC.** The parent run's `RunCostUpdated` events reflect only the parent's own provider calls, not delegated spend. Matches the current behavior for child runs created via `POST /v1/runs/:id/spawn` (operator path).

3. **Session-level aggregate is the unified billing surface.** `SessionCostUpdated` in `crates/cairn-domain/src/events.rs` (cite the type name rather than line number — line numbers drift) aggregates all runs in the session, including children (child runs share the parent's session). Billing alerts fire against the session total. Delegated spend is visible at the billing layer.

4. **Known limitation (tracked in RFC 028)**: operators querying "how much did run X (and its delegated subagents) cost?" have no single number. Workaround: sum session total or aggregate `RunCostUpdated` across `list_by_parent_run`-transitive (O(depth), tolerable for UI).

5. **Commercial budget enforcement is intact.** If a tenant's hourly cost quota is hit mid-delegated-run, the child's next provider call is rejected by `QuotaService` just like a main run. There is no path by which delegated spend escapes quota enforcement — it just escapes per-run attribution.

### Waitpoint key shape

G5's parent-suspend path uses `child_task_id` as the waitpoint key, matching `suspend_for_subagent` at `cairn-fabric/src/worker_sdk.rs:430`.

## Implementation plan

The plan splits the work into 9 PRs across G4-G8. PR-1b was originally a single 2250-LOC landing; round-5 review established that size is un-reviewable and split it into 5 sub-PRs, each with standalone correctness value.

### G4 — this RFC

#### PR-1a — Cross-tenant contract change

**~500 LOC.** Pure signature work + tripwire test. Ships standalone; unblocks PR-1b-*.

- Remove `project: &ProjectKey` from `TaskService::spawn_subagent` (abstract), `RunService::spawn_subagent` (default impl), and `FabricTaskServiceAdapter::spawn_subagent`.
- Adapter impls derive `project` via `self.runs.get(&parent_run_id)?.project`.
- `RunService::spawn_subagent` default impl gains a `self.get(parent_run_id).await?` hop; error shape for parent-not-found in default-impl callers changes (documented + tested).
- `execute_impl.rs` `SpawnSubagent` branch drops `ctx.project`.
- HTTP handler at `lifecycle.rs:836-886` retains tenant-scope auth pre-read; stops forwarding `&parent_run.project`.
- Update ~12 test fakes across `cairn-runtime/tests`, `cairn-app/tests`, `cairn-orchestrator/tests`. Each fake's stubbed `get()` returns a determinate project so the new default-impl hop has something to find.
- **Contract-proof test (mandatory for merge)**: integration test asserting `child.project == parent.project` byte-for-byte on the LLM-initiated path.

#### PR-1b-1 — `in_flight_descendants` + `root_run_id` schema

**~400 LOC.** Standalone correctness; ships before the driver exists.

- V069 migration (pg + sqlite): add both columns with the `NOT NULL DEFAULT 0` / nullable specs above, plus the one-time backfill `UPDATE runs SET root_run_id = run_id WHERE parent_run_id IS NULL`.
- InMemory projection mirror.
- Atomic C&I helper on all three backends.
- Parity test + idempotent-on-replay test.
- Pre-V069-child decrement-against-NULL no-op test.

#### PR-1b-2 — `FailureClass::OrphanChild` + operator endpoint

**~200 LOC.** Ships the failure-class cascade + the operator-recovery path without the driver.

- Variant addition + 74-site exhaustiveness cascade (Rust compiler catches most; UI + OpenAPI need explicit updates).
- `POST /v1/admin/tenants/:tenant_id/runs/:id/cancel-orphan` handler with tenant-scope middleware.
- OpenAPI + TypeScript mirror.

#### PR-1b-3 — Driver scaffolding

**~700 LOC.** The tokio loop + claim + boot wiring. Ships behind `CAIRN_CHILD_RUN_DRIVER_ENABLED=false` (default false). Driver does not claim anything until PR-1b-5 flips the flag.

- Driver tokio loop + `issue_grant_and_claim` integration.
- Lease renewal + re-claim on boot.
- Boot wiring in `main.rs`: explicit await on `recovery_join_handle`.
- Runtime isolation semaphores.
- Metrics + Prometheus.
- Recovery claim predicate (trivial — `state = Pending AND parent_run_id IS NOT NULL`).
- Orphan-child synchronous fail on Phase-2 failure.
- Rollback-on-durable-rejection for `in_flight_descendants` cap races.
- `DEFAULT_MAX_ITERATIONS` constant extract.

#### PR-1b-4 — Integration + SIGKILL tests

**~700 LOC.** Three SIGKILL scenarios, not one.

- **Happy-path SIGKILL (~300 LOC)**: parent spawns child, child claims and runs one iteration, cairn-app SIGKILLs mid-iteration, driver re-claims on boot, child completes. RFC-020 Track 1 equivalent for child runs.
- **Orphan-hot-path SIGKILL (~200 LOC)**: parent spawns child (Phase 1 child row created), cairn-app SIGKILLs mid-Phase-2 (between `RunService::start` and task-submit), child row sits `Pending` forever, operator invokes cancel-orphan endpoint, child transitions to `Failed(OrphanChild)`.
- **Fanout-cap-race SIGKILL (~200 LOC)**: parent spawns at the cap boundary, SIGKILL fires mid-increment, durable counter is authoritative, boot re-claim respects the cap.
- Plus end-to-end happy-path integration (LiveHarness with mock LLM + LLM-initiated spawn + parent observes child completion).

#### PR-1b-5 — Flip feature flag

**~50 LOC.** Default flipped `CAIRN_CHILD_RUN_DRIVER_ENABLED=true`. Merge-gated on the Track-3 verification gate (see §Sequencing). This is the PR that actually turns on subagent execution in production.

### G5 (PR 2) — Parent suspend on `subagent_waitpoint`

Parent run's execute phase, on `ActionStatus::SubagentSpawned`, calls `suspend_for_subagent` with the `child_task_id` key. Child's `RunCompleted` emit (from PR-1b-3) fires a FF signal to the parent's waitpoint. Parent resumes, observes child's final state via `step_history`, continues. **~400 LOC + 200 LOC test.** Depends on PR-1b-5.

### G6 (PR 3) — `agent_role_id` threading

`RunService::start_with_role(session, run_id, parent, role)` — **no `project` parameter** (cross-tenant contract from PR-1a propagates). Child run's orchestrator loop reads role → picks the delegated prompt. **~250 LOC + 150 LOC test.**

### G7 (PR 4) — Child output → parent `step_history`

On child `RunCompleted`, extract `completion_summary`, attach to parent's `step_history` with `action_kind="subagent_complete"`. **~200 LOC + 150 LOC test.**

### G8 (PR 5) — Delete `cairn-agent::subagents`

Cleanup once G3-G7 ship. **~200 LOC deletion.**

## Non-goals (referenced by RFC 028)

Explicitly NOT in this RFC's scope. RFC 028 addresses them:

1. Per-child token-budget reservation at spawn time.
2. Per-run cost rollup for delegated spend.
3. Parent→child budget slice accounting.
4. Per-tenant aggregate descendant/spend caps.

## Review history

This RFC went through 5 rounds of adversarial challenger review before being filed. Key inflection points:

- **Round 1 strongest**: original draft was silent on RFC-020 participation — "Option B saves the child's existence but not its state." Fixed by naming the 4-point participation contract.
- **Round 2 strongest**: contract #4 referenced non-existent `RunRecovered`/`RunRecoveryFailed` events. Fixed by dropping to `state == Pending` claim predicate + startup ordering.
- **Round 3 strongest**: "cross-tenant already-the-case, 0 LOC" was a fabrication — `spawn_subagent` signatures actually take `project` today. Fixed by scoping the true ~500 LOC contract change as PR-1a.
- **Round 4 strongest**: token-budget "`tool_args` on `SubagentSpawned`" was another fabrication — the event has no `tool_args` field. Fixed by deferring budget mechanism to RFC 028 entirely.
- **Round 5 verdict**: REVISE-THEN-MERGE. Two blockers (RFC 028 stub + PR-1a "pure refactor" framing) + 4 should-fix (reviewability split, admin endpoint scope, `root_run_id` backfill, SIGKILL matrix). All addressed in this final draft.
