# RFC-025 — RuntimeAggregate backend abstraction

## Status

**Phase 4 IMPLEMENTED — 2026-04-29. RFC-025 Done.**

Ten milestones are live on `main` (Phase 0 is infrastructure; the
nine service-migration phases build on it):

| Phase  | Landed          | PR    | Summary                                                                                         |
| ------ | --------------- | ----- | ----------------------------------------------------------------------------------------------- |
| 0      | 2026-04-28      | #549  | Projection registry + build.rs exhaustiveness + pre-commit/CI `log_stub` guard + parity harness |
| 1      | 2026-04-28      | #559  | Eval event-sourcing (add `EvalRunScored` + `EvalRubricScored`) + pg/sqlite projections + delete `replay_evals` |
| 1.5a   | 2026-04-29      | #569  | Triggers full-projection-backed (Option B); `replay_triggers` deleted                           |
| 1.5b   | 2026-04-28      | #566  | Graph read-model declared Ephemeral; `replay_graph` deleted                                     |
| 2a.1   | 2026-04-29      | #565  | Fill governance projections — credentials + quotas + budgets + licenses                          |
| 2a.2   | 2026-04-29      | #571  | Remaining governance — delegations + guardrails + retention + entitlements + license overrides |
| 2b.1   | 2026-04-29      | #573  | Add events + projections for audits + scheduled_tasks + outcomes + plan_reviews                  |
| 2b.2a  | 2026-04-29      | #580  | external_workers projection + restart durability                                                 |
| 3      | 2026-04-29      | #572  | Provider bindings + connections → Projected; pools + health stay Ephemeral                      |
| 4      | 2026-04-29      | #585  | Rename `InMemoryServices` → `RuntimeServices` + doc cleanup (this PR)                           |

**Final projection registry counts** (as of Phase 4 landing, tracked in `crates/cairn-store/src/projection_registry.rs`):

- **95 Projected** — every lifecycle-carrying variant writes to a named
  read-model table inside the event-append transaction (pg / sqlite /
  in-memory byte-equal per the parity harness).
- **18 Ephemeral** — observability-only, policy-scoped, or owned by a
  downstream projection (graph, signal routing, FF lease history).
  Documented in-registry with a `reason` string.
- **45 Stubbed** — tracked for future phases outside RFC-025's scope.
  The remaining set is Phase 2b.2b (channels + route policy Updated +
  checkpoint strategy + subagent spawning + misc operator events), covered
  by a separate follow-up issue (#574) and held off this RFC because the
  parity story for each group requires dedicated design. The boot guard
  surfaces the list at WARN on pg/sqlite so no operator is silently
  reading empty projection tables.

**Boot + read-path semantics after Phase 4 (honest accounting).** The
three hand-rolled cairn-side walkers that motivated the RFC —
`replay_evals`, `replay_graph`, `replay_triggers` — are deleted. Every
`Projected` variant now lands in a read-model table in the configured
backend inside the event-append transaction (pg/sqlite/in-memory
byte-equal per the parity harness). Restart-durability integration
tests cover evals, triggers, templates, governance, audits, scheduled
tasks, outcomes, plan reviews, external workers, provider bindings +
connections.

**What Phase 4 does NOT yet deliver** (and what RFC-025 does NOT
unilaterally claim):

- **Boot is not O(1) in the production binary yet.** `main.rs` still
  runs a `Startup replay from durable event log` block that reads pg
  or sqlite in 10k-event batches and replays them through the
  `InMemoryStore::append` path on every restart. This warms the
  in-memory projections because the handler hot-paths today still
  read from the `InMemoryStore`-backed `*ServiceImpl<InMemoryStore>`
  fields, not directly from the pg/sqlite read-model tables. The
  durable projection tables exist and pass the parity harness, but
  cutting handler reads over to them — and deleting the startup
  replay — is a follow-up refactor outside RFC-025's scope. The
  per-domain `replay_*` walker deletion still matters: it removed
  the domain-specific hand-rolled projection logic that duplicated
  the `SyncProjection` contract and drifted from it.
- **Writes still route through `InMemoryStore` first.** `main.rs` calls
  `InMemoryStore::set_secondary_log(pg_or_sqlite_event_log)` so every
  service-layer append lands in the InMemory projection first and
  then dual-writes to the durable backend. Pg + sqlite therefore see
  a strict superset of what the in-memory projection saw, which is
  why the parity harness can compare in-memory ↔ sqlite byte-equal.
  A future refactor that promotes pg/sqlite to the primary writer
  would drop the secondary-log indirection.
- **`RuntimeServices` fields are still concretely `*ServiceImpl<InMemoryStore>`.**
  The rename is semantic (the aggregate is no longer named after one
  specific storage engine) but the backend-generic `RuntimeServices<S: Store>`
  that the original RFC §"Core shape" sketched is still future work.

What Phase 4 DOES deliver:

1. Naming honesty — `RuntimeServices` is correct for the direction of
   travel, and distinguishes the aggregate from `cairn-store`'s
   `InMemoryStore` and `cairn-memory`'s `InMemoryServices` type alias.
2. A complete projection registry (Projected / Ephemeral / Stubbed)
   with compile-time exhaustiveness + boot-time assertion + pre-commit
   + CI `log_stub` guard.
3. Durable pg/sqlite read-model tables for every `Projected` variant —
   the substrate on which the "pg/sqlite reads are primary" follow-up
   will land without further schema churn.
4. Deletion of the three ad-hoc `replay_*` walkers that were not
   backed by the `SyncProjection` framework.

**Closes** #434 (critical), #435, #436, #437 (high) — the four
original issues — and stands up the primitive that later phases can
extend if additional ephemeral-today services become durable.

Authored via iterated proposer/challenger debate (3 rounds). Final verdict:
**ACCEPTED**. Open questions resolved 2026-04-28:

- **Silent-read mechanism:** registry (compile-time exhaustive via
  `build.rs`) + pre-commit / CI `log_stub` grep. The original debate
  mentioned `#[derive(ProjectionRegistry)]` as one candidate; Phase 0
  shipped a static const-array registry + a `build.rs` script that
  parses `cairn-domain/src/events.rs` against the registry — simpler
  than a new proc-macro crate, same exhaustiveness guarantee.
- **Sprint cadence:** full 7-phase plan (shipped as 10 PRs after
  Phase 2a + Phase 2b were subdivided into 2a.1/2a.2 and 2b.1/2b.2a
  per the risk-mitigation clause in each phase).
- **Provider split (bindings vs connections):** research doc landed
  alongside Phase 0 at `RFC-025-provider-boundary-research.md`.
  Both `provider_bindings` and `provider_connections` ship Projected
  in Phase 3; `provider_pools` + `provider_health` remain Ephemeral.

**Phase 0 shipped (PR #549):**
- Projection registry at `crates/cairn-store/src/projection_registry.rs`
  with all `RuntimeEvent` variants classified (see "Final projection
  registry counts" above for current 95 / 18 / 45).
- Compile-time exhaustiveness via `crates/cairn-store/build.rs`
  (parses `cairn-domain/src/events.rs` and rejects the build when any
  variant drifts in or out of the registry).
- Pre-commit hook (`.githooks/pre-commit`) rejects new `log_stub(` sites
  in pg/sqlite projections; CI mirror job `projection-stub-guard` in
  `.github/workflows/ci.yml` runs the same check on every PR.
- `scripts/install-hooks.sh` points `git config core.hooksPath` at
  `.githooks/` so hooks update with `git pull`.
- Parity harness at `crates/cairn-store/tests/projection_parity.rs`
  exercising InMemory ↔ SQLite. Extended variant-by-variant by every
  subsequent phase's migration so the harness always matches the live
  projected set.
- `AppState::new_with_runtime` calls
  `assert_no_stubs_for_persistent_backend` at boot and logs the
  Stubbed variant list at WARN. The Phase 2c flip to a hard boot
  failure is descoped — the remaining Stubbed set is tracked by
  dedicated follow-ups (#574) rather than blocked on RFC-025 closure.

**Phase 4 shipped (this PR):**
- `cairn_runtime::InMemoryServices` renamed to
  `cairn_runtime::RuntimeServices`. The name was the RFC-025 motivating
  misnomer — the aggregate is not in-memory-only.
- `cairn_memory::InMemoryServices` is a separate type-alias for the
  memory-pipeline-on-in-memory-backends bundle. The name there is
  genuinely accurate and is left alone.
- `cairn_store::InMemoryStore` (the actual in-memory event-log backend)
  is untouched; that name is correct.
- Operator-facing + developer-guide docs refreshed to use the new
  name and to describe the post-RFC-025 storage contract honestly:
  95 Projected event variants now write to durable pg/sqlite
  read-model tables, the three hand-rolled walkers are gone, and
  the startup in-memory-warm-up replay is explicitly flagged as
  remaining (targeted by a follow-up refactor).

## Summary

Historically (pre-RFC-025), the runtime service aggregate — then named
`InMemoryServices` at `crates/cairn-runtime/src/aggregate.rs:40` —
hard-coded `Arc<InMemoryStore>` for ~30 non-execution services
(approvals, evals, credentials, quotas, provider bindings, etc.),
regardless of whether the operator configured Postgres, SQLite, or
`--db memory`. Write paths persisted to pg/sqlite correctly; read
paths always went through an in-memory materialization rebuilt by
O(N) replay of the full event log on every boot. Additionally, the
eval subsystem mutated state in four handlers without emitting
events, causing silent data loss on restart (#435, #337 class).

This RFC moved the affected services off the three hand-rolled
boot walkers onto durable `SyncProjection` read-models, phased the
work across 10 PRs (delivered against an original 7-phase plan, with
Phase 2a + Phase 2b subdivided after audit), and preserved
`--db memory` as a first-class runtime mode. Every `Projected` event
variant now writes to a named pg/sqlite read-model table inside the
event-append transaction (95 Projected / 18 Ephemeral / 45 Stubbed in
the registry as of Phase 4 close). The three hand-rolled walkers
(`replay_evals`, `replay_graph`, `replay_triggers`) are deleted and
the aggregate is renamed `RuntimeServices` to match.

What this RFC **did not** promise and Phase 4 **does not claim**: an
O(1) production boot today. `main.rs` still runs a full-log replay
into the in-memory projections on pg/sqlite startup so handler
reads (which still go through `*ServiceImpl<InMemoryStore>`) see a
warm state on restart. Cutting handler reads over to the durable
projection tables and removing the startup replay is a follow-up
refactor that RFC-025's infrastructure enables — see the "Boot +
read-path semantics" block above for the explicit accounting.

## Motivation

### Issues (verbatim)

**#434 [critical]** — *"RuntimeAggregate hard-codes InMemoryStore for ~30 services"*
> The `InMemoryServices` struct declares ~30 services as concrete `*ServiceImpl<InMemoryStore>` … Regardless of whether the operator configures `DATABASE_URL=postgres` or `--db memory`, the runtime service aggregate always materializes projections INTO an in-memory store and rebuilds them by replaying the full event log on every boot. … Every new backend pays a full-log-replay tax on every restart.

**#435 [high]** — *"Eval score / start / complete handlers mutate state without emitting events"*
> Four handlers write directly to `in-memory state.evals` without appending a `RuntimeEvent`: `start_eval_run_handler`, `complete_eval_run_handler`, `score_eval_run_handler`, `score_eval_rubric_handler`. … *"metrics recorded via `/v1/evals/runs/:id/score` are NOT yet in the event log, so they will not be visible after a restart."*

**#436 [high]** — *"EvalRunArchived projection is a no-op in pg/sqlite but real in in-memory"*
> pg and sqlite projection writers handle `EvalRunArchived` with `log_stub` (no-op) while the in-memory writer updates `archived_at`. … the asymmetric projection is a latent trap: any future pg-backed `EvalRunReadModel` reading from a table will show archived runs as live because no migration wrote them.

**#437 [high]** — *"`replay_evals` is a parallel O(N) event-log walker at boot instead of a SyncProjection"*
> `AppState::replay_evals()` reads the entire event log every boot and rebuilds `state.evals`. … hand-rolled projection outside the `SyncProjection` framework — two projection paths for one domain, only one backend-agnostic.

### Code evidence (verified 2026-04-28)

- `crates/cairn-runtime/src/aggregate.rs:40-151` — declares 30+ services as `*ServiceImpl<InMemoryStore>`.
- `crates/cairn-store/src/pg/event_log.rs:77-86` — sync projection already fires inside the same `&mut tx` that inserted the event. Atomicity exists today.
- `crates/cairn-store/src/pg/projections.rs` — 80 `log_stub` call sites (no-op projections).
- `crates/cairn-store/src/sqlite/projections.rs` — 101 `log_stub` call sites.
- `crates/cairn-domain/src/events.rs:189-191, 2000-2018` — `CredentialStored`, `CredentialKeyRotated`, `CredentialRevoked` already defined.
- `crates/cairn-domain/src/events.rs:139-143` — `EvalRunStarted`, `EvalRunCompleted`, `EvalRunArchived` defined. `EvalRunScored` and `EvalRubricScored` **do not exist** — must be added.
- `crates/cairn-evals/src/services/eval_service.rs:74, 132, 151, 427` — `create_run`, `start_run`, `complete_run`, `score_with_rubric`. Only `create_run` and `archive_run` emit events; four mutation points do not.
- `crates/cairn-app/src/state.rs:713, 736, 856` — `replay_graph`, `replay_evals`, `replay_triggers` are hand-rolled O(N) walkers.
- `crates/cairn-app/src/main.rs:1478-1480` — all three invoked on boot.

### Why now

The 2026-04-28 multi-agent audit surfaced this as a four-issue cluster. Deferring leaves pg/sqlite as partially-working backends indefinitely: writes land durably, but the read path reconstructs from a full replay on every restart, and one whole domain (evals scoring) silently drops data on restart. The pattern is already baked into 27 services; each new service compounds the cost.

## Non-goals

- **Not removing `--db memory`.** It remains a first-class runtime mode, documented in CLAUDE.md, exercised by smoke tests, and used for local dev.
- **Not introducing a new storage engine.** SurrealDB (#21 in cairn-rs) is explicitly out of scope; this RFC concerns the abstraction layer, not the set of backends.
- **Not changing the event model or wire format.** Two new event variants (`EvalRunScored`, `EvalRubricScored`) are added; no breaking changes. No SSE contract changes.
- **Not touching FF-owned recovery paths** (lease expiry scanners, etc.). Those are owned by FlowFabric per RFC-020.
- **Not introducing API churn on `EventLog::append`.** The atomicity mechanism stays internal to the store implementation.

## Proposal

### Core shape

Replace `InMemoryServices` with `RuntimeServices<S: Store>` where `S` is the configured backend (Postgres, SQLite, or InMemory). The services are already generic over `S` internally (`EvalRunServiceImpl<S>`, etc.); the migration is largely a rename and type-parameter plumbing pass in the aggregate + boot code.

```text
InMemoryServices { store: Arc<InMemoryStore>, eval_runs: EvalRunServiceImpl<InMemoryStore>, ... }
       ↓
RuntimeServices<S> { store: Arc<S>, eval_runs: EvalRunServiceImpl<S>, ... }
```

cairn-app picks the backend once at boot (existing `--db` / `DATABASE_URL` logic) and threads `Arc<S>` through the aggregate. No handler-level changes; the trait-method surface is unchanged.

### Projected vs ephemeral services

Services split into two classes.

> **Note on granularity.** The lists below describe the target
> *service-level* classification (end-state of Phase 4). The Phase 0
> registry in `crates/cairn-store/src/projection_registry.rs` classifies
> at a finer *event-level* granularity. Some services below list
> `Projected` as a whole but contain individual events that are classed
> `Ephemeral` at the event level because they are observability-only
> (e.g. `RunSlaBreached`, `RunSlaSet`, `ApprovalPolicyCreated`,
> `PromptRolloutStarted`, the `Trigger*` audit events). The registry is
> the source of truth for the day-to-day pg/sqlite contract; the
> service lists below describe where the operator-facing service
> boundary lives once all Phase 2a/2b projections ship.

**Projected services** (backed by one or more SyncProjection read-model
tables — pg/sqlite/in-memory byte-equal; the abstraction's
end-state target is O(1) boot, with the startup-replay removal tracked
as a post-Phase-4 follow-up):

- Approvals, approval_policies, checkpoints, tool_call_approvals
- Prompt_assets, prompt_releases, prompt_versions
- Ingest_jobs, eval_runs, mailbox
- Signals, signal_router, channels
- Observability (record-writing events; observability-only events stay
  Ephemeral at the event level)
- Provider_bindings (the binding record itself)
- Credentials, defaults, licenses, guardrails, quotas, retention,
  route_policies, run_cost_alerts, budgets — **run_sla is operator
  observability** (Ephemeral at the event level; no durable table)
- Notifications, operator_profiles, workspace_memberships, audits
- Tenants, workspaces, projects

**Ephemeral services** (per-process runtime caches; not subject to
restart durability; documented as such):

- Provider_connections (live HTTP/gRPC connection pools)
- Provider_pools (pooling state)
- Provider_health (health-probe in-flight state)
- Provider_registry (derived from provider_bindings at boot)
- External_workers (tracked in FF; cairn-side registration is
  projected, live heartbeat is ephemeral)

The split is declared at the service-type level and documented in a
single table in `docs/design/runtime-services.md` (post-Phase-0 TODO).
Provider_bindings (projected) and provider_connections (ephemeral) are
the split point that caused round-1 ambiguity; this RFC resolves it
explicitly.

### Sync projections are already atomic

Round-2 challenger conceded on atomicity after proposer cited `pg/event_log.rs:77-86`. The insert and projection run in the same `&mut tx`; the store never commits a position that hasn't been projected. No API change to `EventLog::append` is needed.

What the round-3 exchange agreed is missing is a **compile-time exhaustiveness registry** so no new event type can slip through without a projection decision. Each `RuntimeEvent` variant must be enumerated in a central `PROJECTION_REGISTRY` (a `match` in the projection trait implementation that the compiler forces to be exhaustive today — this RFC makes that implicit property explicit via a central registry type that lists every variant and declares its projection status: `Projected(table_name)`, `Ephemeral`, or `Stubbed(tracking_issue)`).

### Silent-read protection

The risk that prompted this RFC: a `log_stub` projection in pg/sqlite
combined with a handler reading from a table that was never populated
produces silently-empty reads (no error, no warning).

Phase 0 ships defense-in-depth: a registry + build-script exhaustiveness
check backed by a pre-commit / CI `log_stub` grep. The concrete
mechanism:

- Every `RuntimeEvent` variant has a `ProjectionEntry` in
  `crates/cairn-store/src/projection_registry.rs` with one of three
  statuses: `Projected { table }`, `Stubbed { tracking }`, or
  `Ephemeral { reason }`.
- `crates/cairn-store/build.rs` parses the `RuntimeEvent` enum out of
  `cairn-domain/src/events.rs` and rejects the build if the two sets
  differ in either direction (missing-from-registry or orphaned-
  registry-entry). Early-design considered a `#[derive(ProjectionRegistry)]`
  proc-macro for the same guarantee; the `build.rs` approach was
  shipped because it reaches the same invariant without introducing a
  `cairn-store-derive` crate.
- On boot, `AppState::new_with_runtime` calls
  `assert_no_stubs_for_persistent_backend` on pg/sqlite and logs the
  stubbed variant list at WARN. Phase 0 is infrastructure-only so the
  severity is WARN, not fatal — Phase 2c flips to a hard boot failure
  once the stub set is empty. `--db memory` is permitted to boot with
  stubbed projections because it never reads through the projection
  layer.
- `.githooks/pre-commit` and the `projection-stub-guard` CI job reject
  any PR that adds a new `log_stub(` site to
  `crates/cairn-store/src/{pg,sqlite}/projections.rs`. This catches
  the reverse case: a new variant that slips into pg/sqlite with a
  silent-no-op projection *and* a matching registry entry.
- The parity harness at `crates/cairn-store/tests/projection_parity.rs`
  enforces byte-equality at the read level across InMemory ↔ SQLite
  for a representative set of Projected variants; each Phase 2a/2b
  migration adds its variants to the harness as part of the same PR.

Open question closed 2026-04-28: user picked (a) + (c) from the
round-3 mitigation menu — registry-level fail-fast + pre-commit/CI
grep. See **Open Questions — RESOLVED** below for the full history.

## Phased implementation

7 phases, ~8500 LOC. Each phase is one PR. Dependencies are strict: no phase runs in parallel with its predecessor.

### Phase 0 — Parity harness + atomicity docs (IMPLEMENTED 2026-04-28)

**Scope.** Infrastructure-only. Build the projection registry + build.rs
exhaustiveness check, the pre-commit hook + CI mirror, the parity
harness, and the boot-time WARN for Stubbed variants. No service
migrated; the output is purely a safety-rail set that everything after
Phase 0 builds on.

**Services.** None migrated; this phase is pure infrastructure.

**Shipped:**
- `crates/cairn-store/src/projection_registry.rs` — single const array
  with every `RuntimeEvent` variant + `ProjectionStatus` (Projected /
  Ephemeral / Stubbed) + the `assert_no_stubs_for_persistent_backend`
  boot check.
- `crates/cairn-store/build.rs` — reads `cairn-domain/src/events.rs` and
  the registry; fails the build on any variant drift. Replaces the
  proposed `#[derive(ProjectionRegistry)]` proc-macro (simpler, same
  guarantee, no extra crate).
- `.githooks/pre-commit` — rejects new `log_stub(` sites in pg/sqlite
  projections. Installs via `./scripts/install-hooks.sh`.
- `.github/workflows/ci.yml` job `projection-stub-guard` — same grep as
  the pre-commit hook, enforced on every PR so hook-bypass is
  caught before merge.
- `crates/cairn-store/tests/projection_parity.rs` — parity tests for a
  representative cross-section of the 48 Projected variants (session,
  run, task, approval, org hierarchy) across InMemory ↔ SQLite. Pg
  parity gated behind `TEST_DATABASE_URL` for nightly CI per the risk
  mitigation below. Further parity tests land alongside each Phase
  1/2a/2b migration.
- `AppState::new_with_runtime` — logs the Stubbed variant list at WARN
  on pg/sqlite boots. In-memory boots skip the check (no silent-read
  surface).

**Intentional Phase 0 choices (scope discipline):**
- Boot check is WARN, not fatal. Phase 0 is infrastructure only;
  flipping to fatal in Phase 0 would block CI immediately since the
  current registry lists 77 Stubbed variants. Phase 2c flips the
  severity once Phases 1/2a/2b land and the stub count hits zero.
- Parity harness covers ~10 Projected variants, not all 48. Phase 1
  (evals) and Phases 2a/2b (remaining projections) extend the harness
  as each new projection ships. The framework is in place; adding a
  variant is a ~30-line addition to `projection_parity.rs`.
- No `cairn-store/README.md` atomicity write-up in this PR — the
  atomicity contract is already documented inline at
  `pg/event_log.rs:77-86` with a cross-reference. Adding a duplicate
  markdown section is a documentation-is-not-a-gate trap.

**Dependencies.** None.

**Risk + mitigation.** pg testcontainer CI cost. Mitigation: pg parity
gated on `TEST_DATABASE_URL`; default PR lanes exercise InMemory ↔
SQLite only, nightly CI adds pg.

### Phase 1 — Evals (~1500 LOC)

**Scope.** Resolve #435, #436, #437.

- Add `EvalRunScored` and `EvalRubricScored` event variants to `crates/cairn-domain/src/events.rs`.
- Wire `EvalRunStarted` emission in `start_eval_run_handler` (eval_service.rs:132 currently mutates without emitting).
- Wire `EvalRunCompleted` emission in `complete_eval_run_handler` (eval_service.rs:151).
- Emit `EvalRunScored` in `score_eval_run_handler`.
- Emit `EvalRubricScored` in `score_eval_rubric_handler`.
- Add SyncProjection impl for all 6 eval event types in pg/sqlite/in-memory (replace `log_stub`).
- Delete `AppState::replay_evals()` (state.rs:736) and its call site in main.rs:1479.

**Services.** eval_runs.

**Tests added.** Integration: cairn-app restart-and-read-evals-data across all three backends. Projection parity per Phase 0 harness.

**Dependencies.** Phase 0 (parity harness must exist to validate).

**Risk + mitigation.** New event variants require migration + schema update. Mitigation: single PR, no staged rollout; pre-release so no back-compat concern (per `feedback_no_users_no_deprecation.md`).

### Phase 1.5a — `replay_triggers` cleanup (~400 LOC)

**Scope.** Move trigger projection (`trigger_service.rs:532` — "state is rebuilt from the event log on startup") into SyncProjection. Delete `AppState::replay_triggers()` (state.rs:856).

**Services.** Signal routing / triggers.

**Tests added.** Restart-persistence test for triggers.

**Dependencies.** Phase 0.

**Risk + mitigation.** Trigger state is already narrow; main risk is a silent regression during restart. Mitigation: dedicated integration test before deleting the walker.

### Phase 1.5b — `replay_graph` cleanup (~400 LOC)

**Scope.** Move graph read-model (`state.rs:713`) into SyncProjection or document as ephemeral (graph is a derived index over entities/edges — decide as part of this phase).

**Services.** Graph index.

**Tests added.** Restart-persistence OR explicit `ephemeral` declaration in registry + smoke test demonstrating rebuild.

**Dependencies.** Phase 0.

**Risk + mitigation.** Graph is the largest read-model; migration table size could be significant. Mitigation: measure on a representative dogfood dataset before deciding Projected vs Ephemeral.

**Decision (landed, 2026-04-28).** The graph read-model is declared **Ephemeral**. `AppState::replay_graph()` and its call site in `main.rs` are removed. Rationale:

- `AppState.graph` is hard-coded as `Arc<InMemoryGraphStore>` — a process-scoped in-memory derived index over the event log. Every node and edge is reconstructable from events; the graph holds no authoritative state.
- Every event-log append routed through `publish_runtime_frames_since` (`crates/cairn-app/src/handlers/sse.rs`) already projects the event into the graph on the write path. This is the async-derived pattern: graph updates happen after the event is durable in the store but before the SSE frame fans out.
- The boot walker (`replay_graph`) existed to re-seed the in-memory graph against pre-existing events on Postgres / SQLite backends. It duplicated the write-path projection logic and ran O(N) over the event log on every restart.
- Keeping the walker blocked the graph from being honestly classified — it pretended the graph was durable when the underlying `Arc<InMemoryGraphStore>` was explicitly not.

**Post-Phase-1.5b semantics.** The graph is empty immediately after boot regardless of backend. It is populated lazily as new events flow through `publish_runtime_frames_since`. Pre-restart node IDs return empty subgraphs on persistent backends until those entities participate in new events. This is the defining property of an Ephemeral read-model and matches the contract already applied to `TaskDependencyAdded` / `TaskDependencyResolved` in the projection registry (marked Ephemeral with the reason "graph projection owns the read model, not cairn-store").

**Future work (out of scope for Phase 1.5b).** If provenance traversal across restarts becomes a product requirement, a later phase can wire `PgGraphStore` (already implemented at `crates/cairn-graph/src/pg/store.rs`) and a matching SQLite store into `AppState.graph` behind a `GraphProjection` trait object, moving the graph from Ephemeral to Projected. That migration would drop the boot walker too — not reintroduce it — because a persistent graph store would survive restart under its own backend. Phase 1.5b chose the simpler Ephemeral path because the migration table for a full graph is "significant" (per the risk note above) and the cost of a wasted boot walker today is real, while the cost of deferring Projected-graph is the documented Ephemeral semantics.

**Registry impact.** None. Graph-relevant event variants (`SessionCreated`, `RunCreated`, `TaskCreated`, `ApprovalRequested`, `TriggerCreated`, `TriggerFired`, `CheckpointRecorded`, `MailboxMessageAppended`, `ToolInvocationStarted`, `SignalIngested`, `IngestJobStarted`, `SubagentSpawned`, `CheckpointRestored`, `EvalRunStarted`, `EvalRunCompleted`) all continue to carry their existing cairn-store projection classification (most Projected with their own read-model tables). The graph is a second derived index — not something the cairn-store registry tracks — so no registry row moves as part of Phase 1.5b.

### Phase 2a — Fill projection stubs for services with existing events (~2000 LOC)

**Scope.** Fill the 80 pg + 101 sqlite `log_stub` call sites for variants whose `RuntimeEvent` already exists. Concretely: credentials, approvals, approval_policies, quotas, budgets, licenses, guardrails, retention. Each stub fill is ~20-30 LOC (projection function body + schema column + read-path wiring); many can be elided as no-op when the event is truly ephemeral.

**Services.** Credentials, approvals, approval_policies, quotas, budgets, licenses, guardrails, retention.

**Tests added.** Projection parity harness coverage for each variant. Per-service restart-persistence integration test.

**Dependencies.** Phase 0 (registry + harness); Phase 1 (establishes the pattern).

**Risk + mitigation.** Largest LOC phase; risk of landing one PR that breaks multiple services. Mitigation: pre-phase subdivision spike — if the stub count audits to >2500 LOC, split into 2a.1, 2a.2, 2a.3 along service boundaries, each with its own parity pass. User decision on sprint cadence (open question below) determines whether to split or land as one big PR.

### Phase 2b — Add missing events + projections (~1800 LOC)

**Scope.** Services whose events don't exist yet. Per audit: audits, plugins, skills, scheduled_tasks, outcome_recording, signal ingestion — exact list derived from the `log_stub` set that doesn't match an existing domain event. Each gets: (a) new `RuntimeEvent` variant(s), (b) matching command(s) if the domain requires it, (c) projection impl in all three backends, (d) handler wiring.

**Services.** Audits, plugins, skills, scheduled_tasks, outcomes, signal_ingest (final set enumerated during Phase 2a audit).

**Tests added.** For each new event: parity harness entry, restart-persistence test, emission test on the write path.

**Dependencies.** Phase 2a (pattern + registry conventions).

**Risk + mitigation.** New events mean schema migrations + possibly new domain commands. Mitigation: each service's event addition is scoped small enough to fit within Phase 2b's PR; if any single service's event addition exceeds 400 LOC on its own, it spins out to its own phase.

### Phase 3 — Provider / routing boundary (~400-500 LOC, revised)

**Scope revised 2026-04-28** per research at `docs/design/rfcs/RFC-025-provider-boundary-research.md`. The split is:

- **Projected**: `provider_bindings` (project→connection→model mappings; `ProviderBindingCreated` + `ProviderBindingStateChanged` already exist) AND `provider_connections` (tenant→endpoint registration; `ProviderConnectionRegistered` + `ProviderConnectionDeleted` already exist, hard-delete semantics require persistence for ID re-creation).
- **Ephemeral**: `provider_pools` (HTTP connection pool state), `provider_health` (probe results). These are per-process + cannot outlive a restart.
- `provider_registry` is derived from the projected bindings + connections at boot.

Hot-path read analysis in the research found no >1000/sec consumers — no extra in-memory cache layer needed.

**Services.** provider_bindings, provider_connections (both projected), provider_pools, provider_health (both ephemeral), provider_registry (derived).

**Tests added.** Restart-persistence for bindings + connections. Explicit test that pools are rebuilt from connections on boot. Registry annotation test asserts the ephemeral set vs projected set matches the research classification.

**Dependencies.** Phase 2a/2b. Phase 3 LOC dropped from ~1200 to ~400-500 because 2 of the 4 services classified originally as "split ephemeral" are actually already event-sourced and just need their pg/sqlite `log_stub` projections replaced with real ones.

**Risk + mitigation.** The ephemeral set must be genuinely ephemeral. Mitigation: audit each ephemeral service's field set against the registry declaration before PR.

### Phase 4 — Cleanup (IMPLEMENTED 2026-04-29, PR #585)

**Scope delivered:**

- `cairn_runtime::InMemoryServices` → `cairn_runtime::RuntimeServices`
  rename across all call sites: `crates/cairn-runtime/src/aggregate.rs`
  (struct + impl), `cairn-runtime/src/lib.rs` (re-export),
  `cairn-runtime/src/runtime_config.rs` (doc-comment references),
  `cairn-app/src/state.rs`, `cairn-app/src/router.rs`,
  `cairn-app/src/main.rs`, `cairn-app/src/bin_main/bin_state.rs`, and
  cairn-app integration tests (`support/mod.rs`,
  `support/fake_fabric.rs`, `system_status.rs`). No struct-body
  rewrites; the field types are unchanged from Phase 3.
- Developer-facing docs updated: `docs/developer-guide.md`
  ("AppState fields" table now reads `Arc<RuntimeServices>`),
  `docs/design/CAIRN-FABRIC-FINALIZED.md` (current-state paragraphs
  use `RuntimeServices`; historical prose keeps the old name in
  past-tense context), CLAUDE.md (architecture section carries the
  honest storage + boot accounting — durable dual-writes, the
  startup warm-up replay that still runs today, and what the
  aggregate rename unblocks), README (architecture
  diagram + backends list + RFC-025 closure footnote).
- Archived historical designs (`docs/design/archive/ORCHESTRATOR_DESIGN.md`
  et al.) are left unchanged — they describe a past-tense state.

**Deliberately NOT in scope:**

- A generic `RuntimeServices<S: Store>` abstraction. Every field in
  `RuntimeServices` still type-parameterizes over `<InMemoryStore>`;
  true backend-generic dispatch (`<Arc<dyn Store>>` or
  `<S: Store>` per-field) is a further refactor outside RFC-025's
  scope, tracked separately. What Phase 4 delivers is the naming
  honesty — pg/sqlite reads already flow through the projection layer
  installed by Phases 0–3, so the aggregate is correctly "runtime
  services" regardless of where the read-model tables live.
- Deleting ephemeral-only services' persistence stubs. Audited in
  this PR and confirmed not present: every Ephemeral variant in the
  registry lands in the pg/sqlite `=> {}` intentional-no-op arm, not
  in a dead `save`/`load` path.
- Deleting `replay_*` walkers. Already done in Phases 1 / 1.5a / 1.5b;
  no additional walkers remain in `crates/cairn-app/src/{state.rs,main.rs}`.

**Services.** None migrated in this phase; it is a symbol rename +
doc refresh pass.

**Tests added.** None new. The existing suite (cairn-runtime lib,
cairn-app lib, cairn-app integration, cairn-store projection_registry,
cairn-store projection_parity) must still pass byte-equal. The
restart-durability tests added alongside Phases 1 / 1.5a / 2a / 2b /
3 cover the read-path contract.

**Dependencies.** All prior phases (0, 1, 1.5a, 1.5b, 2a.1, 2a.2, 2b.1,
2b.2a, 3).

**Risk + mitigation.** Pure cleanup; risk is accidental behavioral
change during rename. Mitigation: `sed -i 's/\bInMemoryServices\b/RuntimeServices/g'`
on a restricted file list (cairn-memory crate excluded — its
identically-named type alias is legitimate), diff stat is pure symbol
substitution, `cargo check --workspace --tests` green, pre-commit
hook (fmt + clippy + stub guard) green.

## Open questions — RESOLVED 2026-04-28

### 1. Silent-read mechanism → ACCEPTED (a) + (c)

User pick: **both (a) registry-backed runtime fail-fast AND (c)
pre-commit grep hook**, defense-in-depth.

- (a) Every event variant declares a `ProjectionStatus` in
  `crates/cairn-store/src/projection_registry.rs`. pg/sqlite boot runs
  `assert_no_stubs_for_persistent_backend` and surfaces the stubbed
  list (WARN in Phase 0; fatal after Phase 2c, per the staged flip in
  the Phase 0 section above). Early design named this mechanism as a
  `#[derive(ProjectionRegistry)]` proc-macro; Phase 0 **shipped a
  static const registry + `build.rs` exhaustiveness check** instead,
  which gives the same guarantee without introducing a
  `cairn-store-derive` crate.
- (c) Pre-commit grep hook + CI mirror block commits/PRs that add new
  `log_stub` lines to the pg or sqlite projection applier. Catches new
  stubs before they hit `cargo test`.

Rejected: (b) runtime WARN-once — too easy to miss in logs, violates
`feedback_documentation_is_not_a_gate.md`.

**What Phase 0 shipped:**

- Registry: `cairn-store/src/projection_registry.rs` (const array +
  `assert_no_stubs_for_persistent_backend`).
- Build-script exhaustiveness: `cairn-store/build.rs` parses
  `cairn-domain/src/events.rs` and rejects the crate on any drift.
- Pre-commit hook: `.githooks/pre-commit`, installed via
  `./scripts/install-hooks.sh` (sets `core.hooksPath = .githooks`).
- CI mirror: `projection-stub-guard` job in
  `.github/workflows/ci.yml`.

### 2. Sprint cadence → ACCEPTED: full scope

All 7 phases (delivered as 10 PRs after 2a + 2b subdivision). 27
services move off hand-rolled boot walkers onto durable
`SyncProjection` read-models. Pg/sqlite gain byte-equal parity with
in-memory at the read-model-table layer (verified via the parity
harness). ~8500 LOC over 8-12 weeks at ~1 PR/week. The original
design note "boot becomes O(1)" remains the intent of the abstraction
but is a follow-up refactor — Phases 0-4 ship the substrate
(projected tables + registry + guard rails + name) and delete the
three domain-specific hand-rolled walkers; the full-log boot replay
into the in-memory projections still runs in `main.rs` today. See the
"Boot + read-path semantics after Phase 4 (honest accounting)" block
in the Status section.

### 3. Provider bindings vs connections split → RESEARCH LANDED (2026-04-28)

Research report: `docs/design/rfcs/RFC-025-provider-boundary-research.md`.

**Finding:** Both `provider_bindings` AND `provider_connections` must be **projected** — not the split originally proposed.

- `provider_bindings` (project→connection→model mappings): already has `ProviderBindingCreated` + `ProviderBindingStateChanged` events. Operator-visible via `POST /v1/providers/bindings`. Must survive restart.
- `provider_connections` (tenant→provider_family endpoint registration): already has `ProviderConnectionRegistered` + `ProviderConnectionDeleted` events. Hard-delete semantics require persistence for ID re-creation. **Not ephemeral** as originally proposed.
- `provider_pools` + `provider_health`: stay **ephemeral** (in-flight HTTP state + probe results, per-process, cannot outlive a restart).

Hot-path read analysis (in research doc) found no >1000/sec consumers — no in-memory cache layer needed on top of projections.

**Phase 3 scope updated**: ~400-500 LOC replacing `log_stub` projections for 4 event variants in pg/sqlite backends, parity tests, registry annotations. Provider_pools + provider_health classified `Ephemeral` in the projection registry.

## Alternatives considered

### Postgres-only (delete `--db memory`)

Rejected. Breaks:

- CLAUDE.md's documented local-dev mode (`cargo run -p cairn-app -- --db memory`).
- `scripts/smoke-test.sh` which exercises `--db memory` paths.
- The RFC-011 "local-mode first-class" commitment.

Four backends exist (pg, sqlite, in-memory, SurrealDB candidate per #21); abandoning the abstraction re-introduces it the moment a second backend matters.

### Fix in-place (keep in-memory-with-replay forever)

Rejected. Four divergent code paths (pg projection, sqlite projection, in-memory projection, replay walker). O(N) boot forever. Every new service compounds the replay-walker surface. Leaves #437 open indefinitely.

### Two-phase shortcut: abstraction now, fills later

Rejected per `feedback_no_users_no_deprecation.md` and `feedback_orchestrator_quality_bar.md` ("no Phase 2 deferrals"). If the abstraction lands without Phase 2a/2b, pg and sqlite users silently read empty projection tables — worse than the status quo.

---

## Appendix — Debate transcript

### Round 1 — Proposer

Trait-object migration: make `RuntimeAggregate` use `Arc<dyn Store>` so pg/sqlite/in-memory are interchangeable. Services split into two classes: **projected** (persist via SyncProjection read-model tables, boot O(1)) vs **ephemeral** (runtime caches, not persisted). Initial phase plan: Phase 1 evals, Phase 2 governance (8 services), Phase 3 provider/routing (4), Phase 4 remainder (15).

### Round 1 — Challenger (6 concerns raised)

1. [CRITICAL] Missing projection infrastructure — 20 services have no projection tables.
2. [CRITICAL] Transaction atomicity claim is false — projections fire after event insert, not atomically.
3. [HIGH] Event coverage lies — credentials/approvals don't have events yet.
4. [HIGH] Service classification incoherent — decisions and provider_bindings ambiguous.
5. [MEDIUM] Testing catastrophe — no parity harness, tests assume in-memory.
6. [LOW] Rejected "postgres-only" alternative without justification.

### Round 2 — Proposer (verified against code)

1. **Partially valid** — 79 pg + 101 sqlite `log_stub` calls are the fill-in work (~4000 LOC, 20-30 LOC each, spread over 6+ PRs). 46 projection modules already define record types. Not greenfield.
2. **Factually incorrect** — `crates/cairn-store/src/pg/event_log.rs:77-86` shows `PgSyncProjection::apply_async` executes within the `&mut tx` that inserted the event. Atomic.
3. **Half-wrong** — `CredentialStored`, `CredentialKeyRotated`, `CredentialRevoked` DO exist in `crates/cairn-domain/src/events.rs:189-191, 2000-2018`. Some events do need to be added (e.g. `EvalRunScored`).
4. **Decisions solved** (per `aggregate.rs:173-174`). **provider_bindings** — concede binding records projected, active connections ephemeral.
5. **Valid** — propose `crates/cairn-store/tests/projection_parity.rs`: for each event type, assert `InMemoryStore::get_X() == SqliteStore::get_X() == PgStore::get_X()`.
6. **Valid** — postgres-only breaks `--db memory`; fix-in-place creates 4 divergent patches.

Revised plan: Phase 0 parity harness + txn audit → Phase 1 evals → Phase 2a fill stubs for existing events → Phase 2b add missing events → Phase 3+ remainder.

### Round 2 — Challenger

- Accepted #1, #4, #5, #6.
- Partial accept on #2: atomicity exists but the `append_with_projections` API is implicit — formalize.
- Escalated #3: evals.rs has FIVE mutation points; `create_run` + `archive_run` DO emit events (proposer confirmed), but `start_run`, `complete_run`, `score`, `score_with_rubric` all mutate without events — 4 missing events, not 2.
- New concerns:
  - **A (API churn)**: Making atomicity explicit breaks `EventLog::append` ~100 callers.
  - **C (incomplete eval enumeration)**: plan was 60% incomplete.
  - **D (orphaned replay walkers)**: `replay_triggers(:856)` + `replay_graph` not addressed.
  - **E (silent empty reads)** BLOCKING: stub projection + store = silently wrong empty reads. Need fail-fast boot assertion.

### Round 3 — Proposer (conceded all 3 blockers)

1. **4 missing eval events (not 2)**: `start_run`, `complete_run`, `score`, `score_with_rubric`. Two need new events (`EvalRunScored`, `EvalRubricScored`); two need existing events wired.
2. **Phases 1.5a + 1.5b** added for `replay_triggers` and `replay_graph` cleanup.
3. **Silent-read protected** via Phase 0 parity harness + per-PR invariant check.
4. **Keep `EventLog::append` signature** (don't churn 100 callers) — add compile-time exhaustiveness CI check instead.

**Final plan: 8500 LOC across 7 phases.**

### Round 3 — Challenger (this pass)

- #3 (eval enumeration): **accepted**. Concessions match the four mutation points in `eval_service.rs:132, 151, 427` + the score handler. Fix is concrete: add two events, wire two existing, delete `replay_evals`.
- #D (orphaned walkers): **accepted**. Phases 1.5a / 1.5b address both `replay_triggers` and `replay_graph` explicitly. Cleanup loop in Phase 4 catches anything orphaned by the migration.
- #E (silent reads): **partially accepted — needs specificity.** "Parity harness + per-PR invariant" is not a mechanism. Flagged back to proposer: pick one of (a) proc-macro + central registry failing boot on stubbed variants in pg/sqlite mode, (b) runtime `tracing::warn!` once per stub call, (c) pre-commit grep hook. Recommendation (a): only option that fails loudly in production. **→ User open question.**
- #A (API churn): **accepted with caveat.** Keeping `EventLog::append` unchanged is correct — 100 callers is not worth churning for a property the store already enforces. Caveat: the "compile-time exhaustiveness check" requires each `RuntimeEvent` variant to be enumerated in a central registry (the same mechanism as #E's option a). One registry serves both purposes: projection status declaration + exhaustiveness proof.
- New open question added for user: **8500 LOC across 7 phases at 1 PR/week is 8-12 weeks.** Acceptable sprint cadence, or reduce scope (e.g., postgres-only for projected services, keep in-memory only for ephemeral — with the trade-off that a third runtime mode leaks)?

**Verdict: ACCEPTED with 2 open questions** — silent-read mechanism + sprint cadence. RFC draft proceeds; Phase 0 implementation blocked until user resolves both.
