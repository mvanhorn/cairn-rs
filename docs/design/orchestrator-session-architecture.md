# F65 — Orchestrator Session-Lifecycle Architecture

**Status:** PLAN (no code)
**Scope:** cairn-rs product layer
**Upstream touch points:** FF lease/phase primitives (cooperation, not dependency)
**Related:** F47 (CompletionVerification), F48–F64 (dogfood Phase 2), FF#371

---

## 1. Motivation

Phase 2 dogfood (2026-04-26 → 04-27) surfaced 17 findings (F48–F64). Sixteen are fixed; one is still in flight. Reading the set as a whole, a coherent pattern emerges that no single fix can resolve:

- An orchestrate call dispatches an LLM session that iterates on tool calls with **no upper bound** beyond the request-body `max_iterations`. Sessions that converge produce working code in ~7 iterations; sessions that thrash on unfamiliar APIs blow past 14+ and never converge.
- There is **no observability on why a session terminated** beyond pass/fail. Operator sees a run in `failed` state and has to spelunk events to reconstruct what the LLM was doing.
- There is **no compaction**. The orchestrator that spawned the session sees either the raw transcript or nothing. Both are wrong for the continuation decision.
- There is **no per-issue budget**. A misbehaving orchestrator could re-spawn sessions indefinitely against the same work unit.
- Workspaces are **raw filesystem paths** baked into prompts, tool args, and checkpoints. That couples agent behavior to operator layout, leaks host details into LLM context, and makes snapshot/resume impossible.
- Terminal states (lease expired, breaker tripped, crash) drop the session on the floor. F64 is the recovery-loop that proves there is nothing to hand back to the orchestrator.

This document proposes a coherent redesign covering six concerns as **one lifecycle model**, not six bolt-ons. Product scope (cairn-rs); FF cooperation called out where relevant but cairn owns the mechanism.

The concrete use case is **multi-turn agentic code generation** where an outer orchestrator LLM spawns bounded sub-sessions, each a lease-bounded agent drive with tool access; the orchestrator observes outcomes (summaries + checkpoint + workspace snapshot) and decides whether to continue, retry, or abort — subject to a per-issue budget. This is already what cairn wants to do; today each piece is implicit and therefore fragile.

---

## 2. Design principles

1. **Every session terminates with a structured outcome.** No "dropped on floor" paths. Breaker trip, clean complete, crash, timeout — all produce a `SessionOutcome` pointing at a checkpoint + workspace snapshot.
2. **Checkpoint is for continuation, not audit.** Event log remains source of truth for forensics; checkpoint is an opaque blob the orchestrator passes back to spawn-from.
3. **Orchestrator does not do work.** It reads summaries, reads workspace state (read-only tools), decides; it does not edit code. Sub-sessions do the work.
4. **Workspaces are opaque IDs.** The LLM never sees a host path. cairn resolves on tool dispatch.
5. **Budgets are defense in depth.** Three independent breakers + a per-issue cap; any one can end a session; the issue cap bounds the orchestrator loop above.
6. **Portable storage.** New tables use the subset common to Postgres + SQLite + in-memory store (no JSONB, no arrays, no LISTEN/NOTIFY, no advisory locks). JSON stored as TEXT.
7. **Integration tests only.** Every claim in this doc must be verifiable by a LiveHarness test against a real cairn-app subprocess. Unit tests on domain types are scaffolding.
8. **Cairn stays thin.** Where FF already knows how to bound/lease/suspend, we reuse. Cairn adds the product-shaped lifecycle on top.

---

## 3. Domain model

### 3.1 New types

```
IssueId            opaque ULID   parent work unit spanning multiple attempts
IssueBudget        { issue_id, max_attempts, attempts_used, wall_clock_ms_cap }
SessionAttempt     one bounded agent invocation (existing Session + breaker limits)
CircuitBreakerKind { RoundCap, TokenCap, NoToolUseStreak, WallClock }
CircuitBreakerTrip { kind, limit, measured, at_iteration }
Checkpoint         { checkpoint_id, attempt_id, body, created_at, schema_version }  // body stored inline as JSON-as-TEXT; large-body blob spillover is a future optimization, not baseline
EphemeralWorkspace { workspace_id, snapshot_ref, parent_snapshot_id, created_at }
WorkspaceId        opaque ULID (newtype; never a path)
SessionOutcome     { attempt_id, checkpoint_id, workspace_id, termination, work_summary, next_step_hint }
TerminationReason  { Completed | BreakerTripped(trip) | LeaseLost | Crashed | Cancelled | WaitingApproval | WaitingSubagent }
OrchestratorDecision { Continue(checkpoint_id) | Retry(from_issue_start) | Abort(reason) }
```

### 3.2 Relationship

```
 IssueId  ──1:N──  SessionAttempt  ──1:1──  Checkpoint
    │                    │
    │                    └──1:1──  SessionOutcome
    │                                   │
    │                                   └── references  EphemeralWorkspace (WorkspaceId)
    │
    └── bounded by IssueBudget
```

One IssueId can produce up to `max_attempts` SessionAttempts. Each attempt ends with exactly one SessionOutcome. The orchestrator reads outcomes from prior attempts (via summary) and issues an `OrchestratorDecision` that becomes the seed for the next attempt (or terminates the issue).

### 3.3 What IS the "issue"?

**Recommendation: new layer, `IssueId`.** Not RunId, not SessionId, not TaskDependency.

- `RunId` is one orchestrator drive (currently conflates "one LLM session" with "one work unit"). Keep as "one attempt."
- `SessionId` is conversational. An attempt may contain one session; sessions don't span attempts.
- `TaskDependency` is the task graph — orthogonal; one task may become one IssueId, or a task may fan into several IssueIds if it decomposes.

`IssueId` is the stable parent the orchestrator iterates within. A failed attempt doesn't fail the issue; an exhausted budget does.

**Open question #1** (§11): confirm this vs. repurpose RunId.

### 3.4 Re-framing existing types

- `LoopTermination` becomes a subset of `TerminationReason`. The existing variants (Completed, Failed, TimedOut, MaxIterationsReached, WaitingApproval, WaitingSubagent, PlanProposed) fold in. `MaxIterationsReached` becomes `BreakerTripped(RoundCap)`.
- `F47 CompletionVerification` stays. It's the successful-completion analyzer. `SessionOutcome` wraps it as one possible `work_summary` shape; other shapes (breaker-trip summary, crash-stub summary) exist alongside.

---

## 4. Concern-by-concern design

### 4.1 Circuit breakers

Three per-attempt breakers, enforced by `loop_runner`:

| Breaker | Measures | Default | Config |
|---|---|---|---|
| RoundCap | DECIDE/GATHER/EXECUTE iterations | 30 | `orchestrator.breakers.round_cap` + per-dispatch override |
| TokenCap | cumulative in+out tokens across DECIDE calls | 200_000 | `orchestrator.breakers.token_cap` + override |
| NoToolUseStreak | consecutive DECIDE responses with zero tool_calls | 3 | `orchestrator.breakers.no_tool_use_streak` + override |
| WallClock | per-attempt elapsed wall-clock ms | 900_000 (15min) | `orchestrator.breakers.wall_clock_ms` + override |

`WallClock` here is the **per-attempt** breaker (independent of the per-issue `IssueBudget.wall_clock_ms_cap` in §4.2). The issue-level cap bounds the orchestrator loop; the per-attempt WallClock bounds a single bounded sub-agent invocation. Both are needed — a single pathological attempt shouldn't consume the whole issue's wall-clock budget without a break point.

Precedence: whichever trips first wins. Every trip produces a `CircuitBreakerTrip { kind, limit, measured, at_iteration }` and terminates with `TerminationReason::BreakerTripped(trip)`.

**Where:** `loop_runner` enforces; `decide_impl` reports token counts; a small `BreakerState` struct is threaded through the loop alongside `LoopContext`.

**Observability:**
- counter `cairn_orchestrator_breaker_trips_total{kind}`
- histogram `cairn_orchestrator_breaker_measured_at_trip{kind}`
- SSE event `BreakerTripped { kind, limit, measured }` before session teardown

**Config surface:** new `OrchestratorConfig` section in FabricConfig (defaults). `POST /runs/{id}/orchestrate` body gains `breaker_overrides: { round_cap?, token_cap?, no_tool_use_streak? }` — the outer orchestrator, when spawning a sub-session, can tighten but not loosen defaults.

### 4.2 Per-issue session cap

A new table `issue_budgets` (see §7) holds `{ issue_id, max_attempts, attempts_used, wall_clock_ms_cap, wall_clock_ms_used, created_at, updated_at }`.

Orchestrator (HTTP flow):
1. Create issue → `IssueBudgetCreated` event, default `max_attempts = 5`, `wall_clock_ms_cap = 3_600_000` (1h).
2. Spawn attempt → if `attempts_used < max_attempts`, increment + start; else return `IssueBudgetExhausted`.
3. Attempt ends → persist outcome, evaluate orchestrator decision.
4. On `OrchestratorDecision::Continue|Retry`, goto (2).

**Default `max_attempts = 5`** (per concern #2 prompt; 3 is likely too tight for API-unfamiliar thrash cases seen in F64 M1-v2). Wall-clock cap is a coarse second line of defense.

### 4.3 Checkpoint + ephemeral workspace preservation

Checkpoint captures enough state to resume a session. It is **not a projection of the event log**; it is an immutable, schema-versioned snapshot emitted at termination.

Contents:
```
CheckpointV1 {
    schema_version: 1,
    attempt_id,
    issue_id,
    llm_context: { messages[], tool_calls[], tool_results[], token_accounting },
    orchestrator_progress: { iterations, last_decision, breaker_state },
    pending_state: { approvals[], tool_invocations[], subagent_waits[] },
    workspace_id,
    created_at,
}
```

Storage: new table `checkpoints (checkpoint_id PK, attempt_id, issue_id, schema_version INT, body TEXT, created_at TIMESTAMP)` portable across pg+sqlite. `body` is JSON-as-TEXT, canonical-serialized for determinism. Size expected 50KB–2MB per checkpoint; large transcripts may spill to blob storage (deferred).

Ephemeral workspace: a snapshot of the working directory at termination. Three candidate strategies (**open question #3**):

1. **Tarball** — simplest; `tar.zst` keyed by `workspace_id`; stored in `.cairn/workspaces/{id}.tar.zst` or blob store. Disk footprint = full WS size per snapshot.
2. **Git tree** — `git add -A && git write-tree` against a scratch repo keyed per-issue; snapshot is a tree SHA. Deduplicates identical files across attempts. Requires a git dep at runtime but content-addressable is a nice property.
3. **Copy-on-write** — btrfs/overlayfs/APFS clonefile. Cheapest for large workspaces; OS-dependent; not portable for the Docker/k8s story.

**Recommendation:** git tree. Matches the "multi-attempt on the same issue" pattern naturally (each snapshot is a commit-ish on a per-issue branch), is portable (no kernel-feature dep), deduplicates across attempts, and integrates with the eventual IDE view.

`workspace_snapshots` table: `(workspace_id PK, issue_id, parent_snapshot_id NULLABLE, tree_ref TEXT, created_at)`.

Resume semantics — identity disambiguation:

- `WorkspaceId` (§4.6) identifies a **live working directory** bound to an attempt. Assigned at attempt start (ULID). It is mutable while the attempt runs.
- `WorkspaceSnapshotId` (new; aliased to the `workspace_id` column in the `workspace_snapshots` table for schema simplicity — see §7 note) identifies an **immutable snapshot** (git tree SHA lineage). Emitted at attempt termination.

The orchestrator passes `{ checkpoint_id, base_snapshot_id }` on the next `spawn_attempt`. cairn:
1. Allocates a **new `WorkspaceId`** for the resumed attempt.
2. Materializes `base_snapshot_id`'s tree into the new WS path.
3. Loads checkpoint, pre-populates LLM context.
4. Loop restarts.

The next LLM prompt is the orchestrator's responsibility — checkpoints have no "next prompt" slot. Lineage is preserved via `workspace_snapshots.parent_snapshot_id`.

Retention: `checkpoints` + `workspace_snapshots` follow issue lifecycle. On issue `Completed` or `Aborted`, retain 30d then TTL-sweep (configurable). Operator-clearable via `DELETE /issues/{id}` admin endpoint.

### 4.4 Compacted summary contract

Every termination produces a `SessionOutcome.work_summary: CompactedWork`:

```
CompactedWork {
    goal: String,                   // what the LLM was asked to do
    tool_calls: [ { name, outcome: Ok|Warn|Err, brief } ],
    artifacts: [ { kind: file|test|build, status, path_opaque_or_summary } ],
    final_state: { compiles: Option<bool>, tests: Option<TestSummary>, lints: Option<LintSummary> },
    termination: TerminationReason,
    next_step_hint: Option<String>,
    pointers: { checkpoint_id, workspace_id, event_log_cursor },
}
```

**Compaction strategy:** deterministic first, LLM-enrichment optional.

- Deterministic: crawl the event log + F47 `CompletionVerification` output + tool-call index. No LLM cost. Predictable output shape. Covers 90% of what the orchestrator needs.
- LLM-enriched (optional): a cheap summarizer model is called iff `OrchestratorConfig.llm_summary = true`; populates `next_step_hint` with natural-language "the agent got stuck trying to X; try Y next" text.

**Recommendation:** ship deterministic in v1; add LLM enrichment in a follow-up once we have enough breaker-tripped sessions to fine-tune a cheap summarizer. This is **open question #2**.

Relationship to F47: `CompletionVerification` becomes a sub-structure of `CompactedWork.final_state` when termination is `Completed`. When breaker-tripped, `final_state` is populated from best-effort telemetry (last known compile state, last test run), not from a verifier.

### 4.5 Orchestrator prompt hygiene

The outer orchestrator LLM has a separate tool whitelist from sub-session LLMs:

| Tool | Orchestrator | Sub-session |
|---|---|---|
| read, grep, glob | yes (read-only, on opaque WS) | yes |
| bash, write, edit | **no** | yes |
| spawn_attempt | yes | no |
| continue_from_checkpoint | yes | no |
| abort_issue | yes | no |
| terminate_attempt | yes | no |
| approve/deny_approval | out-of-band (operator) | no |

System-prompt contract for the orchestrator (shape, not final copy):

> You are an orchestrator. You do not edit code. You read the workspace, you read prior attempt summaries, you spawn bounded sub-agent attempts, and you decide when the issue is done. You have `max_attempts` attempts per issue. Use them deliberately.

The `spawn_attempt` tool accepts:
```
spawn_attempt({
    goal: String,
    breaker_overrides: Option<BreakerConfig>,
    base_checkpoint: Option<CheckpointId>,     // for resume
    workspace_id: WorkspaceId,                 // caller holds this
    role_hint: Option<String>,                 // "generalist" | "debugger" | "refactorer"
})
```

Enforcement: cairn validates the orchestrator LLM only emits tools from the whitelist; any other tool call fails fast with `OrchestratorToolForbidden`.

### 4.6 WorkspaceId abstraction

`WorkspaceId` is an opaque ULID newtype. It never appears as a path. A central `WorkspaceResolver` maps `WorkspaceId → PathBuf` in a process-local registry (plus a `workspace_registry` table for durability).

Agents see: "You are working in WS-01JABCDEF. Use relative paths; tool calls run with pwd=WS."

On tool dispatch (bash/read/write/grep/glob/edit), cairn:
1. Resolves `WorkspaceId` from the attempt context.
2. Rejects any tool arg whose path escapes the WS root (canonicalize + prefix check; reject symlinks crossing the boundary).
3. Substitutes pwd = resolved path for bash; rewrites absolute-path args for file tools.

ID assignment: ULID at workspace creation. Content-addressable IDs (git-tree SHA) are tempting but couple identity to content — an empty WS and a snapshot-restored WS shouldn't share ID. ULID + `parent_snapshot_id` gives lineage without aliasing.

Security: the LLM never holds a host path. A compromised LLM cannot address `/etc/passwd` because the path resolver rejects any arg whose canonical form is outside the WS root. This hardens the existing harness-tools permission model rather than replacing it.

IDE integration (follow-up): IDE addresses workspaces by ID via a thin API `GET /workspaces/{id}` that returns a signed, short-lived path for local mount. **Open question #5**: in scope of this redesign, or follow-up?

---

## 5. Event shape

New events (portable-JSON bodies, appended to existing event store):

```
IssueBudgetCreated { issue_id, max_attempts, wall_clock_ms_cap }
SessionAttemptStarted { attempt_id, issue_id, workspace_id, base_checkpoint_id?, breaker_config }
SessionAttemptBreakerTripped { attempt_id, kind, limit, measured, at_iteration }
CheckpointPersisted { checkpoint_id, attempt_id, schema_version, body_size_bytes }
EphemeralWorkspaceSnapshot { workspace_id, parent_snapshot_id?, tree_ref }
SessionOutcomePersisted { attempt_id, termination, checkpoint_id, workspace_id }
OrchestratorDecisionMade { issue_id, decision, target_checkpoint_id? }
IssueClosed { issue_id, final_status: Completed|BudgetExhausted|Aborted }
```

All emitted through the existing `emitter.rs` SSE pipe. Durable via the event store. These are **product-layer events**; FF fabric events are unchanged.

---

## 6. Integration with existing systems

### 6.1 F47 CompletionVerification

Extended, not replaced. Runs only on `TerminationReason::Completed`. Its output is embedded in `CompactedWork.final_state`. Non-completed terminations skip the verifier and use best-effort telemetry.

### 6.2 F51–F64 lease/phase mechanics

Circuit breakers are **above** FF lease mechanics, not replacing them. Lease still bounds wall-clock failure; breakers bound logical work. A session can trip a breaker and still cleanly hand its lease back. A session whose lease is lost produces `TerminationReason::LeaseLost` with whatever checkpoint we managed to emit (best-effort; may be incomplete).

F64 (terminal recovery loop) is directly addressed: every termination path emits `SessionOutcomePersisted`; there is no "nothing to recover to." The outer orchestrator always has something to consume.

FF#371 (dual-door lease deadlock) cooperation: when FF ships the phase-probe primitive, cairn's complete-run path uses it to distinguish "lease lost due to terminal transition" from "lease lost due to expiry." Breaker logic is orthogonal.

### 6.3 Backward compat

Existing running runs at migration time:
- No checkpoints exist. A migration populates a synthetic "legacy" IssueId + single attempt for each in-flight run.
- Legacy runs finish under old semantics (no breakers beyond `max_iterations`); new runs enter the new lifecycle.
- Event replay for runs that predate the new events treats missing events as "legacy" and derives a bare `SessionOutcome` at final status.

---

## 7. Storage layout

All new tables portable across pg+sqlite+in-memory. No JSONB, no arrays, no advisory locks, no LISTEN/NOTIFY.

```sql
CREATE TABLE issues (
    issue_id        TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    status          TEXT NOT NULL,            -- Open|Completed|BudgetExhausted|Aborted
    created_at      TIMESTAMP NOT NULL,
    closed_at       TIMESTAMP
);

CREATE TABLE issue_budgets (
    issue_id            TEXT PRIMARY KEY REFERENCES issues(issue_id),
    max_attempts        INTEGER NOT NULL,
    attempts_used       INTEGER NOT NULL DEFAULT 0,
    wall_clock_ms_cap   BIGINT NOT NULL,
    wall_clock_ms_used  BIGINT NOT NULL DEFAULT 0,
    created_at          TIMESTAMP NOT NULL,
    updated_at          TIMESTAMP NOT NULL
);

CREATE TABLE workspace_registry (
    workspace_id    TEXT PRIMARY KEY,         -- live WS identity (ULID)
    attempt_id      TEXT REFERENCES session_attempts(attempt_id),
    fs_root         TEXT NOT NULL,            -- resolved host path (never exposed to LLM)
    status          TEXT NOT NULL,            -- Active|Snapshotted|Reaped
    created_at      TIMESTAMP NOT NULL,
    reaped_at       TIMESTAMP
);

CREATE TABLE session_attempts (
    attempt_id      TEXT PRIMARY KEY,
    issue_id        TEXT NOT NULL REFERENCES issues(issue_id),
    run_id          TEXT NOT NULL,            -- links to existing runs table
    workspace_id    TEXT NOT NULL,
    base_checkpoint_id TEXT,
    status          TEXT NOT NULL,            -- Running|Terminated
    termination     TEXT,                     -- serialized TerminationReason (JSON-as-TEXT)
    started_at      TIMESTAMP NOT NULL,
    ended_at        TIMESTAMP
);

CREATE TABLE checkpoints (
    checkpoint_id   TEXT PRIMARY KEY,
    attempt_id      TEXT NOT NULL REFERENCES session_attempts(attempt_id),
    issue_id        TEXT NOT NULL,
    schema_version  INTEGER NOT NULL,
    body            TEXT NOT NULL,            -- canonical JSON blob
    body_size_bytes INTEGER NOT NULL,
    created_at      TIMESTAMP NOT NULL
);

CREATE TABLE workspace_snapshots (
    workspace_id       TEXT PRIMARY KEY,                       -- reused as WorkspaceSnapshotId (see §4.3 note)
    issue_id           TEXT NOT NULL REFERENCES issues(issue_id),
    parent_snapshot_id TEXT REFERENCES workspace_snapshots(workspace_id),
    tree_ref           TEXT NOT NULL,                          -- git tree SHA or tarball path
    created_at         TIMESTAMP NOT NULL
);

CREATE TABLE session_outcomes (
    attempt_id      TEXT PRIMARY KEY REFERENCES session_attempts(attempt_id),
    checkpoint_id   TEXT NOT NULL REFERENCES checkpoints(checkpoint_id),
    workspace_id    TEXT NOT NULL REFERENCES workspace_snapshots(workspace_id),
    termination     TEXT NOT NULL,            -- serialized TerminationReason (JSON-as-TEXT), indexed for observability
    next_step_hint  TEXT,                     -- optional orchestrator handoff hint
    work_summary    TEXT NOT NULL,            -- CompactedWork as JSON-as-TEXT; includes details beyond indexed fields
    created_at      TIMESTAMP NOT NULL
);

CREATE INDEX idx_session_attempts_issue ON session_attempts(issue_id);
CREATE INDEX idx_checkpoints_issue ON checkpoints(issue_id, created_at);
CREATE INDEX idx_workspace_snapshots_issue ON workspace_snapshots(issue_id, created_at);
CREATE INDEX idx_session_outcomes_termination ON session_outcomes(termination);
```

Migration files: `crates/cairn-store/src/pg/migrations/NNNN_f65_*.sql` and `crates/cairn-store/src/sqlite/NNNN_f65_*.sql`. Symmetric schemas.

---

## 8. PR split

Dependency graph:

```
 PR1 domain types + events (no behavior)
   │
   ├── PR2 store projections (pg+sqlite+in-memory)
   │     │
   │     ├── PR3 circuit breaker enforcement (loop_runner)
   │     │
   │     ├── PR4 checkpoint emission on all termination paths
   │     │     │
   │     │     └── PR5 ephemeral workspace snapshotting (git-tree)
   │     │
   │     └── PR6 WorkspaceId abstraction + path resolver
   │
   └── PR7 orchestrator LLM prompt + spawn/continue/abort tools
         │
         └── PR8 integration + E2E tests (LiveHarness)
```

**PR1 — Domain types + events.** Add `IssueId`, `WorkspaceId` newtypes, `CircuitBreakerKind/Trip`, `Checkpoint`, `EphemeralWorkspace`, `SessionOutcome`, `TerminationReason` enum, `OrchestratorDecision`. Add event variants. No behavior change. ~400 LOC.

**PR2 — Store projections.** Migrations for pg + sqlite. In-memory store impls. Service layer CRUD for issues, attempts, checkpoints, snapshots, outcomes. Portable queries only. ~800 LOC.

**PR3 — Circuit breaker enforcement.** `BreakerState` in `loop_runner`; token accounting in `decide_impl`; `NoToolUseStreak` counter. Trip emits event + `LoopTermination` variant. SSE event wired in emitter. Config surface in `FabricConfig`. Request-body override in `POST /runs/{id}/orchestrate`. ~600 LOC. Depends on PR1.

**PR4 — Checkpoint emission.** On every termination path in `loop_runner`, serialize `CheckpointV1` and persist. `SessionOutcome` emission with deterministic `CompactedWork`. F47 output embedded on Completed. ~700 LOC. Depends on PR2+PR3.

**PR5 — Ephemeral workspace snapshotting.** Git-tree-backed snapshot on attempt end. `workspace_snapshots` populated. Resume path: materialize tree to fresh WS path. ~600 LOC. Depends on PR4+PR6.

**PR6 — WorkspaceId abstraction.** `WorkspaceResolver`, registry table, path confinement check in tool dispatch. LLM context strings switched to opaque IDs. ~500 LOC. Depends on PR2.

**PR7 — Orchestrator LLM prompt + tools.** `spawn_attempt`, `continue_from_checkpoint`, `abort_issue`, `terminate_attempt` tools. System-prompt contract. Whitelist enforcement. `IssueBudget` creation + consumption wiring. ~700 LOC. Depends on PR1+PR4.

**PR8 — Integration + E2E tests.** LiveHarness scenarios (see §9). ~1000 LOC test-only.

Total estimate: ~4000 LOC product + ~1000 LOC test. Seven code-bearing PRs + one test PR; can compress to 5 if PR3/PR4 and PR5/PR6 combine when agent context allows.

---

## 9. Integration tests

Only LiveHarness tests count (per feedback_integration_tests_only). All spawn real cairn-app subprocess.

1. `test_breaker_round_cap_trips_and_emits_outcome` — max_iterations=3, dispatch LLM that never completes; assert `SessionOutcome` persisted with `BreakerTripped(RoundCap)` and measured=3.
2. `test_breaker_token_cap_trips_mid_session` — small token_cap; assert trip fires and checkpoint is emitted before tool-call completes its echo.
3. `test_breaker_no_tool_use_streak_trips_on_narration` — stub LLM with 3 consecutive no-tool-call responses; assert trip.
4. `test_breaker_overrides_tighten_from_request_body` — default 30, override 5; assert override wins.
5. `test_issue_budget_exhausts_after_max_attempts` — budget=2; spawn 3; assert third returns `IssueBudgetExhausted`.
6. `test_checkpoint_roundtrips_resume_produces_same_ctx` — terminate-then-resume; assert LLM context after resume equals pre-termination canonical form.
7. `test_workspace_snapshot_deduplicates_across_attempts` — two attempts, same files; assert same tree_ref.
8. `test_workspace_path_confinement_rejects_escape` — LLM attempts `bash(cd /etc && cat passwd)`; assert rejected with `PathEscape` error, no host read.
9. `test_orchestrator_tool_whitelist_rejects_bash` — orchestrator LLM emits `bash`; assert rejected before dispatch.
10. `test_compacted_summary_populated_on_breaker_trip` — trip; assert `CompactedWork` has termination, tool_calls, and pointers.
11. `test_legacy_run_migrates_to_synthetic_issue` — start run pre-migration, migrate, finish; assert synthetic IssueId exists and outcome emits.
12. `test_lease_lost_still_produces_outcome` — kill attempt with SIGKILL; restart; assert a best-effort `SessionOutcome` exists with `TerminationReason::LeaseLost`.
13. `test_f47_completion_verification_embedded_on_completed` — happy path; assert `CompactedWork.final_state` contains F47 output.

---

## 10. Risk surface

| Risk | Mitigation |
|---|---|
| F47 duplication | Keep F47 as the verifier for `Completed`; `CompactedWork` wraps, does not re-implement. |
| Checkpoint size blowup (long transcripts) | Size budget in `CheckpointV1`; overflow policy = truncate oldest tool_results with elision marker + pointer to event-log cursor. |
| Git-tree snapshot perf on large WS | Shallow scope to tracked files; `.gitignore`-respect; measure on 10k-file WS before landing PR5. |
| Migration of in-flight runs | Synthetic IssueId path; gated by config flag; rollout monitored for 48h before removing legacy code path. |
| Testing breakers without real LLMs | Use the existing LiveHarness scripted mock-provider pattern in `crates/cairn-app/tests/` (e.g. `crates/cairn-app/tests/test_f35_tool_errors_as_feedback.rs`) to replay scripted response sequences. Extend for N-turn scripts with token accounting. |
| FF cooperation regressions | F51–F64 fixes remain intact; breakers are additive. FF#371 phase-probe, when it lands, is an optimization, not a dependency. |
| Portability (pg-only regressions) | CI schema-parity test — already recommended in prior audits — extended to new tables. |
| Convention drift | Single author risk (bus-factor alert from repo-intel 2026-04-22): PR split lets different agents tackle different PRs; mandatory pair-review on PR4 + PR5 (highest-risk). |

---

## 11. Open questions

1. **What IS the "issue"?** Recommend new `IssueId` layer. Alternative: repurpose RunId and add a `parent_run_id` self-ref. New layer is cleaner but adds a table; repurpose is smaller but conflates two concepts that we've already regretted conflating.
2. **Checkpoint compaction strategy.** Recommend deterministic in v1, LLM-enrichment optional in v2. Alternative: LLM-summarized from day one. LLM-first costs tokens per termination but produces better `next_step_hint`.
3. **Workspace snapshot strategy.** Recommend git-tree. Alternatives: tarball (simpler, no git dep) or COW (cheapest, OS-coupled). Git wins on portability + dedup.
4. **Default `max_attempts` per issue.** Recommend 5. Alternative: 3 (aggressive, matches "fail fast" ethos). F64 thrash cases suggest 5 is the floor for thrash-prone issues.
5. **IDE integration in scope?** Recommend follow-up. `WorkspaceId` contract must anticipate it (opaque IDs, signed-path endpoint reserved), but no IDE code in this redesign.
6. **LLM-summarizer model choice.** If v2 adds LLM enrichment, which tier? Recommend `turbo` (glm-5-turbo) for cost; `tiny` likely underfits. Defer until v2.
7. **Event-log retention vs checkpoint retention.** Checkpoints 30d; event log already has its own policy. Should a checkpoint-referenced cursor pin event-log retention? Recommend no — event log is forensic, checkpoint is continuation; decouple.
8. **Orchestrator tool whitelist: should it include `memory_search`?** Recommend yes (read-only). Orchestrator deciding strategy benefits from prior-issue memory. Gated on memory-crate stability — revisit when external crate lands.
9. **Can one attempt span multiple LLM providers?** Recommend no in v1 (keep attempt = one provider/model for reproducibility). Multi-provider attempts = v2 feature.
10. **Backpressure when budget near exhaustion.** Should we signal the orchestrator at `attempts_used == max_attempts - 1`? Recommend yes via `spawn_attempt` response metadata; lets the orchestrator tighten its last-chance prompt.

---

## 12. Non-goals

- Multi-node / multi-instance orchestrator. Single-instance only (matches RFC 020 posture).
- Replacing FF lease/phase primitives. Circuit breakers are above FF; FF still owns lease.
- Cross-issue learning / memory-reuse. Future memory crate scope.
- Cost-aware budget (dollar caps). Token cap approximates; dollar cap is a follow-up once billing surface lands.
- Human-in-the-loop replanning UI. Operator reads outcomes; API shape reserved; UI is separate.

---

## 13. Next steps

1. User review + answers on the 10 open questions.
2. Dispatch PR1 (domain types + events) — low risk, high leverage; unblocks the rest.
3. In parallel, repo-intel refresh on `loop_runner.rs` + `run_service.rs` (current hotspots) to re-baseline risk before PR3/PR4.
4. Draft FF cooperation note for FF#371 phase-probe, citing cairn's `LeaseLost` termination path as the concrete consumer.
