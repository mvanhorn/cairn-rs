# F65 — Orchestrator Session-Lifecycle Architecture

**Status:** PLAN (no code) — revision 2, 2026-04-27 (all design decisions resolved)
**Scope:** cairn-rs product layer
**Upstream touch points:** FF lease/phase primitives (cooperation, not dependency)
**Related:** F47 (CompletionVerification), F48–F64 (dogfood Phase 2), FF#371

---

## 1. Motivation

Phase 2 dogfood (2026-04-26 → 04-27) surfaced 17 findings (F48–F64). Sixteen are fixed; one is still in flight. Reading the set as a whole, a coherent pattern emerges that no single fix can resolve:

- An orchestrate call dispatches an LLM session that iterates on tool calls with **no upper bound** beyond the request-body `max_iterations`. Sessions that converge produce working code in ~7 iterations; sessions that thrash on unfamiliar APIs blow past 14+ and never converge.
- There is **no observability on why a session terminated** beyond pass/fail. Operator sees a run in `failed` state and has to spelunk events to reconstruct what the LLM was doing.
- There is **no compaction**. The orchestrator that spawned the session sees either the raw transcript or nothing. Both are wrong for the continuation decision.
- There is **no per-Session attempt cap**. A misbehaving orchestrator could re-spawn root-Runs indefinitely against the same Session (goal).
- Workspaces are **raw filesystem paths** baked into prompts, tool args, and checkpoints. That couples agent behavior to operator layout, leaks host details into LLM context, and makes snapshot/resume impossible.
- Terminal states (lease expired, breaker tripped, crash) drop the session on the floor. F64 is the recovery-loop that proves there is nothing to hand back to the orchestrator.

This document proposes a coherent redesign covering six concerns as **one lifecycle model**, not six bolt-ons. Product scope (cairn-rs); FF cooperation called out where relevant but cairn owns the mechanism.

The concrete use case is **multi-turn agentic code generation** where an outer orchestrator LLM spawns bounded sub-sessions, each a lease-bounded agent drive with tool access; the orchestrator observes outcomes (summaries + checkpoint + workspace snapshot) and decides whether to continue, retry, or abort — subject to a per-Session aggregate budget. This is already what cairn wants to do; today each piece is implicit and therefore fragile.

---

## 2. Design principles

1. **Every session terminates with a structured outcome.** No "dropped on floor" paths. Breaker trip, clean complete, crash, timeout — all produce a `SessionOutcome` pointing at a checkpoint + workspace snapshot.
2. **Checkpoint is for continuation, not audit.** Event log remains source of truth for forensics; checkpoint is an opaque blob the orchestrator passes back to spawn-from.
3. **Orchestrator does not do work.** It reads summaries, reads workspace state (read-only tools), decides; it does not edit code. Sub-sessions do the work.
4. **Workspaces are opaque IDs.** The LLM never sees a host path. cairn resolves on tool dispatch. Live workspace = overlayfs mount path; resumed workspace = reflinked snapshot path.
5. **Budgets are defense in depth.** Three independent breakers + a per-Session root-Run cap; any one can end a session; the Session cap bounds the orchestrator loop above. An 80%-of-limit warning event fires ahead of each breaker trip.
6. **Portable storage.** New tables use the subset common to Postgres + SQLite + in-memory store (no JSONB, no arrays, no LISTEN/NOTIFY, no advisory locks). JSON stored as TEXT.
7. **Integration tests only.** Every claim in this doc must be verifiable by a LiveHarness test against a real cairn-app subprocess. Unit tests on domain types are scaffolding.
8. **Cairn stays thin.** Where FF already knows how to bound/lease/suspend, we reuse. Cairn adds the product-shaped lifecycle on top.

---

## 3. Domain model

### 3.1 Session-as-issue: extend Session, don't add a new layer

**Decision (Q1, resolved 2026-04-27): extend the existing `Session` entity with goal/budget/attempt-cap fields. Do NOT introduce a new `IssueId` layer.**

Rationale:

- A **Task** in cairn is treated as a worker-queue primitive: leased, with DAG edges, retry/dead-letter semantics. It is not a goal parent.
- A **Run** is one orchestrator drive (one LLM session). Root-Runs already chain via `parent_run_id`, which is the natural substrate for "attempt N within a Session."
- A **Session** already maps one GitHub-issue → one Session via the `IssueQueueEntry` record (`crates/cairn-app/src/state.rs` around the `IssueQueueEntry` struct: each entry carries `session_id` + `run_id` alongside `repo`, `installation_id`, `issue_number`, `title`). The `SessionRecord` projection (`crates/cairn-store/src/projections/session.rs`) is the stable long-lived parent row. ID newtypes including `WorkspaceId` and `SessionId` are defined in `crates/cairn-domain/src/ids.rs`.

Adding a fresh `IssueId` would duplicate what Session already is. The extension below makes Session the single owner of goal + budget + attempt cap.

### 3.2 `SessionRecord` extension

New fields on `SessionRecord` (portable column types; no JSONB):

```
goal_title:      Option<String>        // user-visible goal string for this Session
issue_budget:    Option<IssueBudget>   // aggregate budget across root-Runs in this Session
max_attempts:    u32                   // how many root-Runs may be spawned (default 5)
attempts_used:   u32                   // counter incremented on each root-Run spawn
```

Supporting types:

```
IssueBudget           { wall_clock_ms_cap, wall_clock_ms_used, token_cap, tokens_used, cost_usd_cap, cost_usd_used }
CircuitBreakerKind    { RoundCap, TokenCap, NoToolUseStreak, WallClock }
CircuitBreakerTrip    { kind, limit, measured, at_iteration }
Checkpoint            { checkpoint_id, root_run_id, session_id, body, created_at, schema_version }
WorkspaceSnapshot     { snapshot_id, session_id, parent_snapshot_id, snapshot_path, created_at }
WorkspaceId           opaque ULID newtype; resolves to a live overlayfs mount path OR a reflinked snapshot path
SessionOutcome        { session_id, root_run_id, checkpoint_id, workspace_snapshot_id, termination_reason, compacted_summary, next_step_hint, cost_usd }
TerminationReason     { Completed | BreakerTripped(trip) | LeaseLost | Crashed | Cancelled | WaitingApproval | WaitingSubagent }
OrchestratorDecision  { Continue(checkpoint_id) | Retry(from_session_start) | Abort(reason) }
```

### 3.3 Relationship

```
 Session  ──1:N──  root-Run (attempt)  ──1:1──  Checkpoint
    │                    │
    │                    └──1:1──  SessionOutcome
    │                                    │
    │                                    └── references  WorkspaceSnapshot
    │
    └── bounded by SessionRecord.{max_attempts, issue_budget}
```

Each orchestrator drive spawns **one new root-Run within the Session**. The existing `parent_run_id` chain already supports continuation. A root-Run ends with exactly one `SessionOutcome`. The orchestrator reads outcomes from prior root-Runs (via compacted summary) and issues an `OrchestratorDecision` that seeds the next root-Run (or terminates the Session).

### 3.4 Re-framing existing types

- `LoopTermination` becomes a subset of `TerminationReason`. The existing variants (Completed, Failed, TimedOut, MaxIterationsReached, WaitingApproval, WaitingSubagent, PlanProposed) fold in. `MaxIterationsReached` becomes `BreakerTripped(RoundCap)`.
- `F47 CompletionVerification` stays. It's the successful-completion analyzer. `SessionOutcome` wraps it as one possible `compacted_summary` shape; other shapes (breaker-trip summary, crash-stub summary) exist alongside.
- `IssueQueueEntry` is unchanged — it already points at a `SessionId`, so the extension lines up with no queue-layer surgery.

---

## 4. Concern-by-concern design

### 4.1 Circuit breakers

Four per-root-Run breakers, enforced by `loop_runner`:

| Breaker | Measures | Default | Config |
|---|---|---|---|
| RoundCap | DECIDE/GATHER/EXECUTE iterations | 30 | `orchestrator.breakers.round_cap` + per-dispatch override |
| TokenCap | cumulative in+out tokens across DECIDE calls | 200_000 | `orchestrator.breakers.token_cap` + override |
| NoToolUseStreak | consecutive DECIDE responses with zero tool_calls | 3 | `orchestrator.breakers.no_tool_use_streak` + override |
| WallClock | per-root-Run elapsed wall-clock ms | 900_000 (15min) | `orchestrator.breakers.wall_clock_ms` + override |

`WallClock` here is the **per-root-Run** breaker (independent of the per-Session `SessionRecord.issue_budget.wall_clock_ms_cap` in §4.2). The Session-level cap bounds the orchestrator loop; the per-root-Run WallClock bounds a single bounded sub-agent invocation. Both are needed — a single pathological root-Run shouldn't consume the whole Session's wall-clock budget without a break point.

Precedence: whichever trips first wins. Every trip produces a `CircuitBreakerTrip { kind, limit, measured, at_iteration }` and terminates with `TerminationReason::BreakerTripped(trip)`.

**80%-of-limit warning (Q10).** For each breaker, when `measured / limit >= 0.80` and the warning has not yet fired for this root-Run, emit a `BudgetThresholdCrossed { which_breaker, measured, limit, ratio }` event exactly once. This lets the orchestrator tighten its last-chance prompt or decide to abort early. It does not terminate the session.

**Where:** `loop_runner` enforces; `decide_impl` reports token counts; a small `BreakerState` struct is threaded through the loop alongside `LoopContext`. The 80% threshold check sits in the same tick where the limit check runs.

**Observability:**
- counter `cairn_orchestrator_breaker_trips_total{kind}`
- counter `cairn_orchestrator_breaker_threshold_warns_total{kind}`
- histogram `cairn_orchestrator_breaker_measured_at_trip{kind}`
- SSE events `BreakerTripped { kind, limit, measured }` before session teardown, and `BudgetThresholdCrossed { which_breaker, measured, limit, ratio }` on first crossing of 80%

**Config surface:** new `OrchestratorConfig` section in FabricConfig (defaults). `POST /runs/{id}/orchestrate` body gains `breaker_overrides: { round_cap?, token_cap?, no_tool_use_streak? }` — the outer orchestrator, when spawning a sub-session, can tighten but not loosen defaults.

### 4.2 Per-Session attempt cap on root-Runs

Budget + attempt cap live on `SessionRecord` directly (see §3.2 extension and §7 schema). No separate `issue_budgets` table — the Session row already owns this state.

Orchestrator (HTTP flow):
1. Create Session (via normal Session creation path) → set `goal_title`, `max_attempts` (default 5), `issue_budget` (default `wall_clock_ms_cap = 3_600_000` (1h), `token_cap`, `cost_usd_cap`). Emit `SessionAttemptBudgetInitialized` on first use.
2. Spawn root-Run → if `attempts_used < max_attempts`, increment + start; else return `SessionAttemptCapExhausted`.
3. Root-Run ends → persist `SessionOutcome`, roll cost + tokens + wall-clock into the Session's `issue_budget.*_used` counters, evaluate orchestrator decision.
4. On `OrchestratorDecision::Continue|Retry`, goto (2). The new root-Run is linked via `parent_run_id` to the previous root-Run's id.

**Default `max_attempts = 5`** (Q4, confirmed 2026-04-27). F64 thrash cases suggest 5 is the floor for API-unfamiliar thrash. Wall-clock cap is a coarse second line of defense; token cap + cost cap bound LLM spend independently.

### 4.3 Checkpoint + ephemeral workspace preservation

Two layers: a durable **Checkpoint** (LLM-side resumable state, JSON) and a **WorkspaceSnapshot** (filesystem-side resumable state, reflinked directory tree). Both are emitted at every termination path.

**Decision (Q3, resolved 2026-04-27):** sandbox = mount-namespaced overlayfs gated by Landlock LSM + seccomp-BPF. Snapshot = umount then reflink the overlay upper dir into `~/.cairn/snapshots/<uuid>/`. Resume = reflink the snapshot back into a fresh overlay lower-stack.

Architectural rationale (inlined here to keep this doc self-auditable — see §4.3.2 for the concrete crate + kernel requirements):

- **Mount namespace + overlayfs** is the lowest-friction Linux isolation primitive that gives us a per-root-Run writable workspace without modifying the host tree. Docker and Kubernetes use the same pair.
- **Landlock LSM** is the only in-tree Linux LSM an unprivileged process can configure for itself at runtime. It ships in kernel 5.13+ and stabilized in 6.1. Alternatives (AppArmor, SELinux) require root or pre-configured profiles and were rejected for operator friction.
- **seccomp-BPF** complements Landlock by blocking escape-oriented syscalls that Landlock does not reason about (mount, pivot_root, ptrace, bpf, perf_event_open). Defense in depth.
- **Reflink (`FICLONE`/`FICLONERANGE`)** gives near-zero-cost copy semantics on btrfs, XFS-with-reflink, and bcachefs. Snapshots become O(inodes). ext4 has no reflink; we fall back to full-copy with an operator warning.

Primary sources surveyed before settling on this architecture (external references, not committed):
- kernel.org: Landlock documentation, overlayfs documentation, seccomp-BPF documentation
- `nix` crate docs (mount/sched namespaces), `landlock` crate 0.4+ docs, `seccompiler` (Firecracker) docs
- `rustix::fs::ioctl_ficlone`, `reflink` crate docs
- Firecracker sandbox design notes; gVisor threat model; Docker rootless overlayfs notes

#### 4.3.1 Checkpoint (LLM state)

Checkpoint captures enough state to resume an LLM session. It is **not a projection of the event log**; it is an immutable, schema-versioned snapshot emitted at termination.

Contents:
```
CheckpointV1 {
    schema_version: 1,
    root_run_id,
    session_id,
    llm_context: { messages[], tool_calls[], tool_results[], token_accounting },
    orchestrator_progress: { iterations, last_decision, breaker_state },
    pending_state: { approvals[], tool_invocations[], subagent_waits[] },
    workspace_snapshot_id,
    created_at,
}
```

Storage: `checkpoints (checkpoint_id PK, root_run_id, session_id, schema_version INT, body TEXT, created_at TIMESTAMP)` portable across pg+sqlite. `body` is JSON-as-TEXT, canonical-serialized for determinism. Size expected 50KB–2MB per checkpoint; large transcripts may spill to blob storage (deferred).

#### 4.3.2 Sandbox per root-Run (live workspace)

Each root-Run runs inside its own sandbox. The sandbox is constructed once at root-Run start and torn down at termination; the workspace state it contains becomes the snapshot.

Layers, bottom to top:

1. **Mount namespace.** `nix::sched::unshare(CloneFlags::CLONE_NEWNS)` — the root-Run process sees a private mount table; its overlay mount is invisible to the host and to sibling root-Runs. Requires Linux namespace support (ubiquitous on any modern kernel).
2. **Overlayfs.** `mount -t overlay overlay -o lowerdir=<reflinked-base>,upperdir=<session-upper>,workdir=<session-work>,xino=on <merged>` where:
   - `lowerdir` is a reflinked copy of the base workspace (either a fresh base or a prior snapshot — see §4.3.3 and §4.3.4).
   - `upperdir` and `workdir` are sibling directories on the SAME filesystem, allocated per root-Run.
   - `xino=on` (kernel 5.9+) gives stable inodes across layers — git operations inside the workspace rely on this.
3. **Landlock LSM.** `landlock 0.4+` crate. Ruleset:
   - Write: `upper/` only (everything writable flows through the overlay upper).
   - Read: `/lib`, `/usr/lib`, `/usr/bin` (tool runtime), plus the merged workspace path.
   - **Assert `RulesetStatus::FullyEnforced`** after `restrict_self()`. If partial enforcement is reported (older kernel, compiled-out LSM), bail. Partial enforcement is a silent security hole.
4. **seccomp-BPF.** `seccompiler` crate (Firecracker team). Deny list at minimum:
   - `mount`, `umount2`, `pivot_root` — block escape via remount.
   - `ptrace` — block sibling-process inspection.
   - `bpf`, `perf_event_open` — block kernel-side observation primitives.
   - Default action for deny list: `SCMP_ACT_ERRNO(EPERM)` (fail the syscall, do not kill the process — cleaner recovery).

Rust crate set:

- `nix` — `mount::mount`, `sched::unshare`, `mount::umount2`.
- `landlock` (>= 0.4) — ruleset builder, `restrict_self()`, `RulesetStatus` check.
- `seccompiler` — BPF program builder + loader.
- `rustix::fs::ioctl_ficlone` (preferred) or the `reflink` crate — `FICLONE`/`FICLONERANGE` for whole-file reflink; recursive tree reflink via `reflink_tree` helper.
- `tempfile::TempDir` — scratch for `upper/` + `work/` + `merged/` roots before adoption.
- `uuid` — snapshot IDs.

Kernel and filesystem requirements:

- **Linux 5.13+.** Required for Landlock (LSM shipped 5.13).
- **Kernel < 5.11** needs `CAP_SYS_ADMIN` to mount overlayfs unprivileged. Detect at cairn-app startup: check `/proc/version` and `capget`. If neither condition is met, **fail loud** with an operator-visible error that names the specific feature missing.
- **Reflink-capable filesystem.** btrfs, XFS (`reflink=1`), bcachefs, APFS-on-macOS (not in scope here). On ext4 (no reflink), fall back to full-copy (`copy_dir_all`) and emit an operator warning: `WorkspaceBackendDegraded { reason: "ext4-fallback-full-copy" }`. Suggest provisioning btrfs or XFS for any serious deployment.
- **overlayfs `xino=on`** (kernel 5.9+) — required for inode stability. Without it, git hashes shift across the layer boundary and any in-workspace git use breaks.
- **`upper/` and `work/` on the same filesystem.** This is an overlayfs invariant. The resolver allocates both as siblings under `.cairn/workspaces/<wsid>/`; separate mount points are rejected.
- **btrfs subvolume snapshot is optional**, not a hard dependency. cairn can use `btrfs subvolume snapshot` as a faster snapshot path when the FS happens to be btrfs, but the default path is reflink-based so the design does not require btrfs.

#### 4.3.3 Snapshot on session end

Every root-Run termination path (clean complete, breaker trip, lease loss, crash recovery) follows the same sequence:

1. **umount merged** — `nix::mount::umount2(<merged>, MNT_DETACH)`. This is the **atomicity fence**: never snapshot a live upper directory. Umount first, snapshot after.
2. **Reflink upper → snapshot** — `reflink_tree(<session-upper>, ~/.cairn/snapshots/<uuid>/)`. On reflink-capable FS this is O(inodes) and near-zero disk. On ext4 it degrades to copy + warning event.
3. **Persist row** — insert into `workspace_snapshots(snapshot_id, session_id, parent_snapshot_id, snapshot_path, created_at)` and emit `WorkspaceSnapshotCreated`.
4. **Reap upper/work** — remove the per-root-Run scratch dirs. The reflinked snapshot remains.

#### 4.3.4 Resume from snapshot

When the orchestrator calls `spawn_attempt({ base_snapshot_id, ... })`:

1. **Reflink snapshot → new lower** — `reflink_tree(~/.cairn/snapshots/<base_snapshot_id>/, <new-lower>)`. Near-zero disk on reflink FS.
2. **Fresh overlay** — mount a new overlayfs with the reflinked `<new-lower>` as lowerdir, fresh `<new-upper>` and `<new-work>` dirs.
3. **Allocate a fresh `WorkspaceId`** for the resumed root-Run — the ID is the LIVE mount path, not the snapshot identity. Snapshot lineage is preserved via `workspace_snapshots.parent_snapshot_id`.
4. **Load checkpoint** — pre-populate LLM context from the referenced `CheckpointV1`.
5. Loop restarts. The next LLM prompt is the orchestrator's responsibility — checkpoints have no "next prompt" slot.

#### 4.3.5 Garbage collection

Snapshot GC is enabled from day 1 — not a follow-up.

- Default TTL: 7 days after the Session closes (`Completed` | `BudgetExhausted` | `Aborted`).
- Independent config knob per retention class (checkpoints, snapshots, event log — see Q7).
- GC sweeps run hourly; emit `WorkspaceSnapshotReaped { snapshot_id, age_ms, reason }`.
- Operator-clearable immediately via `DELETE /sessions/{id}/snapshots` admin endpoint.

#### 4.3.6 Identity — WorkspaceId vs WorkspaceSnapshotId

- `WorkspaceId` (§4.6) identifies a **live overlayfs mount** bound to a root-Run. Assigned at root-Run start (ULID). Mutable while the root-Run runs; resolves to the `merged/` path.
- `WorkspaceSnapshotId` identifies an **immutable reflinked snapshot tree**. Emitted at root-Run termination. Resolves to `~/.cairn/snapshots/<uuid>/`.

The `WorkspaceResolver` (§4.6) knows which kind of path an id maps to.

### 4.4 Compacted summary contract

**Decision (Q2, resolved 2026-04-27):** LLM-written summary from the start. Not deterministic-first-with-LLM-later. Every session-end spawns a small/flash-tier summarizer sub-agent.

`SessionOutcome` shape:

```
SessionOutcome {
    session_id,
    root_run_id,
    checkpoint_id,
    workspace_snapshot_id,
    termination_reason: TerminationReason,
    compacted_summary: String,         // LLM-written, structured per prompt below
    next_step_hint: Option<String>,    // LLM-written, may be empty on Completed
    cost_usd: f64,                     // aggregate: root-Run LLM cost + summarizer cost
}
```

**Summarizer tier.** `glm-4.7-flash` (or provider-equivalent cheapest tier — `openrouter/minimax-m2:free`, `gpt-4.1-mini`, `claude-haiku-4.6`). Selected via `OrchestratorConfig.summarizer_model`. One call per termination; single-shot, no tools, streaming off.

**Summarizer prompt (canonical):**

```
SYSTEM:
You are a session summarizer. You produce a compact, structured summary of what an
autonomous coding agent just did in one bounded attempt. The outer orchestrator will
read your summary to decide whether to continue, retry, or abort the goal.

Output MUST be valid JSON with these keys:
  goal:           string         // what the agent was asked to do (one sentence)
  what_happened:  string         // 3-8 sentences, concrete, cite files and tools
  final_state:    object         // { compiles?: bool, tests?: {passed, failed, skipped}, lints?: {errors, warns} }
  tool_calls:     array          // [{name, count, last_outcome: "ok"|"warn"|"err"}]
  blockers:       array          // strings; empty if none
  next_step_hint: string|null    // one concrete suggestion for the next attempt; null if Completed cleanly
  termination_reason: string     // copy of the input termination_reason

Do NOT include the raw transcript. Do NOT speculate beyond evidence in the inputs.

USER:
termination_reason: {{termination_reason}}
goal: {{goal}}

DECIDE transcript (messages + tool_calls + tool_results, most recent 40 turns):
{{decide_transcript_tail}}

Tool result tail (last 20 tool_result blocks, truncated to 2KB each):
{{tool_result_tail}}

Produce the JSON summary.
```

**Inputs** come from the event log + checkpoint:
- `decide_transcript_tail` — last 40 DECIDE turns from the session's event stream (messages + tool_calls + tool_results).
- `tool_result_tail` — last 20 tool_result blocks, each truncated to 2 KB. Preserves evidence for `what_happened` without blowing the summarizer's context.
- `termination_reason` — exact `TerminationReason` variant string.
- `goal` — `SessionRecord.goal_title` (may be empty; summarizer handles that).

**Failure policy.** If the summarizer call fails (rate limit, model error, non-JSON response), emit a skeletal deterministic summary as fallback: termination_reason + tool-call count + last compile/test state from telemetry. Emit `SummarizerFallback { reason }` event. Never block termination on summarizer success.

**Cost accounting.** Summarizer cost is rolled into `cost_usd` on the outcome. A root-Run that costs $0.11 in the agent loop and $0.002 in the summarizer records `cost_usd = 0.112`.

Relationship to F47: `CompletionVerification` runs on `TerminationReason::Completed` BEFORE the summarizer, and its output is included verbatim in `final_state` (summarizer must preserve it). On non-completed terminations, F47 is skipped; `final_state` is populated from best-effort telemetry captured by the loop.

### 4.5 Orchestrator prompt hygiene

The outer orchestrator LLM has a separate tool whitelist from sub-session LLMs:

| Tool | Orchestrator | Sub-session |
|---|---|---|
| read, grep, glob | yes (read-only, on opaque WS) | yes |
| bash, write, edit | **no** | yes |
| memory_search | yes (read-only; Q8 resolved) | yes |
| spawn_attempt | yes | no |
| continue_from_checkpoint | yes | no |
| abort_session | yes | no |
| terminate_attempt | yes | no |
| approve/deny_approval | out-of-band (operator) | no |

`memory_search` is whitelisted read-only — the orchestrator benefits from prior-Session memory when deciding retry strategy. Implementation is gated on the external memory crate stabilizing (see CLAUDE memory note); for now the wire-up expects a `RetrievalService` trait that returns `Vec<MemoryHit>`.

System-prompt contract for the orchestrator (shape, not final copy):

> You are an orchestrator. You do not edit code. You read the workspace, you read prior attempt summaries, you spawn bounded sub-agent attempts, and you decide when the Session goal is reached. You have `max_attempts` root-Run attempts per Session. Use them deliberately. Each attempt has its own circuit breakers (round cap, token cap, wall-clock cap, no-tool-use streak) and you will see an 80%-of-limit warning event before any breaker trips.

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

`WorkspaceId` is an opaque ULID newtype. It never appears as a path. A central `WorkspaceResolver` maps `WorkspaceId → PathBuf` in a process-local registry (plus a `workspace_registry` table for durability). The resolved path is one of two things:

- **Live** — the `merged/` mount path of the root-Run's overlayfs sandbox.
- **Resumed-from-snapshot** — the `merged/` mount path of a fresh overlayfs whose lower is a reflinked copy of a `WorkspaceSnapshot`.

Both cases present the same contract to the LLM; only the resolver distinguishes them.

Agents see: "You are working in WS-01JABCDEF. Use relative paths; tool calls run with pwd=WS."

On tool dispatch (bash/read/write/grep/glob/edit), cairn:
1. Resolves `WorkspaceId` from the root-Run context.
2. Rejects any tool arg whose path escapes the WS root (canonicalize + prefix check; reject symlinks crossing the boundary). The Landlock ruleset enforces this at the kernel level too; the resolver check gives a nicer error message before the syscall faults.
3. Substitutes pwd = resolved path for bash; rewrites absolute-path args for file tools.

ID assignment: ULID at workspace creation. Content-addressable IDs (snapshot SHA) are tempting but couple identity to content — an empty WS and a snapshot-restored WS shouldn't share ID. ULID + `parent_snapshot_id` gives lineage without aliasing.

Security: the LLM never holds a host path. A compromised LLM cannot address `/etc/passwd` because the path resolver rejects any arg whose canonical form is outside the WS root, AND Landlock blocks the syscall at kernel level, AND seccomp blocks mount/ptrace/bpf even if a bug in the resolver let a path through. Defense in depth.

IDE integration: **out of scope for this redesign** (Q5, resolved 2026-04-27). Follow-up, not this redesign. The `WorkspaceId` contract must anticipate it (opaque IDs, signed-path endpoint reserved), but no IDE code lands here.

---

## 5. Event shape

New events (portable-JSON bodies, appended to existing event store):

```
SessionAttemptBudgetInitialized { session_id, max_attempts, wall_clock_ms_cap, token_cap, cost_usd_cap }
SessionAttemptStarted { root_run_id, session_id, workspace_id, base_checkpoint_id?, base_snapshot_id?, breaker_config }
SessionAttemptEnded { root_run_id, session_id, termination_reason, duration_ms }
BreakerTripped { root_run_id, kind, limit, measured, at_iteration }
BudgetThresholdCrossed { root_run_id, which_breaker, measured, limit, ratio }   // fired at 80%, once per root-Run per breaker
CheckpointPersisted { checkpoint_id, root_run_id, session_id, schema_version, body_size_bytes }
WorkspaceSnapshotCreated { snapshot_id, session_id, parent_snapshot_id?, bytes, reflink_used: bool }   // snapshot_path NOT in event; stored in DB only, never exposed over SSE
WorkspaceSnapshotReaped { snapshot_id, age_ms, reason }
WorkspaceBackendDegraded { reason }                                            // e.g. "ext4-fallback-full-copy"
SummarizerFallback { root_run_id, reason }                                     // LLM summarizer failed; skeletal summary emitted
SessionOutcomePersisted { root_run_id, session_id, termination_reason, checkpoint_id, workspace_snapshot_id, cost_usd }
OrchestratorDecisionMade { session_id, decision, target_checkpoint_id? }
SessionClosed { session_id, final_status: Completed|BudgetExhausted|Aborted }
```

All emitted through the existing `emitter.rs` SSE pipe. Durable via the event store. These are **product-layer events**; FF fabric events are unchanged.

---

## 6. Integration with existing systems

### 6.1 F47 CompletionVerification

Extended, not replaced. Runs only on `TerminationReason::Completed`, before the summarizer. Its output is passed into the summarizer's input so that the resulting `compacted_summary` JSON includes a `final_state` key carrying the verifier output verbatim (the summarizer prompt in §4.4 requires preservation). `compacted_summary` itself is stored as a JSON string — downstream consumers parse it to access the `final_state` field. Non-completed terminations skip the verifier and use best-effort telemetry.

### 6.2 F51–F64 lease/phase mechanics

Circuit breakers are **above** FF lease mechanics, not replacing them. Lease still bounds wall-clock failure; breakers bound logical work. A session can trip a breaker and still cleanly hand its lease back. A session whose lease is lost produces `TerminationReason::LeaseLost` with whatever checkpoint we managed to emit (best-effort; may be incomplete).

F64 (terminal recovery loop) is directly addressed: every termination path emits `SessionOutcomePersisted`; there is no "nothing to recover to." The outer orchestrator always has something to consume.

FF#371 (dual-door lease deadlock) cooperation: when FF ships the phase-probe primitive, cairn's complete-run path uses it to distinguish "lease lost due to terminal transition" from "lease lost due to expiry." Breaker logic is orthogonal.

### 6.3 Backward compat

Existing running runs at migration time:
- No checkpoints exist. In-flight runs are treated as single-attempt legacy root-Runs under their existing Session; `attempts_used` for the Session is backfilled to 1.
- Legacy runs finish under old semantics (no breakers beyond `max_iterations`); new runs enter the new lifecycle.
- Event replay for runs that predate the new events treats missing events as "legacy" and derives a bare `SessionOutcome` at final status. No workspace snapshot is created for legacy runs (sandbox was never constructed); the `workspace_snapshot_id` reference is nullable on legacy outcomes only, enforced at the service layer.

---

## 7. Storage layout

All new and extended tables portable across pg+sqlite+in-memory. No JSONB, no arrays, no advisory locks, no LISTEN/NOTIFY.

**Key change from revision 1:** no `issues` table, no `issue_budgets` table. Goal + budget + attempt cap are added as columns on the existing `sessions` projection. `workspace_snapshots` gains an explicit `snapshot_id` PK (distinct from live `WorkspaceId`).

```sql
-- Extend existing sessions projection (ALTER TABLE migration, not CREATE):
ALTER TABLE sessions ADD COLUMN goal_title              TEXT;
ALTER TABLE sessions ADD COLUMN max_attempts            INTEGER NOT NULL DEFAULT 5;
ALTER TABLE sessions ADD COLUMN attempts_used           INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN wall_clock_ms_cap       BIGINT;
ALTER TABLE sessions ADD COLUMN wall_clock_ms_used      BIGINT NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN token_cap               BIGINT;
ALTER TABLE sessions ADD COLUMN tokens_used             BIGINT NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN cost_usd_cap            DOUBLE PRECISION;
ALTER TABLE sessions ADD COLUMN cost_usd_used           DOUBLE PRECISION NOT NULL DEFAULT 0.0;

CREATE TABLE workspace_registry (
    workspace_id    TEXT PRIMARY KEY,         -- live WS identity (ULID); resolves to an overlayfs merged/ path
    root_run_id     TEXT NOT NULL,            -- owning root-Run
    fs_root         TEXT NOT NULL,            -- resolved host path (never exposed to LLM)
    status          TEXT NOT NULL,            -- Active|Snapshotted|Reaped
    created_at      TIMESTAMP NOT NULL,
    reaped_at       TIMESTAMP
);

CREATE TABLE checkpoints (
    checkpoint_id   TEXT PRIMARY KEY,
    root_run_id     TEXT NOT NULL,            -- the root-Run whose state this is
    session_id      TEXT NOT NULL,
    schema_version  INTEGER NOT NULL,
    body            TEXT NOT NULL,            -- canonical JSON blob
    body_size_bytes INTEGER NOT NULL,
    created_at      TIMESTAMP NOT NULL
);

CREATE TABLE workspace_snapshots (
    snapshot_id         TEXT PRIMARY KEY,                        -- immutable snapshot identity
    session_id          TEXT NOT NULL,
    parent_snapshot_id  TEXT REFERENCES workspace_snapshots(snapshot_id),
    snapshot_path       TEXT NOT NULL,                           -- ~/.cairn/snapshots/<uuid>/
    bytes               BIGINT NOT NULL DEFAULT 0,
    reflink_used        BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMP NOT NULL,
    reaped_at           TIMESTAMP
);

CREATE TABLE session_outcomes (
    root_run_id            TEXT PRIMARY KEY,
    session_id             TEXT NOT NULL,
    checkpoint_id          TEXT NOT NULL REFERENCES checkpoints(checkpoint_id),
    workspace_snapshot_id  TEXT REFERENCES workspace_snapshots(snapshot_id),  -- nullable: legacy/pre-migration outcomes predate the sandbox
    termination_reason     TEXT NOT NULL,                        -- serialized, indexed for observability
    compacted_summary      TEXT NOT NULL,                        -- LLM-written JSON
    next_step_hint         TEXT,                                 -- optional orchestrator handoff hint
    cost_usd               DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    created_at             TIMESTAMP NOT NULL
);

CREATE INDEX idx_checkpoints_session ON checkpoints(session_id, created_at);
CREATE INDEX idx_workspace_snapshots_session ON workspace_snapshots(session_id, created_at);
CREATE INDEX idx_session_outcomes_session ON session_outcomes(session_id, created_at);
CREATE INDEX idx_session_outcomes_termination ON session_outcomes(termination_reason);
```

Migration files: `crates/cairn-store/src/pg/migrations/NNNN_f65_*.sql` and `crates/cairn-store/src/sqlite/NNNN_f65_*.sql`. Symmetric schemas. The `sessions` ALTER TABLE must run on both backends; SQLite prior to 3.35 does not support dropping columns — only adds are needed here, so compatibility is fine.

---

## 8. PR split

Seven sequential PRs. Each PR is COMPLETE for its slice (not an MVP, not a v1) — the slice does what it claims, ships with integration tests, and is ready for operator use. Later PRs build on earlier ones; there is no "we'll come back and finish PR3 later."

Dependency chain:

```
 PR-1 domain types + events
   │
   └── PR-2 store projections (pg + sqlite + in-memory)
         │
         ├── PR-3 circuit breaker enforcement in loop_runner (+80% warning)
         │
         ├── PR-4 sandbox runtime (overlayfs + Landlock + seccomp + reflink base)
         │     │
         │     └── PR-5 snapshot on session end + resume + GC
         │            │
         │            └── PR-6 LLM summarizer + SessionOutcome emission
         │
         └── PR-7 WorkspaceId opaque type + path resolver + orchestrator prompt + spawn/continue/abort tools
```

**PR-1 — Domain types + events.** Extend `SessionRecord` with `goal_title`, `max_attempts`, `attempts_used`, `issue_budget` fields. Reuse the existing `WorkspaceId` newtype from `crates/cairn-domain/src/ids.rs` (do NOT introduce a second one). Add `CircuitBreakerKind/Trip`, `Checkpoint`, `WorkspaceSnapshot`, `SessionOutcome`, `TerminationReason`, `OrchestratorDecision`, and event variants (`SessionAttemptBudgetInitialized`, `SessionAttemptStarted/Ended`, `BreakerTripped`, `BudgetThresholdCrossed`, `CheckpointPersisted`, `WorkspaceSnapshotCreated/Reaped`, `WorkspaceBackendDegraded`, `SummarizerFallback`, `SessionOutcomePersisted`, `OrchestratorDecisionMade`, `SessionClosed`). No behavior change. ~400 LOC.

**PR-2 — Store projections.** Migrations for pg + sqlite: ALTER TABLE sessions, CREATE workspace_registry/checkpoints/workspace_snapshots/session_outcomes. Symmetric schemas. In-memory store parity. Service-layer CRUD + read queries. Portable SQL only (no JSONB/arrays/advisory locks). ~800 LOC. Depends on PR-1.

**PR-3 — Circuit breaker enforcement in loop_runner.** `BreakerState` threaded through `LoopContext`; `decide_impl` reports token counts; `NoToolUseStreak` counter; wall-clock tick. Trip emits `BreakerTripped` + `LoopTermination::BreakerTripped(trip)`. **80% warning** fires `BudgetThresholdCrossed` once per breaker per root-Run. Config in `FabricConfig.orchestrator.breakers`. Request-body override in `POST /runs/{id}/orchestrate`. ~600 LOC. Depends on PR-2.

**PR-4 — Sandbox runtime.** Mount namespace via `nix::sched::unshare(CLONE_NEWNS)`. Overlayfs mount (lower = reflinked base, upper + work per root-Run, `xino=on`). Landlock ruleset + `FullyEnforced` assertion (bail otherwise). Seccomp-BPF deny list (mount/umount2/pivot_root/ptrace/bpf/perf_event_open). Startup detection of kernel version, overlayfs unprivileged support, reflink-capable FS with warn-on-ext4. Base workspace provisioning (repo clone → reflinked lower). ~900 LOC. Depends on PR-2.

**PR-5 — Snapshot + resume + GC.** On every termination path: umount merged → `reflink_tree(upper, ~/.cairn/snapshots/<uuid>/)` → persist `workspace_snapshots` row → emit `WorkspaceSnapshotCreated`. Resume path: `reflink_tree(snapshot, new-lower)` → fresh overlayfs → allocate fresh `WorkspaceId`. Hourly GC sweep with 7-day TTL and `WorkspaceSnapshotReaped` event. `DELETE /sessions/{id}/snapshots` admin endpoint. ~700 LOC. Depends on PR-4.

**PR-6 — LLM summarizer + SessionOutcome.** Summarizer sub-agent spawned on every termination path using small/flash-tier model. Canonical prompt from §4.4 inlined. Input = decide_transcript_tail (last 40 turns) + tool_result_tail (last 20, 2KB each) + termination_reason + goal. Output parsed as structured JSON; `SessionOutcome` row inserted; `SessionOutcomePersisted` event emitted. Fallback skeletal summary on summarizer failure with `SummarizerFallback` event. Cost rolled into `cost_usd`. F47 output preserved in `final_state`. ~600 LOC. Depends on PR-5.

**PR-7 — WorkspaceId + orchestrator prompt + tools.** `WorkspaceResolver` (maps WorkspaceId → overlayfs merged/ OR reflinked snapshot path). Path confinement check in tool dispatch. LLM context strings switched to opaque IDs. Orchestrator system-prompt contract. Tool whitelist enforcement (`OrchestratorToolForbidden` on violation). Orchestrator tools: `spawn_attempt`, `continue_from_checkpoint`, `abort_session`, `terminate_attempt`, plus `memory_search` (read-only, gated on RetrievalService availability). Budget + attempts_used wiring. ~800 LOC. Depends on PR-6.

Total estimate: ~4800 LOC product + ~1500 LOC integration tests across the 7 PRs.

---

## 9. Integration tests

Only LiveHarness tests count (per feedback_integration_tests_only). All spawn real cairn-app subprocess.

1. `test_breaker_round_cap_trips_and_emits_outcome` — max_iterations=3, dispatch LLM that never completes; assert `SessionOutcome` persisted with `BreakerTripped(RoundCap)` and measured=3.
2. `test_breaker_token_cap_trips_mid_session` — small token_cap; assert trip fires and checkpoint is emitted before tool-call completes its echo.
3. `test_breaker_no_tool_use_streak_trips_on_narration` — stub LLM with 3 consecutive no-tool-call responses; assert trip.
4. `test_breaker_overrides_tighten_from_request_body` — default 30, override 5; assert override wins.
5. `test_budget_threshold_crossed_fires_at_80_percent` — drive a root-Run to 0.80 * token_cap; assert single `BudgetThresholdCrossed` event with ratio in [0.80, 1.0); confirm it does not terminate.
6. `test_session_attempt_cap_exhausts_after_max_attempts` — max_attempts=2; spawn 3; assert third returns `SessionAttemptCapExhausted`.
7. `test_checkpoint_roundtrips_resume_produces_same_ctx` — terminate-then-resume via snapshot; assert LLM context after resume equals pre-termination canonical form.
8. `test_overlayfs_sandbox_blocks_write_outside_workspace` — sub-session attempts write to `/tmp/evil`; assert Landlock rejects with EACCES.
9. `test_seccomp_denies_mount_and_ptrace` — direct syscall attempt from a tool-bridge process; assert EPERM, session continues.
10. `test_snapshot_created_on_breaker_trip_and_resume_restores_files` — trip, snapshot, spawn resume attempt, assert files present match pre-trip state.
11. `test_snapshot_gc_reaps_after_ttl` — fast-forward clock past 7d TTL after Session close; assert `WorkspaceSnapshotReaped` and path removed.
12. `test_ext4_fallback_emits_degraded_event` — mount cairn over ext4; first snapshot emits `WorkspaceBackendDegraded`; full-copy succeeds.
13. `test_workspace_path_confinement_rejects_escape` — LLM attempts `bash(cd /etc && cat passwd)`; assert rejected (resolver + Landlock), no host read.
14. `test_orchestrator_tool_whitelist_rejects_bash` — orchestrator LLM emits `bash`; assert rejected before dispatch with `OrchestratorToolForbidden`.
15. `test_compacted_summary_written_by_llm_on_breaker_trip` — trip; assert summarizer call observed, outcome contains structured JSON summary with termination_reason + what_happened + next_step_hint.
16. `test_summarizer_failure_falls_back_to_skeletal` — stub summarizer to error; assert `SummarizerFallback` event and outcome still persisted.
17. `test_legacy_run_completes_under_old_semantics` — start run pre-migration, migrate, finish; assert Session's attempts_used=1 and bare outcome emits without snapshot.
18. `test_lease_lost_still_produces_outcome` — kill attempt with SIGKILL; restart; assert a best-effort `SessionOutcome` exists with `TerminationReason::LeaseLost`.
19. `test_f47_completion_verification_embedded_on_completed` — happy path; assert `SessionOutcome.compacted_summary.final_state` contains F47 output.

---

## 10. Risk surface

| Risk | Mitigation |
|---|---|
| F47 duplication | Keep F47 as the verifier for `Completed`; summarizer preserves its output in `final_state`. |
| Checkpoint size blowup (long transcripts) | Size budget in `CheckpointV1`; overflow policy = truncate oldest tool_results with elision marker + pointer to event-log cursor. |
| **Linux 5.13+ kernel requirement** | Landlock LSM requires 5.13+. Detect at cairn-app startup (`/proc/version` + Landlock ABI probe). Fail loud with operator-visible error naming the required feature. No silent degradation. |
| **overlayfs unprivileged mount on kernel < 5.11** | Older kernels need CAP_SYS_ADMIN to mount overlayfs. Detect at startup; bail with a named error if neither condition holds. |
| **Reflink-capable filesystem not present (ext4)** | Detect at startup. On ext4: emit `WorkspaceBackendDegraded { reason: "ext4-fallback-full-copy" }` on first snapshot; use `copy_dir_all` instead of `reflink_tree`. Operator alert suggests provisioning btrfs or XFS. |
| **Landlock partial enforcement** | Assert `RulesetStatus::FullyEnforced` after `restrict_self()`. Partial enforcement = silent security hole; bail the process. |
| **Snapshotting a live upper directory** | umount merged is the atomicity fence. Snapshot code path: umount → reflink → insert row. Never reflink before umount. Tested explicitly in PR-5 integration suite. |
| **upper/ and work/ on different filesystems** | overlayfs invariant. Resolver allocates both as siblings under `.cairn/workspaces/<wsid>/`; cross-FS allocation is rejected at creation. |
| Snapshot disk blowup if GC lags | GC enabled day 1 (PR-5), not deferred. 7-day default TTL + hourly sweep + admin endpoint for immediate clear. Emit `WorkspaceSnapshotReaped` for observability. |
| LLM summarizer failure blocks termination | Skeletal deterministic fallback on summarizer failure. Emit `SummarizerFallback`. Termination never blocks on summarizer. |
| Testing breakers without real LLMs | Use the existing LiveHarness scripted mock-provider pattern in `crates/cairn-app/tests/` (e.g. `crates/cairn-app/tests/test_f35_tool_errors_as_feedback.rs`) to replay scripted response sequences. Extend for N-turn scripts with token accounting. |
| Sandbox tests need root-ish caps | Mount-namespace + overlayfs unprivileged on kernel 5.13+ should not require root in typical dev environments; if CI container disallows, mark tests `#[ignore]` with the kernel/FS reason and document how to run locally. |
| FF cooperation regressions | F51–F64 fixes remain intact; breakers are additive. FF#371 phase-probe, when it lands, is an optimization, not a dependency. |
| Portability (pg-only regressions) | CI schema-parity test — already recommended in prior audits — extended to the `sessions` ALTER + new tables. |
| Convention drift | Single author risk (bus-factor alert from repo-intel 2026-04-22): PR split lets different agents tackle different PRs; mandatory pair-review on PR-4 + PR-5 (highest-risk — kernel/FS interaction). |

---

## 11. Design decisions (all resolved 2026-04-27)

1. **Q1 — Issue placement: EXTEND Session.** Not a new `IssueId` layer. Rationale: Task is a worker-queue primitive (leased, DAG edges, retry/dead-letter), not a goal parent. `IssueQueueEntry` already maps one GH-issue → one Session. Adding `goal_title` + `max_attempts` + `attempts_used` + `issue_budget` columns on SessionRecord reuses existing identity. Audit: crates/cairn-domain/src/ids.rs:60-87, crates/cairn-store/src/projections/session.rs:9-16.
2. **Q2 — Checkpoint compaction: LLM-written from the start.** Not deterministic-first. Summarizer sub-agent spawned on every termination using a small/flash model tier (glm-4.7-flash or equivalent). Prompt inlined in §4.4. Deterministic skeletal fallback only when the summarizer call fails.
3. **Q3 — Sandbox + snapshot: two-layer overlayfs + Landlock + seccomp + reflink.** Sandbox per root-Run (mount namespace + overlayfs + Landlock write/read ruleset + seccomp deny list). Snapshot on session end (umount → reflink upper into `~/.cairn/snapshots/<uuid>/`). Resume (reflink snapshot → fresh overlay). Inline rationale + primary source list in §4.3. Kernel 5.13+. Reflink-capable FS (btrfs/XFS) preferred; ext4 falls back to full-copy with operator warning.
4. **Q4 — Default `max_attempts` per Session: 5.** Confirmed. F64 thrash cases suggest 5 is the floor for API-unfamiliar thrash.
5. **Q5 — IDE integration: out of scope for this redesign.** Follow-up, not this redesign. `WorkspaceId` contract anticipates it (opaque IDs, signed-path endpoint reserved); no IDE code here.
6. **Q6 — LLM-summarizer as part of the full design.** Not deferred. PR-6 ships it. Tier: smallest/cheapest available (`glm-4.7-flash` or equivalent free/cheap tier via OpenRouter).
7. **Q7 — Event-log retention decoupled from checkpoint retention.** Separate config knobs. Event log is forensic (existing policy). Checkpoints are continuation (7-day default post-Session-close). Snapshots are continuation (7-day default post-Session-close). Each independently configurable.
8. **Q8 — `memory_search` in orchestrator tool whitelist: YES (read-only).** Gated on RetrievalService availability — if the service is absent, the tool fails with a clear error rather than silently omitting the capability.
9. **Q9 — Multi-provider attempts: NO in this redesign.** A Session uses a single provider for all its root-Runs. Multi-provider attempts are a follow-up surface.
10. **Q10 — Budget-near-exhaustion warning: 80%-of-limit event.** Each breaker emits `BudgetThresholdCrossed { which_breaker, measured, limit, ratio }` once on crossing 80% of its limit. Lets the orchestrator tighten its last-chance prompt before the breaker trips.

---

## 12. Non-goals

- Multi-node / multi-instance orchestrator. Single-instance only (matches RFC 020 posture).
- Replacing FF lease/phase primitives. Circuit breakers are above FF; FF still owns lease.
- Cross-Session learning / memory-reuse. Future memory crate scope (whitelisted surface exists in PR-7 via `memory_search`, but cross-Session persistence is crate-owned).
- Multi-provider attempts within a Session (Q9). Single provider per Session here; multi-provider = follow-up.
- IDE integration (Q5). `WorkspaceId` contract anticipates it; no IDE code here.
- Human-in-the-loop replanning UI. Operator reads outcomes; API shape reserved; UI is separate.

---

## 13. Next steps

1. Dispatch PR-1 (domain types + events) — low risk, high leverage; unblocks the rest. All design decisions resolved (§11), no further user input required before kickoff.
2. In parallel, repo-intel refresh on `loop_runner.rs` + `run_service.rs` + `session_service.rs` (current hotspots) to re-baseline risk before PR-3/PR-4.
3. Kernel + filesystem probe spike ahead of PR-4: write a minimal binary that exercises `unshare(CLONE_NEWNS)` + overlayfs mount + Landlock `FullyEnforced` + seccomp BPF + reflink, on the target deployment host. Confirm all four succeed on the production Graviton host (Linux 6.17 on m8g.4xlarge) before PR-4 code starts.
4. Draft FF cooperation note for FF#371 phase-probe, citing cairn's `LeaseLost` termination path as the concrete consumer.
