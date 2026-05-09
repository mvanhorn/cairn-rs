# RFC 031: Operator-Defined Agent Roles

Status: draft
Owner: runtime/agents
Amends: [RFC 018](./018-agent-loop-enhancements.md); touches [RFC 007](./007-plugin-protocol-transport.md), [RFC 015](./015-plugin-marketplace-and-scoping.md).
Depends on: [#799](https://github.com/avifenesh/cairn-rs/issues/799) — per-project tool-id listing endpoint for the role editor's autocomplete. Approved in-scope; soft dependency (UI falls back to freehand tool input if #799 hasn't merged by PR-D).

## Summary

Today every `AgentRole` in cairn is hard-coded in `crates/cairn-domain/src/agent_roles.rs::default_roles()` and resolved by id via `assembled_prompt_for(role_id)`. There is no runtime registry, no HTTP surface, no persistence, no event. The UI can render the five built-ins (orchestrator, researcher, executor, reviewer, generic) but cannot add a sixth. Shipping a new role — for a new kind of work (PR review, doc curation, incident triage) — requires a cairn release.

This RFC makes agent roles **operator-defined** and **project-scoped**: an operator can `POST /v1/projects/:project/agent-roles` a new role with its own system prompt, tool allowlist, tier, and response shape, and every subsequent orchestrator run scoped to that project picks it up. Built-in roles ship as the fallback baseline and are shadowable per project from day one (no migration flag, no separate enablement) — the built-ins are a convenience, not a contract.

The review agent we're building is the first caller: glide-review and valkey-review are two distinct projects inside the same tenant, each registering a lane-specific role with lane-specific prompts and lane-specific KB-backed tools. Nothing about the review agent becomes a cairn-kernel concept; it is a *configuration* on top of the generic agent-loop runtime.

## Why

Three concrete failure modes of the compile-time roles model:

1. **Product gap between "cairn is an agent platform" and "cairn has five roles".** An operator adopting cairn to run a PR-review agent, an incident-triage agent, and a doc-curation agent today has one option: fork cairn. That contradicts RFC 007's pluggability story and RFC 015's per-project tool allowlists, both of which assume roles vary per deployment.
2. **Prompt iteration is release-paced.** Improving the `reviewer` role's prompt today means a cairn PR, code review, release. Prompt iteration should be operator-paced (seconds), not release-paced (days). The UI already has affordances for "select role" and "view role prompt" — what's missing is "create role" and "edit prompt".
3. **Lane separation is impossible.** The review agent needs a different system prompt for valkey vs glide (different subsystems, different invariants, different corpus metadata). With hard-coded roles, we either ship two hard-coded reviewer variants (pollutes the kernel with deployment-specific content) or stuff both lanes into one prompt (prompt bloat + cross-contamination). Project-scoped custom roles solve this cleanly.

## Scope

### In scope

- New domain events: `AgentRoleDefined`, `AgentRoleRetracted`, `ToolDeclaredButMissing`. The first two are `Projected` to `project_agent_roles`; the third is `Ephemeral` (DECIDE-time advisory). Append-only projection — latest `Defined` for an `(project, role_id)` wins; `Retracted` sets `retracted_at`.
- New projection: `project_agent_roles` table, keyed `(project_key, role_id)`.
- New service: `AgentRoleService` on `RuntimeServices` (trait signature in §Runtime Resolution Delta). Methods: `define`, `retract`, `resolve(&project, role_id) -> AgentRole`, `list(&project, SourceFilter) -> Vec<ResolvedRole>`. Resolution order: project custom → built-in fallback → generic verbatim. Tenant-scoped resolution is explicitly out (§D1); a tenant operator who wants a role available everywhere registers it per project.
- Field rename (§D10): `AgentRole.allowed_tools` → `AgentRole.tools` with a serde alias on the old name so pre-rename events / snapshots replay cleanly.
- New HTTP surface under `/v1/projects/:project/agent-roles`. Five verbs: list, get one, create, update, retract. All project-scoped; no tenant-global routes.
- Orchestrator runtime resolution swap: every `default_roles()` / `assembled_prompt_for(role_id)` / `response_shape_for(role_id)` call site in `cairn-orchestrator` pivots to the new service. Built-in defaults remain the fallback; zero behaviour change when no custom role is defined.
- Tool allowlist enforcement: when a role declares `tools: [...]`, the orchestrator filters the available tool set to that list at DECIDE time. Unknown tool ids emit a `ToolDeclaredButMissing` event once per `(run_id, role_id, tool_id)` per run; the orchestrator proceeds without the missing tool. Not a POST-time validation — tools come and go with plugin lifecycle.
- UI: new pages under `/agents/:project/` — list, detail (with history panel), editor, editor-for-existing. Role editor uses #799's autocomplete when available; falls back to freehand with inline guidance.

### Out of scope

- **Tenant-global roles.** See §D1. If the need materialises, a future amendment adds a second resolution tier below project-custom; the event shape already supports it.
- **Multi-tenant role marketplace.** Installing a community-published role from the cairn catalog is a distinct flow and will ride on RFC 015's marketplace infrastructure.
- **Role versioning / rollback with immutable refs.** Events are append-only and the event log is the audit trail. Explicit version tags are deferred until operators ask.
- **Inherit-and-override from built-ins.** A custom role with id `reviewer` is a full shadow, not a partial override.
- **Prompt templating.** System prompts are plain strings; `{{project_id}}` / `{{tenant_id}}` interpolation is a future RFC if needed.
- **Warm in-memory cache with cross-node invalidation.** §D14 — v1 reads the projection directly on every DECIDE. The cache is a v1.1 feature if profiling shows it's needed.
- **Dry-run / prompt preview.** §UI Delta — acknowledged gap, v1.1.

### Rename rationale

`AgentRole` the type stays. One field rename lands in PR-A:

- `AgentRole.allowed_tools: Vec<String>` → `AgentRole.tools: Vec<String>` — HTTP surface (§HTTP Surface Delta), event payload (`AgentRoleDefined.role`), and domain struct all use the same name.
- Serde: `#[serde(alias = "allowed_tools")]` on the new field name so event-log replay from pre-rename snapshots keeps working. The alias is *read-only*: emitted events from PR-A onward use `tools`.

## Decisions

**D1. Scope = project only, tenant out.**

RFC 015 already establishes `ProjectKey { tenant, workspace, project }` as the canonical scope unit. Agent roles fit the same shape: one project = one deployment intent = one set of role prompts. Tenant-global imposed would collide with the lane-separation use case (valkey vs glide in the same tenant). The simpler rule — always project-scoped — is more permissive: a tenant-global feel is achievable via registration automation per project. If operators start demanding tenant-level roles, the fix is a second resolution tier **below** project-custom, with a new `AgentRoleDefinedTenantScope` event; the event shape and `resolve` API both stay source-compatible. **Decided: project-scoped only in v1.**

**D2. Built-in override from day one.**

A custom role with id `reviewer` shadows the built-in `reviewer` for that project. No feature flag, no migration period. Reasons:

- Built-ins are a convenience baseline, not a contract. Operators adopting cairn for real workloads *should* own their prompts.
- Gating override behind a flag splits the operator experience and makes the feature feel provisional.
- Every `Defined` event carries `defined_by` and `at`. The audit trail is the event log.
- Risk of footgun is bounded: shadowing is project-scoped, not global; rollback is one POST-to-retract or one DELETE.

**Decided: shadowing built-ins is always allowed; built-ins are a fallback, not a guardrail.**

**D3. Tool allowlist is a declaration, not a contract.**

`AgentRole.tools: Vec<String>` declares the tool ids the role is *permitted* to use. A second boolean field `AgentRole.forbid_all_tools: bool` distinguishes "no role restriction" from "explicitly forbid every tool." Validation is lazy:

- **At POST time**, cairn does not check that each declared tool exists. A role can list `"post_inline_comment"` even before the review plugin is installed.
- **At run time**, the orchestrator loads the role, filters the available tool registry to the declared allowlist, and emits `ToolDeclaredButMissing { run_id, project, role_id, tool_id, at }` once per `(run_id, role_id, tool_id)` per run for each declared-but-absent tool. The run proceeds with whatever subset is actually available.

Allowlist semantics (decided together because they interact):

- `forbid_all_tools: true` → the orchestrator sees an empty tool set regardless of `tools[]` content. POST / PATCH with `forbid_all_tools: true` AND a non-empty `tools[]` returns 422 `ToolsConflict` (expressing "forbid all" and also listing tools is internally inconsistent).
- `forbid_all_tools: false` (default) AND `tools: []` (empty) → "no role restriction" — every tool in the registry is available. Preserves current `default_roles()` behaviour for `executor` and `researcher`, which carry no `allowed_tools` today.
- `forbid_all_tools: false` AND `tools: [...]` (non-empty) → filter to the listed ids; missing ids emit `ToolDeclaredButMissing`.

No magic strings, no reserved id in the `tools[]` namespace. A plugin is free to register any tool id that passes RFC 007 namespacing without worrying about a reserved-sentinel collision.

**Decided: lazy validation; event-surfaced missing tools; `forbid_all_tools: bool` discriminator; empty `tools` = unrestricted; mixing forbid_all=true with non-empty tools is 422.**

**D4. Per-field size caps + total body cap.**

Field caps (POST / PATCH request body):

| Field | Cap | Rationale |
|---|---|---|
| `system_prompt` | 64 KiB | Bounds event-log overhead since `AgentRoleDefined` carries the full prompt inline. Largest real built-in is `orchestrator` at ~8 KiB; 64 KiB is 8× headroom. |
| `name` | 128 chars | Display only; fits any reasonable label. |
| `description` | 4 KiB | Orchestrator-facing role summary (via `agent_description` tool); LLMs don't need more. |
| `tools[]` | ≤ 256 entries; each id matches `[a-z0-9][a-z0-9_-]*` with id length ≤ 64 | Aligned with role id namespace (§D5). |
| Total request body | 128 KiB | Defence in depth. Reverse proxy + Axum default limits catch gross oversize; this cap catches the "1 MB `description`" case with a specific error. |

Overflow on any single field or the total body returns **413 Payload Too Large** with body:

```json
{
  "error": "payload_too_large",
  "limit": "system_prompt",
  "limit_bytes": 65536,
  "actual_bytes": 71213
}
```

`limit` is a closed enum on the wire (serde `rename_all = "snake_case"`):

```rust
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadLimit {
    SystemPrompt,
    Name,
    Description,
    Tools,
    Body,
}
```

**Tiebreaker on simultaneous overflow** — the handler reports exactly one field per 413 response, chosen by:

1. If total body size exceeds 128 KiB → `limit: "body"` (terminal; no field-level check runs).
2. Else, the first field-level cap violated, in the order `system_prompt` → `name` → `description` → `tools`. The order matches the typical size profile (prompts are largest; tools[] rarely hits its cap) so operators see the most relevant limit first. Subsequent violations are not reported in the same response; operators fix the reported one and resubmit.

413 is a distinct response from 422 (structural validation); clients branch on status. OpenAPI schema for the 413 body lives in PR-B's spec delta. **Decided.**

**D5. Role id namespace: `[a-z0-9][a-z0-9_-]*`, max 64 chars.**

Mirrors the plugin id namespace from RFC 007. Reject POST with 422 `InvalidId` on any other shape. **Decided.**

**D6. Role id collisions and re-POST-after-retract.**

- POST with an id that already has an **active** (`retracted_at IS NULL`) row returns **409 Conflict**.
- POST with an id whose most recent row is **retracted** (`retracted_at IS NOT NULL`) returns **201 Created**; the upsert atomically clears `retracted_at = NULL` on success.
- Uniqueness constraint: `(project_key, role_id) WHERE retracted_at IS NULL`.
- PATCH is the update path; idempotent in the latest-wins sense (replaying identical PATCH bodies → identical projection state, not HTTP-level dedupe).

**Decided.**

**D7. Retract semantics.**

`DELETE /v1/projects/:project/agent-roles/:id` emits `AgentRoleRetracted`. After retraction, `resolve(&project, role_id)`:

1. If the retracted role shadowed a built-in, falls through step 2 and returns the built-in.
2. If the retracted role was a new role (no built-in counterpart), falls through step 3 and returns the **generic role verbatim** (its `id` in the returned `AgentRole` is `"generic"`, not the original custom id). This matches the existing `assembled_prompt_for` fallback semantic and keeps downstream `list_agents` / event logs honest about what role is actually running.

Built-in ids themselves cannot be retracted — there's nothing in the projection to remove; DELETE on an id with no `Defined` event for that project returns 404. DELETE on an already-retracted id is idempotent: returns 200 with the existing retracted row's payload.

Running orchestrations are not interrupted on retract — they've already resolved their role for the run. New runs after the retract use the fallback. The UI (§UI Delta) surfaces the consequence before the operator confirms.

**Decided.**

**D8. Shadowing emits a warning, not a block.**

Shadowing any built-in is allowed (§D2), but the response body includes a `warnings[]` advisory specific to the id — e.g. shadowing `orchestrator` warns "this role is dispatched; ensure response_shape is correct"; shadowing `reviewer` warns "built-in reviewer is generic code-review; this shadow should keep the citation-backed mandate or update tests." The `AgentRoleDefined` event records `shadows_builtin: Some("reviewer")`; the audit trail sees it. **Warnings do not block the write.**

**Decided.**

**D9. `default_roles()` stays compiled-in as the fallback baseline.**

The function is not removed. On a `resolve()` lookup miss, the service falls back to `default_roles()` (and then to the generic role per §D7). A fresh project with zero custom roles behaves exactly as today.

**Decided.**

**D10. Field rename: `AgentRole.allowed_tools` → `AgentRole.tools`.**

One name across HTTP body, event payload, domain struct, and the orchestrator's allowlist filter. `#[serde(alias = "allowed_tools")]` on the new field for replay compat with pre-rename events. The alias is read-only; emitted events from PR-A onward use `tools`.

**Decided.**

**D11. Tier is reserved to built-in ids (shadowing is the only path to a non-Standard tier).**

Two rules, one to cover shadows and one to cover non-shadow custom roles:

- **Shadow tier match** — A custom role whose `id` matches a built-in must declare the built-in's tier. Shadowing `orchestrator` requires `tier: "orchestrator"`. Shadowing `reviewer` / `executor` requires `tier: "standard"`. Shadowing `researcher` requires `tier: "research"`. Shadowing `generic` requires `tier: "generic"`. (Canonical source: `AgentRoleTier` enum in `crates/cairn-domain/src/agent_roles.rs`.) Mismatched tier returns **422 `InvalidTier`**.
- **Reserved tiers** — A custom role whose `id` does NOT match any built-in MUST declare `tier: "standard"` or `tier: "research"`. Declaring `tier: "orchestrator"` or `tier: "generic"` with a novel id returns **422 `ReservedTier`**. Rationale: both the runtime dispatch (`assembled_prompt_for_role` branches on `tier == Orchestrator` to skip base prepend) and the prompt-contract regex gate (`sub_agent_identity_shadow`) assume those tiers are owned by the respective built-in ids. `Standard` and `Research` are free for operator use — `Research` is the right tier for specialised research sub-agents (legal, medical, security-audit, etc.) that need the extended-context budget. `Orchestrator` and `Generic` remain reserved to their built-in ids because they change prompt-assembly behaviour rather than just budget.

**PATCH handler check order (PR-B).** When a PATCH body sets both `tier` (immutable) and other mutable fields, the handler checks `ImmutableField` first. This guarantees the 422 response cites the actual root cause: an operator trying to change `tier` on an existing role gets `ImmutableField` regardless of what new tier value they chose, not `InvalidTier` or `ReservedTier` for the (anyway ignored) tier value. Check order: `ImmutableField` → `InvalidId` / namespace → `InvalidTier` / `ReservedTier` → other field-level validators → structural validation → prohibited-pattern matching.

Net effect: the only legal way to get a role with `tier == Orchestrator` into the system is to shadow the `orchestrator` built-in; similarly for `Generic`. Every operator-authored role with a novel id is a Standard sub-agent. This is a conservative default; if operators need more tiers in the future we add new ones explicitly via a migration, not by letting arbitrary roles claim existing ones.

**Decided.**

**D12. `response_shape` closed enum + `max_context_tokens` bounds.**

- `response_shape ∈ {"direct_answer", "procedural_artifact"}`. Closed set matching the existing `ResponseShape` enum in `cairn-domain`. POST with any other value returns 422 `InvalidResponseShape`.
- `max_context_tokens`: integer, `> 0` and `≤ 2_000_000`. Values outside this range return 422 `InvalidMaxContextTokens`. Upper bound is a defensive sanity check (no current model accepts more); operators who need a true uncap can file an issue.

**Decided.**

**D13. Prompt normalization at storage.**

Before persistence and before structural validation, `system_prompt` is normalised:

1. BOM stripped (leading `﻿` removed).
2. CRLF → LF (`\r\n` and bare `\r` → `\n`).
3. Trailing whitespace on **every** line stripped (not just the final line). Trailing WS on intermediate lines affects tokenization on many Claude / GPT-family tokenisers and is never load-bearing in prompts.

The three steps are idempotent (applying them to an already-normalised string yields the same string). Application point:

- **HTTP handler applies normalization exactly once**, before structural validation runs and before `AgentRoleDefined` is emitted. The event payload carries normalised bytes.
- **Event-log replay is pure passthrough.** Pre-D13 events in the log (if any exist from earlier deployments) replay verbatim; their stored bytes are what `assembled_prompt_for_role` reads. Idempotency means even if an operator manually re-POSTs such a prompt after D13 lands, the second pass produces the same bytes.
- **No other surface normalises.** The projection write does not re-normalise; `assembled_prompt_for_role` does not normalise. One site, one application.

No other mutation beyond the three steps. No templating / interpolation.

**Decided.**

**D14. No cross-node warm-cache layer; run-scoped memoization is allowed.**

Resolution layers, in order of preference:

1. **Per-DECIDE `resolve(&project, role_id)` is a direct projection read.** No `ArcSwap`, no cross-node invalidation. Today's DECIDE already does one per-iteration `default_roles()` clone + find plus an `assembled_prompt_for(id)` that re-clones internally (per `decide_impl.rs`). A projection `HashMap::get` with the same shape is the same order of magnitude.
2. **Per-run `list(&project)` is memoized across the run's `OrchestrationContext` clones.** The new call site for operator-defined roles is `spawn_subagent_tool_def` (PR-C site 5), which after the OnceLock retirement needs the set of spawnable role ids per DECIDE. That set changes only when the operator POSTs / retracts, which is bounded by human iteration cadence; a snapshot taken on the first DECIDE and reused for the remainder of the run is correct (a newly-defined role takes effect on the *next* run, not mid-run).

   `OrchestrationContext` today carries `#[derive(Clone)]` and is rebuilt from a checkpoint per iteration on resume (see `context.rs:23–27`). Memoisation state therefore MUST be shareable across clones of the same context. The field shape is `Arc<OnceCell<Vec<ResolvedRole>>>`: `Arc` so clones share one backing cell; `OnceCell` so fill-once is lock-free after initialisation; clones within the same run observe the same filled snapshot. First DECIDE of a run fills the cell via `get_or_try_init`; subsequent iterations (original or clones) see the filled value. The cell is dropped with the last `Arc` clone at run-end, so "invalidated with the run" falls out of ref-counting without explicit cleanup.
3. **Boot-time projection warm-up is automatic on pg/sqlite.** Event-log replay seeds `InMemoryStore` at boot (per CLAUDE.md RFC-025 Phase 4); the projection read is in-memory on every backend. `--db memory` is trivial by construction.

Deliberately rejected: a process-level `ArcSwap<HashMap<(ProjectKey, RoleId), AgentRole>>`. It would buy little (resolve is already fast) while imposing a multi-node invalidation burden — a `Defined` on node A wouldn't reach node B's cache without an SSE subscription or polling. Per-run memoization sidesteps both concerns: no multi-node coordination, and the "new role visible immediately" guarantee is scoped to where it actually matters (new runs pick up the latest projection).

**Cost accounting for PR-C (site-by-site):**

| Site | Pre-RFC | Post-RFC | Delta |
|---|---|---|---|
| 1. allowlist filter | 1× `default_roles()` clone + find | 1× `resolve` (HashMap + fallback) | Same order (both clone/find once). |
| 2. build_system_prompt | 1× `assembled_prompt_for(id)` → re-clones | 1× `assembled_prompt_for_role(&resolved_role)` — role already held from site 1 | Strict win (one fewer `default_roles()` call). |
| 3. memory hint | 1× `response_shape_for(id)` — dedicated static match | 1× field read on the role already held | Strict win. |
| 4. footer | 1× `response_shape_for(id)` | 1× field read | Strict win. |
| 5. spawn_subagent_tool_def | 1× amortised `OnceLock` read (process-lifetime) | 1× `OnceCell` read per run (first DECIDE pays `list`; subsequent turns free) | First DECIDE per run adds one `list` call; subsequent turns are strict parity. |

First-DECIDE-per-run adds one projection scan (site 5). Every subsequent DECIDE turn is net parity-or-better. No cross-node invalidation required.

**Decided.**

## Prompt Engineering Contract

Every role prompt — built-in or operator-defined — must satisfy the contract below. The contract exists for three reasons:

1. Model behaviour is consistent across roles when prompts share structure. Production agent deployments converge on the same patterns (SWE-agent, OpenHands, Cursor, Devin, Windsurf); cairn follows the same convergence.
2. Operators authoring their first custom role get a template rather than a blank textarea.
3. The POST handler validates *structural* properties (presence, size, prohibited-pattern detection) without parsing semantics.

The contract has two layers: what cairn prepends automatically, and what the operator's `system_prompt` must itself contain.

### Cairn-prepended base (non-negotiable)

`assembled_prompt_for_role(&role)` prepends the `BASE_SUBAGENT_PROMPT` constant (defined in `crates/cairn-domain/src/agent_roles.rs`) to every sub-agent role's specialty overlay. The orchestrator role is the sole exception — it is a *parent*, not a sub-agent, so its `system_prompt` is the full identity without the base.

**The switch is driven by `role.tier`, not by `role.id`.** When `role.tier == AgentRoleTier::Orchestrator` the base is NOT prepended; otherwise it IS prepended. §D11 pins tier-must-match-id on shadow, so the id-based exemption in the rest of this section is equivalent — there's no way to construct a role that confuses the two checks.

`BASE_SUBAGENT_PROMPT` establishes:

- **Identity** — "You are a sub-agent dispatched by a parent agent."
- **Autonomous completion mandate** — "Keep going until the goal is fully done at the depth your specialty warrants."
- **Sub-agent contract** — not the operator, not the orchestrator; final report is machine-parseable for the parent; does not spawn further sub-agents; does not introspect its own run.
- **Parent-context awareness** — treat `## Parent context` in the user message as binding direction.

Operators MUST NOT restate any of the above in `system_prompt`. Contradicting it is a structural error (§Prohibited anti-patterns).

### Required sections in `system_prompt`

Every sub-agent role's `system_prompt` MUST contain the following five sections in order. Detection is regex-based, operating on the normalised prompt (§D13 already applied). Orchestrator-shadow roles (tier=orchestrator, id=orchestrator) are exempt from the five-section requirement but must still contain `## Completion criteria` and `## What not to do`.

**Header detection regex** (multiline, case-insensitive):

```regex
^##[ \t]+(?P<title>[^\n#]+?)[ \t:]*$
```

Rules:
- Only ATX-style H2 headers at column 0. Setext-style (`Specialty\n=====`) is rejected.
- Leading whitespace before `##` disqualifies the header.
- `##Specialty` (no space after `##`) is rejected.
- Case-insensitive match on the `title` capture, after stripping trailing colon(s) and whitespace.
- Inline markdown in the header (`## **Specialty**`) causes a miss.
- On duplicate H2s with the same title, the **first** match wins.
- Titles that match multiple synonyms (see below) are assigned to the first matching synonym.

**Title synonyms:**

| Canonical | Accepted |
|---|---|
| `specialty` | `specialty`, `role` |
| `workflow` | `workflow` |
| `tools` | `tools` |
| `completion criteria` | `completion criteria`, `completion` |
| `what not to do` | `what not to do`, `do not` |

**Section scope:** a section's body spans from its header line to the next `^##[ \t]+` H2 header or EOF, whichever comes first. H3 headings, prose, code fences, and HTML comments inside the body count as body content, not as section boundaries.

**Phase counting in `## Workflow`:**

```regex
^###[ \t]+\S
```

Column-0 H3 headings inside the `## Workflow` body. Must be ≥ 2. Intervening prose, code fences, indented H3s (`    ### x`), H4+ headings, and nested blockquotes do not count.

**Bullet counting in `## What not to do`:**

```regex
^[-*+][ \t]+\S
```

Column-0 unordered bullets inside the `## What not to do` body. Must be ≥ 3. Indented bullets (nested lists), ordered list markers (`1.`, `1)`), and prose paragraphs do not count.

**The five required sections (non-orchestrator-shadow):**

| # | Section | Contract |
|---|---|---|
| 1 | `## Specialty` (or `## Role`) | Present. Body length ≥ 1 non-whitespace char. Acts as the one-line description anchor. |
| 2 | `## Workflow` | Present. Body contains ≥ 2 H3 phase headings. |
| 3 | `## Tools` | Present. Body length ≥ 1 non-whitespace char. Guidance on which declared tool is correct in which phase. |
| 4 | `## Completion criteria` | Present. Body length ≥ 1 non-whitespace char. |
| 5 | `## What not to do` | Present. Body contains ≥ 3 column-0 bullets. |

**Short-circuit on zero-H2 prompts.** If the normalised prompt contains zero `^##[ \t]+` headers, the validator emits a **single** `MissingSection` failure with `field: "system_prompt"`, `message: "no H2 section headers found; see RFC 031 for the required structure"`, `span: null`, `suggested_insert_offset: 0` — rather than five per-section failures. Pre-first-H2 prose has no implicit root section and never satisfies any required-section check.

**The two required sections (orchestrator shadow):**

- `## Completion criteria` — ≥ 1 non-whitespace char.
- `## What not to do` — ≥ 3 column-0 bullets.

Orchestrator shadows dispatch rather than execute; the Workflow and Tools contracts don't map. Specialty is nice-to-have for the orchestrator and not required.

### Prohibited anti-patterns (enforced)

The following patterns are detected at POST / PATCH time and rejected with 422 `ProhibitedPattern`. Detection is case-insensitive regex with line-level anchoring.

| Name | Regex (Rust) | Rationale |
|---|---|---|
| `early_completion_directive` | `(?i)(?:call\s+)?complete_run\s+(?:immediately\|right\s+away\|at\s+the\s+start)\s+(?:and\s+)?(?:exit\|return\|stop\|without\s+\w+)\b\|call\s+complete_run\s+now\b\s+and\s+(?:exit\|return\|stop)\|call\s+complete_run\s+(?:before\|without)\s+(?:finishing\|completing\|verifying)` | Every alternative now requires a **bypass verb** (`exit` / `return` / `stop` / `without {anything}` / `before finishing`) so legitimate prose like *"call `complete_run` immediately after `post_summary_comment`"* does NOT trigger — only phrasings that instruct the model to skip work. |
| `caps_adversarial_framing` | Two-stage. Stage 1 (regex) `(?m)^.*\b(CRITICAL\s+RULES?\|MUST\s+NOT\s+FAIL\|FAILURE\s+IS\s+UNACCEPTABLE\|FATAL\s+ERROR\s+IF)\b.*$` identifies candidate lines. Stage 2 (programmatic) `count(c \|c in ['A'-'Z']) / count(c \|c in ['A'-'Z','a'-'z']) >= 0.8`. Lines with zero ASCII letters (e.g. pure symbols) do NOT trigger the rule. "Matched line" is the single physical line (delimited by `\n` after §D13 normalization) containing the regex match's start byte. | Modern Claude responds worse to all-caps adversarial framing; ordinary "must" / "do not" in sentence case is fine. |
| `sub_agent_identity_shadow` | Gated: runs only when `role.tier != AgentRoleTier::Orchestrator`. Regex `(?im)^\s*You\s+are\s+the\s+(orchestrator\|operator\|user)\b`. | Contradicts `BASE_SUBAGENT_PROMPT`. The gate is tier-based (matching the runtime dispatch per §D11), so a legitimate orchestrator shadow never triggers — and per §D11 reserved-tier rule, only a role whose id is `orchestrator` can carry `tier: Orchestrator`, so id-based and tier-based gates are equivalent by construction. |

### Reviewer guidance — NOT enforced

The following anti-patterns are real but not encoded in the validator because they are semantic / contextual. The UI editor surfaces them as soft hints ("this prompt does X; are you sure?"), but they do not block writes.

- **Ambient instructions without workflow scaffolding** — e.g. "you can answer in under a minute" in a procedural-artifact role. Encoded as a hint; validator does not reject.
- **Prompt ends mid-sentence** — catching this reliably requires NLP; operator problem.
- **Prompt contradicts declared `tools` list** — semantic cross-check; deferred.

### Structural validation (wire shape)

```rust
pub struct ValidationReport {
    pub passed: bool,
    pub failures: Vec<ValidationFailure>,
}

pub struct ValidationFailure {
    pub code: FailureCode,
    pub field: &'static str,      // "system_prompt" | "id" | "tools" | "tier" | "max_context_tokens" | "response_shape" | "forbid_all_tools"
    pub message: String,          // non-empty, trimmed
    pub span: Option<Span>,       // byte offsets into the rejected input; always None for MissingSection (see §UI Delta)
    pub suggested_insert_offset: Option<usize>,  // set by MissingSection; byte offset where canonical section would insert
}

pub struct Span { pub start: usize, pub end: usize }

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureCode {
    MissingSection,
    InsufficientPhases,
    InsufficientBullets,
    ProhibitedPattern,
    InvalidId,
    InvalidTier,
    ReservedTier,                 // §D11 non-shadow role claims orchestrator/generic
    InvalidResponseShape,
    InvalidMaxContextTokens,
    ToolsConflict,                // §D3 forbid_all_tools=true with non-empty tools[]
    ImmutableField,               // PATCH attempts to change `id` or `tier`
}
```

`message` is guaranteed non-empty after server-side trim. `SizeExceeded` is NOT in `FailureCode` — 64 KiB prompt overflow is a distinct 413 response with a different body shape (see §D4 above). Validator callers that want to distinguish "prompt too large" from "prompt structurally invalid" must branch on HTTP status (413 vs 422), not on a validation code.

The validator lives at `cairn_domain::agent_roles::validate_prompt_structure(&role) -> ValidationReport`. Unit tests in `cairn-domain` pin each `FailureCode`'s behaviour with minimal counter-example prompts so regressions are caught at crate level, independent of HTTP wiring.

HTTP response body on validation failure:

```json
{
  "error": "validation_failed",
  "failures": [
    {
      "code": "missing_section",
      "field": "system_prompt",
      "message": "required section `## Workflow` not found",
      "span": null
    },
    {
      "code": "prohibited_pattern",
      "field": "system_prompt",
      "message": "pattern `early_completion_directive` matched",
      "span": { "start": 1284, "end": 1319 }
    }
  ]
}
```

### Worked example

A legal custom role for the valkey-review lane — passes every structural check:

```
## Specialty

Review pull requests on valkey-io/valkey with KB-grounded inline comments.

## Workflow

### 1. Explore

Read the PR metadata (title, body, changed files). For each changed file with a
non-trivial patch, call `knowledge_search` with the file path + first 2 KB of
the patch as the query. Read the retrieved corpus chunks.

### 2. Plan

For each file, note the concrete concerns the corpus or diff suggests.
Concerns without either corpus backing or direct diff evidence must not
advance to Phase 3.

### 3. Implement

For each concern with evidence, call `post_inline_comment` with:
- file path + line number from the diff,
- body referencing the evidence,
- `confidence = high` only if both corpus and diff support the claim.

### 4. Deliver

Call `post_summary_comment` exactly once with a structured summary.

## Tools

- `knowledge_search` — the only retrieval tool. Use once per non-trivial file.
- `post_inline_comment` — one call per concrete concern. Body must cite.
- `post_summary_comment` — exactly one call, in Phase 4.
- `read_file` — use to verify a claim before posting a high-confidence comment
  when the diff hunk alone is not sufficient.

## Completion criteria

Before calling `complete_run`, verify ALL of these:
- at least one `post_inline_comment` or `post_summary_comment` call succeeded
- every `post_inline_comment` body cites either a corpus chunk or a diff line
- exactly one `post_summary_comment` has been made
- no tool call returned an error that went unaddressed

## What not to do

- Do not post without citing the diff or a corpus chunk.
- Do not mark every style choice as a bug.
- Do not hallucinate APIs. If unsure a function exists, verify with
  `read_file` first or drop the claim.
- Do not call `complete_run` before `post_summary_comment`.
```

## Event-Sourcing Delta

Three new variants on `RuntimeEvent`:

```rust
AgentRoleDefined {
    project: ProjectKey,
    role: AgentRole,
    shadows_builtin: Option<String>,   // Some("reviewer") when the id matches a built-in
    defined_by: OperatorId,
    at: u64,                           // ms since epoch
},

AgentRoleRetracted {
    project: ProjectKey,
    role_id: String,
    retracted_by: OperatorId,
    at: u64,
},

ToolDeclaredButMissing {
    run_id: RunId,
    project: ProjectKey,
    role_id: String,
    tool_id: String,
    at: u64,
},
```

Registry entries in `crates/cairn-store/src/projection_registry.rs`:

| Variant | Status | Rationale |
|---|---|---|
| `AgentRoleDefined` | `Projected { table: "project_agent_roles" }` | Operator audit trail + projection source |
| `AgentRoleRetracted` | `Projected { table: "project_agent_roles" }` | Sets `retracted_at`; same table |
| `ToolDeclaredButMissing` | `Ephemeral { reason: "advisory — tool declared by role but not registered at DECIDE time" }` | Run-time observability only; not projected. Deduped per `(run_id, role_id, tool_id)` on the run's `OrchestrationContext::declared_but_missing` HashSet (not `ToolContext` — that struct is constructed per-call and would lose state between DECIDE turns). See §Runtime Resolution Delta for the field shape. |

### Projection `project_agent_roles`

| Column | Type | Notes |
|---|---|---|
| `tenant_id` | TEXT | from `ProjectKey` |
| `workspace_id` | TEXT | |
| `project_id` | TEXT | |
| `role_id` | TEXT | `(tenant_id, workspace_id, project_id, role_id)` is the PK |
| `role_json` | JSONB | full `AgentRole` serde (post-rename field `tools`) |
| `shadows_builtin` | TEXT NULL | when this row shadows a built-in id |
| `defined_by` | TEXT | operator id |
| `defined_at` | BIGINT | ms epoch — used as the ETag for If-Match |
| `retracted_at` | BIGINT NULL | NULL = active; non-NULL = retracted |
| `retracted_by` | TEXT NULL | operator id, NULL when active |

Uniqueness constraint per §D6: `(project_key, role_id) WHERE retracted_at IS NULL`.

`AgentRoleDefined` upsert:
- On no matching PK: insert new row with `retracted_at = NULL`.
- On matching PK with `retracted_at IS NULL`: updates `role_json`, `shadows_builtin`, `defined_by`, `defined_at`.
- On matching PK with `retracted_at IS NOT NULL`: updates all of the above AND atomically clears `retracted_at = NULL`, `retracted_by = NULL`.

`AgentRoleRetracted` sets `retracted_at` and `retracted_by`; resolve reads the row only when `retracted_at IS NULL`.

### Event ordering

Projection ordering follows event-log sequence. Latest `AgentRoleDefined` for a given `(project, role_id)` by event-log offset wins on upsert. This is already how the event log is consumed per CLAUDE.md RFC-025 contract.

### Actor extraction

`defined_by` / `retracted_by` populated via `operator_id_from_principal(&AuthPrincipal)` — same convention as `KnowledgeProviderConfigured` in `cairn-app::marketplace_routes`.

## HTTP Surface Delta

Five endpoints, all project-scoped under `/v1/projects/:project/agent-roles`. No tenant-global surface in v1 (§D1).

```
GET    /v1/projects/:project/agent-roles[?source={builtin|custom|custom_shadow}]
GET    /v1/projects/:project/agent-roles/:id
POST   /v1/projects/:project/agent-roles
PATCH  /v1/projects/:project/agent-roles/:id
DELETE /v1/projects/:project/agent-roles/:id
```

Tenant-scoping enforced via `enforce_project_tenant` extractor (existing pattern from `cairn-app::marketplace_routes`).

### Status code contract

| Code | When | Endpoints |
|---|---|---|
| 200 | Success on GET / PATCH / DELETE | all except POST |
| 201 | Success on POST (including re-POST after retract per §D6) | POST |
| 400 | Malformed JSON body / schema type error (e.g. non-string id) | POST / PATCH |
| 401 | Missing or invalid bearer token | all |
| 403 | Not admin (`AdminRoleGuard`) for POST / PATCH / DELETE | POST / PATCH / DELETE |
| 404 | Role id not in projection for this project (GET one / PATCH / DELETE on absent) | GET one / PATCH / DELETE |
| 409 | POST with `(project, role_id)` that already has an **active** row (§D6) | POST |
| 412 | Stale `If-Match` ETag on PATCH | PATCH |
| 413 | Any field or the total body exceeds its §D4 size cap | POST / PATCH |
| 422 | Structural / semantic validation failure — any `FailureCode` (see Prompt Contract §Structural validation) | POST / PATCH |

GET list / GET one are read-only and exempt from `AdminRoleGuard`; any authenticated operator whose token scope covers the target tenant can read. POST / PATCH / DELETE require `AdminRoleGuard` per the plugin-management precedent.

ETag / If-Match semantics: server emits `ETag: "<defined_at_ms>"` on POST / PATCH / single-resource GET 2xx responses. Single-resource GET carries the header; list GET does not (§GET list response). Clients echo the quoted value in `If-Match` on PATCH for lost-update protection. 412 on mismatch.

### POST body

```json
{
  "id": "pr-reviewer-valkey",
  "name": "Valkey PR Reviewer",
  "tier": "standard",
  "description": "Reviews pull requests on valkey-io/valkey with KB-grounded inline comments.",
  "system_prompt": "## Specialty\n...",
  "tools": ["post_inline_comment", "post_summary_comment", "knowledge_search", "read_file"],
  "max_context_tokens": 200000,
  "response_shape": "procedural_artifact"
}
```

Required: `id`, `name`, `tier`, `system_prompt`. Optional: `description`, `tools` (default `[]`), `max_context_tokens` (default role-tier default), `response_shape` (default role-tier default).

### POST / PATCH success response

```json
{
  "role": { /* full AgentRole including normalised system_prompt */ },
  "defined_at": 1730000000000,
  "warnings": [
    { "code": "shadow_warn_orchestrator", "message": "shadowing the orchestrator — ensure response_shape is set correctly" }
  ]
}
```

Response also carries header `ETag: "1730000000000"` (RFC 7232 quoted opaque-tag, value equals `defined_at` as ms epoch). `warnings[]` is always present (empty on clean writes). `message` is non-empty and trimmed.

**Warning code catalog** — closed set, additions are non-breaking:

| Code | Trigger | Default message template |
|---|---|---|
| `shadow_warn_orchestrator` | Creating / updating a role with `id = "orchestrator"` | "shadowing the built-in orchestrator — verify response_shape and base-prompt exemption" |
| `shadow_warn_reviewer` | `id = "reviewer"` | "shadowing the built-in reviewer — ensure citation-backed review mandate is preserved" |
| `shadow_warn_executor` | `id = "executor"` | "shadowing the built-in executor — verify the 5-phase workflow is present" |
| `shadow_warn_researcher` | `id = "researcher"` | "shadowing the built-in researcher — verify evidence-citation mandate" |
| `shadow_warn_generic` | `id = "generic"` | "shadowing the built-in generic role — this is the fallback for unknown role ids; verify deliberate intent" |

The catalog is intentionally shadow-only. Tool-related advisories are **not** surfaced at POST time: missing tools (declared but not registered anywhere) are detected lazily at DECIDE and surface via `ToolDeclaredButMissing` events per §D3; reporting them at POST would require a tool-registry query the POST handler deliberately does not make, and would contradict the lazy-validation decision. Empty-tools-as-unrestricted is the documented default for built-in `executor` / `researcher` roles and not a noise-worthy advisory for operator writes either.

UI fallback for unknown codes: render `message` under a generic **Advisory** heading with `code` in small text. Adding a new code server-side is non-breaking.

### DELETE success response

```json
{
  "role_id": "pr-reviewer-valkey",
  "retracted_at": 1730000000000,
  "retracted_by": "op-abc",
  "warnings": []
}
```

Timestamp source: `retracted_at` equals `AgentRoleRetracted.at` from the event the handler emitted (or the pre-existing event on idempotent repeat — see below). The projection column is written with the same value inside the dual-write transaction; no drift is possible between response body and projection row.

**Idempotent repeat DELETE** (§D7): on DELETE of an already-retracted role, the handler emits **no new event**, reads the existing retracted row, and returns its `retracted_at` and `retracted_by` verbatim with empty `warnings[]`. Status is still 200. Clients observing the same timestamp on two successive DELETEs is the intended signal.

### Timestamp sourcing (POST / PATCH / DELETE)

Every timestamp returned by this API is the **event timestamp** — the value carried on the `AgentRoleDefined` / `AgentRoleRetracted` event the handler emitted, set at command-handler time before projection write. Projection columns (`defined_at`, `retracted_at`) are written with the same value inside the same transaction as the event append. No divergence is possible between event-log, projection, and response body. On idempotent repeat operations (repeat DELETE per §D7), the returned timestamp is the original event's `at`, not a new wall-clock read.

### GET list response

No pagination in v1 — bounded set per project.

```json
{
  "items": [
    {
      "role": { /* AgentRole */ },
      "source": "custom",
      "shadows_builtin": null,
      "defined_at": 1730000000000,
      "defined_by": "op-abc"
    },
    {
      "role": { /* built-in reviewer AgentRole */ },
      "source": "builtin",
      "shadows_builtin": null,
      "defined_at": null,
      "defined_by": null
    }
  ],
  "total": 6,
  "has_more": false
}
```

`source` values:
- `"builtin"` — one of the five compile-time roles, no shadow active for this project
- `"custom"` — operator-defined role with no built-in counterpart id
- `"custom_shadow"` — operator-defined role whose id matches a built-in (a shadow is active)

GET list does NOT carry per-item ETags; clients needing an ETag for `If-Match` on PATCH fetch the single-resource GET, which responds with `ETag: "<defined_at>"`. Per-item ETags in the list envelope would conflict with HTTP's single-resource-header convention and add no capability the per-role GET does not already provide.

`?source=` query filters the returned `items[]`. Accepted values are exactly the three `source` strings (`builtin`, `custom`, `custom_shadow`) plus `all` (the default when the param is absent). `total` reflects the filtered count, so the filtered response satisfies `total == items.len()` (since `has_more` is always `false` in v1).

### PATCH semantics

- Body is a JSON Merge Patch over `AgentRole` fields.
- Mutable fields: `name`, `description`, `system_prompt`, `tools`, `forbid_all_tools`, `max_context_tokens`, `response_shape`.
- Immutable fields: `id`, `tier`. Attempting to change either returns 422 `ImmutableField`.
- Emits `AgentRoleDefined` on success (latest-wins projection handles update; no `Updated` variant).
- Structural validation re-runs on the merged result.
- **`If-Match` ETag** — Server emits `ETag: "<defined_at_ms>"` (quoted opaque-tag per RFC 7232 §2.3) on every POST / PATCH / single-resource GET 2xx response. Client echoes the exact quoted value in `If-Match` on PATCH. Example: `If-Match: "1730000000000"`. Mismatch returns 412 Precondition Failed. Request without `If-Match` is accepted — lost-update protection is opt-in on the client.
- "Idempotent" per §D6 means: replaying the identical PATCH body produces the identical projection state, not HTTP-level dedupe.

### Prompt normalization (§D13) occurs pre-validation

The server applies BOM strip / CRLF→LF / final-line trailing-whitespace trim to `system_prompt` before structural validation runs. The normalised string is what the validator sees, what's stored, and what `assembled_prompt_for_role` reads at DECIDE.

### Scripted / CLI path

No new cairn-app subcommand in v1. Scripted registration uses `curl` against the HTTP API, identical in shape to plugin registration:

```bash
cat > role.json <<EOF
{
  "id": "pr-reviewer-valkey",
  "name": "Valkey PR Reviewer",
  "tier": "standard",
  "description": "...",
  "system_prompt": "...",
  "tools": ["post_inline_comment", "post_summary_comment", "knowledge_search"],
  "response_shape": "procedural_artifact"
}
EOF

curl -X POST "$CAIRN_URL/v1/projects/$TENANT-$WORKSPACE-$PROJECT/agent-roles" \
  -H "Authorization: Bearer $CAIRN_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d @role.json
```

PR-E (review-agent deployment) uses this path.

## Runtime Resolution Delta

### Service surface

```rust
pub struct ResolvedRole {
    pub role: AgentRole,
    pub source: RoleSource,
    pub shadows_builtin: Option<String>,
    pub defined_at: Option<u64>,   // None for built-ins
    pub defined_by: Option<OperatorId>,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RoleSource { Builtin, Custom, CustomShadow }

pub enum SourceFilter { All, Builtin, Custom, CustomShadow }

#[async_trait]
pub trait AgentRoleService: Send + Sync {
    async fn define(&self, project: &ProjectKey, role: AgentRole, actor: OperatorId)
        -> Result<ResolvedRole, AgentRoleError>;
    async fn retract(&self, project: &ProjectKey, role_id: &str, actor: OperatorId)
        -> Result<(), AgentRoleError>;
    async fn resolve(&self, project: &ProjectKey, role_id: &str)
        -> Result<AgentRole, AgentRoleError>;
    async fn list(&self, project: &ProjectKey, filter: SourceFilter)
        -> Result<Vec<ResolvedRole>, AgentRoleError>;
}
```

`list` returns the merged set: for a given project, every active custom row plus every built-in that is NOT shadowed by a custom row. Shadowed built-ins are replaced by their custom_shadow counterpart in the returned vec (the shadow *is* the effective role). The HTTP `GET` list handler wraps each `ResolvedRole` into the wire-envelope shape shown in §HTTP Surface Delta.

`SourceFilter::All` returns everything. `Builtin` returns only rows whose `source == Builtin`. `Custom` returns both `Custom` and `CustomShadow`. `CustomShadow` returns only `CustomShadow`.

### Resolve algorithm

```rust
async fn resolve(&self, project: &ProjectKey, role_id: &str) -> AgentRole {
    // 1. Projection lookup (active row).
    if let Some(row) = self.read_active(project, role_id).await? {
        return row.role;
    }
    // 2. Built-in fallback.
    if let Some(role) = default_roles().into_iter().find(|r| r.role_id == role_id) {
        return role;
    }
    // 3. Generic fallback (verbatim, id = "generic").
    default_roles()
        .into_iter()
        .find(|r| r.role_id == "generic")
        .expect("generic role must exist per #775")
}
```

§D7 identity: step 3 returns the generic role **verbatim** (its `role_id` in the returned `AgentRole` is `"generic"`, not a synthetic carrier for the caller's id).

### `ctx.agent_type` and `role.id` can diverge on fallback

Orchestrator runs carry `ctx.agent_type: String` (the *requested* role id at run-start time) through every DECIDE event for correlation. When `resolve` falls through step 3 (the requested role is retracted or unknown), the returned `role.id` is `"generic"` while `ctx.agent_type` remains the requested id. This is intentional — downstream event consumers (SSE, run-detail UI) need the original id for correlation (which role did the operator attach to this webhook?), while the DECIDE-loop behaviour must match whatever is actually running (`"generic"`). The two are NOT unified; instead, a new optional field surfaces the divergence in the run-level events:

```rust
// Added to RunStarted / DecideCompleted events (existing variants, new optional field):
pub fallback_from: Option<String>,  // Some("pr-reviewer-valkey") when the requested role resolved to a different id
```

Set by the DECIDE pipeline on the first iteration where `resolved_role.role_id != ctx.agent_type`. Run-detail UI surfaces it as an advisory badge: *"requested `pr-reviewer-valkey`, running as `generic` (role retracted)."*

### Orchestrator call-site pivot (PR-C)

Six concrete changes in `crates/cairn-orchestrator/src/decide_impl.rs`:

1. **Tool-allowlist filter** (lines 279–287): `default_roles().iter().find(...)` → `runtime.agent_roles.resolve(&ctx.project, &ctx.agent_type).await`. Read `role.tools` (renamed from `allowed_tools` per §D10). Allowlist semantics per §D3: `forbid_all_tools=true` → empty tool set; `forbid_all_tools=false` AND `tools=[]` → unrestricted; `forbid_all_tools=false` AND `tools=[...]` → filter to listed ids.

2. **`build_system_prompt`** (line 678): `assembled_prompt_for(agent_type)` → `assembled_prompt_for_role(&resolved_role)`. Dispatch inside `assembled_prompt_for_role` branches on `role.tier == AgentRoleTier::Orchestrator` to decide base prepend (§Prompt Contract + §D11).

3. **User-message memory hint** (lines 963–978): `response_shape_for(&ctx.agent_type)` → `resolved_role.response_shape`. The resolved role is threaded from site 2 into the user-message builder so the hot path resolves at most once per DECIDE.

4. **User-message footer** (lines 985–1011): same — read `resolved_role.response_shape`.

5. **`spawn_subagent_tool_def`** (lines 1297–1349): retire the process-lifetime `OnceLock<Vec<String>>` static cache. Thread `&ProjectKey` into the function (callable via `build_decide_pipeline` which already has `ctx.project`). Query via the per-run snapshot (§D14 layer 2):

    ```rust
    let roles = ctx.agent_role_list_cache
        .get_or_try_init(|| async {
            runtime.agent_roles.list(&ctx.project, SourceFilter::All).await
        })
        .await?;
    let role_enum: Vec<String> = roles.iter()
        .map(|r| r.role.role_id.clone())
        .filter(|id| id != "orchestrator")
        .collect();
    ```

    First DECIDE per run pays one `list` call; subsequent turns reuse the snapshot. A newly-defined role takes effect on the *next* run, not mid-run.

6. **`response_shape_for()`** (`agent_roles.rs` line 771): deleted entirely. Every caller reads `role.response_shape` off the resolved role. The pinning contract test `default_roles_response_shapes_match_table` is retired; replacement contract test asserts `resolve(&project, id).response_shape` matches the expected shape for every built-in id.

### `OrchestrationContext` additions

Two new fields land in PR-A (default-initialised), populated in PR-C. Both are wrapped in `Arc` because `OrchestrationContext` carries `#[derive(Clone)]` today (see `crates/cairn-orchestrator/src/context.rs:23–27`) — raw `Mutex` and `OnceCell` are not `Clone`, so a bare field would break the derive. `Arc<T>` is `Clone` regardless of `T`, and the `Arc` ensures clones within the same run share one backing cell / set rather than getting disconnected copies:

```rust
use std::sync::{Arc, Mutex};
use std::collections::HashSet;
use tokio::sync::OnceCell;

pub struct OrchestrationContext {
    // ... existing fields ...

    /// Per-run dedup for ToolDeclaredButMissing emission. Populated at
    /// the allowlist-filter site (PR-C site 1) the first time a
    /// `(role_id, tool_id)` pair is seen for this run; subsequent
    /// DECIDE iterations skip re-emit. Lives here (NOT on ToolContext,
    /// which is built per-call and would lose state between DECIDE
    /// turns). `Arc` so clones of OrchestrationContext across
    /// checkpoint rebuilds share the same dedup set.
    pub declared_but_missing: Arc<Mutex<HashSet<(String, String)>>>,

    /// Per-run snapshot of the project's spawnable role list, taken on
    /// the first DECIDE via `AgentRoleService::list`. Reused for the
    /// rest of the run (§D14 layer 2). `OnceCell` because it is
    /// fill-once. `Arc` so clones of OrchestrationContext share the
    /// same cell — otherwise each clone would refill, defeating the
    /// memoisation.
    pub agent_role_list_cache: Arc<OnceCell<Vec<ResolvedRole>>>,
}
```

Both fields default-initialise to empty (`Arc::new(Mutex::new(HashSet::new()))` / `Arc::new(OnceCell::new())`) at run construction. Clones within the same run share the backing state; at run-end the last `Arc` is dropped and the state is reclaimed. No explicit "invalidated with the run" step is needed — ref-counting handles it.

**Context-lifetime note.** `OrchestrationContext`'s struct-level comment says it's "built once per run (or rebuilt from a checkpoint on resume) and passed by reference to all three phases." On resume, the rebuild constructs a fresh context — meaning a fresh `Arc` for these two fields. That's correct for the dedup set (resume is a new run-leg; previous emissions remain in the event log for observability and a second-leg rediscovery emission is acceptable) and correct for the list cache (the operator may have POSTed / retracted during the pause). If operators want strict run-leg-wide dedup across resume, PR-C could thread the dedup set through checkpoint payload — explicitly out of scope for v1; noted as a future refinement.

### `ToolDeclaredButMissing` emission

At site 1 (the allowlist filter), when a tool id in `role.tools` is not present in the current tool registry:

1. Acquire `ctx.declared_but_missing` (short critical section).
2. If `(role.id, tool_id)` already in the set: skip emit, drop the tool from the filter.
3. Else: insert into the set, emit `ToolDeclaredButMissing { run_id: ctx.run_id, project: ctx.project.clone(), role_id: role.id.clone(), tool_id: tool_id.clone(), at: now_ms() }`, drop the tool from the filter.

The filter then proceeds with whatever subset remains. Missing tools are silently absent from the tool set the LLM sees — the event is the operator-facing signal.

## UI Delta

New and updated pages under `/agents/`:

- **`/agents/:project`** — existing role list, now surfaces a merged view with `source` badges (builtin / custom / custom_shadow). "New role" button routes to editor. URL is tenant-scoped via the operator's active project scope (per CLAUDE.md Multi-Tenancy).
- **`/agents/:project/new`** — editor form (see §Editor form layout).
- **`/agents/:project/:id`** — role detail. Current role + **History** panel listing every `AgentRoleDefined` / `AgentRoleRetracted` event for `(project, role_id)` with `defined_by`, `at`, and a prompt diff between consecutive Defined events. Backed by the existing event-log API; no new storage.
- **`/agents/:project/:id/edit`** — same editor as new, prefilled. Uses `If-Match` ETag on submit for lost-update protection across tabs.

### Editor form layout

Two-pane layout — sufficient spec for PR-D to build without guessing, not a wireframe for a designer.

**Left pane — metadata fields (stacked):**

1. `id` (text input, `[a-z0-9][a-z0-9_-]*` mask; 64 char counter; disabled on edit since id is immutable per §D6).
2. `name` (text input, 128 char counter).
3. `tier` (select, values `standard` / `orchestrator` / `generic`; greyed to `standard` unless id matches a built-in per §D11).
4. `description` (textarea, 4 KiB char counter).
5. `response_shape` (radio: Direct answer / Procedural artifact; help text explains the DECIDE-footer branch per §D12).
6. `max_context_tokens` (number input, placeholder shows role-tier default).
7. `tools` (multi-select autocomplete if #799 endpoint returns data; freehand chips input otherwise). Each tool chip renders the `tier` badge from #799 (core/registered/deferred).
8. `forbid_all_tools` toggle (checkbox with label "Forbid all tools"); when on, the `tools` field is disabled and greyed with a help message *"role will run with zero tools; only useful for read-only / text-only roles."*

**Right pane — prompt editor:**

- Monospace `system_prompt` textarea, flex-fill height.
- **Sticky counter strip** along the top: `system_prompt: X / 64 KiB · total body: Y / 128 KiB`. Both turn red past the cap.
- **Section-indicator rail** along the right edge: badges, one per required section. Each badge shows `present ✓` / `missing ✗` / `insufficient (1/3 bullets)` / `insufficient (1/2 phases)`. Clicking a badge scrolls the textarea to the section (or to the `suggested_insert_offset` when missing — see §Structural-validation failure surface below). **Rail length**: renders 2 badges (`## Completion criteria`, `## What not to do`) iff the client-side form state has `tier === 'orchestrator' && id === 'orchestrator'` (the orchestrator-shadow case per §D11); otherwise renders 5 badges. The switch is live: as the operator types the id / changes the tier, the rail recomputes. During transient invalid states (e.g. id=`orchestra` with tier=`standard`) the 5-badge layout renders.
- **Anti-pattern warnings** appear inline as yellow-background ranges when the regex matchers fire client-side. Server is authoritative; client renders preview only.

**Submission row — bottom:**

- **Save** (POST on new, PATCH on edit).
- **Cancel** — prompts if form is dirty (see §Draft persistence).
- On `custom_shadow` edit: extra **Restore built-in** button (see §Action affordances) in red, outside the main save button group.

**Scope selector interaction:** if the operator changes project via the global scope selector while the form is dirty, a `beforeunload`-style modal prompts *"Discard changes to this role?"* before navigating. See §Draft persistence for the localStorage path that survives refresh.

### Draft persistence

Every keystroke writes a debounced (250 ms) draft to `localStorage`. Key shape:

- **Edit of existing role** — `cairn:agent_role_draft:{tenant}:{workspace}:{project}:edit:{role_id}`. One key per role per browser; last-writer-wins across tabs for the same role is the right behaviour since the operator is editing the same thing in both tabs.
- **New role** — `cairn:agent_role_draft:{tenant}:{workspace}:{project}:new:{tab_uuid}`, where `tab_uuid` is a UUIDv4 generated on editor mount and held in `sessionStorage` for that tab. Two concurrent "new role" tabs in the same project get distinct keys; the mount-time UUID dies with the tab.

On editor mount, if a draft exists and differs from the current server state (for edit) or is non-empty (for new), a banner offers *"Restore draft from HH:MM?"* with [Restore] [Discard] buttons. Draft is cleared on successful save. On creation success, the "new"-shape key is discarded (the UUID is never reused).

Covers three scenarios:
- **Tab refresh / crash** — draft survives.
- **Scope selector change** — `beforeunload` + draft-on-localStorage means no content is lost.
- **Cross-tab collision** — PATCH's `If-Match` ETag catches lost updates; the operator gets a 412 and a "reload current server state?" prompt.

### Action affordances

| Action | Where | Distinct from | Behaviour |
|---|---|---|---|
| **Retract** | Non-shadow custom row | — | Fires DELETE. Post-retract: new runs fall through to generic (§D7). |
| **Restore built-in** | `source: custom_shadow` row only | Retract | Fires DELETE. Post-retract: new runs fall through to built-in. Copy explicitly states this. |
| **Export JSON** | Role detail page | — | Downloads `{id}.json` in POST-body shape (fields from `role`, omitting the `source` / `defined_at` / `defined_by` envelope wrappers that only exist on GET). Explicitly POST-ready for re-use. |
| **Copy to project…** | Role detail page | — | Modal: see §Copy to project. |

### Copy to project

Modal launched from Export JSON's adjacent button:

1. **Target tenant selector** — defaults to current tenant. Other tenants appear only if the operator's token carries admin scope there; otherwise the selector is locked to current.
2. **Target project selector** — typeahead against the operator's accessible project list in the chosen tenant (backed by `GET /v1/projects`). No freehand project-id input.
3. **Target id** — prefilled from source; editable (operator may want to rename when copying, e.g. `pr-reviewer-valkey` → `pr-reviewer-glide`).
4. On submit, fires POST against the target. Error handling:
   - **201** → toast success, link to the new role's detail page.
   - **409** (existing active role with the same id in target) → modal transitions to a **conflict-resolution** step: fires `GET /v1/projects/:target/agent-roles/:id` to fetch the existing target role and its `ETag`. Renders a side-by-side diff of the source body vs the target body (same diff renderer as the History panel). Operator chooses:
     - **[Overwrite target]** → PATCH against target with `If-Match: "<target_etag>"`. If the ETag went stale between the GET and the PATCH (412 response), the conflict-resolution step re-fetches and shows the updated target diff; operator reconfirms.
     - **[Rename in target]** → returns to the Copy modal with the id field focused, prefilled with `{source_id}-copy`.
     - **[Cancel]** → closes modal; no write.
   - **422** → modal shows structural failures the target rejected (same shape as the editor).
   - **403** → "you don't have admin in target tenant/project" with the missing permission name.
   - **404** → "target project does not exist or is not accessible."

### Retract-during-active-run confirmation

Retract / Restore-built-in buttons first query for in-flight runs bound to `(project, role_id)` (existing `/v1/projects/:project/runs?agent_role_id=X&state=active` endpoint is sufficient) and surface a modal:

> **{N} runs are currently using this role.** Retracting now means those runs continue to completion with the retired prompt; new runs start with the fallback.
>
> [Cancel runs & retract] [Retract only] [Keep role]

No change to the technical behaviour (§D7 — running orchestrations never interrupted); operator sees the consequence before clicking.

### Structural-validation failure surface

Editor form renders 422 response failures inline by `code`:

- `MissingSection` → `span` is always null by definition; UI uses the server-supplied `suggested_insert_offset` to scroll the textarea to where the missing section would fall in canonical order. Renders an inline "Add section" chip at that offset, which inserts a header stub (e.g. `## Workflow\n\n### 1. \n\n`). Canonical order is the order given in the required-sections table. If two or more sections are missing, each gets its own chip at the correct canonical insertion point.
- `InsufficientPhases` / `InsufficientBullets` → counter badge on the section-indicator rail (see §Editor form layout) shows current/required.
- `ProhibitedPattern` → span highlight in the prompt using the returned `span` byte offsets; pattern name in hover text; rule explanation in a side panel.
- `InvalidId` / `InvalidTier` / `ReservedTier` / `InvalidResponseShape` / `InvalidMaxContextTokens` / `ImmutableField` / `ToolsConflict` → field-level error next to the offending input.
- **Unknown failure code** (forward-compat) — renders `message` verbatim as a form-level error banner with `code` shown in small text. Adding a new code server-side is non-breaking.

Size overflow is a separate 413 response, not a 422 failure. UI counter strip turns red client-side when either the `system_prompt` or total-body counter is past the cap, and the Save button is disabled.

Client-side mirrors (size check, id namespace regex) fire on input before submit to catch the obvious cases — server is still authoritative.

### Warning surface

POST / PATCH response `warnings[]` rendered after the save toast:

- Single warning → inline banner under the toast showing `message` with a small `code` chip.
- Multiple warnings → collapsed accordion *"{N} advisories"*, expand to show all.
- **Unknown warning code** → same rendering as known codes; `message` verbatim, `code` shown. UI does not branch on code except for styling on known shadow-warn codes (mild blue) vs unknown (grey).
- Empty `warnings[]` → no banner at all.

### Dry-run / preview — deferred to v1.1

An editor "Dry-run against a sample PR" button is not in v1. Operators validate prompt quality by wiring the role into a webhook and inspecting the resulting run in the existing run-detail UI — the same path built-in roles are validated through today.

The gap is real: the only failure-discovery loop today is "attach to webhook and watch." The right shape for dry-run is a sandboxed run against a stubbed tool registry — substantial orchestrator work beyond this RFC. Acknowledging the gap here so a v1.1 amendment has a natural landing point.

## Rollout

Five gates, each independently mergeable. Each step is idempotent and can sit in production for a release cycle before the next.

1. **Unflagged shape skeleton (PR-A).** New event variants (`AgentRoleDefined`, `AgentRoleRetracted`, `ToolDeclaredButMissing`), projection `project_agent_roles`, projection registry counters bumped, parity harness updated, `AgentRole.allowed_tools` → `tools` rename with serde alias, new `forbid_all_tools` field. `AgentRoleService` trait + impl wired into `RuntimeServices`. No HTTP, no orchestrator pivot, no UI.

   **Sole wire-level change**: the `agent_description` / `list_agents` built-in tools rename their JSON key from `"allowed_tools"` to `"tools"` so the LLM-visible contract matches the struct field. Built-in role *prompts* are unchanged, and the recorded DECIDE-trajectory regression fixtures (50+ runs across the five built-ins) are re-run after the rename to assert that orchestrator dispatch, tool-call selection, and finish_reason all remain identical with the new key. Acceptance criterion for merging PR-A: the trajectory-regression diff is empty. If any fixture diverges, the rename is held until the divergence is explained — either a fixture refresh with documented reason or a fix.

   The projection-registry exhaustive-match check (per CLAUDE.md RFC-025) enforces the new variants land with a clear Projected/Ephemeral status on merge.

2. **HTTP + orchestrator pivot behind `CAIRN_OPERATOR_DEFINED_ROLES=1` (PRs B + C).** Five endpoints wired (PR-B). Six orchestrator call sites in `decide_impl.rs` pivot to the service (PR-C). Structural validator runs on every POST / PATCH. With the flag unset, `resolve` always falls through step 1 of the projection read (empty projection) and hits step 2 (built-in fallback) — identical to pre-RFC behaviour.

3. **UI pages (PR-D).** `/agents/:project/new`, `/agents/:project/:id`, history panel, editor with `/v1/projects/:project/tools` autocomplete (falls back to freehand if #799 hasn't merged). Feature-flagged on `CAIRN_OPERATOR_DEFINED_ROLES=1`.

4. **Default-on.** Flip the env default to on. Remove the flag at the start of the subsequent release cycle.

5. **PR-E review-agent deployment** (not a cairn PR — operational change on EC2): register project-scoped reviewer roles for valkey and glide lanes, update webhook routing to attach the correct role per lane.

## Implementation Plan (non-normative)

- **PR-A (step 1)**: events + projection + service trait + `allowed_tools → tools` rename (serde alias) + `forbid_all_tools: bool` field + `OrchestrationContext::declared_but_missing` and `agent_role_list_cache` fields + `fallback_from: Option<String>` on run-level events + build-time counter bump. No HTTP, no UI, no orchestrator behaviour change. Core crate changes:
   - `cairn-domain::events` — three new variants (`AgentRoleDefined`, `AgentRoleRetracted`, `ToolDeclaredButMissing`) + `fallback_from` field on existing run events.
   - `cairn-domain::agent_roles` — `AgentRole::tools` rename with serde alias; new `forbid_all_tools: bool` field with `#[serde(default)]`; new `validate_prompt_structure()`.
   - `cairn-tools::builtins::agent_description` and `cairn-tools::builtins::list_agents` — JSON surfaced to the LLM renames `"allowed_tools"` → `"tools"` to match the struct field. Wire-visible to the LLM; unit tests assert the new shape. Documented as a deliberate LLM-contract change; built-in roles keep the same prompts so behaviour is unchanged for them, and custom roles see the consistent name.
   - `cairn-store::projection_registry` — register the three new variants (two `Projected`, one `Ephemeral`) + counters.
   - `cairn-store::{pg,sqlite,in_memory}` — projection table `project_agent_roles` with the uniqueness constraint (§D6).
   - `crates/cairn-orchestrator/src/context.rs` — add the two new `OrchestrationContext` fields (default-initialised).
   - New `crates/cairn-runtime/src/services/agent_roles.rs` — `AgentRoleService` trait impl; `ResolvedRole`, `SourceFilter`, `AgentRoleError` types.
   - `crates/cairn-runtime/src/aggregate.rs` — new `agent_roles: Arc<dyn AgentRoleService>` field on `RuntimeServices` so `AppState` can expose the service to Axum handlers in PR-B. Follows the existing field pattern (each service held as `Arc<dyn Trait>`).
   - `crates/cairn-store/build.rs` — projection-registry count constants bumped to match the three new variants; exhaustive-match assertion + CI `projection-stub-guard` job updated via the registry addition.
   - Parity harness in `cairn-store::tests` extended to assert byte-equality of the new `project_agent_roles` projection across InMemory / SQLite / Postgres backends, mirroring the existing RFC-025 parity contract.
   - Tests: projection round-trip; replay compat with pre-rename snapshots via serde alias; POST→DELETE→POST upsert atomicity on all three backends; `agent_description` / `list_agents` JSON-shape regression tests with the new key name; orchestrator DECIDE-behaviour regression over a fixture suite of recorded runs (asserts that the built-in role pipeline still produces the same tool-calls / finish_reason after the JSON-key rename).
   ~1300 LOC.

- **PR-B (step 2, HTTP)**: handlers in `cairn-app::handlers::agent_roles`, router wiring, OpenAPI spec delta, `AdminRoleGuard` on writes, structural validator in `cairn-domain::agent_roles::validate_prompt_structure` with all regex checks. End-to-end tests covering every status code (200 / 201 / 400 / 401 / 403 / 404 / 409 / 412 / 413 / 422). ~700 LOC.

- **PR-C (step 2, orchestrator pivot)**: the six call-site changes in `decide_impl.rs` (tool-allowlist filter, build_system_prompt, memory hint, footer, spawn_subagent_tool_def, response_shape_for retirement). Retire `OnceLock` in `spawn_subagent_tool_def`; thread `ProjectKey`. `ToolDeclaredButMissing` emission at the allowlist-filter site. Contract test rewrites (`spawn_subagent_role_enum_derived_from_default_roles` → `resolve-based` variant). ~400 LOC including test updates.

- **PR-D (step 3, UI)**: UI pages, API client bindings, Playwright e2e tests. Depends on #799 for tool-autocomplete (falls back gracefully if absent). ~1100 LOC TS.

- **PR-E (step 5)**: operational rollout. Not a cairn PR. Depends on PRs A–D merged and deployed to the target environment.

## Dependencies

- **[#799](https://github.com/avifenesh/cairn-rs/issues/799)** — `GET /v1/projects/:project/tools`. Per-project tool-id listing endpoint for the role editor's tool-autocomplete. Approved with `tier` / `total` fields added. Soft dependency: PR-D's UI falls back to a freehand tool field + inline guidance if #799 hasn't merged by the time PR-D lands.

## Decided

- **D1**: project-scoped only in v1; tenant-scope deferred.
- **D2**: shadowing built-ins allowed from day one, no flag.
- **D3**: lazy tool-id validation; `ToolDeclaredButMissing` at DECIDE time; empty `tools` = unrestricted; explicit `forbid_all_tools: bool` field for forbid-all; no magic strings in `tools[]`.
- **D4**: per-field size caps (`system_prompt` ≤ 64 KiB, `name` ≤ 128, `description` ≤ 4 KiB, `tools[]` ≤ 256 entries) + total body cap ≤ 128 KiB; overflow is 413 with dedicated body shape.
- **D5**: role id namespace `[a-z0-9][a-z0-9_-]*`, max 64 chars (422 `InvalidId`).
- **D6**: POST is create-only with `(project, role_id) WHERE retracted_at IS NULL` uniqueness (409 on active collision); re-POST after retract returns 201 with atomic `retracted_at = NULL` clear; PATCH is update (emits `AgentRoleDefined`, latest-wins semantic).
- **D7**: retract falls back to built-in or generic **verbatim** (`role.id = "generic"`); `ctx.agent_type` and `role.id` may diverge and the divergence is surfaced via `fallback_from` on run events; built-in ids uncretractable (404 on DELETE).
- **D8**: shadowing emits `warnings[]` advisory, does not block the write; warning-code catalog closed set per §HTTP Surface Delta.
- **D9**: `default_roles()` stays compiled-in as the fallback baseline.
- **D10**: `AgentRole.allowed_tools` → `AgentRole.tools`; serde alias on the old name for replay compat; `agent_description` / `list_agents` LLM-visible JSON key renames in lockstep.
- **D11**: shadowing requires `tier` to match the built-in's tier (422 `InvalidTier`); novel ids MUST declare `tier: "standard"` (422 `ReservedTier`); non-standard tiers are reserved to the built-in ids they belong to.
- **D12**: `response_shape ∈ {"direct_answer", "procedural_artifact"}` closed enum; `max_context_tokens ∈ (0, 2_000_000]`; 422 on violation.
- **D13**: prompt normalization on storage: BOM strip / CRLF→LF / final-line trailing-whitespace trim. Applied once in the HTTP handler before validation + event emit. Replay is passthrough. No templating.
- **D14**: no cross-node warm cache in v1; per-DECIDE `resolve` is a direct projection read; per-run `list` snapshot cached on `OrchestrationContext`.

## Deferred Questions

Revisit when the question materialises:

- **Tenant-scoped roles.** Operators with many projects may want one definition point. Implementation is a new event variant and a second resolution tier.
- **Prompt templating.** `{{project_id}}` / `{{tenant_id}}` interpolation. Not needed for the review agent; add when a concrete use case demands it.
- **Role versioning with immutable refs.** Event log records every change. A concrete use case ("pin the production reviewer to role@abc123") would motivate a dedicated feature.
- **Partial override of built-ins.** "Take the built-in `reviewer` prompt and append this extra section." Clean shadow is simpler; reconsider if operators start copy-pasting built-ins.
- **Role marketplace.** Publish and install community roles via the RFC 015 catalog surface. Natural extension; not v1.
- **Dry-run / prompt preview.** Sandboxed run against stubbed tool registry from the editor. Real gap for v1; v1.1 target.
- **Warm in-memory cache with cross-node invalidation.** If profiling shows projection read is the bottleneck. Until then, §D14 keeps resolve direct.
- **SSE invalidation events on the role-editor tool-autocomplete.** When the live-fleet UI ships, plumb a WebSocket / SSE stream of `PluginReady` / `PluginRetracted` into the editor so tool-list updates don't require a page reload. Tracked as a follow-up to #799.

## References

- [RFC 007](./007-plugin-protocol-transport.md) — capability families and tool protocol.
- [RFC 015](./015-plugin-marketplace-and-scoping.md) — per-project scoping model.
- [RFC 018](./018-agent-loop-enhancements.md) — the `AgentRole` data model (currently compile-time).
- `crates/cairn-domain/src/agent_roles.rs` — current built-in implementation.
- `crates/cairn-orchestrator/src/decide_impl.rs` — the six call sites PR-C pivots.
- `crates/cairn-app/src/knowledge_provider_routes.rs` — similar small project-scoped configuration endpoint; shape reference for §HTTP Surface Delta.
- [#774](https://github.com/avifenesh/cairn-rs/pull/774) — per-iteration footer branches on `response_shape`, demonstrating why roles are load-bearing for the loop.
- [#775](https://github.com/avifenesh/cairn-rs/pull/775) — sub-agent identity + `AgentRole.description` + `ResponseShape` + generic role.
- [#776](https://github.com/avifenesh/cairn-rs/pull/776) — `list_agents` / `agent_description` tools; `OnceLock` in `spawn_subagent_tool_def` that PR-C retires.
- [#794](https://github.com/avifenesh/cairn-rs/pull/794) — response envelope shape precedent (`{items, total, has_more}`).
- [#799](https://github.com/avifenesh/cairn-rs/issues/799) — per-project tool-id listing endpoint dependency.
