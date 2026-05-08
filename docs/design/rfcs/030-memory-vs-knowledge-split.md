# RFC 030: Memory vs Knowledge Split (Amendment to RFC 029)

Status: draft
Owner: knowledge/retrieval
Amends: [RFC 029](./029-pluggable-knowledge-providers.md), [RFC 015](./015-plugin-marketplace-and-scoping.md), [RFC 007](./007-plugin-protocol-transport.md)

## Summary

RFC 029 introduced a `KnowledgeProvider` capability family and routed every retrieval/ingest call through a single per-project provider slot. In practice, we shipped adapters (`cairn-knowledge-mem0`, `cairn-knowledge-bedrock-kb`) backing two fundamentally different concepts under one contract:

- **Knowledge** is a curated, authoritative corpus operators ingest (Bedrock KB, Confluence, document stores). Agents query it to ground answers in facts. Lifecycle is operator-owned.
- **Memory** is episodic, agent-written context scoped per user/session (mem0, Zep, episodic-rs). Agents read + write it to personalize behavior. Lifecycle is agent-owned.

They share a mechanism (vector search over chunks with a scoring breakdown) but not a concept. Calling mem0 a `KnowledgeProvider` conflates the agent's personal recall with the operator's authoritative corpus — a mistake both at the tool-prompt layer (agents can't distinguish "what did I say to alice?" from "what do our docs say?") and at the operator layer (a project can't have mem0 **and** Bedrock-KB simultaneously, which is the common deployment).

This RFC splits the single `KnowledgeProvider` family into two peer families — `KnowledgeProvider` and `MemoryProvider` — with separate project configuration slots, separate tools at the prompt, and separate scoring policies. The shared plumbing (RFC 007 transport, RFC 029 post-hoc rescoring, `MultiProvider*` dispatch, compliance suite shape-checks) stays mostly reusable.

**RFC 029 §"Deferred Questions" anticipated this amendment** ("`MemoryProvider` as a second capability family alongside `KnowledgeProvider` … designed to accommodate that follow-on without rework"). RFC 030 is that follow-on, landed earlier than planned because the adapter work forced the question.

## Why

Three concrete failure modes shipped with RFC 029's single-family model:

1. **Per-project provider monopoly.** `PUT /v1/projects/:id/knowledge-provider` stores one `provider_ref` per project. An operator using Bedrock KB for company docs who also wants mem0 for per-user memory has to pick. They pick one, the other use case silently degrades.
2. **Agent prompt ambiguity.** One `memory_search` tool routes through `MultiProviderRetrieval` regardless of whether the project's provider is mem0 (memory) or Bedrock (knowledge). The LLM doesn't get to ask "is this episodic or authoritative?" — it just calls `memory_search`. Two tools with distinct descriptions teach the model when to reach for which.
3. **Scoring policy cross-talk.** A project configured with `plugin:bedrock-kb` declares `freshness_decay = NotSupported` (authoritative corpus, freshness doesn't decay the way memory does). A project configured with `plugin:mem0` declares `freshness_decay = Surfaced`. A single scoring policy written by the operator can only target one semantic at a time — recent-biased for memory, authority-biased for knowledge. The policy needs to live per-family.

These aren't hypothetical edge cases; they're the immediate consequence of the naming we chose in RFC 029.

## Scope

### In scope

- Add capability family `MemoryProvider` to RFC 007's `CapabilityFamily` enum as a peer to `KnowledgeProvider`.
- Split wire types: `MemoryQueryParams/Result`, `MemoryIngestParams/Ack`, `MemoryIngestStatusParams/Result`, `MemoryListSourcesParams/Result` mirror the knowledge.* family but are separate types so the two contracts can evolve independently. Shared sub-types (`ChunkRecordWire`, `ScoringBreakdownWire`, `DimensionSupport`, `ScoringDimensionSet`) stay shared because the underlying retrieval shape is identical today.
- Two provider-ref slots on every `ProjectKey`: `memory_provider_ref` and `knowledge_provider_ref`. Distinct configuration surfaces: `PUT /v1/projects/:id/memory-provider` and `PUT /v1/projects/:id/knowledge-provider`.
- Distinct domain events: `MemoryProviderConfigured` + 5 lifecycle events mirroring `KnowledgeProviderConfigured` + family.
- Distinct projection tables: `project_memory_providers` / `memory_ingest_jobs` mirror `project_knowledge_providers` / `knowledge_ingest_jobs`.
- Distinct tools at the agent prompt layer: `memory_search` / `memory_store` dispatch through `MultiProviderMemory`; `knowledge_search` dispatch through `MultiProviderKnowledge`. `memory_store` stays gated by the memory provider's `ingest_capable` **AND** by a new `auto_extract: bool` capability flag — when a memory provider advertises `auto_extract = true` (mem0's default mode, where memories are extracted from conversations by a post-turn hook), `memory_store` is suppressed from the agent prompt entirely. A new `knowledge_ingest` admin-level tool is **out of scope** (knowledge ingest is an operator-driven flow through HTTP, not an agent tool).
- Canonical wire method names locked: `memory.query`, `memory.ingest`, `memory.ingest_status`, `memory.list_sources`. Added to `cairn-plugin-proto::wire::methods` alongside the existing `knowledge.*` constants. Any future vendor-prefixed namespace MUST nest under `plugin.<vendor>.*` to avoid collision with the family-level `memory.*` / `knowledge.*` seat.
- Handshake validator in `cairn-tools` rejects any `InitializeResult.capabilities[]` array containing more than one entry with a retrieval-family type (either `knowledge_provider` **or** `memory_provider`, never both). D1 forbids dual-family plugins; the validator enforces it at the wire seam. Compliance-suite check added to lock the invariant.
- Distinct scoring policies per family: `PUT /v1/projects/:id/scoring-policy` splits into `…/memory-scoring-policy` and `…/knowledge-scoring-policy`. Each is validated against the resolved provider of that family. The `PUT` response body returns `warnings[]` when a non-zero weight is set on a dimension the resolved provider marks `not_supported` (soft warning on write, hard rejection only when the policy would produce a mathematically meaningless score — see D3).
- Distinct runtime post-hoc rescorers: `PostHocRescorer<G, C>` takes a `family: CapabilityFamily` parameter on construction; the memory-family instance **skips** `multi_neighbors` entirely (episodic memory chunks are not provenance-graph nodes, so `graph_proximity` always collapses to 0.0 and the call is wasted latency). The knowledge-family instance preserves the existing batched lookup.
- `SourceCredibilityLookup` trait takes `family: CapabilityFamily` on its `lookup` call so a single projection-backed implementation can answer both "per-source authority" (knowledge) and "per-user trust" (memory) questions against the right column.
- Shared wire types (`ChunkRecordWire`, `ScoringBreakdownWire`) carry rustdoc annotations naming which dimensions are primary for which family. The shared `document_id: KnowledgeDocumentId` field is renamed to `document_id: DocumentId` (new family-neutral newtype in `cairn-domain::ids`; `KnowledgeDocumentId` becomes a `pub type` alias for back-compat during the PR series). `SourceTypeWire`'s duplicate `JsonStructured`/`StructuredJson` variants are collapsed to `StructuredJson` only (pre-v1 — no wire consumers outside this repo).
- Diagnostics carry `family: CapabilityFamily` on `KnowledgeQueryDiagnostics` and the new `MemoryQueryDiagnostics`. The runtime sets this unconditionally before emitting; providers cannot spoof the field (host overwrites on the return path). Every audit entry (`RetrievalResultWire`) gains an unambiguous origin marker. Cross-family score comparison in operator UI is **undefined behaviour** — diagnostics carry the family marker precisely so consumers can bucket before comparing.
- Compliance suite split: `cairn-memory-compliance` + `cairn-knowledge-compliance`. Shared shape checks (wire round-trip, field presence, tri-state, runtime-owned overwrite, error shape, diagnostics markers) refactor into a `cairn-provider-compliance-core` helper crate; the two family-specific suites feed their respective fixtures through it. Adds a new shared check: `check_no_dual_family_capabilities` — an adapter that returns both `memory_provider` and `knowledge_provider` in its `InitializeResult.capabilities[]` MUST be rejected at handshake.
- `*ProviderConfigured` events gain `is_bootstrap: bool`. cairn-default's project-creation-time emission sets `is_bootstrap = true`; operator-driven `PUT …-provider` calls set `is_bootstrap = false`. Auditors filtering "operator-chosen provider configuration" project on `is_bootstrap = false`; the default-seeding event stays auditable but distinguishable.
- New unified read endpoint `GET /v1/projects/:id/providers` returning both resolved `{memory_provider_ref, knowledge_provider_ref, memory_snapshot, knowledge_snapshot}` in one JSON body, so CLI auditing doesn't require two separate calls.
- New unified read endpoint `GET /v1/projects/:id/ingest-jobs?family=memory|knowledge|all` backed by a database view `v_all_ingest_jobs` (defined in the V019 migration) that unions `memory_ingest_jobs` + `knowledge_ingest_jobs` with a `family` column. Required for regulated operators doing cross-family audit queries.
- New `GET /v1/projects/:id/{memory,knowledge}-scoring-policy/valid-dimensions` endpoint returning the dimension list the currently-resolved provider surfaces, so UI editors can grey out invalid controls before `PUT`.
- Startup health check: on boot, scan `project_knowledge_providers WHERE provider_ref LIKE 'plugin:%' AND kind = 'configured'`; for each configured plugin, cross-check the adapter manifest's advertised family. Emit `WARN` + a `KnowledgeProviderFamilyMismatch` event per mismatch, surfaced in the operator UI project-health view. Pattern mirrors `assert_no_stubs_for_persistent_backend`.

### cairn-default: the TODO seam

Per the user directive: **cairn-default serves both families, marked clearly as TODO.**

The in-process `InMemoryRetrieval` + `InMemoryIngest` already exists from RFC 003. Under RFC 030, it serves as the default provider for **both** capability families — a project with no explicit provider configuration gets cairn-default for memory and cairn-default for knowledge. The target registration path is **`ProjectCreated` command-handler emission**, not per-boot registration (see Event-Sourcing Delta for the full shape + the pre-RFC-030 idempotent backfill). Short-hand references elsewhere in this RFC that say "at boot" mean "present from the project's first moment of existence"; the actual emission happens at project creation and backfills once for projects that pre-date this RFC. This is deliberate:

- **Operators installing cairn today get a working experience without choosing a provider.** If we split cairn-default to only one family, the other family has no out-of-box option, which breaks the RFC 001 "runs locally with zero setup" promise.
- **Cairn's roadmap is to bring a dedicated default knowledge context** (Bedrock-style ingest pipeline owned by cairn, separate from the agent's episodic memory). That work is tracked but not scoped by this RFC.
- **Until the dedicated knowledge default ships, the dual-family registration is a bridge**. Every call site where cairn-default serves as a knowledge provider must carry a `// TODO(RFC 030): replace with dedicated knowledge default when that context lands` marker.

Concretely:

- `AppState` grows a second `knowledge_retrieval: Arc<InMemoryRetrieval>` alongside `retrieval`, backed by a **separate `InMemoryDocumentStore`** — not a shared Arc. Same concrete type, distinct state. This matters: without the split, any `memory_store` write lands in the same store that `knowledge_search` queries, and the future dedicated-knowledge-default RFC would inherit a store that has been serving both families as a shared mutable bag. Separate stores from day one mean the seam is real; swapping one slot to a dedicated implementation leaves the other untouched.
- The `snapshot_for_provider_ref("cairn-default")` helper stays unchanged; both families resolve it to the same in-proc snapshot shape, but each family's `AdapterState` holds a distinct `Arc<InMemoryRetrieval>`.
- Integration tests verify: (a) default-default projects (no operator config) get cairn-default for both; (b) a project with `knowledge_provider = plugin:bedrock-kb` + unconfigured memory still gets cairn-default for memory; (c) a project with both explicitly configured routes each call to the right adapter; (d) `memory_store` writes are invisible to `knowledge_search` and vice versa (the state-isolation invariant).
- Operator marketplace / UI disambiguation: the knowledge-family `cairn-default` entry carries `status: "placeholder"` and a display name suffix (`cairn-default (placeholder — not a curated corpus)`). The memory-family entry stays `status: "stable"`. An operator reading the list sees both registrations but can't mistake the knowledge-family one for a vetted production adapter.

### Out of scope

- Cross-family federation (a search tool that queries memory + knowledge in one call — operators compose manually until we see signal).
- Auto-migration from an operator's existing `plugin:mem0` config on `knowledge_provider` → `memory_provider`. Existing configs before RFC 030 lands are **invalidated**; operators reconfigure via the new endpoints. This is acceptable because we have no production operators yet (RFC 029 itself is pre-v1).
- Dedicated cairn-default knowledge context implementation (tracked separately — see "cairn-default: the TODO seam").
- Removing the `knowledge_` prefix from existing compliance suite test names. Renames land in a follow-up janitorial pass.

## Decisions

Every non-obvious call locked here so the implementation PRs can proceed without re-opening the design.

### D1 — two separate capability families, not a subtype

The alternative was `KnowledgeProvider { family: Memory | Knowledge }` — one family discriminating at handshake time. Rejected because:

- Plugin host enumeration (`tools.list`, capability introspection) benefits from knowing the family at manifest time, not just at runtime. An operator filtering "show me available memory providers" should see the list before spawning any plugin.
- Two families make cross-family migration explicitly a plugin-rewrite rather than a silent config change. An adapter author that wants to support both concepts ships two binaries, with two distinct compliance-suite runs. That's healthier than one binary with a runtime switch.
- The RFC 015 marketplace category selector is already discriminating. Adding two categories costs nothing; a subtype discriminator would require marketplace plumbing to hide the wrong subset per family filter.

### D2 — memory_store stays an agent tool, gated by `auto_extract`; knowledge ingest does not

Memory write patterns split into two camps:

- **Explicit-store**: the LLM reasons "alice told me she likes coffee, I should remember that" and invokes `memory_store`. First-class tool call. Works for systems like Zep where writes are intentional.
- **Auto-extract**: the memory backend observes every turn and extracts memories via its own post-hook (mem0's default). The LLM never invokes a tool; memories appear server-side between turns.

Exposing `memory_store` as a tool when the backend is auto-extract is worse than not exposing it — the LLM learns to decide when to remember, which is precisely the unreliable behaviour auto-extract exists to sidestep. The capability handshake therefore carries a new `auto_extract: bool` flag; when `true`, `memory_store` is suppressed from the agent prompt regardless of `ingest_capable`. An adapter declares `auto_extract = true` when its backend writes from a post-turn hook rather than an explicit API call.

`knowledge_ingest` is not a first-class tool at all — curating the authoritative corpus is an operator responsibility, fired from HTTP or a batch import job, not from the LLM's tool list. A future amendment might add `knowledge_ingest` for specific librarian-agent workflows; see Deferred Question 3.

### D3 — scoring policies split by storage key, shared weights shape

Both `MemoryScoringPolicy` and `KnowledgeScoringPolicy` use the same `ScoringPolicy` / `ScoringWeights` struct. The load-bearing reason to split is **storage-key isolation**: one `scoring_policy_json` key under RFC 029 meant an operator writing the memory policy would clobber the knowledge one and vice versa. Two keys (`memory_scoring_policy_json`, `knowledge_scoring_policy_json`) fix that directly.

The RFC does **not** prescribe semantic differences in which dimensions matter for which family. Concrete counter-example: a Confluence-backed knowledge provider with daily-updated pages cares about `freshness_decay` exactly as much as mem0 does; claiming "freshness is usually 0 for knowledge" was a just-so story. Operators set weights per their workload; the RFC's only structural claim is that the two policies are stored under distinct keys so they don't overwrite each other.

Cross-family score comparison is **undefined behaviour**. When an agent calls `memory_search` and `knowledge_search` in the same turn, the scores in each response are computed under their own family's weights and are on incomparable scales. Diagnostics carry `family: CapabilityFamily` precisely so UI / LLM consumers can bucket before comparing. The RFC does not ship a cross-family normalization pass; operators who need comparable scores across families write their own post-processing.

### D4 — `ResolvedProviderSnapshot` stays per-family

`VisibilityContext.resolved_knowledge_provider` becomes `resolved_knowledge_provider` + `resolved_memory_provider`. The tool-visibility predicate `is_tool_visible` consults the right field by tool name:

- `memory_store` → `resolved_memory_provider.ingest_capable`
- `memory_search` → `resolved_memory_provider` (must be `Some`)
- `knowledge_search` → `resolved_knowledge_provider` (must be `Some`)

`GATABLE_BUILTINS` grows from `["memory_store"]` to `["memory_store", "memory_search", "knowledge_search"]`, and `is_tool_visible` learns to route by tool name. That's a pure predicate fix, no new infrastructure.

### D5 — cairn-default is explicitly dual-registered, not implicitly

Under D5, cairn-default is not "the fallback if nothing else is configured" — it's explicitly registered as the default provider for both families at boot. The difference matters for the event log: a new project emits two `*ProviderConfigured` events at creation (one per family), not zero. This gives every project an auditable "provider configured" row, even when the operator never explicitly picked.

### D6 — compliance suite split, shared core

The existing `cairn-knowledge-compliance` crate's six shape checks (wire round-trip, required fields, tri-state, runtime-owned overwrite, error shape, diagnostics markers) apply unchanged to both families. Extract into `cairn-provider-compliance-core` with the checks parameterised over a family marker; build `cairn-memory-compliance` + keep `cairn-knowledge-compliance` as thin shells that supply fixtures + invoke the shared core.

### D7 — adapter repo rename

`cairn-knowledge-mem0` is misnamed under this RFC. Rename to `cairn-memory-mem0`. The GitHub rename is reversible; the crate name change is a breaking change, but pre-v1 there are no external consumers. Binary name changes from `cairn-knowledge-mem0` to `cairn-memory-mem0`; operator install instructions update in the README.

`cairn-knowledge-bedrock-kb` stays as-is — it is a knowledge provider.

## Event-Sourcing Delta

RFC 029 landed 6 events for knowledge providers. RFC 030 mirrors them for memory providers:

| Knowledge (existing, RFC 029) | Memory (new, RFC 030) |
|---|---|
| `KnowledgeProviderConfigured` | `MemoryProviderConfigured` |
| `KnowledgeProviderUnavailable` | `MemoryProviderUnavailable` |
| `KnowledgeProviderCapabilityChanged` | `MemoryProviderCapabilityChanged` |
| `KnowledgeIngestSubmitted` | `MemoryIngestSubmitted` |
| `KnowledgeIngestRejected` | `MemoryIngestRejected` |
| `KnowledgeIngestStatusUpdated` | `MemoryIngestStatusUpdated` |

Each new event has its own projection arm, registered as `Projected` in `projection_registry.rs`. Tables:

- `project_memory_providers` — same schema as `project_knowledge_providers`, different name.
- `memory_ingest_jobs` — same schema as `knowledge_ingest_jobs`.
- `v_all_ingest_jobs` — read-only database view defined in V019 as `SELECT *, 'memory' AS family FROM memory_ingest_jobs UNION ALL SELECT *, 'knowledge' AS family FROM knowledge_ingest_jobs`. Backs the unified ingest-jobs HTTP endpoint so regulated operators have one cross-family audit query.

V019 migration uses `CREATE TABLE IF NOT EXISTS` + `CREATE INDEX IF NOT EXISTS` guards throughout, mirroring V018, so a partially-applied V018 followed by V019 is safe to re-run. The migration additionally copies any pre-existing `scoring_policy_json` default-store rows into `knowledge_scoring_policy_json` (one-time backfill; RFC 029 stored the single policy under the old key) so operators who wrote a policy under RFC 029 don't silently lose it at the 029→030 cut.

No `kind` discriminator column is added to the provider or ingest-job tables; separation is by table name. This matches RFC 025's existing per-event-type projection convention.

**Bootstrap events are emitted at `ProjectCreated` time, not at boot.** The dual-family cairn-default registration fires exactly two events (`MemoryProviderConfigured(cairn-default, is_bootstrap=true)`, `KnowledgeProviderConfigured(cairn-default, is_bootstrap=true)`) when a project is created, and never again for that project. A boot-time backfill for projects created **before** RFC 030 (dogfooding state) uses an idempotency check: scan `project_memory_providers` for missing `kind = "configured"` rows and emit exactly one event per gap, matching the pattern `tenant_role_backfill.rs` already established. Subsequent boots see no gaps and emit nothing.

`*ProviderConfigured` events gain an `is_bootstrap: bool` field. Auditors filtering "operator-chosen provider configuration" project on `is_bootstrap = false`. Bootstrap events stay in the log for audit completeness but are trivially distinguishable.

Projection count moves from 135 → 141.
Event-exhaustiveness test count moves from 168 → 174 (6 new `Memory*` variants; PR-B updates `assert_all_variants_covered` match arms, `all_variants()` constructors, and the count assertion in lock-step).

## HTTP Surface Delta

New:
- `PUT /v1/projects/:project/memory-provider` — body `{"provider_ref": "cairn-default" | "plugin:<id>"}`
- `PUT /v1/projects/:project/memory-scoring-policy` — body is `ScoringPolicy` JSON. Response body includes `warnings[]` when non-zero weights target dimensions the resolved provider declares `not_supported`.
- `PUT /v1/projects/:project/knowledge-scoring-policy` — body is `ScoringPolicy` JSON. Same `warnings[]` behaviour.
- `GET /v1/projects/:project/providers` — single-shot read returning both slots + both resolved-provider snapshots. The dual-PUT sequence during project bootstrap is inherently split across two writes (one per family); this read lets operators confirm the post-write state atomically.
- `GET /v1/projects/:project/memory-scoring-policy` — returns the currently stored memory scoring policy (or an empty object when none is set, falling back to `ScoringPolicy::default()` at query time). Mirror `GET …/knowledge-scoring-policy` for knowledge. Pair with the existing `PUT` endpoints so CLI tooling can round-trip edits.
- `GET /v1/projects/:project/memory-scoring-policy/valid-dimensions` — returns the dimensions the currently-resolved memory provider surfaces; lets UI editors grey out invalid controls before `PUT`. Knowledge equivalent symmetrically.
- `GET /v1/projects/:project/ingest-jobs?family=memory|knowledge|all` — unified ingest-job listing backed by the V019 `v_all_ingest_jobs` view. Required for cross-family audit queries.

Renamed (with a redirect window, not an abrupt 410):
- `PUT /v1/projects/:project/scoring-policy` → `PUT /v1/projects/:project/knowledge-scoring-policy`. The old URL returns **308 Permanent Redirect** with `Location: /v1/projects/:project/knowledge-scoring-policy` so HTTP clients auto-follow with the same method + body. The 308 response survives for at least the entire PR series and until operator CLIs update; a follow-up janitorial change (separately tracked, not gated by this RFC) may later flip it to 410 once we confirm no callers remain. The in-flight deploy window never breaks an existing `PUT`.

Unchanged:
- `PUT /v1/projects/:project/knowledge-provider` — stays as-is.

## Runtime Resolution Delta

`MultiProviderRetrieval` stays on the knowledge path. A new `MultiProviderMemory` type mirrors its shape for the memory path. Agent tools dispatch as follows:

| Tool | Target |
|---|---|
| `memory_search` | `MultiProviderMemory::query` |
| `memory_store` | `MultiProviderMemory::ingest` |
| `knowledge_search` | `MultiProviderRetrieval::query` |

`ConcreteMemorySearchTool` / `ConcreteMemoryStoreTool` stay wired to the memory path (name retention, because the in-proc type name has no meaningful family semantics beyond current convention). A new `ConcreteKnowledgeSearchTool` wraps `MultiProviderRetrieval`.

## Scoring Policy Validation Delta

Today: one validator checks that the stored policy doesn't reference dimensions the resolved knowledge provider declared `NotSupported`.

Tomorrow: two validators — the same code, parameterised over which family's resolved provider snapshot to consult. Either family's `PUT …-scoring-policy` endpoint feeds the same `validate_scoring_policy` helper with the right snapshot.

## Integration Tests (Compliance Delta)

Every RFC 029 integration test that asserted on `KnowledgeProvider*` semantics gains a mirror assertion on `MemoryProvider*`:

1. Capability declaration round-trips — both families, tri-state enforced per-family at handshake.
2. Per-project isolation — two projects with different `memory_provider` configs route to different providers; same for knowledge.
3. **New**: A single project with **both** `memory_provider = plugin:mem0` AND `knowledge_provider = plugin:bedrock-kb` configured routes `memory_search` to mem0 and `knowledge_search` to Bedrock. This is the scenario RFC 030 exists to unblock.
4. Runtime post-hoc rescoring — both families overwrite runtime-owned dims unconditionally.
5. Runtime overwrites provider-injected runtime-owned dimensions — both families.
6. Scoring policy rejection — separate per family.
7. `memory_store` hidden under read-only memory provider (e.g. if a future read-only memory provider ships). Today mem0 is write-capable, so this check is primarily forward-looking.
8. `knowledge_search` hidden when no knowledge provider resolved. This is the distinguishing visibility behaviour — a project with only a memory provider sees no `knowledge_search` tool in the agent prompt.

## Rollout

### Pre-v1 simplification

Because cairn has no production consumers, this RFC **does not** preserve wire compatibility with RFC 029 deployments. Existing event log entries for `KnowledgeProviderConfigured` where the intent was memory (e.g. an operator who configured `plugin:mem0` on `knowledge_provider` because RFC 029 was the only slot) are left in place — they describe what was configured at the time, under the RFC 029 naming. New projects use the split surface. Existing projects that configured mem0 on the wrong slot reconfigure via the new memory-provider endpoint.

### Migration safety rails

The RFC ships two mechanisms that keep a running cairn instance from silently degrading across the cut:

1. **Family-mismatch startup scan.** At boot, cairn scans `project_knowledge_providers WHERE provider_ref LIKE 'plugin:%' AND kind = 'configured'` and cross-checks each adapter's advertised capability family. For every mismatch (e.g. `plugin:mem0` configured on the knowledge slot but the manifest declares `memory_provider`), cairn emits `WARN` to the structured log, appends a new `KnowledgeProviderFamilyMismatch` event to the project's audit trail, and surfaces the mismatch in the operator UI's project health view. The operator's first indication is not a silently-degraded `knowledge_search`; it's a red health badge with a specific reconfiguration command.
2. **V019 scoring-policy default-store backfill.** V019 copies any `scoring_policy_json` rows (RFC 029's single-policy storage key) into `knowledge_scoring_policy_json`. Operators who wrote a policy under RFC 029 retain it on the knowledge family by default; memory-family policies start fresh (the common case: nothing was configured under the old key because there was no memory slot). The backfill is idempotent and re-runs of V019 emit zero additional rows.

The 308-not-410 redirect on `PUT /v1/projects/:id/scoring-policy` (see HTTP Surface Delta) provides the third rail: an operator CLI that hits the old URL during the PR rollout window succeeds transparently via the redirect until the CLI updates.

### Migration notes for RFC 029 adopters (retroactive)

| RFC 029 state | RFC 030 action |
|---|---|
| `knowledge_provider = cairn-default` | Unchanged. cairn-default continues to serve both families (see D5). |
| `knowledge_provider = plugin:bedrock-kb` | Unchanged. Bedrock KB is genuinely a knowledge provider. |
| `knowledge_provider = plugin:mem0` | Reconfigure: `memory_provider = plugin:mem0`, leave `knowledge_provider = cairn-default`. The startup family-mismatch scan flags this automatically. |
| `scoring_policy_json` default-store row | Automatically copied to `knowledge_scoring_policy_json` by V019. No operator action required. |

### PR series

See `RFC 030 Implementation Plan` (appended below as non-normative guidance). Breaking changes land as one coordinated PR set; no deprecation window.

## RFC 030 Implementation Plan (non-normative)

Nine PRs in order, each small enough to review. Every PR keeps the workspace compiling and every test from earlier PRs green.

- **PR-A — wire types + handshake validator.** Split `CapabilityFamily` enum; add `MemoryProviderCapability` + `auto_extract: bool`; duplicate + adjust knowledge wire types into memory wire types in `cairn-plugin-proto`. Lock canonical method names (`memory.query`, `memory.ingest`, `memory.ingest_status`, `memory.list_sources`). Collapse `SourceTypeWire::JsonStructured` duplicate variant. Rename shared `KnowledgeDocumentId` → family-neutral `DocumentId` with back-compat alias. Add handshake-time validator in `cairn-tools` rejecting dual-family `capabilities[]` arrays. No consumers yet.
- **PR-B — events + projection + migration.** New `MemoryProvider*` events (6 variants) with `is_bootstrap: bool` field. Update `projection_registry.rs` (count 135→141), `event_exhaustiveness.rs` match arms + constructors + count (168→174). pg V019 migration: `CREATE TABLE IF NOT EXISTS` for `project_memory_providers` + `memory_ingest_jobs`; `CREATE VIEW v_all_ingest_jobs`; backfill `scoring_policy_json → knowledge_scoring_policy_json` in the project-defaults store. Sqlite schema mirror. Registry count test drift updated.
- **PR-C — MultiProviderMemory + visibility.** `MultiProviderMemory` dispatcher mirror of `MultiProviderRetrieval`. `ResolvedProviderSnapshot` gains a second slot on `VisibilityContext`. `is_tool_visible` routes by tool name (`memory_*` → memory snapshot; `knowledge_search` → knowledge snapshot). `GATABLE_BUILTINS` expands.
- **PR-D — agent tool split.** `ConcreteKnowledgeSearchTool` wraps `MultiProviderRetrieval`. `ConcreteMemorySearchTool` / `ConcreteMemoryStoreTool` rewire to `MultiProviderMemory`. `memory_store` adds the `auto_extract` suppression check.
- **PR-E — HTTP surface.** `PUT /v1/projects/:id/memory-provider`, split `PUT …-{memory,knowledge}-scoring-policy` with `warnings[]` responses. `GET /v1/projects/:id/providers` atomic read, `GET …/{memory,knowledge}-scoring-policy/valid-dimensions`, `GET …/ingest-jobs?family=…`. Old `PUT …/scoring-policy` → **308 Permanent Redirect** (not 410). Docs + `http_routes.tsv` + `api_docs_coverage` drift updates.
- **PR-F — post-hoc rescorer split.** `PostHocRescorer` gains `family: CapabilityFamily`. Memory-family instance skips `multi_neighbors` (no-op `graph_proximity = 0.0`). `SourceCredibilityLookup::lookup` gains `family` param. Diagnostics carry `family` field; runtime overwrites on return path.
- **PR-G — cairn-default dual-family registration.** Two `InMemoryRetrieval` instances with **separate** `InMemoryDocumentStore` backing (no Arc sharing). `AppState.knowledge_retrieval` added. `ProjectCreated` command handler emits dual `*ProviderConfigured(is_bootstrap=true)` events. Boot-time backfill for pre-RFC-030 projects with idempotency check. Startup family-mismatch scan emits `KnowledgeProviderFamilyMismatch` events + operator-health badge. Integration tests for default-default, mixed, and state-isolation invariants.
- **PR-H — compliance suite split.** Extract shared checks into `cairn-provider-compliance-core`. Build `cairn-memory-compliance` + keep `cairn-knowledge-compliance` as thin shells. Add `check_no_dual_family_capabilities` shared check. Every existing RFC 029 integration test gets a memory-family mirror.
- **PR-I — adapter repo rename.** Rename GitHub `cairn-knowledge-mem0` → `cairn-memory-mem0`, crate name, binary name, manifest example, README install instructions. Update the adapter's `initialize` handshake to declare `MemoryProvider` family + `auto_extract = true` (mem0's default).

Each PR is independently mergeable and each stays compilable against the prior state. Compliance suite stays green across the sequence (the memory-family checks from PR-H run only against memory-family fixtures; they don't touch existing knowledge-family assertions).

## Decided

- **Two provider-ref slots per project.** Settled.
- **cairn-default serves both families with separate stores.** Deliberate bridge, TODO-marked at every registration site. Separate `InMemoryDocumentStore` per family from day one so the future dedicated-knowledge-default RFC can swap one slot without inheriting a shared mutable bag.
- **memory_store agent tool, gated by `auto_extract`.** Suppressed from the agent prompt when the memory backend auto-extracts via post-turn hook (mem0 default mode).
- **`knowledge_ingest` is NOT an agent tool.** Operator-driven HTTP only. Librarian-agent pattern deferred.
- **Dual-family plugins are forbidden.** Handshake validator rejects any `InitializeResult.capabilities[]` array carrying both `memory_provider` and `knowledge_provider` entries.
- **Canonical wire method names locked.** `memory.{query,ingest,ingest_status,list_sources}`. Vendor-prefixed namespaces nest under `plugin.<vendor>.*`.
- **Shared wire sub-types stay shared; `KnowledgeDocumentId` renamed to family-neutral `DocumentId`.** `ScoringBreakdownWire` gets per-field rustdoc annotating which family each dimension is primary for. `SourceTypeWire::JsonStructured` duplicate variant collapsed.
- **Scoring policies split by storage key, share the weight shape.** The "dimensions X matter for family Y" claim is operator-configurable, not structural. Cross-family score comparison is undefined behaviour; diagnostics carry `family` for bucketing.
- **Post-hoc rescorer parameterised by family.** Memory-family instance skips `multi_neighbors` entirely (episodic chunks aren't in the provenance graph). `SourceCredibilityLookup` takes a family parameter.
- **Diagnostics carry `family: CapabilityFamily`, host-set.** Providers cannot spoof it; runtime overwrites on the return path.
- **`*ProviderConfigured` events carry `is_bootstrap: bool`.** Dual-family cairn-default emission fires exactly once per project (at `ProjectCreated`), never at boot. Pre-RFC-030 projects backfill idempotently.
- **HTTP rename uses 308 Permanent Redirect, not 410 Gone.** Rolling deploys never break in-flight operator callers.
- **V019 migration backfills `scoring_policy_json → knowledge_scoring_policy_json`** and uses `IF NOT EXISTS` guards throughout for safe re-run.
- **Startup family-mismatch scan** emits `KnowledgeProviderFamilyMismatch` events per misconfigured project, surfaced in the operator UI health view.
- **Adapter repo rename is breaking + acceptable pre-v1.** `cairn-knowledge-mem0` → `cairn-memory-mem0`. mem0 declares `auto_extract = true` at handshake.

## Deferred Questions (revisit when the question matters)

1. **Dedicated cairn-default knowledge context.** cairn-default's current implementation is genuinely an episodic memory; registering it as the default knowledge provider is the TODO seam this RFC explicitly flags. The dedicated knowledge default (corpus ingest + curation pipeline owned by cairn) is out of scope here and becomes its own RFC when that context lands. Separate-store construction in D5 is designed so that RFC can swap one slot without touching the other.
2. **Cross-family federation tool + unified result ranking.** Agents can call `memory_search` then `knowledge_search` today, and the LLM is free to reason over both response sets. What the RFC explicitly does **not** ship is a single federated tool or a cross-family score-normalization pass — when the two responses land, the LLM gets family-tagged results (`diagnostics.family`) with per-family scores on incomparable scales. Workflows that require "most relevant regardless of origin" silently degrade until federation ships: the LLM either over-queries (calls both on every recall, doubling latency) or misses context in the un-queried family. Operator prompts should include explicit guidance to call both tools when recall across both corpora matters. If signal accumulates, a follow-up RFC adds a federated tool and a score-normalization contract.
3. **Librarian-agent pattern: knowledge corpus that the agent also writes to.** A concrete operator use case — a research/librarian agent whose outputs feed back into the authoritative corpus — is not representable under RFC 030's taxonomy. The operator must put the knowledge source on `knowledge_provider` and the agent's observations go to `memory_provider`; the agent's findings never reach the authoritative corpus automatically. This is a named silent degradation. The fix is either (a) a `knowledge_ingest` agent tool (asymmetric but possible) or (b) a post-hook that promotes specific memory entries to knowledge. Both land in a follow-up when the librarian pattern is concrete.
4. **Plugin host Mutex bottleneck under dual-family concurrency.** RFC 029 PR #760 already flagged that `StdioKnowledgeDispatcher` holds the plugin-host mutex for the entire round-trip, serialising every `knowledge.*` call globally. RFC 030 adds a second dispatcher (or reuses one with routing by family) against the same `Arc<Mutex<StdioPluginHost>>`; a project with both families configured sees `memory_search` + `knowledge_search` from the same agent turn queue behind one mutex. The fix (per-plugin locks + async transport in `StdioPluginHost`) is already tracked in the dispatcher's module docstring as a dependency of both RFCs. RFC 030 does not block on it, but explicitly acknowledges the serialisation cost: under fan-out, dual-family same-turn concurrency is latency-bounded, not correctness-broken.
5. **`knowledge_ingest` agent tool.** See #3; same deferral.

## References

- [RFC 003](./003-owned-retrieval.md) — original owned-retrieval contract
- [RFC 007](./007-plugin-protocol-transport.md) — plugin protocol (unchanged; gains a new capability family value)
- [RFC 015](./015-plugin-marketplace-and-scoping.md) — marketplace (unchanged; gains a second knowledge/memory category)
- [RFC 029](./029-pluggable-knowledge-providers.md) — this RFC amends
