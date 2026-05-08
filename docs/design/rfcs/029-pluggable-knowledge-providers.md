# RFC 029: Pluggable Knowledge Providers (Amendment to RFC 003)

Status: draft
Owner: knowledge/retrieval
Amends: [RFC 003](./003-owned-retrieval.md), [RFC 007](./007-plugin-protocol-transport.md), [RFC 015](./015-plugin-marketplace-and-scoping.md)

## Summary

RFC 003 is correct that cairn should own its default knowledge-retrieval stack. This RFC adds the companion contract: the default stack is **one implementation among several**, reachable through a pluggable provider interface. The no-lock-in commitment runs in both directions — cairn-rs is not locked into its own default, and operators are not locked into cairn's default.

This RFC adds a new plugin capability family `KnowledgeProvider` to RFC 007's wire protocol, extends RFC 015's marketplace to surface knowledge-provider plugins (and amends RFC 015's tool-visibility contract to cover builtin tools), and specifies how the existing in-process `RetrievalService` / `IngestService` traits become the runtime contract both the default implementation and plugin-based providers satisfy.

**This RFC covers Knowledge only.** Memory (episodic, session/task-scoped context) is the separate domain `cairn-memory` (the crate name) currently conflates into knowledge retrieval. A follow-on RFC will add `MemoryProvider` when episodic-rs is ready to serve as its default implementation. This RFC's architecture is designed to accommodate that follow-on without rework.

## Why

RFC 003 declared that cairn owns ingest, chunking, indexing, hybrid retrieval, scoring, reranking, and deep search. That decision is correct for the **default** experience: an operator installing cairn should get working knowledge retrieval out of the box without buying anything else. But RFC 003 as written implies ownership is exclusive. The current code confirms this framing — `crates/cairn-memory/src/in_memory.rs` has a comment stating "this is a stop-gap … because the external memory crate replaces it in a future PR," which telegraphs a single future owner rather than a pluggable architecture.

Single-owner retrieval has two problems for cairn as a commercial product:

1. **Operators already have retrieval infrastructure.** A team using AWS Bedrock Knowledge Base, mem0, Pinecone, or an internal retrieval service should not be forced to re-ingest corpora into cairn's own stack. "Keep using what you have" is a selling point; "migrate first" is not.
2. **cairn's own default is not ready yet.** The in-process default is acknowledged as a placeholder. The intended future default is external (episodic-rs for memory; an open question for knowledge). Without a plugin seam, every operator including cairn's own dogfooding is blocked on that default shipping.

RFC 007 already defines a plugin protocol; RFC 015 already defines a marketplace. Knowledge-provider-as-plugin is a small additive step that solves both problems.

## Scope

### In scope

- New plugin capability family `KnowledgeProvider` added to RFC 007's `CapabilityFamily` enum. Manifest declares the family only; all capability detail (retrieval modes, ingest capability, per-dimension surfacing) is negotiated at the `initialize` handshake (Layer 2), not in the manifest. Operators configure providers through cairn's runtime surfaces (credential wizard per RFC 015, project-level HTTP API), never by editing the plugin's source or its TOML/JSON manifest.
- Handshake-layer capability snapshot type (`KnowledgeProviderCapability`) with tri-state declaration for each of five provider-required scoring dimensions
- New discoverable enumeration: `knowledge.list_sources` (Layer 3) so operator UI reflects the provider's current-reality source/index list, not a static manifest
- Wire types defined in `cairn-plugin-proto` with `serde` derives; `From` / `TryFrom` bridges to in-process `cairn-memory` types
- Four RFC 007 JSON-RPC calls: `knowledge.query`, `knowledge.ingest`, `knowledge.ingest_status`, `knowledge.list_sources`
- Runtime resolution layer (`MultiProviderRetrieval`) dispatching to in-process default or plugin
- Runtime-owned post-hoc re-scoring for cairn-owned dimensions (graph_proximity, source_credibility, corroboration), unconditionally overwriting any values a provider might return for those fields
- Batched `GraphQueryService::multi_neighbors(chunk_ids[])` to keep post-hoc rescoring latency additive-O(1) in network round-trips rather than O(N) per result
- Runtime-orchestrated deep search over single-hop providers
- Tool-surface update: `ConcreteMemoryStoreTool` self-gates via `VisibilityContext.resolved_knowledge_provider`; RFC 015's `is_plugin_tool_visible` is renamed to `is_tool_visible` and made to handle both plugin and builtin paths
- Compliance test suite as `crates/cairn-knowledge-compliance/` — shape-only
- Operator UX: provider picker at project creation; marketplace section for `KnowledgeProvider` plugins; per-query diagnostics showing provider + per-dimension producer (`computed_by = "provider" | "runtime_post_hoc"`)

### Out of scope

- Memory domain (a follow-on RFC)
- Replacing or removing `cairn-memory`
- Multi-provider query federation
- Cross-provider migration tooling
- Backward-compatibility shims (cairn has no pre-v1 consumers)
- Quality-floor compliance testing (v1 is shape-only)
- Dynamic per-project tool descriptions (the `ToolHandler::description` trait signature stays unchanged in v1)
- Public catalog `download_url` for reference adapters during pre-v1 — private adapters are NOT listed in the bundled catalog and are added via `POST /v1/plugins` after local build

### Explicit amendments to RFC 003

Every RFC 003 subsection that meaningfully changes under pluggable providers is amended below. Anything not listed here applies unchanged.

#### §"Source of Truth"

Amended. "Postgres for canonical storage" applies to **cairn-default**. Plugin providers store corpora in their own backend. cairn's event log remains the source of truth for *configuration* (which provider a project uses, ingest submissions, ingest status updates), not for *corpus content*. Operators choosing a plugin provider accept that their corpus content lives outside cairn's store.

#### §"V1 Lexical Scope"

Amended. "Postgres full-text search plus product-owned normalization, filtering, and reranking is the canonical lexical floor" applies to **cairn-default**. Plugin providers declare their own `retrieval_modes`; a provider not declaring `lexical` lacks lexical retrieval entirely. Operators who need lexical retrieval guarantees use cairn-default or select a provider that declares `lexical`.

#### §"Service Shape In V1"

Amended. The `RetrievalService::query` entry point remains in-process. Actual query execution runs in-process (cairn-default) or in a plugin subprocess via RFC 007's plugin host. Async ingest is runtime-owned for cairn-default; for plugin providers, the plugin owns its own ingest lifecycle and reports via `knowledge.ingest_status`.

#### §"Ingest"

Amended. The eight-stage pipeline (source registration → normalization → parsing → chunking → metadata extraction → deduplication → embedding → index update) is **cairn-default's** pipeline. Plugin providers implement whatever pipeline their backend uses. Plugin providers MUST expose `knowledge.ingest_status` by `KnowledgeDocumentId` so operator UI can show coherent status regardless of backend.

#### §"Supported Document Types"

Amended. The canonical floor (plain text, Markdown, HTML, structured JSON, knowledge-pack imports) applies to **cairn-default**. Plugin providers declare their own `ingest_source_types`. Read-only plugin providers (`ingest_capable = false`) have no ingest surface at all.

Product claim: when an operator configures a read-only plugin provider as the only provider for a project, that project has no cairn-side ingest path. The provider's native ingest surface (e.g., Bedrock console) is where documents land.

#### §"Chunk Model"

Amended. The eight required chunk fields apply to chunks **as returned in retrieval results**. Wire type `ChunkRecord` in `cairn-plugin-proto` carries all eight; fields may be null for providers that can't populate them (notably `graph_linkage` and `credibility_metadata`). Readers treat null as "not provided by this backend."

#### §"Embeddings"

Amended. "Provider-abstracted embeddings" refers to cairn's embedding-provider abstraction (RFC 009), used by cairn-default. Plugin providers use whatever embedding path their backend provides; they do not interact with cairn's embedding-provider abstraction.

#### §"Retrieval"

Amended. The four retrieval modes (lexical-only, vector-only, hybrid, metadata-filtered) plus deep search describe **cairn-default's** supported modes. Plugin providers declare their own `retrieval_modes` subset. Deep search is cairn-runtime-orchestrated and works across any provider.

#### §"Reranking"

Amended. MMR + optional provider-based reranker + deterministic operator-visible scoring factors applies to **cairn-default**. Plugin providers implement their own rerank (opaque to cairn); they cannot expose a "cairn-tunable reranker" — operators who need tunable rerank use cairn-default.

#### §"Scoring Model"

Amended. The eight canonical scoring dimensions are split into two categories:

- **Provider-required dimensions** (5): `semantic_relevance`, `lexical_relevance`, `freshness_decay`, `staleness_penalty`, `recency_of_use`. Every provider MUST declare each one explicitly as either `surfaced` or `not_supported` at capability-declaration time. Implicit omission is rejected at handshake.
- **Runtime-owned dimensions** (3): `graph_proximity`, `source_credibility`, `corroboration`. These use cairn-runtime state (cairn-graph, credibility store) that plugin providers cannot access. Runtime computes these post-hoc by joining returned chunks' ids/paths/graph_linkage into cairn's own state. Plugin providers MUST NOT populate these fields in wire responses; the runtime unconditionally overwrites whatever value (including spoofed values) the wire carries.

When a provider declares `"not_supported"` for a provider-required dimension, operator-configured `ScoringPolicy` values referencing that dimension cause the query to fail with `RetrievalError::PolicyDimensionUnavailable { dimension, provider }`. Silent no-op on unavailable dimensions is forbidden (violates RFC 003 §Configurability Rule's ban on "hidden provider-specific heuristics").

Runtime-owned dimensions are explicitly marked in every diagnostic with `computed_by = "runtime_post_hoc"`. Provider-surfaced dimensions carry `computed_by = "provider"`.

RFC 003 §Configurability Rule line 248 ("must not allow... hidden provider-specific heuristics that cannot be surfaced in diagnostics") still applies. The split here respects that rule because every dimension is explicitly marked.

#### §"Retrieval Diagnostics Requirement"

Amended. The five required diagnostic fields (retrieval mode, candidate-generation stages, contributing scoring dimensions, effective scoring policy, reranker path) apply for **cairn-default**. For plugin providers:

- `retrieval_mode` — required; provider declares in manifest
- `scoring_dimensions_contributing` — required; always populated including runtime-owned dimensions
- `effective_scoring_policy` — required (project-side state)
- `candidate_generation_stages` — may be `"not_surfaced_by_provider"`
- `reranker_path` — may be `"not_surfaced_by_provider"`

Fields a provider cannot surface display in operator UI as "not surfaced by this provider," not as empty.

#### §"Deep Search"

Amended. Deep search remains first-class-owned — **cairn-runtime orchestrates it**. All providers expose single-hop `knowledge.query`. Cairn's deep-search orchestrator iterates queries, applies RFC 003's five required sub-capabilities (query decomposition, iterative retrieval, quality gates, graph expansion hooks, synthesis), and returns multi-hop results.

No provider declares deep-search support; there is no `knowledge.deep_search` wire call. A plugin provider's contribution to deep search is the single-hop query it already exposes.

#### §"Operator Surfaces"

Amended. The seven required views apply unchanged for cairn-default. For plugin providers, the UI surfaces whatever the provider reports via `knowledge.ingest_status` plus what the runtime synthesizes from query diagnostics. Fields a plugin provider cannot report display as "not surfaced by this provider."

#### §"Migration Path"

Amended. "End State: Cairn should not require Bedrock KB for its main knowledge product story" is clarified: **cairn does not require any specific backend**. Bedrock KB is one of several supported backends. cairn requires only that at least one compliant provider is configured per project (cairn-default counts).

#### §"Local Mode Expectations"

Amended. Local-mode expectations (`--db memory`, development, personal use, small-scale evaluation) apply unchanged for projects configured with `knowledge_provider = "cairn-default"`. A project configured with a plugin provider in local mode works only if the operator has provided valid credentials; otherwise queries fail with `RetrievalError::ProviderUnavailable`. There is no silent fallback.

A fresh operator running `cargo run -p cairn-app -- --db memory` gets projects with `cairn-default` (project-creation default), preserving the out-of-box experience.

#### §"Decision"

Amended. Decision bullets reinterpreted:

- "`pgvector` + HNSW", "Postgres full-text plus product-owned normalization/filtering/reranking is the canonical v1 lexical floor" — describe **cairn-default**'s implementation. Plugin providers use their backend's indexing and lexical layer.
- "fixed scoring dimensions with bounded operator-tunable scoring policy" — dimensions are fixed, but producer responsibility splits per §Scoring Model above.
- Other bullets apply unchanged.

RFC 003's non-goals (no dedicated vector cluster, no every-embedding-backend, no every-parser, no internet-scale indexing) continue to apply to **cairn-rs itself**. Plugin providers have their own non-goals that the provider declares.

### Explicit amendments to RFC 015

#### §"Visibility Filtering" — `is_plugin_tool_visible` rename and expansion

Amended. `is_plugin_tool_visible` in `crates/cairn-runtime/src/services/marketplace_service.rs` is renamed to `is_tool_visible` and made to handle both plugin-provided tools and select builtin tools. The rename is backwards-incompatible — all callers update.

The function's contract is extended: for plugin tools, behavior is unchanged (filtering by `enabled_plugins` + `tool_allowlist`). For builtin tools, a new predicate is added: a **compile-time** allowlist of builtins that may be gated by runtime state, named `GATABLE_BUILTINS: &[&str]` in `marketplace_service.rs`. Initial membership: `["memory_store"]` only — `memory_search` was considered but excluded (it is always visible; there is no state in which search should be hidden, so registering it would be a no-op). Builtins not in the allowlist remain always-visible. For `memory_store`, the gate fires when `VisibilityContext.resolved_knowledge_provider` points at a provider declaring `ingest_capable = false`.

`VisibilityContext` (`crates/cairn-domain/src/contexts.rs`) gains a `resolved_knowledge_provider: Option<ResolvedProviderSnapshot>` field. The snapshot carries `provider_id`, `ingest_capable`, and `retrieval_modes`. The field is populated at `VisibilityContext` construction time in `marketplace_service::build_visibility_context_for_run` (the existing helper called during run startup), which looks up the project's configured provider and snapshots its capability flags. No hot-path recomputation during prompt assembly.

The "Built-in cairn tools are always visible" comment in `marketplace_service.rs` (on the renamed `is_tool_visible`) is updated to reflect the allowlist exception for `GATABLE_BUILTINS`.

#### Why amend RFC 015 here

The cleaner split (parallel `is_builtin_tool_visible` function) creates a second code path for the same conceptual concern (tool visibility). CLAUDE.md's rule "prefer tightening scope over adding parallel half-systems" argues for unification. One function with two internal branches (plugin + allowlisted-builtin) is simpler to reason about and easier to extend when future builtins need dynamic gating.

### Explicit amendments to RFC 007

#### §"Capability Objects" (canonical inner schema)

Amended. RFC 007 lists six canonical capability-object inner keys (`tools`, `signals`, `channels`, `hooks`, `policies`, `scorers`). This RFC adds `knowledge_provider` as a seventh family and defines a family-specific inner shape carried at Layer 2 (`initialize.result.capabilities[]`) rather than at Layer 1 (manifest `capabilities[]`). The manifest-layer entry for `knowledge_provider` is intentionally empty (just `type`) because its detail is runtime-negotiated; the handshake-layer entry carries `retrieval_modes`, `ingest_capable`, `scoring_dimensions`.

**Precedent**: future capability families MAY follow the same pattern when their effective capability legitimately depends on runtime state (credentials, backend reachability) that is not available at manifest-parse time. Families whose effective capability is fully static SHOULD NOT — manifest-layer declaration is still the default, as RFC 007 §Plugin Manifest requires. `tool_provider` remains the canonical "static" model (tool names declared identically in manifest and `initialize`).

RFC 007 §"Plugin Manifest" (required fields: capability families, "declared tool names or provider namespaces where applicable") is respected: "where applicable" carves out families with no statically-known namespace. Knowledge providers have no statically-known source list — sources are runtime-discoverable via `knowledge.list_sources` — so the manifest declares the family and nothing else.

#### §"Discover" step — what is validated for `knowledge_provider` manifests

RFC 007 §"Lifecycle step 1 (Discover)" validates schema, executable availability, capability declarations, permission declarations. For `knowledge_provider` manifests, capability-declaration validation at Discover is narrow: family-name enum membership, and the co-occurrence check rejecting manifests that declare both `knowledge_provider` and `signal_source` (`CapabilityConflict` error). All capability detail (retrieval modes, ingest capability, scoring dimensions) is validated at step 3 (Handshake), not Discover.

#### §"Host → Plugin" RPC methods — new method `knowledge.list_sources`

Amended. RFC 007's §"Core RPC Methods" list (`initialize`, `shutdown`, `health.check`, `tools.list`, `tools.invoke`, `signals.poll`, `channels.deliver`, `hooks.post_turn`, `policy.evaluate`, `eval.score`, `cancel`) is extended with `knowledge.list_sources` (and the three `knowledge.*` wire calls this RFC introduces: `knowledge.query`, `knowledge.ingest`, `knowledge.ingest_status`). Per RFC 007's "may add optional fields, but must not remove or rename these fields without amending this RFC" — this is that amendment.

#### §"Plugin → Host Notification Bodies" — new notification `knowledge.sources.changed`

Added. Plugins MAY emit a `knowledge.sources.changed` notification (RFC 007 `event.emit` shape, with `params.type = "knowledge.sources.changed"` and `params.provider_id`) when they detect source-set changes. On receipt, cairn invalidates its cached source list for the provider and refetches via `knowledge.list_sources` on next UI refresh. Plugins that cannot detect source changes (polling-only backends, opaque SaaS) simply never emit this notification; cairn's UI-refresh cadence remains the baseline fallback.

## The `KnowledgeProvider` Capability

### Capability declaration

Knowledge providers follow RFC 007's three-layer declaration model strictly. **No feature-gated behavior is hardcoded in the manifest** — capability detail is discovered at runtime via handshake, not by editing the manifest shipped with the binary.

**Layer 1: Manifest (validates existence, declares family only).**

```json
{
  "id": "com.example.cairn-knowledge-bedrock-kb",
  "name": "Bedrock KB",
  "version": "0.1.0",
  "command": ["cairn-knowledge-bedrock-kb", "--serve"],
  "capabilities": [
    { "type": "knowledge_provider" }
  ],
  "permissions": ["net.egress"],
  "limits": { "maxConcurrency": 4, "defaultTimeoutMs": 30000 }
}
```

The manifest declares **only** that the binary implements the `knowledge_provider` family. No `retrieval_modes`, no `ingest_capable`, no `scoring_dimensions`. Authors who ship a plugin binary do not bake those into the manifest; they come from the running process at handshake.

**Layer 2: `initialize` handshake (negotiated effective capability).**

The existing RFC 007 `initialize` response carries a `capabilities` array. For knowledge providers the `capabilities[].knowledge_provider` sub-object extends that array with the runtime-effective snapshot:

```json
{
  "protocolVersion": "1.0",
  "plugin": { "id": "com.example.cairn-knowledge-bedrock-kb", "name": "Bedrock KB", "version": "0.1.0" },
  "capabilities": [
    {
      "type": "knowledge_provider",
      "retrieval_modes": ["hybrid", "semantic", "keyword"],
      "ingest_capable": false,
      "scoring_dimensions": {
        "semantic_relevance": "surfaced",
        "lexical_relevance": "surfaced",
        "freshness_decay": "not_supported",
        "staleness_penalty": "not_supported",
        "recency_of_use": "not_supported"
      }
    }
  ]
}
```

This snapshot is what cairn stores in `ResolvedProviderSnapshot` and threads into `VisibilityContext`. The plugin MAY decide its effective capability at startup based on its own configuration (env vars, CredentialService-provided credentials, its backend's reachability) — but cairn does not care how; cairn only reads the handshake result. Tri-state scoring-dimension enforcement (each of the 5 provider-required dimensions explicit as `surfaced` or `not_supported`; the 3 runtime-owned dimensions MUST NOT appear) is validated at handshake, not at manifest parse.

**Validation-immediacy tradeoff (vs RFC 007 Discover step).** Handshake-time validation means a misconfigured plugin installs successfully (Discover passes) and fails at first spawn (Handshake rejects the `initialize` response). This is accepted because capability detail legitimately depends on runtime state (credentials, backend reachability) unavailable at manifest-parse time. Cairn's operator UI MUST surface handshake-failure reasons (specific dimension missing, forbidden runtime-owned dimension declared, unknown retrieval mode, etc.) with equal clarity to Discover-time errors — the failure is not logged-and-swallowed, it is presented to the operator in the plugin's install/enable view with the exact reason.

**Handshake is authoritative; snapshot changes across restarts.** Cairn re-reads the handshake snapshot on every plugin process spawn. If a plugin restart changes declared shape (e.g., credentials removed and `ingest_capable` flips from `true` to `false`, or a scoring dimension moves from `surfaced` to `not_supported`), cairn's runtime immediately uses the new snapshot. In-flight runs that were started under the prior snapshot finish with their snapshotted `VisibilityContext` — the snapshot is per-run-startup, not per-query — so an agent mid-run keeps the tool surface it was given. New runs use the new snapshot. Cairn emits a `KnowledgeProviderCapabilityChanged { project, provider_ref, prior, current, at }` event when the handshake snapshot for a given `provider_ref` differs from the previous snapshot (projection table: `project_knowledge_providers`, `kind = "capability_changed"`).

**Layer 3: Discoverable enumeration — `knowledge.list_sources`.**

New JSON-RPC call (host → plugin). Plugin returns the corpora / indices it currently has access to:

```json
{
  "sources": [
    { "source_id": "bedrock-kb:YIPQUB6TVL", "display_name": "valkey review corpus", "estimated_chunks": 1553 },
    { "source_id": "bedrock-kb:KAVTXZA9GD", "display_name": "glide review corpus", "estimated_chunks": 633 }
  ]
}
```

cairn calls `knowledge.list_sources` during operator UI refresh (e.g., when the operator opens the project's knowledge view). The list is not static; a plugin may gain/lose sources between calls as its backend changes. Operator UI reflects current reality.

Plugins MAY emit a `knowledge.sources.changed` notification (RFC 007 `event.emit` shape, `params.type = "knowledge.sources.changed"`) when they detect source-set changes. On receipt, cairn invalidates its cached source list for the provider and refetches via `knowledge.list_sources` on next UI refresh. Plugins that cannot detect changes (polling-only backends, opaque SaaS) simply never emit; cairn's UI-refresh cadence remains the baseline.

**Asymmetry with `tools.list`** (worth naming so future plugins don't cargo-cult the wrong pattern). `tools.list` returns schemas for a fixed set of tool names that were already declared in the manifest and echoed in `initialize.result.capabilities[].tools` — Layer 3 deepens Layer 1/2. `knowledge.list_sources` is a **runtime-data query** — the source set is not declared at manifest or handshake time, and may legitimately change between two calls without a plugin restart. Future capability families choosing between the two Layer-3 styles should pick based on whether their enumerable surface is stable (→ `tools.list`-style) or dynamic (→ `list_sources`-style).

**No compile-time gating.** There are no Cargo features that plugin authors flip to change what their binary advertises. A plugin binary is one behavior; to advertise differently, the plugin binary must decide differently at startup based on its runtime configuration (credentials, env). Operators configure the plugin entirely through cairn's runtime surfaces (credential wizard per RFC 015, `PUT /v1/projects/:id/knowledge-provider`), never by editing adapter source or adapter TOMLs.

Providers whose handshake reports `ingest_capable = false` are read-only. Runtime refuses to schedule `knowledge.ingest` against them; `ConcreteMemoryStoreTool` is hidden from agent prompts via `VisibilityContext`.

### Wire calls

Four calls added to the RFC 007 JSON-RPC surface:

```
knowledge.query          — KnowledgeQueryParams         → KnowledgeQueryResult
knowledge.ingest         — KnowledgeIngestParams        → KnowledgeIngestAck
knowledge.ingest_status  — KnowledgeIngestStatusParams  → KnowledgeIngestStatusResult
knowledge.list_sources   — KnowledgeListSourcesParams   → KnowledgeListSourcesResult
```

`initialize` is unchanged structurally per RFC 007; this RFC only specifies the `knowledge_provider` capability shape inside `capabilities[]` (see Layer 2 above).

Wire types are defined in `cairn-plugin-proto` with `serde` derives. In-process `cairn-memory::retrieval::{RetrievalQuery, RetrievalResponse}` and `cairn-memory::ingest::{IngestRequest, IngestStatus}` gain `From` / `TryFrom` bridges; they are not themselves promoted to `cairn-plugin-proto`.

`KnowledgeQueryResult.chunks[].scoring_breakdown` serializes with all eight dimensions. Runtime-owned dimensions (`graph_proximity`, `source_credibility`, `corroboration`) are **always overwritten** by the runtime on receipt; whatever the provider returns for them is discarded. Compliance suite asserts this by registering a mock plugin that returns non-null runtime-owned values and verifying the runtime discards them.

Wire field name `freshness_decay` is the canonical vocabulary. In-process `ScoringBreakdown.freshness` is renamed to `freshness_decay` in PR A to match.

### Resolution: which provider serves a query

Per-project configuration:

```toml
[knowledge]
provider = "cairn-default"  # or "plugin:bedrock-kb" or "plugin:mem0"
```

Command: `ConfigureKnowledgeProvider { project, provider_ref, actor }` → emits `KnowledgeProviderConfigured { project, provider_ref, configured_by, at }`. HTTP: `PUT /v1/projects/:id/knowledge-provider`.

At query time, `RetrievalService::query` looks up the project's provider:

- `cairn-default` → in-process `cairn-memory` path
- `plugin:<id>` → dispatch `knowledge.query` via RFC 007 plugin host

If the configured provider is unavailable (plugin not spawned, handshake failed, credentials missing), query fails with `RetrievalError::ProviderUnavailable { provider, reason }`; runtime emits `KnowledgeProviderUnavailable { project, provider, reason, at }`. No ambient fallback.

Plugin spawn policy: lazy, per RFC 015's existing tool-only rule — knowledge providers MUST NOT declare `SignalSource` in the same manifest. PR A's manifest validation rejects a manifest that declares both `KnowledgeProvider` and `SignalSource` with a specific error (`CapabilityConflict { families: [KnowledgeProvider, SignalSource] }`); the compliance suite asserts this.

### Tool Surface

`ConcreteMemorySearchTool` rewired to `Arc<MultiProviderRetrieval>`. External schema unchanged. Tool description (`ToolHandler::description`) remains static — per-project dynamic descriptions are out of scope for v1. Operators see per-provider detail in the operator UI and per-query diagnostics, not in agent prompts.

`ConcreteMemoryStoreTool` self-gates via `VisibilityContext.resolved_knowledge_provider` (populated by the amended RFC 015 filter layer). When the resolved provider declares `ingest_capable = false`, the tool is excluded from the agent's prompt by `prompt_tools_for`.

### Scoring Split — runtime post-hoc computation

Every `KnowledgeQueryResult` flows through a post-hoc rescorer before reaching the agent:

1. Runtime discards any provider-returned values for `graph_proximity`, `source_credibility`, `corroboration`
2. Runtime batches a single `GraphQueryService::multi_neighbors(chunk_doc_ids)` call (new API landing with PR B) to fetch upstream + downstream edges for every returned chunk in one round-trip
3. Runtime computes `graph_proximity` per chunk from the batched neighbor data
4. Runtime queries its credibility/provenance projection once per batch (not per chunk) to compute `source_credibility`
5. Runtime cross-references chunks within the response (and optionally against the project's recent query history) to compute `corroboration`
6. Final `score` is recomputed per the project's `ScoringPolicy` using both provider-surfaced and runtime-computed dimensions

**Latency accounting**: post-hoc rescoring adds one batched graph round-trip + one credibility lookup per query, not N per result. On Postgres this is O(1) in SQL calls; on SQLite local-mode it is two local reads. For plugin-provider queries, the network round-trip to the provider dominates; post-hoc is additive noise.

**Provider injection protection**: runtime-owned dimensions coming in from a plugin are unconditionally discarded. Compliance suite explicitly tests this.

### Runtime-orchestrated deep search

cairn's existing deep-search orchestrator (`crates/cairn-memory/src/deep_search_impl.rs`) is already parameterized over `R: RetrievalService`. PR B swaps the `R` it's instantiated with from `InMemoryRetrieval` to `MultiProviderRetrieval`. Per-hop retrieval then dispatches to whichever provider the project configured; quality gates, graph expansion, and synthesis remain runtime-side.

Plugin providers contribute single-hop speed and corpus coverage. Orchestration stays cairn's.

## Event-Sourcing

Six new domain events, all `Projected`:

- `KnowledgeProviderConfigured { project, provider_ref, configured_by, at }` — project picks/changes a provider. Projection table: `project_knowledge_providers` (upserts the row for `(project, provider_ref)`; represents current configuration). Row kind column: `kind = "configured"`.
- `KnowledgeProviderUnavailable { project, provider_ref, reason, at }` — query-time failure surfaced for operator audit. Same table `project_knowledge_providers`, row kind column `kind = "unavailable"`; inserts an audit row (never upserts, never overwrites the `"configured"` row). Operator UI joins by `(project, provider_ref)` to show current state + recent availability failures.
- `KnowledgeIngestSubmitted { project, provider_ref, document_id, source_type, at }` — ingest kicked off (plugin or cairn-default). Projection table: `knowledge_ingest_jobs`
- `KnowledgeIngestRejected { project, provider_ref, reason, at }` — ingest refused before dispatch. Projection table: `knowledge_ingest_jobs`
- `KnowledgeIngestStatusUpdated { project, provider_ref, document_id, status, at }` — ingest status changed. Projection table: `knowledge_ingest_jobs`
- `KnowledgeProviderCapabilityChanged { project, provider_ref, prior, current, at }` — plugin restart yielded a different handshake snapshot than last spawn. Projection table: `project_knowledge_providers`, `kind = "capability_changed"`; audit row, never upserts.

Table names match the existing plural-snake-case convention (verified against `crates/cairn-store/src/projection_registry.rs`). Per-query events are **not** emitted — today's tool-invocation audit (`ToolInvocationCompleted`) already captures per-call trace, matching RFC 015's config/state-events-only granularity.

Commands:

- `ConfigureKnowledgeProvider { project, provider_ref, actor }` → `KnowledgeProviderConfigured`
- Ingest commands inherit from existing ingest command surface; the new events emit alongside existing ones rather than introducing new commands

## Reference Adapters (Half-Included Batteries)

Two adapter implementations are **not part of `avifenesh/cairn-rs`**. They live in separate private repositories during the pre-v1 period:

### `avifenesh/cairn-knowledge-bedrock-kb` (private)

External binary wrapping AWS Bedrock Knowledge Base. Credentials: tenant-scoped `aws_region` + `bedrock_kb_id` per project.

Retrieval modes: hybrid, semantic, keyword. Ingest: not supported. Scoring dimensions: `semantic_relevance = "surfaced"`, `lexical_relevance = "surfaced"`, others `"not_supported"`.

### `avifenesh/cairn-knowledge-mem0` (private)

External binary wrapping mem0. Credentials: tenant-scoped API key.

Retrieval modes: vector, hybrid. Ingest: supported. Scoring dimensions: `semantic_relevance = "surfaced"`, `freshness_decay = "surfaced"`, others `"not_supported"`.

### Distribution during pre-v1

**Private adapters are NOT listed in the bundled catalog** (`crates/cairn-plugin-catalog/catalog.toml`). The catalog is embedded in every cairn-app binary; listing a non-public download_url would produce 404s for operators without private-repo access.

Operators with private-repo access install these adapters via `POST /v1/plugins` with a local-built binary path:

1. Clone the private adapter repo (requires granted access)
2. `cargo build --release`
3. `POST /v1/plugins` with the manifest + binary path
4. `POST /v1/plugins/:id/install` + credential wizard per RFC 015

When the adapter repos go public (future decision, not v1), catalog descriptors are added and the standard marketplace install flow works.

### Why out of main repo

Per RFC 017 precedent: third-party-integration adapters are external binaries so cairn's release cadence is not coupled to vendor API stability.

## Implementation Plan

Three PRs against `avifenesh/cairn-rs` plus two private adapter repos. The adapter repos ship after their PR dependencies land.

### PR A — Capability family + wire types

- `CapabilityFamily::KnowledgeProvider` in `crates/cairn-plugin-proto/src/capabilities.rs`. The **manifest-layer** `PluginCapability::KnowledgeProvider` variant is empty — it declares the family and nothing else (per Layer 1 above).
- **Handshake-layer** `KnowledgeProviderCapability` struct in `cairn-plugin-proto` with `serde` derives: `retrieval_modes`, `ingest_capable`, `scoring_dimensions` as a tri-state-per-dimension map. This is what `initialize.result.capabilities[]` entries deserialize into for `knowledge_provider` family entries.
- Wire types in `cairn-plugin-proto`: `KnowledgeQueryParams`, `KnowledgeQueryResult`, `KnowledgeIngestParams`, `KnowledgeIngestAck`, `KnowledgeIngestStatusParams`, `KnowledgeIngestStatusResult`, `KnowledgeListSourcesParams`, `KnowledgeListSourcesResult`, `ChunkRecord`, `ScoringBreakdown`, `KnowledgeSource`
- `From` / `TryFrom` bridges between wire types and in-process `cairn-memory::{retrieval, ingest}` types
- Rename in-process `ScoringBreakdown.freshness` to `freshness_decay`
- Round-trip tests for every wire type
- **Handshake validation** enforcing tri-state scoring-dimension declaration and rejecting runtime-owned dimensions (graph_proximity, source_credibility, corroboration) if a plugin attempts to declare them. This happens at `initialize` response processing, not at manifest parse.
- Manifest-validation rejection when a manifest declares both `knowledge_provider` and `signal_source` in its `capabilities[]` array (`CapabilityConflict` error) — capability-family co-occurrence is still a manifest-time check because the family set is the manifest's job to advertise.

### PR B1 — Multi-provider dispatch, visibility, events

Foundational plumbing. Provider resolution and visibility gating land without the scoring-split machinery.

- `MultiProviderRetrieval` implementing `RetrievalService`, dispatching to `InMemoryRetrieval` or the plugin host
- Same shape for `IngestService`
- Five event types wired into `projection_registry.rs` with plural-snake-case tables + discriminator column on `project_knowledge_providers`
- `ConfigureKnowledgeProvider` command; `PUT /v1/projects/:id/knowledge-provider` HTTP handler; projection
- `VisibilityContext.resolved_knowledge_provider` field + `ResolvedProviderSnapshot` type
- `GATABLE_BUILTINS` const + `is_plugin_tool_visible` → `is_tool_visible` rename per RFC 015 amendment
- `ConcreteMemorySearchTool` rewired to `MultiProviderRetrieval`; description stays static
- `ConcreteMemoryStoreTool` gated via `is_tool_visible`
- Deep-search orchestrator swapped to `MultiProviderRetrieval` at the `R: RetrievalService` parameter site — no post-hoc rescoring yet, deep search uses whatever scoring the provider returns
- Integration tests 1–5 and 8–12 from §"Integration Tests"

### PR B2 — Post-hoc scoring + scoring-policy validation

Adds the runtime-owned dimensions on top of the dispatch layer from B1. Independently testable because B1 gives us a working pipeline whose scoring is currently "whatever the provider returned" — B2 replaces three specific fields with runtime-computed values.

- `GraphQueryService::multi_neighbors(doc_ids: Vec<KnowledgeDocumentId>) -> Vec<(KnowledgeDocumentId, Vec<Edge>)>` batched API on `cairn-graph`. Three `GraphQueryService` impls update: `InMemoryGraphStore`, `PgGraphStore` (with `WHERE node_id IN (...)` SQL), test-only `MemGraph`. The two Arc forwarder impls in `crates/cairn-graph/src/in_memory.rs` also gain the method
- Runtime post-hoc rescorer using `multi_neighbors` + batched credibility/corroboration lookup; discards provider-returned runtime-owned dimensions
- Scoring-policy validation at `PUT /v1/projects/:id/scoring-policy` time — rejects writes referencing unavailable dimensions
- Integration tests 6 and 7 from §"Integration Tests"

### PR C — Compliance suite

- `crates/cairn-knowledge-compliance/` — shape-only compliance tests:
  - Wire-type round-trips
  - Required-field presence in `KnowledgeQueryResult`
  - Scoring-dimension tri-state declared matches actually-surfaced
  - Runtime-owned dimension injection is rejected/overwritten
  - Error-shape stability
  - Diagnostics parity: `computed_by` marker populated correctly
- Suite passes against `InMemoryRetrieval`
- Suite passes against a minimal mock plugin
- Operator UI: per-query diagnostics view shows provider + per-dimension producer

## Integration Tests (Compliance Proof)

RFC is considered implemented when:

1. Capability declaration round-trips — manifest parses, serializes, handshake succeeds; tri-state scoring-dimension declaration enforced; missing declarations rejected with specific error
2. `cairn-default` behavior unchanged — every existing `cairn-memory` integration test passes with `MultiProviderRetrieval` in front of `InMemoryRetrieval`
3. Plugin dispatch works — mock plugin receives `knowledge.query`, returns canned response, runtime surfaces it through `RetrievalService::query`
4. Per-project isolation — two projects with different provider configs dispatch to different providers
5. Runtime post-hoc rescoring — every result has `graph_proximity`, `source_credibility`, `corroboration` populated with `computed_by = "runtime_post_hoc"` markers
6. Runtime overwrites provider-injected runtime-owned dimensions — mock plugin returning non-null `graph_proximity` is unconditionally overwritten (compliance-suite asserts)
7. Batched graph lookup works — query returning 10 chunks triggers exactly one `multi_neighbors` call (not 10)
8. Scoring policy rejection — policy write referencing a dimension the resolved provider declares unsupported is rejected with structured error
9. `memory_store` hidden under read-only provider — project with `plugin:bedrock-kb` does not show `memory_store` in an agent's tool list (`is_tool_visible` returns false)
10. Deep search works across providers — deep-search call against a project with `plugin:bedrock-kb` produces multi-hop synthesizing several single-hop calls
11. Provider-unavailable fails loudly — query against a project whose plugin is not running fails with `RetrievalError::ProviderUnavailable` + `KnowledgeProviderUnavailable` event; no silent fallback
12. Provider-required dimensions declarable as `not_supported` — provider declaring `freshness_decay = "not_supported"` serves queries; field comes through null; operator UI shows "not surfaced by this provider"
13. Compliance suite green against cairn-default and mock plugin — both pass `cairn-knowledge-compliance` without modification

## Non-Goals

- No multi-provider query federation
- No built-in migration tooling — operators re-source
- No quality-floor compliance in v1
- No performance SLAs on plugin providers from cairn's side
- No memory-domain work (separate follow-on RFC)
- No public catalog `download_url` for reference adapters during pre-v1
- No per-project dynamic tool descriptions in v1 (static descriptions retained)

## Deferred Questions (revisit when the question matters)

1. **Adapter repos go public when?** Deferred — no adapter is listable yet since neither `cairn-knowledge-bedrock-kb` nor `cairn-knowledge-mem0` exists as a binary. Revisit when the first adapter ships.
2. **Memory follow-on RFC timing.** Deferred — no commitment to a date. When episodic-rs's API stabilizes, a sibling RFC adds `MemoryProvider` as a second capability family alongside `KnowledgeProvider`. This RFC's architecture is designed to accommodate that without rework.

## Decided

- Default provider name: `"cairn-default"`
- Plugin spawn: lazy, per RFC 015's tool-only rule
- Compliance suite location: `crates/cairn-knowledge-compliance/` in main cairn-rs repo
- Deep search: runtime-orchestrated, no per-provider declaration
- Scoring: split into provider-required (5, tri-state declaration) and runtime-owned (3, unconditionally runtime-computed)
- Adapter repos: `avifenesh/*` personal, private during pre-v1; NOT listed in bundled catalog
- Tool visibility: amended into RFC 015 via `is_plugin_tool_visible` → `is_tool_visible` rename + `VisibilityContext.resolved_knowledge_provider` field
- Post-hoc scoring cost: acknowledged as additive-O(1) via batched `multi_neighbors`, not "cheap" handwave
- Provider-injection protection: runtime unconditionally overwrites runtime-owned dimensions
- Table names: plural-snake-case per existing convention
- Tool description: stays static in v1
- Freshness naming: wire is `freshness_decay`, in-process rename in PR A
- Command naming: `ConfigureKnowledgeProvider`
- `ChunkRecord` transitive dependencies: `cairn-plugin-proto` depends on `cairn-domain` for shared ID types (`ProjectKey`, `SourceId`, `KnowledgeDocumentId`, `ChunkId`). Cross-language adapter authorship stays a v2 concern; the common cairn pattern (crate depends on `cairn-domain` for ID types) applies here.
- Capability detail at handshake, not manifest: Layer 2 (`initialize`) carries `retrieval_modes`, `ingest_capable`, `scoring_dimensions`. Layer 1 (manifest) declares only family.
- No compile-time feature gating on adapter binaries.

## Decision

Proceed with the amendments to RFC 003, RFC 007, and RFC 015 above, the new capability family, the four-PR implementation path (A / B1 / B2 / C), and the two private reference adapter repos.
