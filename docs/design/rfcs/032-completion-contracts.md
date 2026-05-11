# RFC 032: Completion Contracts — definition-of-done for agent runs

Status: draft (round 3, final — pre-implementation)
Owner: orchestration
Depends on: RFC 016 (sandbox workspace), RFC 018 (agent loop), RFC 020 (durable recovery), RFC 022 (signal routing), #670 G7 (subagent→parent step history), #813 / #844 (workspace path rendering), #825 (FailRun)
Parallel track: capability propagation (separate RFC, filed as follow-up) — closes R38 in conjunction with this RFC, not alone.

Review history: 3 rounds of proposer / challenger debate. Round 0 draft reviewed by 3 parallel adversarial challengers (domain/API correctness, verifier safety, product/UX) — 22 findings, 20 absorbed, 2 rebutted. Round 1 revision reviewed by a regression-focused challenger — 10 findings, all absorbed into this final text. Full transcripts are scratch artefacts and are NOT checked in per RFC-025/027 convention. Inflection points baked into the body; appendix summarises.

## Summary

A cairn run today ends when the LLM calls `complete_run` with a freeform `final_answer`. The completion gate inspects the surrounding tool-output buffer for errors (#660), the answer text for admission sentinels (#831 family), and offers a `fail_run` verb for truthful abandonment (#825). None of those three check whether the thing the operator asked for actually exists.

R38 dogfood (2026-05-11) exposed the gap: sub-agents wrote code into their own sandbox, ran `cargo check`, and called `complete_run` without committing, pushing, or opening a PR. The gate correctly rejected because the model self-admitted the omission — but a more careful model could have said "done, cargo check is green" and the gate would have accepted a run that shipped no deliverable.

The gate is a lie detector. This RFC adds a contract checker.

`CompletionContract` is an optional per-run declaration of what the deliverable is and how cairn verifies its existence. When present (explicit or inferred), the gate runs the declared verifier before accepting `complete_run`. When absent, the run gets a permissive `ProseNonEmpty` floor — effectively today's behaviour.

The primitive is shape-agnostic: code, research, triage, vendor choice, status report, and operational-action goals each get their own verifier. Contracts attach to any run — root or sub-agent — so the fleet case composes: each node in the tree has its own definition of done.

**This RFC does not, on its own, close R38.** Sub-agents in R38 couldn't commit-push-PR because their sandbox lacked GitHub credentials. Contracts correctly refuse such runs but do not *create* the missing capability. Closing R38 also requires capability propagation (separate RFC). Contracts and capability are parallel pieces; both must land before fleet-dogfood runs ship PRs end-to-end. This RFC is a prerequisite for the capability RFC: without a contract, the capability fix cannot be measured against anything concrete.

## Why

Cairn ships a runtime for arbitrary agent goals. Today's gate semantics silently assume the LLM is a good-faith reporter — if `final_answer` doesn't trip an admission sentinel and no tool in the buffer errored, the run is accepted. For goals with objective deliverables, that's too weak. Operator asked for a PR; cairn should check for a PR. Operator asked for research with citations; cairn should verify citations resolve. Operator asked for a ticket to be closed; cairn should re-read ticket state.

Without this, every goal shape drifts the same way: whether a run ships depends on LLM diligence, not on cairn's contract. Dogfood R38 demonstrated the drift on code; production teams using cairn hit the equivalent across every goal shape.

The fix is architectural, not prompt-engineering. No amount of "remember to open the PR" in the role prompt closes the gap — the gate has to refuse the false claim, not ask the model to stop making it.

## Non-goals

- Not a replacement for the strict gate (#660), admission sentinel scan (#831 family), or FailRun (#825). Orthogonal, stacked.
- Not a dynamic / operator-programmable verifier in v1. Four verifier kinds ship in Phase 1; a pluggable registry is RFC-material on its own (Phase 4 placeholder).
- Not binding. Operators can supply a contract, let cairn infer, or get the permissive floor. Cairn serves the operator; does not force.
- Not a quality judge. Contracts check existence and shape, not value. "Is this research thorough?" out of scope; "is there at least one citation that resolves?" in scope. Quality-judging is its own RFC.
- Not a capability fix. See "Parallel: capability propagation" below. Out of scope here, explicitly named.

## Parallel: capability propagation (out of scope, named)

Round-1 challenger C walked the R38-post-RFC trajectory: three sub-agents get an inferred `PullRequest` contract; each writes code and calls `complete_run` without a PR; gate correctly rejects with `contract_not_met[pull_request]`; each sub-agent then either (a) truthfully `fail_run`s because there are no git credentials in the sandbox, (b) hallucinates a `pr_url` that 404s, or (c) loops to the iteration cap. None ships a PR.

**The contract did its job — it refused a false-positive completion.** What it did not do is grant sub-agents the credentials they need to succeed on retry. That is the capability-propagation problem:

- Sub-agent sandbox has no `gh` authentication token
- Sub-agent sandbox has no SSH key for `git push`
- Sub-agent sandbox may have no network at all, depending on sandbox policy

Cairn holds the tenant's GitHub App install token in `cairn-github`. Propagating that (scoped, time-bounded) into a sub-agent's sandbox is the missing piece, in its own RFC.

**This RFC is a strict prerequisite for the capability RFC.** Without contracts, we cannot measure whether a capability fix produced what was asked for. Shipping contracts first gives the capability RFC something to measure against.

## Proposal

### 1. Domain primitive

Add `CompletionContract` to `cairn-domain`, tagged with the dominant workspace convention (`#[serde(tag = "kind", rename_all = "snake_case")]` — cf. 9 of 11 existing data-carrying enums in cairn-domain).

Six variants v1; two stubbed for Phase 2/3:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CompletionContract {
    /// Permissive floor. final_answer non-empty. Matches today's
    /// effective behaviour when a run has no contract and inference
    /// finds no trigger.
    ProseNonEmpty,

    /// Prose with minimum length and minimum resolvable citation
    /// count. "Resolvable" means http(s):// URL tokens whose
    /// presence-verifier check doesn't 404 within budget. Inference
    /// default for research / compare / audit / investigate goals.
    Prose {
        min_chars: u32,         // default 500
        min_citations: u32,     // default 2
    },

    /// One or more files must exist under the run's resolved
    /// workspace, optionally matching a bounded contains regex.
    /// Only valid when the run has a persistent workspace
    /// (allowlisted repo sandbox or local_fs path). Ephemeral runs
    /// reject at contract-accept time (see §2.4).
    File {
        paths: Vec<FileRequirement>,
    },

    /// A PR exists, owned by the run's project, and optionally
    /// matching shape constraints. Verified via cairn-github. The
    /// verifier REJECTS if expected_repo is not in the run's
    /// ProjectRepoAccessService allowlist — same-tenant only.
    PullRequest {
        expected_repo: Option<String>,       // "owner/repo"
        expected_head_branch: Option<BoundedRegex>,
        must_be_open: bool,                  // default true
    },

    /// final_answer parses as JSON matching an operator-supplied
    /// ContractSchema (validated JSON Schema). PHASE 2 — variant
    /// lands in domain; verifier returns NotImplemented in Phase 1.
    Structured {
        schema: Box<ContractSchema>,
    },

    /// Cairn confirms external-system side effect. Webhook-first
    /// with poll fallback. PHASE 3 — variant lands in domain;
    /// verifier returns NotImplemented in Phase 1.
    ExternalState {
        check: ExternalStateCheck,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRequirement {
    pub path: RelPath,
    pub contains_regex: Option<BoundedRegex>,
    pub max_bytes: Option<u64>,
}
```

#### 1.1. `ContractSchema` — typed wrapper, not `serde_json::Value`

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "serde_json::Value", into = "serde_json::Value")]
pub struct ContractSchema {
    inner: serde_json::Value,
}

impl ContractSchema {
    pub fn try_new(value: serde_json::Value) -> Result<Self, SchemaError> {
        let serialized = serde_json::to_vec(&value)
            .map_err(|_| SchemaError::NotSerializable)?;
        if serialized.len() > 32 * 1024 {
            return Err(SchemaError::TooLarge {
                size: serialized.len(),
                cap: 32 * 1024,
            });
        }
        jsonschema::validator_for(&value)
            .map_err(|e| SchemaError::InvalidJsonSchema(e.to_string()))?;
        Ok(Self { inner: value })
    }
}
```

Validator crate: `jsonschema` (added to cairn-runtime in Phase 2; domain can hold the typed wrapper without the validator wired until Phase 2 ships). `schemars` remains the derivation crate for OpenAPI schema generation; `jsonschema` is the runtime validator. Distinct concerns, distinct crates.

Serde `try_from` / `into` runs the validator on every deserialization, so a `ContractSchema` on the wire cannot be forged — a malformed schema rejects at `POST /v1/runs` body-parse time, not at gate-time surprise.

#### 1.2. `RelPath` + `BoundedRegex` — type-level safety

Close path-traversal and ReDoS at the type level. Symlink hardening is done at verifier-runtime (§4.2) because symlinks are a filesystem concern, not a type concern.

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RelPath {
    components: Vec<String>,
}

impl RelPath {
    pub fn try_new(raw: &str) -> Result<Self, PathError> {
        let p = Path::new(raw);
        if p.is_absolute() {
            return Err(PathError::Absolute);
        }
        let mut out = Vec::new();
        for c in p.components() {
            match c {
                Component::Normal(os) => {
                    out.push(os.to_string_lossy().into_owned());
                }
                Component::CurDir | Component::ParentDir => {
                    return Err(PathError::Traversal);
                }
                Component::RootDir | Component::Prefix(_) => {
                    return Err(PathError::Absolute);
                }
            }
        }
        if out.is_empty() {
            return Err(PathError::Empty);
        }
        Ok(Self { components: out })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct BoundedRegex {
    pattern: String,
}

impl BoundedRegex {
    pub fn try_new(raw: String) -> Result<Self, RegexError> {
        // Rust's regex crate is RE2-style (linear match time), but
        // compile-time memory is not bounded by default. Explicit
        // caps protect against pathological compile inputs.
        regex::RegexBuilder::new(&raw)
            .size_limit(64 * 1024)
            .dfa_size_limit(256 * 1024)
            .build()
            .map_err(|e| RegexError::Invalid(e.to_string()))?;
        Ok(Self { pattern: raw })
    }
}
```

Both types use `try_from`/`into` round-trips so a forged / oversized pattern rejects at deserialization.

### 2. Attach point

#### 2.1. Three write paths, one resolution path

- **Explicit on run creation**: new optional `completion_contract` field on `POST /v1/runs` body (shape: `CompletionContract` with external tag `kind`).
- **Explicit on spawn**: new optional `completion_contract` arg on the `spawn_subagent` tool (native tool-call shape AND the legacy nested-args shape, using the same `extract_spawn_subagent_optionals` helper #847 introduced). Sub-agents do NOT auto-inherit from parent — the orchestrator decides what each child is on the hook for. This matches #844's "orchestrator never touches paths; runtime allocates" discipline: the orchestrator describes the deliverable, the runtime verifies it.
- **Inferred**: when no contract is supplied, `infer_contract(goal: &str) -> CompletionContract` runs once at first-orchestrate boot and persists. See §2.3.

All three land in the same storage slot: a run-default under `run:<run_id>:completion_contract` using the `DefaultSettingService` pattern (#775/#813 precedent).

#### 2.2. Typed defaults read + size caps

Challenger A #2 and D #10 both surfaced that `validate_setting_value` (cairn-app/src/handlers/health.rs:981) caps only strings at 4096; non-string values slip through to axum's 10MB body limit.

New plumbing:

- `DefaultSettingService::set_struct<T: Serialize>(key, value) -> Result<_, SizeError>` in cairn-runtime. Serializes `value` to JSON, enforces a per-key cap (64 KiB for contract), writes via the existing `set(..., serde_json::Value)` path. Call site cap is layered with the HTTP-layer cap — both must be set; runtime is the authoritative gate, HTTP is the early-reject nice-to-have.
- `DefaultSettingService::get_struct<T: DeserializeOwned>(key) -> Result<Option<T>, _>` — reads the stored JSON, deserializes. Deserialization failure (drift, corruption, pre-rollback data) returns `Err`; gate logs at INFO and falls back to `ProseNonEmpty`, not panic.

Contract is attached at POST /v1/runs body validation (enforces the 64 KiB cap independently of the runtime service — defense in depth).

#### 2.3. Inference is narrow, one-shot, transparent, re-firing on goal change

Inference triggers (regex, case-insensitive, word-boundaried):

| Trigger | Kind |
|---|---|
| `\b(open\|create\|ship\|submit)\b[^.]{0,80}\bpull request\b` OR `\b(open\|ship)\b[^.]{0,20}\bPR\b` | `PullRequest { must_be_open: true, rest: None }` |
| `\b(research\|compare\|audit\|investigate)\b` | `Prose { min_chars: 500, min_citations: 2 }` |
| `\bclose\b[^.]{0,40}(issue\|ticket)\b[^.]*\b([A-Za-z0-9._-]+/[A-Za-z0-9._-]+)#(\d+)\b` | `ExternalState::GitHubIssueClosed { repo, number }` (Phase 3 — v1 emits event but verifier returns NotImplemented) |
| no match | `ProseNonEmpty` |

The two-letter "PR" trigger requires disambiguation (`open` or `ship` within 20 chars) so "PR as public relations" doesn't false-fire.

**Bias named**: inference biases toward false-positive on ambiguous goals. Stated directly in rollout docs: if your goal is conditional ("research, and if you find something, open a PR"), declare the contract explicitly — inference picks one kind, not both.

**Re-inference on goal change** (closes Challenger A #3, D #3):

- New persisted slot `run:<run_id>:contract_source_goal_hash` = `sha256(resolved_goal)[:16]`.
- At every orchestrate-boot: compare current resolved_goal's hash to stored. If changed AND contract source was `inferred`, re-infer, persist new contract, update stored hash, emit `CompletionContractResolved { source: ContractSource::ReInferredOnGoalChange }`.
- Explicit contracts do NOT auto-change on goal pivot. A goal-change with an explicit contract emits `WARN` to operator trace (never LLM step_history) pointing at the mismatch.
- **Concurrency**: guard re-inference behind a per-run advisory lock (reuse the existing `acquire_run_advisory_lock` pattern — cf. `handlers/runs/orchestrate.rs` F58 lease-renew-discipline). Two concurrent orchestrate POSTs: first wins lock, does re-inference, releases; second sees updated hash, no-op. Projection for the event is keyed by `(run_id, contract_source_goal_hash)` so double-emission on race is idempotent at the read-model layer.

**Every resolution emits `CompletionContractResolved`** (domain event, Projected in `projection_registry.rs` against a new `completion_contracts` read-model table; key `(run_id, goal_hash)`, last-write-wins within the key). The trajectory endpoint (#794) surfaces the resolved contract so operators see it before the gate ever fires.

#### 2.4. Contract validity vs workspace shape

`File` contract only validates on runs with a persistent workspace. The check fires at contract-accept time (POST /v1/runs body or spawn_subagent dispatch), NOT at gate time — a run that cannot satisfy its contract should reject early so the operator gets a 400 before tokens are spent.

Plumbing (closes D #7): `create_run_handler` (`crates/cairn-app/src/handlers/runs/lifecycle.rs:376`) gets a new step between session-existence-check and run-creation: if contract is `File`, consult `ProjectRepoAccessService.list_for_project()` OR `ProjectLocalPaths.list()`. Empty → reject with `400 contract_invalid: file_contract_requires_persistent_workspace`. Same check fires in the spawn_subagent dispatch path at execute_impl, rejecting the spawn attempt rather than dispatching a doomed child.

### 3. Gate integration

`completion_verification.rs` gains a fourth reject condition on the `ActionType::CompleteRun` path, ordered by cost:

```
1. if error_bucket_has_errors → reject(VerificationRejected)
2. if sentinel_scan_fires     → reject(VerificationRejected)   [unchanged]
3. if action_type == FailRun  → fail(ModelReportedFailure)     [unchanged]
4. if action_type == CompleteRun && !contract.verify(...) →
                                  reject(ContractNotMet)       // NEW
5. accept
```

Check 4 only runs on `CompleteRun`; `FailRun` short-circuits. (OQ-5 from round 0 is resolved this way.)

`ContractNotMet` is a new variant of `FailureClass`. Distinguishes from `VerificationRejected` in dashboards and projections.

#### 3.1. Typed diagnostic codes (closes B #6, D #8)

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    // ... existing variants ...
    ContractNotMet {
        code: ContractRejectionCode,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractRejectionCode {
    // Prose / ProseNonEmpty
    ProseEmpty,
    ProseTooShort,
    ProseInsufficientCitations,
    ProseCitationUnresolvable,

    // File
    FileMissing,
    FileSymlinkTraversal,
    FileExceedsMaxBytes,
    FileRegexNoMatch,

    // PullRequest
    PrNotInProjectAllowlist,
    PrUrlMalformed,
    PrUrlMissing,
    PrNotFound,
    PrHeadBranchMismatch,
    PrNotOpen,

    // Structured (Phase 2)
    StructuredParseError,
    StructuredSchemaMismatch,

    // ExternalState (Phase 3)
    ExternalStateNotConfirmed,

    // Generic
    VerifierTimeout,
    VerifierUnavailable,
    NotImplemented,
}
```

Code registry lives as the `ContractRejectionCode` enum in `cairn-domain/src/lifecycle.rs` next to `FailureClass`. Verifiers return `Result<(), (ContractRejectionCode, OperatorTrace)>` where `OperatorTrace` is a non-serialized context struct that never crosses the domain boundary; the gate logs it at `tracing::warn!` scoped to `run_id` and ships only the code into step_history.

The LLM-visible diagnostic is always: `contract_not_met[<kind>]: code=<snake_case_code>. See operator logs for details.` Matches the PR-2 #847 redaction pattern. No cross-tenant content can ever reach step_history.

### 4. Verifier execution

Verifiers run inline in the orchestrator completion path. Constraints:

- **Tenant scoping first.** Before any external call, verifier asks `ProjectRepoAccessService`: is `expected_repo` (or `ExternalStateCheck.repo`) in this run's allowlist? If not, reject with `PrNotInProjectAllowlist`. This is enforced *before* `cairn-github` client selection — we never resolve an install_id for a repo outside the run's project. Closes Challenger B #1 at the architectural level.
- **Reqwest clients carry explicit timeouts.** cairn-github's `GitHubClient` is rebuilt (or a verifier-specific clone is vended) with `.timeout(Duration::from_secs(3))` at construction. Verifier-level `tokio::time::timeout(Duration::from_secs(5), fut)` wraps the whole verify call. Both layers enforced. Closes B #7.
- **Rate-limit budget**: verifier calls share cairn-github's GitHubClient; a follow-up issue (filed alongside Phase 1) tracks 429 handling + `X-RateLimit-Remaining` telemetry across call sites. Contract verification is one more consumer, not a special snowflake.

#### 4.1. Contract shape rendered in user message (closes D #2)

Revised invariant 4. The resolved contract's shape is rendered into the **user message** (not the system prompt) as a new `## Completion contract` section, produced by `build_user_message_with_role` (decide_impl.rs:1181). Parallel to `## Run state` and `## Parent context`. Example rendered text for a `PullRequest` contract:

```
## Completion contract
kind: pull_request
expected_repo: avifenesh/cairn-dogfood-roguelike
expected_head_branch: ^m1/01-cargo-init$
must_be_open: true

Your complete_run's final_answer must include a JSON object with a
`pr_url` field pointing at a real PR matching the above. See
run-docs for the full contract schema.
```

This is not in the role prompt (which is capped at 7650 chars per #846) — it's per-iteration user-message context, uncapped. The LLM sees what it's being graded on without blowing the role-prompt budget. `## Completion contract` is rendered for sub-agent roles; orchestrator-tier roles do not get it because the orchestrator is not the actor producing the final_answer for contract verification (its contract is verified on its own complete_run, at which point the orchestrator renders its OWN user message with the contract visible).

#### 4.2. File verifier: symlink-safe path resolution (closes D #1)

`canonicalize` on a sandbox-internal symlink like `logs/current -> /etc/passwd` can resolve outside working_dir in the overlay-mount case. The File verifier MUST NOT use naive `canonicalize`; instead:

```rust
// For each FileRequirement, walk components under working_dir:
//   - For each component, call fs::symlink_metadata (does NOT follow)
//   - If symlink: reject with FileSymlinkTraversal; NEVER follow
//   - If directory: recurse
//   - If regular file: proceed
// Only open/read after the full walk completes.
// Linux production path: openat2(RESOLVE_NO_SYMLINKS | RESOLVE_BENEATH)
// on the starting fd of working_dir. Non-Linux fallback: manual walk.
```

Symlinks inside the declared path are a reject, not an escape. Operators with legitimate symlink targets declare the symlink's target directly.

The `contains_regex` match runs against the file content with a streaming reader capped at `FileRequirement.max_bytes` (default 16 MiB) — bounds memory independently of the regex crate's own linearity guarantee.

#### 4.3. ExternalState: webhook-first, structured filter (closes D #4, Phase 3)

Not in Phase 1 (variant returns `NotImplemented`). Design for Phase 3:

- `SignalRouterService::subscribe` (cairn-runtime/src/services/signal_router_impl.rs:46) already supports per-run subscriptions via `target_run_id: Option<RunId>`. Contract resolution subscribes the run to the relevant signal kind.
- Existing `signal_matches_filter` uses substring-on-payload — too loose. Phase 3 introduces typed filter variants on signals that the ExternalState verifier uses: `GitHubIssueClosedFilter { repo: String, number: u64 }` applied as structured field-match, not substring.
- At `complete_run`, gate checks the run's signal inbox for a matching event delivered since contract resolution. Yes → accept. No → one poll via cairn-github as fallback (bounded by §4 timeouts).

Explicit degradation: tenants without cairn GitHub App installed get the poll path only.

### 5. Fleet / tree semantics

**Every node in the run tree can carry its own contract.** No automatic inheritance.

- Root runs: contract arrives via `POST /v1/runs` body or gets inferred.
- Sub-agent runs: contract arrives via `spawn_subagent(... completion_contract: Option<CompletionContract>)`. If orchestrator supplies one, child's `complete_run` is gated against it. If not, child's goal is inferred (same as root). Sub-agent goals are typically narrower so inference hits reliably.
- Root's contract verifies the ROOT's final_answer, not the aggregate of child deliverables. If root's synthesis requires "all 3 children shipped PRs," root's contract is a `Structured` variant whose schema enforces the aggregate shape.

#### 5.1. Root-level synthesis visibility (closes D #5)

Round 2 flagged: children report prose; root parses PR URLs out of step_history by regex, which is fragile. Fix: when a child terminates with a verified contract, cairn records its `ContractVerifiedOutput` (a structured record: kind + any extracted fields like `pr_url` for PullRequest) on the subagent_complete step, alongside the free-form summary.

Schema:

```rust
pub struct StepSummary {
    // ... existing fields ...
    pub verified_output: Option<ContractVerifiedOutput>,
}

pub enum ContractVerifiedOutput {
    Prose { citations_resolved: u32 },
    File { paths: Vec<PathBuf> },
    PullRequest { pr_url: String, head_sha: String },
    Structured { /* the verified JSON */ value: serde_json::Value },
    ProseNonEmpty,
}
```

The root's user-message render includes `verified_output` on each `subagent_complete` entry. Root's LLM, when synthesising, sees structured evidence not just prose. Root's own contract (e.g. `Structured { schema: {pr_urls: array<uri>} }`) has reliable input to satisfy.

The record is threaded through #670 G7's `build_subagent_complete_steps` (new field on StepSummary is a domain-layer change, fully additive; stored entries drop the field on read from older event logs).

### 6. Invariants (final)

1. A contract is resolved per run, not per iteration. Re-inference fires only when (goal text changed) AND (source was `inferred`) AND (per-run advisory lock acquired). Explicit contracts never auto-change; goal pivot with an explicit contract emits an operator-visible WARN.
2. Contract kinds are closed enums in cairn-domain. Adding a kind is a domain change. Pluggable verifier registry is Phase 4 and is its own RFC.
3. Verifier outcome is deterministic given (contract, final_answer, workspace state at check time, external state at check time). No LLM calls, no randomness, no retry within a single verifier invocation.
4. The verifier is runtime code, never a sub-agent call. The resolved contract's SHAPE is rendered into the child's **user message** (not system prompt), so the LLM knows what it's being graded on. This is read-only context.
5. Contracts never escalate post-resolution. Strictness changes only via re-inference on goal change (source `inferred` only) and is scoped to the new goal's trigger.
6. Tenant scoping enforced at verifier-entry: every external-facing verifier validates against run's `ProjectKey` allowlist before any external call, before any install_id resolution.
7. All LLM-visible diagnostics are code-based. Free-form external content never crosses into step_history. Full context lives in operator-scoped `tracing::warn!` trails keyed by run_id.
8. File-verifier paths are walked with `symlink_metadata`-per-component (or `openat2(RESOLVE_NO_SYMLINKS)` on Linux). Symlinks within declared paths are a reject, not an escape.
9. `CompletionContractResolved` projects into `(run_id, goal_hash)` → latest-wins read model. Double-emission on concurrent orchestrate POSTs is idempotent at the read-model layer.

## Rollout

**Phase 1** (first PR):
- Domain types: `CompletionContract`, `ContractSchema` (wrapper, no validator yet — validator lands in Phase 2), `RelPath`, `BoundedRegex`, `FailureClass::ContractNotMet { code }`, `ContractRejectionCode`, `RuntimeEvent::CompletionContractResolved` (Projected against new `completion_contracts` table in `projection_registry.rs`), `ContractVerifiedOutput` field on `StepSummary`.
- Verifiers: `ProseNonEmpty`, `Prose`, `File`, `PullRequest` fully implemented. `Structured`, `ExternalState` land as domain variants, verifier returns `NotImplemented`.
- Inference table + re-inference on goal change + advisory-lock guard + event emission.
- Gate integration at check 4.
- API: `completion_contract` on `POST /v1/runs` body + `spawn_subagent` tool.
- `## Completion contract` section in user message (sub-agent roles only).
- `create_run_handler` + spawn_subagent dispatch: reject `File` contract on ephemeral runs with `400 contract_invalid`.
- Tests: 35+. Unit per verifier, inference-table drift-guard, tree-integration, redaction pin (step_history never contains external content), goal-change re-inference with advisory-lock contention test, projection idempotency test, symlink-rejection test, size-cap test for `ContractSchema` and `CompletionContract` overall, backward-compat golden set (pre-contract shapes still pass `ProseNonEmpty`).

**Phase 2** (follow-up PR):
- `Structured` verifier wired (add `jsonschema` dep to cairn-runtime).
- UI: dashboard renders resolved contract; suggest-from-goal button; template library for common shapes.

**Phase 3** (follow-up PR):
- `ExternalState` verifier with webhook-first path; typed signal filters.
- Capability-gap telemetry surface: aggregated metric `runs_blocked_on_missing_capability` — dashboard counter that increments when a run terminates with `ContractNotMet` whose code suggests a capability mismatch (e.g. PR contract rejected N times, sub-agent sandbox lacks gh auth). Closes D #9.

**Phase 4** (its own RFC):
- Pluggable verifier registry.
- Named / saved contracts per project ("standard PR contract for backend team").

**Not in this RFC**: capability propagation. Parallel track. Prerequisite inversion: capability RFC cannot be validated without this one; this one is correct-but-incomplete for R38 without capability.

Dogfood validation target R39 (post-Phase 1): M1-1 with an inferred `PullRequest` contract. Expected outcomes (descending goodness):

1. Sub-agent ships a PR; verifier accepts. **Unlikely until capability RFC lands.**
2. Sub-agent doesn't ship; gate rejects `contract_not_met[pull_request]`; sub-agent truthfully `fail_run`s. Full gate chain working; R38 correctly diagnosed as capability-missing. Partial success.
3. Sub-agent hallucinates PR URL; verifier 404s; contract rejects again; hits rejection cap → `VerificationRejected`. Still correct.
4. Loop on contract rejection hits `MAX_COMPLETION_GATE_REJECTIONS`. Cap correctly firing.

Any outcome EXCEPT #1 indicates capability RFC is needed; none indicates contracts are wrong. Phase 3 capability-gap telemetry distinguishes 2-4 from "contract is broken."

## Open questions (surviving to implementation)

- **OQ-4**: PR verifier redirect handling on renamed repos. Default: reject on redirect with `PrNotInProjectAllowlist` (post-redirect repo is the one that must be in allowlist, and we haven't checked it; be conservative). Operator with renamed repos updates `ProjectRepoAccessService`.
- **OQ-9**: Amend-final_answer path for typo-class rejections. Punt to Phase 2; measure retry cost in Phase 1 dogfood.
- **OQ-10**: Operator education. Phase 1 ships API + trajectory events + docs; Phase 2 ships UI affordance. Success criterion: operators discover contracts without reading docs, after Phase 2.

## Appendix — Debate inflection points

**Round 0 → Round 1** (22 findings, 20 absorbed, 2 rebutted):
- Challenger A (domain/API): typed `ContractSchema` wrapper instead of raw `serde_json::Value`; serde `tag = "kind"` convention committed; `CompletionContractResolved` event added; size caps on typed storage.
- Challenger B (safety): tenant scoping as first verifier check; type-level `RelPath` / `BoundedRegex`; reqwest timeouts explicit; diagnostic redaction pattern; webhook-first `ExternalState`.
- Challenger C (product): R38 framing reversed — "contract is lie detector, not capability fix"; capability-propagation named as parallel track; contract rendered into LLM context; tree semantics added; inference bias (false-positive) named.
- Rebutted: plugin-loadable verifiers (different threat model than tool plugins; Phase 4 is correct deferral). Named registry (valid feature, Phase 2 UX not Phase 1 core).

**Round 1 → Round 2** (10 findings, all absorbed):
- Challenger D regression pass: symlink traversal via `canonicalize` (invariant 8 + §4.2 fix with openat2); contract-in-system-prompt wrong surface (moved to user message in §4.1); goal-change re-inference needed persisted hash + advisory lock (§2.3); webhook filter substring-match too loose (§4.3 typed filters); tree synthesis prose-parsing fragile (`ContractVerifiedOutput` field on StepSummary, §5.1); projection idempotency key (invariant 9); `File` ephemeral-reject plumbing named (§2.4); `ContractRejectionCode` enum in domain, not strings (§3.1); capability-gap telemetry surface (Phase 3, closes C #9); 64 KiB cap site in runtime service not HTTP validator (§2.2).

**Round 3** (this text) absorbs all of the above and is a pre-implementation RFC. Implementation-round review will surface new concerns — those land as PR comments and bug reports against the Phase 1 branch, not RFC revisions.
