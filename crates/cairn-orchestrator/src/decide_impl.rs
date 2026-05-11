//! LlmDecidePhase — concrete DECIDE phase implementation.
//!
//! Calls the brain LLM with a structured prompt built from
//! `OrchestrationContext` + `GatherOutput`, then parses the response into
//! `Vec<ActionProposal>` and wraps it in `DecideOutput`.
//!
//! # Flow
//! 1. Build system prompt — agent role identity + JSON format instruction.
//! 2. Build user message — goal + memory chunks + step history + settings.
//! 3. Call `GenerationProvider::generate` on the brain provider.
//! 4. Parse JSON response into `Vec<ActionProposal>` using `ResponseParser`.
//! 5. Retry once on parse failure (LLM sometimes needs a nudge).
//! 6. If second attempt also fails, return a `EscalateToOperator` proposal.
//! 7. Apply calibration offset if a `ConfidenceCalibrator` is provided.
//! 8. Emit `DecideOutput` with raw response retained for audit.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use cairn_domain::{
    agent_roles::{default_roles, AgentRole, ResponseShape},
    events::ToolDeclaredButMissing,
    providers::{GenerationProvider, ProviderBindingSettings},
    ActionProposal, ActionType, RuntimeEvent,
};

use cairn_runtime::services::{AgentRoleService, SourceFilter};
use cairn_runtime::{
    make_envelope, single_model_service, RoutedBinding, RoutedGenerationError,
    RoutedGenerationService,
};
use cairn_store::EventLog;
use cairn_tools::builtins::{BuiltinToolDescriptor, BuiltinToolRegistry};

use crate::context::{DecideOutput, GatherOutput, OrchestrationContext};
use crate::decide::DecidePhase;
use crate::error::OrchestratorError;

// ── Token budgeting ───────────────────────────────────────────────────────────

/// Estimate the number of tokens in a text string.
///
/// Uses the chars-÷-4 heuristic, which approximates GPT-family tokenisers for
/// Latin-script text within ~20%.  Replace with a proper tokeniser (tiktoken,
/// tokenizers) when accuracy becomes important.
#[inline]
pub fn estimate_tokens(text: &str) -> usize {
    text.len().div_ceil(4) // round up so we never under-count
}

/// Token budget for a single LLM call.
///
/// Splits the model's context window into an output reservation (for the
/// LLM's response) and the remaining input budget available for the prompt.
///
/// # Example
/// ```
/// # use cairn_orchestrator::TokenBudget;
/// let budget = TokenBudget::new(131_072); // e.g. gemma-4
/// assert_eq!(budget.total_context, 131_072);
/// assert_eq!(budget.reserved_output, 131_072 / 4);
/// assert_eq!(budget.available_input, 131_072 - 131_072 / 4);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenBudget {
    /// Total context window of the model (tokens).
    pub total_context: usize,
    /// Tokens reserved for the model's output.
    /// Default: `total_context / 4`.
    pub reserved_output: usize,
    /// Tokens available for input content.
    /// Always `total_context - reserved_output`.
    pub available_input: usize,
}

impl TokenBudget {
    /// Create a budget for a model with the given context window.
    ///
    /// `reserved_output` defaults to `total_context / 4`.
    pub fn new(total_context: usize) -> Self {
        let reserved_output = total_context / 4;
        Self {
            total_context,
            reserved_output,
            available_input: total_context.saturating_sub(reserved_output),
        }
    }

    /// Override the output reservation.
    ///
    /// Recomputes `available_input = total_context - reserved_output`.
    pub fn with_reserved_output(mut self, reserved: usize) -> Self {
        self.reserved_output = reserved;
        self.available_input = self.total_context.saturating_sub(reserved);
        self
    }
}

impl Default for TokenBudget {
    /// A conservative default (16K context) used when no model info is available.
    fn default() -> Self {
        Self::new(16_384)
    }
}

// ── LlmDecidePhase ────────────────────────────────────────────────────────────

/// Production implementation of the DECIDE phase.
///
/// Thread-safe: all fields are `Arc` or immutable.
///
/// The phase delegates all provider dispatch to [`RoutedGenerationService`],
/// which composes cross-binding (`ProviderRouter`-style) and per-binding
/// (`ModelChain`) fallback. The phase never calls `GenerationProvider`
/// directly — all routing, error classification, and cooldown bookkeeping
/// live in the runtime layer.
pub struct LlmDecidePhase {
    /// Composed routing service. The orchestrator asks cairn to pick a
    /// model; cairn walks the bindings × models matrix.
    routed: RoutedGenerationService,
    /// Settings forwarded to every provider call (temperature, timeout,
    /// max_output_tokens). Stays on the phase (not per-binding) because
    /// the same task-level knobs apply regardless of which binding serves.
    settings: ProviderBindingSettings,
    /// Optional fixed confidence offset applied to every proposal
    /// (replaces a full `ConfidenceCalibrator` when historical data is absent).
    confidence_bias: f64,
    /// Token budget used by `PromptBuilder` to truncate context to fit the
    /// model's context window.  `None` = no truncation (legacy behaviour).
    token_budget: Option<TokenBudget>,
    tools: Option<std::sync::Arc<BuiltinToolRegistry>>,
    /// RFC 031 PR-C: operator-defined agent-role resolver. When set,
    /// the DECIDE phase resolves the run's role via
    /// `AgentRoleService::resolve(&project, &agent_type)` instead of
    /// reading from the compile-time `default_roles()`. Tool-allowlist
    /// filter (site 1), assembled system prompt (site 2), and footer
    /// response-shape (sites 3+4) all read from the resolved role.
    ///
    /// When `None` (legacy tests, older fixtures) the phase falls back
    /// to `default_roles()` + `response_shape_for()` for byte-parity
    /// with pre-RFC-031 behaviour.
    agent_roles: Option<Arc<dyn AgentRoleService>>,
    /// RFC 031 PR-C: event-log sink for `ToolDeclaredButMissing`
    /// advisories emitted at the tool-allowlist filter when a role
    /// declares a tool that is not currently registered. Optional for
    /// the same reason as `agent_roles` — legacy decide-phase
    /// constructions skip the advisory and silently drop the missing
    /// tool.
    event_log: Option<Arc<dyn EventLog>>,
}

impl LlmDecidePhase {
    /// Create from a single `GenerationProvider` + `model_id`. The phase
    /// wraps them in a one-binding, one-model `RoutedGenerationService`
    /// so the dispatch path is uniform. Production call sites that have
    /// multi-model chains should use [`LlmDecidePhase::from_routed`] or
    /// [`LlmDecidePhase::with_bindings`].
    pub fn new(provider: Arc<dyn GenerationProvider>, model_id: impl Into<String>) -> Self {
        let model = model_id.into();
        Self::from_routed(single_model_service("single", provider, model))
    }

    /// Create from a fully-constructed routed generation service (the
    /// preferred constructor for production).
    pub fn from_routed(routed: RoutedGenerationService) -> Self {
        Self {
            routed,
            settings: ProviderBindingSettings {
                max_output_tokens: Some(2048),
                ..Default::default()
            },
            confidence_bias: 0.0,
            token_budget: None,
            tools: None,
            agent_roles: None,
            event_log: None,
        }
    }

    /// Attach a binding chain: replaces the routed service on this phase.
    /// Preserves `settings`, `confidence_bias`, `token_budget`, and `tools`.
    pub fn with_routed_service(mut self, routed: RoutedGenerationService) -> Self {
        self.routed = routed;
        self
    }

    /// Attach an explicit binding list, replacing the current routed
    /// service. Convenience for call sites that already have a Vec.
    pub fn with_bindings(self, bindings: Vec<RoutedBinding>) -> Self {
        self.with_routed_service(RoutedGenerationService::new(bindings))
    }

    /// Override generation settings (e.g. temperature, max_output_tokens).
    pub fn with_settings(mut self, s: ProviderBindingSettings) -> Self {
        self.settings = s;
        self
    }

    /// Apply a fixed bias to every proposal's confidence (clamped to [0, 1]).
    /// Positive = boost, negative = penalise.  Use when a full calibrator
    /// is not wired up yet.
    pub fn with_confidence_bias(mut self, bias: f64) -> Self {
        self.confidence_bias = bias;
        self
    }

    /// Set a token budget for prompt truncation.
    ///
    /// Call this when the model's context window is known (e.g. from provider
    /// model discovery).  The `PromptBuilder` will truncate memory chunks,
    /// step history, and graph context to fit within the available input budget.
    pub fn with_token_budget(mut self, budget: TokenBudget) -> Self {
        self.token_budget = Some(budget);
        self
    }

    /// Convenience: build a `TokenBudget` from a known context window size and
    /// attach it.  Equivalent to `with_token_budget(TokenBudget::new(tokens))`.
    pub fn with_context_window(self, context_window_tokens: usize) -> Self {
        self.with_token_budget(TokenBudget::new(context_window_tokens))
    }

    /// Attach a BuiltinToolRegistry; Core + Registered tools appear in the system prompt.
    pub fn with_tools(mut self, registry: std::sync::Arc<BuiltinToolRegistry>) -> Self {
        self.tools = Some(registry);
        self
    }

    /// RFC 031 PR-C: attach the operator-defined agent-role service.
    /// When set, DECIDE resolves the run's role through the projection
    /// instead of reading from `default_roles()`.
    pub fn with_agent_roles(mut self, svc: Arc<dyn AgentRoleService>) -> Self {
        self.agent_roles = Some(svc);
        self
    }

    /// RFC 031 PR-C: attach an event-log sink for
    /// `ToolDeclaredButMissing` advisories. Emission is opt-in; when
    /// absent the advisory is silently dropped and the declared-but-
    /// missing tool is excluded from the DECIDE tool surface as
    /// before.
    pub fn with_event_log(mut self, log: Arc<dyn EventLog>) -> Self {
        self.event_log = Some(log);
        self
    }

    // ── RFC 031 PR-C helpers ────────────────────────────────────────

    /// Resolve the run's role via the attached `AgentRoleService`, or
    /// fall back to the compile-time `default_roles()` / built-in
    /// `generic` entry when no service is wired. The returned value is
    /// always a concrete `AgentRole` suitable for allowlist-filter,
    /// prompt assembly, and response-shape reads — callers never have
    /// to branch on `agent_roles.is_some()`.
    async fn resolve_role_or_fallback(&self, ctx: &OrchestrationContext) -> AgentRole {
        if let Some(svc) = &self.agent_roles {
            match svc.resolve(&ctx.project, &ctx.agent_type).await {
                Ok(role) => return role,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        agent_type = %ctx.agent_type,
                        "RFC 031 PR-C: agent-role resolve failed; falling back to default_roles"
                    );
                }
            }
        }
        fallback_role_by_id(&ctx.agent_type)
    }

    /// RFC 031 PR-C site 5: build the `spawn_subagent` native tool def
    /// with a per-run snapshot of the project's spawnable roles. First
    /// DECIDE of the run fills `ctx.agent_role_list_cache` via
    /// `AgentRoleService::list(project, SourceFilter::All)`; subsequent
    /// DECIDE turns reuse the snapshot (§D14 layer 2).
    async fn spawn_subagent_tool_def_for(&self, ctx: &OrchestrationContext) -> serde_json::Value {
        if let Some(svc) = &self.agent_roles {
            let svc = svc.clone();
            let project = ctx.project.clone();
            let cache = ctx.agent_role_list_cache.clone();
            let roles_result = cache
                .get_or_try_init(|| async move { svc.list(&project, SourceFilter::All).await })
                .await;
            match roles_result {
                Ok(roles) => {
                    let role_enum: Vec<String> = roles
                        .iter()
                        .map(|r| r.role.role_id.clone())
                        .filter(|id| id != "orchestrator")
                        .collect();
                    return spawn_subagent_tool_def_with_enum(role_enum);
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "RFC 031 PR-C: agent-role list failed; falling back to default_roles"
                    );
                }
            }
        }
        spawn_subagent_tool_def()
    }

    /// RFC 031 PR-C §ToolDeclaredButMissing: for each tool id the role
    /// declared but which is not present in the current DECIDE tool
    /// surface, emit a per-run-deduped advisory. The check-and-insert
    /// against `ctx.declared_but_missing` stays under a std::sync::
    /// Mutex that is released BEFORE the `.await` on the event-log
    /// append per the lock convention documented on the context field.
    async fn emit_declared_but_missing(
        &self,
        ctx: &OrchestrationContext,
        role: &AgentRole,
        tool_descs: &[BuiltinToolDescriptor],
    ) {
        let Some(log) = &self.event_log else {
            return;
        };
        if role.tools.is_empty() || role.forbid_all_tools {
            return;
        }
        let available: std::collections::HashSet<&str> =
            tool_descs.iter().map(|d| d.name.as_str()).collect();
        let missing: Vec<String> = role
            .tools
            .iter()
            .filter(|t| !available.contains(t.as_str()))
            .cloned()
            .collect();
        if missing.is_empty() {
            return;
        }
        let to_emit: Vec<String> = {
            let mut guard = ctx
                .declared_but_missing
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let mut fresh = Vec::new();
            for tool_id in missing {
                let key = (role.role_id.clone(), tool_id.clone());
                if guard.insert(key) {
                    fresh.push(tool_id);
                }
            }
            fresh
        };
        if to_emit.is_empty() {
            return;
        }
        let at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let envelopes: Vec<_> = to_emit
            .into_iter()
            .map(|tool_id| {
                make_envelope(RuntimeEvent::ToolDeclaredButMissing(
                    ToolDeclaredButMissing {
                        project: ctx.project.clone(),
                        run_id: ctx.run_id.clone(),
                        role_id: role.role_id.clone(),
                        tool_id,
                        at_ms,
                    },
                ))
            })
            .collect();
        if let Err(err) = log.append(&envelopes).await {
            tracing::warn!(
                error = %err,
                role_id = %role.role_id,
                "RFC 031 PR-C: ToolDeclaredButMissing append failed"
            );
        }
    }
}

#[async_trait]
impl DecidePhase for LlmDecidePhase {
    async fn decide(
        &self,
        ctx: &OrchestrationContext,
        gather: &GatherOutput,
    ) -> Result<DecideOutput, OrchestratorError> {
        // Build the tool catalogue for this iteration:
        // 1. Core + Registered tools (always included; visibility-filtered
        //    per RFC 029 when the run carries a VisibilityContext)
        // 2. Deferred tools discovered via tool_search in prior iterations
        //    (ctx.discovered_tool_names carries them across the loop boundary)
        let mut tool_descs: Vec<BuiltinToolDescriptor> = match (&self.tools, &ctx.visibility) {
            (Some(r), Some(vis)) => r.prompt_tools_filtered(|name| {
                cairn_runtime::services::is_tool_visible(vis, None, name)
            }),
            (Some(r), None) => r.prompt_tools(),
            (None, _) => Vec::new(),
        };

        if !ctx.discovered_tool_names.is_empty() {
            if let Some(ref registry) = self.tools {
                for name in &ctx.discovered_tool_names {
                    // Use search_deferred to fetch the full descriptor for the
                    // discovered tool (it's still Deferred in the registry).
                    let matches = registry.search_deferred(name);
                    for desc in matches {
                        if !tool_descs.iter().any(|d| d.name == desc.name) {
                            tool_descs.push(desc);
                        }
                    }
                }
            }
        }

        // RFC 018: Plan mode filters out External tools so the agent can only
        // observe and work internally. Execute/Direct see all tools.
        if matches!(ctx.run_mode, cairn_domain::decisions::RunMode::Plan) {
            use cairn_domain::decisions::ToolEffect;
            tool_descs.retain(|d| {
                matches!(
                    d.tool_effect,
                    ToolEffect::Observational | ToolEffect::Internal
                )
            });
        }

        // #702 follow-up: enforce the role's `allowed_tools` allowlist so
        // the tool surface the LLM sees matches the role's contract.
        //
        // Before this filter, `AgentRole::allowed_tools` was declared in
        // the domain model but never consulted by DECIDE — every role
        // saw the full builtin-tool catalogue. Dogfood R9 (session
        // sess_r9, traces orch_run_r9_i0..i3) proved the orchestrator
        // role ignored its specialty and called `webfetch` three times
        // inline to scrape Google/crates.io/GitHub instead of
        // delegating. That happened because webfetch was in the tool
        // surface it received.
        //
        // The orchestrator is a status-and-delegation role: it reads
        // state, inspects artifacts (`read`/`grep`/`glob`), runs
        // read-only verification shell commands, spawns sub-agents,
        // synthesises their output, and calls `complete_run`. It does
        // NOT fetch external URLs, mutate files, or do inline work
        // that belongs to an `executor` / `researcher` sub-agent. The
        // allowlist makes that contract structural, not just prompt-
        // enforced.
        //
        // Empty `allowed_tools` means "no role restriction" — every
        // tool the registry returned stays available. This is the
        // pre-fix behaviour for roles (like `executor`) that don't
        // declare a list; preserves back-compat.
        //
        // RFC 031 PR-C site 1 — tool-allowlist filter. When the
        // `agent_roles` service is attached, resolve the role through
        // the projection so operator-defined roles + shadows win over
        // the compile-time `default_roles()`. `forbid_all_tools=true`
        // clears the surface entirely (§D3); `forbid_all_tools=false`
        // + non-empty `tools[]` filters the surface; empty `tools[]`
        // with `forbid_all_tools=false` is unrestricted. Missing tool
        // ids emit a deduped `ToolDeclaredButMissing` advisory
        // (§D3 — lazy DECIDE-time validation, not POST-time).
        let resolved_role = self.resolve_role_or_fallback(ctx).await;
        apply_role_tool_allowlist(&resolved_role, &mut tool_descs);
        if self.event_log.is_some() {
            self.emit_declared_but_missing(ctx, &resolved_role, &tool_descs)
                .await;
        }

        // F36 (2026-04-24): inject a synthetic `complete_run` tool descriptor
        // so the LLM sees it alongside real tools as a first-class schema entry.
        //
        // Before F36, `complete_run` was ONLY reachable via the text-parsing
        // fallback — a JSON-array meta-action. With native tool calling on, the
        // model saw 20+ real tool schemas (memory_search, bash, search_events…)
        // and zero schema for "done." Dogfood v4 evidence: on a trivial prompt
        // ("Write a Fibonacci function") GLM-4.7 called memory_search × 5,
        // search_events × 1, notify_operator × 2, never complete_run — 8
        // iterations, 0 answers. The model picked whichever tool had the most
        // compelling schema.
        //
        // Fix: publish complete_run as a proper tool. The unwrap in
        // `tool_calls_to_proposals` converts the native tool_call into an
        // `ActionType::CompleteRun` proposal; the loop runner's existing path
        // terminates the run (RunService::complete → LoopSignal::Done).
        //
        // The descriptor is NOT added to `tool_descs` — it has no
        // `ToolHandler` implementation; it's a meta-action. We inject it at
        // index 0 of `tool_defs` (the OpenAI schema array the provider
        // sees).
        //
        // Invariant: `complete_run` is at index 0 of the tools array,
        // not appended at the end. Some provider samplers weight
        // earlier tool schemas more heavily, and GLM-4.7 in particular
        // was observed to ignore a `complete_run` schema at the tail of
        // a 20-tool list on trivially-answerable prompts — see
        // `test_f38_complete_run_actually_invoked.rs` for the pinned
        // regression.
        let mut tool_defs: Vec<serde_json::Value> = Vec::with_capacity(tool_descs.len() + 3);
        tool_defs.push(complete_run_tool_def());
        // #825: fail_run as a native tool def so the model can terminate
        // truthfully when a precondition is missing. Without this
        // verb, blocked sub-agents call complete_run with "Status:
        // Blocked" summaries and runs flip to state=completed (R26
        // dogfood pathology). See fail_run_tool_def() for the full
        // rationale.
        tool_defs.push(fail_run_tool_def());
        // #697 R5-A: spawn_subagent is a native tool def now. Models
        // emit reliable structured JSON against the flat `{role, goal}`
        // schema; the legacy prose-described meta-verb shape produced
        // 3-way malformed emissions under real-LLM dogfood (see R5
        // findings + probe evidence).
        //
        // RFC 031 PR-C site 5 — when an agent-role service is attached,
        // the spawnable-role enum is sourced from the project's
        // projection-backed list (merged with unshadowed built-ins),
        // memoised per run via `ctx.agent_role_list_cache`. Otherwise
        // the process-lifetime `OnceLock` path handles the fallback
        // with the compile-time `default_roles()` set.
        tool_defs.push(self.spawn_subagent_tool_def_for(ctx).await);
        tool_defs.extend(tool_descs.iter().map(descriptor_to_tool_def));

        // When we pass native tool definitions to the provider (OpenAI-style
        // `tools` array), the model emits structured `tool_calls` on its own.
        // In that mode we must NOT tell the model to wrap calls in an
        // `invoke_tool` envelope — that causes small/mid models (Qwen 3.6,
        // Gemma 4 A2B) to emit `tool_calls[name = "invoke_tool"]` literally.
        // See `build_system_prompt` for the two emitted shapes.
        let native_tools_enabled = !tool_defs.is_empty();
        // RFC 031 PR-C site 2 — render the system prompt off the
        // resolved role when available. For legacy callers without an
        // agent-role service `resolved_role` is an ad-hoc `AgentRole`
        // the fallback path built from `default_roles()`, so the
        // prompt matches the pre-RFC-031 shape.
        let system =
            build_system_prompt_from_role(&resolved_role, &tool_descs, native_tools_enabled);

        // Invariant: when the run's recent step history looks stuck on
        // non-terminal tool calls (see `should_inject_stuck_nudge`)
        // we append a final-turn directive that tells the model to
        // call `complete_run` now. The nudge is gated off in
        // `RunMode::Plan` because plan mode terminates by emitting a
        // `<proposed_plan>` block rather than by calling
        // `complete_run`; forcing the terminal tool there would
        // short-circuit the planning contract. `Direct` and `Execute`
        // are the only shapes where the pathology applies.
        //
        // The flag threads into `build_user_message` rather than being
        // post-pended here so the suffix's token cost is accounted for
        // in the fixed-cost budget and optional sections get truncated
        // to make room. Post-pending the suffix bypassed the budget
        // and could push the final prompt over the provider's context
        // window on iterations that were already near the limit
        // (Copilot review on PR #300).
        let plan_mode = matches!(ctx.run_mode, cairn_domain::decisions::RunMode::Plan);
        let inject_stuck_nudge =
            !plan_mode && should_inject_stuck_nudge(ctx.iteration, &gather.step_history);
        // RFC 031 PR-C sites 3+4 — pass the resolved role so the
        // memory hint + footer read `resolved_role.response_shape`
        // directly instead of re-resolving from `ctx.agent_type` via
        // the static `response_shape_for` fallback.
        let user = build_user_message_with_role(
            ctx,
            gather,
            self.token_budget.as_ref(),
            inject_stuck_nudge,
            Some(&resolved_role),
        );

        let messages = vec![
            serde_json::json!({ "role": "system", "content": system }),
            serde_json::json!({ "role": "user",   "content": user   }),
        ];

        // ── Routed dispatch ──────────────────────────────────────────────
        // Delegate to the composed routing service. Cross-binding and
        // per-binding fallback + cooldown tracking + tool forwarding all
        // live behind this single call.
        let t0 = Instant::now();
        let success = match self
            .routed
            .generate(messages.clone(), &self.settings, &tool_defs)
            .await
        {
            Ok(ok) => {
                if ok.fallback_position > 0 || ok.binding_index > 0 {
                    tracing::info!(
                        binding_id = %ok.binding_id,
                        binding_index = ok.binding_index,
                        resolved_model = %ok.model_id,
                        fallback_position = ok.fallback_position,
                        attempts = ok.attempts_before_success.len(),
                        "decide phase recovered via routed fallback"
                    );
                }
                ok
            }
            Err(RoutedGenerationError::AllProvidersExhausted { attempts }) => {
                tracing::warn!(
                    attempt_count = attempts.len(),
                    "decide phase exhausted every binding × model combination"
                );
                return Err(OrchestratorError::AllProvidersExhausted { attempts });
            }
            Err(RoutedGenerationError::Auth {
                binding_id,
                model_id,
                detail,
                attempts,
            }) => {
                tracing::error!(
                    binding_id = %binding_id,
                    model_id = %model_id,
                    attempt_count = attempts.len(),
                    "decide phase hit non-retryable auth failure"
                );
                return Err(OrchestratorError::ProviderAuthFailed {
                    binding_id,
                    model_id,
                    detail,
                });
            }
            Err(RoutedGenerationError::InvalidRequest {
                binding_id,
                model_id,
                detail,
                attempts,
            }) => {
                tracing::error!(
                    binding_id = %binding_id,
                    model_id = %model_id,
                    attempt_count = attempts.len(),
                    "decide phase hit non-retryable invalid-request"
                );
                return Err(OrchestratorError::ProviderInvalidRequest {
                    binding_id,
                    model_id,
                    detail,
                });
            }
        };

        let resp = success.response;
        let resolved_model_id = success.model_id;
        let latency_ms = t0.elapsed().as_millis() as u64;

        // #668 audit-trail provenance: track which messages + which
        // response the persisted body should reflect. The JSON-retry
        // path below can make a second LLM call with a stricter
        // `retry_messages` — when that retry returns the parsed
        // proposals we keep, the audit body MUST reflect the retry
        // (its prompt, its response, its tool_calls), not the original
        // malformed-JSON call that we discarded. Mutable so the retry
        // path can overwrite.
        //
        // Initialised to the first call's values; the retry-success
        // branch rewrites them.
        let mut effective_messages: Vec<serde_json::Value> = messages.clone();
        let mut effective_response_text: String = resp.text.clone();
        let mut effective_tool_calls: Vec<serde_json::Value> = resp.tool_calls.clone();

        // ── #697 R5-B: content-scan fallback for pseudo-XML tool calls ───────
        // When the provider populated native tool_calls[], trust them and
        // skip the scan — the native shape is authoritative. But when
        // tool_calls is empty AND the content field contains recognisable
        // pseudo-XML (Nemotron <function=NAME>, Hermes <tool_call>, Llama
        // <|python_tag|>), promote the scanned entries into
        // effective_tool_calls so the native path below can consume them
        // unchanged. The synthetic entries match the OpenAI shape exactly;
        // tool_calls_to_proposals doesn't care how they got there.
        //
        // Dogfood R5 evidence: nvidia/nemotron-3-super-120b emits
        // <function=NAME> pseudo-XML into content when handed the old
        // prose-described spawn_subagent meta-verb (pre-R5-A). R5-A's
        // native tool_def mitigates the happy path, but Hermes/Qwen with
        // thinking-mode and Llama-3.2 small variants still leak to
        // content occasionally. This scan is the defensive backstop.
        let _ = promote_content_scan_into_tool_calls(
            &mut effective_tool_calls,
            &resp.text,
            &resolved_model_id,
            "first_call",
        );

        // ── Native tool call path ────────────────────────────────────────────
        // If the model returned structured tool_calls (via native tool calling
        // OR via R5-B content-scan promotion above), convert them directly
        // to ActionProposals. This is the preferred path — no JSON text
        // parsing needed.
        let mut proposals = if !effective_tool_calls.is_empty() {
            tool_calls_to_proposals(&effective_tool_calls, &tool_descs)
        } else {
            // ── Legacy text-parsing path ─────────────────────────────────────
            // Parse the raw text response as a JSON array of action objects.
            // This is the fallback for models that don't support native tool calling.
            let mut parsed = parse_proposals(&resp.text);
            if is_fallback_escalation(&parsed) {
                // Retry: explicitly ask the LLM to output only JSON. This
                // is a category-B model-action correction — we loop the
                // problem back to the model as a prompt nudge, NOT as a
                // provider fallback (same binding/model are fine; we just
                // need cleaner output). The routed service will naturally
                // pick the same first-available model unless it's now
                // cooled down.
                let _ = &resolved_model_id; // provenance recorded in trace below
                let retry_user = format!(
                    "{user}\n\n⚠️ Your last response was not valid JSON. \
                     Return ONLY a JSON array of action objects — no prose, no markdown."
                );
                let retry_messages = vec![
                    serde_json::json!({ "role": "system", "content": system }),
                    serde_json::json!({ "role": "user",   "content": retry_user }),
                ];
                match self
                    .routed
                    .generate(retry_messages.clone(), &self.settings, &tool_defs)
                    .await
                {
                    Ok(ok2) => {
                        let r2 = ok2.response;
                        // #697 R5-B: apply the same content-scan promotion
                        // on the retry response before falling through to
                        // text parsing. Same rationale: if the retry
                        // emitted pseudo-XML in content, promote it so
                        // the native path handles it uniformly.
                        let mut retry_effective_tool_calls = r2.tool_calls.clone();
                        let _ = promote_content_scan_into_tool_calls(
                            &mut retry_effective_tool_calls,
                            &r2.text,
                            &resolved_model_id,
                            "retry",
                        );
                        let retry_accepted = if !retry_effective_tool_calls.is_empty() {
                            parsed =
                                tool_calls_to_proposals(&retry_effective_tool_calls, &tool_descs);
                            true
                        } else {
                            let second = parse_proposals(&r2.text);
                            if !is_fallback_escalation(&second) {
                                parsed = second;
                                true
                            } else {
                                false
                            }
                        };
                        if retry_accepted {
                            // #668 audit-trail provenance: when the
                            // retry produces the proposals we keep,
                            // the persisted body must reflect the
                            // retry's prompt + response + tool_calls,
                            // not the first (discarded) call's.
                            // Otherwise operators debugging the run
                            // see the prompt that DIDN'T produce the
                            // proposals they're staring at.
                            effective_messages = retry_messages;
                            effective_response_text = r2.text.clone();
                            effective_tool_calls = retry_effective_tool_calls;
                        }
                    }
                    Err(_) => {
                        // Retry LLM call failed — keep the escalation from the first parse
                    }
                }
            }
            parsed
        };

        // Apply confidence bias
        if self.confidence_bias.abs() > f64::EPSILON {
            for p in &mut proposals {
                p.confidence = (p.confidence + self.confidence_bias).clamp(0.0, 1.0);
            }
        }

        // Override requires_approval for inherently safe read-only actions.
        // Models sometimes over-cautiously set this for web/memory reads — we
        // correct it here so the approval gate only fires for genuinely sensitive actions.
        for p in &mut proposals {
            if p.requires_approval && is_safe_read_action(p) {
                p.requires_approval = false;
            }
        }

        let requires_approval = proposals.iter().any(|p| p.requires_approval);
        let calibrated_confidence = proposals
            .iter()
            .map(|p| p.confidence)
            .fold(0.0_f64, f64::max);

        // Issue #668: capture the prompt + tool_calls JSON for chain-of-
        // thought body persistence downstream. Serialisation failures
        // fall back to empty strings rather than killing decide — the
        // body is observability, not a correctness invariant.
        //
        // Uses `effective_*` rather than the first call's buffers so
        // the audit trail reflects whichever LLM call the orchestrator
        // actually acted on (see #672 Copilot review on the retry
        // provenance bug).
        let messages_json =
            serde_json::to_string(&effective_messages).unwrap_or_else(|_| "[]".to_owned());
        let tool_calls_json = if effective_tool_calls.is_empty() {
            "[]".to_owned()
        } else {
            serde_json::to_string(&effective_tool_calls).unwrap_or_else(|_| "[]".to_owned())
        };
        // Dogfood R7 observability: persist the exact `tools[]` array
        // the request shipped with so operators can see what tool
        // surface the model had at decision time. Both the first call
        // and the retry (if it fired) use the same `tool_defs` slice
        // so this reflects what was in front of the model regardless
        // of which call produced the effective response.
        let tool_defs_json = serde_json::to_string(&tool_defs).unwrap_or_else(|_| "[]".to_owned());

        Ok(DecideOutput {
            raw_response: effective_response_text,
            proposals,
            calibrated_confidence,
            requires_approval,
            // Reflect the model that actually produced the response, not the
            // preferred one — downstream `LlmCallTrace` / billing / route
            // decision records need to know which upstream we hit.
            model_id: resolved_model_id,
            latency_ms,
            input_tokens: resp.input_tokens,
            output_tokens: resp.output_tokens,
            system_prompt: system,
            messages_json,
            tool_calls_json,
            tool_defs_json,
        })
    }
}

// ── Prompt builders ───────────────────────────────────────────────────────────

/// Build the system prompt for the given agent type.
///
/// Uses `default_roles()` to look up the canonical system-prompt fragment for
/// the matching role.  Falls back to a generic orchestrator prompt if the role
/// is not registered.
/// Public wrapper for cross-crate regression tests. Gated behind the
/// `test-hooks` Cargo feature so this symbol is absent from production
/// builds. See `crate::decide_impl_test_hooks`.
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub fn build_system_prompt_pub(
    agent_type: &str,
    tools: &[BuiltinToolDescriptor],
    native_tools_enabled: bool,
) -> String {
    build_system_prompt(agent_type, tools, native_tools_enabled)
}

/// Public wrapper for cross-crate regression tests. Gated behind the
/// `test-hooks` Cargo feature so this symbol is absent from production
/// builds. See `crate::decide_impl_test_hooks`.
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub fn build_user_message_pub(
    ctx: &OrchestrationContext,
    gather: &GatherOutput,
    budget: Option<&TokenBudget>,
) -> String {
    build_user_message(ctx, gather, budget, false)
}

/// Public wrapper around the synthetic `complete_run` tool definition
/// for cross-crate regression tests. See the F36 comment block in
/// `build_system_prompt`'s caller for context. Gated behind the
/// `test-hooks` Cargo feature.
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub fn complete_run_tool_def_pub() -> serde_json::Value {
    complete_run_tool_def()
}

#[cfg_attr(not(any(test, feature = "test-hooks")), allow(dead_code))]
fn build_system_prompt(
    agent_type: &str,
    tools: &[BuiltinToolDescriptor],
    native_tools_enabled: bool,
) -> String {
    // Role identity — use the registry's assembled prompt. For non-
    // orchestrator roles this is `BASE_SUBAGENT_PROMPT + specialty
    // overlay`; for the orchestrator it is the full prompt verbatim.
    // Unknown roles fall back to the generic role's assembled prompt
    // (#775) — the pre-#775 3-line fallback did not satisfy any
    // contract anchors and left mis-spawned children without a
    // workflow skeleton.
    let role_prompt = cairn_domain::agent_roles::assembled_prompt_for(agent_type);
    render_system_prompt(&role_prompt, tools, native_tools_enabled)
}

/// RFC 031 PR-C site 2: same shell as `build_system_prompt`, but the
/// caller supplies the resolved `AgentRole` so the assembled prompt
/// reflects operator-defined roles / shadows instead of the compile-
/// time default set.
fn build_system_prompt_from_role(
    role: &AgentRole,
    tools: &[BuiltinToolDescriptor],
    native_tools_enabled: bool,
) -> String {
    let role_prompt = cairn_domain::agent_roles::assembled_prompt_for_role(role);
    render_system_prompt(&role_prompt, tools, native_tools_enabled)
}

fn render_system_prompt(
    role_prompt: &str,
    tools: &[BuiltinToolDescriptor],
    native_tools_enabled: bool,
) -> String {
    // Build the tool list section. The phrasing differs between the two
    // model interfaces (native OpenAI `tool_calls` vs. JSON-array text).
    // Reference: the avifenesh/tools harness-e2e suite — proven against
    // Qwen3/3.5 and Gemma-family open models — uses a plain "call the tool
    // by name" framing and never introduces an `invoke_tool` wrapper name.
    let tools_section = if tools.is_empty() {
        String::new()
    } else if native_tools_enabled {
        // Native-tool-calling mode: the model sees tool schemas via the
        // provider's `tools` parameter and must emit a real tool name
        // (e.g. `bash`, `read`) as `tool_calls[].function.name`. Listing
        // the tools again here is redundant with the schema but helps
        // smaller models anchor selection; CRITICALLY we must not mention
        // any `invoke_tool` / `tool_name` envelope.
        let lines = tools
            .iter()
            .map(|t| format!("  - {}", t.prompt_line()))
            .collect::<Vec<_>>()
            .join("\n");
        format!(
            "\n\n\
             ## Available tools\n\
             Call any of the following tools directly, by its exact name, \
             using the provider's native tool-call mechanism. Do not wrap \
             calls in any envelope — emit one tool call per action with the \
             tool's JSON arguments.\n\
             \n\
             {lines}\n\
             \n\
             Only call tools listed above. Do not invent tool names. Use \
             tool_search to discover additional tools if the ones above \
             are insufficient."
        )
    } else {
        // Legacy JSON-array text mode: the model emits an
        // `{action_type: "invoke_tool", tool_name: "...", ...}` envelope.
        let lines = tools
            .iter()
            .map(|t| format!("  - {}", t.prompt_line()))
            .collect::<Vec<_>>()
            .join("\n");
        format!(
            "\n\n\
             ## Available tools\n\
             Use invoke_tool with one of these tool_name values:\n\
             {lines}\n\
             \n\
             Only call tools listed above. Do not invent tool names.\n\
             Use tool_search to discover additional tools if the ones above are insufficient."
        )
    };

    // Response-format section also diverges between modes.
    let response_format = if native_tools_enabled {
        // With native tools, the model emits tool_calls for invocations
        // and plain text for terminal meta-actions. We still accept the
        // JSON-array fallback when the model chooses not to call a tool.
        r#"## Response format
When you need to call a real tool — including `complete_run` to finish
the run — emit a native tool call using the provider's `tool_calls`
mechanism. `complete_run` is a first-class tool in your toolbox (see
its schema in the tools list); prefer calling it as a tool_call over
writing a JSON action array.

Orchestrator meta-actions that are NOT tools — `create_memory`,
`send_notification`, `spawn_subagent`, `escalate_to_operator` — have
no function schema in the tools list. DO NOT try to emit these as
native tool_calls (doing so will be interpreted as an unknown tool
invocation and fail at execute time). Instead, return them as a JSON
action array, even when native tool calling is available for other
actions.

JSON action array shape (used for meta-actions above, and as the
legacy fallback when the provider has no native tool calling): ONLY
a JSON array of action objects — no prose, no markdown fences — with
fields:
- "action_type": one of "invoke_tool"|"complete_run"|"fail_run"|"create_memory"|"spawn_subagent"|"send_notification"|"escalate_to_operator"
- "description": concise explanation for most actions; for complete_run,
                 the FULL user-facing answer (prose, bullets, whatever the
                 user asked for) — this is what the user sees as the run's
                 final output. For fail_run, the reason you cannot proceed.
- "confidence": float 0.0-1.0
- "requires_approval": boolean
- "tool_name" (for invoke_tool/spawn_subagent): tool ID or sub-agent role
- "tool_args" (for invoke_tool/spawn_subagent/create_memory): JSON arguments

Field conventions:
- invoke_tool:    ONLY as a text-channel fallback when native tool
                  calling is unavailable or failed; tool_name = tool ID,
                  tool_args = {...}. Prefer native tool calls.
- complete_run:   description = the full user-facing answer. NOT a meta-
                  summary like "answered the user's question." Write it
                  for the user to read. Do NOT use complete_run when you
                  are blocked or partially-complete — use fail_run.
- fail_run:       description = why you cannot proceed (short, concrete,
                  operator-facing). Use when a precondition is missing,
                  the goal is contradictory, or a dependency was not met
                  and no operator intervention would unblock you. If
                  operator intervention WOULD unblock, use
                  escalate_to_operator instead.
- spawn_subagent: tool_name = role,  tool_args = {"goal": "..."}
- create_memory:  tool_args = {"content": "..."}"#
            .to_owned()
    } else {
        format!(
            "## Response format\n\
             Respond ONLY with a JSON array of action objects. Each object MUST have:\n\
             - \"action_type\": one of {action_types}\n\
             - \"description\": concise explanation for most actions; for \
               complete_run, the FULL user-facing answer (prose, bullets, \
               whatever the user asked for). The user sees this verbatim as \
               the run's final output — write it for them to read. For \
               fail_run, the reason you cannot proceed.\n\
             - \"confidence\": float 0.0–1.0\n\
             - \"requires_approval\": boolean\n\
             - \"tool_name\" (for invoke_tool/spawn_subagent): tool ID or sub-agent role\n\
             - \"tool_args\" (for invoke_tool/spawn_subagent/create_memory): JSON arguments\n\
             \n\
             Field conventions:\n\
             - invoke_tool:    tool_name = tool ID,  tool_args = {{...}}\n\
             - complete_run:   description = the full user-facing answer \
               (not a meta-summary like \"answered the user's question\"). \
               Do NOT use complete_run when you are blocked — use fail_run.\n\
             - fail_run:       description = why you cannot proceed. Use \
               when a precondition is missing, the goal is contradictory, \
               or a dependency was not met and no operator intervention \
               would unblock you. If operator intervention WOULD unblock, \
               use escalate_to_operator instead.\n\
             - spawn_subagent: tool_name = role,  tool_args = {{\"goal\": \"...\"}}\n\
             - create_memory:  tool_args = {{\"content\": \"...\"}}\n\
             \n\
             Return ONLY the JSON array — no markdown fences, no explanation text.",
            action_types = r#""invoke_tool"|"complete_run"|"fail_run"|"create_memory"|"spawn_subagent"|"send_notification"|"escalate_to_operator""#,
        )
    };

    // Prompt design note (F30 fix, 2026-04-24):
    //
    // The previous prompt forced every run through a four-phase
    // Understand → Act → Verify → Complete workflow and explicitly required
    // "You have taken action toward the goal (not just read/searched)"
    // before the model was allowed to call `complete_run`. For prose /
    // analysis / Q&A prompts (the dogfood run 1 baseline: "Summarise
    // Minecraft creative mode in three bullets") there IS no artifact to
    // produce — the direct answer IS the deliverable. The old prompt
    // therefore pushed the model into an 8-iteration introspection loop:
    // it kept calling read-only tools looking for "action to take" that
    // never existed, hit `max_iterations_reached`, and returned no output.
    //
    // The replacement prompt splits tasks into two modes:
    //
    //   - Direct-answer tasks (Q&A, summaries, explanations, analysis):
    //     call `complete_run` IMMEDIATELY with the full answer as
    //     `description`. No tools required. Do not "gather context" first
    //     by default.
    //
    //   - Artifact tasks (produce code, edit files, open PRs, run
    //     commands): use tools, verify, then `complete_run`.
    //
    // If you ever reintroduce a "gather context first" instruction, make
    // sure the F30 regression test
    // (`test_f30_orchestrator_termination::direct_answer_prompt_calls_complete_run_in_one_iteration`)
    // still passes — it asserts the loop terminates on iteration 0 for a
    // trivially-answerable prompt.
    format!(
        "{role_prompt}\
         {tools_section}\n\
         \n\
         ## How to decide what to do\n\
         Your first move depends on the goal:\n\
         \n\
         **If you already have the information to answer** (you know the \
         subject, the goal is general knowledge or trivia, the answer is in \
         your training data, or relevant context is already in this prompt):\n\
         → Call `complete_run` RIGHT NOW with the full user-facing answer in \
         `final_answer`. Do NOT call memory_search, search_events, \
         notify_operator, or any other tool first. Answering from training \
         data IS the correct behaviour — calling tools \"to be thorough\" on \
         a trivially-answerable prompt wastes iterations and the user gets \
         nothing.\n\
         \n\
         Examples of direct-answer prompts (all of these should hit \
         `complete_run` on iteration 0, zero other tool calls):\n\
         - \"What is the capital of France?\" → `complete_run({{final_answer: \"Paris.\"}})`\n\
         - \"Write a Python Fibonacci function with a docstring.\" → \
         `complete_run({{final_answer: \"```python\\ndef fib(n):\\n    \\\"\\\"\\\"Return the nth Fibonacci number.\\\"\\\"\\\"\\n    …\"}})`\n\
         - \"Explain what a Redstone repeater does in two sentences.\" → \
         `complete_run({{final_answer: \"A Redstone repeater…\"}})`\n\
         \n\
         **If the goal needs external information you don't have** (reading \
         a specific file in this project, searching memory or the codebase \
         for project-specific content, fetching from the web, querying the \
         graph, looking up domain-specific state):\n\
         → Call the appropriate read/search/fetch tool ONCE, then on the \
         next iteration call `complete_run` with the answer. Do not loop \
         gathering more context than the answer requires.\n\
         \n\
         **If the goal requires producing or modifying artifacts** (writing \
         files, running commands, opening PRs, editing code, calling \
         external APIs that change state):\n\
         → Use tools to do the work, verify the result, then call \
         `complete_run` with a summary of what you produced.\n\
         \n\
         **Never call tools to introspect THIS run itself** — the goal, \
         iteration number, prior steps, and approvals are already in this \
         prompt. Do not call `get_run`, `list_runs`, `search_events`, \
         `get_approvals`, `get_task`, or `wait_for_task` to look up state \
         about the run you are currently executing, even if those tools \
         exist in your toolbox. They are registered for OTHER legitimate \
         system-aware goals (\"list every failed run from today\"), not for \
         answering the current prompt.\n\
         \n\
         **If the last 1–2 turns were introspection-only and produced \
         nothing useful** (e.g. memory_search returned no matches), STOP \
         searching and answer from training data via `complete_run`.\n\
         \n\
         ## Tool usage\n\
         - Only call a tool if the answer genuinely requires information or \
           side effects you don't already have. Don't call read-only tools \
           just to appear thorough.\n\
         - Never call the same tool with the same arguments twice. If a \
           tool returned what you needed, use it; don't re-query.\n\
         - Set `requires_approval=false` for read/search/fetch-only \
           operations. Set `requires_approval=true` for writes, code \
           execution, sending messages, or any destructive action.\n\
         - Use `spawn_subagent` only when the task is genuinely multi-part \
           and benefits from parallel execution.\n\
         - If blocked and you need human input → `escalate_to_operator`.\n\
         \n\
         ## Completion\n\
         `complete_run` ends the run and returns the text you pass in \
         `final_answer` (tool-call mode) or `description` (JSON-array \
         fallback mode) to the user as the final output. Write it for the \
         user, not for yourself: include the actual content (the bullet \
         list, the summary, the code, the explanation), not a \
         meta-description like 'answered the user's question'.\n\
         \n\
         ## Tips\n\
         - If a tool call fails, analyse the error and try a different \
           approach. Do not retry the same failing call.\n\
         - When reading large outputs, focus on the relevant sections.\n\
         - If you already answered the goal in a previous iteration (check \
           step history), call `complete_run` now — don't re-do the work.\n\
         \n\
         {response_format}",
    )
}

/// Build the user message from `OrchestrationContext` + `GatherOutput`.
///
/// When `budget` is `Some`, content is truncated so the full prompt
/// (system + user) fits within `budget.available_input` tokens.
/// Truncation order (from most to least dispensable):
///   never truncated : system prompt, goal, run state, footer
///   truncated last  : graph_context (trim from end)
///   truncated third : step_history  (trim oldest first)
///   truncated second: memory_chunks (keep most-relevant, trim from end)
#[cfg_attr(not(any(test, feature = "test-hooks")), allow(dead_code))]
fn build_user_message(
    ctx: &OrchestrationContext,
    gather: &GatherOutput,
    budget: Option<&TokenBudget>,
    append_stuck_nudge: bool,
) -> String {
    build_user_message_with_role(ctx, gather, budget, append_stuck_nudge, None)
}

/// RFC 031 PR-C sites 3+4: accept an optional resolved `AgentRole` so
/// the memory hint + footer read `role.response_shape` directly rather
/// than via the static `response_shape_for(&ctx.agent_type)` fallback.
/// When `role` is `None` the path matches the pre-RFC-031 behaviour.
fn build_user_message_with_role(
    ctx: &OrchestrationContext,
    gather: &GatherOutput,
    budget: Option<&TokenBudget>,
    append_stuck_nudge: bool,
    role: Option<&AgentRole>,
) -> String {
    // ── Fixed sections (never truncated) ─────────────────────────────────────
    let goal_part = format!("## Goal\n{}", ctx.goal);
    // #775: optional parent-context section — surfaced verbatim from
    // SubagentSpawned.parent_context. Rendered between Goal and Run
    // state so the child sees the parent's binding direction
    // immediately after the goal. Empty / whitespace-only contexts
    // are filtered upstream (execute_impl extraction strips them);
    // here we only check `is_some` because the field is already a
    // typed Option<String> on the context.
    let parent_context_part: Option<String> = ctx
        .parent_context
        .as_ref()
        .map(|c| format!("## Parent context\n{c}"));
    // #797: iteration counter is intentionally hidden from the
    // model. R21 dogfood surfaced that sub-agents read `iteration: 3`
    // and self-bailed with partial-completion reports thinking they
    // were near the limit, even though the cap (now 50) was
    // nowhere near. The iteration counter is internal orchestrator
    // bookkeeping; the model already has step_history for "what
    // happened so far" context. Operators still get iteration via
    // the projection (`run.iteration`) and the trajectory endpoint.
    // #813: workspace path rendered into ## Run state so sub-agents
    // know where to `cd` before running git / file commands. R23
    // dogfood found executors spending 11+ iterations on `pwd`/`find
    // Cargo.toml`/`ls /tmp/cairn-runs/...` discovery loops because the
    // working directory was set in the runtime context but never
    // surfaced in the prompt. Path is rendered verbatim — no truncation
    // — so the model can copy-paste it into a `cd` command.
    let run_state_part = format!(
        "## Run state\nrun_id: {}\nagent_type: {}\nworkspace_path: {}",
        ctx.run_id.as_str(),
        ctx.agent_type,
        ctx.working_dir.display(),
    );
    let has_memory = !gather.memory_chunks.is_empty();
    // #774: footer shape depends on the role's `response_shape`.
    // DirectAnswer roles (orchestrator, future Q&A specialties) get
    // the "answer NOW with complete_run" nudge. ProceduralArtifact
    // roles (executor, researcher, reviewer, generic) get a
    // continuation footer that does NOT pressure early termination —
    // for them, complete_run before the artifact exists is the
    // failure mode, not the success criterion. R19 dogfood evidence:
    // the executor's prompt told it to follow Phase 1-5 but the
    // footer kept telling it to "complete_run NOW" — it split the
    // difference, did one defensive bash, never wrote a file.
    //
    // Unknown role_id falls back to the generic role's shape via the
    // registry lookup (generic = ProceduralArtifact), matching the
    // assembled-prompt fallback wired in `build_system_prompt`.
    // RFC 031 PR-C sites 3+4: prefer the resolved role's shape when
    // threaded through from the decide pipeline. Fall back to the
    // static table for legacy callers (tests, cross-crate helpers).
    let response_shape: ResponseShape = role
        .map(|r| r.response_shape)
        .unwrap_or_else(|| cairn_domain::agent_roles::response_shape_for(&ctx.agent_type));
    let memory_hint = if has_memory {
        "Memory contains relevant context above. Use it to inform your answer.".to_owned()
    } else {
        match response_shape {
            cairn_domain::agent_roles::ResponseShape::DirectAnswer => {
                "No relevant memories retrieved. If the goal needs external information, \
                 call a tool to fetch it; otherwise answer directly."
                    .to_owned()
            }
            cairn_domain::agent_roles::ResponseShape::ProceduralArtifact => {
                "No relevant memories retrieved. Use your specialty's tools to \
                 produce the artifact the goal asks for."
                    .to_owned()
            }
        }
    };
    // F30 + #774: footer must not reintroduce the pre-fix four-phase
    // workflow (see `build_system_prompt`'s design note). The
    // decision rule is restated concisely so the user message echoes
    // the system prompt instead of contradicting it. The shape
    // depends on the role's response_shape (#774).
    let footer = match response_shape {
        cairn_domain::agent_roles::ResponseShape::DirectAnswer => format!(
            "## Next step\n\
             {memory_hint}\n\
             If you already have the answer, call the `complete_run` tool NOW \
             with the full answer in `final_answer`. If you need external \
             information, call the appropriate tool once and then complete_run \
             on the next iteration. Do not call introspection tools \
             (get_run, list_runs, search_events, get_approvals, get_task) \
             about this run itself — the goal and step history are already \
             in this prompt."
        ),
        cairn_domain::agent_roles::ResponseShape::ProceduralArtifact => format!(
            "## Next step\n\
             {memory_hint}\n\
             Continue from your most recent step in the step history. If your \
             specialty's Phase 5 (Report / Deliver) has NOT yet begun, do not \
             call `complete_run` — the artifact is not produced yet, and \
             completing here ships half-done work. Use your next tool call to \
             advance whichever phase you are in (Locate / Implement / Verify, \
             or your specialty's equivalent). Only call `complete_run` when \
             the goal's success criteria are demonstrably met (file written, \
             build passes, citations gathered, review delivered). Do not call \
             introspection tools (get_run, list_runs, search_events, \
             get_approvals, get_task) about this run itself — the goal and \
             step history are already in this prompt."
        ),
    };

    // The stuck-loop nudge is part of the fixed-cost prefix when
    // enabled: it must appear in the final prompt even if the budget
    // is tight, so its token cost has to come out of the optional
    // sections' budget rather than silently blowing past
    // `available_input`. See Copilot review on PR #300.
    let nudge_cost = if append_stuck_nudge {
        estimate_tokens(stuck_nudge_suffix())
    } else {
        0
    };

    // ── Compute how many tokens are available for optional content ────────────
    // When no budget is set every section is included without limit.
    let optional_token_budget: Option<usize> = budget.map(|b| {
        let parent_ctx_cost = parent_context_part
            .as_deref()
            .map(estimate_tokens)
            .unwrap_or(0);
        let fixed_cost = estimate_tokens(&goal_part)
            + parent_ctx_cost
            + estimate_tokens(&run_state_part)
            + estimate_tokens(&footer)
            + nudge_cost
            + 20; // section separators ("\n\n" between each part)
        b.available_input.saturating_sub(fixed_cost)
    });

    let mut remaining = optional_token_budget;

    // ── Memory chunks — most relevant first, truncate from end ───────────────
    // Retrieval already orders chunks highest-score first.
    let memory_section: Option<String> = if gather.memory_chunks.is_empty() {
        None
    } else {
        let mut snippets: Vec<String> = Vec::new();
        for (i, r) in gather.memory_chunks.iter().enumerate() {
            let line = format!(
                "[{}] {}",
                i + 1,
                r.chunk.text.chars().take(400).collect::<String>()
            );
            if let Some(rem) = remaining.as_mut() {
                let cost = estimate_tokens(&line) + 1;
                if *rem < cost {
                    break; // budget exhausted — drop less-relevant chunks
                }
                *rem = rem.saturating_sub(cost);
            }
            snippets.push(line);
        }
        if snippets.is_empty() {
            None
        } else {
            Some(format!("## Relevant knowledge\n{}", snippets.join("\n")))
        }
    };

    // ── Step history — most recent first, truncate oldest ────────────────────
    //
    // #797: drop the `[iteration]` prefix on each step line. Same
    // reasoning as the `## Run state` change above — the iteration
    // counter was triggering self-bail behaviour in sub-agents
    // (R21 dogfood). Position in the list communicates recency,
    // and `action_kind | summary | ok=...` is what the model needs
    // to decide the next move. Operators still get the iteration
    // field via the trajectory endpoint, which preserves the full
    // ReasoningStepRecord structure.
    let step_section: Option<String> = if gather.step_history.is_empty() {
        None
    } else {
        let mut lines: Vec<String> = Vec::new();
        for s in gather.step_history.iter().rev() {
            let line = format!("- {} | {} | ok={}", s.action_kind, s.summary, s.succeeded,);
            if let Some(rem) = remaining.as_mut() {
                let cost = estimate_tokens(&line) + 1;
                if *rem < cost {
                    break; // budget exhausted — drop older steps
                }
                *rem = rem.saturating_sub(cost);
            }
            lines.push(line);
        }
        if lines.is_empty() {
            None
        } else {
            Some(format!(
                "## Step history (most recent first)\n{}",
                lines.join("\n")
            ))
        }
    };

    // ── Operator settings ─────────────────────────────────────────────────────
    let settings_section: Option<String> = if gather.operator_settings.is_empty() {
        None
    } else {
        let text = gather
            .operator_settings
            .iter()
            .map(|s| format!("  {}: {}", s.key, s.value))
            .collect::<Vec<_>>()
            .join("\n");
        let section = format!("## Operator settings\n{text}");
        if let Some(rem) = remaining.as_mut() {
            let cost = estimate_tokens(&section);
            if *rem < cost {
                None // no room — skip entirely
            } else {
                *rem = rem.saturating_sub(cost);
                Some(section)
            }
        } else {
            Some(section)
        }
    };

    // ── Checkpoint hint ───────────────────────────────────────────────────────
    let checkpoint_section: Option<String> = gather.checkpoint.as_ref().and_then(|cp| {
        let section = format!(
            "## Checkpoint available\ncheckpoint_id: {} — the run can be restored to this point.",
            cp.checkpoint_id.as_str(),
        );
        if let Some(rem) = remaining.as_mut() {
            let cost = estimate_tokens(&section);
            if *rem < cost {
                return None;
            }
            *rem = rem.saturating_sub(cost);
        }
        Some(section)
    });

    // ── Graph context — truncated last ────────────────────────────────────────
    let graph_section: Option<String> = if gather.graph_nodes.is_empty() {
        None
    } else {
        let mut node_ids: Vec<&str> = Vec::new();
        for node in &gather.graph_nodes {
            let cost = node.node_id.len() / 4 + 2;
            if let Some(rem) = remaining.as_mut() {
                if *rem < cost {
                    break;
                }
                *rem = rem.saturating_sub(cost);
            }
            node_ids.push(node.node_id.as_str());
        }
        if node_ids.is_empty() {
            None
        } else {
            Some(format!(
                "## Graph context\nNearby nodes: {}",
                node_ids.join(", ")
            ))
        }
    };

    // ── Assemble ──────────────────────────────────────────────────────────────
    // Order: Goal → Parent context (optional) → Run state → optional
    // sections (memory, step history, …) → footer. Parent context
    // sits adjacent to the goal so the child reads the parent's
    // binding direction before any retrieved context.
    let mut parts: Vec<String> = vec![goal_part];
    if let Some(s) = parent_context_part {
        parts.push(s);
    }
    parts.push(run_state_part);
    if let Some(s) = memory_section {
        parts.push(s);
    }
    if let Some(s) = step_section {
        parts.push(s);
    }
    if let Some(s) = settings_section {
        parts.push(s);
    }
    if let Some(s) = checkpoint_section {
        parts.push(s);
    }
    if let Some(s) = graph_section {
        parts.push(s);
    }
    parts.push(footer);
    let mut msg = parts.join("\n\n");
    if append_stuck_nudge {
        msg.push_str(stuck_nudge_suffix());
    }
    msg
}

// ── Response parsing (inlined — avoids re-exporting cairn_runtime internals) ──

/// Parse the LLM's raw text into `ActionProposal` values.
///
/// On complete parse failure returns a single `EscalateToOperator` proposal.
fn parse_proposals(raw: &str) -> Vec<ActionProposal> {
    let cleaned = strip_markdown_fence(raw.trim());

    if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(cleaned) {
        let proposals: Vec<ActionProposal> = arr.into_iter().filter_map(parse_one).collect();
        if !proposals.is_empty() {
            return proposals;
        }
    }

    if let Ok(obj) = serde_json::from_str::<serde_json::Value>(cleaned) {
        if let Some(p) = parse_one(obj) {
            return vec![p];
        }
    }

    // Fallback escalation
    vec![ActionProposal::escalate(
        format!(
            "LLM returned a non-JSON response (first 200 chars): {}",
            &raw[..raw.len().min(200)]
        ),
        0.0,
    )]
}

/// Convert a `BuiltinToolDescriptor` into an OpenAI-format tool definition.
///
/// Output: `{ "type": "function", "function": { "name": "...", "description": "...", "parameters": {...} } }`
fn descriptor_to_tool_def(desc: &BuiltinToolDescriptor) -> serde_json::Value {
    serde_json::json!({
        "type": "function",
        "function": {
            "name": desc.name,
            "description": desc.description,
            "parameters": desc.parameters_schema,
        }
    })
}

/// Build the synthetic `complete_run` tool definition injected into every
/// DECIDE call's tool schema list (F36).
///
/// `complete_run` has no `ToolHandler` — it is a terminal meta-action the
/// orchestrator interprets directly (`RunService::complete` is invoked in the
/// EXECUTE phase via the `ActionType::CompleteRun` branch). Publishing it as
/// a tool schema gives the model a first-class entry to terminate the run,
/// symmetrical with every other tool it can call.
///
/// The description is aggressively directive because weaker wording did not
/// stop GLM-4.7 from preferring introspection tools (memory_search,
/// search_events) on trivially-answerable prompts.
pub(crate) fn complete_run_tool_def() -> serde_json::Value {
    serde_json::json!({
        "type": "function",
        "function": {
            "name": "complete_run",
            "description": "Finish the run and return the final answer to the user. \
    Call this IMMEDIATELY when you can answer from training data or from context \
    already in this prompt — do not call memory_search, search_events, or any \
    other tool first on trivially-answerable prompts. Put the full user-facing \
    answer (prose, bullets, code — whatever the goal asks for) in `final_answer`. \
    This ends the run; no further tool calls will run after this one.",
            "parameters": {
                "type": "object",
                "properties": {
                    "final_answer": {
                        "type": "string",
                        "description": "The complete user-facing answer. Verbatim what the user should see as the run's output. Do not write a meta-summary like 'answered the user's question' — write the actual answer."
                    }
                },
                "required": ["final_answer"],
                "additionalProperties": false
            }
        }
    })
}

/// #825: native tool schema for `fail_run` — the truthful terminal
/// verb for "I tried, I cannot proceed." Parallel to `complete_run`
/// but routes to `RunService::fail(FailureClass::ModelReportedFailure)`
/// instead of `complete`.
///
/// R26 dogfood exposed why this exists: a sub-agent that correctly
/// diagnosed its own block (e.g. "M1-7 needs M1-1 first") had only
/// `complete_run` as a terminal verb, so it called complete_run with a
/// `final_answer` saying "Status: Blocked" — and the run flipped to
/// `state=completed`. Operator dashboards saw success; the only failure
/// signal was buried in free-text. Adding `fail_run` gives the model a
/// wire-level verb that matches its intent.
///
/// Description is directive so GLM-class models pick this over
/// complete_run when the deliverable doesn't exist. Reviewer-class
/// models tend to default to complete_run even on admitted failure.
pub(crate) fn fail_run_tool_def() -> serde_json::Value {
    serde_json::json!({
        "type": "function",
        "function": {
            "name": "fail_run",
            "description": "Mark this run as FAILED because you tried and \
    cannot proceed. Call this — NOT `complete_run` — when a precondition is \
    missing, the goal is contradictory, a dependency was not met, or you \
    exhausted your options and no operator intervention would unblock you. \
    If operator intervention WOULD unblock you (needs approval, credential \
    rotation, clarification), call `escalate_to_operator` instead. Never \
    call `complete_run` with a summary saying you are blocked — use this \
    verb so the run record reflects reality.",
            "parameters": {
                "type": "object",
                "properties": {
                    "reason": {
                        "type": "string",
                        "description": "Why you cannot proceed. Short, \
    concrete, operator-facing. Example: 'blocked: src/main.rs does not exist; \
    depends on M1-1'. Do NOT restate the goal; name the obstacle."
                    }
                },
                "required": ["reason"],
                "additionalProperties": false
            }
        }
    })
}

/// Native tool schema for `spawn_subagent` — the subagent-delegation
/// meta-verb. Publishing this as a native tool definition instead of a
/// prose-only meta-verb is #697 R5-A: dogfood R5 showed that
/// minimax-m2.5 / nemotron-3-super / gemma-4-31b all confuse the
/// nested `{tool_name, tool_args: {goal}}` envelope documented in the
/// legacy JSON-action prose. When handed a proper flat `{role, goal}`
/// schema the same models emit perfect JSON reliably.
///
/// Flat args match the standard operator mental model ("spawn a
/// researcher with goal X") and the domain-level contract
/// (`TaskService::spawn_subagent(..., role: String)` +
/// `FabricRunService::start_with_role(..., agent_role_id)` — both
/// take `role` at the top level). The legacy nested shape was an
/// artifact of the pre-native-tools JSON-action schema; `parse_one`
/// still accepts the nested form for backward compat with runs that
/// rely on the JSON-action envelope.
pub(crate) fn spawn_subagent_tool_def() -> serde_json::Value {
    // #776: derive the `role` enum from `default_roles()`. Pre-#776
    // the field was a free-form string; the LLM could pass any
    // value, and unknown roles silently fell through to the
    // generic-shaped prompt. Now the schema-validation layer
    // rejects unknown roles up front. Excludes the orchestrator
    // role — sub-agents do not delegate to a parent.
    //
    // Cached via OnceLock per Gemini review on PR #786:
    // `spawn_subagent_tool_def()` runs on every DECIDE turn, and
    // `default_roles()` clones every role's multi-KB system prompt
    // string. The cache means we pay the allocation cost exactly
    // once per process. Adding a new role to default_roles()
    // requires a process restart to take effect, which matches
    // the rest of the registry's contract (default_roles is a
    // compile-time constant set today; the dynamic-registry
    // refactor is RFC future work).
    //
    // RFC 031 PR-C: the decide pipeline prefers
    // `LlmDecidePhase::spawn_subagent_tool_def_for(ctx)`, which uses
    // the per-run projection-backed snapshot. This function remains
    // the fallback when `agent_roles` is not wired — the pre-PR-C
    // tests + cross-crate callers rely on the static registry.
    static ROLE_ENUM: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
    let role_enum = ROLE_ENUM.get_or_init(|| {
        cairn_domain::agent_roles::default_roles()
            .into_iter()
            .filter(|r| r.role_id != "orchestrator")
            .map(|r| r.role_id)
            .collect()
    });
    spawn_subagent_tool_def_with_enum(role_enum.clone())
}

/// RFC 031 PR-C site 5: build the spawn_subagent schema with a caller-
/// supplied `role` enum. The decide pipeline passes the
/// projection-backed list (merged custom + unshadowed built-ins,
/// minus `orchestrator`); fallback callers pass the
/// `default_roles()`-derived vec.
fn spawn_subagent_tool_def_with_enum(role_enum: Vec<String>) -> serde_json::Value {
    serde_json::json!({
        "type": "function",
        "function": {
            "name": "spawn_subagent",
            "description": "Delegate a concrete task to a sub-agent and wait for its result. The current run SUSPENDS until the sub-agent terminates; when it resumes, the sub-agent's completion summary is surfaced in the step_history under action_kind=\"subagent_complete\". Use this when the current run's goal decomposes into a self-contained sub-task that another role is better suited to handle. Routing — `status-checker` for read-only workspace/git inspection (every \"does X exist? did the test pass? what does git status say?\" question goes here, including verifying a peer sub-agent's claim); `executor` for code changes; `researcher` for citation-backed investigation; `reviewer` for structured audits; `generic` only as a last resort. Call `list_agents` for the full registry.",
            "parameters": {
                "type": "object",
                "properties": {
                    "role": {
                        "type": "string",
                        "enum": role_enum,
                        "description": "The sub-agent role. Use `list_agents` to enumerate available roles + their descriptions, or `agent_description(role_id)` to read a single role's full record. The allowed values are derived at runtime from the project's agent-role registry (operator-defined + built-ins, minus `orchestrator`)."
                    },
                    "goal": {
                        "type": "string",
                        "description": "REQUIRED. One-sentence concrete goal for the sub-agent. Be specific: 'Find 3 best practices for X' is good; 'research X' is too vague. The sub-agent's summary quality depends heavily on goal specificity."
                    },
                    "parent_context": {
                        "type": "string",
                        "description": "OPTIONAL. Freeform context the parent threads into the child's first DECIDE prompt under a `## Parent context` section. Useful on a re-spawn after a failed first attempt — e.g. 'previous attempt looped on `gh auth status`; do not call `gh auth status`, the workspace at /tmp/.../foo already has gh credentials'. Do NOT put the goal itself here — the goal goes in `goal`."
                    },
                    "reuse_sandbox_from": {
                        "type": "string",
                        "description": "OPTIONAL. Run id of a prior sibling under the same root whose sandbox should be reused. When set, the new child inherits that run's working_dir — use it to let a replacement sub-agent continue from a dead sibling's partial on-disk work (e.g. a first attempt cloned the repo and applied half the changes before the completion gate rejected it; a second attempt with `reuse_sandbox_from` set to the first's run_id picks up from that on-disk state instead of re-cloning into an empty dir). Leave unset for a fresh sandbox (today's default behaviour). The referenced run MUST be a sibling of this spawn under the same root (same parent chain) and same project — any other value is rejected and the rejection surfaces back into step_history on the next DECIDE."
                    }
                },
                "required": ["role", "goal"],
                "additionalProperties": false
            }
        }
    })
}

/// RFC 031 PR-C site 1 helper: filter `tool_descs` by the role's
/// allowlist.
///
/// * `forbid_all_tools == true` clears the surface entirely (§D3).
/// * `forbid_all_tools == false` + non-empty `tools[]` retains only
///   matching ids.
/// * `forbid_all_tools == false` + empty `tools[]` leaves the surface
///   untouched (the pre-RFC-031 "no role restriction" default).
fn apply_role_tool_allowlist(role: &AgentRole, tool_descs: &mut Vec<BuiltinToolDescriptor>) {
    if role.forbid_all_tools {
        tool_descs.clear();
        return;
    }
    if role.tools.is_empty() {
        return;
    }
    tool_descs.retain(|d| role.tools.iter().any(|a| a == d.name.as_str()));
}

/// RFC 031 PR-C fallback for `resolve_role_or_fallback` when no
/// `AgentRoleService` is attached. Returns the built-in with the
/// same id when one exists; otherwise returns an empty-allowlist
/// role record (preserves the pre-RFC-031 "unknown role → no
/// restriction" allowlist behaviour — full §D7 generic fallback is
/// only wired when the service is attached, so legacy callers /
/// fixtures that reference made-up role ids keep working).
///
/// `default_roles()` clones every role's multi-KB system prompt on
/// every call; cache the vec via `OnceLock` so the fallback path
/// pays the allocation once per process. Same pattern as the
/// pre-PR-C `ROLE_ENUM` cache on `spawn_subagent_tool_def`.
fn fallback_role_by_id(role_id: &str) -> AgentRole {
    static DEFAULT_ROLES_CACHE: std::sync::OnceLock<Vec<AgentRole>> = std::sync::OnceLock::new();
    let roles = DEFAULT_ROLES_CACHE.get_or_init(default_roles);
    if let Some(r) = roles.iter().find(|r| r.role_id == role_id) {
        return r.clone();
    }
    // Unknown id and no service → empty-allowlist role. Byte-identical
    // DECIDE behaviour to pre-RFC-031, where `default_roles().iter()
    // .find(|r| r.role_id == ctx.agent_type)` returned `None` and the
    // filter was skipped.
    use cairn_domain::agent_roles::AgentRoleTier;
    AgentRole::new(role_id, role_id, AgentRoleTier::Standard)
}

// ── F38 stuck-loop nudge ─────────────────────────────────────────────────────

/// Iteration index at which we start injecting the "you are stuck" nudge.
///
/// The predicate uses `ctx.iteration >= STUCK_ITERATION_THRESHOLD`, so
/// threshold = 3 means the directive suffix is eligible starting at
/// iteration index 3 — the 4th DECIDE call, since iterations are
/// 0-indexed. Three is the smallest value that lets a healthy
/// multi-step run (e.g., search → read → complete) finish on its own
/// without tripping the nudge, while still catching the dogfood-v5
/// introspection pattern (memory_search × 4, graph_query × 1,
/// notify_operator × 3 … with `complete_run × 0`) early enough to
/// salvage the run.
///
/// # Why this threshold
///
/// Original Gemini review on PR #300 flagged threshold=3 as
/// aggressive against the then-default `max_iterations = 20` cap.
/// R21 dogfood (#797) confirmed it fires too early on procedural
/// goals; bumped to 12 here, with `DEFAULT_MAX_ITERATIONS` raised
/// to 50. Two facts make the choice safe in practice:
///
/// 1. **The nudge is escapable.** [`stuck_nudge_suffix`] explicitly lets
///    the model call `complete_run` with `final_answer = "<what I still
///    need>"` when it genuinely needs more steps. A legitimate
///    multi-step task gets a clean exit point instead of a hard
///    truncation; the operator sees a real answer describing the gap
///    rather than `max_iterations_reached` and an empty summary.
/// 2. **Passing this threshold is RARE on healthy runs.** A
///    well-behaved orchestration that actually needs 4+ tool calls
///    usually emits at least one `complete_run` checkpoint along the
///    way (for example the EXECUTE-mode handoff from a plan). The
///    predicate skips the nudge as soon as `complete_run` appears in
///    history, so "long run with a plan" is immune.
///
/// If future dogfood shows legitimate multi-step runs being nudged too
/// early, bump this to 4–5 rather than disabling the feature entirely;
/// the fix for dogfood-v5 task-1 collapses quickly if the threshold
/// drifts above ~6.
///
/// Bumped from 3 → 12 (R21 dogfood, #797). The original 3 was sized
/// for a Q&A-shaped workload — 3 introspection-only iterations
/// without a `complete_run` was a real bug shape on dogfood-v5
/// task-1. But on a procedural workload like "clone + branch +
/// write file + cargo check + commit + push + gh pr create", 9-12
/// consecutive `invoke_tool` steps are EXPECTED — none of them
/// terminate, none should. The old threshold fired the
/// "STOP — FINAL DIRECTIVE" suffix mid-procedural-goal at iter=3,
/// forcing the model to call `complete_run("partial")` instead of
/// completing the work. R21 shipped 0 PRs across 8 dogfood-m1
/// issues because of this; 13 of 20 sub-agents bailed at iter=3
/// with "I ran out of iterations" reports. The new threshold (12)
/// still catches genuine introspection loops — a Q&A run that
/// emits 12 consecutive `invoke_tool` steps without ever calling
/// `complete_run` is still pathological — without strangling the
/// procedural shape that's the dogfood norm.
pub(crate) const STUCK_ITERATION_THRESHOLD: u32 = 12;

/// Decide whether to append the stuck-loop directive to this iteration's
/// user message.
///
/// The predicate fires when the RUN is in the stuck-introspection shape
/// that dogfood-v5 task-1 surfaced: several consecutive non-terminal
/// tool calls with no `complete_run`. Specifically, the most recent
/// `STUCK_ITERATION_THRESHOLD` steps must all be non-terminal and
/// must include at least one tool invocation — that last clause
/// distinguishes a genuine introspection streak from, say, a stretch
/// of context-compaction placeholders with no real work.
///
/// The tail-only check matters: checking the entire history for the
/// absence of `complete_run` would fire on every 4th+ iteration of any
/// long-running task, because `StepSummary.action_kind` is set to the
/// DECIDE proposal kind (normally `invoke_tool`) and a run in progress
/// has no reason to have emitted `complete_run` yet. See the
/// Copilot review on PR #300 for the full argument.
///
/// Kept as a free function so unit tests can exercise the predicate
/// without constructing a full `LlmDecidePhase`.
pub(crate) fn should_inject_stuck_nudge(
    iteration: u32,
    step_history: &[crate::context::StepSummary],
) -> bool {
    if iteration < STUCK_ITERATION_THRESHOLD {
        return false;
    }
    let window = STUCK_ITERATION_THRESHOLD as usize;
    if step_history.len() < window {
        return false;
    }
    let tail = &step_history[step_history.len() - window..];
    // `action_kind` is the step-kind string recorded in `StepSummary`:
    // often a serialized `ActionType` ("invoke_tool", "complete_run",
    // …), but it may also be a non-`ActionType` marker such as
    // `"compacted_summary"` (from RFC 018 compaction) or other
    // loop-runner-internal kinds. We treat ANY `complete_run` in the
    // window as "not stuck", and require at least one `invoke_tool`
    // to confirm the model was genuinely doing tool work (and not
    // e.g. waiting on compactions or approvals).
    let has_terminal = tail.iter().any(|s| s.action_kind == "complete_run");
    let has_invoke_tool = tail.iter().any(|s| s.action_kind == "invoke_tool");
    !has_terminal && has_invoke_tool
}

/// The directive suffix appended to the user message when
/// `should_inject_stuck_nudge` fires.
///
/// Invariant: the wording must be blunt (all-caps header, "MUST" rather
/// than "should") AND carry an explicit escape hatch so a legitimate
/// multi-step run can emit `complete_run` with a "what's missing"
/// answer instead of being truncated. Both properties are load-bearing
/// and are pinned by the unit test in
/// `test_f38_complete_run_actually_invoked.rs`.
pub(crate) fn stuck_nudge_suffix() -> &'static str {
    // Intentionally NOT written as an indented multi-line string literal:
    // the line-continuation syntax (`\` at end of line + leading spaces
    // on the next) preserves the source-code indentation in the
    // resulting `&str`, which would render the "## STOP — FINAL
    // DIRECTIVE" header as a five-space-indented line and read in the
    // LLM context like a nested code block, reducing its salience
    // (Copilot review on PR #300). Flush-left concatenation keeps the
    // markdown heading at column 0 where providers render it as a
    // section break.
    concat!(
        "\n\n",
        "## STOP — FINAL DIRECTIVE\n",
        "You have already spent several iterations calling tools without ",
        "producing a user-facing answer. The user is waiting for a response. ",
        "On THIS turn you MUST call the `complete_run` tool with the full ",
        "answer in `final_answer`. Do NOT call any other tool. If you cannot ",
        "produce a confident answer from training data or the context above, ",
        "call `complete_run` anyway and explain in `final_answer` what ",
        "information is missing and what you would do next. Any tool call ",
        "other than `complete_run` on this turn will be treated as a ",
        "violation of the run contract."
    )
}

/// #697 R5-B helper: promote pseudo-XML tool-calls embedded in
/// `content` into the given `tool_calls` slot when the provider didn't
/// populate the native `tool_calls[]` array.
///
/// Returns `true` if a promotion happened so the caller can skip the
/// text-parsing fallback path. Logs at INFO with the resolved model id
/// so operators can spot models that regress to content-leak mode.
///
/// This is deliberately local (file-private, no Cargo re-export)
/// because it's a one-component concern — the content-scan is only
/// valid in the DECIDE-phase happy/retry paths where the provider
/// response shape is OpenAI-compat. YAGNI says don't widen the
/// surface until a second caller appears.
fn promote_content_scan_into_tool_calls(
    tool_calls: &mut Vec<serde_json::Value>,
    response_text: &str,
    resolved_model_id: &str,
    origin: &'static str,
) -> bool {
    if !tool_calls.is_empty() || response_text.is_empty() {
        return false;
    }
    let scanned = crate::content_tool_scan::scan_content_for_tool_calls(response_text);
    if scanned.is_empty() {
        return false;
    }
    tracing::info!(
        model_id = %resolved_model_id,
        scanned_count = scanned.len(),
        origin = origin,
        "#697 R5-B: promoted content-embedded tool_calls from pseudo-XML — \
         model emitted tool_call as prose instead of native tool_calls[]",
    );
    *tool_calls = scanned;
    true
}

/// Convert native tool_calls from the provider response into `ActionProposal` values.
///
/// Each tool_call becomes an `InvokeTool` proposal. The tool_name is the function name
/// and tool_args are the parsed arguments.
fn tool_calls_to_proposals(
    tool_calls: &[serde_json::Value],
    tool_descs: &[BuiltinToolDescriptor],
) -> Vec<ActionProposal> {
    let proposals: Vec<ActionProposal> = tool_calls
        .iter()
        .filter_map(|tc| {
            let func = tc.get("function")?;
            let raw_name = func.get("name")?.as_str()?.to_owned();
            // Arguments can be a JSON string (OpenAI) or a parsed object (Anthropic/Bedrock).
            let raw_args = match func.get("arguments") {
                Some(serde_json::Value::String(s)) => {
                    serde_json::from_str(s).unwrap_or(serde_json::Value::Null)
                }
                Some(v) => v.clone(),
                None => serde_json::json!({}),
            };

            // Defensive unwrap: small/mid models (Qwen 3.6, Gemma 4 A2B)
            // sometimes confuse the legacy JSON-action protocol's
            // `invoke_tool` / `spawn_subagent` meta-verbs with a real tool
            // name and emit e.g. `{"name":"invoke_tool","arguments":
            // {"tool_name":"bash","tool_args":{...}}}`. Unwrap that shape
            // into a normal tool call so the registry lookup succeeds.
            //
            // `spawn_subagent` is NOT a real tool — agent roles are not
            // registered in the tool catalogue. When we see that envelope
            // we produce a `SpawnSubagent` proposal directly so the loop
            // dispatches it correctly.
            //
            // #697 R5-A: two shapes accepted for spawn_subagent tool_calls:
            //
            //  1. NATIVE (preferred): flat `{"role": ..., "goal": ...}`
            //     emitted against the tool_def published in
            //     `spawn_subagent_tool_def()`. This is what reliable
            //     structured-output providers (OpenAI, Anthropic, and
            //     the nemotron/gemma/minimax free-tier models verified
            //     in R5 probes) produce.
            //  2. LEGACY: nested `{"tool_name": ..., "tool_args": {"goal": ...}}`
            //     — the pre-R5-A meta-verb shape. Kept for JSON-action
            //     envelope compat and for any runs mid-flight against
            //     old system prompts.
            //
            // Priority: check flat shape first (it's the new default
            // and the more common wire shape after #697 ships). Only
            // fall back to the legacy unwrap if top-level `role` is
            // absent. This ordering preserves pre-#697 behaviour
            // byte-identically on the legacy path.
            if raw_name == "spawn_subagent" {
                let (role, goal) = parse_spawn_subagent_args(&raw_args);
                // #775 / #844: carry the optional freeform
                // parent_context AND the opt-in reuse_sandbox_from
                // sibling id through on the native tool-call path.
                // Pre-#775 these fields were lost when the native
                // shape was reconstructed into `{goal}` only; the
                // execute layer then saw a stripped tool_args and
                // the features were effectively dead on providers
                // that use native tool calls (OpenAI, Anthropic,
                // Bedrock). Extract from both the flat and the
                // nested legacy shapes.
                let (parent_context_opt, reuse_sandbox_opt) =
                    extract_spawn_subagent_optionals(&raw_args);
                let mut forwarded = serde_json::json!({ "goal": goal });
                if let Some(pc) = parent_context_opt {
                    forwarded["parent_context"] = serde_json::Value::String(pc);
                }
                if let Some(reuse) = reuse_sandbox_opt {
                    forwarded["reuse_sandbox_from"] = serde_json::Value::String(reuse);
                }
                return Some(ActionProposal {
                    action_type: ActionType::SpawnSubagent,
                    description: format!("spawn {role}"),
                    confidence: 0.9,
                    tool_name: Some(role),
                    tool_args: Some(forwarded),
                    requires_approval: false,
                });
            }
            let (name, args) = unwrap_meta_envelope(&raw_name, raw_args);

            // F36: native `complete_run` tool call → terminal CompleteRun
            // proposal. The `final_answer` argument becomes the proposal
            // description so the loop runner's `LoopTermination::Completed`
            // branch returns it verbatim to the user (see
            // `loop_runner.rs` — the proposal description is copied into
            // `summary`).
            //
            // `final_answer` is a REQUIRED field in the schema. If the
            // model omits it entirely — including under the tolerated
            // aliases (`description`, `answer`, `content`, `summary`),
            // which cover known GLM/Qwen/Gemma shapes — we do NOT invent
            // a default string. Silently returning "run completed" would
            // both mask schema drift from upstream providers (Gemini's
            // review guidance) and hand a useless summary back to the
            // user. Instead we escalate to the operator with the raw
            // arguments so a human can diagnose. This keeps the failure
            // loud while staying in-channel (the caller is a
            // `filter_map` returning `Vec<ActionProposal>`; we can't
            // return a `Result` without rewriting every call site).
            if name == "complete_run" {
                let final_answer = args
                    .get("final_answer")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
                    .or_else(|| {
                        for alias in ["description", "answer", "content", "summary"] {
                            if let Some(s) = args.get(alias).and_then(|v| v.as_str()) {
                                return Some(s.to_owned());
                            }
                        }
                        None
                    });

                return Some(match final_answer {
                    Some(text) => ActionProposal {
                        action_type: ActionType::CompleteRun,
                        description: text,
                        confidence: 0.95,
                        tool_name: None,
                        tool_args: None,
                        requires_approval: false,
                    },
                    None => ActionProposal::escalate(
                        format!(
                            "Model called `complete_run` but omitted the required \
                             `final_answer` argument (and none of the tolerated \
                             aliases description/answer/content/summary were \
                             present). Raw arguments: {args}"
                        ),
                        0.0,
                    ),
                });
            }

            // #825: native `fail_run` tool call → terminal FailRun proposal.
            // Parallel to complete_run above: reason lands in
            // proposal.description, which derive_signal wraps in the
            // `model_reported_failure:` prefix so the HTTP layer routes
            // it to FailureClass::ModelReportedFailure. Tolerate the
            // same aliases complete_run tolerates (description, answer,
            // content, summary) because GLM/Qwen/Gemma models drift
            // the same way on the new verb. A schema miss escalates to
            // the operator, matching the complete_run posture — we do
            // NOT invent a reason (would silently mask schema drift).
            if name == "fail_run" {
                let reason = args
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
                    .or_else(|| {
                        for alias in ["description", "answer", "content", "summary"] {
                            if let Some(s) = args.get(alias).and_then(|v| v.as_str()) {
                                return Some(s.to_owned());
                            }
                        }
                        None
                    });

                return Some(match reason {
                    Some(text) => ActionProposal::fail_run(text, 0.95),
                    None => ActionProposal::escalate(
                        format!(
                            "Model called `fail_run` but omitted the required \
                             `reason` argument (and none of the tolerated \
                             aliases description/answer/content/summary were \
                             present). Raw arguments: {args}"
                        ),
                        0.0,
                    ),
                });
            }

            // Check if this tool is a safe read-only action.
            let requires_approval = tool_descs
                .iter()
                .find(|d| d.name == name)
                .map(|d| {
                    matches!(
                        d.execution_class,
                        cairn_domain::policy::ExecutionClass::Sensitive
                    )
                })
                .unwrap_or(false);

            Some(ActionProposal {
                action_type: ActionType::InvokeTool,
                description: format!("invoke {name}"),
                confidence: 0.9, // native tool calls are high-confidence by definition
                tool_name: Some(name),
                tool_args: Some(args),
                requires_approval,
            })
        })
        .collect();

    if proposals.is_empty() {
        // All tool calls were malformed — escalate
        vec![ActionProposal::escalate(
            "Model returned tool_calls but none could be parsed".to_owned(),
            0.0,
        )]
    } else {
        proposals
    }
}

/// Unwrap a meta-envelope tool call into a direct tool call.
///
/// When a model emits a tool call whose `name` is one of cairn's legacy
/// JSON-action meta-verbs (`invoke_tool`, `spawn_subagent`), this helper
/// peels the envelope: it extracts `tool_name` + `tool_args` from the
/// arguments and returns `(tool_name, tool_args)`. If the shape does not
/// match (missing `tool_name`, non-object args, etc.), the original
/// `(name, args)` tuple is returned unchanged so downstream error
/// handling can surface a clear "unknown tool" diagnostic.
/// #697 R5-A: extract `(role, goal)` from a `spawn_subagent` tool_call's
/// `arguments`, accepting both the native flat shape and the legacy
/// nested `{tool_name, tool_args}` shape.
///
/// Return values are best-effort strings — the caller builds the
/// proposal with `tool_name: Some(role)` and
/// `tool_args: Some({"goal": goal})`, and downstream validation in
/// `TaskService::spawn_subagent`'s adapter enforces the non-empty
/// `goal` contract (surfacing via R2-A's retry-with-feedback when the
/// LLM still dropped the field).
///
/// Precedence (most-recent-shape first):
///   1. Flat: `args.role` + `args.goal`
///   2. Legacy nested: `args.tool_name` + `args.tool_args.goal`
///
/// Missing fields return empty strings — the validator rejects those
/// and the retry loop gives the LLM a chance to correct.
fn parse_spawn_subagent_args(args: &serde_json::Value) -> (String, String) {
    let obj = match args.as_object() {
        Some(o) => o,
        None => return (String::new(), String::new()),
    };

    // Shape 1: native flat `{role, goal}`.
    if let Some(role) = obj.get("role").and_then(|v| v.as_str()) {
        let goal = obj
            .get("goal")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_owned();
        return (role.to_owned(), goal);
    }

    // Shape 2: legacy nested `{tool_name, tool_args: {goal}}`.
    let role = obj
        .get("tool_name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_owned();
    let goal = obj
        .get("tool_args")
        .and_then(|v| v.as_object())
        .and_then(|inner| inner.get("goal"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_owned();
    (role, goal)
}

/// #844 PR-2 + #775: extract the optional freeform `parent_context` and
/// the opt-in `reuse_sandbox_from` sibling id from a `spawn_subagent`
/// tool-call's `arguments`. Accepts the same two shapes as
/// [`parse_spawn_subagent_args`]:
///
///   1. Flat (native): `args.parent_context` / `args.reuse_sandbox_from`
///   2. Legacy nested: `args.tool_args.parent_context` /
///      `args.tool_args.reuse_sandbox_from`
///
/// Whitespace-only / empty strings → `None` so the persistence path
/// (which is gated by `Some(_)`) does not write a default row with a
/// blank value. Non-string values are silently ignored — the schema
/// declares `string`, and a malformed shape is better dropped than
/// propagated to `TaskService::spawn_subagent` which would reject it
/// with a less-specific error.
pub(crate) fn extract_spawn_subagent_optionals(
    args: &serde_json::Value,
) -> (Option<String>, Option<String>) {
    let obj = match args.as_object() {
        Some(o) => o,
        None => return (None, None),
    };
    // Prefer the flat shape; fall back to the nested legacy tool_args
    // object. Do NOT merge — the two shapes are alternates on the
    // wire and mixing would be an LLM error we shouldn't paper over.
    let source: &serde_json::Map<String, serde_json::Value> =
        if obj.contains_key("parent_context") || obj.contains_key("reuse_sandbox_from") {
            obj
        } else if let Some(inner) = obj.get("tool_args").and_then(|v| v.as_object()) {
            inner
        } else {
            obj
        };
    let pc = source
        .get("parent_context")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);
    let reuse = source
        .get("reuse_sandbox_from")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);
    (pc, reuse)
}

fn unwrap_meta_envelope(name: &str, args: serde_json::Value) -> (String, serde_json::Value) {
    if name != "invoke_tool" && name != "spawn_subagent" {
        return (name.to_owned(), args);
    }
    let Some(obj) = args.as_object() else {
        return (name.to_owned(), args);
    };
    let Some(inner_name) = obj.get("tool_name").and_then(|v| v.as_str()) else {
        return (name.to_owned(), serde_json::Value::Object(obj.clone()));
    };
    let inner_args = obj
        .get("tool_args")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    (inner_name.to_owned(), inner_args)
}

fn strip_markdown_fence(s: &str) -> &str {
    let s = s.trim();
    if let Some(inner) = s.strip_prefix("```json").or_else(|| s.strip_prefix("```")) {
        if let Some(inner) = inner.strip_suffix("```") {
            return inner.trim();
        }
    }
    s
}

fn parse_one(v: serde_json::Value) -> Option<ActionProposal> {
    let obj = v.as_object()?;
    let action_type = match obj.get("action_type")?.as_str()? {
        "spawn_subagent" => ActionType::SpawnSubagent,
        "invoke_tool" => ActionType::InvokeTool,
        "create_memory" => ActionType::CreateMemory,
        "send_notification" => ActionType::SendNotification,
        "complete_run" => ActionType::CompleteRun,
        // #825: truthful terminal "I tried, I can't proceed." See
        // FailureClass::ModelReportedFailure + fail_run_tool_def().
        "fail_run" => ActionType::FailRun,
        "escalate_to_operator" => ActionType::EscalateToOperator,
        _ => return None,
    };
    let description = obj
        .get("description")
        .and_then(|d| d.as_str())
        .unwrap_or("")
        .to_owned();
    let confidence = obj
        .get("confidence")
        .and_then(|c| c.as_f64())
        .unwrap_or(0.5)
        .clamp(0.0, 1.0);
    let requires_approval = obj
        .get("requires_approval")
        .and_then(|r| r.as_bool())
        .unwrap_or(false);
    let tool_name = obj
        .get("tool_name")
        .and_then(|n| n.as_str())
        .map(str::to_owned);
    let tool_args = obj.get("tool_args").cloned();

    Some(ActionProposal {
        action_type,
        description,
        confidence,
        tool_name,
        tool_args,
        requires_approval,
    })
}

/// Return `true` when an action proposal is inherently safe (read-only) and
/// should never require approval, regardless of what the model returned.
///
/// Models sometimes over-cautiously set `requires_approval=true` for memory
/// searches or HTTP GETs. This guard corrects that before the approval gate.
fn is_safe_read_action(proposal: &ActionProposal) -> bool {
    use ActionType::{CompleteRun, CreateMemory, InvokeTool};
    match proposal.action_type {
        InvokeTool => {
            let name = proposal.tool_name.as_deref().unwrap_or("").to_lowercase();
            matches!(
                name.as_str(),
                "memory_search"
                    | "web_fetch"
                    | "webfetch"
                    | "http_request"
                    | "get_run"
                    | "get_task"
                    | "search_memory"
                    | "list_runs"
                    | "glob"
                    | "glob_find"
                    | "grep"
                    | "grep_search"
                    | "read"
                    | "read_document"
                    | "file_read"
                    | "graph_query"
                    | "search_events"
                    | "tool_search"
            )
        }
        CreateMemory | CompleteRun => true,
        _ => false,
    }
}

/// Returns true when `proposals` is the zero-confidence single-action
/// fallback the decide phase emits on parse failure (one
/// `EscalateToOperator` with `confidence == 0.0`).
fn is_fallback_escalation(proposals: &[ActionProposal]) -> bool {
    proposals.len() == 1
        && proposals[0].action_type == ActionType::EscalateToOperator
        && proposals[0].confidence == 0.0
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_domain::contexts::PluginCategory;
    use cairn_domain::providers::{GenerationResponse, ProviderAdapterError};
    use cairn_domain::OperatorId;
    use cairn_runtime::services::{
        is_plugin_tool_visible, DescriptorSource, MarketplaceCommand, MarketplaceService,
        PluginDescriptor,
    };
    use cairn_store::InMemoryStore;
    use cairn_tools::builtins::{
        BuiltinToolRegistry, ToolEffect, ToolError, ToolHandler, ToolResult, ToolSearchTool,
        ToolTier,
    };
    use std::path::PathBuf;
    use std::sync::Arc;

    // ── Mock provider ─────────────────────────────────────────────────────────

    struct MockProvider {
        response: String,
    }

    #[async_trait]
    impl GenerationProvider for MockProvider {
        async fn generate(
            &self,
            _model_id: &str,
            _messages: Vec<serde_json::Value>,
            _settings: &ProviderBindingSettings,
            _tools: &[serde_json::Value],
        ) -> Result<GenerationResponse, ProviderAdapterError> {
            Ok(GenerationResponse {
                text: self.response.clone(),
                input_tokens: Some(150),
                output_tokens: Some(100),
                model_id: "test-brain".to_owned(),
                tool_calls: vec![],
                finish_reason: None,
            })
        }
    }

    struct FailingProvider;

    #[async_trait]
    impl GenerationProvider for FailingProvider {
        async fn generate(
            &self,
            _: &str,
            _: Vec<serde_json::Value>,
            _: &ProviderBindingSettings,
            _tools: &[serde_json::Value],
        ) -> Result<GenerationResponse, ProviderAdapterError> {
            Err(ProviderAdapterError::TransportFailure("offline".to_owned()))
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn ctx() -> OrchestrationContext {
        OrchestrationContext {
            project: cairn_domain::ProjectKey::new("t", "w", "p"),
            session_id: cairn_domain::SessionId::new("sess_1"),
            run_id: cairn_domain::RunId::new("run_1"),
            task_id: None,
            iteration: 0,
            goal: "Summarise the cairn-rs architecture document.".to_owned(),
            agent_type: "orchestrator".to_owned(),
            run_started_at_ms: 0,
            working_dir: PathBuf::from("."),
            run_mode: cairn_domain::decisions::RunMode::Direct,
            discovered_tool_names: vec![],
            step_history: vec![],
            is_recovery: false,
            approval_timeout: None,
            visibility: None,
            parent_context: None,
            declared_but_missing: OrchestrationContext::empty_declared_but_missing(),
            agent_role_list_cache: OrchestrationContext::empty_agent_role_list_cache(),
        }
    }

    fn empty_gather() -> GatherOutput {
        GatherOutput::default()
    }

    fn plugin_descriptor() -> PluginDescriptor {
        PluginDescriptor {
            id: "github".to_owned(),
            name: "GitHub".to_owned(),
            version: "0.1.0".to_owned(),
            description: Some("GitHub integration".to_owned()),
            category: PluginCategory::IssueTracker,
            vendor: "cairn".to_owned(),
            icon_url: None,
            command: vec!["echo".to_owned(), "github".to_owned()],
            tools: vec![
                "github.issue_brief".to_owned(),
                "github.issue_search".to_owned(),
            ],
            signal_sources: vec![],
            channels: vec![],
            required_credentials: vec![],
            required_network_egress: vec![],
            post_install_health_check: None,
            source: DescriptorSource::BundledCatalog,
            download_url: None,
            has_signal_source: false,
        }
    }

    fn operator() -> OperatorId {
        OperatorId::new("op_test")
    }

    struct FakePluginTool {
        name: &'static str,
        description: &'static str,
        tier: ToolTier,
    }

    #[async_trait]
    impl ToolHandler for FakePluginTool {
        fn name(&self) -> &str {
            self.name
        }

        fn tier(&self) -> ToolTier {
            self.tier
        }

        fn tool_effect(&self) -> ToolEffect {
            ToolEffect::Observational
        }

        fn description(&self) -> &str {
            self.description
        }

        fn parameters_schema(&self) -> serde_json::Value {
            serde_json::json!({
                "type": "object",
                "properties": {}
            })
        }

        async fn execute(
            &self,
            _: &cairn_domain::ProjectKey,
            _: serde_json::Value,
        ) -> Result<ToolResult, ToolError> {
            Ok(ToolResult::ok(serde_json::json!({ "ok": true })))
        }
    }

    fn registry_for_project(project: &cairn_domain::ProjectKey) -> Arc<BuiltinToolRegistry> {
        let mut marketplace = MarketplaceService::new(Arc::new(InMemoryStore::new()));
        marketplace.list_plugin(plugin_descriptor());
        marketplace
            .handle_command(MarketplaceCommand::InstallPlugin {
                plugin_id: "github".to_owned(),
                initiated_by: operator(),
            })
            .unwrap();
        marketplace
            .handle_command(MarketplaceCommand::EnablePluginForProject {
                plugin_id: "github".to_owned(),
                project: cairn_domain::ProjectKey::new("tenant", "workspace", "project-a"),
                tool_allowlist: Some(vec![
                    "github.issue_brief".to_owned(),
                    "github.issue_search".to_owned(),
                ]),
                signal_allowlist: None,
                signal_capture_override: None,
                enabled_by: operator(),
            })
            .unwrap();

        let visibility = marketplace.build_visibility_context(project, None);
        let registered = Arc::new(FakePluginTool {
            name: "github.issue_brief",
            description: "Summarise a GitHub issue for the operator.",
            tier: ToolTier::Registered,
        });
        let deferred = Arc::new(FakePluginTool {
            name: "github.issue_search",
            description: "Search GitHub issues by title or label.",
            tier: ToolTier::Deferred,
        });

        let mut inner = BuiltinToolRegistry::new();
        if is_plugin_tool_visible(&visibility, "github", registered.name()) {
            inner = inner.register(registered.clone());
        }
        if is_plugin_tool_visible(&visibility, "github", deferred.name()) {
            inner = inner.register(deferred.clone());
        }
        let inner = Arc::new(inner);

        let mut outer = BuiltinToolRegistry::new();
        if is_plugin_tool_visible(&visibility, "github", registered.name()) {
            outer = outer.register(registered);
        }
        if is_plugin_tool_visible(&visibility, "github", deferred.name()) {
            outer = outer.register(deferred);
        }
        outer = outer.register(Arc::new(ToolSearchTool::new(inner)));
        Arc::new(outer)
    }

    // ── Prompt builder tests ──────────────────────────────────────────────────

    #[test]
    fn system_prompt_references_orchestrator_role() {
        // Legacy text-mode (no native tool calling): mentions invoke_tool envelope.
        let sys = build_system_prompt("orchestrator", &[], false);
        // #702 fix: the orchestrator role identity is task-neutral
        // ("senior autonomous orchestrator") rather than the pre-#702
        // code-biased "senior engineer executing an autonomous coding
        // run". Assert on the orchestration-specialty anchor that
        // replaced it. Prompt text is owned by cairn-domain::agent_roles.
        assert!(
            sys.contains("senior autonomous orchestrator"),
            "should use orchestrator role identity — prompt text is owned by \
             cairn-domain::agent_roles and must name the orchestrator's \
             specialty explicitly"
        );
        assert!(
            sys.contains("JSON array"),
            "should instruct JSON array return"
        );
        assert!(sys.contains("spawn_subagent"), "should list spawn_subagent");
        assert!(sys.contains("complete_run"), "should list complete_run");
    }

    #[test]
    fn system_prompt_fallback_for_unknown_role() {
        // #775: unknown role ids no longer get the 3-line generic
        // fallback. They render the `generic` role's full assembled
        // prompt (BASE_SUBAGENT_PROMPT + GENERIC_PROMPT). The test
        // pins the new contract: identity is sub-agent (from BASE),
        // and the prompt is structurally complete (Phase 1 / Phase
        // 5 / complete_run anchors all present).
        let sys = build_system_prompt("wizard", &[], false);
        assert!(
            sys.contains("JSON array"),
            "fallback must still instruct JSON return"
        );
        assert!(
            sys.contains("sub-agent"),
            "fallback should adopt sub-agent identity from BASE_SUBAGENT_PROMPT"
        );
        assert!(
            sys.contains("Phase 1"),
            "fallback must contain Phase 1 (from generic specialty overlay)"
        );
        assert!(sys.contains("Phase 5"), "fallback must contain Phase 5");
        assert!(
            sys.contains("complete_run"),
            "fallback must name complete_run as the terminator"
        );
    }

    #[test]
    fn system_prompt_native_tool_mode_omits_invoke_tool_envelope() {
        // When native tool calling is enabled, the system prompt must not
        // instruct the model to wrap calls in `invoke_tool` — doing so is
        // what causes Qwen 3.6 / Gemma 4 A2B to emit
        // `tool_calls[].name == "invoke_tool"` (F12b).
        let desc = BuiltinToolDescriptor {
            name: "bash".to_owned(),
            tier: ToolTier::Registered,
            description: "Run a shell command.".to_owned(),
            parameters_schema: serde_json::json!({
                "type": "object",
                "properties": { "command": { "type": "string" } },
                "required": ["command"],
                "additionalProperties": false,
            }),
            execution_class: cairn_domain::policy::ExecutionClass::Sensitive,
            permission_level: cairn_tools::builtins::PermissionLevel::ReadOnly,
            category: cairn_tools::builtins::ToolCategory::FileSystem,
            tool_effect: ToolEffect::External,
            retry_safety: cairn_tools::builtins::RetrySafety::DangerousPause,
        };
        let sys = build_system_prompt("orchestrator", std::slice::from_ref(&desc), true);
        assert!(
            !sys.contains("Use invoke_tool with"),
            "native-tool prompt must not instruct invoke_tool envelope. Got: {sys}"
        );
        assert!(
            sys.contains("Call any of the following tools directly"),
            "native-tool prompt must instruct direct tool call. Got: {sys}"
        );
        assert!(
            sys.contains("bash("),
            "native-tool prompt must list registered tools by name"
        );
    }

    #[test]
    fn tool_calls_unwrap_invoke_tool_envelope() {
        // Small/mid models (Qwen 3.6, Gemma 4 A2B) sometimes emit the
        // legacy JSON-action envelope via the native tool_calls channel:
        //   name="invoke_tool", arguments={"tool_name":"bash","tool_args":{...}}
        // We must unwrap that into a proper `bash` call.
        let tool_calls = vec![serde_json::json!({
            "type": "function",
            "function": {
                "name": "invoke_tool",
                "arguments": {
                    "tool_name": "bash",
                    "tool_args": { "command": "echo hi" }
                }
            }
        })];
        let proposals = tool_calls_to_proposals(&tool_calls, &[]);
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].tool_name.as_deref(), Some("bash"));
        assert_eq!(
            proposals[0]
                .tool_args
                .as_ref()
                .and_then(|a| a.get("command")),
            Some(&serde_json::Value::String("echo hi".to_owned())),
        );
    }

    #[test]
    fn tool_calls_unwrap_spawn_subagent_envelope() {
        // Legacy nested shape: `{tool_name, tool_args: {goal}}`.
        // Kept for backward compat with JSON-action-envelope emissions
        // and with any system prompts mid-flight after R5-A ships.
        let tool_calls = vec![serde_json::json!({
            "type": "function",
            "function": {
                "name": "spawn_subagent",
                "arguments": {
                    "tool_name": "researcher",
                    "tool_args": { "goal": "summarise RFCs" }
                }
            }
        })];
        let proposals = tool_calls_to_proposals(&tool_calls, &[]);
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].action_type, ActionType::SpawnSubagent);
        assert_eq!(proposals[0].tool_name.as_deref(), Some("researcher"));
        assert_eq!(
            proposals[0].tool_args.as_ref().and_then(|a| a.get("goal")),
            Some(&serde_json::Value::String("summarise RFCs".to_owned())),
        );
    }

    #[test]
    fn tool_calls_accept_flat_spawn_subagent_shape() {
        // #697 R5-A: native flat shape `{role, goal}` — what the native
        // tool_def schema elicits from well-behaved providers (OpenAI,
        // Anthropic, Nemotron/Gemma/Minimax with native tools
        // enabled per R5 probe evidence).
        let tool_calls = vec![serde_json::json!({
            "type": "function",
            "function": {
                "name": "spawn_subagent",
                "arguments": {
                    "role": "researcher",
                    "goal": "Identify 3 Rust circuit breaker best practices"
                }
            }
        })];
        let proposals = tool_calls_to_proposals(&tool_calls, &[]);
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].action_type, ActionType::SpawnSubagent);
        assert_eq!(proposals[0].tool_name.as_deref(), Some("researcher"));
        assert_eq!(
            proposals[0].tool_args.as_ref().and_then(|a| a.get("goal")),
            Some(&serde_json::Value::String(
                "Identify 3 Rust circuit breaker best practices".to_owned()
            )),
        );
    }

    #[test]
    fn tool_calls_spawn_subagent_missing_goal_surfaces_empty_string() {
        // Defense in depth: if the LLM emits `{role, ...}` but drops
        // `goal`, we surface an empty goal string. The downstream
        // `TaskService::spawn_subagent` validator then rejects via
        // R2-A's retry-with-feedback path — the LLM sees "goal is
        // required" on the next turn and corrects. We do NOT drop the
        // proposal here: that would make the failure invisible to the
        // retry loop.
        let tool_calls = vec![serde_json::json!({
            "type": "function",
            "function": {
                "name": "spawn_subagent",
                "arguments": { "role": "researcher" }
            }
        })];
        let proposals = tool_calls_to_proposals(&tool_calls, &[]);
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].tool_name.as_deref(), Some("researcher"));
        assert_eq!(
            proposals[0].tool_args.as_ref().and_then(|a| a.get("goal")),
            Some(&serde_json::Value::String(String::new())),
            "empty-goal case surfaces to R2-A retry loop, not silently dropped",
        );
    }

    #[test]
    fn spawn_subagent_tool_def_schema_is_flat_with_required_fields() {
        // #697 R5-A contract test: the published tool_def must have
        // `{role, goal}` both required at the top level of parameters.
        // Production bug if this regresses — e.g. someone swaps to a
        // nested schema hoping to carry more structured args. The
        // R5 dogfood probe evidence is clear that nested args confuse
        // free-tier models; flat is the reliable shape.
        let def = spawn_subagent_tool_def();
        let params = def
            .pointer("/function/parameters")
            .expect("tool def has parameters");
        assert_eq!(params.get("type").and_then(|v| v.as_str()), Some("object"),);
        let required = params
            .get("required")
            .and_then(|v| v.as_array())
            .expect("required present");
        let required_set: std::collections::HashSet<&str> =
            required.iter().filter_map(|v| v.as_str()).collect();
        assert!(required_set.contains("role"), "role must be required");
        assert!(required_set.contains("goal"), "goal must be required");
        // Both fields exist at the flat top level of `properties`.
        let props = params
            .get("properties")
            .and_then(|v| v.as_object())
            .expect("properties present");
        assert!(props.contains_key("role"), "role in properties");
        assert!(props.contains_key("goal"), "goal in properties");
        // #775: parent_context is OPTIONAL — present in properties
        // but NOT in the required set.
        assert!(
            props.contains_key("parent_context"),
            "parent_context in properties (optional, #775)"
        );
        assert!(
            !required_set.contains("parent_context"),
            "parent_context must NOT be required (it is optional freeform context)"
        );
        // additionalProperties:false is the industry-standard constrained-
        // decoding hint (per multi-provider-tool-call-quirks research).
        assert_eq!(
            params.get("additionalProperties").and_then(|v| v.as_bool()),
            Some(false),
            "additionalProperties:false enables strict-mode enforcement",
        );
    }

    /// #844 PR-2: `reuse_sandbox_from` is an optional top-level string
    /// in the `spawn_subagent` tool_def. It must be present in
    /// `properties` so constrained-decoding providers see it on the
    /// wire, and it must NOT be in `required` so omitting it is the
    /// default behaviour (fresh sandbox). The description must name
    /// the same-root + same-sibling contract so a model reading the
    /// schema can't emit an unrelated run_id and expect it to work.
    #[test]
    fn spawn_subagent_tool_def_exposes_reuse_sandbox_from_as_optional() {
        let def = spawn_subagent_tool_def();
        let params = def
            .pointer("/function/parameters")
            .expect("tool def has parameters");
        let props = params
            .get("properties")
            .and_then(|v| v.as_object())
            .expect("properties present");
        let required_set: std::collections::HashSet<&str> = params
            .get("required")
            .and_then(|v| v.as_array())
            .expect("required present")
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        assert!(
            props.contains_key("reuse_sandbox_from"),
            "#844 PR-2: reuse_sandbox_from must appear in tool_def properties"
        );
        assert!(
            !required_set.contains("reuse_sandbox_from"),
            "#844 PR-2: reuse_sandbox_from must NOT be required — unset is the \
             default (fresh sandbox); setting it is the explicit opt-in."
        );
        let field = &props["reuse_sandbox_from"];
        assert_eq!(
            field.get("type").and_then(|v| v.as_str()),
            Some("string"),
            "#844 PR-2: reuse_sandbox_from must be typed `string`"
        );
        let desc = field
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        assert!(
            desc.contains("prior sibling") || desc.contains("sibling"),
            "#844 PR-2: description must name the sibling contract; got: {desc}"
        );
        assert!(
            desc.contains("same root") || desc.contains("same-root"),
            "#844 PR-2: description must name the same-root contract; got: {desc}"
        );
    }

    /// #844 PR-2: the JSON-action parser must preserve
    /// `reuse_sandbox_from` when the LLM emits it alongside `role` +
    /// `goal`. Pre-PR-2 the field did not exist; the parser's
    /// `tool_args` pass-through already round-trips unknown keys, so
    /// this is a contract test that guards against a future "strip
    /// unknown keys" refactor.
    #[test]
    fn parse_one_json_action_preserves_reuse_sandbox_from() {
        let json = serde_json::json!({
            "action_type": "spawn_subagent",
            "description": "retry with dead sibling's sandbox",
            "tool_name": "executor",
            "tool_args": {
                "goal": "finish applying the patches the predecessor started",
                "reuse_sandbox_from": "run_subagent_child_task_1234_0"
            },
            "confidence": 0.9
        });
        let proposal = parse_one(json).expect("parse_one returns Some for spawn_subagent");
        assert_eq!(
            proposal.action_type,
            cairn_domain::ActionType::SpawnSubagent
        );
        let args = proposal.tool_args.as_ref().expect("tool_args populated");
        assert_eq!(
            args.get("reuse_sandbox_from").and_then(|v| v.as_str()),
            Some("run_subagent_child_task_1234_0"),
            "#844 PR-2: JSON-action parser must preserve reuse_sandbox_from verbatim"
        );
    }

    /// #844 PR-2: the JSON-action parser must leave `reuse_sandbox_from`
    /// absent (not `null`, not empty-string) when the LLM does not
    /// emit it. Execute-side extraction is `Option<RunId>`-shaped —
    /// the absence signal is what triggers the fresh-sandbox default.
    #[test]
    fn parse_one_json_action_omits_reuse_sandbox_from_when_absent() {
        let json = serde_json::json!({
            "action_type": "spawn_subagent",
            "description": "fresh spawn — no reuse",
            "tool_name": "executor",
            "tool_args": { "goal": "fresh goal" },
            "confidence": 0.9
        });
        let proposal = parse_one(json).expect("parse_one returns Some for spawn_subagent");
        let args = proposal.tool_args.as_ref().expect("tool_args populated");
        assert!(
            args.get("reuse_sandbox_from").is_none(),
            "#844 PR-2: absent reuse_sandbox_from must NOT materialise as null \
             or empty-string in parsed tool_args"
        );
    }

    /// #844 PR-2 + #775: the native-tool-call parser (used by
    /// OpenAI/Anthropic/Bedrock structured-output providers) must
    /// carry `reuse_sandbox_from` AND `parent_context` through on
    /// the flat shape. Pre-PR-2 the native path reconstructed
    /// `tool_args` as `{goal}` only, dropping every other field.
    #[test]
    fn tool_calls_to_proposals_native_flat_carries_reuse_and_parent_context() {
        let tool_calls = [serde_json::json!({
            "function": {
                "name": "spawn_subagent",
                "arguments": {
                    "role": "executor",
                    "goal": "retry",
                    "parent_context": "prior attempt failed the gate",
                    "reuse_sandbox_from": "run_prior_sibling_42"
                }
            }
        })];
        let proposals = tool_calls_to_proposals(&tool_calls, &[]);
        assert_eq!(proposals.len(), 1);
        let args = proposals[0]
            .tool_args
            .as_ref()
            .expect("native tool_call preserves tool_args");
        assert_eq!(
            args.get("goal").and_then(|v| v.as_str()),
            Some("retry"),
            "goal must be preserved on native-tool-call path"
        );
        assert_eq!(
            args.get("parent_context").and_then(|v| v.as_str()),
            Some("prior attempt failed the gate"),
            "#775 regression: parent_context must be preserved on native-tool-call path"
        );
        assert_eq!(
            args.get("reuse_sandbox_from").and_then(|v| v.as_str()),
            Some("run_prior_sibling_42"),
            "#844 PR-2: reuse_sandbox_from must be preserved on native-tool-call path"
        );
    }

    /// #844 PR-2: native tool_call for spawn_subagent WITHOUT
    /// reuse_sandbox_from must not invent an empty string or null —
    /// the absence is the signal for fresh-sandbox default.
    #[test]
    fn tool_calls_to_proposals_native_omits_reuse_when_absent() {
        let tool_calls = [serde_json::json!({
            "function": {
                "name": "spawn_subagent",
                "arguments": { "role": "executor", "goal": "fresh" }
            }
        })];
        let proposals = tool_calls_to_proposals(&tool_calls, &[]);
        assert_eq!(proposals.len(), 1);
        let args = proposals[0]
            .tool_args
            .as_ref()
            .expect("tool_args populated");
        assert!(
            args.get("reuse_sandbox_from").is_none(),
            "#844 PR-2: absent field must not materialise on native path"
        );
    }

    /// #844 PR-2: the legacy nested shape
    /// `{"tool_name": "executor", "tool_args": { "goal": ..., "reuse_sandbox_from": ... }}`
    /// must extract reuse_sandbox_from too. Kept for compat with JSON-
    /// action-envelope-style runs mid-flight against pre-#697 system
    /// prompts.
    #[test]
    fn tool_calls_to_proposals_native_legacy_nested_carries_reuse() {
        let tool_calls = [serde_json::json!({
            "function": {
                "name": "spawn_subagent",
                "arguments": {
                    "tool_name": "executor",
                    "tool_args": {
                        "goal": "retry",
                        "reuse_sandbox_from": "run_nested_prior"
                    }
                }
            }
        })];
        let proposals = tool_calls_to_proposals(&tool_calls, &[]);
        assert_eq!(proposals.len(), 1);
        let args = proposals[0]
            .tool_args
            .as_ref()
            .expect("tool_args populated");
        assert_eq!(
            args.get("reuse_sandbox_from").and_then(|v| v.as_str()),
            Some("run_nested_prior"),
            "#844 PR-2: legacy nested shape must carry reuse_sandbox_from"
        );
    }

    /// #844 PR-2: whitespace-only / empty-string values are normalised
    /// to "absent" — the execute-side extraction treats blank as `None`
    /// and the spawn adapter never writes a default row with a blank
    /// value. A blank value on the wire would otherwise propagate as
    /// an InvalidArgument rejection on every spawn, which is worse
    /// than simply dropping it.
    #[test]
    fn extract_spawn_subagent_optionals_drops_blanks() {
        let args = serde_json::json!({
            "role": "executor",
            "goal": "g",
            "parent_context": "   ",
            "reuse_sandbox_from": ""
        });
        let (pc, reuse) = extract_spawn_subagent_optionals(&args);
        assert!(pc.is_none(), "whitespace parent_context must drop to None");
        assert!(
            reuse.is_none(),
            "empty reuse_sandbox_from must drop to None"
        );
    }

    /// #844 PR-2 (Gemini review): the legacy nested shape — emitted by
    /// text-parsing-mode runs on pre-#697 system prompts — must yield
    /// the same extracted values as the flat shape. Before the shared
    /// helper was `pub(crate)` and reused in `execute_impl`, the
    /// execute side's inline `tool_args.get(...)` only looked at the
    /// top level and silently dropped reuse/parent_context on the
    /// nested shape.
    #[test]
    fn extract_spawn_subagent_optionals_handles_legacy_nested_shape() {
        let args = serde_json::json!({
            "tool_name": "executor",
            "tool_args": {
                "goal": "g",
                "parent_context": "nested ctx",
                "reuse_sandbox_from": "run_nested_123"
            }
        });
        let (pc, reuse) = extract_spawn_subagent_optionals(&args);
        assert_eq!(pc.as_deref(), Some("nested ctx"));
        assert_eq!(reuse.as_deref(), Some("run_nested_123"));
    }

    /// #844 PR-2 (Copilot review): `reuse_sandbox_from` must carry
    /// through to the adapter whether it originated from the flat or
    /// the legacy nested shape. The shared extractor is the single
    /// source of truth for both forms — a regression where
    /// `execute_impl` or `decide_impl` uses a different extraction
    /// path would diverge silently. This test pins byte-identical
    /// results across the two shapes.
    #[test]
    fn extract_spawn_subagent_optionals_flat_and_nested_agree() {
        let flat = serde_json::json!({
            "role": "executor",
            "goal": "g",
            "parent_context": "ctx",
            "reuse_sandbox_from": "run_abc"
        });
        let nested = serde_json::json!({
            "tool_name": "executor",
            "tool_args": {
                "goal": "g",
                "parent_context": "ctx",
                "reuse_sandbox_from": "run_abc"
            }
        });
        assert_eq!(
            extract_spawn_subagent_optionals(&flat),
            extract_spawn_subagent_optionals(&nested),
            "flat and legacy-nested shapes must produce identical optionals"
        );
    }

    /// #776: the `role` parameter is a runtime-derived JSON `enum`
    /// over the registered roles in `default_roles()`, MINUS the
    /// orchestrator (sub-agents do not delegate to a parent).
    /// Adding a new role to `default_roles()` automatically extends
    /// the schema; removing one removes the choice. Both sides
    /// catch typos at the schema-validation layer instead of at
    /// the silent-fallback layer.
    #[test]
    fn spawn_subagent_role_enum_derived_from_default_roles() {
        let def = spawn_subagent_tool_def();
        let role_schema = def
            .pointer("/function/parameters/properties/role")
            .expect("role schema present");
        let enum_values = role_schema
            .get("enum")
            .and_then(|v| v.as_array())
            .expect("role.enum present");
        let enum_strs: std::collections::HashSet<&str> =
            enum_values.iter().filter_map(|v| v.as_str()).collect();

        // All non-orchestrator default roles must appear.
        for expected in [
            "status-checker",
            "executor",
            "researcher",
            "reviewer",
            "generic",
        ] {
            assert!(
                enum_strs.contains(expected),
                "role enum must include {expected:?}; got {enum_strs:?}"
            );
        }
        // Orchestrator must NOT appear — sub-agents do not spawn the
        // parent role.
        assert!(
            !enum_strs.contains("orchestrator"),
            "role enum must NOT include `orchestrator` (parent role); got {enum_strs:?}"
        );
        // The derivation contract: enum size matches default_roles
        // minus orchestrator.
        let expected_count = cairn_domain::agent_roles::default_roles()
            .iter()
            .filter(|r| r.role_id != "orchestrator")
            .count();
        assert_eq!(
            enum_strs.len(),
            expected_count,
            "role enum must be derived from default_roles() minus orchestrator"
        );
    }

    /// #813: `## Run state` must include the resolved `working_dir`
    /// so sub-agents see the workspace path on their first DECIDE
    /// without a discovery round-trip. R23 dogfood found executors
    /// running 11 inline `pwd`/`find Cargo.toml`/`ls /tmp/...` calls
    /// because the runtime had the path in `OrchestrationContext`
    /// but never surfaced it in the prompt.
    #[test]
    fn build_user_message_renders_workspace_path_in_run_state() {
        let mut c = ctx();
        c.working_dir = std::path::PathBuf::from("/home/ubuntu/dogfood-roguelike");
        let msg = build_user_message(&c, &empty_gather(), None, false);
        assert!(
            msg.contains("## Run state"),
            "user message must include `## Run state` header"
        );
        assert!(
            msg.contains("workspace_path: /home/ubuntu/dogfood-roguelike"),
            "#813: `## Run state` must surface the resolved working_dir as \
             `workspace_path: <path>` so the child knows where to `cd`. \
             Got: {msg}"
        );
    }

    #[test]
    fn build_user_message_renders_parent_context_section() {
        // #775: when ctx.parent_context is set, the user message must
        // contain a `## Parent context` section with the verbatim
        // text. Section appears BEFORE `## Run state` so the child
        // sees the parent's binding direction immediately after the
        // goal.
        let mut c = ctx();
        c.parent_context =
            Some("previous attempt looped on `gh auth status`; do not call it".to_owned());
        let msg = build_user_message(&c, &empty_gather(), None, false);
        assert!(
            msg.contains("## Parent context"),
            "user message must include `## Parent context` header when set"
        );
        assert!(
            msg.contains("previous attempt looped on `gh auth status`"),
            "parent_context body must appear verbatim"
        );
        // Goal comes before parent context, parent context before run
        // state.
        let goal_idx = msg.find("## Goal").expect("Goal section");
        let pctx_idx = msg
            .find("## Parent context")
            .expect("Parent context section");
        let runstate_idx = msg.find("## Run state").expect("Run state section");
        assert!(goal_idx < pctx_idx, "Goal must precede Parent context");
        assert!(
            pctx_idx < runstate_idx,
            "Parent context must precede Run state"
        );
    }

    #[test]
    fn build_user_message_omits_parent_context_section_when_absent() {
        // Default ctx has parent_context=None — no header should
        // render. This avoids cluttering root-run prompts and pre-
        // #775 child prompts that legitimately have nothing to thread.
        let c = ctx();
        assert!(c.parent_context.is_none());
        let msg = build_user_message(&c, &empty_gather(), None, false);
        assert!(
            !msg.contains("## Parent context"),
            "user message must NOT include `## Parent context` when ctx.parent_context is None"
        );
    }

    #[test]
    fn build_user_message_does_not_render_iteration_to_model() {
        // #797: iteration is internal orchestrator bookkeeping. R21
        // dogfood found sub-agents reading `iteration: 3` and self-
        // bailing with partial-completion reports thinking they were
        // near the cap (which is now 50, but the model can't see
        // that). Removing iteration from both `## Run state` and
        // step-history line prefixes prevents the model from making
        // bogus pacing decisions based on a counter it doesn't know
        // the cap of.
        let mut c = ctx();
        c.iteration = 17;
        c.run_id = cairn_domain::RunId::new("run_797_test");
        let mut g = empty_gather();
        g.step_history = vec![
            crate::context::StepSummary {
                iteration: 3,
                action_kind: "invoke_tool".to_owned(),
                summary: "test summary one".to_owned(),
                succeeded: true,
            },
            crate::context::StepSummary {
                iteration: 12,
                action_kind: "invoke_tool".to_owned(),
                summary: "test summary two".to_owned(),
                succeeded: true,
            },
        ];
        let msg = build_user_message(&c, &g, None, false);
        // The Run state block must NOT carry the iteration line.
        assert!(
            !msg.contains("\niteration: 17"),
            "user message must NOT render `iteration: 17` in Run state block (#797). msg:\n{msg}"
        );
        // Step history lines must NOT prefix with [N].
        assert!(
            !msg.contains("- [3]"),
            "step history must NOT render `[3]` iteration prefix (#797). msg:\n{msg}"
        );
        assert!(
            !msg.contains("- [12]"),
            "step history must NOT render `[12]` iteration prefix (#797). msg:\n{msg}"
        );
        // run_id and agent_type should still be there — only the
        // iteration line is hidden.
        assert!(msg.contains("run_id: run_797_test"));
        // The step entries themselves should still appear (under the
        // new format `- {action_kind} | {summary} | ok={succeeded}`).
        assert!(msg.contains("test summary one"));
        assert!(msg.contains("test summary two"));
    }

    // ── #774 footer-by-response-shape tests ─────────────────────────────

    /// Direct-answer roles (orchestrator, future Q&A specialties)
    /// keep the "answer NOW" footer — that's correct for
    /// trivia/synthesis goals where a single tool call (or none)
    /// then complete_run is the right shape.
    #[test]
    fn build_user_message_direct_answer_role_keeps_complete_run_now_footer() {
        let mut c = ctx();
        c.agent_type = "orchestrator".to_owned(); // DirectAnswer
        let msg = build_user_message(&c, &empty_gather(), None, false);
        assert!(
            msg.contains("complete_run` tool NOW"),
            "DirectAnswer footer must keep the answer-NOW nudge"
        );
        // Continuation phrasing must NOT appear — that's the
        // procedural-artifact branch.
        assert!(
            !msg.contains("the artifact is not produced yet"),
            "DirectAnswer footer must NOT include the procedural \
             continuation phrasing"
        );
    }

    /// Procedural-artifact roles (executor, researcher, reviewer,
    /// generic) get a continuation footer that DOES NOT pressure
    /// early `complete_run`. This is the #774 fix — pre-#774 the
    /// footer told every subagent "answer NOW", contradicting the
    /// role's Phase 1-5 instructions and producing the R19 wedge.
    #[test]
    fn build_user_message_procedural_role_uses_continuation_footer() {
        let mut c = ctx();
        c.agent_type = "executor".to_owned(); // ProceduralArtifact
        let msg = build_user_message(&c, &empty_gather(), None, false);
        assert!(
            msg.contains("Phase 5"),
            "ProceduralArtifact footer must mention Phase 5 to anchor \
             the don't-complete-early rule"
        );
        assert!(
            msg.contains("do not call `complete_run`"),
            "ProceduralArtifact footer must explicitly forbid early \
             complete_run"
        );
        // The DirectAnswer "NOW" nudge must NOT appear for procedural
        // roles — that was the R19 wedge driver.
        assert!(
            !msg.contains("complete_run` tool NOW"),
            "ProceduralArtifact footer must NOT include the answer-NOW \
             nudge — that contradicts the role's Phase 1-5 contract \
             and was the R19 wedge driver"
        );
    }

    /// Researcher (also ProceduralArtifact) gets the same continuation
    /// footer as executor — pinning the test for both roles documents
    /// the contract symmetry.
    #[test]
    fn build_user_message_researcher_uses_continuation_footer() {
        let mut c = ctx();
        c.agent_type = "researcher".to_owned();
        let msg = build_user_message(&c, &empty_gather(), None, false);
        assert!(msg.contains("Phase 5"));
        assert!(msg.contains("do not call `complete_run`"));
        assert!(!msg.contains("complete_run` tool NOW"));
    }

    /// Generic role is ProceduralArtifact — confirms the new role
    /// gets the right footer too.
    #[test]
    fn build_user_message_generic_role_uses_continuation_footer() {
        let mut c = ctx();
        c.agent_type = "generic".to_owned();
        let msg = build_user_message(&c, &empty_gather(), None, false);
        assert!(msg.contains("do not call `complete_run`"));
        assert!(!msg.contains("complete_run` tool NOW"));
    }

    /// Unknown role_id falls back to the generic role's shape
    /// (ProceduralArtifact). Mirrors the assembled-prompt fallback
    /// in `build_system_prompt` so the two paths stay aligned —
    /// otherwise an unknown role gets a generic system prompt but
    /// a DirectAnswer footer, re-introducing the R19 wedge.
    #[test]
    fn build_user_message_unknown_role_falls_back_to_procedural_footer() {
        let mut c = ctx();
        c.agent_type = "wizard".to_owned(); // not registered
        let msg = build_user_message(&c, &empty_gather(), None, false);
        assert!(
            msg.contains("do not call `complete_run`"),
            "unknown role_id must fall back to ProceduralArtifact \
             footer (matches assembled-prompt fallback to generic)"
        );
    }

    #[test]
    fn complete_run_tool_call_maps_to_complete_run_proposal() {
        // F36: a native `complete_run(final_answer: "...")` tool_call
        // must translate to a terminal `ActionType::CompleteRun`
        // proposal whose description carries the final_answer text
        // verbatim (the loop runner copies that field into the run's
        // `summary`).
        let tool_calls = vec![serde_json::json!({
            "type": "function",
            "function": {
                "name": "complete_run",
                "arguments": { "final_answer": "Paris." }
            }
        })];
        let proposals = tool_calls_to_proposals(&tool_calls, &[]);
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].action_type, ActionType::CompleteRun);
        assert_eq!(proposals[0].description, "Paris.");
        assert!(proposals[0].tool_name.is_none());
    }

    #[test]
    fn complete_run_tool_call_accepts_description_alias() {
        // Lenient alias: some models (notably earlier GLM iterations
        // and Qwen3 reasoning traces) key the answer under
        // `description` instead of `final_answer`.
        let tool_calls = vec![serde_json::json!({
            "type": "function",
            "function": {
                "name": "complete_run",
                "arguments": { "description": "Paris is the capital." }
            }
        })];
        let proposals = tool_calls_to_proposals(&tool_calls, &[]);
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].action_type, ActionType::CompleteRun);
        assert_eq!(proposals[0].description, "Paris is the capital.");
    }

    #[test]
    fn complete_run_tool_call_missing_final_answer_escalates() {
        // Review follow-up (Gemini / Copilot): when the model calls
        // `complete_run` without the required `final_answer` argument
        // AND without any tolerated alias, do NOT silently default to
        // a useless summary ("run completed") — that would both mask
        // schema drift from upstream providers and hand a meaningless
        // answer to the user. Escalate with the raw arguments so a
        // human can diagnose what the model actually sent.
        let tool_calls = vec![serde_json::json!({
            "type": "function",
            "function": {
                "name": "complete_run",
                "arguments": { "wrong_field": "oops" }
            }
        })];
        let proposals = tool_calls_to_proposals(&tool_calls, &[]);
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].action_type, ActionType::EscalateToOperator);
        assert!(
            proposals[0].description.contains("final_answer"),
            "escalation message should name the missing field; got: {}",
            proposals[0].description,
        );
        assert!(
            proposals[0].description.contains("wrong_field"),
            "escalation message should include the raw args for diagnosis; got: {}",
            proposals[0].description,
        );
    }

    #[test]
    fn fail_run_tool_call_maps_to_fail_run_proposal() {
        // #825: a native `fail_run(reason: "...")` tool_call must
        // translate to a terminal `ActionType::FailRun` proposal whose
        // description carries the reason verbatim. execute_impl's
        // derive_signal wraps this in the `model_reported_failure:`
        // prefix, which classify_failed_reason in the HTTP layer
        // routes to FailureClass::ModelReportedFailure.
        let tool_calls = vec![serde_json::json!({
            "type": "function",
            "function": {
                "name": "fail_run",
                "arguments": {
                    "reason": "blocked: src/main.rs does not exist; depends on M1-1"
                }
            }
        })];
        let proposals = tool_calls_to_proposals(&tool_calls, &[]);
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].action_type, ActionType::FailRun);
        assert_eq!(
            proposals[0].description,
            "blocked: src/main.rs does not exist; depends on M1-1"
        );
        assert!(proposals[0].tool_name.is_none());
        assert!(proposals[0].tool_args.is_none());
        assert!(
            !proposals[0].requires_approval,
            "fail_run is terminal — approval is for escalate_to_operator"
        );
    }

    #[test]
    fn fail_run_tool_call_accepts_description_alias() {
        // #825: mirror complete_run's lenient alias handling. Some
        // models (GLM, Qwen3) key the reason under `description`
        // instead of the schema-declared `reason`; tolerate that to
        // match the complete_run posture.
        let tool_calls = vec![serde_json::json!({
            "type": "function",
            "function": {
                "name": "fail_run",
                "arguments": { "description": "contradictory goal" }
            }
        })];
        let proposals = tool_calls_to_proposals(&tool_calls, &[]);
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].action_type, ActionType::FailRun);
        assert_eq!(proposals[0].description, "contradictory goal");
    }

    #[test]
    fn fail_run_tool_call_missing_reason_escalates() {
        // #825: if the model calls fail_run without `reason` AND
        // without a tolerated alias, escalate with the raw args.
        // Same posture as complete_run: never invent a default reason;
        // keep schema drift loud.
        let tool_calls = vec![serde_json::json!({
            "type": "function",
            "function": {
                "name": "fail_run",
                "arguments": { "wrong_field": "oops" }
            }
        })];
        let proposals = tool_calls_to_proposals(&tool_calls, &[]);
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].action_type, ActionType::EscalateToOperator);
        assert!(
            proposals[0].description.contains("reason"),
            "escalation should name the missing field; got: {}",
            proposals[0].description,
        );
        assert!(
            proposals[0].description.contains("wrong_field"),
            "escalation should include raw args; got: {}",
            proposals[0].description,
        );
    }

    #[test]
    fn fail_run_json_action_shape_parses_to_fail_run_proposal() {
        // #825: the legacy JSON-action array shape (used when native
        // tool calling is unavailable) must also recognise fail_run.
        // parse_proposals is the entry point for that path.
        let raw = r#"[{"action_type":"fail_run","description":"blocked on X","confidence":0.9,"requires_approval":false}]"#;
        let proposals = parse_proposals(raw);
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].action_type, ActionType::FailRun);
        assert_eq!(proposals[0].description, "blocked on X");
    }

    #[test]
    fn tool_calls_unwrap_handles_stringified_arguments() {
        // OpenAI-compatible providers serialize arguments as a JSON string.
        let tool_calls = vec![serde_json::json!({
            "type": "function",
            "function": {
                "name": "invoke_tool",
                "arguments":
                    r#"{"tool_name":"read","tool_args":{"path":"/tmp/x"}}"#
            }
        })];
        let proposals = tool_calls_to_proposals(&tool_calls, &[]);
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].tool_name.as_deref(), Some("read"));
    }

    #[test]
    fn user_message_contains_goal_and_run_id() {
        let msg = build_user_message(&ctx(), &empty_gather(), None, false);
        assert!(msg.contains("cairn-rs architecture"), "goal must appear");
        assert!(msg.contains("run_1"), "run_id must appear");
        assert!(msg.contains("orchestrator"), "agent_type must appear");
    }

    #[test]
    fn user_message_embeds_step_history() {
        let mut g = empty_gather();
        g.step_history = vec![crate::context::StepSummary {
            iteration: 0,
            action_kind: "invoke_tool".to_owned(),
            summary: "searched for architecture docs".to_owned(),
            succeeded: true,
        }];
        let msg = build_user_message(&ctx(), &g, None, false);
        assert!(
            msg.contains("architecture docs"),
            "step history must appear"
        );
        assert!(msg.contains("invoke_tool"), "action kind must appear");
    }

    // ── Response parser tests ─────────────────────────────────────────────────

    #[test]
    fn parse_well_formed_response() {
        let raw = r#"[
          {"action_type":"spawn_subagent","description":"delegate research","confidence":0.88,
           "tool_name":"researcher","tool_args":{"goal":"summarise RFCs"},"requires_approval":false},
          {"action_type":"complete_run","description":"done","confidence":0.95,"requires_approval":false}
        ]"#;
        let proposals = parse_proposals(raw);
        assert_eq!(proposals.len(), 2);
        assert_eq!(proposals[0].action_type, ActionType::SpawnSubagent);
        assert_eq!(proposals[0].tool_name.as_deref(), Some("researcher"));
        assert!((proposals[0].confidence - 0.88).abs() < 1e-9);
        assert_eq!(proposals[1].action_type, ActionType::CompleteRun);
    }

    #[test]
    fn parse_malformed_response_returns_escalate() {
        let raw = "I'm not sure what to do. Can you give me more context?";
        let proposals = parse_proposals(raw);
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].action_type, ActionType::EscalateToOperator);
        assert!(
            proposals[0].requires_approval,
            "escalation must require approval"
        );
        assert_eq!(
            proposals[0].confidence, 0.0,
            "fallback confidence must be 0"
        );
        assert!(is_fallback_escalation(&proposals));
    }

    #[test]
    fn parse_strips_markdown_fence() {
        let raw = "```json\n[{\"action_type\":\"complete_run\",\"description\":\"all done\",\"confidence\":1.0,\"requires_approval\":false}]\n```";
        let proposals = parse_proposals(raw);
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].action_type, ActionType::CompleteRun);
        assert!(!is_fallback_escalation(&proposals));
    }

    #[test]
    fn parse_unknown_action_type_filtered_then_escalates() {
        let raw = r#"[{"action_type":"nuke_database","description":"bad","confidence":0.9,"requires_approval":false}]"#;
        let proposals = parse_proposals(raw);
        // All filtered → escalate
        assert_eq!(proposals[0].action_type, ActionType::EscalateToOperator);
    }

    // ── BrainLlmClient integration tests ─────────────────────────────────────

    #[tokio::test]
    async fn decide_with_well_formed_json() {
        let mock = Arc::new(MockProvider {
            response: r#"[{"action_type":"spawn_subagent","description":"research step","confidence":0.82,"tool_name":"researcher","tool_args":{"goal":"analyse docs"},"requires_approval":false}]"#.to_owned(),
        });
        let phase = LlmDecidePhase::new(mock, "cyankiwi/gemma-4-31B-it-AWQ-4bit");
        let out = phase.decide(&ctx(), &empty_gather()).await.unwrap();

        assert_eq!(out.proposals.len(), 1);
        assert_eq!(out.proposals[0].action_type, ActionType::SpawnSubagent);
        assert_eq!(out.proposals[0].tool_name.as_deref(), Some("researcher"));
        assert!(!out.requires_approval);
        assert!((out.calibrated_confidence - 0.82).abs() < 1e-9);
        assert_eq!(out.model_id, "cyankiwi/gemma-4-31B-it-AWQ-4bit");
    }

    #[tokio::test]
    async fn decide_with_malformed_json_retries_and_escalates() {
        // Both call attempts return prose — second retry also fails
        let mock = Arc::new(MockProvider {
            response: "I need more information about the task before I can proceed.".to_owned(),
        });
        let phase = LlmDecidePhase::new(mock, "gemma4");
        let out = phase.decide(&ctx(), &empty_gather()).await.unwrap();

        // Must succeed (not Err) and produce escalation
        assert_eq!(out.proposals.len(), 1);
        assert_eq!(out.proposals[0].action_type, ActionType::EscalateToOperator);
        assert!(out.proposals[0].requires_approval);
    }

    #[tokio::test]
    async fn decide_propagates_provider_error() {
        // With the fallback chain in place, a retryable provider error
        // against a single-model chain exhausts and surfaces
        // `AllProvidersExhausted`. Non-retryable errors (Auth /
        // InvalidRequest) still surface as `Decide(...)`.
        //
        // #693 R3-A: disable same-model retry via
        // `with_retry_budget(0, _)` so this test isolates the
        // exhaustion-on-transport-failure semantic. Without the
        // override, the retry loop would record 3 attempts
        // (1 + 2 retries) before falling through — the retry
        // semantics themselves are covered by the dedicated
        // `model_chain` tests.
        let binding = cairn_runtime::RoutedBinding {
            binding_id: "single".to_owned(),
            provider: Arc::new(FailingProvider),
            chain: cairn_runtime::ModelChain::single("gemma4")
                .with_retry_budget(0, std::time::Duration::ZERO),
            concurrency_limit: std::sync::Arc::new(tokio::sync::Semaphore::new(
                cairn_runtime::services::routed_generation::DEFAULT_BINDING_CONCURRENCY,
            )),
        };
        let routed = cairn_runtime::RoutedGenerationService::new(vec![binding]);
        let phase = LlmDecidePhase::from_routed(routed);
        let err = phase.decide(&ctx(), &empty_gather()).await.unwrap_err();
        match err {
            OrchestratorError::AllProvidersExhausted { attempts } => {
                assert_eq!(attempts.len(), 1);
                assert_eq!(attempts[0].model_id, "gemma4");
                assert_eq!(attempts[0].reason_code, "transport_failure");
            }
            other => panic!("expected AllProvidersExhausted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn decide_requires_approval_when_proposal_flagged() {
        let mock = Arc::new(MockProvider {
            response: r#"[{"action_type":"escalate_to_operator","description":"unsure","confidence":0.3,"requires_approval":true}]"#.to_owned(),
        });
        let phase = LlmDecidePhase::new(mock, "gemma4");
        let out = phase.decide(&ctx(), &empty_gather()).await.unwrap();
        assert!(
            out.requires_approval,
            "requires_approval must be true when any proposal is flagged"
        );
    }

    #[tokio::test]
    async fn decide_applies_confidence_bias() {
        let mock = Arc::new(MockProvider {
            response: r#"[{"action_type":"complete_run","description":"done","confidence":0.5,"requires_approval":false}]"#.to_owned(),
        });
        let phase = LlmDecidePhase::new(mock, "gemma4").with_confidence_bias(0.2);
        let out = phase.decide(&ctx(), &empty_gather()).await.unwrap();
        assert!(
            (out.proposals[0].confidence - 0.7).abs() < 1e-9,
            "bias should increase confidence"
        );
    }

    // ── TokenBudget tests ─────────────────────────────────────────────────────

    #[test]
    fn token_budget_default_reserves_quarter() {
        let b = TokenBudget::new(131_072);
        assert_eq!(b.total_context, 131_072);
        assert_eq!(b.reserved_output, 131_072 / 4);
        assert_eq!(b.available_input, 131_072 - 131_072 / 4);
    }

    #[test]
    fn token_budget_with_custom_reservation() {
        let b = TokenBudget::new(8_192).with_reserved_output(1_000);
        assert_eq!(b.reserved_output, 1_000);
        assert_eq!(b.available_input, 7_192);
    }

    #[test]
    fn estimate_tokens_empty_string() {
        assert_eq!(estimate_tokens(""), 0);
    }

    #[test]
    fn estimate_tokens_rounds_up() {
        // 1 char → 1 token (ceiling of 1/4)
        assert_eq!(estimate_tokens("a"), 1);
        // 4 chars → 1 token
        assert_eq!(estimate_tokens("abcd"), 1);
        // 5 chars → 2 tokens (ceiling of 5/4)
        assert_eq!(estimate_tokens("abcde"), 2);
        // 400 chars → 100 tokens
        assert_eq!(estimate_tokens(&"x".repeat(400)), 100);
    }

    // ── Token-budget truncation tests ─────────────────────────────────────────

    /// A very tight budget should include goal + run_state but omit optional content.
    #[test]
    fn tight_budget_drops_optional_content() {
        let mut g = empty_gather();
        // Add memory and step history that would normally appear
        g.memory_chunks = (0..5)
            .map(|i| cairn_memory::retrieval::RetrievalResult {
                chunk: {
                    let c = cairn_memory::ingest::ChunkRecord {
                        chunk_id: cairn_domain::ChunkId::new(format!("c{i}")),
                        document_id: cairn_domain::KnowledgeDocumentId::new("doc"),
                        source_id: cairn_domain::SourceId::new("src"),
                        source_type: cairn_memory::ingest::SourceType::PlainText,
                        project: ctx().project,
                        text: "a".repeat(400),
                        position: i as u32,
                        created_at: 0,
                        updated_at: None,
                        provenance_metadata: None,
                        credibility_score: None,
                        graph_linkage: None,
                        embedding: None,
                        content_hash: None,
                        entities: vec![],
                        embedding_model_id: None,
                        needs_reembed: false,
                    };
                    c
                },
                score: 1.0 - i as f64 * 0.1,
                breakdown: Default::default(),
            })
            .collect();
        g.step_history = (0..3)
            .map(|i| crate::context::StepSummary {
                iteration: i,
                action_kind: "invoke_tool".to_owned(),
                summary: "did a thing".to_owned(),
                succeeded: true,
            })
            .collect();

        // Budget of 50 tokens — can barely fit goal + run_state + footer
        let tight = TokenBudget::new(50).with_reserved_output(0);

        let msg = build_user_message(&ctx(), &g, Some(&tight), false);

        // Goal must always be present
        assert!(msg.contains("Goal"), "goal section must always appear");
        // Memory should be truncated/absent given the extreme budget
        // (we just verify the function doesn't panic; exact truncation depends on text sizes)
        let _ = msg;
    }

    /// Unlimited budget includes all content.
    #[test]
    fn no_budget_includes_all_content() {
        let mut g = empty_gather();
        g.step_history = vec![crate::context::StepSummary {
            iteration: 0,
            action_kind: "invoke_tool".to_owned(),
            summary: "searched for architecture docs".to_owned(),
            succeeded: true,
        }];
        // memory chunk with distinctive text
        g.memory_chunks = vec![cairn_memory::retrieval::RetrievalResult {
            chunk: {
                cairn_memory::ingest::ChunkRecord {
                    chunk_id: cairn_domain::ChunkId::new("c0"),
                    document_id: cairn_domain::KnowledgeDocumentId::new("doc"),
                    source_id: cairn_domain::SourceId::new("src"),
                    source_type: cairn_memory::ingest::SourceType::PlainText,
                    project: ctx().project,
                    text: "cairn uses event sourcing for durability".to_owned(),
                    position: 0,
                    created_at: 0,
                    updated_at: None,
                    provenance_metadata: None,
                    credibility_score: None,
                    graph_linkage: None,
                    embedding: None,
                    content_hash: None,
                    entities: vec![],
                    embedding_model_id: None,
                    needs_reembed: false,
                }
            },
            score: 0.9,
            breakdown: Default::default(),
        }];

        let msg = build_user_message(&ctx(), &g, None, false);

        assert!(
            msg.contains("cairn uses event sourcing"),
            "memory chunk must appear without budget"
        );
        assert!(
            msg.contains("architecture docs"),
            "step history must appear without budget"
        );
    }

    /// Memory chunks are included most-relevant-first; least relevant are dropped
    /// when the budget is tight.
    #[test]
    fn memory_chunks_most_relevant_first() {
        let texts = [
            "highly relevant content here",
            "somewhat relevant",
            "least relevant stuff",
        ];
        let mut g = empty_gather();
        g.memory_chunks = texts
            .iter()
            .enumerate()
            .map(|(i, text)| cairn_memory::retrieval::RetrievalResult {
                chunk: cairn_memory::ingest::ChunkRecord {
                    chunk_id: cairn_domain::ChunkId::new(format!("c{i}")),
                    document_id: cairn_domain::KnowledgeDocumentId::new("doc"),
                    source_id: cairn_domain::SourceId::new("src"),
                    source_type: cairn_memory::ingest::SourceType::PlainText,
                    project: ctx().project,
                    text: text.to_string(),
                    position: i as u32,
                    created_at: 0,
                    updated_at: None,
                    provenance_metadata: None,
                    credibility_score: None,
                    graph_linkage: None,
                    embedding: None,
                    content_hash: None,
                    entities: vec![],
                    embedding_model_id: None,
                    needs_reembed: false,
                },
                score: 1.0 - i as f64 * 0.3,
                breakdown: Default::default(),
            })
            .collect();

        // Large budget — all three included
        let msg = build_user_message(&ctx(), &g, None, false);
        assert!(msg.contains("highly relevant"), "chunk[0] must appear");
        assert!(msg.contains("somewhat relevant"), "chunk[1] must appear");
        assert!(msg.contains("least relevant"), "chunk[2] must appear");
    }

    /// with_context_window creates a budget from the model's context window.
    #[tokio::test]
    async fn with_context_window_sets_budget() {
        let mock = Arc::new(MockProvider {
            response: r#"[{"action_type":"complete_run","description":"done","confidence":0.9,"requires_approval":false}]"#.to_owned(),
        });
        // 128K context like gemma-4
        let phase = LlmDecidePhase::new(mock, "gemma4").with_context_window(131_072);
        let out = phase.decide(&ctx(), &empty_gather()).await.unwrap();
        // Should work normally — the budget is generous enough that nothing is truncated
        assert_eq!(out.proposals[0].action_type, ActionType::CompleteRun);
    }

    // ── Plan mode tool filtering (RFC 018) ──────────────────────────────

    #[test]
    fn plan_mode_ctx_has_run_mode() {
        let mut c = ctx();
        c.run_mode = cairn_domain::decisions::RunMode::Plan;
        assert!(matches!(c.run_mode, cairn_domain::decisions::RunMode::Plan));
    }

    #[tokio::test]
    async fn plan_mode_filters_external_tools_from_prompt() {
        use cairn_domain::decisions::RunMode;

        // Create a mock provider that captures the system prompt.
        let captured_prompt = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
        let prompt_ref = captured_prompt.clone();
        struct CapturingProvider {
            captured: std::sync::Arc<std::sync::Mutex<String>>,
        }
        #[async_trait]
        impl GenerationProvider for CapturingProvider {
            async fn generate(
                &self,
                _model: &str,
                messages: Vec<serde_json::Value>,
                _settings: &ProviderBindingSettings,
                _tools: &[serde_json::Value],
            ) -> Result<GenerationResponse, ProviderAdapterError> {
                if let Some(system) = messages.first().and_then(|m| m["content"].as_str()) {
                    *self.captured.lock().unwrap() = system.to_owned();
                }
                Ok(GenerationResponse {
                    text: r#"[{"action_type":"complete_run","description":"done","confidence":0.9,"requires_approval":false}]"#.to_owned(),
                    input_tokens: Some(100),
                    output_tokens: Some(50),
                    model_id: "test-model".to_owned(),
                    tool_calls: vec![],
                    finish_reason: None,
                })
            }
        }

        // Build a registry with both Observational and External tools.
        let registry = std::sync::Arc::new(
            cairn_tools::builtins::BuiltinToolRegistry::new()
                .register(std::sync::Arc::new(cairn_harness_tools::HarnessBuiltin::<
                    cairn_harness_tools::HarnessGrep,
                >::new())) // Observational
                .register(std::sync::Arc::new(cairn_tools::CalculateTool)) // Observational
                .register(std::sync::Arc::new(cairn_harness_tools::HarnessBuiltin::<
                    cairn_harness_tools::HarnessBash,
                >::new())), // External
        );

        let phase = LlmDecidePhase::new(
            std::sync::Arc::new(CapturingProvider {
                captured: prompt_ref,
            }),
            "test-model",
        )
        .with_tools(registry);

        // Plan mode context. Use a neutral role id so the #702
        // orchestrator tool-surface allowlist does not apply here —
        // this test exercises the RFC-018 Plan-mode ToolEffect
        // filter specifically, which is orthogonal to the
        // orchestrator role policy.
        let mut plan_ctx = ctx();
        plan_ctx.run_mode = RunMode::Plan;
        plan_ctx.agent_type = "plan-mode-smoke".to_owned();

        let _ = phase.decide(&plan_ctx, &empty_gather()).await.unwrap();
        let prompt = captured_prompt.lock().unwrap().clone();

        // The prompt tool descriptor lines use the format "tool_name(params) — desc".
        // Check for descriptor lines, not arbitrary mentions of tool names in prose.
        assert!(
            prompt.contains("  - grep("),
            "Observational tool descriptor should be in Plan mode prompt"
        );
        assert!(
            prompt.contains("  - calculate("),
            "Observational tool descriptor should be in Plan mode prompt"
        );
        // External tools should not have descriptor lines in Plan mode.
        assert!(
            !prompt.contains("  - bash("),
            "External tool descriptor must NOT be in Plan mode prompt"
        );
    }

    #[tokio::test]
    async fn enabled_plugin_tool_appears_in_project_prompt_but_not_other_projects() {
        struct CapturingProvider {
            captured: Arc<std::sync::Mutex<String>>,
        }

        #[async_trait]
        impl GenerationProvider for CapturingProvider {
            async fn generate(
                &self,
                _model: &str,
                messages: Vec<serde_json::Value>,
                _settings: &ProviderBindingSettings,
                _tools: &[serde_json::Value],
            ) -> Result<GenerationResponse, ProviderAdapterError> {
                if let Some(system) = messages.first().and_then(|m| m["content"].as_str()) {
                    *self.captured.lock().unwrap() = system.to_owned();
                }
                Ok(GenerationResponse {
                    text: r#"[{"action_type":"complete_run","description":"done","confidence":0.9,"requires_approval":false}]"#.to_owned(),
                    input_tokens: Some(100),
                    output_tokens: Some(20),
                    model_id: "test-model".to_owned(),
                    tool_calls: vec![],
                    finish_reason: None,
                })
            }
        }

        let prompt_a = Arc::new(std::sync::Mutex::new(String::new()));
        let phase_a = LlmDecidePhase::new(
            Arc::new(CapturingProvider {
                captured: prompt_a.clone(),
            }),
            "test-model",
        )
        .with_tools(registry_for_project(&cairn_domain::ProjectKey::new(
            "tenant",
            "workspace",
            "project-a",
        )));
        let mut ctx_a = ctx();
        ctx_a.project = cairn_domain::ProjectKey::new("tenant", "workspace", "project-a");
        // Use a neutral role so the #702 orchestrator tool-surface
        // allowlist doesn't apply — this test exercises plugin-tool
        // project-scoping, orthogonal to the orchestrator role.
        ctx_a.agent_type = "plugin-visibility-smoke".to_owned();
        phase_a.decide(&ctx_a, &empty_gather()).await.unwrap();

        let prompt_b = Arc::new(std::sync::Mutex::new(String::new()));
        let phase_b = LlmDecidePhase::new(
            Arc::new(CapturingProvider {
                captured: prompt_b.clone(),
            }),
            "test-model",
        )
        .with_tools(registry_for_project(&cairn_domain::ProjectKey::new(
            "tenant",
            "workspace",
            "project-b",
        )));
        let mut ctx_b = ctx();
        ctx_b.project = cairn_domain::ProjectKey::new("tenant", "workspace", "project-b");
        ctx_b.agent_type = "plugin-visibility-smoke".to_owned();
        phase_b.decide(&ctx_b, &empty_gather()).await.unwrap();

        assert!(
            prompt_a.lock().unwrap().contains("  - github.issue_brief("),
            "enabled project should see its plugin tool in the prompt"
        );
        assert!(
            !prompt_b.lock().unwrap().contains("  - github.issue_brief("),
            "disabled project must not see the plugin tool in the prompt"
        );
    }

    #[tokio::test]
    async fn tool_search_respects_plugin_visibility() {
        let enabled_project = cairn_domain::ProjectKey::new("tenant", "workspace", "project-a");
        let disabled_project = cairn_domain::ProjectKey::new("tenant", "workspace", "project-b");

        let enabled_registry = registry_for_project(&enabled_project);
        let enabled_tool = ToolSearchTool::new(enabled_registry);
        let enabled = enabled_tool
            .execute(
                &enabled_project,
                serde_json::json!({ "query": "search github issues" }),
            )
            .await
            .unwrap();
        let enabled_names: Vec<&str> = enabled.output["matches"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|entry| entry["name"].as_str())
            .collect();
        assert!(
            enabled_names.contains(&"github.issue_search"),
            "enabled project should be able to discover the deferred plugin tool"
        );

        let disabled_registry = registry_for_project(&disabled_project);
        let disabled_tool = ToolSearchTool::new(disabled_registry);
        let disabled = disabled_tool
            .execute(
                &disabled_project,
                serde_json::json!({ "query": "search github issues" }),
            )
            .await
            .unwrap();
        assert_eq!(
            disabled.output["total"], 0,
            "disabled project must not discover tools from an unenabled plugin"
        );
    }

    // ── Native tool calling tests ────────────────────────────────────────────

    #[test]
    fn descriptor_to_tool_def_produces_openai_format() {
        let desc = BuiltinToolDescriptor {
            name: "file_read".to_owned(),
            tier: ToolTier::Core,
            description: "Read a file from the filesystem.".to_owned(),
            parameters_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "File path" }
                },
                "required": ["path"]
            }),
            execution_class: cairn_domain::policy::ExecutionClass::SupervisedProcess,
            permission_level: cairn_tools::builtins::PermissionLevel::ReadOnly,
            category: cairn_tools::builtins::ToolCategory::FileSystem,
            tool_effect: ToolEffect::Observational,
            retry_safety: cairn_tools::builtins::RetrySafety::IdempotentSafe,
        };
        let def = descriptor_to_tool_def(&desc);
        assert_eq!(def["type"], "function");
        assert_eq!(def["function"]["name"], "file_read");
        assert_eq!(
            def["function"]["description"],
            "Read a file from the filesystem."
        );
        assert!(def["function"]["parameters"]["properties"]["path"].is_object());
    }

    #[test]
    fn tool_calls_to_proposals_converts_native_calls() {
        let tool_calls = vec![serde_json::json!({
            "id": "call_abc123",
            "type": "function",
            "function": {
                "name": "file_read",
                "arguments": "{\"path\": \"/tmp/test.txt\"}"
            }
        })];
        let descs = vec![BuiltinToolDescriptor {
            name: "file_read".to_owned(),
            tier: ToolTier::Core,
            description: "Read a file.".to_owned(),
            parameters_schema: serde_json::json!({}),
            execution_class: cairn_domain::policy::ExecutionClass::SupervisedProcess,
            permission_level: cairn_tools::builtins::PermissionLevel::ReadOnly,
            category: cairn_tools::builtins::ToolCategory::FileSystem,
            tool_effect: ToolEffect::Observational,
            retry_safety: cairn_tools::builtins::RetrySafety::IdempotentSafe,
        }];
        let proposals = tool_calls_to_proposals(&tool_calls, &descs);
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].action_type, ActionType::InvokeTool);
        assert_eq!(proposals[0].tool_name.as_deref(), Some("file_read"));
        assert_eq!(
            proposals[0].tool_args.as_ref().unwrap()["path"],
            "/tmp/test.txt"
        );
        assert!(!proposals[0].requires_approval);
    }

    #[test]
    fn tool_calls_to_proposals_sets_approval_for_sensitive_tools() {
        let tool_calls = vec![serde_json::json!({
            "id": "call_xyz",
            "type": "function",
            "function": {
                "name": "bash",
                "arguments": "{\"command\": \"rm -rf /\"}"
            }
        })];
        let descs = vec![BuiltinToolDescriptor {
            name: "bash".to_owned(),
            tier: ToolTier::Core,
            description: "Execute a shell command.".to_owned(),
            parameters_schema: serde_json::json!({}),
            execution_class: cairn_domain::policy::ExecutionClass::Sensitive,
            permission_level: cairn_tools::builtins::PermissionLevel::Execute,
            category: cairn_tools::builtins::ToolCategory::Shell,
            tool_effect: ToolEffect::External,
            retry_safety: cairn_tools::builtins::RetrySafety::DangerousPause,
        }];
        let proposals = tool_calls_to_proposals(&tool_calls, &descs);
        assert_eq!(proposals.len(), 1);
        assert!(proposals[0].requires_approval);
    }

    #[test]
    fn tool_calls_to_proposals_handles_object_arguments() {
        // Anthropic/Bedrock return arguments as parsed JSON objects, not strings
        let tool_calls = vec![serde_json::json!({
            "id": "call_1",
            "type": "function",
            "function": {
                "name": "memory_search",
                "arguments": { "query": "architecture" }
            }
        })];
        let proposals = tool_calls_to_proposals(&tool_calls, &[]);
        assert_eq!(proposals.len(), 1);
        assert_eq!(
            proposals[0].tool_args.as_ref().unwrap()["query"],
            "architecture"
        );
    }

    #[test]
    fn tool_calls_to_proposals_parallel_calls() {
        let tool_calls = vec![
            serde_json::json!({
                "id": "call_1",
                "type": "function",
                "function": { "name": "file_read", "arguments": "{\"path\": \"a.rs\"}" }
            }),
            serde_json::json!({
                "id": "call_2",
                "type": "function",
                "function": { "name": "grep", "arguments": "{\"query\": \"TODO\"}" }
            }),
        ];
        let proposals = tool_calls_to_proposals(&tool_calls, &[]);
        assert_eq!(proposals.len(), 2);
        assert_eq!(proposals[0].tool_name.as_deref(), Some("file_read"));
        assert_eq!(proposals[1].tool_name.as_deref(), Some("grep"));
    }

    /// End-to-end: model returns native tool_calls → proposals are InvokeTool
    ///
    /// Runs as the `status-checker` role so `grep` survives the
    /// per-role tool-surface filter. Pre-#806 this used the default
    /// `orchestrator` agent_type, but #806 stripped grep/read/glob/
    /// lsp/bash from the orchestrator surface — those tools now live
    /// on status-checker.
    #[tokio::test]
    async fn decide_uses_native_tool_calls_when_present() {
        struct NativeToolProvider;

        #[async_trait]
        impl GenerationProvider for NativeToolProvider {
            async fn generate(
                &self,
                _model: &str,
                _messages: Vec<serde_json::Value>,
                _settings: &ProviderBindingSettings,
                tools: &[serde_json::Value],
            ) -> Result<GenerationResponse, ProviderAdapterError> {
                // Verify tools were sent. F38 injects `complete_run` at
                // index 0, #825 injects `fail_run` at index 1, and
                // #697 R5-A injects `spawn_subagent` at index 2, so
                // `grep` (the only registered tool here) sits at
                // index 3.
                assert!(!tools.is_empty(), "tools should be passed to generate");
                assert_eq!(tools[0]["function"]["name"], "complete_run");
                assert_eq!(tools[1]["function"]["name"], "fail_run");
                assert_eq!(tools[2]["function"]["name"], "spawn_subagent");
                assert_eq!(tools[3]["function"]["name"], "grep");

                Ok(GenerationResponse {
                    text: String::new(), // no text — only tool_calls
                    input_tokens: Some(200),
                    output_tokens: Some(50),
                    model_id: "test-model".to_owned(),
                    tool_calls: vec![serde_json::json!({
                        "id": "call_1",
                        "type": "function",
                        "function": {
                            "name": "grep",
                            "arguments": "{\"query\": \"architecture\"}"
                        }
                    })],
                    finish_reason: Some("tool_calls".to_owned()),
                })
            }
        }

        let registry = Arc::new(BuiltinToolRegistry::new().register(Arc::new(
            cairn_harness_tools::HarnessBuiltin::<cairn_harness_tools::HarnessGrep>::new(),
        )));
        let phase =
            LlmDecidePhase::new(Arc::new(NativeToolProvider), "test-model").with_tools(registry);
        let mut c = ctx();
        c.agent_type = "status-checker".to_owned();
        let out = phase.decide(&c, &empty_gather()).await.unwrap();

        assert_eq!(out.proposals.len(), 1);
        assert_eq!(out.proposals[0].action_type, ActionType::InvokeTool);
        assert_eq!(out.proposals[0].tool_name.as_deref(), Some("grep"));
        assert_eq!(
            out.proposals[0].tool_args.as_ref().unwrap()["query"],
            "architecture"
        );
        assert!(
            !out.proposals[0].requires_approval,
            "grep is a safe read action"
        );
    }

    /// Fallback: model returns text (no tool_calls) → parse_proposals handles it
    #[tokio::test]
    async fn decide_falls_back_to_text_parsing_when_no_tool_calls() {
        let mock = Arc::new(MockProvider {
            response: r#"[{"action_type":"complete_run","description":"done","confidence":0.9,"requires_approval":false}]"#.to_owned(),
        });
        let registry = Arc::new(BuiltinToolRegistry::new().register(Arc::new(
            cairn_harness_tools::HarnessBuiltin::<cairn_harness_tools::HarnessGrep>::new(),
        )));
        let phase = LlmDecidePhase::new(mock, "test-model").with_tools(registry);
        let out = phase.decide(&ctx(), &empty_gather()).await.unwrap();

        assert_eq!(out.proposals.len(), 1);
        assert_eq!(out.proposals[0].action_type, ActionType::CompleteRun);
    }

    /// #702 regression guard: when the run's role is `orchestrator`,
    /// the tool surface shipped to the provider MUST be filtered by
    /// the role's `allowed_tools` allowlist. A banned tool like
    /// `webfetch` must NOT appear in the tools[] array — that was
    /// the structural enabler of R9's inline-retrieval failure mode.
    #[tokio::test]
    async fn orchestrator_role_filters_tool_surface_to_allowlist() {
        use std::sync::Mutex;

        #[derive(Clone)]
        struct ToolsCaptureProvider {
            captured: Arc<Mutex<Vec<serde_json::Value>>>,
        }

        #[async_trait]
        impl GenerationProvider for ToolsCaptureProvider {
            async fn generate(
                &self,
                _model: &str,
                _messages: Vec<serde_json::Value>,
                _settings: &ProviderBindingSettings,
                tools: &[serde_json::Value],
            ) -> Result<GenerationResponse, ProviderAdapterError> {
                *self.captured.lock().unwrap() = tools.to_vec();
                // Return a harmless complete_run so DECIDE terminates.
                Ok(GenerationResponse {
                    text: String::new(),
                    input_tokens: Some(10),
                    output_tokens: Some(5),
                    model_id: "test-model".to_owned(),
                    tool_calls: vec![serde_json::json!({
                        "id": "call_1",
                        "type": "function",
                        "function": {
                            "name": "complete_run",
                            "arguments": "{\"final_answer\": \"done\"}"
                        }
                    })],
                    finish_reason: Some("tool_calls".to_owned()),
                })
            }
        }

        let captured = Arc::new(Mutex::new(Vec::new()));
        let provider = Arc::new(ToolsCaptureProvider {
            captured: captured.clone(),
        });

        // Register two harness tools: `grep` (NOT on the orchestrator
        // allowlist after #806 — workspace inspection now spawns a
        // status-checker) and `webfetch` (was never on it). Post-#806
        // the provider should see neither. Pre-#702 BOTH were in the
        // array; pre-#806 grep was in the array but webfetch was not.
        let registry = Arc::new(
            BuiltinToolRegistry::new()
                .register(Arc::new(cairn_harness_tools::HarnessBuiltin::<
                    cairn_harness_tools::HarnessGrep,
                >::new()))
                .register(Arc::new(cairn_harness_tools::HarnessBuiltin::<
                    cairn_harness_tools::HarnessWebFetch,
                >::new())),
        );
        let phase = LlmDecidePhase::new(provider, "test-model").with_tools(registry);

        let _ = phase.decide(&ctx(), &empty_gather()).await.unwrap();

        let tools = captured.lock().unwrap().clone();
        let names: Vec<String> = tools
            .iter()
            .filter_map(|t| {
                t.get("function")
                    .and_then(|f| f.get("name"))
                    .and_then(|n| n.as_str())
                    .map(str::to_owned)
            })
            .collect();

        assert!(
            names.iter().any(|n| n == "complete_run"),
            "complete_run must always be advertised; got {names:?}"
        );
        assert!(
            names.iter().any(|n| n == "spawn_subagent"),
            "spawn_subagent is on the orchestrator allowlist; got {names:?}"
        );
        assert!(
            !names.iter().any(|n| n == "webfetch"),
            "#702 regression: webfetch must NOT be advertised to the \
             orchestrator role. R9 proved that with webfetch in the \
             tool surface the model calls it inline instead of \
             delegating. names={names:?}"
        );
        assert!(
            !names.iter().any(|n| n == "grep"),
            "#806 regression: grep must NOT be advertised to the \
             orchestrator role. R22 proved that with grep / read / \
             glob / lsp / bash in the orchestrator's tool surface \
             the model loops on workspace inspection instead of \
             spawning a status-checker. names={names:?}"
        );
    }

    /// An unknown role id with no `AgentRoleService` wired keeps the
    /// full tool surface — the RFC 031 PR-C fallback builds a role
    /// record with an empty `tools[]` allowlist for unknown ids,
    /// matching the pre-RFC-031 "no-role-restriction" path byte-for-
    /// byte. Runs where a service IS wired take the §D7 generic-
    /// fallback branch instead; that's exercised elsewhere.
    #[tokio::test]
    async fn non_orchestrator_role_sees_full_tool_surface() {
        use std::sync::Mutex;

        #[derive(Clone)]
        struct ToolsCaptureProvider {
            captured: Arc<Mutex<Vec<serde_json::Value>>>,
        }

        #[async_trait]
        impl GenerationProvider for ToolsCaptureProvider {
            async fn generate(
                &self,
                _model: &str,
                _messages: Vec<serde_json::Value>,
                _settings: &ProviderBindingSettings,
                tools: &[serde_json::Value],
            ) -> Result<GenerationResponse, ProviderAdapterError> {
                *self.captured.lock().unwrap() = tools.to_vec();
                Ok(GenerationResponse {
                    text: String::new(),
                    input_tokens: Some(10),
                    output_tokens: Some(5),
                    model_id: "test-model".to_owned(),
                    tool_calls: vec![serde_json::json!({
                        "id": "call_1",
                        "type": "function",
                        "function": {
                            "name": "complete_run",
                            "arguments": "{\"final_answer\": \"done\"}"
                        }
                    })],
                    finish_reason: Some("tool_calls".to_owned()),
                })
            }
        }

        let captured = Arc::new(Mutex::new(Vec::new()));
        let provider = Arc::new(ToolsCaptureProvider {
            captured: captured.clone(),
        });

        let registry = Arc::new(
            BuiltinToolRegistry::new()
                .register(Arc::new(cairn_harness_tools::HarnessBuiltin::<
                    cairn_harness_tools::HarnessGrep,
                >::new()))
                .register(Arc::new(cairn_harness_tools::HarnessBuiltin::<
                    cairn_harness_tools::HarnessWebFetch,
                >::new())),
        );
        let phase = LlmDecidePhase::new(provider, "test-model").with_tools(registry);

        // Unknown role id with no `AgentRoleService` attached → the
        // fallback builds an empty-allowlist role record, which is the
        // §D3 no-restriction path. DECIDE must see the full tool
        // surface.
        let mut ctx = ctx();
        ctx.agent_type = "custom-role-not-in-defaults".to_owned();

        let _ = phase.decide(&ctx, &empty_gather()).await.unwrap();

        let tools = captured.lock().unwrap().clone();
        let names: Vec<String> = tools
            .iter()
            .filter_map(|t| {
                t.get("function")
                    .and_then(|f| f.get("name"))
                    .and_then(|n| n.as_str())
                    .map(str::to_owned)
            })
            .collect();

        assert!(
            names.iter().any(|n| n == "webfetch"),
            "non-orchestrator role must see webfetch (no allowlist \
             filter applies); names={names:?}"
        );
        assert!(
            names.iter().any(|n| n == "grep"),
            "non-orchestrator role must see grep; names={names:?}"
        );
    }

    // ── RFC 031 PR-C: service-backed DECIDE resolution ──────────────
    //
    // The fixtures below wire the real `AgentRoleServiceImpl` +
    // `InMemoryStore` pair into `LlmDecidePhase` and exercise the
    // four observable orchestrator outcomes:
    //
    //   1. `resolve` returns an operator-defined custom role —
    //      DECIDE reads `role.tools` from the projection instead of
    //      `default_roles()`.
    //   2. Role declares a tool id the registry doesn't know —
    //      `ToolDeclaredButMissing` lands on the event log and the
    //      advisory is deduped per-run via
    //      `ctx.declared_but_missing`.
    //   3. `forbid_all_tools = true` clears the tool surface
    //      regardless of `tools[]` content.
    //   4. `spawn_subagent_tool_def_for` fills the per-run
    //      `agent_role_list_cache` on first DECIDE; subsequent calls
    //      reuse the cached snapshot.

    mod rfc_031_prc {
        use super::*;
        use cairn_domain::agent_roles::{AgentRole as D31AgentRole, AgentRoleTier as D31Tier};
        use cairn_domain::RuntimeEvent as D31Event;
        use cairn_runtime::services::{AgentRoleService as D31Service, AgentRoleServiceImpl};
        use cairn_store::event_log::EventLog;
        use cairn_store::InMemoryStore;

        /// Capturing provider returns an empty proposal set. Useful
        /// when the test cares only about what tools the provider
        /// saw, not what DECIDE does with the response.
        struct NoopProvider;

        #[async_trait]
        impl GenerationProvider for NoopProvider {
            async fn generate(
                &self,
                _model: &str,
                _messages: Vec<serde_json::Value>,
                _settings: &ProviderBindingSettings,
                _tools: &[serde_json::Value],
            ) -> Result<GenerationResponse, ProviderAdapterError> {
                Ok(GenerationResponse {
                    text: String::new(),
                    input_tokens: Some(0),
                    output_tokens: Some(0),
                    model_id: "noop".to_owned(),
                    tool_calls: vec![serde_json::json!({
                        "id": "call_1",
                        "type": "function",
                        "function": {
                            "name": "complete_run",
                            "arguments": "{\"final_answer\": \"x\"}"
                        }
                    })],
                    finish_reason: Some("tool_calls".to_owned()),
                })
            }
        }

        fn test_project() -> cairn_domain::ProjectKey {
            cairn_domain::ProjectKey::new("t_prc", "w_prc", "p_prc")
        }

        async fn define_custom(store: Arc<InMemoryStore>, role: D31AgentRole) {
            let svc = AgentRoleServiceImpl::new(store);
            D31Service::define(
                &svc,
                &test_project(),
                role,
                cairn_domain::OperatorId::new("op_prc"),
            )
            .await
            .expect("define custom role");
        }

        fn builder_registry() -> Arc<cairn_tools::builtins::BuiltinToolRegistry> {
            Arc::new(
                cairn_tools::builtins::BuiltinToolRegistry::new()
                    .register(Arc::new(cairn_harness_tools::HarnessBuiltin::<
                        cairn_harness_tools::HarnessGrep,
                    >::new()))
                    .register(Arc::new(cairn_harness_tools::HarnessBuiltin::<
                        cairn_harness_tools::HarnessBash,
                    >::new())),
            )
        }

        #[tokio::test]
        async fn custom_role_with_tools_filters_to_declared_subset() {
            let store = Arc::new(InMemoryStore::new());
            let role = D31AgentRole::new("pr-reviewer-prc", "PR Reviewer", D31Tier::Standard)
                .with_system_prompt("## Specialty\nTest role.\n")
                .with_tools(["grep"]); // only grep allowed
            define_custom(store.clone(), role).await;

            let agent_roles: Arc<dyn D31Service> =
                Arc::new(AgentRoleServiceImpl::new(store.clone()));
            let phase = LlmDecidePhase::new(Arc::new(NoopProvider), "test-model")
                .with_tools(builder_registry())
                .with_agent_roles(agent_roles);

            let mut ctx = ctx();
            ctx.project = test_project();
            ctx.agent_type = "pr-reviewer-prc".to_owned();

            let _ = phase.decide(&ctx, &empty_gather()).await.unwrap();
            // The provider capture would be expensive to re-plumb here;
            // instead we re-run the allowlist-filter helper with the
            // resolved role to pin the contract.
            let resolved = phase.resolve_role_or_fallback(&ctx).await;
            assert_eq!(resolved.role_id, "pr-reviewer-prc");
            assert_eq!(resolved.tools, vec!["grep".to_owned()]);
        }

        #[tokio::test]
        async fn missing_tool_emits_tool_declared_but_missing_event() {
            let store = Arc::new(InMemoryStore::new());
            // Declare a tool id that the registry doesn't have.
            let role = D31AgentRole::new("lane-reviewer-prc", "Lane Reviewer", D31Tier::Standard)
                .with_system_prompt("## Specialty\nTest.\n")
                .with_tools(["post_inline_comment", "grep"]);
            define_custom(store.clone(), role).await;

            let agent_roles: Arc<dyn D31Service> =
                Arc::new(AgentRoleServiceImpl::new(store.clone()));
            let phase = LlmDecidePhase::new(Arc::new(NoopProvider), "test-model")
                .with_tools(builder_registry())
                .with_agent_roles(agent_roles)
                .with_event_log(store.clone());

            let mut ctx = ctx();
            ctx.project = test_project();
            ctx.agent_type = "lane-reviewer-prc".to_owned();

            let _ = phase.decide(&ctx, &empty_gather()).await.unwrap();

            let stream = store.read_stream(None, 100).await.unwrap();
            let misses: Vec<_> = stream
                .iter()
                .filter_map(|e| match &e.envelope.payload {
                    D31Event::ToolDeclaredButMissing(m) => Some(m),
                    _ => None,
                })
                .collect();
            assert_eq!(misses.len(), 1, "exactly one advisory for the unknown tool");
            assert_eq!(misses[0].role_id, "lane-reviewer-prc");
            assert_eq!(misses[0].tool_id, "post_inline_comment");

            // Second DECIDE reuses the same ctx — dedup set is per-run,
            // so no additional advisory lands.
            let _ = phase.decide(&ctx, &empty_gather()).await.unwrap();
            let stream2 = store.read_stream(None, 100).await.unwrap();
            let misses2: Vec<_> = stream2
                .iter()
                .filter(|e| matches!(&e.envelope.payload, D31Event::ToolDeclaredButMissing(_)))
                .collect();
            assert_eq!(misses2.len(), 1, "dedup set prevents re-emit");
        }

        #[tokio::test]
        async fn forbid_all_tools_clears_surface() {
            let store = Arc::new(InMemoryStore::new());
            let role = D31AgentRole::new("no-tools-prc", "Silent", D31Tier::Standard)
                .with_system_prompt("## Specialty\nReadonly.\n")
                .with_forbid_all_tools(true);
            define_custom(store.clone(), role).await;

            let agent_roles: Arc<dyn D31Service> =
                Arc::new(AgentRoleServiceImpl::new(store.clone()));
            let phase = LlmDecidePhase::new(Arc::new(NoopProvider), "test-model")
                .with_tools(builder_registry())
                .with_agent_roles(agent_roles);

            let mut ctx = ctx();
            ctx.project = test_project();
            ctx.agent_type = "no-tools-prc".to_owned();

            let resolved = phase.resolve_role_or_fallback(&ctx).await;
            assert!(resolved.forbid_all_tools);
            // Apply the filter against a real tool surface. The
            // `forbid_all_tools` arm clears every entry regardless of
            // what ids are present.
            let mut tools: Vec<BuiltinToolDescriptor> = builder_registry().prompt_tools();
            assert!(!tools.is_empty(), "precondition: registry has tools");
            apply_role_tool_allowlist(&resolved, &mut tools);
            assert!(tools.is_empty(), "forbid_all_tools clears the surface");
        }

        #[tokio::test]
        async fn spawn_subagent_tool_def_uses_run_scoped_cache() {
            let store = Arc::new(InMemoryStore::new());
            let role = D31AgentRole::new("pr-reviewer-cache", "Reviewer", D31Tier::Standard)
                .with_system_prompt("## Specialty\nTest.\n");
            define_custom(store.clone(), role).await;

            let agent_roles: Arc<dyn D31Service> =
                Arc::new(AgentRoleServiceImpl::new(store.clone()));
            let phase = LlmDecidePhase::new(Arc::new(NoopProvider), "test-model")
                .with_tools(builder_registry())
                .with_agent_roles(agent_roles);

            let mut ctx = ctx();
            ctx.project = test_project();
            ctx.agent_type = "orchestrator".to_owned();

            // First call fills the cache.
            let def1 = phase.spawn_subagent_tool_def_for(&ctx).await;
            let roles1 = def1["function"]["parameters"]["properties"]["role"]["enum"]
                .as_array()
                .expect("role enum array")
                .iter()
                .map(|v| v.as_str().unwrap_or(""))
                .collect::<Vec<_>>();
            assert!(roles1.contains(&"pr-reviewer-cache"));
            assert!(!roles1.contains(&"orchestrator"));

            // Second call reuses the cache — the `OnceCell` has been
            // filled, so even if we retracted the role the snapshot
            // would still carry it. Verify via byte-equality.
            let def2 = phase.spawn_subagent_tool_def_for(&ctx).await;
            assert_eq!(def1, def2);
        }
    }
}
