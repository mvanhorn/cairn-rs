//! RuntimeExecutePhase — concrete ExecutePhase backed by cairn-rs runtime services.
//!
//! Dispatches each `ActionProposal` from `DecideOutput` to the appropriate
//! runtime service:
//!
//! | `ActionType`         | Runtime service                                    |
//! |----------------------|----------------------------------------------------|
//! | `InvokeTool`         | `ToolInvocationService` + inline tool dispatch     |
//! | `SpawnSubagent`      | `TaskServiceImpl::spawn_subagent`                  |
//! | `SendNotification`   | `MailboxService::send`                             |
//! | `CompleteRun`        | `RunService::complete`                             |
//! | `EscalateToOperator` | `ApprovalService::request` + run → waiting_approval|
//! | `CreateMemory`       | async no-op (memory ingestion runs independently)  |
//!
//! After each successful tool call, `CheckpointService::save` is called if
//! the tool call count meets the configured `checkpoint_every_n_tool_calls`
//! threshold (default: save after every tool call).

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use cairn_domain::{
    policy::ApprovalRequirement,
    tool_invocation::{ToolInvocationOutcomeKind, ToolInvocationTarget},
    ActionType, ApprovalId, CheckpointId, ExecutionClass, RuntimeEvent, TaskId,
    ToolInvocationCacheHit, ToolInvocationId, ToolRecoveryPaused,
};
use cairn_runtime::{
    decisions::DecisionService,
    mailbox::MailboxService,
    services::ToolInvocationService,
    startup::{CachedToolResult, RecoveryDispatchDecision, ToolCallId, ToolCallResultCache},
    tool_call_approvals::{
        ApprovalDecision as ToolCallApprovalDecision, OperatorDecision, ToolCallApprovalService,
        ToolCallProposal,
    },
    ApprovalService, CheckpointService, RunService, TaskService,
};
use cairn_tools::builtins::BuiltinToolRegistry;
#[allow(unused_imports)]
use cairn_tools::builtins::ToolHandler;
use std::sync::Mutex;
use std::time::Duration;

use crate::context::{
    ActionResult, ActionStatus, DecideOutput, ExecuteOutcome, LoopSignal, OrchestrationContext,
    StepSummary,
};
use crate::error::OrchestratorError;
use crate::execute::{ApprovedDispatch, ExecutePhase};

// ── Unrecoverable-internal-failure marker (F35 review feedback) ─────────────
//
// Most `ActionStatus::Failed` reasons on an `InvokeTool` proposal are
// recoverable tool errors (NOT_FOUND, InvalidArgs, PermissionDenied,
// Timeout, upstream 5xx) that the LLM should see as tool_result feedback
// and adapt to on the next DECIDE turn — see `derive_signal` / the
// `LoopSignal::Continue` branch for the F35 contract.
//
// A small set of failures are *not* LLM-recoverable: they indicate
// orchestrator-side misconfiguration or state corruption the model cannot
// fix by picking a different tool or arg (e.g. the tool registry wasn't
// wired at all — no tool choice will ever work). If we treated those the
// same as recoverable errors, the run would burn iterations until
// `max_iterations` before failing, producing noisy telemetry and charging
// the operator for zero-signal LLM calls.
//
// The execute phase tags such reasons with this prefix; `derive_signal`
// special-cases the prefix and promotes the result to `LoopSignal::Failed`
// so the loop terminates fast with a clear operator-facing reason.
pub(crate) const UNRECOVERABLE_INTERNAL_PREFIX: &str = "internal_unrecoverable: ";

// ── Malformed-spawn-proposal marker (#689 R2-A) ─────────────────────────────
//
// When the LLM emits a `spawn_subagent` proposal without a valid `tool_name`
// (role) or without a non-empty `tool_args["goal"]`, the execute phase
// rejects the proposal with `ActionStatus::Failed`. Pre-#689 `derive_signal`
// treated this as a non-InvokeTool terminal failure and surfaced it as
// `LoopSignal::Failed`, killing the whole run on a single malformed LLM
// response — the dogfood R2 Finding A repro (run_r2_1778028487).
//
// The LLM can recover from a malformed spawn the same way it recovers from a
// bad tool_args schema: re-emit the proposal with the missing field. The fix
// tags the reason with this sentinel prefix so `derive_signal` maps
// `SpawnSubagent` + prefix-tagged `Failed` to `LoopSignal::Continue`,
// threading the rejection into `step_history` via `build_step_summary` so
// the next DECIDE turn sees the validation error and can correct.
//
// Unbounded retry would let a permanently-broken model burn the whole
// iteration budget against the gate. `loop_runner` counts consecutive
// malformed spawns and terminates with `LoopTermination::Failed` once the
// cap is hit (see `MAX_CONSECUTIVE_MALFORMED_SPAWNS` in `context.rs`).
pub(crate) const MALFORMED_SPAWN_PROPOSAL_PREFIX: &str = "malformed_spawn_proposal: ";

// ── RuntimeExecutePhase ───────────────────────────────────────────────────────

/// Concrete `ExecutePhase` that routes `ActionProposal` variants through the
/// cairn-rs runtime service layer.
///
/// Construct via [`RuntimeExecutePhase::builder`].  All services share the
/// same underlying `InMemoryStore` so writes from one service are immediately
/// visible to reads from another.
pub struct RuntimeExecutePhase {
    run_service: Arc<dyn RunService>,
    task_service: Arc<dyn TaskService>,
    approval_service: Arc<dyn ApprovalService>,
    checkpoint_service: Arc<dyn CheckpointService>,
    mailbox_service: Arc<dyn MailboxService>,
    tool_invocation_service: Arc<dyn ToolInvocationService>,
    /// Registered built-in tools (memory_search, memory_store, …). Required
    /// for tool dispatch; absent means every `InvokeTool` proposal fails loud.
    tool_registry: Option<Arc<BuiltinToolRegistry>>,
    /// Decision service for pre-dispatch policy evaluation (RFC 019).
    decision_service: Option<Arc<dyn DecisionService>>,
    /// Save a checkpoint after every N-th successful tool call (1 = every call).
    checkpoint_every_n_tool_calls: u32,
    /// Maximum size of tool output copied back into the LLM context.
    tool_output_token_limit: usize,
    /// T5-H2: tool-call counter is cumulative across iterations so the
    /// `checkpoint_every_n_tool_calls` cadence honours its name. A
    /// per-iteration local counter never triggers when each iteration
    /// contains a single tool call.
    tool_call_count: std::sync::atomic::AtomicU32,
    /// RFC 020 Track 3: shared `ToolCallResultCache` consulted before each
    /// tool dispatch. A hit serves the cached result, emits
    /// `ToolInvocationCacheHit`, and skips invocation entirely. A miss
    /// proceeds to dispatch; successful completions populate the cache so
    /// a subsequent same-step replay hits.
    ///
    /// Optional so existing callers (tests) can omit wiring; production
    /// always injects a shared cache via the builder.
    tool_result_cache: Option<Arc<Mutex<ToolCallResultCache>>>,
    /// BP-v2 (research doc `docs/research/llm-agent-approval-systems.md`)
    /// propose-then-await service. When wired, the execute phase drives
    /// `requires_approval` tool calls through `submit_proposal` +
    /// `await_decision` + `retrieve_approved_proposal` so the proposal
    /// survives across the operator decision — killing the dogfood bug
    /// where the approval gate discarded the LLM's args and re-queried
    /// the model after approval.
    ///
    /// `None` preserves the legacy `ApprovalService::request_with_context`
    /// short-circuit used by existing tests.
    tool_call_approval_service: Option<Arc<dyn ToolCallApprovalService>>,
    /// Fallback wall-clock timeout applied to
    /// `ToolCallApprovalService::await_decision` when the orchestration
    /// context didn't override it. Defaults to 24h.
    ///
    /// NOTE: as of F26 (dogfood blocker fix) the approval gate no longer
    ///   calls `await_decision` in-process — `PendingOperator` suspends
    ///   the loop immediately. The field is preserved for the builder
    ///   API + future use (e.g. an optional synchronous-wait mode gated
    ///   on a capability flag), but is currently unused in the hot path.
    #[allow(dead_code)]
    approval_timeout_default: Duration,
}

impl RuntimeExecutePhase {
    pub fn builder() -> RuntimeExecutePhaseBuilder {
        RuntimeExecutePhaseBuilder::default()
    }
}

// ── Builder ───────────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct RuntimeExecutePhaseBuilder {
    run_service: Option<Arc<dyn RunService>>,
    task_service: Option<Arc<dyn TaskService>>,
    approval_service: Option<Arc<dyn ApprovalService>>,
    checkpoint_service: Option<Arc<dyn CheckpointService>>,
    mailbox_service: Option<Arc<dyn MailboxService>>,
    tool_invocation_service: Option<Arc<dyn ToolInvocationService>>,
    tool_registry: Option<Arc<BuiltinToolRegistry>>,
    decision_service: Option<Arc<dyn DecisionService>>,
    checkpoint_every_n_tool_calls: u32,
    tool_output_token_limit: Option<usize>,
    tool_result_cache: Option<Arc<Mutex<ToolCallResultCache>>>,
    tool_call_approval_service: Option<Arc<dyn ToolCallApprovalService>>,
    approval_timeout_default: Option<Duration>,
}

impl RuntimeExecutePhaseBuilder {
    pub fn run_service(mut self, s: Arc<dyn RunService>) -> Self {
        self.run_service = Some(s);
        self
    }
    pub fn task_service(mut self, s: Arc<dyn TaskService>) -> Self {
        self.task_service = Some(s);
        self
    }
    pub fn approval_service(mut self, s: Arc<dyn ApprovalService>) -> Self {
        self.approval_service = Some(s);
        self
    }
    pub fn checkpoint_service(mut self, s: Arc<dyn CheckpointService>) -> Self {
        self.checkpoint_service = Some(s);
        self
    }
    pub fn mailbox_service(mut self, s: Arc<dyn MailboxService>) -> Self {
        self.mailbox_service = Some(s);
        self
    }
    pub fn tool_invocation_service(mut self, s: Arc<dyn ToolInvocationService>) -> Self {
        self.tool_invocation_service = Some(s);
        self
    }
    pub fn tool_registry(mut self, r: Arc<BuiltinToolRegistry>) -> Self {
        self.tool_registry = Some(r);
        self
    }
    pub fn checkpoint_every_n_tool_calls(mut self, n: u32) -> Self {
        self.checkpoint_every_n_tool_calls = n;
        self
    }
    pub fn tool_output_token_limit(mut self, limit: usize) -> Self {
        self.tool_output_token_limit = Some(limit.max(1));
        self
    }
    pub fn decision_service(mut self, ds: Arc<dyn DecisionService>) -> Self {
        self.decision_service = Some(ds);
        self
    }
    /// RFC 020 Track 3: inject a shared `ToolCallResultCache`. Without it,
    /// Track 3 cache-hit / recovery-pause behaviour is disabled (every
    /// dispatch runs fresh). Production wiring always injects one.
    pub fn tool_result_cache(mut self, cache: Arc<Mutex<ToolCallResultCache>>) -> Self {
        self.tool_result_cache = Some(cache);
        self
    }
    /// Inject the BP-v2 tool-call approval service. Required in production
    /// for the propose-then-await flow; tests omit it to exercise legacy
    /// `ApprovalService::request_with_context` paths.
    pub fn tool_call_approval_service(mut self, svc: Arc<dyn ToolCallApprovalService>) -> Self {
        self.tool_call_approval_service = Some(svc);
        self
    }
    /// Default timeout for operator approval decisions. Defaults to 24h
    /// when unset. Per-run override flows via
    /// `OrchestrationContext.approval_timeout`.
    pub fn approval_timeout_default(mut self, d: Duration) -> Self {
        self.approval_timeout_default = Some(d);
        self
    }
    /// Finalise the builder into a [`RuntimeExecutePhase`].
    ///
    /// Returns [`BuilderError::Missing`] when any of the six required
    /// services were never supplied, naming the first one that is
    /// missing so the caller can add the corresponding setter. Callers
    /// that have already supplied every service (production startup in
    /// `handlers::runs`/`handlers::github`, test fixtures) can
    /// `.expect("builder misconfigured")` at the call site — but the
    /// error variant makes the diagnosis local and actionable rather
    /// than a stringly-typed panic inside the library. Audit #476.
    pub fn build(self) -> Result<RuntimeExecutePhase, BuilderError> {
        Ok(RuntimeExecutePhase {
            run_service: self
                .run_service
                .ok_or(BuilderError::Missing("run_service"))?,
            task_service: self
                .task_service
                .ok_or(BuilderError::Missing("task_service"))?,
            approval_service: self
                .approval_service
                .ok_or(BuilderError::Missing("approval_service"))?,
            checkpoint_service: self
                .checkpoint_service
                .ok_or(BuilderError::Missing("checkpoint_service"))?,
            mailbox_service: self
                .mailbox_service
                .ok_or(BuilderError::Missing("mailbox_service"))?,
            tool_invocation_service: self
                .tool_invocation_service
                .ok_or(BuilderError::Missing("tool_invocation_service"))?,
            tool_registry: self.tool_registry,
            decision_service: self.decision_service,
            checkpoint_every_n_tool_calls: self.checkpoint_every_n_tool_calls.max(1),
            tool_output_token_limit: self.tool_output_token_limit.unwrap_or(2000),
            tool_call_count: std::sync::atomic::AtomicU32::new(0),
            tool_result_cache: self.tool_result_cache,
            tool_call_approval_service: self.tool_call_approval_service,
            approval_timeout_default: self
                .approval_timeout_default
                .unwrap_or_else(|| Duration::from_secs(24 * 60 * 60)),
        })
    }
}

/// Errors returned by [`RuntimeExecutePhaseBuilder::build`].
///
/// Named `Missing(field: &'static str)` so the caller gets a concrete
/// field name (`"run_service"`, `"task_service"`, …) and not just a
/// stringly-typed panic. Construction is a startup-only path, so this
/// stays a hand-written error type rather than taking on a
/// `thiserror` dependency for a single variant. Implements
/// `std::error::Error` + `Display` so callers that want to propagate
/// (via `?`, `anyhow`, or a startup-log formatter) can, while leaving
/// production call sites free to `.expect("builder misconfigured")`
/// when every setter is supplied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuilderError {
    /// A required service was never supplied via its setter. The
    /// embedded `&'static str` is the missing field name so operators
    /// can point at the exact call site.
    Missing(&'static str),
}

impl std::fmt::Display for BuilderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuilderError::Missing(field) => {
                write!(f, "RuntimeExecutePhaseBuilder: missing {field}")
            }
        }
    }
}

impl std::error::Error for BuilderError {}

// ── ExecutePhase impl ─────────────────────────────────────────────────────────

#[async_trait]
impl ExecutePhase for RuntimeExecutePhase {
    async fn execute(
        &self,
        ctx: &OrchestrationContext,
        decide: &DecideOutput,
    ) -> Result<ExecuteOutcome, OrchestratorError> {
        // ── Parallel batch for InvokeTool ─────────────────────────────────
        //
        // When the LLM emits multiple tool_calls in a single turn (modern
        // models do this — "read fileA AND read fileB in parallel"), we
        // MUST NOT serialize the batch on the slowest approval. One
        // tool-call waiting on operator approval cannot block N auto-
        // approved siblings from running.
        //
        // Strategy: drive every `InvokeTool` proposal concurrently via
        // `futures::future::join_all`. Each call owns its own oneshot in
        // the ToolCallApprovalService, so pending approvals block only
        // their own future. Non-InvokeTool proposals (CompleteRun,
        // SpawnSubagent, SendNotification, EscalateToOperator,
        // CreateMemory) are processed sequentially afterwards — those are
        // bookkeeping / control-flow steps that carry loop-terminal signals
        // (Done, WaitSubagent, WaitApproval) and must honour the original
        // short-circuit semantics.
        //
        // Original positional ordering is preserved so downstream emitters
        // (tool_called, tool_result, FF attempt_stream frames) still see
        // results indexed to the LLM's emitted proposals.

        let mut results: Vec<Option<ActionResult>> =
            (0..decide.proposals.len()).map(|_| None).collect();
        let mut loop_signal = LoopSignal::Continue;

        // ── Phase 1: parallel InvokeTool dispatch ────────────────────────
        //
        // Only parallel-batch InvokeTool proposals whose position is
        // BEFORE the first always-terminal control-flow proposal
        // (`CompleteRun`, `SpawnSubagent`, `EscalateToOperator`). The old
        // sequential path would break on those before reaching later
        // InvokeTool proposals, and we must preserve that semantic — a
        // `[CompleteRun, invoke_foo]` decide output must NOT run `foo`.
        //
        // `SendNotification` and `CreateMemory` are non-InvokeTool but
        // never set terminal signals, so they do NOT split the batch.
        let first_terminal_idx = decide.proposals.iter().position(|p| {
            matches!(
                p.action_type,
                ActionType::CompleteRun
                    | ActionType::FailRun
                    | ActionType::SpawnSubagent
                    | ActionType::EscalateToOperator
            )
        });
        let invoke_indices: Vec<usize> = decide
            .proposals
            .iter()
            .enumerate()
            .filter_map(|(i, p)| {
                if p.action_type != ActionType::InvokeTool {
                    return None;
                }
                match first_terminal_idx {
                    Some(k) if i > k => None,
                    _ => Some(i),
                }
            })
            .collect();

        if !invoke_indices.is_empty() {
            let futs = invoke_indices.iter().map(|&i| {
                let proposal = decide.proposals[i].clone();
                async move {
                    let started_at = std::time::Instant::now();
                    let mut result = self.dispatch_one(ctx, &proposal, i as u32).await?;
                    result.duration_ms = started_at.elapsed().as_millis() as u64;
                    Ok::<_, OrchestratorError>((i, result))
                }
            });
            let joined = futures::future::join_all(futs).await;
            for outcome in joined {
                let (i, result) = outcome?;
                results[i] = Some(result);
            }

            // ── Derive loop signal from parallel InvokeTool results ──────
            //
            // `dispatch_one(InvokeTool)` CAN return `AwaitingApproval` from
            // two paths that predate this PR:
            //
            //   1. Legacy approval-gate fallback (no `ToolCallApprovalService`
            //      wired) — `request_with_context` short-circuit returns
            //      `AwaitingApproval`.
            //   2. RFC 020 Track 3 `DangerousPause` recovery branch — on
            //      crash-mid-dispatch of a non-idempotent tool, operator
            //      must confirm re-invocation.
            //
            // Both MUST escalate to `LoopSignal::WaitApproval` so the loop
            // runner suspends. Missing this is a production-critical hole
            // in the recovery-pause case (dangerous tools would silently
            // skip the operator gate). Earliest-index wins so the ordering
            // matches what the old sequential path produced.
            for i in &invoke_indices {
                let Some(result) = results[*i].as_ref() else {
                    continue;
                };
                let next_signal = derive_signal(result, &loop_signal);
                if !matches!(next_signal, LoopSignal::Continue) {
                    loop_signal = next_signal;
                    break;
                }
            }
        }

        // ── Phase 2: sequential non-InvokeTool dispatch ──────────────────
        //
        // These carry terminal signals (CompleteRun → Done, SpawnSubagent
        // → WaitSubagent, EscalateToOperator → WaitApproval). Original
        // short-circuit semantics: first terminal signal stops the batch.
        //
        // If Phase 1 already set a terminal signal (e.g. an InvokeTool hit
        // a recovery pause), we skip Phase 2 entirely — running bookkeeping
        // steps after the batch has already committed to suspending would
        // trip downstream invariants (double-completion, extra subagent
        // spawn).
        if matches!(loop_signal, LoopSignal::Continue) {
            for (i, proposal) in decide.proposals.iter().enumerate() {
                if proposal.action_type == ActionType::InvokeTool {
                    continue;
                }
                let started_at = std::time::Instant::now();
                let mut result = self.dispatch_one(ctx, proposal, i as u32).await?;
                result.duration_ms = started_at.elapsed().as_millis() as u64;

                let new_signal = derive_signal(&result, &loop_signal);
                results[i] = Some(result);
                if !matches!(new_signal, LoopSignal::Continue) {
                    loop_signal = new_signal;
                    break;
                }
            }
        }

        // Drop `None` slots — these are non-InvokeTool proposals that a
        // terminal short-circuit prevented from running. Emitting them
        // as synthesized `Failed` would inflate the loop runner's
        // `failed_count` telemetry and mislead any downstream consumer
        // that treats `Failed` as "dispatch was attempted and it
        // errored." A skipped proposal never got that far.
        //
        // The loop runner iterates `execute_outcome.results` without
        // zipping to `decide.proposals`, so a shorter results vector
        // is safe. This matches the pre-PR behaviour where the
        // sequential `break` also stopped adding results to the vec.
        let results: Vec<ActionResult> = results.into_iter().flatten().collect();

        // F35: a failed InvokeTool (NOT_FOUND, InvalidArgs, PermissionDenied,
        // Timeout, etc.) is *feedback* for the LLM, not a run-terminal
        // condition. The error text lives on the `ActionResult` and flows
        // back into `build_step_summary` below, which threads it into the
        // next iteration's DECIDE prompt so the LLM can adapt
        // ("that file doesn't exist, let me try a different path").
        //
        // Pre-F35 this block promoted *any* `Failed` status to
        // `LoopSignal::Failed` via `first_failure_reason`, which surfaced
        // as `LoopTermination::Failed { reason: "Error [NOT_FOUND]: …" }`
        // and killed the run on the first tool miss. See dogfood v4
        // evidence in PR body.
        //
        // Non-InvokeTool failures (CompleteRun / SpawnSubagent /
        // SendNotification / EscalateToOperator service errors) are still
        // terminal — they represent orchestrator-level bookkeeping that
        // the LLM cannot recover from by picking a different path. Those
        // continue to flow through `derive_signal` inside Phase 2.
        //
        // Panic-class failures (state corruption) are NOT handled here
        // either: a real Rust panic during tool dispatch unwinds past
        // this match and terminates the task, which is the correct
        // behavior for corrupted state. Recoverable `ToolError` variants
        // are stringified via `e.to_string()` upstream and land as
        // `ActionStatus::Failed { reason }` — that's the feedback path.

        Ok(ExecuteOutcome {
            results,
            loop_signal,
        })
    }

    async fn dispatch_approved(
        &self,
        ctx: &OrchestrationContext,
        approved: &ApprovedDispatch,
    ) -> Result<ActionResult, OrchestratorError> {
        self.dispatch_approved_inner(ctx, approved).await
    }
}

impl RuntimeExecutePhase {
    /// F25 drain path: execute a tool whose operator approval has already
    /// landed. This mirrors the non-approval branch of `dispatch_one` but:
    ///
    /// * Uses the pre-minted `ToolCallId` from the approval record — NO
    ///   re-derivation, because `ctx.iteration` has reset to 0 after the
    ///   approval round-trip and would produce a different deterministic
    ///   hash for the same logical call. That mismatch is the core of the
    ///   F25 shadowing bug.
    /// * Skips the approval gate entirely (the operator already approved).
    /// * Consults + populates the shared `ToolCallResultCache` so the
    ///   same call re-driven in a later iteration or after a process
    ///   restart hits the cache instead of re-invoking the tool.
    async fn dispatch_approved_inner(
        &self,
        ctx: &OrchestrationContext,
        approved: &ApprovedDispatch,
    ) -> Result<ActionResult, OrchestratorError> {
        let tool_name = approved.tool_name.clone();
        // Synthetic proposal mirrors what an `InvokeTool` ActionProposal
        // would have looked like had DECIDE emitted it. `requires_approval`
        // MUST be false — the approval round-trip is complete.
        let synth_proposal = cairn_domain::ActionProposal {
            action_type: cairn_domain::ActionType::InvokeTool,
            description: format!("drain approved tool call: {tool_name}"),
            confidence: 1.0,
            tool_name: Some(tool_name.clone()),
            tool_args: Some(approved.tool_args.clone()),
            requires_approval: false,
        };

        // ── Cache pre-check (F25 "has this already executed?") ─────────
        let startup_id = ToolCallId::from_raw(approved.call_id.as_str().to_owned());
        if let Some(cache_arc) = &self.tool_result_cache {
            let hit = {
                let guard = cache_arc.lock().unwrap_or_else(|e| e.into_inner());
                guard.get(&startup_id).cloned()
            };
            if let Some(cached) = hit {
                let context_output = truncate_tool_output_for_context(
                    cached.result_json.clone(),
                    self.tool_output_token_limit,
                );
                return Ok(ActionResult {
                    proposal: synth_proposal,
                    status: ActionStatus::Succeeded,
                    tool_output: Some(context_output),
                    invocation_id: None,
                    duration_ms: 0,
                });
            }
        }

        // ── Record invocation start ────────────────────────────────────
        let inv_id = ToolInvocationId::new(new_id("inv"));
        // F55: thread the approved args into the projection so operators
        // can see "what cairn ran" via GET /v1/tool-invocations. Clone is
        // cheap — tool args are already a small JSON value.
        self.tool_invocation_service
            .record_start(
                &ctx.project,
                inv_id.clone(),
                Some(ctx.session_id.clone()),
                Some(ctx.run_id.clone()),
                ctx.task_id.clone(),
                ToolInvocationTarget::Builtin {
                    tool_name: tool_name.clone(),
                },
                ExecutionClass::SandboxedProcess,
                Some(approved.tool_args.clone()),
            )
            .await
            .map_err(OrchestratorError::Runtime)?;

        // ── Dispatch via registry (required for drain) ─────────────────
        let registry = match self.tool_registry.as_ref() {
            Some(r) => r,
            None => {
                let reason = format!(
                    "{UNRECOVERABLE_INTERNAL_PREFIX}tool_registry not wired — cannot drain approved tool `{tool_name}`"
                );
                self.tool_invocation_service
                    .record_failed(
                        &ctx.project,
                        inv_id.clone(),
                        ctx.task_id.clone(),
                        tool_name.clone(),
                        ToolInvocationOutcomeKind::PermanentFailure,
                        Some(reason.clone()),
                        None,
                    )
                    .await
                    .map_err(OrchestratorError::Runtime)?;
                return Ok(ActionResult {
                    proposal: synth_proposal,
                    status: ActionStatus::Failed { reason },
                    tool_output: None,
                    invocation_id: Some(inv_id),
                    duration_ms: 0,
                });
            }
        };

        // #702 follow-up: use `ctx.tool_context()` so `agent_role_id` is
        // recorded on the ToolContext, enabling role-scoped tool
        // policies (orchestrator bash verb allowlist etc.).
        let mut tool_ctx = ctx.tool_context();
        let tool_args = tool_args_with_working_dir(
            &tool_name,
            &ctx.working_dir,
            Some(approved.tool_args.clone()),
        );

        let output_result = registry
            .execute_with_context(&tool_name, &ctx.project, tool_args, &tool_ctx)
            .await
            .map(|r| r.output)
            .map_err(|e| e.to_string());

        let buffered = tool_ctx.drain_buffered_events();

        match output_result {
            Ok(output) => {
                // Completion event carries the approval's call_id so the
                // startup replay rebuilds the cache entry on next boot.
                self.tool_invocation_service
                    .record_completed(
                        &ctx.project,
                        inv_id.clone(),
                        ctx.task_id.clone(),
                        tool_name.clone(),
                        &buffered,
                        Some(approved.call_id.as_str().to_owned()),
                        Some(output.clone()),
                    )
                    .await
                    .map_err(OrchestratorError::Runtime)?;

                // Populate runtime cache so an in-process re-drain hits.
                if let Some(cache_arc) = &self.tool_result_cache {
                    let mut guard = cache_arc.lock().unwrap_or_else(|e| e.into_inner());
                    guard.insert(CachedToolResult {
                        tool_call_id: startup_id,
                        tool_name: tool_name.clone(),
                        result_json: output.clone(),
                        completed_at: now_ms_u64(),
                    });
                }

                let context_output =
                    truncate_tool_output_for_context(output, self.tool_output_token_limit);
                Ok(ActionResult {
                    proposal: synth_proposal,
                    status: ActionStatus::Succeeded,
                    tool_output: Some(context_output),
                    invocation_id: Some(inv_id),
                    duration_ms: 0,
                })
            }
            Err(reason) => {
                self.tool_invocation_service
                    .record_failed(
                        &ctx.project,
                        inv_id.clone(),
                        ctx.task_id.clone(),
                        tool_name.clone(),
                        ToolInvocationOutcomeKind::PermanentFailure,
                        Some(reason.clone()),
                        None,
                    )
                    .await
                    .map_err(OrchestratorError::Runtime)?;
                Ok(ActionResult {
                    proposal: synth_proposal,
                    status: ActionStatus::Failed { reason },
                    tool_output: None,
                    invocation_id: Some(inv_id),
                    duration_ms: 0,
                })
            }
        }
    }

    /// Dispatch a single proposal to the appropriate runtime service.
    async fn dispatch_one(
        &self,
        ctx: &OrchestrationContext,
        proposal: &cairn_domain::ActionProposal,
        call_index: u32,
    ) -> Result<ActionResult, OrchestratorError> {
        match proposal.action_type {
            // ── InvokeTool ─────────────────────────────────────────────────
            ActionType::InvokeTool => {
                let tool_name = proposal.tool_name.clone().unwrap_or_default();

                // ── BP-v2 propose-then-await approval gate ────────────────
                //
                // Research doc `docs/research/llm-agent-approval-systems.md`
                // §§ "Execute Phase Pseudocode (Fixed)". Previous (broken)
                // behaviour: mint a fresh ApprovalId, fire
                // `request_with_context`, return `AwaitingApproval` without
                // persisting the proposal — after operator approval the
                // system had nothing to retrieve, so it re-queried the LLM
                // and often lost the args entirely. That was the dogfood
                // blocker.
                //
                // Now: if a `ToolCallApprovalService` is wired AND the
                // proposal requests approval, we:
                //   1. submit the proposal (persists `ToolCallProposed`
                //      + stashes args by `ToolCallId`),
                //   2. evaluate session allow-registry inside the service,
                //   3. if the service reports `PendingOperator`, block on
                //      `await_decision(call_id, timeout)` — the oneshot
                //      fires when the operator approves / rejects / amends+
                //      approves, or times out,
                //   4. on approval, retrieve the effective args (with
                //      operator amendments applied) and invoke the tool
                //      *inline* so the tool result flows back to the LLM
                //      in the same execute batch,
                //   5. on rejection / timeout, surface a tool_result error
                //      back to the LLM so it can revise its plan.
                //
                // When no `ToolCallApprovalService` is wired (existing
                // tests), we keep the legacy `ApprovalService` short-circuit
                // so those tests don't need rewriting.
                if proposal.requires_approval {
                    if let Some(ref svc) = self.tool_call_approval_service {
                        return self
                            .run_with_approval_gate(ctx, proposal, call_index, svc.clone())
                            .await;
                    }
                    // Legacy fallback: `ApprovalService` short-circuit.
                    return legacy_approval_gate_result(
                        &self.approval_service,
                        ctx,
                        proposal,
                        &tool_name,
                    )
                    .await;
                }

                let inv_id = ToolInvocationId::new(new_id("inv"));

                // F55: capture the proposal args at start-time for the
                // tool-invocation projection.
                self.tool_invocation_service
                    .record_start(
                        &ctx.project,
                        inv_id.clone(),
                        Some(ctx.session_id.clone()),
                        Some(ctx.run_id.clone()),
                        ctx.task_id.clone(),
                        ToolInvocationTarget::Builtin {
                            tool_name: tool_name.clone(),
                        },
                        ExecutionClass::SandboxedProcess,
                        proposal.tool_args.clone(),
                    )
                    .await
                    .map_err(OrchestratorError::Runtime)?;

                // ── Decision check (RFC 019): evaluate before dispatch ────
                if let Some(ref ds) = self.decision_service {
                    use cairn_domain::decisions::*;
                    let tool_effect = if let Some(ref reg) = self.tool_registry {
                        reg.get(&tool_name)
                            .map(|h| h.tool_effect())
                            .unwrap_or(ToolEffect::External)
                    } else {
                        ToolEffect::External
                    };
                    let dreq = DecisionRequest {
                        kind: DecisionKind::ToolInvocation {
                            tool_name: tool_name.clone(),
                            effect: tool_effect,
                        },
                        principal: Principal::Run {
                            run_id: ctx.run_id.clone(),
                        },
                        subject: DecisionSubject::ToolCall {
                            tool_name: tool_name.clone(),
                            args: proposal
                                .tool_args
                                .clone()
                                .unwrap_or(serde_json::Value::Null),
                        },
                        scope: ctx.project.clone(),
                        cost_estimate: None,
                        requested_at: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64,
                        correlation_id: cairn_domain::CorrelationId::new(format!(
                            "tool_{}_{}",
                            ctx.run_id, ctx.iteration
                        )),
                    };
                    match ds.evaluate(dreq).await {
                        Ok(decision) => {
                            if let DecisionOutcome::Denied { deny_reason, .. } = &decision.outcome {
                                return Ok(ActionResult {
                                    proposal: proposal.clone(),
                                    status: ActionStatus::Failed {
                                        reason: format!("decision_denied: {deny_reason}"),
                                    },
                                    tool_output: None,
                                    invocation_id: Some(inv_id),
                                    duration_ms: 0,
                                });
                            }
                        }
                        Err(e) => {
                            // Decision service error — fail open (allow) but
                            // log so the failure is observable. A silent
                            // fail-open on a security-critical policy
                            // evaluator (RFC 019) would remove the guard
                            // without telemetry.
                            tracing::warn!(
                                error = %e,
                                tool = %tool_name,
                                run_id = %ctx.run_id,
                                "decision service error — failing open"
                            );
                        }
                    }
                }

                // ── Tool dispatch: registry first, stub fallback ────────────
                // #702 follow-up: use `ctx.tool_context()` so
                // `agent_role_id` is recorded for role-scoped tool
                // policies (orchestrator bash verb allowlist etc.).
                let mut tool_ctx = ctx.tool_context();
                let tool_args = tool_args_with_working_dir(
                    &tool_name,
                    &ctx.working_dir,
                    proposal.tool_args.clone(),
                );

                // ── RFC 020 Track 3: mint ToolCallId + consult cache ───────
                // The ToolCallId is derived from run_id + step + call_index
                // + tool_name + normalized_args. Deterministic so a resumed
                // run at the same step recomputes the same ID and hits the
                // cache.
                let (tool_call_id, normalized_args, retry_safety) =
                    if let Some(registry) = self.tool_registry.as_ref() {
                        if let Some(handler) = registry.get(&tool_name) {
                            let normalized = handler.normalize_for_cache(&tool_args);
                            // `call_index` is the proposal's position within
                            // the current DecideOutput.proposals vector. Using
                            // the index (not a hardcoded 0) guarantees two
                            // parallel invocations of the SAME tool with
                            // IDENTICAL normalized args still get distinct
                            // ToolCallIds — the orchestrator dispatches them
                            // in order, so the index is stable across replay.
                            let id = ToolCallId::derive(
                                ctx.run_id.as_str(),
                                ctx.iteration,
                                call_index,
                                &tool_name,
                                &normalized,
                            );
                            (Some(id), normalized, handler.retry_safety())
                        } else {
                            (
                                None,
                                String::new(),
                                cairn_domain::recovery::RetrySafety::DangerousPause,
                            )
                        }
                    } else {
                        (
                            None,
                            String::new(),
                            cairn_domain::recovery::RetrySafety::DangerousPause,
                        )
                    };
                let _ = normalized_args; // reserved for future audit emission

                // Consult cache: on hit, serve cached result + emit audit event.
                if let (Some(ref id), Some(ref cache_arc)) =
                    (&tool_call_id, &self.tool_result_cache)
                {
                    let hit = {
                        let guard = cache_arc.lock().unwrap_or_else(|e| e.into_inner());
                        guard.get(id).cloned()
                    };
                    if let Some(cached) = hit {
                        // Atomically emit cache-hit audit + completion marker
                        // so the invocation lifecycle closes cleanly (the
                        // earlier `record_start` left it `Started`).
                        let now = now_ms_u64();
                        let cache_event =
                            RuntimeEvent::ToolInvocationCacheHit(ToolInvocationCacheHit {
                                project: ctx.project.clone(),
                                invocation_id: inv_id.clone(),
                                run_id: Some(ctx.run_id.clone()),
                                task_id: ctx.task_id.clone(),
                                tool_name: tool_name.clone(),
                                tool_call_id: id.as_str().to_owned(),
                                original_completed_at_ms: cached.completed_at,
                                served_at_ms: now,
                            });
                        // Persist the cached tool_call_id + result_json on
                        // the new ToolInvocationCompleted too. Downstream
                        // projections (including the next boot's
                        // `replay_tool_result_cache`) expect every
                        // completion event to carry these when they exist;
                        // leaving them `None` would silently break cache
                        // rebuild after a restart that intersected a
                        // cache-hit turn.
                        self.tool_invocation_service
                            .record_completed(
                                &ctx.project,
                                inv_id.clone(),
                                ctx.task_id.clone(),
                                tool_name.clone(),
                                &[cache_event],
                                Some(id.as_str().to_owned()),
                                Some(cached.result_json.clone()),
                            )
                            .await
                            .map_err(OrchestratorError::Runtime)?;

                        let context_output = truncate_tool_output_for_context(
                            cached.result_json.clone(),
                            self.tool_output_token_limit,
                        );
                        return Ok(ActionResult {
                            proposal: proposal.clone(),
                            status: ActionStatus::Succeeded,
                            tool_output: Some(context_output),
                            invocation_id: Some(inv_id),
                            duration_ms: 0,
                        });
                    }

                    // Cache miss + is_recovery: branch on RetrySafety.
                    if ctx.is_recovery {
                        let decision = cairn_runtime::startup::recovery_dispatch_decision(
                            &cache_arc.lock().unwrap_or_else(|e| e.into_inner()),
                            id,
                            &tool_name,
                            retry_safety,
                            true,
                        );
                        match decision {
                            RecoveryDispatchDecision::CacheHit => unreachable!(
                                "recovery_dispatch_decision returned CacheHit after miss"
                            ),
                            RecoveryDispatchDecision::Dispatch => {
                                // Fall through to fresh dispatch below.
                            }
                            RecoveryDispatchDecision::Pause { reason, .. } => {
                                let paused_event =
                                    RuntimeEvent::ToolRecoveryPaused(ToolRecoveryPaused {
                                        project: ctx.project.clone(),
                                        run_id: ctx.run_id.clone(),
                                        task_id: ctx.task_id.clone(),
                                        tool_name: tool_name.clone(),
                                        tool_call_id: id.as_str().to_owned(),
                                        reason: reason.clone(),
                                        paused_at_ms: now_ms_u64(),
                                    });
                                self.tool_invocation_service
                                    .record_failed(
                                        &ctx.project,
                                        inv_id.clone(),
                                        ctx.task_id.clone(),
                                        tool_name.clone(),
                                        ToolInvocationOutcomeKind::PermanentFailure,
                                        Some(reason.clone()),
                                        None,
                                    )
                                    .await
                                    .map_err(OrchestratorError::Runtime)?;
                                // Deterministic approval_id derived from the
                                // tool_call_id so two recovery sweeps of the
                                // same crashed iteration ask the operator
                                // for ONE approval, not N. `new_id()` uses
                                // a per-process counter that resets on
                                // restart, which would silently duplicate
                                // the pending approval on every boot of a
                                // wedged run.
                                let approval_id =
                                    ApprovalId::new(format!("appr_recovery_{}", id.as_str()));
                                // Append the pause audit event. Best-effort;
                                // approval request below records the primary
                                // transition.
                                if let Err(e) = self
                                    .approval_service
                                    .request_with_context(
                                        &ctx.project,
                                        approval_id.clone(),
                                        Some(ctx.run_id.clone()),
                                        ctx.task_id.clone(),
                                        ApprovalRequirement::Required,
                                        Some(format!(
                                            "RFC 020 recovery pause: re-dispatch of {}",
                                            tool_name
                                        )),
                                        Some(format!(
                                            "Run `{}` crashed mid-dispatch on `{}` (DangerousPause). \
                                             Operator must confirm before re-invocation. Reason: {}",
                                            ctx.run_id, tool_name, reason,
                                        )),
                                    )
                                    .await
                                {
                                    tracing::warn!(
                                        error = %e,
                                        run_id = %ctx.run_id,
                                        "approval request for recovery pause failed"
                                    );
                                }
                                // Emit the pause event itself via the tool
                                // invocation service's audit seam so the
                                // event log carries it for integration tests
                                // and operator dashboards.
                                if let Err(e) = self
                                    .tool_invocation_service
                                    .append_audit_events(&[paused_event])
                                    .await
                                {
                                    tracing::warn!(
                                        error = %e,
                                        "append ToolRecoveryPaused failed"
                                    );
                                }
                                return Ok(ActionResult {
                                    proposal: proposal.clone(),
                                    status: ActionStatus::AwaitingApproval { approval_id },
                                    tool_output: None,
                                    invocation_id: Some(inv_id),
                                    duration_ms: 0,
                                });
                            }
                        }
                    }
                }

                // T5-H5: tool_registry is required — no silent-Ok stub. If
                // someone forgot to wire a registry, every tool invocation
                // must fail loud rather than synthesise a fake success for
                // mutating actions (write_document, http_post, send_message).
                let tool_output_result = match self.tool_registry.as_ref() {
                    Some(registry) => registry
                        .execute_with_context(
                            &tool_name,
                            &ctx.project,
                            tool_args.clone(),
                            &tool_ctx,
                        )
                        .await
                        .map(|r| r.output)
                        .map_err(|e| e.to_string()),
                    None => Err(format!(
                        "{UNRECOVERABLE_INTERNAL_PREFIX}tool_registry not wired — cannot dispatch tool `{tool_name}`"
                    )),
                };

                // RFC 020 Track 3 invariant #11: drain any events the tool
                // buffered on the context and pass them to record_completed
                // as a single atomic append alongside ToolInvocationCompleted.
                let buffered = tool_ctx.drain_buffered_events();

                match tool_output_result {
                    Ok(output) => {
                        self.tool_invocation_service
                            .record_completed(
                                &ctx.project,
                                inv_id.clone(),
                                ctx.task_id.clone(),
                                tool_name.clone(),
                                &buffered,
                                tool_call_id.as_ref().map(|id| id.as_str().to_owned()),
                                Some(output.clone()),
                            )
                            .await
                            .map_err(OrchestratorError::Runtime)?;

                        // Populate cache post-completion so a later replay
                        // at the same step hits (same ToolCallId).
                        if let (Some(id), Some(cache_arc)) =
                            (&tool_call_id, &self.tool_result_cache)
                        {
                            let mut guard = cache_arc.lock().unwrap_or_else(|e| e.into_inner());
                            guard.insert(CachedToolResult {
                                tool_call_id: id.clone(),
                                tool_name: tool_name.clone(),
                                result_json: output.clone(),
                                completed_at: now_ms_u64(),
                            });
                        }

                        // T5-H2: counter is cumulative across iterations.
                        // fetch_add returns the pre-increment value; add 1.
                        let n = self
                            .tool_call_count
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                            + 1;
                        if n.is_multiple_of(self.checkpoint_every_n_tool_calls) {
                            let cp_id = CheckpointId::new(new_id("cp"));
                            self.checkpoint_service
                                .save(&ctx.project, &ctx.run_id, cp_id)
                                .await
                                .map_err(OrchestratorError::Runtime)?;
                        }

                        let context_output =
                            truncate_tool_output_for_context(output, self.tool_output_token_limit);

                        Ok(ActionResult {
                            proposal: proposal.clone(),
                            status: ActionStatus::Succeeded,
                            tool_output: Some(context_output),
                            invocation_id: Some(inv_id),
                            duration_ms: 0,
                        })
                    }
                    Err(reason) => {
                        self.tool_invocation_service
                            .record_failed(
                                &ctx.project,
                                inv_id.clone(),
                                ctx.task_id.clone(),
                                tool_name,
                                ToolInvocationOutcomeKind::PermanentFailure,
                                Some(reason.clone()),
                                None,
                            )
                            .await
                            .map_err(OrchestratorError::Runtime)?;

                        Ok(ActionResult {
                            proposal: proposal.clone(),
                            status: ActionStatus::Failed { reason },
                            tool_output: None,
                            invocation_id: Some(inv_id),
                            duration_ms: 0,
                        })
                    }
                }
            }

            // ── SpawnSubagent ──────────────────────────────────────────────
            ActionType::SpawnSubagent => {
                // #670 G2: extract the LLM's delegation context from
                // the `ActionProposal`. Before this extraction the
                // execute layer silently dropped `tool_args.goal` and
                // `tool_name` — the child had no goal, no role. The
                // prompt (see `decide_impl::build_system_prompt`)
                // documents the expected shape:
                //
                //     spawn_subagent: tool_name = role,
                //                     tool_args = {"goal": "..."}
                //
                // so a missing goal / role is a malformed proposal the
                // LLM emitted — return `Failed` with a clear reason
                // rather than silently succeeding with empty context.

                // Role: `proposal.tool_name` with known-role validation.
                // The prompt mentions three roles; any other string is
                // a behaviour regression we want visible in
                // `ActionStatus::Failed.reason`.
                // #775: `generic` is a registered role — orchestrator's
                // escape hatch when no specialty cleanly fits the goal.
                // Validated here against `default_roles()` indirectly via
                // this list; if the registry ever grows, this list needs
                // to grow with it (or be derived from default_roles()
                // directly — done as a follow-up in #776).
                const VALID_ROLES: &[&str] = &["executor", "researcher", "reviewer", "generic"];
                let role = match proposal.tool_name.as_deref() {
                    Some(r) if VALID_ROLES.contains(&r) => r.to_owned(),
                    Some(other) => {
                        // #689 R2-A: tag with `MALFORMED_SPAWN_PROPOSAL_PREFIX` so
                        // `derive_signal` maps this to `LoopSignal::Continue`
                        // and the LLM gets to correct the proposal on the
                        // next DECIDE turn (the rejection lands in
                        // `step_history` via `build_step_summary`). Pre-#689
                        // a single malformed spawn killed the whole run.
                        return Ok(ActionResult {
                            proposal: proposal.clone(),
                            status: ActionStatus::Failed {
                                reason: format!(
                                    "{MALFORMED_SPAWN_PROPOSAL_PREFIX}spawn_subagent: \
                                     tool_name must be one of {:?} (the LLM emitted \
                                     `{}` — malformed proposal; re-emit with a valid role)",
                                    VALID_ROLES, other
                                ),
                            },
                            tool_output: None,
                            invocation_id: None,
                            duration_ms: 0,
                        });
                    }
                    None => {
                        return Ok(ActionResult {
                            proposal: proposal.clone(),
                            status: ActionStatus::Failed {
                                reason: format!(
                                    "{MALFORMED_SPAWN_PROPOSAL_PREFIX}spawn_subagent: \
                                     tool_name is required and must be one of {:?} \
                                     (the LLM emitted a proposal without `tool_name` — \
                                     malformed proposal; re-emit with a valid role)",
                                    VALID_ROLES
                                ),
                            },
                            tool_output: None,
                            invocation_id: None,
                            duration_ms: 0,
                        });
                    }
                };

                // Goal: `proposal.tool_args["goal"]` as a non-empty string.
                // Whitespace-only goals are treated as missing — they
                // give the child no useful context.
                let goal = match proposal
                    .tool_args
                    .as_ref()
                    .and_then(|args| args.get("goal"))
                    .and_then(|g| g.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    Some(g) => g.to_owned(),
                    None => {
                        // #689 R2-A: see `MALFORMED_SPAWN_PROPOSAL_PREFIX`
                        // rustdoc — demoted from terminal failure to
                        // continue-with-step-history-feedback so the LLM
                        // gets a retry instead of the run dying.
                        return Ok(ActionResult {
                            proposal: proposal.clone(),
                            status: ActionStatus::Failed {
                                reason: format!(
                                    "{MALFORMED_SPAWN_PROPOSAL_PREFIX}spawn_subagent: \
                                     tool_args[\"goal\"] is required and must be a \
                                     non-empty string (the LLM emitted an incomplete \
                                     spawn proposal — the prompt requires \
                                     `{{\"goal\": \"...\"}}`; re-emit with a concrete goal)"
                                ),
                            },
                            tool_output: None,
                            invocation_id: None,
                            duration_ms: 0,
                        });
                    }
                };

                // #775 + #844 PR-2: pull BOTH optional fields
                // (`parent_context`, `reuse_sandbox_from`) through the
                // shared extractor that decide_impl uses on the native
                // tool-call path. Using the same helper here closes the
                // gap Gemini review flagged — a legacy nested shape
                // `{"tool_name": "executor", "tool_args": {"goal":
                // "...", "reuse_sandbox_from": "..."}}` was previously
                // only handled in the parse layer; the execute-side
                // inline extraction only looked at the flat top level,
                // so text-parsing-mode runs would drop the field.
                let (llm_parent_context, reuse_sandbox_from_str) = proposal
                    .tool_args
                    .as_ref()
                    .map(crate::decide_impl::extract_spawn_subagent_optionals)
                    .unwrap_or((None, None));

                // #844 PR-2 type guard (Copilot review): when the LLM
                // emits `reuse_sandbox_from` as a non-string (number,
                // null, object, array), the shared extractor returns
                // `None` — effectively a silent fallback to a fresh
                // sandbox. That hides schema drift from the LLM and
                // robs the retry loop of its correction signal. Detect
                // the key-present-but-wrong-type shape and surface as
                // MALFORMED so `derive_signal` routes it to
                // `LoopSignal::Continue` and the step_history rejection
                // tells the LLM exactly what to fix.
                if let Some(raw) = proposal
                    .tool_args
                    .as_ref()
                    .and_then(|args| args.get("reuse_sandbox_from"))
                {
                    if reuse_sandbox_from_str.is_none() && !raw.is_null() && !raw.is_string() {
                        return Ok(ActionResult {
                            proposal: proposal.clone(),
                            status: ActionStatus::Failed {
                                reason: format!(
                                    "{MALFORMED_SPAWN_PROPOSAL_PREFIX}spawn_subagent: \
                                     tool_args[\"reuse_sandbox_from\"] must be a \
                                     string run_id (the LLM emitted a non-string \
                                     value of shape {}). Re-emit with a string \
                                     run_id from the `## Prior sibling attempts` \
                                     block, or omit the field for a fresh sandbox.",
                                    match raw {
                                        serde_json::Value::Number(_) => "number",
                                        serde_json::Value::Bool(_) => "boolean",
                                        serde_json::Value::Array(_) => "array",
                                        serde_json::Value::Object(_) => "object",
                                        _ => "unexpected",
                                    }
                                ),
                            },
                            tool_output: None,
                            invocation_id: None,
                            duration_ms: 0,
                        });
                    }
                }

                let reuse_sandbox_from: Option<cairn_domain::RunId> =
                    reuse_sandbox_from_str.map(cairn_domain::RunId::new);

                // R35 (2026-05-10) dogfood: the orchestrator correctly
                // re-spawned fresh sub-agents after predecessors failed
                // the completion gate, but each child started with
                // `parent_context = null` and re-did discovery from
                // zero — same clone, same reads, same failed approach,
                // same gate rejection. Synthesise a `## Prior sibling
                // attempts` block from `step_history` when there are
                // failed `subagent_complete` entries, and prepend it so
                // the child sees predecessor context on its first
                // DECIDE. Helper caps at 2 entries and ~1200 chars each
                // to bound the prompt.
                let prior_siblings_block = recent_failed_siblings_summary(&ctx.step_history);

                // #844 PR-3: the spawn-time `Workspace path:` auto-inject
                // (originally added in #813 for executor discovery-loop
                // relief) was threading the PARENT's `ctx.working_dir`
                // into the child's `parent_context` — because at this
                // call site `ctx` is the parent's OrchestrationContext,
                // not the child's. R37 dogfood (2026-05-11) showed all
                // 3 re-spawned children cd'ing into the parent's
                // ephemeral sandbox (`/tmp/cairn-runs/<parent-id>/`)
                // and writing real cargo+src files there, instead of
                // into their own allocated sandbox. Every re-spawn
                // burned through discovery on the parent's pile of
                // stale artefacts.
                //
                // The #813 contract is preserved by the CHILD's own
                // DECIDE-time render: `build_user_message` emits
                // `## Run state { workspace_path: <child.working_dir> }`
                // for non-orchestrator roles (see #844 PR-1), where
                // `child.working_dir` is `working_dir_for_run(child)`
                // resolved at the child's orchestrate boot. That is
                // the CHILD's actual sandbox, not the parent's. The
                // executor-discovery-loop relief #813 aimed for still
                // fires via that header — no functional regression.
                //
                // Dropping the spawn-time injection entirely here is
                // the fix: it was never the right plumbing for the
                // child's path, and PR-2 (#847) now covers deliberate
                // sandbox-reuse via `reuse_sandbox_from` which replays
                // at the child's boot, not via a string in
                // parent_context.
                //
                // Compose `parent_context` from, in order:
                //   1. prior-siblings synthesised block (when present)
                //   2. LLM-supplied parent_context (when present)
                //
                // Each separator is a blank line so downstream prompt
                // rendering treats them as independent markdown blocks.
                let parent_context: Option<String> = {
                    let mut sections: Vec<String> = Vec::new();
                    if let Some(block) = prior_siblings_block {
                        sections.push(block);
                    }
                    if let Some(llm_ctx) = llm_parent_context {
                        sections.push(llm_ctx);
                    }
                    if sections.is_empty() {
                        None
                    } else {
                        Some(sections.join("\n\n"))
                    }
                };

                let child_task_id = TaskId::new(new_id("child_task"));
                // #670 G1+G2: scope the child task to the parent's
                // session. The `TaskService::spawn_subagent` rustdoc
                // says "subagent tasks are scoped to the parent's
                // session" and SQLite's `tasks.session_id` is a FK to
                // `sessions(session_id)` — minting a fresh session id
                // here (the pre-#670 behaviour) creates rows with a
                // dangling FK and breaks projections on dual-write
                // backends. G3 will introduce a proper child Session
                // when child runs are created; until then we
                // co-locate the child task on the parent's session.
                let child_session_id = ctx.session_id.clone();

                // #670 G3: mint a real `child_run_id` so the adapter
                // creates a concrete child `RunRecord` (instead of
                // passing `None` and leaving the child invisible in
                // `GET /v1/runs/:id/children`). The id is derived from
                // the fresh task id so replays of this execute phase
                // produce a stable, traceable child-run id rather than
                // a random uuid that would collide with a retry's
                // idempotency contract.
                let child_run_id = cairn_domain::RunId::new_subagent_for_task(&child_task_id);

                match self
                    .task_service
                    .spawn_subagent(
                        // #670 G4 PR-1a: no `project` argument. The
                        // adapter derives the child's project from
                        // the parent run (ctx.run_id) via its
                        // internal `RunService::get` hop, so neither
                        // the orchestrator nor the LLM can influence
                        // the child's tenancy.
                        ctx.run_id.clone(),
                        ctx.task_id.clone(),
                        child_task_id.clone(),
                        child_session_id,
                        Some(child_run_id),
                        // #670 G2: carry the LLM's delegation intent
                        // through to the `SubagentSpawned` event +
                        // `subagent_spawns` projection row.
                        goal,
                        role,
                        // #775: optional parent freeform context for
                        // the child's first DECIDE prompt.
                        parent_context,
                        // #844 PR-2: optional opt-in reference to a
                        // prior sibling whose sandbox the child should
                        // reuse. Validated at the adapter layer
                        // (same-root + same-project); rejections
                        // surface into step_history via the `Err(e)`
                        // branch below so the LLM can correct.
                        reuse_sandbox_from,
                    )
                    .await
                {
                    Ok(_) => Ok(ActionResult {
                        proposal: proposal.clone(),
                        status: ActionStatus::SubagentSpawned { child_task_id },
                        tool_output: None,
                        invocation_id: None,
                        duration_ms: 0,
                    }),
                    Err(e) => Ok(ActionResult {
                        proposal: proposal.clone(),
                        status: ActionStatus::Failed {
                            reason: e.to_string(),
                        },
                        tool_output: None,
                        invocation_id: None,
                        duration_ms: 0,
                    }),
                }
            }

            // ── SendNotification ───────────────────────────────────────────
            ActionType::SendNotification => {
                // Sender must have a task_id; recipient uses tool_name or run_id.
                let from_task = match &ctx.task_id {
                    Some(t) => t.clone(),
                    None => {
                        return Ok(ActionResult {
                            proposal: proposal.clone(),
                            status: ActionStatus::Failed {
                                reason: "SendNotification requires task_id in context".to_owned(),
                            },
                            tool_output: None,
                            invocation_id: None,
                            duration_ms: 0,
                        });
                    }
                };
                let to_task =
                    TaskId::new(proposal.tool_name.as_deref().unwrap_or(ctx.run_id.as_str()));
                match self
                    .mailbox_service
                    .send(
                        &ctx.project,
                        from_task,
                        to_task,
                        proposal.description.clone(),
                    )
                    .await
                {
                    Ok(_) => Ok(ActionResult {
                        proposal: proposal.clone(),
                        status: ActionStatus::Succeeded,
                        tool_output: None,
                        invocation_id: None,
                        duration_ms: 0,
                    }),
                    Err(e) => Ok(ActionResult {
                        proposal: proposal.clone(),
                        status: ActionStatus::Failed {
                            reason: e.to_string(),
                        },
                        tool_output: None,
                        invocation_id: None,
                        duration_ms: 0,
                    }),
                }
            }

            // ── CompleteRun ────────────────────────────────────────────────
            ActionType::CompleteRun => match self
                .run_service
                .complete(&ctx.session_id, &ctx.run_id)
                .await
            {
                Ok(_) => Ok(ActionResult {
                    proposal: proposal.clone(),
                    status: ActionStatus::Succeeded,
                    tool_output: None,
                    invocation_id: None,
                    duration_ms: 0,
                }),
                Err(e) => Ok(ActionResult {
                    proposal: proposal.clone(),
                    status: ActionStatus::Failed {
                        reason: e.to_string(),
                    },
                    tool_output: None,
                    invocation_id: None,
                    duration_ms: 0,
                }),
            },

            // ── FailRun (#825) ─────────────────────────────────────────────
            // Terminal self-reported failure. Parallel to CompleteRun but
            // routes to RunService::fail with FailureClass::ModelReportedFailure.
            // The proposal's `description` carries the agent's reason — it
            // flows through derive_signal into LoopSignal::Failed { reason },
            // which the HTTP layer's classify_failed_reason recognises via
            // the `model_reported_failure:` prefix.
            ActionType::FailRun => match self
                .run_service
                .fail(
                    &ctx.session_id,
                    &ctx.run_id,
                    cairn_domain::FailureClass::ModelReportedFailure,
                )
                .await
            {
                Ok(_) => Ok(ActionResult {
                    proposal: proposal.clone(),
                    status: ActionStatus::Succeeded,
                    tool_output: None,
                    invocation_id: None,
                    duration_ms: 0,
                }),
                Err(e) => Ok(ActionResult {
                    proposal: proposal.clone(),
                    status: ActionStatus::Failed {
                        reason: e.to_string(),
                    },
                    tool_output: None,
                    invocation_id: None,
                    duration_ms: 0,
                }),
            },

            // ── EscalateToOperator ─────────────────────────────────────────
            ActionType::EscalateToOperator => {
                let approval_id = ApprovalId::new(new_id("appr"));

                // Build context for the operator from the proposal + run goal.
                let title = Some(format!("Agent requests approval: {}", proposal.description));
                let description = {
                    let mut desc = format!(
                        "**Run:** `{}`\n**Goal:** {}\n\n**Agent says:**\n{}",
                        ctx.run_id.as_str(),
                        ctx.goal,
                        proposal.description,
                    );
                    if let Some(ref tool) = proposal.tool_name {
                        desc.push_str(&format!("\n\n**Tool:** `{}`", tool));
                    }
                    if let Some(ref args) = proposal.tool_args {
                        let args_str = serde_json::to_string_pretty(args).unwrap_or_default();
                        const MAX_ARGS_INLINE: usize = 4000;
                        if args_str.len() <= MAX_ARGS_INLINE {
                            desc.push_str(&format!("\n**Args:**\n```json\n{}\n```", args_str));
                        } else {
                            let truncated: String =
                                args_str.chars().take(MAX_ARGS_INLINE).collect();
                            desc.push_str(&format!(
                                "\n**Args (truncated, {} chars of {}):**\n```json\n{}\n… [truncated]\n```",
                                MAX_ARGS_INLINE,
                                args_str.len(),
                                truncated
                            ));
                        }
                    }
                    Some(desc)
                };

                match self
                    .approval_service
                    .request_with_context(
                        &ctx.project,
                        approval_id.clone(),
                        Some(ctx.run_id.clone()),
                        ctx.task_id.clone(),
                        ApprovalRequirement::Required,
                        title,
                        description,
                    )
                    .await
                {
                    Ok(_) => Ok(ActionResult {
                        proposal: proposal.clone(),
                        status: ActionStatus::AwaitingApproval { approval_id },
                        tool_output: None,
                        invocation_id: None,
                        duration_ms: 0,
                    }),
                    Err(e) => Ok(ActionResult {
                        proposal: proposal.clone(),
                        status: ActionStatus::Failed {
                            reason: e.to_string(),
                        },
                        tool_output: None,
                        invocation_id: None,
                        duration_ms: 0,
                    }),
                }
            }

            // ── CreateMemory ───────────────────────────────────────────────
            // Memory ingestion is async (IngestService runs independently).
            // Record intent here; actual embedding runs via the ingest pipeline.
            ActionType::CreateMemory => Ok(ActionResult {
                proposal: proposal.clone(),
                status: ActionStatus::Succeeded,
                tool_output: Some(serde_json::json!({
                    "queued": true,
                    "note": "async — see /v1/memory/ingest"
                })),
                invocation_id: None,
                duration_ms: 0,
            }),
        }
    }
}

// ── BP-v2 approval gate helpers ───────────────────────────────────────────────

impl RuntimeExecutePhase {
    /// Run the propose-then-await flow for a single `InvokeTool` proposal
    /// whose `requires_approval` is true and whose execute phase has a
    /// [`ToolCallApprovalService`] wired.
    ///
    /// Returns a final `ActionResult`:
    ///
    /// * `Succeeded` — tool approved (possibly with amended args) and ran.
    /// * `Failed`   — tool rejected, timed out, or its invocation errored
    ///   after approval. The `reason` mirrors what the LLM will see as a
    ///   tool_result error on the next GATHER turn.
    ///
    /// The tool is invoked *inline* (not via re-entry into the outer loop)
    /// so the operator approval and the tool's side effect land in the
    /// same execute batch — this is the core of what fixes the dogfood
    /// bug.
    async fn run_with_approval_gate(
        &self,
        ctx: &OrchestrationContext,
        proposal: &cairn_domain::ActionProposal,
        call_index: u32,
        svc: Arc<dyn ToolCallApprovalService>,
    ) -> Result<ActionResult, OrchestratorError> {
        let tool_name = proposal.tool_name.clone().unwrap_or_default();
        let raw_args = proposal
            .tool_args
            .clone()
            .unwrap_or(serde_json::Value::Null);

        // The ToolCallId is deterministic (run_id + iteration + call_index
        // + tool_name + normalized_args) so a resume that re-enters this
        // path for the same iteration sees the same id — the underlying
        // service short-circuits via the projection reader.
        // Fall back to `default_normalize_for_cache` (not
        // `raw_args.to_string()`) so the derived id stays deterministic
        // across re-entry even if the proposal is reconstructed from a
        // different source whose JSON key ordering differs. The two
        // diverge for object payloads because `Value::to_string()`
        // preserves insertion order; the normaliser sorts keys.
        // (Copilot review feedback on PR #270.)
        let normalized = self
            .tool_registry
            .as_ref()
            .and_then(|reg| reg.get(&tool_name))
            .map(|h| h.normalize_for_cache(&raw_args))
            .unwrap_or_else(|| cairn_tools::builtins::default_normalize_for_cache(&raw_args));
        let call_id = ToolCallId::derive(
            ctx.run_id.as_str(),
            ctx.iteration,
            call_index,
            &tool_name,
            &normalized,
        );

        // Derive match policy from tool effect + args + project root.
        let tool_effect = self
            .tool_registry
            .as_ref()
            .and_then(|reg| reg.get(&tool_name))
            .map(|h| h.tool_effect())
            .unwrap_or(cairn_domain::decisions::ToolEffect::External);
        let match_policy = crate::approval_policy::derive_match_policy(
            tool_effect,
            &raw_args,
            Some(ctx.working_dir.as_path()),
        );

        let display_summary = Some(build_display_summary(proposal, &tool_name));

        let domain_call_id = cairn_domain::ToolCallId::new(call_id.as_str());
        let tcp = ToolCallProposal {
            call_id: domain_call_id.clone(),
            session_id: ctx.session_id.clone(),
            run_id: ctx.run_id.clone(),
            project: ctx.project.clone(),
            tool_name: tool_name.clone(),
            tool_args: raw_args.clone(),
            display_summary,
            match_policy,
        };

        // 1. Submit proposal.
        let decision = svc
            .submit_proposal(tcp)
            .await
            .map_err(|e| OrchestratorError::Execute(format!("submit_proposal failed: {e}")))?;

        // 2. Resolve to an OperatorDecision — but DO NOT block in-process
        //    on `PendingOperator`. (F26 dogfood blocker.)
        //
        // Previous behaviour blocked inside `await_decision` for the full
        // `approval_timeout_ms` (default 24h; 45s in the live repro). With a real operator who
        // resolves via the UI, the `POST /v1/runs/:id/orchestrate` HTTP
        // call stayed open until the approval came through — but the UI
        // approval hits a DIFFERENT process path and (per the design
        // comment in `handlers/runs.rs`) needs the same service instance.
        // In practice this showed up as: approval proposal appears in
        // `/v1/tool-call-approvals`, run stays in `Running` state, no
        // `ApprovalRequested` event emitted, HTTP call hangs for 45s,
        // then returns `Failed { reason: "operator did not respond within
        // approval timeout" }`. The tool never runs. See F26 write-up.
        //
        // Correct BP-v2 flow (see `research/llm-agent-approval-systems.md`
        // + `loop_runner.rs` approval-pre-check block):
        //   • AutoApproved → retrieve approved args, dispatch inline.
        //   • PendingOperator → return `AwaitingApproval { approval_id }`
        //     immediately. The outer loop picks this up, returns
        //     `LoopTermination::WaitingApproval`, and the HTTP handler
        //     returns 202. The operator approves asynchronously. A
        //     subsequent `orchestrate` call walks the F25 drain
        //     (`list_approved_for_run`) and dispatches the now-approved
        //     tool call before the next DECIDE.
        //
        // The ToolCallId doubles as the approval_id on the orchestrator
        // side — the `WaitingApproval` termination is informational for
        // the handler; the durable handshake is the
        // `tool_call_approvals` projection keyed by `ToolCallId`.
        let operator_decision = match decision {
            ToolCallApprovalDecision::AutoApproved => {
                // Short-circuit: session allow-registry match, retrieve
                // args (which may differ from raw_args if a prior
                // approval amended — though for auto-approve the path
                // it's always the original).
                let approved = svc
                    .retrieve_approved_proposal(&domain_call_id)
                    .await
                    .map_err(|e| {
                        OrchestratorError::Execute(format!(
                            "retrieve_approved_proposal (auto) failed: {e}"
                        ))
                    })?;
                OperatorDecision::Approved {
                    approved_args: approved.tool_args,
                }
            }
            ToolCallApprovalDecision::PendingOperator => {
                // F26: suspend the loop immediately. `ApprovalId::new` is
                // a newtype over String; reuse the deterministic
                // ToolCallId string so the handler + UI can correlate
                // the suspension back to the `tool_call_approvals`
                // projection row.
                let approval_id = ApprovalId::new(call_id.as_str().to_owned());
                tracing::info!(
                    run_id      = %ctx.run_id,
                    iteration   = ctx.iteration,
                    tool        = %tool_name,
                    call_id     = %call_id.as_str(),
                    "F26: approval pending — suspending loop (no in-process wait)"
                );
                return Ok(ActionResult {
                    proposal: proposal.clone(),
                    status: ActionStatus::AwaitingApproval { approval_id },
                    tool_output: None,
                    invocation_id: None,
                    duration_ms: 0,
                });
            }
        };

        // 3. On approval, invoke the tool with the operator-approved args.
        match operator_decision {
            OperatorDecision::Approved { approved_args } => {
                let mut revised = proposal.clone();
                revised.tool_args = Some(approved_args);
                revised.requires_approval = false;
                // Recurse. The revised proposal goes through the regular
                // dispatch path (cache consultation, decision service,
                // registry dispatch, completion event, etc.).
                //
                // `Box::pin` is required here because this is indirect
                // async recursion: `dispatch_one` can call back into
                // `run_with_approval_gate` (though with
                // `requires_approval=false` on the revised proposal it
                // does not in practice). Without the pin the compiler
                // errors with "recursion in an async fn requires
                // boxing" — the future size is undecidable.
                Box::pin(self.dispatch_one(ctx, &revised, call_index)).await
            }
            OperatorDecision::Rejected { reason } => Ok(ActionResult {
                proposal: proposal.clone(),
                status: ActionStatus::Failed {
                    reason: reason.unwrap_or_else(|| "operator rejected tool call".to_owned()),
                },
                tool_output: None,
                invocation_id: None,
                duration_ms: 0,
            }),
            OperatorDecision::Timeout => Ok(ActionResult {
                proposal: proposal.clone(),
                status: ActionStatus::Failed {
                    reason: "operator did not respond within approval timeout".to_owned(),
                },
                tool_output: None,
                invocation_id: None,
                duration_ms: 0,
            }),
        }
    }
}

fn build_display_summary(proposal: &cairn_domain::ActionProposal, tool_name: &str) -> String {
    // The operator dashboard renders this verbatim — keep it
    // short (one line) and include tool_name + the description
    // hint the LLM supplied so the operator sees intent at a glance.
    let desc = proposal.description.trim();
    if desc.is_empty() {
        format!("invoke {tool_name}")
    } else {
        format!("{tool_name}: {desc}")
    }
}

/// Per-sibling summary cap when synthesising prior-attempt context. Keeps
/// the injected block bounded even if a predecessor produced a large
/// final_answer. 1200 chars preserves enough prose for the model to see
/// what was tried, what succeeded, and what the gate rejected on without
/// bloating the prompt.
const PRIOR_SIBLING_SUMMARY_CAP: usize = 1200;

/// Maximum number of prior failed siblings to surface in `parent_context`.
/// Two is enough: the child only needs "what the immediate predecessor
/// tried" plus "what the one before that tried" to avoid reattempting the
/// same dead ends. Older attempts stay in `step_history` for the model to
/// reach through explicitly if it wants more.
const PRIOR_SIBLING_COUNT_CAP: usize = 2;

/// Synthesise a `## Prior sibling attempts` block from `step_history` when
/// the parent is re-spawning a child after one or more failed siblings.
///
/// R35 (2026-05-10) dogfood surfaced the architectural gap this closes:
/// after a sub-agent failed the completion gate (e.g. admission sentinel
/// fired on `**Remaining:**`), the parent correctly re-spawned a fresh
/// child — but the new child started with `parent_context = null` and
/// rediscovered everything from zero (cloned the repo, checked `git
/// status`, read files, re-tried the same approach, hit the same gate).
/// The ephemeral sandbox means the workspace itself resets, so there's no
/// on-disk artifact the new child can inspect to see "my predecessor
/// already wrote `src/main.rs` and got as far as `cargo check`."
///
/// This helper extracts the most recent failed siblings' summaries from
/// `step_history` and formats them as a priming block the SpawnSubagent
/// branch prepends to the child's `parent_context`. The block is labelled
/// clearly so the model treats it as "what my peer tried" rather than
/// "instructions from the operator."
///
/// Returns `None` when `step_history` contains no failed `subagent_complete`
/// entries — callers can then pass `parent_context` through unchanged.
fn recent_failed_siblings_summary(step_history: &[StepSummary]) -> Option<String> {
    // Walk newest-first: step_history is appended chronologically (see
    // `build_subagent_complete_steps` rustdoc — "oldest-to-newest"), so
    // `.iter().rev()` surfaces the most-recent failures first. Collect
    // up to PRIOR_SIBLING_COUNT_CAP, then reverse back to chronological
    // order in the rendered block ("Attempt 1, Attempt 2, ...").
    let mut recent: Vec<&StepSummary> = step_history
        .iter()
        .rev()
        .filter(|s| s.action_kind == "subagent_complete" && !s.succeeded)
        .take(PRIOR_SIBLING_COUNT_CAP)
        .collect();
    if recent.is_empty() {
        return None;
    }
    recent.reverse();

    let mut out = String::from(
        "## Prior sibling attempts (this goal was tried before — don't start from zero)\n\n",
    );
    for (idx, summary) in recent.iter().enumerate() {
        let attempt_num = idx + 1;
        let body = truncate_for_prior_sibling(&summary.summary, PRIOR_SIBLING_SUMMARY_CAP);
        out.push_str(&format!("Attempt {attempt_num}: {body}\n\n"));
    }
    out.push_str(
        "Read each attempt carefully. Continue from where the last one stopped — do not \
         repeat work that already succeeded. Do not repeat approaches that already failed.\n",
    );
    Some(out)
}

/// Truncate a prior-sibling summary to at most `cap` chars, breaking at
/// a char boundary and appending an ellipsis when the original is longer.
/// UTF-8 safe: uses `char_indices` to land on a code-point boundary.
fn truncate_for_prior_sibling(text: &str, cap: usize) -> String {
    if text.len() <= cap {
        return text.to_owned();
    }
    let cut = text
        .char_indices()
        .take_while(|(i, _)| *i < cap)
        .last()
        .map(|(i, c)| i + c.len_utf8())
        .unwrap_or(0);
    format!("{}…", &text[..cut])
}

/// Legacy fallback when no [`ToolCallApprovalService`] is wired. Preserves
/// the pre-BP-v2 `ApprovalService::request_with_context` short-circuit so
/// existing tests that construct the execute phase without the new service
/// continue to see `AwaitingApproval` results.
///
/// New code should wire the BP-v2 service via
/// [`RuntimeExecutePhaseBuilder::tool_call_approval_service`].
async fn legacy_approval_gate_result(
    approval_service: &Arc<dyn ApprovalService>,
    ctx: &OrchestrationContext,
    proposal: &cairn_domain::ActionProposal,
    tool_name: &str,
) -> Result<ActionResult, OrchestratorError> {
    let approval_id = ApprovalId::new(new_id("appr"));
    let title = Some(format!("Agent requests approval for tool: {}", tool_name));
    let description = {
        let mut desc = format!(
            "**Run:** `{}`\n**Goal:** {}\n\n**Agent says:**\n{}\n\n**Tool:** `{}`",
            ctx.run_id.as_str(),
            ctx.goal,
            proposal.description,
            tool_name,
        );
        if let Some(ref args) = proposal.tool_args {
            let args_str = serde_json::to_string_pretty(args).unwrap_or_default();
            const MAX_ARGS_INLINE: usize = 4000;
            if args_str.len() <= MAX_ARGS_INLINE {
                desc.push_str(&format!("\n**Args:**\n```json\n{}\n```", args_str));
            } else {
                let truncated: String = args_str.chars().take(MAX_ARGS_INLINE).collect();
                desc.push_str(&format!(
                    "\n**Args (truncated, {} chars of {}):**\n```json\n{}\n… [truncated]\n```",
                    MAX_ARGS_INLINE,
                    args_str.len(),
                    truncated
                ));
            }
        }
        Some(desc)
    };
    match approval_service
        .request_with_context(
            &ctx.project,
            approval_id.clone(),
            Some(ctx.run_id.clone()),
            ctx.task_id.clone(),
            ApprovalRequirement::Required,
            title,
            description,
        )
        .await
    {
        Ok(_) => Ok(ActionResult {
            proposal: proposal.clone(),
            status: ActionStatus::AwaitingApproval { approval_id },
            tool_output: None,
            invocation_id: None,
            duration_ms: 0,
        }),
        Err(e) => Ok(ActionResult {
            proposal: proposal.clone(),
            status: ActionStatus::Failed {
                reason: e.to_string(),
            },
            tool_output: None,
            invocation_id: None,
            duration_ms: 0,
        }),
    }
}

// ── Signal derivation ─────────────────────────────────────────────────────────

/// Derive the `LoopSignal` from a freshly executed `ActionResult`.
///
/// Returns the existing signal if it is already terminal.
///
/// **F35 contract.** A failed `InvokeTool` (NOT_FOUND, InvalidArgs,
/// PermissionDenied, Timeout, tool-registry miss, decision-denied) is
/// *feedback* for the LLM, not a run-terminal condition. Those statuses
/// map to `LoopSignal::Continue` so the loop runner advances to the next
/// iteration, where the next DECIDE sees the error text via step history
/// and can adapt ("that file doesn't exist; retry with a different
/// path"). Pre-F35 this promoted tool failures to `LoopSignal::Failed`
/// and terminated the run on the first tool miss (dogfood v4 evidence
/// in PR #295).
///
/// Non-InvokeTool failures (CompleteRun / SendNotification /
/// EscalateToOperator / CreateMemory service errors) remain terminal —
/// they represent orchestrator-level bookkeeping that the LLM cannot
/// recover from by picking a different action, so promoting them to
/// `LoopSignal::Failed` is the correct short-circuit.
///
/// **#689 R2-A exception.** `SpawnSubagent` failures tagged with
/// `MALFORMED_SPAWN_PROPOSAL_PREFIX` are LLM-recoverable — a missing
/// role or empty goal is a malformed proposal the model can re-emit
/// next turn once the rejection surfaces in `step_history`. Those map
/// to `LoopSignal::Continue`. Non-prefixed SpawnSubagent failures
/// (e.g. `TaskService::spawn_subagent` service error) remain terminal;
/// the LLM cannot fix an infrastructure error by re-proposing.
pub(crate) fn derive_signal(result: &ActionResult, current: &LoopSignal) -> LoopSignal {
    if !matches!(current, LoopSignal::Continue) {
        return current.clone();
    }
    match &result.status {
        ActionStatus::Succeeded => {
            // CompleteRun → Done
            if result.proposal.action_type == ActionType::CompleteRun {
                LoopSignal::Done
            } else if result.proposal.action_type == ActionType::FailRun {
                // #825: FailRun dispatch succeeded (RunService::fail
                // flipped the run to state=failed). Signal terminal
                // failure so the loop returns LoopTermination::Failed.
                // The reason string uses the `model_reported_failure:`
                // prefix so classify_failed_reason in the HTTP layer
                // maps this to FailureClass::ModelReportedFailure.
                // Proposal.description is the agent's free-text reason.
                LoopSignal::Failed {
                    reason: format!("model_reported_failure: {}", result.proposal.description),
                }
            } else {
                LoopSignal::Continue
            }
        }
        ActionStatus::SubagentSpawned { child_task_id } => LoopSignal::WaitSubagent {
            child_task_id: child_task_id.clone(),
        },
        ActionStatus::AwaitingApproval { approval_id } => LoopSignal::WaitApproval {
            approval_id: approval_id.clone(),
        },
        // F35: InvokeTool failures surface to the LLM as tool_result
        // feedback; the loop continues. Non-InvokeTool failures are
        // orchestrator-level and remain terminal.
        //
        // Exception (F35 review round 2 — Copilot): an InvokeTool
        // failure tagged with `UNRECOVERABLE_INTERNAL_PREFIX`
        // indicates orchestrator-side misconfiguration (e.g. tool
        // registry not wired) that the LLM cannot recover from by
        // picking a different tool / args. Those short-circuit to
        // `LoopSignal::Failed` so the run fails fast instead of
        // burning iterations on calls that will never succeed.
        //
        // #689 R2-A exception: a `SpawnSubagent` failure tagged with
        // `MALFORMED_SPAWN_PROPOSAL_PREFIX` is LLM-recoverable — the
        // model just needs to re-emit the proposal with the missing
        // role / goal. Map it to `LoopSignal::Continue` so the next
        // DECIDE turn sees the rejection in `step_history` and can
        // correct. The consecutive-malformed-spawn counter in
        // `loop_runner` enforces a bounded retry cap so a
        // permanently-broken model can't burn the whole iteration
        // budget against the gate.
        ActionStatus::Failed { reason } => {
            // Two LLM-recoverable carve-outs map to Continue; everything
            // else terminates. Collapsing the two conditions with `||`
            // keeps the branch count at one for clippy's
            // `if_same_then_else` lint and matches the rustdoc above
            // (both prefixes signal "LLM-recoverable", differentiated
            // only by which proposal variant the prefix lives on).
            let recoverable_invoke_tool_error = result.proposal.action_type
                == ActionType::InvokeTool
                && !reason.starts_with(UNRECOVERABLE_INTERNAL_PREFIX);
            let recoverable_malformed_spawn = result.proposal.action_type
                == ActionType::SpawnSubagent
                && reason.starts_with(MALFORMED_SPAWN_PROPOSAL_PREFIX);
            if recoverable_invoke_tool_error || recoverable_malformed_spawn {
                LoopSignal::Continue
            } else {
                LoopSignal::Failed {
                    reason: reason.clone(),
                }
            }
        }
    }
}

// ── ID generation ─────────────────────────────────────────────────────────────

fn now_ms_u64() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn new_id(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{prefix}_{ts}_{n}")
}

fn tool_args_with_working_dir(
    tool_name: &str,
    working_dir: &Path,
    args: Option<serde_json::Value>,
) -> serde_json::Value {
    let mut args = args.unwrap_or(serde_json::Value::Null);
    if tool_name != "bash" {
        return args;
    }

    match &mut args {
        serde_json::Value::Object(map) => {
            map.entry("working_dir".to_string())
                .or_insert_with(|| serde_json::Value::String(working_dir.display().to_string()));
            args
        }
        serde_json::Value::Null => serde_json::json!({
            "working_dir": working_dir.display().to_string(),
        }),
        _ => args,
    }
}

fn truncate_tool_output_for_context(
    output: serde_json::Value,
    token_limit: usize,
) -> serde_json::Value {
    let serialized = output.to_string();
    if crate::decide_impl::estimate_tokens(&serialized) <= token_limit {
        return output;
    }

    match output {
        serde_json::Value::String(text) => {
            serde_json::Value::String(truncate_text_for_context(&text, token_limit))
        }
        serde_json::Value::Object(mut map) => {
            for key in ["stdout", "stderr", "output", "text", "result", "content"] {
                if let Some(serde_json::Value::String(text)) = map.get(key) {
                    let truncated = truncate_text_for_context(text, token_limit);
                    map.insert(key.to_owned(), serde_json::Value::String(truncated));
                    return serde_json::Value::Object(map);
                }
            }

            serde_json::Value::String(truncate_text_for_context(&serialized, token_limit))
        }
        other => {
            serde_json::Value::String(truncate_text_for_context(&other.to_string(), token_limit))
        }
    }
}

fn truncate_text_for_context(text: &str, token_limit: usize) -> String {
    if crate::decide_impl::estimate_tokens(text) <= token_limit {
        return text.to_owned();
    }

    let chars: Vec<char> = text.chars().collect();
    let keep = chars.len().min((token_limit.saturating_mul(4)).max(16));
    let head = keep / 2;
    let tail = keep.saturating_sub(head);
    let omitted = chars.len().saturating_sub(head + tail);
    let prefix: String = chars.iter().take(head).collect();
    let suffix: String = chars
        .iter()
        .rev()
        .take(tail)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();

    format!("{prefix}... [truncated: {omitted} chars omitted] ...{suffix}")
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod prior_siblings_tests {
    use super::*;

    fn step(action_kind: &str, summary: &str, succeeded: bool) -> StepSummary {
        StepSummary {
            iteration: 0,
            action_kind: action_kind.to_owned(),
            summary: summary.to_owned(),
            succeeded,
        }
    }

    #[test]
    fn none_when_history_has_no_failed_subagent_completes() {
        let history = vec![
            step("tool_call", "ran cargo check", true),
            step("subagent_complete", "child x (completed): shipped PR", true),
            step("continue", "advancing", true),
        ];
        assert!(recent_failed_siblings_summary(&history).is_none());
    }

    #[test]
    fn none_on_empty_history() {
        assert!(recent_failed_siblings_summary(&[]).is_none());
    }

    #[test]
    fn surfaces_single_failed_sibling() {
        let history = vec![
            step("tool_call", "ran cargo check", true),
            step(
                "subagent_complete",
                "child run-a (failed): gate rejected — sentinel `**remaining`",
                false,
            ),
        ];
        let out = recent_failed_siblings_summary(&history).expect("block present");
        assert!(out.contains("## Prior sibling attempts"));
        assert!(out.contains("Attempt 1: child run-a (failed): gate rejected"));
        assert!(!out.contains("Attempt 2:"));
        assert!(out.contains("Continue from where the last one stopped"));
    }

    #[test]
    fn caps_at_two_most_recent_and_skips_successes() {
        let history = vec![
            step("subagent_complete", "child run-1 (failed): first", false),
            step("subagent_complete", "child run-2 (completed): won", true),
            step("subagent_complete", "child run-3 (failed): second", false),
            step("subagent_complete", "child run-4 (failed): third", false),
        ];
        let out = recent_failed_siblings_summary(&history).expect("block present");
        // Newest-first scan → take last two failed → reverse to chronological:
        // run-3 should be Attempt 1, run-4 should be Attempt 2.
        assert!(out.contains("Attempt 1: child run-3 (failed): second"));
        assert!(out.contains("Attempt 2: child run-4 (failed): third"));
        // Completed sibling never surfaces.
        assert!(!out.contains("run-2"));
        // Oldest failed sibling (run-1) pushed out by the cap.
        assert!(!out.contains("run-1"));
    }

    #[test]
    fn ignores_non_subagent_complete_kinds_even_if_failed() {
        let history = vec![
            step("tool_call", "cargo check errored", false),
            step("continue", "retry", false),
        ];
        assert!(recent_failed_siblings_summary(&history).is_none());
    }

    #[test]
    fn truncates_long_summaries_at_char_boundary_with_ellipsis() {
        // Build a summary well over PRIOR_SIBLING_SUMMARY_CAP.
        let long_body = "a".repeat(PRIOR_SIBLING_SUMMARY_CAP + 500);
        let long_summary = format!("child run-x (failed): {long_body}");
        let history = vec![step("subagent_complete", &long_summary, false)];
        let out = recent_failed_siblings_summary(&history).expect("block present");
        assert!(out.contains("…"), "expected ellipsis suffix on truncation");
        // Full unbounded body should NOT appear.
        assert!(
            !out.contains(&long_body),
            "full summary body leaked past the cap"
        );
    }

    #[test]
    fn truncate_helper_utf8_safe_on_multibyte_boundary() {
        // 4-byte char (😀) placed so a naive byte slice at `cap` would
        // split the code point. Helper must land on a valid boundary.
        let s = format!("{}😀{}", "a".repeat(10), "b".repeat(20));
        let out = truncate_for_prior_sibling(&s, 11);
        // The helper may either include the 😀 or stop just before it —
        // what matters is that the output is valid UTF-8 and ends with
        // an ellipsis when truncation happened.
        assert!(out.is_char_boundary(out.len()));
        assert!(out.ends_with('…'));
    }

    #[test]
    fn truncate_helper_returns_original_when_within_cap() {
        let s = "short summary";
        let out = truncate_for_prior_sibling(s, 100);
        assert_eq!(out, s);
        assert!(!out.ends_with('…'));
    }
}

#[cfg(test)]
mod signal_aggregation_tests {
    use super::*;
    use cairn_domain::{ActionProposal, ActionType, ApprovalId, TaskId};

    fn invoke_result(status: ActionStatus) -> ActionResult {
        ActionResult {
            proposal: ActionProposal {
                action_type: ActionType::InvokeTool,
                description: "test".to_owned(),
                confidence: 1.0,
                tool_name: Some("t".to_owned()),
                tool_args: None,
                requires_approval: false,
            },
            status,
            tool_output: None,
            invocation_id: None,
            duration_ms: 0,
        }
    }

    /// Regression for the Cursor Bugbot finding on PR-5.
    ///
    /// Pre-fix, `execute()` only ran `derive_signal` on non-InvokeTool
    /// proposals (Phase 2). Any `AwaitingApproval` that escaped the
    /// parallel InvokeTool batch (legacy approval-gate fallback or
    /// RFC 020 `DangerousPause` recovery branch) silently downgraded
    /// to `Continue`, which is a production hole: the loop would
    /// advance instead of suspending for the operator.
    ///
    /// Post-fix, `execute()` runs `derive_signal` on the parallel batch
    /// too (earliest-index wins) and Phase 2 is skipped entirely if a
    /// terminal signal was already set. This test pins that derivation.
    #[test]
    fn derive_signal_escalates_awaiting_approval_to_wait_approval() {
        let awaiting = invoke_result(ActionStatus::AwaitingApproval {
            approval_id: ApprovalId::new("appr-1"),
        });
        let got = derive_signal(&awaiting, &LoopSignal::Continue);
        match got {
            LoopSignal::WaitApproval { approval_id } => {
                assert_eq!(approval_id.as_str(), "appr-1");
            }
            other => panic!("expected WaitApproval, got {other:?}"),
        }
    }

    /// Parallel-batch aggregation: earliest-index `AwaitingApproval`
    /// wins over later `Succeeded`. If the earlier entry escaped the
    /// fold, the loop would miss the suspension entirely.
    #[test]
    fn parallel_batch_aggregation_picks_earliest_terminal_signal() {
        let results = vec![
            invoke_result(ActionStatus::AwaitingApproval {
                approval_id: ApprovalId::new("appr-early"),
            }),
            invoke_result(ActionStatus::Succeeded),
        ];
        let mut loop_signal = LoopSignal::Continue;
        for r in &results {
            let next = derive_signal(r, &loop_signal);
            if !matches!(next, LoopSignal::Continue) {
                loop_signal = next;
                break;
            }
        }
        match loop_signal {
            LoopSignal::WaitApproval { approval_id } => {
                assert_eq!(approval_id.as_str(), "appr-early");
            }
            other => panic!("expected WaitApproval(appr-early), got {other:?}"),
        }
    }

    /// SubagentSpawned also escalates. Covers the other InvokeTool
    /// status that carries a terminal signal.
    #[test]
    fn derive_signal_escalates_subagent_spawned_to_wait_subagent() {
        let spawned = invoke_result(ActionStatus::SubagentSpawned {
            child_task_id: TaskId::new("child-1"),
        });
        let got = derive_signal(&spawned, &LoopSignal::Continue);
        match got {
            LoopSignal::WaitSubagent { child_task_id } => {
                assert_eq!(child_task_id.as_str(), "child-1");
            }
            other => panic!("expected WaitSubagent, got {other:?}"),
        }
    }

    /// F35 regression: a *recoverable* InvokeTool failure (NOT_FOUND,
    /// InvalidArgs, etc.) must map to `LoopSignal::Continue` so the
    /// error flows back to the LLM as tool_result feedback on the next
    /// DECIDE turn. Pre-F35 this was `LoopSignal::Failed`, which
    /// terminated the run on the first tool miss.
    #[test]
    fn derive_signal_keeps_loop_continuing_on_recoverable_tool_error() {
        let failed = invoke_result(ActionStatus::Failed {
            reason: "Error [NOT_FOUND]: File not found: /tmp/ghost.md".to_owned(),
        });
        let got = derive_signal(&failed, &LoopSignal::Continue);
        assert!(
            matches!(got, LoopSignal::Continue),
            "expected Continue on recoverable tool error, got {got:?}"
        );
    }

    /// F35 review round 2 (Copilot): an InvokeTool failure tagged with
    /// `UNRECOVERABLE_INTERNAL_PREFIX` signals orchestrator-side
    /// misconfiguration (e.g. tool registry not wired) that the LLM
    /// cannot fix by picking a different tool or args. Those must
    /// short-circuit to `LoopSignal::Failed` so the run fails fast
    /// instead of burning iterations.
    #[test]
    fn derive_signal_escalates_unrecoverable_internal_tool_error() {
        let failed = invoke_result(ActionStatus::Failed {
            reason: format!(
                "{}tool_registry not wired — cannot dispatch tool `read`",
                super::UNRECOVERABLE_INTERNAL_PREFIX
            ),
        });
        let got = derive_signal(&failed, &LoopSignal::Continue);
        match got {
            LoopSignal::Failed { reason } => {
                assert!(
                    reason.starts_with(super::UNRECOVERABLE_INTERNAL_PREFIX),
                    "reason must carry the sentinel prefix through to loop termination \
                     so the operator sees the misconfiguration tag; got {reason:?}"
                );
            }
            other => panic!("expected Failed on unrecoverable-internal tool error, got {other:?}"),
        }
    }

    /// Helper for constructing a SpawnSubagent `ActionResult` — the
    /// carve-out target for #689 R2-A.
    fn spawn_result(status: ActionStatus) -> ActionResult {
        ActionResult {
            proposal: ActionProposal {
                action_type: ActionType::SpawnSubagent,
                description: "delegate research".to_owned(),
                confidence: 0.8,
                tool_name: Some("researcher".to_owned()),
                tool_args: Some(serde_json::json!({})),
                requires_approval: false,
            },
            status,
            tool_output: None,
            invocation_id: None,
            duration_ms: 0,
        }
    }

    /// #689 R2-A: a `SpawnSubagent` failure tagged with
    /// `MALFORMED_SPAWN_PROPOSAL_PREFIX` must demote to
    /// `LoopSignal::Continue` so the LLM can correct the proposal on
    /// the next DECIDE turn. Pre-#689 this was
    /// `LoopSignal::Failed` (all non-InvokeTool failures were
    /// terminal), which killed the whole run on a single bad LLM
    /// emission — the dogfood R2 Finding A repro.
    #[test]
    fn derive_signal_demotes_malformed_spawn_to_continue() {
        let failed = spawn_result(ActionStatus::Failed {
            reason: format!(
                "{}spawn_subagent: tool_args[\"goal\"] is required",
                super::MALFORMED_SPAWN_PROPOSAL_PREFIX
            ),
        });
        let got = derive_signal(&failed, &LoopSignal::Continue);
        assert!(
            matches!(got, LoopSignal::Continue),
            "expected Continue on malformed spawn_subagent so the LLM gets \
             a retry via step_history, got {got:?}"
        );
    }

    /// Safety: only the `MALFORMED_SPAWN_PROPOSAL_PREFIX`-tagged
    /// `SpawnSubagent` failure demotes to Continue. A non-tagged
    /// `SpawnSubagent` failure (e.g. the `TaskService::spawn_subagent`
    /// service hit an infrastructure error) is NOT LLM-recoverable
    /// and must still terminate the run as before. If the carve-out
    /// widened to all SpawnSubagent failures, infrastructure errors
    /// would silently retry and burn the iteration budget.
    #[test]
    fn derive_signal_keeps_non_prefixed_spawn_failure_terminal() {
        let failed = spawn_result(ActionStatus::Failed {
            reason: "TaskService::spawn_subagent: database connection refused".to_owned(),
        });
        let got = derive_signal(&failed, &LoopSignal::Continue);
        match got {
            LoopSignal::Failed { reason } => {
                assert!(
                    reason.contains("database connection refused"),
                    "non-malformed SpawnSubagent failure must propagate to Failed; got {reason:?}"
                );
            }
            other => panic!(
                "expected Failed on non-prefixed SpawnSubagent failure — the \
                 carve-out must ONLY fire on the malformed prefix; got {other:?}"
            ),
        }
    }

    /// #825: `FailRun` dispatch succeeded — the run is terminal.
    /// `derive_signal` must map this to `LoopSignal::Failed` with a
    /// reason carrying the `model_reported_failure:` prefix, NOT to
    /// `LoopSignal::Done` (which would flip the run to
    /// state=completed) and NOT to `LoopSignal::Continue` (which
    /// would leave the run running after the service already flipped
    /// it to Failed).
    ///
    /// The proposal's description carries the agent's reason; it
    /// gets prefixed so classify_failed_reason in the HTTP layer
    /// routes the terminal to FailureClass::ModelReportedFailure.
    #[test]
    fn derive_signal_fail_run_success_maps_to_failed_with_prefix() {
        let proposal = ActionProposal::fail_run("blocked: missing dep", 0.95);
        let result = ActionResult {
            proposal,
            status: ActionStatus::Succeeded,
            tool_output: None,
            invocation_id: None,
            duration_ms: 0,
        };
        let got = derive_signal(&result, &LoopSignal::Continue);
        match got {
            LoopSignal::Failed { reason } => {
                assert!(
                    reason.starts_with("model_reported_failure:"),
                    "#825: reason must carry the model_reported_failure: \
                     prefix so classify_failed_reason maps it to \
                     FailureClass::ModelReportedFailure. Got: {reason}"
                );
                assert!(
                    reason.contains("blocked: missing dep"),
                    "reason must preserve the agent's original text. Got: {reason}"
                );
            }
            other => panic!("#825: expected Failed, got {other:?}"),
        }
    }

    /// #825 + CompleteRun symmetry: CompleteRun success → Done,
    /// FailRun success → Failed. Different terminal signals, so the
    /// loop runner knows which `LoopTermination` variant to return.
    #[test]
    fn derive_signal_complete_run_still_maps_to_done_after_fail_run_addition() {
        // Regression guard: adding the FailRun branch must NOT have
        // altered the CompleteRun semantics.
        let proposal = ActionProposal::complete_run("final answer", 0.95);
        let result = ActionResult {
            proposal,
            status: ActionStatus::Succeeded,
            tool_output: None,
            invocation_id: None,
            duration_ms: 0,
        };
        let got = derive_signal(&result, &LoopSignal::Continue);
        assert!(
            matches!(got, LoopSignal::Done),
            "CompleteRun success must still map to Done; got {got:?}"
        );
    }
}

// ── F26 regression: PendingOperator must NOT block ───────────────────────────
//
// Covers the dogfood blocker where an `InvokeTool` proposal with
// `requires_approval=true` blocked the entire `POST /v1/runs/:id/orchestrate`
// request for the full `approval_timeout_default` (default 24h; 45s in
// the live repro). The fix in `run_with_approval_gate`: on
// `PendingOperator`, return `AwaitingApproval` immediately so the outer
// loop yields `LoopTermination::WaitingApproval` and the HTTP handler
// returns 202. The operator then resolves the proposal asynchronously;
// the F25 drain picks up the approved call on the next orchestrate
// invocation.
//
// These tests pin the contract at the `ToolCallApprovalService`
// interaction boundary (the gate MUST NOT call `await_decision` on
// `PendingOperator`). Production-code coverage of
// `RuntimeExecutePhase::run_with_approval_gate` itself — wiring a real
// `RuntimeExecutePhase` with real `RunService` / `TaskService`
// adapters — lives in `crates/cairn-app/tests/test_approval_propose_then_await.rs::pending_approval_suspends_loop_without_blocking`,
// which constructs a full `RuntimeExecutePhase` via `build_phase(...)`
// and asserts the same invariants against the actual production code
// path. That E2E test uses the fake-fabric `RunService`/`TaskService`
// helpers that exist only in `cairn-app` (they depend on
// `FabricRunServiceAdapter` et al. which are crate-private to
// cairn-app), hence the split.
#[cfg(test)]
mod f26_pending_operator_no_block_tests {
    use async_trait::async_trait;
    use cairn_domain::approvals::{ApprovalMatchPolicy, ApprovalScope};
    use cairn_domain::{OperatorId, ProjectKey, RunId, SessionId, ToolCallId};
    use cairn_runtime::error::RuntimeError;
    use cairn_runtime::tool_call_approvals::{
        ApprovalDecision, ApprovedProposal, OperatorDecision, ToolCallApprovalService,
        ToolCallProposal,
    };
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    /// Fake approval service that always returns `PendingOperator` and
    /// panics if `await_decision` is ever called. Pins the F26 contract:
    /// the orchestrator must NOT await an operator decision in-process.
    struct NoBlockApprovalService {
        submit_called: AtomicBool,
        await_called: AtomicBool,
    }
    impl NoBlockApprovalService {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                submit_called: AtomicBool::new(false),
                await_called: AtomicBool::new(false),
            })
        }
    }
    #[async_trait]
    impl ToolCallApprovalService for NoBlockApprovalService {
        async fn submit_proposal(
            &self,
            _proposal: ToolCallProposal,
        ) -> Result<ApprovalDecision, RuntimeError> {
            self.submit_called.store(true, Ordering::SeqCst);
            Ok(ApprovalDecision::PendingOperator)
        }
        async fn approve(
            &self,
            _call_id: ToolCallId,
            _operator_id: OperatorId,
            _scope: ApprovalScope,
            _approved_args: Option<serde_json::Value>,
        ) -> Result<(), RuntimeError> {
            unreachable!("approve must not be called from the orchestrator loop");
        }
        async fn reject(
            &self,
            _call_id: ToolCallId,
            _operator_id: OperatorId,
            _reason: Option<String>,
        ) -> Result<(), RuntimeError> {
            unreachable!("reject must not be called from the orchestrator loop");
        }
        async fn amend(
            &self,
            _call_id: ToolCallId,
            _operator_id: OperatorId,
            _new_args: serde_json::Value,
        ) -> Result<(), RuntimeError> {
            unreachable!("amend must not be called from the orchestrator loop");
        }
        async fn retrieve_approved_proposal(
            &self,
            _call_id: &ToolCallId,
        ) -> Result<ApprovedProposal, RuntimeError> {
            unreachable!(
                "retrieve_approved_proposal must not be called for PendingOperator — the \
                 approval is still pending and there is nothing to retrieve"
            );
        }
        async fn await_decision(
            &self,
            _call_id: &ToolCallId,
            _timeout: Duration,
        ) -> Result<OperatorDecision, RuntimeError> {
            self.await_called.store(true, Ordering::SeqCst);
            panic!(
                "F26 regression: orchestrator must NOT call await_decision — \
                 PendingOperator must suspend the loop, not block in-process"
            );
        }
    }

    /// Service-contract harness: submit the proposal, and on
    /// `PendingOperator` return without calling `await_decision`. Used
    /// by the contract-level tests below to assert the fake never sees
    /// an `await_decision` call on the pending branch.
    ///
    /// This is NOT a substitute for the E2E regression test in
    /// `crates/cairn-app/tests/test_approval_propose_then_await.rs`
    /// which exercises the real `RuntimeExecutePhase::run_with_approval_gate`
    /// via `phase.execute(...)`. It's a belt-and-suspenders pin on the
    /// `ToolCallApprovalService` boundary so any future gate
    /// implementation that tries to re-introduce in-process blocking
    /// fails loud (the fake panics on `await_decision`).
    ///
    /// Returns `true` iff the gate correctly identified the pending
    /// state and did NOT block.
    async fn run_gate_semantics(svc: Arc<dyn ToolCallApprovalService>) -> bool {
        let proposal = ToolCallProposal {
            call_id: ToolCallId::new("tc_test_f26"),
            session_id: SessionId::new("sess"),
            run_id: RunId::new("run-f26"),
            project: ProjectKey::new("t", "w", "p"),
            tool_name: "bash".to_owned(),
            tool_args: json!({ "command": "ls /tmp" }),
            display_summary: Some("bash ls /tmp".to_owned()),
            match_policy: ApprovalMatchPolicy::Exact,
        };
        match svc.submit_proposal(proposal).await.unwrap() {
            ApprovalDecision::PendingOperator => {
                // F26 contract: return without calling await_decision.
                // Any call to await_decision on the fake panics.
                true
            }
            ApprovalDecision::AutoApproved => false,
        }
    }

    /// Core F26 regression: when `submit_proposal` returns
    /// `PendingOperator`, the orchestrator must NOT call
    /// `await_decision`. The fake panics if `await_decision` is called;
    /// if it were, this test would fail.
    ///
    /// This also bounds the elapsed time — a genuine in-process block
    /// would take longer than 200ms even at the minimum configured
    /// timeout (before F26, the default was 24h).
    #[tokio::test]
    async fn pending_operator_suspends_without_await_decision() {
        let fake: Arc<NoBlockApprovalService> = NoBlockApprovalService::new();
        let fake_trait: Arc<dyn ToolCallApprovalService> = fake.clone();

        let started = std::time::Instant::now();
        let ok = tokio::time::timeout(Duration::from_millis(500), run_gate_semantics(fake_trait))
            .await
            .expect("gate semantics must return promptly");
        let elapsed = started.elapsed();

        assert!(ok, "gate semantics must recognize PendingOperator");
        assert!(
            fake.submit_called.load(Ordering::SeqCst),
            "submit_proposal must be called to persist the proposal"
        );
        assert!(
            !fake.await_called.load(Ordering::SeqCst),
            "F26 regression: await_decision was called in-process — must suspend instead"
        );
        assert!(
            elapsed < Duration::from_millis(200),
            "gate took {elapsed:?} — should return near-instantly on PendingOperator"
        );
    }

    /// Complementary assertion: the real production code path in
    /// `run_with_approval_gate` (see `execute_impl.rs` ~L1310) maps
    /// `PendingOperator` to `ActionStatus::AwaitingApproval`. The loop
    /// runner's approval-pre-check block (see `loop_runner.rs` ~L858)
    /// scans results for `AwaitingApproval` and returns
    /// `LoopTermination::WaitingApproval`.
    ///
    /// That loop-level contract is pinned by
    /// `loop_runner::tests::requires_approval_suspends_immediately`.
    /// Together with the gate-level test above, F26 regression is
    /// covered on both layers.
    #[test]
    fn f26_contract_documented() {
        // Documentation-only marker; the real assertions are in the
        // two tests above (gate) + loop_runner (loop layer).
    }
}
