//! OrchestratorLoop — ties GatherPhase → DecidePhase → ExecutePhase together.
//!
//! The loop drives one run from start to a terminal state (or a suspension
//! point like `waiting_approval` or `waiting_dependency`).  It is the only
//! component that advances the run through the GATHER → DECIDE → EXECUTE
//! cycle, enforces the iteration cap and wall-clock timeout, and records a
//! `StepSummary` checkpoint after each iteration.
//!
//! # Pseudocode (per design doc)
//!
//! ```text
//! loop:
//!   1. check_timeout()
//!   2. gather_output  = gather(ctx)
//!   3. decide_output  = decide(ctx, gather_output)
//!   4. if decide_output.requires_approval → execute(ctx, decide_output) → WaitApproval
//!   5. execute_outcome = execute(ctx, decide_output)
//!   6. checkpoint(ctx, decide_output, execute_outcome)   ← per checkpoint policy
//!   7. match execute_outcome.loop_signal:
//!        Done          → Completed
//!        Failed        → Failed
//!        WaitApproval  → WaitingApproval
//!        WaitSubagent  → WaitingSubagent
//!        Continue      → ctx.iteration += 1, loop
//! max_iterations exceeded → MaxIterationsReached
//! ```

use std::sync::Arc;

use crate::context::{
    ActionResult, ActionStatus, DecideOutput, ExecuteOutcome, GatherOutput, LoopConfig, LoopSignal,
    LoopTermination, OrchestrationContext, StepSummary,
};
use crate::decide::DecidePhase;
use crate::emitter::{NoOpEmitter, OrchestratorEventEmitter};
use crate::error::OrchestratorError;
use crate::execute::{ApprovedDispatch, ExecutePhase};
use crate::gather::GatherPhase;
use crate::task_sink::{NoOpTaskSink, TaskFrameSink};

// ── CheckpointHook ────────────────────────────────────────────────────────────

/// Optional hook called after step 5 (execute) to persist iteration state.
///
/// The concrete implementation is injected by the HTTP entry point or test
/// harness. `NoOpCheckpointHook` is used when no durable checkpoint is needed
/// (local/test mode).
///
/// The execute phase already handles per-tool-call checkpointing via
/// `CheckpointService::save` per `LoopConfig::checkpoint_every_n_tool_calls`.
/// This hook is for the loop-level checkpoint that captures the full iteration
/// summary (goal + step history + decide output) for run resumability.
#[async_trait::async_trait]
pub trait CheckpointHook: Send + Sync {
    /// Persist a snapshot of the current orchestration iteration.
    ///
    /// Called unconditionally after execute — implementations may apply their
    /// own skip logic (e.g., skip if 0 tool calls were dispatched this step).
    async fn save(
        &self,
        ctx: &OrchestrationContext,
        gather: &GatherOutput,
        decide: &DecideOutput,
        execute: &ExecuteOutcome,
    ) -> Result<(), OrchestratorError>;

    /// RFC 020 Track 4 — persist the `Intent` checkpoint after decide
    /// produces proposals and Track 3 mints their `ToolCallId`s, but
    /// *before* execute dispatches. On crash between decide and execute,
    /// recovery reads this checkpoint, walks the planned `ToolCallId`s,
    /// consults the `ToolCallResultCache`, and re-dispatches only the
    /// misses (per RFC 020 Gap 13 resolution).
    ///
    /// Default impl: no-op. `NoOpCheckpointHook` and test doubles inherit
    /// the no-op; production wiring overrides with a real save that calls
    /// `CheckpointService::save_dual(..., CheckpointKind::Intent, ...)`.
    async fn save_intent(
        &self,
        _ctx: &OrchestrationContext,
        _gather: &GatherOutput,
        _decide: &DecideOutput,
    ) -> Result<(), OrchestratorError> {
        Ok(())
    }

    /// RFC 020 Track 4 — persist the `Result` checkpoint after all
    /// dispatches settle (success / timeout / fail). Complements
    /// `save_intent`: the two checkpoints form the per-iteration pair that
    /// closes RFC 020 invariant #5. Default delegates to `save` so
    /// implementors with a single-checkpoint history keep working.
    async fn save_result(
        &self,
        ctx: &OrchestrationContext,
        gather: &GatherOutput,
        decide: &DecideOutput,
        execute: &ExecuteOutcome,
    ) -> Result<(), OrchestratorError> {
        self.save(ctx, gather, decide, execute).await
    }
}

/// RFC 020 Track 4 — `CheckpointHook` that emits dual (Intent + Result)
/// checkpoints via `CheckpointService::save_dual`.
///
/// Intent is written after decide (before execute dispatches) and carries
/// the planned `ToolCallId`s Track 3 minted; Result is written after
/// execute completes and carries the post-iteration message history with
/// an empty `tool_call_ids` (the Intent checkpoint owns the full list per
/// Q4 resolution).
///
/// Full-snapshot bodies (Gap 3 resolution — v1 ships full snapshots, not
/// diffs). The emitted `CheckpointRecorded` event carries
/// `message_history_size` so operators can monitor cost and decide if
/// Track 4b diff compaction is worth the complexity.
pub struct DualCheckpointHook {
    project: cairn_domain::ProjectKey,
    checkpoints: std::sync::Arc<dyn cairn_runtime::CheckpointService>,
}

impl DualCheckpointHook {
    pub fn new(
        project: cairn_domain::ProjectKey,
        checkpoints: std::sync::Arc<dyn cairn_runtime::CheckpointService>,
    ) -> Self {
        Self {
            project,
            checkpoints,
        }
    }

    fn mint_checkpoint_id() -> cairn_domain::CheckpointId {
        cairn_domain::CheckpointId::new(format!("cp_{}", uuid::Uuid::now_v7()))
    }

    fn planned_tool_call_ids(decide: &DecideOutput) -> Vec<String> {
        // Advisory planned-call markers for the Intent checkpoint audit
        // body. These are NOT the hashed `ToolCallId`s execute mints —
        // bugbot #84/medium flagged that my earlier implementation sorted
        // by (tool_name, args) and normalized via `Value::to_string()`,
        // which diverged from `execute_impl.rs` (positional `call_index`
        // + per-handler `normalize_for_cache`). Rather than reconstruct
        // the handler registry in the checkpoint hook (a layering break —
        // the registry lives inside `RuntimeExecutePhase`), we:
        //
        // 1. Preserve proposal order (so `call_index` in the marker
        //    matches execute's positional dispatch order).
        // 2. Carry the raw proposal args (JSON `to_string`), clearly
        //    labelled as a PLAN marker, not the hashed cache key.
        //
        // The accurate hashed `ToolCallId` lands on
        // `ToolInvocationCompleted.tool_call_id` at dispatch time, which
        // is what recovery reads via `ToolCallResultCache::get`. The
        // Intent checkpoint's `tool_call_ids` is a human-readable audit
        // record of the plan, useful for operator dashboards and the
        // future resume path (Gap 13 consumer).
        //
        // Proposals without a `tool_name` (CompleteRun /
        // EscalateToOperator / CreateMemory) have no stream-facing tool
        // analogue and produce no marker.
        decide
            .proposals
            .iter()
            .enumerate()
            .filter_map(|(call_index, proposal)| {
                let tool_name = proposal.tool_name.as_deref()?;
                let args_json = proposal
                    .tool_args
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "null".to_owned());
                Some(format!("planned:{call_index}:{tool_name}:{args_json}"))
            })
            .collect()
    }

    fn build_history_snapshot(
        ctx: &OrchestrationContext,
        gather: &GatherOutput,
        decide: &DecideOutput,
        execute: Option<&ExecuteOutcome>,
    ) -> serde_json::Value {
        // Full-snapshot body (RFC 020 Gap 3): serialize the iteration
        // artifacts we have in hand. Concrete shape is stable JSON so a
        // future resume path can parse it without schema migration.
        serde_json::json!({
            "run_id": ctx.run_id.as_str(),
            "iteration": ctx.iteration,
            "goal": ctx.goal,
            "step_history": ctx.step_history,
            "gather": {
                "memory_chunk_count": gather.memory_chunks.len(),
                "step_history_count": gather.step_history.len(),
            },
            "decide": {
                "proposal_count": decide.proposals.len(),
                "requires_approval": decide.requires_approval,
                "model_id": decide.model_id,
                "latency_ms": decide.latency_ms,
            },
            "execute": execute.map(|e| serde_json::json!({
                "result_count": e.results.len(),
                "loop_signal": format!("{:?}", e.loop_signal),
            })),
        })
    }
}

#[async_trait::async_trait]
impl CheckpointHook for DualCheckpointHook {
    async fn save(
        &self,
        ctx: &OrchestrationContext,
        gather: &GatherOutput,
        decide: &DecideOutput,
        execute: &ExecuteOutcome,
    ) -> Result<(), OrchestratorError> {
        // Fallback path for callers using the legacy single-`save` API.
        // `save_result` is the canonical Track 4 post-execute path.
        self.save_result(ctx, gather, decide, execute).await
    }

    async fn save_intent(
        &self,
        ctx: &OrchestrationContext,
        gather: &GatherOutput,
        decide: &DecideOutput,
    ) -> Result<(), OrchestratorError> {
        let cp_id = Self::mint_checkpoint_id();
        let body = Self::build_history_snapshot(ctx, gather, decide, None);
        let tool_call_ids = Self::planned_tool_call_ids(decide);
        self.checkpoints
            .save_dual(
                &self.project,
                &ctx.run_id,
                cp_id,
                cairn_domain::CheckpointKind::Intent,
                body,
                tool_call_ids,
            )
            .await
            .map(|_| ())
            .map_err(OrchestratorError::Runtime)
    }

    async fn save_result(
        &self,
        ctx: &OrchestrationContext,
        gather: &GatherOutput,
        decide: &DecideOutput,
        execute: &ExecuteOutcome,
    ) -> Result<(), OrchestratorError> {
        let cp_id = Self::mint_checkpoint_id();
        let body = Self::build_history_snapshot(ctx, gather, decide, Some(execute));
        // RFC 020 Track 4 §6 Q4 — Result checkpoint carries an empty
        // `tool_call_ids`. Intent already owns the full planned list;
        // duplicating here only inflates the event body.
        self.checkpoints
            .save_dual(
                &self.project,
                &ctx.run_id,
                cp_id,
                cairn_domain::CheckpointKind::Result,
                body,
                Vec::new(),
            )
            .await
            .map(|_| ())
            .map_err(OrchestratorError::Runtime)
    }
}

/// A no-op `CheckpointHook` — used in tests and local mode where durability
/// is provided by InMemoryStore rather than an external checkpoint store.
pub struct NoOpCheckpointHook;

#[async_trait::async_trait]
impl CheckpointHook for NoOpCheckpointHook {
    async fn save(
        &self,
        _ctx: &OrchestrationContext,
        _gather: &GatherOutput,
        _decide: &DecideOutput,
        _execute: &ExecuteOutcome,
    ) -> Result<(), OrchestratorError> {
        Ok(())
    }
}

/// Reason string carried on `LoopTermination::Failed` when the loop
/// aborts because the underlying FF lease renewal failed 3+ times.
/// Downstream code (handler error-mapping, FF `fail_with_retry`) matches
/// on this exact string — use the const to avoid typo-driven breakage.
pub const LEASE_UNHEALTHY_REASON: &str = "lease unhealthy";

/// Minimum wall-clock budget remaining (ms) required to start a DECIDE.
///
/// When less than this is left on the run deadline we terminate cleanly as
/// `LoopTermination::TimedOut` instead of firing an LLM call that is
/// guaranteed to miss the deadline mid-flight. 5s is a conservative heuristic:
/// smaller than any provider's default timeout so it only fires near the
/// actual deadline, but large enough that a successful DECIDE could realistically
/// finish in the remaining window on fast paths. Chosen over the per-provider
/// timeout because different bindings in a routing chain have different
/// defaults — 5s is the largest common lower bound that doesn't leak those
/// internals up to the loop.
pub const MIN_DECIDE_BUDGET_MS: u64 = 5_000;

// ── OrchestratorLoop ──────────────────────────────────────────────────────────

/// One operator-rejected tool call surfaced by the F46 rejection
/// drain.
///
/// Thin wrapper around the `StepSummary` that the next DECIDE sees;
/// earlier drafts also carried `tool_name` + `preview` for prospective
/// SSE/metrics plumbing, but those fields were never read and
/// "reserved for later" accumulated churn on every refactor. When an
/// SSE or metrics consumer materialises we will reinstate the fields
/// alongside the emission site in the same PR — audit #474.
struct DrainedRejection {
    /// Step summary that will be pushed into `step_history` so the
    /// next DECIDE's user message sees the rejection verbatim.
    summary: StepSummary,
}

/// Drives the GATHER → DECIDE → EXECUTE loop for a single run.
///
/// # Type parameters
/// - `G`: [`GatherPhase`] implementation
/// - `D`: [`DecidePhase`] implementation
/// - `E`: [`ExecutePhase`] implementation
///
/// # Loop contract (per RFC 005)
/// - One `OrchestratorLoop` owns exactly one run's execution.
/// - All state changes flow through runtime events (RFC 002).
/// - The loop may be suspended and resumed (approval / subagent wait).
/// - `max_iterations` guards against infinite loops.
/// - `timeout_ms` provides a wall-clock deadline for the whole run.
pub struct OrchestratorLoop<G, D, E> {
    gather: G,
    decide: D,
    execute: E,
    config: LoopConfig,
    checkpoint_hook: Arc<dyn CheckpointHook>,
    emitter: Arc<dyn OrchestratorEventEmitter>,
    /// FF task-stream sink. `NoOpTaskSink` in the default construction —
    /// call `with_task_sink` to install a `cairn_fabric::CairnTask`-backed
    /// sink once the handler has claimed a task. Non-consuming
    /// (frames only); terminal + suspension ops stay at the caller.
    task_sink: Arc<dyn TaskFrameSink>,
    /// F25 drain: optional read of the tool-call approval projection.
    /// When wired, the loop drains any operator-approved-but-not-executed
    /// proposals for the run at the top of each `run_inner` invocation
    /// BEFORE calling DECIDE. Without this, a re-orchestrate after
    /// approval never reaches the approved tool — the LLM just sees the
    /// un-changed context and emits the same proposal again (which the
    /// approval service then treats as a duplicate and auto-approves
    /// without re-dispatch, looping forever).
    approval_reader: Option<Arc<dyn cairn_runtime::tool_call_approvals::ToolCallApprovalReader>>,
}

impl<G, D, E> OrchestratorLoop<G, D, E>
where
    G: GatherPhase,
    D: DecidePhase,
    E: ExecutePhase,
{
    /// Construct a new loop with the given phases and configuration.
    /// Uses `NoOpCheckpointHook`, `NoOpEmitter`, and `NoOpTaskSink` — call
    /// the builder methods to override.
    pub fn new(gather: G, decide: D, execute: E, config: LoopConfig) -> Self {
        Self {
            gather,
            decide,
            execute,
            config,
            checkpoint_hook: Arc::new(NoOpCheckpointHook),
            emitter: Arc::new(NoOpEmitter),
            task_sink: Arc::new(NoOpTaskSink),
            approval_reader: None,
        }
    }

    /// F25 drain: install the tool-call approval reader so the loop
    /// drains operator-approved proposals for the run before the next
    /// DECIDE iteration. Without this, re-orchestrate after an approval
    /// round-trip silently drops the approved tool call (the dogfood
    /// blocker).
    pub fn with_approval_reader(
        mut self,
        reader: Arc<dyn cairn_runtime::tool_call_approvals::ToolCallApprovalReader>,
    ) -> Self {
        self.approval_reader = Some(reader);
        self
    }

    /// Replace the checkpoint hook (e.g., a durable Postgres checkpoint writer).
    pub fn with_checkpoint_hook(mut self, hook: Arc<dyn CheckpointHook>) -> Self {
        self.checkpoint_hook = hook;
        self
    }

    /// Replace the event emitter (e.g., an SSE broadcaster for live progress).
    pub fn with_emitter(mut self, emitter: Arc<dyn OrchestratorEventEmitter>) -> Self {
        self.emitter = emitter;
        self
    }

    /// Install a task-stream sink so FF receives `tool_call`, `tool_result`,
    /// `llm_response`, and `checkpoint` frames for the attempt, and the
    /// loop can poll `is_lease_healthy` between iterations.
    ///
    /// Pass an `Arc<cairn_fabric::CairnTask>` (via the blanket impl in
    /// [`crate::task_sink`]) once the caller has claimed an FF task.
    /// Callers without an FF task can omit this — the default
    /// `NoOpTaskSink` leaves FF-side telemetry silent while the rest of
    /// the loop (EventLog bridge events, store projections, SSE) runs
    /// unchanged.
    pub fn with_task_sink(mut self, sink: Arc<dyn TaskFrameSink>) -> Self {
        self.task_sink = sink;
        self
    }

    /// Drive the GATHER → DECIDE → EXECUTE cycle until a terminal state.
    ///
    /// # Returns
    /// - `Ok(LoopTermination)` for all expected stop conditions.
    /// - `Err(OrchestratorError)` only for unexpected infrastructure errors.
    ///
    /// # Resume after suspension
    /// Rebuild `OrchestrationContext` from the last checkpoint (restoring
    /// `iteration` and any relevant state), then call `run()` again.
    /// The gather phase sees the current durable state; the loop resumes.
    pub async fn run(
        &self,
        mut ctx: OrchestrationContext,
    ) -> Result<LoopTermination, OrchestratorError> {
        self.emitter.on_started(&ctx).await;
        let result = self.run_inner(&mut ctx).await;
        // Emit on_finished for every terminal outcome. For the Err branch
        // propagate the underlying OrchestratorError's Display string so
        // dashboards see the real cause (e.g. "decide: model 404",
        // "memory: kb unavailable") rather than "infrastructure error".
        let run_terminal = match &result {
            Ok(t) => {
                self.emitter.on_finished(&ctx, t).await;
                t.drives_run_to_terminal()
            }
            Err(e) => {
                let term = LoopTermination::Failed {
                    reason: e.to_string(),
                };
                self.emitter.on_finished(&ctx, &term).await;
                // Infrastructure errors still end the run for good —
                // there is no resume path for an Err branch.
                true
            }
        };
        if run_terminal {
            // #606: evict harness-tools caches (write ledger + LSP
            // clients) so rust-analyzer child processes terminate
            // and the maps stay bounded to live runs.
            cairn_harness_tools::evict_run(&ctx.tool_context(), &ctx.project);
        }
        result
    }

    /// F25 drain: execute any operator-approved tool calls for this run
    /// whose `ToolCallId` the caller has not already drained this
    /// `run_inner` invocation. Returns one `ActionResult` per drained
    /// proposal in oldest-first order.
    ///
    /// `already_drained` is a per-invocation ledger of `ToolCallId`
    /// strings the loop has already processed. Without it, every
    /// iteration would re-fetch the same Approved rows and re-emit
    /// tool_called / tool_result / StepSummary for each one — even if
    /// `dispatch_approved` silently served a cache hit, the bloat in
    /// `step_history` would poison the next DECIDE's context. This
    /// caller-owned set caps the work at once per call_id per invocation.
    ///
    /// `dispatch_approved` still performs its own
    /// `ToolCallResultCache`-presence check as a second line of defence:
    /// a long-running loop that outlives a restart (theoretical; current
    /// runner does not) would hit the cache on the post-restart rebuild.
    ///
    /// When no approval reader is wired (default), the drain is a no-op.
    async fn drain_approved_pending(
        &self,
        ctx: &OrchestrationContext,
        already_drained: &mut std::collections::HashSet<String>,
    ) -> Result<Vec<ActionResult>, OrchestratorError> {
        let Some(reader) = &self.approval_reader else {
            return Ok(Vec::new());
        };

        let approved = reader
            .list_approved_for_run(&ctx.run_id)
            .await
            .map_err(OrchestratorError::Runtime)?;
        if approved.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        for ap in approved {
            let call_id_str = ap.call_id.as_str().to_owned();
            if !already_drained.insert(call_id_str.clone()) {
                // Already handled this call_id earlier in the same
                // `run_inner` invocation — skip to avoid duplicate
                // emissions and step_history entries.
                continue;
            }

            let started_at = std::time::Instant::now();
            let dispatch = ApprovedDispatch {
                call_id: ap.call_id,
                tool_name: ap.tool_name,
                tool_args: ap.tool_args,
            };
            let mut result = self.execute.dispatch_approved(ctx, &dispatch).await?;
            result.duration_ms = started_at.elapsed().as_millis() as u64;
            tracing::info!(
                run_id = %ctx.run_id,
                tool = ?dispatch.tool_name,
                succeeded = matches!(result.status, ActionStatus::Succeeded),
                "F25 drain: dispatched approved tool call"
            );
            results.push(result);
        }
        Ok(results)
    }

    /// F46 rejection drain: surface every operator-rejected tool call
    /// for this run that the caller has not already processed. Returns
    /// one `StepSummary` per rejection so the next DECIDE's user
    /// message carries the rejection reason verbatim. Without this, a
    /// rejected proposal leaves no trace in step history and the LLM
    /// re-proposes the same call — the F46 dogfood repro.
    ///
    /// Dedup shares the `already_drained` ledger with the approved
    /// drain: a call_id can be in at most one terminal state
    /// (approved-then-executed OR rejected), so one ledger covers both.
    async fn drain_rejected_pending(
        &self,
        ctx: &OrchestrationContext,
        already_drained: &mut std::collections::HashSet<String>,
    ) -> Result<Vec<DrainedRejection>, OrchestratorError> {
        let Some(reader) = &self.approval_reader else {
            return Ok(Vec::new());
        };

        let rejected = reader
            .list_rejected_for_run(&ctx.run_id)
            .await
            .map_err(OrchestratorError::Runtime)?;
        if rejected.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for rj in rejected {
            let call_id_str = rj.call_id.as_str().to_owned();
            if !already_drained.insert(call_id_str.clone()) {
                continue;
            }
            let reason_text = rj
                .reason
                .as_deref()
                .unwrap_or("operator rejected tool call (no reason provided)");
            let preview = truncate_for_summary(reason_text, 400);
            let tool_name = rj.tool_name;
            tracing::info!(
                run_id = %ctx.run_id,
                tool = %tool_name,
                call_id = %call_id_str,
                "F46 drain: surfacing rejected tool call to next DECIDE"
            );
            let summary = StepSummary {
                iteration: ctx.iteration,
                action_kind: "invoke_tool".to_owned(),
                // Format mirrors `build_step_summary` so all three
                // outcomes (ok / ERROR / REJECTED) render through the
                // same "tool_result[<name>] ..." grammar downstream.
                // Header uses "drained rejected" — avoids the
                // contradictory "rejected approved proposal" wording
                // while staying parallel to the approved drain's
                // "drained approved: <tool>" header.
                summary: format!(
                    "drained rejected: {tool_name}\n  tool_result[{tool_name}] REJECTED: {preview}"
                ),
                // Rejection is a terminal *non-failure* from the loop's
                // perspective — the run continues, just without the
                // tool call. Mark succeeded=true so the DECIDE prompt
                // doesn't render `ok=false` (which would signal a hard
                // failure). The summary text itself carries the
                // rejection semantics.
                succeeded: true,
            };
            out.push(DrainedRejection { summary });
        }
        Ok(out)
    }

    /// F65 PR-3 helper: dispatch a `BreakerCheck` result. `Continue`
    /// is a no-op; `Warning` logs + invokes `on_budget_threshold_crossed`
    /// and returns `None` so the caller keeps running; `Tripped` logs +
    /// invokes `on_breaker_tripped` and returns
    /// `Some(LoopTermination::BreakerTripped)` which the caller
    /// propagates as the final termination.
    ///
    /// Extracted to close the pre-gather / post-decide duplication
    /// Gemini flagged on PR #348 review.
    async fn handle_breaker_check(
        &self,
        ctx: &OrchestrationContext,
        check: crate::breakers::BreakerCheck,
        where_label: &'static str,
    ) -> Option<LoopTermination> {
        use crate::breakers::BreakerCheck;
        match check {
            BreakerCheck::Continue => None,
            BreakerCheck::Warning {
                which,
                measured,
                limit,
                ratio_bps,
            } => {
                tracing::warn!(
                    run_id    = %ctx.run_id,
                    iteration = ctx.iteration,
                    which     = ?which,
                    measured,
                    limit,
                    ratio_bps,
                    where_label,
                    "orchestrator budget threshold crossed"
                );
                self.emitter
                    .on_budget_threshold_crossed(ctx, which, measured, limit, ratio_bps)
                    .await;
                None
            }
            BreakerCheck::Tripped(trip) => {
                tracing::warn!(
                    run_id    = %ctx.run_id,
                    iteration = ctx.iteration,
                    which     = ?trip.which,
                    measured  = trip.measured,
                    limit     = trip.limit,
                    where_label,
                    "orchestrator circuit breaker tripped"
                );
                self.emitter.on_breaker_tripped(ctx, &trip).await;
                Some(LoopTermination::BreakerTripped { trip })
            }
        }
    }

    async fn run_inner(
        &self,
        ctx: &mut OrchestrationContext,
    ) -> Result<LoopTermination, OrchestratorError> {
        let deadline_ms = ctx.run_started_at_ms.saturating_add(self.config.timeout_ms);

        // Local step history — carried across iterations within this invocation.
        // On resume from a checkpoint the gather phase rebuilds history from the
        // store; this vec accumulates steps taken during the *current* invocation.
        //
        // #670 G7: seed from `ctx.step_history` so callers (e.g. cairn-app's
        // `drive_run_iteration`) can prepend cross-run context before the
        // loop starts. Without this seed the loop overwrites `ctx.step_history`
        // with its own empty vec at the top of the first gather, dropping
        // the subagent-completion entries that G7 injects. `std::mem::take`
        // avoids a clone: the caller's `ctx.step_history` is consumed into
        // the local vec, then re-synced on the next `ctx.step_history =
        // step_history.clone()` below.
        let mut step_history: Vec<StepSummary> = std::mem::take(&mut ctx.step_history);
        let mut last_compaction_iteration: Option<u32> = None;
        // F25 drain dedup ledger: every approved `ToolCallId` the drain
        // has processed in THIS `run_inner` invocation. Prevents
        // duplicate tool_called/tool_result/StepSummary emissions when
        // subsequent iterations re-list the same projection row.
        let mut drained_call_ids: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        // F47 PR1 incremental verification accumulator. Each
        // `ActionResult` produced by execute (main path, approval-gate
        // inline path, AND the F25 drain path) is fed to
        // `verification_acc.observe(...)` which scans the tool_output in
        // place and retains only the bounded bucket output (50 entries *
        // 500 chars per bucket). Keeps per-run memory flat even when a
        // run issues a `read` on a large file — we never retain the full
        // `tool_output` past the scan. At Done `verification_acc.finish()`
        // produces the `CompletionVerification` sidecar attached to
        // `LoopTermination::Completed`. Copilot review on #312 flagged
        // the original `Vec<ActionResult>` form as a memory risk; this
        // scan-and-discard design closes that. Cursor Bugbot on #312
        // flagged the drain path as uncovered; every observe-site below
        // is audited against the three dispatch paths.
        let mut verification_acc = crate::completion_verification::VerificationAccumulator::new();

        // F65 PR-3: circuit-breaker state, anchored to Instant::now() for
        // monotonic wall-clock measurement independent of system-clock jumps.
        // `check_pre_gather` fires at the top of each iteration (Round +
        // WallClock); `after_decide` fires after DECIDE (Tokens +
        // NoToolUseConsecutive). The config's caps are resolved by the
        // HTTP handler via the 3-layer RuntimeConfig fallback plus per-run
        // overrides.
        let mut breaker_state = crate::breakers::BreakerState::new(self.config.breakers.clone());
        // Issue #689 Finding R2-B: per-run state tracking consecutive
        // `bash` + bare-`echo` prose-playing turns. Observational only —
        // the loop continues even when the detector fires; the
        // emitter-side metric + WARN log surfaces the signal to
        // operators without auto-failing the run. See
        // `crate::echo_detector` rustdoc for the heuristic + scope
        // guardrails.
        let mut echo_detector_state = crate::echo_detector::EchoDetectorState::new();
        // Warn-once latch: fired when a DECIDE response lacks both input
        // and output token counts so token-cap accounting silently
        // under-counts. Set on first occurrence and never cleared; rare
        // in practice (providers routinely report usage) but we refuse
        // to silently accept a stuck under-count.
        let mut decide_usage_absent_warned = false;
        // Issue #660: strict completion-gate rejection counter. Increments
        // every time the LLM proposes `complete_run` while the F47
        // verification accumulator still has non-empty errors. Three
        // consecutive rejections end the run in `Failed
        // (VerificationRejected)` so a non-converging model can't ping-
        // pong against the gate until `max_iterations` exhausts the
        // budget. Gated by `config.orchestrator_strict_completion_gate`.
        let mut completion_gate_rejections: u32 = 0;

        tracing::info!(
            run_id    = %ctx.run_id,
            goal      = %ctx.goal,
            agent     = %ctx.agent_type,
            max_iter  = self.config.max_iterations,
            timeout_s = self.config.timeout_ms / 1_000,
            round_cap = self.config.breakers.round_cap,
            token_cap = self.config.breakers.token_cap,
            no_tool_use_streak = self.config.breakers.no_tool_use_streak,
            wall_clock_ms = self.config.breakers.wall_clock_ms,
            "orchestrator loop starting"
        );
        for _iter in 0..self.config.max_iterations {
            // ── (0) F25 drain: flush operator-approved tool calls ────────────
            //
            // Before GATHER reads the event log, replay any
            // `ToolCallApproved`-state proposals for this run that don't
            // yet have a matching `ToolInvocationCompleted`. Without this
            // step, a re-orchestrate after approval never invokes the
            // approved tool: the LLM sees the same context as last turn,
            // emits the same proposal, the approval service (correctly)
            // returns AutoApproved from its cache, but nothing actually
            // *runs* the tool — the dogfood F25 blocker. See
            // `CLAUDE.md` + `project_session_2026_04_22_part4.md`.
            //
            // Failures inside the drain surface as synthesized
            // StepSummary entries + tool_result events so the next
            // DECIDE sees what went wrong and the LLM can self-correct.
            let drained = self
                .drain_approved_pending(ctx, &mut drained_call_ids)
                .await?;
            if !drained.is_empty() {
                for result in &drained {
                    // F47 PR1 (Cursor Bugbot #312 fix): the drain path
                    // dispatches approved tool calls, and its outputs MUST
                    // flow into the verification sidecar — otherwise an
                    // operator-approved bash command that emits warnings
                    // would be invisible to the SSE evidence consumer,
                    // defeating the F47 contract for approval-gated runs.
                    verification_acc.observe(result);

                    let Some(tool_name) = result.proposal.tool_name.as_deref() else {
                        continue;
                    };
                    let args = result
                        .proposal
                        .tool_args
                        .clone()
                        .unwrap_or(serde_json::Value::Null);

                    // SSE: tool_called + tool_result (mirrors main path).
                    self.emitter
                        .on_tool_called(ctx, tool_name, Some(&args))
                        .await;
                    let (succeeded, error) = match &result.status {
                        ActionStatus::Succeeded => (true, None),
                        ActionStatus::Failed { reason } => (false, Some(reason.as_str())),
                        _ => (false, None),
                    };
                    self.emitter
                        .on_tool_result(
                            ctx,
                            tool_name,
                            succeeded,
                            result.tool_output.as_ref(),
                            error,
                            result.duration_ms,
                        )
                        .await;

                    // FF attempt-stream: tool_call + tool_result frames.
                    // `restore_frames()` on resume expects these pair
                    // with every tool dispatch, including drain. Best-
                    // effort (warn+continue) per the sink contract.
                    if let Err(e) = self.task_sink.log_tool_call(tool_name, &args).await {
                        tracing::warn!(
                            run_id = %ctx.run_id,
                            tool = %tool_name,
                            error = %e,
                            "drain: task_sink.log_tool_call failed — frame lost"
                        );
                    }
                    let output = match &result.tool_output {
                        Some(v) => v.clone(),
                        None => match error {
                            Some(reason) => serde_json::json!({"error": reason}),
                            None => serde_json::Value::Null,
                        },
                    };
                    if let Err(e) = self
                        .task_sink
                        .log_tool_result(tool_name, &output, succeeded, result.duration_ms)
                        .await
                    {
                        tracing::warn!(
                            run_id = %ctx.run_id,
                            tool = %tool_name,
                            error = %e,
                            "drain: task_sink.log_tool_result failed — frame lost"
                        );
                    }

                    // Append StepSummary so DECIDE's next gather sees
                    // the drained action as part of the run's history.
                    //
                    // F46: mirror `build_step_summary`'s tool_result line
                    // format so the LLM sees stdout/stderr (bash) or file
                    // content (read) or error text on the NEXT DECIDE
                    // turn. Pre-F46 this just wrote "drained approved:
                    // {tool_name}" with no payload — the LLM saw an
                    // opaque success marker and re-proposed the same
                    // call (dogfood M1 repro).
                    let action_kind = "invoke_tool".to_owned();
                    let header = format!("drained approved: {tool_name}");
                    let summary = match &result.status {
                        ActionStatus::Succeeded => match result.tool_output.as_ref() {
                            Some(output) => {
                                // F54: tool-aware preview so bash output
                                // keeps its tail + exit_code line.
                                let preview = render_tool_output_preview(tool_name, output);
                                format!("{header}\n  tool_result[{tool_name}] ok: {preview}")
                            }
                            None => header,
                        },
                        ActionStatus::Failed { reason } => {
                            let preview = truncate_for_summary(reason, 400);
                            format!("{header}\n  tool_result[{tool_name}] ERROR: {preview}")
                        }
                        _ => header,
                    };
                    step_history.push(StepSummary {
                        iteration: ctx.iteration,
                        action_kind,
                        summary,
                        succeeded,
                    });
                }
            }

            // ── (0b) F46 rejection drain: surface rejected proposals ─────────
            //
            // Operator rejections leave no trace in the in-memory
            // step_history across suspend/resume boundaries — the
            // projection row moves to `Rejected` but no drain-side
            // dispatch emits a StepSummary. Without this pass the next
            // DECIDE can't see the rejection reason and re-proposes the
            // same call (F46 dogfood M1 repro). Runs every iteration
            // because a second approval may be rejected while a first
            // is executing.
            let rejections = self
                .drain_rejected_pending(ctx, &mut drained_call_ids)
                .await?;
            for DrainedRejection { summary } in rejections {
                // Rejections are NOT tool executions — the tool was
                // never invoked. Emitting a `tool_result` SSE frame
                // (succeeded=false) would mislead UI consumers and skew
                // tool-failure metrics. The rejection is already
                // surfaced to operators via the ToolCallApproval
                // projection + its dedicated `/reject` endpoint — the
                // UI consumes that stream directly. Here we only need
                // to thread the rejection into the next DECIDE's user
                // message, which happens via `step_history` below.
                step_history.push(summary);
            }

            // ── (1) Timeout check ─────────────────────────────────────────────
            let now_ms = now_millis();
            if now_ms >= deadline_ms {
                tracing::warn!(
                    run_id    = %ctx.run_id,
                    iteration = ctx.iteration,
                    "orchestrator loop timed out"
                );
                return Ok(LoopTermination::TimedOut);
            }

            // ── (1a) F65 PR-3: pre-GATHER breaker check ─────────────────────
            // Round + WallClock caps are consulted here so a trip fires
            // BEFORE we commit irreversible side effects (LLM call, tool
            // dispatch, checkpoint write) for this iteration. The
            // emitter hook appends `RuntimeEvent::CircuitBreakerTripped`
            // via the cairn-app `TracingEmitter`; a `Warning` variant
            // records `BudgetThresholdCrossed` without terminating.
            if let Some(term) = self
                .handle_breaker_check(
                    ctx,
                    breaker_state.check_pre_gather(ctx.iteration),
                    "pre_gather",
                )
                .await
            {
                return Ok(term);
            }

            // ── (1b) Lease health gate ───────────────────────────────────────
            // FF's ClaimedTask tracks consecutive renewal failures; after 3
            // misses `is_lease_healthy()` returns false and every downstream
            // FCALL will reject as stale_lease. Bail before committing any
            // irreversible side effect (LLM call, tool dispatch, checkpoint
            // write) — the caller sees `LoopTermination::Failed { reason:
            // "lease unhealthy" }` and can fail the run cleanly via the
            // CairnTask handle it still owns.
            if !self.task_sink.is_lease_healthy() {
                tracing::warn!(
                    run_id    = %ctx.run_id,
                    iteration = ctx.iteration,
                    "lease unhealthy (3+ renewal failures) — aborting loop"
                );
                return Ok(LoopTermination::Failed {
                    reason: LEASE_UNHEALTHY_REASON.to_owned(),
                });
            }

            let remaining_ms = deadline_ms.saturating_sub(now_ms);

            tracing::debug!(
                run_id       = %ctx.run_id,
                iteration    = ctx.iteration,
                remaining_ms = remaining_ms,
                "iteration start"
            );

            // ── (2) GATHER ────────────────────────────────────────────────────
            // T5-H1: surface the loop-maintained step_history so
            // `StandardGatherPhase::gather` threads it into `GatherOutput.step_history`
            // and `LlmDecidePhase::build_user_message` can render prior
            // iterations into the LLM context.
            ctx.step_history = step_history.clone();
            let gather_output = self.gather.gather(ctx).await.map_err(|e| {
                tracing::error!(run_id = %ctx.run_id, iteration = ctx.iteration, error = %e, "gather failed");
                e
            })?;

            tracing::debug!(
                run_id        = %ctx.run_id,
                iteration     = ctx.iteration,
                memory_chunks = gather_output.memory_chunks.len(),
                recent_events = gather_output.recent_events.len(),
                "gather complete"
            );
            self.emitter.on_gather_completed(ctx, &gather_output).await;

            // ── (2b) COMPACTION CHECK (RFC 018) ──────────────────────────────
            // If step history exceeds the compaction threshold, compress older
            // steps into a summary, keeping the most recent N steps verbatim.
            if let Some(compaction) = maybe_compact_history(
                &mut step_history,
                ctx.iteration,
                &self.config.compaction,
                &mut last_compaction_iteration,
            ) {
                tracing::info!(
                    run_id         = %ctx.run_id,
                    iteration      = ctx.iteration,
                    before_steps   = compaction.before_steps,
                    after_steps    = compaction.after_steps,
                    before_tokens  = compaction.before_tokens_est,
                    after_tokens   = compaction.after_tokens_est,
                    "context compacted"
                );

                self.emitter
                    .on_context_compacted(
                        ctx,
                        compaction.before_steps,
                        compaction.after_steps,
                        compaction.before_tokens_est,
                        compaction.after_tokens_est,
                    )
                    .await;
            }

            // ── (2c) Pre-DECIDE budget check ─────────────────────────────────
            // GATHER just finished; if the remaining budget is too small
            // to reasonably complete a DECIDE round-trip, bail now rather
            // than firing the LLM call only to have the wall-clock
            // deadline trip in the middle and leave a stranded provider
            // request. The threshold is heuristic: smaller than the
            // smallest per-provider default would guarantee a provider
            // timeout fires before the loop deadline — wasteful. Larger
            // than DECIDE's typical latency avoids false positives.
            //
            // Uses `now_millis()` fresh: GATHER may itself have taken
            // meaningful time (retrieval + chunk scoring), so the
            // `remaining_ms` computed at iteration start is stale.
            let pre_decide_now_ms = now_millis();
            let remaining_before_decide_ms = deadline_ms.saturating_sub(pre_decide_now_ms);
            if remaining_before_decide_ms < MIN_DECIDE_BUDGET_MS {
                tracing::warn!(
                    run_id        = %ctx.run_id,
                    iteration     = ctx.iteration,
                    remaining_ms  = remaining_before_decide_ms,
                    min_budget_ms = MIN_DECIDE_BUDGET_MS,
                    "orchestrator loop budget too low to start DECIDE — timing out cleanly"
                );
                return Ok(LoopTermination::TimedOut);
            }

            // ── (3) DECIDE ────────────────────────────────────────────────────
            // `mut` so the #660 strict completion gate can strip a refused
            // `complete_run` proposal in place before execute dispatches it.
            let mut decide_output = self.decide.decide(ctx, &gather_output).await.map_err(|e| {
                tracing::error!(run_id = %ctx.run_id, iteration = ctx.iteration, error = %e, "decide failed");
                e
            })?;

            let first_action = decide_output
                .proposals
                .first()
                .map(|p| format!("{:?}", p.action_type))
                .unwrap_or_else(|| "none".to_owned());

            tracing::debug!(
                run_id     = %ctx.run_id,
                iteration  = ctx.iteration,
                proposals  = decide_output.proposals.len(),
                first      = %first_action,
                confidence = decide_output.calibrated_confidence,
                "decide complete"
            );
            self.emitter.on_decide_completed(ctx, &decide_output).await;

            // ── (3a) F65 PR-3: post-DECIDE breaker check ────────────────────
            // DecideOutput carries provider-reported token counts; we
            // build a "tool or terminal" proposal count (see below) for
            // the NoToolUseConsecutive streak. If both input AND output
            // are None for a given DECIDE round, the provider did not
            // report usage — we warn once per run and treat this round as
            // zero tokens. Providers that PERMANENTLY omit usage will
            // cause the token-cap breaker to under-count for the whole
            // run; operators should confirm their provider reports usage
            // before relying on the token-cap breaker. This is the
            // intentional trade-off — we prefer under-counting to
            // refusing to run entirely.
            if decide_output.input_tokens.is_none()
                && decide_output.output_tokens.is_none()
                && !decide_usage_absent_warned
            {
                decide_usage_absent_warned = true;
                tracing::warn!(
                    run_id    = %ctx.run_id,
                    iteration = ctx.iteration,
                    model     = %decide_output.model_id,
                    "DECIDE response carried no token usage — token-cap breaker may \
                     under-count for this run (warn-once)"
                );
            }
            // NoToolUseConsecutive streak counts as "non-zero" for any
            // proposal that either carries a concrete tool_name OR is
            // a terminal / operator-gated action type (complete_run /
            // escalate_to_operator / spawn_subagent). Without this
            // carve-out, a legitimate `complete_run` after two prior
            // narration rounds would trip the streak breaker BEFORE
            // execute dispatches the terminal action — converting the
            // user's intentional completion into a BreakerTripped
            // failure. Cursor Bugbot flagged this HIGH-severity on
            // PR #348.
            //
            // Classification logic lives on `DecideOutput` so future
            // DECIDE refactors (batched tool bursts, provider-side
            // segmented counts, `.terminal_action_count()` etc.) can
            // memoise it without touching this call site — see #510.
            let tool_or_terminal_count = decide_output.tool_or_terminal_count();
            let post_decide_check = breaker_state.after_decide(
                ctx.iteration,
                decide_output.input_tokens.unwrap_or(0),
                decide_output.output_tokens.unwrap_or(0),
                tool_or_terminal_count,
            );
            if let Some(term) = self
                .handle_breaker_check(ctx, post_decide_check, "post_decide")
                .await
            {
                return Ok(term);
            }

            // ── (3a'') Issue #689 R2-B: echo-via-bash prose-playing ──────────
            // Non-terminal detector. When the LLM emits consecutive
            // `bash` + bare-`echo` proposals (no redirects / pipes /
            // operators), it's narrating its intentions instead of
            // dispatching the action. WARN-log + emitter hook, then
            // continue — the operator decides what to do. No run
            // modification, no breaker trip, no cancel.
            use crate::echo_detector::EchoDetectorCheck;
            match echo_detector_state.on_decide(&decide_output) {
                EchoDetectorCheck::Detected { consecutive_count } => {
                    tracing::warn!(
                        run_id             = %ctx.run_id,
                        iteration          = ctx.iteration,
                        model              = %decide_output.model_id,
                        consecutive_count  = consecutive_count,
                        pattern            = "echo_via_bash",
                        "issue #689 R2-B: LLM appears to be prose-playing — emitting \
                         consecutive `bash` + bare-`echo` proposals instead of the \
                         correct ActionType. Operator visibility only; the run \
                         continues."
                    );
                    self.emitter
                        .on_prose_playing_detected(ctx, consecutive_count)
                        .await;
                }
                EchoDetectorCheck::Continue => {}
            }

            // ── (3a') Emitter fatal-error check ──────────────────────────────
            // `on_decide_completed` is the only callback that dual-writes
            // provider-call telemetry into the durable secondary (see the
            // `TracingEmitter` implementation in `cairn-app`). When its
            // append fails, the in-memory and durable logs have diverged
            // — the next iteration would read stale state from the
            // primary, so the loop must abort with a store error rather
            // than silently continue toward the iteration cap.
            if let Some(msg) = self.emitter.take_fatal_error() {
                tracing::error!(
                    run_id    = %ctx.run_id,
                    iteration = ctx.iteration,
                    error     = %msg,
                    "orchestrator emitter reported fatal error — aborting loop"
                );
                return Err(OrchestratorError::Store(cairn_store::StoreError::Internal(
                    msg,
                )));
            }

            // ── (3b') FF stream: llm_response frame ──────────────────────────
            // DecideOutput already carries model_id, token counts, and
            // latency. Surface those on FF's attempt stream so cost
            // reconciliation + audit replay work off a single durable source
            // without cairn-store having to parse the raw LLM body. Token
            // counts default to 0 when the provider didn't report them (FF
            // stream format requires u64).
            if let Err(e) = self
                .task_sink
                .log_llm_response(
                    &decide_output.model_id,
                    decide_output.input_tokens.unwrap_or(0) as u64,
                    decide_output.output_tokens.unwrap_or(0) as u64,
                    decide_output.latency_ms,
                )
                .await
            {
                tracing::warn!(
                    run_id    = %ctx.run_id,
                    iteration = ctx.iteration,
                    model     = %decide_output.model_id,
                    error     = %e,
                    "task_sink.log_llm_response failed — frame lost, loop continues"
                );
            }

            // ── (3b) Plan artifact detection (RFC 018) ───────────────────────
            // In Plan mode, check if the LLM response contains a <proposed_plan>
            // block. If so, extract the plan markdown and terminate the run.
            if matches!(ctx.run_mode, cairn_domain::decisions::RunMode::Plan) {
                if let Some(plan_md) = extract_proposed_plan(&decide_output.raw_response) {
                    tracing::info!(
                        run_id    = %ctx.run_id,
                        iteration = ctx.iteration,
                        plan_len  = plan_md.len(),
                        "plan artifact detected — terminating Plan-mode run"
                    );
                    self.emitter.on_plan_proposed(ctx, &plan_md).await;
                    return Ok(LoopTermination::PlanProposed {
                        plan_markdown: plan_md,
                    });
                }
            }

            // ── (4) Approval pre-check ────────────────────────────────────────
            // When requires_approval is true, the execute phase emits an
            // ApprovalRequested event and transitions the run to waiting_approval.
            // The loop suspends here — it will resume once the approval resolves.
            if decide_output.requires_approval {
                tracing::info!(
                    run_id    = %ctx.run_id,
                    iteration = ctx.iteration,
                    "decision requires approval — suspending for ApprovalRequested"
                );

                // FF stream: log tool_call frames for the approval-gate
                // proposals before execute. The approval path emits
                // `ApprovalRequested` rather than dispatching; the tool_call
                // frame captures the INTENT of the action that triggered the
                // gate so replay can reconstruct the audit trail.
                for proposal in &decide_output.proposals {
                    if let Some(tool_name) = &proposal.tool_name {
                        let args = proposal
                            .tool_args
                            .clone()
                            .unwrap_or(serde_json::Value::Null);
                        if let Err(e) = self.task_sink.log_tool_call(tool_name, &args).await {
                            tracing::warn!(
                                run_id = %ctx.run_id,
                                tool = %tool_name,
                                error = %e,
                                "task_sink.log_tool_call (approval gate) failed — frame lost"
                            );
                        }
                    }
                }

                let execute_outcome = self.execute.execute(ctx, &decide_output).await.map_err(|e| {
                    tracing::error!(run_id = %ctx.run_id, error = %e, "execute (approval gate) failed");
                    e
                })?;

                // F47 PR1: scan this iteration's results incrementally so
                // tool_output payloads aren't retained across the whole run.
                // Approval-gate inline dispatch path.
                for r in &execute_outcome.results {
                    verification_acc.observe(r);
                }

                // T5-M8: mirror the main-path post-execute bookkeeping so
                // resuming from a checkpointed approval-suspended run sees a
                // step_summary, a persisted checkpoint, and a step_completed
                // emission for this iteration. Sync ctx.step_history before
                // calling the hook so the checkpoint snapshot captures the
                // freshly-pushed summary (not just the prior iterations).
                let step_summary = build_step_summary(ctx, &decide_output, &execute_outcome);
                step_history.push(step_summary);
                ctx.step_history = step_history.clone();
                if let Err(e) = self
                    .checkpoint_hook
                    .save(ctx, &gather_output, &decide_output, &execute_outcome)
                    .await
                {
                    tracing::warn!(
                        run_id = %ctx.run_id,
                        iteration = ctx.iteration,
                        error = %e,
                        "approval gate: checkpoint save failed — continuing without checkpoint"
                    );
                }
                self.emitter
                    .on_step_completed(ctx, &decide_output, &execute_outcome)
                    .await;

                // The execute phase returns AwaitingApproval for the relevant action.
                for result in &execute_outcome.results {
                    if let ActionStatus::AwaitingApproval { approval_id } = &result.status {
                        tracing::info!(
                            run_id      = %ctx.run_id,
                            approval_id = %approval_id,
                            "run suspended — waiting for approval"
                        );
                        return Ok(LoopTermination::WaitingApproval {
                            approval_id: approval_id.clone(),
                        });
                    }
                }

                // No AwaitingApproval result — the BP-v2 ToolCallApproval
                // path ran the whole propose-then-await flow inline: the
                // proposal was submitted, the operator resolved it (or
                // it timed out), and the tool already dispatched +
                // recorded its result in this very execute_outcome.
                //
                // Treat the terminal loop_signal from the outcome as
                // authoritative: Continue (if the tool succeeded and
                // there's more work) flows back into the main loop via
                // the logic below; Done/Failed/etc. terminate via the
                // same match arms the non-approval path uses.
                //
                // Emit per-result tool_called/tool_result AND FF
                // tool_result frames so SSE + FF attempt-stream
                // telemetry stay consistent with the non-approval
                // path. The approval-gate pre-execute log_tool_call
                // already fired above; we emit on_tool_called here
                // too so SSE timelines render a matching begin/end
                // pair (the dashboard treats on_tool_called as the
                // "started" event — without it the dispatch would
                // show only a "result" with no corresponding call).
                for result in &execute_outcome.results {
                    let Some(tool_name) = result.proposal.tool_name.as_deref() else {
                        continue;
                    };
                    let args = result
                        .proposal
                        .tool_args
                        .clone()
                        .unwrap_or(serde_json::Value::Null);
                    self.emitter
                        .on_tool_called(ctx, tool_name, Some(&args))
                        .await;
                    let (succeeded, error) = match &result.status {
                        ActionStatus::Succeeded => (true, None),
                        ActionStatus::Failed { reason } => (false, Some(reason.as_str())),
                        _ => (false, None),
                    };
                    self.emitter
                        .on_tool_result(
                            ctx,
                            tool_name,
                            succeeded,
                            result.tool_output.as_ref(),
                            error,
                            result.duration_ms,
                        )
                        .await;
                    // FF attempt-stream: tool_result frame mirrors
                    // the main-path log_tool_result at loop-runner
                    // line ~820. `restore_frames()` expects this on
                    // every dispatched call.
                    let output = match &result.tool_output {
                        Some(v) => v.clone(),
                        None => match error {
                            Some(reason) => serde_json::json!({"error": reason}),
                            None => serde_json::Value::Null,
                        },
                    };
                    if let Err(e) = self
                        .task_sink
                        .log_tool_result(tool_name, &output, succeeded, result.duration_ms)
                        .await
                    {
                        tracing::warn!(
                            run_id = %ctx.run_id,
                            tool = %tool_name,
                            error = %e,
                            "BP-v2 inline: task_sink.log_tool_result failed — frame lost"
                        );
                    }
                }

                match execute_outcome.loop_signal.clone() {
                    LoopSignal::Done => {
                        let summary = decide_output
                            .proposals
                            .iter()
                            .find(|p| p.action_type == cairn_domain::ActionType::CompleteRun)
                            .map(|p| p.description.clone())
                            .unwrap_or_else(|| "run completed".to_owned());
                        // F47 PR1: finalise the incremental verification
                        // sidecar. The extractor is pure; no IO happens
                        // on this path. `.clone().finish()` because the
                        // second Done branch (legacy fall-through) owns
                        // the binding and only one branch executes per
                        // run; clippy's dead-code analysis would
                        // otherwise flag the unreachable arm.
                        let verification = verification_acc.clone().finish();
                        return Ok(LoopTermination::Completed {
                            summary,
                            verification,
                        });
                    }
                    LoopSignal::Failed { reason } => {
                        return Ok(LoopTermination::Failed { reason });
                    }
                    LoopSignal::WaitSubagent { child_task_id } => {
                        return Ok(LoopTermination::WaitingSubagent { child_task_id });
                    }
                    LoopSignal::WaitApproval { approval_id } => {
                        // Redundant with the `AwaitingApproval` scan
                        // above, but covers derive_signal paths that
                        // set WaitApproval without the per-result
                        // status (legacy Escalate path).
                        return Ok(LoopTermination::WaitingApproval { approval_id });
                    }
                    LoopSignal::PlanProposed { plan_markdown } => {
                        return Ok(LoopTermination::PlanProposed { plan_markdown });
                    }
                    LoopSignal::Continue => {
                        // BP-v2 dispatched successfully; bump iteration
                        // and fall through to the next loop turn.
                        ctx.iteration = ctx.iteration.saturating_add(1);
                        continue;
                    }
                }
            }

            // ── (4b) RFC 020 Track 4: INTENT CHECKPOINT ───────────────────────
            // Before any dispatch, persist the decide output + planned
            // `ToolCallId`s (Track 3 minted these in execute's pre-dispatch
            // stage; see `execute_impl.rs` — the Intent checkpoint captures
            // the *intent* to invoke them). On crash between here and
            // execute completion, recovery walks the planned IDs, consults
            // `ToolCallResultCache`, and re-dispatches only misses.
            // Default `CheckpointHook::save_intent` is a no-op; production
            // wiring overrides to invoke `CheckpointService::save_dual(…,
            // CheckpointKind::Intent, …)`. Failure is logged + swallowed;
            // RFC 020 invariant #5 is best-effort, not blocking.
            if let Err(e) = self
                .checkpoint_hook
                .save_intent(ctx, &gather_output, &decide_output)
                .await
            {
                tracing::warn!(
                    run_id    = %ctx.run_id,
                    iteration = ctx.iteration,
                    error     = %e,
                    "intent checkpoint save failed — continuing without intent checkpoint"
                );
            }

            // ── (4c) #660 strict completion gate ─────────────────────────────
            //
            // Belt-and-suspenders for the role-prompt completion gate
            // shipped in #662. The role prompt tells the LLM "don't emit
            // `complete_run` with a failing `completion_verification`";
            // this gate enforces it in case the LLM lies. When the
            // accumulator has at least one error line AND DECIDE emitted
            // a `CompleteRun` proposal:
            //
            //   * Strip the `CompleteRun` from `decide_output.proposals`
            //     so execute never dispatches `RunService::complete`
            //     (which would flip the run to `state=completed`).
            //   * Push a synthesised `StepSummary` into `step_history`
            //     carrying a short excerpt of the first errors so the
            //     next DECIDE turn sees the rejection + the concrete
            //     diagnostics in its user-message context.
            //   * On the third consecutive rejection, terminate with
            //     `LoopTermination::Failed { reason = "verification_rejected: …" }`
            //     so `finalize_run_failure` can map the reason to
            //     `FailureClass::VerificationRejected` (issue #660). Three
            //     rejections is enough for a genuinely self-correcting
            //     model to recover; any more would just burn budget
            //     against a stuck loop.
            //
            // Soft-fail posture: if anything in the gate's inspection goes
            // sideways (unlikely — it's a pure read of `verification_acc`),
            // log WARN + fall through to the pre-fix behaviour. A platform
            // gate that blocks the happy path when its own check breaks
            // is worse than the non-authoritative baseline.
            if self.config.orchestrator_strict_completion_gate {
                let gate_would_reject = decide_output
                    .proposals
                    .iter()
                    .any(|p| p.action_type == cairn_domain::ActionType::CompleteRun)
                    && verification_acc.error_count() > 0;

                if gate_would_reject {
                    let error_count = verification_acc.error_count();
                    let preview: Vec<String> = verification_acc
                        .errors()
                        .iter()
                        .take(crate::context::COMPLETION_GATE_ERROR_PREVIEW)
                        .cloned()
                        .collect();

                    completion_gate_rejections = completion_gate_rejections.saturating_add(1);

                    tracing::warn!(
                        run_id        = %ctx.run_id,
                        iteration     = ctx.iteration,
                        rejection_num = completion_gate_rejections,
                        error_count,
                        "#660 strict completion gate rejecting complete_run — \
                         verification accumulator has errors"
                    );

                    if completion_gate_rejections >= crate::context::MAX_COMPLETION_GATE_REJECTIONS
                    {
                        // Budget cap hit. Terminate the run so it can't
                        // ping-pong against the gate until `max_iterations`.
                        // Reason string is matched by the handler's
                        // `classify_failed_reason` to set the run's
                        // `FailureClass::VerificationRejected` terminal
                        // state. Keep the literal prefix stable — it's a
                        // contract with `crates/cairn-app/src/handlers/runs/helpers.rs`.
                        let reason = format!(
                            "verification_rejected: {error_count} error(s) after \
                             {completion_gate_rejections} complete_run attempts. \
                             First errors: {}",
                            preview.join(" | "),
                        );
                        tracing::warn!(
                            run_id    = %ctx.run_id,
                            iteration = ctx.iteration,
                            %reason,
                            "#660 completion gate hit rejection cap — failing run"
                        );
                        return Ok(LoopTermination::Failed { reason });
                    }

                    // Strip the CompleteRun proposal(s) from this turn's
                    // decide output so execute dispatches only the
                    // non-terminal remainder (most turns have nothing
                    // else, in which case execute is a no-op).
                    decide_output
                        .proposals
                        .retain(|p| p.action_type != cairn_domain::ActionType::CompleteRun);

                    // Synthesised rejection step so the next DECIDE turn
                    // sees the concrete error excerpts in its user
                    // message via the `## Step history` section (see
                    // `decide_impl::build_user_message`). Marked
                    // `succeeded=false` so the model reads it as a
                    // failure signal, not a completed action.
                    let rejection_summary = if preview.is_empty() {
                        format!(
                            "complete_run refused by strict completion gate: \
                             verification accumulator has {error_count} error(s). \
                             Fix them before calling complete_run again \
                             (attempt {completion_gate_rejections} of {}).",
                            crate::context::MAX_COMPLETION_GATE_REJECTIONS,
                        )
                    } else {
                        format!(
                            "complete_run refused by strict completion gate: \
                             verification reports {error_count} error(s). \
                             Fix them before calling complete_run again \
                             (attempt {completion_gate_rejections} of {}). \
                             First errors: {}",
                            crate::context::MAX_COMPLETION_GATE_REJECTIONS,
                            preview.join(" | "),
                        )
                    };
                    let rejection_step = StepSummary {
                        iteration: ctx.iteration,
                        action_kind: "complete_run_rejected".to_owned(),
                        summary: rejection_summary,
                        succeeded: false,
                    };
                    step_history.push(rejection_step);
                    ctx.step_history = step_history.clone();

                    // If nothing non-terminal is left to dispatch this
                    // turn, skip execute entirely and let the next
                    // iteration re-run DECIDE with the rejection
                    // summary in view. Otherwise fall through to the
                    // normal execute path for the remaining proposals.
                    if decide_output.proposals.is_empty() {
                        ctx.iteration = ctx.iteration.saturating_add(1);
                        continue;
                    }
                }
            }

            // ── (5a) FF stream: tool_call frames (intent) ────────────────────
            // Appended BEFORE execute so a process restart mid-dispatch leaves
            // an in-flight marker that `restore_frames()` can observe. Only
            // proposals with a concrete tool_name are framed; bookkeeping
            // action types (CompleteRun, EscalateToOperator, …) have no
            // stream-facing analogue.
            for proposal in &decide_output.proposals {
                if let Some(tool_name) = &proposal.tool_name {
                    let args = proposal
                        .tool_args
                        .clone()
                        .unwrap_or(serde_json::Value::Null);
                    if let Err(e) = self.task_sink.log_tool_call(tool_name, &args).await {
                        tracing::warn!(
                            run_id    = %ctx.run_id,
                            iteration = ctx.iteration,
                            tool      = %tool_name,
                            error     = %e,
                            "task_sink.log_tool_call failed — frame lost, loop continues"
                        );
                    }
                }
            }

            // ── (5b) EXECUTE ──────────────────────────────────────────────────
            let execute_outcome = self.execute.execute(ctx, &decide_output).await.map_err(|e| {
                tracing::error!(run_id = %ctx.run_id, iteration = ctx.iteration, error = %e, "execute failed");
                e
            })?;

            // F47 PR1: scan this iteration's results incrementally
            // (main execute path). See `verification_acc` rustdoc for why
            // scan-and-discard is preferred over retaining `ActionResult`.
            for r in &execute_outcome.results {
                verification_acc.observe(r);
            }

            let succeeded_count = execute_outcome
                .results
                .iter()
                .filter(|r| r.status == ActionStatus::Succeeded)
                .count();
            let failed_count = execute_outcome
                .results
                .iter()
                .filter(|r| matches!(r.status, ActionStatus::Failed { .. }))
                .count();

            tracing::debug!(
                run_id    = %ctx.run_id,
                iteration = ctx.iteration,
                succeeded = succeeded_count,
                failed    = failed_count,
                signal    = ?execute_outcome.loop_signal,
                "execute complete"
            );

            // Emit per-action tool_called / tool_result events AND append
            // matching FF stream frames. The two channels are independent:
            // `OrchestratorEventEmitter` drives cairn-store projections + SSE
            // (existing behavior); `task_sink.log_tool_result` appends to
            // FF's attempt-scoped stream so `restore_frames()` can replay on
            // resume. Stream-frame failures are logged and swallowed.
            //
            // `result.duration_ms` is stamped per-proposal inside
            // `ExecutePhase::execute` (see `execute_impl.rs::execute`). 0
            // means "unknown / below-timer-resolution" or "result was
            // synthesised by a test stub that bypassed the dispatch wrapper" —
            // NOT "zero time." Downstream consumers MUST treat 0 as no-signal.
            // The `ActionResult.duration_ms` rustdoc is the canonical reference.
            for result in &execute_outcome.results {
                // T5-M4: only emit tool_called/tool_result for proposals
                // that actually carry a tool_name. CompleteRun /
                // EscalateToOperator / CreateMemory have no tool_name and
                // previously leaked `tool_name = description` (e.g.
                // `"all done"`) into the SSE stream, producing a misleading
                // tool-call timeline.
                let Some(tool_name) = result.proposal.tool_name.as_deref() else {
                    continue;
                };
                self.emitter
                    .on_tool_called(ctx, tool_name, result.proposal.tool_args.as_ref())
                    .await;
                let (succeeded, error) = match &result.status {
                    ActionStatus::Succeeded => (true, None),
                    ActionStatus::Failed { reason } => (false, Some(reason.as_str())),
                    // AwaitingApproval / SubagentSpawned are not
                    // success/failure terminal states for the tool; report
                    // succeeded=false without an error string so the
                    // dashboard doesn't claim the tool ran.
                    _ => (false, None),
                };
                self.emitter
                    .on_tool_result(
                        ctx,
                        tool_name,
                        succeeded,
                        result.tool_output.as_ref(),
                        error,
                        result.duration_ms,
                    )
                    .await;

                // FF stream frame — only emitted when the proposal carried a
                // concrete tool_name (matches the pre-execute log_tool_call).
                // AwaitingApproval / SubagentSpawned statuses have no output
                // to log; record success=false with a null output so the
                // frame pair is balanced.
                if result.proposal.tool_name.is_some() {
                    let output = match &result.tool_output {
                        Some(v) => v.clone(),
                        None => {
                            if let Some(reason) = error {
                                serde_json::json!({ "error": reason })
                            } else {
                                serde_json::Value::Null
                            }
                        }
                    };
                    if let Err(e) = self
                        .task_sink
                        .log_tool_result(tool_name, &output, succeeded, result.duration_ms)
                        .await
                    {
                        tracing::warn!(
                            run_id    = %ctx.run_id,
                            iteration = ctx.iteration,
                            tool      = %tool_name,
                            error     = %e,
                            "task_sink.log_tool_result failed — frame lost, loop continues"
                        );
                    }
                }
            }

            // ── (6) CHECKPOINT ────────────────────────────────────────────────
            // Build a step summary for this iteration so the gather phase can
            // reconstruct history on the next run or after a resume.
            // The execute phase has already handled per-tool-call checkpointing
            // (per LoopConfig::checkpoint_every_n_tool_calls); this step captures
            // the iteration-level summary and calls the injected checkpoint hook.
            let step_summary = build_step_summary(ctx, &decide_output, &execute_outcome);
            step_history.push(step_summary);
            // Sync ctx.step_history so the hook sees the just-pushed summary.
            ctx.step_history = step_history.clone();

            // RFC 020 Track 4: call `save_result` (dual-checkpoint Result
            // side). Default impl delegates to `save`, so legacy single-
            // checkpoint hooks keep their existing behavior.
            if let Err(e) = self
                .checkpoint_hook
                .save_result(ctx, &gather_output, &decide_output, &execute_outcome)
                .await
            {
                // Checkpoint failures are logged but do NOT abort the run.
                // The next successful checkpoint will capture the current state.
                tracing::warn!(
                    run_id    = %ctx.run_id,
                    iteration = ctx.iteration,
                    error     = %e,
                    "result checkpoint save failed — continuing without checkpoint"
                );
            } else {
                tracing::debug!(
                    run_id    = %ctx.run_id,
                    iteration = ctx.iteration,
                    "result checkpoint saved"
                );
            }

            // ── (6b) FF stream: checkpoint frame ─────────────────────────────
            // Write half of the `restore_frames()` read path. Serializes the
            // per-iteration context snapshot (iteration number, run/session
            // ids, the step summary just pushed, and the loop_signal) as
            // JSON and appends it as a `checkpoint` frame on the attempt
            // stream. A cross-process resumer reads the stream
            // via `restore_frames()` and rebuilds enough context to pick up
            // where the previous attempt left off.
            //
            // Same best-effort contract as tool/llm frames: failure = WARN +
            // continue. See `task_sink` module docs for the nuance (a lost
            // checkpoint frame means restart-resumption silently misses this
            // iteration's state; kept advisory for consistency with the
            // existing `CheckpointHook::save` failure policy).
            let checkpoint_snapshot = serde_json::json!({
                "iteration": ctx.iteration,
                "run_id": ctx.run_id.to_string(),
                "session_id": ctx.session_id.to_string(),
                "step_summary": step_history.last(),
                "loop_signal": format!("{:?}", execute_outcome.loop_signal),
            });
            match serde_json::to_vec(&checkpoint_snapshot) {
                Ok(checkpoint_bytes) => {
                    if let Err(e) = self.task_sink.save_checkpoint(&checkpoint_bytes).await {
                        tracing::warn!(
                            run_id    = %ctx.run_id,
                            iteration = ctx.iteration,
                            error     = %e,
                            "task_sink.save_checkpoint failed — frame lost, loop continues"
                        );
                    }
                }
                Err(e) => {
                    // Should be unreachable (the snapshot is built from
                    // owned primitives + Debug format), but don't eat the
                    // failure silently. Loss of a checkpoint frame is
                    // advisory (see CAIRN-FABRIC-FINALIZED.md §4.5) —
                    // WARN + continue matches the other sink failure paths.
                    tracing::warn!(
                        run_id    = %ctx.run_id,
                        iteration = ctx.iteration,
                        error     = %e,
                        "failed to serialize checkpoint snapshot — frame lost, loop continues"
                    );
                }
            }

            self.emitter
                .on_step_completed(ctx, &decide_output, &execute_outcome)
                .await;

            // ── (7) Loop signal ───────────────────────────────────────────────
            match execute_outcome.loop_signal {
                LoopSignal::Done => {
                    let summary = decide_output
                        .proposals
                        .iter()
                        .find(|p| p.action_type == cairn_domain::ActionType::CompleteRun)
                        .map(|p| p.description.clone())
                        .unwrap_or_else(|| "run completed".to_owned());
                    // F47 PR1: verification sidecar from the incremental
                    // accumulator. See extractor rustdoc for semantics.
                    let verification = verification_acc.clone().finish();

                    tracing::info!(
                        run_id        = %ctx.run_id,
                        iteration     = ctx.iteration,
                        summary       = %summary,
                        warnings      = verification.warnings.len(),
                        errors        = verification.errors.len(),
                        scanned       = verification.tool_results_scanned,
                        "orchestrator loop completed"
                    );
                    return Ok(LoopTermination::Completed {
                        summary,
                        verification,
                    });
                }

                LoopSignal::Failed { reason } => {
                    tracing::warn!(
                        run_id    = %ctx.run_id,
                        iteration = ctx.iteration,
                        reason    = %reason,
                        "orchestrator loop failed"
                    );
                    return Ok(LoopTermination::Failed { reason });
                }

                LoopSignal::WaitApproval { approval_id } => {
                    tracing::info!(
                        run_id      = %ctx.run_id,
                        approval_id = %approval_id,
                        "orchestrator loop suspended — waiting for approval"
                    );
                    return Ok(LoopTermination::WaitingApproval { approval_id });
                }

                LoopSignal::WaitSubagent { child_task_id } => {
                    tracing::info!(
                        run_id        = %ctx.run_id,
                        child_task_id = %child_task_id,
                        "orchestrator loop suspended — waiting for subagent"
                    );
                    return Ok(LoopTermination::WaitingSubagent { child_task_id });
                }

                LoopSignal::PlanProposed { plan_markdown } => {
                    tracing::info!(
                        run_id    = %ctx.run_id,
                        iteration = ctx.iteration,
                        "plan proposed via loop signal"
                    );
                    return Ok(LoopTermination::PlanProposed { plan_markdown });
                }

                LoopSignal::Continue => {
                    // Extract any tools discovered via tool_search this iteration
                    // and carry them into the next iteration's context so that
                    // LlmDecidePhase can inject their descriptors into the prompt.
                    let newly_discovered = extract_tool_search_discoveries(&execute_outcome);
                    if !newly_discovered.is_empty() {
                        tracing::debug!(
                            run_id    = %ctx.run_id,
                            iteration = ctx.iteration,
                            tools     = ?newly_discovered,
                            "tool_search discovered new tools — injecting into next prompt"
                        );
                        for name in newly_discovered {
                            if !ctx.discovered_tool_names.contains(&name) {
                                ctx.discovered_tool_names.push(name);
                            }
                        }
                    }
                    ctx.iteration = ctx.iteration.saturating_add(1);
                    tracing::debug!(
                        run_id    = %ctx.run_id,
                        iteration = ctx.iteration,
                        "continue to next iteration"
                    );
                }
            }
        }

        // All iterations exhausted.
        tracing::warn!(
            run_id     = %ctx.run_id,
            iterations = self.config.max_iterations,
            "orchestrator loop reached iteration cap"
        );
        Ok(LoopTermination::MaxIterationsReached)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CompactionResult {
    before_steps: usize,
    after_steps: usize,
    before_tokens_est: usize,
    after_tokens_est: usize,
}

fn maybe_compact_history(
    step_history: &mut Vec<StepSummary>,
    iteration: u32,
    config: &crate::CompactionConfig,
    last_compaction_iteration: &mut Option<u32>,
) -> Option<CompactionResult> {
    if !config.enabled || step_history.len() < config.min_steps {
        return None;
    }

    if let Some(last_iteration) = *last_compaction_iteration {
        let cooldown = config.cooldown_iterations;
        if cooldown > 0 && iteration.saturating_sub(last_iteration) < cooldown {
            return None;
        }
    }

    let history_text: String = step_history
        .iter()
        .map(|s| format!("[iter {}] {}: {}", s.iteration, s.action_kind, s.summary))
        .collect::<Vec<_>>()
        .join("\n");
    let history_tokens = crate::decide_impl::estimate_tokens(&history_text);
    // Use a rough context budget estimate (default 16K if no budget set).
    let context_budget = 16_384_usize;
    let threshold_tokens = (context_budget as u64 * config.threshold_pct as u64 / 100) as usize;

    if history_tokens <= threshold_tokens {
        return None;
    }

    let keep = config.keep_last;
    let to_compact = if step_history.len() > keep {
        step_history.len() - keep
    } else {
        0
    };

    if to_compact == 0 {
        return None;
    }

    let before_steps = step_history.len();

    let compacted_text: String = step_history[..to_compact]
        .iter()
        .map(|s| {
            let status = if s.succeeded { "ok" } else { "fail" };
            format!("  iter {}: {} [{}]", s.iteration, s.action_kind, status)
        })
        .collect::<Vec<_>>()
        .join("\n");

    let summary = StepSummary {
        iteration: step_history[to_compact - 1].iteration,
        action_kind: "compacted_summary".to_owned(),
        summary: format!("Compacted {} prior steps:\n{}", to_compact, compacted_text),
        succeeded: true,
    };

    let recent: Vec<StepSummary> = step_history[to_compact..].to_vec();
    step_history.clear();
    step_history.push(summary);
    step_history.extend(recent);

    let after_tokens = step_history
        .iter()
        .map(|s| crate::decide_impl::estimate_tokens(&s.summary))
        .sum::<usize>();

    *last_compaction_iteration = Some(iteration);

    Some(CompactionResult {
        before_steps,
        after_steps: step_history.len(),
        before_tokens_est: history_tokens,
        after_tokens_est: after_tokens,
    })
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Extract tool names from any `tool_search` results in the execute outcome.
///
/// When the LLM calls `tool_search`, the execute phase stores the JSON result
/// in `ActionResult::tool_output`.  This function parses the `matches` array
/// and returns the discovered tool names so the loop runner can carry them into
/// the next iteration's `OrchestrationContext::discovered_tool_names`.
fn extract_tool_search_discoveries(outcome: &ExecuteOutcome) -> Vec<String> {
    let mut names = Vec::new();
    for result in &outcome.results {
        // Only look at results for tool_search invocations
        let is_tool_search = result.proposal.tool_name.as_deref() == Some("tool_search");
        if !is_tool_search {
            continue;
        }
        if let Some(output) = &result.tool_output {
            if let Some(matches) = output.get("matches").and_then(|m| m.as_array()) {
                for m in matches {
                    if let Some(name) = m.get("name").and_then(|n| n.as_str()) {
                        names.push(name.to_owned());
                    }
                }
            }
        }
    }
    names
}

/// Extract the `<proposed_plan>` block from an LLM response (RFC 018).
///
/// Returns `Some(plan_markdown)` if the response contains a `<proposed_plan>`
/// block, `None` otherwise. Strips the XML tags.
fn extract_proposed_plan(response: &str) -> Option<String> {
    let start_tag = "<proposed_plan>";
    let end_tag = "</proposed_plan>";
    let start = response.find(start_tag)?;
    let content_start = start + start_tag.len();
    let end = response[content_start..].find(end_tag)?;
    let plan = response[content_start..content_start + end].trim();
    if plan.is_empty() {
        None
    } else {
        Some(plan.to_owned())
    }
}

/// Build a `StepSummary` from the completed iteration.
fn build_step_summary(
    ctx: &OrchestrationContext,
    decide: &DecideOutput,
    execute: &ExecuteOutcome,
) -> StepSummary {
    let action_kind = decide
        .proposals
        .first()
        .map(|p| {
            serde_json::to_value(&p.action_type)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_else(|| "unknown".to_owned())
        })
        .unwrap_or_else(|| "no_op".to_owned());

    let description = decide
        .proposals
        .first()
        .map(|p| p.description.clone())
        .unwrap_or_else(|| format!("iteration {} complete", ctx.iteration));

    // F35: threading tool errors back into the next DECIDE turn.
    //
    // The orchestrator does not maintain a multi-turn assistant/tool
    // message history (each DECIDE is a fresh `system`+`user` call with
    // the step history embedded). So the only channel that carries a
    // failed tool result's error text into the next LLM turn is the
    // per-step `summary` string. Pre-F35 this only carried the proposal
    // `description` ("read the design document"), so even once we stop
    // terminating on tool errors the LLM would still be blind to *what*
    // failed — it would just see `ok=false` with no explanation and
    // repeat the same broken call.
    //
    // Fix: for InvokeTool proposals whose status is `Failed`, append the
    // concrete error text. For successful tool calls, append a short
    // output preview so the LLM doesn't have to guess what came back.
    // Both are truncated so a massive payload doesn't dominate the step
    // history budget on later iterations.
    // Cursor Bugbot + Copilot review feedback on PR #295: iterate
    // `execute.results` directly rather than zipping against
    // `decide.proposals`. `ExecuteOutcome::results` is flattened in
    // `execute_impl.rs` (`results.into_iter().flatten().collect()`) to
    // drop None slots for proposals that a terminal short-circuit
    // prevented from running — for example when Phase 1 sets
    // `WaitApproval` on an InvokeTool at index N, any non-InvokeTool
    // proposal at indices < N that had not yet been dispatched by Phase
    // 2 will be `None` and removed by `flatten()`. Zipping against
    // `decide.proposals` after that would shift results left and pair
    // each result with the wrong proposal, silently misattributing tool
    // error / success text.
    //
    // Each `ActionResult` already carries its own `proposal`, so the
    // correct iteration is over `execute.results` using
    // `result.proposal` as the source of truth.
    let mut summary = description;
    for result in execute.results.iter() {
        if result.proposal.action_type != cairn_domain::ActionType::InvokeTool {
            continue;
        }
        let tool_name = result.proposal.tool_name.as_deref().unwrap_or("<unknown>");
        match &result.status {
            ActionStatus::Failed { reason } => {
                let preview = truncate_for_summary(reason, 400);
                summary.push_str(&format!("\n  tool_result[{tool_name}] ERROR: {preview}"));
            }
            ActionStatus::Succeeded => {
                if let Some(output) = result.tool_output.as_ref() {
                    // F54: tool-aware preview. For bash tools this
                    // prepends `exit_code:` and keeps the tail of
                    // stdout/stderr so the LLM can see the trailing
                    // `warning:` / `error:` / success banner that
                    // previously was clipped by the head+tail 400-char
                    // cap on the serialized JSON blob.
                    let preview = render_tool_output_preview(tool_name, output);
                    summary.push_str(&format!("\n  tool_result[{tool_name}] ok: {preview}"));
                }
            }
            // AwaitingApproval / SubagentSpawned have their own dedicated
            // step-kinds — no inline note needed.
            _ => {}
        }
    }

    // A step counts as failed only when the loop itself failed — i.e. a
    // non-InvokeTool terminal error. InvokeTool failures are recoverable
    // feedback (F35) and must not mark the step as failed, otherwise the
    // LLM sees `ok=false` and a terminal "run failed" signal in the
    // prompt even though execution is continuing.
    let succeeded = !matches!(execute.loop_signal, LoopSignal::Failed { .. });

    StepSummary {
        iteration: ctx.iteration,
        action_kind,
        summary,
        succeeded,
    }
}

/// F54: tool-aware preview rendering for the `tool_result[<name>] ok:`
/// line in a [`StepSummary`].
///
/// For `bash` / `shell_exec` / `run_bash`, we do not fall through to
/// the generic JSON serialisation + head-and-tail truncation. Instead
/// we:
///
///   1. Surface `exit_code` on its own line so the LLM can pattern-match
///      "did the previous invocation succeed?" without having to parse
///      a truncated JSON object. Accepts every exit-code key spelling
///      that [`completion_verification::extract_exit_code`] already
///      recognises (`exit_code`, `exitCode`, `returncode`,
///      `return_code`, `status`) so adapter drift does not silently
///      degrade the preview to `exit_code: ?`.
///   2. Preserve the **tail** of stdout/stderr (last ~2 KB) because
///      build tools stream "Compiling X" headers and put the decisive
///      signal — `warning:`, `error:`, or a success banner — at the
///      very end.
///
/// For non-bash tools we keep the existing head+tail 400-char cap: read
/// tools dump a file body whose head is usually the most informative
/// section, and the LLM has already seen the surrounding action
/// description.
///
/// The bash-name matching and exit-code extraction are shared with
/// `completion_verification` via the `is_bash_tool` and
/// `extract_exit_code` helpers so the two subsystems cannot drift
/// (e.g. adding a new bash-class adapter name elsewhere).
fn render_tool_output_preview(tool_name: &str, output: &serde_json::Value) -> String {
    if crate::completion_verification::is_bash_tool(tool_name) {
        if let serde_json::Value::Object(map) = output {
            return render_bash_preview(output, map);
        }
    }

    truncate_for_summary(&output.to_string(), 400)
}

/// Tail-biased bash preview: prepend `exit_code`, then render the tail
/// of `stdout`, then the tail of `stderr`. Total cap stays under ~2.2
/// KB so a busy run with many iterations does not balloon the
/// DECIDE-phase prompt.
///
/// Why stderr last: `cargo`, `rustc`, and most build tools emit
/// diagnostics on stderr while using stdout for informational banners
/// ("Compiling X v0.1.0"). Putting stderr at the end means the
/// compiler diagnostic — the signal the LLM actually needs to decide
/// "did the gate pass?" — survives any downstream truncation done by
/// the decide-phase prompt builder.
fn render_bash_preview(
    output: &serde_json::Value,
    map: &serde_json::Map<String, serde_json::Value>,
) -> String {
    let mut out = String::with_capacity(2048);

    // Use the shared extract_exit_code so alternate key spellings
    // (`exitCode`, `returncode`, …) surface the code correctly instead
    // of degrading to `exit_code: ?`.
    let exit_code = crate::completion_verification::extract_exit_code(output)
        .map(|n| n.to_string())
        .unwrap_or_else(|| "?".to_owned());
    out.push_str(&format!("exit_code: {exit_code}"));

    // Split the 2 KB budget 3:1 between stdout and stderr. Build-time
    // diagnostics usually land on stderr but are small; stdout can
    // carry multi-megabyte "Compiling X" streams.
    let stdout = map.get("stdout").and_then(|v| v.as_str()).unwrap_or("");
    let stderr = map.get("stderr").and_then(|v| v.as_str()).unwrap_or("");

    if !stdout.is_empty() {
        out.push_str("\nstdout(tail):\n");
        out.push_str(&tail_of(stdout, 1536));
    }
    if !stderr.is_empty() {
        out.push_str("\nstderr(tail):\n");
        out.push_str(&tail_of(stderr, 512));
    }

    out
}

/// Return the trailing `max_chars` of `text`, prepended with a
/// `[truncated …]` marker when material was dropped.
///
/// Review feedback (Gemini on PR #315): the previous implementation
/// was an O(N) forward scan over every char. For multi-megabyte tool
/// outputs that scan is a real cost on the orchestrator's hot path.
/// We now scan backwards via `char_indices().rev()` to locate the
/// tail's starting byte offset in O(max_chars) work, then slice the
/// original `&str` verbatim — no intermediate buffer, no per-char
/// copy. Memory stays O(max_chars) regardless of input size.
///
/// Edge case (Gemini): `max_chars == 0` returns an empty string when
/// the input is non-empty (with a truncation marker indicating the
/// whole input was dropped), rather than the degenerate
/// "full-string-plus-marker" the deque-based approach produced.
fn tail_of(text: &str, max_chars: usize) -> String {
    // Byte-length pre-check is a safe "definitely fits" fast path:
    // bytes ≥ chars for any valid UTF-8.
    if text.len() <= max_chars {
        return text.to_owned();
    }

    // Walk back through char boundaries; stop once we've counted
    // `max_chars` chars or exhausted the input. `char_indices().rev()`
    // is O(max_chars) in this use because we break early.
    let mut start = text.len();
    for (counted, (idx, _)) in text.char_indices().rev().enumerate() {
        if counted == max_chars {
            break;
        }
        start = idx;
    }

    // If the input turned out to have ≤ max_chars chars despite
    // `text.len() > max_chars` (possible with multi-byte UTF-8), return
    // verbatim with no marker.
    if start == 0 {
        return text.to_owned();
    }

    format!("[truncated …] …{}", &text[start..])
}

/// Truncate a string for inclusion in a step history `summary`. Keeps
/// head + tail so both the error kind prefix (e.g. `Error [NOT_FOUND]:`)
/// and the tail (usually the path or detail) survive even when the
/// middle of a multi-line payload is clipped. 1 char ≈ 0.25 tokens in
/// the decide-phase estimator.
fn truncate_for_summary(text: &str, max_chars: usize) -> String {
    // Review feedback on PR #295 (Gemini + Copilot):
    //   * Short-circuit path: fast byte-length pre-check so ASCII-only
    //     strings never touch the char iterator — `text.len()` is bytes
    //     but `bytes >= chars` for any valid UTF-8, so a `len <= max`
    //     byte test is a safe "definitely fits" fast path.
    //   * Truncation path: stream the prefix directly off
    //     `text.chars()` (bounded-size Vec) instead of collecting the
    //     whole input, so a 10 MB tool output doesn't materialise a
    //     10 MB `Vec<char>` just to keep 400 chars. The tail uses a
    //     bounded ring-buffer (`VecDeque` capacity `tail`) so the
    //     allocation stays O(max_chars) regardless of input size.
    if text.len() <= max_chars {
        return text.to_owned();
    }

    let head_cap = max_chars / 2;
    let tail_cap = max_chars.saturating_sub(head_cap);

    let mut prefix = String::with_capacity(head_cap * 4);
    let mut tail_buf: std::collections::VecDeque<char> =
        std::collections::VecDeque::with_capacity(tail_cap);
    let mut total = 0usize;

    for (i, c) in text.chars().enumerate() {
        total = i + 1;
        if i < head_cap {
            prefix.push(c);
        } else if tail_cap > 0 {
            if tail_buf.len() == tail_cap {
                tail_buf.pop_front();
            }
            tail_buf.push_back(c);
        }
    }

    // Final char-count check: if the input turned out to be short (bytes
    // > max_chars but chars ≤ max_chars — possible with multi-byte
    // characters), don't synthesise the truncation marker. Rebuild
    // verbatim from the streamed buffers.
    if total <= max_chars {
        prefix.extend(tail_buf);
        return prefix;
    }

    let omitted = total.saturating_sub(head_cap + tail_cap);
    let suffix: String = tail_buf.into_iter().collect();
    format!("{prefix}… [{omitted} chars omitted] …{suffix}")
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "loop_runner_tests.rs"]
mod tests;
