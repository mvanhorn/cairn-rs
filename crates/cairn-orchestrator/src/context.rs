//! Core types shared across all orchestrator phases.
//!
//! These types represent the data flowing between GatherPhase → DecidePhase
//! → ExecutePhase, plus the loop control signals and configuration.

use std::path::PathBuf;

use cairn_domain::{
    decisions::RunMode, session_orchestration::CircuitBreakerTrip, ActionProposal, ApprovalId,
    CompletionVerification, DefaultSetting, ProjectKey, RunId, SessionId, TaskId, ToolInvocationId,
};
use cairn_graph::GraphNode;
use cairn_memory::retrieval::RetrievalResult;
use cairn_store::projections::CheckpointRecord;
use cairn_store::StoredEvent;
use serde::{Deserialize, Serialize};

// ── OrchestrationContext ──────────────────────────────────────────────────────

/// Immutable context threaded through every phase of a single iteration.
///
/// Built once per run (or rebuilt from a checkpoint on resume) and passed
/// by reference to all three phases.
#[derive(Clone, Debug)]
pub struct OrchestrationContext {
    /// The project this execution belongs to.
    pub project: ProjectKey,
    pub session_id: SessionId,
    pub run_id: RunId,
    /// The task that holds the current execution lease, if any.
    pub task_id: Option<TaskId>,
    /// Which iteration of the loop we are on (0-based).
    pub iteration: u32,
    /// The original user goal / input message that started this run.
    pub goal: String,
    /// Agent type label used for routing, confidence calibration, and logging.
    pub agent_type: String,
    /// Wall-clock millisecond timestamp when this run began (for timeout checks).
    pub run_started_at_ms: u64,
    /// Working directory used for filesystem and process-oriented tools.
    pub working_dir: PathBuf,
    /// Execution mode for this run (RFC 018).
    ///
    /// - `Direct` — all tools visible, agent acts freely.
    /// - `Plan` — only Observational + Internal tools visible; run terminates
    ///   with `plan_proposed` when agent emits `<proposed_plan>`.
    /// - `Execute { plan_run_id }` — seeded with an approved plan artifact.
    pub run_mode: RunMode,
    /// Deferred tool names discovered via `tool_search` in previous iterations.
    ///
    /// The loop runner populates this after execute when it detects a
    /// `tool_search` result with matches.  `LlmDecidePhase` reads these names
    /// and injects their descriptors into the *next* iteration's system prompt,
    /// making discovered tools visible to the LLM without permanently promoting
    /// them to Registered tier.
    pub discovered_tool_names: Vec<String>,
    /// T5-H1: step summaries from prior iterations of this run, threaded
    /// from the loop runner into each iteration's gather phase so the
    /// LLM sees its own recent history. The loop mutates this in place
    /// after every iteration; gather copies it into `GatherOutput.step_history`
    /// (optionally merged with checkpointed entries on resume).
    pub step_history: Vec<StepSummary>,
    /// RFC 020 Track 3: true when this iteration is replaying after a crash.
    ///
    /// Set by the loop runner on entry to the execute phase if the run was
    /// resumed from a checkpoint. The execute phase consults this (together
    /// with the tool's `RetrySafety` classification) to decide whether to
    /// silently re-dispatch, serve from cache, or pause for operator approval.
    ///
    /// Defaults to `false` for fresh (non-recovered) runs. Set only for the
    /// first post-recovery iteration; subsequent iterations use `false`.
    pub is_recovery: bool,

    /// Maximum wall-clock the execute phase will wait on an operator
    /// approval before auto-rejecting the proposal. Threaded from
    /// `OrchestrateRequest.approval_timeout_ms` (HTTP) or defaulted to
    /// 24h when the caller didn't provide one.
    ///
    /// `None` means "never set" — downstream defaults to 24h. We keep the
    /// field `Option<Duration>` so `Default` / `Clone`-constructed contexts
    /// continue to work without having to pick a sentinel value here.
    pub approval_timeout: Option<std::time::Duration>,
}

impl OrchestrationContext {
    /// Build a `ToolContext` from this orchestration context.
    ///
    /// Populates `session_id`, `run_id`, `working_dir`, and leaves the
    /// remaining fields at their `Default` (tenant/workspace/project
    /// stay as the orchestrator doesn't thread tenant IDs through —
    /// the harness tools key their caches on `(session, run)` inside
    /// the supplied `ProjectKey`, which is passed separately).
    ///
    /// Used by the loop runner's `#606` cache-eviction hook and by
    /// tests that need a `ToolContext` matching an
    /// `OrchestrationContext`.
    pub fn tool_context(&self) -> cairn_tools::builtins::ToolContext {
        let mut tool_ctx = cairn_tools::builtins::ToolContext::default();
        tool_ctx.session_id = Some(self.session_id.to_string());
        tool_ctx.run_id = Some(self.run_id.to_string());
        tool_ctx.working_dir = self.working_dir.clone();
        tool_ctx
    }
}

// ── GatherOutput ─────────────────────────────────────────────────────────────

/// Context snapshot produced by the GatherPhase.
///
/// Everything the DecidePhase needs to build a decision prompt is
/// contained in this struct.
#[derive(Clone, Debug, Default)]
pub struct GatherOutput {
    /// Relevant memory chunks from semantic + lexical retrieval.
    /// Source: `cairn_memory::RetrievalService::query`
    pub memory_chunks: Vec<RetrievalResult>,

    /// Recent events for the current run (tool calls, approvals, checkpoints).
    /// Source: `cairn_store::EventLog::read_by_entity(EntityRef::Run(…))`
    pub recent_events: Vec<StoredEvent>,

    /// Graph neighbourhood nodes linked to this run/session (depth ≤ 2).
    /// Source: `cairn_graph::GraphQueryService`
    pub graph_nodes: Vec<GraphNode>,

    /// Operator settings that apply to this project/tenant.
    /// Source: `DefaultsReadModel::list_by_scope(Scope::Project, …)`
    pub operator_settings: Vec<DefaultSetting>,

    /// Most recent checkpoint for this run (for resume awareness).
    /// Source: `CheckpointService::latest_for_run`
    pub checkpoint: Option<CheckpointRecord>,

    /// Compressed summaries of prior steps in this run.
    pub step_history: Vec<StepSummary>,
}

/// A compressed record of what happened in a prior orchestration step.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StepSummary {
    pub iteration: u32,
    /// High-level kind: "tool_call", "subagent", "continue", "approval_wait", etc.
    pub action_kind: String,
    /// Human-readable or LLM-generated summary of the step.
    pub summary: String,
    /// Whether the step succeeded, failed, or is still pending.
    pub succeeded: bool,
}

// ── DecideOutput ─────────────────────────────────────────────────────────────

/// The proposed action set produced by the DecidePhase after calling the LLM.
#[derive(Clone, Debug)]
pub struct DecideOutput {
    /// Raw LLM response text (retained for audit and replay).
    pub raw_response: String,
    /// Structured actions the LLM proposed (parsed from `raw_response`).
    /// Re-uses `ActionProposal` from `cairn_domain::orchestrator`.
    pub proposals: Vec<ActionProposal>,
    /// Calibrated confidence after applying `ConfidenceCalibrator` adjustments.
    pub calibrated_confidence: f64,
    /// Whether any proposal requires operator approval before execution.
    pub requires_approval: bool,
    /// LLM model ID used for this decision.
    pub model_id: String,
    /// Round-trip latency of the LLM call in milliseconds.
    pub latency_ms: u64,
    /// Input (prompt) token count from the provider response.
    pub input_tokens: Option<u32>,
    /// Output (completion) token count from the provider response.
    pub output_tokens: Option<u32>,
    /// Issue #668: system prompt the LLM received for this decide
    /// iteration. Retained so the orchestrator's post-decide tracing
    /// can persist the full round-trip body for operator audit (see
    /// `LlmCompletionRecorded`). Pre-redaction — the emit site runs
    /// `cairn_providers::redact::redact_secrets` before persisting.
    pub system_prompt: String,
    /// Issue #668: JSON-serialised `Vec<Message>` (role + content) the
    /// orchestrator sent to the provider. Serialised rather than typed
    /// so the wire shape evolves with the provider library without
    /// forcing a domain-layer migration. Pre-redaction.
    pub messages_json: String,
    /// Issue #668: JSON-serialised `Vec<ToolCall>` the model returned
    /// via native tool calling, or `[]` if the model went through the
    /// legacy JSON-array text path. Pre-redaction.
    pub tool_calls_json: String,
}

impl DecideOutput {
    /// Number of proposals that count as "forward progress" for the
    /// NoToolUseConsecutive circuit breaker. A proposal counts if it
    /// either carries a concrete tool name OR targets a terminal /
    /// operator-gated action (complete_run / escalate_to_operator /
    /// spawn_subagent). The carve-out prevents a legitimate
    /// `complete_run` from tripping the streak breaker BEFORE the
    /// execute phase dispatches it (Cursor Bugbot HIGH on PR #348).
    ///
    /// `create_memory` is intentionally NOT in the terminal set —
    /// memorising a thought is not forward progress the user asked for.
    ///
    /// **Perf note (#510)**: today `proposals` is small (≤5 per DECIDE
    /// in production traces) so the O(n) walk costs tens of ns. This
    /// helper exists so that when DECIDE later emits batched tool
    /// bursts (50+ proposals), the count can be memoised on
    /// `DecideOutput` construction or routed through a provider-side
    /// segmented count without touching call sites.
    pub fn tool_or_terminal_count(&self) -> usize {
        self.proposals
            .iter()
            .filter(|p| {
                p.tool_name.is_some()
                    || matches!(
                        p.action_type,
                        cairn_domain::ActionType::CompleteRun
                            | cairn_domain::ActionType::EscalateToOperator
                            | cairn_domain::ActionType::SpawnSubagent
                    )
            })
            .count()
    }
}

// ── ExecuteOutcome ────────────────────────────────────────────────────────────

/// What actually happened after running the proposals from `DecideOutput`.
#[derive(Clone, Debug)]
pub struct ExecuteOutcome {
    /// Per-proposal results, in the same order as `decide_output.proposals`.
    pub results: Vec<ActionResult>,
    /// Loop control signal derived from the execution results.
    pub loop_signal: LoopSignal,
}

/// The result of executing one `ActionProposal`.
#[derive(Clone, Debug)]
pub struct ActionResult {
    /// The proposal that was (attempted to be) executed.
    pub proposal: ActionProposal,
    /// What happened when we tried to execute it.
    pub status: ActionStatus,
    /// For tool calls: the raw tool output (observation for next gather).
    pub tool_output: Option<serde_json::Value>,
    /// The `ToolInvocationId` recorded in the event log (for replay linkage).
    pub invocation_id: Option<ToolInvocationId>,
    /// Wall-clock duration of the dispatch, in milliseconds. Stamped by the
    /// `ExecutePhase` caller after `dispatch_one` returns; inner construction
    /// sites initialise to 0 and the outer wrapper overwrites with the real
    /// elapsed value.
    ///
    /// **0 means "unknown / below-timer-resolution" or "result was synthesised
    /// by a test stub that did not actually dispatch,"** NOT "the tool ran in
    /// literally zero time." Downstream consumers (FF attempt-stream replay,
    /// cost reconciliation, latency percentiles) MUST treat 0 as no-signal
    /// rather than zero-duration; averaging or percentile computations that
    /// trust 0-as-measured will under-report latency. The same convention
    /// applies to the `tool_result` frame the loop emits to FF's
    /// attempt_stream — see `loop_runner.rs` where this field feeds
    /// `log_tool_result`.
    pub duration_ms: u64,
}

/// Status of a single executed action.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActionStatus {
    /// The action completed without error.
    Succeeded,
    /// The action produced an error.
    Failed { reason: String },
    /// The action is blocked on an operator approval decision.
    /// The run has been transitioned to `waiting_approval`.
    AwaitingApproval { approval_id: ApprovalId },
    /// A subagent was spawned.  `child_task_id` is the schedulable unit.
    SubagentSpawned { child_task_id: TaskId },
}

// ── LoopSignal ────────────────────────────────────────────────────────────────

/// Tells `OrchestratorLoop` what to do after an `ExecutePhase` completes.
///
/// Mirrors `cairn_agent::react::LoopSignal` but defined here to avoid
/// a hard dependency on cairn-agent internals.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LoopSignal {
    /// Continue to the next iteration.
    Continue,
    /// Agent declared its work done.
    Done,
    /// Agent (or execute phase) hit an unrecoverable error.
    Failed { reason: String },
    /// Run is now blocked on an approval; loop suspends.
    WaitApproval { approval_id: ApprovalId },
    /// Run is blocked waiting for a spawned subagent to finish.
    WaitSubagent { child_task_id: TaskId },
    /// Plan-mode run: agent emitted a `<proposed_plan>` block (RFC 018).
    PlanProposed { plan_markdown: String },
}

impl LoopSignal {
    /// Canonical snake_case tag for telemetry/SSE, matching the rest of
    /// the cairn-domain `#[serde(rename_all = "snake_case")]` convention.
    pub fn kind(&self) -> &'static str {
        match self {
            LoopSignal::Continue => "continue",
            LoopSignal::Done => "done",
            LoopSignal::Failed { .. } => "failed",
            LoopSignal::WaitApproval { .. } => "wait_approval",
            LoopSignal::WaitSubagent { .. } => "wait_subagent",
            LoopSignal::PlanProposed { .. } => "plan_proposed",
        }
    }
}

// ── LoopTermination ──────────────────────────────────────────────────────────

/// Why `OrchestratorLoop::run` returned.
///
/// The HTTP handler (or task worker) inspects this to decide what to do
/// with the run and task lease.
#[derive(Clone, Debug)]
pub enum LoopTermination {
    /// Agent declared itself done; run has been completed.
    ///
    /// `verification` is the F47 PR1 sidecar: an extractor-produced summary
    /// of warning/error lines and per-command exit codes distilled from the
    /// tool_result frames observed during the run. The field is always
    /// populated (even for runs with no tool calls, in which case it is
    /// `CompletionVerification::default()` with `tool_results_scanned = 0`)
    /// so downstream consumers — SSE emitters, dashboards, PR2 persistence
    /// — can rely on a stable shape without optional plumbing.
    Completed {
        summary: String,
        verification: CompletionVerification,
    },
    /// Agent or runtime hit an unrecoverable error; run has been failed.
    Failed { reason: String },
    /// Iteration cap reached; run has been failed with `MaxIterations`.
    MaxIterationsReached,
    /// Wall-clock timeout; run has been failed with `Timeout`.
    TimedOut,
    /// Run is waiting for operator approval; loop suspended.
    /// Resume by re-entering the loop after `ApprovalResolved`.
    WaitingApproval { approval_id: ApprovalId },
    /// Run is waiting for a child subagent.  Loop suspended.
    /// Resume when the child task completes (dependency sweep).
    WaitingSubagent { child_task_id: TaskId },
    /// Plan-mode run completed with a plan artifact (RFC 018).
    /// The run is Completed with outcome `plan_proposed`.
    PlanProposed { plan_markdown: String },
    /// F65 PR-3: a circuit breaker tripped and terminated the loop.
    ///
    /// The `trip` payload (`BreakerKind`, measured, limit, at_iteration) is
    /// carried through to the HTTP response body, the SSE `orchestrate_finished`
    /// frame, and the `RuntimeEvent::CircuitBreakerTripped` event that the
    /// loop appends via the emitter before returning this termination.
    BreakerTripped { trip: CircuitBreakerTrip },
}

impl LoopTermination {
    /// Whether this termination drives the run to a terminal
    /// `RunState` (`Completed` / `Failed` / `Canceled`).
    ///
    /// `WaitingApproval` and `WaitingSubagent` are suspension points,
    /// not terminals — the run persists across cairn-app restarts and
    /// resumes via a second `run()` call. Every other variant ends the
    /// run for good.
    ///
    /// Used by the loop runner to fire run-terminal side effects (e.g.
    /// evicting harness-tools caches, see cairn-rs #606).
    pub fn drives_run_to_terminal(&self) -> bool {
        match self {
            LoopTermination::WaitingApproval { .. } | LoopTermination::WaitingSubagent { .. } => {
                false
            }
            LoopTermination::Completed { .. }
            | LoopTermination::Failed { .. }
            | LoopTermination::MaxIterationsReached
            | LoopTermination::TimedOut
            | LoopTermination::PlanProposed { .. }
            | LoopTermination::BreakerTripped { .. } => true,
        }
    }
}

// ── LoopConfig ───────────────────────────────────────────────────────────────

/// Tuning parameters for `OrchestratorLoop`.
#[derive(Clone, Debug)]
pub struct LoopConfig {
    /// Maximum iterations before the loop fails with `MaxIterations`.
    pub max_iterations: u32,
    /// Wall-clock timeout for the entire run in milliseconds.
    pub timeout_ms: u64,
    /// Save a checkpoint after every N tool calls (0 = every step).
    pub checkpoint_every_n_tool_calls: u32,
    /// Context compaction settings (RFC 018).
    pub compaction: CompactionConfig,
    /// F65 PR-3: circuit-breaker caps enforced by `OrchestratorLoop`.
    pub breakers: BreakerConfig,
    /// Issue #660: platform-level gate on `complete_run`.
    ///
    /// When `true` (default), the loop consults the incremental F47
    /// `VerificationAccumulator` before dispatching any
    /// `CompleteRun` proposal. If the accumulator reports at least one
    /// error line (cargo/rustc/clippy `error:` or `error[EXXXX]:`),
    /// the `CompleteRun` is refused — the loop synthesises a rejection
    /// `StepSummary` carrying the first few error excerpts, re-enters
    /// DECIDE, and gives the model another turn to fix the build.
    /// After three consecutive rejections the loop terminates with
    /// `LoopTermination::Failed { reason = "verification_rejected: …" }`
    /// so the run moves to the terminal [`FailureClass::VerificationRejected`]
    /// state (see `cairn_domain::lifecycle::FailureClass`).
    ///
    /// When `false`, the loop keeps the pre-#660 behaviour: the LLM's
    /// `CompleteRun` flows through to execute regardless of the
    /// verification sidecar. Intended for runs that want to surface a
    /// partial/best-effort summary without blocking on a red build.
    ///
    /// Operators override via the per-run defaults projection under
    /// `run:<id>:orchestrator_strict_completion_gate` (stored as the
    /// string `"true"` / `"false"`). The HTTP orchestrate handler
    /// resolves the key with the same 3-layer helper used for
    /// `max_iterations` + `goal`.
    pub orchestrator_strict_completion_gate: bool,
}

/// Default iteration cap for a run's orchestrator loop. Extracted
/// from the magic number embedded in `LoopConfig::default` so other
/// callers (child-run driver in RFC 027 §Child resource budgets) can
/// reference the cairn-wide default by name rather than duplicating
/// the literal. Operator override continues to flow through the
/// per-run / per-project / per-system `max_iterations` default.
pub const DEFAULT_MAX_ITERATIONS: u32 = 20;

impl Default for LoopConfig {
    fn default() -> Self {
        Self {
            max_iterations: DEFAULT_MAX_ITERATIONS,
            timeout_ms: 5 * 60 * 1_000, // 5 minutes
            checkpoint_every_n_tool_calls: 1,
            compaction: CompactionConfig::default(),
            breakers: BreakerConfig::default(),
            // #660: belt-and-suspenders with the role-prompt completion gate.
            // Default on; flip to false per run to accept partial completions.
            orchestrator_strict_completion_gate: true,
        }
    }
}

/// Issue #660: hard cap on consecutive `complete_run` rejections before
/// the loop gives up and terminates with [`FailureClass::VerificationRejected`].
///
/// Chosen as 3 so the model gets at least one "fix" turn after its first
/// reject + re-attempt (three rejects = three passes where the build still
/// failed). Lower values would terminate legit self-correcting runs; higher
/// values would let a broken model burn the full `max_iterations` budget
/// on rejection ping-pong.
pub const MAX_COMPLETION_GATE_REJECTIONS: u32 = 3;

/// Issue #660: how many error lines the rejection `StepSummary` should
/// carry back to DECIDE. The model only needs a few concrete lines to
/// re-plan; more than three is context pollution.
pub const COMPLETION_GATE_ERROR_PREVIEW: usize = 3;

// ── BreakerConfig ────────────────────────────────────────────────────────────

/// F65 PR-3: circuit-breaker caps enforced inside `OrchestratorLoop`.
///
/// Each field is a hard upper bound; when the measured value reaches or
/// exceeds the cap the loop emits `RuntimeEvent::CircuitBreakerTripped` and
/// terminates with `LoopTermination::BreakerTripped`.
///
/// Hardcoded defaults: `round_cap = 30`, `token_cap = 200_000`,
/// `no_tool_use_streak = 3`, `wall_clock_ms = 900_000` (15 minutes).
///
/// Defaults are resolved via the `RuntimeConfig` 3-layer fallback
/// (store → env → default) by the HTTP handler; request bodies may tighten
/// (but never loosen) these caps per-run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BreakerConfig {
    /// Maximum orchestrator iterations before the `Round` breaker trips.
    ///
    /// Enforced at the top of each loop iteration BEFORE gather. Trips
    /// when `iteration >= round_cap` so a `round_cap` of 5 allows
    /// iterations 0..=4 and trips entering iteration 5.
    pub round_cap: u32,
    /// Maximum cumulative LLM tokens (input + output) before the `Tokens`
    /// breaker trips. Counted after each DECIDE phase using
    /// `DecideOutput.input_tokens` + `DecideOutput.output_tokens`.
    pub token_cap: u64,
    /// Maximum consecutive DECIDE rounds with no forward progress
    /// before the `NoToolUseConsecutive` breaker trips.
    ///
    /// A "forward-progress" round is one whose proposals contain at
    /// least one of:
    ///   * a proposal with a concrete `tool_name: Some(_)` (invoke_tool),
    ///   * an `ActionType::CompleteRun` proposal (intentional terminal),
    ///   * an `ActionType::EscalateToOperator` proposal (intentional gate),
    ///   * an `ActionType::SpawnSubagent` proposal (intentional delegation).
    ///
    /// Rounds that contain only narration-shaped proposals (e.g.
    /// `create_memory`, `send_notification`) are treated as zero-
    /// progress and increment the streak; any forward-progress round
    /// resets the streak to zero. The carve-out for CompleteRun et al.
    /// prevents the streak breaker from tripping immediately before
    /// execute dispatches an intentional terminal action (regression
    /// caught on PR #348 review).
    ///
    /// A streak equal to this value trips the breaker (e.g. `3` trips
    /// after the third consecutive zero-progress round).
    pub no_tool_use_streak: u32,
    /// Wall-clock milliseconds from loop start before the `WallClock`
    /// breaker trips. Measured with a monotonic `std::time::Instant`
    /// captured at loop entry.
    pub wall_clock_ms: u64,
    /// Basis-point threshold that fires the once-per-run
    /// `BudgetThresholdCrossed` warning. `10_000` = 100 %; `8_000` = 80 %.
    /// Default `8_000` matches the historical const before #479 promoted
    /// it to a tunable. Set `>= 10_000` to suppress the warning (it will
    /// never fire before the trip path runs).
    ///
    /// Only Round / WallClock / Tokens participate in the warning —
    /// `NoToolUseConsecutive` skips it by design (see rustdoc on
    /// `no_tool_use_streak` + arch-doc §4.1).
    pub warn_ratio_bps: u32,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        Self {
            round_cap: 30,
            token_cap: 200_000,
            no_tool_use_streak: 3,
            wall_clock_ms: 15 * 60 * 1_000, // 15 minutes
            warn_ratio_bps: 8_000,          // 80 %
        }
    }
}

// ── CompactionConfig ────────────────────────────────────────────────────────

/// Configuration for inline context compaction (RFC 018 Enhancement 3).
///
/// When the step history exceeds `threshold_pct` of the estimated context
/// budget, the loop runner triggers a summarization turn to compress
/// older history into a compact summary, preserving the most recent
/// `keep_last` steps verbatim.
#[derive(Clone, Debug)]
pub struct CompactionConfig {
    /// Whether compaction is enabled.
    pub enabled: bool,
    /// Trigger compaction when step_history tokens exceed this percentage
    /// of the model's context window (0–100).
    pub threshold_pct: u32,
    /// Minimum number of steps before compaction can trigger.
    pub min_steps: usize,
    /// Always keep the most recent N steps verbatim (not compacted).
    pub keep_last: usize,
    /// Maximum tokens for the compacted summary.
    pub summary_token_budget: usize,
    /// Minimum number of iterations between compaction passes.
    ///
    /// This prevents runaway compaction on every turn when the threshold is
    /// set aggressively low.
    pub cooldown_iterations: u32,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            threshold_pct: 70,
            min_steps: 10,
            keep_last: 4,
            summary_token_budget: 2000,
            cooldown_iterations: 5,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_domain::{ActionProposal, ActionType};

    fn empty_decide(proposals: Vec<ActionProposal>) -> DecideOutput {
        DecideOutput {
            raw_response: String::new(),
            proposals,
            calibrated_confidence: 1.0,
            requires_approval: false,
            model_id: "test".into(),
            latency_ms: 0,
            input_tokens: None,
            output_tokens: None,
            system_prompt: String::new(),
            messages_json: "[]".to_owned(),
            tool_calls_json: "[]".to_owned(),
        }
    }

    fn proposal(action_type: ActionType, tool_name: Option<&str>) -> ActionProposal {
        ActionProposal {
            action_type,
            description: String::new(),
            confidence: 1.0,
            tool_name: tool_name.map(str::to_owned),
            tool_args: None,
            requires_approval: false,
        }
    }

    #[test]
    fn tool_or_terminal_count_empty() {
        let d = empty_decide(vec![]);
        assert_eq!(d.tool_or_terminal_count(), 0);
    }

    #[test]
    fn tool_or_terminal_count_counts_tool_name() {
        // `InvokeTool` with a concrete tool_name counts.
        let d = empty_decide(vec![
            proposal(ActionType::InvokeTool, Some("shell")),
            proposal(ActionType::InvokeTool, Some("read")),
        ]);
        assert_eq!(d.tool_or_terminal_count(), 2);
    }

    #[test]
    fn tool_or_terminal_count_counts_complete_run_without_tool_name() {
        // Terminal action with no tool_name still counts — the
        // carve-out Cursor Bugbot added to keep `complete_run` from
        // tripping the NoToolUseConsecutive breaker.
        let d = empty_decide(vec![proposal(ActionType::CompleteRun, None)]);
        assert_eq!(d.tool_or_terminal_count(), 1);
    }

    #[test]
    fn tool_or_terminal_count_counts_escalate_and_subagent() {
        let d = empty_decide(vec![
            proposal(ActionType::EscalateToOperator, None),
            proposal(ActionType::SpawnSubagent, None),
        ]);
        assert_eq!(d.tool_or_terminal_count(), 2);
    }

    #[test]
    fn tool_or_terminal_count_excludes_pure_narration_and_memory() {
        // create_memory / send_notification without a tool_name is
        // deliberately NOT counted — memorising a thought or pinging
        // Slack is not forward progress the user asked for.
        let d = empty_decide(vec![
            proposal(ActionType::CreateMemory, None),
            proposal(ActionType::SendNotification, None),
        ]);
        assert_eq!(d.tool_or_terminal_count(), 0);
    }

    #[test]
    fn tool_or_terminal_count_mixed_proposal_set() {
        let d = empty_decide(vec![
            proposal(ActionType::SendNotification, None),
            proposal(ActionType::InvokeTool, Some("grep")),
            proposal(ActionType::CreateMemory, None),
            proposal(ActionType::CompleteRun, None),
        ]);
        // grep + complete_run count; notification + create_memory do not.
        assert_eq!(d.tool_or_terminal_count(), 2);
    }
}
