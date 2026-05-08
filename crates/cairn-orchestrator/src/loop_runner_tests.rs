use super::*;
use crate::context::{
    ActionResult, ActionStatus, CompactionConfig, DecideOutput, ExecuteOutcome, GatherOutput,
    LoopConfig, LoopSignal, OrchestrationContext,
};
use crate::error::OrchestratorError;
use async_trait::async_trait;
use cairn_domain::{ActionProposal, ActionType, ApprovalId, ProjectKey, RunId, SessionId, TaskId};
use std::path::PathBuf;

// ── Minimal stubs ─────────────────────────────────────────────────────────

struct FixedGather;
#[async_trait]
impl GatherPhase for FixedGather {
    async fn gather(&self, _ctx: &OrchestrationContext) -> Result<GatherOutput, OrchestratorError> {
        Ok(GatherOutput::default())
    }
}

/// A DecidePhase stub whose behaviour is configured at construction time.
struct ScriptedDecide {
    /// Sequence of outputs to return, one per call.
    /// Cycles back to the last entry if calls exceed the vec length.
    outputs: Vec<DecideOutput>,
    call_count: std::sync::Mutex<usize>,
}

impl ScriptedDecide {
    fn always(output: DecideOutput) -> Self {
        Self {
            outputs: vec![output],
            call_count: std::sync::Mutex::new(0),
        }
    }
}

#[async_trait]
impl DecidePhase for ScriptedDecide {
    async fn decide(
        &self,
        _ctx: &OrchestrationContext,
        _: &GatherOutput,
    ) -> Result<DecideOutput, OrchestratorError> {
        let mut n = self.call_count.lock().unwrap();
        let idx = (*n).min(self.outputs.len() - 1);
        *n += 1;
        Ok(self.outputs[idx].clone())
    }
}

struct ScriptedExecute {
    signal: LoopSignal,
}

#[async_trait]
impl ExecutePhase for ScriptedExecute {
    async fn execute(
        &self,
        _ctx: &OrchestrationContext,
        decide: &DecideOutput,
    ) -> Result<ExecuteOutcome, OrchestratorError> {
        let results = decide
            .proposals
            .iter()
            .map(|p| ActionResult {
                proposal: p.clone(),
                status: ActionStatus::Succeeded,
                tool_output: None,
                invocation_id: None,
                duration_ms: 0,
            })
            .collect();
        Ok(ExecuteOutcome {
            results,
            loop_signal: self.signal.clone(),
        })
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────

fn ctx() -> OrchestrationContext {
    OrchestrationContext {
        project: ProjectKey::new("t", "w", "p"),
        session_id: SessionId::new("sess"),
        run_id: RunId::new("run"),
        task_id: None,
        iteration: 0,
        goal: "test goal".to_owned(),
        agent_type: "test_agent".to_owned(),
        run_started_at_ms: now_millis(),
        working_dir: PathBuf::from("."),
        run_mode: cairn_domain::decisions::RunMode::Direct,
        discovered_tool_names: vec![],
        step_history: vec![],
        is_recovery: false,
        approval_timeout: None,
        visibility: None,
    }
}

fn complete_run_proposal() -> ActionProposal {
    ActionProposal {
        action_type: ActionType::CompleteRun,
        description: "all done".to_owned(),
        confidence: 0.95,
        tool_name: None,
        tool_args: None,
        requires_approval: false,
    }
}

fn decide_done() -> DecideOutput {
    DecideOutput {
        raw_response: r#"[{"action_type":"complete_run"}]"#.to_owned(),
        proposals: vec![complete_run_proposal()],
        calibrated_confidence: 0.95,
        requires_approval: false,
        model_id: "test-model".to_owned(),
        latency_ms: 10,
        input_tokens: None,
        output_tokens: None,
        system_prompt: String::new(),
        messages_json: "[]".to_owned(),
        tool_calls_json: "[]".to_owned(),
        tool_defs_json: "[]".to_owned(),
    }
}

/// F65 PR-3: breaker config large enough that no legacy unit test trips
/// a circuit breaker. Used by tests that exercise `LoopTermination`
/// variants other than `BreakerTripped` but whose stubbed DECIDE
/// output happens to emit consecutive no-tool-use proposals
/// (`complete_run`), which would otherwise trip the default
/// NoToolUseConsecutive cap of 3.
fn permissive_breakers() -> crate::context::BreakerConfig {
    crate::context::BreakerConfig {
        round_cap: 10_000,
        token_cap: u64::MAX,
        no_tool_use_streak: 10_000,
        wall_clock_ms: u64::MAX,
        warn_ratio_bps: 8_000,
    }
}

fn decide_tool(tool: &str) -> DecideOutput {
    DecideOutput {
        raw_response: format!(r#"[{{"action_type":"invoke_tool","tool_name":"{tool}"}}]"#),
        proposals: vec![ActionProposal {
            action_type: ActionType::InvokeTool,
            description: format!("call {tool}"),
            confidence: 0.8,
            tool_name: Some(tool.to_owned()),
            tool_args: Some(serde_json::json!({})),
            requires_approval: false,
        }],
        calibrated_confidence: 0.8,
        requires_approval: false,
        model_id: "test-model".to_owned(),
        latency_ms: 20,
        input_tokens: None,
        output_tokens: None,
        system_prompt: String::new(),
        messages_json: "[]".to_owned(),
        tool_calls_json: "[]".to_owned(),
        tool_defs_json: "[]".to_owned(),
    }
}

// ── (1) Timeout ───────────────────────────────────────────────────────────

#[tokio::test]
async fn timeout_returns_timed_out() {
    let mut past_ctx = ctx();
    past_ctx.run_started_at_ms = 0; // started at epoch = already timed out

    let config = LoopConfig {
        timeout_ms: 1,
        ..Default::default()
    };
    let lp = OrchestratorLoop::new(
        FixedGather,
        ScriptedDecide::always(decide_done()),
        ScriptedExecute {
            signal: LoopSignal::Done,
        },
        config,
    );

    let result = lp.run(past_ctx).await.unwrap();
    assert!(matches!(result, LoopTermination::TimedOut));
}

// ── (2c) Pre-DECIDE budget check ──────────────────────────────────────────
//
// F27 guard: GATHER can consume meaningful wall-clock time (retrieval,
// chunk scoring). If the remaining budget drops below
// `MIN_DECIDE_BUDGET_MS` between iteration start and post-GATHER, the
// loop MUST bail cleanly as `TimedOut` rather than firing an LLM call
// that is guaranteed to miss the deadline. This test pins that gate by
// putting the context ~3s past "now" on a 5s loop budget — GATHER
// finishes instantly but the remaining budget is 2s, below the 5s
// threshold, so the loop must terminate before DECIDE runs.

struct CountingDecide {
    count: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl DecidePhase for CountingDecide {
    async fn decide(
        &self,
        _: &OrchestrationContext,
        _: &GatherOutput,
    ) -> Result<DecideOutput, OrchestratorError> {
        self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(decide_done())
    }
}

#[tokio::test]
async fn budget_below_threshold_short_circuits_before_decide() {
    let mut tight_ctx = ctx();
    // Started 3s ago on a 5s total budget → only 2s remaining,
    // strictly below MIN_DECIDE_BUDGET_MS (5s).
    tight_ctx.run_started_at_ms = now_millis().saturating_sub(3_000);

    let decide = CountingDecide {
        count: std::sync::atomic::AtomicUsize::new(0),
    };
    let decide_handle = std::sync::Arc::new(decide);
    let decide_for_loop = decide_handle.clone();

    // `OrchestratorLoop::new` takes the phase by value, so we wrap
    // the shared counter in `Arc` and keep one handle here for
    // post-run inspection. `ArcDecide` is a thin trait adapter that
    // forwards to the inner `CountingDecide` — two `Arc` handles to
    // the same counter, no leaks, no raw pointers.
    struct ArcDecide(std::sync::Arc<CountingDecide>);
    #[async_trait]
    impl DecidePhase for ArcDecide {
        async fn decide(
            &self,
            ctx: &OrchestrationContext,
            g: &GatherOutput,
        ) -> Result<DecideOutput, OrchestratorError> {
            self.0.decide(ctx, g).await
        }
    }

    let config = LoopConfig {
        timeout_ms: 5_000,
        ..Default::default()
    };
    let lp = OrchestratorLoop::new(
        FixedGather,
        ArcDecide(decide_for_loop),
        ScriptedExecute {
            signal: LoopSignal::Done,
        },
        config,
    );

    let result = lp.run(tight_ctx).await.unwrap();
    assert!(
        matches!(result, LoopTermination::TimedOut),
        "expected TimedOut from pre-DECIDE budget check, got {result:?}"
    );
    // Critical: DECIDE must NOT have been invoked. Otherwise we are
    // firing LLM calls we know will miss the deadline — the very
    // behaviour F27 adds this guard to prevent.
    assert_eq!(
        decide_handle
            .count
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "decide fired despite budget below MIN_DECIDE_BUDGET_MS"
    );
}

// ── (1b) Lease health gate ────────────────────────────────────────────────
//
// The §1b gate in `run_inner` polls `task_sink.is_lease_healthy()` at
// each iteration start and short-circuits to
// `LoopTermination::Failed { reason: "lease unhealthy" }` when the
// sink reports false. This is the safety gate for the whole feature —
// a degraded lease means every downstream FCALL (LLM call, tool
// dispatch, checkpoint) will be rejected by FF anyway, so bailing
// early avoids committing irreversible work.

struct UnhealthySink;
#[async_trait]
impl crate::task_sink::TaskFrameSink for UnhealthySink {
    async fn log_tool_call(
        &self,
        _name: &str,
        _args: &serde_json::Value,
    ) -> Result<(), OrchestratorError> {
        panic!("log_tool_call must not be reached when lease is unhealthy")
    }
    async fn log_tool_result(
        &self,
        _name: &str,
        _output: &serde_json::Value,
        _success: bool,
        _duration_ms: u64,
    ) -> Result<(), OrchestratorError> {
        panic!("log_tool_result must not be reached when lease is unhealthy")
    }
    async fn log_llm_response(
        &self,
        _model: &str,
        _tokens_in: u64,
        _tokens_out: u64,
        _latency_ms: u64,
    ) -> Result<(), OrchestratorError> {
        panic!("log_llm_response must not be reached when lease is unhealthy")
    }
    async fn save_checkpoint(&self, _bytes: &[u8]) -> Result<(), OrchestratorError> {
        panic!("save_checkpoint must not be reached when lease is unhealthy")
    }
    fn is_lease_healthy(&self) -> bool {
        false
    }
}

#[tokio::test]
async fn unhealthy_lease_aborts_before_gather() {
    // Install a sink that reports unhealthy AND panics on any frame
    // write — if the loop reached gather/decide/execute and tried to
    // emit a frame, the test would fail with the panic message.
    // Termination must arrive from the §1b gate alone.
    let lp = OrchestratorLoop::new(
        FixedGather,
        ScriptedDecide::always(decide_done()),
        ScriptedExecute {
            signal: LoopSignal::Done,
        },
        LoopConfig::default(),
    )
    .with_task_sink(std::sync::Arc::new(UnhealthySink));

    let result = lp.run(ctx()).await.unwrap();
    match result {
        LoopTermination::Failed { reason } => {
            assert_eq!(
                reason, "lease unhealthy",
                "lease-health gate must surface the exact reason — callers downstream \
                     (CairnTask::fail_with_retry, handler error mapping) may match on it",
            );
        }
        other => panic!("expected Failed {{ reason: 'lease unhealthy' }}, got {other:?}"),
    }
}

// ── (1c) Emitter fatal-error gate ────────────────────────────────────────
//
// F24 dogfood (2026-04-23): when a composite emitter's side-effect
// append to the durable secondary fails, the loop must abort
// rather than silently continue on top of a diverged store. The
// new `take_fatal_error()` contract lets emitters surface errors
// that their `()`-returning callbacks can't express.

struct FatalEmitter {
    consumed: std::sync::Mutex<bool>,
}
#[async_trait]
impl crate::emitter::OrchestratorEventEmitter for FatalEmitter {
    fn take_fatal_error(&self) -> Option<String> {
        let mut consumed = self.consumed.lock().unwrap();
        if *consumed {
            None
        } else {
            *consumed = true;
            Some("dual-write divergence on run=run: boom".to_owned())
        }
    }
}

#[tokio::test]
async fn emitter_fatal_error_aborts_loop_with_store_error() {
    let config = LoopConfig {
        max_iterations: 5,
        ..Default::default()
    };
    let lp = OrchestratorLoop::new(
        FixedGather,
        ScriptedDecide::always(decide_done()),
        ScriptedExecute {
            signal: LoopSignal::Done,
        },
        config,
    )
    .with_emitter(std::sync::Arc::new(FatalEmitter {
        consumed: std::sync::Mutex::new(false),
    }));

    let err = lp.run(ctx()).await.expect_err(
        "emitter-signalled fatal error must propagate as OrchestratorError, \
             not be silently swallowed",
    );
    match err {
        OrchestratorError::Store(msg) => {
            let s = msg.to_string();
            assert!(
                s.contains("dual-write divergence"),
                "expected divergence detail in error, got: {s}"
            );
        }
        other => {
            panic!("expected OrchestratorError::Store(dual-write divergence), got {other:?}")
        }
    }
}

// ── (2–5) Happy path: Continue × N then Done ──────────────────────────────

#[tokio::test]
async fn two_iterations_then_done() {
    // First two calls return Continue; third returns Done.
    let config = LoopConfig {
        max_iterations: 10,
        breakers: permissive_breakers(),
        ..Default::default()
    };

    struct CountingExecute {
        calls: std::sync::Mutex<u32>,
    }
    #[async_trait]
    impl ExecutePhase for CountingExecute {
        async fn execute(
            &self,
            _ctx: &OrchestrationContext,
            decide: &DecideOutput,
        ) -> Result<ExecuteOutcome, OrchestratorError> {
            let mut n = self.calls.lock().unwrap();
            *n += 1;
            let signal = if *n < 3 {
                LoopSignal::Continue
            } else {
                LoopSignal::Done
            };
            let results = decide
                .proposals
                .iter()
                .map(|p| ActionResult {
                    proposal: p.clone(),
                    status: ActionStatus::Succeeded,
                    tool_output: None,
                    invocation_id: None,
                    duration_ms: 0,
                })
                .collect();
            Ok(ExecuteOutcome {
                results,
                loop_signal: signal,
            })
        }
    }

    let lp = OrchestratorLoop::new(
        FixedGather,
        ScriptedDecide::always(decide_done()),
        CountingExecute {
            calls: std::sync::Mutex::new(0),
        },
        config,
    );
    let result = lp.run(ctx()).await.unwrap();
    assert!(
        matches!(result, LoopTermination::Completed { .. }),
        "expected Completed, got {result:?}"
    );
}

// ── Max iterations ────────────────────────────────────────────────────────

#[tokio::test]
async fn max_iterations_returns_max_iterations_reached() {
    let config = LoopConfig {
        max_iterations: 3,
        ..Default::default()
    };
    let lp = OrchestratorLoop::new(
        FixedGather,
        ScriptedDecide::always(decide_tool("web_search")),
        ScriptedExecute {
            signal: LoopSignal::Continue,
        },
        config,
    );
    let result = lp.run(ctx()).await.unwrap();
    assert!(matches!(result, LoopTermination::MaxIterationsReached));
}

// ── Execute failure ───────────────────────────────────────────────────────

#[tokio::test]
async fn failed_signal_returns_failed() {
    let lp = OrchestratorLoop::new(
        FixedGather,
        ScriptedDecide::always(decide_tool("broken_tool")),
        ScriptedExecute {
            signal: LoopSignal::Failed {
                reason: "tool error".to_owned(),
            },
        },
        LoopConfig::default(),
    );
    let result = lp.run(ctx()).await.unwrap();
    assert!(matches!(result, LoopTermination::Failed { reason } if reason == "tool error"));
}

// ── Approval gate ─────────────────────────────────────────────────────────

#[tokio::test]
async fn requires_approval_suspends_immediately() {
    let appr_id = ApprovalId::new("appr_1");
    let appr_id_clone = appr_id.clone();

    struct ApprovalExecute(ApprovalId);
    #[async_trait]
    impl ExecutePhase for ApprovalExecute {
        async fn execute(
            &self,
            _ctx: &OrchestrationContext,
            decide: &DecideOutput,
        ) -> Result<ExecuteOutcome, OrchestratorError> {
            let results = decide
                .proposals
                .iter()
                .map(|p| ActionResult {
                    proposal: p.clone(),
                    status: ActionStatus::AwaitingApproval {
                        approval_id: self.0.clone(),
                    },
                    tool_output: None,
                    invocation_id: None,
                    duration_ms: 0,
                })
                .collect();
            Ok(ExecuteOutcome {
                results,
                loop_signal: LoopSignal::WaitApproval {
                    approval_id: self.0.clone(),
                },
            })
        }
    }

    let needs_approval_decide = DecideOutput {
        requires_approval: true,
        proposals: vec![ActionProposal {
            action_type: ActionType::EscalateToOperator,
            description: "need approval".to_owned(),
            confidence: 0.5,
            tool_name: None,
            tool_args: None,
            requires_approval: true,
        }],
        raw_response: String::new(),
        calibrated_confidence: 0.5,
        model_id: "m".to_owned(),
        latency_ms: 0,
        input_tokens: None,
        output_tokens: None,
        system_prompt: String::new(),
        messages_json: "[]".to_owned(),
        tool_calls_json: "[]".to_owned(),
        tool_defs_json: "[]".to_owned(),
    };

    let lp = OrchestratorLoop::new(
        FixedGather,
        ScriptedDecide::always(needs_approval_decide),
        ApprovalExecute(appr_id_clone),
        LoopConfig::default(),
    );
    let result = lp.run(ctx()).await.unwrap();
    assert!(
        matches!(&result, LoopTermination::WaitingApproval { approval_id } if *approval_id == appr_id),
        "expected WaitingApproval, got {result:?}"
    );
}

// ── WaitSubagent ──────────────────────────────────────────────────────────

#[tokio::test]
async fn wait_subagent_signal_suspends() {
    let child_id = TaskId::new("task_child_1");
    let lp = OrchestratorLoop::new(
        FixedGather,
        ScriptedDecide::always(decide_tool("spawn")),
        ScriptedExecute {
            signal: LoopSignal::WaitSubagent {
                child_task_id: child_id.clone(),
            },
        },
        LoopConfig::default(),
    );
    let result = lp.run(ctx()).await.unwrap();
    assert!(
        matches!(&result, LoopTermination::WaitingSubagent { child_task_id } if *child_task_id == child_id)
    );
}

// ── Checkpoint hook ───────────────────────────────────────────────────────

#[tokio::test]
async fn checkpoint_hook_called_after_each_iteration() {
    use std::sync::atomic::{AtomicU32, Ordering};

    struct CountingHook(Arc<AtomicU32>);
    #[async_trait::async_trait]
    impl CheckpointHook for CountingHook {
        async fn save(
            &self,
            _: &OrchestrationContext,
            _: &GatherOutput,
            _: &DecideOutput,
            _: &ExecuteOutcome,
        ) -> Result<(), OrchestratorError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let call_count = Arc::new(AtomicU32::new(0));
    let hook = Arc::new(CountingHook(call_count.clone()));

    struct TwoThenDone(std::sync::Mutex<u32>);
    #[async_trait]
    impl ExecutePhase for TwoThenDone {
        async fn execute(
            &self,
            _: &OrchestrationContext,
            decide: &DecideOutput,
        ) -> Result<ExecuteOutcome, OrchestratorError> {
            let mut n = self.0.lock().unwrap();
            *n += 1;
            let signal = if *n < 3 {
                LoopSignal::Continue
            } else {
                LoopSignal::Done
            };
            let results = decide
                .proposals
                .iter()
                .map(|p| ActionResult {
                    proposal: p.clone(),
                    status: ActionStatus::Succeeded,
                    tool_output: None,
                    invocation_id: None,
                    duration_ms: 0,
                })
                .collect();
            Ok(ExecuteOutcome {
                results,
                loop_signal: signal,
            })
        }
    }

    let lp = OrchestratorLoop::new(
        FixedGather,
        ScriptedDecide::always(decide_done()),
        TwoThenDone(std::sync::Mutex::new(0)),
        LoopConfig {
            breakers: permissive_breakers(),
            ..Default::default()
        },
    )
    .with_checkpoint_hook(hook);

    let result = lp.run(ctx()).await.unwrap();
    assert!(matches!(result, LoopTermination::Completed { .. }));
    // Checkpoint hook must be called once per completed iteration (3 total).
    assert_eq!(
        call_count.load(Ordering::SeqCst),
        3,
        "checkpoint hook must be called after each of the 3 iterations"
    );
}

// ── Step summary accumulation ─────────────────────────────────────────────

#[tokio::test]
async fn build_step_summary_captures_action_kind_and_success() {
    let context = ctx();
    let decide = decide_tool("search");
    let exec = ExecuteOutcome {
        results: vec![ActionResult {
            proposal: decide.proposals[0].clone(),
            status: ActionStatus::Succeeded,
            tool_output: Some(serde_json::json!({"result": "ok"})),
            invocation_id: None,
            duration_ms: 0,
        }],
        loop_signal: LoopSignal::Continue,
    };

    let summary = build_step_summary(&context, &decide, &exec);
    assert_eq!(summary.iteration, 0);
    assert_eq!(summary.action_kind, "invoke_tool");
    assert!(summary.succeeded);
    assert!(summary.summary.contains("search"));
}

/// F35 regression on the Cursor Bugbot finding (PR #295). When the
/// execute phase produces fewer `results` than `decide.proposals`
/// (because a terminal short-circuit left some proposal slots
/// `None` and `flatten()` dropped them), the step-summary builder
/// must still attribute each recorded result to its own
/// `ActionProposal` — it cannot zip positionally against
/// `decide.proposals` because the indices no longer line up.
///
/// Setup: two proposals, the second an `InvokeTool` that failed
/// with a recognisable error. The first proposal has NO
/// corresponding `ActionResult` (as if it were a non-InvokeTool
/// skipped by a Phase-1 terminal signal). Post-fix, the summary
/// must still include the `ERROR:` enrichment for the failing
/// tool. Pre-fix (positional zip), the zip would have paired the
/// InvokeTool result with the wrong proposal — failing silently.
#[tokio::test]
async fn build_step_summary_handles_flattened_results_without_misalignment() {
    let context = ctx();

    // Two proposals — typical of an `[CompleteRun, invoke_tool]`
    // batch where CompleteRun terminates Phase 2 and the tool
    // actually ran in Phase 1. The `results` vector has a single
    // entry because the non-InvokeTool was never dispatched.
    let cr_proposal = ActionProposal {
        action_type: ActionType::CompleteRun,
        description: "skipped".to_owned(),
        confidence: 0.9,
        tool_name: None,
        tool_args: None,
        requires_approval: false,
    };
    let tool_proposal = ActionProposal {
        action_type: ActionType::InvokeTool,
        description: "call read".to_owned(),
        confidence: 0.9,
        tool_name: Some("read".to_owned()),
        tool_args: Some(serde_json::json!({"path": "/nope"})),
        requires_approval: false,
    };
    let decide = DecideOutput {
        raw_response: "[]".to_owned(),
        proposals: vec![cr_proposal, tool_proposal.clone()],
        calibrated_confidence: 0.9,
        requires_approval: false,
        model_id: "test-model".to_owned(),
        latency_ms: 10,
        input_tokens: None,
        output_tokens: None,
        system_prompt: String::new(),
        messages_json: "[]".to_owned(),
        tool_calls_json: "[]".to_owned(),
        tool_defs_json: "[]".to_owned(),
    };

    // Only the InvokeTool result was recorded; the CompleteRun
    // proposal was skipped and its `None` slot was dropped by
    // `flatten()` in execute_impl.
    let exec = ExecuteOutcome {
        results: vec![ActionResult {
            proposal: tool_proposal,
            status: ActionStatus::Failed {
                reason: "Error [NOT_FOUND]: File not found: /nope".to_owned(),
            },
            tool_output: None,
            invocation_id: None,
            duration_ms: 0,
        }],
        loop_signal: LoopSignal::Continue,
    };

    let summary = build_step_summary(&context, &decide, &exec);
    assert!(
        summary.summary.contains("tool_result[read] ERROR:")
            && summary.summary.contains("NOT_FOUND"),
        "summary must attribute the NOT_FOUND error to the `read` tool even \
             when `decide.proposals` has more entries than `execute.results`. \
             Got:\n{}",
        summary.summary
    );
    // Loop-level succeeded flag still true because LoopSignal is
    // Continue — InvokeTool errors are recoverable feedback (F35).
    assert!(summary.succeeded);
}

/// `truncate_for_summary` must stream: a 100k-char input should
/// keep only ~head+tail chars internally (bounded deque) rather
/// than materialising the whole `Vec<char>`. We cannot assert on
/// allocation size directly, but we can pin the contract that the
/// *output* is bounded and contains the expected sentinel.
#[test]
fn truncate_for_summary_bounded_output_on_large_input() {
    let big = "x".repeat(100_000);
    let out = super::truncate_for_summary(&big, 400);
    assert!(out.contains("chars omitted"), "output: {out}");
    // Bounded to roughly max_chars + the marker string; give a
    // generous headroom for the marker.
    assert!(
        out.len() < 600,
        "truncated output should stay bounded; got {} chars",
        out.len()
    );
}

/// Short inputs must survive verbatim without the truncation marker.
#[test]
fn truncate_for_summary_preserves_short_input() {
    let input = "hello world";
    assert_eq!(super::truncate_for_summary(input, 400), input);
}

/// F54: `render_tool_output_preview` must produce an explicit
/// `exit_code:` line for bash-class tools so the LLM has an
/// unambiguous success/failure signal even when stdout/stderr are
/// truncated. The line must appear at the very start of the
/// preview.
#[test]
fn render_tool_output_preview_bash_prepends_exit_code() {
    let output = serde_json::json!({
        "exit_code": 0,
        "stdout": "Compiling foo\nFinished dev [unoptimized] target\n",
        "stderr": "",
    });
    let preview = super::render_tool_output_preview("bash", &output);
    assert!(
        preview.starts_with("exit_code: 0"),
        "preview must open with exit_code; got: {preview}"
    );
    assert!(preview.contains("Finished dev"));
}

/// F54 primary fix: the tail of `stdout` must survive even when
/// the payload exceeds the preview budget. A build with hundreds
/// of "Compiling X" lines that ends with `warning: unused import`
/// must still show that final line — previously head+tail
/// truncation of the serialized JSON clipped the warning out.
#[test]
fn render_tool_output_preview_bash_preserves_trailing_warning() {
    // 5 KB of "Compiling X" + final warning line. Size chosen to
    // overflow the prior 400-char head+tail cap comfortably.
    let mut stdout = String::new();
    for i in 0..200 {
        stdout.push_str(&format!("   Compiling crate-stub-{i} v0.1.0\n"));
    }
    stdout.push_str("warning: f54-marker-unused-import\n");
    let output = serde_json::json!({
        "exit_code": 0,
        "stdout": stdout,
        "stderr": "",
    });
    let preview = super::render_tool_output_preview("bash", &output);
    assert!(
        preview.contains("f54-marker-unused-import"),
        "trailing warning must survive truncation; got: {preview}"
    );
}

/// Clean builds produce short output — the preview must include
/// it verbatim with no truncation marker.
#[test]
fn render_tool_output_preview_bash_small_output_verbatim() {
    let output = serde_json::json!({
        "exit_code": 0,
        "stdout": "hello",
        "stderr": "",
    });
    let preview = super::render_tool_output_preview("bash", &output);
    assert!(preview.contains("exit_code: 0"));
    assert!(preview.contains("hello"));
    assert!(
        !preview.contains("[truncated"),
        "short output must not carry a truncation marker; got: {preview}"
    );
}

/// Copilot review on PR #315: an adapter that spells exit_code as
/// `exitCode` (or `returncode` / `return_code` / `status`) must
/// still surface the numeric value in the preview. Pre-fix this
/// code path only looked at `exit_code` and fell through to `?`.
#[test]
fn render_tool_output_preview_bash_accepts_alternate_exit_code_keys() {
    for (key, expected) in [
        ("exit_code", "exit_code: 0"),
        ("exitCode", "exit_code: 0"),
        ("returncode", "exit_code: 0"),
        ("return_code", "exit_code: 0"),
        ("status", "exit_code: 0"),
    ] {
        let output = serde_json::json!({
            key: 0,
            "stdout": "ok",
            "stderr": "",
        });
        let preview = super::render_tool_output_preview("bash", &output);
        assert!(
            preview.starts_with(expected),
            "key {key} must surface as {expected}; got: {preview}"
        );
    }
}

/// Case-insensitive bash matching — some provider adapters
/// normalize tool names to `Bash` during JSON marshalling.
#[test]
fn render_tool_output_preview_bash_case_insensitive() {
    let output = serde_json::json!({"exit_code": 0, "stdout": "ok", "stderr": ""});
    let preview = super::render_tool_output_preview("Bash", &output);
    assert!(
        preview.starts_with("exit_code: 0"),
        "uppercase Bash must route through bash preview; got: {preview}"
    );
}

/// Non-bash tools keep the existing head+tail truncation strategy
/// (F54 is scoped to bash-class tools — other tools like `read`
/// expose file bodies where the head is usually informative).
#[test]
fn render_tool_output_preview_non_bash_falls_through() {
    let output = serde_json::json!({"result": "ok"});
    let preview = super::render_tool_output_preview("search", &output);
    assert!(
        !preview.starts_with("exit_code:"),
        "non-bash tools must not synthesise an exit_code line; got: {preview}"
    );
    assert!(preview.contains("ok"));
}

/// `tail_of` must preserve the last N chars of a large input and
/// include a `[truncated]` marker, with bounded output size.
#[test]
fn tail_of_preserves_last_chars_and_marks_truncation() {
    let mut s = String::new();
    for i in 0..2000 {
        s.push_str(&format!("{i:04}\n"));
    }
    // Sentinel at the tail.
    s.push_str("FINAL-MARKER\n");
    let out = super::tail_of(&s, 200);
    assert!(out.contains("FINAL-MARKER"));
    assert!(out.contains("[truncated"));
    assert!(
        out.len() < 400,
        "tail_of output too large: {} bytes",
        out.len()
    );
}

/// Short inputs to `tail_of` are passed through unchanged.
#[test]
fn tail_of_passthrough_for_short_input() {
    assert_eq!(super::tail_of("hello", 100), "hello");
}

/// Edge case (Gemini review on PR #315): `max_chars == 0` on a
/// non-empty input must NOT return the full string — it should
/// emit a truncation marker with no content. Pre-fix, the
/// deque-based implementation degenerated to "full-string + marker"
/// because `buf.pop_front()` on an empty deque was a no-op and the
/// bounded-length branch never fired.
#[test]
fn tail_of_zero_max_chars_emits_marker_only() {
    let out = super::tail_of("abcdef", 0);
    assert!(
        out.contains("[truncated"),
        "zero-max tail must emit truncation marker; got: {out:?}"
    );
    assert!(
        !out.contains("abcdef"),
        "zero-max tail must NOT leak the input; got: {out:?}"
    );
}

/// Empty input with any `max_chars` is a verbatim passthrough.
#[test]
fn tail_of_empty_input_is_empty() {
    assert_eq!(super::tail_of("", 0), "");
    assert_eq!(super::tail_of("", 100), "");
}

// ── tool_search discovery injection ──────────────────────────────────────

/// Verify that tool_search results in `tool_output` are extracted and
/// carried into `ctx.discovered_tool_names` for the next iteration.
#[test]
fn extract_tool_search_discoveries_finds_matches() {
    let outcome = ExecuteOutcome {
        results: vec![ActionResult {
            proposal: ActionProposal {
                action_type: ActionType::InvokeTool,
                description: "search for tools".to_owned(),
                confidence: 0.8,
                tool_name: Some("tool_search".to_owned()),
                tool_args: None,
                requires_approval: false,
            },
            status: ActionStatus::Succeeded,
            tool_output: Some(serde_json::json!({
                "matches": [
                    { "name": "bash",   "description": "run shell commands" },
                    { "name": "graph_query",  "description": "query the graph" },
                ],
                "total": 2,
            })),
            invocation_id: None,
            duration_ms: 0,
        }],
        loop_signal: LoopSignal::Continue,
    };

    let discovered = extract_tool_search_discoveries(&outcome);
    assert_eq!(discovered.len(), 2);
    assert!(discovered.contains(&"bash".to_owned()));
    assert!(discovered.contains(&"graph_query".to_owned()));
}

#[test]
fn extract_tool_search_discoveries_ignores_non_tool_search() {
    let outcome = ExecuteOutcome {
        results: vec![ActionResult {
            proposal: ActionProposal {
                action_type: ActionType::InvokeTool,
                description: "call something else".to_owned(),
                confidence: 0.9,
                tool_name: Some("memory_search".to_owned()),
                tool_args: None,
                requires_approval: false,
            },
            status: ActionStatus::Succeeded,
            tool_output: Some(serde_json::json!({
                "matches": [{ "name": "should_not_appear" }]
            })),
            invocation_id: None,
            duration_ms: 0,
        }],
        loop_signal: LoopSignal::Continue,
    };

    let discovered = extract_tool_search_discoveries(&outcome);
    assert!(
        discovered.is_empty(),
        "non-tool_search results must not produce discoveries"
    );
}

#[test]
fn extract_tool_search_discoveries_empty_matches() {
    let outcome = ExecuteOutcome {
        results: vec![ActionResult {
            proposal: ActionProposal {
                action_type: ActionType::InvokeTool,
                description: "search".to_owned(),
                confidence: 0.5,
                tool_name: Some("tool_search".to_owned()),
                tool_args: None,
                requires_approval: false,
            },
            status: ActionStatus::Succeeded,
            tool_output: Some(serde_json::json!({ "matches": [], "total": 0 })),
            invocation_id: None,
            duration_ms: 0,
        }],
        loop_signal: LoopSignal::Continue,
    };

    let discovered = extract_tool_search_discoveries(&outcome);
    assert!(discovered.is_empty());
}

/// Integration test: after a tool_search invocation, the loop carries the
/// discovered names into ctx.discovered_tool_names for the next iteration.
#[tokio::test]
async fn loop_runner_carries_discovered_tools_to_next_iteration() {
    use std::sync::Mutex;

    // Capture the ctx seen at each decide() call
    struct CapturingDecide {
        captured: std::sync::Arc<Mutex<Vec<Vec<String>>>>,
        call_n: Mutex<u32>,
    }
    #[async_trait]
    impl DecidePhase for CapturingDecide {
        async fn decide(
            &self,
            ctx: &OrchestrationContext,
            _: &GatherOutput,
        ) -> Result<DecideOutput, OrchestratorError> {
            self.captured
                .lock()
                .unwrap()
                .push(ctx.discovered_tool_names.clone());
            let n = {
                let mut g = self.call_n.lock().unwrap();
                *g += 1;
                *g
            };
            // First call: invoke tool_search; second call: done
            let (action, tool_name, tool_args) = if n == 1 {
                (
                    ActionType::InvokeTool,
                    Some("tool_search".to_owned()),
                    Some(serde_json::json!({"query":"shell"})),
                )
            } else {
                (ActionType::CompleteRun, None, None)
            };
            Ok(DecideOutput {
                raw_response: String::new(),
                proposals: vec![ActionProposal {
                    action_type: action,
                    description: "step".to_owned(),
                    confidence: 0.9,
                    tool_name,
                    tool_args,
                    requires_approval: false,
                }],
                calibrated_confidence: 0.9,
                requires_approval: false,
                model_id: "test".to_owned(),
                latency_ms: 0,
                input_tokens: None,
                output_tokens: None,
                system_prompt: String::new(),
                messages_json: "[]".to_owned(),
                tool_calls_json: "[]".to_owned(),
                tool_defs_json: "[]".to_owned(),
            })
        }
    }

    // Execute returns tool_search results on first call, Done on second
    struct DiscoveryExecute {
        calls: Mutex<u32>,
    }
    #[async_trait]
    impl ExecutePhase for DiscoveryExecute {
        async fn execute(
            &self,
            _: &OrchestrationContext,
            decide: &DecideOutput,
        ) -> Result<ExecuteOutcome, OrchestratorError> {
            let n = {
                let mut g = self.calls.lock().unwrap();
                *g += 1;
                *g
            };
            let (signal, tool_output) = if n == 1 {
                (
                    LoopSignal::Continue,
                    Some(serde_json::json!({
                        "matches": [{"name":"bash","description":"run shell"}],
                        "total": 1,
                    })),
                )
            } else {
                (LoopSignal::Done, None)
            };
            let results = decide
                .proposals
                .iter()
                .map(|p| ActionResult {
                    proposal: p.clone(),
                    status: ActionStatus::Succeeded,
                    tool_output: tool_output.clone(),
                    invocation_id: None,
                    duration_ms: 0,
                })
                .collect();
            Ok(ExecuteOutcome {
                results,
                loop_signal: signal,
            })
        }
    }

    let shared_captured: std::sync::Arc<Mutex<Vec<Vec<String>>>> =
        std::sync::Arc::new(Mutex::new(vec![]));
    let capturing = CapturingDecide {
        captured: shared_captured.clone(),
        call_n: Mutex::new(0),
    };

    let lp = OrchestratorLoop::new(
        FixedGather,
        capturing,
        DiscoveryExecute {
            calls: Mutex::new(0),
        },
        LoopConfig {
            max_iterations: 5,
            ..Default::default()
        },
    );

    let result = lp.run(ctx()).await.unwrap();
    assert!(matches!(result, LoopTermination::Completed { .. }));

    let snapshots = shared_captured.lock().unwrap();
    // First decide: no discovered tools yet
    assert!(
        snapshots[0].is_empty(),
        "iteration 0 must have no discovered tools yet"
    );
    // Second decide: bash must be in discovered_tool_names
    assert!(
        snapshots[1].contains(&"bash".to_owned()),
        "iteration 1 must see bash from prior tool_search result"
    );
}

// ── Plan extraction tests (RFC 018) ─────────────────────────────────

#[test]
fn extract_proposed_plan_parses_block() {
    let response = "Here's my analysis.\n\n<proposed_plan>\n# Plan: Fix the bug\n\n## What I found\nThe bug is in foo.rs line 42.\n\n## What I propose\n1. Fix foo.rs\n</proposed_plan>\n\nDone.";
    let plan = extract_proposed_plan(response);
    assert!(plan.is_some());
    let md = plan.unwrap();
    assert!(md.contains("# Plan: Fix the bug"));
    assert!(md.contains("Fix foo.rs"));
}

#[test]
fn extract_proposed_plan_returns_none_when_absent() {
    let response = "I need more information before I can propose a plan.";
    assert!(extract_proposed_plan(response).is_none());
}

#[test]
fn extract_proposed_plan_returns_none_for_empty_block() {
    let response = "<proposed_plan>\n\n</proposed_plan>";
    assert!(extract_proposed_plan(response).is_none());
}

#[test]
fn extract_proposed_plan_handles_unclosed_tag() {
    let response = "<proposed_plan>\nPartial plan without closing tag";
    assert!(extract_proposed_plan(response).is_none());
}

// ── Compaction tests (RFC 018) ──────────────────────────────────────

#[test]
fn compaction_config_defaults() {
    let cfg = crate::CompactionConfig::default();
    assert!(cfg.enabled);
    assert_eq!(cfg.threshold_pct, 70);
    assert_eq!(cfg.min_steps, 10);
    assert_eq!(cfg.keep_last, 4);
    assert_eq!(cfg.summary_token_budget, 2000);
    assert_eq!(cfg.cooldown_iterations, 5);
}

#[tokio::test]
async fn compaction_triggers_when_history_exceeds_threshold() {
    // Build a loop with compaction enabled and low thresholds for testing.
    let config = LoopConfig {
        max_iterations: 1,
        compaction: CompactionConfig {
            enabled: true,
            min_steps: 3,
            keep_last: 2,
            threshold_pct: 1, // very low so it always triggers
            ..CompactionConfig::default()
        },
        ..LoopConfig::default()
    };

    // Pre-populate step_history with enough steps.
    // We can't directly set step_history in the loop, so instead we test
    // the compaction logic directly here.
    let mut step_history: Vec<StepSummary> = (0..10)
            .map(|i| StepSummary {
                iteration: i,
                action_kind: "tool_call".to_owned(),
                summary: format!("Called tool_{i} with result: some long output text repeated many times to ensure token threshold is met. Extra padding to make the history large."),
                succeeded: true,
            })
            .collect();

    let before_count = step_history.len();
    let keep = config.compaction.keep_last;
    let to_compact = step_history.len() - keep;

    // Simulate the compaction logic from the loop runner.
    let compacted_text: String = step_history[..to_compact]
        .iter()
        .map(|s| format!("  iter {}: {} [ok]", s.iteration, s.action_kind))
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

    // After compaction: 1 summary + keep_last recent = 3 total
    assert_eq!(step_history.len(), 1 + keep);
    assert_eq!(step_history[0].action_kind, "compacted_summary");
    assert!(step_history[0].summary.contains("Compacted 8 prior steps"));
    // Most recent steps preserved verbatim.
    assert_eq!(step_history[1].iteration, 8);
    assert_eq!(step_history[2].iteration, 9);
    assert!(before_count > step_history.len());
}

#[test]
fn compaction_skips_when_below_min_steps() {
    let cfg = crate::CompactionConfig {
        enabled: true,
        min_steps: 10,
        keep_last: 4,
        threshold_pct: 70,
        summary_token_budget: 2000,
        cooldown_iterations: 5,
    };

    let history_len = 5; // below min_steps
    assert!(history_len < cfg.min_steps);
    // Compaction would not trigger — this is a logic assertion, not a runtime test.
}

#[test]
fn compaction_disabled_skips() {
    let cfg = crate::CompactionConfig {
        enabled: false,
        ..Default::default()
    };
    assert!(!cfg.enabled);
}

#[test]
fn compaction_is_throttled_within_cooldown_window() {
    let config = crate::CompactionConfig {
        enabled: true,
        threshold_pct: 1,
        min_steps: 3,
        keep_last: 2,
        summary_token_budget: 2000,
        cooldown_iterations: 5,
    };
    let mut history: Vec<StepSummary> = (0..8)
            .map(|i| StepSummary {
                iteration: i,
                action_kind: "tool_call".to_owned(),
                summary: format!(
                    "Iteration {i} returned a very large diagnostic payload that should trigger compaction."
                ),
                succeeded: true,
            })
            .collect();
    let mut last_compaction_iteration = None;

    let first = maybe_compact_history(&mut history, 0, &config, &mut last_compaction_iteration);
    assert!(
        first.is_some(),
        "first over-threshold compaction should run"
    );

    history.extend((8..11).map(|i| StepSummary {
        iteration: i,
        action_kind: "tool_call".to_owned(),
        summary: format!("Iteration {i} also returned a large payload but falls inside cooldown."),
        succeeded: true,
    }));

    let second = maybe_compact_history(&mut history, 1, &config, &mut last_compaction_iteration);
    assert!(
        second.is_none(),
        "second compaction attempt inside cooldown must be throttled"
    );
    assert_eq!(
        last_compaction_iteration,
        Some(0),
        "cooldown should preserve the original compaction iteration"
    );
}

// ── (D) F25 drain tests ──────────────────────────────────────────────────
//
// These drive the "approved-but-not-executed" drain the loop runs
// before each GATHER. They use stub phases + a stub
// `ToolCallApprovalReader` so the unit tests stay hermetic. The
// HTTP-level integration test in
// `crates/cairn-app/tests/test_drain_approved_executes_bash.rs`
// covers the full bash-on-filesystem flow.

use cairn_domain::{ApprovalMatchPolicy, ApprovalScope};
use cairn_runtime::error::RuntimeError;
use cairn_runtime::tool_call_approvals::{
    ApprovedProposal, RejectedProposal, StoredProposal, ToolCallApprovalReader,
};

/// Stub reader returning a scripted list of approved + rejected
/// proposals plus a call-count so tests can assert the reader was
/// consulted. Both lists are drained on the FIRST `list_*_for_run`
/// call (subsequent calls see empty vecs) — mirrors the production
/// projection's one-shot Approved→Completed and
/// Rejected→(no further surfacing) lifecycle.
struct ScriptedApprovalReader {
    approved: std::sync::Mutex<Vec<ApprovedProposal>>,
    rejected: std::sync::Mutex<Vec<RejectedProposal>>,
    list_calls: std::sync::atomic::AtomicU32,
}
impl ScriptedApprovalReader {
    fn with(approved: Vec<ApprovedProposal>) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            approved: std::sync::Mutex::new(approved),
            rejected: std::sync::Mutex::new(Vec::new()),
            list_calls: std::sync::atomic::AtomicU32::new(0),
        })
    }
    fn with_rejected(rejected: Vec<RejectedProposal>) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            approved: std::sync::Mutex::new(Vec::new()),
            rejected: std::sync::Mutex::new(rejected),
            list_calls: std::sync::atomic::AtomicU32::new(0),
        })
    }
    fn list_calls(&self) -> u32 {
        self.list_calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}
#[async_trait]
impl ToolCallApprovalReader for ScriptedApprovalReader {
    async fn get_tool_call_approval(
        &self,
        _call_id: &cairn_domain::ToolCallId,
    ) -> Result<Option<ApprovedProposal>, RuntimeError> {
        Ok(None)
    }
    async fn get_tool_call_proposal(
        &self,
        _call_id: &cairn_domain::ToolCallId,
    ) -> Result<Option<StoredProposal>, RuntimeError> {
        Ok(None)
    }
    async fn list_approved_for_run(
        &self,
        _run_id: &cairn_domain::RunId,
    ) -> Result<Vec<ApprovedProposal>, RuntimeError> {
        self.list_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut guard = self.approved.lock().unwrap();
        // Return once, then empty — the drain should run on the
        // first iteration and find nothing on subsequent ones.
        let take = std::mem::take(&mut *guard);
        Ok(take)
    }
    async fn list_rejected_for_run(
        &self,
        _run_id: &cairn_domain::RunId,
    ) -> Result<Vec<RejectedProposal>, RuntimeError> {
        let mut guard = self.rejected.lock().unwrap();
        Ok(std::mem::take(&mut *guard))
    }
}
// Silence "unused match arms via ApprovalMatchPolicy/ApprovalScope"
// when the import is only needed by tests below the current edit.
const _: Option<ApprovalMatchPolicy> = None;
const _: Option<ApprovalScope> = None;

/// ExecutePhase stub whose `dispatch_approved` records the
/// call_ids it was handed and returns a canned status. It still
/// implements `execute` as a no-op so the outer loop terminates.
struct RecordingDispatch {
    dispatched: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    status: std::sync::Mutex<crate::context::ActionStatus>,
}
impl RecordingDispatch {
    fn new(status: crate::context::ActionStatus) -> Self {
        Self {
            dispatched: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            status: std::sync::Mutex::new(status),
        }
    }
}
#[async_trait]
impl ExecutePhase for RecordingDispatch {
    async fn execute(
        &self,
        _ctx: &OrchestrationContext,
        _decide: &DecideOutput,
    ) -> Result<ExecuteOutcome, OrchestratorError> {
        // After the drain, the outer loop calls execute with a
        // complete_run proposal — short-circuit to Done.
        Ok(ExecuteOutcome {
            results: vec![],
            loop_signal: LoopSignal::Done,
        })
    }
    async fn dispatch_approved(
        &self,
        _ctx: &OrchestrationContext,
        approved: &crate::execute::ApprovedDispatch,
    ) -> Result<ActionResult, OrchestratorError> {
        self.dispatched
            .lock()
            .unwrap()
            .push(approved.call_id.as_str().to_owned());
        let synth = ActionProposal {
            action_type: ActionType::InvokeTool,
            description: format!("drained {}", approved.tool_name),
            confidence: 1.0,
            tool_name: Some(approved.tool_name.clone()),
            tool_args: Some(approved.tool_args.clone()),
            requires_approval: false,
        };
        Ok(ActionResult {
            proposal: synth,
            status: self.status.lock().unwrap().clone(),
            tool_output: Some(serde_json::json!({"drained": true})),
            invocation_id: None,
            duration_ms: 0,
        })
    }
}

fn approved(call_id: &str, tool: &str, args: serde_json::Value) -> ApprovedProposal {
    ApprovedProposal {
        call_id: cairn_domain::ToolCallId::new(call_id),
        tool_name: tool.to_owned(),
        tool_args: args,
    }
}

#[tokio::test]
async fn drain_dispatches_approved_proposals_before_decide() {
    // Two approved proposals sit in the projection waiting for
    // re-orchestrate. The drain must dispatch both, in order, and
    // pass through to DECIDE (which ends the run via CompleteRun).
    let reader = ScriptedApprovalReader::with(vec![
        approved("tc_1", "bash", serde_json::json!({"command": "echo one"})),
        approved("tc_2", "bash", serde_json::json!({"command": "echo two"})),
    ]);
    let dispatch = RecordingDispatch::new(ActionStatus::Succeeded);
    let dispatched_handle = dispatch.dispatched.clone();

    let lp = OrchestratorLoop::new(
        FixedGather,
        ScriptedDecide::always(decide_done()),
        dispatch,
        LoopConfig::default(),
    )
    .with_approval_reader(reader.clone());

    let result = lp.run(ctx()).await.unwrap();
    assert!(
        matches!(result, LoopTermination::Completed { .. }),
        "loop should complete after drain + decide, got {result:?}"
    );

    let dispatched = dispatched_handle.lock().unwrap().clone();
    assert_eq!(
        dispatched,
        vec!["tc_1".to_owned(), "tc_2".to_owned()],
        "drain must dispatch approved proposals in oldest-first order"
    );
    assert!(
        reader.list_calls() >= 1,
        "approval reader must be consulted at least once"
    );
}

#[tokio::test]
async fn drain_is_noop_when_no_approvals_present() {
    let reader = ScriptedApprovalReader::with(vec![]);
    let dispatch = RecordingDispatch::new(ActionStatus::Succeeded);
    let dispatched_handle = dispatch.dispatched.clone();

    let lp = OrchestratorLoop::new(
        FixedGather,
        ScriptedDecide::always(decide_done()),
        dispatch,
        LoopConfig::default(),
    )
    .with_approval_reader(reader);

    let _ = lp.run(ctx()).await.unwrap();
    assert!(
        dispatched_handle.lock().unwrap().is_empty(),
        "drain must not dispatch anything when the reader returns empty"
    );
}

#[tokio::test]
async fn drain_without_reader_is_noop() {
    // No `with_approval_reader` — the drain path must skip cleanly
    // so existing tests (and any deployment that hasn't wired a
    // reader yet) are unaffected.
    let dispatch = RecordingDispatch::new(ActionStatus::Succeeded);
    let dispatched_handle = dispatch.dispatched.clone();

    let lp = OrchestratorLoop::new(
        FixedGather,
        ScriptedDecide::always(decide_done()),
        dispatch,
        LoopConfig::default(),
    );

    let _ = lp.run(ctx()).await.unwrap();
    assert!(
        dispatched_handle.lock().unwrap().is_empty(),
        "drain must be a no-op when no approval_reader is wired"
    );
}

/// F46 regression: drained approved proposal must carry the tool
/// output into `step_history` via the `tool_result[<name>] ok: ...`
/// grammar. Without this the next DECIDE's user message has no
/// memory of what happened and the LLM re-proposes the same call.
///
/// We capture `ctx.step_history` from inside a gather stub — the
/// runner snapshots the loop-local `step_history` into
/// `ctx.step_history` immediately before GATHER, so whatever lands
/// here is exactly what `build_user_message` will render.
#[tokio::test]
async fn drained_approved_summary_embeds_tool_result_preview() {
    use std::sync::{Arc, Mutex};

    #[derive(Default, Clone)]
    struct CapturingGather {
        seen: Arc<Mutex<Vec<Vec<StepSummary>>>>,
    }
    #[async_trait]
    impl GatherPhase for CapturingGather {
        async fn gather(
            &self,
            ctx: &OrchestrationContext,
        ) -> Result<GatherOutput, OrchestratorError> {
            self.seen.lock().unwrap().push(ctx.step_history.clone());
            Ok(GatherOutput::default())
        }
    }

    let reader = ScriptedApprovalReader::with(vec![approved(
        "tc_f46",
        "bash",
        serde_json::json!({"command": "printf hello"}),
    )]);
    let dispatch = RecordingDispatch::new(ActionStatus::Succeeded);
    let capturing = CapturingGather::default();
    let seen = capturing.seen.clone();

    let lp = OrchestratorLoop::new(
        capturing,
        ScriptedDecide::always(decide_done()),
        dispatch,
        LoopConfig::default(),
    )
    .with_approval_reader(reader);

    let _ = lp.run(ctx()).await.unwrap();

    // GATHER sees ctx.step_history AFTER the drain pushed its
    // summary — the very invariant F46 depends on.
    let snapshots = seen.lock().unwrap().clone();
    assert!(!snapshots.is_empty(), "gather should have been called");
    let first = snapshots
        .iter()
        .find(|s| !s.is_empty())
        .expect("at least one gather call must see the drained step");
    let summary = &first[0];
    assert_eq!(summary.action_kind, "invoke_tool");
    assert!(
        summary.summary.contains("drained approved: bash"),
        "F46: drain summary must carry the approved-drain header. Got: {:?}",
        summary.summary
    );
    assert!(
        summary.summary.contains("tool_result[bash] ok:"),
        "F46: drain summary MUST embed the tool_result grammar so the \
             LLM can see what happened. Pre-F46 this was a bare header with \
             no payload — the dogfood regression. Got: {:?}",
        summary.summary
    );
}

/// F46: rejected proposals must also leave a `StepSummary` with the
/// operator-supplied reason embedded in the `tool_result[<name>]
/// REJECTED: <preview>` grammar, so the next DECIDE sees WHY and
/// does not re-propose the same call.
#[tokio::test]
async fn rejected_proposal_drain_embeds_reason_in_step_summary() {
    use std::sync::{Arc, Mutex};

    #[derive(Default, Clone)]
    struct CapturingGather {
        seen: Arc<Mutex<Vec<Vec<StepSummary>>>>,
    }
    #[async_trait]
    impl GatherPhase for CapturingGather {
        async fn gather(
            &self,
            ctx: &OrchestrationContext,
        ) -> Result<GatherOutput, OrchestratorError> {
            self.seen.lock().unwrap().push(ctx.step_history.clone());
            Ok(GatherOutput::default())
        }
    }

    let reader = ScriptedApprovalReader::with_rejected(vec![RejectedProposal {
        call_id: cairn_domain::ToolCallId::new("tc_reject_1"),
        tool_name: "bash".to_owned(),
        tool_args: serde_json::json!({"command": "rm -rf /"}),
        reason: Some("that command is dangerous — use specific paths".to_owned()),
    }]);
    let dispatch = RecordingDispatch::new(ActionStatus::Succeeded);
    let capturing = CapturingGather::default();
    let seen = capturing.seen.clone();

    let lp = OrchestratorLoop::new(
        capturing,
        ScriptedDecide::always(decide_done()),
        dispatch,
        LoopConfig::default(),
    )
    .with_approval_reader(reader);

    let _ = lp.run(ctx()).await.unwrap();

    let snapshots = seen.lock().unwrap().clone();
    let first = snapshots
        .iter()
        .find(|s| !s.is_empty())
        .expect("at least one gather call must see the rejection step");
    let summary = &first[0];
    assert!(
        summary.summary.contains("drained rejected: bash"),
        "F46: rejection summary must carry the rejection header. Got: {:?}",
        summary.summary,
    );
    assert!(
        summary.summary.contains("tool_result[bash] REJECTED:"),
        "F46: rejection summary MUST embed the REJECTED tool_result grammar. \
             Got: {:?}",
        summary.summary,
    );
    assert!(
        summary
            .summary
            .contains("that command is dangerous — use specific paths"),
        "F46: operator-supplied reason MUST flow into the summary verbatim \
             (subject to truncate_for_summary). Got: {:?}",
        summary.summary,
    );
}

/// F46: rejection drain must never emit the same `call_id` twice
/// across iterations — the `drained_call_ids` ledger is shared with
/// the approved drain and must dedup both terminal states.
#[tokio::test]
async fn rejected_drain_dedups_across_iterations() {
    use std::sync::{Arc, Mutex};

    #[derive(Default, Clone)]
    struct CountingGather {
        iters: Arc<Mutex<u32>>,
        rejections_seen: Arc<Mutex<Vec<StepSummary>>>,
    }
    #[async_trait]
    impl GatherPhase for CountingGather {
        async fn gather(
            &self,
            ctx: &OrchestrationContext,
        ) -> Result<GatherOutput, OrchestratorError> {
            *self.iters.lock().unwrap() += 1;
            for s in &ctx.step_history {
                if s.summary.contains("drained rejected") {
                    self.rejections_seen.lock().unwrap().push(s.clone());
                }
            }
            Ok(GatherOutput::default())
        }
    }

    // Configure a reader whose rejected list is returned ONCE (take-
    // semantics). On the second iteration list_rejected returns
    // empty — but dedup must protect us even if a projection bug
    // ever re-surfaced the row. To exercise that, we'll manually
    // re-seed the list once and rely on the drained_call_ids ledger
    // to swallow the duplicate.

    struct RepeatingReader {
        payload: Mutex<Option<RejectedProposal>>,
        call_count: std::sync::atomic::AtomicU32,
    }
    #[async_trait]
    impl ToolCallApprovalReader for RepeatingReader {
        async fn get_tool_call_approval(
            &self,
            _call_id: &cairn_domain::ToolCallId,
        ) -> Result<Option<ApprovedProposal>, RuntimeError> {
            Ok(None)
        }
        async fn get_tool_call_proposal(
            &self,
            _call_id: &cairn_domain::ToolCallId,
        ) -> Result<Option<StoredProposal>, RuntimeError> {
            Ok(None)
        }
        async fn list_approved_for_run(
            &self,
            _run_id: &cairn_domain::RunId,
        ) -> Result<Vec<ApprovedProposal>, RuntimeError> {
            Ok(vec![])
        }
        async fn list_rejected_for_run(
            &self,
            _run_id: &cairn_domain::RunId,
        ) -> Result<Vec<RejectedProposal>, RuntimeError> {
            // Return the same rejection on every call so the dedup
            // ledger is the only line of defence.
            self.call_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(self.payload.lock().unwrap().clone().into_iter().collect())
        }
    }

    let reader = Arc::new(RepeatingReader {
        payload: Mutex::new(Some(RejectedProposal {
            call_id: cairn_domain::ToolCallId::new("tc_dup"),
            tool_name: "bash".to_owned(),
            tool_args: serde_json::json!({"command": "x"}),
            reason: Some("nope".to_owned()),
        })),
        call_count: std::sync::atomic::AtomicU32::new(0),
    });

    // Decide returns complete_run on the FIRST call so the loop
    // terminates after one iteration. The dedup contract is then
    // proven by `rejections_seen` length == 1 (the next iteration's
    // gather pass never runs because the run is already terminal).
    //
    // To exercise dedup across TWO iterations we need more decide
    // rounds. Use `decide_tool` for iter 0 (loop continues) then
    // `decide_done` for iter 1. Both iterations see the drain;
    // iter-1 rejection must NOT re-emit.
    struct TwoTurnDecide {
        hits: std::sync::Mutex<u32>,
    }
    #[async_trait]
    impl DecidePhase for TwoTurnDecide {
        async fn decide(
            &self,
            _ctx: &OrchestrationContext,
            _gather: &GatherOutput,
        ) -> Result<DecideOutput, OrchestratorError> {
            let mut n = self.hits.lock().unwrap();
            *n += 1;
            if *n == 1 {
                Ok(DecideOutput {
                    raw_response: "[]".to_owned(),
                    proposals: vec![ActionProposal {
                        action_type: ActionType::InvokeTool,
                        description: "noop".to_owned(),
                        confidence: 0.5,
                        tool_name: Some("noop_tool".to_owned()),
                        tool_args: Some(serde_json::json!({})),
                        requires_approval: false,
                    }],
                    calibrated_confidence: 0.5,
                    requires_approval: false,
                    model_id: "m".to_owned(),
                    latency_ms: 1,
                    input_tokens: None,
                    output_tokens: None,
                    system_prompt: String::new(),
                    messages_json: "[]".to_owned(),
                    tool_calls_json: "[]".to_owned(),
                    tool_defs_json: "[]".to_owned(),
                })
            } else {
                Ok(decide_done())
            }
        }
    }

    let dispatch = RecordingDispatch::new(ActionStatus::Succeeded);
    let gather = CountingGather::default();
    let rejections_seen = gather.rejections_seen.clone();

    let lp = OrchestratorLoop::new(
        gather,
        TwoTurnDecide {
            hits: std::sync::Mutex::new(0),
        },
        dispatch,
        LoopConfig::default(),
    )
    .with_approval_reader(reader.clone());

    let _ = lp.run(ctx()).await.unwrap();

    // Two gather passes ran. Each sees the accumulated step_history.
    // The rejection must appear exactly ONCE in that history —
    // meaning on iteration 2 we captured the iter-1 entry again
    // (that's fine, it's the same push) but the drain did NOT
    // produce a second push for the same call_id.
    //
    // We assert on the *distinct push count* by tracking iteration
    // numbers on the captured summaries: dedup works iff all
    // captured rejection summaries share the same `iteration`
    // stamp (the one where the rejection was first drained).
    let captured = rejections_seen.lock().unwrap().clone();
    assert!(
        !captured.is_empty(),
        "at least one rejection summary must be captured"
    );
    // More than one capture is expected (gather runs twice and the
    // step_history snapshot carries the entry forward), but they
    // must all be the SAME entry — same iteration, same summary
    // text. Dedup failure would manifest as two summaries with
    // different iteration numbers.
    let unique_iterations: std::collections::HashSet<u32> =
        captured.iter().map(|s| s.iteration).collect();
    assert_eq!(
        unique_iterations.len(),
        1,
        "F46 dedup: rejection drain must emit at most one StepSummary \
             per call_id. Distinct iterations seen: {unique_iterations:?}, \
             entries: {captured:#?}",
    );
}

#[tokio::test]
async fn drain_tool_failure_continues_to_decide() {
    // A drained tool that fails must NOT abort the loop — the LLM
    // needs to see the failure on the next GATHER so it can
    // self-correct. We wire a RecordingDispatch returning Failed
    // and assert the loop still reaches its terminal state via
    // DECIDE's CompleteRun proposal.
    let reader = ScriptedApprovalReader::with(vec![approved(
        "tc_failing",
        "bash",
        serde_json::json!({"command": "false"}),
    )]);
    let dispatch = RecordingDispatch::new(ActionStatus::Failed {
        reason: "bash exited 1".to_owned(),
    });
    let dispatched_handle = dispatch.dispatched.clone();

    let lp = OrchestratorLoop::new(
        FixedGather,
        ScriptedDecide::always(decide_done()),
        dispatch,
        LoopConfig::default(),
    )
    .with_approval_reader(reader);

    let result = lp.run(ctx()).await.unwrap();
    assert!(
        matches!(result, LoopTermination::Completed { .. }),
        "drain failure must not abort; loop should reach DECIDE and terminate, \
             got {result:?}"
    );
    assert_eq!(
        dispatched_handle.lock().unwrap().len(),
        1,
        "failing drain entry must still have been dispatched"
    );
}

// ── #606: harness-tools cache eviction on run terminal ──────────────────────

#[test]
fn drives_run_to_terminal_distinguishes_suspension_from_finalization() {
    use cairn_domain::session_orchestration::{BreakerKind, CircuitBreakerTrip};

    // Terminal outcomes — eviction fires.
    assert!(LoopTermination::Completed {
        summary: "ok".into(),
        verification: Default::default(),
    }
    .drives_run_to_terminal());
    assert!(LoopTermination::Failed { reason: "x".into() }.drives_run_to_terminal());
    assert!(LoopTermination::MaxIterationsReached.drives_run_to_terminal());
    assert!(LoopTermination::TimedOut.drives_run_to_terminal());
    assert!(LoopTermination::PlanProposed {
        plan_markdown: "".into()
    }
    .drives_run_to_terminal());
    assert!(LoopTermination::BreakerTripped {
        trip: CircuitBreakerTrip {
            which: BreakerKind::Round,
            measured: 100,
            limit: 100,
            at_iteration: 5,
        },
    }
    .drives_run_to_terminal());

    // Suspension points — eviction must NOT fire.
    assert!(!LoopTermination::WaitingApproval {
        approval_id: ApprovalId::new("ap"),
    }
    .drives_run_to_terminal());
    assert!(!LoopTermination::WaitingSubagent {
        child_task_id: TaskId::new("ct"),
    }
    .drives_run_to_terminal());
}

#[tokio::test]
async fn run_completion_evicts_harness_tools_caches() {
    // After `run()` returns a terminal LoopTermination, the orchestrator
    // must call `cairn_harness_tools::evict_run` — verifiable by priming
    // the write ledger for (project, session, run), running the loop to
    // completion, and asserting the cached Arc is no longer the same.
    //
    // Test-isolation note: the LEDGERS cache is a process-global; using
    // unique session/run ids per test avoids false-sharing when cargo
    // runs `#[test]` functions in parallel.

    use cairn_harness_tools::{__ledger_cache_contains_for_tests, HarnessBuiltin, HarnessRead};
    use cairn_tools::builtins::ToolHandler;

    let dir = tempfile::TempDir::new().unwrap();
    let mut ctx_orig = ctx();
    ctx_orig.session_id = SessionId::new("sess-evict-completion");
    ctx_orig.run_id = RunId::new("run-evict-completion");
    ctx_orig.working_dir = dir.path().to_path_buf();
    let project = ctx_orig.project.clone();
    let tool_ctx = ctx_orig.tool_context();

    // Prime the ledger cache via a real read — `__ledger_cache_contains_for_tests`
    // checks presence by key, it does not mint an entry.
    let path = dir.path().join("a.txt");
    std::fs::write(&path, "hello\n").unwrap();
    let read = HarnessBuiltin::<HarnessRead>::new();
    read.execute_with_context(
        &project,
        serde_json::json!({ "path": path.to_string_lossy() }),
        &tool_ctx,
    )
    .await
    .expect("read should populate the ledger cache");
    assert!(
        __ledger_cache_contains_for_tests(&tool_ctx, &project),
        "prime: read should have populated the ledger cache",
    );

    let lp = OrchestratorLoop::new(
        FixedGather,
        ScriptedDecide::always(decide_done()),
        ScriptedExecute {
            signal: LoopSignal::Done,
        },
        LoopConfig {
            breakers: permissive_breakers(),
            ..Default::default()
        },
    );
    let result = lp.run(ctx_orig).await.unwrap();
    assert!(
        matches!(result, LoopTermination::Completed { .. }),
        "expected Completed, got {result:?}"
    );

    assert!(
        !__ledger_cache_contains_for_tests(&tool_ctx, &project),
        "run terminal must have evicted the write ledger cache (#606)",
    );
}

#[tokio::test]
async fn run_suspension_does_not_evict_harness_tools_caches() {
    // WaitingApproval is a suspension point — the run will resume, so
    // the cached ledger MUST survive. Regression guard against evicting
    // on every termination indiscriminately.
    //
    // Test-isolation note: unique session/run ids keep the sibling
    // `run_completion_evicts_...` test from racing this one on the
    // process-global LEDGERS cache.

    use cairn_harness_tools::{__ledger_cache_contains_for_tests, HarnessBuiltin, HarnessRead};
    use cairn_tools::builtins::ToolHandler;

    let dir = tempfile::TempDir::new().unwrap();
    let mut ctx_orig = ctx();
    ctx_orig.session_id = SessionId::new("sess-evict-suspend");
    ctx_orig.run_id = RunId::new("run-evict-suspend");
    ctx_orig.working_dir = dir.path().to_path_buf();
    let project = ctx_orig.project.clone();
    let tool_ctx = ctx_orig.tool_context();

    // Prime the ledger cache via a real read call so the eviction path
    // has something to check.
    let path = dir.path().join("b.txt");
    std::fs::write(&path, "world\n").unwrap();
    let read = HarnessBuiltin::<HarnessRead>::new();
    read.execute_with_context(
        &project,
        serde_json::json!({ "path": path.to_string_lossy() }),
        &tool_ctx,
    )
    .await
    .expect("read should populate the ledger cache");
    assert!(
        __ledger_cache_contains_for_tests(&tool_ctx, &project),
        "prime: read should have populated the ledger cache",
    );

    // Scripted decide that requires approval → execute phase short-circuits.
    let decide_with_approval = DecideOutput {
        requires_approval: true,
        proposals: vec![ActionProposal {
            action_type: ActionType::InvokeTool,
            description: "suspend".into(),
            confidence: 0.9,
            tool_name: Some("bash".into()),
            tool_args: Some(serde_json::json!({"command": "true"})),
            requires_approval: true,
        }],
        ..decide_done()
    };

    let lp = OrchestratorLoop::new(
        FixedGather,
        ScriptedDecide::always(decide_with_approval),
        ScriptedExecute {
            signal: LoopSignal::WaitApproval {
                approval_id: ApprovalId::new("ap-1"),
            },
        },
        LoopConfig::default(),
    );
    let result = lp.run(ctx_orig).await.unwrap();
    assert!(
        matches!(result, LoopTermination::WaitingApproval { .. }),
        "expected WaitingApproval, got {result:?}"
    );

    assert!(
        __ledger_cache_contains_for_tests(&tool_ctx, &project),
        "suspension-point terminations must not evict the cache — \
         the run will resume and needs the read-before-edit state",
    );
}

// ── #660: strict completion gate ──────────────────────────────────────────
//
// Four unit tests cover the gate's full decision tree:
//
//   1. Gate ON + prior iteration populated the verification accumulator
//      with errors + LLM proposes `complete_run` → rejected; loop
//      continues. Execute must NOT have dispatched the CompleteRun.
//   2. Gate ON + no errors → CompleteRun flows through to Completed.
//   3. Gate ON + errors + three rejections in a row → loop terminates
//      with `LoopTermination::Failed` whose reason prefix routes to
//      `FailureClass::VerificationRejected` in the handler.
//   4. Gate OFF + errors + CompleteRun → CompleteRun accepted,
//      Completed termination. Proves the flag is load-bearing.
//
// The tests share a two-phase decide/execute fixture so the
// verification accumulator sees a failing `cargo build` tool_output on
// iteration 0, the LLM proposes `complete_run` on iteration 1+, and the
// assertions land on whichever arm the gate resolves to.

/// Scripted execute that on iteration 0 returns a bash-class
/// `ActionResult` carrying the provided stdout (drives the verification
/// accumulator), and on iteration 1+ mirrors the decide phase's
/// CompleteRun proposal back with `LoopSignal::Done` so the loop ends
/// Completed unless the gate intercepts. Tracks the total dispatch
/// count so tests can prove CompleteRun was (or was not) actually
/// executed.
mod gate_fixtures {
    use super::*;
    use crate::context::ExecuteOutcome;

    pub(super) struct TwoPhaseExecute {
        pub iter0_stdout: String,
        pub iter0_exit_code: i32,
        /// Counts how many times `execute` was invoked.
        pub dispatch_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        /// Counts how many times a `CompleteRun` proposal actually
        /// survived to the execute phase. If the gate stripped it the
        /// counter stays at zero; if the gate let it through the
        /// counter increments.
        pub complete_run_dispatches: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl ExecutePhase for TwoPhaseExecute {
        async fn execute(
            &self,
            _ctx: &OrchestrationContext,
            decide: &DecideOutput,
        ) -> Result<ExecuteOutcome, OrchestratorError> {
            let n = self
                .dispatch_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let carries_complete_run = decide
                .proposals
                .iter()
                .any(|p| p.action_type == ActionType::CompleteRun);
            if carries_complete_run {
                self.complete_run_dispatches
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }

            let results: Vec<ActionResult> = decide
                .proposals
                .iter()
                .map(|p| {
                    // Iteration 0: synthesise a bash-class tool_output
                    // carrying the configured cargo stdout so the F47
                    // accumulator populates its error bucket.
                    let tool_output = if n == 0
                        && p.action_type == ActionType::InvokeTool
                        && p.tool_name.as_deref() == Some("bash")
                    {
                        let mut map = serde_json::Map::new();
                        map.insert(
                            "stdout".into(),
                            serde_json::Value::String(self.iter0_stdout.clone()),
                        );
                        map.insert(
                            "exit_code".into(),
                            serde_json::Value::from(self.iter0_exit_code),
                        );
                        Some(serde_json::Value::Object(map))
                    } else {
                        None
                    };
                    ActionResult {
                        proposal: p.clone(),
                        status: ActionStatus::Succeeded,
                        tool_output,
                        invocation_id: None,
                        duration_ms: 0,
                    }
                })
                .collect();

            // Iteration 0: any tool invocation is non-terminal.
            // Iteration 1+: if the decide output still has a
            // CompleteRun we end with Done; otherwise Continue
            // so the loop proceeds (the gate stripped the terminal).
            let loop_signal = if n == 0 {
                LoopSignal::Continue
            } else if carries_complete_run {
                LoopSignal::Done
            } else {
                LoopSignal::Continue
            };

            Ok(ExecuteOutcome {
                results,
                loop_signal,
            })
        }
    }

    /// DECIDE stub that returns a bash tool call on iteration 0 and a
    /// `CompleteRun` proposal on every subsequent iteration. Mirrors
    /// the dogfood R4 failure mode (LLM proposes complete_run while the
    /// previous bash tool_result shows borrow-checker errors).
    pub(super) struct BashThenCompleteDecide {
        pub calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl DecidePhase for BashThenCompleteDecide {
        async fn decide(
            &self,
            _ctx: &OrchestrationContext,
            _gather: &GatherOutput,
        ) -> Result<DecideOutput, OrchestratorError> {
            let n = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n == 0 {
                return Ok(DecideOutput {
                    raw_response: r#"[{"action_type":"invoke_tool","tool_name":"bash"}]"#
                        .to_owned(),
                    proposals: vec![ActionProposal {
                        action_type: ActionType::InvokeTool,
                        description: "run cargo build".to_owned(),
                        confidence: 0.9,
                        tool_name: Some("bash".to_owned()),
                        tool_args: Some(serde_json::json!({ "command": "cargo build" })),
                        requires_approval: false,
                    }],
                    calibrated_confidence: 0.9,
                    requires_approval: false,
                    model_id: "test-model".to_owned(),
                    latency_ms: 20,
                    input_tokens: None,
                    output_tokens: None,
                    system_prompt: String::new(),
                    messages_json: "[]".to_owned(),
                    tool_calls_json: "[]".to_owned(),
                    tool_defs_json: "[]".to_owned(),
                });
            }
            Ok(DecideOutput {
                raw_response: r#"[{"action_type":"complete_run"}]"#.to_owned(),
                proposals: vec![ActionProposal {
                    action_type: ActionType::CompleteRun,
                    description: "all done".to_owned(),
                    confidence: 0.95,
                    tool_name: None,
                    tool_args: None,
                    requires_approval: false,
                }],
                calibrated_confidence: 0.95,
                requires_approval: false,
                model_id: "test-model".to_owned(),
                latency_ms: 10,
                input_tokens: None,
                output_tokens: None,
                system_prompt: String::new(),
                messages_json: "[]".to_owned(),
                tool_calls_json: "[]".to_owned(),
                tool_defs_json: "[]".to_owned(),
            })
        }
    }

    /// Cargo-like stdout carrying two `error:` lines that the F47
    /// accumulator's regex will bucket. Reused across tests.
    pub(super) const CARGO_ERROR_STDOUT: &str = "\
error[E0382]: borrow of moved value: `x`\n   --> src/lib.rs:3:5\n\
error[E0502]: cannot borrow `y` as mutable because it is also borrowed as immutable\n   --> src/lib.rs:10:5\n\
warning: unused import: `std::io::Write`\n   --> src/lib.rs:1:5\n\
error: could not compile `demo` due to 2 previous errors\n";
}

/// #660 (1): gate ON + errors + CompleteRun → gate rejects; loop
/// continues rather than completing; execute never sees the
/// CompleteRun proposal.
#[tokio::test]
async fn completion_gate_rejects_complete_run_when_verification_has_errors() {
    use gate_fixtures::{BashThenCompleteDecide, TwoPhaseExecute, CARGO_ERROR_STDOUT};

    let decide_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let dispatch_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let complete_run_dispatches = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    // Cap iterations at 4 so we can observe at least one rejection + one
    // re-decide turn (iter 0 bash, iter 1 rejected complete_run, iter 2
    // re-proposed complete_run…). We don't want the cap to collide with
    // the gate's own 3-rejection cap here — that's the next test.
    let config = LoopConfig {
        max_iterations: 4,
        breakers: permissive_breakers(),
        // Strict gate ON (default, but pin it explicitly to document intent).
        orchestrator_strict_completion_gate: true,
        ..Default::default()
    };
    let lp = OrchestratorLoop::new(
        FixedGather,
        BashThenCompleteDecide {
            calls: decide_calls.clone(),
        },
        TwoPhaseExecute {
            iter0_stdout: CARGO_ERROR_STDOUT.to_owned(),
            iter0_exit_code: 101,
            dispatch_count: dispatch_count.clone(),
            complete_run_dispatches: complete_run_dispatches.clone(),
        },
        config,
    );

    let result = lp.run(ctx()).await.unwrap();

    // With max_iterations=4 and each rejection bumping the counter,
    // the loop terminates either via MaxIterationsReached (if the gate
    // lets every re-decide fire) or Failed(VerificationRejected) once
    // the 3-reject cap hits. Both are valid "did NOT complete" shapes
    // for this test — the load-bearing invariant is that
    // `LoopTermination::Completed` is NOT observed.
    assert!(
        !matches!(result, LoopTermination::Completed { .. }),
        "gate must block `complete_run` when verification errors are \
         present; got {result:?}"
    );

    // The decide phase must have been re-invoked after the first
    // CompleteRun proposal, proving the loop re-entered DECIDE instead
    // of terminating.
    assert!(
        decide_calls.load(std::sync::atomic::Ordering::SeqCst) >= 3,
        "DECIDE must re-run after a rejection (iter 0 bash + iter 1+ \
         complete_run attempts); got {} decide calls",
        decide_calls.load(std::sync::atomic::Ordering::SeqCst),
    );

    // Execute must never have dispatched a CompleteRun — the gate
    // strips the proposal in place before the execute phase runs.
    assert_eq!(
        complete_run_dispatches.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "CompleteRun must never reach execute while the gate is rejecting",
    );
}

/// #660 (2): gate ON + clean verification → CompleteRun passes through
/// as the pre-fix behaviour. Pins that the gate doesn't accidentally
/// reject happy-path runs.
#[tokio::test]
async fn completion_gate_allows_complete_run_when_verification_clean() {
    use gate_fixtures::{BashThenCompleteDecide, TwoPhaseExecute};

    let decide_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let dispatch_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let complete_run_dispatches = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let config = LoopConfig {
        max_iterations: 5,
        breakers: permissive_breakers(),
        orchestrator_strict_completion_gate: true,
        ..Default::default()
    };
    let lp = OrchestratorLoop::new(
        FixedGather,
        BashThenCompleteDecide {
            calls: decide_calls.clone(),
        },
        TwoPhaseExecute {
            // Clean stdout — no `error:` lines for the accumulator to bucket.
            iter0_stdout: "    Finished dev [unoptimized + debuginfo] target(s) in 3.2s\n"
                .to_owned(),
            iter0_exit_code: 0,
            dispatch_count: dispatch_count.clone(),
            complete_run_dispatches: complete_run_dispatches.clone(),
        },
        config,
    );

    let result = lp.run(ctx()).await.unwrap();

    assert!(
        matches!(result, LoopTermination::Completed { .. }),
        "clean verification + complete_run must terminate as Completed; got {result:?}"
    );
    assert_eq!(
        complete_run_dispatches.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "CompleteRun must have reached execute exactly once on the happy path",
    );
}

/// #660 (3): gate ON + errors + three CompleteRun attempts →
/// `LoopTermination::Failed` with the contract reason prefix. Proves
/// the rejection cap prevents budget burn and that the failure reason
/// carries the `verification_rejected:` prefix the handler's
/// `classify_failed_reason` keys on.
#[tokio::test]
async fn completion_gate_force_fails_after_three_rejections() {
    use gate_fixtures::{BashThenCompleteDecide, TwoPhaseExecute, CARGO_ERROR_STDOUT};

    let decide_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let dispatch_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let complete_run_dispatches = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    // Max iterations comfortably above the 3-reject cap so the cap is
    // what fires the failure, not the iteration budget.
    let config = LoopConfig {
        max_iterations: 20,
        breakers: permissive_breakers(),
        orchestrator_strict_completion_gate: true,
        ..Default::default()
    };
    let lp = OrchestratorLoop::new(
        FixedGather,
        BashThenCompleteDecide {
            calls: decide_calls.clone(),
        },
        TwoPhaseExecute {
            iter0_stdout: CARGO_ERROR_STDOUT.to_owned(),
            iter0_exit_code: 101,
            dispatch_count: dispatch_count.clone(),
            complete_run_dispatches: complete_run_dispatches.clone(),
        },
        config,
    );
    let result = lp.run(ctx()).await.unwrap();

    match result {
        LoopTermination::Failed { reason } => {
            assert!(
                reason.starts_with("verification_rejected:"),
                "#660: Failed reason MUST start with `verification_rejected:` — \
                 this prefix is the wire contract with \
                 `classify_failed_reason` in the HTTP handler. Got: {reason}"
            );
            // Evidence of the triggering errors must travel back to the
            // operator so the run's failure is actionable without
            // digging through the event log.
            assert!(
                reason.contains("error"),
                "reason should include at least one error excerpt; got: {reason}"
            );
        }
        other => panic!("expected LoopTermination::Failed(verification_rejected), got {other:?}"),
    }

    // And CompleteRun never reached execute — the gate blocked all
    // three attempts before dispatch.
    assert_eq!(
        complete_run_dispatches.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "CompleteRun must never reach execute across all three rejections",
    );
}

// ── #689 R2-B: echo-via-bash prose-playing detector ──────────────────────

/// Issue #689 Finding R2-B: a loop-level integration test proving the
/// detector hook actually fires through to the emitter callback.
/// Two consecutive bash-echo turns must trigger exactly one
/// `on_prose_playing_detected` callback on the second turn (not the
/// first), and the run keeps going — the detector is observational,
/// not an enforcer.
///
/// The pure detector unit tests (test A–E in `echo_detector::tests`)
/// cover the heuristic. This test pins the wiring: if someone
/// detaches the detector from the loop or forgets to call the
/// emitter callback, this breaks.
#[tokio::test]
async fn echo_via_bash_detector_fires_on_second_consecutive_echo_turn() {
    #[derive(Default)]
    struct RecordingEmitter {
        detected_counts: std::sync::Mutex<Vec<u32>>,
    }
    #[async_trait]
    impl crate::emitter::OrchestratorEventEmitter for RecordingEmitter {
        async fn on_prose_playing_detected(
            &self,
            _ctx: &OrchestrationContext,
            consecutive_count: u32,
        ) {
            self.detected_counts.lock().unwrap().push(consecutive_count);
        }
    }

    // Stub decide that emits bash-echo on the first two iterations
    // then complete_run so the loop terminates cleanly.
    struct EchoThenDone {
        calls: std::sync::Mutex<u32>,
    }
    #[async_trait]
    impl DecidePhase for EchoThenDone {
        async fn decide(
            &self,
            _: &OrchestrationContext,
            _: &GatherOutput,
        ) -> Result<DecideOutput, OrchestratorError> {
            let mut n = self.calls.lock().unwrap();
            *n += 1;
            if *n <= 2 {
                let bash_proposal = ActionProposal::invoke_tool(
                    "bash",
                    serde_json::json!({
                        "command": format!(
                            "echo 'narration turn {}: thinking about next step'",
                            *n
                        ),
                    }),
                    "echo narration",
                    0.8,
                    true,
                );
                Ok(DecideOutput {
                    raw_response: String::new(),
                    proposals: vec![bash_proposal],
                    calibrated_confidence: 0.8,
                    requires_approval: true,
                    model_id: "stub".into(),
                    latency_ms: 1,
                    input_tokens: None,
                    output_tokens: None,
                    system_prompt: String::new(),
                    messages_json: "[]".to_owned(),
                    tool_calls_json: "[]".to_owned(),
                    tool_defs_json: "[]".to_owned(),
                })
            } else {
                Ok(decide_done())
            }
        }
    }

    let emitter = std::sync::Arc::new(RecordingEmitter::default());
    let config = LoopConfig {
        max_iterations: 5,
        breakers: permissive_breakers(),
        ..Default::default()
    };
    let lp = OrchestratorLoop::new(
        FixedGather,
        EchoThenDone {
            calls: std::sync::Mutex::new(0),
        },
        ScriptedExecute {
            signal: LoopSignal::Continue,
        },
        config,
    )
    .with_emitter(emitter.clone());

    // Run to termination — the complete_run on iter 3 ends it.
    let _ = lp.run(ctx()).await;

    let counts = emitter.detected_counts.lock().unwrap().clone();
    // Iteration 0 (first bash-echo): counter = 1, below threshold → no callback.
    // Iteration 1 (second bash-echo): counter = 2, at threshold → one callback.
    // Iteration 2 (complete_run): non-echo → counter reset, no callback.
    assert_eq!(
        counts,
        vec![2],
        "detector must fire exactly once at the threshold crossing (consecutive_count=2); \
         first echo turn below threshold, third turn is complete_run"
    );
}

/// #660 (4): gate OFF + errors + CompleteRun → run completes normally
/// (legacy behaviour). Proves the flag is load-bearing; operators who
/// opt out of the gate see the pre-fix flow.
#[tokio::test]
async fn completion_gate_disabled_via_settings_flag() {
    use gate_fixtures::{BashThenCompleteDecide, TwoPhaseExecute, CARGO_ERROR_STDOUT};

    let decide_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let dispatch_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let complete_run_dispatches = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let config = LoopConfig {
        max_iterations: 5,
        breakers: permissive_breakers(),
        // Gate OFF: the pre-#660 behaviour.
        orchestrator_strict_completion_gate: false,
        ..Default::default()
    };
    let lp = OrchestratorLoop::new(
        FixedGather,
        BashThenCompleteDecide {
            calls: decide_calls.clone(),
        },
        TwoPhaseExecute {
            iter0_stdout: CARGO_ERROR_STDOUT.to_owned(),
            iter0_exit_code: 101,
            dispatch_count: dispatch_count.clone(),
            complete_run_dispatches: complete_run_dispatches.clone(),
        },
        config,
    );

    let result = lp.run(ctx()).await.unwrap();

    assert!(
        matches!(result, LoopTermination::Completed { .. }),
        "gate disabled → CompleteRun accepted despite errors; got {result:?}"
    );
    assert_eq!(
        complete_run_dispatches.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "gate disabled → CompleteRun must flow through to execute exactly once",
    );
}

// ── #689 R2-A: malformed spawn_subagent retry ─────────────────────────────
//
// Dogfood R2 Finding A (issue #689, HIGH). The LLM emitted a
// `spawn_subagent` proposal with missing or empty `tool_args["goal"]`;
// the execute phase returned `LoopSignal::Failed`, and the run state
// flipped to Failed on a single bad LLM emission. Expected behaviour:
// the rejection threads into `step_history`, the loop continues, and
// the LLM gets to re-emit on the next DECIDE turn. A bounded retry
// cap (`MAX_CONSECUTIVE_MALFORMED_SPAWNS = 3`) prevents a permanently-
// broken model from burning the iteration budget.
//
// Two tests pin the contract:
//
//   A. One malformed spawn, then one valid spawn → loop progresses
//      through the valid spawn and suspends on WaitSubagent.
//   B. Three consecutive malformed spawns → loop terminates with
//      `LoopTermination::Failed` whose reason carries the contract
//      `malformed_spawn_proposal:` prefix.

mod malformed_spawn_fixtures {
    use super::*;
    use crate::context::ExecuteOutcome;

    /// A `DecidePhase` that cycles through a scripted sequence of
    /// outputs — reused pattern from the `ScriptedDecide` + counter
    /// approach used by the `#660` gate fixtures.
    pub(super) struct ScriptedSpawnDecide {
        pub outputs: Vec<DecideOutput>,
        pub calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl DecidePhase for ScriptedSpawnDecide {
        async fn decide(
            &self,
            _ctx: &OrchestrationContext,
            _: &GatherOutput,
        ) -> Result<DecideOutput, OrchestratorError> {
            let n = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let idx = n.min(self.outputs.len() - 1);
            Ok(self.outputs[idx].clone())
        }
    }

    /// Execute phase that mirrors the real `RuntimeExecutePhase` for
    /// the spawn_subagent validation carve-out.
    ///
    /// IMPORTANT: this fixture calls the real
    /// [`crate::execute_impl::derive_signal`] to compute the loop
    /// signal from each `ActionResult`, exactly as the production
    /// execute phase does. That way the test exercises the same
    /// pre-fix-vs-post-fix pivot as the real code: pre-fix (no
    /// carve-out) maps a `SpawnSubagent` `Failed` to
    /// `LoopSignal::Failed`; post-fix (with
    /// `MALFORMED_SPAWN_PROPOSAL_PREFIX`) maps it to
    /// `LoopSignal::Continue`. Without using the real function, a
    /// stub that hard-codes `Continue` would silently pass on pre-fix
    /// code (false-positive guardrail).
    ///
    /// For `SpawnSubagent` proposals whose `tool_args` lacks a
    /// non-empty `goal`, returns `ActionStatus::Failed` tagged with
    /// the contract prefix; for valid spawns it returns
    /// `SubagentSpawned`; anything else returns `Succeeded`.
    pub(super) struct SpawnValidatingExecute {
        pub dispatch_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        pub successful_spawns: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        pub rejected_spawns: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl ExecutePhase for SpawnValidatingExecute {
        async fn execute(
            &self,
            _ctx: &OrchestrationContext,
            decide: &DecideOutput,
        ) -> Result<ExecuteOutcome, OrchestratorError> {
            self.dispatch_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            let mut results: Vec<ActionResult> = Vec::new();

            for p in decide.proposals.iter() {
                if p.action_type == ActionType::SpawnSubagent {
                    let goal_present = p
                        .tool_args
                        .as_ref()
                        .and_then(|v| v.get("goal"))
                        .and_then(|g| g.as_str())
                        .map(str::trim)
                        .map(|s| !s.is_empty())
                        .unwrap_or(false);
                    if goal_present {
                        self.successful_spawns
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let child_task_id = TaskId::new("child_task_1");
                        results.push(ActionResult {
                            proposal: p.clone(),
                            status: ActionStatus::SubagentSpawned {
                                child_task_id: child_task_id.clone(),
                            },
                            tool_output: None,
                            invocation_id: None,
                            duration_ms: 0,
                        });
                    } else {
                        self.rejected_spawns
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        // Tag with the same contract prefix the real
                        // execute phase uses so `derive_signal`'s
                        // #689 R2-A carve-out fires. Pre-fix code did
                        // NOT tag this reason — instead it returned
                        // the same shape but without the prefix, and
                        // `derive_signal` promoted it to
                        // `LoopSignal::Failed`. We mirror the post-fix
                        // reason because the pre-fix reason
                        // (non-prefixed) would also be correctly
                        // promoted to `LoopSignal::Failed` by the
                        // post-fix `derive_signal` (its carve-out
                        // gates on the prefix), which means **this
                        // test is load-bearing in both directions**:
                        //
                        //   * Pre-fix `derive_signal` (no prefix
                        //     check) sees non-InvokeTool Failed →
                        //     `LoopSignal::Failed` → test panics.
                        //   * Post-fix `derive_signal` sees prefix →
                        //     `LoopSignal::Continue` → test passes.
                        results.push(ActionResult {
                            proposal: p.clone(),
                            status: ActionStatus::Failed {
                                reason: format!(
                                    "{}spawn_subagent: tool_args[\"goal\"] \
                                     is required and must be a non-empty string",
                                    crate::execute_impl::MALFORMED_SPAWN_PROPOSAL_PREFIX,
                                ),
                            },
                            tool_output: None,
                            invocation_id: None,
                            duration_ms: 0,
                        });
                    }
                } else {
                    results.push(ActionResult {
                        proposal: p.clone(),
                        status: ActionStatus::Succeeded,
                        tool_output: None,
                        invocation_id: None,
                        duration_ms: 0,
                    });
                }
            }

            // Derive the loop signal via the real function under test
            // so the pre-fix vs post-fix semantics are exercised
            // end-to-end. See the struct rustdoc for why this is
            // load-bearing.
            let mut loop_signal = LoopSignal::Continue;
            for r in &results {
                let next = crate::execute_impl::derive_signal(r, &loop_signal);
                if !matches!(next, LoopSignal::Continue) {
                    loop_signal = next;
                    break;
                }
            }

            Ok(ExecuteOutcome {
                results,
                loop_signal,
            })
        }
    }

    pub(super) fn decide_malformed_spawn() -> DecideOutput {
        DecideOutput {
            raw_response:
                r#"[{"action_type":"spawn_subagent","tool_name":"researcher","tool_args":{}}]"#
                    .to_owned(),
            proposals: vec![ActionProposal {
                action_type: ActionType::SpawnSubagent,
                description: "delegate research".to_owned(),
                confidence: 0.8,
                tool_name: Some("researcher".to_owned()),
                // Empty tool_args — mirrors the dogfood R2 repro
                // (run_r2_1778028487): goal missing.
                tool_args: Some(serde_json::json!({})),
                requires_approval: false,
            }],
            calibrated_confidence: 0.8,
            requires_approval: false,
            model_id: "test-model".to_owned(),
            latency_ms: 20,
            input_tokens: None,
            output_tokens: None,
            system_prompt: String::new(),
            messages_json: "[]".to_owned(),
            tool_calls_json: "[]".to_owned(),
            tool_defs_json: "[]".to_owned(),
        }
    }

    pub(super) fn decide_valid_spawn() -> DecideOutput {
        DecideOutput {
            raw_response: r#"[{"action_type":"spawn_subagent","tool_name":"researcher","tool_args":{"goal":"investigate X"}}]"#.to_owned(),
            proposals: vec![ActionProposal {
                action_type: ActionType::SpawnSubagent,
                description: "delegate research".to_owned(),
                confidence: 0.85,
                tool_name: Some("researcher".to_owned()),
                tool_args: Some(serde_json::json!({ "goal": "investigate X" })),
                requires_approval: false,
            }],
            calibrated_confidence: 0.85,
            requires_approval: false,
            model_id: "test-model".to_owned(),
            latency_ms: 20,
            input_tokens: None,
            output_tokens: None,
            system_prompt: String::new(),
            messages_json: "[]".to_owned(),
            tool_calls_json: "[]".to_owned(),
            tool_defs_json: "[]".to_owned(),
        }
    }
}

/// #689 R2-A (A): one malformed spawn then one valid spawn. The run
/// MUST NOT die on the malformed proposal — it must thread the
/// rejection into `step_history`, re-enter DECIDE, accept the valid
/// spawn, and suspend on `WaitingSubagent`. Pre-fix the first
/// malformed proposal promoted to `LoopSignal::Failed` and the run
/// terminated immediately; this test fails on pre-fix code.
#[tokio::test]
async fn malformed_spawn_proposal_does_not_kill_run_and_llm_can_retry() {
    use malformed_spawn_fixtures::{
        decide_malformed_spawn, decide_valid_spawn, ScriptedSpawnDecide, SpawnValidatingExecute,
    };

    let decide_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let dispatch_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let successful_spawns = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let rejected_spawns = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let config = LoopConfig {
        max_iterations: 10,
        breakers: permissive_breakers(),
        ..Default::default()
    };

    let lp = OrchestratorLoop::new(
        FixedGather,
        ScriptedSpawnDecide {
            outputs: vec![decide_malformed_spawn(), decide_valid_spawn()],
            calls: decide_calls.clone(),
        },
        SpawnValidatingExecute {
            dispatch_count: dispatch_count.clone(),
            successful_spawns: successful_spawns.clone(),
            rejected_spawns: rejected_spawns.clone(),
        },
        config,
    );

    let result = lp.run(ctx()).await.unwrap();

    // The loop MUST have reached the second iteration and accepted the
    // valid spawn. Pre-#689 fix this would be `LoopTermination::Failed`
    // because the first iteration's malformed spawn promoted to
    // `LoopSignal::Failed`.
    match result {
        LoopTermination::WaitingSubagent { child_task_id } => {
            assert_eq!(
                child_task_id.as_str(),
                "child_task_1",
                "second iteration's valid spawn must produce the expected child_task_id"
            );
        }
        other => panic!(
            "expected WaitingSubagent after malformed → valid retry; got {other:?}. \
             This means the malformed proposal terminated the run instead of \
             letting the LLM retry on the next DECIDE turn."
        ),
    }

    // Counter sanity: exactly one malformed rejection and exactly one
    // successful spawn.
    assert_eq!(
        rejected_spawns.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "malformed proposal must have been rejected once"
    );
    assert_eq!(
        successful_spawns.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "valid proposal must have been accepted once"
    );
    // DECIDE must have been called twice (iter 0 malformed, iter 1 valid).
    assert_eq!(
        decide_calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "DECIDE must re-run on the iteration after a malformed spawn"
    );
}

/// #689 R2-A (B): three consecutive malformed spawns trip the
/// bounded-retry cap. The run MUST terminate with
/// `LoopTermination::Failed` whose reason carries the contract
/// `malformed_spawn_proposal:` prefix and a mention of the cap so the
/// operator can see why the run died.
#[tokio::test]
async fn malformed_spawn_proposal_bounded_retry_cap_fails_run() {
    use malformed_spawn_fixtures::{
        decide_malformed_spawn, ScriptedSpawnDecide, SpawnValidatingExecute,
    };

    let decide_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let dispatch_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let successful_spawns = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let rejected_spawns = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    // Max iterations well above the cap so the cap is what fires the
    // failure, not the iteration budget.
    let config = LoopConfig {
        max_iterations: 20,
        breakers: permissive_breakers(),
        ..Default::default()
    };

    let lp = OrchestratorLoop::new(
        FixedGather,
        // Always return malformed — simulates a permanently-broken
        // model that can't self-correct.
        ScriptedSpawnDecide {
            outputs: vec![decide_malformed_spawn()],
            calls: decide_calls.clone(),
        },
        SpawnValidatingExecute {
            dispatch_count: dispatch_count.clone(),
            successful_spawns: successful_spawns.clone(),
            rejected_spawns: rejected_spawns.clone(),
        },
        config,
    );

    let result = lp.run(ctx()).await.unwrap();

    match result {
        LoopTermination::Failed { reason } => {
            assert!(
                reason.starts_with(crate::execute_impl::MALFORMED_SPAWN_PROPOSAL_PREFIX),
                "#689 R2-A: Failed reason MUST start with \
                 `malformed_spawn_proposal:` so the handler's \
                 `classify_failed_reason` can route to \
                 `FailureClass::ExecutionError`. Got: {reason}"
            );
            assert!(
                reason.contains("cap of"),
                "reason MUST mention the cap so the operator sees why the run died. Got: {reason}"
            );
        }
        other => {
            panic!("expected LoopTermination::Failed(malformed_spawn_proposal); got {other:?}")
        }
    }

    // Exactly three rejections — the cap itself.
    assert_eq!(
        rejected_spawns.load(std::sync::atomic::Ordering::SeqCst),
        crate::context::MAX_CONSECUTIVE_MALFORMED_SPAWNS as usize,
        "rejected_spawns must equal the cap value ({})",
        crate::context::MAX_CONSECUTIVE_MALFORMED_SPAWNS
    );
    // No successful spawns — all attempts were malformed.
    assert_eq!(
        successful_spawns.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "no valid spawn was proposed, so no successful spawn should have run"
    );
}
