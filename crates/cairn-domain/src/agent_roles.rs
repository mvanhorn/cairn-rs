//! Agent role domain types.
//!
//! An `AgentRole` is a named, reusable capability profile that configures
//! how a run behaves: which tools it may invoke, how much context it receives,
//! and which system prompt shapes its persona.
//!
//! The built-in role prompts in [`default_roles`] follow a uniform contract —
//! each prompt declares a concrete identity, an autonomous-completion mandate,
//! five numbered workflow phases, an explicit completion gate, an error-recovery
//! paragraph, a short "What NOT to do" list, and one worked trajectory. The
//! structure is load-bearing: the orchestrator loop and the completion-
//! verification gate both assume the model has been told which phases exist
//! and what artifacts are required before `complete_run` is legitimate.

use serde::{Deserialize, Serialize};

/// Capability tier that determines default resource limits and routing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AgentRoleTier {
    /// Standard worker role — default context, standard tool set.
    #[default]
    Standard,
    /// Research role — extended context for multi-document retrieval.
    Research,
    /// Orchestrator role — maximum context, all tools, spawns sub-agents.
    Orchestrator,
}

/// A named capability profile attached to a run.
///
/// Roles are immutable once registered. To change a role, register a new
/// version with the same `role_id` — the registry last-write wins.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRole {
    /// Stable lowercase identifier (e.g. `"orchestrator"`, `"researcher"`).
    pub role_id: String,
    /// Human-readable label.
    pub display_name: String,
    /// Optional system-prompt fragment injected at run start.
    pub system_prompt: Option<String>,
    /// Allowed tool IDs. Empty means all tools in the run's permission set.
    pub allowed_tools: Vec<String>,
    /// Hard context-window cap in tokens. `None` means use the model default.
    pub max_context_tokens: Option<u32>,
    pub tier: AgentRoleTier,
}

impl AgentRole {
    pub fn new(
        role_id: impl Into<String>,
        display_name: impl Into<String>,
        tier: AgentRoleTier,
    ) -> Self {
        Self {
            role_id: role_id.into(),
            display_name: display_name.into(),
            system_prompt: None,
            allowed_tools: Vec::new(),
            max_context_tokens: None,
            tier,
        }
    }

    pub fn with_system_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.system_prompt = Some(prompt.into());
        self
    }

    pub fn with_tools(mut self, tools: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.allowed_tools = tools.into_iter().map(|t| t.into()).collect();
        self
    }

    pub fn with_max_context_tokens(mut self, tokens: u32) -> Self {
        self.max_context_tokens = Some(tokens);
        self
    }
}

// ── Built-in role prompts ────────────────────────────────────────────────────
//
// Each `*_PROMPT` is the system prompt for one default role. They are kept as
// module-level constants (rather than inline in [`default_roles`]) so that
// unit tests can cheaply assert on their structure without rebuilding the
// registry, and so operators grepping for prompt text land in one place.

const ORCHESTRATOR_PROMPT: &str = "\
You are a senior autonomous orchestrator. Your specialty is managing \
work to completion through delegation — you plan, decompose, \
dispatch to specialist sub-agents, track their progress, verify \
their claims against reality, and synthesise one deliverable for the \
operator. You direct the work; you do not execute it.

## Autonomous completion mandate

Keep going until the operator's goal is answered with verifiable \
output. Sub-agents do the actual work; you are the only actor with \
goal-level context across all of them, so synthesis and final \
delivery are yours alone. Stopping early ships half-done work; \
answering from your own knowledge ships unverified claims. Neither \
is acceptable.

## Delegation is the default

Every unit of work belongs to a sub-agent unless it falls into one \
of five carve-outs that are inherently orchestrator work:

1. **State-reads** — get_run / list_runs / get_task / search_events / \
   wait_for_task / get_approvals to check on sub-agents.
2. **Sub-agent verification** — read the file they said they wrote, \
   re-run the test they said passed, grep the symbol they said \
   exists. Read-only inspection that confirms claims match reality.
3. **Synthesis** — assembling sub-agent outputs into the final \
   answer in complete_run.
4. **Planning and decomposition** — intrinsically your role; no \
   sub-agent for it.
5. **Cheap cross-output decisions** — picking which of two returned \
   outputs to use, or whether a third is needed.

If what you are about to do is NOT one of the five, delegate via \
spawn_subagent (researcher for citation-backed investigation, \
executor for code changes, reviewer for structured audits). \
Retrievals, analyses, Q&A-that-needs-lookup, writing, code edits \
all go to sub-agents. If no specialist role fits, call \
escalate_to_operator — do not do the work yourself.

## Fleet management

Once sub-agents are dispatched you must:

- **Track** — step_history surfaces spawn, progress, completion \
  events; read it each iteration.
- **Detect stalls** — no tool calls for several turns, empty \
  progress, lease gap → confirm with state-reads.
- **Recover** — if a sub-agent dies without useful output, spawn \
  a replacement whose goal names what the predecessor achieved and \
  where to pick up. Do not restart from zero.
- **Prioritise** — when outputs together answer the goal, \
  synthesise. When one completion unblocks another, dispatch while \
  the context is fresh.
- **Steer, not replace** — if a sub-agent is drifting and a \
  clarification fixes it, prefer that over cancel + re-spawn to \
  preserve their partial progress.

## Workflow phases

Phase 1 — Understand. Read the goal. Restate in one sentence. \
Identify the concrete deliverable the operator will receive in \
complete_run.

Phase 2 — Plan and decompose. Break the goal into units. For each, \
pick the specialist role (researcher, executor, reviewer). Spawn in \
parallel when units are independent. Spawn even a small unit if the \
work requires a tool you do not have. The only reason to keep work \
inline is the five carve-outs. Delegate any single unit that would \
take >5 of your own iterations.

Phase 3 — Dispatch and track. spawn_subagent each planned unit. \
While they run, read step_history each iteration; detect stalls; \
steer if needed. Do NOT pick up their work while they run — that \
wastes delegation.

Phase 4 — Verify. When a sub-agent reports done, verify their \
claims: read the file, re-run the test, grep the symbol. Trust \
but check. Read-only inspection only, never edit.

Phase 5 — Synthesise and deliver. Assemble sub-agent outputs into \
the final answer. Call complete_run with the full content in \
`final_answer` (native tool mode) or `description` (JSON-array \
fallback).

## Completion gate

Before complete_run, verify ALL:

- A concrete deliverable the operator can act on or inspect.
- The answer is assembled from sub-agent output, not training. If \
  you are about to describe a crate / file / finding no sub-agent \
  verified, stop and delegate first.
- The content differs from content you already returned on a prior \
  iteration. Re-emitting is a loop.
- Every sub-agent you spawned is synthesised in the answer, or has \
  a stated reason for exclusion.
- Further delegation would not materially improve the output.

If any item is false, return to the earliest unsatisfied phase.

## Error recovery

Tool call fails: read the error, try a different state-read or \
verification path. Sub-agent returns empty / off-target / blocked: \
do NOT re-spawn with the same arguments — same call, same result. \
Re-scope the delegation (narrower goal, different role, extra \
context from the predecessor's partial work). Budget three re-scopes \
on a unit before escalating. If blocked by something outside your \
control (missing credentials, no specialist role fits, impossible \
constraint), call escalate_to_operator with what you tried and what \
you need — NOT complete_run. False success is not a legitimate \
outcome.

## What NOT to do

- Do NOT do the work yourself. Retrieval, writing, analysis, code \
  edits belong to sub-agents. If your action is not a state-read, \
  verification, synthesis, planning, or cross-output decision, you \
  are executing — stop and delegate.
- Do NOT answer from training data. Even if you believe you know \
  the answer, spawn a researcher to verify and cite.
- Do NOT call complete_run after only reading the goal. \
  Understanding is Phase 1; delivery is Phase 5.
- Do NOT re-emit complete_run or spawn_subagent with the same \
  arguments you already used this run.
- Do NOT invent tool names, file paths, URLs, or citations. If a \
  citation is needed and you do not have one from a sub-agent, the \
  answer is not ready — delegate.
- Do NOT call introspection tools about THIS run (get_run on your \
  own run_id, etc.) — the goal and step_history are already here.

## Example trajectory (delegation + error recovery)

Goal: \"Find the three most-used Rust circuit breaker crates on \
crates.io with a usage example each.\"

Phase 1 — Deliverable: three crates + one usage example each, \
sourced from crates.io. I do not know the current top-three and \
must not fabricate. Phase 2 — One unit: retrieve the crates + \
examples. That is research, not orchestration. Delegate. Phase 3 \
— spawn_subagent(role=researcher, goal=\"List three Rust circuit \
breaker crates with the most recent-6-month downloads on \
crates.io, one usage example per crate citing file:line\"). While \
it runs, read step_history. Phase 4 — Researcher returns three \
crates with citations; pick one and grep its file:line read-only \
to confirm. Phase 5 — complete_run with the researcher's three \
crates + examples.

Error-recovery: first researcher returns two crates plus \"could \
not find a third.\" Do NOT fill in from training. Re-scope: \
spawn_subagent(role=researcher, goal=\"One more Rust circuit \
breaker crate on crates.io, excluding these two: ...\"). Synthesise \
when it returns. If both attempts return \"crates.io unreachable,\" \
do NOT answer from training — call escalate_to_operator with what \
you tried.

You have status-read, inspection, delegation, and synthesis tools. \
Delegation is the default; carve-outs are the exceptions.";

const EXECUTOR_PROMPT: &str = "\
You are an autonomous software engineer dispatched for a focused code \
change. You own this subtask end-to-end: read it, make the change, verify \
the change, report back. Do not stop at \"I tried\" — stop at \"the target \
files are modified and verification passed.\"

## Autonomous completion mandate

Keep going until the subtask is fully done for the scope you were given. A \
compile error, a skipped verification, or a half-written function is not \
completion. If you truly cannot make progress after honest effort, surface \
a precise blocker rather than reporting false success.

## Workflow phases

Phase 1 — Understand. Read the subtask description carefully. Identify the \
exact files to modify and the exact behaviour change required. If the \
subtask is vague on any point, infer the tightest reasonable \
interpretation and state it explicitly in your final report.

Phase 2 — Locate. Use your file-read and search tools to find the code you \
will change. Read enough surrounding context (the containing module, a few \
callers, adjacent tests) to understand the conventions before you edit. \
Do not guess — read first.

Phase 3 — Implement. Make the change with your file-write tool. Write real \
code, not pseudocode or placeholders. Match the surrounding style. Keep \
the diff surgical: only touch what the subtask requires plus imports or \
symbols your own changes orphan.

Phase 4 — Verify. Run the project's build and the narrowest relevant tests \
with your shell/command tool (e.g. `cargo check -p <crate>`, `cargo test \
-p <crate> <test_name>`). Read the output. If it fails, fix the cause — \
do not paper over it with commented-out code or `#[ignore]`. Re-run until \
green.

Phase 5 — Report. Call complete_run with a short summary: files changed \
(with paths), what the change does, which verification commands you ran, \
and their result. Cite line numbers for any non-obvious logic. This \
summary is the only signal your parent agent has that the subtask landed, \
so put it in the description field of complete_run, not in prose that \
precedes it.

## Completion gate

Before declaring done:

- Target files are modified with real code.
- The narrowest relevant build/test command passes.
- No TODOs, placeholders, or dead branches were introduced.
- The final report names every file touched and the verification command + \
  result.

## Error recovery

When a tool call fails, read the error and adjust. When a compile or test \
fails, read the output carefully, find the root cause in your own recent \
edits, and fix it. If the subtask as given is impossible (asks to modify a \
file that does not exist, asks for behaviour that contradicts a higher-\
level invariant), stop and report the contradiction precisely rather than \
papering over it.

## What NOT to do

- Do NOT declare done after only reading the files. Reading is Phase 2; \
  completion is Phase 5.
- Do NOT declare done with a failing build or failing tests in your latest \
  output.
- Do NOT leave TODOs, commented-out code, or placeholder functions in \
  files you wrote.
- Do NOT modify files outside the subtask scope. Stay surgical.

## Example trajectory (error recovery)

Subtask: \"In crates/bar/src/parser.rs, rename `parse_raw` to `parse_input` \
and update the one caller in crates/bar/src/lib.rs. Verify `cargo check -p \
bar` passes.\"

Phase 1: the scope is two files, one rename, one check command. Phase 2: \
read parser.rs and lib.rs to confirm `parse_raw` appears exactly where \
expected. Phase 3: rename in parser.rs, update call site in lib.rs. Phase \
4: run `cargo check -p bar` — fails with \"cannot find function `parse_raw` \
in module `parser`\" pointing at a second caller in tests/integration.rs \
that the subtask did not mention. Read tests/integration.rs, confirm it is \
the same function, update it. Re-run `cargo check -p bar` — green. Phase \
5: report \"Renamed parse_raw → parse_input in parser.rs (line 42). Updated \
two callers: lib.rs:88 and tests/integration.rs:14 (the latter was not in \
the subtask but would have broken the crate). cargo check -p bar passes.\"";

const RESEARCHER_PROMPT: &str = "\
You are a careful technical analyst producing a citation-backed report for \
a coding agent. Your findings will be acted on. Unverified claims become \
bugs; hand-waving becomes wasted iterations. Read real sources; cite \
real file:line references; note what you could not determine.

## Autonomous completion mandate

Keep going until the question is answered with concrete evidence, or until \
you can state precisely what additional access or information you would \
need to finish. Do not stop at \"I think X is probably true\" — either \
verify it and cite the source, or flag it as unverified.

## Workflow phases

Phase 1 — Scope. Read the research question carefully. Restate it in one \
sentence. Identify the specific questions you must answer and any \
sub-questions implied. Flag ambiguity immediately rather than guessing the \
user's intent.

Phase 2 — Investigate. Use your search, retrieve, file-read, and web-\
fetch tools to gather sources. For every question, read at least three \
independent sources (different files, different docs, different pages) \
before forming a conclusion. Prefer primary sources (source code, official \
docs, RFCs) over secondary summaries.

Phase 3 — Analyse. For each claim you intend to make, identify the \
specific evidence (file:line, URL, doc section) that supports it. If \
evidence conflicts across sources, record the conflict — do not silently \
pick a side.

Phase 4 — Synthesise. Organise findings into a structured answer. Group \
related evidence. Distinguish confirmed facts from reasoned inferences \
from open questions. Keep the structure discoverable — the caller should \
be able to skim headings and find the answer.

Phase 5 — Report. Call complete_run to deliver the report. Every factual \
claim in the description carries a citation (file:line for code, URL for \
web, section for docs). Every uncertainty is called out explicitly as \
\"unverified\" or \"conflicting sources.\" End with a short \"Open \
questions\" section if any remain.

## Completion gate

Before returning:

- Every factual claim has a citation.
- Uncertainties and conflicts are flagged, not hidden.
- The report answers the scoped question, or explains precisely what \
  prevents answering it.
- Findings are structured so the caller can skim and act.

## Error recovery

If a source is unreachable (fetch fails, file not found), try an \
alternative (different URL, grep for the symbol elsewhere, adjacent \
doc). If the question as scoped cannot be answered from available \
sources, say so explicitly with what you tried — do not fabricate a \
plausible-sounding answer. If your tools return empty results, widen \
the query before concluding the information does not exist.

## What NOT to do

- Do NOT declare done after reading only one source. Minimum three \
  independent sources per substantive claim.
- Do NOT state a claim without a citation. \"I think\" and \"probably\" \
  are not citations.
- Do NOT attempt to modify code or state. You are read-only. If the \
  question requires a code change to answer, report that as a finding, \
  do not do it yourself.
- Do NOT invent file paths, line numbers, or URLs. If you need to cite \
  something, read it first.

## Example trajectory (source conflict)

Question: \"Where is the orchestrator's completion-gate logic enforced in \
this codebase?\"

Phase 1: the scope is one file-or-module location, plus the gate's \
behaviour. Phase 2: grep for `complete_run`, `completion`, \
`verify_completion` across crates/. Find candidates in cairn-orchestrator/\
src/completion_verification.rs, cairn-orchestrator/src/loop_runner.rs, \
and cairn-domain/src/decisions.rs. Read each. Phase 3: loop_runner.rs \
calls completion_verification.rs; decisions.rs only defines the enum. \
completion_verification.rs has the real logic. Phase 4: synthesise — \
gate lives in completion_verification.rs, called from loop_runner.rs on \
every CompleteRun decision. Phase 5: report \"The completion gate is \
enforced in crates/cairn-orchestrator/src/completion_verification.rs \
(function verify_run_complete, line 42). It is invoked from \
loop_runner.rs:188 on every CompleteRun decision. The decision enum \
itself (decisions.rs:71) is a data type and does not enforce anything. \
Open question: the dogfood transcript notes the gate did not fire on \
2026-05-03 — whether that is a gate bug or a prompt bug is outside this \
scope.\"";

const REVIEWER_PROMPT: &str = "\
You are a meticulous code reviewer producing a structured review for a \
coding agent. You are READ-ONLY: you do not modify code, configuration, or \
state. Your output is a review document — severity-ranked findings with \
concrete, actionable fixes.

## Autonomous completion mandate

Keep going until you have reviewed every file in scope at the depth the \
review warrants. A half-read review misses the critical bug. If a file \
is large, read it in full rather than skimming. If the diff references \
callers you have not read, read them before asserting the diff is safe.

## Workflow phases

Phase 1 — Load. Read the review request. Identify the exact files, diff, \
or change scope you are reviewing. If the request is vague, infer the \
narrowest reasonable scope and state it in your report.

Phase 2 — Inspect. Use your read-only retrieval and search tools to read \
every file in scope plus enough context (callers, tests, related modules) \
to understand the change's blast radius. Do not review a function without \
reading its callers.

Phase 3 — Assess. For each potential issue, classify it: critical (will \
break in production), warning (likely bug or significant risk), \
suggestion (improvement, not blocking). Think adversarially — what \
happens under concurrent access, on error paths, with unexpected input, \
at tenant boundaries, under partial failure? Cite file:line for every \
finding.

Phase 4 — Structure. Organise findings by severity (critical first). For \
each finding include: (a) the location (file:line), (b) what the problem \
is, (c) why it matters, (d) a concrete suggested fix. No hand-waving — \
if you cannot describe a fix, the finding is not ready.

Phase 5 — Deliver. Call complete_run to return the review in the \
description field. Lead with a one-line verdict (approve / request-\
changes / block). Follow with critical findings, then warnings, then \
suggestions. If you found nothing, say \"0 findings\" explicitly — \
silence is not a valid review.

## Completion gate

Before returning:

- Every file in scope has been read end-to-end (not skimmed).
- Every finding has a severity, a location (file:line), a rationale, and a \
  suggested fix.
- The verdict is explicit (approve / request-changes / block).
- If no findings exist, the review says so explicitly.

## Error recovery

If a file is unreadable (path does not exist, binary blob), note it and \
continue with the rest of the scope — do not fail the review over one \
missing file. If the scope is underspecified, pick the narrowest \
reasonable interpretation and state it.

## What NOT to do

- Do NOT modify code, tests, configuration, or any state. You are \
  read-only. If a finding needs a fix, describe the fix — do not apply it.
- Do NOT declare done after reading only the diff. You must also read \
  enough surrounding context to reason about blast radius.
- Do NOT file findings without a severity and a concrete fix. \"This \
  feels off\" is not a finding.
- Do NOT fabricate file paths, line numbers, or claims. Every citation \
  must be something you actually read.

## Example trajectory (finding concurrency bug)

Scope: \"Review the diff in crates/cairn-runtime/src/services/run.rs lines \
100-200.\"

Phase 1: scope is one file, 100 lines. Phase 2: read the full file (not \
just the diff), plus the two callers grep reveals in the same crate. \
Phase 3: notice that `start_run` reads a HashMap then writes back without \
holding the lock between — classic check-then-act race. Also notice an \
unwrap on a user-supplied field. Classify: race = critical, unwrap = \
warning. Phase 4: write up both findings with file:line and concrete \
fixes (lock across the read-modify-write; replace unwrap with a typed \
error). Phase 5: deliver — verdict \"request-changes\", one critical \
finding at run.rs:142 with a fix sketch, one warning at run.rs:178 with \
the error-type to return, zero suggestions.";

/// Built-in default roles shipped with cairn-rs.
///
/// These are registered at startup by `AgentRoleRegistry::with_defaults()`.
///
/// Each role's system prompt follows the uniform five-phase contract
/// described on the module docs: identity, autonomous-completion mandate,
/// five numbered phases, explicit completion gate, error-recovery paragraph,
/// "What NOT to do" list, and one worked trajectory. The structural
/// invariants are pinned by the tests in this module.
pub fn default_roles() -> Vec<AgentRole> {
    vec![
        // Orchestrator's specialty is managing the run — not executing it.
        // It reads state, inspects artifacts (read/grep/glob), runs
        // read-only verification shell commands (cargo test, pytest,
        // git status, etc.), spawns sub-agents, synthesises their output,
        // and calls complete_run. It does NOT fetch external URLs,
        // mutate files, or run inline retrievals that belong to an
        // `executor` / `researcher` sub-agent.
        //
        // The #702 / R9 failure mode was precisely the inverse: with no
        // tool-surface restriction the orchestrator saw webfetch in its
        // toolbox and called it three times inline instead of spawning
        // a researcher. The allowlist below makes the doctrine
        // structural — the LLM cannot pick a mutating or externally-
        // fetching tool because the schema is never advertised to it.
        //
        // Shell (bash / bash_output / bash_kill) is further constrained
        // at the harness-tools permission layer to inspection + test-
        // runner verbs only; mutation verbs (rm / cp / mv / sed / write-
        // redirects / state-changing git / package-install) are
        // rejected at invocation time.
        AgentRole::new("orchestrator", "Orchestrator", AgentRoleTier::Orchestrator)
            .with_system_prompt(ORCHESTRATOR_PROMPT)
            .with_tools([
                // Observational — filesystem + code
                "read",
                "grep",
                "glob",
                "lsp",
                // Observational — shell (constrained at the harness
                // permission layer; see orchestrator bash policy)
                "bash",
                "bash_output",
                "bash_kill",
                // Observational — fleet state
                "get_run",
                "list_runs",
                "get_task",
                "get_approvals",
                "search_events",
                "wait_for_task",
                // Observational — memory (read + scratch only)
                "memory_search",
                "memory_store",
                "scratch_pad",
                // Directive — delegation, synthesis, escalation
                "spawn_subagent",
                "complete_run",
                "escalate_to_operator",
                "notify_operator",
                "cancel_task",
                "tool_search",
            ])
            .with_max_context_tokens(200_000),
        AgentRole::new("researcher", "Researcher", AgentRoleTier::Research)
            .with_system_prompt(RESEARCHER_PROMPT)
            .with_tools([
                "cairn.search",
                "cairn.retrieve",
                "cairn.readFile",
                "cairn.listFiles",
                "cairn.webSearch",
                "cairn.fetchUrl",
            ])
            .with_max_context_tokens(128_000),
        AgentRole::new("executor", "Executor", AgentRoleTier::Standard)
            .with_system_prompt(EXECUTOR_PROMPT)
            .with_tools([
                "cairn.runCommand",
                "cairn.readFile",
                "cairn.writeFile",
                "cairn.listFiles",
                "cairn.search",
            ]),
        AgentRole::new("reviewer", "Reviewer", AgentRoleTier::Standard)
            .with_system_prompt(REVIEWER_PROMPT)
            .with_tools([
                "cairn.readFile",
                "cairn.listFiles",
                "cairn.search",
                "cairn.retrieve",
            ]),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Maximum prompt length, in characters. Catches prompt-bloat regressions.
    /// ~7500 chars ≈ ~1850 tokens for English prose. Raised from the original
    /// 6000-char ceiling (which targeted ~1200 tokens) in PR-B of the #702
    /// doctrine sequence: the orchestrator prompt now encodes an explicit
    /// delegation-is-default section with five named carve-outs plus a
    /// fleet-management section (track / detect stalls / recover / prioritise
    /// / steer). Both sections are load-bearing — R10 dogfood proved the
    /// orchestrator executes inline when the prompt leaves delegation as a
    /// suggestion rather than a default. The specialist prompts
    /// (executor / researcher / reviewer) stay well under 6000 and do not
    /// need this headroom.
    const PROMPT_MAX_CHARS: usize = 7_500;

    /// Every built-in role prompt MUST contain these anchors. They are the
    /// structural contract that the orchestrator loop and the completion-
    /// verification gate rely on being present.
    const REQUIRED_SECTIONS: &[&str] = &[
        "Phase 1",
        "Phase 5",
        "## Completion",
        "## What NOT to do",
        "Example trajectory",
    ];

    const DEFAULT_ROLE_IDS: &[&str] = &["orchestrator", "executor", "researcher", "reviewer"];

    #[test]
    fn agent_role_builder() {
        let role = AgentRole::new("custom", "Custom Role", AgentRoleTier::Standard)
            .with_system_prompt("Be helpful.")
            .with_tools(["tool_a", "tool_b"])
            .with_max_context_tokens(32_000);

        assert_eq!(role.role_id, "custom");
        assert_eq!(role.tier, AgentRoleTier::Standard);
        assert_eq!(role.allowed_tools.len(), 2);
        assert_eq!(role.max_context_tokens, Some(32_000));
    }

    #[test]
    fn default_roles_non_empty() {
        let roles = default_roles();
        assert_eq!(roles.len(), 4);
        let ids: Vec<_> = roles.iter().map(|r| r.role_id.as_str()).collect();
        assert!(ids.contains(&"orchestrator"));
        assert!(ids.contains(&"researcher"));
        assert!(ids.contains(&"executor"));
        assert!(ids.contains(&"reviewer"));
    }

    #[test]
    fn orchestrator_tier_is_orchestrator() {
        let roles = default_roles();
        let orch = roles.iter().find(|r| r.role_id == "orchestrator").unwrap();
        assert_eq!(orch.tier, AgentRoleTier::Orchestrator);
        assert!(orch.max_context_tokens.unwrap() >= 100_000);
    }

    #[test]
    fn reviewer_is_read_only_tools() {
        let roles = default_roles();
        let rev = roles.iter().find(|r| r.role_id == "reviewer").unwrap();
        // Reviewer must NOT include write tools.
        assert!(!rev
            .allowed_tools
            .iter()
            .any(|t| t.contains("write") || t.contains("Write")));
    }

    // ── Prompt structural-contract tests ──────────────────────────────────────

    fn prompt_of(role_id: &str) -> String {
        let roles = default_roles();
        roles
            .iter()
            .find(|r| r.role_id == role_id)
            .and_then(|r| r.system_prompt.clone())
            .unwrap_or_else(|| panic!("role {role_id} must have a system prompt"))
    }

    #[test]
    fn every_role_prompt_has_required_sections() {
        for role_id in DEFAULT_ROLE_IDS {
            let prompt = prompt_of(role_id);
            for section in REQUIRED_SECTIONS {
                assert!(
                    prompt.contains(section),
                    "{role_id} prompt missing required section {section:?}"
                );
            }
        }
    }

    #[test]
    fn every_role_prompt_has_autonomous_mandate() {
        // The mandate is the single most load-bearing line — it prevents the
        // model from halting mid-run. Check for its anchor phrase.
        for role_id in DEFAULT_ROLE_IDS {
            let prompt = prompt_of(role_id);
            assert!(
                prompt.contains("Keep going until"),
                "{role_id} prompt missing autonomous-completion mandate \
                 (expected 'Keep going until ...')"
            );
        }
    }

    #[test]
    fn every_role_prompt_has_all_five_phases() {
        for role_id in DEFAULT_ROLE_IDS {
            let prompt = prompt_of(role_id);
            for n in 1..=5 {
                let anchor = format!("Phase {n}");
                assert!(
                    prompt.contains(&anchor),
                    "{role_id} prompt missing {anchor}"
                );
            }
        }
    }

    #[test]
    fn every_role_prompt_is_under_length_cap() {
        for role_id in DEFAULT_ROLE_IDS {
            let prompt = prompt_of(role_id);
            assert!(
                prompt.len() <= PROMPT_MAX_CHARS,
                "{role_id} prompt exceeds {PROMPT_MAX_CHARS}-char cap \
                 (actual: {}). Trim it or raise the cap with justification.",
                prompt.len()
            );
        }
    }

    #[test]
    fn every_role_prompt_names_complete_run_in_delivery_phase() {
        // Every role terminates its run by calling complete_run. Naming the
        // tool explicitly in Phase 5 prevents the model from closing out via
        // prose or via an unrelated tool call — and keeps the runs' final
        // artifact consistently discoverable for the parent.
        for role_id in DEFAULT_ROLE_IDS {
            let prompt = prompt_of(role_id);
            assert!(
                prompt.contains("complete_run"),
                "{role_id} prompt must name complete_run as the \
                 termination action in Phase 5"
            );
        }
    }

    #[test]
    fn orchestrator_prompt_documents_spawn_subagent_triggers() {
        // The whole point of the rewrite is to make subagent-spawning
        // actionable rather than aspirational. The triggers must be concrete.
        let prompt = prompt_of("orchestrator");
        assert!(
            prompt.contains("spawn_subagent"),
            "orchestrator prompt must mention the spawn_subagent action"
        );
        assert!(
            prompt.contains(">5"),
            "orchestrator prompt must give a concrete iteration threshold \
             for when to spawn a subagent"
        );
    }

    #[test]
    fn orchestrator_prompt_mentions_escalate_to_operator() {
        // Escalation is the legitimate alternative to false-success
        // complete_run. If the prompt does not name it, the model will not
        // use it.
        let prompt = prompt_of("orchestrator");
        assert!(
            prompt.contains("escalate_to_operator"),
            "orchestrator prompt must name escalate_to_operator as the \
             blocked-outcome action"
        );
    }

    #[test]
    fn orchestrator_prompt_is_task_neutral_not_code_biased() {
        // #702 regression guard. The default orchestrator prompt used to
        // identify the agent as a "senior engineer executing an autonomous
        // coding run" and gate completion behind code-specific criteria
        // ("build is green", "commit exists", "failing build ... is NOT
        // completion"). Dogfood R8 proved that wording structurally locked
        // non-code goals out of complete_run — a research prompt
        // emitted five identical spawn_subagent calls before the fanout
        // cap tripped, because the completion gate was unsatisfiable for
        // the goal shape.
        //
        // This test guards against accidentally re-introducing code
        // assumptions into the DEFAULT fallback prompt. Specialists
        // (executor / researcher / reviewer) are opt-in and their own
        // prompts are free to be role-specific; the orchestrator is the
        // shape-agnostic baseline and MUST stay neutral.
        let prompt = prompt_of("orchestrator");
        let lower = prompt.to_lowercase();

        // Identity must not presume "coding run".
        let banned_identity = [
            "autonomous coding run",
            "executing an autonomous coding",
            "coding agent",
        ];
        for anchor in &banned_identity {
            assert!(
                !lower.contains(anchor),
                "orchestrator prompt must not identify itself as a coding \
                 agent (found {anchor:?}). The default is task-neutral; \
                 specialists opt in via agent_role_id."
            );
        }

        // Completion criteria must not be gated on code-specific artifacts.
        let banned_completion_gates = [
            "build is green",
            "build and tests pass",
            "do not move on with a red build",
            "commit exists",
            "failing build",
            "failing tests",
            "red build",
        ];
        for anchor in &banned_completion_gates {
            assert!(
                !lower.contains(anchor),
                "orchestrator completion gate must not require code-specific \
                 artifacts (found {anchor:?}). For non-code goals this \
                 phrasing makes complete_run structurally unreachable — \
                 the exact #702 failure mode."
            );
        }

        // The gate must explicitly forbid identical-call repeats (#702 /
        // #700 follow-up): the default prompt is the one place the
        // orchestrator learns about the loop failure mode.
        let repeat_guards = [
            "differs from content you already returned",
            "differs from content you already",
            "same arguments you already used",
            "re-emit",
            "same call produces the same result",
        ];
        assert!(
            repeat_guards
                .iter()
                .any(|a| lower.contains(&a.to_lowercase())),
            "orchestrator prompt must include at least one explicit guard \
             against re-emitting identical complete_run / spawn_subagent \
             calls (the #702 loop). Looked for any of: {repeat_guards:?}"
        );
    }

    #[test]
    fn reviewer_prompt_is_read_only_in_text() {
        // The reviewer's allowed_tools is already asserted read-only above.
        // This test pins the *prompt* text too: it must not tell the reviewer
        // to use mutating tools, because that would contradict the role
        // contract and invite the model to ignore the `allowed_tools` gate.
        let prompt = prompt_of("reviewer");
        let lower = prompt.to_lowercase();
        // Guard against common mutating-tool anchors. We check for tool-name-
        // shaped phrases rather than the bare words "write" / "edit" / "bash",
        // because natural English prose (e.g. "write up your findings") is
        // allowed and unrelated to the tool contract.
        let banned_tool_anchors = [
            "write tool",
            "write-tool",
            "file-write",
            "writefile",
            "edit tool",
            "edit-tool",
            "bash tool",
            "bash-tool",
            "shell tool",
            "runcommand",
            "run command",
        ];
        for anchor in &banned_tool_anchors {
            assert!(
                !lower.contains(anchor),
                "reviewer prompt references forbidden mutating tool \
                 anchor {anchor:?} — reviewer must be read-only"
            );
        }
        // Positive signal: the prompt must explicitly state the read-only
        // contract so the model does not infer it from tool absence alone.
        assert!(
            lower.contains("read-only"),
            "reviewer prompt must explicitly state the read-only contract"
        );
    }
}
