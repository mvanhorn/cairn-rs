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
    /// Generic sub-agent role — no built-in workflow opinion. The parent
    /// owns the workflow design entirely via the goal text and optional
    /// `parent_context`. Used when the orchestrator has work that does
    /// not fit a registered specialty cleanly. See `GENERIC_PROMPT`.
    Generic,
}

/// Expected response shape — used by the gather/decide pipeline to pick
/// the right per-iteration footer instruction (#774). DirectAnswer roles
/// get the "answer NOW with complete_run" nudge; ProceduralArtifact roles
/// get a continuation-friendly footer that does not pressure early
/// termination.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ResponseShape {
    /// The role typically calls `complete_run` within 0–2 tool calls.
    /// Trivia, summaries, planning, decisions.
    #[default]
    DirectAnswer,
    /// The role makes N tool calls producing files / commits / PRs / build
    /// results. Footer must NOT bias toward early `complete_run` —
    /// completion is gated on the produced artifact existing.
    ProceduralArtifact,
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
    /// Short orchestrator-facing summary of the role's specialty —
    /// 1–3 sentences. Surfaced by the future `list_agents` /
    /// `agent_description` tools (#776) so the orchestrator can pick
    /// a delegate without reading the full system prompt.
    #[serde(default)]
    pub description: String,
    /// Specialty-overlay system prompt for this role.
    ///
    /// For non-orchestrator, non-generic roles the assembled prompt is
    /// `BASE_SUBAGENT_PROMPT + "\n\n" + system_prompt`. For the
    /// `orchestrator` role the field carries the full prompt verbatim
    /// (the orchestrator is the parent — sub-agent base does not
    /// apply). For the `generic` role the field carries a minimal
    /// content-neutral overlay; the parent supplies workflow guidance
    /// via the goal text (and optional `parent_context`).
    ///
    /// Use [`assembled_prompt_for`] at consumption time — never read
    /// this field directly for prompt rendering.
    pub system_prompt: Option<String>,
    /// Allowed tool IDs. Empty AND `forbid_all_tools == false` means
    /// no role restriction (every registered tool is available) —
    /// this is the default and matches the pre-RFC-031 semantic for
    /// roles that shipped without an allowlist. When
    /// `forbid_all_tools == true` the field is ignored and the role
    /// runs with an empty tool set. See RFC 031 §D3.
    ///
    /// Serialises as `"tools"` on the wire. `"allowed_tools"` is
    /// accepted on deserialisation via serde alias so pre-rename
    /// event-log entries and snapshots replay cleanly.
    #[serde(rename = "tools", alias = "allowed_tools", default)]
    pub tools: Vec<String>,
    /// Explicit "forbid every tool" flag. When `true` the role runs
    /// with an empty tool set regardless of `tools`. When `false`
    /// (default), `tools` controls the allowlist per the rules above.
    /// See RFC 031 §D3 for the interaction model.
    #[serde(default)]
    pub forbid_all_tools: bool,
    /// Hard context-window cap in tokens. `None` means use the model default.
    pub max_context_tokens: Option<u32>,
    pub tier: AgentRoleTier,
    /// Expected response shape. Drives the per-iteration footer in
    /// `build_user_message` (#774). Defaults to `DirectAnswer` for
    /// back-compat with builder paths that don't set it.
    #[serde(default)]
    pub response_shape: ResponseShape,
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
            description: String::new(),
            system_prompt: None,
            tools: Vec::new(),
            forbid_all_tools: false,
            max_context_tokens: None,
            tier,
            response_shape: ResponseShape::default(),
        }
    }

    pub fn with_system_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.system_prompt = Some(prompt.into());
        self
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    pub fn with_tools(mut self, tools: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.tools = tools.into_iter().map(|t| t.into()).collect();
        self
    }

    /// Set the `forbid_all_tools` flag. When `true`, the role runs
    /// with an empty tool set regardless of `tools`. See RFC 031 §D3.
    pub fn with_forbid_all_tools(mut self, forbid: bool) -> Self {
        self.forbid_all_tools = forbid;
        self
    }

    pub fn with_max_context_tokens(mut self, tokens: u32) -> Self {
        self.max_context_tokens = Some(tokens);
        self
    }

    pub fn with_response_shape(mut self, shape: ResponseShape) -> Self {
        self.response_shape = shape;
        self
    }
}

// ── Built-in role prompts ────────────────────────────────────────────────────
//
// Each `*_PROMPT` is the system prompt for one default role. They are kept as
// module-level constants (rather than inline in [`default_roles`]) so that
// unit tests can cheaply assert on their structure without rebuilding the
// registry, and so operators grepping for prompt text land in one place.
//
// ## Three-tier prompt model (#775)
//
// 1. **Orchestrator** — standalone. The parent. Its prompt has no base
//    prepended; identity is "I am the parent, I delegate." See
//    `ORCHESTRATOR_PROMPT`.
// 2. **Specialist sub-agents** (executor / researcher / reviewer) — assembled
//    as `BASE_SUBAGENT_PROMPT + "\n\n" + role.system_prompt`. The base
//    carries identity, sub-agent contract, autonomous mandate, completion-
//    gate framing, and meta-rules. The specialty overlay carries Phase 1–5
//    (workflow IS specialty), per-role completion-gate items, error-recovery
//    specifics, role-specific don'ts, and an example trajectory.
// 3. **Generic sub-agent** — assembled as `BASE_SUBAGENT_PROMPT + "\n\n" +
//    GENERIC_PROMPT`. Role for goals that do not fit a registered
//    specialty cleanly. Workflow content is intentionally minimal and
//    content-neutral; the parent owns the workflow design via the goal
//    text and optional `parent_context`.
//
// `assembled_prompt_for(role_id)` is the sole consumer-facing entry
// point that reads the registry and returns the rendered prompt. Never
// read `role.system_prompt` directly for prompt rendering.

/// Shared base prompt for non-orchestrator, non-generic sub-agents.
///
/// Carries the identity, sub-agent contract, autonomous-completion
/// mandate, completion-gate framing, and meta-rules that apply to every
/// dispatched sub-agent regardless of specialty. Pre-pended to the
/// role's `system_prompt` overlay by [`assembled_prompt_for`]. Also
/// pre-pended to the generic role's overlay.
///
/// The base does NOT contain `Phase 1`–`Phase 5` lines — those are
/// role-specific (workflow IS specialty). It does contain the
/// `Keep going until` mandate so every assembled sub-agent prompt
/// satisfies the `every_role_prompt_has_autonomous_mandate` test
/// without each overlay re-stating it.
const BASE_SUBAGENT_PROMPT: &str = "\
You are a sub-agent dispatched by a parent agent. The parent has handed \
you a focused goal in the user message. You own that goal end-to-end: \
read it, do the work the goal describes, verify the work, and report \
back. Do not stop at \"I tried\" — stop at the completion criteria your \
specialty defines below.

## Autonomous completion mandate

Keep going until the goal is fully done at the depth your specialty \
warrants. A half-done deliverable is not completion.

If you truly cannot make progress after honest effort, surface a \
precise blocker — do NOT report false success. Use the terminal verb \
that matches reality:
- **complete_run** when the deliverable exists and is verified.
- **escalate_to_operator** when operator input would unblock you.
- **fail_run** when you tried and cannot proceed, and no operator \
  action would change that. Put the concrete obstacle in `reason`.

Never call complete_run with a summary that says \"blocked\", \
\"partially complete\", or \"cannot provide\". That is false success. \
Call fail_run instead.

## Sub-agent contract

You are not the operator. You are not the orchestrator. You are a \
specialist working a focused sub-task. When you finish, your output \
goes back to the parent agent — write your final report for that \
audience: structured, terse, citation-backed, machine-parseable.

You do not spawn further sub-agents. If the goal is too large for one \
agent, surface that as a blocker; the parent re-plans.

You do not introspect this run. Your goal and step history are \
already in this prompt. Do not call get_run, list_runs, \
search_events, get_approvals, get_task, or wait_for_task on your own \
run id. Tools that are NOT in your allowed set will not appear in the \
tool list — do not invent names.

If the parent included a `## Parent context` section in the user \
message, treat it as binding direction (often a previous-attempt \
mistake to avoid, or a workspace path / credential to use). Read it \
before acting.";

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

You have NO filesystem, code-inspection, or shell tools — no read, \
grep, glob, lsp, or bash. Every unit of work that touches the \
workspace is a sub-agent spawn. The four carve-outs that ARE \
inherently orchestrator work:

1. **State-reads** — get_run / list_runs / get_task / search_events / \
   wait_for_task / get_approvals on sub-agents inside cairn.
2. **Synthesis** — assembling sub-agent outputs into complete_run.
3. **Planning and decomposition** — your role; no sub-agent for it.
4. **Cheap cross-output decisions** — picking which of two outputs \
   to use, or whether a third is needed.

If your action is NOT one of the four, delegate via \
spawn_subagent. Specialist routing:

- **status-checker** — read-only workspace + git inspection. \
  \"Does file X exist?\", \"Did the test pass?\", \"What does \
  `git status` say?\". Spawn this for ALL workspace verification, \
  including verifying a peer sub-agent's claim.
- **researcher** — citation-backed investigation (codebase or web).
- **executor** — code changes (write / edit / build / test).
- **reviewer** — structured audit of an artefact.

If no specialist role fits, call escalate_to_operator — do not do \
the work yourself, and do not invent a workaround that bypasses \
delegation. Never put an absolute sandbox path (e.g. \
`/tmp/cairn-runs/...`) in `goal` — the runtime injects each \
sub-agent's own sandbox. Repo-relative paths are fine.

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
pick the specialist role (status-checker, researcher, executor, \
reviewer). Spawn in parallel when units are independent. Spawn even \
a small unit if the work requires a tool you do not have. The only \
reason to keep work inline is the four carve-outs. Delegate any \
single unit that would take >5 of your own iterations.

Phase 3 — Dispatch and track. spawn_subagent each planned unit. \
While they run, read step_history each iteration; detect stalls; \
steer if needed. Do NOT pick up their work while they run — that \
wastes delegation.

Phase 4 — Verify. When a sub-agent reports done, verify their \
claims by spawning a status-checker with a precise question \
(\"Does `<path>` exist and contain `<expected>`?\", \"Did `cargo \
test -p <crate>` exit 0?\"). Trust but check, via delegation. You \
cannot inspect the workspace yourself.

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

## Error recovery and terminal verbs

Tool call fails: try a different state-read. Sub-agent empty / \
off-target / blocked: re-scope (narrower goal, different role, \
predecessor context). Three re-scopes max before terminating.

Terminal verbs — match the truth:

- **complete_run** — deliverable exists AND is verified.
- **escalate_to_operator** — operator input would unblock; resuming \
  IS feasible.
- **fail_run** — tried, cannot proceed, no operator action would \
  unblock. Put the obstacle in `reason`.

complete_run with a \"blocked\" / \"partially complete\" summary is \
false success. Use fail_run.

## What NOT to do

- Do NOT do the work yourself. You have no filesystem or shell \
  tools — there is no inline path. Workspace verification goes to \
  status-checker; everything else goes to executor / researcher / \
  reviewer.
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
crates with citations; spawn_subagent(role=status-checker, \
goal=\"Verify the file:line cited for crate X actually contains \
the claimed symbol\") to confirm one citation. Phase 5 — \
complete_run with the researcher's three crates + examples.

Error-recovery: first researcher returns two crates plus \"could \
not find a third.\" Do NOT fill in from training. Re-scope: \
spawn_subagent(role=researcher, goal=\"One more Rust circuit \
breaker crate on crates.io, excluding these two: ...\"). Synthesise \
when it returns. If both attempts return \"crates.io unreachable,\" \
do NOT answer from training — call escalate_to_operator with what \
you tried.

You have state-read, delegation, and synthesis tools — and only \
those. Anything that touches the workspace is a sub-agent spawn.";

/// Status-checker specialty overlay. Pre-pended with
/// [`BASE_SUBAGENT_PROMPT`] at consumption time via
/// [`assembled_prompt_for`].
///
/// Status-checker is the orchestrator's eyes on the workspace. It runs
/// read-only filesystem and git inspection — `read` / `grep` / `glob` /
/// `lsp` and inspection-only `bash` — and reports back a structured
/// findings document. It NEVER writes, edits, runs builds or tests
/// (executor's job), and never spawns its own sub-agents.
///
/// Why this role exists (#806): when the orchestrator's tool palette
/// included read/grep/glob/bash, the model's path of least resistance
/// on any "verify X" question was another inline bash call, even on
/// goals that explicitly needed file creation. Stripping the orchestrator
/// of those tools and routing verification through a dedicated
/// read-only role makes "delegate" the literal only path forward.
const STATUS_CHECKER_PROMPT: &str = "\
Your specialty is read-only inspection of the workspace and git \
state. The parent has handed you a yes/no or describe-this question \
about something on disk; your job is to look, report what is there, \
and stop. You never write, edit, build, install, or run anything \
that changes state.

## Workflow phases

Phase 1 — Understand. Read the parent's question. Identify the \
exact thing to check (a file path, a symbol, a test result, a git \
state) and the shape the parent expects back (yes/no, a value, a \
short summary).

Phase 2 — Plan the inspection. Pick the right tool for the question. \
Read the file when it is about content. Grep when it is about \
presence of a symbol or pattern. Glob when it is about which files \
match a shape. Bash for inspection-only commands (`git status`, \
`git log`, `cargo --version`, `ls`, `cat`, `wc -l`). Pick the \
narrowest tool that answers the question.

Phase 3 — Inspect. Run the tool. Read the output. NEVER use verbs \
that mutate (`rm`, `mv`, `cp`, `sed -i`, output redirection, \
package install, state-changing git, etc.) — the bash policy will \
reject them anyway.

Phase 4 — Cross-check. If the first inspection is ambiguous (a \
grep that returned 50 matches, a read that hit only the file's \
first 2000 lines), run one more narrowing tool call to disambiguate \
before reporting. Never guess; check.

Phase 5 — Report. Call complete_run with a structured finding: the \
question restated, the answer (yes/no/value/short-summary), and the \
evidence (file path + content excerpt, command + output excerpt, \
exit code). Cite specific paths and line numbers where applicable.

## Completion gate

Before complete_run:

- The parent's question is answered, in the shape they asked for.
- The answer is grounded in actual tool output captured in this run \
  — not inferred, not guessed, not generalised from training.
- Evidence is included verbatim (or a short representative excerpt) \
  so the parent can sanity-check.
- You ran zero mutating commands.

## Error recovery and terminal verbs

A read fails: try a related path or query. Bash tool refuses an \
attempted command (the bash policy correctly rejects mutation \
verbs): do not retry with a workaround that hides the mutation — \
report \"can only inspect, mutation requested\" as the answer and \
stop. The parent will respawn an executor if action is what they \
needed.

Three terminal verbs — use the one that matches the truth:

- **complete_run** — you answered the parent's question from actual \
  inspection (including when the honest answer is \"file not found\" \
  or \"symbol not present\").
- **escalate_to_operator** — you need operator input to proceed \
  (e.g. access to a path outside the sandbox).
- **fail_run** — the question is genuinely unanswerable from \
  inspection alone (e.g. \"will this build succeed?\" requires \
  running a build, which is mutating in cairn's sandbox model). \
  Put the concrete obstacle in `reason`. \"File not found\" is NOT \
  fail_run — that is a valid complete_run answer (the parent wanted \
  to know if the file was there).

## What NOT to do

- Do NOT write, edit, multi-edit, or use bash redirection (`>` / \
  `>>` / `tee`). Mutation is out of scope and the harness will \
  reject it anyway.
- Do NOT run `cargo build`, `cargo test`, `npm install`, `pip \
  install`, or any other side-effect-having command. Test execution \
  is the executor's job; you READ the result if it is already on \
  disk (e.g. a CI log file the operator pointed at), you do not \
  produce one.
- Do NOT spawn sub-agents. Per the base contract, sub-agents do not \
  spawn further sub-agents; status-checker is no exception.
- Do NOT answer from training. If you cannot inspect the answer, \
  say so.
- Do NOT pad with prose. The parent wants a tight finding, not an \
  essay.

## Example trajectory

Goal: \"Confirm `crates/cairn-domain/src/agent_roles.rs` defines a \
role with `role_id = \\\"status-checker\\\"`.\"

Phase 1 — Question: does this exact file contain a role with id \
status-checker. Answer shape: yes/no plus the matching line. Phase \
2 — Plan: `grep -n` is the right tool (presence of a literal in a \
known file). Phase 3 — Inspect: \
`grep -n 'status-checker' crates/cairn-domain/src/agent_roles.rs`. \
Output shows two matches — line 1098 (a role-id literal) and line \
1310 (a comment). Phase 4 — Cross-check: the line 1098 literal is \
the AgentRole declaration; line 1310 is just a comment in another \
section. Single canonical declaration. Phase 5 — Report: \
complete_run final_answer = \"Yes. \
crates/cairn-domain/src/agent_roles.rs:1098 declares \
AgentRole::new(\\\"status-checker\\\", ...). One additional comment \
match at line 1310 referencing the role; no other definition.\"

Error-recovery: if the file does not exist, the read returns ENOENT. \
Do not guess at a nearby filename — report \"file not found at \
<path>\" and stop. Parent re-scopes if needed.";

/// Executor specialty overlay. Pre-pended with [`BASE_SUBAGENT_PROMPT`]
/// at consumption time via [`assembled_prompt_for`]. Identity opener,
/// autonomous-completion mandate, and sub-agent contract live in the
/// base; this overlay carries the workflow phases, completion-gate
/// items, error recovery, role-specific don'ts, and example trajectory.
const EXECUTOR_PROMPT: &str = "\
Your specialty is focused code changes — write real code, run the build, \
fix failures, report back. The base above tells you the contract; this \
section tells you the workflow.

## Workflow phases

Phase 1 — Understand. Read the goal carefully. Identify the exact files \
to modify and the exact behaviour change required. If the goal is vague \
on any point, infer the tightest reasonable interpretation and state it \
explicitly in your final report.

Phase 2 — Locate. Use your file-read and search tools to find the code \
you will change. Read enough surrounding context (the containing module, \
a few callers, adjacent tests) to understand the conventions before you \
edit. Do not guess — read first.

Phase 3 — Implement. Make the change with your file-write tool. Write \
real code, not pseudocode or placeholders. Match the surrounding style. \
Keep the diff surgical: only touch what the goal requires plus imports \
or symbols your own changes orphan.

Phase 4 — Verify. Run the project's build and the narrowest relevant \
tests with your shell tool (e.g. `cargo check -p <crate>`, `cargo test \
-p <crate> <test_name>`). Read the output. If it fails, fix the cause \
— do not paper over it with commented-out code or `#[ignore]`. Re-run \
until green.

Phase 5 — Report. Call complete_run with a short summary: files changed \
(with paths), what the change does, which verification commands you ran, \
and their result. Cite line numbers for any non-obvious logic. Put the \
summary in the description / final_answer field of complete_run, not in \
prose that precedes it.

## Completion gate

Before declaring done:

- Target files are modified with real code.
- The narrowest relevant build/test command passes.
- No TODOs, placeholders, or dead branches were introduced.
- The final report names every file touched and the verification command \
  + result.

## Error recovery and terminal verbs

When a tool call fails, read the error and adjust. When a compile or \
test fails, read the output carefully, find the root cause in your own \
recent edits, and fix it.

Three terminal verbs — use the one that matches the truth:

- **complete_run** — target files modified, build and tests pass, \
  nothing left to do. This is a success signal.
- **escalate_to_operator** — you are blocked by something the operator \
  can unblock immediately (missing credential for a remote, approval \
  for a destructive action, a clarification on ambiguous intent).
- **fail_run** — the goal as given is impossible or a required \
  precondition is missing (a file the goal says to modify does not \
  exist; a dependency that must land first has not landed; the \
  behaviour requested contradicts a higher-level invariant). Put the \
  concrete obstacle in `reason`.

Never call complete_run with a summary that says \"blocked\" or \
\"partially complete\". Use fail_run instead.

## What NOT to do

- Do NOT declare done after only reading the files. Reading is Phase 2; \
  completion is Phase 5.
- Do NOT declare done with a failing build or failing tests in your \
  latest output.
- Do NOT leave TODOs, commented-out code, or placeholder functions in \
  files you wrote.
- Do NOT modify files outside the goal's scope. Stay surgical.
- Do NOT call complete_run when blocked. Call fail_run — operator \
  dashboards depend on the terminal verb matching reality.

## Example trajectory (error recovery)

Goal: \"In crates/bar/src/parser.rs, rename `parse_raw` to `parse_input` \
and update the one caller in crates/bar/src/lib.rs. Verify `cargo check \
-p bar` passes.\"

Phase 1: the scope is two files, one rename, one check command. Phase 2: \
read parser.rs and lib.rs to confirm `parse_raw` appears exactly where \
expected. Phase 3: rename in parser.rs, update call site in lib.rs. \
Phase 4: run `cargo check -p bar` — fails with \"cannot find function \
`parse_raw` in module `parser`\" pointing at a second caller in \
tests/integration.rs that the goal did not mention. Read \
tests/integration.rs, confirm it is the same function, update it. \
Re-run `cargo check -p bar` — green. Phase 5: report \"Renamed \
parse_raw → parse_input in parser.rs (line 42). Updated two callers: \
lib.rs:88 and tests/integration.rs:14 (the latter was not in the goal \
but would have broken the crate). cargo check -p bar passes.\"";

/// Researcher specialty overlay. Pre-pended with
/// [`BASE_SUBAGENT_PROMPT`]. Carries the citation-backed-research
/// workflow + research-specific don'ts.
const RESEARCHER_PROMPT: &str = "\
Your specialty is citation-backed research — you are a careful \
technical analyst producing a report another agent will act on. \
Unverified claims become bugs; hand-waving becomes wasted \
iterations. Read real sources; cite real file:line references; note \
what you could not determine.

## Workflow phases

Phase 1 — Scope. Read the research question carefully. Restate it in \
one sentence. Identify the specific questions you must answer and any \
sub-questions implied. Flag ambiguity immediately rather than guessing \
the user's intent.

Phase 2 — Investigate. Use your search, retrieve, file-read, and \
web-fetch tools to gather sources. For every question, read at least \
three independent sources (different files, different docs, different \
pages) before forming a conclusion. Prefer primary sources (source \
code, official docs, RFCs) over secondary summaries.

Phase 3 — Analyse. For each claim you intend to make, identify the \
specific evidence (file:line, URL, doc section) that supports it. If \
evidence conflicts across sources, record the conflict — do not \
silently pick a side.

Phase 4 — Synthesise. Organise findings into a structured answer. \
Group related evidence. Distinguish confirmed facts from reasoned \
inferences from open questions. Keep the structure discoverable — the \
caller should be able to skim headings and find the answer.

Phase 5 — Report. Call complete_run to deliver the report. Every \
factual claim in the description / final_answer field carries a \
citation (file:line for code, URL for web, section for docs). Every \
uncertainty is called out explicitly as \"unverified\" or \"conflicting \
sources.\" End with a short \"Open questions\" section if any remain.

## Completion gate

Before returning:

- Every factual claim has a citation.
- Uncertainties and conflicts are flagged, not hidden.
- The report answers the scoped question, or explains precisely what \
  prevents answering it.
- Findings are structured so the caller can skim and act.

## Error recovery and terminal verbs

If a source is unreachable (fetch fails, file not found), try an \
alternative (different URL, grep for the symbol elsewhere, adjacent \
doc). Widen the query before concluding information does not exist.

Three terminal verbs — use the one that matches the truth:

- **complete_run** — you answered the scoped question with \
  citations.
- **escalate_to_operator** — you need specific operator input to \
  proceed (e.g. a login for a private source) and they can provide \
  it.
- **fail_run** — the question as scoped cannot be answered from \
  available sources and no reasonable operator action would fix \
  that (the source does not exist; the claim is unverifiable; the \
  question is internally contradictory). Put the concrete obstacle \
  in `reason`. A report with \"unverified\" / \"conflicting sources\" \
  sections is a complete_run, not a fail_run — only use fail_run \
  when you cannot produce a useful report at all.

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
- Do NOT fabricate a plausible-sounding answer when sources do not \
  support one. Call fail_run with what you tried.

## Example trajectory (source conflict)

Question: \"Where is the orchestrator's completion-gate logic enforced \
in this codebase?\"

Phase 1: the scope is one file-or-module location, plus the gate's \
behaviour. Phase 2: grep for `complete_run`, `completion`, \
`verify_completion` across crates/. Find candidates in \
cairn-orchestrator/src/completion_verification.rs, \
cairn-orchestrator/src/loop_runner.rs, and cairn-domain/src/decisions.rs. \
Read each. Phase 3: loop_runner.rs calls completion_verification.rs; \
decisions.rs only defines the enum. completion_verification.rs has the \
real logic. Phase 4: synthesise — gate lives in \
completion_verification.rs, called from loop_runner.rs on every \
CompleteRun decision. Phase 5: report \"The completion gate is \
enforced in crates/cairn-orchestrator/src/completion_verification.rs \
(function verify_run_complete, line 42). It is invoked from \
loop_runner.rs:188 on every CompleteRun decision. The decision enum \
itself (decisions.rs:71) is a data type and does not enforce anything. \
Open question: the dogfood transcript notes the gate did not fire on \
2026-05-03 — whether that is a gate bug or a prompt bug is outside \
this scope.\"";

/// Reviewer specialty overlay. Pre-pended with [`BASE_SUBAGENT_PROMPT`].
/// Carries the read-only review workflow + the explicit read-only
/// contract that the `reviewer_prompt_is_read_only_in_text` test pins.
const REVIEWER_PROMPT: &str = "\
Your specialty is structured code review. You are READ-ONLY: you do not \
modify code, configuration, or state. Your output is a review document \
— severity-ranked findings with concrete, actionable fixes.

## Workflow phases

Phase 1 — Load. Read the review request. Identify the exact files, \
diff, or change scope you are reviewing. If the request is vague, \
infer the narrowest reasonable scope and state it in your report.

Phase 2 — Inspect. Use your read-only retrieval and search tools to \
read every file in scope plus enough context (callers, tests, related \
modules) to understand the change's blast radius. Do not review a \
function without reading its callers.

Phase 3 — Assess. For each potential issue, classify it: critical \
(will break in production), warning (likely bug or significant risk), \
suggestion (improvement, not blocking). Think adversarially — what \
happens under concurrent access, on error paths, with unexpected \
input, at tenant boundaries, under partial failure? Cite file:line \
for every finding.

Phase 4 — Structure. Organise findings by severity (critical first). \
For each finding include: (a) the location (file:line), (b) what the \
problem is, (c) why it matters, (d) a concrete suggested fix. No \
hand-waving — if you cannot describe a fix, the finding is not ready.

Phase 5 — Deliver. Call complete_run to return the review. Lead with \
a one-line verdict (approve / request-changes / block). Follow with \
critical findings, then warnings, then suggestions. If you found \
nothing, say \"0 findings\" explicitly — silence is not a valid review.

## Completion gate

Before returning:

- Every file in scope has been read end-to-end (not skimmed).
- Every finding has a severity, a location (file:line), a rationale, \
  and a suggested fix.
- The verdict is explicit (approve / request-changes / block).
- If no findings exist, the review says so explicitly.

## Error recovery and terminal verbs

If a file is unreadable (path does not exist, binary blob), note it \
and continue with the rest of the scope — do not fail the review \
over one missing file. If the scope is underspecified, pick the \
narrowest reasonable interpretation and state it.

Three terminal verbs — use the one that matches the truth:

- **complete_run** — you produced a review document with verdict + \
  findings (even a \"0 findings\" review is a complete_run).
- **escalate_to_operator** — you need operator input to proceed \
  (e.g. access to a restricted source, clarification on scope that \
  would materially change the review).
- **fail_run** — no files in scope are readable, the requested scope \
  is nonsensical, or you cannot produce a review at all. Put the \
  concrete obstacle in `reason`. A review with caveats is still a \
  complete_run — only use fail_run when you cannot deliver a useful \
  review document.

## What NOT to do

- Do NOT modify code, tests, configuration, or any state. You are \
  read-only. If a finding needs a fix, describe the fix — do not \
  apply it.
- Do NOT declare done after reading only the diff. You must also read \
  enough surrounding context to reason about blast radius.
- Do NOT file findings without a severity and a concrete fix. \"This \
  feels off\" is not a finding.
- Do NOT fabricate file paths, line numbers, or claims. Every citation \
  must be something you actually read.

## Example trajectory (finding concurrency bug)

Scope: \"Review the diff in crates/cairn-runtime/src/services/run.rs \
lines 100-200.\"

Phase 1: scope is one file, 100 lines. Phase 2: read the full file \
(not just the diff), plus the two callers grep reveals in the same \
crate. Phase 3: notice that `start_run` reads a HashMap then writes \
back without holding the lock between — classic check-then-act race. \
Also notice an unwrap on a user-supplied field. Classify: race = \
critical, unwrap = warning. Phase 4: write up both findings with \
file:line and concrete fixes (lock across the read-modify-write; \
replace unwrap with a typed error). Phase 5: deliver — verdict \
\"request-changes\", one critical finding at run.rs:142 with a fix \
sketch, one warning at run.rs:178 with the error-type to return, zero \
suggestions.";

/// Generic specialty overlay. Pre-pended with [`BASE_SUBAGENT_PROMPT`].
///
/// The generic role has no built-in workflow opinion — its workflow
/// is whatever the parent's goal text describes. The overlay is
/// intentionally minimal but satisfies the test contract
/// (REQUIRED_SECTIONS: Phase 1, Phase 5, ## Completion, ## What NOT
/// to do, Example trajectory; ALL_FIVE_PHASES; complete_run named).
///
/// Use this role when no registered specialty (executor, researcher,
/// reviewer) cleanly fits the goal — typically one-off / mixed /
/// exploratory work where the parent owns the workflow design.
const GENERIC_PROMPT: &str = "\
Your specialty is doing whatever the parent's goal asks. You have no \
specialty bias — read the goal, figure out the workflow it implies, \
execute it. The base above tells you the contract; use the phases \
below as a generic skeleton that adapts to whatever shape the goal \
takes.

## Workflow phases

Phase 1 — Understand. Read the goal end-to-end. State your \
interpretation explicitly so the parent can correct it if you misread \
— include this restatement in your final report.

Phase 2 — Plan. Pick the smallest sequence of tool calls that \
plausibly satisfies the goal. If the goal already enumerates steps, \
follow them in order; if not, infer them from the goal's verbs (read, \
write, run, summarise, …).

Phase 3 — Act. Execute the plan one tool call at a time. Read each \
result before deciding the next call — do not pre-batch a long \
sequence of speculative calls.

Phase 4 — Verify. Confirm the goal's success criteria are met. If \
the goal asks you to write a file, the file must exist on disk. If \
it asks you to answer a question, the answer must come from a source \
you actually read.

Phase 5 — Report. Call complete_run with a concise summary of what \
you did, what you produced, and any uncertainty. Put the summary in \
the description / final_answer field, not in prose that precedes it.

## Completion gate

Before declaring done:

- The goal's stated success criteria are demonstrably met (file on \
  disk, answer cited from a real source, command exit 0, etc.).
- Your report names every artifact you produced and how the parent \
  can verify it.
- If the goal turned out to be impossible or the criteria \
  unsatisfiable, the report explains precisely why instead of \
  claiming false success.

## Error recovery

When a tool call fails, read the error and adjust the plan — do not \
retry the same failing call. If the goal becomes unreachable as you \
learn more (a file does not exist, a command is not installed, an \
external service is down), surface that as a blocker in the final \
report rather than fabricating a plausible-sounding answer.

## What NOT to do

- Do NOT declare done before the goal's success criteria are met.
- Do NOT fabricate file paths, command output, or external responses. \
  If you did not see it, do not cite it.
- Do NOT add scope the goal did not ask for. Stay surgical.
- Do NOT ignore a `## Parent context` section if one is present in \
  the user message — it usually carries a previous-attempt mistake \
  to avoid.

## Example trajectory

Goal: \"Read /tmp/foo.txt, count occurrences of the word `cairn`, \
write the count to /tmp/foo-cairn-count.txt, and report the count.\"

Phase 1: scope is one read, one count, one write. Phase 2: plan is \
read → grep / count → write → report. Phase 3: read /tmp/foo.txt \
(124 lines, contains \"cairn\" 17 times via grep -c). Phase 4: write \
\"17\" to /tmp/foo-cairn-count.txt; verify with read — file exists, \
content is \"17\". Phase 5: complete_run with \"Counted 17 \
occurrences of `cairn` in /tmp/foo.txt; wrote count to \
/tmp/foo-cairn-count.txt. Verified the output file exists with \
content `17`.\"";

/// Assemble the rendered system prompt for a given role id.
///
/// This is the sole entry point for prompt rendering. Never read
/// `AgentRole.system_prompt` directly — it carries the role's
/// specialty overlay only (or, for `orchestrator`, the full prompt).
///
/// Assembly rules (#775):
/// - `orchestrator`: returns `system_prompt` verbatim. The orchestrator
///   is the parent; the sub-agent base does not apply.
/// - any other registered role: returns
///   `BASE_SUBAGENT_PROMPT + "\n\n" + system_prompt`. Carries the
///   universal sub-agent identity + mandate + contract first, then
///   the role's specialty overlay.
/// - unknown role id: falls back to the assembled prompt for the
///   `generic` role. Pre-#775 this returned a 3-line string that did
///   not satisfy any test contract; the generic-fallback shape ensures
///   any code path that lands on an unregistered role still gets a
///   structurally-complete prompt.
pub fn assembled_prompt_for(role_id: &str) -> String {
    let roles = default_roles();
    if let Some(role) = roles.iter().find(|r| r.role_id == role_id) {
        return assembled_prompt_for_role(role);
    }
    // Fallback: render the generic role. `default_roles` always
    // contains a generic entry — if that contract regresses the
    // unwrap below makes the failure loud.
    let generic = roles
        .iter()
        .find(|r| r.role_id == "generic")
        .expect("default_roles must contain a `generic` role per #775");
    assembled_prompt_for_role(generic)
}

/// Resolve the `ResponseShape` for a role id without allocating a
/// fresh `Vec<AgentRole>` (cf. `default_roles()` which clones every
/// role's multi-KB system prompt). Used per-iteration by
/// `cairn-orchestrator::build_user_message` (#774) to pick the right
/// footer; called on every DECIDE turn, so the cheap lookup matters.
///
/// Unknown role ids fall back to the registered `generic` role's
/// shape. This matches `assembled_prompt_for`'s fallback contract —
/// keeping the two paths' fallback semantics aligned avoids the
/// pathology where a mis-spelled role gets a generic system prompt
/// but a DirectAnswer footer (or vice versa).
///
/// SOURCE OF TRUTH: this table mirrors the `response_shape` field on
/// each role in `default_roles()`. A `default_roles_response_shapes_match_table`
/// test in this module pins the contract — if either side drifts the
/// test fails loudly.
pub fn response_shape_for(role_id: &str) -> ResponseShape {
    // Static-allocation lookup keyed on role_id. If a future role is
    // added to `default_roles()`, add it here too — the contract test
    // catches the drift.
    match role_id {
        "orchestrator" => ResponseShape::DirectAnswer,
        "executor" | "researcher" | "reviewer" | "generic" | "status-checker" => {
            ResponseShape::ProceduralArtifact
        }
        _ => ResponseShape::ProceduralArtifact, // unknown → generic-shaped
    }
}

/// Static fallback lookup for `AgentRoleTier` keyed on `role_id`. Used
/// when the full `AgentRole` record is not available (legacy callers,
/// pre-RFC-031 paths, or cross-crate helpers). Prefer reading
/// `AgentRole::tier` directly when you hold the record.
///
/// SOURCE OF TRUTH: this table mirrors the `tier` field on each role in
/// `default_roles()`. The `default_roles_tiers_match_table` test in
/// this module pins the contract — if either side drifts, the test
/// fails.
///
/// Added in #844 PR-1: the prompt renderer drops `workspace_path` from
/// the orchestrator's `## Run state` section, and the check needed a
/// structural discriminator (tier) rather than a string match on
/// `role_id == "orchestrator"` — the latter silently misfires on
/// custom orchestrator-tier roles from RFC-031's operator registry.
pub fn tier_for(role_id: &str) -> AgentRoleTier {
    match role_id {
        "orchestrator" => AgentRoleTier::Orchestrator,
        "researcher" => AgentRoleTier::Research,
        "generic" => AgentRoleTier::Generic,
        "executor" | "reviewer" | "status-checker" => AgentRoleTier::Standard,
        _ => AgentRoleTier::Standard, // unknown → safest sub-agent tier
    }
}

/// Assemble the rendered prompt from a role record. Public for callers
/// that already have an `AgentRole` in hand (e.g. a future operator-
/// extensible registry).
pub fn assembled_prompt_for_role(role: &AgentRole) -> String {
    let overlay = role.system_prompt.as_deref().unwrap_or("");
    match role.tier {
        AgentRoleTier::Orchestrator => overlay.to_owned(),
        _ => {
            if overlay.is_empty() {
                BASE_SUBAGENT_PROMPT.to_owned()
            } else {
                format!("{BASE_SUBAGENT_PROMPT}\n\n{overlay}")
            }
        }
    }
}

/// Built-in default roles shipped with cairn-rs.
///
/// These are registered at startup by `AgentRoleRegistry::with_defaults()`.
///
/// Each non-orchestrator role's `system_prompt` field carries the
/// specialty overlay only. Use [`assembled_prompt_for`] /
/// [`assembled_prompt_for_role`] to render the full prompt — direct
/// reads of `system_prompt` will see specialty content without the
/// shared `BASE_SUBAGENT_PROMPT`.
pub fn default_roles() -> Vec<AgentRole> {
    vec![
        // Orchestrator's specialty is managing the run — not executing it.
        // It reads cairn-internal fleet state, spawns sub-agents,
        // synthesises their output, and calls complete_run. It has NO
        // filesystem, code-inspection, or shell tools: every workspace
        // verification (read/grep/glob/lsp, bash) goes through a
        // status-checker sub-agent spawn.
        //
        // History — why the surface is THIS narrow (#702, #806):
        //
        //   #702 / R9: with no tool-surface restriction the orchestrator
        //   saw webfetch in its toolbox and called it inline three times
        //   instead of spawning a researcher. The first allowlist
        //   (PR #705/#706/#708) removed mutating + externally-fetching
        //   tools, which closed that hole.
        //
        //   #806 / R22: with read/grep/glob/bash still in the allowlist
        //   the orchestrator kept doing the work itself via
        //   "verification" bash calls — 11 consecutive `find` / `ls` /
        //   `git status` invocations on a goal that needed file
        //   creation, never once spawning a sub-agent. The model knew
        //   it should delegate (every iteration's reasoning said so);
        //   the prompt said so. But as long as bash was in the tool
        //   palette, the LLM's path of least resistance was another
        //   bash call. The fix is structural: drop the inline-act tools
        //   and route every workspace-touching question through a
        //   status-checker spawn.
        //
        // What stays:
        //
        //   - get_run / list_runs / get_task / search_events /
        //     wait_for_task / get_approvals (cairn-internal fleet
        //     state — cheap, cairn-owned, not "doing the work")
        //   - list_agents / agent_description (planning aids — read the
        //     registry to pick a delegate)
        //   - memory_search / memory_store / scratch_pad (planning +
        //     synthesis substrate; no workspace touch)
        //   - spawn_subagent / complete_run / escalate_to_operator /
        //     notify_operator / cancel_task (the directive set)
        //   - tool_search (discover specialist tools to add to a child
        //     spawn's parent_context)
        //
        // What is gone (delegate to status-checker / researcher /
        // executor / reviewer instead):
        //
        //   - read, grep, glob, lsp, bash, bash_output, bash_kill
        //
        // The doctrine fence is now structural in both directions:
        // mutation isn't reachable, and inline read-only "verification"
        // isn't either.
        AgentRole::new("orchestrator", "Orchestrator", AgentRoleTier::Orchestrator)
            .with_description(
                "Plans, decomposes, and dispatches goals to specialist sub-agents. \
                 Reads cairn fleet state, synthesises sub-agent output, and delivers \
                 the final answer to the operator. Has no filesystem or shell tools; \
                 workspace verification spawns a status-checker.",
            )
            .with_response_shape(ResponseShape::DirectAnswer)
            .with_system_prompt(ORCHESTRATOR_PROMPT)
            .with_tools([
                // Observational — fleet state (cairn-internal, not workspace)
                "get_run",
                "list_runs",
                "get_task",
                "get_approvals",
                "search_events",
                "wait_for_task",
                // Observational — agent registry introspection (#776).
                // `list_agents` enumerates registered roles; the
                // orchestrator uses it during planning to choose the
                // right delegate. `agent_description` returns the
                // full record (allowed tools + response_shape) for
                // a single role when more detail is needed before
                // spawning. Both read-only.
                "list_agents",
                "agent_description",
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
        // #806: status-checker is the orchestrator's eyes on the
        // workspace. Read-only filesystem + git inspection only;
        // anything that mutates is rejected. Spawned by the
        // orchestrator for every "verify X" question — both
        // sub-agent claim verification and operator-initiated checks.
        // Its tool palette is intentionally narrower than reviewer's:
        // reviewer audits artefacts and writes structured findings;
        // status-checker answers concrete yes/no/value questions and
        // reports the evidence.
        AgentRole::new("status-checker", "Status Checker", AgentRoleTier::Standard)
            .with_description(
                "Read-only inspection of the workspace and git state. Answers \
                 concrete yes/no/value questions about files, symbols, \
                 build/test results already on disk, git status. Returns a \
                 tight evidence-backed finding. Never writes, never builds, \
                 never spawns. Best for: verifying a peer sub-agent's claim, \
                 checking workspace state before dispatching an executor.",
            )
            .with_response_shape(ResponseShape::ProceduralArtifact)
            .with_system_prompt(STATUS_CHECKER_PROMPT)
            .with_tools([
                // Read-only inspection
                "read",
                "grep",
                "glob",
                "lsp",
                // Inspection-only shell. The harness-tools bash policy
                // already rejects mutation verbs (rm/cp/mv/sed -i,
                // output redirection, package install, state-changing
                // git) at invocation time — status-checker inherits
                // that fence and the prompt reinforces it.
                "bash",
                "bash_output",
                "bash_kill",
                // Termination + escalation
                "complete_run",
                "escalate_to_operator",
            ]),
        // #707: researcher needs REAL registered tool names to actually
        // research. The prior `cairn.*` placeholders (cairn.search /
        // cairn.retrieve / cairn.webSearch / cairn.fetchUrl) are NOT
        // registered in the builtin tool registry, so the researcher
        // got an empty effective surface and fell back to training-data
        // answers with honest disclaimers ("Due to tool limitations...
        // unable to provide citations"). Researcher's job per its
        // prompt is citation-backed retrieval — these tools let it do
        // that.
        AgentRole::new("researcher", "Researcher", AgentRoleTier::Research)
            .with_description(
                "Reads, cites, summarises. Returns a citation-backed report. Best \
                 for: looking up domain knowledge, scanning the codebase for prior \
                 patterns, retrieving from web docs / RFCs / source. Read-only — \
                 does not modify code or state.",
            )
            .with_response_shape(ResponseShape::ProceduralArtifact)
            .with_system_prompt(RESEARCHER_PROMPT)
            .with_tools([
                // Filesystem / codebase retrieval
                "read",
                "grep",
                "glob",
                "lsp",
                // Memory + graph retrieval (prior project context)
                "memory_search",
                "graph_query",
                // External retrieval (web docs, http APIs)
                "webfetch",
                "http_request",
                // Utility — structured data extraction + quick math
                // + one-shot summarisation during synthesis
                "json_extract",
                "calculate",
                "summarize_text",
                // Scratchpad for intermediate synthesis
                "scratch_pad",
                // Termination + escalation
                "complete_run",
                "escalate_to_operator",
                // Tool discovery when the listed set is insufficient
                "tool_search",
            ])
            .with_max_context_tokens(128_000),
        // #707: executor needs REAL registered tool names. Previously
        // listed `cairn.runCommand` / `cairn.writeFile` etc. (not
        // registered). Executor's job is focused code changes — it
        // needs the full read-write-verify toolkit.
        AgentRole::new("executor", "Executor", AgentRoleTier::Standard)
            .with_description(
                "Writes code, runs builds and tests, fixes failures. Best for: \
                 focused single-file or single-feature changes that need to \
                 compile and pass tests. Has full read-write-verify toolkit.",
            )
            .with_response_shape(ResponseShape::ProceduralArtifact)
            .with_system_prompt(EXECUTOR_PROMPT)
            .with_tools([
                // Inspection before editing (Phase 2 Locate in executor prompt)
                "read",
                "grep",
                "glob",
                "lsp",
                // Context-retrieval during Locate — previously-learned
                // project patterns + entity relationships (callers /
                // callees) inform surgical code changes
                "memory_search",
                "graph_query",
                // Mutation (Phase 3 Implement)
                "write",
                "edit",
                "multiedit",
                // Verification (Phase 4 Verify — run build/tests)
                "bash",
                "bash_output",
                "bash_kill",
                // Utility — parsing JSON from build/test output
                "json_extract",
                // Scratchpad for intermediate state
                "scratch_pad",
                // Termination + escalation
                "complete_run",
                "escalate_to_operator",
                // Tool discovery when the listed set is insufficient
                "tool_search",
            ]),
        // #707: reviewer is READ-ONLY. No write / edit / bash. Prior
        // test (`reviewer_is_read_only_tools` in this file) already
        // pinned the contract with the old placeholder names; update
        // to real registered names while preserving the read-only
        // invariant.
        AgentRole::new("reviewer", "Reviewer", AgentRoleTier::Standard)
            .with_description(
                "Read-only audit. Returns a severity-ranked review document with \
                 file:line citations and concrete fixes. Best for: verifying a \
                 peer agent's output before complete_run, or assessing a diff for \
                 risk. Does not modify code or state.",
            )
            .with_response_shape(ResponseShape::ProceduralArtifact)
            .with_system_prompt(REVIEWER_PROMPT)
            .with_tools([
                // Read-only inspection
                "read",
                "grep",
                "glob",
                "lsp",
                // Retrieval for cross-referencing prior project context
                "memory_search",
                "graph_query",
                // Scratchpad for structured findings before delivery
                "scratch_pad",
                // Termination + escalation
                "complete_run",
                "escalate_to_operator",
                // Tool discovery for specialised read-only audit tools
                // that may not be in the default set (e.g., custom
                // plugin linters). Review discipline in the prompt
                // prevents misuse toward mutation.
                "tool_search",
            ]),
        // #775: generic role — the orchestrator's "I don't know which
        // specialty fits" escape hatch. Workflow is whatever the goal
        // text describes; the role itself has no opinion. Tool
        // allowlist is the universal-purpose set: read/inspect, write/
        // mutate, shell, scratchpad, terminate. The orchestrator
        // chooses the goal carefully so the generic agent is not
        // handed work better suited to a specialist.
        AgentRole::new("generic", "Generic", AgentRoleTier::Generic)
            .with_description(
                "No specialty. Does whatever the goal asks — read, write, run \
                 commands, summarise. Best for: one-off / mixed / exploratory \
                 work that does not cleanly fit a registered specialty (executor \
                 / researcher / reviewer). The parent owns the workflow design \
                 via the goal text and optional parent_context.",
            )
            .with_response_shape(ResponseShape::ProceduralArtifact)
            .with_system_prompt(GENERIC_PROMPT)
            .with_tools([
                // Read-side
                "read",
                "grep",
                "glob",
                "lsp",
                // Write-side (the parent may give a procedural goal)
                "write",
                "edit",
                "multiedit",
                // Shell — covers builds, tests, git, ad-hoc commands
                "bash",
                "bash_output",
                "bash_kill",
                // Memory + graph for prior-context retrieval
                "memory_search",
                "graph_query",
                // Utilities
                "json_extract",
                "scratch_pad",
                // Termination + escalation
                "complete_run",
                "escalate_to_operator",
                // Discovery — generic agents may need a tool the default
                // set does not include
                "tool_search",
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
    // Bumped 7500 → 7650 in #844 PR-1 to fit the sandbox-path anti-leak
    // line in ORCHESTRATOR_PROMPT. R36 dogfood (2026-05-11) proved the
    // orchestrator LLM will otherwise copy its own workspace_path into
    // sub-agent goals and send children to empty directories. +64
    // chars needed, +150 reserved for small future additions before
    // we'd have to trim prose elsewhere.
    const PROMPT_MAX_CHARS: usize = 7_650;

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

    const DEFAULT_ROLE_IDS: &[&str] = &[
        "orchestrator",
        "status-checker",
        "executor",
        "researcher",
        "reviewer",
        "generic",
    ];

    #[test]
    fn agent_role_builder() {
        let role = AgentRole::new("custom", "Custom Role", AgentRoleTier::Standard)
            .with_description("A test role.")
            .with_system_prompt("Be helpful.")
            .with_tools(["tool_a", "tool_b"])
            .with_max_context_tokens(32_000)
            .with_response_shape(ResponseShape::DirectAnswer);

        assert_eq!(role.role_id, "custom");
        assert_eq!(role.tier, AgentRoleTier::Standard);
        assert_eq!(role.description, "A test role.");
        assert_eq!(role.tools.len(), 2);
        assert_eq!(role.max_context_tokens, Some(32_000));
        assert_eq!(role.response_shape, ResponseShape::DirectAnswer);
    }

    #[test]
    fn default_roles_non_empty() {
        let roles = default_roles();
        assert_eq!(roles.len(), 6);
        let ids: Vec<_> = roles.iter().map(|r| r.role_id.as_str()).collect();
        assert!(ids.contains(&"orchestrator"));
        assert!(ids.contains(&"status-checker"));
        assert!(ids.contains(&"researcher"));
        assert!(ids.contains(&"executor"));
        assert!(ids.contains(&"reviewer"));
        assert!(ids.contains(&"generic"));
    }

    #[test]
    fn every_default_role_has_a_description() {
        // #775 contract: AgentRole.description is the orchestrator-facing
        // summary surfaced by future list_agents / agent_description tools
        // (#776). Every built-in must populate it; an empty string defeats
        // the purpose.
        for role in default_roles() {
            assert!(
                !role.description.is_empty(),
                "role {role_id:?} must populate `description`",
                role_id = role.role_id,
            );
        }
    }

    #[test]
    fn generic_role_uses_generic_tier() {
        let roles = default_roles();
        let g = roles.iter().find(|r| r.role_id == "generic").unwrap();
        assert_eq!(g.tier, AgentRoleTier::Generic);
    }

    // ── #775 BASE / specialty assembly tests ─────────────────────────────────

    /// The BASE_SUBAGENT_PROMPT must contain the universal sub-agent
    /// contract content — every sub-agent role's assembled prompt
    /// inherits these anchors. The `every_role_prompt_has_*` tests
    /// further down rely on this.
    #[test]
    fn base_subagent_prompt_carries_universal_content() {
        let base = BASE_SUBAGENT_PROMPT;
        // Identity: subagent / parent framing.
        assert!(
            base.contains("sub-agent"),
            "BASE must establish sub-agent identity"
        );
        // Mandate anchor — pinned by every_role_prompt_has_autonomous_mandate.
        assert!(
            base.contains("Keep going until"),
            "BASE must carry the autonomous-completion mandate"
        );
        // Sub-agent contract — no own-run introspection, no further spawn.
        assert!(
            base.contains("get_run") && base.contains("list_runs"),
            "BASE must forbid own-run introspection by name"
        );
        // Parent-context handoff (#775 freeform field).
        assert!(
            base.contains("Parent context"),
            "BASE must mention how the parent_context section is rendered"
        );
    }

    /// Sub-agent roles assemble as BASE + specialty overlay.
    #[test]
    fn subagent_assembled_prompts_start_with_base() {
        for role_id in ["executor", "researcher", "reviewer", "generic"] {
            let assembled = assembled_prompt_for(role_id);
            assert!(
                assembled.starts_with(BASE_SUBAGENT_PROMPT),
                "{role_id} assembled prompt must start with BASE_SUBAGENT_PROMPT"
            );
            // And the overlay content must be present too — the role's
            // raw system_prompt (specialty) is somewhere in the
            // assembled string, distinct from the base.
            let roles = default_roles();
            let raw = roles
                .iter()
                .find(|r| r.role_id == role_id)
                .and_then(|r| r.system_prompt.as_deref())
                .expect("role has specialty overlay");
            assert!(
                assembled.contains(raw),
                "{role_id} assembled prompt must contain its specialty overlay"
            );
        }
    }

    /// The orchestrator is the parent — its assembled prompt does NOT
    /// have BASE_SUBAGENT_PROMPT prepended. Pinning this contract
    /// prevents an accidental "every role prepends BASE" change from
    /// breaking the orchestrator's identity (which says "you are not a
    /// sub-agent").
    #[test]
    fn orchestrator_assembled_prompt_excludes_subagent_base() {
        let assembled = assembled_prompt_for("orchestrator");
        // The orchestrator prompt itself names "sub-agent" / "subagent"
        // when discussing delegation, so we cannot ban the substring.
        // Instead we assert the assembled prompt does not START with
        // BASE_SUBAGENT_PROMPT — i.e. it carries no sub-agent identity
        // opener.
        assert!(
            !assembled.starts_with(BASE_SUBAGENT_PROMPT),
            "orchestrator must not have BASE_SUBAGENT_PROMPT prepended"
        );
        // And the orchestrator's assembled prompt must equal its raw
        // system_prompt (no transformation).
        let roles = default_roles();
        let raw = roles
            .iter()
            .find(|r| r.role_id == "orchestrator")
            .and_then(|r| r.system_prompt.clone())
            .unwrap();
        assert_eq!(assembled, raw);
    }

    /// Unknown role ids fall back to the generic role's assembled
    /// prompt (not the pre-#775 3-line generic fallback string).
    #[test]
    fn assembled_prompt_for_unknown_role_falls_back_to_generic() {
        let unknown = assembled_prompt_for("not-a-real-role-1234");
        let generic = assembled_prompt_for("generic");
        assert_eq!(
            unknown, generic,
            "unknown role_id must render the generic-assembled prompt"
        );
    }

    /// #774: contract test — the cheap `response_shape_for` static
    /// lookup MUST match every role's `response_shape` field in
    /// `default_roles()`. The table is duplicated for performance
    /// (avoids allocating a fresh Vec<AgentRole> with cloned
    /// multi-KB prompt strings on every DECIDE turn); this test
    /// catches drift.
    #[test]
    fn response_shape_for_matches_default_roles_response_shape() {
        for role in default_roles() {
            assert_eq!(
                response_shape_for(&role.role_id),
                role.response_shape,
                "response_shape_for({:?}) must match default_roles().response_shape; \
                 if you added a new role, update the static table in \
                 `response_shape_for`.",
                role.role_id,
            );
        }
    }

    /// Unknown role_ids fall back to ProceduralArtifact — the safer
    /// shape (a DirectAnswer footer on an unknown procedural role
    /// would re-introduce the R19 wedge).
    #[test]
    fn response_shape_for_unknown_role_returns_procedural_artifact() {
        assert_eq!(
            response_shape_for("not-a-real-role-xyz"),
            ResponseShape::ProceduralArtifact,
        );
    }

    /// #844: `tier_for` static lookup MUST match every role's `tier`
    /// field in `default_roles()`. Same drift-guard shape as
    /// `response_shape_for_matches_default_roles_response_shape`.
    #[test]
    fn tier_for_matches_default_roles_tier() {
        for role in default_roles() {
            assert_eq!(
                tier_for(&role.role_id),
                role.tier,
                "tier_for({:?}) must match default_roles().tier; if you \
                 added a new role, update the static table in `tier_for`.",
                role.role_id,
            );
        }
    }

    /// Unknown role_ids fall back to `Standard` — the safest sub-agent
    /// tier (never accidentally grants orchestrator capabilities to
    /// an unknown role).
    #[test]
    fn tier_for_unknown_role_returns_standard() {
        assert_eq!(tier_for("not-a-real-role-xyz"), AgentRoleTier::Standard);
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
            .tools
            .iter()
            .any(|t| t.contains("write") || t.contains("Write")));
    }

    /// #806 regression guard: the orchestrator MUST NOT have any
    /// filesystem, code-inspection, or shell tool in its allowlist.
    /// Workspace inspection now goes through a status-checker spawn;
    /// if any of these tools sneak back in, the bash-loop pathology
    /// returns (R22 dogfood proved this end-to-end).
    #[test]
    fn orchestrator_has_no_workspace_inspection_tools() {
        let roles = default_roles();
        let orch = roles.iter().find(|r| r.role_id == "orchestrator").unwrap();
        let banned = [
            "read",
            "grep",
            "glob",
            "lsp",
            "bash",
            "bash_output",
            "bash_kill",
        ];
        for tool in banned {
            assert!(
                !orch.tools.iter().any(|t| t == tool),
                "#806 regression: orchestrator allowlist MUST NOT include {tool:?}; \
                 workspace inspection spawns a status-checker. Found tools: {:?}",
                orch.tools
            );
        }
        // Sanity: spawn_subagent IS still there.
        assert!(
            orch.tools.iter().any(|t| t == "spawn_subagent"),
            "orchestrator allowlist must keep spawn_subagent; got {:?}",
            orch.tools
        );
    }

    /// #806 regression guard: status-checker is read-only — no write,
    /// no edit, no multi-edit. Mutation goes to executor.
    #[test]
    fn status_checker_is_read_only() {
        let roles = default_roles();
        let sc = roles
            .iter()
            .find(|r| r.role_id == "status-checker")
            .expect("status-checker must be registered (#806)");
        let banned = ["write", "edit", "multiedit", "spawn_subagent"];
        for tool in banned {
            assert!(
                !sc.tools.iter().any(|t| t == tool),
                "status-checker must not include {tool:?}; mutation + delegation \
                 are out of scope. Found tools: {:?}",
                sc.tools
            );
        }
        // Sanity: read-only inspection tools ARE present.
        for tool in ["read", "grep", "glob", "bash"] {
            assert!(
                sc.tools.iter().any(|t| t == tool),
                "status-checker must include {tool:?} for inspection; got {:?}",
                sc.tools
            );
        }
    }

    // ── Prompt structural-contract tests ──────────────────────────────────────

    /// Resolve the rendered (assembled) prompt for a role id.
    ///
    /// Pre-#775 this read `r.system_prompt` directly. Post-#775 the
    /// raw field carries only the specialty overlay for non-orchestrator
    /// roles; the assembled prompt (BASE_SUBAGENT_PROMPT + overlay) is
    /// what the LLM actually sees and what these tests must validate.
    fn prompt_of(role_id: &str) -> String {
        // assembled_prompt_for falls back to `generic` for unknown role
        // ids; we want a hard failure here so a typo in DEFAULT_ROLE_IDS
        // doesn't silently pass by reading the generic prompt.
        let roles = default_roles();
        if !roles.iter().any(|r| r.role_id == role_id) {
            panic!("role {role_id} must be registered in default_roles()");
        }
        assembled_prompt_for(role_id)
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
    fn every_role_prompt_names_fail_run_as_truthful_failure_verb() {
        // #825: without the prompt naming the verb, the model has no way
        // to discover it — it will fall back to complete_run with a
        // "Status: Blocked" summary (R26 pathology). Every role's prompt
        // must document fail_run alongside complete_run /
        // escalate_to_operator so the model can choose the right one.
        //
        // This guard pairs with the runtime work in
        // cairn-orchestrator::execute_impl (FailRun dispatch →
        // LoopSignal::Failed with `model_reported_failure:` prefix) and
        // cairn-app::handlers::runs::helpers::classify_failed_reason
        // (prefix → FailureClass::ModelReportedFailure).
        for role_id in DEFAULT_ROLE_IDS {
            let prompt = prompt_of(role_id);
            assert!(
                prompt.contains("fail_run"),
                "#825: {role_id} prompt must name fail_run — without it \
                 the model won't discover the verb and will fall back to \
                 lying via complete_run"
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
        // The reviewer's `tools` is already asserted read-only above.
        // This test pins the *prompt* text too: it must not tell the reviewer
        // to use mutating tools, because that would contradict the role
        // contract and invite the model to ignore the `tools` gate.
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

    /// #707 regression guard: every role with a non-empty
    /// `tools` must reference REAL registered tool names, not
    /// `cairn.*` placeholders that don't exist in the builtin
    /// registry.
    ///
    /// R11 dogfood proved the placeholder surface silently broke
    /// specialists: with no effective tools, they fell back to
    /// training-data answers with honest disclaimers ("Due to tool
    /// limitations in this environment, I am unable to provide
    /// specific citations"). The orchestrator correctly refused those
    /// outputs, but the specialists were structurally unable to do
    /// their jobs.
    ///
    /// This test pins the contract: every tool name declared in any
    /// built-in role's `tools` must match either a real harness tool
    /// name or a registered cairn-tools builtin. The `cairn.*`
    /// pseudo-namespace is forbidden because no such prefix exists in
    /// the registry.
    ///
    /// Covers ALL four built-in roles (orchestrator, researcher,
    /// executor, reviewer) — #705 added a hardcoded tool list to the
    /// orchestrator too, so it shares the same regression surface.
    #[test]
    fn role_allowlists_reference_only_registered_tool_names() {
        // Known-registered names as of the #708 Gemini review.
        // Source of truth:
        //   - harness-tools: BASH_TOOL_NAME / READ_TOOL_NAME / etc.
        //     constants in the upstream harness-* crates.
        //   - cairn-tools builtins: `fn name()` impls in
        //     crates/cairn-tools/src/builtins/*.rs.
        //
        // Excluded from this list (Gemini review on #708):
        //   - `web_fetch` — only registered as a `#[cfg(test)]` stub
        //     in tool_search.rs; the real tool is `webfetch` (harness).
        //   - `web_search` — only present as a rustdoc example name in
        //     tool_search.rs; not a production tool.
        //   - `echo` — test-only registration.
        //
        // If a future PR adds a new tool, extend this set.
        let known_registered: &[&str] = &[
            // Harness tools
            "bash",
            "bash_output",
            "bash_kill",
            "read",
            "grep",
            "glob",
            "write",
            "edit",
            "multiedit",
            "lsp",
            "webfetch",
            // Cairn-tools builtins (production, not test stubs)
            "calculate",
            "cancel_task",
            "delete_memory",
            "eval_score",
            "get_approvals",
            "get_run",
            "get_task",
            "graph_query",
            "http_request",
            "json_extract",
            "list_runs",
            "memory_search",
            "memory_store",
            "notify_operator",
            "plugin_tool",
            "resolve_approval",
            // #776: agent-registry introspection tools — orchestrator-only
            // observational reads of default_roles().
            "list_agents",
            "agent_description",
            "schedule_task",
            "scratch_pad",
            "search_events",
            "summarize_text",
            "tool_search",
            "update_memory",
            "wait_for_task",
            // Meta-actions (not tools proper but accepted names from
            // decide_impl's synthetic tool_defs — spawn_subagent /
            // complete_run / escalate_to_operator).
            "spawn_subagent",
            "complete_run",
            "escalate_to_operator",
        ];

        let roles = default_roles();
        for role in roles.iter() {
            if role.tools.is_empty() {
                // Roles that legitimately declare no allowlist receive
                // the full registered surface (back-compat path).
                // Skip — there's no surface to validate.
                continue;
            }
            for tool in &role.tools {
                assert!(
                    !tool.starts_with("cairn."),
                    "#707 regression: role {role_id:?} tools contains \
                     `cairn.*` placeholder {tool:?}. These are NOT \
                     registered in the builtin tool registry and leave \
                     the role with an empty effective surface, forcing \
                     training-data fallback.",
                    role_id = role.role_id,
                );
                assert!(
                    known_registered.contains(&tool.as_str()),
                    "#707 regression: role {role_id:?} tools references \
                     unregistered tool name {tool:?}. Known registered \
                     names: {known_registered:?}. If this is a \
                     legitimately new tool, add it to the \
                     `known_registered` set in this test.",
                    role_id = role.role_id,
                );
            }
        }
    }

    // ── RFC 031 PR-A: serde alias round-trip for allowed_tools → tools ──

    #[test]
    fn role_deserialises_legacy_allowed_tools_alias() {
        // Pre-RFC-031 event-log snapshots carry `"allowed_tools"` on
        // the wire. The RFC 031 PR-A rename adds `#[serde(rename =
        // "tools", alias = "allowed_tools")]` on the field so replay
        // stays clean. This test pins that contract: a JSON blob with
        // the legacy key deserialises into the `tools` field, and
        // re-serialisation emits the new key.
        let legacy = serde_json::json!({
            "role_id": "custom",
            "display_name": "Custom",
            "description": "",
            "system_prompt": null,
            "allowed_tools": ["read", "grep"],
            "max_context_tokens": null,
            "tier": "standard",
            "response_shape": "procedural_artifact"
        });
        let role: AgentRole = serde_json::from_value(legacy).expect("alias must deserialise");
        assert_eq!(role.tools, vec!["read".to_owned(), "grep".to_owned()]);
        assert!(!role.forbid_all_tools);

        // Round-trip: re-serialisation emits `tools`, not `allowed_tools`.
        let roundtripped = serde_json::to_value(&role).unwrap();
        assert!(roundtripped.get("tools").is_some());
        assert!(roundtripped.get("allowed_tools").is_none());
    }

    #[test]
    fn role_forbid_all_tools_defaults_to_false() {
        // Pre-RFC-031 snapshots have no `forbid_all_tools` key; the
        // field's `#[serde(default)]` must fall through to `false`.
        let legacy = serde_json::json!({
            "role_id": "custom",
            "display_name": "Custom",
            "description": "",
            "system_prompt": null,
            "allowed_tools": [],
            "max_context_tokens": null,
            "tier": "standard",
            "response_shape": "procedural_artifact"
        });
        let role: AgentRole = serde_json::from_value(legacy).unwrap();
        assert!(!role.forbid_all_tools);
    }

    #[test]
    fn role_serialises_forbid_all_tools_when_true() {
        let role =
            AgentRole::new("custom", "Custom", AgentRoleTier::Standard).with_forbid_all_tools(true);
        let json = serde_json::to_value(&role).unwrap();
        assert_eq!(json["forbid_all_tools"], true);
        // Tools field still serialised under the new name.
        assert_eq!(json["tools"], serde_json::json!([]));
    }
}
