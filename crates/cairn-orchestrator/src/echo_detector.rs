//! Issue #689 Finding R2-B: detect "prose-playing" via `bash` + `echo`.
//!
//! Free-tier LLMs (observed with `minimax/minimax-m2.5:free` on Round-2
//! dogfood) sometimes role-play actions by echoing narrative prose
//! through the `bash` tool instead of emitting the correct
//! `ActionType`. Example observed across three consecutive turns on a
//! single run:
//!
//! ```json
//! {"name": "bash", "arguments": {"command": "echo 'Starting research delegation for circuit breakers in Rust'"}}
//! {"name": "bash", "arguments": {"command": "echo 'Spawning researcher subagent to research circuit breaker best practices in Rust'"}}
//! ```
//!
//! Because `bash` is classified as `SupervisedProcess`, each such
//! turn blocks on an operator approval — approving one just produces
//! the next echo. The LLM never emits the real `spawn_subagent` /
//! `invoke_tool` action. This detector surfaces the pattern early so
//! operators can see what's happening and intervene.
//!
//! # Scope guardrail
//!
//! The detector is **observational only**. It never auto-fails or
//! auto-cancels the run; the loop still dispatches the proposal as
//! normal. See the task brief on issue #689 for the rationale — the
//! existing circuit-breaker suite already owns terminal enforcement,
//! and misclassifying a legit `echo "$VAR" > file.txt` as prose-play
//! would produce a noisier failure than the signal is worth.
//!
//! # Heuristic
//!
//! A turn is "echo-bash" when the first proposal is:
//!
//! * `action_type == InvokeTool`,
//! * `tool_name == "bash"`,
//! * and the extracted command (or shell-invoked body) is a bare
//!   `echo STRING` with **no** redirect (`>`, `>>`, `|`, `&`, `;`,
//!   backtick, or `$(…)`).
//!
//! The redirect / operator filter preserves legitimate side-effects
//! (`echo "$VAR" > file.txt` is real work). Shell-prefix forms like
//! `sh -c "echo ..."` and `bash -c 'echo ...'` unwrap to the inner
//! command before classification.
//!
//! # State
//!
//! [`EchoDetectorState`] counts **consecutive** echo-bash turns and
//! returns [`EchoDetectorCheck::Detected`] once the count reaches
//! [`ECHO_BASH_DETECTION_THRESHOLD`] (= 2). A single isolated echo
//! turn is NOT a signal (the model might be announcing a boundary
//! before real work); two in a row says "this is what the model is
//! doing as work."

use cairn_domain::{ActionProposal, ActionType};

use crate::context::DecideOutput;

/// Detection fires when this many consecutive turns are classified as
/// echo-bash. 1 is too trigger-happy (isolated echoes happen in
/// legitimate runs — "echo starting build" followed by `cargo build`);
/// 3 wastes operator approvals because each echo-bash turn is a
/// SupervisedProcess gate. 2 is the conservative middle ground the
/// task brief calls for.
pub const ECHO_BASH_DETECTION_THRESHOLD: u32 = 2;

/// Outcome of classifying one DECIDE turn against the detector.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EchoDetectorCheck {
    /// The turn is not echo-bash OR the consecutive count is still
    /// below the threshold. No action needed.
    Continue,
    /// The turn IS echo-bash AND the consecutive count has just
    /// reached or passed [`ECHO_BASH_DETECTION_THRESHOLD`]. The loop
    /// runner should log a WARN and notify the emitter.
    ///
    /// Fires on the transition (at threshold exactly) AND on every
    /// subsequent consecutive echo-bash turn — the operator-facing
    /// log is rate-limited by the counter, but the metric counter on
    /// the emitter side increments on every Detected event so
    /// dashboards show continued prose-play accurately.
    Detected { consecutive_count: u32 },
}

/// Per-run state tracking consecutive echo-bash turns.
///
/// Constructed fresh at loop entry and mutated once per DECIDE. The
/// counter resets to 0 on any non-echo-bash turn.
#[derive(Clone, Debug, Default)]
pub struct EchoDetectorState {
    consecutive_echo_bash_turns: u32,
}

impl EchoDetectorState {
    /// Construct a fresh detector with the counter at 0.
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot the current counter. Exposed for tests + telemetry.
    pub fn consecutive_count(&self) -> u32 {
        self.consecutive_echo_bash_turns
    }

    /// Classify the freshly-completed DECIDE output.
    ///
    /// Returns [`EchoDetectorCheck::Detected`] on every consecutive
    /// echo-bash turn from the threshold onwards; returns
    /// [`EchoDetectorCheck::Continue`] otherwise.
    pub fn on_decide(&mut self, decide: &DecideOutput) -> EchoDetectorCheck {
        if is_echo_bash_turn(decide) {
            self.consecutive_echo_bash_turns = self.consecutive_echo_bash_turns.saturating_add(1);
            if self.consecutive_echo_bash_turns >= ECHO_BASH_DETECTION_THRESHOLD {
                return EchoDetectorCheck::Detected {
                    consecutive_count: self.consecutive_echo_bash_turns,
                };
            }
        } else {
            self.consecutive_echo_bash_turns = 0;
        }
        EchoDetectorCheck::Continue
    }
}

/// Pure classifier: is this DECIDE turn an "echo-bash" prose-play
/// turn? Looks only at the first proposal — the task brief confines
/// the detection to the primary action the model picked.
///
/// Separate public fn so tests can exercise the classifier in isolation
/// without spinning up an `EchoDetectorState`.
pub fn is_echo_bash_turn(decide: &DecideOutput) -> bool {
    let Some(first) = decide.proposals.first() else {
        return false;
    };
    is_echo_bash_proposal(first)
}

/// Pure classifier on a single [`ActionProposal`]. Returns `true` only
/// when the proposal targets `bash` with a bare `echo STRING` body.
pub fn is_echo_bash_proposal(proposal: &ActionProposal) -> bool {
    if proposal.action_type != ActionType::InvokeTool {
        return false;
    }
    let Some(tool_name) = proposal.tool_name.as_deref() else {
        return false;
    };
    if tool_name != "bash" {
        return false;
    }
    let Some(args) = proposal.tool_args.as_ref() else {
        return false;
    };
    let Some(command) = extract_bash_command(args) else {
        return false;
    };
    is_bare_echo_command(command)
}

/// Pull the `command` string out of the bash tool arguments JSON.
/// Returns `None` when the shape is unexpected (no `command` key, or
/// it's not a string).
///
/// Returns a borrowed `&str` tied to the input JSON — the detector hot
/// path never needs to own the command string, so we avoid the clone.
fn extract_bash_command(args: &serde_json::Value) -> Option<&str> {
    args.get("command").and_then(|v| v.as_str())
}

/// Is `cmd` a bare `echo STRING` with no redirects, pipes, or shell
/// operators that would make it a real side-effect?
///
/// Strategy:
/// 1. Strip a leading shell-prefix form (`sh -c "…"`, `bash -c '…'`,
///    `zsh -c "…"`, `/bin/sh -c …`, `/bin/bash -c …`) so the inner
///    body is evaluated on its own.
/// 2. Trim whitespace.
/// 3. First token must be `echo` (case-sensitive — shell builtin).
/// 4. Reject if the remainder contains any of: `>`, `<`, `|`, `;`,
///    `&`, backtick, `$(`, `$((`. These are the shell operators that
///    turn an echo into a side-effect (file write, command sub, etc.).
///
/// False-positive note: legitimate `echo` without redirect DOES exist
/// (e.g. a smoke-test run emits `echo "preflight ok"` before calling
/// real tools). The detector accepts that; a single isolated echo
/// doesn't fire because [`EchoDetectorState`] requires
/// [`ECHO_BASH_DETECTION_THRESHOLD`] consecutive turns.
fn is_bare_echo_command(cmd: &str) -> bool {
    let unwrapped = unwrap_shell_c(cmd);
    let trimmed = unwrapped.trim();
    if trimmed.is_empty() {
        return false;
    }
    // First token must be exactly `echo`. Split on any whitespace —
    // `echo\thello` is still `echo`.
    let mut parts = trimmed.splitn(2, char::is_whitespace);
    let Some(first) = parts.next() else {
        return false;
    };
    if first != "echo" {
        return false;
    }
    let remainder = parts.next().unwrap_or("");
    if has_shell_side_effect(remainder) {
        return false;
    }
    true
}

/// If `cmd` is a shell-prefix form (`sh -c "…"`, `bash -c '…'`, `zsh -c …`),
/// return the inner body with the outer quotes stripped. Otherwise
/// return `cmd` unchanged.
///
/// Handles the three shapes we observed in R2 and in grep of our own
/// test fixtures; exotic shapes (e.g. `env VAR=1 sh -c '…'`) fall
/// through to the un-unwrapped branch and are evaluated as-is, which
/// is the conservative default (we prefer to miss a prose-play
/// detection than to false-positive on a legitimate multi-arg shell
/// invocation).
///
/// Returns a `&str` tied to `cmd` — no allocation on the detector hot
/// path.
fn unwrap_shell_c(cmd: &str) -> &str {
    let trimmed = cmd.trim();
    for prefix in [
        "sh -c ",
        "bash -c ",
        "zsh -c ",
        "/bin/sh -c ",
        "/bin/bash -c ",
    ] {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            let rest = rest.trim_start();
            // Strip matching outer quotes.
            if let Some(inner) = strip_matching_quotes(rest) {
                return inner;
            }
            // Unquoted — return the tail as-is.
            return rest;
        }
    }
    cmd
}

/// Strip matching outer `"…"` or `'…'` quotes from `s`. Returns `None`
/// when the string is not a well-formed quoted literal.
fn strip_matching_quotes(s: &str) -> Option<&str> {
    let bytes = s.as_bytes();
    if bytes.len() < 2 {
        return None;
    }
    let first = bytes[0];
    let last = bytes[bytes.len() - 1];
    if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
        return Some(&s[1..s.len() - 1]);
    }
    None
}

/// Detect shell operators that turn an `echo` into a real side-effect.
/// The list below is intentionally conservative — any of these
/// disqualifies the turn from the "prose-playing" classification.
///
/// * `>` / `>>` — redirect to file (real side-effect).
/// * `<` — redirect input (rare in echo use, but still "doing work").
/// * `|` — pipe into another command.
/// * `;` — command separator; a second command follows.
/// * `&` — background / logical-and.
/// * `` ` `` — command substitution (legacy form).
/// * `$(` / `$((` — command / arithmetic substitution.
fn has_shell_side_effect(s: &str) -> bool {
    // Cheap byte scan; these characters are ASCII in every relevant
    // shell metachar so no UTF-8 boundary issues.
    for ch in s.chars() {
        match ch {
            '>' | '<' | '|' | ';' | '&' | '`' => return true,
            _ => {}
        }
    }
    s.contains("$(")
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_domain::ActionProposal;
    use serde_json::json;

    fn mk_decide(proposals: Vec<ActionProposal>) -> DecideOutput {
        DecideOutput {
            raw_response: String::new(),
            proposals,
            calibrated_confidence: 0.9,
            requires_approval: false,
            model_id: "stub".into(),
            latency_ms: 0,
            input_tokens: None,
            output_tokens: None,
            system_prompt: String::new(),
            messages_json: "[]".to_owned(),
            tool_calls_json: "[]".to_owned(),
            tool_defs_json: "[]".to_owned(),
        }
    }

    fn bash_proposal(command: &str) -> ActionProposal {
        ActionProposal::invoke_tool(
            "bash",
            json!({ "command": command }),
            "run a shell command",
            0.9,
            true,
        )
    }

    fn grep_proposal() -> ActionProposal {
        ActionProposal::invoke_tool(
            "grep",
            json!({ "pattern": "foo", "path": "." }),
            "search the code",
            0.9,
            false,
        )
    }

    fn spawn_proposal() -> ActionProposal {
        ActionProposal {
            action_type: ActionType::SpawnSubagent,
            description: "delegate research".to_owned(),
            confidence: 0.9,
            tool_name: Some("researcher".to_owned()),
            tool_args: Some(json!({"goal": "research circuit breakers"})),
            requires_approval: false,
        }
    }

    // ── Pure classifier: is_bare_echo_command ─────────────────────────────

    #[test]
    fn bare_echo_is_detected() {
        assert!(is_bare_echo_command("echo hello"));
        assert!(is_bare_echo_command(
            "echo 'Starting research delegation for circuit breakers in Rust'"
        ));
        assert!(is_bare_echo_command("  echo   'hi'  "));
    }

    #[test]
    fn echo_with_redirect_is_real_work() {
        assert!(!is_bare_echo_command("echo hello > file.txt"));
        assert!(!is_bare_echo_command("echo hello >> file.txt"));
        assert!(!is_bare_echo_command("echo \"$VAR\" > file.txt"));
    }

    #[test]
    fn echo_with_pipe_is_real_work() {
        assert!(!is_bare_echo_command("echo hello | tee out.txt"));
        assert!(!is_bare_echo_command("echo hello | grep h"));
    }

    #[test]
    fn echo_with_semicolon_is_real_work() {
        assert!(!is_bare_echo_command("echo hi; ls"));
    }

    #[test]
    fn echo_with_command_substitution_is_real_work() {
        assert!(!is_bare_echo_command("echo $(date)"));
        assert!(!is_bare_echo_command("echo `date`"));
    }

    #[test]
    fn non_echo_command_is_not_detected() {
        assert!(!is_bare_echo_command("cargo test"));
        assert!(!is_bare_echo_command("ls -la"));
        assert!(!is_bare_echo_command("printf hello"));
    }

    #[test]
    fn empty_command_is_not_detected() {
        assert!(!is_bare_echo_command(""));
        assert!(!is_bare_echo_command("   "));
    }

    #[test]
    fn shell_c_prefix_unwraps_to_inner_echo() {
        assert!(is_bare_echo_command("sh -c \"echo hello\""));
        assert!(is_bare_echo_command("bash -c 'echo hi'"));
        assert!(is_bare_echo_command("/bin/sh -c \"echo whatever\""));
    }

    #[test]
    fn shell_c_prefix_wrapping_redirect_is_still_real_work() {
        assert!(!is_bare_echo_command("sh -c \"echo hello > file.txt\""));
        assert!(!is_bare_echo_command("bash -c 'echo hi | cat'"));
    }

    // ── is_echo_bash_proposal ─────────────────────────────────────────────

    #[test]
    fn non_invoke_tool_proposals_are_not_echo_bash() {
        assert!(!is_echo_bash_proposal(&spawn_proposal()));
        assert!(!is_echo_bash_proposal(&ActionProposal::complete_run(
            "done", 0.9
        )));
    }

    #[test]
    fn non_bash_tool_is_not_echo_bash() {
        assert!(!is_echo_bash_proposal(&grep_proposal()));
    }

    #[test]
    fn bash_with_missing_command_arg_is_not_echo_bash() {
        let bad = ActionProposal::invoke_tool(
            "bash",
            json!({ "args": ["echo", "hi"] }), // wrong key
            "run",
            0.9,
            true,
        );
        assert!(!is_echo_bash_proposal(&bad));
    }

    #[test]
    fn bash_with_null_args_is_not_echo_bash() {
        let bad = ActionProposal {
            action_type: ActionType::InvokeTool,
            description: "bash".into(),
            confidence: 0.9,
            tool_name: Some("bash".to_owned()),
            tool_args: None,
            requires_approval: true,
        };
        assert!(!is_echo_bash_proposal(&bad));
    }

    // ── Test A — 3 consecutive bash-echo turns fire at turn 2 ─────────────

    #[test]
    fn test_a_three_consecutive_echo_bash_fires_on_turn_two() {
        let mut state = EchoDetectorState::new();
        let turn = mk_decide(vec![bash_proposal(
            "echo 'Starting research delegation for circuit breakers in Rust'",
        )]);

        // Turn 1: counter increments to 1, still below threshold.
        assert_eq!(state.on_decide(&turn), EchoDetectorCheck::Continue);
        assert_eq!(state.consecutive_count(), 1);

        // Turn 2: counter increments to 2, threshold reached → Detected.
        assert_eq!(
            state.on_decide(&turn),
            EchoDetectorCheck::Detected {
                consecutive_count: 2
            }
        );

        // Turn 3: still echo-bash, counter keeps climbing and detector
        // still fires (metrics should record continued prose-play).
        assert_eq!(
            state.on_decide(&turn),
            EchoDetectorCheck::Detected {
                consecutive_count: 3
            }
        );
    }

    // ── Test B — bash with legitimate non-echo command: no detection ──────

    #[test]
    fn test_b_legit_bash_command_does_not_fire() {
        let mut state = EchoDetectorState::new();
        let cargo_test = mk_decide(vec![bash_proposal("cargo test --workspace")]);

        for _ in 0..5 {
            assert_eq!(state.on_decide(&cargo_test), EchoDetectorCheck::Continue);
        }
        assert_eq!(state.consecutive_count(), 0);
    }

    // ── Test C — echo with redirect: no detection ─────────────────────────

    #[test]
    fn test_c_echo_with_redirect_does_not_fire() {
        let mut state = EchoDetectorState::new();
        let write_file = mk_decide(vec![bash_proposal("echo hello > file.txt")]);

        for _ in 0..5 {
            assert_eq!(state.on_decide(&write_file), EchoDetectorCheck::Continue);
        }
        assert_eq!(state.consecutive_count(), 0);
    }

    // ── Test D — mixed: echo, grep, echo → counter resets on non-echo ─────

    #[test]
    fn test_d_counter_resets_on_non_echo_turn() {
        let mut state = EchoDetectorState::new();
        let echo_turn = mk_decide(vec![bash_proposal("echo 'thinking about stuff'")]);
        let grep_turn = mk_decide(vec![grep_proposal()]);

        // First echo turn: count = 1.
        assert_eq!(state.on_decide(&echo_turn), EchoDetectorCheck::Continue);
        assert_eq!(state.consecutive_count(), 1);

        // Grep turn: real work → counter resets to 0.
        assert_eq!(state.on_decide(&grep_turn), EchoDetectorCheck::Continue);
        assert_eq!(state.consecutive_count(), 0);

        // Second echo turn: count = 1 again (not 2) — threshold NOT reached.
        assert_eq!(state.on_decide(&echo_turn), EchoDetectorCheck::Continue);
        assert_eq!(state.consecutive_count(), 1);
    }

    // ── Test E — spawn_subagent: no detection (happy path) ────────────────

    #[test]
    fn test_e_spawn_subagent_never_triggers_detection() {
        let mut state = EchoDetectorState::new();
        let spawn_turn = mk_decide(vec![spawn_proposal()]);

        for _ in 0..5 {
            assert_eq!(state.on_decide(&spawn_turn), EchoDetectorCheck::Continue);
        }
        assert_eq!(state.consecutive_count(), 0);
    }

    // ── Extra hardening ───────────────────────────────────────────────────

    #[test]
    fn empty_proposals_do_not_trigger_detection() {
        let mut state = EchoDetectorState::new();
        let empty = mk_decide(vec![]);
        for _ in 0..5 {
            assert_eq!(state.on_decide(&empty), EchoDetectorCheck::Continue);
        }
        assert_eq!(state.consecutive_count(), 0);
    }

    #[test]
    fn first_proposal_is_what_counts_even_if_others_are_real() {
        // If the LLM emits bash-echo FIRST and a real tool second, the
        // detection fires on the first proposal — this matches the
        // R2 dogfood evidence (the LLM only ever emitted one proposal
        // per turn) and keeps the classifier simple. If we ever see
        // multi-proposal rounds where this undercounts, extend the
        // classifier to scan all proposals rather than the first.
        let mut state = EchoDetectorState::new();
        let mixed = mk_decide(vec![bash_proposal("echo 'planning'"), grep_proposal()]);
        assert_eq!(state.on_decide(&mixed), EchoDetectorCheck::Continue);
        assert_eq!(
            state.on_decide(&mixed),
            EchoDetectorCheck::Detected {
                consecutive_count: 2
            }
        );
    }

    #[test]
    fn detector_state_default_starts_at_zero() {
        let s = EchoDetectorState::default();
        assert_eq!(s.consecutive_count(), 0);
    }
}
