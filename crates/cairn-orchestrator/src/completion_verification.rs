//! F47 PR1: tool_result scanner that produces a [`CompletionVerification`]
//! sidecar for the SSE `finished` event.
//!
//! # Motivation
//!
//! Dogfood M1 (2026-04-26) shipped a Rust crate that emitted
//! `warning: unused imports: Constraint, Direction, Layout, text::Line` in a
//! stored bash tool_result, while the LLM's `complete_run` summary claimed
//! "cargo check must pass with no warnings ✓". Operators had no independent
//! signal that the summary lied. This scanner is that signal.
//!
//! # Contract
//!
//! * Pure scanning — no IO. Fully unit-testable without a runtime.
//! * Never fabricates. If an exit code is not present in the tool_result
//!   structure, `CommandOutcome::exit_code = None`.
//! * Bounded retained output. The returned warning / error vectors are
//!   capped at [`MAX_ENTRIES_PER_BUCKET`] entries, each truncated to
//!   [`MAX_LINE_LEN`] chars. Scanning may use small per-call temporaries
//!   but memory retained across a run is proportional to the cap, not the
//!   tool_output size.
//! * Advisory by default, enforcing when the strict completion gate is on.
//!   The scanner itself still just reports what tool outputs contain; the
//!   orchestrator's loop signal remains the source of truth for run state.
//!   Since issue #660 the loop additionally consults the accumulator via
//!   [`VerificationAccumulator::error_count`] when the LLM proposes
//!   `CompleteRun` — with `orchestrator_strict_completion_gate = true`
//!   (default) a non-zero error count rejects the `complete_run` and
//!   re-enters DECIDE so the model must address the diagnostics before
//!   terminating the run. Operators who want the legacy "LLM calls it
//!   done no matter what" flow can flip the flag to `false` per run.
//!
//! # Usage
//!
//! Two entry points cover the orchestrator loop's access patterns:
//!
//! * [`VerificationAccumulator`] — incremental. Feed one `ActionResult` at
//!   a time from the loop's execute phase; the full `tool_output` payload
//!   is scanned and discarded in place, so retained memory stays bounded
//!   even when the run reads large files via a `read` tool. Used by the
//!   production loop to avoid holding every iteration's raw `tool_output`
//!   in memory for the whole run.
//! * [`extract_verification`] — batch. Accepts a slice of `ActionResult`s
//!   and internally delegates to the accumulator. Kept for the unit tests
//!   and for callers that already have the full set in hand.
//!
//! # Scope
//!
//! PR1 (this file) makes the sidecar visible on the SSE `finished` event.
//! PR2 adds persistence (event + projection + REST surface). PR3 adds UI.

use cairn_domain::{CommandOutcome, CompletionVerification};
use serde_json::Value;

use crate::context::ActionResult;

/// Maximum number of warning or error entries kept per bucket. Lines past
/// this cap are silently dropped; the count is implicit in the vector
/// length. Chosen so a pathological tool_result (e.g. clippy with thousands
/// of lints) cannot bloat an SSE frame.
pub const MAX_ENTRIES_PER_BUCKET: usize = 50;

/// Maximum length of a single matched line kept in a bucket. Longer lines
/// are truncated with an ellipsis marker appended. 500 chars comfortably
/// holds a full Rust diagnostic header without flooding the SSE payload.
pub const MAX_LINE_LEN: usize = 500;

/// Current extractor version. Bump when the matching or truncation policy
/// changes in a way that downstream consumers need to notice. v1 = F47 PR1.
pub const EXTRACTOR_VERSION: u32 = 1;

/// Bash-class tool names. These are scanned for structured `command` and
/// `exit_code` fields. Other tools still contribute to warning / error
/// scanning via their text output but produce no [`CommandOutcome`] entry.
pub(crate) const BASH_TOOL_NAMES: &[&str] = &["bash", "shell_exec", "run_bash"];

/// Case-insensitive match on any member of [`BASH_TOOL_NAMES`]. Shared
/// with `loop_runner::render_tool_output_preview` so the preview path
/// and the verification scanner agree on which tool names are
/// "bash-class". Matching is case-insensitive because some provider
/// adapters normalize `"bash"` to `"Bash"` during JSON marshalling.
pub(crate) fn is_bash_tool(tool_name: &str) -> bool {
    BASH_TOOL_NAMES
        .iter()
        .any(|b| b.eq_ignore_ascii_case(tool_name))
}

/// Incremental builder. The orchestrator loop feeds one `ActionResult` per
/// iteration into [`VerificationAccumulator::observe`]; at Done it calls
/// [`VerificationAccumulator::finish`] to produce the sidecar. This avoids
/// retaining full `tool_output` payloads (which can be very large — a
/// `read` tool may return an entire file) across the run's lifetime:
/// each result is scanned and dropped in place, leaving only the bounded
/// bucket output in memory.
#[derive(Clone, Debug, Default)]
pub struct VerificationAccumulator {
    warnings: Vec<String>,
    errors: Vec<String>,
    commands: Vec<CommandOutcome>,
    tool_results_scanned: usize,
    /// #821: total errors observed across the run's lifetime,
    /// independent of the [`MAX_ENTRIES_PER_BUCKET`] cap on the
    /// `errors` vector. Used together with `errors_baseline_total`
    /// to compute the gate's "errors since last attempt" decision —
    /// counting the *capped* `errors.len()` would silently freeze
    /// once the bucket fills (Gemini caught this on PR #823: at
    /// `errors.len() == 50`, `mark_baseline()` would lock the
    /// baseline at 50 and `errors_since_baseline` would return 0
    /// forever, opening a bypass for the strict completion gate).
    /// This counter increments on every observed error line even
    /// when the bucket is full and the line is dropped from the
    /// preview slice.
    errors_total_observed: usize,
    /// #821: snapshot of `errors_total_observed` at the last
    /// `mark_baseline()` call. The strict completion gate uses
    /// `errors_total_observed - errors_baseline_total` to decide
    /// whether a new attempt has accumulated fresh errors since
    /// the previous rejection. Without this, a single transient
    /// `error:` line in iter 5 (e.g. a stderr fragment from a
    /// successful `git status`) would permanently block complete_run
    /// regardless of whether the model addressed it. R25 dogfood
    /// proved this end-to-end: the executor ran 12 iterations,
    /// completed the actual work, and was rejected on every
    /// complete_run attempt by errors from earlier iterations the
    /// model had already moved past.
    errors_baseline_total: usize,
    /// #821: snapshot of `errors.len()` (the capped buffer's
    /// length) at the last `mark_baseline()` call. Used only by
    /// `errors_since_baseline_slice()` to slice the preview the
    /// rejection step shows the model — distinct from the count
    /// used by the gate decision (which uses the uncapped totals
    /// above). When the cap is reached the slice may be shorter
    /// than the count; that's expected — operators see a sample,
    /// the gate sees the truth.
    errors_baseline_slice_idx: usize,
    /// #823 review (Gemini): true once the gate has rejected at
    /// least one complete_run (i.e. `mark_baseline` has been called).
    /// Until the model issues at least one tool call AFTER that
    /// rejection, the gate keeps treating the next complete_run as
    /// "no progress" and rejects again. Without this, the #821
    /// recovery carve-out would let the model bypass the gate by
    /// retrying complete_run with no work done in between (the gate
    /// sees errors_since_baseline == 0 and allows). Test fixture
    /// `test_660_completion_gate.rs` exercises exactly this shape.
    baseline_active: bool,
    /// #823 review (Gemini): incremented on every `observe(...)` of
    /// an `InvokeTool` action that completes after `mark_baseline`.
    /// Reset to 0 by `mark_baseline`. The gate consults this through
    /// `made_progress_since_baseline()` to require the model to
    /// actually try something between rejections — not just retry
    /// complete_run with no intervening tool call.
    tool_observations_since_baseline: usize,
}

impl VerificationAccumulator {
    /// Create a fresh accumulator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Scan a single `ActionResult`. Non-`InvokeTool` proposals are
    /// ignored up front — they never produce tool output.
    pub fn observe(&mut self, result: &ActionResult) {
        if result.proposal.action_type != cairn_domain::ActionType::InvokeTool {
            return;
        }
        self.tool_results_scanned += 1;
        // #823 review (Gemini): a tool call after `mark_baseline()`
        // counts as "the model tried something" — without this signal
        // the gate would let a model bypass by retrying complete_run
        // with zero work done between rejections.
        if self.baseline_active {
            self.tool_observations_since_baseline =
                self.tool_observations_since_baseline.saturating_add(1);
        }

        let tool_name = result.proposal.tool_name.as_deref().unwrap_or("<unknown>");

        // Command outcome. Only bash-class tools produce a CommandOutcome
        // entry; non-bash tools contribute to warning/error scanning via
        // their text output but do not appear in `commands[]`.
        if is_bash_tool(tool_name) {
            let cmd = result
                .proposal
                .tool_args
                .as_ref()
                .and_then(|v| v.get("command"))
                .and_then(Value::as_str)
                .map(|s| truncate(s, MAX_LINE_LEN))
                .unwrap_or_default();
            let exit_code = result.tool_output.as_ref().and_then(extract_exit_code);
            self.commands.push(CommandOutcome {
                tool_name: tool_name.to_owned(),
                cmd,
                exit_code,
            });
        }

        // Text scan. Iterate string sources lazily, scanning in place to
        // avoid cloning large stdout/stderr payloads. We keep scanning
        // even after the preview buckets fill so the uncapped
        // `errors_total_observed` counter (#821) reflects every error
        // line — the strict completion gate relies on this counter, not
        // on `errors.len()`. The bucket pushes themselves still cap at
        // [`MAX_ENTRIES_PER_BUCKET`] inside `scan_text`, so memory is
        // bounded regardless.
        for src in iter_text_sources(result) {
            self.scan_text(&src);
        }
    }

    /// Consume the accumulator and produce the sidecar.
    pub fn finish(self) -> CompletionVerification {
        CompletionVerification {
            warnings: self.warnings,
            errors: self.errors,
            commands: self.commands,
            tool_results_scanned: self.tool_results_scanned,
            extractor_version: EXTRACTOR_VERSION,
        }
    }

    /// #660: non-consuming peek at the accumulated error lines. Used by
    /// the strict completion gate in `loop_runner` to decide whether to
    /// reject an LLM-proposed `CompleteRun` before it reaches the
    /// execute phase. Returning a slice keeps callers from being able
    /// to mutate the bucket; the loop only reads to build a rejection
    /// `StepSummary` that steers the next DECIDE turn.
    pub fn errors(&self) -> &[String] {
        &self.errors
    }

    /// #660: non-consuming count of scanned error lines. Equivalent to
    /// `self.errors().len()` but keeps the call sites clear at the
    /// completion-gate decision point — the zero-check is the only
    /// thing the gate really cares about on the hot path.
    pub fn error_count(&self) -> usize {
        self.errors.len()
    }

    /// #821: count of errors since the last `mark_baseline()` call.
    /// Used by the strict completion gate to scope its decision to
    /// "new since the last attempt" rather than "ever." The gate
    /// records the baseline AFTER each rejection so the next attempt
    /// is judged only on whether NEW errors appeared.
    ///
    /// Counts against the uncapped `errors_total_observed` counter,
    /// not `errors.len()`, so the gate keeps observing new errors
    /// even after the [`MAX_ENTRIES_PER_BUCKET`] preview bucket is
    /// full. (Counting `errors.len()` would let a model bypass the
    /// gate by retrying complete_run after the buffer hit 50 — once
    /// `mark_baseline()` snapshots a full bucket, `errors.len() -
    /// baseline` is always 0 even if new errors are streaming in.)
    pub fn errors_since_baseline(&self) -> usize {
        self.errors_total_observed
            .saturating_sub(self.errors_baseline_total)
    }

    /// #821: non-consuming peek at errors observed since the last
    /// `mark_baseline()` call. The strict completion gate uses this
    /// to build the rejection's user-facing preview — the model
    /// should see only the errors it's expected to address THIS
    /// attempt, not a cumulative ledger of every transient `error:`
    /// line the run has ever produced.
    ///
    /// This slice is bounded by the [`MAX_ENTRIES_PER_BUCKET`] cap
    /// on `errors`. When the bucket is full the slice may be shorter
    /// than `errors_since_baseline()` reports — that count is the
    /// truth, this slice is a sample for the rejection preview.
    pub fn errors_since_baseline_slice(&self) -> &[String] {
        let cap = self.errors_baseline_slice_idx.min(self.errors.len());
        &self.errors[cap..]
    }

    /// #821: advance the baseline so future calls to
    /// `errors_since_baseline*` return only errors observed AFTER
    /// this call. The strict completion gate calls this immediately
    /// after rejecting a complete_run so the next attempt is judged
    /// fresh. The full error history remains in `self.errors` and
    /// flows into `finish()`'s sidecar — the baseline only affects
    /// the gate's per-attempt window.
    ///
    /// Snapshots two state variables: the uncapped total counter
    /// (used for the gate decision) and the buffer index (used for
    /// the preview slice). See the field docs on
    /// `errors_total_observed` for why we track both.
    ///
    /// Also flips on `baseline_active` and resets the tool-progress
    /// counter so the next gate consultation can require the model
    /// to actually try something between rejections (#823 review).
    pub fn mark_baseline(&mut self) {
        self.errors_baseline_total = self.errors_total_observed;
        self.errors_baseline_slice_idx = self.errors.len();
        self.baseline_active = true;
        self.tool_observations_since_baseline = 0;
    }

    /// #823 review (Gemini): has the model issued at least one tool
    /// call since the most recent `mark_baseline()`? Always true
    /// before the first rejection (no baseline set yet). After a
    /// rejection, returns false until the model `observe`s at least
    /// one `InvokeTool` action — i.e. it has tried something to
    /// address the rejection feedback rather than just retrying
    /// complete_run with no progress.
    ///
    /// The gate uses this in addition to `errors_since_baseline()`:
    /// it allows complete_run only when (zero new errors AND the
    /// model made progress) OR (no rejection has happened yet).
    pub fn made_progress_since_baseline(&self) -> bool {
        !self.baseline_active || self.tool_observations_since_baseline > 0
    }

    fn scan_text(&mut self, text: &str) {
        for raw_line in text.lines() {
            // Trim leading whitespace so indented diagnostics still match.
            // Don't trim trailing — loss of a trailing period or bracket
            // changes the meaning of the line.
            let line = raw_line.trim_start();

            if is_warning_line(line) {
                // Warnings are buffer-only; we don't track a gate
                // counter for them, so skipping past the cap is fine.
                if self.warnings.len() < MAX_ENTRIES_PER_BUCKET {
                    self.warnings.push(truncate(line, MAX_LINE_LEN));
                }
            } else if is_error_line(line) {
                // #821: increment the uncapped total even when the
                // preview bucket is full so the gate's
                // errors_since_baseline math stays correct. The
                // bucket itself only fills to MAX_ENTRIES_PER_BUCKET
                // so previews remain bounded; the gate decision uses
                // errors_total_observed instead of errors.len().
                self.errors_total_observed = self.errors_total_observed.saturating_add(1);
                if self.errors.len() < MAX_ENTRIES_PER_BUCKET {
                    self.errors.push(truncate(line, MAX_LINE_LEN));
                }
            }
        }
    }
}

/// Batch wrapper over [`VerificationAccumulator`]. Prefer
/// `VerificationAccumulator::observe` from the loop so large `tool_output`
/// payloads aren't retained across the whole run; this helper is kept for
/// unit tests and any caller that already holds the full set.
pub fn extract_verification(tool_results: &[ActionResult]) -> CompletionVerification {
    let mut acc = VerificationAccumulator::new();
    for result in tool_results {
        acc.observe(result);
    }
    acc.finish()
}

/// Maximum chars the broad-sentinel scan inspects at the start of a
/// `complete_run` final answer. Admission phrases for the R26/R27/R28
/// shapes land in the opening of the summary; anchoring there keeps
/// broader phrases (like `"failed"` / `"not implemented"`) from
/// false-positing on non-admission prose further down.
///
/// R29 bumped this from 200 → 1000: the R29 summary opened with a
/// `**Completed:**` section listing a long GitHub URL + file paths
/// that pushed the `**Remaining tasks:**` admission past the prior
/// 200-char head. 1000 covers the full status prologue + transition
/// into an admission section for every observed shape while staying
/// bounded. Scan cost is one allocation of ≤1000 chars per DECIDE
/// turn; amortised cost vs. the LLM call that produced `final_answer`
/// is negligible.
///
/// If a future dogfood round surfaces an admission past 1000 chars,
/// consider (a) adding the phrase to [`HIGH_SPECIFICITY_FULL_BODY_SENTINELS`]
/// instead of widening the head, or (b) raising this to 1500 only if
/// the false-positive audit in `sentinel_scan_does_not_fire_on_legitimate_answers`
/// still passes.
const SENTINEL_SCAN_HEAD_CHARS: usize = 1000;

/// Phrases whose presence in the opening of a `complete_run`
/// `final_answer` indicates the model is self-reporting failure
/// rather than delivering a real result. R27 dogfood surfaced that
/// even with `ActionType::FailRun` published as a native tool,
/// glm-4.7 still picks `complete_run` and writes summaries like
/// `"Task incomplete. The following was NOT performed: ..."`.
///
/// Each phrase is lower-cased ASCII — the scanner lowercases the
/// head of the answer before matching so `"## Status: Blocked"`
/// matches `"status: blocked"` without the caller having to think
/// about casing.
///
/// Keep this list **narrow**. Additions must be:
/// * admissions of the run's terminal status — not neutral text that
///   happens to mention failure (a research report describing an
///   upstream failure is NOT an admission of failure by the current
///   run);
/// * phrases that appear at the TOP of the admission, because the
///   scan is head-anchored;
/// * distinctive enough that they don't fire on legitimate prose.
///
/// Every phrase here must be covered by the
/// `sentinel_scan_matches_r26_r27_admission_shapes` unit test.
const FAILURE_ADMISSION_SENTINELS: &[&str] = &[
    // R26 / R27 observed shapes — exact phrases the models emitted.
    "status: blocked",
    "status: partially complete",
    "task incomplete",
    "cannot provide",
    "cannot complete",
    "unable to complete",
    "unable to proceed",
    // R27 summary verb-specific variants — "was never executed"
    // ("cargo init was never run"), "was never run", "was never
    // written" (src/main.rs), "was never added" (deps), "was never
    // committed" (git flow). Anchored to the verb so legitimate
    // prose like "performance was never a concern" doesn't match.
    // The list is narrow by design; add a new verb here (and the
    // sentinel test) only when dogfood surfaces it.
    "was never executed",
    "was never run",
    "was never written",
    "was never added",
    "was never committed",
    "was never created",
    // Plural variants of the above ("were never performed", "were
    // never added", "were never committed"). Same anchoring
    // discipline — the bigram `were never` alone is too broad.
    "were never performed",
    "were never added",
    "were never completed",
    "were never committed",
    "were never run",
    // Bulleted-status shapes seen in R27: "- ❌ <item>" at the top.
    // We don't anchor on the ❌ emoji itself because that's not
    // reliably ASCII-decomposable, but the accompanying prose
    // "not performed" / "not completed" is.
    "not performed",
    "not completed",
    // #832 / R28: model reached further than R27 (wrote Cargo.toml,
    // src/main.rs, ran cargo check) but bailed at push+PR with
    // "The work is **nearly complete** but not fully finished".
    // The phrases below were all emitted in the same summary
    // opening — adding each so the scan catches this variant.
    "nearly complete",
    "not fully finished",
    "not yet complete",
    "still remaining",
    // "still remains to be <verb>ed" — R28 anticipated variant.
    // Gemini review (#834): the shorter "remains to be" would fire
    // on legitimate success phrases like "Nothing remains to be
    // done" or "It remains to be seen if..."; the "still" anchor
    // specialises the match to admissions of unfinished work.
    "still remains to be",
    // Section headers that (by R27 and R28 convention) prefix a
    // list of unfinished items. Head-anchored: the scan only fires
    // when the summary OPENS with a "Remaining:" section; free
    // text further down that mentions remaining work flows through.
    // Gemini review (#834): drop the trailing `:**` on the bold
    // variant so `**Remaining**:`, `**Remaining** :`, and bare
    // `**Remaining**` all match.
    "### remaining",
    "**remaining",
];

/// High-specificity sentinels that scan the FULL final_answer body,
/// not just the head. These catch admissions at any depth — R29
/// surfaced shapes that put the admission past the 1000-char head
/// of a verbose Completed:/Remaining: summary.
///
/// Contract: each phrase here falls into one of TWO categories
/// (Gemini review #836 noted the distinction should be explicit):
///
/// 1. **Structural section headers** — markdown artifacts that
///    introduce a list of unfinished items. Examples:
///    `**Remaining tasks:**`, `### Remaining tasks`. By convention
///    these only appear when the model is literally naming an
///    unfinished-work section; legitimate prose doesn't use them.
///
/// 2. **Closing-scaffold prose phrases** — multi-word fragments
///    the model uses to hedge when it knows the deliverable isn't
///    ready. Examples: `cannot be provided until`,
///    `the final deliverable cannot`. These carry slightly higher
///    false-positive risk than structural headers (a disclaimer in
///    an unrelated report could mention "cannot be provided until
///    <X> is received"), but the phrase specificity makes that
///    risk low in practice. Each prose entry here must be backed
///    by a dogfood observation — do NOT add speculative prose.
///
/// If an observed false positive fires against this list, narrow
/// the phrase, move a structural variant to category 1, or move
/// an over-broad prose variant to the head-anchored
/// [`FAILURE_ADMISSION_SENTINELS`] list instead.
const HIGH_SPECIFICITY_FULL_BODY_SENTINELS: &[&str] = &[
    // Category 1: structural section headers. R29 exact header
    // plus slight-variation siblings. The model used
    // `**Remaining tasks:**` at char ~500 of a summary that opened
    // with a `**Completed:**` list.
    "**remaining tasks:**",
    "**remaining tasks**",
    "### remaining tasks",
    "## remaining tasks",
    // Category 2: closing-scaffold prose phrases. R29's closing
    // sentence explicitly said `"The final deliverable ... cannot be
    // provided until these steps are completed."` Both fragments are
    // model scaffolding — operators don't legitimately emit a
    // final_answer that describes what cannot be provided by the
    // run itself.
    "cannot be provided until",
    "the final deliverable cannot",
];

/// Scan the opening of a `complete_run` proposal's `final_answer` for
/// phrases that indicate the model is self-reporting failure. Returns
/// the first matched sentinel if one is present, otherwise `None`.
///
/// The strict completion gate in `loop_runner` calls this as a third
/// reject condition (alongside `errors_since_baseline > 0` and
/// `stalled_since_rejection`). See the [`FAILURE_ADMISSION_SENTINELS`]
/// rustdoc for the list and the contract additions must honour.
///
/// R26/R27 pathology: models had `ActionType::FailRun` available
/// (published in tool_defs, documented in the role prompts) and still
/// picked `complete_run` for admitted failures. This scan is the
/// server-side backstop for that non-compliance.
pub fn detect_self_reported_failure(final_answer: &str) -> Option<&'static str> {
    // Head-scan: bounded to `SENTINEL_SCAN_HEAD_CHARS`, matches the
    // broad [`FAILURE_ADMISSION_SENTINELS`] list. Admissions for most
    // observed shapes land in the opening of the summary; anchoring
    // there keeps broader phrases from false-positing on
    // non-admission prose further down.
    //
    // Lower-case each char during iteration so the scan runs against
    // a single allocation. `to_ascii_lowercase` on a char is a no-op
    // for non-ASCII; the sentinels are all ASCII so non-ASCII chars
    // can't contribute to a match.
    let head: String = final_answer
        .chars()
        .take(SENTINEL_SCAN_HEAD_CHARS)
        .map(|c| c.to_ascii_lowercase())
        .collect();

    if let Some(hit) = FAILURE_ADMISSION_SENTINELS
        .iter()
        .find(|sentinel| head.contains(*sentinel))
        .copied()
    {
        return Some(hit);
    }

    // Full-body scan: match the narrower
    // [`HIGH_SPECIFICITY_FULL_BODY_SENTINELS`] list against the
    // entire final_answer. R29 surfaced shapes that put the admission
    // past the 1000-char head (verbose Completed:/Remaining: summaries
    // with long file paths / URLs pushing text forward). Each phrase
    // here is constrained to be essentially impossible in legitimate
    // prose, so scanning the full body is safe.
    //
    // Lower-cases the whole body in one pass. For a pathological
    // multi-megabyte final_answer this allocates once — still cheap
    // compared to the LLM call.
    let lower = final_answer.to_ascii_lowercase();
    HIGH_SPECIFICITY_FULL_BODY_SENTINELS
        .iter()
        .find(|sentinel| lower.contains(*sentinel))
        .copied()
}

/// Yield each scannable `&str` from an `ActionResult` without cloning. For
/// structured JSON tool outputs we look at a small set of canonical key
/// names (`stdout`, `stderr`, `output`, `message`, …) rather than the
/// whole blob so the scanner doesn't match on unrelated JSON keys like
/// `"warning_count": 3`. Falls back to the entire object's `to_string()`
/// only when no canonical key matched — that fallback path does allocate
/// but covers tool adapters that flatten their payload into a
/// non-standard shape.
///
/// **Only scans `tool_output`.** `ActionStatus::Failed.reason` is
/// deliberately excluded: those strings (e.g. `"Error [NOT_FOUND]: File
/// not found: …"`) are tool-invocation-layer bookkeeping — the tool
/// couldn't run, argument invalid, file missing, etc. — not build /
/// compile diagnostics. F35 already handles these via
/// `LoopSignal::Continue` so the LLM can adapt. Routing them into the
/// `errors` bucket would cause the #660 completion gate to reject
/// `complete_run` on benign exploration failures (agent trying to read
/// a file that doesn't exist yet) that have nothing to do with whether
/// the produced code builds. The gate must fire only on real tool
/// output — cargo/rustc/clippy/pytest/ruff diagnostics — which lives in
/// `tool_output`, not in the orchestrator's bookkeeping reason.
fn iter_text_sources(result: &ActionResult) -> Vec<std::borrow::Cow<'_, str>> {
    use std::borrow::Cow;
    let mut out: Vec<Cow<'_, str>> = Vec::new();
    if let Some(value) = result.tool_output.as_ref() {
        match value {
            Value::String(s) => out.push(Cow::Borrowed(s.as_str())),
            Value::Object(map) => {
                for key in ["stdout", "stderr", "output", "message", "text", "content"] {
                    if let Some(Value::String(s)) = map.get(key) {
                        out.push(Cow::Borrowed(s.as_str()));
                    }
                }
                if out.is_empty() {
                    // Only rarely reached — serialises the whole object
                    // once, not the full tool_result.
                    out.push(Cow::Owned(value.to_string()));
                }
            }
            other => out.push(Cow::Owned(other.to_string())),
        }
    }
    out
}

/// Extract an exit code from a bash-class tool_output if structurally
/// present. Accepts several canonical key names used by different harness
/// adapters (`exit_code`, `exitCode`, `returncode`, …). Non-integer values
/// return `None` rather than coercing.
pub(crate) fn extract_exit_code(output: &Value) -> Option<i32> {
    for key in [
        "exit_code",
        "exitCode",
        "returncode",
        "return_code",
        "status",
    ] {
        if let Some(v) = output.get(key) {
            if let Some(n) = v.as_i64() {
                return i32::try_from(n).ok();
            }
        }
    }
    None
}

/// Case-insensitive ASCII prefix match on the leading bytes of `line`.
/// Byte-level comparison avoids any UTF-8 boundary pitfall when non-ASCII
/// tool output is mixed in: the prefix we match on (`warning`, `error`) is
/// ASCII, so comparing bytes is well-defined whether or not subsequent
/// bytes start a multi-byte sequence. Cursor / Gemini review flagged the
/// earlier `str::get(..N)` char-slice form for missing lines like
/// `"warning: über-thing"` when the char index landed inside a multi-byte
/// char; this helper closes that gap.
fn ascii_prefix_ci(line: &str, prefix: &str) -> bool {
    let bytes = line.as_bytes();
    let pb = prefix.as_bytes();
    if bytes.len() < pb.len() {
        return false;
    }
    bytes[..pb.len()]
        .iter()
        .zip(pb.iter())
        .all(|(a, b)| a.eq_ignore_ascii_case(b))
}

/// Match `warning:` at the start of a (left-trimmed) line. Case-insensitive
/// but ASCII-only — `WARN:` and other variants are intentionally NOT
/// matched. The scanner is tuned for `rustc`/`cargo`/`clippy` and the
/// generic `warning:` prefix, which covers the M1 dogfood regression.
fn is_warning_line(line: &str) -> bool {
    ascii_prefix_ci(line, "warning:")
}

/// Match `error:` / `error[…]:` / `error <ws>` at the start of a
/// left-trimmed line. Accepts the Rust diagnostic form
/// `error[E0308]: mismatched types` as well as plain `error:` from bash
/// output. Rejects `errored out`, `errorless` etc. so false positives in
/// ordinary prose stay out of the bucket.
fn is_error_line(line: &str) -> bool {
    if !ascii_prefix_ci(line, "error") {
        return false;
    }
    // Byte indexing is safe here — `error` is ASCII so the 5th byte lies
    // on a valid char boundary regardless of subsequent multi-byte bytes.
    matches!(
        line.as_bytes().get("error".len()),
        Some(b':') | Some(b'[') | Some(b' ') | Some(b'\t')
    )
}

/// Clip `s` to `max` chars, appending an ellipsis marker when trimmed.
/// Walks `char_indices` and stops as soon as the cap is known to be
/// exceeded — work is proportional to `max`, not to the full string
/// length. This matters for pathological tool outputs (MB-scale
/// single-line payloads) where `s.chars().count()` would scan the
/// entire buffer.
fn truncate(s: &str, max: usize) -> String {
    // Fast path: ASCII strings that fit.
    if s.len() <= max && s.is_ascii() {
        return s.to_owned();
    }
    // Walk up to `max` chars. Track the byte offset where the `max-3`rd
    // char ended, so we can truncate there on overflow.
    let mut seen = 0usize;
    let mut split_at = 0usize;
    let cutoff = max.saturating_sub(3);
    for (idx, ch) in s.char_indices() {
        if seen == cutoff {
            split_at = idx;
        }
        seen += 1;
        if seen > max {
            let mut out = String::with_capacity(split_at + 3);
            out.push_str(&s[..split_at]);
            out.push_str("...");
            return out;
        }
        // Keep `split_at` up to date for the terminating case where the
        // string is exactly `max` chars long (no truncation needed).
        if seen == max {
            split_at = idx + ch.len_utf8();
        }
    }
    // Reached end within the cap — no truncation needed.
    s.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{ActionResult, ActionStatus};
    use cairn_domain::ActionProposal;
    use serde_json::json;

    fn bash_result(command: &str, stdout: &str, exit_code: Option<i32>) -> ActionResult {
        let mut output = serde_json::Map::new();
        output.insert("stdout".into(), Value::String(stdout.to_owned()));
        if let Some(code) = exit_code {
            output.insert("exit_code".into(), Value::from(code));
        }
        ActionResult {
            proposal: ActionProposal::invoke_tool(
                "bash",
                json!({ "command": command }),
                "run a shell command",
                0.95,
                false,
            ),
            status: ActionStatus::Succeeded,
            tool_output: Some(Value::Object(output)),
            invocation_id: None,
            duration_ms: 0,
        }
    }

    fn failed_bash(command: &str, reason: &str) -> ActionResult {
        ActionResult {
            proposal: ActionProposal::invoke_tool(
                "bash",
                json!({ "command": command }),
                "run a shell command",
                0.95,
                false,
            ),
            status: ActionStatus::Failed {
                reason: reason.to_owned(),
            },
            tool_output: None,
            invocation_id: None,
            duration_ms: 0,
        }
    }

    /// (a) Two warnings + one error on a cargo-check-style payload must
    /// land in the right buckets with the matching text preserved.
    #[test]
    fn warnings_and_errors_are_bucketed() {
        let cargo_stdout = "\
warning: unused imports: `Constraint`, `Direction`
  --> src/lib.rs:1:5
warning: function `dead` is never used
  --> src/lib.rs:10:4
error[E0308]: mismatched types
  --> src/lib.rs:20:1
";
        let v = extract_verification(&[bash_result("cargo check", cargo_stdout, Some(1))]);

        assert_eq!(v.extractor_version, EXTRACTOR_VERSION);
        assert_eq!(v.tool_results_scanned, 1);
        assert_eq!(v.warnings.len(), 2, "warnings: {:#?}", v.warnings);
        assert!(v.warnings[0].contains("unused imports"));
        assert!(v.warnings[1].contains("never used"));
        assert_eq!(v.errors.len(), 1, "errors: {:#?}", v.errors);
        assert!(v.errors[0].contains("mismatched types"));
        assert_eq!(v.commands.len(), 1);
        assert_eq!(v.commands[0].tool_name, "bash");
        assert_eq!(v.commands[0].cmd, "cargo check");
        assert_eq!(v.commands[0].exit_code, Some(1));
    }

    /// (b) Clean tool output produces empty warning / error vectors but a
    /// non-zero `tool_results_scanned` — this is the "verified clean"
    /// signal operators rely on.
    #[test]
    fn clean_output_yields_empty_buckets_with_nonzero_scan() {
        let v = extract_verification(&[bash_result("echo hello", "hello\n", Some(0))]);
        assert!(v.warnings.is_empty());
        assert!(v.errors.is_empty());
        assert_eq!(v.tool_results_scanned, 1);
        assert_eq!(v.commands[0].exit_code, Some(0));
    }

    /// Empty input produces defaults — distinguishes "no tool calls" from
    /// "scanned and found nothing."
    #[test]
    fn empty_input_produces_default_with_version_stamp() {
        let v = extract_verification(&[]);
        assert_eq!(v.tool_results_scanned, 0);
        assert_eq!(v.extractor_version, EXTRACTOR_VERSION);
        assert!(v.warnings.is_empty());
        assert!(v.errors.is_empty());
        assert!(v.commands.is_empty());
    }

    /// (c) Truncation. Synthesise 60 warning lines and a single 700-char
    /// warning line; verify the cap at 50 entries and the 500-char per-line
    /// trim.
    #[test]
    fn truncation_caps_entries_and_line_length() {
        let mut stdout = String::new();
        for i in 0..60 {
            stdout.push_str(&format!("warning: lint {i}\n"));
        }
        stdout.push_str(&format!("warning: {}\n", "x".repeat(700)));

        let v = extract_verification(&[bash_result("cargo clippy", &stdout, Some(0))]);

        assert_eq!(
            v.warnings.len(),
            MAX_ENTRIES_PER_BUCKET,
            "must cap at {MAX_ENTRIES_PER_BUCKET}"
        );
        for w in &v.warnings {
            assert!(
                w.chars().count() <= MAX_LINE_LEN,
                "line of {} chars exceeded {} cap: {w}",
                w.chars().count(),
                MAX_LINE_LEN,
            );
        }
    }

    /// (d) Exit code surfaces when present; `None` when absent. The
    /// extractor never fabricates `0`.
    #[test]
    fn exit_code_surfaces_only_when_structurally_present() {
        let with_code = extract_verification(&[bash_result("true", "", Some(0))]);
        assert_eq!(with_code.commands[0].exit_code, Some(0));

        let without_code = extract_verification(&[bash_result("true", "", None)]);
        assert_eq!(without_code.commands[0].exit_code, None);
    }

    /// Non-bash tools contribute text scanning but produce no CommandOutcome.
    #[test]
    fn non_bash_tool_contributes_scan_but_no_command_entry() {
        let result = ActionResult {
            proposal: ActionProposal::invoke_tool(
                "read",
                json!({ "path": "/tmp/x" }),
                "read a file",
                0.9,
                false,
            ),
            status: ActionStatus::Succeeded,
            tool_output: Some(json!({ "content": "warning: header mismatch\nok" })),
            invocation_id: None,
            duration_ms: 0,
        };
        let v = extract_verification(&[result]);
        assert_eq!(v.warnings.len(), 1);
        assert!(v.commands.is_empty(), "read is not bash-class");
        assert_eq!(v.tool_results_scanned, 1);
    }

    /// #660 regression guard: `ActionStatus::Failed.reason` strings are
    /// tool-invocation-layer bookkeeping (file not found, invalid args,
    /// etc.) — NOT build diagnostics. They must not flow into the
    /// `errors` bucket because the strict completion gate consults that
    /// bucket to decide whether to reject `complete_run`. If these
    /// benign tool-runtime failures poisoned the count, the gate would
    /// fire on every exploration attempt (agent reads a file that
    /// doesn't exist yet, etc.) which F35 explicitly routes as
    /// `LoopSignal::Continue`. The test at
    /// `crates/cairn-app/tests/test_f35_tool_errors_as_feedback.rs`
    /// guards the end-to-end behavior; this unit test pins the
    /// scanner's contract.
    #[test]
    fn failed_tool_reason_does_not_leak_into_buckets() {
        let v = extract_verification(&[failed_bash(
            "cat /nope",
            "Error [NOT_FOUND]: File not found: /nope",
        )]);
        assert!(
            v.errors.is_empty(),
            "Failed.reason must not feed the errors bucket; got {:?}",
            v.errors
        );
        assert!(v.warnings.is_empty());
        // But the tool_results_scanned counter still advances — the
        // scanner saw the frame, it just had nothing to bucket.
        assert_eq!(v.tool_results_scanned, 1);
    }

    /// Rust's bracketed diagnostic form `error[E0308]: …` must match the
    /// error bucket — this is the shape that motivated the F47 triage.
    #[test]
    fn rust_bracketed_error_form_matches() {
        assert!(is_error_line("error[E0308]: mismatched types"));
        assert!(is_error_line("error: linker failed"));
        assert!(!is_error_line("errored out"));
        assert!(!is_error_line("errorless"));
    }

    /// Multi-byte characters after the ASCII prefix must not break
    /// detection. Cursor / Gemini review flagged this as a real hazard —
    /// a `"warning: über-thing"` line would have been silently missed by
    /// the earlier `str::get(..N)` char-slice form when byte 10 landed
    /// inside the 2-byte encoding of `ü`.
    #[test]
    fn multi_byte_characters_do_not_break_detection() {
        assert!(is_warning_line("warning: über-thing went wrong"));
        assert!(is_error_line("error[E0308]: mismatched types — 😤"));
        assert!(!is_warning_line("über-thing: not a warning"));
    }

    /// Incremental accumulator behaves identically to the batch helper
    /// for the same inputs. The loop runner uses the incremental form to
    /// avoid retaining full `tool_output` across the whole run.
    #[test]
    fn accumulator_matches_batch_helper() {
        let inputs = [
            bash_result("cargo check", "warning: a\nerror: b\n", Some(1)),
            bash_result("ls", "hello\n", Some(0)),
        ];
        let batch = extract_verification(&inputs);
        let mut acc = VerificationAccumulator::new();
        for r in &inputs {
            acc.observe(r);
        }
        let incremental = acc.finish();
        assert_eq!(batch, incremental);
    }

    /// #821 / #823 review (Gemini high-pri): `errors_since_baseline()`
    /// must NOT freeze at 0 once the preview bucket fills to
    /// [`MAX_ENTRIES_PER_BUCKET`]. Pre-fix path:
    /// `mark_baseline()` snapshotted `errors.len()` (capped at 50);
    /// at full bucket the next `errors_since_baseline()` returned
    /// `50 - 50 = 0` even when fresh errors were still arriving,
    /// opening a strict-gate bypass. Post-fix path tracks an uncapped
    /// `errors_total_observed` counter that increments on every error
    /// line — the slice for the rejection preview stays bounded by
    /// the bucket, but the gate's count is the truth.
    #[test]
    fn errors_since_baseline_keeps_counting_past_bucket_cap() {
        // 60 error lines is 10 over the cap. The preview bucket
        // tops out at 50; the total counter should reach 60.
        let mut stdout = String::new();
        for i in 0..60 {
            stdout.push_str(&format!("error: synthetic {i}\n"));
        }
        let mut acc = VerificationAccumulator::new();
        acc.observe(&bash_result("cargo check", &stdout, Some(1)));

        assert_eq!(
            acc.errors().len(),
            MAX_ENTRIES_PER_BUCKET,
            "preview bucket caps at {MAX_ENTRIES_PER_BUCKET}"
        );
        assert_eq!(
            acc.errors_since_baseline(),
            60,
            "uncapped counter must reflect every observed error \
             before mark_baseline()"
        );

        // Mark baseline; pre-fix this would lock the count at 0
        // forever. Post-fix, fresh errors keep advancing the count.
        acc.mark_baseline();
        assert_eq!(
            acc.errors_since_baseline(),
            0,
            "right after mark_baseline the delta is 0"
        );

        // Add 5 more error lines via a second tool result. The
        // preview bucket is already full so these don't appear in
        // `errors_since_baseline_slice()`, but the count advances.
        let mut more = String::new();
        for i in 0..5 {
            more.push_str(&format!("error: post-baseline {i}\n"));
        }
        acc.observe(&bash_result("cargo check", &more, Some(1)));

        assert_eq!(
            acc.errors().len(),
            MAX_ENTRIES_PER_BUCKET,
            "preview bucket stays at cap (no new pushes)"
        );
        assert_eq!(
            acc.errors_since_baseline(),
            5,
            "uncapped counter must report 5 fresh errors past the \
             bucket cap; this is the strict-gate bypass Gemini caught \
             on PR #823"
        );

        // The slice for the rejection preview is bounded by the
        // bucket — operators see a sample, the gate sees the truth.
        // After mark_baseline at full bucket, the slice is empty
        // (no entries are PUSHED past the cap), which is fine: the
        // gate decision uses the count, not the slice.
        assert!(
            acc.errors_since_baseline_slice().is_empty(),
            "preview slice cannot show entries that didn't push"
        );
    }

    /// Sanity: `mark_baseline()` followed by `mark_baseline()` is a
    /// no-op on the uncapped counter (no errors observed between
    /// them), so the gate sees 0. This is the well-behaved path —
    /// the test above covered the past-cap edge case.
    #[test]
    fn double_mark_baseline_with_no_intervening_errors_reports_zero() {
        let mut acc = VerificationAccumulator::new();
        acc.observe(&bash_result("ls", "hello\n", Some(0)));
        acc.mark_baseline();
        assert_eq!(acc.errors_since_baseline(), 0);
        acc.mark_baseline();
        assert_eq!(acc.errors_since_baseline(), 0);
    }

    /// #823 review (Gemini): `made_progress_since_baseline()` must
    /// return false after a rejection until the model issues at
    /// least one tool call. The strict completion gate uses this to
    /// refuse complete_run when the model retries without doing
    /// any work between rejections — closes the bypass that the
    /// errors_since_baseline-only check left open.
    #[test]
    fn made_progress_since_baseline_tracks_tool_calls_after_rejection() {
        let mut acc = VerificationAccumulator::new();

        // Before any rejection there is no baseline; "progress" is
        // vacuously true so the gate doesn't block the very first
        // complete_run attempt unless errors say so.
        assert!(
            acc.made_progress_since_baseline(),
            "no baseline yet → vacuously progressed"
        );

        // Simulate the gate rejecting a complete_run.
        acc.mark_baseline();
        assert!(
            !acc.made_progress_since_baseline(),
            "right after mark_baseline, model has done nothing yet"
        );

        // Model retries complete_run with no tool calls. The gate
        // should still see "no progress" — this is the bypass we're
        // refusing.
        acc.mark_baseline();
        assert!(
            !acc.made_progress_since_baseline(),
            "another rejection with no tool call between → still no progress"
        );

        // Now the model actually runs a tool. Progress = true.
        acc.observe(&bash_result("ls", "hello\n", Some(0)));
        assert!(
            acc.made_progress_since_baseline(),
            "tool call after baseline counts as progress"
        );

        // Next rejection resets progress to false again.
        acc.mark_baseline();
        assert!(!acc.made_progress_since_baseline());
    }

    // ── #830: sentinel-scan on complete_run.final_answer ────────────────────

    /// Every R26/R27 observed admission shape must match. If a new
    /// dogfood round surfaces a new shape, add it here before
    /// extending [`FAILURE_ADMISSION_SENTINELS`] — the dogfood
    /// finding is the contract.
    #[test]
    fn sentinel_scan_matches_r26_r27_admission_shapes() {
        // R26 (M1-7): "## Status: Blocked - src/main.rs does not exist"
        assert!(detect_self_reported_failure(
            "## Status: Blocked - src/main.rs does not exist\n\nThe repository was cloned..."
        )
        .is_some());

        // R26 (M1-8): "**Status: Partially Complete - Cannot provide final deliverables**"
        assert!(detect_self_reported_failure(
            "**Status: Partially Complete - Cannot provide final deliverables**"
        )
        .is_some());

        // R27 (M1-1): "Task incomplete. The repository was cloned and branch ..."
        assert!(detect_self_reported_failure(
            "Task incomplete. The repository was cloned and branch `m1/01-cargo-init` \
             was created, but the following required steps were NOT performed:\n\
             1. `cargo init --name roguelike` was never executed (no Cargo.toml or src/ exists)"
        )
        .is_some());

        // R27 shape — specific verb forms the executor's summary used.
        assert!(
            detect_self_reported_failure("The cargo check was never run to verify the build.")
                .is_some()
        );
        assert!(
            detect_self_reported_failure("cargo init was never executed").is_some(),
            "R27 exact phrase"
        );
        assert!(
            detect_self_reported_failure("src/main.rs was never written").is_some(),
            "R27 exact phrase"
        );
        assert!(
            detect_self_reported_failure("Dependencies were never added to Cargo.toml").is_some(),
            "R27 plural-verb exact phrase"
        );

        // "Cannot provide" / "Unable to complete" variants.
        assert!(detect_self_reported_failure("Cannot provide the requested files.").is_some());
        assert!(detect_self_reported_failure("Unable to complete this task.").is_some());
    }

    /// Case-insensitivity is a contract — the scan lower-cases the
    /// head before matching. Models emit these phrases with varying
    /// capitalisation (`## Status: Blocked`, `STATUS: BLOCKED`, etc.)
    /// so the scan must catch all of them without per-case sentinels.
    #[test]
    fn sentinel_scan_is_case_insensitive() {
        assert!(detect_self_reported_failure("STATUS: BLOCKED - missing dep").is_some());
        assert!(detect_self_reported_failure("Status: Blocked - missing dep").is_some());
        assert!(detect_self_reported_failure("status: blocked - missing dep").is_some());
    }

    /// Which sentinel matched is surfaced verbatim in the rejection
    /// `StepSummary` so the model sees the exact phrase that tripped
    /// the gate. Assert that the returned &str is actually one of
    /// the configured sentinels (not some reconstructed substring).
    #[test]
    fn sentinel_scan_returns_the_matched_phrase() {
        let matched = detect_self_reported_failure("## Status: Blocked - missing dep")
            .expect("expected a sentinel match");
        assert_eq!(matched, "status: blocked");
    }

    /// Regression: **legitimate** complete_run answers must NOT
    /// match a sentinel. The scan is head-anchored so a bullet list
    /// or prose answer with no admission prefix flows through.
    #[test]
    fn sentinel_scan_does_not_fire_on_legitimate_answers() {
        // Factual prose answer.
        assert!(detect_self_reported_failure("Paris is the capital of France.").is_none());

        // Structured bullet deliverable.
        assert!(detect_self_reported_failure(
            "Changes applied:\n- Renamed parse_raw → parse_input in parser.rs:42\n\
             - Updated caller at lib.rs:88\n- cargo check -p bar passes."
        )
        .is_none());

        // A researcher's report mentioning an upstream failure far
        // from the top — past the SENTINEL_SCAN_HEAD_CHARS window.
        let padding = "a".repeat(SENTINEL_SCAN_HEAD_CHARS);
        let legitimate = format!(
            "Summary of findings.\n\n{padding}\nThe upstream API failed when X (unrelated)."
        );
        assert!(
            detect_self_reported_failure(&legitimate).is_none(),
            "text past the head-scan window must NOT fire the gate"
        );

        // An answer that happens to use words from the sentinel list
        // in non-admission contexts.
        assert!(
            detect_self_reported_failure(
                "The algorithm works as follows. Performance was never a concern because X."
            )
            .is_none(),
            "free text with 'was never' but no terminal-status admission must not match"
        );
    }

    /// Empty / whitespace-only final_answer: no match. Those are a
    /// different kind of bug (complete_run without a useful summary)
    /// that decide_impl's missing-final_answer fallback handles by
    /// escalating to the operator.
    #[test]
    fn sentinel_scan_empty_answer_does_not_match() {
        assert!(detect_self_reported_failure("").is_none());
        assert!(detect_self_reported_failure("   \n\n\t ").is_none());
    }

    /// #835 / R29 regression: verbatim R29 summary opened with a
    /// long Completed: bullet list (GitHub URL + file paths) that
    /// pushed the `**Remaining tasks:**` admission past the old
    /// 200-char head window. The new 1000-char window plus the
    /// full-body high-specificity sentinel set both catch it.
    #[test]
    fn sentinel_scan_matches_r29_past_200_char_admission() {
        // Verbatim R29 summary from run_subagent_child_task_1778420725297_61.
        let r29_summary = "The goal was to set up a Rust roguelike project with \
                           ratatui+crossterm scaffolding and create a PR. Based on \
                           the step history, here is the current status:\n\n\
                           **Completed:**\n\
                           - ✓ Cloned https://github.com/avifenesh/cairn-dogfood-roguelike \
                           to /tmp/cairn-runs/dogfood-r29-m1-1-1778419867\n\
                           - ✓ Created branch m1/01-cargo-init\n\
                           - ✓ Ran cargo init --name roguelike\n\
                           - ✓ Added ratatui=0.27 and crossterm=0.28 to Cargo.toml\n\
                           - ✓ Replaced src/main.rs with a crossterm+ratatui stub \
                           (51 lines)\n\n\
                           **Remaining tasks:**\n\
                           - ⚠ Run cargo check and verify zero warnings\n\
                           - ⚠ Commit with message \"M1-1: Initialize Rust binary \
                           crate with ratatui+crossterm (#1)\"\n\
                           - ⚠ Push the branch\n\
                           - ⚠ Create PR with gh pr create\n";

        let matched = detect_self_reported_failure(r29_summary).expect(
            "#835: R29 summary MUST match — either via widened head scan \
                     hitting '**remaining' or via high-specificity full-body scan \
                     hitting '**remaining tasks:**'",
        );

        // The widened head now includes "**Remaining tasks:**" so the
        // head-scan's "**remaining" sentinel from #832 catches it
        // first. Either match is acceptable; the assertion names both
        // so a future refactor that shifts the detection path still
        // passes.
        assert!(
            matched == "**remaining" || matched == "**remaining tasks:**",
            "expected the '**remaining' head sentinel or the \
             '**remaining tasks:**' full-body sentinel; got {matched:?}"
        );
    }

    /// #835: the high-specificity full-body sentinels must match
    /// even when the admission lives past the head window. Each
    /// phrase tested individually so a future narrowing of the list
    /// still passes the specific case.
    #[test]
    fn sentinel_scan_high_specificity_full_body_sentinels_match_past_head() {
        let padding = "a".repeat(SENTINEL_SCAN_HEAD_CHARS + 200);

        // `**Remaining tasks:**` buried past the head — the primary
        // R29 shape.
        let s = format!("Summary of work.\n\n{padding}\n\n**Remaining tasks:**\n- push");
        assert!(
            detect_self_reported_failure(&s).is_some(),
            "#835: '**remaining tasks:**' past the head must match via \
             the full-body high-specificity scan"
        );

        // Variant without trailing colon.
        let s = format!("Summary.\n\n{padding}\n\n### Remaining tasks\n- commit");
        assert!(
            detect_self_reported_failure(&s).is_some(),
            "#835: '### remaining tasks' heading variant must match"
        );

        // "Cannot be provided until" — R29 closing admission.
        let s = format!("Summary.\n\n{padding}\n\nThe PR URL cannot be provided until we push.");
        assert!(
            detect_self_reported_failure(&s).is_some(),
            "#835: 'cannot be provided until' must match anywhere in body"
        );
    }

    /// #835 regression: the high-specificity list must NOT fire on
    /// legitimate prose even with the full-body scan. Each phrase
    /// was chosen to be essentially impossible in non-admission
    /// text, but we still assert that legitimate answers flow.
    #[test]
    fn sentinel_scan_full_body_does_not_fire_on_legitimate_long_summaries() {
        // A real change summary that's long, detailed, and mentions
        // words from the sentinel list in non-admission contexts.
        let legit = "Renamed parse_raw to parse_input across the public API. \
            Touched 3 files: parser.rs (line 42, definition), lib.rs (line 88, \
            re-export), tests/integration.rs (line 14, caller). Ran cargo check \
            -p bar — passes with zero warnings. Ran cargo test -p bar — 47 \
            tests pass, 0 failures, 0 ignored. Committed as 'refactor: rename \
            parse_raw -> parse_input'. PR opened at https://github.com/x/y/pull/42. \
            Downstream consumers were notified via the #releases channel. This \
            change is backwards-compatible because the old name remains as a \
            #[deprecated] re-export for one release cycle. Nothing further \
            remains to be done in this PR; follow-up work tracked at #XYZ.";
        assert!(
            detect_self_reported_failure(legit).is_none(),
            "legitimate long success summary must not false-positive on any \
             head or full-body sentinel"
        );

        // A researcher's report that explicitly talks about unfinished
        // upstream work but is itself a completed run. This is the
        // shape most at risk of a false positive — it genuinely
        // describes something incomplete, but the subject isn't the
        // run's own deliverable.
        let research = "Summary of findings on the upstream library's state. \
            The project's public API is stable but several advanced features \
            remain underdocumented. Specifically, 3 of the 7 extension points \
            lack worked examples. My recommendation is to file upstream issues \
            requesting docs for each. This completes the scoped research; see \
            sections below for the full citation trail and evidence for each \
            claim.";
        assert!(
            detect_self_reported_failure(research).is_none(),
            "researcher's report describing upstream incompleteness must not \
             match — the run's own deliverable (the report) is complete"
        );
    }

    /// #832 / R28 regression: glm-4.7's M1-1 sub-agent wrote
    /// Cargo.toml, src/main.rs, ran cargo check (passed), committed
    /// — then bailed at push+PR with a summary opening `"The work is
    /// **nearly complete** but not fully finished. Here's what was
    /// done: ... ### Remaining: 1. Restore .gitignore 2. Push the
    /// branch 3. Create PR"`. The original R26/R27 sentinels missed
    /// all four signal phrases. Pin each so R28 doesn't come back.
    #[test]
    fn sentinel_scan_matches_r28_admission_shapes() {
        // Exact R28 opening — "nearly complete" anchors the scan.
        assert!(detect_self_reported_failure(
            "## Progress Summary\n\nThe work is **nearly complete** but not fully finished. \
             Here's what was done:"
        )
        .is_some());

        // Each phrase individually — the list is what the scan
        // keys on, so assert them one-for-one.
        assert!(detect_self_reported_failure("Work is nearly complete.").is_some());
        assert!(detect_self_reported_failure("not fully finished — a few steps remain").is_some());
        assert!(
            detect_self_reported_failure("Status: not yet complete; pending PR creation").is_some()
        );
        assert!(detect_self_reported_failure("Still remaining: push + PR.").is_some());
        // Gemini review (#834): matches the full specific anchor
        // "still remains to be" so legitimate "nothing remains to
        // be done" / "it remains to be seen" don't false-positive.
        assert!(detect_self_reported_failure(
            "Summary: work still remains to be pushed to origin."
        )
        .is_some());

        // Section-header anchors. R27 and R28 both used "### Remaining:"
        // and "**Remaining:**" as a bulleted list of unfinished
        // items at the top of the summary. Matching the header
        // anchors catches the shape even if the surrounding prose
        // doesn't happen to hit another sentinel.
        assert!(detect_self_reported_failure(
            "Summary of changes.\n\n### Remaining:\n1. Push the branch"
        )
        .is_some());
        assert!(detect_self_reported_failure(
            "Done:\n- wrote code\n\n**Remaining:**\n- push to origin"
        )
        .is_some());
        // Gemini review (#834): the bold anchor must tolerate
        // variation in colon placement / missing colon so subtle
        // markdown drift ("**Remaining**:" vs "**Remaining:**" vs
        // just "**Remaining**") all match.
        assert!(detect_self_reported_failure(
            "Done:\n- wrote code\n\n**Remaining**:\n- push to origin"
        )
        .is_some());
        assert!(detect_self_reported_failure(
            "Done:\n- wrote code\n\n**Remaining**\n- push to origin"
        )
        .is_some());
    }

    /// #832 regression: legitimate summaries that happen to include
    /// the word "remaining" or "complete" must NOT fire. The head
    /// anchor and phrase specificity are the defence.
    #[test]
    fn sentinel_scan_does_not_fire_on_legitimate_r28_shape_near_misses() {
        // Gemini review (#834): test the exact false-positive
        // shapes the narrowed sentinel is designed to avoid. Before
        // the narrowing, the shorter "remains to be" bigram would
        // have matched these — the specific "still remains to be"
        // anchor keeps them safely out.
        assert!(
            detect_self_reported_failure("Nothing remains to be done.").is_none(),
            "'remains to be' without 'still' must not match on a success shape"
        );
        assert!(
            detect_self_reported_failure("It remains to be seen whether the user accepts.")
                .is_none(),
            "idiomatic 'it remains to be seen' must not match"
        );

        // Real change summary that lists files left to test but
        // doesn't self-admit run failure.
        assert!(
            detect_self_reported_failure(
                "Renamed parse_raw -> parse_input. All callers updated. \
             cargo check -p bar passes. It remains to be seen if the \
             user requires further changes — filing #XYZ to track."
            )
            .is_none(),
            "'remains to be' in non-admission prose must not match"
        );

        // A researcher's report mentioning "nearly complete" in a
        // citation about something else. Past the 200-char head the
        // scan is silent.
        let padding = "a".repeat(SENTINEL_SCAN_HEAD_CHARS);
        let legit =
            format!("Summary of findings.\n\n{padding}\nThe upstream library is nearly complete.");
        assert!(
            detect_self_reported_failure(&legit).is_none(),
            "post-head 'nearly complete' must not match"
        );

        // "completed" in a success shape — opposite of "not
        // completed". The scan must not false-positive.
        assert!(
            detect_self_reported_failure("Task completed. All acceptance criteria satisfied.")
                .is_none()
        );
    }
}
