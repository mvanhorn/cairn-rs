//! RFC 031 §Prompt Engineering Contract — structural validator for
//! `AgentRole`. Runs at `POST /v1/projects/:project/agent-roles` time
//! (and `PATCH`) before any event emits, so malformed roles never
//! reach the projection.
//!
//! This module owns **structure**, not semantics. "Is this prompt
//! good?" is the operator's problem; "does this prompt have the five
//! sections we need for the DECIDE loop" is the validator's.
//!
//! See [`validate_prompt_structure`] for the entry point and the
//! `FailureCode` enum for the wire-visible failure taxonomy.

use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::agent_roles::{AgentRole, AgentRoleTier};

/// Role id namespace (RFC 031 §D5).
///
/// `[a-z0-9][a-z0-9_-]*` with length ≤ 64. Matched case-sensitively.
pub const ROLE_ID_MAX_LEN: usize = 64;

/// Per-field caps per RFC 031 §D4. These are the **per-field** ceilings
/// that the HTTP handler maps to 413 PAYLOAD_TOO_LARGE; the router also
/// layers a separate 128 KiB **total-body** cap via `DefaultBodyLimit`
/// that covers the entire JSON request (all fields + framing). A role
/// with a 64 KiB `system_prompt` plus small name/description/tools still
/// fits inside the 128 KiB body envelope with room for JSON framing.
pub const SYSTEM_PROMPT_MAX_BYTES: usize = 65_536;
pub const NAME_MAX_CHARS: usize = 128;
pub const DESCRIPTION_MAX_BYTES: usize = 4_096;
pub const TOOLS_MAX_ENTRIES: usize = 256;

/// Structural-validation report. `passed` is `failures.is_empty()`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationReport {
    pub passed: bool,
    pub failures: Vec<ValidationFailure>,
}

/// Single structural failure. `span` is byte offsets into the
/// `system_prompt` body when applicable (anti-pattern matches);
/// `suggested_insert_offset` is set for `MissingSection` and points
/// at where the canonical section would be inserted (UI uses it to
/// render "Add section" chips).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationFailure {
    pub code: FailureCode,
    /// Which request-body field the failure attaches to. Free-form
    /// string — canonical values are declared as `&'static str`
    /// constants at the field-failure construction sites
    /// (`"system_prompt"`, `"id"`, `"tier"`, `"tools"`,
    /// `"forbid_all_tools"`, `"max_context_tokens"`).
    pub field: String,
    pub message: String,
    pub span: Option<Span>,
    pub suggested_insert_offset: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

/// Closed set of structural failure codes. RFC 031 §Structural
/// validation (wire shape). Note that `SizeExceeded` is **not** in
/// this enum — 64 KiB overflow (§D4) is a separate 413 response with
/// a distinct body shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureCode {
    MissingSection,
    InsufficientPhases,
    InsufficientBullets,
    ProhibitedPattern,
    InvalidId,
    InvalidTier,
    /// Non-built-in id declared `tier: "orchestrator"` or
    /// `"generic"`. Only built-in `orchestrator` / `generic` ids
    /// may carry those tiers (§D11 reserved tiers).
    ReservedTier,
    InvalidResponseShape,
    InvalidMaxContextTokens,
    /// POST / PATCH with `forbid_all_tools: true` AND a non-empty
    /// `tools[]` — internally inconsistent (§D3).
    ToolsConflict,
    /// PATCH body attempted to change `id` or `tier`.
    ImmutableField,
}

// ── Built-in id taxonomy ─────────────────────────────────────────────

/// Built-in role ids whose tier is RFC-pinned.
///
/// Kept in lockstep with `default_roles()` in the parent module.
const BUILTIN_IDS: &[&str] = &[
    "orchestrator",
    "executor",
    "researcher",
    "reviewer",
    "generic",
    "status-checker",
];

fn expected_tier_for_builtin_id(id: &str) -> Option<AgentRoleTier> {
    match id {
        "orchestrator" => Some(AgentRoleTier::Orchestrator),
        "executor" | "reviewer" | "status-checker" => Some(AgentRoleTier::Standard),
        "researcher" => Some(AgentRoleTier::Research),
        "generic" => Some(AgentRoleTier::Generic),
        _ => None,
    }
}

// ── Regex cache ──────────────────────────────────────────────────────

/// H2 header: ATX-style only, column 0, mandatory space after `##`.
/// Captures `title` (trailing colons and whitespace stripped via the
/// consumer).
static H2_HEADER_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?m)^##[ \t]+(?P<title>[^\n#]+?)[ \t:]*$").expect("H2_HEADER_RE must compile")
});

/// H3 phase heading inside `## Workflow`. Column 0.
static H3_PHASE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?m)^###[ \t]+\S").expect("H3_PHASE_RE must compile"));

/// Column-0 unordered bullet in `## What not to do`.
static BULLET_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?m)^[-*+][ \t]+\S").expect("BULLET_RE must compile"));

/// §D5 role id namespace.
static ROLE_ID_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-z0-9][a-z0-9_-]*$").expect("ROLE_ID_RE must compile"));

// Anti-pattern regexes (§Prohibited anti-patterns).

/// `early_completion_directive` — every alternative requires a bypass
/// verb so legitimate prose like "call complete_run after
/// post_summary_comment" does NOT trigger.
static EARLY_COMPLETION_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?i)(?:call\s+)?complete_run\s+(?:immediately|right\s+away|at\s+the\s+start)\s+(?:and\s+)?(?:exit|return|stop|without\s+\w+)\b|call\s+complete_run\s+now\b\s+and\s+(?:exit|return|stop)|call\s+complete_run\s+(?:before|without)\s+(?:finishing|completing|verifying)",
    )
    .expect("EARLY_COMPLETION_RE must compile")
});

/// `caps_adversarial_framing` Stage 1 — candidate line match.
static CAPS_STAGE1_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?m)^.*\b(?:CRITICAL\s+RULES?|MUST\s+NOT\s+FAIL|FAILURE\s+IS\s+UNACCEPTABLE|FATAL\s+ERROR\s+IF)\b.*$",
    )
    .expect("CAPS_STAGE1_RE must compile")
});

/// `sub_agent_identity_shadow` — keyed on `role.tier !=
/// AgentRoleTier::Orchestrator` (matches the runtime dispatch).
static SUBAGENT_IDENTITY_SHADOW_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?im)^\s*You\s+are\s+the\s+(?:orchestrator|operator|user)\b")
        .expect("SUBAGENT_IDENTITY_SHADOW_RE must compile")
});

// ── Required-section taxonomy ────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CanonicalSection {
    Specialty,
    Workflow,
    Tools,
    CompletionCriteria,
    WhatNotToDo,
}

impl CanonicalSection {
    /// Title synonyms mapped to canonical sections. Match is
    /// case-insensitive after trailing-colon strip.
    fn from_title(raw_title: &str) -> Option<Self> {
        let trimmed = raw_title.trim().trim_end_matches(':').trim().to_lowercase();
        match trimmed.as_str() {
            "specialty" | "role" => Some(Self::Specialty),
            "workflow" => Some(Self::Workflow),
            "tools" => Some(Self::Tools),
            "completion criteria" | "completion" => Some(Self::CompletionCriteria),
            "what not to do" | "do not" => Some(Self::WhatNotToDo),
            _ => None,
        }
    }

    fn display_header(self) -> &'static str {
        match self {
            Self::Specialty => "## Specialty",
            Self::Workflow => "## Workflow",
            Self::Tools => "## Tools",
            Self::CompletionCriteria => "## Completion criteria",
            Self::WhatNotToDo => "## What not to do",
        }
    }
}

/// One detected H2 section in the prompt.
#[derive(Debug)]
struct DetectedSection {
    canonical: CanonicalSection,
    /// Byte offset of the section body start (char after header
    /// newline). Equal to the prompt length if header is the last
    /// line.
    body_start: usize,
    /// Byte offset of the section body end (start of next H2 or EOF).
    body_end: usize,
}

// ── Main entry ───────────────────────────────────────────────────────

/// Validate an `AgentRole`'s structural contract. Returns a report
/// whose `passed` flag reflects `failures.is_empty()`.
///
/// Called by the HTTP handler (PR-B) pre-persist. Callers MUST
/// short-circuit on 413 (size caps per §D4) before calling this —
/// the validator trusts `system_prompt` to fit in memory.
pub fn validate_prompt_structure(role: &AgentRole) -> ValidationReport {
    let mut failures = Vec::new();

    // ── Field-level checks ──

    if !ROLE_ID_RE.is_match(&role.role_id) || role.role_id.len() > ROLE_ID_MAX_LEN {
        failures.push(ValidationFailure {
            code: FailureCode::InvalidId,
            field: "id".to_owned(),
            message: format!(
                "role id must match `[a-z0-9][a-z0-9_-]*` and be at most {ROLE_ID_MAX_LEN} chars"
            ),
            span: None,
            suggested_insert_offset: None,
        });
    }

    // §D11 reserved tiers + shadow tier match.
    check_tier(role, &mut failures);

    // §D3 forbid_all_tools + tools[] consistency.
    if role.forbid_all_tools && !role.tools.is_empty() {
        failures.push(ValidationFailure {
            code: FailureCode::ToolsConflict,
            field: "forbid_all_tools".to_owned(),
            message: "forbid_all_tools=true is inconsistent with a non-empty tools[] list; \
                 pick one"
                .to_owned(),
            span: None,
            suggested_insert_offset: None,
        });
    }

    // §D12 max_context_tokens bounds.
    if let Some(tokens) = role.max_context_tokens {
        if tokens == 0 || tokens > 2_000_000 {
            failures.push(ValidationFailure {
                code: FailureCode::InvalidMaxContextTokens,
                field: "max_context_tokens".to_owned(),
                message: "max_context_tokens must be > 0 and ≤ 2_000_000".to_owned(),
                span: None,
                suggested_insert_offset: None,
            });
        }
    }

    // ── Prompt structural checks ──

    let prompt_source: &str = role.system_prompt.as_deref().unwrap_or("");

    // Detect every H2 section in canonical order.
    let sections = collect_sections(prompt_source);

    // §Short-circuit on zero-H2 prompts: single failure, no per-section fanout.
    if sections.is_empty() && !prompt_source.is_empty() {
        failures.push(ValidationFailure {
            code: FailureCode::MissingSection,
            field: "system_prompt".to_owned(),
            message: "no H2 section headers found; see RFC 031 for the required structure"
                .to_owned(),
            span: None,
            suggested_insert_offset: Some(0),
        });
    } else if prompt_source.is_empty() {
        // Empty prompt — treat the same way (one consolidated
        // failure). `None` on `system_prompt` + empty strings both
        // route here.
        failures.push(ValidationFailure {
            code: FailureCode::MissingSection,
            field: "system_prompt".to_owned(),
            message: "system_prompt is empty".to_owned(),
            span: None,
            suggested_insert_offset: Some(0),
        });
    } else {
        // Full per-section checks.
        let is_orchestrator_shadow =
            role.role_id == "orchestrator" && role.tier == AgentRoleTier::Orchestrator;

        let required: &[CanonicalSection] = if is_orchestrator_shadow {
            // §Orchestrator-shadow exemption: only these two are
            // required. Orchestrators dispatch; Workflow/Tools don't
            // map.
            &[
                CanonicalSection::CompletionCriteria,
                CanonicalSection::WhatNotToDo,
            ]
        } else {
            &[
                CanonicalSection::Specialty,
                CanonicalSection::Workflow,
                CanonicalSection::Tools,
                CanonicalSection::CompletionCriteria,
                CanonicalSection::WhatNotToDo,
            ]
        };

        for canon in required {
            let section = sections.iter().find(|s| s.canonical == *canon);
            match section {
                None => {
                    failures.push(ValidationFailure {
                        code: FailureCode::MissingSection,
                        field: "system_prompt".to_owned(),
                        message: format!("required section `{}` not found", canon.display_header()),
                        span: None,
                        suggested_insert_offset: Some(canonical_insert_offset(
                            canon,
                            &sections,
                            prompt_source.len(),
                        )),
                    });
                }
                Some(section) => {
                    let body = &prompt_source[section.body_start..section.body_end];
                    check_section_body(*canon, body, section.body_start, &mut failures);
                }
            }
        }
    }

    // §Prohibited anti-patterns — run across the full prompt even on
    // structural failures (operators benefit from seeing all issues at
    // once).
    check_anti_patterns(role, prompt_source, &mut failures);

    ValidationReport {
        passed: failures.is_empty(),
        failures,
    }
}

fn check_tier(role: &AgentRole, failures: &mut Vec<ValidationFailure>) {
    let is_builtin = BUILTIN_IDS.contains(&role.role_id.as_str());
    if is_builtin {
        // Shadow tier match.
        if let Some(expected) = expected_tier_for_builtin_id(&role.role_id) {
            if role.tier != expected {
                failures.push(ValidationFailure {
                    code: FailureCode::InvalidTier,
                    field: "tier".to_owned(),
                    message: format!(
                        "shadowing built-in `{}` requires tier `{:?}`, got `{:?}`",
                        role.role_id, expected, role.tier
                    ),
                    span: None,
                    suggested_insert_offset: None,
                });
            }
        }
    } else {
        // Reserved tiers: novel ids cannot claim Orchestrator or Generic.
        if matches!(
            role.tier,
            AgentRoleTier::Orchestrator | AgentRoleTier::Generic
        ) {
            failures.push(ValidationFailure {
                code: FailureCode::ReservedTier,
                field: "tier".to_owned(),
                message: format!(
                    "tier `{:?}` is reserved to the corresponding built-in id; \
                     novel roles must declare `Standard` or `Research`",
                    role.tier
                ),
                span: None,
                suggested_insert_offset: None,
            });
        }
    }
}

fn collect_sections(prompt: &str) -> Vec<DetectedSection> {
    let mut raw: Vec<(CanonicalSection, usize, usize)> = Vec::new();
    for cap in H2_HEADER_RE.captures_iter(prompt) {
        let whole = cap.get(0).unwrap();
        let title = cap.name("title").unwrap().as_str();
        if let Some(canon) = CanonicalSection::from_title(title) {
            // Skip if we already have this canonical section — first
            // match wins.
            if raw.iter().any(|(c, _, _)| *c == canon) {
                continue;
            }
            raw.push((canon, whole.start(), whole.end()));
        }
    }

    // Determine body end for each section: start of the next H2
    // (regardless of canonical) or EOF.
    let mut all_h2_starts: Vec<usize> = H2_HEADER_RE
        .captures_iter(prompt)
        .map(|c| c.get(0).unwrap().start())
        .collect();
    all_h2_starts.sort_unstable();

    let mut sections = Vec::with_capacity(raw.len());
    for (canon, header_start, header_end) in raw {
        // Body starts at the character after the header newline.
        let body_start = prompt[header_end..]
            .find('\n')
            .map(|nl| header_end + nl + 1)
            .unwrap_or(prompt.len());
        let body_end = all_h2_starts
            .iter()
            .copied()
            .find(|s| *s > header_start)
            .unwrap_or(prompt.len());
        sections.push(DetectedSection {
            canonical: canon,
            body_start,
            body_end,
        });
    }
    sections
}

fn canonical_insert_offset(
    missing: &CanonicalSection,
    present: &[DetectedSection],
    prompt_len: usize,
) -> usize {
    // Canonical order. If the missing section's predecessor is
    // present, insert right after its body; otherwise insert at the
    // start (0).
    let predecessor = match missing {
        CanonicalSection::Specialty => None,
        CanonicalSection::Workflow => Some(CanonicalSection::Specialty),
        CanonicalSection::Tools => Some(CanonicalSection::Workflow),
        CanonicalSection::CompletionCriteria => Some(CanonicalSection::Tools),
        CanonicalSection::WhatNotToDo => Some(CanonicalSection::CompletionCriteria),
    };
    if let Some(pred) = predecessor {
        if let Some(section) = present.iter().find(|s| s.canonical == pred) {
            return section.body_end.min(prompt_len);
        }
    }
    // No predecessor in canonical order, or predecessor missing:
    // insert at the start.
    0
}

fn check_section_body(
    canon: CanonicalSection,
    body: &str,
    body_offset: usize,
    failures: &mut Vec<ValidationFailure>,
) {
    let has_non_whitespace = body.chars().any(|c| !c.is_whitespace());
    match canon {
        CanonicalSection::Workflow => {
            let phase_count = H3_PHASE_RE.find_iter(body).count();
            if phase_count < 2 {
                failures.push(ValidationFailure {
                    code: FailureCode::InsufficientPhases,
                    field: "system_prompt".to_owned(),
                    message: format!(
                        "`## Workflow` must contain ≥ 2 column-0 H3 phase headings; found {phase_count}"
                    ),
                    span: Some(Span {
                        start: body_offset,
                        end: body_offset + body.len(),
                    }),
                    suggested_insert_offset: None,
                });
            }
        }
        CanonicalSection::WhatNotToDo => {
            let bullet_count = BULLET_RE.find_iter(body).count();
            if bullet_count < 3 {
                failures.push(ValidationFailure {
                    code: FailureCode::InsufficientBullets,
                    field: "system_prompt".to_owned(),
                    message: format!(
                        "`## What not to do` must contain ≥ 3 column-0 bullets; found {bullet_count}"
                    ),
                    span: Some(Span {
                        start: body_offset,
                        end: body_offset + body.len(),
                    }),
                    suggested_insert_offset: None,
                });
            }
        }
        _ => {
            if !has_non_whitespace {
                failures.push(ValidationFailure {
                    code: FailureCode::MissingSection,
                    field: "system_prompt".to_owned(),
                    message: format!("`{}` is present but empty", canon.display_header()),
                    span: Some(Span {
                        start: body_offset,
                        end: body_offset + body.len(),
                    }),
                    suggested_insert_offset: None,
                });
            }
        }
    }
}

fn check_anti_patterns(role: &AgentRole, prompt: &str, failures: &mut Vec<ValidationFailure>) {
    // `early_completion_directive`.
    if let Some(m) = EARLY_COMPLETION_RE.find(prompt) {
        failures.push(ValidationFailure {
            code: FailureCode::ProhibitedPattern,
            field: "system_prompt".to_owned(),
            message: "prompt contains an `early_completion_directive` — use \
                 response_shape=\"direct_answer\" instead of bypassing the workflow"
                .to_owned(),
            span: Some(Span {
                start: m.start(),
                end: m.end(),
            }),
            suggested_insert_offset: None,
        });
    }

    // `caps_adversarial_framing` Stage 1 + Stage 2 letter-ratio gate.
    for m in CAPS_STAGE1_RE.find_iter(prompt) {
        let line = m.as_str();
        if line_is_caps_heavy(line) {
            failures.push(ValidationFailure {
                code: FailureCode::ProhibitedPattern,
                field: "system_prompt".to_owned(),
                message: "prompt contains `caps_adversarial_framing` — modern Claude \
                     responds worse to all-caps adversarial framing; use sentence-case \
                     directives"
                    .to_owned(),
                span: Some(Span {
                    start: m.start(),
                    end: m.end(),
                }),
                suggested_insert_offset: None,
            });
            break; // one finding per kind is enough
        }
    }

    // `sub_agent_identity_shadow` — gated on tier != Orchestrator.
    if role.tier != AgentRoleTier::Orchestrator {
        if let Some(m) = SUBAGENT_IDENTITY_SHADOW_RE.find(prompt) {
            failures.push(ValidationFailure {
                code: FailureCode::ProhibitedPattern,
                field: "system_prompt".to_owned(),
                message: "prompt claims orchestrator/operator/user identity in a \
                     non-orchestrator role — contradicts BASE_SUBAGENT_PROMPT"
                    .to_owned(),
                span: Some(Span {
                    start: m.start(),
                    end: m.end(),
                }),
                suggested_insert_offset: None,
            });
        }
    }
}

/// Stage 2 letter-ratio gate for `caps_adversarial_framing`.
/// Line must be ≥ 80% uppercase of its ASCII letters, with at least
/// one ASCII letter present (prevents pure-symbol lines from
/// triggering).
fn line_is_caps_heavy(line: &str) -> bool {
    let mut upper = 0usize;
    let mut letters = 0usize;
    for c in line.chars() {
        if c.is_ascii_alphabetic() {
            letters += 1;
            if c.is_ascii_uppercase() {
                upper += 1;
            }
        }
    }
    letters > 0 && (upper * 100 / letters) >= 80
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_roles::{AgentRole, AgentRoleTier, ResponseShape};

    fn legal_prompt() -> String {
        // Pinned worked example from RFC 031 §Worked example.
        "\
## Specialty

Review PRs on valkey-io/valkey with citation-backed inline comments.

## Workflow

### 1. Explore

Read the PR and retrieved corpus chunks.

### 2. Deliver

Post inline comments and a summary comment.

## Tools

- `post_inline_comment` — per concrete concern.
- `post_summary_comment` — one call, Phase 2.

## Completion criteria

Before `complete_run`: all concerns posted; one summary comment.

## What not to do

- Do not post without citing.
- Do not mark style as a bug.
- Do not hallucinate APIs.
"
        .to_owned()
    }

    fn sample_role() -> AgentRole {
        AgentRole::new(
            "pr-reviewer-valkey",
            "Valkey PR Reviewer",
            AgentRoleTier::Standard,
        )
        .with_system_prompt(legal_prompt())
        .with_response_shape(ResponseShape::ProceduralArtifact)
    }

    #[test]
    fn legal_prompt_passes() {
        let report = validate_prompt_structure(&sample_role());
        assert!(
            report.passed,
            "legal prompt should pass: {:?}",
            report.failures
        );
    }

    #[test]
    fn empty_prompt_emits_single_missing_section() {
        let mut role = sample_role();
        role.system_prompt = Some(String::new());
        let r = validate_prompt_structure(&role);
        assert!(!r.passed);
        let missing = r
            .failures
            .iter()
            .filter(|f| f.code == FailureCode::MissingSection)
            .count();
        assert_eq!(
            missing, 1,
            "empty prompt must emit exactly one MissingSection"
        );
    }

    #[test]
    fn zero_h2_prompt_short_circuits_to_single_failure() {
        let mut role = sample_role();
        role.system_prompt = Some(
            "This is a flat prose prompt with no H2 headers at all. Just some text.".to_owned(),
        );
        let r = validate_prompt_structure(&role);
        let missing: Vec<&ValidationFailure> = r
            .failures
            .iter()
            .filter(|f| f.code == FailureCode::MissingSection)
            .collect();
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].suggested_insert_offset, Some(0));
    }

    #[test]
    fn missing_workflow_section_yields_insufficient_phases_or_missing() {
        let mut role = sample_role();
        role.system_prompt = Some(
            "## Specialty\n\nHi.\n\n## Tools\n\n- x\n\n## Completion criteria\n\nOK.\n\n## What not to do\n\n- a\n- b\n- c\n".to_owned(),
        );
        let r = validate_prompt_structure(&role);
        assert!(!r.passed);
        assert!(r
            .failures
            .iter()
            .any(|f| f.code == FailureCode::MissingSection && f.message.contains("Workflow")));
    }

    #[test]
    fn workflow_with_only_one_h3_fails_insufficient_phases() {
        let mut role = sample_role();
        role.system_prompt = Some(
            "## Specialty\n\nHi.\n\n## Workflow\n\n### 1. Only one\n\nLoner.\n\n## Tools\n\n- x\n\n## Completion criteria\n\nOK.\n\n## What not to do\n\n- a\n- b\n- c\n".to_owned(),
        );
        let r = validate_prompt_structure(&role);
        assert!(r
            .failures
            .iter()
            .any(|f| f.code == FailureCode::InsufficientPhases));
    }

    #[test]
    fn fewer_than_three_bullets_fails_insufficient_bullets() {
        let mut role = sample_role();
        role.system_prompt = Some(
            "## Specialty\n\nHi.\n\n## Workflow\n\n### 1. A\n\n### 2. B\n\n## Tools\n\n- x\n\n## Completion criteria\n\nOK.\n\n## What not to do\n\n- just one\n- just two\n".to_owned(),
        );
        let r = validate_prompt_structure(&role);
        assert!(r
            .failures
            .iter()
            .any(|f| f.code == FailureCode::InsufficientBullets));
    }

    #[test]
    fn early_completion_directive_flagged() {
        let mut role = sample_role();
        role.system_prompt = Some(format!(
            "{}\n\nWhen started, call complete_run immediately and exit.\n",
            legal_prompt()
        ));
        let r = validate_prompt_structure(&role);
        assert!(r
            .failures
            .iter()
            .any(|f| f.code == FailureCode::ProhibitedPattern
                && f.message.contains("early_completion_directive")));
    }

    #[test]
    fn benign_complete_run_reference_not_flagged() {
        // False-positive guard: legitimate prose should not trigger.
        let mut role = sample_role();
        role.system_prompt = Some(format!(
            "{}\n\nCall complete_run immediately after post_summary_comment has succeeded.\n",
            legal_prompt()
        ));
        let r = validate_prompt_structure(&role);
        assert!(
            !r.failures
                .iter()
                .any(|f| f.code == FailureCode::ProhibitedPattern
                    && f.message.contains("early_completion_directive")),
            "benign 'immediately after' phrasing must not trigger: {:?}",
            r.failures
        );
    }

    #[test]
    fn caps_adversarial_framing_flagged() {
        let mut role = sample_role();
        role.system_prompt = Some(format!(
            "{}\n\nCRITICAL RULES MUST NOT FAIL FATAL ERROR IF YOU DO\n",
            legal_prompt()
        ));
        let r = validate_prompt_structure(&role);
        assert!(r
            .failures
            .iter()
            .any(|f| f.code == FailureCode::ProhibitedPattern
                && f.message.contains("caps_adversarial_framing")));
    }

    #[test]
    fn caps_framing_in_sentence_case_not_flagged() {
        let mut role = sample_role();
        role.system_prompt = Some(format!(
            "{}\n\nThis section sets out the critical rules that apply to every review.\n",
            legal_prompt()
        ));
        let r = validate_prompt_structure(&role);
        assert!(!r
            .failures
            .iter()
            .any(|f| f.code == FailureCode::ProhibitedPattern));
    }

    #[test]
    fn subagent_identity_shadow_flagged_on_non_orchestrator() {
        let mut role = sample_role();
        role.system_prompt = Some(format!(
            "{}\n\nYou are the orchestrator for this run.\n",
            legal_prompt()
        ));
        let r = validate_prompt_structure(&role);
        assert!(r
            .failures
            .iter()
            .any(|f| f.code == FailureCode::ProhibitedPattern
                && f.message.contains("sub_agent_identity_shadow")
                || f.message.contains("orchestrator/operator/user")));
    }

    #[test]
    fn subagent_identity_shadow_allowed_on_orchestrator_tier() {
        let role = AgentRole::new(
            "orchestrator",
            "Custom Orchestrator",
            AgentRoleTier::Orchestrator,
        )
        .with_system_prompt(format!(
            "{}\n\nYou are the orchestrator for this run.\n",
            orchestrator_shadow_prompt()
        ));
        let r = validate_prompt_structure(&role);
        assert!(
            !r.failures
                .iter()
                .any(|f| f.message.contains("orchestrator/operator/user")),
            "orchestrator-tier role must not trigger subagent_identity_shadow"
        );
    }

    fn orchestrator_shadow_prompt() -> String {
        // Minimum legal prompt for orchestrator shadow per §Prompt
        // Contract: only Completion criteria + What not to do required.
        "\
## Completion criteria

All sub-agents returned.

## What not to do

- Do not execute inline.
- Do not skip delegation.
- Do not complete before aggregating.
"
        .to_owned()
    }

    #[test]
    fn orchestrator_shadow_minimum_sections_passes() {
        let role = AgentRole::new(
            "orchestrator",
            "Custom Orchestrator",
            AgentRoleTier::Orchestrator,
        )
        .with_system_prompt(orchestrator_shadow_prompt());
        let r = validate_prompt_structure(&role);
        assert!(
            r.passed,
            "orchestrator shadow with 2 sections should pass: {:?}",
            r.failures
        );
    }

    #[test]
    fn invalid_id_rejected() {
        let mut role = sample_role();
        role.role_id = "PR-Reviewer".to_owned();
        let r = validate_prompt_structure(&role);
        assert!(r.failures.iter().any(|f| f.code == FailureCode::InvalidId));
    }

    #[test]
    fn shadow_tier_mismatch_rejected() {
        // Shadowing `researcher` with `tier: Standard` (wrong — should be Research).
        let mut role = sample_role();
        role.role_id = "researcher".to_owned();
        role.tier = AgentRoleTier::Standard;
        let r = validate_prompt_structure(&role);
        assert!(r
            .failures
            .iter()
            .any(|f| f.code == FailureCode::InvalidTier));
    }

    #[test]
    fn reserved_tier_rejected_for_novel_id() {
        let role = AgentRole::new("my-orchestrator", "Bogus", AgentRoleTier::Orchestrator)
            .with_system_prompt(legal_prompt());
        let r = validate_prompt_structure(&role);
        assert!(r
            .failures
            .iter()
            .any(|f| f.code == FailureCode::ReservedTier));
    }

    #[test]
    fn research_tier_allowed_for_novel_id() {
        let role = AgentRole::new(
            "legal-researcher",
            "Legal Researcher",
            AgentRoleTier::Research,
        )
        .with_system_prompt(legal_prompt());
        let r = validate_prompt_structure(&role);
        assert!(
            !r.failures
                .iter()
                .any(|f| f.code == FailureCode::ReservedTier),
            "Research tier is free for novel ids per §D11"
        );
    }

    #[test]
    fn forbid_all_tools_with_non_empty_tools_rejected() {
        let mut role = sample_role();
        role.forbid_all_tools = true;
        role.tools = vec!["x".to_owned()];
        let r = validate_prompt_structure(&role);
        assert!(r
            .failures
            .iter()
            .any(|f| f.code == FailureCode::ToolsConflict));
    }

    #[test]
    fn max_context_tokens_zero_rejected() {
        let mut role = sample_role();
        role.max_context_tokens = Some(0);
        let r = validate_prompt_structure(&role);
        assert!(r
            .failures
            .iter()
            .any(|f| f.code == FailureCode::InvalidMaxContextTokens));
    }

    #[test]
    fn max_context_tokens_too_large_rejected() {
        let mut role = sample_role();
        role.max_context_tokens = Some(3_000_000);
        let r = validate_prompt_structure(&role);
        assert!(r
            .failures
            .iter()
            .any(|f| f.code == FailureCode::InvalidMaxContextTokens));
    }

    #[test]
    fn line_is_caps_heavy_semantics() {
        assert!(line_is_caps_heavy("CRITICAL RULES"));
        assert!(!line_is_caps_heavy("Critical rules in sentence case"));
        assert!(!line_is_caps_heavy("")); // no letters → not caps-heavy
        assert!(!line_is_caps_heavy("!!!!")); // pure symbols → not caps-heavy
    }
}
