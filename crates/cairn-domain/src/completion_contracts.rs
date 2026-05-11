//! RFC 032 Phase 1 — completion contracts primitive.
//!
//! PR-1 (this PR) lands the [`ContractVerifiedOutput`] enum used by
//! [`StepSummary::verified_output`]. PR-2 adds the full
//! [`CompletionContract`] enum, `ContractSchema`, `RelPath`,
//! `BoundedRegex`, `FileRequirement`, `ContractRejectionCode`,
//! `FailureClass::ContractNotMet`, and
//! `RuntimeEvent::CompletionContractResolved`. PR-3 wires the
//! verifiers. PR-4 adds gate integration + inference. PR-5 adds the
//! API surface + prompt render.
//!
//! The RFC lives at `docs/design/rfcs/032-completion-contracts.md`.

use serde::{Deserialize, Serialize};

/// Structured evidence a sub-agent carried past its own completion
/// contract, surfaced to the parent via `StepSummary::verified_output`
/// in #670 G7's rollup.
///
/// Exists so root-level synthesis does not have to regex-parse child
/// prose summaries for URLs or citation counts — the verifier already
/// extracted the relevant field at gate time; this record propagates
/// the extracted shape to the parent's LLM context.
///
/// Rendered into the parent's user-message `## Step history` section
/// alongside the free-form `summary` (PR-5). Both are visible to the
/// parent LLM; the structured form is the load-bearing one for
/// parent contracts that require aggregate shape checks.
///
/// All variants are stable on-the-wire — `StepSummary` deserialization
/// accepts historical records that predate this field via
/// `#[serde(default)]` on the containing field.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ContractVerifiedOutput {
    /// Matched `CompletionContract::ProseNonEmpty`. No extracted
    /// payload — the contract only asserted "final_answer non-empty."
    ProseNonEmpty,

    /// Matched `CompletionContract::Prose`. Reports the citation count
    /// the verifier resolved (so a parent contract asking for
    /// "≥ 3 child reports with ≥ 2 citations each" has structured
    /// input, not prose parsing).
    Prose { citations_resolved: u32 },

    /// Matched `CompletionContract::File`. Reports the paths the
    /// verifier confirmed exist under the child's resolved
    /// workspace. Paths are workspace-relative (NOT absolute) —
    /// parent contracts that care about location declare relative
    /// paths too.
    ///
    /// Carried as `Vec<String>` (not `Vec<PathBuf>`) to keep the
    /// JSON wire shape OS-independent. Rust's `PathBuf` serializes
    /// as a string today but the shape is not contractually stable
    /// across platforms (non-UTF-8 bytes on Unix round-trip with
    /// loss). RFC 032 PR-2 introduces a typed `RelPath` newtype
    /// that this variant will migrate to in a follow-up; for PR-1
    /// the wire shape is pinned to plain strings.
    File { paths: Vec<String> },

    /// Matched `CompletionContract::PullRequest`. Reports the
    /// verified PR URL and its head commit SHA so a parent
    /// contract aggregating "N PRs shipped" has structured
    /// evidence. `pr_url` is guaranteed to be in the run's
    /// project allowlist — the verifier rejects cross-tenant
    /// references before this record is emitted.
    PullRequest { pr_url: String, head_sha: String },

    /// Matched `CompletionContract::Structured`. Reports the
    /// verified JSON value the child's `final_answer` parsed as.
    /// Carries the entire structured payload so the parent can
    /// slice fields it cares about.
    Structured { value: serde_json::Value },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prose_non_empty_round_trips_snake_case() {
        let value = ContractVerifiedOutput::ProseNonEmpty;
        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(json, r#"{"kind":"prose_non_empty"}"#);
        let back: ContractVerifiedOutput = serde_json::from_str(&json).unwrap();
        assert_eq!(back, value);
    }

    #[test]
    fn prose_carries_citation_count() {
        let value = ContractVerifiedOutput::Prose {
            citations_resolved: 3,
        };
        let json = serde_json::to_string(&value).unwrap();
        assert!(json.contains(r#""kind":"prose""#));
        assert!(json.contains(r#""citations_resolved":3"#));
        let back: ContractVerifiedOutput = serde_json::from_str(&json).unwrap();
        assert_eq!(back, value);
    }

    #[test]
    fn pull_request_carries_url_and_head_sha() {
        let value = ContractVerifiedOutput::PullRequest {
            pr_url: "https://github.com/avifenesh/cairn-rs/pull/851".to_owned(),
            head_sha: "263f4c0d48adbbc73b33f2a484d2c7f3674f5d60".to_owned(),
        };
        let json = serde_json::to_string(&value).unwrap();
        let back: ContractVerifiedOutput = serde_json::from_str(&json).unwrap();
        assert_eq!(back, value);
    }

    #[test]
    fn file_variant_preserves_path_order() {
        let value = ContractVerifiedOutput::File {
            paths: vec![
                "src/main.rs".to_owned(),
                "Cargo.toml".to_owned(),
                ".github/workflows/ci.yml".to_owned(),
            ],
        };
        let json = serde_json::to_string(&value).unwrap();
        let back: ContractVerifiedOutput = serde_json::from_str(&json).unwrap();
        // Path order matters — parent contracts may expect
        // verifier-provided ordering.
        assert_eq!(back, value);
        // Wire shape pinned: paths render as plain strings,
        // not PathBuf-serialized variants.
        assert!(
            json.contains(r#""paths":["src/main.rs","Cargo.toml",".github/workflows/ci.yml"]"#),
            "paths must serialize as JSON strings; got: {json}",
        );
    }

    #[test]
    fn structured_variant_carries_arbitrary_json() {
        let value = ContractVerifiedOutput::Structured {
            value: serde_json::json!({
                "verdict": "approve",
                "findings": [
                    {"severity": "high", "count": 2},
                    {"severity": "low", "count": 5},
                ],
            }),
        };
        let json = serde_json::to_string(&value).unwrap();
        let back: ContractVerifiedOutput = serde_json::from_str(&json).unwrap();
        assert_eq!(back, value);
    }
}
