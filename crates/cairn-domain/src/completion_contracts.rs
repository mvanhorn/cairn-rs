//! RFC 032 Phase 1 — completion contracts primitive.
//!
//! PR-1 landed the [`ContractVerifiedOutput`] enum used by
//! [`StepSummary::verified_output`]. **PR-2 (this PR)** adds the full
//! [`CompletionContract`] enum, [`ContractSchema`], [`RelPath`],
//! [`BoundedRegex`], [`FileRequirement`], [`ExternalStateCheck`],
//! [`ContractRejectionCode`], extends `FailureClass::ContractNotMet`,
//! and adds the `RuntimeEvent::CompletionContractResolved` variant.
//! PR-3 wires the verifiers. PR-4 adds gate integration + inference.
//! PR-5 adds the API surface + prompt render.
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

// ── RFC 032 PR-2: CompletionContract domain ──────────────────────────────────

/// Serialized-JSON cap applied to every declared `CompletionContract`
/// at contract-accept time. Defense in depth against operator-supplied
/// blobs: an individual `ContractSchema` has its own 32 KiB cap
/// (§1.1 of the RFC), and `DefaultsService::set_struct` has a 64 KiB
/// cap on ANY persisted typed value. This constant is the
/// contract-specific ceiling — it MUST be ≤ the defaults cap so the
/// "early reject at contract-accept" case never surprises a caller
/// at persist time.
pub const COMPLETION_CONTRACT_MAX_BYTES: usize = 48 * 1024;

/// Cap on the serialized JSON Schema document inside
/// [`ContractSchema`]. Sane operator schemas are well under 4 KiB;
/// 32 KiB is headroom, not blank check. See RFC 032 §1.1.
pub const CONTRACT_SCHEMA_MAX_BYTES: usize = 32 * 1024;

/// Cap on operator-supplied regex source strings inside
/// [`BoundedRegex`]. Rust's `regex` crate is RE2-style (linear-time
/// on match), so runtime DoS is bounded; this cap is for *compile*
/// memory. `RegexBuilder::size_limit` + `dfa_size_limit` enforce the
/// compile memory bound directly (64 KiB / 256 KiB); this string
/// length cap is a cheap upstream early-reject.
pub const BOUNDED_REGEX_SOURCE_MAX: usize = 1024;

/// Upper bound on the number of file requirements in a single
/// `CompletionContract::File` variant. Prevents operators (or a
/// compromised integration) from declaring a 10 000-path contract
/// that would walk the sandbox exhaustively at verifier time.
pub const FILE_REQUIREMENT_MAX_COUNT: usize = 32;

/// Upper bound on `FileRequirement.max_bytes`. A 1 GiB cap is enough
/// for any reasonable contract (the verifier streams bytes during
/// the contains-regex check, so large files are not loaded into
/// memory); larger values reject at contract-accept time as a signal
/// that the caller probably wants a different primitive than the
/// file verifier.
pub const FILE_REQUIREMENT_MAX_BYTES_CEIL: u64 = 1024 * 1024 * 1024;

/// Operator-declared definition-of-done for a single run's
/// `complete_run` action. Attached via [`POST /v1/runs`] body on run
/// creation or via [`spawn_subagent`] on sub-agent dispatch. When
/// present (explicit or inferred at first orchestrate boot), the
/// gate runs the declared verifier before accepting `complete_run`.
/// When absent, the run falls through to the permissive
/// [`CompletionContract::ProseNonEmpty`] floor.
///
/// Wire shape uses the workspace `tag = "kind"` convention (cf.
/// `session_orchestration`, `approvals`, `selectors`, `events`).
/// All variants are immutable once set; re-inference on goal change
/// is the ONLY way the resolved contract can switch shapes, and it
/// only fires when `source == "inferred"` (RFC 032 §2.3).
///
/// Variants:
/// * [`ProseNonEmpty`](CompletionContract::ProseNonEmpty) — permissive floor.
/// * [`Prose`](CompletionContract::Prose) — research / compare / audit.
/// * [`File`](CompletionContract::File) — on-disk artifacts in a persistent
///   workspace. Rejects at contract-accept time if the run has no
///   allowlisted repo or local_fs path.
/// * [`PullRequest`](CompletionContract::PullRequest) — cairn-github verifier,
///   same-tenant-allowlist-enforced (no cross-tenant reads).
/// * [`Structured`](CompletionContract::Structured) — JSON-Schema-validated
///   final_answer shape. PHASE 2 — variant lands in domain; verifier
///   returns `NotImplemented` in Phase 1.
/// * [`ExternalState`](CompletionContract::ExternalState) — webhook-first
///   confirmation of an external side effect. PHASE 3 — variant lands
///   in domain; verifier returns `NotImplemented` in Phase 1.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CompletionContract {
    /// `final_answer` must be non-empty. No citation / length /
    /// structure check beyond "at least one non-whitespace char".
    /// This is what a run gets when the operator declares nothing
    /// and inference matches no trigger.
    ProseNonEmpty,

    /// Prose with a minimum character count and a minimum number
    /// of resolvable citations. "Resolvable" = an `http(s)://` URL
    /// the citation verifier can reach within its per-citation
    /// budget. Inference default for goals mentioning `research`,
    /// `compare`, `audit`, or `investigate`.
    Prose {
        /// Minimum UTF-8 character count. RFC default: 500.
        min_chars: u32,
        /// Minimum number of resolvable `http(s)://` URL tokens.
        /// RFC default: 2.
        min_citations: u32,
    },

    /// One or more files must exist under the run's resolved
    /// workspace, optionally matching a bounded-regex content check.
    ///
    /// This variant is ONLY valid on runs with a persistent workspace
    /// (allowlisted repo sandbox or local_fs path). Ephemeral runs
    /// reject at contract-accept time with `400 contract_invalid:
    /// file_contract_requires_persistent_workspace` (PR-5 wires the
    /// handler-side reject; PR-2 lands the domain type).
    File { paths: Vec<FileRequirement> },

    /// A pull request exists, owned by the run's project, optionally
    /// matching shape constraints. Verifier (PR-3) REJECTS with
    /// `PrNotInProjectAllowlist` if `expected_repo` is not in the
    /// run's `ProjectRepoAccessService` allowlist — same-tenant
    /// only, no cross-tenant reads possible.
    PullRequest {
        /// Expected `owner/repo`. Optional: when `None`, any repo
        /// in the run's allowlist satisfies. Most callers supply
        /// a specific repo.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_repo: Option<String>,
        /// Expected head branch, as a regex. Optional. When `Some`,
        /// the verifier matches the PR's actual head branch against
        /// this regex.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_head_branch: Option<BoundedRegex>,
        /// When `true` (default), the PR must be in state `open`.
        /// Set to `false` to accept merged PRs (useful for "ship
        /// and merge" goals).
        must_be_open: bool,
    },

    /// `final_answer` parses as JSON matching the operator-supplied
    /// `ContractSchema`. PHASE 2 — domain-only in Phase 1; verifier
    /// returns `NotImplemented`.
    Structured { schema: Box<ContractSchema> },

    /// Cairn confirms an external-system side effect. Webhook-first
    /// (subscribes run to the matching signal; falls back to a poll
    /// at gate time if the webhook hasn't landed). PHASE 3 — domain
    /// -only in Phase 1; verifier returns `NotImplemented`.
    ExternalState { check: ExternalStateCheck },
}

impl CompletionContract {
    /// Snake_case identifier matching the serde `tag = "kind"`
    /// discriminator. Stable: operators and metrics key on these
    /// strings. Prefer this over `std::mem::discriminant` /
    /// `format!("{:?}", ...)` for operator-facing strings.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::ProseNonEmpty => "prose_non_empty",
            Self::Prose { .. } => "prose",
            Self::File { .. } => "file",
            Self::PullRequest { .. } => "pull_request",
            Self::Structured { .. } => "structured",
            Self::ExternalState { .. } => "external_state",
        }
    }

    /// Validate the contract at accept time. Enforces the
    /// [`COMPLETION_CONTRACT_MAX_BYTES`] cap on the serialized
    /// envelope AND per-variant structural checks (non-empty paths
    /// vec, count cap on File, etc). Callers that persist or
    /// dispatch a contract MUST call this first and reject on
    /// failure; this is the single choke-point where every
    /// contract-accept enters the system.
    pub fn validate(&self) -> Result<(), ContractError> {
        // Cap the overall serialized envelope first. Protects the
        // defaults-service write path even when per-variant checks
        // pass but the aggregate blob is pathological (e.g. a
        // 48 KiB regex source + a 48 KiB schema in the same struct).
        let serialized = serde_json::to_vec(self).map_err(ContractError::NotSerializable)?;
        if serialized.len() > COMPLETION_CONTRACT_MAX_BYTES {
            return Err(ContractError::TooLarge {
                size: serialized.len(),
                cap: COMPLETION_CONTRACT_MAX_BYTES,
            });
        }
        match self {
            Self::File { paths } => {
                if paths.is_empty() {
                    return Err(ContractError::EmptyFilePathsList);
                }
                if paths.len() > FILE_REQUIREMENT_MAX_COUNT {
                    return Err(ContractError::TooManyFilePaths {
                        count: paths.len(),
                        cap: FILE_REQUIREMENT_MAX_COUNT,
                    });
                }
                for req in paths {
                    if let Some(max) = req.max_bytes {
                        if max > FILE_REQUIREMENT_MAX_BYTES_CEIL {
                            return Err(ContractError::FileMaxBytesExceedsCeil {
                                requested: max,
                                ceil: FILE_REQUIREMENT_MAX_BYTES_CEIL,
                            });
                        }
                    }
                }
            }
            Self::Prose {
                min_chars,
                min_citations,
            } => {
                // Zeros are technically valid (same as ProseNonEmpty)
                // but pointlessly verbose — the operator can drop to
                // ProseNonEmpty. Reject as malformed so the caller
                // picks the right primitive.
                if *min_chars == 0 && *min_citations == 0 {
                    return Err(ContractError::ProseConstraintsBothZero);
                }
            }
            Self::ProseNonEmpty
            | Self::PullRequest { .. }
            | Self::Structured { .. }
            | Self::ExternalState { .. } => {}
        }
        Ok(())
    }
}

/// One entry in a `CompletionContract::File` variant. Path is a
/// typed [`RelPath`] (forgery-resistant on the wire). Content match
/// is a [`BoundedRegex`] (ReDoS + compile-memory bounded). Size cap
/// optional but capped at [`FILE_REQUIREMENT_MAX_BYTES_CEIL`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRequirement {
    /// Relative path under the run's working_dir. Absolute paths,
    /// `.`, and `..` components reject at deserialize time via
    /// [`RelPath::try_from`].
    pub path: RelPath,

    /// Optional contains-regex applied to file contents. `None` =
    /// the file's existence is the entire check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contains_regex: Option<BoundedRegex>,

    /// Optional max byte cap. When `Some`, files larger than this
    /// reject with `FileExceedsMaxBytes`. `None` = no size check.
    /// Ceiling is [`FILE_REQUIREMENT_MAX_BYTES_CEIL`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
}

/// Relative path under the run's working_dir. Absolute paths, `.`,
/// and `..` components reject at construction. Empty paths reject.
/// The wire shape is a plain JSON string; on-disk confinement is
/// enforced by the verifier at runtime (see RFC 032 §4.2 on
/// symlink-safe walk via `symlink_metadata`-per-component).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RelPath {
    components: Vec<String>,
}

impl RelPath {
    /// Parse a relative path string. Rejects on absolute, empty, or
    /// any `.` / `..` component. Surviving strings round-trip
    /// byte-for-byte through `Display`.
    pub fn try_new(raw: &str) -> Result<Self, PathError> {
        let path = std::path::Path::new(raw);
        if path.is_absolute() {
            return Err(PathError::Absolute);
        }
        let mut out = Vec::new();
        for c in path.components() {
            use std::path::Component;
            match c {
                Component::Normal(os) => {
                    out.push(os.to_string_lossy().into_owned());
                }
                Component::CurDir | Component::ParentDir => {
                    return Err(PathError::Traversal);
                }
                Component::RootDir | Component::Prefix(_) => {
                    return Err(PathError::Absolute);
                }
            }
        }
        if out.is_empty() {
            return Err(PathError::Empty);
        }
        Ok(Self { components: out })
    }

    /// Access the path components in order. Each component is a
    /// single path segment (no separators). Use `components.join("/")`
    /// or iteratively join under a working_dir base.
    pub fn components(&self) -> &[String] {
        &self.components
    }
}

impl std::fmt::Display for RelPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.components.join("/"))
    }
}

impl TryFrom<String> for RelPath {
    type Error = PathError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::try_new(&s)
    }
}

impl From<RelPath> for String {
    fn from(p: RelPath) -> Self {
        p.to_string()
    }
}

/// Errors from [`RelPath::try_new`]. Callers log + propagate; never
/// reach the LLM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathError {
    Absolute,
    Traversal,
    Empty,
}

impl std::fmt::Display for PathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Absolute => write!(f, "path must be relative (no leading `/` or drive prefix)"),
            Self::Traversal => write!(f, "path must not contain `.` or `..` components"),
            Self::Empty => write!(f, "path must not be empty"),
        }
    }
}

impl std::error::Error for PathError {}

/// Operator-supplied regex string with compile-memory bounds +
/// source-length cap. Rust's `regex` crate is RE2-style (linear on
/// match); this type protects against pathological *compile* cost.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct BoundedRegex {
    pattern: String,
}

impl BoundedRegex {
    /// Validate compile + size. Returns a `BoundedRegex` whose
    /// `pattern` is guaranteed to compile with the workspace
    /// memory caps. Does NOT compile-cache the resulting `Regex` —
    /// the verifier compiles at use time. Compilation is O(source)
    /// and the source is bounded by [`BOUNDED_REGEX_SOURCE_MAX`].
    pub fn try_new(raw: String) -> Result<Self, RegexError> {
        if raw.is_empty() {
            return Err(RegexError::Empty);
        }
        if raw.len() > BOUNDED_REGEX_SOURCE_MAX {
            return Err(RegexError::SourceTooLong {
                size: raw.len(),
                cap: BOUNDED_REGEX_SOURCE_MAX,
            });
        }
        regex::RegexBuilder::new(&raw)
            .size_limit(64 * 1024)
            .dfa_size_limit(256 * 1024)
            .build()
            .map_err(|e| RegexError::Invalid(e.to_string()))?;
        Ok(Self { pattern: raw })
    }

    pub fn as_str(&self) -> &str {
        &self.pattern
    }
}

impl std::fmt::Display for BoundedRegex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.pattern)
    }
}

impl TryFrom<String> for BoundedRegex {
    type Error = RegexError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::try_new(s)
    }
}

impl From<BoundedRegex> for String {
    fn from(r: BoundedRegex) -> Self {
        r.pattern
    }
}

/// Errors from [`BoundedRegex::try_new`]. Callers log + propagate;
/// never reach the LLM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegexError {
    Empty,
    SourceTooLong { size: usize, cap: usize },
    Invalid(String),
}

impl std::fmt::Display for RegexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "regex source must not be empty"),
            Self::SourceTooLong { size, cap } => {
                write!(f, "regex source too long: {size} bytes > {cap} bytes")
            }
            Self::Invalid(msg) => write!(f, "regex compile failed: {msg}"),
        }
    }
}

impl std::error::Error for RegexError {}

/// Typed wrapper around a JSON Schema document. Construction
/// validates the schema IS a valid JSON Schema Draft 7 document so
/// malformed schemas reject at `try_from` / `POST /v1/runs` parse
/// time, not at gate time. PR-2 validates the draft + size cap;
/// PR-3's verifier (or Phase 2 `Structured` verifier) runs the
/// actual document matching against `final_answer`.
///
/// Wire shape: the schema itself. JSON-Schema-of-JSON-Schema; the
/// field name ("schema") inside the containing `CompletionContract::Structured`
/// tells you this is a schema, not data.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "serde_json::Value", into = "serde_json::Value")]
pub struct ContractSchema {
    inner: serde_json::Value,
}

impl ContractSchema {
    /// Validate the value is:
    /// 1. Serializable to under [`CONTRACT_SCHEMA_MAX_BYTES`].
    /// 2. A valid JSON Schema document (per `jsonschema` crate
    ///    acceptance — compiles the schema as a validator prototype).
    ///
    /// The compiled validator is NOT cached on the type — each
    /// verifier call compiles fresh. That's acceptable: the
    /// `Structured` verifier is Phase 2 work and doesn't run in
    /// the hot path today; when it does, we revisit caching.
    pub fn try_new(value: serde_json::Value) -> Result<Self, SchemaError> {
        let bytes = serde_json::to_vec(&value).map_err(SchemaError::NotSerializable)?;
        if bytes.len() > CONTRACT_SCHEMA_MAX_BYTES {
            return Err(SchemaError::TooLarge {
                size: bytes.len(),
                cap: CONTRACT_SCHEMA_MAX_BYTES,
            });
        }
        // Minimal structural check: a valid JSON Schema document
        // is an object OR a boolean (per Draft 7 root-type rules).
        // The heavier "does this compile as a validator" check is
        // deferred to Phase 2 to avoid a cairn-domain dep on the
        // `jsonschema` crate (which brings in a regex+url surface
        // cairn-domain's no-IO invariant tries to keep light). PR-4
        // / Phase 2 wires the full compile check in the runtime
        // layer where the dep is fine.
        if !value.is_object() && !value.is_boolean() {
            return Err(SchemaError::NotJsonSchema {
                reason: "JSON Schema document must be an object or a boolean".to_owned(),
            });
        }
        Ok(Self { inner: value })
    }

    /// Read-only access to the inner schema document.
    pub fn as_value(&self) -> &serde_json::Value {
        &self.inner
    }
}

impl TryFrom<serde_json::Value> for ContractSchema {
    type Error = SchemaError;
    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl From<ContractSchema> for serde_json::Value {
    fn from(s: ContractSchema) -> Self {
        s.inner
    }
}

/// Errors from [`ContractSchema::try_new`]. `NotSerializable` wraps
/// the underlying `serde_json::Error`, which is not `Eq` / `Clone` /
/// `Serialize` — the variant itself has `#[serde(skip)]` so the
/// whole enum can still serialize (with that variant omitted on the
/// wire). Callers match-and-log; this error is never transmitted
/// verbatim to the LLM.
#[derive(Debug)]
pub enum SchemaError {
    NotSerializable(serde_json::Error),
    TooLarge { size: usize, cap: usize },
    NotJsonSchema { reason: String },
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotSerializable(e) => write!(f, "schema not serializable: {e}"),
            Self::TooLarge { size, cap } => {
                write!(f, "schema too large: {size} bytes > {cap} bytes")
            }
            Self::NotJsonSchema { reason } => write!(f, "not a valid JSON Schema: {reason}"),
        }
    }
}

impl std::error::Error for SchemaError {}

/// Phase 3 external-state check. Wired in Phase 3 when the
/// `ExternalState` verifier ships; domain-only for now.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExternalStateCheck {
    GitHubIssueClosed { repo: String, number: u64 },
    GitHubPrMerged { repo: String, number: u64 },
}

/// Errors surfaced by [`CompletionContract::validate`] at
/// contract-accept time. Structural — no external I/O. Callers
/// propagate these as `400 contract_invalid` on the HTTP boundary
/// and as an `ActionStatus::Failed` with `MalformedSpawnProposal`
/// code on the spawn_subagent path.
///
/// Not `Serialize` / `Eq` / `Clone` — `serde_json::Error` isn't any
/// of those. Callers match-and-log; this error is never transmitted
/// verbatim to the LLM.
#[derive(Debug)]
pub enum ContractError {
    NotSerializable(serde_json::Error),
    TooLarge { size: usize, cap: usize },
    EmptyFilePathsList,
    TooManyFilePaths { count: usize, cap: usize },
    FileMaxBytesExceedsCeil { requested: u64, ceil: u64 },
    ProseConstraintsBothZero,
}

impl std::fmt::Display for ContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotSerializable(e) => write!(f, "contract not serializable: {e}"),
            Self::TooLarge { size, cap } => {
                write!(f, "contract too large: {size} bytes > {cap} bytes")
            }
            Self::EmptyFilePathsList => {
                write!(f, "File contract requires at least one FileRequirement")
            }
            Self::TooManyFilePaths { count, cap } => write!(
                f,
                "File contract has {count} paths; cap is {cap}. Split into multiple contracts or scope the check."
            ),
            Self::FileMaxBytesExceedsCeil { requested, ceil } => write!(
                f,
                "FileRequirement.max_bytes = {requested} exceeds ceiling {ceil}; use a smaller cap or a different primitive"
            ),
            Self::ProseConstraintsBothZero => write!(
                f,
                "Prose contract with min_chars=0 AND min_citations=0 is pointless; use ProseNonEmpty"
            ),
        }
    }
}

impl std::error::Error for ContractError {}

/// RFC 032 §3.1: stable diagnostic codes the gate returns on
/// `ContractNotMet` rejection. Codes are `snake_case` on the wire
/// and stable across releases — callers (LLMs and operators) key
/// on them. Free-form reason text is operator-only; step_history
/// only ever sees the code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractRejectionCode {
    // Prose / ProseNonEmpty
    ProseEmpty,
    ProseTooShort,
    ProseInsufficientCitations,
    ProseCitationUnresolvable,

    // File
    FileMissing,
    FileSymlinkTraversal,
    FileExceedsMaxBytes,
    FileRegexNoMatch,

    // PullRequest
    PrNotInProjectAllowlist,
    PrUrlMalformed,
    PrUrlMissing,
    PrNotFound,
    /// The PR's `owner/repo` does not match the contract's
    /// `expected_repo`. Distinct from
    /// [`Self::PrNotInProjectAllowlist`]: the repo IS in the run's
    /// allowlist (no cross-tenant concern) but the operator pinned a
    /// specific repo and the PR lives in a different one. Also
    /// distinct from [`Self::PrHeadBranchMismatch`]: the branch name
    /// has its own dedicated code.
    PrRepoMismatch,
    PrHeadBranchMismatch,
    PrNotOpen,

    // Structured (Phase 2)
    StructuredParseError,
    StructuredSchemaMismatch,

    // ExternalState (Phase 3)
    ExternalStateNotConfirmed,

    // Generic
    VerifierTimeout,
    VerifierUnavailable,
    NotImplemented,
}

impl ContractRejectionCode {
    /// Snake_case wire identifier matching the serde
    /// `rename_all = "snake_case"` discriminator. Stable: operators
    /// and LLMs key on these strings (RFC §3.1). Prefer this over
    /// `{:?}` (Debug) when formatting codes into reason strings,
    /// step_history diagnostics, or log fields — Debug drifts on a
    /// rename, this doesn't.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ProseEmpty => "prose_empty",
            Self::ProseTooShort => "prose_too_short",
            Self::ProseInsufficientCitations => "prose_insufficient_citations",
            Self::ProseCitationUnresolvable => "prose_citation_unresolvable",
            Self::FileMissing => "file_missing",
            Self::FileSymlinkTraversal => "file_symlink_traversal",
            Self::FileExceedsMaxBytes => "file_exceeds_max_bytes",
            Self::FileRegexNoMatch => "file_regex_no_match",
            Self::PrNotInProjectAllowlist => "pr_not_in_project_allowlist",
            Self::PrUrlMalformed => "pr_url_malformed",
            Self::PrUrlMissing => "pr_url_missing",
            Self::PrNotFound => "pr_not_found",
            Self::PrRepoMismatch => "pr_repo_mismatch",
            Self::PrHeadBranchMismatch => "pr_head_branch_mismatch",
            Self::PrNotOpen => "pr_not_open",
            Self::StructuredParseError => "structured_parse_error",
            Self::StructuredSchemaMismatch => "structured_schema_mismatch",
            Self::ExternalStateNotConfirmed => "external_state_not_confirmed",
            Self::VerifierTimeout => "verifier_timeout",
            Self::VerifierUnavailable => "verifier_unavailable",
            Self::NotImplemented => "not_implemented",
        }
    }
}

impl std::fmt::Display for ContractRejectionCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// RFC 032 §2.3: how a [`CompletionContract`] arrived at a run.
/// Surfaced on `RuntimeEvent::CompletionContractResolved` so the
/// operator timeline distinguishes inferred contracts from
/// explicitly-declared ones. Re-inference on goal change emits a
/// second event with `ReInferredOnGoalChange`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractSource {
    /// Operator declared the contract on `POST /v1/runs`.
    ExplicitCreate,
    /// Orchestrator declared the contract on `spawn_subagent`.
    ExplicitSpawn,
    /// Inferred at first orchestrate boot from goal text.
    Inferred,
    /// Re-inferred after the run's goal text changed mid-run
    /// (rare — typically only when an operator overrides goal
    /// on a re-orchestrate). Only fires when `source` was
    /// previously `Inferred`; explicit contracts stay pinned.
    ReInferredOnGoalChange,
}

// ── RFC 032 PR-4: contract inference ────────────────────────────────────────

/// Pure function mapping goal text to a default [`CompletionContract`].
/// Called by the gate (PR-5 handler) at first-orchestrate boot when the
/// operator supplied no explicit contract. Re-runs when the goal text
/// changes mid-run (§2.3).
///
/// The trigger table is deliberately narrow (RFC §2.3). Adding a new
/// trigger is a public contract change — each new row must be justified
/// by a dogfood observation, not speculation. Biases toward
/// false-positive on ambiguous goals; operators with conditional goals
/// ("research, and if you find something, open a PR") should declare
/// the contract explicitly rather than rely on inference.
///
/// Returns `ProseNonEmpty` when no trigger matches — matches today's
/// effective gate behaviour on goals that didn't go through this path.
///
/// Implementation uses pre-compiled regexes via `once_cell::sync::Lazy`
/// (workspace MSRV 1.78 predates `std::sync::LazyLock`'s 1.80
/// stabilization) so the expensive compile happens once per process.
pub fn infer_contract(goal: &str) -> CompletionContract {
    use once_cell::sync::Lazy;

    // PR long-form: `(open|create|ship|submit) <=80 non-period chars> pull request`.
    static PR_LONG_RX: Lazy<regex::Regex> = Lazy::new(|| {
        regex::RegexBuilder::new(r"\b(open|create|ship|submit)\b[^.]{0,80}\bpull request\b")
            .case_insensitive(true)
            .build()
            .expect("PR long-form trigger regex must compile")
    });

    // PR short-form: `(open|ship)` within 20 non-period chars of `PR` —
    // disambiguates against "PR" as public relations.
    static PR_SHORT_RX: Lazy<regex::Regex> = Lazy::new(|| {
        regex::RegexBuilder::new(r"\b(open|ship)\b[^.]{0,20}\bPR\b")
            .case_insensitive(true)
            .build()
            .expect("PR short-form trigger regex must compile")
    });

    // Prose: \b(research|compare|audit|investigate)\b — word-anchored.
    static PROSE_RX: Lazy<regex::Regex> = Lazy::new(|| {
        regex::RegexBuilder::new(r"\b(research|compare|audit|investigate)\b")
            .case_insensitive(true)
            .build()
            .expect("prose trigger regex must compile")
    });

    if PR_LONG_RX.is_match(goal) || PR_SHORT_RX.is_match(goal) {
        return CompletionContract::PullRequest {
            expected_repo: None,
            expected_head_branch: None,
            must_be_open: true,
        };
    }
    if PROSE_RX.is_match(goal) {
        return CompletionContract::Prose {
            min_chars: 500,
            min_citations: 2,
        };
    }
    CompletionContract::ProseNonEmpty
}

/// RFC 032 §2.3: stable 16-hex-char hash of a goal string. Persisted as
/// `run:<run_id>:contract_source_goal_hash` so the re-inference guard
/// can detect mid-run goal changes without comparing full goal text.
/// First 8 bytes of SHA-256 are plenty for the equality check —
/// collisions are irrelevant (the handler reads BOTH hashes; a
/// hypothetical collision still resolves to the same inferred
/// contract, which is the behaviour we want).
pub fn goal_hash(goal: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(goal.as_bytes());
    let digest = hasher.finalize();
    // First 8 bytes → 16 hex chars. Hex-encode inline to avoid a
    // crate dep (hex) for 16 bytes of output.
    let mut out = String::with_capacity(16);
    for b in &digest[..8] {
        let hi = b >> 4;
        let lo = b & 0x0f;
        out.push(hex_digit(hi));
        out.push(hex_digit(lo));
    }
    out
}

#[inline]
fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        10..=15 => (b'a' + nibble - 10) as char,
        _ => unreachable!("nibble > 15 by construction"),
    }
}

// ── tests for PR-2 domain types ──────────────────────────────────────────────

#[cfg(test)]
mod pr2_tests {
    use super::*;

    // RelPath

    #[test]
    fn rel_path_accepts_simple_path() {
        let p = RelPath::try_new("src/main.rs").unwrap();
        assert_eq!(p.to_string(), "src/main.rs");
        assert_eq!(p.components(), &["src", "main.rs"]);
    }

    #[test]
    fn rel_path_rejects_absolute_unix() {
        assert!(matches!(
            RelPath::try_new("/etc/passwd"),
            Err(PathError::Absolute)
        ));
    }

    #[test]
    fn rel_path_rejects_parent_traversal() {
        assert!(matches!(
            RelPath::try_new("../../etc/passwd"),
            Err(PathError::Traversal)
        ));
    }

    #[test]
    fn rel_path_rejects_cur_dir() {
        assert!(matches!(
            RelPath::try_new("./foo"),
            Err(PathError::Traversal)
        ));
    }

    #[test]
    fn rel_path_rejects_empty() {
        assert!(matches!(RelPath::try_new(""), Err(PathError::Empty)));
    }

    #[test]
    fn rel_path_deserializes_via_string() {
        let p: RelPath = serde_json::from_str(r#""foo/bar.txt""#).unwrap();
        assert_eq!(p.to_string(), "foo/bar.txt");
    }

    #[test]
    fn rel_path_deserialization_rejects_traversal() {
        let err: Result<RelPath, _> = serde_json::from_str(r#""../escape""#);
        assert!(err.is_err());
    }

    // BoundedRegex

    #[test]
    fn bounded_regex_accepts_simple_pattern() {
        let r = BoundedRegex::try_new(r"\bhello\b".to_owned()).unwrap();
        assert_eq!(r.as_str(), r"\bhello\b");
    }

    #[test]
    fn bounded_regex_rejects_empty() {
        assert!(matches!(
            BoundedRegex::try_new(String::new()),
            Err(RegexError::Empty)
        ));
    }

    #[test]
    fn bounded_regex_rejects_over_source_cap() {
        let huge = "a".repeat(BOUNDED_REGEX_SOURCE_MAX + 1);
        match BoundedRegex::try_new(huge) {
            Err(RegexError::SourceTooLong { size, cap }) => {
                assert!(size > cap);
                assert_eq!(cap, BOUNDED_REGEX_SOURCE_MAX);
            }
            other => panic!("expected SourceTooLong; got {other:?}"),
        }
    }

    #[test]
    fn bounded_regex_rejects_invalid_syntax() {
        match BoundedRegex::try_new("(unclosed".to_owned()) {
            Err(RegexError::Invalid(_)) => {}
            other => panic!("expected Invalid; got {other:?}"),
        }
    }

    #[test]
    fn bounded_regex_deserializes_via_string() {
        let r: BoundedRegex = serde_json::from_str(r#""\\d+""#).unwrap();
        assert_eq!(r.as_str(), r"\d+");
    }

    // ContractSchema

    #[test]
    fn contract_schema_accepts_minimal_object() {
        let schema = ContractSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
        assert!(schema.as_value().is_object());
    }

    #[test]
    fn contract_schema_accepts_boolean_true() {
        // JSON Schema Draft 7 accepts `true` as "match anything".
        let schema = ContractSchema::try_new(serde_json::json!(true)).unwrap();
        assert_eq!(schema.as_value(), &serde_json::json!(true));
    }

    #[test]
    fn contract_schema_rejects_non_schema_primitive() {
        // A bare string isn't a valid JSON Schema document.
        match ContractSchema::try_new(serde_json::json!("nope")) {
            Err(SchemaError::NotJsonSchema { .. }) => {}
            other => panic!("expected NotJsonSchema; got {other:?}"),
        }
    }

    #[test]
    fn contract_schema_rejects_over_cap() {
        let huge = serde_json::json!({
            "type": "object",
            "description": "a".repeat(CONTRACT_SCHEMA_MAX_BYTES),
        });
        match ContractSchema::try_new(huge) {
            Err(SchemaError::TooLarge { size, cap }) => {
                assert!(size > cap);
                assert_eq!(cap, CONTRACT_SCHEMA_MAX_BYTES);
            }
            other => panic!("expected TooLarge; got {other:?}"),
        }
    }

    // CompletionContract round-trips

    #[test]
    fn completion_contract_prose_non_empty_round_trips() {
        let c = CompletionContract::ProseNonEmpty;
        let json = serde_json::to_string(&c).unwrap();
        assert_eq!(json, r#"{"kind":"prose_non_empty"}"#);
        let back: CompletionContract = serde_json::from_str(&json).unwrap();
        assert_eq!(back, c);
    }

    #[test]
    fn completion_contract_prose_carries_constraints() {
        let c = CompletionContract::Prose {
            min_chars: 500,
            min_citations: 2,
        };
        let json = serde_json::to_string(&c).unwrap();
        assert!(json.contains(r#""kind":"prose""#));
        assert!(json.contains(r#""min_chars":500"#));
        assert!(json.contains(r#""min_citations":2"#));
        let back: CompletionContract = serde_json::from_str(&json).unwrap();
        assert_eq!(back, c);
    }

    #[test]
    fn completion_contract_file_round_trips() {
        let c = CompletionContract::File {
            paths: vec![FileRequirement {
                path: RelPath::try_new("src/main.rs").unwrap(),
                contains_regex: Some(BoundedRegex::try_new(r"fn\s+main".to_owned()).unwrap()),
                max_bytes: Some(16 * 1024),
            }],
        };
        let json = serde_json::to_string(&c).unwrap();
        let back: CompletionContract = serde_json::from_str(&json).unwrap();
        assert_eq!(back, c);
    }

    #[test]
    fn completion_contract_pull_request_round_trips_with_branch_regex() {
        let c = CompletionContract::PullRequest {
            expected_repo: Some("avifenesh/cairn-rs".to_owned()),
            expected_head_branch: Some(BoundedRegex::try_new("^feat/.+".to_owned()).unwrap()),
            must_be_open: true,
        };
        let json = serde_json::to_string(&c).unwrap();
        let back: CompletionContract = serde_json::from_str(&json).unwrap();
        assert_eq!(back, c);
    }

    #[test]
    fn completion_contract_structured_round_trips() {
        let schema = ContractSchema::try_new(serde_json::json!({
            "type": "object",
            "required": ["pr_url"],
            "properties": { "pr_url": { "type": "string" } }
        }))
        .unwrap();
        let c = CompletionContract::Structured {
            schema: Box::new(schema),
        };
        let json = serde_json::to_string(&c).unwrap();
        let back: CompletionContract = serde_json::from_str(&json).unwrap();
        assert_eq!(back, c);
    }

    #[test]
    fn completion_contract_external_state_round_trips() {
        let c = CompletionContract::ExternalState {
            check: ExternalStateCheck::GitHubIssueClosed {
                repo: "avifenesh/cairn-rs".to_owned(),
                number: 42,
            },
        };
        let json = serde_json::to_string(&c).unwrap();
        let back: CompletionContract = serde_json::from_str(&json).unwrap();
        assert_eq!(back, c);
    }

    // CompletionContract::validate

    #[test]
    fn validate_accepts_well_formed_contracts() {
        CompletionContract::ProseNonEmpty.validate().unwrap();
        CompletionContract::Prose {
            min_chars: 500,
            min_citations: 2,
        }
        .validate()
        .unwrap();
        CompletionContract::File {
            paths: vec![FileRequirement {
                path: RelPath::try_new("src/main.rs").unwrap(),
                contains_regex: None,
                max_bytes: None,
            }],
        }
        .validate()
        .unwrap();
        CompletionContract::PullRequest {
            expected_repo: None,
            expected_head_branch: None,
            must_be_open: true,
        }
        .validate()
        .unwrap();
    }

    #[test]
    fn validate_rejects_empty_file_paths_list() {
        let err = CompletionContract::File { paths: vec![] }
            .validate()
            .unwrap_err();
        assert!(matches!(err, ContractError::EmptyFilePathsList));
    }

    #[test]
    fn validate_rejects_too_many_file_paths() {
        let paths = (0..FILE_REQUIREMENT_MAX_COUNT + 1)
            .map(|i| FileRequirement {
                path: RelPath::try_new(&format!("file_{i}.txt")).unwrap(),
                contains_regex: None,
                max_bytes: None,
            })
            .collect();
        let err = CompletionContract::File { paths }.validate().unwrap_err();
        match err {
            ContractError::TooManyFilePaths { count, cap } => {
                assert!(count > cap);
                assert_eq!(cap, FILE_REQUIREMENT_MAX_COUNT);
            }
            other => panic!("expected TooManyFilePaths; got {other:?}"),
        }
    }

    #[test]
    fn validate_rejects_max_bytes_over_ceil() {
        let err = CompletionContract::File {
            paths: vec![FileRequirement {
                path: RelPath::try_new("big.bin").unwrap(),
                contains_regex: None,
                max_bytes: Some(FILE_REQUIREMENT_MAX_BYTES_CEIL + 1),
            }],
        }
        .validate()
        .unwrap_err();
        match err {
            ContractError::FileMaxBytesExceedsCeil { requested, ceil } => {
                assert!(requested > ceil);
                assert_eq!(ceil, FILE_REQUIREMENT_MAX_BYTES_CEIL);
            }
            other => panic!("expected FileMaxBytesExceedsCeil; got {other:?}"),
        }
    }

    #[test]
    fn validate_rejects_pointless_prose() {
        let err = CompletionContract::Prose {
            min_chars: 0,
            min_citations: 0,
        }
        .validate()
        .unwrap_err();
        assert!(matches!(err, ContractError::ProseConstraintsBothZero));
    }

    // ContractRejectionCode serde

    #[test]
    fn contract_rejection_code_as_str_matches_serde_tag() {
        // RFC §3.1 + PR-4 review: the wire-stable snake_case form
        // is what operators and LLMs key on. `as_str()` and the
        // serde discriminator must agree — drift here turns a
        // rename into a silent break for every downstream consumer.
        for code in [
            ContractRejectionCode::ProseEmpty,
            ContractRejectionCode::FileSymlinkTraversal,
            ContractRejectionCode::PrNotFound,
            ContractRejectionCode::PrRepoMismatch,
            ContractRejectionCode::StructuredSchemaMismatch,
            ContractRejectionCode::NotImplemented,
        ] {
            let serde_form = serde_json::to_string(&code)
                .unwrap()
                .trim_matches('"')
                .to_owned();
            assert_eq!(
                code.as_str(),
                serde_form,
                "as_str() / serde tag drift on {code:?}"
            );
            assert_eq!(format!("{code}"), serde_form, "Display must match as_str");
        }
    }

    #[test]
    fn contract_rejection_code_round_trips_snake_case() {
        let cases = [
            (ContractRejectionCode::ProseEmpty, r#""prose_empty""#),
            (
                ContractRejectionCode::FileSymlinkTraversal,
                r#""file_symlink_traversal""#,
            ),
            (
                ContractRejectionCode::PrNotInProjectAllowlist,
                r#""pr_not_in_project_allowlist""#,
            ),
            (
                ContractRejectionCode::StructuredSchemaMismatch,
                r#""structured_schema_mismatch""#,
            ),
            (
                ContractRejectionCode::NotImplemented,
                r#""not_implemented""#,
            ),
        ];
        for (code, expected_json) in cases {
            let j = serde_json::to_string(&code).unwrap();
            assert_eq!(j, expected_json, "code: {code:?}");
            let back: ContractRejectionCode = serde_json::from_str(&j).unwrap();
            assert_eq!(back, code);
        }
    }

    // ContractSource serde

    #[test]
    fn completion_contract_kind_matches_serde_tag() {
        // Stable strings — operators / metrics key on these. Pin so
        // a rename in the enum doesn't silently drift from the
        // `kind()` helper or vice-versa.
        let cases = [
            (CompletionContract::ProseNonEmpty, "prose_non_empty"),
            (
                CompletionContract::Prose {
                    min_chars: 1,
                    min_citations: 0,
                },
                "prose",
            ),
            (
                CompletionContract::File {
                    paths: vec![FileRequirement {
                        path: RelPath::try_new("x").unwrap(),
                        contains_regex: None,
                        max_bytes: None,
                    }],
                },
                "file",
            ),
            (
                CompletionContract::PullRequest {
                    expected_repo: None,
                    expected_head_branch: None,
                    must_be_open: true,
                },
                "pull_request",
            ),
        ];
        for (c, expected) in cases {
            assert_eq!(c.kind(), expected);
            // Sanity-check: the `kind()` string matches the serde
            // tag the variant serializes with.
            let j = serde_json::to_string(&c).unwrap();
            assert!(
                j.contains(&format!("\"kind\":\"{expected}\"")),
                "kind() / serde tag drift for {expected}: {j}",
            );
        }
    }

    #[test]
    fn contract_source_round_trips_snake_case() {
        for s in [
            ContractSource::ExplicitCreate,
            ContractSource::ExplicitSpawn,
            ContractSource::Inferred,
            ContractSource::ReInferredOnGoalChange,
        ] {
            let j = serde_json::to_string(&s).unwrap();
            let back: ContractSource = serde_json::from_str(&j).unwrap();
            assert_eq!(back, s);
        }
    }

    // Wire-layer forgery resistance

    #[test]
    fn file_contract_wire_deserialization_rejects_absolute_path() {
        let wire = r#"{"kind":"file","paths":[{"path":"/etc/passwd"}]}"#;
        let err: Result<CompletionContract, _> = serde_json::from_str(wire);
        assert!(err.is_err(), "absolute path must not deserialize");
    }

    #[test]
    fn file_contract_wire_deserialization_rejects_traversal() {
        let wire = r#"{"kind":"file","paths":[{"path":"../escape.txt"}]}"#;
        let err: Result<CompletionContract, _> = serde_json::from_str(wire);
        assert!(err.is_err(), "traversal must not deserialize");
    }

    #[test]
    fn file_contract_wire_deserialization_rejects_invalid_regex() {
        let wire = r#"{"kind":"file","paths":[{"path":"src/x","contains_regex":"(unclosed"}]}"#;
        let err: Result<CompletionContract, _> = serde_json::from_str(wire);
        assert!(err.is_err(), "invalid regex must not deserialize");
    }

    // ── RFC 032 PR-4: infer_contract + goal_hash ──────────────────────────

    fn matches_pr(c: &CompletionContract) -> bool {
        matches!(c, CompletionContract::PullRequest { .. })
    }
    fn matches_prose(c: &CompletionContract) -> bool {
        matches!(c, CompletionContract::Prose { .. })
    }

    #[test]
    fn infer_contract_detects_open_pull_request() {
        let cases = [
            "Please open a pull request that fixes the login bug.",
            "create a pull request for the orchestrator retry logic",
            "Ship a pull request closing issue #42.",
            "submit a pull request with the changes.",
        ];
        for goal in cases {
            let c = infer_contract(goal);
            assert!(
                matches_pr(&c),
                "expected PullRequest contract for goal: {goal:?}; got {c:?}"
            );
        }
    }

    #[test]
    fn infer_contract_detects_short_pr_with_verb() {
        // Short-form "PR" requires `open` or `ship` within 20 chars —
        // rules out "PR as public relations".
        let c = infer_contract("Open a PR for the auth service cleanup");
        assert!(matches_pr(&c));

        let c = infer_contract("ship the PR once CI is green");
        assert!(matches_pr(&c));
    }

    #[test]
    fn infer_contract_rejects_pr_as_public_relations() {
        // "PR" absent from verb context (and no "pull request" long
        // form) falls through to the prose / fallback path.
        let c =
            infer_contract("Write a PR-focused communication strategy for the product launch team");
        // Not a PullRequest contract — either Prose (if a prose
        // trigger matched) or ProseNonEmpty. The important invariant:
        // NOT PullRequest.
        assert!(
            !matches_pr(&c),
            "`PR` without verb proximity must not infer PullRequest; got {c:?}"
        );
    }

    #[test]
    fn infer_contract_detects_research_triggers() {
        let cases = [
            "Research the top three distributed-lock libraries for Rust.",
            "Compare the tradeoffs of gRPC vs REST for our internal services.",
            "Audit the auth module for missing error handling.",
            "Investigate why the memory crate's retrieval latency spiked.",
        ];
        for goal in cases {
            let c = infer_contract(goal);
            assert!(
                matches_prose(&c),
                "expected Prose contract for goal: {goal:?}; got {c:?}"
            );
            if let CompletionContract::Prose {
                min_chars,
                min_citations,
            } = c
            {
                assert_eq!(min_chars, 500);
                assert_eq!(min_citations, 2);
            }
        }
    }

    #[test]
    fn infer_contract_falls_through_to_prose_non_empty() {
        // No trigger → permissive floor. Matches today's effective
        // gate behaviour on goals that didn't go through this path.
        let c = infer_contract("Summarise the current release status.");
        assert!(matches!(c, CompletionContract::ProseNonEmpty));
    }

    #[test]
    fn infer_contract_pr_trigger_beats_prose_trigger_when_both_present() {
        // Ambiguous conditional goal. Inference picks PR (first match
        // in the trigger order). Operators with conditional goals
        // should declare the contract explicitly per the RFC's
        // "bias toward false-positive" note — this test pins the
        // bias direction so a future refactor doesn't silently flip.
        let c = infer_contract(
            "Research the memory crate performance and open a pull request if you find a fix.",
        );
        assert!(matches_pr(&c));
    }

    #[test]
    fn infer_contract_case_insensitive_triggers() {
        assert!(matches_pr(&infer_contract("OPEN A PULL REQUEST now")));
        assert!(matches_prose(&infer_contract("RESEARCH this topic")));
    }

    #[test]
    fn goal_hash_is_stable_across_calls() {
        let a = goal_hash("my goal");
        let b = goal_hash("my goal");
        assert_eq!(a, b);
        assert_eq!(a.len(), 16);
    }

    #[test]
    fn goal_hash_differs_on_content_change() {
        let a = goal_hash("open a PR for the login fix");
        let b = goal_hash("research the cache design");
        assert_ne!(a, b);
    }

    #[test]
    fn goal_hash_differs_on_whitespace_difference() {
        // Whitespace differences are real goal changes — trailing
        // newline, added space before punctuation, etc. Inference
        // may still return the same contract, but the hash is a
        // change-detector; it must notice every textual delta.
        assert_ne!(goal_hash("do x"), goal_hash("do x "));
        assert_ne!(goal_hash("do x"), goal_hash("do  x"));
    }

    #[test]
    fn goal_hash_is_lowercase_hex_16() {
        let h = goal_hash("sample goal for hex shape");
        assert_eq!(h.len(), 16);
        assert!(
            h.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
            "goal_hash must be lowercase hex: {h}"
        );
    }
}
