//! RFC 029 PR-C: shape-only compliance suite for the KnowledgeProvider
//! plugin contract.
//!
//! The suite is deliberately shape-only — it does not exercise storage,
//! embeddings, or any backend-specific behaviour. It checks that the
//! wire contract between cairn's runtime and any knowledge-provider
//! plugin stays stable:
//!
//!   1. Every wire type in `cairn-plugin-proto::knowledge` round-trips
//!      through serde without losing fields.
//!   2. A well-formed `KnowledgeQueryResult` carries the required
//!      diagnostics fields.
//!   3. Tri-state scoring-dimension declarations match what the plugin
//!      actually surfaces on results (declaring `Surfaced` must mean
//!      the field is populated; declaring `NotSupported` must mean
//!      the field is `null` on the wire).
//!   4. Runtime-owned dimensions (graph_proximity, source_credibility,
//!      corroboration) populated by a provider are unconditionally
//!      overwritten by cairn's post-hoc rescorer.
//!   5. Error shapes (`RetrievalError::ProviderUnavailable`,
//!      `IngestError::ProviderRejected`, `IngestError::ProviderUnavailable`)
//!      carry stable Display text.
//!   6. After rescoring, `diagnostics.scoring_dimensions_used` carries
//!      `computed_by = runtime_post_hoc` markers for each runtime-owned
//!      dim.
//!
//! The suite runs against two fixtures in PR-C:
//!
//! - **cairn-default path**: a `MultiProviderRetrieval` wired to
//!   `InMemoryRetrieval` + `PostHocRescorer`. Proves the in-process
//!   default satisfies the contract.
//! - **Mock plugin path**: a minimal in-process mock
//!   `KnowledgePluginDispatcher` returning canned wire responses.
//!   Proves the plugin-dispatch path satisfies the contract without
//!   needing a real subprocess.
//!
//! The suite's public API is a collection of `check_*` functions that
//! take a runtime configuration and return structured results. Each
//! check is independently exercised by a `#[tokio::test]` in the
//! crate's `tests/` directory.

pub mod checks;
pub mod fixtures;

pub use checks::{
    check_diagnostics_computed_by_markers, check_error_shape_stability,
    check_required_field_presence, check_runtime_owned_overwritten,
    check_tri_state_matches_surfaced, check_wire_type_round_trips, ComplianceError,
    ComplianceResult,
};
