//! #789 — per-iteration reasoning step projection.
//!
//! Materializes the compacted chain-of-thought records emitted by the
//! orchestrator on every DECIDE phase. Operators read this projection
//! through two endpoints:
//!
//! - `GET /v1/admin/agents/live` — scans the most-recent step for
//!   each active run to produce a fleet view.
//! - `GET /v1/runs/:id/trajectory` — returns all steps for one run
//!   in chronological order for post-mortem replay.

use async_trait::async_trait;
use cairn_domain::{
    events::{ProposedActionSummary, RunReasoningStep},
    RunId, SessionId,
};
use serde::{Deserialize, Serialize};

use crate::error::StoreError;

/// Read-model row for a single iteration of a single run. Mirrors the
/// fields on `RunReasoningStep` plus a small amount of operator-facing
/// metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReasoningStepRecord {
    pub run_id: RunId,
    pub session_id: SessionId,
    pub iteration: u32,
    pub recorded_at_ms: u64,
    pub model_id: String,
    pub reasoning_compact: String,
    pub proposed_action: ProposedActionSummary,
    pub step_history_snapshot: String,
    pub confidence: f64,
}

impl ReasoningStepRecord {
    pub fn from_event(event: &RunReasoningStep) -> Self {
        Self {
            run_id: event.run_id.clone(),
            session_id: event.session_id.clone(),
            iteration: event.iteration,
            recorded_at_ms: event.recorded_at_ms,
            model_id: event.model_id.clone(),
            reasoning_compact: event.reasoning_compact.clone(),
            proposed_action: event.proposed_action.clone(),
            step_history_snapshot: event.step_history_snapshot.clone(),
            confidence: event.confidence,
        }
    }
}

/// Read-model trait for fetching reasoning steps. Both backends
/// (in-memory + future pg/sqlite) implement this; handlers consume
/// `&dyn ReasoningStepReadModel` so the endpoint code stays
/// backend-agnostic.
#[async_trait]
pub trait ReasoningStepReadModel: Send + Sync {
    /// All steps for one run in chronological order (oldest first).
    /// `limit` caps the number returned; pass `usize::MAX` for "all".
    async fn list_by_run(
        &self,
        run_id: &RunId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ReasoningStepRecord>, StoreError>;

    /// Most-recent step for one run, or `None` if the run has not yet
    /// emitted a reasoning step. Cheap by design — the in-memory
    /// implementation is O(1) on the per-run vector tail.
    async fn latest_for_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<ReasoningStepRecord>, StoreError>;

    /// Total count of reasoning steps for one run. Used by the
    /// `GET /v1/runs/:id/trajectory` endpoint to populate `total`
    /// independently of the paginated `items` slice (Gemini PR #794
    /// review). On the in-memory backend this is `vec.len()`.
    async fn count_for_run(&self, run_id: &RunId) -> Result<usize, StoreError>;
}

/// Per-run cap on how many reasoning steps the in-memory store
/// retains. Bounds memory growth on long-running runs.
pub const REASONING_STEP_CAP_PER_RUN: usize = 200;
