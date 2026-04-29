use async_trait::async_trait;
use cairn_domain::{
    EvalMetrics, EvalRunId, OperatorId, ProjectKey, PromptAssetId, PromptReleaseId, PromptVersionId,
};
use serde::{Deserialize, Serialize};

use crate::error::StoreError;

/// Current-state record for an eval run projection.
///
/// Shape grew in RFC-025 Phase 1 (milestone 6) to carry the dataset /
/// rubric / baseline / prompt-release bindings that were previously held
/// only on `state.evals` in-memory. The projection now owns every field
/// that the `GET /v1/evals/runs/:id` handler needs to respond after a
/// restart, so `replay_evals` could be deleted (#437) without regressing
/// `eval_dataset_id_survives_restart` / `eval_run_linkage_survives_restart`.
///
/// All newly-added fields carry `#[serde(default)]` so event-log entries
/// persisted before Phase 1 (which only had `EvalRunStarted/Completed/
/// Archived` + a narrow projection record) deserialise cleanly.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvalRunRecord {
    pub eval_run_id: EvalRunId,
    pub project: ProjectKey,
    pub subject_kind: String,
    pub evaluator_type: String,
    pub success: Option<bool>,
    pub error_message: Option<String>,
    pub started_at: u64,
    pub completed_at: Option<u64>,
    /// Issue #244: `Some(ts)` when the run has been soft-deleted. `None` for
    /// active runs. `#[serde(default)]` so event-log entries persisted before
    /// the column existed still deserialise.
    #[serde(default)]
    pub archived_at: Option<u64>,
    /// RFC-025 Phase 1 (milestone 3/4/5): most recent `EvalMetrics` snapshot
    /// from an `EvalRunScored` (or `EvalRunCompleted`) event. `None` until
    /// a score lands. Projected from the `metrics_json` TEXT column on
    /// pg/sqlite (a portable JSON-in-TEXT shape per
    /// `feedback_no_db_specific_features.md`) and from an in-memory
    /// `EvalMetrics` on the InMemoryStore. `#[serde(default)]` so
    /// pre-Phase-1 event logs deserialise cleanly.
    #[serde(default)]
    pub metrics: Option<EvalMetrics>,
    /// RFC-025 Phase 1: most recent rubric verdict from an
    /// `EvalRubricScored` event. `None` until the operator posts to
    /// `POST /v1/evals/runs/:id/rubric-score`.
    #[serde(default)]
    pub rubric_score: Option<EvalRubricScoreSummary>,
    /// Issue #220: dataset bound to this run at create time. Stored on
    /// `EvalRunStarted` and projected here so `GET /v1/evals/runs/:id`
    /// continues to surface it after a process restart (the test
    /// `eval_dataset_id_survives_restart` locks this contract).
    #[serde(default)]
    pub dataset_id: Option<String>,
    /// Issue #223: rubric bound to the run at create time.
    #[serde(default)]
    pub rubric_id: Option<String>,
    /// Issue #223: baseline bound to the run at create time.
    #[serde(default)]
    pub baseline_id: Option<String>,
    /// RFC 004: prompt asset being evaluated, when applicable.
    #[serde(default)]
    pub prompt_asset_id: Option<PromptAssetId>,
    #[serde(default)]
    pub prompt_version_id: Option<PromptVersionId>,
    #[serde(default)]
    pub prompt_release_id: Option<PromptReleaseId>,
    /// Operator who created the run, if captured on `EvalRunStarted`.
    #[serde(default)]
    pub created_by: Option<OperatorId>,
}

/// Per-run rubric verdict summary projected from `EvalRubricScored`.
///
/// Stored as a serde-JSON blob on pg/sqlite (TEXT column) and as an
/// in-memory struct on the `InMemoryStore`; the read shape is the same
/// across backends so the parity harness can byte-compare.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvalRubricScoreSummary {
    pub rubric_id: String,
    pub dimension_scores: Vec<(String, f64)>,
    pub overall: f64,
    pub recorded_at_ms: u64,
}

impl PartialEq for EvalRubricScoreSummary {
    fn eq(&self, other: &Self) -> bool {
        self.rubric_id == other.rubric_id
            && self.recorded_at_ms == other.recorded_at_ms
            && self.overall.to_bits() == other.overall.to_bits()
            && self.dimension_scores.len() == other.dimension_scores.len()
            && self
                .dimension_scores
                .iter()
                .zip(other.dimension_scores.iter())
                .all(|((an, av), (bn, bv))| an == bn && av.to_bits() == bv.to_bits())
    }
}

/// Read-model for eval run current state.
#[async_trait]
pub trait EvalRunReadModel: Send + Sync {
    async fn get(&self, eval_run_id: &EvalRunId) -> Result<Option<EvalRunRecord>, StoreError>;

    async fn list_by_project(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EvalRunRecord>, StoreError>;
}
