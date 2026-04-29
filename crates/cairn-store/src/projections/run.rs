use async_trait::async_trait;
use cairn_domain::{
    CompletionVerification, FailureClass, PauseReason, ProjectKey, PromptReleaseId, ResumeTrigger,
    RunId, RunState, SessionId, TenantId,
};
use serde::{Deserialize, Serialize};

use crate::error::StoreError;

/// Current-state record for a run.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunRecord {
    pub run_id: RunId,
    pub session_id: SessionId,
    pub parent_run_id: Option<RunId>,
    pub project: ProjectKey,
    pub state: RunState,
    pub prompt_release_id: Option<PromptReleaseId>,
    /// GAP-011: role attached at run creation (e.g. "researcher", "executor").
    #[serde(default)]
    pub agent_role_id: Option<String>,
    pub failure_class: Option<FailureClass>,
    pub pause_reason: Option<PauseReason>,
    pub resume_trigger: Option<ResumeTrigger>,
    pub version: u64,
    pub created_at: u64,
    pub updated_at: u64,
    /// F47 PR2: LLM free-text summary from `LoopTermination::Completed`.
    /// `None` until the run terminates via the normal completion path;
    /// also `None` for records projected from pre-F47-PR2 event logs
    /// (no `RunCompletionAnnotated` ever landed).
    ///
    /// `skip_serializing_if`: the public run surface exposes completion
    /// only via the top-level `completion: RunCompletion` object on
    /// `GET /v1/runs/:id` (F47 PR2). Emitting `completion_summary: null`
    /// on `RunRecord` / `RunRecordView` would double-publish the same
    /// data and leak an internal field name into every list/detail
    /// response — Copilot review on #313 flagged this. Keep the
    /// projection field so the store roundtrips correctly, but omit
    /// it from wire responses unless populated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_summary: Option<String>,
    /// F47 PR2: extractor-produced evidence from tool_results observed
    /// during the run. Paired with `completion_summary`; both populate
    /// on `RunCompletionAnnotated`. `None` mirrors the summary —
    /// post-completion annotation has not (yet) been projected.
    ///
    /// `skip_serializing_if`: see `completion_summary` above — the
    /// top-level `completion` REST object is the intended public
    /// surface; the projection field stays but is omitted from the
    /// wire unless populated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_verification: Option<CompletionVerification>,
    /// F47 PR2: wall-clock ms when `RunCompletionAnnotated` was emitted.
    /// Distinct from `updated_at` because `updated_at` is set by the
    /// projection applier to the current wall-clock on every event it
    /// handles; `completion_annotated_at_ms` is the domain-time the
    /// orchestrator recorded the completion.
    ///
    /// `skip_serializing_if`: see `completion_summary` above.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_annotated_at_ms: Option<u64>,
    /// F64: summary of the most recent terminal-write recovery loop, if
    /// one fired for this run. `None` on the hot path (no recovery
    /// needed). Populated by the `TerminalRecoveryAttempted` event
    /// projection and stored as JSON alongside the run row.
    ///
    /// This is a supported optional API field on `RunRecord` — it
    /// appears in the OpenAPI spec and is intended for operator +
    /// audit inspection of runs that hit the recovery path. The
    /// underlying bridge loop retires when FF#371 lands upstream; at
    /// that point writes stop, but existing annotations remain
    /// queryable and the column/event variant stay in the schema.
    ///
    /// `skip_serializing_if`: runs that never hit the recovery path
    /// stay silent in the response body — no noisy
    /// `terminal_write_recovery: null` on every run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_write_recovery: Option<TerminalRecoveryRecord>,
}

/// F64: projection shape for the latest terminal-write recovery attempt.
/// Mirrors `cairn_domain::events::TerminalRecoveryAttempted` minus the
/// ProjectKey + RunId (already carried by the parent `RunRecord`).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TerminalRecoveryRecord {
    pub fcall: String,
    pub attempts: u32,
    pub wall_time_ms: u64,
    pub outcome: String,
    pub occurred_at_ms: u64,
}

/// Read-model for run current state.
#[async_trait]
pub trait RunReadModel: Send + Sync {
    async fn get(&self, run_id: &RunId) -> Result<Option<RunRecord>, StoreError>;

    async fn list_by_session(
        &self,
        session_id: &SessionId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<RunRecord>, StoreError>;

    /// List non-terminal runs in a session (used by session state derivation).
    async fn any_non_terminal(&self, session_id: &SessionId) -> Result<bool, StoreError>;

    /// Get the latest root run (no parent_run_id) in a session.
    async fn latest_root_run(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<RunRecord>, StoreError>;

    /// List runs in a specific state (used by recovery sweeps).
    async fn list_by_state(
        &self,
        state: RunState,
        limit: usize,
    ) -> Result<Vec<RunRecord>, StoreError>;

    /// RFC 010: list non-terminal (active) runs across ALL sessions in a project.
    ///
    /// Operators must be able to view active runs regardless of which session
    /// originated them — session membership is irrelevant to the control-plane
    /// view.
    async fn list_active_by_project(
        &self,
        project: &ProjectKey,
        limit: usize,
    ) -> Result<Vec<RunRecord>, StoreError>;

    /// List child runs of `parent_run_id`, ordered `(created_at, run_id)` ASC.
    ///
    /// Postgres / SQLite use `idx_runs_parent` (partial index on
    /// `parent_run_id WHERE NOT NULL`); InMemoryStore filters the live
    /// map. Caller supplies `limit`; there is no implicit cap.
    async fn list_by_parent_run(
        &self,
        parent_run_id: &RunId,
        limit: usize,
    ) -> Result<Vec<RunRecord>, StoreError>;

    /// List stalled runs for a tenant — non-terminal runs that have
    /// not updated their projection row for longer than
    /// `stale_after_ms` relative to `now_ms` (issue #570).
    ///
    /// Combines state + staleness at the query surface so callers
    /// stop scanning 10 000 Running + 10 000 Pending rows + filtering
    /// in memory on every `/v1/runs/stalled` refresh (the pre-#570
    /// shape). Results are ordered `updated_at ASC, run_id ASC` so
    /// the most-stale runs surface on page 1.
    ///
    /// Callers pass `limit + 1` to detect `has_more`. The InMemory
    /// implementation filters in-memory; pg/sqlite implementations
    /// (if added later) should apply the predicate at the SQL layer.
    async fn list_stalled(
        &self,
        tenant_id: &TenantId,
        now_ms: u64,
        stale_after_ms: u64,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<RunRecord>, StoreError>;
}
