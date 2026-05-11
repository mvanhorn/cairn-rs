//! RFC 020 Track 3 / RFC-025 Phase 2b.2b m6: read-model for tool
//! recovery pauses.
//!
//! Emitted when a tool call classified as `DangerousPause` cannot be
//! safely re-dispatched on recovery — the run transitions to
//! `WaitingApproval` and the operator must confirm before proceeding.

use async_trait::async_trait;
use cairn_domain::tenancy::ProjectKey;
use cairn_domain::{RunId, TaskId};

use crate::error::StoreError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolRecoveryPauseRecord {
    pub tool_call_id: String,
    pub project: ProjectKey,
    pub run_id: RunId,
    pub task_id: Option<TaskId>,
    pub tool_name: String,
    pub reason: String,
    pub paused_at_ms: u64,
}

#[async_trait]
pub trait ToolRecoveryPauseReadModel: Send + Sync {
    async fn get(&self, tool_call_id: &str) -> Result<Option<ToolRecoveryPauseRecord>, StoreError>;

    /// List pauses on a run ordered by `(paused_at_ms ASC, tool_call_id ASC)`.
    async fn list_by_run(&self, run_id: &RunId)
        -> Result<Vec<ToolRecoveryPauseRecord>, StoreError>;
}
