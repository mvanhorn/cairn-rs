use async_trait::async_trait;
use cairn_domain::{RunId, ToolInvocationId};

use crate::error::StoreError;

pub use cairn_domain::tool_invocation::{ToolInvocationRecord, ToolInvocationState};

use cairn_domain::{ProjectKey, TaskId};

/// Latest-progress row for a tool invocation.
///
/// One row per `invocation_id`, upserted on every
/// `RuntimeEvent::ToolInvocationProgressUpdated`. Carries the project key
/// so `GET /v1/tool-invocations/:id/progress` can filter by
/// `tenant_scope` without a second hop through `ToolInvocationReadModel`.
///
/// Replaces the previous `read_stream(None, 10_000)` scan in
/// `get_tool_invocation_progress_handler` — that scan was both a DoS
/// risk (bounded by a fixed 10k window that silently masked data past
/// it) and a cross-tenant read (returned any tenant's progress).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolInvocationProgressRecord {
    pub invocation_id: ToolInvocationId,
    pub project: ProjectKey,
    pub progress_pct: u8,
    pub message: Option<String>,
    pub updated_at_ms: u64,
}

/// Read-model for the latest progress update of a tool invocation.
/// Backed by `tool_invocation_progress` in pg/sqlite and an in-memory
/// map in the `--db memory` backend.
#[async_trait]
pub trait ToolInvocationProgressReadModel: Send + Sync {
    /// Return the most recent progress row for the given invocation, or
    /// `None` when the invocation has not reported progress yet.
    async fn get(
        &self,
        invocation_id: &ToolInvocationId,
    ) -> Result<Option<ToolInvocationProgressRecord>, StoreError>;
}

/// F52: projected row of `ToolInvocationCacheHit`. One per cache-hit
/// event. Operators can count/list via this read model without scanning
/// the event log; backends must write one row per event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolInvocationCacheHitRecord {
    pub invocation_id: ToolInvocationId,
    pub project: ProjectKey,
    pub run_id: Option<RunId>,
    pub task_id: Option<TaskId>,
    pub tool_name: String,
    pub tool_call_id: String,
    pub original_completed_at_ms: u64,
    pub served_at_ms: u64,
}

/// F52: read-model for cache hits. Optional by design — infra that only
/// cares about execution state can keep ignoring the table.
#[async_trait]
pub trait ToolInvocationCacheHitReadModel: Send + Sync {
    /// List cache hits for a run in served-at order (newest first by
    /// default at the backend level).
    async fn list_by_run(
        &self,
        run_id: &RunId,
        limit: usize,
    ) -> Result<Vec<ToolInvocationCacheHitRecord>, StoreError>;

    /// Count cache hits for a run.
    async fn count_by_run(&self, run_id: &RunId) -> Result<usize, StoreError>;
}

/// Read-model for tool invocation current state.
#[async_trait]
pub trait ToolInvocationReadModel: Send + Sync {
    async fn get(
        &self,
        invocation_id: &ToolInvocationId,
    ) -> Result<Option<ToolInvocationRecord>, StoreError>;

    /// List tool invocations for a run (timeline view).
    async fn list_by_run(
        &self,
        run_id: &RunId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ToolInvocationRecord>, StoreError>;
}

#[cfg(test)]
mod tests {
    use super::ToolInvocationState;

    #[test]
    fn terminal_states_are_correct() {
        assert!(ToolInvocationState::Completed.is_terminal());
        assert!(ToolInvocationState::Failed.is_terminal());
        assert!(ToolInvocationState::Canceled.is_terminal());
        assert!(!ToolInvocationState::Requested.is_terminal());
        assert!(!ToolInvocationState::Started.is_terminal());
    }
}
