//! RFC 014 / RFC-025 Phase 2b.2b m3: read-model for subagent spawn linkage.
//!
//! `SubagentSpawned` records the parent→child linkage when an agent
//! creates a subagent task. The cairn-store projection captures the
//! spawn event itself (distinct from the `tasks` row's
//! `parent_run_id` / `parent_task_id` fields which the in-memory
//! applier also updates), giving operator dashboards a row-per-spawn
//! audit surface without walking the event log.

use async_trait::async_trait;
use cairn_domain::tenancy::ProjectKey;
use cairn_domain::{RunId, SessionId, TaskId};

use crate::error::StoreError;

/// One row per `SubagentSpawned` event. `spawned_at_ms` is the
/// projection-time wall clock (millisecond) captured by the applier,
/// mirroring the pattern used by `SessionCreated` + `RunCreated`
/// which also have no on-event timestamp.
///
/// `goal` + `role` capture the LLM's delegation intent (`#670` G2):
/// the sub-goal the parent run asked for and the agent role it
/// delegated to. Both are empty strings on pre-G2 rows (pre-existing
/// event-log entries that predate the G2 extension).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubagentSpawnRecord {
    pub child_task_id: TaskId,
    pub project: ProjectKey,
    pub parent_run_id: RunId,
    pub parent_task_id: Option<TaskId>,
    pub child_session_id: SessionId,
    pub child_run_id: Option<RunId>,
    pub spawned_at_ms: u64,
    pub goal: String,
    pub role: String,
}

#[async_trait]
pub trait SubagentSpawnReadModel: Send + Sync {
    /// Look up the spawn record by child task id. Used by the
    /// orchestrator to resolve the parent lineage on subagent
    /// completion.
    async fn get_by_child_task(
        &self,
        child_task_id: &TaskId,
    ) -> Result<Option<SubagentSpawnRecord>, StoreError>;

    /// #670 G5: look up the spawn record by child **run** id (not task).
    /// Feeds the `RunService::{complete,fail,cancel}` terminal hook
    /// that maps a terminating child back to the parent's waitpoint
    /// key. The child's RunRecord carries `parent_run_id` but NOT
    /// `child_task_id`; the spawn record carries both, so the terminal
    /// hook resolves child_task_id → waitpoint via this method.
    ///
    /// Semantically a point lookup. pg/sqlite serve the query via
    /// the partial index `idx_subagent_spawns_child_run_id` (pg
    /// V070, sqlite schema.rs). In-memory does a linear scan of the
    /// all-spawns HashMap — acceptable for the in-memory backend's
    /// dev-only scope and the terminal-hook fire-and-forget call
    /// path.
    async fn get_by_child_run_id(
        &self,
        child_run_id: &RunId,
    ) -> Result<Option<SubagentSpawnRecord>, StoreError>;

    /// Enumerate all subagents spawned from a single parent run, in
    /// `(spawned_at_ms ASC, child_task_id ASC)` order.
    async fn list_by_parent_run(
        &self,
        parent_run_id: &RunId,
    ) -> Result<Vec<SubagentSpawnRecord>, StoreError>;
}
