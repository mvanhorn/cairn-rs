use async_trait::async_trait;
use cairn_domain::{CheckpointStrategy, ProjectKey, RunId};

use crate::error::StoreError;

#[async_trait]
pub trait CheckpointStrategyReadModel: Send + Sync {
    async fn get_by_run(&self, run_id: &RunId) -> Result<Option<CheckpointStrategy>, StoreError>;
}

/// Default `max_checkpoints` applied when the event payload carries
/// `max_checkpoints = 0`. Shared by pg / sqlite / in-memory appliers so
/// a 0-valued event rehydrates identically on every backend.
pub const CHECKPOINT_STRATEGY_DEFAULT_MAX_CHECKPOINTS: u32 = 10;

/// Sentinel `ProjectKey` returned on `get_by_run` for rows written by
/// `CheckpointStrategySet`. The event payload carries no project scope
/// (checkpoint cadence is a run-local policy, and `run_id` is the
/// indexed key), so pg/sqlite/in-memory all surface the same sentinel
/// rather than inventing a plausible-but-wrong tenant/workspace/project
/// triple at read time.
pub fn checkpoint_strategy_sentinel_project() -> ProjectKey {
    ProjectKey::new("_strategy", "_strategy", "_strategy")
}
