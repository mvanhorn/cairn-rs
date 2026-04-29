//! RFC-025 Phase 2b.2b m4: read-model for user-sent messages on a run.
//!
//! `UserMessageAppended` is emitted for chat-style follow-ups on a
//! paused run, operator "/intervene" injects, and API-posted follow-up
//! prompts. The projection keys on `(run_id, sequence)` so the
//! client-supplied `sequence` drives stable ordering within a run.

use async_trait::async_trait;
use cairn_domain::tenancy::ProjectKey;
use cairn_domain::{RunId, SessionId};

use crate::error::StoreError;

/// One row per `UserMessageAppended` event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserMessageRecord {
    pub run_id: RunId,
    pub sequence: u64,
    pub project: ProjectKey,
    pub session_id: SessionId,
    /// Envelope event id — used for replay dedupe + traceability.
    pub event_id: String,
    pub content: String,
    pub appended_at_ms: u64,
}

#[async_trait]
pub trait UserMessageReadModel: Send + Sync {
    /// List user messages for a run, ordered `(sequence ASC, appended_at_ms ASC)`.
    /// `limit` is the maximum number of rows returned; `offset` supports
    /// paged traversal over long histories.
    async fn list_by_run(
        &self,
        run_id: &RunId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<UserMessageRecord>, StoreError>;

    /// Count of user messages appended to a run. Used by UI badges
    /// without forcing a full list fetch.
    async fn count_by_run(&self, run_id: &RunId) -> Result<u64, StoreError>;
}
