use async_trait::async_trait;
use cairn_domain::{IssueBudget, ProjectKey, SessionId, SessionState};
use serde::{Deserialize, Serialize};

use crate::error::StoreError;

/// Default cap on session attempts when none has been configured explicitly.
///
/// Referenced from PR-3 when the circuit-breaker enforcement lands; exposed
/// here so construction sites that default the field stay consistent.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 5;

/// Current-state record for a session.
///
/// The F65 fields (`goal_title`, `issue_budget`, `max_attempts`,
/// `attempts_used`) are strictly additive: every new field carries
/// `#[serde(default)]` so event logs written before F65 PR-1 deserialize
/// cleanly and the projection falls back to empty / zero / default-capped
/// values. PR-2 wires projection writers; PR-3 enforces `max_attempts` via
/// the circuit breaker; PR-6 populates the summarizer-backed outcome chain.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionRecord {
    pub session_id: SessionId,
    pub project: ProjectKey,
    pub state: SessionState,
    pub version: u64,
    pub created_at: u64,
    pub updated_at: u64,
    /// F65: operator-visible short title describing the session's goal.
    /// Populated by PR-2 projection writers; empty on legacy replay.
    #[serde(default)]
    pub goal_title: Option<String>,
    /// F65: per-session budget envelope. `None` means "no session-level
    /// override"; per-run defaults still apply under the circuit breaker.
    #[serde(default)]
    pub issue_budget: Option<IssueBudget>,
    /// F65: maximum session attempts. Defaults to [`DEFAULT_MAX_ATTEMPTS`]
    /// when deserialised from a legacy record that predates the field.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// F65: count of attempts used so far within the session. Pre-F65
    /// records deserialise to `0`.
    #[serde(default)]
    pub attempts_used: u32,
}

fn default_max_attempts() -> u32 {
    DEFAULT_MAX_ATTEMPTS
}

/// Read-model for session current state.
#[async_trait]
pub trait SessionReadModel: Send + Sync {
    async fn get(&self, session_id: &SessionId) -> Result<Option<SessionRecord>, StoreError>;

    async fn list_by_project(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<SessionRecord>, StoreError>;

    /// RFC fleet view: list non-terminal sessions across all projects in a workspace.
    ///
    /// Returns at most `limit` sessions sorted by `updated_at` descending.
    /// Used by `GET /v1/fleet` to enumerate active agent sessions.
    async fn list_active(&self, limit: usize) -> Result<Vec<SessionRecord>, StoreError>;
}
