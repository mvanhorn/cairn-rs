//! RFC 031 PR-B: `project_agent_roles` projection.
//!
//! `AgentRoleDefined` upserts; `AgentRoleRetracted` sets
//! `retracted_at`. Uniqueness constraint is
//! `(tenant_id, workspace_id, project_id, role_id) WHERE retracted_at IS NULL`
//! per §D6. Re-POST after retract atomically clears `retracted_at`
//! back to `NULL` on the matching row.

use async_trait::async_trait;
use cairn_domain::agent_roles::AgentRole;
use cairn_domain::{OperatorId, ProjectKey};
use serde::{Deserialize, Serialize};

use crate::error::StoreError;

/// One row of the `project_agent_roles` projection.
///
/// `retracted_at.is_none()` ⇔ "active row". The `resolve` and `list`
/// paths filter on that; the event log retains full history for
/// audit regardless of projection state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRoleRecord {
    pub project: ProjectKey,
    pub role_id: String,
    pub role: AgentRole,
    /// `Some("reviewer")` etc. when the id matches a built-in and
    /// this row shadows it. `None` for novel ids.
    pub shadows_builtin: Option<String>,
    pub defined_by: OperatorId,
    /// Event-log timestamp (ms since epoch). Used as the `ETag`
    /// value on the HTTP surface for `If-Match` lost-update
    /// protection.
    pub defined_at: u64,
    pub retracted_at: Option<u64>,
    pub retracted_by: Option<OperatorId>,
}

impl AgentRoleRecord {
    /// `true` iff `retracted_at.is_none()` — the row is the
    /// currently-active definition for `(project, role_id)`.
    pub fn is_active(&self) -> bool {
        self.retracted_at.is_none()
    }
}

/// Read-model for `project_agent_roles`. Both the orchestrator
/// (via `AgentRoleService::resolve` / `list`) and the HTTP handlers
/// consume this trait.
#[async_trait]
pub trait AgentRoleReadModel: Send + Sync {
    /// Return the active row for `(project, role_id)` if one exists.
    /// "Active" means `retracted_at IS NULL` per §D6; retracted rows
    /// are filtered out.
    async fn get_active(
        &self,
        project: &ProjectKey,
        role_id: &str,
    ) -> Result<Option<AgentRoleRecord>, StoreError>;

    /// Return the latest row for `(project, role_id)` regardless of
    /// active/retracted status. Used by the re-POST-after-retract
    /// path to decide between 201-upsert vs 409-conflict.
    async fn get_any(
        &self,
        project: &ProjectKey,
        role_id: &str,
    ) -> Result<Option<AgentRoleRecord>, StoreError>;

    /// Return every active row for the project, sorted by `role_id`
    /// ascending so the HTTP GET-list response is stable.
    async fn list_active(&self, project: &ProjectKey) -> Result<Vec<AgentRoleRecord>, StoreError>;
}
