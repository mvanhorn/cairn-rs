use async_trait::async_trait;
use cairn_domain::{ApprovalDecision, ApprovalId, ApprovalRequirement, ProjectKey, RunId, TaskId};
use serde::{Deserialize, Serialize};

use crate::error::StoreError;

/// Audit record for a single `ApprovalDelegated` event.
///
/// Each delegation is a discrete audit entry — replaying the event is
/// idempotent on `(approval_id, delegation_id)` (pg/sqlite PK), while
/// successive delegations of the same approval (re-delegation over time,
/// to different operators, or rapid re-delegation to the *same* operator
/// within the same millisecond) produce distinct rows because the
/// runtime service mints a monotonic `delegation_id` per emit.
///
/// `delegation_id` is `#[serde(default)]` for backward-compat with
/// pre-Phase-2a.2 event-log entries that predate the field; such rows
/// round-trip as `""` and coexist with post-field rows in the same
/// table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalDelegationRecord {
    pub approval_id: ApprovalId,
    pub delegated_to: String,
    pub delegated_at_ms: u64,
    #[serde(default)]
    pub delegation_id: String,
}

/// Current-state record for an approval request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApprovalRecord {
    pub approval_id: ApprovalId,
    pub project: ProjectKey,
    pub run_id: Option<RunId>,
    pub task_id: Option<TaskId>,
    pub requirement: ApprovalRequirement,
    pub decision: Option<ApprovalDecision>,
    /// Product-level title for operator/SSE surfaces.
    pub title: Option<String>,
    /// Product-level description/context for operator/SSE surfaces.
    pub description: Option<String>,
    pub version: u64,
    pub created_at: u64,
    pub updated_at: u64,
}

/// Read-model for approval current state.
#[async_trait]
pub trait ApprovalReadModel: Send + Sync {
    async fn get(&self, approval_id: &ApprovalId) -> Result<Option<ApprovalRecord>, StoreError>;

    /// List pending approvals for a project (operator inbox).
    async fn list_pending(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ApprovalRecord>, StoreError>;

    /// List all approvals (pending + resolved) for a project.
    async fn list_all(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ApprovalRecord>, StoreError>;

    /// Check if a run has any pending (unresolved) approvals.
    async fn has_pending_for_run(&self, run_id: &RunId) -> Result<bool, StoreError>;
}

/// Read-model for the delegation audit trail.
///
/// Separate from `ApprovalReadModel` because the storage shape is
/// append-only audit rows rather than current-state rows — compare
/// `CredentialRotationReadModel` vs `CredentialReadModel`.
#[async_trait]
pub trait ApprovalDelegationReadModel: Send + Sync {
    /// List every delegation for the given approval, ordered by
    /// `(delegated_at_ms ASC, delegation_id ASC)` — oldest-first audit
    /// trail with a stable tiebreaker when two delegations share the
    /// same millisecond. The `delegation_id` is monotonic per emit so
    /// within a single ms it's also in mint-order.
    async fn list_for_approval(
        &self,
        approval_id: &ApprovalId,
    ) -> Result<Vec<ApprovalDelegationRecord>, StoreError>;
}
