use async_trait::async_trait;
use cairn_domain::{OperatorId, ProjectKey, RunId, SessionId};
use serde::{Deserialize, Serialize};

use crate::error::StoreError;

/// RFC 018 plan-review lifecycle state. Snake-case for wire/storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanReviewState {
    /// `PlanProposed` recorded; awaiting operator action.
    Proposed,
    /// `PlanApproved` recorded; an Execute-mode run is expected next.
    Approved,
    /// `PlanRejected` recorded; no execution will follow.
    Rejected,
    /// `PlanRevisionRequested` recorded; a new Plan-mode run follows.
    RevisionRequested,
}

/// RFC-025 Phase 2b.1 m4 projection record for the Plan-review lifecycle.
///
/// One row per plan run. `state` holds the current lifecycle position;
/// the resolver-side fields populate depending on which resolution
/// event fired:
///
/// * `PlanApproved` populates `resolved_by` + `resolved_at` + `reviewer_comments`.
/// * `PlanRejected` populates `resolved_by` + `resolved_at` + `rejection_reason`.
/// * `PlanRevisionRequested` populates `resolved_at` + `reviewer_comments` +
///   `revision_run_id`. The event intentionally does NOT carry a
///   `requested_by` operator id today (RFC 018 gap, pre-existing — not
///   widened by this PR), so `resolved_by` stays `None` on that branch.
///
/// All resolver fields stay `None` while the plan is in `Proposed`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanReviewRecord {
    pub plan_run_id: RunId,
    pub project: ProjectKey,
    pub session_id: SessionId,
    pub plan_markdown: String,
    pub state: PlanReviewState,
    pub proposed_at: u64,
    pub resolved_by: Option<OperatorId>,
    pub resolved_at: Option<u64>,
    /// Free-text comments left by the reviewer on Approve /
    /// RevisionRequested. Rejected uses `rejection_reason` instead
    /// so the domain model preserves RFC 018's separate "reason"
    /// field on that branch.
    pub reviewer_comments: Option<String>,
    /// Reason text supplied with `PlanRejected`.
    pub rejection_reason: Option<String>,
    /// Successor plan-run id on `PlanRevisionRequested`.
    pub revision_run_id: Option<RunId>,
}

#[async_trait]
pub trait PlanReviewReadModel: Send + Sync {
    async fn get(&self, plan_run_id: &RunId) -> Result<Option<PlanReviewRecord>, StoreError>;

    /// List plan reviews under a project, newest-proposed-first. Used
    /// by the operator dashboard (tenant-wide "needs review" view
    /// filters on `state = proposed` client-side or via
    /// `list_pending_by_project`).
    async fn list_by_project(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<PlanReviewRecord>, StoreError>;

    /// List pending (state = Proposed) plan reviews under a project.
    /// Dashboard hot path — operators clear their review queue from
    /// this list.
    async fn list_pending_by_project(
        &self,
        project: &ProjectKey,
        limit: usize,
    ) -> Result<Vec<PlanReviewRecord>, StoreError>;

    /// List plan reviews belonging to a given session, oldest-proposed
    /// first so callers can walk the (plan → revision → revision) chain.
    async fn list_by_session(
        &self,
        session_id: &SessionId,
        limit: usize,
    ) -> Result<Vec<PlanReviewRecord>, StoreError>;
}
