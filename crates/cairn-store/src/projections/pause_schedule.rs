use async_trait::async_trait;
use cairn_domain::{ProjectKey, RunId, TenantId};
use serde::{Deserialize, Serialize};

use crate::error::StoreError;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PauseScheduledRecord {
    pub run_id: RunId,
    pub project: ProjectKey,
    pub resume_at_ms: u64,
    pub created_at_ms: u64,
}

#[async_trait]
pub trait PauseScheduleReadModel: Send + Sync {
    /// List scheduled resume records whose `resume_at_ms <= before_ms`
    /// for the given tenant, capped at `limit` rows (issue #570).
    ///
    /// Tenant filter + `limit` move into the trait so cross-tenant
    /// bleed cannot happen at the handler layer and busy tenants cannot
    /// stall the dashboard by materialising every historically-paused
    /// run. Results are ordered by `resume_at_ms ASC, run_id ASC` so
    /// page-by-page iteration is stable.
    ///
    /// Callers pass `limit + 1` to detect `has_more` without
    /// re-scanning. The handler's pagination sits around this call —
    /// the projection itself has no `offset` because the scheduler
    /// pipeline consumes due records deterministically from the head
    /// (a skipped due record is a correctness bug, not a paging
    /// concern).
    async fn list_due(
        &self,
        tenant_id: &TenantId,
        before_ms: u64,
        limit: usize,
    ) -> Result<Vec<PauseScheduledRecord>, StoreError>;
}
