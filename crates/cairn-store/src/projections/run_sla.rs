use async_trait::async_trait;
use cairn_domain::{
    sla::{SlaBreach, SlaConfig},
    RunId, TenantId,
};

use crate::error::StoreError;

/// Read model for run SLA configurations and breach records.
#[async_trait]
pub trait RunSlaReadModel: Send + Sync {
    async fn get_sla(&self, run_id: &RunId) -> Result<Option<SlaConfig>, StoreError>;
    async fn get_breach(&self, run_id: &RunId) -> Result<Option<SlaBreach>, StoreError>;

    /// List SLA breaches for a tenant with storage-layer pagination
    /// (issue #570). `limit + 1` fetch-to-detect-more is the contract —
    /// implementations apply `limit` + `offset` at the query surface so
    /// busy tenants don't materialise every historical breach on a
    /// single request. Results are ordered newest-first by
    /// `breached_at_ms`.
    async fn list_breached_by_tenant(
        &self,
        tenant_id: &TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<SlaBreach>, StoreError>;
}
