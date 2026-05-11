use async_trait::async_trait;
use cairn_domain::{recovery::RecoveryEscalation, RunId, TenantId};

use crate::error::StoreError;

/// Read model for recovery escalations.
#[async_trait]
pub trait RecoveryEscalationReadModel: Send + Sync {
    async fn get_by_run(&self, run_id: &RunId) -> Result<Option<RecoveryEscalation>, StoreError>;

    /// List recovery escalations for a tenant with storage-layer
    /// pagination (issue #570). `limit + 1` fetch-to-detect-more is the
    /// contract — implementations apply `limit` + `offset` at the query
    /// surface so a tenant with months of escalations doesn't force a
    /// full-table scan on every operator dashboard refresh. Results are
    /// ordered newest-first by `escalated_at_ms`.
    async fn list_by_tenant(
        &self,
        tenant_id: &TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<RecoveryEscalation>, StoreError>;
}
