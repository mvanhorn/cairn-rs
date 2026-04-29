use async_trait::async_trait;
use cairn_domain::{TenantId, TenantQuota};
use serde::{Deserialize, Serialize};

use crate::error::StoreError;

#[async_trait]
pub trait QuotaReadModel: Send + Sync {
    async fn get_quota(&self, tenant_id: &TenantId) -> Result<Option<TenantQuota>, StoreError>;
}

/// Audit record for a single `TenantQuotaViolated` event. The projection
/// keeps one row per (tenant_id, quota_type, occurred_at_ms) so operators
/// can query a tenant's quota-pressure history without walking the event
/// log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaViolationRecord {
    pub tenant_id: TenantId,
    pub quota_type: String,
    pub current: u32,
    pub limit: u32,
    pub occurred_at_ms: u64,
}

#[async_trait]
pub trait QuotaViolationReadModel: Send + Sync {
    /// List quota violations for a tenant, most-recent first.
    async fn list_violations(
        &self,
        tenant_id: &TenantId,
        limit: usize,
    ) -> Result<Vec<QuotaViolationRecord>, StoreError>;
}
