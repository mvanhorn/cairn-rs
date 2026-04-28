//! Reader for the RFC 009 provider connection pools.
//!
//! Split out of the monolithic `provider.rs` in #441.

use async_trait::async_trait;
use cairn_domain::providers::ProviderConnectionPool;
use cairn_domain::TenantId;

use crate::error::StoreError;

/// RFC 009: read-model for provider connection pools.
#[async_trait]
pub trait ProviderPoolReadModel: Send + Sync {
    async fn get_pool(&self, pool_id: &str) -> Result<Option<ProviderConnectionPool>, StoreError>;
    async fn list_pools_by_tenant(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Vec<ProviderConnectionPool>, StoreError>;
}
