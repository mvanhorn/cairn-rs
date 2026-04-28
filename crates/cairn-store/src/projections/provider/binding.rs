//! Reader for the `provider_bindings` table and RFC 009 per-binding
//! cost statistics.
//!
//! Split out of the monolithic `provider.rs` in #441.

use async_trait::async_trait;
use cairn_domain::providers::{OperationKind, ProviderBindingCostStats, ProviderBindingRecord};
use cairn_domain::{ProjectKey, ProviderBindingId, TenantId};

use crate::error::StoreError;

#[async_trait]
pub trait ProviderBindingReadModel: Send + Sync {
    async fn get(
        &self,
        id: &ProviderBindingId,
    ) -> Result<Option<ProviderBindingRecord>, StoreError>;

    async fn list_by_project(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ProviderBindingRecord>, StoreError>;

    async fn list_by_tenant(
        &self,
        tenant_id: &TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ProviderBindingRecord>, StoreError>;

    async fn list_active(
        &self,
        project: &ProjectKey,
        operation: OperationKind,
    ) -> Result<Vec<ProviderBindingRecord>, StoreError>;
}

/// RFC 009: read-model for per-binding actual cost statistics.
#[async_trait]
pub trait ProviderBindingCostStatsReadModel: Send + Sync {
    async fn get(
        &self,
        binding_id: &ProviderBindingId,
    ) -> Result<Option<ProviderBindingCostStats>, StoreError>;

    async fn list_by_tenant(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Vec<ProviderBindingCostStats>, StoreError>;
}
