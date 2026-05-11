//! Reader for the `provider_connections` table.
//!
//! Split out of the monolithic `provider.rs` in #441 — 13 separate
//! read-model traits in one file made drift between them invisible
//! and made it hard to review a single entity in isolation.

use async_trait::async_trait;
use cairn_domain::providers::ProviderConnectionRecord;
use cairn_domain::{ProviderConnectionId, TenantId};

use crate::error::StoreError;

#[async_trait]
pub trait ProviderConnectionReadModel: Send + Sync {
    async fn get(
        &self,
        id: &ProviderConnectionId,
    ) -> Result<Option<ProviderConnectionRecord>, StoreError>;

    async fn list_by_tenant(
        &self,
        tenant_id: &TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ProviderConnectionRecord>, StoreError>;
}
