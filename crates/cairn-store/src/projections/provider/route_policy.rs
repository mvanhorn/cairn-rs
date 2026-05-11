//! Reader for the `route_policies` table.
//!
//! Split out of the monolithic `provider.rs` in #441.

use async_trait::async_trait;
use cairn_domain::providers::RoutePolicy;
use cairn_domain::TenantId;

use crate::error::StoreError;

#[async_trait]
pub trait RoutePolicyReadModel: Send + Sync {
    async fn get(&self, policy_id: &str) -> Result<Option<RoutePolicy>, StoreError>;

    async fn list_by_tenant(
        &self,
        tenant_id: &TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<RoutePolicy>, StoreError>;
}
