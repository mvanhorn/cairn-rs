//! Reader for the `provider_budgets` table.
//!
//! Split out of the monolithic `provider.rs` in #441.

use async_trait::async_trait;
use cairn_domain::providers::{ProviderBudget, ProviderBudgetPeriod};
use cairn_domain::TenantId;

use crate::error::StoreError;

#[async_trait]
pub trait ProviderBudgetReadModel: Send + Sync {
    async fn get_by_tenant_period(
        &self,
        tenant_id: &TenantId,
        period: ProviderBudgetPeriod,
    ) -> Result<Option<ProviderBudget>, StoreError>;

    async fn list_by_tenant(&self, tenant_id: &TenantId)
        -> Result<Vec<ProviderBudget>, StoreError>;
}
