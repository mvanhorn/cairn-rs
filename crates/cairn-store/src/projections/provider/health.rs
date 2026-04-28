//! Reader for the `provider_health` table and the health-check
//! schedule registry.
//!
//! Split out of the monolithic `provider.rs` in #441.

use async_trait::async_trait;
use cairn_domain::providers::{ProviderHealthRecord, ProviderHealthSchedule};
use cairn_domain::{ProviderConnectionId, TenantId};

use crate::error::StoreError;

#[async_trait]
pub trait ProviderHealthReadModel: Send + Sync {
    async fn get(
        &self,
        connection_id: &ProviderConnectionId,
    ) -> Result<Option<ProviderHealthRecord>, StoreError>;

    async fn list_by_tenant(
        &self,
        tenant_id: &TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ProviderHealthRecord>, StoreError>;
}

#[async_trait]
pub trait ProviderHealthScheduleReadModel: Send + Sync {
    async fn get_schedule(
        &self,
        schedule_id: &str,
    ) -> Result<Option<ProviderHealthSchedule>, StoreError>;

    async fn list_schedules_by_tenant(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Vec<ProviderHealthSchedule>, StoreError>;

    async fn list_enabled_schedules(&self) -> Result<Vec<ProviderHealthSchedule>, StoreError>;
}
