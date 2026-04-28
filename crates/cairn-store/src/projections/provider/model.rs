//! Reader for the RFC 009 provider-model capability registry.
//!
//! Split out of the monolithic `provider.rs` in #441.

use async_trait::async_trait;
use cairn_domain::providers::ProviderModelCapability;
use cairn_domain::ProviderConnectionId;

use crate::error::StoreError;

/// Read-model for the provider model capability registry (RFC 009).
#[async_trait]
pub trait ProviderModelReadModel: Send + Sync {
    /// Get capabilities for a specific model_id.
    async fn get_model(
        &self,
        model_id: &str,
    ) -> Result<Option<ProviderModelCapability>, StoreError>;

    /// List all registered models for a connection.
    async fn list_by_connection(
        &self,
        connection_id: &ProviderConnectionId,
    ) -> Result<Vec<ProviderModelCapability>, StoreError>;
}
