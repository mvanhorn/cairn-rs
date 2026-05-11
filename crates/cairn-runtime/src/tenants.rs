//! Tenant service boundary for org hierarchy management.

use async_trait::async_trait;
use cairn_domain::{TenantId, TenantRecord};

use crate::error::RuntimeError;

/// Patch payload for `TenantService::update`. Each field uses PATCH
/// semantics: `None` means "leave the stored value alone", `Some(v)`
/// means "replace with v". Only `name` is editable today — the
/// `tenants` projection table has no other mutable column (RFC 026
/// PR-A2). Adding a future `metadata: Option<...>` field here is a
/// pure append; the service emits `TenantUpdated` with whatever
/// fields the caller populated.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TenantUpdatePatch {
    /// New display name. `None` leaves the stored name alone;
    /// `Some("...")` replaces it.
    pub name: Option<String>,
}

impl TenantUpdatePatch {
    /// True when the patch carries no field updates. Handlers convert
    /// an empty patch to a 422 instead of emitting a no-op event.
    pub fn is_empty(&self) -> bool {
        self.name.is_none()
    }
}

/// Tenant service boundary.
///
/// Manages tenant lifecycle within the organization hierarchy.
#[async_trait]
pub trait TenantService: Send + Sync {
    /// Create a new tenant.
    async fn create(&self, tenant_id: TenantId, name: String)
        -> Result<TenantRecord, RuntimeError>;

    /// Get a tenant by ID.
    async fn get(&self, tenant_id: &TenantId) -> Result<Option<TenantRecord>, RuntimeError>;

    /// List tenants with pagination.
    async fn list(&self, limit: usize, offset: usize) -> Result<Vec<TenantRecord>, RuntimeError>;

    /// Apply a PATCH edit to a tenant. `updated_by` is the authenticated
    /// principal id for the audit trail. Returns the updated record.
    /// Errors:
    ///
    /// - `RuntimeError::NotFound` when `tenant_id` has no row.
    /// - `RuntimeError::Validation { reason: "empty_patch" }` when the
    ///   patch carries no field updates.
    /// - Other errors surface as `RuntimeError::Internal`.
    async fn update(
        &self,
        tenant_id: TenantId,
        patch: TenantUpdatePatch,
        updated_by: String,
    ) -> Result<TenantRecord, RuntimeError>;
}
