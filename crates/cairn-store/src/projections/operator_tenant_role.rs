//! RFC 026 PR-A0: operator → tenant-role projection.
//!
//! Backs the `operator_tenant_roles` table on pg/sqlite (V066) and the
//! matching in-memory `HashMap`. One row per `(tenant_id, operator_id)`
//! pair; grants upsert, revocations mark `revoked_at_ms` + `revoked_by`
//! without deleting the row so the audit trail survives.

use async_trait::async_trait;
use cairn_domain::ids::{OperatorId, TenantId};
use cairn_domain::tenancy::TenantRole;
use serde::{Deserialize, Serialize};

use crate::error::StoreError;

/// Flat read-model record for an operator's tenant-scope role.
///
/// RFC 026 PR-A0: carries the role, the grant identity + time, and the
/// revocation identity + time when the row has been revoked. An active
/// grant has `revoked_at_ms == None`; once revoked, a fresh grant
/// replaces the row and clears the revocation fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorTenantRoleRecord {
    pub tenant_id: TenantId,
    pub operator_id: OperatorId,
    pub role: TenantRole,
    pub granted_at_ms: u64,
    pub granted_by: String,
    #[serde(default)]
    pub revoked_at_ms: Option<u64>,
    #[serde(default)]
    pub revoked_by: Option<String>,
}

impl OperatorTenantRoleRecord {
    /// `true` when the row represents an active (non-revoked) grant —
    /// callers that need "current tenant-admin membership" filter on
    /// this.
    pub fn is_active(&self) -> bool {
        self.revoked_at_ms.is_none()
    }
}

/// Read-model for operator/tenant role membership (RFC 026 PR-A0).
#[async_trait]
pub trait OperatorTenantRoleReadModel: Send + Sync {
    /// Look up a single grant by `(tenant_id, operator_id)`. Returns
    /// `None` when no row has ever been inserted — distinct from a
    /// revoked row, which returns `Some(_)` with `revoked_at_ms.is_some()`.
    async fn get(
        &self,
        tenant_id: &TenantId,
        operator_id: &OperatorId,
    ) -> Result<Option<OperatorTenantRoleRecord>, StoreError>;

    /// List every tenant role an operator currently holds (any status,
    /// active or revoked). Callers that need active-only must filter on
    /// `is_active()`.
    async fn list_by_operator(
        &self,
        operator_id: &OperatorId,
    ) -> Result<Vec<OperatorTenantRoleRecord>, StoreError>;

    /// List every operator associated with a tenant (any status).
    async fn list_by_tenant(
        &self,
        tenant_id: &TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<OperatorTenantRoleRecord>, StoreError>;
}
