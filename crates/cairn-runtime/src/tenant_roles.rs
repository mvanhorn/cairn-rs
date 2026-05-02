//! RFC 026 PR-A0: tenant-admin role service boundary.
//!
//! Emits `TenantRoleGranted` / `TenantRoleRevoked` events and exposes
//! the projection reads the middleware + admin handlers consume to
//! answer the `TenantAdminGuard` decision.

use async_trait::async_trait;
use cairn_domain::tenancy::TenantRole;
use cairn_domain::{OperatorId, TenantId};
use cairn_store::projections::OperatorTenantRoleRecord;

use crate::error::RuntimeError;

#[async_trait]
pub trait TenantRoleService: Send + Sync {
    /// Grant `role` on `tenant_id` to `operator_id`. `granted_by` is the
    /// identity (operator id or `"system"` / `"upgrade-backfill"`) that
    /// authorized the grant — surfaced on the emitted
    /// `TenantRoleGranted` event for audit.
    ///
    /// Idempotent: re-granting an already-granted pair updates the
    /// `granted_by` / `at_ms` fields and clears any prior revocation.
    async fn grant(
        &self,
        tenant_id: TenantId,
        operator_id: OperatorId,
        role: TenantRole,
        granted_by: String,
    ) -> Result<OperatorTenantRoleRecord, RuntimeError>;

    /// Revoke any role on `tenant_id` for `operator_id`. Soft delete —
    /// the row stays, `revoked_at_ms` + `revoked_by` are set. A revoke
    /// against a non-existent pair is a no-op (returns `Ok(None)`).
    async fn revoke(
        &self,
        tenant_id: TenantId,
        operator_id: OperatorId,
        revoked_by: String,
    ) -> Result<Option<OperatorTenantRoleRecord>, RuntimeError>;

    /// Look up a single `(tenant_id, operator_id)` pair.
    async fn get(
        &self,
        tenant_id: &TenantId,
        operator_id: &OperatorId,
    ) -> Result<Option<OperatorTenantRoleRecord>, RuntimeError>;

    /// List every tenant role an operator holds (any status).
    async fn list_by_operator(
        &self,
        operator_id: &OperatorId,
    ) -> Result<Vec<OperatorTenantRoleRecord>, RuntimeError>;
}
