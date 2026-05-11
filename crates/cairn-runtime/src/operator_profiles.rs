//! Operator profile service boundary for tenant-scoped operator management.

use async_trait::async_trait;
use cairn_domain::org::OperatorProfile;
use cairn_domain::{OperatorId, TenantId, WorkspaceRole};

use crate::error::RuntimeError;

/// Patch payload for `OperatorProfileService::patch_profile`. Each
/// field uses PATCH semantics: `None` means "leave the stored value
/// alone", `Some(v)` means "replace with v". RFC 026 PR-A2.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OperatorProfilePatch {
    pub display_name: Option<String>,
    pub email: Option<String>,
    pub role: Option<WorkspaceRole>,
}

impl OperatorProfilePatch {
    /// True when the patch carries no field updates. Handlers convert
    /// an empty patch to a 422 instead of emitting a no-op event.
    pub fn is_empty(&self) -> bool {
        self.display_name.is_none() && self.email.is_none() && self.role.is_none()
    }
}

#[async_trait]
pub trait OperatorProfileService: Send + Sync {
    async fn create(
        &self,
        tenant_id: TenantId,
        display_name: String,
        email: String,
        role: WorkspaceRole,
    ) -> Result<OperatorProfile, RuntimeError>;

    async fn get(&self, profile_id: &OperatorId) -> Result<Option<OperatorProfile>, RuntimeError>;

    async fn list(
        &self,
        tenant_id: &TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<OperatorProfile>, RuntimeError>;

    async fn update(
        &self,
        profile_id: &OperatorId,
        display_name: String,
        email: String,
    ) -> Result<OperatorProfile, RuntimeError>;

    /// RFC 026 PR-A2: apply a PATCH edit to an operator profile.
    /// Unlike `update`, every field is optional and the caller may
    /// also edit `role`. `None` fields leave their stored values
    /// alone; at least one field must be `Some(..)` or the service
    /// returns `RuntimeError::Validation { reason: "empty_patch" }`.
    /// 404 when the operator id has no profile.
    async fn patch_profile(
        &self,
        profile_id: &OperatorId,
        patch: OperatorProfilePatch,
    ) -> Result<OperatorProfile, RuntimeError>;

    /// RFC 008: update ergonomic/presentation preferences for an operator.
    ///
    /// Rejects any preference keys that could silently affect canonical runtime
    /// outcomes (provider routing, execution policy, etc.).
    async fn set_preferences(
        &self,
        profile_id: &OperatorId,
        preferences: serde_json::Value,
    ) -> Result<OperatorProfile, RuntimeError>;
}
