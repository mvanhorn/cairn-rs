//! RFC-025 Phase 2b.2b m5: read-model for soul-patch proposals and
//! applications.
//!
//! A "soul patch" is a proposed modification to the operator's tuning
//! document (task loop preferences, memory pinning, etc.). Patches
//! go through `Proposed` → `Applied` on operator approval.

use async_trait::async_trait;
use cairn_domain::tenancy::ProjectKey;

use crate::error::StoreError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SoulPatchState {
    Proposed,
    Applied,
}

impl SoulPatchState {
    pub fn as_str(self) -> &'static str {
        match self {
            SoulPatchState::Proposed => "proposed",
            SoulPatchState::Applied => "applied",
        }
    }

    pub fn from_str_opt(s: &str) -> Option<SoulPatchState> {
        match s {
            "proposed" => Some(SoulPatchState::Proposed),
            "applied" => Some(SoulPatchState::Applied),
            _ => None,
        }
    }
}

/// One row per proposed patch. Lifecycle mutations (Applied) update
/// `state`, `applied_at_ms`, and `new_version` in-place.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SoulPatchRecord {
    pub patch_id: String,
    pub project: ProjectKey,
    pub state: SoulPatchState,
    pub patch_content: String,
    pub requires_approval: bool,
    pub proposed_at_ms: u64,
    pub applied_at_ms: Option<u64>,
    pub new_version: Option<u32>,
}

#[async_trait]
pub trait SoulPatchReadModel: Send + Sync {
    async fn get(&self, patch_id: &str) -> Result<Option<SoulPatchRecord>, StoreError>;

    /// List patches for a project, newest-first by `proposed_at_ms`,
    /// tiebreaking on `patch_id DESC` for stable pagination.
    async fn list_by_project(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<SoulPatchRecord>, StoreError>;
}
