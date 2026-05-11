use async_trait::async_trait;
use cairn_domain::credentials::{CredentialRecord, CredentialRotationRecord};
use cairn_domain::{CredentialId, TenantId};

use crate::error::StoreError;

#[async_trait]
pub trait CredentialReadModel: Send + Sync {
    async fn get(&self, id: &CredentialId) -> Result<Option<CredentialRecord>, StoreError>;

    async fn list_by_tenant(
        &self,
        tenant_id: &TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<CredentialRecord>, StoreError>;

    /// Return every active credential row across all tenants in a single
    /// pass.
    ///
    /// This is used by the boot-time legacy-format scanner
    /// (`scan_legacy_ciphertexts`) to avoid the N+1 pattern of listing
    /// tenants and then fetching per-tenant. Backends that support a
    /// single-pass implementation override with `Ok(Some(rows))`. The
    /// default impl returns `Ok(None)` so callers can unambiguously
    /// distinguish "backend does not support this operation" (fall back
    /// to per-tenant scan) from "backend supports it and there are zero
    /// rows" (`Ok(Some(Vec::new()))`). An `Ok(Vec::new())`-as-sentinel
    /// approach conflated the two states and either skipped the fallback
    /// scan on legitimately empty deployments or triggered unnecessary
    /// expensive fallbacks. Copilot review on PR #535 (line 38 / line 671).
    ///
    /// `limit` caps the number of rows returned across the whole set so
    /// a pathological deployment cannot stall boot on an unbounded read.
    /// A `Some(rows)` return with fewer than `limit` rows does NOT mean
    /// the scan is complete — callers should check `< limit` only as a
    /// soft hint and log a warning if the cap is hit. The primary
    /// production backend (`InMemoryStore`, behind pg/sqlite dual-write)
    /// provides a single-pass override. Gemini review on PR #535 (line
    /// 594).
    async fn list_all_active(
        &self,
        limit: usize,
    ) -> Result<Option<Vec<CredentialRecord>>, StoreError> {
        // Default: unsupported. Backends that want the faster boot-time
        // scan override with a single-pass implementation. Callers fall
        // back to the per-tenant path when this returns `None`, while
        // `Some(Vec::new())` remains a legitimate empty result.
        let _ = limit;
        Ok(None)
    }
}

#[async_trait]
pub trait CredentialRotationReadModel: Send + Sync {
    async fn list_rotations(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Vec<CredentialRotationRecord>, StoreError>;
}
