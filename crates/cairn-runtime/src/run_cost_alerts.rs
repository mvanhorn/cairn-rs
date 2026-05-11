//! Run cost alert service boundary per RFC 010.

use async_trait::async_trait;
use cairn_domain::providers::RunCostAlert;
use cairn_domain::{RunId, TenantId};

use crate::error::RuntimeError;

#[async_trait]
pub trait RunCostAlertService: Send + Sync {
    /// Set an alert threshold for a run. Fires RunCostAlertTriggered when
    /// the accumulated cost exceeds threshold_micros.
    async fn set_alert(
        &self,
        run_id: RunId,
        tenant_id: TenantId,
        threshold_micros: u64,
    ) -> Result<(), RuntimeError>;

    /// Explicitly check current cost against the threshold and emit
    /// RunCostAlertTriggered if the threshold is crossed and not yet triggered.
    async fn check_and_trigger(&self, run_id: &RunId) -> Result<bool, RuntimeError>;

    /// Get the alert record for a run (None if no threshold set).
    async fn get_alert(&self, run_id: &RunId) -> Result<Option<RunCostAlert>, RuntimeError>;

    /// List triggered alerts for a tenant with storage-layer
    /// pagination (issue #570). Callers pass `limit + 1` to detect
    /// `has_more` on the wire without re-scanning; results are
    /// ordered newest-first.
    async fn list_triggered_by_tenant(
        &self,
        tenant_id: &TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<RunCostAlert>, RuntimeError>;
}
