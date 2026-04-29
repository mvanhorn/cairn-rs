//! Readers for provider-cost projections:
//! `session_costs`, `project_costs`, `workspace_costs`, `run_costs`,
//! and the RFC 010 `run_cost_alerts` thresholds.
//!
//! Split out of the monolithic `provider.rs` in #441. These tables
//! share the same upsert transaction (see `SessionCostUpdated`
//! handler) so their readers stay grouped in a single file.

use async_trait::async_trait;
use cairn_domain::providers::{
    ProjectCostRecord, RunCostAlert, RunCostRecord, SessionCostRecord, WorkspaceCostRecord,
};
use cairn_domain::{ProjectKey, RunId, SessionId, TenantId};

use crate::error::StoreError;

#[async_trait]
pub trait SessionCostReadModel: Send + Sync {
    async fn get_session_cost(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<SessionCostRecord>, StoreError>;

    /// List per-session cost rows for a tenant, newest-first, starting
    /// at `since_ms` (inclusive lower bound on `updated_at_ms`).
    ///
    /// `limit` caps the returned rows and `offset` skips that many
    /// rows from the head — callers use `limit + 1` to detect whether
    /// additional pages exist. Implementations MUST apply both bounds
    /// at the query layer where possible (issue #423): a tenant with
    /// months of activity can exceed 100k rows, and the historical
    /// unbounded `list_by_tenant` led to OOM and latency incidents.
    async fn list_by_tenant(
        &self,
        tenant_id: &TenantId,
        since_ms: u64,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<SessionCostRecord>, StoreError>;
}

/// F29 CD-2: lifetime cost rollups at the project and workspace level.
///
/// Fed by the same `SessionCostUpdated` handler that updates
/// `SessionCostReadModel` — project and workspace totals stay consistent
/// with the per-session breakdown because all three upserts run in the
/// same transaction on Postgres / SQLite.
///
/// Time-range queries (daily buckets) are out of scope for v1 — records
/// are always lifetime-total. Callers that need a time-range fall back
/// to the per-session list + client-side filter on `updated_at_ms`.
#[async_trait]
pub trait ProjectCostReadModel: Send + Sync {
    async fn get_project_cost(
        &self,
        project: &ProjectKey,
    ) -> Result<Option<ProjectCostRecord>, StoreError>;

    async fn get_workspace_cost(
        &self,
        tenant_id: &TenantId,
        workspace_id: &str,
    ) -> Result<Option<WorkspaceCostRecord>, StoreError>;
}

#[async_trait]
pub trait RunCostReadModel: Send + Sync {
    async fn get_run_cost(&self, run_id: &RunId) -> Result<Option<RunCostRecord>, StoreError>;

    async fn list_by_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<RunCostRecord>, StoreError>;
}

/// RFC 010: read-model for run cost alert thresholds and triggered alerts.
#[async_trait]
pub trait RunCostAlertReadModel: Send + Sync {
    async fn get_alert(&self, run_id: &RunId) -> Result<Option<RunCostAlert>, StoreError>;

    /// List triggered alerts for a tenant with storage-layer pagination
    /// (issue #570). Callers pass `limit + 1` to detect `has_more` on
    /// the wire without re-scanning — implementations apply `limit` +
    /// `offset` at the query surface so the caller never materialises
    /// every row for a busy tenant. Results are ordered by
    /// `triggered_at_ms DESC` so page 1 is the most-recent triggers.
    async fn list_triggered_by_tenant(
        &self,
        tenant_id: &TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<RunCostAlert>, StoreError>;
}
