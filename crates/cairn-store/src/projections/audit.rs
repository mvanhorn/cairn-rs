use async_trait::async_trait;
use cairn_domain::{audit::AuditOutcome, AuditLogEntry, TenantId};
use serde::{Deserialize, Serialize};

use crate::error::StoreError;

/// RFC-025 Phase 2b.1 projection record for an `AuditLogEntryRecorded`
/// event. Columns mirror the `audit_log_entries` table on pg/sqlite.
///
/// `metadata` is kept as the runtime `AuditLogEntry.metadata` shape
/// (`serde_json::Value`) so the read trait can return the domain type
/// without an extra deserialisation hop at the call site. The backing
/// stores persist the value as a JSON TEXT column; defaults to `{}`
/// because `AuditLogEntryRecorded` does not carry metadata on the wire
/// (the event was kept Eq-able at RFC 002 time, pre-dating the full
/// `AuditLogEntry.metadata` field).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditLogEntryRecord {
    pub entry_id: String,
    pub tenant_id: TenantId,
    pub actor_id: String,
    pub action: String,
    pub resource_type: String,
    pub resource_id: String,
    pub outcome: AuditOutcome,
    pub metadata: serde_json::Value,
    pub occurred_at_ms: u64,
}

impl AuditLogEntryRecord {
    /// Reconstruct the domain `AuditLogEntry` surface from a projection
    /// row. The `request_id` / `ip_address` fields are always `None`
    /// because `AuditLogEntryRecorded` does not carry them — that is a
    /// known gap tracked by the audit service, not a projection defect.
    pub fn into_entry(self) -> AuditLogEntry {
        AuditLogEntry {
            entry_id: self.entry_id,
            tenant_id: self.tenant_id,
            actor_id: self.actor_id,
            action: self.action,
            resource_type: self.resource_type,
            resource_id: self.resource_id,
            outcome: self.outcome,
            request_id: None,
            ip_address: None,
            occurred_at_ms: self.occurred_at_ms,
            metadata: self.metadata,
        }
    }
}

/// Cross-backend upper bound on `list_by_resource`. Per-resource audit
/// trails are single-digit to low-triple-digit in practice, but the
/// event log is append-only and an operator (or adversary) could
/// generate a pathological resource with a much higher row count. The
/// cap protects the admin dashboard's read path without taking a
/// `limit` argument at the trait boundary (the existing REST handler
/// already paginates downstream). Shared here so the in-memory,
/// pg, and sqlite impls converge on the same ceiling — Copilot PR #573
/// review pointed out the prior in-memory impl was unbounded.
pub const LIST_BY_RESOURCE_MAX_ROWS: usize = 10_000;

/// Convert the `since_ms` + `before_ms` window bounds to the
/// `[since_i64, before_i64)` range that pg + sqlite
/// `AuditLogReadModel::list_by_tenant` compare against via
/// `occurred_at_ms >= since AND occurred_at_ms < before` (inclusive
/// lower, exclusive upper — same convention as the trait doc).
/// Unbounded sides fall back to the i64 extremes. Surfaces a
/// descriptive `StoreError::Internal` on `u64 → i64` overflow rather
/// than silently clamping.
///
/// Shared by pg + sqlite `AuditLogReadModel::list_by_tenant` impls —
/// Gemini PR #573 review flagged the repetition.
pub fn window_bounds_ms(
    since_ms: Option<u64>,
    before_ms: Option<u64>,
) -> Result<(i64, i64), StoreError> {
    let since = since_ms
        .map(i64::try_from)
        .transpose()
        .map_err(|_| StoreError::Internal("since_ms exceeds i64::MAX".into()))?
        .unwrap_or(i64::MIN);
    let before = before_ms
        .map(i64::try_from)
        .transpose()
        .map_err(|_| StoreError::Internal("before_ms exceeds i64::MAX".into()))?
        .unwrap_or(i64::MAX);
    Ok((since, before))
}

#[async_trait]
pub trait AuditLogReadModel: Send + Sync {
    /// List audit-log entries for a tenant, newest-first.
    ///
    /// `since_ms` / `before_ms` bound the `occurred_at_ms` window
    /// (inclusive lower / exclusive upper — `[since, before)`). Either may be
    /// `None` to leave that side unbounded. `limit` caps the returned count.
    async fn list_by_tenant(
        &self,
        tenant_id: &TenantId,
        since_ms: Option<u64>,
        before_ms: Option<u64>,
        limit: usize,
    ) -> Result<Vec<AuditLogEntry>, StoreError>;

    /// List audit-log entries for a (resource_type, resource_id) tuple,
    /// newest-first. Does not bound by tenant — the caller filters
    /// downstream (admin dashboard cross-tenant view). Capped at
    /// [`LIST_BY_RESOURCE_MAX_ROWS`] across every backend.
    async fn list_by_resource(
        &self,
        resource_type: &str,
        resource_id: &str,
    ) -> Result<Vec<AuditLogEntry>, StoreError>;
}
