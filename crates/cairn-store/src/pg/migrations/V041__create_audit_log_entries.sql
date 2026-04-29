-- RFC-025 Phase 2b.1: audit_log_entries projection.
--
-- Before this migration the `AuditLogEntryRecorded` event was routed
-- through `log_stub` in pg/sqlite — the event log recorded the fact but
-- no read-model row existed on persistent backends. The in-memory store
-- was the only projection, and its implementation scanned the event log
-- on every call (no dedicated map) — which meant:
--   (a) on pg/sqlite, `GET /v1/admin/audit-log` returned empty after
--       every restart even though the event was durable;
--   (b) on in-memory, the scan cost grew linearly with total event
--       count, and the trait-documented "newest-first" ordering was
--       violated (rows came back in insertion order).
--
-- This migration creates the read-model table. Columns mirror the
-- event shape (metadata is out-of-band — the event carries only
-- Eq-able fields per an earlier RFC 002 design decision, so the
-- projection persists the empty object as a portable TEXT default).
--
-- Portability: TEXT for all string-shaped columns, BIGINT for the
-- timestamp, no JSONB / pg arrays. `outcome` is an enum serialised as
-- snake_case text via `enum_to_str`.

CREATE TABLE IF NOT EXISTS audit_log_entries (
    entry_id        TEXT    PRIMARY KEY,
    tenant_id       TEXT    NOT NULL,
    actor_id        TEXT    NOT NULL,
    action          TEXT    NOT NULL,
    resource_type   TEXT    NOT NULL,
    resource_id     TEXT    NOT NULL,
    outcome         TEXT    NOT NULL,
    metadata_json   TEXT    NOT NULL DEFAULT '{}',
    occurred_at_ms  BIGINT  NOT NULL
);

-- Tenant dashboard hot path: newest-first within a time window. Both
-- sort keys are DESC so the index matches the adapter's
-- `ORDER BY occurred_at_ms DESC, entry_id DESC` exactly — no extra sort
-- step when same-timestamp rows tie (Copilot PR #573 review).
CREATE INDEX IF NOT EXISTS idx_audit_log_tenant
    ON audit_log_entries (tenant_id, occurred_at_ms DESC, entry_id DESC);

-- Per-resource audit trail (runs / tenants / approvals / etc.). Same
-- DESC discipline as the tenant index.
CREATE INDEX IF NOT EXISTS idx_audit_log_resource
    ON audit_log_entries (resource_type, resource_id, occurred_at_ms DESC, entry_id DESC);
