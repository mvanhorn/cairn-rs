-- RFC-025 Phase 2a.1 milestone 4: licenses projection table.
--
-- Projects `LicenseActivated` into a durable read model keyed by
-- `tenant_id`. The in-memory projection keys by tenant as well
-- (at most one active license per tenant in v1; the activated event
-- replaces the prior row on upsert).
--
-- `entitlements` is persisted as a JSON array stored in a TEXT column
-- for cross-backend parity — no pg arrays per
-- `feedback_no_db_specific_features.md`. The `in_memory` applier
-- currently initialises entitlements to an empty vector; this
-- projection preserves the same shape.
--
-- `tier` is stored snake_case ('local_eval', 'team_self_hosted',
-- 'enterprise_self_hosted') matching the serde contract.

CREATE TABLE IF NOT EXISTS licenses (
    tenant_id         TEXT    PRIMARY KEY,
    license_key       TEXT,
    tier              TEXT    NOT NULL,
    entitlements_json TEXT    NOT NULL DEFAULT '[]',
    issued_at         BIGINT  NOT NULL,
    expires_at        BIGINT,
    created_at        BIGINT  NOT NULL,
    updated_at        BIGINT  NOT NULL
);
