-- RFC-025 Phase 2b.3 milestone 4: notification_preferences + notifications
-- projections (RFC 008).
--
-- Before this migration the `NotificationPreferenceSet` +
-- `NotificationSent` events were routed through `log_stub` on pg/sqlite:
-- `NotificationReadModel::{get_preferences,list_preferences_by_tenant,
-- list_sent_notifications,list_failed_notifications}` returned empty
-- on a cold boot. Every restart wiped the operator notification matrix
-- and delivery audit ledger.
--
-- Two tables:
-- * `notification_preferences` — one row per (tenant_id, operator_id)
--   keyed on `pref_id = "{tenant_id}:{operator_id}"`. `event_types` +
--   `channels` are variable-length vectors and land as JSON TEXT so
--   pg + sqlite stay byte-equal (no pg-specific JSONB / arrays).
-- * `notifications` — one row per `record_id` (tenant_id scoped). The
--   `payload` column is likewise JSON TEXT.
--
-- Replayed events are idempotent: preferences upsert on the composite
-- PK (last-write-wins on the vector fields); sent-audit is ON CONFLICT
-- DO NOTHING keyed on record_id.

CREATE TABLE IF NOT EXISTS notification_preferences (
    tenant_id       TEXT NOT NULL,
    operator_id     TEXT NOT NULL,
    pref_id         TEXT NOT NULL,
    event_types_json TEXT NOT NULL,
    channels_json   TEXT NOT NULL,
    set_at_ms       BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, operator_id)
);

CREATE TABLE IF NOT EXISTS notifications (
    record_id       TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    operator_id     TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    channel_kind    TEXT NOT NULL,
    channel_target  TEXT NOT NULL,
    payload_json    TEXT NOT NULL,
    sent_at_ms      BIGINT NOT NULL,
    -- INTEGER 0/1 rather than BOOLEAN so the column shape stays byte-
    -- equal with the sqlite parity schema and the parity harness can
    -- diff raw rows directly. Copilot PR #594 review.
    delivered       INTEGER NOT NULL,
    delivery_error  TEXT
);

CREATE INDEX IF NOT EXISTS idx_notifications_tenant
    ON notifications (tenant_id, sent_at_ms, record_id);

CREATE INDEX IF NOT EXISTS idx_notifications_tenant_delivered
    ON notifications (tenant_id, delivered, sent_at_ms, record_id);
