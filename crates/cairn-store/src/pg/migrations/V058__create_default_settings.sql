-- RFC-025 Phase 2b.3 milestone 2: default_settings projection.
--
-- Before this migration the `DefaultSettingSet` and `DefaultSettingCleared`
-- events were routed through `log_stub` on pg/sqlite — the event log
-- recorded the intent but `DefaultsReadModel::{get,list_by_scope}`
-- returned empty on a cold boot. Every restart wiped the operator's
-- layered defaults store.
--
-- PK is (scope, scope_id, key) — one row per (scope, scope_id, key)
-- with upsert-on-set (last write wins) and hard-delete on Cleared.
-- The value column stores the JSON-encoded `serde_json::Value` as TEXT
-- so every supported backend sees the same byte shape (we deliberately
-- avoid pg JSONB per CLAUDE.md "no Postgres-specific features").
--
-- Neither `DefaultSettingSet` nor `DefaultSettingCleared` carry an
-- emit timestamp on the event payload; the projection is a pure
-- key-value store — ordering for `list_by_scope` is the deterministic
-- `(scope, scope_id, key)` tuple, matched byte-for-byte by the
-- in-memory sort.

CREATE TABLE IF NOT EXISTS default_settings (
    scope         TEXT NOT NULL,
    scope_id      TEXT NOT NULL,
    key           TEXT NOT NULL,
    value_json    TEXT NOT NULL,
    PRIMARY KEY (scope, scope_id, key)
);
