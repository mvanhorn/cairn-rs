-- RFC-025 Phase 2b.4 milestone 3: operator_profiles projection (RFC 008).
--
-- Before this migration the `OperatorProfileCreated` + `OperatorProfile
-- Updated` events were routed through `log_stub` on pg/sqlite: the
-- `OperatorProfileReadModel::{get, list_by_tenant}` trait returned
-- empty on a cold boot. Operator display names, emails, and workspace
-- roles were wiped by every restart on persistent backends.
--
-- PK is `operator_id` — one row per operator. `Created` upserts the
-- full row (all five fields), `Updated` is a patch-shape that only
-- touches `display_name` and/or `email` when the event carries
-- Some(_) for that field. `role` is immutable via `Updated` — matches
-- the in-memory applier, which only mutates display_name + email.
--
-- Scoping: events carry `tenant_id`, indexed for `list_by_tenant`.
-- Tiebreaker on operator_id ASC for deterministic cross-backend sort.

CREATE TABLE IF NOT EXISTS operator_profiles (
    operator_id    TEXT    PRIMARY KEY,
    tenant_id      TEXT    NOT NULL,
    display_name   TEXT    NOT NULL,
    email          TEXT    NOT NULL,
    role           TEXT    NOT NULL,
    created_at_ms  BIGINT  NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_operator_profiles_tenant
    ON operator_profiles (tenant_id, operator_id);
