-- #364: durable projection for `ToolInvocationProgressUpdated` events.
-- One row per `invocation_id`, UPSERTed on every progress event so
-- `GET /v1/tool-invocations/:id/progress` can serve a tenant-scoped
-- read in O(1) instead of scanning the event log.
--
-- Replaces the previous `read_stream(None, 10_000)` + filter loop in
-- `get_tool_invocation_progress_handler`. That scan was both a DoS
-- risk (bounded by a fixed 10k window that silently masked data past
-- it) and a cross-tenant read oracle.
--
-- Portable surface only: no JSONB, no arrays, no Postgres-specific
-- operators. Mirrors the SQLite schema in
-- `crates/cairn-store/src/sqlite/schema.rs`.

CREATE TABLE IF NOT EXISTS tool_invocation_progress (
    invocation_id  TEXT   PRIMARY KEY,
    tenant_id      TEXT   NOT NULL,
    workspace_id   TEXT   NOT NULL,
    project_id     TEXT   NOT NULL,
    progress_pct   SMALLINT NOT NULL,
    message        TEXT,
    updated_at_ms  BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tool_invocation_progress_tenant
    ON tool_invocation_progress (tenant_id, workspace_id, project_id);
