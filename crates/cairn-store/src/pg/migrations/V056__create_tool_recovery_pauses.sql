-- RFC-025 Phase 2b.2b milestone 6: tool_recovery_pauses projection.
--
-- Before this migration the `ToolRecoveryPaused` event (RFC 020 Track 3:
-- a tool call classified as DangerousPause that cannot be safely
-- re-dispatched on recovery) was routed through `log_stub` on pg/sqlite
-- and no-op on in-memory. The event log recorded the pause fact but
-- operators had no read-model surface answering "why did this run
-- pause on recovery?" — they had to walk the event log.
--
-- PK is `tool_call_id` — each recovery pause targets exactly one
-- tool call invocation. Replayed events are idempotent via ON
-- CONFLICT DO NOTHING.
--
-- Scoping: event carries ProjectKey + run_id. Index on (run_id,
-- paused_at_ms) backs the per-run pause history hot path.

CREATE TABLE IF NOT EXISTS tool_recovery_pauses (
    tool_call_id   TEXT    PRIMARY KEY,
    tenant_id      TEXT    NOT NULL,
    workspace_id   TEXT    NOT NULL,
    project_id     TEXT    NOT NULL,
    run_id         TEXT    NOT NULL,
    task_id        TEXT,
    tool_name      TEXT    NOT NULL,
    reason         TEXT    NOT NULL,
    paused_at_ms   BIGINT  NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tool_recovery_pauses_run
    ON tool_recovery_pauses (run_id, paused_at_ms, tool_call_id);

CREATE INDEX IF NOT EXISTS idx_tool_recovery_pauses_project
    ON tool_recovery_pauses (tenant_id, workspace_id, project_id, paused_at_ms, tool_call_id);
