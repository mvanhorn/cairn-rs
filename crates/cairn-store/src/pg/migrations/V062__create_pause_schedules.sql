-- Issue #592: pause_schedules projection.
--
-- Before this migration `PauseScheduleReadModel::list_due` was a pure
-- event-log walker inside `InMemoryStore` — it scanned every
-- `RunStateChanged` event on every call, computed `resume_at_ms` in
-- the handler, and filtered out runs that had since resumed by
-- re-walking subsequent transitions. Pg and sqlite had no projection
-- at all; the handler path relied on the in-memory shadow that
-- `main.rs` rebuilds by replaying the durable log at boot (O(N) in
-- event-log size).
--
-- After this migration the event-log walker is replaced by an
-- evict-on-resume projection: `RunStateChanged(→Paused)` with a
-- `resume_after_ms` INSERTs a row; `RunStateChanged(→Running|
-- Completed|Failed|Canceled)` DELETEs it. `list_due` becomes an
-- indexed range scan with an ORDER BY + LIMIT. No event-log scan
-- regardless of history size.
--
-- Portable SQL: TEXT for ids, BIGINT for unix-ms timestamps. No
-- JSONB, no arrays, no partial indexes with `WHERE` (MySQL lacks
-- them) — the composite index below does the filtering work the
-- partial index would have done.

CREATE TABLE IF NOT EXISTS pause_schedules (
    run_id          TEXT    PRIMARY KEY,
    tenant_id       TEXT    NOT NULL,
    workspace_id    TEXT    NOT NULL,
    project_id      TEXT    NOT NULL,
    resume_at_ms    BIGINT  NOT NULL,
    created_at_ms   BIGINT  NOT NULL
);

-- `list_due(tenant_id, before_ms, limit)` hot path — the index
-- covers the tenant filter and the `resume_at_ms <= before_ms`
-- range query plus the deterministic `ORDER BY resume_at_ms ASC,
-- run_id ASC` tie-breaker used by the in-memory impl. Keeping the
-- same key order across backends is what the parity harness
-- asserts byte-for-byte.
CREATE INDEX IF NOT EXISTS idx_pause_schedules_due
    ON pause_schedules (tenant_id, resume_at_ms, run_id);
