-- RFC-025 Phase 2b.3 milestone 1: ingest_jobs projection (RFC 003).
--
-- Before this migration the `IngestJobStarted` and `IngestJobCompleted`
-- events were routed through `log_stub` on pg/sqlite — the event log
-- recorded the job fact but `IngestJobReadModel::{get,list_by_project}`
-- returned empty on a cold boot. Operators' memory-ingest catalog was
-- wiped by every restart on persistent backends.
--
-- PK is `job_id` — one row per ingest job, upsert on Completed to flip
-- state and attach error_message. Replayed events are idempotent via
-- ON CONFLICT.
--
-- Scoping: event carries ProjectKey. Composite project index backs the
-- `list_by_project` hot path (ordered by created_at ASC, job_id ASC for
-- deterministic tiebreaks when two jobs are created in the same
-- millisecond).

CREATE TABLE IF NOT EXISTS ingest_jobs (
    job_id           TEXT    PRIMARY KEY,
    tenant_id        TEXT    NOT NULL,
    workspace_id     TEXT    NOT NULL,
    project_id       TEXT    NOT NULL,
    source_id        TEXT,
    document_count   INTEGER NOT NULL,
    state            TEXT    NOT NULL,
    error_message    TEXT,
    created_at_ms    BIGINT  NOT NULL,
    updated_at_ms    BIGINT  NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ingest_jobs_project
    ON ingest_jobs (tenant_id, workspace_id, project_id, created_at_ms, job_id);
