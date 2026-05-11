-- RFC-025 Phase 2b.2b milestone 2: signal_ingestions projection.
--
-- Before this migration the `SignalIngested` event was routed through
-- `log_stub` on pg/sqlite — the event log durably recorded every
-- incoming signal (webhook, trigger callback, manual ingest) but no
-- read-model row existed on persistent backends. Every restart wiped
-- the operator's signal history from `GET /v1/signals` reads even
-- though the events were durable on the log (issue #581).
--
-- The in-memory store maintains `signals: HashMap<String, SignalRecord>`
-- keyed by signal_id; this migration mirrors it row-for-row. Replay
-- discipline: `ON CONFLICT (signal_id) DO NOTHING` keeps a double-
-- delivery idempotent — signal_id is caller-supplied via
-- `SignalServiceImpl::ingest` and is expected to be unique per emit;
-- replayed duplicates (boot-time walk) find the first row and leave
-- it untouched.
--
-- Scoping: signals are project-scoped (the event carries ProjectKey).
-- The hot path is list-by-project for operator dashboards; we add an
-- index on (tenant_id, workspace_id, project_id, timestamp_ms,
-- signal_id) so the ORDER BY on the service layer is index-resolved.
--
-- `payload` is a JSON object stored as TEXT (no JSONB — portable to
-- SQLite). An empty payload defaults to `{}` on the event envelope;
-- the projection stores whatever serde_json::to_string yielded from
-- the event (typically `"null"` for default `Value::Null` or the
-- caller's object/array).

CREATE TABLE IF NOT EXISTS signal_ingestions (
    signal_id      TEXT    PRIMARY KEY,
    tenant_id      TEXT    NOT NULL,
    workspace_id   TEXT    NOT NULL,
    project_id     TEXT    NOT NULL,
    source         TEXT    NOT NULL,
    payload_json   TEXT    NOT NULL DEFAULT 'null',
    timestamp_ms   BIGINT  NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_signal_ingestions_project
    ON signal_ingestions (tenant_id, workspace_id, project_id, timestamp_ms, signal_id);
