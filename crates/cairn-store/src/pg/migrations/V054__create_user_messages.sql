-- RFC-025 Phase 2b.2b milestone 4: user_messages projection.
--
-- Before this migration the `UserMessageAppended` event was routed
-- through `log_stub` on pg/sqlite — the event log durably recorded
-- every inbound user message (chat-style follow-up on a paused run,
-- operator "/intervene" injects, API-posted follow-up prompts) but
-- no read-model row existed. `GET /v1/runs/:id/messages` had to walk
-- the event log every time, scaling linearly with total event count.
--
-- Schema keys on `(run_id, sequence)` so the client-supplied
-- `sequence` field drives stable ordering within a run; ties break
-- on `appended_at_ms`. The PK is the composite key so the "latest
-- message per run" lookup is an index scan.
--
-- `event_id` is the envelope event_id carried into the row for
-- traceability. A UNIQUE INDEX on `event_id` defends against a
-- hand-crafted duplicate event_id slipping through the event log:
-- the INSERT fails the transaction rather than silently creating a
-- ghost row. The PK's `ON CONFLICT DO NOTHING` only covers
-- `(run_id, sequence)` duplicates — a unique-constraint violation
-- on `event_id` is NOT swallowed; it surfaces as an error the
-- operator can inspect. In practice event_id is globally unique by
-- construction so this constraint is a safety rail, not a hot path.

CREATE TABLE IF NOT EXISTS user_messages (
    run_id          TEXT    NOT NULL,
    sequence        BIGINT  NOT NULL,
    tenant_id       TEXT    NOT NULL,
    workspace_id    TEXT    NOT NULL,
    project_id      TEXT    NOT NULL,
    session_id      TEXT    NOT NULL,
    event_id        TEXT    NOT NULL,
    content         TEXT    NOT NULL DEFAULT '',
    appended_at_ms  BIGINT  NOT NULL,
    PRIMARY KEY (run_id, sequence)
);

-- Replay dedupe — should never collide since event_id is globally
-- unique, but we keep the constraint so a hand-crafted duplicate
-- event cannot silently create a ghost row.
CREATE UNIQUE INDEX IF NOT EXISTS idx_user_messages_event_id
    ON user_messages (event_id);

-- Hot path: list-by-session for operator "conversation view" across
-- multiple runs in the same session.
CREATE INDEX IF NOT EXISTS idx_user_messages_session
    ON user_messages (session_id, appended_at_ms, run_id, sequence);
