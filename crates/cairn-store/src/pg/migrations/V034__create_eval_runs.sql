-- RFC-025 Phase 1 eval_runs projection table.
--
-- Projects five eval lifecycle events into a durable read model so
-- operators can query runs after a process restart without walking the
-- event log. Before this migration, pg/sqlite backends logged
-- EvalRunStarted / Completed / Archived / Scored / RubricScored with
-- `log_stub` — the event hit the event_log but no read-model row was
-- written, so AppState::replay_evals had to rebuild state.evals from
-- the full event log on every boot (#437).
--
-- Schema kept portable for SQLite parity: no JSONB, no pg arrays. The
-- `metrics_json`/`rubric_score_json` columns hold serde-JSON blobs
-- stored as TEXT on both backends; the projection applier deserialises
-- them into EvalMetrics / RubricScoreResult. See
-- feedback_no_db_specific_features.md in MEMORY.md.
--
-- The primary key is `eval_run_id` — one row per run, mutated on
-- subsequent lifecycle events (Completed, Scored, Archived, etc.).
-- Multi-score support lands via last-write-wins on `metrics_json`;
-- the full score history lives in the event log if needed.

CREATE TABLE IF NOT EXISTS eval_runs (
    eval_run_id       TEXT    PRIMARY KEY,
    tenant_id         TEXT    NOT NULL,
    workspace_id      TEXT    NOT NULL,
    project_id        TEXT    NOT NULL,
    subject_kind      TEXT    NOT NULL,
    evaluator_type    TEXT    NOT NULL,
    -- NULL until EvalRunCompleted fires.
    success           BOOLEAN,
    error_message     TEXT,
    started_at        BIGINT  NOT NULL,
    completed_at      BIGINT,
    -- NULL for active runs; set to the event's `archived_at` when
    -- EvalRunArchived fires (idempotent: earliest-wins, matching the
    -- in-memory archive semantics).
    archived_at       BIGINT,
    -- RFC-025 milestone 3/4/5: serialised EvalMetrics snapshot from the
    -- most recent EvalRunScored event (or EvalRunCompleted, whichever
    -- carried metrics). Stored as TEXT for cross-backend parity.
    metrics_json      TEXT,
    -- RFC-025 milestone 3/4/5: serialised RubricScoreResult from the
    -- most recent EvalRubricScored event.
    rubric_score_json TEXT,
    -- RFC-025 milestone 6: run-bound metadata previously only held on
    -- the in-memory EvalRunService and restored by `replay_evals`. Owning
    -- these in the projection is what lets milestone 6 delete
    -- replay_evals without regressing the #220/#223 restart tests. All
    -- nullable — a create event need not bind dataset/rubric/baseline.
    dataset_id        TEXT,
    rubric_id         TEXT,
    baseline_id       TEXT,
    prompt_asset_id   TEXT,
    prompt_version_id TEXT,
    prompt_release_id TEXT,
    created_by        TEXT
);

-- Tenant-scoped list queries are the common read pattern
-- (`GET /v1/evals/runs?tenant_id=…&workspace_id=…&project_id=…`).
CREATE INDEX IF NOT EXISTS idx_eval_runs_project
    ON eval_runs (tenant_id, workspace_id, project_id, started_at);
