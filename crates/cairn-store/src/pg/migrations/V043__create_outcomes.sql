-- RFC-025 Phase 2b.1 milestone 3: outcomes projection.
--
-- `OutcomeRecorded` events (emitted by the `eval_score` tool) were
-- routed through `log_stub` on pg/sqlite before this migration. The
-- in-memory projection kept the record in `state.outcomes`, but the
-- evaluator-optimizer feedback loop lost its calibration inputs on
-- restart — confidence-vs-actual calibration rebuilt from the event
-- log on each boot (eval-pipeline regression).
--
-- `actual_outcome` is a snake_case serialised enum (Success / Failure
-- / Partial). `predicted_confidence` is an f64 on the wire; pg and
-- SQLite both store it as DOUBLE PRECISION / REAL respectively.
-- Portable SQL otherwise.

CREATE TABLE IF NOT EXISTS outcomes (
    outcome_id             TEXT             PRIMARY KEY,
    run_id                 TEXT             NOT NULL,
    tenant_id              TEXT             NOT NULL,
    workspace_id           TEXT             NOT NULL,
    project_id             TEXT             NOT NULL,
    agent_type             TEXT             NOT NULL,
    predicted_confidence   DOUBLE PRECISION NOT NULL,
    actual_outcome         TEXT             NOT NULL,
    recorded_at            BIGINT           NOT NULL
);

-- Per-run lookup — calibration queries usually scope to a run id.
CREATE INDEX IF NOT EXISTS idx_outcomes_run
    ON outcomes (run_id, recorded_at, outcome_id);

-- Per-project list — operator dashboard + offline calibration batches.
CREATE INDEX IF NOT EXISTS idx_outcomes_project
    ON outcomes (tenant_id, workspace_id, project_id, recorded_at, outcome_id);
