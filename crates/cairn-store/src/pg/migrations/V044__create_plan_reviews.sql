-- RFC-025 Phase 2b.1 milestone 4: plan_reviews projection (RFC 018).
--
-- Plan-mode runs produce an artifact that operators approve / reject /
-- ask revisions on. Pre-Phase-2b.1 all four Plan-review events
-- (`PlanProposed`, `PlanApproved`, `PlanRejected`, `PlanRevisionRequested`)
-- routed through `log_stub` in pg/sqlite AND were a no-op in the
-- in-memory projection — zero durability across every backend. The
-- plan artifact survived as an immutable event log entry but the
-- read-model surface did not exist, so operator workflows
-- (`GET /v1/runs/:id/plan`) degraded to full-scan event-log lookup on
-- every request.
--
-- One row per plan run, keyed by `plan_run_id`. The state column
-- carries the lifecycle transition (proposed → approved / rejected /
-- revision_requested), and a set of paired columns persists the
-- resolver identity + timestamp + free-text note. `revision_run_id`
-- captures the successor plan run when a revision is requested.
--
-- Portable SQL: TEXT for ids + markdown + reason, BIGINT/INTEGER for
-- timestamps.

CREATE TABLE IF NOT EXISTS plan_reviews (
    plan_run_id         TEXT    PRIMARY KEY,
    tenant_id           TEXT    NOT NULL,
    workspace_id        TEXT    NOT NULL,
    project_id          TEXT    NOT NULL,
    session_id          TEXT    NOT NULL,
    plan_markdown       TEXT    NOT NULL,
    state               TEXT    NOT NULL,
    proposed_at         BIGINT  NOT NULL,
    resolved_by         TEXT,
    resolved_at         BIGINT,
    reviewer_comments   TEXT,
    rejection_reason    TEXT,
    revision_run_id     TEXT
);

-- Project + state hot path — dashboards list "pending plan reviews"
-- scoped to a project.
CREATE INDEX IF NOT EXISTS idx_plan_reviews_project_state
    ON plan_reviews (tenant_id, workspace_id, project_id, state, proposed_at, plan_run_id);

-- Session lineage — a session can spawn several plan runs (original +
-- revisions). list_by_session sorts by proposed_at ASC.
CREATE INDEX IF NOT EXISTS idx_plan_reviews_session
    ON plan_reviews (session_id, proposed_at, plan_run_id);
