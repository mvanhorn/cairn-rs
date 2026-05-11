-- RFC-025 Phase 2b.2b milestone 3: subagent_spawns projection (RFC 014).
--
-- Before this migration the `SubagentSpawned` event was routed through
-- `log_stub` on pg/sqlite — the event log durably recorded the
-- parent→child spawn linkage but no read-model row existed on
-- persistent backends. The in-memory applier writes the linkage back
-- onto the child's `tasks` row (parent_run_id / parent_task_id); this
-- table captures the spawn event itself so operator dashboards can
-- enumerate a run's subagent graph without walking the event log.
--
-- `child_task_id` is the natural PK: every spawn event creates a
-- distinct child task. A replayed event is silently discarded via
-- ON CONFLICT DO NOTHING (mirrors the in-memory applier's `if let
-- Some(rec) = state.tasks.get_mut` which is a no-op when the task
-- record is already linked).
--
-- Scoping: the event carries ProjectKey — we persist the triplet so
-- the "my tenant's subagents" operator list is resolvable without a
-- join against `tasks`.
--
-- `parent_task_id` and `child_run_id` are nullable to mirror the
-- event's `Option<…>` fields.

CREATE TABLE IF NOT EXISTS subagent_spawns (
    child_task_id     TEXT    PRIMARY KEY,
    tenant_id         TEXT    NOT NULL,
    workspace_id      TEXT    NOT NULL,
    project_id        TEXT    NOT NULL,
    parent_run_id     TEXT    NOT NULL,
    parent_task_id    TEXT,
    child_session_id  TEXT    NOT NULL,
    child_run_id      TEXT,
    spawned_at_ms     BIGINT  NOT NULL
);

-- Hot path: enumerate a run's direct subagent spawns. Sort by
-- (spawned_at_ms, child_task_id) for deterministic listing.
CREATE INDEX IF NOT EXISTS idx_subagent_spawns_parent_run
    ON subagent_spawns (parent_run_id, spawned_at_ms, child_task_id);

-- Tenant dashboard: enumerate all subagent spawns for a project.
CREATE INDEX IF NOT EXISTS idx_subagent_spawns_project
    ON subagent_spawns (tenant_id, workspace_id, project_id, spawned_at_ms, child_task_id);
