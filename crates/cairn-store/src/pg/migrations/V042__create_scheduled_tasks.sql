-- RFC-025 Phase 2b.1 milestone 2: scheduled_tasks projection.
--
-- The `ScheduledTaskCreated` event (emitted by the `schedule_task` tool
-- in cairn-tools) was routed through `log_stub` in pg/sqlite before
-- this migration. Operators could POST a scheduled task and see it in
-- the live session's scheduled-task list, but the entry vanished on
-- restart: the runtime recovery sweep (`list_due`) returned empty and
-- cron-scheduled work never fired after a reboot.
--
-- The `ScheduledTaskRecord` domain shape (cairn-domain/src/scheduled_task.rs)
-- carries fields that the event does not: `last_run_at`, `updated_at`,
-- `enabled`. On first insert the projection synthesises those from the
-- event's `created_at` + defaults (`last_run_at = NULL`,
-- `updated_at = created_at`, `enabled = true`) — mirrors the in-memory
-- projection landed earlier. Later lifecycle events (cancel / last-run
-- update) would mutate the row; those events don't exist yet and are
-- tracked as a Phase 2b.2 follow-up.
--
-- Portable SQL: TEXT for ids, BIGINT for timestamps, BOOLEAN for
-- `enabled` (SQLite aliases to INTEGER 0/1 per sqlx convention).

CREATE TABLE IF NOT EXISTS scheduled_tasks (
    scheduled_task_id  TEXT    PRIMARY KEY,
    tenant_id          TEXT    NOT NULL,
    name               TEXT    NOT NULL,
    cron_expression    TEXT    NOT NULL,
    last_run_at        BIGINT,
    next_run_at        BIGINT,
    enabled            BOOLEAN NOT NULL DEFAULT TRUE,
    created_at         BIGINT  NOT NULL,
    updated_at         BIGINT  NOT NULL
);

-- Tenant dashboard list — list_by_tenant is paginated by (created_at ASC,
-- id ASC) to mirror the in-memory `sort_by_key(created_at)` ordering.
CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_tenant
    ON scheduled_tasks (tenant_id, created_at, scheduled_task_id);

-- Recovery-sweep hot path — list_due wants enabled tasks whose
-- next_run_at is <= now. The partial-index shape varies per backend
-- so the index carries the filter columns without a WHERE clause;
-- the engine can still prune on enabled + next_run_at.
CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_due
    ON scheduled_tasks (enabled, next_run_at, scheduled_task_id);
