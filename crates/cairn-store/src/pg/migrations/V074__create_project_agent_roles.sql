-- RFC 031 PR-B2: `project_agent_roles` projection table (pg parity).
--
-- Projects `AgentRoleDefined` + `AgentRoleRetracted` into a durable
-- read model so `AgentRoleReadModel` on the pg adapter returns the
-- same rows the InMemoryStore applier writes. PR-B landed the event
-- wiring + InMemoryStore applier; this migration closes the durable-
-- backend gap so pg boots no longer need the `replay-into-InMemory`
-- step to power `/v1/projects/:project/agent-roles`.
--
-- Uniqueness contract (§D6):
--   POST with an id that has an **active** row (retracted_at IS NULL)
--   returns 409. Re-POST with a retracted id upserts the same row
--   AND clears `retracted_at = NULL` atomically. A single composite
--   primary key on `(tenant_id, workspace_id, project_id, role_id)`
--   is enough — the applier explicitly overwrites `retracted_at` to
--   NULL on every `AgentRoleDefined`, so "active row" is any row
--   where `retracted_at IS NULL` and "retracted row" is
--   `retracted_at IS NOT NULL`. No partial index needed.
--
-- Portability (feedback_no_db_specific_features.md):
-- * No JSONB — `role_json` is a TEXT column carrying `serde_json::to_string(&AgentRole)`.
-- * No partial indexes — the `(pk) + retracted_at IS NULL` filter is
--   cheap on a bounded-size table (tens of rows per project).
-- * Timestamps are BIGINT ms-since-epoch, matching the rest of the schema.
--
-- Mirrored in `crates/cairn-store/src/sqlite/schema.rs` (same table name +
-- columns + index).

CREATE TABLE IF NOT EXISTS project_agent_roles (
    tenant_id          TEXT   NOT NULL,
    workspace_id       TEXT   NOT NULL,
    project_id         TEXT   NOT NULL,
    role_id            TEXT   NOT NULL,
    -- serde-JSON blob of the full `AgentRole` struct. Rehydrated on read
    -- with `serde_json::from_str::<AgentRole>` so shadowed built-ins and
    -- novel ids round-trip identically.
    role_json          TEXT   NOT NULL,
    -- `Some("reviewer")` etc. when the id matches a built-in and the row
    -- shadows it; NULL for novel ids. Duplicates the discriminator on the
    -- event for the `source == custom_shadow` wire response without
    -- re-parsing `role_json`.
    shadows_builtin    TEXT,
    -- Operator id carried on the latest `AgentRoleDefined` event.
    defined_by         TEXT   NOT NULL,
    -- Event-log timestamp (ms since epoch) — also the `ETag` value
    -- emitted on 2xx responses for `If-Match` lost-update protection.
    defined_at         BIGINT NOT NULL,
    -- NULL means the row is the currently-active definition; non-NULL
    -- means the role has been retracted (the next POST with the same
    -- id clears this back to NULL per §D6).
    retracted_at       BIGINT,
    retracted_by       TEXT,
    PRIMARY KEY (tenant_id, workspace_id, project_id, role_id)
);

-- Sorted `list_active` against a single project: filter on project
-- prefix + retracted_at IS NULL, order by role_id. The PK index covers
-- the lookup but a dedicated index on the project prefix speeds the
-- active-only listing when a tenant owns many workspaces/projects.
CREATE INDEX IF NOT EXISTS idx_project_agent_roles_active
    ON project_agent_roles (tenant_id, workspace_id, project_id, role_id);
