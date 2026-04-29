-- Issue #218: soft-delete for workspaces.
--
-- Adds `archived_at` (unix-ms) column to `workspaces`. A NULL value means the
-- workspace is active; a populated value means it has been archived via
-- `DELETE /v1/admin/tenants/:t/workspaces/:w` and should be filtered out of
-- default list responses.
--
-- Originally authored as `V020__workspace_archived_at.sql` under
-- `crates/cairn-store/migrations/` (PR #225) but never wired into
-- `pg/migration_runner.rs` — the V020 slot was already occupied by
-- `V020__add_checkpoint_data_json.sql`. Renumbered + moved in #578
-- so fresh Postgres installs actually get the column. SQLite was
-- unaffected because `sqlite/adapter.rs` adds the column inline via
-- `ALTER TABLE workspaces ADD COLUMN archived_at` after pragma check.
--
-- Slot shifted V045 → V050 after main published Phase 2a.2 (V045-V048)
-- and Phase 2b.2 (V049) during this PR's review cycle.

ALTER TABLE workspaces
    ADD COLUMN IF NOT EXISTS archived_at BIGINT;

CREATE INDEX IF NOT EXISTS idx_workspaces_tenant_archived
    ON workspaces (tenant_id, archived_at, created_at, workspace_id);
