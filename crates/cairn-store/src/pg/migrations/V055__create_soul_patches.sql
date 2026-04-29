-- RFC-025 Phase 2b.2b milestone 5: soul_patches projection.
--
-- Before this migration the `SoulPatchProposed` + `SoulPatchApplied`
-- events were routed through `log_stub` on pg/sqlite and were no-ops
-- on in-memory — no read-model row existed anywhere for proposed or
-- applied soul patches.
--
-- Lifecycle: `SoulPatchProposed` inserts a row with state='proposed';
-- `SoulPatchApplied` UPSERTs state='applied' + applied_at + new_version.
-- Out-of-order replay (Applied arrives first on partial restore) is
-- handled by allowing the Applied arm to insert a row that's already
-- in applied state (inserts a synthetic 'applied' row with a zero
-- `proposed_at` rather than failing the transaction). In practice
-- SoulPatchApplied never fires before Proposed (the service layer
-- enforces the ordering), but the applier is defensive.
--
-- Scoping: events are project-scoped; we persist the project triplet
-- so tenant/workspace dashboards can list their patch history.

CREATE TABLE IF NOT EXISTS soul_patches (
    patch_id            TEXT    PRIMARY KEY,
    tenant_id           TEXT    NOT NULL,
    workspace_id        TEXT    NOT NULL,
    project_id          TEXT    NOT NULL,
    state               TEXT    NOT NULL DEFAULT 'proposed',
    patch_content       TEXT    NOT NULL DEFAULT '',
    requires_approval   BOOLEAN NOT NULL DEFAULT TRUE,
    proposed_at_ms      BIGINT  NOT NULL DEFAULT 0,
    applied_at_ms       BIGINT,
    new_version         INTEGER
);

-- Project dashboard: recent patches by project, sorted newest-first.
CREATE INDEX IF NOT EXISTS idx_soul_patches_project
    ON soul_patches (tenant_id, workspace_id, project_id, proposed_at_ms DESC, patch_id DESC);

-- Pending-approval queue: patches still in 'proposed' state.
CREATE INDEX IF NOT EXISTS idx_soul_patches_state
    ON soul_patches (state, proposed_at_ms, patch_id);
