-- F65 PR-2: orchestrator-session projections.
--
-- Adds four projection surfaces (one extended, three new) so the control
-- plane can persist the durable milestones of an orchestrated session
-- without walking the event log. Shapes come from `docs/design/orchestrator-
-- session-architecture.md` §7.
--
-- Portability: every column uses the pg/sqlite common subset. JSON payloads
-- (checkpoint body, compacted_summary) stay as TEXT — no JSONB, no arrays,
-- no advisory locks, no LISTEN/NOTIFY. SQLite runs the equivalent DDL via
-- `crates/cairn-store/src/sqlite/schema.rs` (SCHEMA_SQL) + pragma_table_info
-- column adds in `sqlite/adapter.rs::migrate`.

-- ── (1) workspace_registry ─────────────────────────────────────────────────
-- Live workspace-id → host-path mapping. `fs_root` is never exposed to LLM
-- context; the WorkspaceResolver (PR-7) is the only consumer. `status`
-- tracks the overlayfs lifecycle described in arch-doc §4.3.
CREATE TABLE IF NOT EXISTS workspace_registry (
    workspace_id    TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    workspace_scope TEXT NOT NULL,
    project_id      TEXT NOT NULL,
    root_run_id     TEXT NOT NULL,
    fs_root         TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'active',
    created_at      BIGINT NOT NULL,
    reaped_at       BIGINT
);
CREATE INDEX IF NOT EXISTS idx_workspace_registry_root_run
    ON workspace_registry (root_run_id);
CREATE INDEX IF NOT EXISTS idx_workspace_registry_status
    ON workspace_registry (status);

-- ── (2) checkpoints F65 extension ──────────────────────────────────────────
-- The existing `checkpoints` table (V006 + V020) owns the checkpoint_id
-- identity space; F65 adds orchestrator-resumable fields alongside the
-- RFC 005 columns. Nullable because legacy RFC 005 checkpoints never carry
-- them (shapes §4.3.1 + §7 in the arch doc).
ALTER TABLE checkpoints ADD COLUMN IF NOT EXISTS session_id      TEXT;
ALTER TABLE checkpoints ADD COLUMN IF NOT EXISTS schema_version  INTEGER;
ALTER TABLE checkpoints ADD COLUMN IF NOT EXISTS body            TEXT;
ALTER TABLE checkpoints ADD COLUMN IF NOT EXISTS body_size_bytes BIGINT;
ALTER TABLE checkpoints ADD COLUMN IF NOT EXISTS iteration       INTEGER;
-- Lookups by session for the F65 checkpoint read path.
CREATE INDEX IF NOT EXISTS idx_checkpoints_session_iteration
    ON checkpoints (session_id, iteration)
    WHERE session_id IS NOT NULL;

-- ── (3) workspace_snapshots ────────────────────────────────────────────────
-- Immutable reflinked snapshot trees. `parent_snapshot_id` is the
-- lineage chain for GC walk. `snapshot_path` is stored as the relative-
-- to-configured-root string (portability contract documented on
-- `WorkspaceSnapshot.snapshot_path`). `reflink_used` records whether
-- reflink worked (btrfs / xfs) or we fell back to full-copy on ext4.
CREATE TABLE IF NOT EXISTS workspace_snapshots (
    snapshot_id         TEXT PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    workspace_scope     TEXT NOT NULL,
    project_id          TEXT NOT NULL,
    session_id          TEXT NOT NULL,
    workspace_id        TEXT NOT NULL,
    parent_snapshot_id  TEXT REFERENCES workspace_snapshots(snapshot_id),
    snapshot_path       TEXT NOT NULL,
    bytes               BIGINT NOT NULL DEFAULT 0,
    reflink_used        BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          BIGINT NOT NULL,
    reaped_at           BIGINT
);
CREATE INDEX IF NOT EXISTS idx_workspace_snapshots_session
    ON workspace_snapshots (session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_workspace_snapshots_parent
    ON workspace_snapshots (parent_snapshot_id)
    WHERE parent_snapshot_id IS NOT NULL;

-- ── (4) session_outcomes ───────────────────────────────────────────────────
-- One rich outcome per session terminal attempt; primary key is
-- root_run_id (one outcome per root-Run). `workspace_snapshot_id` is
-- **nullable** by design: legacy runs that predate the sandbox (arch-doc
-- §6.3) never produce a snapshot. `compacted_summary` is the LLM-written
-- structured JSON string from PR-6; on pre-PR-6 outcomes it is the empty
-- string. `cost_micros` stores USD micros (integer storage keeps the
-- outcome equatable for log replay, matches `SessionCostUpdated` shape).
CREATE TABLE IF NOT EXISTS session_outcomes (
    root_run_id            TEXT PRIMARY KEY,
    tenant_id              TEXT NOT NULL,
    workspace_scope        TEXT NOT NULL,
    project_id             TEXT NOT NULL,
    session_id             TEXT NOT NULL,
    checkpoint_id          TEXT NOT NULL REFERENCES checkpoints(checkpoint_id),
    workspace_snapshot_id  TEXT REFERENCES workspace_snapshots(snapshot_id),
    -- `termination_reason` is the short discriminator (snake_case) used
    -- by the `idx_session_outcomes_termination` index so operator filters
    -- stay cheap. The full payload (provider error message, breaker trip
    -- fields, crash details) is carried on `termination_reason_json` —
    -- JSON-as-TEXT because the portable pg+sqlite subset doesn't include
    -- JSONB. Readers rehydrate the full `TerminationReason` from the
    -- JSON column, falling back to the discriminator on legacy rows.
    termination_reason     TEXT NOT NULL,
    termination_reason_json TEXT,
    compacted_summary      TEXT NOT NULL DEFAULT '',
    next_step_hint         TEXT,
    cost_micros            BIGINT NOT NULL DEFAULT 0,
    created_at             BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_session_outcomes_session
    ON session_outcomes (session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_session_outcomes_termination
    ON session_outcomes (termination_reason);

-- Idempotent add for `termination_reason_json` — the migration runner
-- tracks only `(version, name)` in `_cairn_migrations`, so if an early
-- copy of V032 (without this column) ran against a DB during PR review,
-- the column must be added without re-executing the whole migration.
-- `ADD COLUMN IF NOT EXISTS` is a no-op when the table was CREATED above
-- with the column already present.
ALTER TABLE session_outcomes ADD COLUMN IF NOT EXISTS termination_reason_json TEXT;
