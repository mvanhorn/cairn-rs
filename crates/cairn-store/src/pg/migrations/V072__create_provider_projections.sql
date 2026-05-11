-- RFC 030 PR-B: pluggable memory-provider projections + retroactive
-- RFC 029 knowledge-provider projections on pg.
--
-- Context: the RFC 029 PR-B1 ship (#753) added
-- `crates/cairn-store/migrations/V018__create_knowledge_providers.sql`
-- at the top-level migrations dir but never wired it into the pg
-- migration runner (`src/pg/migrations/`). As a result fresh pg boots
-- today project `KnowledgeProvider*` events against tables that don't
-- exist, silently losing the read model. SQLite boots were fine because
-- the sqlite schema bootstrap includes the knowledge DDL inline.
--
-- This migration closes that gap defensively: `CREATE TABLE IF NOT
-- EXISTS` on the knowledge pair first (idempotent for operators who
-- manually applied the orphan V018.sql), then the RFC 030 memory pair,
-- then the cross-family view, then the one-shot scoring-policy key
-- rename. Running this migration twice on any database is a no-op.
--
-- Schema matches the sqlite mirror in `src/sqlite/schema.rs` byte-for-byte
-- modulo type names (`BOOLEAN` on pg, `INTEGER` on sqlite; `BIGINT` on pg,
-- `INTEGER` on sqlite — sqlite's `INTEGER` covers 64-bit signed).

-- ── project_knowledge_providers (RFC 029, retroactively wired to pg) ─────

-- RFC 030 adds `is_bootstrap BOOLEAN` to `KnowledgeProviderConfigured` as a
-- sibling to the memory-family equivalent. Operators on a database that
-- pre-dates RFC 030 get `ALTER TABLE IF EXISTS … ADD COLUMN IF NOT EXISTS`
-- applied below; fresh databases get the column inline via the CREATE
-- TABLE. Both paths converge on the same shape so the projection applier
-- can bind `is_bootstrap` unconditionally.

CREATE TABLE IF NOT EXISTS project_knowledge_providers (
    tenant_id           TEXT NOT NULL,
    workspace_id        TEXT NOT NULL,
    project_id          TEXT NOT NULL,
    provider_ref        TEXT NOT NULL,
    kind                TEXT NOT NULL,
    at_ms               BIGINT NOT NULL,
    configured_by       TEXT,
    is_bootstrap        BOOLEAN NOT NULL DEFAULT FALSE,
    reason              TEXT,
    prior_snapshot_json TEXT,
    current_snapshot_json TEXT,
    PRIMARY KEY (tenant_id, workspace_id, project_id, provider_ref, kind, at_ms)
);

-- Back-compat: pre-RFC-030 databases that created
-- `project_knowledge_providers` via the unwired top-level V018 file (or via
-- any out-of-band DDL before V072 landed) are missing the new column.
-- `ADD COLUMN IF NOT EXISTS` is idempotent and safe on a freshly-created
-- table too.
ALTER TABLE project_knowledge_providers
    ADD COLUMN IF NOT EXISTS is_bootstrap BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_project_knowledge_providers_project
    ON project_knowledge_providers (tenant_id, workspace_id, project_id, at_ms);

CREATE TABLE IF NOT EXISTS knowledge_ingest_jobs (
    tenant_id       TEXT NOT NULL,
    workspace_id    TEXT NOT NULL,
    project_id      TEXT NOT NULL,
    document_id     TEXT NOT NULL,
    provider_ref    TEXT NOT NULL,
    status          TEXT NOT NULL,
    source_type     TEXT,
    reason          TEXT,
    submitted_at_ms BIGINT NOT NULL,
    updated_at_ms   BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id, project_id, document_id)
);

CREATE INDEX IF NOT EXISTS idx_knowledge_ingest_jobs_status
    ON knowledge_ingest_jobs (tenant_id, workspace_id, project_id, status);

-- ── project_memory_providers (RFC 030) ──────────────────────────────────
-- Mirror of `project_knowledge_providers` plus `is_bootstrap BOOLEAN`
-- which distinguishes ProjectCreated-emitted / backfilled bootstrap
-- bindings from operator-driven re-configurations.

CREATE TABLE IF NOT EXISTS project_memory_providers (
    tenant_id           TEXT NOT NULL,
    workspace_id        TEXT NOT NULL,
    project_id          TEXT NOT NULL,
    provider_ref        TEXT NOT NULL,
    kind                TEXT NOT NULL,
    at_ms               BIGINT NOT NULL,
    configured_by       TEXT,
    is_bootstrap        BOOLEAN NOT NULL DEFAULT FALSE,
    reason              TEXT,
    prior_snapshot_json TEXT,
    current_snapshot_json TEXT,
    PRIMARY KEY (tenant_id, workspace_id, project_id, provider_ref, kind, at_ms)
);

CREATE INDEX IF NOT EXISTS idx_project_memory_providers_project
    ON project_memory_providers (tenant_id, workspace_id, project_id, at_ms);

CREATE TABLE IF NOT EXISTS memory_ingest_jobs (
    tenant_id       TEXT NOT NULL,
    workspace_id    TEXT NOT NULL,
    project_id      TEXT NOT NULL,
    document_id     TEXT NOT NULL,
    provider_ref    TEXT NOT NULL,
    status          TEXT NOT NULL,
    source_type     TEXT,
    reason          TEXT,
    submitted_at_ms BIGINT NOT NULL,
    updated_at_ms   BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id, project_id, document_id)
);

CREATE INDEX IF NOT EXISTS idx_memory_ingest_jobs_status
    ON memory_ingest_jobs (tenant_id, workspace_id, project_id, status);

-- ── v_all_ingest_jobs (cross-family operator view) ──────────────────────
-- Backs `GET /v1/projects/:project/ingest-jobs?family=…` (PR-E). Using
-- `CREATE OR REPLACE VIEW` so the migration replays idempotently even
-- if the view was added out-of-band.

CREATE OR REPLACE VIEW v_all_ingest_jobs AS
    SELECT
        'knowledge'::text     AS family,
        tenant_id,
        workspace_id,
        project_id,
        document_id,
        provider_ref,
        status,
        source_type,
        reason,
        submitted_at_ms,
        updated_at_ms
      FROM knowledge_ingest_jobs
    UNION ALL
    SELECT
        'memory'::text        AS family,
        tenant_id,
        workspace_id,
        project_id,
        document_id,
        provider_ref,
        status,
        source_type,
        reason,
        submitted_at_ms,
        updated_at_ms
      FROM memory_ingest_jobs;

-- ── scoring-policy backfill ─────────────────────────────────────────────
-- RFC 029 PR-B2 introduced `scoring_policy_json` as a project-scope
-- `default_settings` row (one scoring policy for the single-family world).
-- RFC 030 splits that into `knowledge_scoring_policy_json` +
-- `memory_scoring_policy_json`. This rename migrates existing rows;
-- memory-family policies start empty (rescorer uses
-- `ScoringPolicy::default()` until a PUT lands). Idempotent: the UPDATE
-- matches nothing on re-run.

UPDATE default_settings
   SET key = 'knowledge_scoring_policy_json'
 WHERE scope = 'project'
   AND key   = 'scoring_policy_json';
