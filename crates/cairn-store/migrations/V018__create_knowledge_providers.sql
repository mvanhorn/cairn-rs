-- RFC 029: Pluggable Knowledge Providers.
-- Two tables, matching the projection_registry entries added alongside.

-- ── project_knowledge_providers ──────────────────────────────────────────
-- One row per (project, provider_ref, kind). kind = "configured" rows are
-- upserted (one per project/provider pair representing current state); kind
-- in {"unavailable", "capability_changed"} are audit inserts keyed by at_ms
-- so multiple such rows coexist per (project, provider_ref).
--
-- The composite primary key (project, provider_ref, kind, at_ms) reflects
-- that requirement: "configured" rows are effectively deduped by the
-- ON CONFLICT DO UPDATE upsert (same (project, provider, "configured"),
-- at_ms updates); audit rows land distinct at_ms values.

CREATE TABLE IF NOT EXISTS project_knowledge_providers (
    tenant_id           TEXT NOT NULL,
    workspace_id        TEXT NOT NULL,
    project_id          TEXT NOT NULL,
    provider_ref        TEXT NOT NULL,
    -- "configured" | "unavailable" | "capability_changed"
    kind                TEXT NOT NULL,
    at_ms               BIGINT NOT NULL,
    -- "configured" row fields
    configured_by       TEXT,
    -- "unavailable" row fields
    reason              TEXT,
    -- "capability_changed" row fields (snapshots serialized as JSON text)
    prior_snapshot_json TEXT,
    current_snapshot_json TEXT,

    PRIMARY KEY (tenant_id, workspace_id, project_id, provider_ref, kind, at_ms)
);

CREATE INDEX IF NOT EXISTS idx_project_knowledge_providers_project
    ON project_knowledge_providers (tenant_id, workspace_id, project_id, at_ms);

-- ── knowledge_ingest_jobs ────────────────────────────────────────────────
-- One row per (project, document_id). Submitted/Rejected events insert;
-- StatusUpdated updates the status column. Rejected rows stop transitioning
-- (their status stays "rejected").

CREATE TABLE IF NOT EXISTS knowledge_ingest_jobs (
    tenant_id       TEXT NOT NULL,
    workspace_id    TEXT NOT NULL,
    project_id      TEXT NOT NULL,
    document_id     TEXT NOT NULL,
    provider_ref    TEXT NOT NULL,
    -- "submitted" | "pending" | "parsing" | "chunking" | "embedding"
    --  | "indexing" | "completed" | "failed" | "rejected"
    status          TEXT NOT NULL,
    source_type     TEXT,
    reason          TEXT,
    submitted_at_ms BIGINT NOT NULL,
    updated_at_ms   BIGINT NOT NULL,

    PRIMARY KEY (tenant_id, workspace_id, project_id, document_id)
);

CREATE INDEX IF NOT EXISTS idx_knowledge_ingest_jobs_status
    ON knowledge_ingest_jobs (tenant_id, workspace_id, project_id, status);
