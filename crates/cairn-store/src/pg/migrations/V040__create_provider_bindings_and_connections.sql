-- RFC-025 Phase 3: provider_bindings + provider_connections projection
-- tables (operator-configured provider state survives restart).
--
-- Before this migration the four provider-state events
-- (ProviderBindingCreated, ProviderBindingStateChanged,
-- ProviderConnectionRegistered, ProviderConnectionDeleted) were routed
-- through `log_stub` in pg/sqlite — the event log recorded them but no
-- read-model row was written on persistent backends. The in-memory
-- store was the only projection; restart wiped the state a second after
-- the first process-kill. Operators saw "connection not found" on the
-- next boot even though the register event was durable.
--
-- Classification per RFC-025-provider-boundary-research.md (2026-04-28):
--   * provider_bindings     — Projected (this table)
--   * provider_connections  — Projected (this table)
--   * provider_pools        — Ephemeral (live HTTP-client pool state,
--                             not persistable; rebuilt from bindings +
--                             connections at boot)
--   * provider_health       — Ephemeral (in-flight probe state; next
--                             health cycle rebuilds)
--
-- Portability: TEXT everywhere, no JSONB, no pg arrays. Complex fields
-- (ProviderBindingSettings, supported_models) ride on TEXT columns
-- carrying JSON serialised at the projection layer. Matches the
-- credentials / licenses / quotas pattern landed in Phase 2a.1.

-- ── provider_connections ─────────────────────────────────────────────
--
-- Tenant-level record of a configured LLM endpoint (openai / bedrock /
-- vertex / etc.). `provider_connection_id` is globally unique and is
-- hard-deleted on `ProviderConnectionDeleted` so the id can be re-used.
-- `supported_models_json` is a JSON array of model-identifier strings —
-- stored as TEXT because SQLite has no JSONB and pg-only array types
-- would break the portability contract.

CREATE TABLE IF NOT EXISTS provider_connections (
    provider_connection_id  TEXT    PRIMARY KEY,
    tenant_id               TEXT    NOT NULL,
    provider_family         TEXT    NOT NULL,
    adapter_type            TEXT    NOT NULL,
    supported_models_json   TEXT    NOT NULL DEFAULT '[]',
    status                  TEXT    NOT NULL,
    created_at              BIGINT  NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_provider_connections_tenant
    ON provider_connections (tenant_id, created_at, provider_connection_id);

-- ── provider_bindings ────────────────────────────────────────────────
--
-- Project-level mapping from (project, operation) to a
-- (connection, model) pair. One project can have many bindings; the
-- `list_active` query is the routing hot-path.
--
-- `settings_json` carries the full ProviderBindingSettings struct
-- (temperature_milli, max_output_tokens, timeout_ms,
-- structured_output_mode, required_capabilities, disabled_capabilities,
-- cost_type, daily_budget_micros). It is written atomically with the
-- row; readers deserialise wholesale. JSON is chosen over normalised
-- columns because the settings set is operator-tunable and expected to
-- grow without schema churn.
--
-- `ON CONFLICT (provider_binding_id) DO UPDATE` at projection time
-- means `ProviderBindingCreated` replay is idempotent — replaying the
-- creation event twice does not zero the `active` flag that a
-- subsequent `ProviderBindingStateChanged` already applied, because the
-- projection applier preserves the active column on conflict.

CREATE TABLE IF NOT EXISTS provider_bindings (
    provider_binding_id     TEXT    PRIMARY KEY,
    tenant_id               TEXT    NOT NULL,
    workspace_id            TEXT    NOT NULL,
    project_id              TEXT    NOT NULL,
    provider_connection_id  TEXT    NOT NULL,
    provider_model_id       TEXT    NOT NULL,
    operation_kind          TEXT    NOT NULL,
    settings_json           TEXT    NOT NULL DEFAULT '{}',
    active                  BOOLEAN NOT NULL DEFAULT TRUE,
    created_at              BIGINT  NOT NULL
);

-- Project-scoped list + list_active hot-path. Sort tiebreaker is
-- (created_at, provider_binding_id) — matches the in-memory
-- `list_active` ordering so cross-backend parity tests pass.
CREATE INDEX IF NOT EXISTS idx_provider_bindings_project_active
    ON provider_bindings (tenant_id, workspace_id, project_id, active, operation_kind, created_at, provider_binding_id);

-- Tenant-level list (`list_by_tenant`) for operator dashboards.
CREATE INDEX IF NOT EXISTS idx_provider_bindings_tenant
    ON provider_bindings (tenant_id, created_at, provider_binding_id);
