-- RFC-025 Phase 2b.2 milestone 1: external_workers projection.
-- (Renumbered V045 → V049 after Phase 2a.2 published V045-V048 on
-- main during this PR's review cycle.)
--
-- GAP-005 registered external workers (polling-mode worker fleet) were
-- projected into `InMemoryStore.external_workers` but `log_stub`-ed on
-- pg/sqlite: every restart wiped the operator's worker catalog even
-- though the `ExternalWorkerRegistered`/`Suspended`/`Reactivated`/
-- `Reported` events were durably on the log.
--
-- Semantics mirror `ExternalWorkerRecord` (crates/cairn-domain/src/workers.rs):
--   * `status` is TEXT "active" | "suspended" — mirrors the in-memory
--     applier. `offline` is a future status and reserved.
--   * `current_task_id` is nullable so a worker can be idle.
--   * Worker-health columns (`last_heartbeat_ms`, `is_alive`,
--     `active_task_count`) are on the same row rather than a separate
--     table; heartbeat reporting is frequent but the rolling state is
--     current-only (no history table) — matches the in-memory record's
--     `health: WorkerHealth` field.
--
-- Scoping: `worker_id` is globally unique by GAP-005 contract but the
-- read model filters by `tenant_id` (every list query scopes by
-- tenant). We keep `worker_id` as the PK and add a tenant index for the
-- list-by-tenant hot path.
--
-- Portability: TEXT / BIGINT / INTEGER only. No JSONB / arrays / enums.
-- Booleans store as INTEGER 0/1 on sqlite and BOOLEAN on pg — the
-- adapter code reads either via sqlx's bool casting.

CREATE TABLE IF NOT EXISTS external_workers (
    worker_id           TEXT     PRIMARY KEY,
    tenant_id           TEXT     NOT NULL,
    display_name        TEXT     NOT NULL,
    status              TEXT     NOT NULL DEFAULT 'active',
    registered_at       BIGINT   NOT NULL,
    updated_at          BIGINT   NOT NULL,
    last_heartbeat_ms   BIGINT   NOT NULL DEFAULT 0,
    is_alive            BOOLEAN  NOT NULL DEFAULT FALSE,
    active_task_count   INTEGER  NOT NULL DEFAULT 0,
    current_task_id     TEXT
);

-- Tenant list hot path: operators fetch the worker catalog per tenant.
-- The in-memory applier sorts by `registered_at` — we tiebreak on
-- `worker_id` for stable ordering under same-ms registrations.
CREATE INDEX IF NOT EXISTS idx_external_workers_tenant
    ON external_workers (tenant_id, registered_at, worker_id);
