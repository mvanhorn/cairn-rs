-- RFC-025 Phase 2a.1 milestone 2: tenant_quotas + tenant_quota_violations
-- projection tables.
--
-- `tenant_quotas` stores the operator-configured quota baseline from
-- `TenantQuotaSet`: max_concurrent_runs, max_sessions_per_hour,
-- max_tasks_per_run. The dynamic current_active_runs /
-- sessions_this_hour counters are computed on read by joining the
-- sessions and runs projection tables — mirroring the in-memory
-- `QuotaReadModel` applier that fills them from state.sessions /
-- state.runs live (in_memory.rs::get_quota).
--
-- `tenant_quota_violations` stores the audit trail from
-- `TenantQuotaViolated`. One row per violation event, keyed by
-- (tenant_id, occurred_at_ms, quota_type) so the same event replayed
-- does not duplicate.
--
-- Portability: no JSONB, no pg arrays; `feedback_no_db_specific_features.md`.

CREATE TABLE IF NOT EXISTS tenant_quotas (
    tenant_id              TEXT    PRIMARY KEY,
    max_concurrent_runs    INTEGER NOT NULL,
    max_sessions_per_hour  INTEGER NOT NULL,
    max_tasks_per_run      INTEGER NOT NULL,
    created_at             BIGINT  NOT NULL,
    updated_at             BIGINT  NOT NULL
);

CREATE TABLE IF NOT EXISTS tenant_quota_violations (
    tenant_id      TEXT    NOT NULL,
    quota_type     TEXT    NOT NULL,
    occurred_at_ms BIGINT  NOT NULL,
    current_value  INTEGER NOT NULL,
    limit_value    INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, quota_type, occurred_at_ms)
);

CREATE INDEX IF NOT EXISTS idx_tenant_quota_violations_tenant_time
    ON tenant_quota_violations (tenant_id, occurred_at_ms DESC);
