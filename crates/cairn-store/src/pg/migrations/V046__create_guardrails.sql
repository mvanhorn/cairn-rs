-- RFC-025 Phase 2a.2 milestone 2: guardrail_policies + guardrail_evaluations
-- projection tables.
--
-- `guardrail_policies` is the current-state projection of
-- `GuardrailPolicyCreated`: operator-authored tenant-scoped policies
-- with one row per policy. The in-memory applier keys on `policy_id`
-- alone because `policy_id` already contains the creation timestamp and
-- is globally unique, so `ON CONFLICT (policy_id) DO UPDATE` gives
-- create-or-refresh semantics aligned with the in-memory map insert.
--
-- `rules` is stored as a JSON array on TEXT per the portability rule
-- (`feedback_no_db_specific_features.md`): no JSONB, no pg arrays.
-- The read-model deserializes `rules_json` back to `Vec<GuardrailRule>`
-- on query.
--
-- `guardrail_evaluations` is the per-event audit trail from
-- `GuardrailPolicyEvaluated`. One row per evaluation keyed by
-- (tenant_id, policy_id, subject_type, subject_id_or_empty, action,
-- evaluated_at_ms) so replaying the same event is idempotent. `tenant_id`
-- leads the key so cross-tenant collisions are impossible even when a
-- shared runtime-emitted `policy_id` like `"implicit_allow"` is evaluated
-- for the same subject in the same ms across tenants. Using COALESCE-style
-- empty-string sentinels for nullable `subject_id` keeps the composite
-- key primary-key compatible across pg and sqlite (no deferrable null
-- uniqueness).

CREATE TABLE IF NOT EXISTS guardrail_policies (
    policy_id   TEXT    PRIMARY KEY,
    tenant_id   TEXT    NOT NULL,
    name        TEXT    NOT NULL,
    rules_json  TEXT    NOT NULL DEFAULT '[]',
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  BIGINT  NOT NULL,
    updated_at  BIGINT  NOT NULL
);

-- Tenant-scoped list queries are the primary read pattern
-- (`GET /v1/guardrails?tenant_id=…` + the
-- `GuardrailService::evaluate` hot path that scans every policy).
CREATE INDEX IF NOT EXISTS idx_guardrail_policies_tenant
    ON guardrail_policies (tenant_id, policy_id);

CREATE TABLE IF NOT EXISTS guardrail_evaluations (
    policy_id        TEXT    NOT NULL,
    tenant_id        TEXT    NOT NULL,
    subject_type     TEXT    NOT NULL,
    -- Empty string is the "no subject_id" sentinel so the composite key
    -- stays pure NOT NULL and the primary-key index is usable. Pg allows
    -- multi-column nullable PKs but sqlite treats NULL as distinct under
    -- UNIQUE which would break replay idempotency — keep the sentinel
    -- uniform across backends.
    subject_id       TEXT    NOT NULL DEFAULT '',
    action           TEXT    NOT NULL,
    decision         TEXT    NOT NULL,
    reason           TEXT,
    evaluated_at_ms  BIGINT  NOT NULL,
    created_at       BIGINT  NOT NULL,
    -- `tenant_id` leads the PK so cross-tenant collisions are
    -- impossible under replay: runtime-emitted `policy_id`s like
    -- `"implicit_allow"` are shared globally and different tenants
    -- evaluating the same subject in the same ms would otherwise
    -- collide on ON CONFLICT DO NOTHING and drop audit rows.
    -- Copilot review #571.
    PRIMARY KEY (tenant_id, policy_id, subject_type, subject_id, action, evaluated_at_ms)
);

-- Query pattern: "show evaluation history for tenant T" (governance UI)
-- and "show evaluations matching subject S within policy P" (debugging).
-- Composite PK leads with `tenant_id` so tenant-scoped point lookups are
-- served directly, but its remaining key order (policy_id, subject_type,
-- subject_id, action, evaluated_at_ms) is not optimized for tenant-wide
-- history ordered by `evaluated_at_ms DESC`, so keep a dedicated index
-- for that read pattern (Copilot review #571 round 2).
CREATE INDEX IF NOT EXISTS idx_guardrail_evaluations_tenant_time
    ON guardrail_evaluations (tenant_id, evaluated_at_ms DESC);
