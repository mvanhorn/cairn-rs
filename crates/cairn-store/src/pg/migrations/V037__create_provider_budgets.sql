-- RFC-025 Phase 2a.1 milestone 3: provider_budgets projection table.
--
-- Projects three provider-budget events:
--
-- * `ProviderBudgetSet` — creates/updates the operator-configured budget.
--   Keyed on `budget_id` (not `tenant_id:period`) because the Alert and
--   Exceeded events reference `budget_id` directly. The in_memory
--   projection also keys on `budget_id` after this migration — the
--   previous `tenant_id:period` composite key was inconsistent with
--   the subsequent events and could not be joined.
-- * `ProviderBudgetAlertTriggered` — updates current_spend_micros on the
--   row for that budget_id.
-- * `ProviderBudgetExceeded` — updates current_spend_micros +
--   mark_exceeded_at_ms so operators can surface which budget tripped.
--
-- Portability: no JSONB, no pg arrays; enum stored as TEXT
-- ('daily' / 'monthly') per snake_case serde convention.

CREATE TABLE IF NOT EXISTS provider_budgets (
    budget_id               TEXT    PRIMARY KEY,
    tenant_id               TEXT    NOT NULL,
    period                  TEXT    NOT NULL,
    limit_micros            BIGINT  NOT NULL,
    alert_threshold_percent INTEGER NOT NULL DEFAULT 80,
    current_spend_micros    BIGINT  NOT NULL DEFAULT 0,
    alert_triggered_at_ms   BIGINT,
    exceeded_at_ms          BIGINT,
    created_at              BIGINT  NOT NULL,
    updated_at              BIGINT  NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_provider_budgets_tenant_period
    ON provider_budgets (tenant_id, period);
