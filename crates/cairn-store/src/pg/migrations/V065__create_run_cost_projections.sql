-- RFC-025 Phase 2b.4 milestone 4: run_costs + run_cost_alerts
-- projections.
--
-- Before this migration the `RunCostUpdated`, `RunCostAlertSet`, and
-- `RunCostAlertTriggered` events were routed through `log_stub` on
-- pg/sqlite. `RunCostReadModel::{get_run_cost, list_by_session}` and
-- `RunCostAlertReadModel::{get_alert, list_triggered_by_tenant}`
-- returned empty on a cold boot. Every restart wiped the per-run
-- cost rollup + operator alert ledger on persistent backends.
--
-- Two tables:
-- * `run_costs` — one row per run_id, accumulated on every
--   `RunCostUpdated`. Matches the in-memory applier which
--   saturating_adds the four per-call deltas into a `RunCostRecord`.
--   Upsert on run_id: the INSERT-if-missing path seeds a zeroed row,
--   the UPDATE path accumulates. Counter semantics mean
--   `ON CONFLICT DO UPDATE SET total = run_costs.total + EXCLUDED.total`
--   — *not* `EXCLUDED.total` — because the delta, not the absolute
--   value, lives on the event.
-- * `run_cost_alerts` — one row per run_id. `Set` seeds or re-seeds
--   the threshold with `triggered_at_ms = 0, actual_cost_micros = 0`
--   (in-memory resets on re-set). `Triggered` updates the row's
--   `triggered_at_ms` + `actual_cost_micros` in place — matches the
--   in-memory applier which unconditionally assigns both fields
--   (`a.triggered_at_ms = ...; a.actual_cost_micros = ...`). Re-
--   delivery of the same Triggered event is therefore idempotent
--   (UPDATE to the same values), but delivery of a later Triggered
--   with different `actual_cost_micros` WILL overwrite in place —
--   not a no-op. Eventual consistency sits on the in-memory applier's
--   `if alert.triggered_at_ms == 0` guard, which suppresses duplicate
--   emission of the derived Triggered event at the service layer.

CREATE TABLE IF NOT EXISTS run_costs (
    run_id             TEXT    PRIMARY KEY,
    total_cost_micros  BIGINT  NOT NULL DEFAULT 0,
    total_tokens_in    BIGINT  NOT NULL DEFAULT 0,
    total_tokens_out   BIGINT  NOT NULL DEFAULT 0,
    provider_calls     BIGINT  NOT NULL DEFAULT 0,
    updated_at_ms      BIGINT  NOT NULL
);

CREATE TABLE IF NOT EXISTS run_cost_alerts (
    run_id              TEXT    PRIMARY KEY,
    tenant_id           TEXT    NOT NULL,
    threshold_micros    BIGINT  NOT NULL,
    -- 0 means "not yet triggered" — matches the in-memory sentinel
    -- so cross-backend parity holds on freshly-`Set` alerts.
    triggered_at_ms     BIGINT  NOT NULL DEFAULT 0,
    actual_cost_micros  BIGINT  NOT NULL DEFAULT 0,
    set_at_ms           BIGINT  NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_run_cost_alerts_tenant_triggered
    ON run_cost_alerts (tenant_id, triggered_at_ms DESC, run_id);
