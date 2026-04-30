-- RFC-025 Phase 2b.4 milestone 2: eval datasets + rubrics + baselines
-- projections (RFC 004).
--
-- Before this migration the five Eval* config events were routed through
-- `log_stub` on pg/sqlite: `EvalDatasetReadModel`, `EvalRubricReadModel`,
-- and `EvalBaselineReadModel` returned empty on a cold boot. Every restart
-- wiped the operator eval catalog on persistent backends — operators had
-- to re-create datasets, rubrics, and baselines for each bounce.
--
-- Three tables (plus a child table for dataset entries):
-- * `eval_datasets` — one row per dataset_id carrying the dataset-level
--   metadata (name, subject_kind, created_at_ms). Entries live in a
--   separate `eval_dataset_entries` child table keyed on
--   (dataset_id, entry_id) so same-ms event deliveries stay idempotent
--   via the composite PK (see `EvalDatasetEntryAdded` applier below).
--   The in-memory `EvalDataset.entries: Vec<EvalDatasetEntryAdded>`
--   field is reconstituted by `EvalDatasetReadModel::get_dataset` on
--   the sqlite adapter via a join back to `eval_dataset_entries`.
-- * `eval_rubrics` — one row per rubric_id. Dimensions carried as JSON
--   TEXT so pg + sqlite stay byte-equal (no pg-specific JSONB / arrays).
-- * `eval_baselines` — one row per baseline_id. `metrics_json` holds the
--   full `EvalMetrics` blob (ten optional numeric fields); `locked` lands
--   as INTEGER 0/1 so the column shape is byte-identical between pg +
--   sqlite. `EvalBaselineLocked` flips `locked` to 1 without resetting
--   any other field — mirrors the in-memory `if let Some(baseline) =
--   state.eval_baselines.get_mut(...)` guard.
--
-- Tenant scoping: events carry no `tenant_id`, so the projection stores
-- the sentinel empty string (matches the in-memory applier that writes
-- `TenantId::new("")`). A follow-up Phase 2b.5 task (the parent ticket
-- also tracks the RFC-025 registry rationale) should bump these events
-- to carry `tenant_id` explicitly — once the domain event version moves
-- we can tighten the list_by_tenant filter. For now the sentinel means
-- list_by_tenant returns every row when `tenant_id` is empty, matching
-- the in-memory behaviour.
--
-- Replay: all three tables use `ON CONFLICT (pk) DO NOTHING` on create
-- so replayed events are idempotent. EvalBaselineSet on an already-set
-- baseline re-writes metrics/name only when `locked = 0` — the in-memory
-- rule is "only update if not locked". EvalDatasetEntryAdded inserts
-- into `eval_dataset_entries` with `ON CONFLICT (dataset_id, entry_id)
-- DO NOTHING` to match the in-memory `already_exists` dedup guard.

CREATE TABLE IF NOT EXISTS eval_datasets (
    dataset_id     TEXT    PRIMARY KEY,
    tenant_id      TEXT    NOT NULL,
    name           TEXT    NOT NULL,
    subject_kind   TEXT    NOT NULL,
    created_at_ms  BIGINT  NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_eval_datasets_tenant
    ON eval_datasets (tenant_id, created_at_ms, dataset_id);

CREATE TABLE IF NOT EXISTS eval_dataset_entries (
    dataset_id    TEXT NOT NULL,
    entry_id      TEXT NOT NULL,
    added_at_ms   BIGINT NOT NULL,
    PRIMARY KEY (dataset_id, entry_id)
);

CREATE TABLE IF NOT EXISTS eval_rubrics (
    rubric_id      TEXT    PRIMARY KEY,
    tenant_id      TEXT    NOT NULL,
    name           TEXT    NOT NULL,
    dimensions_json TEXT   NOT NULL,
    created_at_ms  BIGINT  NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_eval_rubrics_tenant
    ON eval_rubrics (tenant_id, rubric_id);

CREATE TABLE IF NOT EXISTS eval_baselines (
    baseline_id     TEXT    PRIMARY KEY,
    tenant_id       TEXT    NOT NULL,
    name            TEXT    NOT NULL,
    prompt_asset_id TEXT    NOT NULL,
    metrics_json    TEXT    NOT NULL,
    created_at_ms   BIGINT  NOT NULL,
    -- INTEGER 0/1 rather than BOOLEAN so the column shape stays byte-
    -- equal with the sqlite parity schema and the parity harness can
    -- diff raw rows directly. Matches the pattern used by
    -- notifications.delivered and checkpoint_strategies
    -- .trigger_on_task_complete (PR #594).
    locked          INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_eval_baselines_tenant
    ON eval_baselines (tenant_id, baseline_id);
