-- RFC-025 Phase 2b.3 milestone 5: checkpoint_strategies projection.
--
-- Before this migration the `CheckpointStrategySet` event was routed
-- through `log_stub` on pg/sqlite — `CheckpointStrategyReadModel::
-- get_by_run` on pg/sqlite returned `Ok(None)` unconditionally (see
-- the stub impl in pg/adapter.rs / sqlite/adapter.rs). Every restart
-- wiped operator-configured checkpoint cadence on persistent backends.
--
-- PK is `run_id` — one strategy per run, upsert-on-set (last write
-- wins). Events with `run_id = None` are skipped (no key to index on),
-- matching the in-memory applier's `if let Some(run_id) = &e.run_id`
-- guard.
--
-- `trigger_on_task_complete` lands as INTEGER 0/1 rather than BOOLEAN
-- so the column shape is byte-identical between pg + sqlite; the
-- Rust read path converts `!= 0` → bool.

CREATE TABLE IF NOT EXISTS checkpoint_strategies (
    run_id                    TEXT    PRIMARY KEY,
    strategy_id               TEXT    NOT NULL,
    interval_ms               BIGINT  NOT NULL,
    max_checkpoints           INTEGER NOT NULL,
    trigger_on_task_complete  INTEGER NOT NULL,
    set_at_ms                 BIGINT  NOT NULL
);
