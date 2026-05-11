-- #670 G4 PR-1b-1: concurrent-descendants counter on runs projection.
--
-- Adds two columns to the existing `runs` table:
--
--   * `in_flight_descendants BIGINT NOT NULL DEFAULT 0` — counts the
--     in-flight (non-terminal) descendant runs spawned under each
--     root run. Only root runs accumulate; non-root descendants
--     always hold 0. Signed (BIGINT) rather than unsigned so an
--     underflow bug surfaces as an audit metric, not a
--     silently-wrapping catastrophe.
--
--   * `root_run_id TEXT` (nullable) — absolute-root pointer so the
--     decrement-on-terminal path doesn't need to re-traverse the
--     `parent_run_id` chain. Set at spawn time to the captured
--     root id.
--
-- The `NOT NULL DEFAULT 0` on the counter is mandatory: without it,
-- existing rows would be NULL, `NULL + 1 = NULL`, and `NULL < :cap`
-- evaluates to NULL (not true) — so every spawn against a pre-V069
-- root would falsely reject with a fanout-cap error.
--
-- The backfill `UPDATE ... WHERE parent_run_id IS NULL AND
-- root_run_id IS NULL` sets `root_run_id = run_id` on existing
-- root runs so the counter-increment path can target them. Pre-
-- V069 CHILD rows intentionally stay `root_run_id = NULL` — they
-- never participated in the counter, so the decrement path's
-- no-op-on-NULL is correct for them.

ALTER TABLE runs
    ADD COLUMN IF NOT EXISTS in_flight_descendants BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS root_run_id TEXT;

UPDATE runs
SET root_run_id = run_id
WHERE parent_run_id IS NULL
  AND root_run_id IS NULL;
