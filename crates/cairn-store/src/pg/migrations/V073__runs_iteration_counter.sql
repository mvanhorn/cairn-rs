-- #791: projection-backed iteration counter on the runs read model.
--
-- Replaces the interim event-log scan PR #790 introduced for #788. The
-- column is incremented in the RunStateChanged projection apply on
-- every (waiting_approval → running) transition, so orchestrate.rs can
-- read run.iteration directly in O(1) instead of forward-scanning the
-- event log on every /orchestrate POST.
--
-- Existing rows backfill to 0. The counter starts accumulating from
-- the next RunStateChanged event after this migration lands; on
-- replays of pre-V073 event logs the apply increments correctly so a
-- full rebuild produces the right value.

ALTER TABLE runs
ADD COLUMN iteration INTEGER NOT NULL DEFAULT 0
CHECK (iteration >= 0);
