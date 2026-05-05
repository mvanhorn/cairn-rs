-- #670 G5: index on subagent_spawns(child_run_id) for the
-- parent-auto-resume terminal hook lookup path.
--
-- `SubagentSpawnReadModel::get_by_child_run_id` fires on every child
-- subagent terminal transition (`RunService::{complete, fail,
-- cancel}` hooks into `fire_parent_resume_if_child` in
-- cairn-fabric). Without an index the query degrades to a full scan
-- as the table grows. Partial index on NOT NULL because the column
-- is nullable (G3 left a pre-G3 backfill row path where child_run_id
-- is NULL) — the terminal-hook lookup never queries for NULL values,
-- so skipping them keeps the index dense.
CREATE INDEX IF NOT EXISTS idx_subagent_spawns_child_run_id
    ON subagent_spawns (child_run_id)
    WHERE child_run_id IS NOT NULL;
