-- F64: event-sourced persistence of terminal-write recovery loop
-- outcomes. The cairn-side bridge for the FF#371 dual-door deadlock
-- replaces F59's short-circuit with a bounded retry loop; this column
-- stores the outcome (attempts, wall_time_ms, recovered | deadlocked)
-- so operators can see on `GET /v1/runs/:id` whether recovery fired
-- and whether it saved the run.
--
-- Nullable TEXT holding serde-JSON (`TerminalRecoveryRecord`). Absent
-- for the hot path. Retained for historical audit/backward-compat
-- even after the bridge retires — once FF#371 lands upstream the
-- active recovery-loop code becomes dead, but writes stop and the
-- column persists so legacy incident rows remain inspectable. No
-- schema-removal migration is planned.
ALTER TABLE runs
    ADD COLUMN IF NOT EXISTS terminal_write_recovery_json TEXT NULL;
