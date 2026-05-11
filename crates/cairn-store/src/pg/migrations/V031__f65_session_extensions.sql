-- F65 PR-2: extend the existing `sessions` projection with goal / budget /
-- attempt-cap columns. The arch doc §3.2 + §7 specify these as ALTER TABLE
-- additions on the existing row rather than a separate `issues` table:
-- Session already owns the goal identity, so adding columns is the minimal
-- schema change that honours "cairn stays thin" (§2 principle 8).
--
-- Portability: every new column uses the pg/sqlite common subset —
-- `TEXT`, `INTEGER`, `BIGINT`, `DOUBLE PRECISION`. No JSONB / arrays /
-- advisory locks / dialect operators (per project memory
-- `feedback_no_db_specific_features`).
--
-- Back-compat: every NOT NULL column carries a DEFAULT so existing rows
-- pick up sane values. `attempts_used` backfills to 1 — the arch doc
-- §6.3 specifies that in-flight runs at migration time are treated as
-- single-attempt legacy root-Runs. Rows with no associated active run
-- stay at 0, which is indistinguishable on replay for terminal sessions.

ALTER TABLE sessions ADD COLUMN IF NOT EXISTS goal_title          TEXT;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS max_attempts        INTEGER NOT NULL DEFAULT 5;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS attempts_used       INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS wall_clock_ms_cap   BIGINT;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS wall_clock_ms_used  BIGINT NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS token_cap           BIGINT;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS tokens_used         BIGINT NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS cost_usd_cap        DOUBLE PRECISION;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS cost_usd_used       DOUBLE PRECISION NOT NULL DEFAULT 0.0;

-- F65 §6.3 back-compat: legacy in-flight sessions have run rows but zero
-- attempts_used. Backfill to 1 for any session that already has at least
-- one root run (parent_run_id IS NULL) in the `runs` projection. Sessions
-- with no runs stay at 0 (no attempts launched yet — identical on replay).
UPDATE sessions s
   SET attempts_used = 1
  WHERE attempts_used = 0
    AND EXISTS (
        SELECT 1 FROM runs r
         WHERE r.session_id = s.session_id
           AND r.parent_run_id IS NULL
    );
