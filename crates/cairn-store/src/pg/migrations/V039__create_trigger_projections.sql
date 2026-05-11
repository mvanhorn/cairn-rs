-- RFC-025 Phase 1.5a: trigger + run_template + trigger_fires projection tables.
--
-- Projects the 8 state-carrying Trigger/RunTemplate lifecycle events into
-- durable read models and the 5 audit Trigger* events into a flat
-- append-only audit log so operators can query triggers + templates + fire
-- outcomes after a process restart without walking the event log.
--
-- Before this migration, pg/sqlite backends logged every Trigger*/
-- RunTemplate* event with `log_stub` — the event hit the event_log but no
-- read-model row was written, so `AppState::replay_triggers` had to
-- rebuild `state.triggers` from the full event log on every boot. This
-- migration closes that replay loop (ref RFC-025 §Phase 1.5a / #434).
--
-- Portability — portable SQL only (per `feedback_no_db_specific_features.md`):
-- * No JSONB, no array columns, no partial indexes.
-- * `conditions_json` stores the `Vec<TriggerCondition>` as a TEXT column
--   holding a serde_json blob; the applier calls `serde_json::from_str` on
--   read. Mirror at sqlite/schema.rs.
-- * `plugin_allowlist_json` / `tool_allowlist_json` / `required_fields_json`
--   follow the same TEXT-carrying-JSON-array convention.
--
-- Tables:
-- * `triggers`          — 1 row per trigger, mutated on enable/disable/suspend/resume/delete.
-- * `run_templates`     — 1 row per template, inserted on create, deleted on RunTemplateDeleted.
-- * `trigger_fires`     — append-only audit of every fire attempt + outcome
--                         (fired, skipped, denied, rate_limited, pending_approval).
--                         Classification in the registry stays Ephemeral because no
--                         runtime state depends on reading back individual audit rows;
--                         the table exists for observability + rolling-window counts
--                         used by rate-limit / project-budget checks.
-- See `docs/design/rfcs/RFC-025-runtime-aggregate-backend-abstraction.md`
-- §Phase 1.5a for the migration roadmap.

CREATE TABLE IF NOT EXISTS triggers (
    trigger_id        TEXT    PRIMARY KEY,
    tenant_id         TEXT    NOT NULL,
    workspace_id      TEXT    NOT NULL,
    project_id        TEXT    NOT NULL,
    name              TEXT    NOT NULL,
    description       TEXT,
    signal_type       TEXT    NOT NULL,
    plugin_id         TEXT,
    -- serde-JSON blob of `Vec<TriggerCondition>` as stored on TriggerCreated.
    conditions_json   TEXT    NOT NULL,
    run_template_id   TEXT    NOT NULL,
    -- Enabled | Disabled | Suspended — serialised via enum_to_str helper.
    state             TEXT    NOT NULL,
    -- Only populated when `state = 'disabled'`.
    state_reason      TEXT,
    -- Only populated when `state = 'suspended'`.
    suspension_reason TEXT,
    -- Wall-clock ms since epoch for Disabled/Suspended state transitions.
    state_since       BIGINT,
    max_per_minute    BIGINT  NOT NULL,
    max_burst         BIGINT  NOT NULL,
    max_chain_depth   INTEGER NOT NULL,
    created_by        TEXT    NOT NULL,
    created_at        BIGINT  NOT NULL,
    updated_at        BIGINT  NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_triggers_project
    ON triggers (tenant_id, workspace_id, project_id, trigger_id);
CREATE INDEX IF NOT EXISTS idx_triggers_signal_match
    ON triggers (tenant_id, workspace_id, project_id, signal_type);

CREATE TABLE IF NOT EXISTS run_templates (
    template_id                       TEXT    PRIMARY KEY,
    tenant_id                         TEXT    NOT NULL,
    workspace_id                      TEXT    NOT NULL,
    project_id                        TEXT    NOT NULL,
    name                              TEXT    NOT NULL,
    description                       TEXT,
    -- RunMode serialised via enum_to_str.
    default_mode                      TEXT    NOT NULL,
    system_prompt                     TEXT    NOT NULL,
    initial_user_message              TEXT,
    -- serde-JSON array (Option<Vec<String>>): null for "no restriction".
    plugin_allowlist_json             TEXT,
    tool_allowlist_json               TEXT,
    budget_max_tokens                 BIGINT,
    budget_max_wall_clock_ms          BIGINT,
    budget_max_iterations             BIGINT,
    budget_exploration_budget_share   DOUBLE PRECISION,
    sandbox_hint                      TEXT,
    -- serde-JSON array (Vec<String>): always present (may be empty array).
    required_fields_json              TEXT    NOT NULL,
    created_by                        TEXT    NOT NULL,
    created_at                        BIGINT  NOT NULL,
    updated_at                        BIGINT  NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_run_templates_project
    ON run_templates (tenant_id, workspace_id, project_id, template_id);

-- Append-only audit of every fire-attempt outcome. One row per
-- TriggerFired / TriggerSkipped / TriggerDenied / TriggerRateLimited /
-- TriggerPendingApproval event. Classification in
-- projection_registry.rs stays Ephemeral because this table is purely
-- observability — no runtime state is recovered from it at boot.
--
-- The fire_ledger semantic (prevent duplicate runs on webhook retry / signal
-- replay) is implemented as a SELECT over this table filtered by
-- `(trigger_id, signal_id, outcome='fired')`; the previous in-memory
-- HashMap<(TriggerId, SignalId), u64> is gone.
--
-- Rate-limit (per-trigger per-minute) and project-budget (per-project
-- per-hour) windows are computed as COUNT queries over this table,
-- bounded by `(trigger_id, at_ms > NOW - 60_000)` /
-- `(project, at_ms > NOW - 3_600_000)` using the indexes below. The bound
-- is small enough (max_per_minute <= 10000, max_project_budget = 100
-- default) that the COUNT(*) is cheap even on a busy trigger.
CREATE TABLE IF NOT EXISTS trigger_fires (
    fire_id          BIGSERIAL PRIMARY KEY,
    trigger_id       TEXT   NOT NULL,
    tenant_id        TEXT   NOT NULL,
    workspace_id     TEXT   NOT NULL,
    project_id       TEXT   NOT NULL,
    signal_id        TEXT   NOT NULL,
    -- One of: 'fired', 'skipped', 'denied', 'rate_limited', 'pending_approval'.
    outcome          TEXT   NOT NULL,
    -- Type of signal that attempted to fire the trigger.
    signal_type      TEXT,
    -- Additional context keyed to outcome (reason / decision_id / bucket_capacity /
    -- approval_id / run_id). Stored as serde-JSON blob.
    metadata_json    TEXT,
    -- Milliseconds since UNIX epoch, sourced from the event payload.
    at_ms            BIGINT NOT NULL
);

-- Fire-ledger lookup: does (trigger_id, signal_id) have a 'fired' row?
CREATE INDEX IF NOT EXISTS idx_trigger_fires_ledger
    ON trigger_fires (trigger_id, signal_id, outcome);

-- Rolling-window per-trigger rate limit: COUNT(*) WHERE trigger_id=? AND at_ms > ?.
CREATE INDEX IF NOT EXISTS idx_trigger_fires_rate_limit
    ON trigger_fires (trigger_id, at_ms);

-- Rolling-window per-project budget: COUNT(*) WHERE project=? AND at_ms > ?.
CREATE INDEX IF NOT EXISTS idx_trigger_fires_project_budget
    ON trigger_fires (tenant_id, workspace_id, project_id, at_ms);
