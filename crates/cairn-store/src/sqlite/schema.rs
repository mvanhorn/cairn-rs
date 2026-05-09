/// SQLite schema DDL for local-mode.
///
/// Single string applied in one transaction. Mirrors the Postgres
/// migrations but uses SQLite-compatible types.
pub const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS _cairn_migrations (
    version     INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    applied_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS event_log (
    position       INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id       TEXT NOT NULL UNIQUE,
    source_type    TEXT NOT NULL,
    source_meta    TEXT NOT NULL DEFAULT '{}',
    ownership      TEXT NOT NULL,
    causation_id   TEXT,
    correlation_id TEXT,
    payload        TEXT NOT NULL,
    stored_at      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id   TEXT PRIMARY KEY,
    tenant_id    TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    project_id   TEXT NOT NULL,
    state        TEXT NOT NULL DEFAULT 'open',
    version      INTEGER NOT NULL DEFAULT 1,
    created_at   INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL,
    -- F65 PR-2: goal + per-session budget + attempt cap. Columns mirror
    -- the Postgres V031 migration exactly. SQLite uses INTEGER and REAL
    -- where Postgres uses BIGINT and DOUBLE PRECISION — equivalent storage
    -- shape (see schema_parity test). All NOT NULL columns carry DEFAULTs
    -- so in-place ALTER works on pre-F65 rows.
    goal_title          TEXT,
    max_attempts        INTEGER NOT NULL DEFAULT 5,
    attempts_used       INTEGER NOT NULL DEFAULT 0,
    wall_clock_ms_cap   INTEGER,
    wall_clock_ms_used  INTEGER NOT NULL DEFAULT 0,
    token_cap           INTEGER,
    tokens_used         INTEGER NOT NULL DEFAULT 0,
    cost_usd_cap        REAL,
    cost_usd_used       REAL NOT NULL DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS runs (
    run_id        TEXT PRIMARY KEY,
    session_id    TEXT NOT NULL REFERENCES sessions(session_id),
    parent_run_id TEXT REFERENCES runs(run_id),
    tenant_id     TEXT NOT NULL,
    workspace_id  TEXT NOT NULL,
    project_id    TEXT NOT NULL,
    state         TEXT NOT NULL DEFAULT 'pending',
    failure_class TEXT,
    version       INTEGER NOT NULL DEFAULT 1,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL,
    -- F47 PR2: event-sourced persistence of run completion annotation.
    -- All three columns nullable so pre-F47-PR2 rows (projected before
    -- `RunCompletionAnnotated` existed) stay valid. `completion_verification_json`
    -- is TEXT (no native JSONB in SQLite) — matches the portable
    -- JSON-over-TEXT pattern used by route_policies.rules etc.
    completion_summary            TEXT,
    completion_verification_json  TEXT,
    completion_annotated_at_ms    INTEGER,
    -- F64: nullable JSON sidecar capturing the most recent
    -- terminal-write recovery loop outcome (bridge for FF#371).
    -- Retained for historical audit/backward-compat even after the
    -- upstream fix lands; only the active recovery-loop code becomes
    -- dead at that point — no schema-removal migration is planned.
    terminal_write_recovery_json  TEXT,
    -- #670 G4 PR-1b-1: concurrent-descendants counter for subagent
    -- fan-out. Root runs (parent_run_id IS NULL) use their own
    -- run_id as `root_run_id`; non-root descendants capture the
    -- absolute root at spawn time. `in_flight_descendants` is i64
    -- (underflow becomes a signed audit metric, not a wrapping
    -- catastrophe).
    in_flight_descendants        INTEGER NOT NULL DEFAULT 0,
    root_run_id                  TEXT,
    -- #791: prior-iteration counter incremented on every
    -- waiting_approval → running transition in the RunStateChanged
    -- projection apply. Read by orchestrate.rs to seed
    -- OrchestrationContext.iteration on /orchestrate-resume.
    iteration                    INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS tasks (
    task_id        TEXT PRIMARY KEY,
    tenant_id      TEXT NOT NULL,
    workspace_id   TEXT NOT NULL,
    project_id     TEXT NOT NULL,
    parent_run_id  TEXT REFERENCES runs(run_id),
    parent_task_id TEXT REFERENCES tasks(task_id),
    -- Session binding populated from TaskCreated.session_id at insert time.
    session_id     TEXT REFERENCES sessions(session_id),
    state          TEXT NOT NULL DEFAULT 'queued',
    failure_class  TEXT,
    lease_owner    TEXT,
    lease_expires_at INTEGER,
    lease_version  INTEGER NOT NULL DEFAULT 0,
    title          TEXT,
    description    TEXT,
    version        INTEGER NOT NULL DEFAULT 1,
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_session_id ON tasks(session_id)
    WHERE session_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS approvals (
    approval_id  TEXT PRIMARY KEY,
    tenant_id    TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    project_id   TEXT NOT NULL,
    run_id       TEXT REFERENCES runs(run_id),
    task_id      TEXT REFERENCES tasks(task_id),
    requirement  TEXT NOT NULL DEFAULT 'required',
    decision     TEXT,
    title        TEXT,
    description  TEXT,
    version      INTEGER NOT NULL DEFAULT 1,
    created_at   INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL
);

-- PR BP-2: projection for ToolCall* approval events.
-- `state` is one of pending|approved|rejected|timeout. JSON fields are
-- stored as TEXT since SQLite has no native JSONB.
CREATE TABLE IF NOT EXISTS tool_call_approvals (
    call_id              TEXT PRIMARY KEY,
    session_id           TEXT NOT NULL,
    run_id               TEXT NOT NULL,
    tenant_id            TEXT NOT NULL,
    workspace_id         TEXT NOT NULL,
    project_id           TEXT NOT NULL,
    tool_name            TEXT NOT NULL,
    original_tool_args   TEXT NOT NULL,
    amended_tool_args    TEXT,
    approved_tool_args   TEXT,
    display_summary      TEXT,
    match_policy         TEXT NOT NULL,
    state                TEXT NOT NULL DEFAULT 'pending',
    operator_id          TEXT,
    scope                TEXT,
    reason               TEXT,
    proposed_at_ms       INTEGER NOT NULL,
    approved_at_ms       INTEGER,
    rejected_at_ms       INTEGER,
    last_amended_at_ms   INTEGER,
    version              INTEGER NOT NULL DEFAULT 1,
    created_at           INTEGER NOT NULL,
    updated_at           INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tool_call_approvals_project_state
    ON tool_call_approvals (tenant_id, workspace_id, project_id, state);
CREATE INDEX IF NOT EXISTS idx_tool_call_approvals_session
    ON tool_call_approvals (session_id);
CREATE INDEX IF NOT EXISTS idx_tool_call_approvals_run
    ON tool_call_approvals (run_id);

CREATE TABLE IF NOT EXISTS checkpoints (
    checkpoint_id TEXT PRIMARY KEY,
    tenant_id     TEXT NOT NULL,
    workspace_id  TEXT NOT NULL,
    project_id    TEXT NOT NULL,
    run_id        TEXT NOT NULL REFERENCES runs(run_id),
    disposition   TEXT NOT NULL DEFAULT 'latest',
    version       INTEGER NOT NULL DEFAULT 1,
    created_at    INTEGER NOT NULL,
    -- F65 PR-2: orchestrator-resumable state. Nullable alongside the
    -- RFC 005 fields so existing CheckpointRecorded rows stay valid.
    -- `body` is canonical-serialised JSON stored as TEXT (no JSONB in
    -- SQLite; matches the pg V032 migration contract).
    session_id      TEXT,
    schema_version  INTEGER,
    body            TEXT,
    body_size_bytes INTEGER,
    iteration       INTEGER
);
CREATE INDEX IF NOT EXISTS idx_checkpoints_session_iteration
    ON checkpoints (session_id, iteration)
    WHERE session_id IS NOT NULL;

-- F65 PR-2: workspace_registry — live workspace id → host path mapping.
-- Shape mirrors pg V032; see that file for the column-level rationale.
CREATE TABLE IF NOT EXISTS workspace_registry (
    workspace_id    TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    workspace_scope TEXT NOT NULL,
    project_id      TEXT NOT NULL,
    root_run_id     TEXT NOT NULL,
    fs_root         TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'active',
    created_at      INTEGER NOT NULL,
    reaped_at       INTEGER
);
CREATE INDEX IF NOT EXISTS idx_workspace_registry_root_run
    ON workspace_registry (root_run_id);
CREATE INDEX IF NOT EXISTS idx_workspace_registry_status
    ON workspace_registry (status);

-- F65 PR-2: workspace_snapshots — immutable reflinked workspace trees.
CREATE TABLE IF NOT EXISTS workspace_snapshots (
    snapshot_id         TEXT PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    workspace_scope     TEXT NOT NULL,
    project_id          TEXT NOT NULL,
    session_id          TEXT NOT NULL,
    workspace_id        TEXT NOT NULL,
    parent_snapshot_id  TEXT REFERENCES workspace_snapshots(snapshot_id),
    snapshot_path       TEXT NOT NULL,
    bytes               INTEGER NOT NULL DEFAULT 0,
    reflink_used        INTEGER NOT NULL DEFAULT 0,
    created_at          INTEGER NOT NULL,
    reaped_at           INTEGER
);
CREATE INDEX IF NOT EXISTS idx_workspace_snapshots_session
    ON workspace_snapshots (session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_workspace_snapshots_parent
    ON workspace_snapshots (parent_snapshot_id)
    WHERE parent_snapshot_id IS NOT NULL;

-- F65 PR-2: session_outcomes — one rich outcome per root-Run terminal.
-- `workspace_snapshot_id` is nullable to tolerate legacy runs predating
-- the sandbox (arch-doc §6.3). `termination_reason` carries the short
-- discriminator for cheap index-based filtering;
-- `termination_reason_json` carries the full payload (provider error
-- messages, breaker trip detail, crash metadata). Both live here so
-- readers get the full picture without walking the event log.
CREATE TABLE IF NOT EXISTS session_outcomes (
    root_run_id            TEXT PRIMARY KEY,
    tenant_id              TEXT NOT NULL,
    workspace_scope        TEXT NOT NULL,
    project_id             TEXT NOT NULL,
    session_id             TEXT NOT NULL,
    checkpoint_id          TEXT NOT NULL REFERENCES checkpoints(checkpoint_id),
    workspace_snapshot_id  TEXT REFERENCES workspace_snapshots(snapshot_id),
    termination_reason     TEXT NOT NULL,
    termination_reason_json TEXT,
    compacted_summary      TEXT NOT NULL DEFAULT '',
    next_step_hint         TEXT,
    cost_micros            INTEGER NOT NULL DEFAULT 0,
    created_at             INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_session_outcomes_session
    ON session_outcomes (session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_session_outcomes_termination
    ON session_outcomes (termination_reason);

CREATE TABLE IF NOT EXISTS mailbox_messages (
    message_id   TEXT PRIMARY KEY,
    tenant_id    TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    project_id   TEXT NOT NULL,
    run_id       TEXT REFERENCES runs(run_id),
    task_id      TEXT REFERENCES tasks(task_id),
    version      INTEGER NOT NULL DEFAULT 1,
    created_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tool_invocations (
    invocation_id   TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    workspace_id    TEXT NOT NULL,
    project_id      TEXT NOT NULL,
    session_id      TEXT REFERENCES sessions(session_id),
    run_id          TEXT REFERENCES runs(run_id),
    task_id         TEXT REFERENCES tasks(task_id),
    target          TEXT NOT NULL,
    execution_class TEXT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'requested',
    outcome         TEXT,
    error_message   TEXT,
    version         INTEGER NOT NULL DEFAULT 1,
    requested_at_ms INTEGER NOT NULL,
    started_at_ms   INTEGER,
    finished_at_ms  INTEGER,
    -- F55: structured tool args (TEXT-over-JSON since SQLite has no
    -- native JSONB — mirrors the Postgres V028 migration) and a UTF-8
    -- safe, truncated preview of the tool output for operator
    -- observability. Both nullable for pre-F55 rows.
    args_json       TEXT,
    output_preview  TEXT,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS documents (
    document_id   TEXT PRIMARY KEY,
    source_id     TEXT NOT NULL,
    tenant_id     TEXT NOT NULL,
    workspace_id  TEXT NOT NULL,
    project_id    TEXT NOT NULL,
    source_type   TEXT NOT NULL,
    title         TEXT,
    ingest_status TEXT NOT NULL DEFAULT 'pending',
    version       INTEGER NOT NULL DEFAULT 1,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id     TEXT PRIMARY KEY,
    document_id  TEXT NOT NULL REFERENCES documents(document_id),
    source_id    TEXT NOT NULL,
    tenant_id    TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    project_id   TEXT NOT NULL,
    source_type  TEXT NOT NULL,
    text         TEXT NOT NULL,
    position     INTEGER NOT NULL,
    embedding    BLOB,
    created_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS graph_nodes (
    node_id      TEXT PRIMARY KEY,
    kind         TEXT NOT NULL,
    tenant_id    TEXT,
    workspace_id TEXT,
    project_id   TEXT,
    metadata     TEXT NOT NULL DEFAULT '{}',
    created_at   INTEGER NOT NULL
);

-- FTS5 virtual table for lexical retrieval in local-mode (RFC 003).
-- FTS sync is handled in application code (SqliteDocumentStore.insert_chunks)
-- rather than triggers, because semicolons in trigger bodies break the
-- simple statement-split migration runner.
CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
    chunk_id,
    text
);

CREATE TABLE IF NOT EXISTS graph_edges (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_node_id  TEXT NOT NULL REFERENCES graph_nodes(node_id),
    target_node_id  TEXT NOT NULL REFERENCES graph_nodes(node_id),
    kind            TEXT NOT NULL,
    metadata        TEXT NOT NULL DEFAULT '{}',
    created_at      INTEGER NOT NULL,
    UNIQUE(source_node_id, target_node_id, kind)
);

CREATE TABLE IF NOT EXISTS ff_lease_history_cursors (
    partition_id    TEXT NOT NULL,
    execution_id    TEXT NOT NULL,
    last_stream_id  TEXT NOT NULL,
    updated_at_ms   INTEGER NOT NULL,
    PRIMARY KEY (partition_id, execution_id)
);
CREATE INDEX IF NOT EXISTS idx_ff_lease_history_cursors_partition
    ON ff_lease_history_cursors(partition_id);

-- ── Organization hierarchy (mirrors V017 Postgres migration) ─────────────
-- RFC 008 requires durable tenant/workspace/project reads in team-mode.

CREATE TABLE IF NOT EXISTS tenants (
    tenant_id   TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS workspaces (
    workspace_id TEXT PRIMARY KEY,
    tenant_id    TEXT NOT NULL REFERENCES tenants(tenant_id),
    name         TEXT NOT NULL,
    created_at   INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL,
    archived_at  INTEGER
);
CREATE INDEX IF NOT EXISTS idx_workspaces_tenant
    ON workspaces (tenant_id, created_at, workspace_id);

CREATE TABLE IF NOT EXISTS projects (
    project_id    TEXT PRIMARY KEY,
    workspace_id  TEXT NOT NULL REFERENCES workspaces(workspace_id),
    tenant_id     TEXT NOT NULL REFERENCES tenants(tenant_id),
    name          TEXT NOT NULL,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_projects_workspace
    ON projects (tenant_id, workspace_id, created_at, project_id);

-- ── Workspace membership (mirrors V019 Postgres migration) ──────────────
-- RFC 008 RBAC enforcement.

CREATE TABLE IF NOT EXISTS workspace_members (
    workspace_id TEXT    NOT NULL,
    operator_id  TEXT    NOT NULL,
    role         TEXT    NOT NULL,
    added_at_ms  INTEGER NOT NULL,
    PRIMARY KEY (workspace_id, operator_id)
);
CREATE INDEX IF NOT EXISTS idx_workspace_members_lookup
    ON workspace_members (workspace_id, operator_id);
CREATE INDEX IF NOT EXISTS idx_workspace_members_by_operator
    ON workspace_members (operator_id, workspace_id);

-- ── Prompt registry (mirrors V016 Postgres migration, prompt_* tables) ──

CREATE TABLE IF NOT EXISTS prompt_assets (
    prompt_asset_id TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    workspace_id    TEXT NOT NULL,
    project_id      TEXT NOT NULL,
    name            TEXT NOT NULL,
    kind            TEXT NOT NULL,
    scope           TEXT,
    status          TEXT,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_prompt_assets_project
    ON prompt_assets (tenant_id, workspace_id, project_id);

CREATE TABLE IF NOT EXISTS prompt_versions (
    prompt_version_id TEXT PRIMARY KEY,
    prompt_asset_id   TEXT NOT NULL REFERENCES prompt_assets(prompt_asset_id),
    tenant_id         TEXT NOT NULL,
    workspace_id      TEXT NOT NULL,
    project_id        TEXT NOT NULL,
    -- The projection allocates version_number via COALESCE(MAX, 0) + 1
    -- in a serialized append path. UNIQUE(prompt_asset_id, version_number)
    -- defends against a future concurrent-append path.
    version_number    INTEGER NOT NULL,
    content_hash      TEXT NOT NULL,
    content           TEXT,
    format            TEXT,
    created_by        TEXT,
    created_at        INTEGER NOT NULL,
    UNIQUE(prompt_asset_id, version_number)
);
CREATE INDEX IF NOT EXISTS idx_prompt_versions_asset
    ON prompt_versions (prompt_asset_id, created_at, prompt_version_id);

CREATE TABLE IF NOT EXISTS prompt_releases (
    prompt_release_id TEXT PRIMARY KEY,
    prompt_asset_id   TEXT NOT NULL REFERENCES prompt_assets(prompt_asset_id),
    prompt_version_id TEXT NOT NULL REFERENCES prompt_versions(prompt_version_id),
    tenant_id         TEXT NOT NULL,
    workspace_id      TEXT NOT NULL,
    project_id        TEXT NOT NULL,
    release_tag       TEXT,
    state             TEXT NOT NULL DEFAULT 'draft',
    rollout_target    TEXT,
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_prompt_releases_project
    ON prompt_releases (tenant_id, workspace_id, project_id, created_at, prompt_release_id);
CREATE INDEX IF NOT EXISTS idx_prompt_releases_selector
    ON prompt_releases (tenant_id, workspace_id, project_id, prompt_asset_id, state, rollout_target);

-- ── Routing/provider state (mirrors V016 route_decisions + provider_calls) ──
-- selector_context uses TEXT (JSON string) instead of Postgres JSONB —
-- the column is only written and read wholesale, never queried with
-- JSONB operators, so this is a portable substitution.

CREATE TABLE IF NOT EXISTS route_decisions (
    route_decision_id             TEXT PRIMARY KEY,
    tenant_id                     TEXT NOT NULL,
    workspace_id                  TEXT NOT NULL,
    project_id                    TEXT NOT NULL,
    operation_kind                TEXT NOT NULL,
    route_policy_id               TEXT,
    terminal_route_attempt_id     TEXT,
    selected_provider_binding_id  TEXT,
    selected_route_attempt_id     TEXT,
    selector_context              TEXT,
    attempt_count                 INTEGER NOT NULL DEFAULT 0,
    fallback_used                 INTEGER NOT NULL DEFAULT 0,
    final_status                  TEXT NOT NULL,
    created_at                    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_route_decisions_project
    ON route_decisions (tenant_id, workspace_id, project_id, created_at, route_decision_id);

CREATE TABLE IF NOT EXISTS provider_calls (
    provider_call_id        TEXT PRIMARY KEY,
    route_decision_id       TEXT NOT NULL REFERENCES route_decisions(route_decision_id),
    route_attempt_id        TEXT NOT NULL,
    tenant_id               TEXT NOT NULL,
    workspace_id            TEXT NOT NULL,
    project_id              TEXT NOT NULL,
    operation_kind          TEXT NOT NULL,
    provider_binding_id     TEXT NOT NULL,
    provider_connection_id  TEXT NOT NULL,
    provider_adapter        TEXT NOT NULL DEFAULT '',
    provider_model_id       TEXT NOT NULL,
    task_id                 TEXT,
    run_id                  TEXT,
    prompt_release_id       TEXT,
    fallback_position       INTEGER NOT NULL DEFAULT 0,
    status                  TEXT NOT NULL,
    latency_ms              INTEGER,
    input_tokens            INTEGER,
    output_tokens           INTEGER,
    cost_micros             INTEGER,
    error_class             TEXT,
    raw_error_message       TEXT,
    retry_count             INTEGER NOT NULL DEFAULT 0,
    created_at              INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_provider_calls_decision
    ON provider_calls (route_decision_id, created_at, provider_call_id);

-- ── Route policies (RFC 007 provider routing) ──────────────────────────
-- `rules` is a JSON array of RoutePolicyRule, written and read
-- wholesale (no server-side JSONB operators), so TEXT is a portable
-- substitute for Postgres JSONB.

CREATE TABLE IF NOT EXISTS route_policies (
    policy_id   TEXT PRIMARY KEY,
    tenant_id   TEXT NOT NULL,
    name        TEXT NOT NULL,
    rules       TEXT NOT NULL DEFAULT '[]',
    enabled     INTEGER NOT NULL DEFAULT 1,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_route_policies_tenant
    ON route_policies (tenant_id, created_at, policy_id);
CREATE INDEX IF NOT EXISTS idx_route_policies_tenant_enabled
    ON route_policies (tenant_id, enabled, created_at);

-- ── F29 CD-2: cost rollup projections ───────────────────────────────────
-- Mirrors the Postgres V025 migration. Session, project, and workspace
-- totals are upserted together in the SessionCostUpdated handler so the
-- three tables are guaranteed consistent — project_costs.total_cost_micros
-- equals the sum of session_costs.total_cost_micros for the same
-- (tenant_id, workspace_id, project_id).

CREATE TABLE IF NOT EXISTS session_costs (
    session_id         TEXT PRIMARY KEY,
    tenant_id          TEXT NOT NULL,
    workspace_id       TEXT NOT NULL,
    project_id         TEXT NOT NULL,
    total_cost_micros  INTEGER NOT NULL DEFAULT 0,
    total_tokens_in    INTEGER NOT NULL DEFAULT 0,
    total_tokens_out   INTEGER NOT NULL DEFAULT 0,
    provider_calls     INTEGER NOT NULL DEFAULT 0,
    updated_at_ms      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_session_costs_project
    ON session_costs (tenant_id, workspace_id, project_id);
CREATE INDEX IF NOT EXISTS idx_session_costs_tenant
    ON session_costs (tenant_id, updated_at_ms);

CREATE TABLE IF NOT EXISTS project_costs (
    tenant_id          TEXT NOT NULL,
    workspace_id       TEXT NOT NULL,
    project_id         TEXT NOT NULL,
    total_cost_micros  INTEGER NOT NULL DEFAULT 0,
    total_tokens_in    INTEGER NOT NULL DEFAULT 0,
    total_tokens_out   INTEGER NOT NULL DEFAULT 0,
    provider_calls     INTEGER NOT NULL DEFAULT 0,
    updated_at_ms      INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id, project_id)
);
CREATE INDEX IF NOT EXISTS idx_project_costs_workspace
    ON project_costs (tenant_id, workspace_id);

CREATE TABLE IF NOT EXISTS workspace_costs (
    tenant_id          TEXT NOT NULL,
    workspace_id       TEXT NOT NULL,
    total_cost_micros  INTEGER NOT NULL DEFAULT 0,
    total_tokens_in    INTEGER NOT NULL DEFAULT 0,
    total_tokens_out   INTEGER NOT NULL DEFAULT 0,
    provider_calls     INTEGER NOT NULL DEFAULT 0,
    updated_at_ms      INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id)
);

-- ── F39: recovery + decision projections ────────────────────────────────
-- Each table keys on an event-intrinsic identifier (envelope event_id,
-- decision_id, boot_id, or warmed_at) so projection replay is
-- idempotent: appending the same event twice leaves the table at one
-- row via ON CONFLICT DO NOTHING in the projection applier. Recovery
-- and decision audits therefore survive restart and are queryable
-- without rebuilding from the event_log. The `recovered` column is
-- stored as INTEGER (0/1) because sqlx maps `bool` to INTEGER on
-- SQLite.
CREATE TABLE IF NOT EXISTS recovery_attempts (
    event_id        TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    workspace_id    TEXT NOT NULL,
    project_id      TEXT NOT NULL,
    run_id          TEXT,
    task_id         TEXT,
    reason          TEXT NOT NULL,
    boot_id         TEXT,
    recorded_at_ms  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_recovery_attempts_project
    ON recovery_attempts (tenant_id, workspace_id, project_id, recorded_at_ms);
CREATE INDEX IF NOT EXISTS idx_recovery_attempts_boot
    ON recovery_attempts (boot_id)
    WHERE boot_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_recovery_attempts_run
    ON recovery_attempts (run_id)
    WHERE run_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_recovery_attempts_task
    ON recovery_attempts (task_id)
    WHERE task_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS recovery_completions (
    event_id        TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    workspace_id    TEXT NOT NULL,
    project_id      TEXT NOT NULL,
    run_id          TEXT,
    task_id         TEXT,
    recovered       INTEGER NOT NULL,
    boot_id         TEXT,
    recorded_at_ms  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_recovery_completions_project
    ON recovery_completions (tenant_id, workspace_id, project_id, recorded_at_ms);
CREATE INDEX IF NOT EXISTS idx_recovery_completions_boot
    ON recovery_completions (boot_id)
    WHERE boot_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_recovery_completions_run
    ON recovery_completions (run_id)
    WHERE run_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_recovery_completions_task
    ON recovery_completions (task_id)
    WHERE task_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS recovery_summaries (
    boot_id                     TEXT PRIMARY KEY,
    tenant_id                   TEXT NOT NULL,
    workspace_id                TEXT NOT NULL,
    project_id                  TEXT NOT NULL,
    recovered_runs              INTEGER NOT NULL,
    recovered_tasks             INTEGER NOT NULL,
    recovered_sandboxes         INTEGER NOT NULL,
    preserved_sandboxes         INTEGER NOT NULL,
    orphaned_sandboxes_cleaned  INTEGER NOT NULL,
    decision_cache_entries      INTEGER NOT NULL,
    stale_pending_cleared       INTEGER NOT NULL,
    tool_result_cache_entries   INTEGER NOT NULL,
    memory_projection_entries   INTEGER NOT NULL,
    graph_nodes_recovered       INTEGER NOT NULL,
    graph_edges_recovered       INTEGER NOT NULL,
    webhook_dedup_entries       INTEGER NOT NULL,
    trigger_projections         INTEGER NOT NULL,
    startup_ms                  INTEGER NOT NULL,
    summary_at_ms               INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_recovery_summaries_tenant
    ON recovery_summaries (tenant_id, summary_at_ms);

CREATE TABLE IF NOT EXISTS decision_records (
    decision_id        TEXT PRIMARY KEY,
    tenant_id          TEXT NOT NULL,
    workspace_id       TEXT NOT NULL,
    project_id         TEXT NOT NULL,
    decision_key_json  TEXT NOT NULL,
    outcome_kind       TEXT NOT NULL,
    cached             INTEGER NOT NULL,
    expires_at         INTEGER NOT NULL,
    decided_at         INTEGER NOT NULL,
    event_json         TEXT NOT NULL,
    recorded_at_ms     INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_decision_records_project
    ON decision_records (tenant_id, workspace_id, project_id, decided_at);
CREATE INDEX IF NOT EXISTS idx_decision_records_cached
    ON decision_records (cached, expires_at)
    WHERE cached = 1;

CREATE TABLE IF NOT EXISTS decision_cache_warmups (
    warmed_at            INTEGER PRIMARY KEY,
    cached               INTEGER NOT NULL,
    expired_and_dropped  INTEGER NOT NULL
);

-- F52: durable projection for `ToolInvocationCacheHit` events. Each row
-- records one "the cache served a prior result instead of re-dispatching
-- the tool" occurrence so operators can query cache effectiveness without
-- replaying the event log. Portable across Postgres/SQLite (no JSONB, no
-- arrays, no Postgres-specific types).
CREATE TABLE IF NOT EXISTS tool_invocation_cache_hits (
    invocation_id            TEXT    PRIMARY KEY,
    tenant_id                TEXT    NOT NULL,
    workspace_id             TEXT    NOT NULL,
    project_id               TEXT    NOT NULL,
    run_id                   TEXT,
    task_id                  TEXT,
    tool_name                TEXT    NOT NULL,
    tool_call_id             TEXT    NOT NULL,
    original_completed_at_ms INTEGER NOT NULL,
    served_at_ms             INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tool_invocation_cache_hits_run
    ON tool_invocation_cache_hits (run_id) WHERE run_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tool_invocation_cache_hits_tool_call
    ON tool_invocation_cache_hits (tool_call_id);
CREATE INDEX IF NOT EXISTS idx_tool_invocation_cache_hits_served_at
    ON tool_invocation_cache_hits (served_at_ms);

-- #364: durable projection for `ToolInvocationProgressUpdated` events.
-- One row per `invocation_id`, UPSERTed on every progress update so the
-- HTTP handler can answer tenant-scoped reads in O(1) without scanning
-- the event log. Mirrors the Postgres schema at
-- `crates/cairn-store/src/pg/migrations/V033__create_tool_invocation_progress.sql`.
CREATE TABLE IF NOT EXISTS tool_invocation_progress (
    invocation_id  TEXT    PRIMARY KEY,
    tenant_id      TEXT    NOT NULL,
    workspace_id   TEXT    NOT NULL,
    project_id     TEXT    NOT NULL,
    progress_pct   INTEGER NOT NULL,
    message        TEXT,
    updated_at_ms  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tool_invocation_progress_tenant
    ON tool_invocation_progress (tenant_id, workspace_id, project_id);

-- RFC-025 Phase 1 (milestone 4): eval_runs projection — parity with the
-- pg V034 schema. Portable types only (no JSONB, no arrays): metrics +
-- rubric verdict ride on TEXT columns as serde-JSON blobs. Mirror at
-- `crates/cairn-store/src/pg/migrations/V034__create_eval_runs.sql`.
CREATE TABLE IF NOT EXISTS eval_runs (
    eval_run_id       TEXT    PRIMARY KEY,
    tenant_id         TEXT    NOT NULL,
    workspace_id      TEXT    NOT NULL,
    project_id        TEXT    NOT NULL,
    subject_kind      TEXT    NOT NULL,
    evaluator_type    TEXT    NOT NULL,
    -- sqlx maps Option<bool> onto SQLite INTEGER 0/1 (NULL for unset).
    success           INTEGER,
    error_message     TEXT,
    started_at        INTEGER NOT NULL,
    completed_at      INTEGER,
    archived_at       INTEGER,
    metrics_json      TEXT,
    rubric_score_json TEXT,
    -- RFC-025 milestone 6: run-bound metadata. See pg V034 for rationale.
    dataset_id        TEXT,
    rubric_id         TEXT,
    baseline_id       TEXT,
    prompt_asset_id   TEXT,
    prompt_version_id TEXT,
    prompt_release_id TEXT,
    created_by        TEXT
);

CREATE INDEX IF NOT EXISTS idx_eval_runs_project
    ON eval_runs (tenant_id, workspace_id, project_id, started_at);

-- RFC-025 Phase 1.5a: trigger + run_template + trigger_fires projections.
-- Parity with pg V035; portable types only (no JSONB, no arrays).
-- Conditions + allowlists + required-fields ride on TEXT columns as serde-JSON
-- blobs. Mirror at
-- `crates/cairn-store/src/pg/migrations/V035__create_trigger_projections.sql`.
CREATE TABLE IF NOT EXISTS triggers (
    trigger_id        TEXT    PRIMARY KEY,
    tenant_id         TEXT    NOT NULL,
    workspace_id      TEXT    NOT NULL,
    project_id        TEXT    NOT NULL,
    name              TEXT    NOT NULL,
    description       TEXT,
    signal_type       TEXT    NOT NULL,
    plugin_id         TEXT,
    conditions_json   TEXT    NOT NULL,
    run_template_id   TEXT    NOT NULL,
    state             TEXT    NOT NULL,
    state_reason      TEXT,
    suspension_reason TEXT,
    state_since       INTEGER,
    max_per_minute    INTEGER NOT NULL,
    max_burst         INTEGER NOT NULL,
    max_chain_depth   INTEGER NOT NULL,
    created_by        TEXT    NOT NULL,
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL
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
    default_mode                      TEXT    NOT NULL,
    system_prompt                     TEXT    NOT NULL,
    initial_user_message              TEXT,
    plugin_allowlist_json             TEXT,
    tool_allowlist_json               TEXT,
    budget_max_tokens                 INTEGER,
    budget_max_wall_clock_ms          INTEGER,
    budget_max_iterations             INTEGER,
    budget_exploration_budget_share   REAL,
    sandbox_hint                      TEXT,
    required_fields_json              TEXT    NOT NULL,
    created_by                        TEXT    NOT NULL,
    created_at                        INTEGER NOT NULL,
    updated_at                        INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_run_templates_project
    ON run_templates (tenant_id, workspace_id, project_id, template_id);

CREATE TABLE IF NOT EXISTS trigger_fires (
    fire_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    trigger_id       TEXT    NOT NULL,
    tenant_id        TEXT    NOT NULL,
    workspace_id     TEXT    NOT NULL,
    project_id       TEXT    NOT NULL,
    signal_id        TEXT    NOT NULL,
    outcome          TEXT    NOT NULL,
    signal_type      TEXT,
    metadata_json    TEXT,
    at_ms            INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trigger_fires_ledger
    ON trigger_fires (trigger_id, signal_id, outcome);

CREATE INDEX IF NOT EXISTS idx_trigger_fires_rate_limit
    ON trigger_fires (trigger_id, at_ms);

CREATE INDEX IF NOT EXISTS idx_trigger_fires_project_budget
    ON trigger_fires (tenant_id, workspace_id, project_id, at_ms);

-- RFC-025 Phase 2a.1 (credentials): sqlite parity with pg migration V035.
-- Portable types only (no BYTEA — sqlite takes BLOB). Mirror at
-- `crates/cairn-store/src/pg/migrations/V035__create_credentials.sql`.
CREATE TABLE IF NOT EXISTS credentials (
    credential_id   TEXT    PRIMARY KEY,
    tenant_id       TEXT    NOT NULL,
    name            TEXT    NOT NULL,
    provider_id     TEXT    NOT NULL,
    credential_type TEXT    NOT NULL,
    encrypted_value BLOB    NOT NULL,
    key_id          TEXT,
    key_version     TEXT,
    active          INTEGER NOT NULL DEFAULT 1,
    encrypted_at_ms INTEGER,
    revoked_at_ms   INTEGER,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_credentials_tenant_active
    ON credentials (tenant_id, active);

CREATE TABLE IF NOT EXISTS credential_rotations (
    rotation_id         TEXT    PRIMARY KEY,
    tenant_id           TEXT    NOT NULL,
    credential_id       TEXT    NOT NULL DEFAULT '',
    old_key_id          TEXT    NOT NULL,
    new_key_id          TEXT    NOT NULL,
    rotated_credentials INTEGER NOT NULL DEFAULT 0,
    started_at_ms       INTEGER NOT NULL,
    completed_at_ms     INTEGER,
    rotated_at          INTEGER NOT NULL,
    rotated_by          TEXT
);

CREATE INDEX IF NOT EXISTS idx_credential_rotations_tenant
    ON credential_rotations (tenant_id, rotated_at);

-- RFC-025 Phase 2a.1 milestone 2 (quotas): sqlite parity with pg V036.
CREATE TABLE IF NOT EXISTS tenant_quotas (
    tenant_id              TEXT    PRIMARY KEY,
    max_concurrent_runs    INTEGER NOT NULL,
    max_sessions_per_hour  INTEGER NOT NULL,
    max_tasks_per_run      INTEGER NOT NULL,
    created_at             INTEGER NOT NULL,
    updated_at             INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tenant_quota_violations (
    tenant_id      TEXT    NOT NULL,
    quota_type     TEXT    NOT NULL,
    occurred_at_ms INTEGER NOT NULL,
    current_value  INTEGER NOT NULL,
    limit_value    INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, quota_type, occurred_at_ms)
);

CREATE INDEX IF NOT EXISTS idx_tenant_quota_violations_tenant_time
    ON tenant_quota_violations (tenant_id, occurred_at_ms DESC);

-- RFC-025 Phase 2a.1 milestone 3 (provider budgets): sqlite parity
-- with pg V037.
CREATE TABLE IF NOT EXISTS provider_budgets (
    budget_id               TEXT    PRIMARY KEY,
    tenant_id               TEXT    NOT NULL,
    period                  TEXT    NOT NULL,
    limit_micros            INTEGER NOT NULL,
    alert_threshold_percent INTEGER NOT NULL DEFAULT 80,
    current_spend_micros    INTEGER NOT NULL DEFAULT 0,
    alert_triggered_at_ms   INTEGER,
    exceeded_at_ms          INTEGER,
    created_at              INTEGER NOT NULL,
    updated_at              INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_provider_budgets_tenant_period
    ON provider_budgets (tenant_id, period);

-- RFC-025 Phase 2a.1 milestone 4 (licenses): sqlite parity with pg V038.
CREATE TABLE IF NOT EXISTS licenses (
    tenant_id         TEXT    PRIMARY KEY,
    license_key       TEXT,
    tier              TEXT    NOT NULL,
    entitlements_json TEXT    NOT NULL DEFAULT '[]',
    issued_at         INTEGER NOT NULL,
    expires_at        INTEGER,
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL
);

-- RFC-025 Phase 3 (provider bindings + connections): sqlite parity with
-- pg V040. See that migration for the full rationale; TL;DR: operator
-- provider config must survive restart. Complex fields ride on TEXT /
-- JSON (no JSONB in SQLite, no pg-array types either — keeps the
-- schema-parity contract portable).
CREATE TABLE IF NOT EXISTS provider_connections (
    provider_connection_id  TEXT    PRIMARY KEY,
    tenant_id               TEXT    NOT NULL,
    provider_family         TEXT    NOT NULL,
    adapter_type            TEXT    NOT NULL,
    supported_models_json   TEXT    NOT NULL DEFAULT '[]',
    status                  TEXT    NOT NULL,
    created_at              INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_provider_connections_tenant
    ON provider_connections (tenant_id, created_at, provider_connection_id);

CREATE TABLE IF NOT EXISTS provider_bindings (
    provider_binding_id     TEXT    PRIMARY KEY,
    tenant_id               TEXT    NOT NULL,
    workspace_id            TEXT    NOT NULL,
    project_id              TEXT    NOT NULL,
    provider_connection_id  TEXT    NOT NULL,
    provider_model_id       TEXT    NOT NULL,
    operation_kind          TEXT    NOT NULL,
    settings_json           TEXT    NOT NULL DEFAULT '{}',
    active                  BOOLEAN NOT NULL DEFAULT 1,
    created_at              INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_provider_bindings_project_active
    ON provider_bindings (tenant_id, workspace_id, project_id, active, operation_kind, created_at, provider_binding_id);

CREATE INDEX IF NOT EXISTS idx_provider_bindings_tenant
    ON provider_bindings (tenant_id, created_at, provider_binding_id);

-- RFC-025 Phase 2b.1: audit_log_entries parity table (pg V041). Mirror
-- of the pg migration; TEXT + INTEGER only so the schema-parity test
-- treats both backends as equivalent.
CREATE TABLE IF NOT EXISTS audit_log_entries (
    entry_id        TEXT    PRIMARY KEY,
    tenant_id       TEXT    NOT NULL,
    actor_id        TEXT    NOT NULL,
    action          TEXT    NOT NULL,
    resource_type   TEXT    NOT NULL,
    resource_id     TEXT    NOT NULL,
    outcome         TEXT    NOT NULL,
    metadata_json   TEXT    NOT NULL DEFAULT '{}',
    occurred_at_ms  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_log_tenant
    ON audit_log_entries (tenant_id, occurred_at_ms DESC, entry_id DESC);

CREATE INDEX IF NOT EXISTS idx_audit_log_resource
    ON audit_log_entries (resource_type, resource_id, occurred_at_ms DESC, entry_id DESC);

-- RFC-025 Phase 2b.1 m2: scheduled_tasks parity table (pg V042).
CREATE TABLE IF NOT EXISTS scheduled_tasks (
    scheduled_task_id  TEXT    PRIMARY KEY,
    tenant_id          TEXT    NOT NULL,
    name               TEXT    NOT NULL,
    cron_expression    TEXT    NOT NULL,
    last_run_at        INTEGER,
    next_run_at        INTEGER,
    enabled            BOOLEAN NOT NULL DEFAULT 1,
    created_at         INTEGER NOT NULL,
    updated_at         INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_tenant
    ON scheduled_tasks (tenant_id, created_at, scheduled_task_id);

CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_due
    ON scheduled_tasks (enabled, next_run_at, scheduled_task_id);

-- RFC-025 Phase 2b.1 m3: outcomes parity table (pg V043).
-- SQLite uses REAL for f64; pg uses DOUBLE PRECISION.
CREATE TABLE IF NOT EXISTS outcomes (
    outcome_id             TEXT    PRIMARY KEY,
    run_id                 TEXT    NOT NULL,
    tenant_id              TEXT    NOT NULL,
    workspace_id           TEXT    NOT NULL,
    project_id             TEXT    NOT NULL,
    agent_type             TEXT    NOT NULL,
    predicted_confidence   REAL    NOT NULL,
    actual_outcome         TEXT    NOT NULL,
    recorded_at            INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_outcomes_run
    ON outcomes (run_id, recorded_at, outcome_id);

CREATE INDEX IF NOT EXISTS idx_outcomes_project
    ON outcomes (tenant_id, workspace_id, project_id, recorded_at, outcome_id);

-- RFC-025 Phase 2b.1 m4: plan_reviews parity table (pg V044).
CREATE TABLE IF NOT EXISTS plan_reviews (
    plan_run_id         TEXT    PRIMARY KEY,
    tenant_id           TEXT    NOT NULL,
    workspace_id        TEXT    NOT NULL,
    project_id          TEXT    NOT NULL,
    session_id          TEXT    NOT NULL,
    plan_markdown       TEXT    NOT NULL,
    state               TEXT    NOT NULL,
    proposed_at         INTEGER NOT NULL,
    resolved_by         TEXT,
    resolved_at         INTEGER,
    reviewer_comments   TEXT,
    rejection_reason    TEXT,
    revision_run_id     TEXT
);

CREATE INDEX IF NOT EXISTS idx_plan_reviews_project_state
    ON plan_reviews (tenant_id, workspace_id, project_id, state, proposed_at, plan_run_id);

CREATE INDEX IF NOT EXISTS idx_plan_reviews_session
    ON plan_reviews (session_id, proposed_at, plan_run_id);

-- RFC-025 Phase 2a.2 milestone 1 (approval delegations): sqlite parity
-- with pg V045 (renumbered V039 → V041 → V045 across this PR's review
-- cycle as main published new migrations). Audit trail — one row per
-- `ApprovalDelegated` event. PK is `(approval_id, delegation_id)` so
-- two distinct delegations of the same approval to the same operator
-- within the same millisecond coexist without collapsing (Copilot #571
-- round 4). `delegation_id` is monotonic per emit (see
-- `approval_impl::next_delegation_id`). A supporting index on
-- `(approval_id, delegated_at_ms, delegation_id)` serves the read
-- model's `ORDER BY delegated_at_ms ASC, delegation_id ASC` without a
-- secondary sort pass.
CREATE TABLE IF NOT EXISTS approval_delegations (
    approval_id     TEXT    NOT NULL,
    delegation_id   TEXT    NOT NULL DEFAULT '',
    delegated_to    TEXT    NOT NULL,
    delegated_at_ms INTEGER NOT NULL,
    created_at      INTEGER NOT NULL,
    PRIMARY KEY (approval_id, delegation_id)
);

CREATE INDEX IF NOT EXISTS idx_approval_delegations_read_model
    ON approval_delegations (approval_id, delegated_at_ms, delegation_id);

-- RFC-025 Phase 2a.2 milestone 2 (guardrails): sqlite parity with pg
-- V046 (renumbered V040 → V042 → V046). `rules_json` is a JSON array
-- stored as TEXT (no JSONB in SQLite). `enabled` maps BOOLEAN → INTEGER 0/1.
CREATE TABLE IF NOT EXISTS guardrail_policies (
    policy_id   TEXT    PRIMARY KEY,
    tenant_id   TEXT    NOT NULL,
    name        TEXT    NOT NULL,
    rules_json  TEXT    NOT NULL DEFAULT '[]',
    enabled     INTEGER NOT NULL DEFAULT 1,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_guardrail_policies_tenant
    ON guardrail_policies (tenant_id, policy_id);

CREATE TABLE IF NOT EXISTS guardrail_evaluations (
    policy_id       TEXT    NOT NULL,
    tenant_id       TEXT    NOT NULL,
    subject_type    TEXT    NOT NULL,
    subject_id      TEXT    NOT NULL DEFAULT '',
    action          TEXT    NOT NULL,
    decision        TEXT    NOT NULL,
    reason          TEXT,
    evaluated_at_ms INTEGER NOT NULL,
    created_at      INTEGER NOT NULL,
    -- `tenant_id` leads the PK (parity with pg V046) — see that
    -- migration for the cross-tenant-collision rationale.
    PRIMARY KEY (tenant_id, policy_id, subject_type, subject_id, action, evaluated_at_ms)
);

CREATE INDEX IF NOT EXISTS idx_guardrail_evaluations_tenant_time
    ON guardrail_evaluations (tenant_id, evaluated_at_ms DESC);

-- RFC-025 Phase 2a.2 milestone 3 (retention policies): sqlite parity
-- with pg V047 (renumbered V041 → V043 → V047).
CREATE TABLE IF NOT EXISTS retention_policies (
    tenant_id              TEXT    PRIMARY KEY,
    policy_id              TEXT    NOT NULL,
    full_history_days      INTEGER NOT NULL,
    current_state_days     INTEGER NOT NULL,
    max_events_per_entity  INTEGER,
    created_at             INTEGER NOT NULL,
    updated_at             INTEGER NOT NULL
);

-- RFC-025 Phase 2a.2 milestone 4 (entitlement overrides): sqlite parity
-- with pg V048 (renumbered V042 → V044 → V048). Keyed by (tenant_id, feature)
-- so per-feature overrides upsert latest-wins, mirroring the in-memory
-- HashMap::insert keyed on `{tenant}:{feature}`. `allowed` is INTEGER
-- 0/1 (sqlite's BOOLEAN).
CREATE TABLE IF NOT EXISTS entitlement_overrides (
    tenant_id   TEXT    NOT NULL,
    feature     TEXT    NOT NULL,
    allowed     INTEGER NOT NULL,
    reason      TEXT,
    set_at_ms   INTEGER NOT NULL,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, feature)
);

-- RFC-025 Phase 2b.2 m1: external_workers parity table (pg V049 —
-- renumbered from V045 after Phase 2a.2 landed on main mid-review).
-- Booleans use INTEGER 0/1 on sqlite (no native BOOLEAN). The
-- corresponding pg column is BOOLEAN — sqlx reads both into Rust `bool`
-- transparently.
CREATE TABLE IF NOT EXISTS external_workers (
    worker_id           TEXT    PRIMARY KEY,
    tenant_id           TEXT    NOT NULL,
    display_name        TEXT    NOT NULL,
    status              TEXT    NOT NULL DEFAULT 'active',
    registered_at       INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL,
    last_heartbeat_ms   INTEGER NOT NULL DEFAULT 0,
    is_alive            INTEGER NOT NULL DEFAULT 0,
    active_task_count   INTEGER NOT NULL DEFAULT 0,
    current_task_id     TEXT
);

CREATE INDEX IF NOT EXISTS idx_external_workers_tenant
    ON external_workers (tenant_id, registered_at, worker_id);

-- RFC-025 Phase 2b.2b m1: resource_shares parity table (pg V051).
-- `permissions_json` is a JSON array stored as TEXT (no pg arrays, no
-- JSONB) — portable across backends. `ResourceShareRevoked` DELETEs the
-- row (matches the in-memory applier's `remove`).
CREATE TABLE IF NOT EXISTS resource_shares (
    share_id              TEXT    PRIMARY KEY,
    tenant_id             TEXT    NOT NULL,
    source_workspace_id   TEXT    NOT NULL,
    target_workspace_id   TEXT    NOT NULL,
    resource_type         TEXT    NOT NULL,
    resource_id           TEXT    NOT NULL,
    permissions_json      TEXT    NOT NULL DEFAULT '[]',
    shared_at_ms          INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_resource_shares_target
    ON resource_shares (tenant_id, target_workspace_id, shared_at_ms, share_id);

CREATE INDEX IF NOT EXISTS idx_resource_shares_resource
    ON resource_shares (tenant_id, target_workspace_id, resource_type, resource_id);

-- RFC-025 Phase 2b.2b m2: signal_ingestions parity table (pg V052).
-- `payload_json` is the event payload serialized as TEXT (no JSONB).
-- Before this table, `GET /v1/signals` returned empty across restarts
-- on pg/sqlite because SignalIngested was log_stubbed.
CREATE TABLE IF NOT EXISTS signal_ingestions (
    signal_id      TEXT    PRIMARY KEY,
    tenant_id      TEXT    NOT NULL,
    workspace_id   TEXT    NOT NULL,
    project_id     TEXT    NOT NULL,
    source         TEXT    NOT NULL,
    payload_json   TEXT    NOT NULL DEFAULT 'null',
    timestamp_ms   INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_signal_ingestions_project
    ON signal_ingestions (tenant_id, workspace_id, project_id, timestamp_ms, signal_id);

-- RFC-025 Phase 2b.2b m3: subagent_spawns parity table (pg V053 — RFC 014).
-- #670 G2: `goal` + `role` capture the LLM's delegation context taken
-- from the `spawn_subagent` `ActionProposal` (tool_args["goal"] and
-- tool_name respectively). Defaults mirror the `#[serde(default)]` on
-- the `SubagentSpawned` domain event so a pre-G2 event replayed after
-- the schema change persists as empty strings instead of NULL.
CREATE TABLE IF NOT EXISTS subagent_spawns (
    child_task_id     TEXT    PRIMARY KEY,
    tenant_id         TEXT    NOT NULL,
    workspace_id      TEXT    NOT NULL,
    project_id        TEXT    NOT NULL,
    parent_run_id     TEXT    NOT NULL,
    parent_task_id    TEXT,
    child_session_id  TEXT    NOT NULL,
    child_run_id      TEXT,
    spawned_at_ms     INTEGER NOT NULL,
    goal              TEXT    NOT NULL DEFAULT '',
    role              TEXT    NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_subagent_spawns_parent_run
    ON subagent_spawns (parent_run_id, spawned_at_ms, child_task_id);

CREATE INDEX IF NOT EXISTS idx_subagent_spawns_project
    ON subagent_spawns (tenant_id, workspace_id, project_id, spawned_at_ms, child_task_id);

-- #670 G5: child_run_id lookup index for the parent-auto-resume
-- terminal hook (cairn-fabric `fire_parent_resume_if_child`). Mirrors
-- pg V070. Partial index on NOT NULL because the column is nullable
-- and the terminal-hook lookup never queries for NULL.
CREATE INDEX IF NOT EXISTS idx_subagent_spawns_child_run_id
    ON subagent_spawns (child_run_id)
    WHERE child_run_id IS NOT NULL;

-- Issue #668: LLM chain-of-thought body projection (pg V068).
-- Sibling to provider_calls — stores the full round-trip body
-- (system prompt + messages + response + tool_calls) alongside the
-- metadata on provider_calls / llm_traces. Used by operators to
-- audit what the LLM was shown and what it said.
CREATE TABLE IF NOT EXISTS llm_completions (
    trace_id         TEXT    PRIMARY KEY,
    tenant_id        TEXT    NOT NULL,
    workspace_id     TEXT    NOT NULL,
    project_id       TEXT    NOT NULL,
    session_id       TEXT    NOT NULL,
    run_id           TEXT,
    model_id         TEXT    NOT NULL,
    system_prompt    TEXT    NOT NULL DEFAULT '',
    messages_json    TEXT    NOT NULL DEFAULT '[]',
    response_text    TEXT    NOT NULL DEFAULT '',
    tool_calls_json  TEXT    NOT NULL DEFAULT '[]',
    -- Dogfood R7 observability: tools[] array shipped TO the model.
    tool_defs_json   TEXT    NOT NULL DEFAULT '[]',
    recorded_at_ms   INTEGER NOT NULL,
    created_at       INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_completions_session_time
    ON llm_completions (session_id, recorded_at_ms);

CREATE INDEX IF NOT EXISTS idx_llm_completions_run
    ON llm_completions (run_id)
    WHERE run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_llm_completions_tenant_time
    ON llm_completions (tenant_id, recorded_at_ms);

-- RFC-025 Phase 2b.2b m4: user_messages parity table (pg V054).
CREATE TABLE IF NOT EXISTS user_messages (
    run_id          TEXT    NOT NULL,
    sequence        INTEGER NOT NULL,
    tenant_id       TEXT    NOT NULL,
    workspace_id    TEXT    NOT NULL,
    project_id      TEXT    NOT NULL,
    session_id      TEXT    NOT NULL,
    event_id        TEXT    NOT NULL,
    content         TEXT    NOT NULL DEFAULT '',
    appended_at_ms  INTEGER NOT NULL,
    PRIMARY KEY (run_id, sequence)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_user_messages_event_id
    ON user_messages (event_id);

CREATE INDEX IF NOT EXISTS idx_user_messages_session
    ON user_messages (session_id, appended_at_ms, run_id, sequence);

-- RFC-025 Phase 2b.2b m5: soul_patches parity table (pg V055).
-- `requires_approval` maps BOOLEAN → INTEGER 0/1 on sqlite.
CREATE TABLE IF NOT EXISTS soul_patches (
    patch_id            TEXT    PRIMARY KEY,
    tenant_id           TEXT    NOT NULL,
    workspace_id        TEXT    NOT NULL,
    project_id          TEXT    NOT NULL,
    state               TEXT    NOT NULL DEFAULT 'proposed',
    patch_content       TEXT    NOT NULL DEFAULT '',
    requires_approval   INTEGER NOT NULL DEFAULT 1,
    proposed_at_ms      INTEGER NOT NULL DEFAULT 0,
    applied_at_ms       INTEGER,
    new_version         INTEGER
);

CREATE INDEX IF NOT EXISTS idx_soul_patches_project
    ON soul_patches (tenant_id, workspace_id, project_id, proposed_at_ms DESC, patch_id DESC);

CREATE INDEX IF NOT EXISTS idx_soul_patches_state
    ON soul_patches (state, proposed_at_ms, patch_id);

-- RFC-025 Phase 2b.2b m6: tool_recovery_pauses parity table (pg V056).
-- RFC 020 Track 3 audit: tool calls paused on recovery as DangerousPause.
CREATE TABLE IF NOT EXISTS tool_recovery_pauses (
    tool_call_id   TEXT    PRIMARY KEY,
    tenant_id      TEXT    NOT NULL,
    workspace_id   TEXT    NOT NULL,
    project_id     TEXT    NOT NULL,
    run_id         TEXT    NOT NULL,
    task_id        TEXT,
    tool_name      TEXT    NOT NULL,
    reason         TEXT    NOT NULL,
    paused_at_ms   INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tool_recovery_pauses_run
    ON tool_recovery_pauses (run_id, paused_at_ms, tool_call_id);

CREATE INDEX IF NOT EXISTS idx_tool_recovery_pauses_project
    ON tool_recovery_pauses (tenant_id, workspace_id, project_id, paused_at_ms, tool_call_id);

-- RFC-025 Phase 2b.3 milestone 1: ingest_jobs parity table (pg V057).
-- RFC 003 memory ingest pipeline. Started → state="processing"; Completed
-- flips state to "completed" or "failed" and attaches error_message.
CREATE TABLE IF NOT EXISTS ingest_jobs (
    job_id           TEXT    PRIMARY KEY,
    tenant_id        TEXT    NOT NULL,
    workspace_id     TEXT    NOT NULL,
    project_id       TEXT    NOT NULL,
    source_id        TEXT,
    document_count   INTEGER NOT NULL,
    state            TEXT    NOT NULL,
    error_message    TEXT,
    created_at_ms    INTEGER NOT NULL,
    updated_at_ms    INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ingest_jobs_project
    ON ingest_jobs (tenant_id, workspace_id, project_id, created_at_ms, job_id);

-- RFC-025 Phase 2b.3 milestone 2: default_settings parity table (pg V058).
-- Layered defaults keyed on (scope, scope_id, key) — upsert on Set,
-- hard-delete on Cleared. `value_json` stores `serde_json::Value` as
-- TEXT so pg and sqlite agree byte-for-byte.
CREATE TABLE IF NOT EXISTS default_settings (
    scope         TEXT NOT NULL,
    scope_id      TEXT NOT NULL,
    key           TEXT NOT NULL,
    value_json    TEXT NOT NULL,
    PRIMARY KEY (scope, scope_id, key)
);

-- RFC-025 Phase 2b.3 milestone 3: channels + channel_messages parity
-- tables (pg V059). Two projections for one service: channel lifecycle
-- + message ledger with consume-tracking.
CREATE TABLE IF NOT EXISTS channels (
    channel_id     TEXT    PRIMARY KEY,
    tenant_id      TEXT    NOT NULL,
    workspace_id   TEXT    NOT NULL,
    project_id     TEXT    NOT NULL,
    name           TEXT    NOT NULL,
    capacity       INTEGER NOT NULL,
    created_at_ms  INTEGER NOT NULL,
    updated_at_ms  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_channels_project
    ON channels (tenant_id, workspace_id, project_id, created_at_ms, channel_id);

CREATE TABLE IF NOT EXISTS channel_messages (
    channel_id       TEXT    NOT NULL,
    message_id       TEXT    NOT NULL,
    sender_id        TEXT    NOT NULL,
    body             TEXT    NOT NULL,
    sent_at_ms       INTEGER NOT NULL,
    consumed_by      TEXT,
    consumed_at_ms   INTEGER,
    PRIMARY KEY (channel_id, message_id)
);

CREATE INDEX IF NOT EXISTS idx_channel_messages_channel
    ON channel_messages (channel_id, sent_at_ms, message_id);

-- RFC-025 Phase 2b.3 milestone 4: notification_preferences + notifications
-- parity tables (pg V060). RFC 008 operator notification matrix +
-- delivery audit ledger.
CREATE TABLE IF NOT EXISTS notification_preferences (
    tenant_id        TEXT NOT NULL,
    operator_id      TEXT NOT NULL,
    pref_id          TEXT NOT NULL,
    event_types_json TEXT NOT NULL,
    channels_json    TEXT NOT NULL,
    set_at_ms        INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, operator_id)
);

CREATE TABLE IF NOT EXISTS notifications (
    record_id       TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    operator_id     TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    channel_kind    TEXT NOT NULL,
    channel_target  TEXT NOT NULL,
    payload_json    TEXT NOT NULL,
    sent_at_ms      INTEGER NOT NULL,
    delivered       INTEGER NOT NULL,
    delivery_error  TEXT
);

CREATE INDEX IF NOT EXISTS idx_notifications_tenant
    ON notifications (tenant_id, sent_at_ms, record_id);

CREATE INDEX IF NOT EXISTS idx_notifications_tenant_delivered
    ON notifications (tenant_id, delivered, sent_at_ms, record_id);

-- RFC-025 Phase 2b.3 milestone 5: checkpoint_strategies parity table
-- (pg V061). RFC 002 checkpoint cadence policy: one row per run_id,
-- upsert on set. Events with `run_id = None` are skipped (matches the
-- in-memory `if let Some(run_id)` guard).
CREATE TABLE IF NOT EXISTS checkpoint_strategies (
    run_id                    TEXT    PRIMARY KEY,
    strategy_id               TEXT    NOT NULL,
    interval_ms               INTEGER NOT NULL,
    max_checkpoints           INTEGER NOT NULL,
    trigger_on_task_complete  INTEGER NOT NULL,
    set_at_ms                 INTEGER NOT NULL
);

-- PR #595 (issue #592): pause_schedules parity table (pg V062).
-- Evict-on-resume projection that replaces
-- `PauseScheduleReadModel::list_due`'s event-log walker.
-- `RunStateChanged(→Paused)` with a non-None `resume_after_ms`
-- INSERTs; any transition away from Paused DELETEs by run_id.
CREATE TABLE IF NOT EXISTS pause_schedules (
    run_id         TEXT    PRIMARY KEY,
    tenant_id      TEXT    NOT NULL,
    workspace_id   TEXT    NOT NULL,
    project_id     TEXT    NOT NULL,
    resume_at_ms   INTEGER NOT NULL,
    created_at_ms  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pause_schedules_due
    ON pause_schedules (tenant_id, resume_at_ms, run_id);

-- RFC-025 Phase 2b.4 milestone 2: eval datasets + rubrics + baselines
-- parity tables (pg V063, renumbered from V062 after PR #595 took
-- V062 for pause_schedules). RFC 004 eval catalog. tenant_id is
-- carried as a sentinel empty string because the five Eval* events
-- do not (yet) carry tenant_id; matches the in-memory applier that
-- writes `TenantId::new("")`. See V063 for the detailed rationale.
CREATE TABLE IF NOT EXISTS eval_datasets (
    dataset_id     TEXT    PRIMARY KEY,
    tenant_id      TEXT    NOT NULL,
    name           TEXT    NOT NULL,
    subject_kind   TEXT    NOT NULL,
    created_at_ms  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_eval_datasets_tenant
    ON eval_datasets (tenant_id, created_at_ms, dataset_id);

CREATE TABLE IF NOT EXISTS eval_dataset_entries (
    dataset_id    TEXT NOT NULL,
    entry_id      TEXT NOT NULL,
    added_at_ms   INTEGER NOT NULL,
    PRIMARY KEY (dataset_id, entry_id)
);

CREATE TABLE IF NOT EXISTS eval_rubrics (
    rubric_id       TEXT    PRIMARY KEY,
    tenant_id       TEXT    NOT NULL,
    name            TEXT    NOT NULL,
    dimensions_json TEXT    NOT NULL,
    created_at_ms   INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_eval_rubrics_tenant
    ON eval_rubrics (tenant_id, rubric_id);

CREATE TABLE IF NOT EXISTS eval_baselines (
    baseline_id     TEXT    PRIMARY KEY,
    tenant_id       TEXT    NOT NULL,
    name            TEXT    NOT NULL,
    prompt_asset_id TEXT    NOT NULL,
    metrics_json    TEXT    NOT NULL,
    created_at_ms   INTEGER NOT NULL,
    locked          INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_eval_baselines_tenant
    ON eval_baselines (tenant_id, baseline_id);

-- RFC-025 Phase 2b.4 milestone 3: operator_profiles parity table
-- (pg V064, renumbered from V063). RFC 008 operator directory.
CREATE TABLE IF NOT EXISTS operator_profiles (
    operator_id    TEXT    PRIMARY KEY,
    tenant_id      TEXT    NOT NULL,
    display_name   TEXT    NOT NULL,
    email          TEXT    NOT NULL,
    role           TEXT    NOT NULL,
    created_at_ms  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_operator_profiles_tenant
    ON operator_profiles (tenant_id, operator_id);

-- RFC-025 Phase 2b.4 milestone 4: run_costs + run_cost_alerts parity
-- tables (pg V065, renumbered from V064). Per-run cost rollup + alert
-- ledger.
CREATE TABLE IF NOT EXISTS run_costs (
    run_id             TEXT    PRIMARY KEY,
    total_cost_micros  INTEGER NOT NULL DEFAULT 0,
    total_tokens_in    INTEGER NOT NULL DEFAULT 0,
    total_tokens_out   INTEGER NOT NULL DEFAULT 0,
    provider_calls     INTEGER NOT NULL DEFAULT 0,
    updated_at_ms      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS run_cost_alerts (
    run_id              TEXT    PRIMARY KEY,
    tenant_id           TEXT    NOT NULL,
    threshold_micros    INTEGER NOT NULL,
    triggered_at_ms     INTEGER NOT NULL DEFAULT 0,
    actual_cost_micros  INTEGER NOT NULL DEFAULT 0,
    set_at_ms           INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_run_cost_alerts_tenant_triggered
    ON run_cost_alerts (tenant_id, triggered_at_ms DESC, run_id);

-- RFC 026 PR-A0: operator_tenant_roles parity table (pg V066).
-- N-to-M operator-to-tenant role mapping for the admin-surface UI series.
-- Grants upsert; revocations set `revoked_at_ms` + `revoked_by` (row is
-- retained so the audit trail survives).
CREATE TABLE IF NOT EXISTS operator_tenant_roles (
    tenant_id      TEXT    NOT NULL,
    operator_id    TEXT    NOT NULL,
    role           TEXT    NOT NULL,
    granted_at_ms  INTEGER NOT NULL,
    granted_by     TEXT    NOT NULL,
    revoked_at_ms  INTEGER,
    revoked_by     TEXT,
    PRIMARY KEY (tenant_id, operator_id)
);

CREATE INDEX IF NOT EXISTS idx_operator_tenant_roles_operator
    ON operator_tenant_roles (operator_id);

-- RFC 029 pluggable knowledge providers. Mirrors pg migration V018.
-- See `migrations/V018__create_knowledge_providers.sql` for the pg definition
-- and rationale.
-- RFC 030 added `is_bootstrap INTEGER NOT NULL DEFAULT 0` to match the pg
-- V072 column; fresh sqlite databases get it inline. Pre-RFC-030 sqlite
-- databases only exist in dev (we don't ship a sqlite migration runner
-- today), so no `ALTER TABLE` recovery path is needed here.
CREATE TABLE IF NOT EXISTS project_knowledge_providers (
    tenant_id             TEXT    NOT NULL,
    workspace_id          TEXT    NOT NULL,
    project_id            TEXT    NOT NULL,
    provider_ref          TEXT    NOT NULL,
    kind                  TEXT    NOT NULL,
    at_ms                 INTEGER NOT NULL,
    configured_by         TEXT,
    is_bootstrap          INTEGER NOT NULL DEFAULT 0,
    reason                TEXT,
    prior_snapshot_json   TEXT,
    current_snapshot_json TEXT,
    PRIMARY KEY (tenant_id, workspace_id, project_id, provider_ref, kind, at_ms)
);

CREATE INDEX IF NOT EXISTS idx_project_knowledge_providers_project
    ON project_knowledge_providers (tenant_id, workspace_id, project_id, at_ms);

CREATE TABLE IF NOT EXISTS knowledge_ingest_jobs (
    tenant_id        TEXT    NOT NULL,
    workspace_id     TEXT    NOT NULL,
    project_id       TEXT    NOT NULL,
    document_id      TEXT    NOT NULL,
    provider_ref     TEXT    NOT NULL,
    status           TEXT    NOT NULL,
    source_type      TEXT,
    reason           TEXT,
    submitted_at_ms  INTEGER NOT NULL,
    updated_at_ms    INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id, project_id, document_id)
);

CREATE INDEX IF NOT EXISTS idx_knowledge_ingest_jobs_status
    ON knowledge_ingest_jobs (tenant_id, workspace_id, project_id, status);

-- RFC 030 pluggable memory providers. Mirrors pg migration V019.
-- See `migrations/V019__create_memory_providers.sql` for the pg definition
-- and rationale. `is_bootstrap INTEGER NOT NULL DEFAULT 0` — SQLite has no
-- native BOOLEAN type; we use the 0/1 convention and the pg side stores
-- it as BOOLEAN (the event projection binds the `bool` field either way).
CREATE TABLE IF NOT EXISTS project_memory_providers (
    tenant_id             TEXT    NOT NULL,
    workspace_id          TEXT    NOT NULL,
    project_id            TEXT    NOT NULL,
    provider_ref          TEXT    NOT NULL,
    kind                  TEXT    NOT NULL,
    at_ms                 INTEGER NOT NULL,
    configured_by         TEXT,
    is_bootstrap          INTEGER NOT NULL DEFAULT 0,
    reason                TEXT,
    prior_snapshot_json   TEXT,
    current_snapshot_json TEXT,
    PRIMARY KEY (tenant_id, workspace_id, project_id, provider_ref, kind, at_ms)
);

CREATE INDEX IF NOT EXISTS idx_project_memory_providers_project
    ON project_memory_providers (tenant_id, workspace_id, project_id, at_ms);

CREATE TABLE IF NOT EXISTS memory_ingest_jobs (
    tenant_id        TEXT    NOT NULL,
    workspace_id     TEXT    NOT NULL,
    project_id       TEXT    NOT NULL,
    document_id      TEXT    NOT NULL,
    provider_ref     TEXT    NOT NULL,
    status           TEXT    NOT NULL,
    source_type      TEXT,
    reason           TEXT,
    submitted_at_ms  INTEGER NOT NULL,
    updated_at_ms    INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id, project_id, document_id)
);

CREATE INDEX IF NOT EXISTS idx_memory_ingest_jobs_status
    ON memory_ingest_jobs (tenant_id, workspace_id, project_id, status);

-- Cross-family view unifying knowledge + memory ingest jobs, mirroring the
-- pg V019 view. SQLite supports UNION ALL views; use CREATE VIEW IF NOT
-- EXISTS so the schema bootstrap is idempotent.
CREATE VIEW IF NOT EXISTS v_all_ingest_jobs AS
    SELECT 'knowledge' AS family, tenant_id, workspace_id, project_id,
           document_id, provider_ref, status, source_type, reason,
           submitted_at_ms, updated_at_ms
      FROM knowledge_ingest_jobs
    UNION ALL
    SELECT 'memory' AS family, tenant_id, workspace_id, project_id,
           document_id, provider_ref, status, source_type, reason,
           submitted_at_ms, updated_at_ms
      FROM memory_ingest_jobs;
"#;
