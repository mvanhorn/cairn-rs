use sqlx::PgPool;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::StoreError;
use crate::migrations::AppliedMigration;

/// Postgres-backed migration runner.
///
/// Reads SQL files embedded at compile time from the migrations directory
/// and applies them in version order within a transaction.
pub struct PgMigrationRunner {
    pool: PgPool,
}

/// Embedded migrations from the migrations directory.
/// Each entry is (version, name, sql).
const MIGRATIONS: &[(u32, &str, &str)] = &[
    (
        1,
        "create_event_log",
        include_str!("../../migrations/V001__create_event_log.sql"),
    ),
    (
        2,
        "create_sessions",
        include_str!("../../migrations/V002__create_sessions.sql"),
    ),
    (
        3,
        "create_runs",
        include_str!("../../migrations/V003__create_runs.sql"),
    ),
    (
        4,
        "create_tasks",
        include_str!("../../migrations/V004__create_tasks.sql"),
    ),
    (
        5,
        "create_approvals",
        include_str!("../../migrations/V005__create_approvals.sql"),
    ),
    (
        6,
        "create_checkpoints",
        include_str!("../../migrations/V006__create_checkpoints.sql"),
    ),
    (
        7,
        "create_mailbox",
        include_str!("../../migrations/V007__create_mailbox.sql"),
    ),
    (
        8,
        "create_tool_invocations",
        include_str!("../../migrations/V008__create_tool_invocations.sql"),
    ),
    (
        9,
        "create_migration_history",
        include_str!("../../migrations/V009__create_migration_history.sql"),
    ),
    (
        10,
        "create_documents",
        include_str!("../../migrations/V010__create_documents.sql"),
    ),
    (
        11,
        "create_chunks",
        include_str!("../../migrations/V011__create_chunks.sql"),
    ),
    (
        12,
        "create_graph_nodes",
        include_str!("../../migrations/V012__create_graph_nodes.sql"),
    ),
    (
        13,
        "create_graph_edges",
        include_str!("../../migrations/V013__create_graph_edges.sql"),
    ),
    (
        14,
        "add_chunks_fts",
        include_str!("../../migrations/V014__add_chunks_fts.sql"),
    ),
    (
        15,
        "add_task_approval_titles",
        include_str!("../../migrations/V015__add_task_approval_titles.sql"),
    ),
    (
        16,
        "create_prompt_and_routing_state",
        include_str!("../../migrations/V016__create_prompt_and_routing_state.sql"),
    ),
    (
        17,
        "create_org_hierarchy",
        include_str!("../../migrations/V017__create_org_hierarchy.sql"),
    ),
    (
        18,
        "create_route_policies",
        include_str!("migrations/V018__create_route_policies.sql"),
    ),
    (
        19,
        "create_workspace_members",
        include_str!("migrations/V019__create_workspace_members.sql"),
    ),
    (
        20,
        "add_checkpoint_data_json",
        include_str!("migrations/V020__add_checkpoint_data_json.sql"),
    ),
    (
        21,
        "add_task_session_id",
        include_str!("migrations/V021__add_task_session_id.sql"),
    ),
    (
        22,
        "create_ff_lease_history_cursors",
        include_str!("migrations/V022__create_ff_lease_history_cursors.sql"),
    ),
    (
        23,
        "harden_prompt_schema",
        include_str!("migrations/V023__harden_prompt_schema.sql"),
    ),
    (
        24,
        "create_tool_call_approvals",
        include_str!("migrations/V024__create_tool_call_approvals.sql"),
    ),
    (
        25,
        "create_cost_projections",
        include_str!("migrations/V025__create_cost_projections.sql"),
    ),
    (
        26,
        "create_recovery_and_decision_projections",
        include_str!("migrations/V026__create_recovery_and_decision_projections.sql"),
    ),
    (
        27,
        "add_run_completion_annotation",
        include_str!("migrations/V027__add_run_completion_annotation.sql"),
    ),
    (
        28,
        "create_tool_invocation_cache_hits",
        include_str!("migrations/V028__create_tool_invocation_cache_hits.sql"),
    ),
    (
        29,
        "tool_invocation_args_output",
        include_str!("migrations/V029__tool_invocation_args_output.sql"),
    ),
    (
        30,
        "add_terminal_write_recovery",
        include_str!("migrations/V030__add_terminal_write_recovery.sql"),
    ),
    (
        31,
        "f65_session_extensions",
        include_str!("migrations/V031__f65_session_extensions.sql"),
    ),
    (
        32,
        "f65_orchestrator_projections",
        include_str!("migrations/V032__f65_orchestrator_projections.sql"),
    ),
    (
        33,
        "create_tool_invocation_progress",
        include_str!("migrations/V033__create_tool_invocation_progress.sql"),
    ),
    (
        34,
        "create_eval_runs",
        include_str!("migrations/V034__create_eval_runs.sql"),
    ),
    // RFC-025 Phase 2a.1 milestone 1 (credentials): durable credentials +
    // credential_rotations projection.
    (
        35,
        "create_credentials",
        include_str!("migrations/V035__create_credentials.sql"),
    ),
    // RFC-025 Phase 2a.1 milestone 2 (tenant quotas): tenant_quotas +
    // tenant_quota_violations.
    (
        36,
        "create_tenant_quotas",
        include_str!("migrations/V036__create_tenant_quotas.sql"),
    ),
    // RFC-025 Phase 2a.1 milestone 3 (provider budgets): provider_budgets.
    (
        37,
        "create_provider_budgets",
        include_str!("migrations/V037__create_provider_budgets.sql"),
    ),
    // RFC-025 Phase 2a.1 milestone 4 (licenses).
    (
        38,
        "create_licenses",
        include_str!("migrations/V038__create_licenses.sql"),
    ),
    // RFC-025 Phase 1.5a (triggers): triggers + run_templates +
    // trigger_fires. Renamed from V035 in Phase 3 to resolve a version
    // collision with the credentials migration above. The underlying DDL
    // is unchanged — this is a rename only; the governance + trigger
    // migrations were never wired into the runner before Phase 3.
    (
        39,
        "create_trigger_projections",
        include_str!("migrations/V039__create_trigger_projections.sql"),
    ),
    // RFC-025 Phase 3 (provider bindings + connections): projection
    // tables so operator-configured provider state survives restart.
    (
        40,
        "create_provider_bindings_and_connections",
        include_str!("migrations/V040__create_provider_bindings_and_connections.sql"),
    ),
    // RFC-025 Phase 2b.1: read-model table for `AuditLogEntryRecorded`
    // so audit records survive a pg/sqlite restart (previously `log_stub`
    // on both SQL backends — silent loss on restart). Landed on main in
    // #573 while Phase 2a.2 (#571) was in review.
    (
        41,
        "create_audit_log_entries",
        include_str!("migrations/V041__create_audit_log_entries.sql"),
    ),
    // RFC-025 Phase 2b.1 milestone 2: scheduled_tasks projection so the
    // runtime recovery sweep can rehydrate due-task state on boot.
    (
        42,
        "create_scheduled_tasks",
        include_str!("migrations/V042__create_scheduled_tasks.sql"),
    ),
    // RFC-025 Phase 2b.1 milestone 3: outcomes projection so the
    // evaluator-optimizer feedback loop keeps calibration inputs
    // across restart.
    (
        43,
        "create_outcomes",
        include_str!("migrations/V043__create_outcomes.sql"),
    ),
    // RFC-025 Phase 2b.1 milestone 4: plan_reviews projection (RFC 018)
    // so plan-mode artifacts + operator resolutions survive restart.
    (
        44,
        "create_plan_reviews",
        include_str!("migrations/V044__create_plan_reviews.sql"),
    ),
    // RFC-025 Phase 2a.2 milestone 1 (approval delegations): one audit
    // row per `ApprovalDelegated` event. Renumbered V039 → V041 → V045
    // as main published new migrations during this PR's review cycle
    // (trigger projections at V039; Phase 2b.1 audits/scheduled_tasks/
    // outcomes/plan_reviews at V041-V044).
    (
        45,
        "create_approval_delegations",
        include_str!("migrations/V045__create_approval_delegations.sql"),
    ),
    // RFC-025 Phase 2a.2 milestone 2 (guardrails): guardrail_policies +
    // guardrail_evaluations. Renumbered V040 → V042 → V046.
    (
        46,
        "create_guardrails",
        include_str!("migrations/V046__create_guardrails.sql"),
    ),
    // RFC-025 Phase 2a.2 milestone 3 (retention policies). Renumbered
    // V041 → V043 → V047.
    (
        47,
        "create_retention_policies",
        include_str!("migrations/V047__create_retention_policies.sql"),
    ),
    // RFC-025 Phase 2a.2 milestone 4 (entitlement overrides). Renumbered
    // V042 → V044 → V048.
    (
        48,
        "create_entitlement_overrides",
        include_str!("migrations/V048__create_entitlement_overrides.sql"),
    ),
    // RFC-025 Phase 2b.2 milestone 1: external_workers projection
    // (GAP-005). The four `ExternalWorker*` event variants projected
    // into `InMemoryStore.external_workers` but `log_stub`-ed on pg/
    // sqlite — restart wiped the fleet catalog. `worker_id` is the PK
    // with a tenant_id index for the list-by-tenant hot path.
    // Renumbered V045 → V049 after main published Phase 2a.2
    // (V045-V048) during this PR's review cycle.
    (
        49,
        "create_external_workers",
        include_str!("migrations/V049__create_external_workers.sql"),
    ),
    // Issue #578: add `workspaces.archived_at` on Postgres. The column
    // backs the soft-delete flow shipped in #225 (issue #218). The
    // original migration landed as `V020__workspace_archived_at.sql`
    // under `crates/cairn-store/migrations/` but was never wired into
    // this registry (the V020 slot was taken by
    // `add_checkpoint_data_json`). Fresh Postgres installs therefore
    // ran `UPDATE workspaces SET archived_at = …` against a non-existent
    // column. SQLite sidestepped the bug via an inline pragma-gated
    // ALTER in `sqlite/adapter.rs`. Renumbered V045 → V050 after main
    // published Phase 2a.2 (V045-V048) and Phase 2b.2 (V049) during
    // this PR's review cycle.
    (
        50,
        "add_workspace_archived_at",
        include_str!("migrations/V050__add_workspace_archived_at.sql"),
    ),
    // RFC-025 Phase 2b.2b (PR #593) and 2b.3 (PR #594) together
    // landed eleven migration SQL files on disk — V051..V061 — but
    // failed to register them here, so fresh Postgres installs would
    // hit `relation "resource_shares" does not exist` (etc.) on the
    // first projection write. The `sequential no gaps` contract test
    // only hard-checks V001..V017, so the omission slipped through
    // review on both PRs. Wiring them in here (introduced by PR #595
    // alongside V062 = pause_schedules and extended by this PR's
    // V063..V065).
    (
        51,
        "create_resource_shares",
        include_str!("migrations/V051__create_resource_shares.sql"),
    ),
    (
        52,
        "create_signal_ingestions",
        include_str!("migrations/V052__create_signal_ingestions.sql"),
    ),
    (
        53,
        "create_subagent_spawns",
        include_str!("migrations/V053__create_subagent_spawns.sql"),
    ),
    (
        54,
        "create_user_messages",
        include_str!("migrations/V054__create_user_messages.sql"),
    ),
    (
        55,
        "create_soul_patches",
        include_str!("migrations/V055__create_soul_patches.sql"),
    ),
    (
        56,
        "create_tool_recovery_pauses",
        include_str!("migrations/V056__create_tool_recovery_pauses.sql"),
    ),
    (
        57,
        "create_ingest_jobs",
        include_str!("migrations/V057__create_ingest_jobs.sql"),
    ),
    (
        58,
        "create_default_settings",
        include_str!("migrations/V058__create_default_settings.sql"),
    ),
    (
        59,
        "create_channels",
        include_str!("migrations/V059__create_channels.sql"),
    ),
    (
        60,
        "create_notifications",
        include_str!("migrations/V060__create_notifications.sql"),
    ),
    (
        61,
        "create_checkpoint_strategies",
        include_str!("migrations/V061__create_checkpoint_strategies.sql"),
    ),
    // PR #595 (issue #592): pause_schedules projection. Replaces the
    // event-log walker in `PauseScheduleReadModel::list_due` with
    // an evict-on-resume table — `RunStateChanged(→Paused)` with a
    // `resume_after_ms` INSERTs a row, and any transition away from
    // Paused DELETEs it.
    (
        62,
        "create_pause_schedules",
        include_str!("migrations/V062__create_pause_schedules.sql"),
    ),
    // RFC-025 Phase 2b.4 milestone 2: eval datasets + rubrics +
    // baselines. Renumbered V062 → V063 after PR #595 took V062.
    (
        63,
        "create_eval_projections",
        include_str!("migrations/V063__create_eval_projections.sql"),
    ),
    // RFC-025 Phase 2b.4 milestone 3: operator_profiles projection.
    // Renumbered V063 → V064.
    (
        64,
        "create_operator_profiles",
        include_str!("migrations/V064__create_operator_profiles.sql"),
    ),
    // RFC-025 Phase 2b.4 milestone 4: run_costs + run_cost_alerts.
    // Renumbered V064 → V065.
    (
        65,
        "create_run_cost_projections",
        include_str!("migrations/V065__create_run_cost_projections.sql"),
    ),
    // RFC 026 PR-A0: operator_tenant_roles projection for the
    // tenant-admin role model (blocker for admin-UI series A1..A6).
    (
        66,
        "create_operator_tenant_roles",
        include_str!("migrations/V066__create_operator_tenant_roles.sql"),
    ),
    // Epic #670 G2: extend the subagent_spawns projection with
    // the LLM's delegation intent (goal + role). The G1 emitter
    // writes these columns on every spawn; without this migration
    // the INSERT fails with "column \"goal\" of relation does not
    // exist" on existing pg installs that already have
    // subagent_spawns from V049.
    (
        67,
        "subagent_spawns_goal_role",
        include_str!("migrations/V067__subagent_spawns_goal_role.sql"),
    ),
    // Issue #668: LLM chain-of-thought body projection. Sibling
    // to provider_calls — stores system prompt, messages, response
    // text, and tool calls for every successful LLM call.
    (
        68,
        "create_llm_completions",
        include_str!("migrations/V068__create_llm_completions.sql"),
    ),
    // Issue #670 G4 PR-1b-1: concurrent-descendants counter on runs.
    // Adds `in_flight_descendants BIGINT NOT NULL DEFAULT 0` and
    // `root_run_id TEXT` (nullable) so the subagent driver can bound
    // per-root fan-out atomically via compare-and-increment. Backfills
    // `root_run_id = run_id` on existing root runs.
    (
        69,
        "runs_descendants_counter",
        include_str!("migrations/V069__runs_descendants_counter.sql"),
    ),
    // #670 G5: partial index on `subagent_spawns.child_run_id` for
    // the parent-auto-resume terminal-hook lookup. The SQL file has
    // existed on disk since G5 shipped; it was not wired into the
    // runner registry, meaning fresh pg boots did not apply the
    // index and the terminal-hook lookup degraded to a full-scan
    // once `subagent_spawns` grew. Picking it up here alongside
    // V071 — CREATE INDEX IF NOT EXISTS is idempotent so operators
    // who manually applied V070 out-of-band are unaffected.
    (
        70,
        "subagent_spawns_child_run_id_index",
        include_str!("migrations/V070__subagent_spawns_child_run_id_index.sql"),
    ),
    // Dogfood R7 observability: `tool_defs_json` column on
    // `llm_completions`. See the migration file for the full
    // motivation + back-compat contract.
    (
        71,
        "add_tool_defs_to_llm_completions",
        include_str!("migrations/V071__add_tool_defs_to_llm_completions.sql"),
    ),
    // RFC 030 PR-B: memory-provider projection tables + retroactive
    // knowledge-provider pg wiring + cross-family view + scoring-policy
    // key rename. PR-B1 (RFC 029) shipped the sqlite knowledge schema
    // inline but its top-level `migrations/V018__*.sql` file was never
    // wired into this pg runner — V072 closes the gap defensively
    // (CREATE TABLE IF NOT EXISTS is idempotent for operators who
    // applied V018.sql manually).
    (
        72,
        "create_provider_projections",
        include_str!("migrations/V072__create_provider_projections.sql"),
    ),
    // #791: projection-backed iteration counter on runs. The SQL file
    // shipped in #792 but was never added to this registry (the
    // in-memory + sqlite paths carried the column via their monolithic
    // schema modules). Wiring it here keeps pg operators who boot via
    // the migration runner in lockstep with the reader-side column
    // the applier expects. `CHECK (iteration >= 0)` is idempotent on
    // re-run, and `ADD COLUMN … DEFAULT 0` is a no-op when the column
    // already exists.
    (
        73,
        "runs_iteration_counter",
        include_str!("migrations/V073__runs_iteration_counter.sql"),
    ),
    // RFC 031 PR-B2: `project_agent_roles` projection table. PR-B wired
    // the InMemoryStore applier + HTTP surface + tests; PR-B2 closes
    // the durable-backend gap so pg serves the handler directly rather
    // than via the boot-time replay into `InMemoryStore`. Mirrored in
    // `sqlite/schema.rs` (same table shape).
    (
        74,
        "create_project_agent_roles",
        include_str!("migrations/V074__create_project_agent_roles.sql"),
    ),
];

/// Return the compile-time migration registry as (version, name, sql) triples.
///
/// Used by contract tests to validate the registry without a live database.
pub fn registered_migrations() -> &'static [(u32, &'static str, &'static str)] {
    MIGRATIONS
}

/// Split a SQL script into individual statements, correctly handling:
///
/// - `$$`-dollar-quoted blocks (PL/pgSQL function bodies, etc.)
/// - single-line `--` comments
/// - block `/* … */` comments
///
/// A `;` that appears inside a dollar-quoted block is treated as part of the
/// block rather than a statement terminator.  This avoids the bug where naive
/// `split(';')` breaks `CREATE FUNCTION … AS $$ … ; … $$`.
fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements: Vec<String> = Vec::new();
    let mut current = String::new();
    let chars: Vec<char> = sql.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        // Dollar-quoted string: $tag$ … $tag$
        // Detect opening $…$ tag.
        if chars[i] == '$' {
            // Collect the full dollar-quote tag (e.g. "$$" or "$body$").
            let tag_start = i;
            let mut j = i + 1;
            while j < len && chars[j] != '$' {
                j += 1;
            }
            if j < len {
                // We found a closing '$' for the tag.
                let tag: String = chars[tag_start..=j].iter().collect(); // includes both $
                                                                         // Now scan forward to find the matching closing tag.
                let closing_start = j + 1;
                let tag_chars: Vec<char> = tag.chars().collect();
                let tag_len = tag_chars.len();
                let mut k = closing_start;
                let mut found_close = false;
                while k + tag_len <= len {
                    let slice: Vec<char> = chars[k..k + tag_len].to_vec();
                    if slice == tag_chars {
                        // Found closing tag — include everything up to and
                        // including the closing $tag$ in `current`.
                        let block: String = chars[tag_start..k + tag_len].iter().collect();
                        current.push_str(&block);
                        i = k + tag_len;
                        found_close = true;
                        break;
                    }
                    k += 1;
                }
                if found_close {
                    continue;
                }
                // No closing tag found — treat '$' as a literal character.
            }
            current.push(chars[i]);
            i += 1;
            continue;
        }

        // Single-line comment: -- … \n  — skip entirely, don't add to current.
        if i + 1 < len && chars[i] == '-' && chars[i + 1] == '-' {
            while i < len && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }

        // Block comment: /* … */  — skip entirely.
        if i + 1 < len && chars[i] == '/' && chars[i + 1] == '*' {
            i += 2;
            while i + 1 < len && !(chars[i] == '*' && chars[i + 1] == '/') {
                i += 1;
            }
            if i + 1 < len {
                i += 2; // skip closing */
            }
            continue;
        }

        // Statement terminator.
        if chars[i] == ';' {
            let stmt = current.trim().to_owned();
            if !stmt.is_empty() {
                statements.push(stmt);
            }
            current.clear();
            i += 1;
            continue;
        }

        current.push(chars[i]);
        i += 1;
    }

    // Trailing statement without a final semicolon.
    let stmt = current.trim().to_owned();
    if !stmt.is_empty() {
        statements.push(stmt);
    }

    statements
}

impl PgMigrationRunner {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Ensure the migration history table exists.
    async fn ensure_history_table(&self) -> Result<(), StoreError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS _cairn_migrations (
                version     INTEGER PRIMARY KEY,
                name        TEXT NOT NULL,
                applied_at  BIGINT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Migration(e.to_string()))?;
        Ok(())
    }

    /// List migrations that have already been applied.
    pub async fn applied(&self) -> Result<Vec<AppliedMigration>, StoreError> {
        self.ensure_history_table().await?;

        let rows = sqlx::query_as::<_, (i32, String, i64)>(
            "SELECT version, name, applied_at FROM _cairn_migrations ORDER BY version",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Migration(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|(version, name, applied_at)| AppliedMigration {
                version: version as u32,
                name,
                applied_at: applied_at as u64,
            })
            .collect())
    }

    /// Apply all pending migrations in version order within a transaction.
    pub async fn run_pending(&self) -> Result<Vec<AppliedMigration>, StoreError> {
        self.ensure_history_table().await?;

        let applied = self.applied().await?;
        let max_applied = applied.iter().map(|m| m.version).max().unwrap_or(0);

        let pending: Vec<_> = MIGRATIONS
            .iter()
            .filter(|(v, _, _)| *v > max_applied)
            .collect();

        if pending.is_empty() {
            return Ok(vec![]);
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StoreError::Migration(e.to_string()))?;

        let mut results = Vec::new();

        for (version, name, sql) in &pending {
            // Execute the migration SQL statement by statement.
            // Uses a dollar-quote-aware splitter so that PL/pgSQL function
            // bodies like:
            //   CREATE OR REPLACE FUNCTION ... AS $$
            //   BEGIN ... ; ... END;
            //   $$ LANGUAGE plpgsql;
            // are not incorrectly broken at the semicolons inside $$...$$.
            for statement in split_sql_statements(sql) {
                sqlx::query(&statement)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StoreError::Migration(format!("V{version:03}__{name}: {e}")))?;
            }

            // Record in history.
            sqlx::query(
                "INSERT INTO _cairn_migrations (version, name, applied_at) VALUES ($1, $2, $3)",
            )
            .bind(*version as i32)
            .bind(*name)
            .bind(now as i64)
            .execute(&mut *tx)
            .await
            .map_err(|e| StoreError::Migration(e.to_string()))?;

            results.push(AppliedMigration {
                version: *version,
                name: name.to_string(),
                applied_at: now,
            });
        }

        tx.commit()
            .await
            .map_err(|e| StoreError::Migration(e.to_string()))?;

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::split_sql_statements;

    #[test]
    fn simple_statements_are_split() {
        let sql = "CREATE TABLE a (id INT); CREATE TABLE b (id INT);";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].starts_with("CREATE TABLE a"));
        assert!(stmts[1].starts_with("CREATE TABLE b"));
    }

    #[test]
    fn dollar_quoted_function_is_not_split() {
        let sql = r#"
CREATE OR REPLACE FUNCTION tsv_trigger() RETURNS trigger AS $$
BEGIN
    NEW.tsv := to_tsvector('english', NEW.text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg BEFORE INSERT ON chunks FOR EACH ROW EXECUTE FUNCTION tsv_trigger();
"#;
        let stmts = split_sql_statements(sql);
        // Should be exactly 2: the function and the trigger.
        assert_eq!(stmts.len(), 2, "got: {stmts:?}");
        assert!(
            stmts[0].contains("LANGUAGE plpgsql"),
            "first stmt: {}",
            stmts[0]
        );
        assert!(
            stmts[1].contains("CREATE TRIGGER"),
            "second stmt: {}",
            stmts[1]
        );
    }

    #[test]
    fn single_line_comments_are_handled() {
        let sql = "-- comment\nCREATE TABLE a (id INT); -- trailing\nCREATE TABLE b (id INT);";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn empty_input_returns_empty() {
        assert!(split_sql_statements("").is_empty());
        assert!(split_sql_statements("   -- just a comment\n   ").is_empty());
    }

    #[test]
    fn v014_migration_splits_into_four_statements() {
        // The actual V014 migration content should split into exactly 4 statements:
        // ALTER TABLE, CREATE INDEX, CREATE FUNCTION, CREATE TRIGGER.
        let sql = include_str!("../../migrations/V014__add_chunks_fts.sql");
        let stmts = split_sql_statements(sql);
        assert_eq!(
            stmts.len(),
            4,
            "expected 4 statements, got {}: {stmts:?}",
            stmts.len()
        );
    }
}
