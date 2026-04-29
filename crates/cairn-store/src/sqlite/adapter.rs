use async_trait::async_trait;
use cairn_domain::tenancy::ProjectKey;
use cairn_domain::tool_invocation::{ToolInvocationOutcomeKind, ToolInvocationRecord};
use cairn_domain::{
    ApprovalDecision, ApprovalId, ApprovalMatchPolicy, ApprovalRequirement, ApprovalScope,
    CheckpointDisposition, CheckpointId, FailureClass, MailboxMessageId, OperatorId, RunId,
    RunState, SessionId, SessionState, TaskId, TaskState, ToolCallId, ToolInvocationId,
};
use serde::de::DeserializeOwned;
use sqlx::SqlitePool;

use crate::db::{Backend, DbAdapter};
use crate::error::StoreError;
use crate::projections::{
    ApprovalReadModel, ApprovalRecord, CheckpointReadModel, CheckpointRecord,
    CheckpointStrategyReadModel, MailboxReadModel, MailboxRecord, RunReadModel, RunRecord,
    SessionReadModel, SessionRecord, TaskReadModel, TaskRecord, ToolCallApprovalReadModel,
    ToolCallApprovalRecord, ToolCallApprovalState, ToolInvocationReadModel,
};

/// SQLite-backed database adapter for local-mode.
pub struct SqliteAdapter {
    pool: SqlitePool,
}

impl SqliteAdapter {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Create an in-memory SQLite database with schema applied.
    /// Useful for tests.
    pub async fn in_memory() -> Result<Self, StoreError> {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;

        let adapter = Self::new(pool);
        adapter.migrate().await?;
        Ok(adapter)
    }
}

#[async_trait]
impl DbAdapter for SqliteAdapter {
    fn backend(&self) -> Backend {
        Backend::Sqlite
    }

    async fn health_check(&self) -> Result<(), StoreError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        Ok(())
    }

    async fn migrate(&self) -> Result<(), StoreError> {
        // Enable WAL mode for better concurrency.
        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Migration(e.to_string()))?;

        // Enable foreign keys.
        sqlx::query("PRAGMA foreign_keys=ON")
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Migration(e.to_string()))?;

        // Apply the full schema in one shot. This avoids brittle semicolon splitting
        // for FTS and other multi-line DDL.
        sqlx::raw_sql(super::schema::SCHEMA_SQL)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Migration(format!("schema: {e}")))?;

        // Column adds for pre-existing databases. SQLite lacks `ADD COLUMN IF
        // NOT EXISTS`, so we consult `pragma_table_info` to decide whether the
        // column is already present before running ALTER — this avoids relying
        // on brittle substring matches against sqlx error strings that differ
        // across SQLite and sqlx versions / locales.
        let archived_at_exists = sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM pragma_table_info('workspaces') \
               WHERE name = 'archived_at' LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Migration(format!("pragma workspaces.archived_at: {e}")))?
        .is_some();

        if !archived_at_exists {
            let stmt = "ALTER TABLE workspaces ADD COLUMN archived_at INTEGER";
            sqlx::query(stmt)
                .execute(&self.pool)
                .await
                .map_err(|e| StoreError::Migration(format!("{stmt}: {e}")))?;
        }

        // F47 PR2: completion annotation columns. Same pattern as
        // workspaces.archived_at — SQLite has no `ADD COLUMN IF NOT
        // EXISTS`, so consult pragma_table_info first. New installs
        // get the columns from the fresh CREATE TABLE in SCHEMA_SQL;
        // upgrades from pre-F47-PR2 databases pick them up here.
        for (column, kind) in [
            ("completion_summary", "TEXT"),
            ("completion_verification_json", "TEXT"),
            ("completion_annotated_at_ms", "INTEGER"),
            // F64: sidecar for terminal-write recovery loop annotations.
            ("terminal_write_recovery_json", "TEXT"),
        ] {
            let exists = sqlx::query_scalar::<_, i64>(
                "SELECT 1 FROM pragma_table_info('runs') WHERE name = ? LIMIT 1",
            )
            .bind(column)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Migration(format!("pragma runs.{column}: {e}")))?
            .is_some();
            if !exists {
                let stmt = format!("ALTER TABLE runs ADD COLUMN {column} {kind}");
                sqlx::query(&stmt)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| StoreError::Migration(format!("{stmt}: {e}")))?;
            }
        }

        // F55: args_json + output_preview on tool_invocations. Pre-F55
        // databases do not get backfilled — the event log is not replayed
        // at migration time, so legacy rows keep NULL in both columns.
        // Events arriving post-upgrade populate the columns going forward,
        // which is sufficient for the dogfood observability fix.
        for (column, kind) in [("args_json", "TEXT"), ("output_preview", "TEXT")] {
            let exists = sqlx::query_scalar::<_, i64>(
                "SELECT 1 FROM pragma_table_info('tool_invocations') WHERE name = ? LIMIT 1",
            )
            .bind(column)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Migration(format!("pragma tool_invocations.{column}: {e}")))?
            .is_some();
            if !exists {
                let stmt = format!("ALTER TABLE tool_invocations ADD COLUMN {column} {kind}");
                sqlx::query(&stmt)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| StoreError::Migration(format!("{stmt}: {e}")))?;
            }
        }

        // F65 PR-2: add the session-extension columns (goal / budget /
        // attempt cap) to pre-F65 databases. We fetch `pragma_table_info`
        // once and check column presence in memory — gemini-code-assist
        // flagged the per-column pragma query as N+1. The column shapes
        // must match SCHEMA_SQL exactly; a parity test enforces it.
        // Mirrors the pg V031 migration; see schema.rs for the SQLite type
        // mapping.
        let existing_session_cols: std::collections::HashSet<String> =
            sqlx::query_scalar::<_, String>("SELECT name FROM pragma_table_info('sessions')")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| StoreError::Migration(format!("pragma sessions: {e}")))?
                .into_iter()
                .collect();
        for (column, kind) in [
            ("goal_title", "TEXT"),
            ("max_attempts", "INTEGER NOT NULL DEFAULT 5"),
            ("attempts_used", "INTEGER NOT NULL DEFAULT 0"),
            ("wall_clock_ms_cap", "INTEGER"),
            ("wall_clock_ms_used", "INTEGER NOT NULL DEFAULT 0"),
            ("token_cap", "INTEGER"),
            ("tokens_used", "INTEGER NOT NULL DEFAULT 0"),
            ("cost_usd_cap", "REAL"),
            ("cost_usd_used", "REAL NOT NULL DEFAULT 0.0"),
        ] {
            if !existing_session_cols.contains(column) {
                let stmt = format!("ALTER TABLE sessions ADD COLUMN {column} {kind}");
                sqlx::query(&stmt)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| StoreError::Migration(format!("{stmt}: {e}")))?;
            }
        }

        // F65 PR-2 §6.3 back-compat: backfill attempts_used = 1 for any
        // session that already has at least one root run — identical
        // semantics to the Postgres V031 UPDATE. In-flight runs at
        // migration time are treated as single-attempt legacy root-Runs
        // (arch-doc §6.3). Sessions with no runs stay at 0.
        sqlx::query(
            "UPDATE sessions
                SET attempts_used = 1
              WHERE attempts_used = 0
                AND EXISTS (
                    SELECT 1 FROM runs r
                     WHERE r.session_id = sessions.session_id
                       AND r.parent_run_id IS NULL
                )",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Migration(format!("f65 attempts_used backfill: {e}")))?;

        // F65 PR-2: add the orchestrator-resumable extension columns to
        // the existing `checkpoints` table. Same single-pragma-fetch
        // strategy as the session columns above. Shape mirrors SCHEMA_SQL
        // and the pg V032 migration; all nullable so legacy RFC 005
        // checkpoint rows remain valid.
        let existing_checkpoint_cols: std::collections::HashSet<String> =
            sqlx::query_scalar::<_, String>("SELECT name FROM pragma_table_info('checkpoints')")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| StoreError::Migration(format!("pragma checkpoints: {e}")))?
                .into_iter()
                .collect();
        for (column, kind) in [
            ("session_id", "TEXT"),
            ("schema_version", "INTEGER"),
            ("body", "TEXT"),
            ("body_size_bytes", "INTEGER"),
            ("iteration", "INTEGER"),
        ] {
            if !existing_checkpoint_cols.contains(column) {
                let stmt = format!("ALTER TABLE checkpoints ADD COLUMN {column} {kind}");
                sqlx::query(&stmt)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| StoreError::Migration(format!("{stmt}: {e}")))?;
            }
        }

        // F65 PR-2 follow-up: `termination_reason_json` was added after
        // the initial session_outcomes CREATE to carry the full
        // TerminationReason payload alongside the short discriminator.
        // Pre-existing databases may have created the table without
        // the column; add it here using the same pragma pattern.
        let existing_outcome_cols: std::collections::HashSet<String> =
            sqlx::query_scalar::<_, String>(
                "SELECT name FROM pragma_table_info('session_outcomes')",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Migration(format!("pragma session_outcomes: {e}")))?
            .into_iter()
            .collect();
        if !existing_outcome_cols.is_empty()
            && !existing_outcome_cols.contains("termination_reason_json")
        {
            let stmt = "ALTER TABLE session_outcomes ADD COLUMN termination_reason_json TEXT";
            sqlx::query(stmt)
                .execute(&self.pool)
                .await
                .map_err(|e| StoreError::Migration(format!("{stmt}: {e}")))?;
        }

        Ok(())
    }
}

/// F65 PR-2: SQLite-side column list for session reads. Mirrors
/// `SESSION_SELECT_COLS` in the pg adapter; kept as a module-private
/// constant so the three `SessionReadModel` queries don't drift.
const SESSION_SELECT_COLS: &str = "session_id, tenant_id, workspace_id, project_id, state, \
     version, created_at, updated_at, \
     goal_title, max_attempts, attempts_used, \
     wall_clock_ms_cap, wall_clock_ms_used, \
     token_cap, tokens_used, \
     cost_usd_cap, cost_usd_used";

#[async_trait]
impl SessionReadModel for SqliteAdapter {
    async fn get(&self, session_id: &SessionId) -> Result<Option<SessionRecord>, StoreError> {
        let sql = format!("SELECT {SESSION_SELECT_COLS} FROM sessions WHERE session_id = $1");
        let row = sqlx::query_as::<_, SessionRow>(&sql)
            .bind(session_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        row.map(SessionRow::into_record).transpose()
    }

    async fn list_by_project(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<SessionRecord>, StoreError> {
        let sql = format!(
            "SELECT {SESSION_SELECT_COLS} FROM sessions
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3
             ORDER BY created_at ASC, session_id ASC
             LIMIT $4 OFFSET $5"
        );
        let rows = sqlx::query_as::<_, SessionRow>(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        rows.into_iter().map(SessionRow::into_record).collect()
    }

    async fn list_active(&self, limit: usize) -> Result<Vec<SessionRecord>, StoreError> {
        // Tie-break on session_id to match the pg ORDER BY and give a
        // stable cross-backend order when two sessions share the same
        // updated_at (timestamps can collide at millisecond resolution
        // under rapid-fire projection updates).
        let sql = format!(
            "SELECT {SESSION_SELECT_COLS} FROM sessions
             WHERE state = 'open'
             ORDER BY updated_at DESC, session_id ASC
             LIMIT $1"
        );
        let rows = sqlx::query_as::<_, SessionRow>(&sql)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter().map(SessionRow::into_record).collect()
    }
}

#[async_trait]
impl RunReadModel for SqliteAdapter {
    async fn get(&self, run_id: &RunId) -> Result<Option<RunRecord>, StoreError> {
        let row = sqlx::query_as::<_, RunRow>(
            "SELECT run_id, session_id, parent_run_id, tenant_id, workspace_id, project_id,
                    state, failure_class, version, created_at, updated_at,
                    completion_summary, completion_verification_json, completion_annotated_at_ms,
                    terminal_write_recovery_json
             FROM runs
             WHERE run_id = $1",
        )
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        row.map(RunRow::into_record).transpose()
    }

    async fn list_by_session(
        &self,
        session_id: &SessionId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<RunRecord>, StoreError> {
        let rows = sqlx::query_as::<_, RunRow>(
            "SELECT run_id, session_id, parent_run_id, tenant_id, workspace_id, project_id,
                    state, failure_class, version, created_at, updated_at,
                    completion_summary, completion_verification_json, completion_annotated_at_ms,
                    terminal_write_recovery_json
             FROM runs
             WHERE session_id = $1
             ORDER BY created_at ASC, run_id ASC
             LIMIT $2 OFFSET $3",
        )
        .bind(session_id.as_str())
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        rows.into_iter().map(RunRow::into_record).collect()
    }

    async fn any_non_terminal(&self, session_id: &SessionId) -> Result<bool, StoreError> {
        let row: (i64,) = sqlx::query_as(
            "SELECT EXISTS(
                SELECT 1 FROM runs
                WHERE session_id = $1
                  AND state NOT IN ('completed', 'failed', 'canceled')
             )",
        )
        .bind(session_id.as_str())
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(row.0 != 0)
    }

    async fn latest_root_run(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<RunRecord>, StoreError> {
        let row = sqlx::query_as::<_, RunRow>(
            "SELECT run_id, session_id, parent_run_id, tenant_id, workspace_id, project_id,
                    state, failure_class, version, created_at, updated_at,
                    completion_summary, completion_verification_json, completion_annotated_at_ms,
                    terminal_write_recovery_json
             FROM runs
             WHERE session_id = $1 AND parent_run_id IS NULL
             ORDER BY created_at DESC, run_id DESC
             LIMIT 1",
        )
        .bind(session_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        row.map(RunRow::into_record).transpose()
    }

    async fn list_by_state(
        &self,
        state: RunState,
        limit: usize,
    ) -> Result<Vec<RunRecord>, StoreError> {
        let state_str = serde_json::to_value(state)
            .ok()
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_else(|| format!("{state:?}").to_lowercase());
        let rows = sqlx::query_as::<_, RunRow>(
            "SELECT run_id, session_id, parent_run_id, tenant_id, workspace_id, project_id,
                    state, failure_class, version, created_at, updated_at,
                    completion_summary, completion_verification_json, completion_annotated_at_ms,
                    terminal_write_recovery_json
             FROM runs
             WHERE state = $1
             ORDER BY created_at ASC
             LIMIT $2",
        )
        .bind(&state_str)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        rows.into_iter().map(RunRow::into_record).collect()
    }

    async fn list_active_by_project(
        &self,
        project: &ProjectKey,
        limit: usize,
    ) -> Result<Vec<RunRecord>, StoreError> {
        let terminal_states = ["completed", "failed", "canceled", "dead_lettered"];
        let placeholders = terminal_states
            .iter()
            .enumerate()
            .map(|(i, _)| format!("${}", i + 4))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT run_id, session_id, parent_run_id, tenant_id, workspace_id, project_id,
                    state, failure_class, version, created_at, updated_at,
                    completion_summary, completion_verification_json, completion_annotated_at_ms,
                    terminal_write_recovery_json
             FROM runs
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3
               AND state NOT IN ({placeholders})
             ORDER BY created_at ASC
             LIMIT ${}",
            4 + terminal_states.len()
        );
        let mut q = sqlx::query_as::<_, RunRow>(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str());
        for s in &terminal_states {
            q = q.bind(*s);
        }
        q = q.bind(limit as i64);
        q.fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_iter()
            .map(RunRow::into_record)
            .collect()
    }

    async fn list_by_parent_run(
        &self,
        parent_run_id: &RunId,
        limit: usize,
    ) -> Result<Vec<RunRecord>, StoreError> {
        // Served by `idx_runs_parent` (V003__create_runs.sql partial
        // index on `parent_run_id WHERE NOT NULL`).
        let rows = sqlx::query_as::<_, RunRow>(
            "SELECT run_id, session_id, parent_run_id, tenant_id, workspace_id, project_id,
                    state, failure_class, version, created_at, updated_at,
                    completion_summary, completion_verification_json, completion_annotated_at_ms,
                    terminal_write_recovery_json
             FROM runs
             WHERE parent_run_id = $1
             ORDER BY created_at ASC, run_id ASC
             LIMIT $2",
        )
        .bind(parent_run_id.as_str())
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter().map(RunRow::into_record).collect()
    }
}

#[async_trait]
impl TaskReadModel for SqliteAdapter {
    async fn get(&self, task_id: &TaskId) -> Result<Option<TaskRecord>, StoreError> {
        let row = sqlx::query_as::<_, TaskRow>(
            "SELECT task_id, tenant_id, workspace_id, project_id, parent_run_id, parent_task_id, session_id,
                    state, failure_class, lease_owner, lease_expires_at, title, description, version, created_at, updated_at
             FROM tasks
             WHERE task_id = $1",
        )
        .bind(task_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        row.map(TaskRow::into_record).transpose()
    }

    async fn list_by_state(
        &self,
        project: &ProjectKey,
        task_state: TaskState,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, StoreError> {
        let rows = sqlx::query_as::<_, TaskRow>(
            "SELECT task_id, tenant_id, workspace_id, project_id, parent_run_id, parent_task_id, session_id,
                    state, failure_class, lease_owner, lease_expires_at, title, description, version, created_at, updated_at
             FROM tasks
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3 AND state = $4
             ORDER BY created_at ASC, task_id ASC
             LIMIT $5",
        )
        .bind(project.tenant_id.as_str())
        .bind(project.workspace_id.as_str())
        .bind(project.project_id.as_str())
        .bind(enum_string(&task_state)?)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        rows.into_iter().map(TaskRow::into_record).collect()
    }

    async fn list_expired_leases(
        &self,
        now: u64,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, StoreError> {
        let rows = sqlx::query_as::<_, TaskRow>(
            "SELECT task_id, tenant_id, workspace_id, project_id, parent_run_id, parent_task_id, session_id,
                    state, failure_class, lease_owner, lease_expires_at, title, description, version, created_at, updated_at
             FROM tasks
             WHERE state = 'leased'
               AND lease_expires_at IS NOT NULL
               AND lease_expires_at < $1
             ORDER BY lease_expires_at ASC, task_id ASC
             LIMIT $2",
        )
        .bind(now as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        rows.into_iter().map(TaskRow::into_record).collect()
    }

    async fn list_by_parent_run(
        &self,
        parent_run_id: &RunId,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, StoreError> {
        let rows = sqlx::query_as::<_, TaskRow>(
            "SELECT task_id, tenant_id, workspace_id, project_id, parent_run_id, parent_task_id, session_id,
                    state, failure_class, lease_owner, lease_expires_at, title, description, version, created_at, updated_at
             FROM tasks
             WHERE parent_run_id = $1
             ORDER BY created_at ASC, task_id ASC
             LIMIT $2",
        )
        .bind(parent_run_id.as_str())
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        rows.into_iter().map(TaskRow::into_record).collect()
    }

    async fn any_non_terminal_children(&self, parent_run_id: &RunId) -> Result<bool, StoreError> {
        let row: (i64,) = sqlx::query_as(
            "SELECT EXISTS(
                SELECT 1 FROM tasks
                WHERE parent_run_id = $1
                  AND state NOT IN ('completed', 'failed', 'canceled', 'dead_lettered')
            ) AS has_non_terminal",
        )
        .bind(parent_run_id.as_str())
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(row.0 != 0)
    }
}

#[async_trait]
impl ApprovalReadModel for SqliteAdapter {
    async fn get(&self, approval_id: &ApprovalId) -> Result<Option<ApprovalRecord>, StoreError> {
        let row = sqlx::query_as::<_, ApprovalRow>(
            "SELECT approval_id, tenant_id, workspace_id, project_id, run_id, task_id,
                    requirement, decision, title, description, version, created_at, updated_at
             FROM approvals
             WHERE approval_id = $1",
        )
        .bind(approval_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        row.map(ApprovalRow::into_record).transpose()
    }

    async fn list_pending(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ApprovalRecord>, StoreError> {
        let rows = sqlx::query_as::<_, ApprovalRow>(
            "SELECT approval_id, tenant_id, workspace_id, project_id, run_id, task_id,
                    requirement, decision, title, description, version, created_at, updated_at
             FROM approvals
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3
               AND decision IS NULL
             ORDER BY created_at ASC, approval_id ASC
             LIMIT $4 OFFSET $5",
        )
        .bind(project.tenant_id.as_str())
        .bind(project.workspace_id.as_str())
        .bind(project.project_id.as_str())
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        rows.into_iter().map(ApprovalRow::into_record).collect()
    }

    async fn list_all(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ApprovalRecord>, StoreError> {
        let rows = sqlx::query_as::<_, ApprovalRow>(
            "SELECT approval_id, tenant_id, workspace_id, project_id, run_id, task_id,
                    requirement, decision, title, description, version, created_at, updated_at
             FROM approvals
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3
             ORDER BY created_at ASC, approval_id ASC
             LIMIT $4 OFFSET $5",
        )
        .bind(project.tenant_id.as_str())
        .bind(project.workspace_id.as_str())
        .bind(project.project_id.as_str())
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        rows.into_iter().map(ApprovalRow::into_record).collect()
    }

    async fn has_pending_for_run(&self, run_id: &RunId) -> Result<bool, StoreError> {
        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM approvals WHERE run_id = $1 AND decision IS NULL")
                .bind(run_id.as_str())
                .fetch_one(&self.pool)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(count.0 > 0)
    }
}

#[async_trait]
impl CheckpointStrategyReadModel for SqliteAdapter {
    async fn get_by_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<cairn_domain::CheckpointStrategy>, StoreError> {
        // Checkpoint strategies are stored as events; query the strategies table if it exists,
        // otherwise return None (strategy not configured).
        let _ = run_id;
        Ok(None)
    }
}

#[async_trait]
impl CheckpointReadModel for SqliteAdapter {
    async fn get(
        &self,
        checkpoint_id: &CheckpointId,
    ) -> Result<Option<CheckpointRecord>, StoreError> {
        let row = sqlx::query_as::<_, CheckpointRow>(
            "SELECT checkpoint_id, tenant_id, workspace_id, project_id, run_id, disposition, version, created_at
             FROM checkpoints
             WHERE checkpoint_id = $1",
        )
        .bind(checkpoint_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        row.map(CheckpointRow::into_record).transpose()
    }

    async fn latest_for_run(&self, run_id: &RunId) -> Result<Option<CheckpointRecord>, StoreError> {
        let row = sqlx::query_as::<_, CheckpointRow>(
            "SELECT checkpoint_id, tenant_id, workspace_id, project_id, run_id, disposition, version, created_at
             FROM checkpoints
             WHERE run_id = $1 AND disposition = 'latest'
             ORDER BY created_at DESC, checkpoint_id DESC
             LIMIT 1",
        )
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        row.map(CheckpointRow::into_record).transpose()
    }

    async fn list_by_run(
        &self,
        run_id: &RunId,
        limit: usize,
    ) -> Result<Vec<CheckpointRecord>, StoreError> {
        let rows = sqlx::query_as::<_, CheckpointRow>(
            "SELECT checkpoint_id, tenant_id, workspace_id, project_id, run_id, disposition, version, created_at
             FROM checkpoints
             WHERE run_id = $1
             ORDER BY created_at DESC, checkpoint_id DESC
             LIMIT $2",
        )
        .bind(run_id.as_str())
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        rows.into_iter().map(CheckpointRow::into_record).collect()
    }
}

#[async_trait]
impl MailboxReadModel for SqliteAdapter {
    async fn get(
        &self,
        message_id: &MailboxMessageId,
    ) -> Result<Option<MailboxRecord>, StoreError> {
        let row = sqlx::query_as::<_, MailboxRow>(
            "SELECT message_id, tenant_id, workspace_id, project_id, run_id, task_id, version, created_at
             FROM mailbox_messages
             WHERE message_id = $1",
        )
        .bind(message_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        row.map(MailboxRow::into_record).transpose()
    }

    async fn list_by_run(
        &self,
        run_id: &RunId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<MailboxRecord>, StoreError> {
        let rows = sqlx::query_as::<_, MailboxRow>(
            "SELECT message_id, tenant_id, workspace_id, project_id, run_id, task_id, version, created_at
             FROM mailbox_messages
             WHERE run_id = $1
             ORDER BY created_at ASC, message_id ASC
             LIMIT $2 OFFSET $3",
        )
        .bind(run_id.as_str())
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        rows.into_iter().map(MailboxRow::into_record).collect()
    }

    async fn list_by_task(
        &self,
        task_id: &TaskId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<MailboxRecord>, StoreError> {
        let rows = sqlx::query_as::<_, MailboxRow>(
            "SELECT message_id, tenant_id, workspace_id, project_id, run_id, task_id, version, created_at
             FROM mailbox_messages
             WHERE task_id = $1
             ORDER BY created_at ASC, message_id ASC
             LIMIT $2 OFFSET $3",
        )
        .bind(task_id.as_str())
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        rows.into_iter().map(MailboxRow::into_record).collect()
    }

    async fn list_pending(
        &self,
        _now_ms: u64,
        _limit: usize,
    ) -> Result<Vec<MailboxRecord>, StoreError> {
        // SQLite migration for deliver_at_ms column is out of scope; stub returns empty.
        Ok(vec![])
    }
}

#[async_trait]
impl ToolInvocationReadModel for SqliteAdapter {
    async fn get(
        &self,
        invocation_id: &ToolInvocationId,
    ) -> Result<Option<ToolInvocationRecord>, StoreError> {
        let row = sqlx::query_as::<_, ToolInvocationRow>(
            "SELECT invocation_id, tenant_id, workspace_id, project_id, session_id, run_id, task_id,
                    target, execution_class, state, outcome, error_message, version,
                    requested_at_ms, started_at_ms, finished_at_ms, args_json, output_preview
             FROM tool_invocations
             WHERE invocation_id = $1",
        )
        .bind(invocation_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        row.map(ToolInvocationRow::into_record).transpose()
    }

    async fn list_by_run(
        &self,
        run_id: &RunId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ToolInvocationRecord>, StoreError> {
        let rows = sqlx::query_as::<_, ToolInvocationRow>(
            "SELECT invocation_id, tenant_id, workspace_id, project_id, session_id, run_id, task_id,
                    target, execution_class, state, outcome, error_message, version,
                    requested_at_ms, started_at_ms, finished_at_ms, args_json, output_preview
             FROM tool_invocations
             WHERE run_id = $1
             ORDER BY requested_at_ms ASC, invocation_id ASC
             LIMIT $2 OFFSET $3",
        )
        .bind(run_id.as_str())
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        rows.into_iter()
            .map(ToolInvocationRow::into_record)
            .collect()
    }
}

#[async_trait]
impl crate::projections::ToolInvocationProgressReadModel for SqliteAdapter {
    async fn get(
        &self,
        invocation_id: &ToolInvocationId,
    ) -> Result<Option<crate::projections::ToolInvocationProgressRecord>, StoreError> {
        let row: Option<(String, String, String, i64, Option<String>, i64)> = sqlx::query_as(
            "SELECT tenant_id, workspace_id, project_id,
                    progress_pct, message, updated_at_ms
             FROM tool_invocation_progress
             WHERE invocation_id = ?",
        )
        .bind(invocation_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        // Checked conversions from the signed SQLite integer columns.
        // Writes always originate from `u8` / `u64`, but silently
        // wrapping a corrupt/oversized row via `as u8` / `as u64` would
        // hand the handler a garbage record. Flagged on PR #537 by
        // Copilot. Mirrors the equivalent check in `pg/adapter.rs`.
        row.map(
            |(tenant, ws, project, pct, msg, updated)| -> Result<_, StoreError> {
                let progress_pct = u8::try_from(pct).map_err(|_| {
                    StoreError::Internal(format!(
                        "tool_invocation_progress.progress_pct out of u8 range for {}: {pct}",
                        invocation_id.as_str(),
                    ))
                })?;
                let updated_at_ms = u64::try_from(updated).map_err(|_| {
                    StoreError::Internal(format!(
                        "tool_invocation_progress.updated_at_ms out of u64 range for {}: {updated}",
                        invocation_id.as_str(),
                    ))
                })?;
                Ok(crate::projections::ToolInvocationProgressRecord {
                    invocation_id: invocation_id.clone(),
                    project: cairn_domain::ProjectKey::new(
                        tenant.as_str(),
                        ws.as_str(),
                        project.as_str(),
                    ),
                    progress_pct,
                    message: msg,
                    updated_at_ms,
                })
            },
        )
        .transpose()
    }
}

#[derive(sqlx::FromRow)]
struct ToolInvocationRow {
    invocation_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    session_id: Option<String>,
    run_id: Option<String>,
    task_id: Option<String>,
    target: String,
    execution_class: String,
    state: String,
    outcome: Option<String>,
    error_message: Option<String>,
    version: i64,
    requested_at_ms: i64,
    started_at_ms: Option<i64>,
    finished_at_ms: Option<i64>,
    // F55: persisted tool args + output preview. SQLite has no native
    // JSONB, so args are stored as a JSON string and parsed app-side
    // into `serde_json::Value` — matches the portable-TEXT pattern
    // used elsewhere in this crate.
    args_json: Option<String>,
    output_preview: Option<String>,
}

impl ToolInvocationRow {
    fn into_record(self) -> Result<ToolInvocationRecord, StoreError> {
        let project = project_key_from_parts(self.tenant_id, self.workspace_id, self.project_id);
        let args_json = match self.args_json.as_deref() {
            Some(text) => Some(
                serde_json::from_str(text).map_err(|e| StoreError::Serialization(e.to_string()))?,
            ),
            None => None,
        };
        Ok(ToolInvocationRecord {
            invocation_id: ToolInvocationId::new(self.invocation_id),
            project,
            session_id: self.session_id.map(SessionId::new),
            run_id: self.run_id.map(RunId::new),
            task_id: self.task_id.map(TaskId::new),
            target: serde_json::from_str(&self.target)
                .map_err(|e| StoreError::Serialization(e.to_string()))?,
            execution_class: parse_string_enum(&self.execution_class)?,
            prompt_release_id: None,
            state: parse_string_enum(&self.state)?,
            version: self.version as u64,
            requested_at_ms: self.requested_at_ms as u64,
            started_at_ms: self.started_at_ms.map(|value| value as u64),
            finished_at_ms: self.finished_at_ms.map(|value| value as u64),
            outcome: self
                .outcome
                .as_deref()
                .map(parse_string_enum::<ToolInvocationOutcomeKind>)
                .transpose()?,
            error_message: self.error_message,
            args_json,
            output_preview: self.output_preview,
        })
    }
}

fn parse_string_enum<T: DeserializeOwned>(raw: &str) -> Result<T, StoreError> {
    serde_json::from_value(serde_json::Value::String(raw.to_owned()))
        .map_err(|e| StoreError::Serialization(e.to_string()))
}

fn enum_string<T: serde::Serialize>(value: &T) -> Result<String, StoreError> {
    serde_json::to_value(value)
        .map_err(|e| StoreError::Serialization(e.to_string()))?
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| StoreError::Serialization("enum did not serialize to string".to_owned()))
}

#[derive(sqlx::FromRow)]
struct SessionRow {
    session_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    state: String,
    version: i64,
    created_at: i64,
    updated_at: i64,
    // F65 PR-2: session-extension columns. SQLite stores the same logical
    // values the pg V031 migration uses — integers and REAL instead of
    // BIGINT + DOUBLE PRECISION. All carry DEFAULTs at the schema level,
    // so rows from pre-F65 databases project cleanly via the pragma-driven
    // ALTER path in `migrate`.
    goal_title: Option<String>,
    max_attempts: i64,
    attempts_used: i64,
    wall_clock_ms_cap: Option<i64>,
    wall_clock_ms_used: i64,
    token_cap: Option<i64>,
    tokens_used: i64,
    cost_usd_cap: Option<f64>,
    cost_usd_used: f64,
}

impl SessionRow {
    fn into_record(self) -> Result<SessionRecord, StoreError> {
        let project = project_key_from_parts(self.tenant_id, self.workspace_id, self.project_id);
        let budget = if self.wall_clock_ms_cap.is_some()
            || self.token_cap.is_some()
            || self.cost_usd_cap.is_some()
        {
            Some(cairn_domain::IssueBudget {
                max_tokens: self.token_cap.map(|v| v.max(0) as u64),
                max_cost_micros: self
                    .cost_usd_cap
                    .map(|usd| (usd.max(0.0) * 1_000_000.0).round() as u64),
                max_wall_seconds: self.wall_clock_ms_cap.map(|ms| (ms.max(0) as u64) / 1_000),
            })
        } else {
            None
        };
        let _ = (
            self.wall_clock_ms_used,
            self.tokens_used,
            self.cost_usd_used,
        );
        Ok(SessionRecord {
            session_id: SessionId::new(self.session_id),
            project,
            state: parse_string_enum::<SessionState>(&self.state)?,
            version: self.version as u64,
            created_at: self.created_at as u64,
            updated_at: self.updated_at as u64,
            goal_title: self.goal_title,
            issue_budget: budget,
            max_attempts: self.max_attempts.max(0) as u32,
            attempts_used: self.attempts_used.max(0) as u32,
        })
    }
}

#[derive(sqlx::FromRow)]
struct RunRow {
    run_id: String,
    session_id: String,
    parent_run_id: Option<String>,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    state: String,
    failure_class: Option<String>,
    version: i64,
    created_at: i64,
    updated_at: i64,
    // F47 PR2: nullable completion annotation (SQLite has no JSONB; we
    // store verification as TEXT containing serde-JSON, matching the
    // portable pattern used by `route_policies.rules` and the other
    // JSON-over-TEXT columns in the SQLite schema).
    completion_summary: Option<String>,
    completion_verification_json: Option<String>,
    completion_annotated_at_ms: Option<i64>,
    // F64: nullable terminal-write recovery annotation (serde-JSON).
    terminal_write_recovery_json: Option<String>,
}

impl RunRow {
    fn into_record(self) -> Result<RunRecord, StoreError> {
        let project = project_key_from_parts(self.tenant_id, self.workspace_id, self.project_id);
        let completion_verification = self
            .completion_verification_json
            .as_deref()
            .map(serde_json::from_str::<cairn_domain::CompletionVerification>)
            .transpose()
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        // Checked conversion (Copilot review on #313): mirror the
        // pg adapter — a negative `completion_annotated_at_ms` wraps
        // to a huge `u64` via `as u64` and would silently surface an
        // invalid timestamp. Fail loud instead.
        let completion_annotated_at_ms = self
            .completion_annotated_at_ms
            .map(|v| {
                u64::try_from(v).map_err(|_| {
                    StoreError::Serialization(format!(
                        "runs.completion_annotated_at_ms {} is negative or out of u64 range",
                        v
                    ))
                })
            })
            .transpose()?;
        Ok(RunRecord {
            run_id: RunId::new(self.run_id),
            session_id: SessionId::new(self.session_id),
            parent_run_id: self.parent_run_id.map(RunId::new),
            project,
            state: parse_string_enum::<RunState>(&self.state)?,
            prompt_release_id: None,
            agent_role_id: None,
            failure_class: self
                .failure_class
                .as_deref()
                .map(parse_string_enum::<FailureClass>)
                .transpose()?,
            pause_reason: None,
            resume_trigger: None,
            version: self.version as u64,
            created_at: self.created_at as u64,
            updated_at: self.updated_at as u64,
            completion_summary: self.completion_summary,
            completion_verification,
            completion_annotated_at_ms,
            terminal_write_recovery: self
                .terminal_write_recovery_json
                .as_deref()
                .map(serde_json::from_str::<crate::projections::TerminalRecoveryRecord>)
                .transpose()
                .map_err(|e| StoreError::Serialization(e.to_string()))?,
        })
    }
}

#[derive(sqlx::FromRow)]
struct TaskRow {
    task_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    parent_run_id: Option<String>,
    parent_task_id: Option<String>,
    session_id: Option<String>,
    state: String,
    failure_class: Option<String>,
    lease_owner: Option<String>,
    lease_expires_at: Option<i64>,
    title: Option<String>,
    description: Option<String>,
    version: i64,
    created_at: i64,
    updated_at: i64,
}

impl TaskRow {
    fn into_record(self) -> Result<TaskRecord, StoreError> {
        let project = project_key_from_parts(self.tenant_id, self.workspace_id, self.project_id);
        Ok(TaskRecord {
            task_id: TaskId::new(self.task_id),
            project,
            parent_run_id: self.parent_run_id.map(RunId::new),
            parent_task_id: self.parent_task_id.map(TaskId::new),
            session_id: self.session_id.map(cairn_domain::SessionId::new),
            state: parse_string_enum::<TaskState>(&self.state)?,
            prompt_release_id: None,
            failure_class: self
                .failure_class
                .as_deref()
                .map(parse_string_enum::<FailureClass>)
                .transpose()?,
            lease_owner: self.lease_owner,
            lease_expires_at: self.lease_expires_at.map(|value| value as u64),
            title: self.title,
            description: self.description,
            pause_reason: None,
            resume_trigger: None,
            retry_count: 0,
            version: self.version as u64,
            created_at: self.created_at as u64,
            updated_at: self.updated_at as u64,
        })
    }
}

#[derive(sqlx::FromRow)]
struct ApprovalRow {
    approval_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    run_id: Option<String>,
    task_id: Option<String>,
    requirement: String,
    decision: Option<String>,
    title: Option<String>,
    description: Option<String>,
    version: i64,
    created_at: i64,
    updated_at: i64,
}

impl ApprovalRow {
    fn into_record(self) -> Result<ApprovalRecord, StoreError> {
        let project = project_key_from_parts(self.tenant_id, self.workspace_id, self.project_id);
        Ok(ApprovalRecord {
            approval_id: ApprovalId::new(self.approval_id),
            project,
            run_id: self.run_id.map(RunId::new),
            task_id: self.task_id.map(TaskId::new),
            requirement: parse_string_enum::<ApprovalRequirement>(&self.requirement)?,
            decision: self
                .decision
                .as_deref()
                .map(parse_string_enum::<ApprovalDecision>)
                .transpose()?,
            title: self.title,
            description: self.description,
            version: self.version as u64,
            created_at: self.created_at as u64,
            updated_at: self.updated_at as u64,
        })
    }
}

#[derive(sqlx::FromRow)]
struct CheckpointRow {
    checkpoint_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    run_id: String,
    disposition: String,
    version: i64,
    created_at: i64,
}

impl CheckpointRow {
    fn into_record(self) -> Result<CheckpointRecord, StoreError> {
        let project = project_key_from_parts(self.tenant_id, self.workspace_id, self.project_id);
        Ok(CheckpointRecord {
            checkpoint_id: CheckpointId::new(self.checkpoint_id),
            project,
            run_id: RunId::new(self.run_id),
            disposition: parse_string_enum::<CheckpointDisposition>(&self.disposition)?,
            data: None,
            version: self.version as u64,
            created_at: self.created_at as u64,
        })
    }
}

#[derive(sqlx::FromRow)]
struct MailboxRow {
    message_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    run_id: Option<String>,
    task_id: Option<String>,
    version: i64,
    created_at: i64,
}

impl MailboxRow {
    fn into_record(self) -> Result<MailboxRecord, StoreError> {
        let project = project_key_from_parts(self.tenant_id, self.workspace_id, self.project_id);
        Ok(MailboxRecord {
            message_id: MailboxMessageId::new(self.message_id),
            project,
            run_id: self.run_id.map(RunId::new),
            task_id: self.task_id.map(TaskId::new),
            from_task_id: None,
            content: String::new(),
            from_run_id: None,
            deliver_at_ms: 0,
            sender: None,
            recipient: None,
            body: None,
            sent_at: None,
            delivery_status: None,
            version: self.version as u64,
            created_at: self.created_at as u64,
        })
    }
}

fn project_key_from_parts(
    tenant_id: String,
    workspace_id: String,
    project_id: String,
) -> ProjectKey {
    ProjectKey::new(tenant_id, workspace_id, project_id)
}

#[async_trait]
impl crate::projections::FfLeaseHistoryCursorStore for SqliteAdapter {
    async fn get(
        &self,
        partition_id: &str,
        execution_id: &str,
    ) -> Result<Option<crate::projections::FfLeaseHistoryCursor>, StoreError> {
        let row: Option<(String, String, String, i64)> = sqlx::query_as(
            "SELECT partition_id, execution_id, last_stream_id, updated_at_ms
             FROM ff_lease_history_cursors
             WHERE partition_id = ?1 AND execution_id = ?2",
        )
        .bind(partition_id)
        .bind(execution_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(
            row.map(|(p, e, s, ts)| crate::projections::FfLeaseHistoryCursor {
                partition_id: p,
                execution_id: e,
                last_stream_id: s,
                updated_at_ms: ts as u64,
            }),
        )
    }

    async fn list_by_partition(
        &self,
        partition_id: &str,
    ) -> Result<Vec<crate::projections::FfLeaseHistoryCursor>, StoreError> {
        let rows: Vec<(String, String, String, i64)> = sqlx::query_as(
            "SELECT partition_id, execution_id, last_stream_id, updated_at_ms
             FROM ff_lease_history_cursors
             WHERE partition_id = ?1",
        )
        .bind(partition_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(|(p, e, s, ts)| crate::projections::FfLeaseHistoryCursor {
                partition_id: p,
                execution_id: e,
                last_stream_id: s,
                updated_at_ms: ts as u64,
            })
            .collect())
    }

    async fn upsert(
        &self,
        cursor: &crate::projections::FfLeaseHistoryCursor,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO ff_lease_history_cursors
                (partition_id, execution_id, last_stream_id, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (partition_id, execution_id) DO UPDATE
                SET last_stream_id = excluded.last_stream_id,
                    updated_at_ms = excluded.updated_at_ms",
        )
        .bind(&cursor.partition_id)
        .bind(&cursor.execution_id)
        .bind(&cursor.last_stream_id)
        .bind(cursor.updated_at_ms as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn delete(&self, partition_id: &str, execution_id: &str) -> Result<(), StoreError> {
        sqlx::query(
            "DELETE FROM ff_lease_history_cursors
             WHERE partition_id = ?1 AND execution_id = ?2",
        )
        .bind(partition_id)
        .bind(execution_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }
}

// -- PR BP-2: ToolCallApprovalReadModel --

#[derive(sqlx::FromRow)]
struct SqliteToolCallApprovalRow {
    call_id: String,
    session_id: String,
    run_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    tool_name: String,
    original_tool_args: String,
    amended_tool_args: Option<String>,
    approved_tool_args: Option<String>,
    display_summary: Option<String>,
    match_policy: String,
    state: String,
    operator_id: Option<String>,
    scope: Option<String>,
    reason: Option<String>,
    proposed_at_ms: i64,
    approved_at_ms: Option<i64>,
    rejected_at_ms: Option<i64>,
    last_amended_at_ms: Option<i64>,
    version: i64,
    created_at: i64,
    updated_at: i64,
}

impl SqliteToolCallApprovalRow {
    fn into_record(self) -> Result<ToolCallApprovalRecord, StoreError> {
        let original: serde_json::Value = serde_json::from_str(&self.original_tool_args)
            .map_err(|e| StoreError::Serialization(format!("original_tool_args decode: {e}")))?;
        let amended = self
            .amended_tool_args
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|e: serde_json::Error| {
                StoreError::Serialization(format!("amended_tool_args decode: {e}"))
            })?;
        let approved = self
            .approved_tool_args
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|e: serde_json::Error| {
                StoreError::Serialization(format!("approved_tool_args decode: {e}"))
            })?;
        let match_policy: ApprovalMatchPolicy = serde_json::from_str(&self.match_policy)
            .map_err(|e| StoreError::Serialization(format!("match_policy decode: {e}")))?;
        let scope: Option<ApprovalScope> = self
            .scope
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|e: serde_json::Error| {
                StoreError::Serialization(format!("scope decode: {e}"))
            })?;
        Ok(ToolCallApprovalRecord {
            call_id: ToolCallId::new(self.call_id),
            session_id: SessionId::new(self.session_id),
            run_id: RunId::new(self.run_id),
            project: ProjectKey::new(self.tenant_id, self.workspace_id, self.project_id),
            tool_name: self.tool_name,
            original_tool_args: original,
            amended_tool_args: amended,
            approved_tool_args: approved,
            display_summary: self.display_summary,
            match_policy,
            state: ToolCallApprovalState::parse(&self.state)?,
            operator_id: self.operator_id.map(OperatorId::new),
            scope,
            reason: self.reason,
            proposed_at_ms: self.proposed_at_ms as u64,
            approved_at_ms: self.approved_at_ms.map(|v| v as u64),
            rejected_at_ms: self.rejected_at_ms.map(|v| v as u64),
            last_amended_at_ms: self.last_amended_at_ms.map(|v| v as u64),
            version: self.version as u64,
            created_at: self.created_at as u64,
            updated_at: self.updated_at as u64,
        })
    }
}

const SQLITE_TOOL_CALL_APPROVAL_SELECT: &str =
    "SELECT call_id, session_id, run_id, tenant_id, workspace_id, project_id, \
     tool_name, original_tool_args, amended_tool_args, approved_tool_args, \
     display_summary, match_policy, state, operator_id, scope, reason, \
     proposed_at_ms, approved_at_ms, rejected_at_ms, last_amended_at_ms, \
     version, created_at, updated_at FROM tool_call_approvals";

#[async_trait]
impl ToolCallApprovalReadModel for SqliteAdapter {
    async fn get(
        &self,
        call_id: &ToolCallId,
    ) -> Result<Option<ToolCallApprovalRecord>, StoreError> {
        let sql = format!("{SQLITE_TOOL_CALL_APPROVAL_SELECT} WHERE call_id = ?");
        let row = sqlx::query_as::<_, SqliteToolCallApprovalRow>(&sql)
            .bind(call_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(SqliteToolCallApprovalRow::into_record).transpose()
    }

    async fn list_for_run(
        &self,
        run_id: &RunId,
    ) -> Result<Vec<ToolCallApprovalRecord>, StoreError> {
        let sql = format!(
            "{SQLITE_TOOL_CALL_APPROVAL_SELECT} WHERE run_id = ? \
             ORDER BY proposed_at_ms ASC, call_id ASC"
        );
        let rows = sqlx::query_as::<_, SqliteToolCallApprovalRow>(&sql)
            .bind(run_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(SqliteToolCallApprovalRow::into_record)
            .collect()
    }

    async fn list_for_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<ToolCallApprovalRecord>, StoreError> {
        let sql = format!(
            "{SQLITE_TOOL_CALL_APPROVAL_SELECT} WHERE session_id = ? \
             ORDER BY proposed_at_ms ASC, call_id ASC"
        );
        let rows = sqlx::query_as::<_, SqliteToolCallApprovalRow>(&sql)
            .bind(session_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(SqliteToolCallApprovalRow::into_record)
            .collect()
    }

    async fn list_pending_for_project(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ToolCallApprovalRecord>, StoreError> {
        let sql = format!(
            "{SQLITE_TOOL_CALL_APPROVAL_SELECT} \
             WHERE tenant_id = ? AND workspace_id = ? AND project_id = ? \
               AND state = 'pending' \
             ORDER BY proposed_at_ms ASC, call_id ASC \
             LIMIT ? OFFSET ?"
        );
        let rows = sqlx::query_as::<_, SqliteToolCallApprovalRow>(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(SqliteToolCallApprovalRow::into_record)
            .collect()
    }

    async fn list_all_pending(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ToolCallApprovalRecord>, StoreError> {
        // See pg/adapter.rs:list_all_pending — guard `usize` → `i64`
        // so out-of-range values surface as an error instead of
        // wrapping to a negative `LIMIT`/`OFFSET` at the SQL layer.
        let limit_i64 =
            i64::try_from(limit).map_err(|_| StoreError::Internal("limit overflows i64".into()))?;
        let offset_i64 = i64::try_from(offset)
            .map_err(|_| StoreError::Internal("offset overflows i64".into()))?;
        let sql = format!(
            "{SQLITE_TOOL_CALL_APPROVAL_SELECT} \
             WHERE state = 'pending' \
             ORDER BY proposed_at_ms ASC, call_id ASC \
             LIMIT ? OFFSET ?"
        );
        let rows = sqlx::query_as::<_, SqliteToolCallApprovalRow>(&sql)
            .bind(limit_i64)
            .bind(offset_i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(SqliteToolCallApprovalRow::into_record)
            .collect()
    }
}

// ── F65 PR-2: orchestrator-session read models (SQLite) ───────────────────

const F65_SESSION_OUTCOME_SELECT: &str =
    "SELECT root_run_id, tenant_id, workspace_scope, project_id, \
     session_id, checkpoint_id, workspace_snapshot_id, \
     termination_reason, termination_reason_json, \
     compacted_summary, next_step_hint, \
     cost_micros, created_at FROM session_outcomes";

#[derive(sqlx::FromRow)]
struct F65SessionOutcomeRow {
    root_run_id: String,
    tenant_id: String,
    workspace_scope: String,
    project_id: String,
    session_id: String,
    checkpoint_id: String,
    workspace_snapshot_id: Option<String>,
    termination_reason: String,
    termination_reason_json: Option<String>,
    compacted_summary: String,
    next_step_hint: Option<String>,
    cost_micros: i64,
    created_at: i64,
}

impl F65SessionOutcomeRow {
    fn into_record(self) -> Result<crate::projections::SessionOutcomeRecord, StoreError> {
        // Rehydrate the full TerminationReason from the JSON sidecar
        // column so payload fields (`ProviderError.message`,
        // `CircuitBreakerTripped.trip.*`, `Crashed.message`) round-trip
        // through the DB. Falls back to the short discriminator on
        // legacy rows written before the column existed (or when JSON
        // is malformed — operator filters keep working from the kind).
        let reason = crate::projections::rehydrate_termination_reason(
            self.termination_reason.as_str(),
            self.termination_reason_json.as_deref(),
        )?;
        Ok(crate::projections::SessionOutcomeRecord {
            root_run_id: RunId::new(self.root_run_id),
            project: project_key_from_parts(self.tenant_id, self.workspace_scope, self.project_id),
            session_id: SessionId::new(self.session_id),
            checkpoint_id: CheckpointId::new(self.checkpoint_id),
            workspace_snapshot_id: self
                .workspace_snapshot_id
                .map(cairn_domain::WorkspaceSnapshotId::new),
            termination_reason: reason,
            compacted_summary: self.compacted_summary,
            next_step_hint: self.next_step_hint,
            cost_micros: self.cost_micros.max(0) as u64,
            created_at: self.created_at.max(0) as u64,
        })
    }
}

#[async_trait]
impl crate::projections::SessionOutcomeReadModel for SqliteAdapter {
    async fn get_by_root_run(
        &self,
        project: &ProjectKey,
        root_run_id: &RunId,
    ) -> Result<Option<crate::projections::SessionOutcomeRecord>, StoreError> {
        // Tenant-isolation (issue #438): scope-tuple guard at the
        // query layer. See the PgAdapter counterpart for rationale.
        let sql = format!(
            "{F65_SESSION_OUTCOME_SELECT} WHERE root_run_id = ? \
             AND tenant_id = ? AND workspace_scope = ? AND project_id = ?"
        );
        let row = sqlx::query_as::<_, F65SessionOutcomeRow>(&sql)
            .bind(root_run_id.as_str())
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(F65SessionOutcomeRow::into_record).transpose()
    }

    async fn list_by_session(
        &self,
        project: &ProjectKey,
        session_id: &SessionId,
    ) -> Result<Vec<crate::projections::SessionOutcomeRecord>, StoreError> {
        let sql = format!(
            "{F65_SESSION_OUTCOME_SELECT} WHERE session_id = ? \
             AND tenant_id = ? AND workspace_scope = ? AND project_id = ? \
             ORDER BY created_at ASC, root_run_id ASC"
        );
        let rows = sqlx::query_as::<_, F65SessionOutcomeRow>(&sql)
            .bind(session_id.as_str())
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(F65SessionOutcomeRow::into_record)
            .collect()
    }
}

const F65_WORKSPACE_SNAPSHOT_SELECT: &str =
    "SELECT snapshot_id, tenant_id, workspace_scope, project_id, \
     session_id, workspace_id, parent_snapshot_id, snapshot_path, \
     bytes, reflink_used, created_at, reaped_at FROM workspace_snapshots";

#[derive(sqlx::FromRow)]
struct F65WorkspaceSnapshotRow {
    snapshot_id: String,
    tenant_id: String,
    workspace_scope: String,
    project_id: String,
    session_id: String,
    workspace_id: String,
    parent_snapshot_id: Option<String>,
    snapshot_path: String,
    bytes: i64,
    // SQLite stores booleans as INTEGER 0/1.
    reflink_used: i64,
    created_at: i64,
    reaped_at: Option<i64>,
}

impl F65WorkspaceSnapshotRow {
    fn into_record(self) -> crate::projections::WorkspaceSnapshotRecord {
        crate::projections::WorkspaceSnapshotRecord {
            snapshot_id: cairn_domain::WorkspaceSnapshotId::new(self.snapshot_id),
            project: project_key_from_parts(self.tenant_id, self.workspace_scope, self.project_id),
            session_id: SessionId::new(self.session_id),
            workspace_id: cairn_domain::WorkspaceId::new(self.workspace_id),
            parent_snapshot_id: self
                .parent_snapshot_id
                .map(cairn_domain::WorkspaceSnapshotId::new),
            snapshot_path: self.snapshot_path,
            bytes: self.bytes.max(0) as u64,
            reflink_used: self.reflink_used != 0,
            created_at: self.created_at.max(0) as u64,
            reaped_at: self.reaped_at.map(|v| v.max(0) as u64),
        }
    }
}

#[async_trait]
impl crate::projections::WorkspaceSnapshotWriter for SqliteAdapter {
    async fn stamp_metadata(
        &self,
        snapshot_id: &cairn_domain::WorkspaceSnapshotId,
        snapshot_path: &str,
        bytes: u64,
        reflink_used: bool,
        parent_snapshot_id: Option<&cairn_domain::WorkspaceSnapshotId>,
    ) -> Result<(), StoreError> {
        let bytes_i64 = i64::try_from(bytes).map_err(|_| {
            StoreError::Internal(format!(
                "WorkspaceSnapshotWriter.stamp_metadata.bytes {bytes} exceeds i64::MAX"
            ))
        })?;
        sqlx::query(
            "UPDATE workspace_snapshots
                SET snapshot_path     = ?,
                    bytes             = ?,
                    reflink_used      = ?,
                    parent_snapshot_id = ?
              WHERE snapshot_id = ?",
        )
        .bind(snapshot_path)
        .bind(bytes_i64)
        .bind(i64::from(reflink_used))
        .bind(parent_snapshot_id.map(|p| p.as_str().to_owned()))
        .bind(snapshot_id.as_str())
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl crate::projections::WorkspaceSnapshotReadModel for SqliteAdapter {
    async fn get(
        &self,
        project: &ProjectKey,
        snapshot_id: &cairn_domain::WorkspaceSnapshotId,
    ) -> Result<Option<crate::projections::WorkspaceSnapshotRecord>, StoreError> {
        let sql = format!(
            "{F65_WORKSPACE_SNAPSHOT_SELECT} WHERE snapshot_id = ? \
             AND tenant_id = ? AND workspace_scope = ? AND project_id = ?"
        );
        let row = sqlx::query_as::<_, F65WorkspaceSnapshotRow>(&sql)
            .bind(snapshot_id.as_str())
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(F65WorkspaceSnapshotRow::into_record))
    }

    async fn list_by_session(
        &self,
        project: &ProjectKey,
        session_id: &SessionId,
    ) -> Result<Vec<crate::projections::WorkspaceSnapshotRecord>, StoreError> {
        let sql = format!(
            "{F65_WORKSPACE_SNAPSHOT_SELECT} WHERE session_id = ? \
             AND tenant_id = ? AND workspace_scope = ? AND project_id = ? \
             ORDER BY created_at ASC, snapshot_id ASC"
        );
        let rows = sqlx::query_as::<_, F65WorkspaceSnapshotRow>(&sql)
            .bind(session_id.as_str())
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(F65WorkspaceSnapshotRow::into_record)
            .collect())
    }

    async fn lineage(
        &self,
        project: &ProjectKey,
        start: &cairn_domain::WorkspaceSnapshotId,
    ) -> Result<Vec<crate::projections::WorkspaceSnapshotRecord>, StoreError> {
        // Iterative walk. A recursive CTE would be faster but is not
        // supported in every SQLite build we target — stick to the
        // portable subset (per project memory `feedback_no_db_specific_features`).
        //
        // Issue #438: every hop re-checks the project scope via the
        // per-row `get` above, so a chain that crosses tenants stops
        // at the boundary instead of leaking the foreign row.
        let mut chain: Vec<crate::projections::WorkspaceSnapshotRecord> = Vec::new();
        let mut cursor = Some(start.as_str().to_owned());
        // Bound the walk to avoid spinning on cycles. Even though the FK
        // makes cycles unreachable in normal operation, we still defend
        // against hand-edited rows in dev DBs.
        const MAX_DEPTH: usize = 1024;
        for _ in 0..MAX_DEPTH {
            let Some(id) = cursor.take() else {
                break;
            };
            let Some(rec) = <Self as crate::projections::WorkspaceSnapshotReadModel>::get(
                self,
                project,
                &cairn_domain::WorkspaceSnapshotId::new(id.clone()),
            )
            .await?
            else {
                break;
            };
            cursor = rec
                .parent_snapshot_id
                .as_ref()
                .map(|p| p.as_str().to_owned());
            chain.push(rec);
        }
        Ok(chain)
    }
}

#[async_trait]
impl crate::projections::WorkspaceRegistryReadModel for SqliteAdapter {
    async fn get(
        &self,
        project: &ProjectKey,
        workspace_id: &cairn_domain::WorkspaceId,
    ) -> Result<Option<crate::projections::WorkspaceRegistryRecord>, StoreError> {
        let row: Option<(
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            i64,
            Option<i64>,
        )> = sqlx::query_as(
            "SELECT workspace_id, tenant_id, workspace_scope, project_id, \
             root_run_id, fs_root, status, created_at, reaped_at \
             FROM workspace_registry WHERE workspace_id = ? \
             AND tenant_id = ? AND workspace_scope = ? AND project_id = ?",
        )
        .bind(workspace_id.as_str())
        .bind(project.tenant_id.as_str())
        .bind(project.workspace_id.as_str())
        .bind(project.project_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(tuple_to_workspace_registry).transpose()
    }

    async fn get_by_root_run(
        &self,
        project: &ProjectKey,
        root_run_id: &RunId,
    ) -> Result<Option<crate::projections::WorkspaceRegistryRecord>, StoreError> {
        let row: Option<(
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            i64,
            Option<i64>,
        )> = sqlx::query_as(
            "SELECT workspace_id, tenant_id, workspace_scope, project_id, \
             root_run_id, fs_root, status, created_at, reaped_at \
             FROM workspace_registry WHERE root_run_id = ? \
             AND tenant_id = ? AND workspace_scope = ? AND project_id = ? \
             ORDER BY created_at DESC LIMIT 1",
        )
        .bind(root_run_id.as_str())
        .bind(project.tenant_id.as_str())
        .bind(project.workspace_id.as_str())
        .bind(project.project_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(tuple_to_workspace_registry).transpose()
    }
}

fn tuple_to_workspace_registry(
    t: (
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        i64,
        Option<i64>,
    ),
) -> Result<crate::projections::WorkspaceRegistryRecord, StoreError> {
    let (
        workspace_id,
        tenant_id,
        ws_scope,
        project_id,
        root_run_id,
        fs_root,
        status,
        created_at,
        reaped_at,
    ) = t;
    // Fail loud on unknown status — silently defaulting masks schema
    // drift or data corruption per `feedback_no_silent_fallbacks`.
    let parsed_status = status.parse()?;
    Ok(crate::projections::WorkspaceRegistryRecord {
        workspace_id: cairn_domain::WorkspaceId::new(workspace_id),
        project: project_key_from_parts(tenant_id, ws_scope, project_id),
        root_run_id: RunId::new(root_run_id),
        fs_root,
        status: parsed_status,
        created_at: created_at.max(0) as u64,
        reaped_at: reaped_at.map(|v| v.max(0) as u64),
    })
}

#[async_trait]
impl crate::projections::F65CheckpointReadModel for SqliteAdapter {
    async fn get_f65(
        &self,
        project: &ProjectKey,
        checkpoint_id: &CheckpointId,
    ) -> Result<Option<crate::projections::F65CheckpointRecord>, StoreError> {
        // Single-query SELECT: include `session_id` (NOT-NULL-gated by the
        // WHERE clause so legacy RFC 005 rows without F65 state still
        // return None) and the project parts in one round-trip.
        // Previously we did two queries — the second was redundant.
        let row: Option<(
            String,
            String,
            String,
            String,
            String,
            String,
            Option<String>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            i64,
        )> = sqlx::query_as(
            "SELECT checkpoint_id, session_id, tenant_id, workspace_id, project_id, \
             run_id, body, body_size_bytes, schema_version, iteration, created_at \
             FROM checkpoints \
             WHERE checkpoint_id = ? AND session_id IS NOT NULL \
             AND tenant_id = ? AND workspace_id = ? AND project_id = ?",
        )
        .bind(checkpoint_id.as_str())
        .bind(project.tenant_id.as_str())
        .bind(project.workspace_id.as_str())
        .bind(project.project_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        let Some((cid, sid, tenant, ws, proj, run_id, body, body_size, schema_ver, it, ts)) = row
        else {
            return Ok(None);
        };
        Ok(Some(crate::projections::F65CheckpointRecord {
            checkpoint_id: CheckpointId::new(cid),
            project: project_key_from_parts(tenant, ws, proj),
            session_id: SessionId::new(sid),
            root_run_id: RunId::new(run_id),
            schema_version: schema_ver.unwrap_or(1).max(0) as u32,
            body: body.unwrap_or_default(),
            body_size_bytes: body_size.unwrap_or(0).max(0) as u64,
            iteration: it.unwrap_or(0).max(0) as u32,
            created_at: ts.max(0) as u64,
        }))
    }

    async fn list_by_session(
        &self,
        project: &ProjectKey,
        session_id: &SessionId,
    ) -> Result<Vec<crate::projections::F65CheckpointRecord>, StoreError> {
        let rows: Vec<(
            String,
            String,
            String,
            String,
            String,
            String,
            Option<String>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            i64,
        )> = sqlx::query_as(
            "SELECT checkpoint_id, tenant_id, workspace_id, project_id, \
             run_id, session_id, body, body_size_bytes, schema_version, iteration, created_at \
             FROM checkpoints \
             WHERE session_id = ? \
             AND tenant_id = ? AND workspace_id = ? AND project_id = ? \
             ORDER BY iteration ASC, created_at ASC, checkpoint_id ASC",
        )
        .bind(session_id.as_str())
        .bind(project.tenant_id.as_str())
        .bind(project.workspace_id.as_str())
        .bind(project.project_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(
                |(cid, tenant, ws, proj, run_id, sid, body, body_size, schema_ver, it, ts)| {
                    crate::projections::F65CheckpointRecord {
                        checkpoint_id: CheckpointId::new(cid),
                        project: project_key_from_parts(tenant, ws, proj),
                        session_id: SessionId::new(sid),
                        root_run_id: RunId::new(run_id),
                        schema_version: schema_ver.unwrap_or(1).max(0) as u32,
                        body: body.unwrap_or_default(),
                        body_size_bytes: body_size.unwrap_or(0).max(0) as u64,
                        iteration: it.unwrap_or(0).max(0) as u32,
                        created_at: ts.max(0) as u64,
                    }
                },
            )
            .collect())
    }
}

// ── RFC-025 Phase 1 (milestone 4): EvalRunReadModel ──────────────────────────
//
// sqlite parity with `PgAdapter`'s eval_runs impl. Same 20-column
// SELECT (shape documented by the `EvalRunRow` struct below), same
// serde_json::from_str for metrics_json / rubric_score_json. Parity
// harness asserts byte-equality against the in-memory store + pg under
// TEST_DATABASE_URL.

/// Named row struct for the `eval_runs` projection on sqlite. Mirrors
/// `pg::adapter::EvalRunRow` — 20 columns, which exceeds sqlx's tuple
/// `FromRow` impls (capped at 16), so we use derive(FromRow).
#[derive(sqlx::FromRow)]
struct EvalRunRow {
    eval_run_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    subject_kind: String,
    evaluator_type: String,
    success: Option<bool>,
    error_message: Option<String>,
    started_at: i64,
    completed_at: Option<i64>,
    archived_at: Option<i64>,
    metrics_json: Option<String>,
    rubric_score_json: Option<String>,
    dataset_id: Option<String>,
    rubric_id: Option<String>,
    baseline_id: Option<String>,
    prompt_asset_id: Option<String>,
    prompt_version_id: Option<String>,
    prompt_release_id: Option<String>,
    created_by: Option<String>,
}

const EVAL_RUN_SELECT_COLS: &str = "eval_run_id, tenant_id, workspace_id, project_id, \
     subject_kind, evaluator_type, \
     success, error_message, started_at, completed_at, \
     archived_at, metrics_json, rubric_score_json, \
     dataset_id, rubric_id, baseline_id, \
     prompt_asset_id, prompt_version_id, prompt_release_id, \
     created_by";

#[async_trait]
impl crate::projections::EvalRunReadModel for SqliteAdapter {
    async fn get(
        &self,
        eval_run_id: &cairn_domain::EvalRunId,
    ) -> Result<Option<crate::projections::EvalRunRecord>, StoreError> {
        let sql = format!("SELECT {EVAL_RUN_SELECT_COLS} FROM eval_runs WHERE eval_run_id = ?");
        let row: Option<EvalRunRow> = sqlx::query_as(&sql)
            .bind(eval_run_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        row.map(sqlite_row_to_eval_run_record).transpose()
    }

    async fn list_by_project(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::EvalRunRecord>, StoreError> {
        let sql = format!(
            "SELECT {EVAL_RUN_SELECT_COLS} FROM eval_runs
             WHERE tenant_id = ? AND workspace_id = ? AND project_id = ?
             ORDER BY started_at ASC, eval_run_id ASC
             LIMIT ? OFFSET ?"
        );
        let rows: Vec<EvalRunRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        rows.into_iter()
            .map(sqlite_row_to_eval_run_record)
            .collect()
    }
}

/// sqlite equivalent of `pg::adapter::pg_row_to_eval_run_record`. Parity
/// harness asserts the two produce byte-identical `EvalRunRecord`s for
/// the same event sequence.
fn sqlite_row_to_eval_run_record(
    row: EvalRunRow,
) -> Result<crate::projections::EvalRunRecord, StoreError> {
    let metrics = match row.metrics_json.as_deref() {
        Some(s) => Some(
            serde_json::from_str::<cairn_domain::EvalMetrics>(s).map_err(|e| {
                StoreError::Internal(format!(
                    "eval_runs.metrics_json parse error for {}: {e}",
                    row.eval_run_id
                ))
            })?,
        ),
        None => None,
    };
    let rubric_score = match row.rubric_score_json.as_deref() {
        Some(s) => Some(
            serde_json::from_str::<crate::projections::EvalRubricScoreSummary>(s).map_err(|e| {
                StoreError::Internal(format!(
                    "eval_runs.rubric_score_json parse error for {}: {e}",
                    row.eval_run_id
                ))
            })?,
        ),
        None => None,
    };
    Ok(crate::projections::EvalRunRecord {
        eval_run_id: cairn_domain::EvalRunId::new(row.eval_run_id),
        project: cairn_domain::ProjectKey::new(row.tenant_id, row.workspace_id, row.project_id),
        subject_kind: row.subject_kind,
        evaluator_type: row.evaluator_type,
        success: row.success,
        error_message: row.error_message,
        started_at: row.started_at.max(0) as u64,
        completed_at: row.completed_at.map(|v| v.max(0) as u64),
        archived_at: row.archived_at.map(|v| v.max(0) as u64),
        metrics,
        rubric_score,
        dataset_id: row.dataset_id,
        rubric_id: row.rubric_id,
        baseline_id: row.baseline_id,
        prompt_asset_id: row.prompt_asset_id.map(cairn_domain::PromptAssetId::new),
        prompt_version_id: row
            .prompt_version_id
            .map(cairn_domain::PromptVersionId::new),
        prompt_release_id: row
            .prompt_release_id
            .map(cairn_domain::PromptReleaseId::new),
        created_by: row.created_by.map(cairn_domain::OperatorId::new),
    })
}

// ── RFC-025 Phase 2a.1: CredentialReadModel + CredentialRotationReadModel ────
//
// sqlite parity with PgAdapter's credential impl. Same column list,
// same ordering; `active` arrives as INTEGER 0/1 and is coerced through
// bool via sqlx. The parity harness asserts byte-equality against the
// pg adapter under TEST_DATABASE_URL.

#[derive(sqlx::FromRow)]
struct CredentialRow {
    credential_id: String,
    tenant_id: String,
    name: String,
    provider_id: String,
    credential_type: String,
    encrypted_value: Vec<u8>,
    key_id: Option<String>,
    key_version: Option<String>,
    active: bool,
    encrypted_at_ms: Option<i64>,
    revoked_at_ms: Option<i64>,
    created_at: i64,
    updated_at: i64,
}

impl CredentialRow {
    fn into_record(self) -> cairn_domain::credentials::CredentialRecord {
        cairn_domain::credentials::CredentialRecord {
            id: cairn_domain::CredentialId::new(self.credential_id),
            tenant_id: cairn_domain::TenantId::new(self.tenant_id),
            name: self.name,
            credential_type: self.credential_type,
            // Wrap the ciphertext as soon as it leaves the DB driver so
            // the projection heap copy is scrubbed on drop (#579).
            encrypted_value: cairn_domain::credentials::RedactedCiphertext::from(
                self.encrypted_value,
            ),
            created_at: self.created_at.max(0) as u64,
            updated_at: self.updated_at.max(0) as u64,
            active: self.active,
            provider_id: self.provider_id,
            encrypted_at_ms: self.encrypted_at_ms.map(|v| v.max(0) as u64),
            key_id: self.key_id,
            key_version: self.key_version,
            revoked_at_ms: self.revoked_at_ms.map(|v| v.max(0) as u64),
        }
    }
}

const CREDENTIAL_SELECT_COLS: &str = "credential_id, tenant_id, name, provider_id, \
     credential_type, encrypted_value, key_id, key_version, active, \
     encrypted_at_ms, revoked_at_ms, created_at, updated_at";

#[async_trait]
impl crate::projections::CredentialReadModel for SqliteAdapter {
    async fn get(
        &self,
        id: &cairn_domain::CredentialId,
    ) -> Result<Option<cairn_domain::credentials::CredentialRecord>, StoreError> {
        let sql =
            format!("SELECT {CREDENTIAL_SELECT_COLS} FROM credentials WHERE credential_id = ?");
        let row: Option<CredentialRow> = sqlx::query_as(&sql)
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(CredentialRow::into_record))
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::credentials::CredentialRecord>, StoreError> {
        let sql = format!(
            "SELECT {CREDENTIAL_SELECT_COLS} FROM credentials
             WHERE tenant_id = ?
             ORDER BY created_at ASC, credential_id ASC
             LIMIT ? OFFSET ?"
        );
        let rows: Vec<CredentialRow> = sqlx::query_as(&sql)
            .bind(tenant_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows.into_iter().map(CredentialRow::into_record).collect())
    }

    async fn list_all_active(
        &self,
        limit: usize,
    ) -> Result<Option<Vec<cairn_domain::credentials::CredentialRecord>>, StoreError> {
        let sql = format!(
            "SELECT {CREDENTIAL_SELECT_COLS} FROM credentials
             WHERE active = 1
             ORDER BY created_at ASC, credential_id ASC
             LIMIT ?"
        );
        let rows: Vec<CredentialRow> = sqlx::query_as(&sql)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(Some(
            rows.into_iter().map(CredentialRow::into_record).collect(),
        ))
    }
}

#[derive(sqlx::FromRow)]
struct CredentialRotationRow {
    rotation_id: String,
    tenant_id: String,
    credential_id: String,
    old_key_id: String,
    new_key_id: String,
    rotated_credentials: i32,
    started_at_ms: i64,
    completed_at_ms: Option<i64>,
    rotated_at: i64,
    rotated_by: Option<String>,
}

impl CredentialRotationRow {
    fn into_record(self) -> cairn_domain::credentials::CredentialRotationRecord {
        cairn_domain::credentials::CredentialRotationRecord {
            rotation_id: self.rotation_id,
            tenant_id: cairn_domain::TenantId::new(self.tenant_id),
            credential_id: cairn_domain::CredentialId::new(self.credential_id),
            rotated_at: self.rotated_at.max(0) as u64,
            rotated_by: self.rotated_by,
            old_key_id: self.old_key_id,
            new_key_id: self.new_key_id,
            rotated_credentials: self.rotated_credentials.max(0) as u32,
            started_at_ms: self.started_at_ms.max(0) as u64,
            completed_at_ms: self.completed_at_ms.map(|v| v.max(0) as u64),
        }
    }
}

const CREDENTIAL_ROTATION_SELECT_COLS: &str = "rotation_id, tenant_id, credential_id, \
     old_key_id, new_key_id, rotated_credentials, \
     started_at_ms, completed_at_ms, rotated_at, rotated_by";

// ── RFC-025 Phase 2a.1 milestone 2: QuotaReadModel + QuotaViolationReadModel
//    (sqlite parity with pg) ──────────────────────────────────────────────

#[derive(sqlx::FromRow)]
struct TenantQuotaRow {
    max_concurrent_runs: i32,
    max_sessions_per_hour: i32,
    max_tasks_per_run: i32,
}

#[async_trait]
impl crate::projections::QuotaReadModel for SqliteAdapter {
    async fn get_quota(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Option<cairn_domain::TenantQuota>, StoreError> {
        let baseline: Option<TenantQuotaRow> = sqlx::query_as(
            "SELECT max_concurrent_runs, max_sessions_per_hour, max_tasks_per_run
             FROM tenant_quotas
             WHERE tenant_id = ?",
        )
        .bind(tenant_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        let Some(baseline) = baseline else {
            return Ok(None);
        };

        let active_runs_row: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM runs
             WHERE tenant_id = ?
               AND state NOT IN ('completed', 'failed', 'canceled', 'dead_lettered')",
        )
        .bind(tenant_id.as_str())
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        let sessions_row: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM sessions WHERE tenant_id = ?")
                .bind(tenant_id.as_str())
                .fetch_one(&self.pool)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(Some(cairn_domain::TenantQuota {
            tenant_id: tenant_id.clone(),
            max_concurrent_runs: baseline.max_concurrent_runs.max(0) as u32,
            max_sessions_per_hour: baseline.max_sessions_per_hour.max(0) as u32,
            max_tasks_per_run: baseline.max_tasks_per_run.max(0) as u32,
            current_active_runs: active_runs_row.0.max(0) as u32,
            sessions_this_hour: sessions_row.0.max(0) as u32,
        }))
    }
}

#[derive(sqlx::FromRow)]
struct QuotaViolationRow {
    tenant_id: String,
    quota_type: String,
    occurred_at_ms: i64,
    current_value: i32,
    limit_value: i32,
}

impl QuotaViolationRow {
    fn into_record(self) -> crate::projections::QuotaViolationRecord {
        crate::projections::QuotaViolationRecord {
            tenant_id: cairn_domain::TenantId::new(self.tenant_id),
            quota_type: self.quota_type,
            current: self.current_value.max(0) as u32,
            limit: self.limit_value.max(0) as u32,
            occurred_at_ms: self.occurred_at_ms.max(0) as u64,
        }
    }
}

#[async_trait]
impl crate::projections::QuotaViolationReadModel for SqliteAdapter {
    async fn list_violations(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
    ) -> Result<Vec<crate::projections::QuotaViolationRecord>, StoreError> {
        let rows: Vec<QuotaViolationRow> = sqlx::query_as(
            "SELECT tenant_id, quota_type, occurred_at_ms, current_value, limit_value
             FROM tenant_quota_violations
             WHERE tenant_id = ?
             ORDER BY occurred_at_ms DESC, quota_type ASC
             LIMIT ?",
        )
        .bind(tenant_id.as_str())
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(QuotaViolationRow::into_record)
            .collect())
    }
}

// ── RFC-025 Phase 2a.1 milestone 3: ProviderBudgetReadModel (sqlite parity)

#[derive(sqlx::FromRow)]
struct ProviderBudgetRow {
    tenant_id: String,
    period: String,
    limit_micros: i64,
    alert_threshold_percent: i32,
    current_spend_micros: i64,
    created_at: i64,
    updated_at: i64,
}

impl ProviderBudgetRow {
    fn into_record(self) -> Result<cairn_domain::providers::ProviderBudget, StoreError> {
        let period = match self.period.as_str() {
            "daily" => cairn_domain::providers::ProviderBudgetPeriod::Daily,
            "monthly" => cairn_domain::providers::ProviderBudgetPeriod::Monthly,
            other => {
                return Err(StoreError::Internal(format!(
                    "provider_budgets.period: unknown value {other:?}"
                )))
            }
        };
        Ok(cairn_domain::providers::ProviderBudget {
            tenant_id: cairn_domain::TenantId::new(self.tenant_id),
            period,
            limit_micros: self.limit_micros.max(0) as u64,
            alert_threshold_percent: self.alert_threshold_percent.max(0) as u32,
            current_spend_micros: self.current_spend_micros.max(0) as u64,
            created_at: self.created_at.max(0) as u64,
            updated_at: self.updated_at.max(0) as u64,
        })
    }
}

const PROVIDER_BUDGET_SELECT_COLS: &str = "tenant_id, period, limit_micros, \
     alert_threshold_percent, current_spend_micros, created_at, updated_at";

#[async_trait]
impl crate::projections::ProviderBudgetReadModel for SqliteAdapter {
    async fn get_by_tenant_period(
        &self,
        tenant_id: &cairn_domain::TenantId,
        period: cairn_domain::providers::ProviderBudgetPeriod,
    ) -> Result<Option<cairn_domain::providers::ProviderBudget>, StoreError> {
        let period_str = match period {
            cairn_domain::providers::ProviderBudgetPeriod::Daily => "daily",
            cairn_domain::providers::ProviderBudgetPeriod::Monthly => "monthly",
        };
        let sql = format!(
            "SELECT {PROVIDER_BUDGET_SELECT_COLS} FROM provider_budgets
             WHERE tenant_id = ? AND period = ?
             ORDER BY created_at ASC, limit_micros ASC
             LIMIT 1"
        );
        let row: Option<ProviderBudgetRow> = sqlx::query_as(&sql)
            .bind(tenant_id.as_str())
            .bind(period_str)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(ProviderBudgetRow::into_record).transpose()
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Vec<cairn_domain::providers::ProviderBudget>, StoreError> {
        let sql = format!(
            "SELECT {PROVIDER_BUDGET_SELECT_COLS} FROM provider_budgets
             WHERE tenant_id = ?
             ORDER BY created_at ASC, period ASC"
        );
        let rows: Vec<ProviderBudgetRow> = sqlx::query_as(&sql)
            .bind(tenant_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(ProviderBudgetRow::into_record)
            .collect()
    }
}

// ── RFC-025 Phase 2a.1 milestone 4: LicenseReadModel (sqlite parity) ─────────

#[derive(sqlx::FromRow)]
struct LicenseRow {
    tenant_id: String,
    license_key: Option<String>,
    tier: String,
    entitlements_json: String,
    issued_at: i64,
    expires_at: Option<i64>,
}

impl LicenseRow {
    fn into_record(self) -> Result<cairn_domain::LicenseRecord, StoreError> {
        let tier = match self.tier.as_str() {
            "local_eval" => cairn_domain::commercial::ProductTier::LocalEval,
            "team_self_hosted" => cairn_domain::commercial::ProductTier::TeamSelfHosted,
            "enterprise_self_hosted" => cairn_domain::commercial::ProductTier::EnterpriseSelfHosted,
            other => {
                return Err(StoreError::Internal(format!(
                    "licenses.tier: unknown value {other:?}"
                )))
            }
        };
        let entitlements: Vec<cairn_domain::commercial::Entitlement> =
            serde_json::from_str(&self.entitlements_json).map_err(|e| {
                StoreError::Internal(format!("licenses.entitlements_json parse error: {e}"))
            })?;
        Ok(cairn_domain::LicenseRecord {
            tenant_id: cairn_domain::TenantId::new(self.tenant_id),
            tier,
            entitlements,
            issued_at: self.issued_at.max(0) as u64,
            expires_at: self.expires_at.map(|v| v.max(0) as u64),
            license_key: self.license_key,
        })
    }
}

const LICENSE_SELECT_COLS: &str = "tenant_id, license_key, tier, entitlements_json, \
     issued_at, expires_at";

#[async_trait]
impl crate::projections::LicenseReadModel for SqliteAdapter {
    async fn get_active(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Option<cairn_domain::LicenseRecord>, StoreError> {
        let sql = format!("SELECT {LICENSE_SELECT_COLS} FROM licenses WHERE tenant_id = ?");
        let row: Option<LicenseRow> = sqlx::query_as(&sql)
            .bind(tenant_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(LicenseRow::into_record).transpose()
    }

    async fn list_overrides(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Vec<cairn_domain::EntitlementOverrideRecord>, StoreError> {
        let rows: Vec<EntitlementOverrideRow> = sqlx::query_as(
            "SELECT tenant_id, feature, allowed, reason, set_at_ms
             FROM entitlement_overrides
             WHERE tenant_id = ?
             ORDER BY feature ASC",
        )
        .bind(tenant_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(EntitlementOverrideRow::into_record)
            .collect())
    }
}

// ── RFC-025 Phase 2a.2 milestone 4: entitlement_overrides read row ───────────

#[derive(sqlx::FromRow)]
struct EntitlementOverrideRow {
    tenant_id: String,
    feature: String,
    allowed: bool,
    reason: Option<String>,
    set_at_ms: i64,
}

impl EntitlementOverrideRow {
    fn into_record(self) -> cairn_domain::EntitlementOverrideRecord {
        // Parity with pg — same synthetic override_id + hardcoded
        // `AdvancedAdmin` entitlement as the in-memory applier.
        let set_at_ms = u64::try_from(self.set_at_ms.max(0)).unwrap_or(0);
        cairn_domain::EntitlementOverrideRecord {
            override_id: format!("override_{}_{}", self.tenant_id, self.feature),
            tenant_id: cairn_domain::TenantId::new(self.tenant_id),
            entitlement: cairn_domain::commercial::Entitlement::AdvancedAdmin,
            granted: self.allowed,
            reason: self.reason,
            applied_at: set_at_ms,
            feature: self.feature,
            allowed: self.allowed,
            set_at_ms,
        }
    }
}

#[async_trait]
impl crate::projections::CredentialRotationReadModel for SqliteAdapter {
    async fn list_rotations(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Vec<cairn_domain::credentials::CredentialRotationRecord>, StoreError> {
        let sql = format!(
            "SELECT {CREDENTIAL_ROTATION_SELECT_COLS} FROM credential_rotations
             WHERE tenant_id = ?
             ORDER BY rotated_at ASC, rotation_id ASC"
        );
        let rows: Vec<CredentialRotationRow> = sqlx::query_as(&sql)
            .bind(tenant_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(CredentialRotationRow::into_record)
            .collect())
    }
}

// ── RFC-025 Phase 2a.2 milestone 1: ApprovalDelegationReadModel ─────────────

#[derive(sqlx::FromRow)]
struct ApprovalDelegationRow {
    approval_id: String,
    delegation_id: String,
    delegated_to: String,
    delegated_at_ms: i64,
}

impl ApprovalDelegationRow {
    fn into_record(self) -> crate::projections::ApprovalDelegationRecord {
        crate::projections::ApprovalDelegationRecord {
            approval_id: ApprovalId::new(self.approval_id),
            delegated_to: self.delegated_to,
            delegated_at_ms: self.delegated_at_ms.max(0) as u64,
            delegation_id: self.delegation_id,
        }
    }
}

#[async_trait]
impl crate::projections::ApprovalDelegationReadModel for SqliteAdapter {
    async fn list_for_approval(
        &self,
        approval_id: &ApprovalId,
    ) -> Result<Vec<crate::projections::ApprovalDelegationRecord>, StoreError> {
        let rows: Vec<ApprovalDelegationRow> = sqlx::query_as(
            "SELECT approval_id, delegation_id, delegated_to, delegated_at_ms
             FROM approval_delegations
             WHERE approval_id = ?
             ORDER BY delegated_at_ms ASC, delegation_id ASC",
        )
        .bind(approval_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(ApprovalDelegationRow::into_record)
            .collect())
    }
}

// ── RFC-025 Phase 2a.2 milestone 2: GuardrailReadModel + GuardrailEvaluationReadModel ─

#[derive(sqlx::FromRow)]
struct GuardrailPolicyRow {
    policy_id: String,
    name: String,
    rules_json: String,
    // sqlx maps SQLite INTEGER 0/1 → bool via the FromRow helper.
    enabled: bool,
}

impl GuardrailPolicyRow {
    fn into_record(self) -> Result<cairn_domain::policy::GuardrailPolicy, StoreError> {
        let rules: Vec<cairn_domain::policy::GuardrailRule> =
            serde_json::from_str(&self.rules_json).map_err(|e| {
                StoreError::Internal(format!("guardrail_policies.rules_json parse: {e}"))
            })?;
        Ok(cairn_domain::policy::GuardrailPolicy {
            policy_id: self.policy_id,
            name: self.name,
            rules,
            enabled: self.enabled,
        })
    }
}

#[async_trait]
impl crate::projections::GuardrailReadModel for SqliteAdapter {
    async fn get_policy(
        &self,
        policy_id: &str,
    ) -> Result<Option<cairn_domain::policy::GuardrailPolicy>, StoreError> {
        let row: Option<GuardrailPolicyRow> = sqlx::query_as(
            "SELECT policy_id, name, rules_json, enabled
             FROM guardrail_policies
             WHERE policy_id = ?",
        )
        .bind(policy_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(GuardrailPolicyRow::into_record).transpose()
    }

    async fn list_policies(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::policy::GuardrailPolicy>, StoreError> {
        let rows: Vec<GuardrailPolicyRow> = sqlx::query_as(
            "SELECT policy_id, name, rules_json, enabled
             FROM guardrail_policies
             WHERE tenant_id = ?
             ORDER BY policy_id ASC
             LIMIT ? OFFSET ?",
        )
        .bind(tenant_id.as_str())
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(GuardrailPolicyRow::into_record)
            .collect()
    }
}

#[derive(sqlx::FromRow)]
struct GuardrailEvaluationRow {
    policy_id: String,
    tenant_id: String,
    subject_type: String,
    subject_id: String,
    action: String,
    decision: String,
    reason: Option<String>,
    evaluated_at_ms: i64,
}

impl GuardrailEvaluationRow {
    fn into_record(self) -> Result<crate::projections::GuardrailEvaluationRecord, StoreError> {
        use cairn_domain::policy::{GuardrailDecisionKind as D, GuardrailSubjectType as T};
        let subject_type = match self.subject_type.as_str() {
            "run" => T::Run,
            "task" => T::Task,
            "session" => T::Session,
            "tool" => T::Tool,
            "provider" => T::Provider,
            other => {
                return Err(StoreError::Internal(format!(
                    "guardrail_evaluations.subject_type unknown {other:?}"
                )))
            }
        };
        let decision = match self.decision.as_str() {
            "allowed" => D::Allowed,
            "denied" => D::Denied,
            "warned" => D::Warned,
            other => {
                return Err(StoreError::Internal(format!(
                    "guardrail_evaluations.decision unknown {other:?}"
                )))
            }
        };
        let subject_id = if self.subject_id.is_empty() {
            None
        } else {
            Some(self.subject_id)
        };
        Ok(crate::projections::GuardrailEvaluationRecord {
            policy_id: self.policy_id,
            tenant_id: cairn_domain::TenantId::new(self.tenant_id),
            subject_type,
            subject_id,
            action: self.action,
            decision,
            reason: self.reason,
            evaluated_at_ms: self.evaluated_at_ms.max(0) as u64,
        })
    }
}

#[async_trait]
impl crate::projections::GuardrailEvaluationReadModel for SqliteAdapter {
    async fn list_evaluations(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
    ) -> Result<Vec<crate::projections::GuardrailEvaluationRecord>, StoreError> {
        let rows: Vec<GuardrailEvaluationRow> = sqlx::query_as(
            "SELECT policy_id, tenant_id, subject_type, subject_id,
                    action, decision, reason, evaluated_at_ms
             FROM guardrail_evaluations
             WHERE tenant_id = ?
             ORDER BY evaluated_at_ms DESC, policy_id ASC
             LIMIT ?",
        )
        .bind(tenant_id.as_str())
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(GuardrailEvaluationRow::into_record)
            .collect()
    }
}

// ── RFC-025 Phase 2a.2 milestone 3: RetentionPolicyReadModel ────────────────

#[derive(sqlx::FromRow)]
struct RetentionPolicyRow {
    tenant_id: String,
    policy_id: String,
    full_history_days: i32,
    current_state_days: i32,
    max_events_per_entity: Option<i64>,
}

impl RetentionPolicyRow {
    fn into_record(self) -> cairn_domain::RetentionPolicy {
        let max_events_per_entity = self
            .max_events_per_entity
            .map(|v| u32::try_from(v.max(0)).unwrap_or(u32::MAX))
            .unwrap_or(0);
        cairn_domain::RetentionPolicy {
            policy_id: self.policy_id,
            tenant_id: cairn_domain::TenantId::new(self.tenant_id),
            full_history_days: u32::try_from(self.full_history_days.max(0)).unwrap_or(0),
            current_state_days: u32::try_from(self.current_state_days.max(0)).unwrap_or(0),
            max_events_per_entity,
        }
    }
}

#[async_trait]
impl crate::projections::RetentionPolicyReadModel for SqliteAdapter {
    async fn get_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Option<cairn_domain::RetentionPolicy>, StoreError> {
        let row: Option<RetentionPolicyRow> = sqlx::query_as(
            "SELECT tenant_id, policy_id, full_history_days, current_state_days,
                    max_events_per_entity
             FROM retention_policies
             WHERE tenant_id = ?",
        )
        .bind(tenant_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(RetentionPolicyRow::into_record))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_domain::tool_invocation::{ToolInvocationState, ToolInvocationTarget};
    use cairn_domain::{ApprovalDecision, CheckpointDisposition, TaskState};

    #[tokio::test]
    async fn sqlite_adapter_reads_tool_invocations_in_request_order() {
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let target = serde_json::to_string(&ToolInvocationTarget::Builtin {
            tool_name: "fs.read".to_owned(),
        })
        .unwrap();

        sqlx::query(
            "INSERT INTO sessions (
                session_id, tenant_id, workspace_id, project_id, state, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("sess_1")
        .bind("tenant")
        .bind("workspace")
        .bind("project")
        .bind("open")
        .bind(1_i64)
        .bind(1_i64)
        .bind(1_i64)
        .execute(adapter.pool())
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO runs (
                run_id, session_id, parent_run_id, tenant_id, workspace_id, project_id,
                state, failure_class, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("run_1")
        .bind("sess_1")
        .bind(Option::<&str>::None)
        .bind("tenant")
        .bind("workspace")
        .bind("project")
        .bind("running")
        .bind(Option::<&str>::None)
        .bind(1_i64)
        .bind(1_i64)
        .bind(1_i64)
        .execute(adapter.pool())
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO tool_invocations (
                invocation_id, tenant_id, workspace_id, project_id, session_id, run_id, task_id,
                target, execution_class, state, outcome, error_message, version,
                requested_at_ms, started_at_ms, finished_at_ms, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("tool_new")
        .bind("tenant")
        .bind("workspace")
        .bind("project")
        .bind(Some("sess_1"))
        .bind(Some("run_1"))
        .bind(Option::<&str>::None)
        .bind(&target)
        .bind("sandboxed_process")
        .bind("canceled")
        .bind(Some("canceled"))
        .bind(Some("canceled"))
        .bind(2_i64)
        .bind(200_i64)
        .bind(Some(201_i64))
        .bind(Some(205_i64))
        .bind(200_i64)
        .bind(205_i64)
        .execute(adapter.pool())
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO tool_invocations (
                invocation_id, tenant_id, workspace_id, project_id, session_id, run_id, task_id,
                target, execution_class, state, outcome, error_message, version,
                requested_at_ms, started_at_ms, finished_at_ms, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("tool_old")
        .bind("tenant")
        .bind("workspace")
        .bind("project")
        .bind(Some("sess_1"))
        .bind(Some("run_1"))
        .bind(Option::<&str>::None)
        .bind(&target)
        .bind("supervised_process")
        .bind("started")
        .bind(Option::<&str>::None)
        .bind(Option::<&str>::None)
        .bind(1_i64)
        .bind(100_i64)
        .bind(Some(101_i64))
        .bind(Option::<i64>::None)
        .bind(100_i64)
        .bind(101_i64)
        .execute(adapter.pool())
        .await
        .unwrap();

        let records = ToolInvocationReadModel::list_by_run(&adapter, &RunId::new("run_1"), 10, 0)
            .await
            .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].invocation_id.as_str(), "tool_old");
        assert_eq!(records[1].invocation_id.as_str(), "tool_new");
        assert_eq!(records[1].state, ToolInvocationState::Canceled);
        assert_eq!(
            records[1].outcome,
            Some(ToolInvocationOutcomeKind::Canceled)
        );
        assert_eq!(records[1].error_message.as_deref(), Some("canceled"));
    }

    #[tokio::test]
    async fn sqlite_adapter_reads_sessions_and_runs() {
        let adapter = SqliteAdapter::in_memory().await.unwrap();

        sqlx::query(
            "INSERT INTO sessions (
                session_id, tenant_id, workspace_id, project_id, state, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("sess_1")
        .bind("tenant")
        .bind("workspace")
        .bind("project")
        .bind("open")
        .bind(1_i64)
        .bind(10_i64)
        .bind(10_i64)
        .bind("sess_2")
        .bind("tenant")
        .bind("workspace")
        .bind("project")
        .bind("completed")
        .bind(2_i64)
        .bind(20_i64)
        .bind(25_i64)
        .execute(adapter.pool())
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO runs (
                run_id, session_id, parent_run_id, tenant_id, workspace_id, project_id,
                state, failure_class, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("run_root")
        .bind("sess_1")
        .bind(Option::<&str>::None)
        .bind("tenant")
        .bind("workspace")
        .bind("project")
        .bind("running")
        .bind(Option::<&str>::None)
        .bind(1_i64)
        .bind(100_i64)
        .bind(101_i64)
        .bind("run_child")
        .bind("sess_1")
        .bind(Some("run_root"))
        .bind("tenant")
        .bind("workspace")
        .bind("project")
        .bind("completed")
        .bind(Option::<&str>::None)
        .bind(2_i64)
        .bind(110_i64)
        .bind(120_i64)
        .execute(adapter.pool())
        .await
        .unwrap();

        let sessions = SessionReadModel::list_by_project(
            &adapter,
            &ProjectKey::new("tenant", "workspace", "project"),
            10,
            0,
        )
        .await
        .unwrap();
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].session_id.as_str(), "sess_1");

        let runs = RunReadModel::list_by_session(&adapter, &SessionId::new("sess_1"), 10, 0)
            .await
            .unwrap();
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].run_id.as_str(), "run_root");
        assert!(
            RunReadModel::any_non_terminal(&adapter, &SessionId::new("sess_1"))
                .await
                .unwrap()
        );

        let latest_root = RunReadModel::latest_root_run(&adapter, &SessionId::new("sess_1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest_root.run_id.as_str(), "run_root");
    }

    #[tokio::test]
    async fn sqlite_adapter_reads_task_approval_checkpoint_and_mailbox_models() {
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let project = ProjectKey::new("tenant", "workspace", "project");

        sqlx::query(
            "INSERT INTO sessions (
                session_id, tenant_id, workspace_id, project_id, state, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("sess_1")
        .bind("tenant")
        .bind("workspace")
        .bind("project")
        .bind("open")
        .bind(1_i64)
        .bind(10_i64)
        .bind(10_i64)
        .execute(adapter.pool())
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO runs (
                run_id, session_id, parent_run_id, tenant_id, workspace_id, project_id,
                state, failure_class, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("run_root")
        .bind("sess_1")
        .bind(Option::<&str>::None)
        .bind("tenant")
        .bind("workspace")
        .bind("project")
        .bind("running")
        .bind(Option::<&str>::None)
        .bind(1_i64)
        .bind(20_i64)
        .bind(20_i64)
        .execute(adapter.pool())
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO tasks (
                task_id, tenant_id, workspace_id, project_id, parent_run_id, parent_task_id,
                state, failure_class, lease_owner, lease_expires_at, lease_version, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("task_expired")
        .bind("tenant")
        .bind("workspace")
        .bind("project")
        .bind(Some("run_root"))
        .bind(Option::<&str>::None)
        .bind("leased")
        .bind(Option::<&str>::None)
        .bind(Some("worker-a"))
        .bind(Some(50_i64))
        .bind(1_i64)
        .bind(1_i64)
        .bind(30_i64)
        .bind(40_i64)
        .bind("task_queued")
        .bind("tenant")
        .bind("workspace")
        .bind("project")
        .bind(Some("run_root"))
        .bind(Option::<&str>::None)
        .bind("queued")
        .bind(Option::<&str>::None)
        .bind(Option::<&str>::None)
        .bind(Option::<i64>::None)
        .bind(0_i64)
        .bind(1_i64)
        .bind(35_i64)
        .bind(35_i64)
        .execute(adapter.pool())
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO approvals (
                approval_id, tenant_id, workspace_id, project_id, run_id, task_id,
                requirement, decision, title, description, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("approval_pending")
        .bind("tenant")
        .bind("workspace")
        .bind("project")
        .bind(Some("run_root"))
        .bind(Some("task_queued"))
        .bind("required")
        .bind(Option::<&str>::None)
        .bind(Option::<&str>::None) // title
        .bind(Option::<&str>::None) // description
        .bind(1_i64)
        .bind(40_i64)
        .bind(40_i64)
        .bind("approval_resolved")
        .bind("tenant")
        .bind("workspace")
        .bind("project")
        .bind(Some("run_root"))
        .bind(Some("task_expired"))
        .bind("required")
        .bind(Some("approved"))
        .bind(Option::<&str>::None) // title
        .bind(Option::<&str>::None) // description
        .bind(2_i64)
        .bind(45_i64)
        .bind(46_i64)
        .execute(adapter.pool())
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO checkpoints (
                checkpoint_id, tenant_id, workspace_id, project_id, run_id, disposition, version, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("cp_old")
        .bind("tenant")
        .bind("workspace")
        .bind("project")
        .bind("run_root")
        .bind("superseded")
        .bind(1_i64)
        .bind(50_i64)
        .bind("cp_latest")
        .bind("tenant")
        .bind("workspace")
        .bind("project")
        .bind("run_root")
        .bind("latest")
        .bind(2_i64)
        .bind(60_i64)
        .execute(adapter.pool())
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO mailbox_messages (
                message_id, tenant_id, workspace_id, project_id, run_id, task_id, version, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("msg_run")
        .bind("tenant")
        .bind("workspace")
        .bind("project")
        .bind(Some("run_root"))
        .bind(Option::<&str>::None)
        .bind(1_i64)
        .bind(70_i64)
        .bind("msg_task")
        .bind("tenant")
        .bind("workspace")
        .bind("project")
        .bind(Option::<&str>::None)
        .bind(Some("task_expired"))
        .bind(1_i64)
        .bind(80_i64)
        .execute(adapter.pool())
        .await
        .unwrap();

        let queued = TaskReadModel::list_by_state(&adapter, &project, TaskState::Queued, 10)
            .await
            .unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].task_id.as_str(), "task_queued");

        let expired = TaskReadModel::list_expired_leases(&adapter, 60, 10)
            .await
            .unwrap();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].task_id.as_str(), "task_expired");

        let pending = ApprovalReadModel::list_pending(&adapter, &project, 10, 0)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].approval_id.as_str(), "approval_pending");
        assert_eq!(pending[0].decision, None);

        let resolved = ApprovalReadModel::get(&adapter, &ApprovalId::new("approval_resolved"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resolved.decision, Some(ApprovalDecision::Approved));

        let latest = CheckpointReadModel::latest_for_run(&adapter, &RunId::new("run_root"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.checkpoint_id.as_str(), "cp_latest");
        assert_eq!(latest.disposition, CheckpointDisposition::Latest);

        let checkpoints = CheckpointReadModel::list_by_run(&adapter, &RunId::new("run_root"), 10)
            .await
            .unwrap();
        assert_eq!(checkpoints.len(), 2);
        assert_eq!(checkpoints[0].checkpoint_id.as_str(), "cp_latest");

        let run_messages = MailboxReadModel::list_by_run(&adapter, &RunId::new("run_root"), 10, 0)
            .await
            .unwrap();
        assert_eq!(run_messages.len(), 1);
        assert_eq!(run_messages[0].message_id.as_str(), "msg_run");

        let task_messages =
            MailboxReadModel::list_by_task(&adapter, &TaskId::new("task_expired"), 10, 0)
                .await
                .unwrap();
        assert_eq!(task_messages.len(), 1);
        assert_eq!(task_messages[0].message_id.as_str(), "msg_task");
    }
}

// ── RFC-025 Phase 1.5a: TriggerReadModel / RunTemplateReadModel /
// TriggerFireReadModel ────────────────────────────────────────────────
//
// sqlite parity with the pg adapter. Same three read models, same
// indexed predicates, same sort-order-by-trigger-id for deterministic
// evaluation. Parity harness asserts byte-equal records across backends.

#[derive(sqlx::FromRow)]
struct TriggerRow {
    trigger_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    name: String,
    description: Option<String>,
    signal_type: String,
    plugin_id: Option<String>,
    conditions_json: String,
    run_template_id: String,
    state: String,
    state_reason: Option<String>,
    suspension_reason: Option<String>,
    state_since: Option<i64>,
    max_per_minute: i64,
    max_burst: i64,
    max_chain_depth: i64,
    created_by: String,
    created_at: i64,
    updated_at: i64,
}

const TRIGGER_SELECT_COLS: &str = "trigger_id, tenant_id, workspace_id, project_id, \
    name, description, signal_type, plugin_id, conditions_json, run_template_id, \
    state, state_reason, suspension_reason, state_since, \
    max_per_minute, max_burst, max_chain_depth, created_by, created_at, updated_at";

fn sqlite_row_to_trigger_record(
    row: TriggerRow,
) -> Result<crate::projections::TriggerRecord, StoreError> {
    let state = crate::projections::TriggerStateKind::parse_str(&row.state)?;
    Ok(crate::projections::TriggerRecord {
        trigger_id: cairn_domain::ids::TriggerId::new(row.trigger_id),
        project: ProjectKey::new(row.tenant_id, row.workspace_id, row.project_id),
        name: row.name,
        description: row.description,
        signal_type: row.signal_type,
        plugin_id: row.plugin_id,
        conditions_json: row.conditions_json,
        run_template_id: cairn_domain::ids::RunTemplateId::new(row.run_template_id),
        state,
        state_reason: row.state_reason,
        suspension_reason: row.suspension_reason,
        state_since: row.state_since.map(|v| v.max(0) as u64),
        // Saturating down-casts so a corrupted / out-of-range row can't
        // silently wrap to a tiny value (PR #569 review). `sqlite::i64`
        // column type means we clamp to u32/u8 max rather than wrap.
        max_per_minute: row.max_per_minute.clamp(0, u32::MAX as i64) as u32,
        max_burst: row.max_burst.clamp(0, u32::MAX as i64) as u32,
        max_chain_depth: row.max_chain_depth.clamp(0, u8::MAX as i64) as u8,
        created_by: OperatorId::new(row.created_by),
        created_at: row.created_at.max(0) as u64,
        updated_at: row.updated_at.max(0) as u64,
    })
}

#[async_trait]
impl crate::projections::TriggerReadModel for SqliteAdapter {
    async fn get_trigger(
        &self,
        trigger_id: &cairn_domain::ids::TriggerId,
    ) -> Result<Option<crate::projections::TriggerRecord>, StoreError> {
        let sql = format!("SELECT {TRIGGER_SELECT_COLS} FROM triggers WHERE trigger_id = ?");
        let row: Option<TriggerRow> = sqlx::query_as(&sql)
            .bind(trigger_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(sqlite_row_to_trigger_record).transpose()
    }

    async fn list_triggers_by_project(
        &self,
        project: &ProjectKey,
    ) -> Result<Vec<crate::projections::TriggerRecord>, StoreError> {
        let sql = format!(
            "SELECT {TRIGGER_SELECT_COLS} FROM triggers \
             WHERE tenant_id = ? AND workspace_id = ? AND project_id = ? \
             ORDER BY trigger_id ASC"
        );
        let rows: Vec<TriggerRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter().map(sqlite_row_to_trigger_record).collect()
    }

    async fn list_matching_enabled(
        &self,
        project: &ProjectKey,
        signal_type: &str,
        plugin_id: &str,
    ) -> Result<Vec<crate::projections::TriggerRecord>, StoreError> {
        let sql = format!(
            "SELECT {TRIGGER_SELECT_COLS} FROM triggers \
             WHERE tenant_id = ? AND workspace_id = ? AND project_id = ? \
             AND state = 'enabled' \
             AND signal_type = ? \
             AND (plugin_id IS NULL OR plugin_id = ?) \
             ORDER BY trigger_id ASC"
        );
        let rows: Vec<TriggerRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .bind(signal_type)
            .bind(plugin_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter().map(sqlite_row_to_trigger_record).collect()
    }
}

#[derive(sqlx::FromRow)]
struct RunTemplateRow {
    template_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    name: String,
    description: Option<String>,
    default_mode: String,
    system_prompt: String,
    initial_user_message: Option<String>,
    plugin_allowlist_json: Option<String>,
    tool_allowlist_json: Option<String>,
    budget_max_tokens: Option<i64>,
    budget_max_wall_clock_ms: Option<i64>,
    budget_max_iterations: Option<i64>,
    budget_exploration_budget_share: Option<f64>,
    sandbox_hint: Option<String>,
    required_fields_json: String,
    created_by: String,
    created_at: i64,
    updated_at: i64,
}

const RUN_TEMPLATE_SELECT_COLS: &str = "template_id, tenant_id, workspace_id, project_id, \
    name, description, default_mode, system_prompt, initial_user_message, \
    plugin_allowlist_json, tool_allowlist_json, \
    budget_max_tokens, budget_max_wall_clock_ms, budget_max_iterations, \
    budget_exploration_budget_share, sandbox_hint, required_fields_json, \
    created_by, created_at, updated_at";

fn sqlite_row_to_run_template_record(row: RunTemplateRow) -> crate::projections::RunTemplateRecord {
    crate::projections::RunTemplateRecord {
        template_id: cairn_domain::ids::RunTemplateId::new(row.template_id),
        project: ProjectKey::new(row.tenant_id, row.workspace_id, row.project_id),
        name: row.name,
        description: row.description,
        default_mode: row.default_mode,
        system_prompt: row.system_prompt,
        initial_user_message: row.initial_user_message,
        plugin_allowlist_json: row.plugin_allowlist_json,
        tool_allowlist_json: row.tool_allowlist_json,
        budget_max_tokens: row.budget_max_tokens.map(|v| v.max(0) as u64),
        budget_max_wall_clock_ms: row.budget_max_wall_clock_ms.map(|v| v.max(0) as u64),
        budget_max_iterations: row.budget_max_iterations.map(|v| v.max(0) as u32),
        budget_exploration_budget_share: row.budget_exploration_budget_share.map(|v| v as f32),
        sandbox_hint: row.sandbox_hint,
        required_fields_json: row.required_fields_json,
        created_by: OperatorId::new(row.created_by),
        created_at: row.created_at.max(0) as u64,
        updated_at: row.updated_at.max(0) as u64,
    }
}

#[async_trait]
impl crate::projections::RunTemplateReadModel for SqliteAdapter {
    async fn get_template(
        &self,
        template_id: &cairn_domain::ids::RunTemplateId,
    ) -> Result<Option<crate::projections::RunTemplateRecord>, StoreError> {
        let sql =
            format!("SELECT {RUN_TEMPLATE_SELECT_COLS} FROM run_templates WHERE template_id = ?");
        let row: Option<RunTemplateRow> = sqlx::query_as(&sql)
            .bind(template_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(sqlite_row_to_run_template_record))
    }

    async fn list_templates_by_project(
        &self,
        project: &ProjectKey,
    ) -> Result<Vec<crate::projections::RunTemplateRecord>, StoreError> {
        let sql = format!(
            "SELECT {RUN_TEMPLATE_SELECT_COLS} FROM run_templates \
             WHERE tenant_id = ? AND workspace_id = ? AND project_id = ? \
             ORDER BY template_id ASC"
        );
        let rows: Vec<RunTemplateRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(sqlite_row_to_run_template_record)
            .collect())
    }

    async fn triggers_referencing_template(
        &self,
        template_id: &cairn_domain::ids::RunTemplateId,
    ) -> Result<Vec<cairn_domain::ids::TriggerId>, StoreError> {
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT trigger_id FROM triggers WHERE run_template_id = ?")
                .bind(template_id.as_str())
                .fetch_all(&self.pool)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(|(id,)| cairn_domain::ids::TriggerId::new(id))
            .collect())
    }
}

#[async_trait]
impl crate::projections::TriggerFireReadModel for SqliteAdapter {
    async fn has_fired(
        &self,
        trigger_id: &cairn_domain::ids::TriggerId,
        signal_id: &str,
    ) -> Result<bool, StoreError> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT 1 FROM trigger_fires \
             WHERE trigger_id = ? AND signal_id = ? AND outcome = 'fired' \
             LIMIT 1",
        )
        .bind(trigger_id.as_str())
        .bind(signal_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.is_some())
    }

    async fn count_fires_since(
        &self,
        trigger_id: &cairn_domain::ids::TriggerId,
        since_ms: u64,
    ) -> Result<u32, StoreError> {
        let row: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM trigger_fires \
             WHERE trigger_id = ? AND outcome = 'fired' AND at_ms > ?",
        )
        .bind(trigger_id.as_str())
        .bind(since_ms as i64)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.0.max(0) as u32)
    }

    async fn count_project_fires_since(
        &self,
        project: &ProjectKey,
        since_ms: u64,
    ) -> Result<u32, StoreError> {
        let row: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM trigger_fires \
             WHERE tenant_id = ? AND workspace_id = ? AND project_id = ? \
             AND outcome = 'fired' AND at_ms > ?",
        )
        .bind(project.tenant_id.as_str())
        .bind(project.workspace_id.as_str())
        .bind(project.project_id.as_str())
        .bind(since_ms as i64)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.0.max(0) as u32)
    }
}

// ── RFC-025 Phase 3: ProviderConnectionReadModel + ProviderBindingReadModel ──
//
// SQLite parity with the pg impls in `pg/adapter.rs`. Row structs +
// `into_record` mirror the pg side exactly; SQL dialect differences
// (`?` vs `$N` placeholders, `active = 1` vs `active = TRUE`) are the
// only delta. Every sort-tiebreaker matches so the cross-backend parity
// test in `projection_parity.rs` holds.

#[derive(sqlx::FromRow)]
struct SqliteProviderConnectionRow {
    provider_connection_id: String,
    tenant_id: String,
    provider_family: String,
    adapter_type: String,
    supported_models_json: String,
    status: String,
    created_at: i64,
}

impl SqliteProviderConnectionRow {
    fn into_record(self) -> Result<cairn_domain::providers::ProviderConnectionRecord, StoreError> {
        let status = match self.status.as_str() {
            "active" => cairn_domain::providers::ProviderConnectionStatus::Active,
            "disabled" => cairn_domain::providers::ProviderConnectionStatus::Disabled,
            other => {
                return Err(StoreError::Internal(format!(
                    "provider_connections.status: unknown value {other:?}"
                )))
            }
        };
        let supported_models: Vec<String> = serde_json::from_str(&self.supported_models_json)
            .map_err(|e| {
                StoreError::Internal(format!(
                    "provider_connections.supported_models_json parse error: {e}"
                ))
            })?;
        Ok(cairn_domain::providers::ProviderConnectionRecord {
            provider_connection_id: cairn_domain::ProviderConnectionId::new(
                self.provider_connection_id,
            ),
            tenant_id: cairn_domain::TenantId::new(self.tenant_id),
            provider_family: self.provider_family,
            adapter_type: self.adapter_type,
            supported_models,
            status,
            created_at: self.created_at.max(0) as u64,
        })
    }
}

const PROVIDER_CONNECTION_SELECT_COLS_SQLITE: &str = "provider_connection_id, tenant_id, \
     provider_family, adapter_type, supported_models_json, status, created_at";

#[async_trait]
impl crate::projections::ProviderConnectionReadModel for SqliteAdapter {
    async fn get(
        &self,
        id: &cairn_domain::ProviderConnectionId,
    ) -> Result<Option<cairn_domain::providers::ProviderConnectionRecord>, StoreError> {
        let sql = format!(
            "SELECT {PROVIDER_CONNECTION_SELECT_COLS_SQLITE} FROM provider_connections
             WHERE provider_connection_id = ?"
        );
        let row: Option<SqliteProviderConnectionRow> = sqlx::query_as(&sql)
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(SqliteProviderConnectionRow::into_record)
            .transpose()
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::providers::ProviderConnectionRecord>, StoreError> {
        let sql = format!(
            "SELECT {PROVIDER_CONNECTION_SELECT_COLS_SQLITE} FROM provider_connections
             WHERE tenant_id = ?
             ORDER BY created_at ASC, provider_connection_id ASC
             LIMIT ? OFFSET ?"
        );
        let rows: Vec<SqliteProviderConnectionRow> = sqlx::query_as(&sql)
            .bind(tenant_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(SqliteProviderConnectionRow::into_record)
            .collect()
    }
}

#[derive(sqlx::FromRow)]
struct SqliteProviderBindingRow {
    provider_binding_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    provider_connection_id: String,
    provider_model_id: String,
    operation_kind: String,
    settings_json: String,
    active: bool,
    created_at: i64,
}

impl SqliteProviderBindingRow {
    fn into_record(self) -> Result<cairn_domain::providers::ProviderBindingRecord, StoreError> {
        let operation_kind = match self.operation_kind.as_str() {
            "generate" => cairn_domain::providers::OperationKind::Generate,
            "embed" => cairn_domain::providers::OperationKind::Embed,
            "rerank" => cairn_domain::providers::OperationKind::Rerank,
            other => {
                return Err(StoreError::Internal(format!(
                    "provider_bindings.operation_kind: unknown value {other:?}"
                )))
            }
        };
        let settings: cairn_domain::providers::ProviderBindingSettings =
            serde_json::from_str(&self.settings_json).map_err(|e| {
                StoreError::Internal(format!("provider_bindings.settings_json parse error: {e}"))
            })?;
        Ok(cairn_domain::providers::ProviderBindingRecord {
            provider_binding_id: cairn_domain::ProviderBindingId::new(self.provider_binding_id),
            project: cairn_domain::ProjectKey {
                tenant_id: cairn_domain::TenantId::new(self.tenant_id),
                workspace_id: cairn_domain::WorkspaceId::new(self.workspace_id),
                project_id: cairn_domain::ProjectId::new(self.project_id),
            },
            provider_connection_id: cairn_domain::ProviderConnectionId::new(
                self.provider_connection_id,
            ),
            provider_model_id: cairn_domain::ProviderModelId::new(self.provider_model_id),
            operation_kind,
            settings,
            active: self.active,
            created_at: self.created_at.max(0) as u64,
        })
    }
}

const PROVIDER_BINDING_SELECT_COLS_SQLITE: &str = "provider_binding_id, tenant_id, workspace_id, \
     project_id, provider_connection_id, provider_model_id, operation_kind, \
     settings_json, active, created_at";

#[async_trait]
impl crate::projections::ProviderBindingReadModel for SqliteAdapter {
    async fn get(
        &self,
        id: &cairn_domain::ProviderBindingId,
    ) -> Result<Option<cairn_domain::providers::ProviderBindingRecord>, StoreError> {
        let sql = format!(
            "SELECT {PROVIDER_BINDING_SELECT_COLS_SQLITE} FROM provider_bindings
             WHERE provider_binding_id = ?"
        );
        let row: Option<SqliteProviderBindingRow> = sqlx::query_as(&sql)
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(SqliteProviderBindingRow::into_record).transpose()
    }

    async fn list_by_project(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::providers::ProviderBindingRecord>, StoreError> {
        let sql = format!(
            "SELECT {PROVIDER_BINDING_SELECT_COLS_SQLITE} FROM provider_bindings
             WHERE tenant_id = ? AND workspace_id = ? AND project_id = ?
             ORDER BY created_at ASC, provider_binding_id ASC
             LIMIT ? OFFSET ?"
        );
        let rows: Vec<SqliteProviderBindingRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(SqliteProviderBindingRow::into_record)
            .collect()
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::providers::ProviderBindingRecord>, StoreError> {
        let sql = format!(
            "SELECT {PROVIDER_BINDING_SELECT_COLS_SQLITE} FROM provider_bindings
             WHERE tenant_id = ?
             ORDER BY created_at ASC, provider_binding_id ASC
             LIMIT ? OFFSET ?"
        );
        let rows: Vec<SqliteProviderBindingRow> = sqlx::query_as(&sql)
            .bind(tenant_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(SqliteProviderBindingRow::into_record)
            .collect()
    }

    async fn list_active(
        &self,
        project: &cairn_domain::ProjectKey,
        operation: cairn_domain::providers::OperationKind,
    ) -> Result<Vec<cairn_domain::providers::ProviderBindingRecord>, StoreError> {
        let operation_str = match operation {
            cairn_domain::providers::OperationKind::Generate => "generate",
            cairn_domain::providers::OperationKind::Embed => "embed",
            cairn_domain::providers::OperationKind::Rerank => "rerank",
        };
        // Sort tiebreaker matches in_memory + pg (created_at ASC,
        // provider_binding_id ASC) — cross-backend parity asserted in
        // projection_parity.rs.
        let sql = format!(
            "SELECT {PROVIDER_BINDING_SELECT_COLS_SQLITE} FROM provider_bindings
             WHERE tenant_id = ? AND workspace_id = ? AND project_id = ?
               AND active = 1
               AND operation_kind = ?
             ORDER BY created_at ASC, provider_binding_id ASC"
        );
        let rows: Vec<SqliteProviderBindingRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .bind(operation_str)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(SqliteProviderBindingRow::into_record)
            .collect()
    }
}

// ── RFC-025 Phase 2b.1: audit_log_entries read model ────────────────

#[derive(sqlx::FromRow)]
struct SqliteAuditLogEntryRow {
    entry_id: String,
    tenant_id: String,
    actor_id: String,
    action: String,
    resource_type: String,
    resource_id: String,
    outcome: String,
    metadata_json: String,
    occurred_at_ms: i64,
}

impl SqliteAuditLogEntryRow {
    fn into_record(self) -> Result<crate::projections::AuditLogEntryRecord, StoreError> {
        let outcome = match self.outcome.as_str() {
            "success" => cairn_domain::audit::AuditOutcome::Success,
            "failure" => cairn_domain::audit::AuditOutcome::Failure,
            other => {
                return Err(StoreError::Internal(format!(
                    "audit_log_entries.outcome: unknown value {other:?}"
                )))
            }
        };
        let metadata: serde_json::Value =
            serde_json::from_str(&self.metadata_json).map_err(|e| {
                StoreError::Internal(format!("audit_log_entries.metadata_json parse error: {e}"))
            })?;
        Ok(crate::projections::AuditLogEntryRecord {
            entry_id: self.entry_id,
            tenant_id: cairn_domain::TenantId::new(self.tenant_id),
            actor_id: self.actor_id,
            action: self.action,
            resource_type: self.resource_type,
            resource_id: self.resource_id,
            outcome,
            metadata,
            occurred_at_ms: self.occurred_at_ms.max(0) as u64,
        })
    }
}

const AUDIT_LOG_SELECT_COLS_SQLITE: &str = "entry_id, tenant_id, actor_id, action, \
     resource_type, resource_id, outcome, metadata_json, occurred_at_ms";

#[async_trait]
impl crate::projections::AuditLogReadModel for SqliteAdapter {
    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        since_ms: Option<u64>,
        before_ms: Option<u64>,
        limit: usize,
    ) -> Result<Vec<cairn_domain::AuditLogEntry>, StoreError> {
        // Shared window + overflow handling with pg — see
        // `window_bounds_ms` (Gemini PR #573 review).
        let (since, before) = crate::projections::window_bounds_ms(since_ms, before_ms)?;
        let sql = format!(
            "SELECT {AUDIT_LOG_SELECT_COLS_SQLITE} FROM audit_log_entries
             WHERE tenant_id = ?
               AND occurred_at_ms >= ?
               AND occurred_at_ms < ?
             ORDER BY occurred_at_ms DESC, entry_id DESC
             LIMIT ?"
        );
        let rows: Vec<SqliteAuditLogEntryRow> = sqlx::query_as(&sql)
            .bind(tenant_id.as_str())
            .bind(since)
            .bind(before)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(|r| r.into_record().map(|rec| rec.into_entry()))
            .collect()
    }

    async fn list_by_resource(
        &self,
        resource_type: &str,
        resource_id: &str,
    ) -> Result<Vec<cairn_domain::AuditLogEntry>, StoreError> {
        // Cap mirrors `LIST_BY_RESOURCE_MAX_ROWS` shared with pg +
        // in-memory — Copilot PR #573 review flagged the prior
        // in-memory impl as unbounded.
        let sql = format!(
            "SELECT {AUDIT_LOG_SELECT_COLS_SQLITE} FROM audit_log_entries
             WHERE resource_type = ? AND resource_id = ?
             ORDER BY occurred_at_ms DESC, entry_id DESC
             LIMIT ?"
        );
        let rows: Vec<SqliteAuditLogEntryRow> = sqlx::query_as(&sql)
            .bind(resource_type)
            .bind(resource_id)
            .bind(crate::projections::LIST_BY_RESOURCE_MAX_ROWS as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(|r| r.into_record().map(|rec| rec.into_entry()))
            .collect()
    }
}

// ── RFC-025 Phase 2b.1 m2: scheduled_tasks read model ──────────────

#[derive(sqlx::FromRow)]
struct SqliteScheduledTaskRow {
    scheduled_task_id: String,
    tenant_id: String,
    name: String,
    cron_expression: String,
    last_run_at: Option<i64>,
    next_run_at: Option<i64>,
    enabled: bool,
    created_at: i64,
    updated_at: i64,
}

impl SqliteScheduledTaskRow {
    fn into_record(self) -> cairn_domain::ScheduledTaskRecord {
        cairn_domain::ScheduledTaskRecord {
            scheduled_task_id: cairn_domain::ScheduledTaskId::new(self.scheduled_task_id),
            tenant_id: cairn_domain::TenantId::new(self.tenant_id),
            name: self.name,
            cron_expression: self.cron_expression,
            last_run_at: self.last_run_at.map(|v| v.max(0) as u64),
            next_run_at: self.next_run_at.map(|v| v.max(0) as u64),
            enabled: self.enabled,
            created_at: self.created_at.max(0) as u64,
            updated_at: self.updated_at.max(0) as u64,
        }
    }
}

const SCHEDULED_TASK_SELECT_COLS_SQLITE: &str = "scheduled_task_id, tenant_id, name, \
     cron_expression, last_run_at, next_run_at, enabled, created_at, updated_at";

#[async_trait]
impl crate::projections::ScheduledTaskReadModel for SqliteAdapter {
    async fn get(
        &self,
        id: &cairn_domain::ScheduledTaskId,
    ) -> Result<Option<cairn_domain::ScheduledTaskRecord>, StoreError> {
        let sql = format!(
            "SELECT {SCHEDULED_TASK_SELECT_COLS_SQLITE} FROM scheduled_tasks
             WHERE scheduled_task_id = ?"
        );
        let row: Option<SqliteScheduledTaskRow> = sqlx::query_as(&sql)
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(SqliteScheduledTaskRow::into_record))
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::ScheduledTaskRecord>, StoreError> {
        let sql = format!(
            "SELECT {SCHEDULED_TASK_SELECT_COLS_SQLITE} FROM scheduled_tasks
             WHERE tenant_id = ?
             ORDER BY created_at ASC, scheduled_task_id ASC
             LIMIT ? OFFSET ?"
        );
        let rows: Vec<SqliteScheduledTaskRow> = sqlx::query_as(&sql)
            .bind(tenant_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(SqliteScheduledTaskRow::into_record)
            .collect())
    }

    async fn list_due(
        &self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<cairn_domain::ScheduledTaskRecord>, StoreError> {
        let now = i64::try_from(now_ms)
            .map_err(|_| StoreError::Internal("now_ms exceeds i64::MAX".into()))?;
        let sql = format!(
            "SELECT {SCHEDULED_TASK_SELECT_COLS_SQLITE} FROM scheduled_tasks
             WHERE enabled = 1 AND next_run_at IS NOT NULL AND next_run_at <= ?
             ORDER BY next_run_at ASC, scheduled_task_id ASC
             LIMIT ?"
        );
        let rows: Vec<SqliteScheduledTaskRow> = sqlx::query_as(&sql)
            .bind(now)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(SqliteScheduledTaskRow::into_record)
            .collect())
    }
}

// ── RFC-025 Phase 2b.1 m3: outcomes read model ────────────────────

#[derive(sqlx::FromRow)]
struct SqliteOutcomeRow {
    outcome_id: String,
    run_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    agent_type: String,
    predicted_confidence: f64,
    actual_outcome: String,
    recorded_at: i64,
}

impl SqliteOutcomeRow {
    fn into_record(self) -> Result<crate::projections::OutcomeRecord, StoreError> {
        let actual_outcome = match self.actual_outcome.as_str() {
            "success" => cairn_domain::events::ActualOutcome::Success,
            "failure" => cairn_domain::events::ActualOutcome::Failure,
            "partial" => cairn_domain::events::ActualOutcome::Partial,
            other => {
                return Err(StoreError::Internal(format!(
                    "outcomes.actual_outcome: unknown value {other:?}"
                )))
            }
        };
        Ok(crate::projections::OutcomeRecord {
            outcome_id: cairn_domain::OutcomeId::new(self.outcome_id),
            run_id: cairn_domain::RunId::new(self.run_id),
            project: cairn_domain::ProjectKey {
                tenant_id: cairn_domain::TenantId::new(self.tenant_id),
                workspace_id: cairn_domain::WorkspaceId::new(self.workspace_id),
                project_id: cairn_domain::ProjectId::new(self.project_id),
            },
            agent_type: self.agent_type,
            predicted_confidence: self.predicted_confidence,
            actual_outcome,
            recorded_at: self.recorded_at.max(0) as u64,
        })
    }
}

const OUTCOME_SELECT_COLS_SQLITE: &str = "outcome_id, run_id, tenant_id, workspace_id, \
     project_id, agent_type, predicted_confidence, actual_outcome, recorded_at";

#[async_trait]
impl crate::projections::OutcomeReadModel for SqliteAdapter {
    async fn get(
        &self,
        outcome_id: &cairn_domain::OutcomeId,
    ) -> Result<Option<crate::projections::OutcomeRecord>, StoreError> {
        let sql = format!(
            "SELECT {OUTCOME_SELECT_COLS_SQLITE} FROM outcomes
             WHERE outcome_id = ?"
        );
        let row: Option<SqliteOutcomeRow> = sqlx::query_as(&sql)
            .bind(outcome_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(SqliteOutcomeRow::into_record).transpose()
    }

    async fn list_by_run(
        &self,
        run_id: &cairn_domain::RunId,
        limit: usize,
    ) -> Result<Vec<crate::projections::OutcomeRecord>, StoreError> {
        let sql = format!(
            "SELECT {OUTCOME_SELECT_COLS_SQLITE} FROM outcomes
             WHERE run_id = ?
             ORDER BY recorded_at ASC, outcome_id ASC
             LIMIT ?"
        );
        let rows: Vec<SqliteOutcomeRow> = sqlx::query_as(&sql)
            .bind(run_id.as_str())
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(SqliteOutcomeRow::into_record)
            .collect()
    }

    async fn list_by_project(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::OutcomeRecord>, StoreError> {
        let sql = format!(
            "SELECT {OUTCOME_SELECT_COLS_SQLITE} FROM outcomes
             WHERE tenant_id = ? AND workspace_id = ? AND project_id = ?
             ORDER BY recorded_at ASC, outcome_id ASC
             LIMIT ? OFFSET ?"
        );
        let rows: Vec<SqliteOutcomeRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(SqliteOutcomeRow::into_record)
            .collect()
    }
}

// ── RFC-025 Phase 2b.1 m4: plan_reviews read model (RFC 018) ─────

#[derive(sqlx::FromRow)]
struct SqlitePlanReviewRow {
    plan_run_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    session_id: String,
    plan_markdown: String,
    state: String,
    proposed_at: i64,
    resolved_by: Option<String>,
    resolved_at: Option<i64>,
    reviewer_comments: Option<String>,
    rejection_reason: Option<String>,
    revision_run_id: Option<String>,
}

impl SqlitePlanReviewRow {
    fn into_record(self) -> Result<crate::projections::PlanReviewRecord, StoreError> {
        let state = match self.state.as_str() {
            "proposed" => crate::projections::PlanReviewState::Proposed,
            "approved" => crate::projections::PlanReviewState::Approved,
            "rejected" => crate::projections::PlanReviewState::Rejected,
            "revision_requested" => crate::projections::PlanReviewState::RevisionRequested,
            other => {
                return Err(StoreError::Internal(format!(
                    "plan_reviews.state: unknown value {other:?}"
                )))
            }
        };
        Ok(crate::projections::PlanReviewRecord {
            plan_run_id: cairn_domain::RunId::new(self.plan_run_id),
            project: cairn_domain::ProjectKey {
                tenant_id: cairn_domain::TenantId::new(self.tenant_id),
                workspace_id: cairn_domain::WorkspaceId::new(self.workspace_id),
                project_id: cairn_domain::ProjectId::new(self.project_id),
            },
            session_id: cairn_domain::SessionId::new(self.session_id),
            plan_markdown: self.plan_markdown,
            state,
            proposed_at: self.proposed_at.max(0) as u64,
            resolved_by: self.resolved_by.map(cairn_domain::OperatorId::new),
            resolved_at: self.resolved_at.map(|v| v.max(0) as u64),
            reviewer_comments: self.reviewer_comments,
            rejection_reason: self.rejection_reason,
            revision_run_id: self.revision_run_id.map(cairn_domain::RunId::new),
        })
    }
}

const PLAN_REVIEW_SELECT_COLS_SQLITE: &str = "plan_run_id, tenant_id, workspace_id, project_id, \
     session_id, plan_markdown, state, proposed_at, resolved_by, resolved_at, \
     reviewer_comments, rejection_reason, revision_run_id";

#[async_trait]
impl crate::projections::PlanReviewReadModel for SqliteAdapter {
    async fn get(
        &self,
        plan_run_id: &cairn_domain::RunId,
    ) -> Result<Option<crate::projections::PlanReviewRecord>, StoreError> {
        let sql = format!(
            "SELECT {PLAN_REVIEW_SELECT_COLS_SQLITE} FROM plan_reviews
             WHERE plan_run_id = ?"
        );
        let row: Option<SqlitePlanReviewRow> = sqlx::query_as(&sql)
            .bind(plan_run_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(SqlitePlanReviewRow::into_record).transpose()
    }

    async fn list_by_project(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::PlanReviewRecord>, StoreError> {
        let sql = format!(
            "SELECT {PLAN_REVIEW_SELECT_COLS_SQLITE} FROM plan_reviews
             WHERE tenant_id = ? AND workspace_id = ? AND project_id = ?
             ORDER BY proposed_at DESC, plan_run_id DESC
             LIMIT ? OFFSET ?"
        );
        let rows: Vec<SqlitePlanReviewRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(SqlitePlanReviewRow::into_record)
            .collect()
    }

    async fn list_pending_by_project(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
    ) -> Result<Vec<crate::projections::PlanReviewRecord>, StoreError> {
        let sql = format!(
            "SELECT {PLAN_REVIEW_SELECT_COLS_SQLITE} FROM plan_reviews
             WHERE tenant_id = ? AND workspace_id = ? AND project_id = ?
               AND state = 'proposed'
             ORDER BY proposed_at DESC, plan_run_id DESC
             LIMIT ?"
        );
        let rows: Vec<SqlitePlanReviewRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(SqlitePlanReviewRow::into_record)
            .collect()
    }

    async fn list_by_session(
        &self,
        session_id: &cairn_domain::SessionId,
        limit: usize,
    ) -> Result<Vec<crate::projections::PlanReviewRecord>, StoreError> {
        let sql = format!(
            "SELECT {PLAN_REVIEW_SELECT_COLS_SQLITE} FROM plan_reviews
             WHERE session_id = ?
             ORDER BY proposed_at ASC, plan_run_id ASC
             LIMIT ?"
        );
        let rows: Vec<SqlitePlanReviewRow> = sqlx::query_as(&sql)
            .bind(session_id.as_str())
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(SqlitePlanReviewRow::into_record)
            .collect()
    }
}

// ── RFC-025 Phase 2b.2 m1: external_workers read model (GAP-005) ──

#[derive(sqlx::FromRow)]
struct SqliteExternalWorkerRow {
    worker_id: String,
    tenant_id: String,
    display_name: String,
    status: String,
    registered_at: i64,
    updated_at: i64,
    last_heartbeat_ms: i64,
    is_alive: bool,
    active_task_count: i32,
    current_task_id: Option<String>,
}

impl SqliteExternalWorkerRow {
    fn into_record(self) -> cairn_domain::workers::ExternalWorkerRecord {
        cairn_domain::workers::ExternalWorkerRecord {
            worker_id: cairn_domain::WorkerId::new(self.worker_id),
            tenant_id: cairn_domain::TenantId::new(self.tenant_id),
            display_name: self.display_name,
            status: self.status,
            registered_at: self.registered_at.max(0) as u64,
            updated_at: self.updated_at.max(0) as u64,
            health: cairn_domain::workers::WorkerHealth {
                last_heartbeat_ms: self.last_heartbeat_ms.max(0) as u64,
                is_alive: self.is_alive,
                active_task_count: self.active_task_count.max(0) as u32,
            },
            current_task_id: self.current_task_id.map(cairn_domain::TaskId::new),
        }
    }
}

const EXTERNAL_WORKER_SELECT_COLS_SQLITE: &str =
    "worker_id, tenant_id, display_name, status, registered_at, updated_at, \
     last_heartbeat_ms, is_alive, active_task_count, current_task_id";

#[async_trait]
impl crate::projections::ExternalWorkerReadModel for SqliteAdapter {
    async fn get(
        &self,
        id: &cairn_domain::WorkerId,
    ) -> Result<Option<cairn_domain::workers::ExternalWorkerRecord>, StoreError> {
        let sql = format!(
            "SELECT {EXTERNAL_WORKER_SELECT_COLS_SQLITE} FROM external_workers
             WHERE worker_id = ?"
        );
        let row: Option<SqliteExternalWorkerRow> = sqlx::query_as(&sql)
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(SqliteExternalWorkerRow::into_record))
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::workers::ExternalWorkerRecord>, StoreError> {
        // Matches the pg adapter's ORDER BY: ascending registered_at
        // with worker_id tiebreak (mirrors in-memory `sort_by_key`).
        let sql = format!(
            "SELECT {EXTERNAL_WORKER_SELECT_COLS_SQLITE} FROM external_workers
             WHERE tenant_id = ?
             ORDER BY registered_at ASC, worker_id ASC
             LIMIT ? OFFSET ?"
        );
        let rows: Vec<SqliteExternalWorkerRow> = sqlx::query_as(&sql)
            .bind(tenant_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(SqliteExternalWorkerRow::into_record)
            .collect())
    }
}
