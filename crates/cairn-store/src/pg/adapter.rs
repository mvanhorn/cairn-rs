use async_trait::async_trait;
use cairn_domain::tenancy::ProjectKey;
use cairn_domain::tool_invocation::{ToolInvocationOutcomeKind, ToolInvocationRecord};
use cairn_domain::{
    ApprovalDecision, ApprovalId, ApprovalMatchPolicy, ApprovalRequirement, ApprovalScope,
    CheckpointDisposition, CheckpointId, FailureClass, MailboxMessageId, OperatorId, RunId,
    RunState, SessionId, SessionState, TaskId, TaskState, ToolCallId, ToolInvocationId,
};
use serde::de::DeserializeOwned;
use sqlx::PgPool;

use crate::db::{Backend, DbAdapter};
use crate::error::StoreError;
use crate::projections::{
    rehydrate_termination_reason, ApprovalReadModel, ApprovalRecord, CheckpointReadModel,
    CheckpointRecord, CheckpointStrategyReadModel, MailboxReadModel, MailboxRecord, RunReadModel,
    RunRecord, SessionReadModel, SessionRecord, TaskReadModel, TaskRecord,
    ToolCallApprovalReadModel, ToolCallApprovalRecord, ToolCallApprovalState,
    ToolInvocationReadModel,
};

/// Postgres-backed database adapter.
///
/// Wraps a `sqlx::PgPool` and provides the transactional boundary
/// that ties event-log appends to synchronous projection updates.
pub struct PgAdapter {
    pool: PgPool,
}

impl PgAdapter {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl DbAdapter for PgAdapter {
    fn backend(&self) -> Backend {
        Backend::Postgres
    }

    async fn health_check(&self) -> Result<(), StoreError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        Ok(())
    }

    async fn migrate(&self) -> Result<(), StoreError> {
        use super::migration_runner::PgMigrationRunner;

        let runner = PgMigrationRunner::new(self.pool.clone());
        runner.run_pending().await?;
        Ok(())
    }
}

/// F65 PR-2: column list for session reads. Extracted as a constant so the
/// three `SessionReadModel` queries stay in sync — adding a column to
/// `SessionRow` must be reflected here or sqlx's row-to-struct binder will
/// error out at runtime with "column not found".
const SESSION_SELECT_COLS: &str = "session_id, tenant_id, workspace_id, project_id, state, \
     version, created_at, updated_at, \
     goal_title, max_attempts, attempts_used, \
     wall_clock_ms_cap, wall_clock_ms_used, \
     token_cap, tokens_used, \
     cost_usd_cap, cost_usd_used";

#[async_trait]
impl SessionReadModel for PgAdapter {
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
impl RunReadModel for PgAdapter {
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
        let row: (bool,) = sqlx::query_as(
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

        Ok(row.0)
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
        let rows = sqlx::query_as::<_, RunRow>(
            "SELECT run_id, session_id, parent_run_id, tenant_id, workspace_id, project_id,
                    state, failure_class, version, created_at, updated_at,
                    completion_summary, completion_verification_json, completion_annotated_at_ms,
                    terminal_write_recovery_json
             FROM runs
             WHERE state = $1
             ORDER BY created_at ASC, run_id ASC
             LIMIT $2",
        )
        .bind(enum_string(&state)?)
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
        let rows = sqlx::query_as::<_, RunRow>(
            "SELECT run_id, session_id, parent_run_id, tenant_id, workspace_id, project_id,
                    state, failure_class, version, created_at, updated_at,
                    completion_summary, completion_verification_json, completion_annotated_at_ms,
                    terminal_write_recovery_json
             FROM runs
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3
               AND state NOT IN ('completed', 'failed', 'canceled', 'dead_lettered')
             ORDER BY created_at ASC, run_id ASC
             LIMIT $4",
        )
        .bind(project.tenant_id.as_str())
        .bind(project.workspace_id.as_str())
        .bind(project.project_id.as_str())
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter().map(RunRow::into_record).collect()
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
impl TaskReadModel for PgAdapter {
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
        let row: (bool,) = sqlx::query_as(
            "SELECT EXISTS(
                SELECT 1 FROM tasks
                WHERE parent_run_id = $1
                  AND state NOT IN ('completed', 'failed', 'canceled', 'dead_lettered')
            )",
        )
        .bind(parent_run_id.as_str())
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(row.0)
    }
}

#[async_trait]
impl ApprovalReadModel for PgAdapter {
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
impl CheckpointStrategyReadModel for PgAdapter {
    async fn get_by_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<cairn_domain::CheckpointStrategy>, StoreError> {
        let _ = run_id;
        Ok(None)
    }
}

#[async_trait]
impl CheckpointReadModel for PgAdapter {
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
impl MailboxReadModel for PgAdapter {
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
        // Postgres migration for deliver_at_ms column is out of scope; stub returns empty.
        Ok(vec![])
    }
}

#[async_trait]
impl ToolInvocationReadModel for PgAdapter {
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
impl crate::projections::ToolInvocationProgressReadModel for PgAdapter {
    async fn get(
        &self,
        invocation_id: &ToolInvocationId,
    ) -> Result<Option<crate::projections::ToolInvocationProgressRecord>, StoreError> {
        let row: Option<(String, String, String, i16, Option<String>, i64)> = sqlx::query_as(
            "SELECT tenant_id, workspace_id, project_id,
                    progress_pct, message, updated_at_ms
             FROM tool_invocation_progress
             WHERE invocation_id = $1",
        )
        .bind(invocation_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        // Checked conversions from the signed DB columns. Writes always
        // originate from `u8` / `u64`, but corruption / manual edits /
        // unexpected schema drift could yield negative or oversized
        // values — silently wrapping via `as u8` / `as u64` would hand
        // the handler a garbage row. Flagged on PR #537 by Copilot.
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

#[async_trait]
impl crate::projections::FfLeaseHistoryCursorStore for PgAdapter {
    async fn get(
        &self,
        partition_id: &str,
        execution_id: &str,
    ) -> Result<Option<crate::projections::FfLeaseHistoryCursor>, StoreError> {
        let row: Option<(String, String, String, i64)> = sqlx::query_as(
            "SELECT partition_id, execution_id, last_stream_id, updated_at_ms
             FROM ff_lease_history_cursors
             WHERE partition_id = $1 AND execution_id = $2",
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
             WHERE partition_id = $1",
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
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (partition_id, execution_id) DO UPDATE
                SET last_stream_id = EXCLUDED.last_stream_id,
                    updated_at_ms = EXCLUDED.updated_at_ms",
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
             WHERE partition_id = $1 AND execution_id = $2",
        )
        .bind(partition_id)
        .bind(execution_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
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
    target: serde_json::Value,
    execution_class: String,
    state: String,
    outcome: Option<String>,
    error_message: Option<String>,
    version: i64,
    requested_at_ms: i64,
    started_at_ms: Option<i64>,
    finished_at_ms: Option<i64>,
    // F55: persisted tool args + output preview. Postgres stores
    // args as JSONB which sqlx decodes as `serde_json::Value`.
    // Both nullable for pre-F55 rows.
    args_json: Option<serde_json::Value>,
    output_preview: Option<String>,
}

impl ToolInvocationRow {
    fn into_record(self) -> Result<ToolInvocationRecord, StoreError> {
        Ok(ToolInvocationRecord {
            invocation_id: ToolInvocationId::new(self.invocation_id),
            project: ProjectKey::new(self.tenant_id, self.workspace_id, self.project_id),
            session_id: self.session_id.map(SessionId::new),
            run_id: self.run_id.map(RunId::new),
            task_id: self.task_id.map(TaskId::new),
            prompt_release_id: None,
            target: serde_json::from_value(self.target)
                .map_err(|e| StoreError::Serialization(e.to_string()))?,
            execution_class: parse_string_enum(&self.execution_class)?,
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
            args_json: self.args_json,
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
    // F65 PR-2: session-extension columns. All nullable / defaulted in the
    // schema (pg V031 migration), so legacy rows pick up sane values.
    goal_title: Option<String>,
    max_attempts: i32,
    attempts_used: i32,
    wall_clock_ms_cap: Option<i64>,
    wall_clock_ms_used: i64,
    token_cap: Option<i64>,
    tokens_used: i64,
    cost_usd_cap: Option<f64>,
    cost_usd_used: f64,
}

impl SessionRow {
    fn into_record(self) -> Result<SessionRecord, StoreError> {
        // Roll the budget/cost counters into a single `IssueBudget` if the
        // operator configured at least one cap. If all three caps are NULL
        // we leave the record's `issue_budget` as None so the "no override"
        // signal survives pg → in-memory → pg round trips.
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
        // Silence unused-variable lints for the `_used` columns on pg
        // side: the runtime reads them through per-service queries when
        // enforcement lands in PR-3. They round-trip via SQLite parity
        // tests.
        let _ = (
            self.wall_clock_ms_used,
            self.tokens_used,
            self.cost_usd_used,
        );
        Ok(SessionRecord {
            session_id: SessionId::new(self.session_id),
            project: ProjectKey::new(self.tenant_id, self.workspace_id, self.project_id),
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
    // F47 PR2: nullable completion annotation. TEXT columns hold
    // serde-JSON for `completion_verification_json` so pg + sqlite use
    // the same serialisation path (no JSONB operators, portable).
    completion_summary: Option<String>,
    completion_verification_json: Option<String>,
    completion_annotated_at_ms: Option<i64>,
    // F64: nullable terminal-write recovery annotation (JSON). Absent
    // on the hot path; populated by the TerminalRecoveryAttempted
    // projection when the cairn-side recovery loop fires.
    terminal_write_recovery_json: Option<String>,
}

impl RunRow {
    fn into_record(self) -> Result<RunRecord, StoreError> {
        let completion_verification = self
            .completion_verification_json
            .as_deref()
            .map(serde_json::from_str::<cairn_domain::CompletionVerification>)
            .transpose()
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        // Checked conversion (Copilot review on #313): a negative
        // `completion_annotated_at_ms` would wrap to a huge `u64` via
        // `as u64` and silently surface a year-18-decillion timestamp
        // on the REST response. Postgres can't produce a negative here
        // (the projection applier writes `u64::try_from(…)`), but we
        // guard against manual DB edits / corrupted backups rather
        // than propagating the bad value.
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
            project: ProjectKey::new(self.tenant_id, self.workspace_id, self.project_id),
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
        Ok(TaskRecord {
            task_id: TaskId::new(self.task_id),
            project: ProjectKey::new(self.tenant_id, self.workspace_id, self.project_id),
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
            pause_reason: None,
            resume_trigger: None,
            retry_count: 0,
            lease_owner: self.lease_owner,
            lease_expires_at: self.lease_expires_at.map(|value| value as u64),
            title: self.title,
            description: self.description,
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
        Ok(ApprovalRecord {
            approval_id: ApprovalId::new(self.approval_id),
            project: ProjectKey::new(self.tenant_id, self.workspace_id, self.project_id),
            run_id: self.run_id.map(RunId::new),
            task_id: self.task_id.map(TaskId::new),
            requirement: parse_string_enum::<ApprovalRequirement>(&self.requirement)?,
            title: self.title,
            description: self.description,
            decision: self
                .decision
                .as_deref()
                .map(parse_string_enum::<ApprovalDecision>)
                .transpose()?,
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
        Ok(CheckpointRecord {
            checkpoint_id: CheckpointId::new(self.checkpoint_id),
            project: ProjectKey::new(self.tenant_id, self.workspace_id, self.project_id),
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
        Ok(MailboxRecord {
            message_id: MailboxMessageId::new(self.message_id),
            project: ProjectKey::new(self.tenant_id, self.workspace_id, self.project_id),
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

// -- PR BP-2: ToolCallApprovalReadModel --

#[derive(sqlx::FromRow)]
struct ToolCallApprovalRow {
    call_id: String,
    session_id: String,
    run_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    tool_name: String,
    original_tool_args: serde_json::Value,
    amended_tool_args: Option<serde_json::Value>,
    approved_tool_args: Option<serde_json::Value>,
    display_summary: Option<String>,
    match_policy: serde_json::Value,
    state: String,
    operator_id: Option<String>,
    scope: Option<serde_json::Value>,
    reason: Option<String>,
    proposed_at_ms: i64,
    approved_at_ms: Option<i64>,
    rejected_at_ms: Option<i64>,
    last_amended_at_ms: Option<i64>,
    version: i64,
    created_at: i64,
    updated_at: i64,
}

impl ToolCallApprovalRow {
    fn into_record(self) -> Result<ToolCallApprovalRecord, StoreError> {
        let match_policy: ApprovalMatchPolicy = serde_json::from_value(self.match_policy)
            .map_err(|e| StoreError::Serialization(format!("match_policy decode: {e}")))?;
        let scope: Option<ApprovalScope> = self
            .scope
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| StoreError::Serialization(format!("scope decode: {e}")))?;
        Ok(ToolCallApprovalRecord {
            call_id: ToolCallId::new(self.call_id),
            session_id: SessionId::new(self.session_id),
            run_id: RunId::new(self.run_id),
            project: ProjectKey::new(self.tenant_id, self.workspace_id, self.project_id),
            tool_name: self.tool_name,
            original_tool_args: self.original_tool_args,
            amended_tool_args: self.amended_tool_args,
            approved_tool_args: self.approved_tool_args,
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

const TOOL_CALL_APPROVAL_SELECT: &str =
    "SELECT call_id, session_id, run_id, tenant_id, workspace_id, project_id, \
     tool_name, original_tool_args, amended_tool_args, approved_tool_args, \
     display_summary, match_policy, state, operator_id, scope, reason, \
     proposed_at_ms, approved_at_ms, rejected_at_ms, last_amended_at_ms, \
     version, created_at, updated_at FROM tool_call_approvals";

#[async_trait]
impl ToolCallApprovalReadModel for PgAdapter {
    async fn get(
        &self,
        call_id: &ToolCallId,
    ) -> Result<Option<ToolCallApprovalRecord>, StoreError> {
        let sql = format!("{TOOL_CALL_APPROVAL_SELECT} WHERE call_id = $1");
        let row = sqlx::query_as::<_, ToolCallApprovalRow>(&sql)
            .bind(call_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(ToolCallApprovalRow::into_record).transpose()
    }

    async fn list_for_run(
        &self,
        run_id: &RunId,
    ) -> Result<Vec<ToolCallApprovalRecord>, StoreError> {
        let sql = format!(
            "{TOOL_CALL_APPROVAL_SELECT} WHERE run_id = $1 \
             ORDER BY proposed_at_ms ASC, call_id ASC"
        );
        let rows = sqlx::query_as::<_, ToolCallApprovalRow>(&sql)
            .bind(run_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(ToolCallApprovalRow::into_record)
            .collect()
    }

    async fn list_for_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<ToolCallApprovalRecord>, StoreError> {
        let sql = format!(
            "{TOOL_CALL_APPROVAL_SELECT} WHERE session_id = $1 \
             ORDER BY proposed_at_ms ASC, call_id ASC"
        );
        let rows = sqlx::query_as::<_, ToolCallApprovalRow>(&sql)
            .bind(session_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(ToolCallApprovalRow::into_record)
            .collect()
    }

    async fn list_pending_for_project(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ToolCallApprovalRecord>, StoreError> {
        let sql = format!(
            "{TOOL_CALL_APPROVAL_SELECT} \
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3 \
               AND state = 'pending' \
             ORDER BY proposed_at_ms ASC, call_id ASC \
             LIMIT $4 OFFSET $5"
        );
        let rows = sqlx::query_as::<_, ToolCallApprovalRow>(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(ToolCallApprovalRow::into_record)
            .collect()
    }

    async fn list_all_pending(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ToolCallApprovalRecord>, StoreError> {
        // Guard against `usize` values > `i64::MAX` wrapping into a
        // negative `LIMIT`/`OFFSET` at the SQL layer. On 64-bit
        // platforms this is unreachable in practice (handler caps are
        // well below `i64::MAX`) but surfacing it loudly is cheaper
        // than a corrupt query plan.
        let limit_i64 =
            i64::try_from(limit).map_err(|_| StoreError::Internal("limit overflows i64".into()))?;
        let offset_i64 = i64::try_from(offset)
            .map_err(|_| StoreError::Internal("offset overflows i64".into()))?;
        let sql = format!(
            "{TOOL_CALL_APPROVAL_SELECT} \
             WHERE state = 'pending' \
             ORDER BY proposed_at_ms ASC, call_id ASC \
             LIMIT $1 OFFSET $2"
        );
        let rows = sqlx::query_as::<_, ToolCallApprovalRow>(&sql)
            .bind(limit_i64)
            .bind(offset_i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(ToolCallApprovalRow::into_record)
            .collect()
    }
}

// ── F65 PR-2: orchestrator-session read models (Postgres) ─────────────────

const F65_SESSION_OUTCOME_SELECT_PG: &str =
    "SELECT root_run_id, tenant_id, workspace_scope, project_id, \
     session_id, checkpoint_id, workspace_snapshot_id, \
     termination_reason, termination_reason_json, \
     compacted_summary, next_step_hint, \
     cost_micros, created_at FROM session_outcomes";

#[derive(sqlx::FromRow)]
struct F65SessionOutcomeRowPg {
    root_run_id: String,
    tenant_id: String,
    workspace_scope: String,
    project_id: String,
    session_id: String,
    checkpoint_id: String,
    workspace_snapshot_id: Option<String>,
    termination_reason: String,
    /// JSON-as-TEXT serialization of the full `TerminationReason`
    /// (writer always sets this; nullable only to tolerate hypothetical
    /// hand-inserted rows). Readers prefer this when present so the
    /// payload variants carry real fields instead of empty placeholders.
    termination_reason_json: Option<String>,
    compacted_summary: String,
    next_step_hint: Option<String>,
    cost_micros: i64,
    created_at: i64,
}

impl F65SessionOutcomeRowPg {
    fn into_record(self) -> Result<crate::projections::SessionOutcomeRecord, StoreError> {
        let reason = rehydrate_termination_reason(
            self.termination_reason.as_str(),
            self.termination_reason_json.as_deref(),
        )?;
        Ok(crate::projections::SessionOutcomeRecord {
            root_run_id: RunId::new(self.root_run_id),
            project: ProjectKey::new(self.tenant_id, self.workspace_scope, self.project_id),
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
impl crate::projections::SessionOutcomeReadModel for PgAdapter {
    async fn get_by_root_run(
        &self,
        project: &ProjectKey,
        root_run_id: &RunId,
    ) -> Result<Option<crate::projections::SessionOutcomeRecord>, StoreError> {
        // Tenant-isolation (issue #438): scope-tuple guard runs at the
        // query layer so a handler that forgets to pre-check cannot
        // leak a foreign row. Matches pg/sqlite row layout which
        // always stores `tenant_id`, `workspace_scope`, `project_id`.
        let sql = format!(
            "{F65_SESSION_OUTCOME_SELECT_PG} WHERE root_run_id = $1 \
             AND tenant_id = $2 AND workspace_scope = $3 AND project_id = $4"
        );
        let row = sqlx::query_as::<_, F65SessionOutcomeRowPg>(&sql)
            .bind(root_run_id.as_str())
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(F65SessionOutcomeRowPg::into_record).transpose()
    }

    async fn list_by_session(
        &self,
        project: &ProjectKey,
        session_id: &SessionId,
    ) -> Result<Vec<crate::projections::SessionOutcomeRecord>, StoreError> {
        let sql = format!(
            "{F65_SESSION_OUTCOME_SELECT_PG} WHERE session_id = $1 \
             AND tenant_id = $2 AND workspace_scope = $3 AND project_id = $4 \
             ORDER BY created_at ASC, root_run_id ASC"
        );
        let rows = sqlx::query_as::<_, F65SessionOutcomeRowPg>(&sql)
            .bind(session_id.as_str())
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(F65SessionOutcomeRowPg::into_record)
            .collect()
    }
}

const F65_WORKSPACE_SNAPSHOT_SELECT_PG: &str =
    "SELECT snapshot_id, tenant_id, workspace_scope, project_id, \
     session_id, workspace_id, parent_snapshot_id, snapshot_path, \
     bytes, reflink_used, created_at, reaped_at FROM workspace_snapshots";

#[derive(sqlx::FromRow)]
struct F65WorkspaceSnapshotRowPg {
    snapshot_id: String,
    tenant_id: String,
    workspace_scope: String,
    project_id: String,
    session_id: String,
    workspace_id: String,
    parent_snapshot_id: Option<String>,
    snapshot_path: String,
    bytes: i64,
    reflink_used: bool,
    created_at: i64,
    reaped_at: Option<i64>,
}

impl F65WorkspaceSnapshotRowPg {
    fn into_record(self) -> crate::projections::WorkspaceSnapshotRecord {
        crate::projections::WorkspaceSnapshotRecord {
            snapshot_id: cairn_domain::WorkspaceSnapshotId::new(self.snapshot_id),
            project: ProjectKey::new(self.tenant_id, self.workspace_scope, self.project_id),
            session_id: SessionId::new(self.session_id),
            workspace_id: cairn_domain::WorkspaceId::new(self.workspace_id),
            parent_snapshot_id: self
                .parent_snapshot_id
                .map(cairn_domain::WorkspaceSnapshotId::new),
            snapshot_path: self.snapshot_path,
            bytes: self.bytes.max(0) as u64,
            reflink_used: self.reflink_used,
            created_at: self.created_at.max(0) as u64,
            reaped_at: self.reaped_at.map(|v| v.max(0) as u64),
        }
    }
}

#[async_trait]
impl crate::projections::WorkspaceSnapshotWriter for PgAdapter {
    async fn stamp_metadata(
        &self,
        snapshot_id: &cairn_domain::WorkspaceSnapshotId,
        snapshot_path: &str,
        bytes: u64,
        reflink_used: bool,
        parent_snapshot_id: Option<&cairn_domain::WorkspaceSnapshotId>,
    ) -> Result<(), StoreError> {
        // UPDATE is idempotent under replay. If the snapshot row doesn't
        // exist yet, no-op — the caller is responsible for emitting
        // `WorkspaceSnapshotCreated` first (which projects the initial
        // row via the zero-filled INSERT path in pg/projections.rs).
        let bytes_i64 = i64::try_from(bytes).map_err(|_| {
            StoreError::Internal(format!(
                "WorkspaceSnapshotWriter.stamp_metadata.bytes {bytes} exceeds i64::MAX"
            ))
        })?;
        sqlx::query(
            "UPDATE workspace_snapshots
                SET snapshot_path     = $1,
                    bytes             = $2,
                    reflink_used      = $3,
                    parent_snapshot_id = $4
              WHERE snapshot_id = $5",
        )
        .bind(snapshot_path)
        .bind(bytes_i64)
        .bind(reflink_used)
        .bind(parent_snapshot_id.map(|p| p.as_str().to_owned()))
        .bind(snapshot_id.as_str())
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl crate::projections::WorkspaceSnapshotReadModel for PgAdapter {
    async fn get(
        &self,
        project: &ProjectKey,
        snapshot_id: &cairn_domain::WorkspaceSnapshotId,
    ) -> Result<Option<crate::projections::WorkspaceSnapshotRecord>, StoreError> {
        let sql = format!(
            "{F65_WORKSPACE_SNAPSHOT_SELECT_PG} WHERE snapshot_id = $1 \
             AND tenant_id = $2 AND workspace_scope = $3 AND project_id = $4"
        );
        let row = sqlx::query_as::<_, F65WorkspaceSnapshotRowPg>(&sql)
            .bind(snapshot_id.as_str())
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(F65WorkspaceSnapshotRowPg::into_record))
    }

    async fn list_by_session(
        &self,
        project: &ProjectKey,
        session_id: &SessionId,
    ) -> Result<Vec<crate::projections::WorkspaceSnapshotRecord>, StoreError> {
        let sql = format!(
            "{F65_WORKSPACE_SNAPSHOT_SELECT_PG} WHERE session_id = $1 \
             AND tenant_id = $2 AND workspace_scope = $3 AND project_id = $4 \
             ORDER BY created_at ASC, snapshot_id ASC"
        );
        let rows = sqlx::query_as::<_, F65WorkspaceSnapshotRowPg>(&sql)
            .bind(session_id.as_str())
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(F65WorkspaceSnapshotRowPg::into_record)
            .collect())
    }

    async fn lineage(
        &self,
        project: &ProjectKey,
        start: &cairn_domain::WorkspaceSnapshotId,
    ) -> Result<Vec<crate::projections::WorkspaceSnapshotRecord>, StoreError> {
        // Iterative walk rather than a recursive CTE — SQLite-parity
        // requires we stay on the portable SQL subset
        // (project memory `feedback_no_db_specific_features`). A cycle in
        // the parent chain would be an FK bug upstream; we still cap the
        // walk defensively.
        //
        // Issue #438: every hop re-checks the project scope via the
        // per-row `get` above, so a chain that crosses tenants (writer
        // corruption) stops at the boundary instead of leaking the
        // foreign row.
        let mut chain: Vec<crate::projections::WorkspaceSnapshotRecord> = Vec::new();
        let mut cursor = Some(start.as_str().to_owned());
        const MAX_DEPTH: usize = 1024;
        for _ in 0..MAX_DEPTH {
            let Some(id) = cursor.take() else {
                break;
            };
            let Some(rec) = <Self as crate::projections::WorkspaceSnapshotReadModel>::get(
                self,
                project,
                &cairn_domain::WorkspaceSnapshotId::new(id),
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
impl crate::projections::WorkspaceRegistryReadModel for PgAdapter {
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
             FROM workspace_registry WHERE workspace_id = $1 \
             AND tenant_id = $2 AND workspace_scope = $3 AND project_id = $4",
        )
        .bind(workspace_id.as_str())
        .bind(project.tenant_id.as_str())
        .bind(project.workspace_id.as_str())
        .bind(project.project_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(tuple_to_workspace_registry_pg).transpose()
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
             FROM workspace_registry WHERE root_run_id = $1 \
             AND tenant_id = $2 AND workspace_scope = $3 AND project_id = $4 \
             ORDER BY created_at DESC LIMIT 1",
        )
        .bind(root_run_id.as_str())
        .bind(project.tenant_id.as_str())
        .bind(project.workspace_id.as_str())
        .bind(project.project_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(tuple_to_workspace_registry_pg).transpose()
    }
}

fn tuple_to_workspace_registry_pg(
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
        project: ProjectKey::new(tenant_id, ws_scope, project_id),
        root_run_id: RunId::new(root_run_id),
        fs_root,
        status: parsed_status,
        created_at: created_at.max(0) as u64,
        reaped_at: reaped_at.map(|v| v.max(0) as u64),
    })
}

#[async_trait]
impl crate::projections::F65CheckpointReadModel for PgAdapter {
    async fn get_f65(
        &self,
        project: &ProjectKey,
        checkpoint_id: &CheckpointId,
    ) -> Result<Option<crate::projections::F65CheckpointRecord>, StoreError> {
        let row: Option<(
            String,
            String,
            String,
            String,
            String,
            String,
            Option<String>,
            Option<i64>,
            Option<i32>,
            Option<i32>,
            i64,
        )> = sqlx::query_as(
            "SELECT checkpoint_id, tenant_id, workspace_id, project_id, \
             run_id, session_id, body, body_size_bytes, schema_version, iteration, created_at \
             FROM checkpoints \
             WHERE checkpoint_id = $1 AND session_id IS NOT NULL \
             AND tenant_id = $2 AND workspace_id = $3 AND project_id = $4",
        )
        .bind(checkpoint_id.as_str())
        .bind(project.tenant_id.as_str())
        .bind(project.workspace_id.as_str())
        .bind(project.project_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        let Some((cid, tenant, ws, proj, run_id, sid, body, body_size, schema_ver, it, ts)) = row
        else {
            return Ok(None);
        };
        Ok(Some(crate::projections::F65CheckpointRecord {
            checkpoint_id: CheckpointId::new(cid),
            project: ProjectKey::new(tenant, ws, proj),
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
            Option<i32>,
            Option<i32>,
            i64,
        )> = sqlx::query_as(
            "SELECT checkpoint_id, tenant_id, workspace_id, project_id, \
             run_id, session_id, body, body_size_bytes, schema_version, iteration, created_at \
             FROM checkpoints \
             WHERE session_id = $1 \
             AND tenant_id = $2 AND workspace_id = $3 AND project_id = $4 \
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
                        project: ProjectKey::new(tenant, ws, proj),
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

// ── RFC-025 Phase 1 (milestone 3): EvalRunReadModel ──────────────────────────
//
// Projects eval lifecycle events (Started / Completed / Archived / Scored /
// RubricScored) into the `eval_runs` projection table created by migration
// V034. Row shape maps 1:1 to `EvalRunRecord`; metrics + rubric verdict are
// stored as JSON-in-TEXT for cross-backend parity with sqlite (no JSONB,
// see `feedback_no_db_specific_features.md`).

/// Row struct for the `eval_runs` projection. sqlx `FromRow` on tuples
/// only goes up to 16 columns; the `eval_runs` schema has 20, so we use
/// a named struct with derive(FromRow) just like `SessionRow` above.
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
impl crate::projections::EvalRunReadModel for PgAdapter {
    async fn get(
        &self,
        eval_run_id: &cairn_domain::EvalRunId,
    ) -> Result<Option<crate::projections::EvalRunRecord>, StoreError> {
        let sql = format!("SELECT {EVAL_RUN_SELECT_COLS} FROM eval_runs WHERE eval_run_id = $1");
        let row: Option<EvalRunRow> = sqlx::query_as(&sql)
            .bind(eval_run_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        row.map(pg_row_to_eval_run_record).transpose()
    }

    async fn list_by_project(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::EvalRunRecord>, StoreError> {
        let sql = format!(
            "SELECT {EVAL_RUN_SELECT_COLS} FROM eval_runs
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3
             ORDER BY started_at ASC, eval_run_id ASC
             LIMIT $4 OFFSET $5"
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

        rows.into_iter().map(pg_row_to_eval_run_record).collect()
    }
}

/// Convert a raw pg row tuple into `EvalRunRecord`, deserialising the two
/// JSON-in-TEXT columns. Invalid JSON surfaces as `StoreError::Internal`
/// rather than silently dropping the score: a corrupted projection row is
/// strictly worse than a loud error the operator can triage.
fn pg_row_to_eval_run_record(
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
        project: ProjectKey::new(row.tenant_id, row.workspace_id, row.project_id),
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

// ── RFC-025 Phase 1.5a: TriggerReadModel / RunTemplateReadModel /
// TriggerFireReadModel ────────────────────────────────────────────────
//
// Reads against the three projection tables created in migration V035.
// The write-side arms in `pg/projections.rs` keep the rows in sync
// inside every event-append transaction; these read-side methods back
// the new async `TriggerService` contract defined in cairn-runtime.

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
    max_chain_depth: i32,
    created_by: String,
    created_at: i64,
    updated_at: i64,
}

// ── RFC-025 Phase 2a.1: CredentialReadModel + CredentialRotationReadModel ────
//
// Projects the three credential lifecycle events (Stored / Revoked /
// KeyRotated) into the `credentials` and `credential_rotations`
// projection tables created by migration V035. Row shapes map 1:1 to
// the in-memory records; the parity harness asserts byte-equality
// across InMemory ↔ SQLite ↔ Postgres (nightly CI under
// TEST_DATABASE_URL).

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

const TRIGGER_SELECT_COLS: &str = "trigger_id, tenant_id, workspace_id, project_id, \
    name, description, signal_type, plugin_id, conditions_json, run_template_id, \
    state, state_reason, suspension_reason, state_since, \
    max_per_minute, max_burst, max_chain_depth, created_by, created_at, updated_at";

fn pg_row_to_trigger_record(
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
        // Saturating down-casts so a corrupted / out-of-range row
        // doesn't silently wrap to a tiny value. max_per_minute + max_burst
        // cap at u32::MAX; max_chain_depth caps at u8::MAX (255). Review
        // PR #569 Copilot note.
        max_per_minute: row.max_per_minute.clamp(0, u32::MAX as i64) as u32,
        max_burst: row.max_burst.clamp(0, u32::MAX as i64) as u32,
        max_chain_depth: row.max_chain_depth.clamp(0, u8::MAX as i32) as u8,
        created_by: OperatorId::new(row.created_by),
        created_at: row.created_at.max(0) as u64,
        updated_at: row.updated_at.max(0) as u64,
    })
}

#[async_trait]
impl crate::projections::TriggerReadModel for PgAdapter {
    async fn get_trigger(
        &self,
        trigger_id: &cairn_domain::ids::TriggerId,
    ) -> Result<Option<crate::projections::TriggerRecord>, StoreError> {
        let sql = format!("SELECT {TRIGGER_SELECT_COLS} FROM triggers WHERE trigger_id = $1");
        let row: Option<TriggerRow> = sqlx::query_as(&sql)
            .bind(trigger_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(pg_row_to_trigger_record).transpose()
    }

    async fn list_triggers_by_project(
        &self,
        project: &ProjectKey,
    ) -> Result<Vec<crate::projections::TriggerRecord>, StoreError> {
        let sql = format!(
            "SELECT {TRIGGER_SELECT_COLS} FROM triggers \
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3 \
             ORDER BY trigger_id ASC"
        );
        let rows: Vec<TriggerRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter().map(pg_row_to_trigger_record).collect()
    }

    async fn list_matching_enabled(
        &self,
        project: &ProjectKey,
        signal_type: &str,
        plugin_id: &str,
    ) -> Result<Vec<crate::projections::TriggerRecord>, StoreError> {
        // Match rule from the pre-refactor in-memory path:
        //   * signal_type must be an exact string match
        //   * trigger.plugin_id is Option<String>: None (broad) matches any
        //     incoming plugin_id; Some(x) requires plugin_id == x.
        // Sort by trigger_id so evaluation order stays deterministic.
        let sql = format!(
            "SELECT {TRIGGER_SELECT_COLS} FROM triggers \
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3 \
             AND state = 'enabled' \
             AND signal_type = $4 \
             AND (plugin_id IS NULL OR plugin_id = $5) \
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
        rows.into_iter().map(pg_row_to_trigger_record).collect()
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

fn pg_row_to_run_template_record(row: RunTemplateRow) -> crate::projections::RunTemplateRecord {
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
impl crate::projections::RunTemplateReadModel for PgAdapter {
    async fn get_template(
        &self,
        template_id: &cairn_domain::ids::RunTemplateId,
    ) -> Result<Option<crate::projections::RunTemplateRecord>, StoreError> {
        let sql =
            format!("SELECT {RUN_TEMPLATE_SELECT_COLS} FROM run_templates WHERE template_id = $1");
        let row: Option<RunTemplateRow> = sqlx::query_as(&sql)
            .bind(template_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(pg_row_to_run_template_record))
    }

    async fn list_templates_by_project(
        &self,
        project: &ProjectKey,
    ) -> Result<Vec<crate::projections::RunTemplateRecord>, StoreError> {
        let sql = format!(
            "SELECT {RUN_TEMPLATE_SELECT_COLS} FROM run_templates \
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3 \
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
            .map(pg_row_to_run_template_record)
            .collect())
    }

    async fn triggers_referencing_template(
        &self,
        template_id: &cairn_domain::ids::RunTemplateId,
    ) -> Result<Vec<cairn_domain::ids::TriggerId>, StoreError> {
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT trigger_id FROM triggers WHERE run_template_id = $1")
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
impl crate::projections::TriggerFireReadModel for PgAdapter {
    async fn has_fired(
        &self,
        trigger_id: &cairn_domain::ids::TriggerId,
        signal_id: &str,
    ) -> Result<bool, StoreError> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT 1 FROM trigger_fires \
             WHERE trigger_id = $1 AND signal_id = $2 AND outcome = 'fired' \
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
             WHERE trigger_id = $1 AND outcome = 'fired' AND at_ms > $2",
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
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3 \
             AND outcome = 'fired' AND at_ms > $4",
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

impl CredentialRow {
    fn into_record(self) -> cairn_domain::credentials::CredentialRecord {
        cairn_domain::credentials::CredentialRecord {
            id: cairn_domain::CredentialId::new(self.credential_id),
            tenant_id: cairn_domain::TenantId::new(self.tenant_id),
            name: self.name,
            credential_type: self.credential_type,
            encrypted_value: self.encrypted_value,
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
impl crate::projections::CredentialReadModel for PgAdapter {
    async fn get(
        &self,
        id: &cairn_domain::CredentialId,
    ) -> Result<Option<cairn_domain::credentials::CredentialRecord>, StoreError> {
        let sql =
            format!("SELECT {CREDENTIAL_SELECT_COLS} FROM credentials WHERE credential_id = $1");
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
             WHERE tenant_id = $1
             ORDER BY created_at ASC, credential_id ASC
             LIMIT $2 OFFSET $3"
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

    /// Single-pass scan across all tenants: used by
    /// `cairn_runtime::services::scan_legacy_ciphertexts` at boot.
    /// Returns `Some(rows)` so the caller skips the per-tenant fallback.
    async fn list_all_active(
        &self,
        limit: usize,
    ) -> Result<Option<Vec<cairn_domain::credentials::CredentialRecord>>, StoreError> {
        let sql = format!(
            "SELECT {CREDENTIAL_SELECT_COLS} FROM credentials
             WHERE active = TRUE
             ORDER BY created_at ASC, credential_id ASC
             LIMIT $1"
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

// ── RFC-025 Phase 2a.1 milestone 2: QuotaReadModel + QuotaViolationReadModel ──
//
// Projects `TenantQuotaSet` into a `tenant_quotas` baseline table and
// `TenantQuotaViolated` into a `tenant_quota_violations` audit trail.
// `get_quota` pulls the baseline row and joins sessions/runs for the
// dynamic `current_active_runs` and `sessions_this_hour` counters
// (mirrors the in-memory QuotaReadModel).

#[derive(sqlx::FromRow)]
struct TenantQuotaRow {
    max_concurrent_runs: i32,
    max_sessions_per_hour: i32,
    max_tasks_per_run: i32,
}

#[async_trait]
impl crate::projections::QuotaReadModel for PgAdapter {
    async fn get_quota(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Option<cairn_domain::TenantQuota>, StoreError> {
        let baseline: Option<TenantQuotaRow> = sqlx::query_as(
            "SELECT max_concurrent_runs, max_sessions_per_hour, max_tasks_per_run
             FROM tenant_quotas
             WHERE tenant_id = $1",
        )
        .bind(tenant_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        let Some(baseline) = baseline else {
            return Ok(None);
        };

        // Dynamic counters. `current_active_runs` = non-terminal runs for
        // this tenant. Matches the runs-table state filter used elsewhere
        // in this adapter (see `any_non_terminal`).
        let active_runs_row: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM runs
             WHERE tenant_id = $1
               AND state NOT IN ('completed', 'failed', 'canceled', 'dead_lettered')",
        )
        .bind(tenant_id.as_str())
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        // `sessions_this_hour` mirrors in_memory: count all sessions for
        // the tenant. The event-based "this hour" semantics will arrive
        // with a dedicated counter column in a future migration.
        let sessions_row: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM sessions WHERE tenant_id = $1")
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
impl crate::projections::QuotaViolationReadModel for PgAdapter {
    async fn list_violations(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
    ) -> Result<Vec<crate::projections::QuotaViolationRecord>, StoreError> {
        let rows: Vec<QuotaViolationRow> = sqlx::query_as(
            "SELECT tenant_id, quota_type, occurred_at_ms, current_value, limit_value
             FROM tenant_quota_violations
             WHERE tenant_id = $1
             ORDER BY occurred_at_ms DESC, quota_type ASC
             LIMIT $2",
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

// ── RFC-025 Phase 2a.1 milestone 3: ProviderBudgetReadModel ──────────────────

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
impl crate::projections::ProviderBudgetReadModel for PgAdapter {
    async fn get_by_tenant_period(
        &self,
        tenant_id: &cairn_domain::TenantId,
        period: cairn_domain::providers::ProviderBudgetPeriod,
    ) -> Result<Option<cairn_domain::providers::ProviderBudget>, StoreError> {
        let period_str = match period {
            cairn_domain::providers::ProviderBudgetPeriod::Daily => "daily",
            cairn_domain::providers::ProviderBudgetPeriod::Monthly => "monthly",
        };
        // `ORDER BY created_at ASC` picks the earliest budget for
        // deterministic selection when multiple rows share (tenant,
        // period) — matches the in_memory selection policy.
        let sql = format!(
            "SELECT {PROVIDER_BUDGET_SELECT_COLS} FROM provider_budgets
             WHERE tenant_id = $1 AND period = $2
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
             WHERE tenant_id = $1
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

// ── RFC-025 Phase 2a.1 milestone 4: LicenseReadModel ─────────────────────────
//
// `list_overrides` returns an empty vec on pg/sqlite until Phase 2a.2
// projects `EntitlementOverrideSet`. The in_memory impl owns the
// overrides today; operators querying through pg/sqlite should rely on
// the central service-layer cache until 2a.2 lands the dedicated table.

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
impl crate::projections::LicenseReadModel for PgAdapter {
    async fn get_active(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Option<cairn_domain::LicenseRecord>, StoreError> {
        let sql = format!("SELECT {LICENSE_SELECT_COLS} FROM licenses WHERE tenant_id = $1");
        let row: Option<LicenseRow> = sqlx::query_as(&sql)
            .bind(tenant_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(LicenseRow::into_record).transpose()
    }

    async fn list_overrides(
        &self,
        _tenant_id: &cairn_domain::TenantId,
    ) -> Result<Vec<cairn_domain::EntitlementOverrideRecord>, StoreError> {
        // RFC-025 Phase 2a.2 will fill EntitlementOverrideSet and add a
        // dedicated table + impl. Until then, the pg adapter returns an
        // empty list rather than claiming data it cannot produce. The
        // in-memory projection continues to serve overrides to cairn-app
        // via the service-layer cache.
        Ok(Vec::new())
    }
}

#[async_trait]
impl crate::projections::CredentialRotationReadModel for PgAdapter {
    async fn list_rotations(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Vec<cairn_domain::credentials::CredentialRotationRecord>, StoreError> {
        let sql = format!(
            "SELECT {CREDENTIAL_ROTATION_SELECT_COLS} FROM credential_rotations
             WHERE tenant_id = $1
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

// ── RFC-025 Phase 3: ProviderConnectionReadModel + ProviderBindingReadModel ──
//
// Backs the V040 projection. Both surfaces share the row-struct pattern
// established for evals / credentials / licenses: `FromRow` extracts
// column values, `into_record` hydrates the domain type with enum +
// JSON parsing. Failing to decode is surfaced as `StoreError::Internal`
// with a column-scoped message so operators can see which row + field
// tripped.

#[derive(sqlx::FromRow)]
struct ProviderConnectionRow {
    provider_connection_id: String,
    tenant_id: String,
    provider_family: String,
    adapter_type: String,
    supported_models_json: String,
    status: String,
    created_at: i64,
}

impl ProviderConnectionRow {
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

const PROVIDER_CONNECTION_SELECT_COLS: &str = "provider_connection_id, tenant_id, \
     provider_family, adapter_type, supported_models_json, status, created_at";

#[async_trait]
impl crate::projections::ProviderConnectionReadModel for PgAdapter {
    async fn get(
        &self,
        id: &cairn_domain::ProviderConnectionId,
    ) -> Result<Option<cairn_domain::providers::ProviderConnectionRecord>, StoreError> {
        let sql = format!(
            "SELECT {PROVIDER_CONNECTION_SELECT_COLS} FROM provider_connections
             WHERE provider_connection_id = $1"
        );
        let row: Option<ProviderConnectionRow> = sqlx::query_as(&sql)
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(ProviderConnectionRow::into_record).transpose()
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::providers::ProviderConnectionRecord>, StoreError> {
        let sql = format!(
            "SELECT {PROVIDER_CONNECTION_SELECT_COLS} FROM provider_connections
             WHERE tenant_id = $1
             ORDER BY created_at ASC, provider_connection_id ASC
             LIMIT $2 OFFSET $3"
        );
        let rows: Vec<ProviderConnectionRow> = sqlx::query_as(&sql)
            .bind(tenant_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(ProviderConnectionRow::into_record)
            .collect()
    }
}

#[derive(sqlx::FromRow)]
struct ProviderBindingRow {
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

impl ProviderBindingRow {
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

const PROVIDER_BINDING_SELECT_COLS: &str = "provider_binding_id, tenant_id, workspace_id, \
     project_id, provider_connection_id, provider_model_id, operation_kind, \
     settings_json, active, created_at";

#[async_trait]
impl crate::projections::ProviderBindingReadModel for PgAdapter {
    async fn get(
        &self,
        id: &cairn_domain::ProviderBindingId,
    ) -> Result<Option<cairn_domain::providers::ProviderBindingRecord>, StoreError> {
        let sql = format!(
            "SELECT {PROVIDER_BINDING_SELECT_COLS} FROM provider_bindings
             WHERE provider_binding_id = $1"
        );
        let row: Option<ProviderBindingRow> = sqlx::query_as(&sql)
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(ProviderBindingRow::into_record).transpose()
    }

    async fn list_by_project(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::providers::ProviderBindingRecord>, StoreError> {
        let sql = format!(
            "SELECT {PROVIDER_BINDING_SELECT_COLS} FROM provider_bindings
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3
             ORDER BY created_at ASC, provider_binding_id ASC
             LIMIT $4 OFFSET $5"
        );
        let rows: Vec<ProviderBindingRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(ProviderBindingRow::into_record)
            .collect()
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::providers::ProviderBindingRecord>, StoreError> {
        let sql = format!(
            "SELECT {PROVIDER_BINDING_SELECT_COLS} FROM provider_bindings
             WHERE tenant_id = $1
             ORDER BY created_at ASC, provider_binding_id ASC
             LIMIT $2 OFFSET $3"
        );
        let rows: Vec<ProviderBindingRow> = sqlx::query_as(&sql)
            .bind(tenant_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(ProviderBindingRow::into_record)
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
        // Sort tiebreaker matches in_memory (list_active sorts by
        // created_at then provider_binding_id) — cross-backend parity
        // test in projection_parity.rs asserts this.
        let sql = format!(
            "SELECT {PROVIDER_BINDING_SELECT_COLS} FROM provider_bindings
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3
               AND active = TRUE
               AND operation_kind = $4
             ORDER BY created_at ASC, provider_binding_id ASC"
        );
        let rows: Vec<ProviderBindingRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .bind(operation_str)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(ProviderBindingRow::into_record)
            .collect()
    }
}
