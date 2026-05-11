use std::time::{SystemTime, UNIX_EPOCH};

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
/// Wall-clock millis as `i64` for `updated_at` writes. Mirrors
/// `in_memory::now_millis` but returns signed — pg/sqlite store
/// `updated_at` as BIGINT/INTEGER (signed), so match the column type.
fn now_millis_i64() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

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

/// Column list for `runs` projection reads. Shared across every
/// `RunReadModel` method so adding a column doesn't require finding
/// all seven SELECT sites and updating them in lockstep.
const RUN_SELECT_COLS: &str =
    "run_id, session_id, parent_run_id, tenant_id, workspace_id, project_id, \
     state, failure_class, version, created_at, updated_at, \
     completion_summary, completion_verification_json, completion_annotated_at_ms, \
     terminal_write_recovery_json, in_flight_descendants, root_run_id, iteration";

#[async_trait]
impl RunReadModel for PgAdapter {
    async fn get(&self, run_id: &RunId) -> Result<Option<RunRecord>, StoreError> {
        let sql = format!("SELECT {RUN_SELECT_COLS} FROM runs WHERE run_id = $1");
        let row = sqlx::query_as::<_, RunRow>(&sql)
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
        let sql = format!(
            "SELECT {RUN_SELECT_COLS} FROM runs \
             WHERE session_id = $1 \
             ORDER BY created_at ASC, run_id ASC \
             LIMIT $2 OFFSET $3"
        );
        let rows = sqlx::query_as::<_, RunRow>(&sql)
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
        let sql = format!(
            "SELECT {RUN_SELECT_COLS} FROM runs \
             WHERE session_id = $1 AND parent_run_id IS NULL \
             ORDER BY created_at DESC, run_id DESC \
             LIMIT 1"
        );
        let row = sqlx::query_as::<_, RunRow>(&sql)
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
        let sql = format!(
            "SELECT {RUN_SELECT_COLS} FROM runs \
             WHERE state = $1 \
             ORDER BY created_at ASC, run_id ASC \
             LIMIT $2"
        );
        let rows = sqlx::query_as::<_, RunRow>(&sql)
            .bind(enum_string(&state)?)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        rows.into_iter().map(RunRow::into_record).collect()
    }

    /// #670 G4 / RFC 027 + PR-1b-4: pushed-down predicate for the
    /// `ChildRunDriver` scan — child runs in `Pending` or `Running`
    /// state. Uses `idx_runs_parent` (partial index on
    /// `parent_run_id WHERE NOT NULL` from V003) so the IS NOT NULL
    /// predicate narrows to child rows cheaply before the state
    /// filter runs.
    ///
    /// `Running` is included so the driver can re-claim crashed
    /// children post-recovery; FF's atomic
    /// `issue_grant_and_claim` rejects live-lease duplicates.
    async fn list_driver_claimable_children(
        &self,
        limit: usize,
    ) -> Result<Vec<RunRecord>, StoreError> {
        let sql = format!(
            "SELECT {RUN_SELECT_COLS} FROM runs \
             WHERE state IN ('pending', 'running') AND parent_run_id IS NOT NULL \
             ORDER BY created_at ASC, run_id ASC \
             LIMIT $1"
        );
        let rows = sqlx::query_as::<_, RunRow>(&sql)
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
        let sql = format!(
            "SELECT {RUN_SELECT_COLS} FROM runs \
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3 \
               AND state NOT IN ('completed', 'failed', 'canceled', 'dead_lettered') \
             ORDER BY created_at ASC, run_id ASC \
             LIMIT $4"
        );
        let rows = sqlx::query_as::<_, RunRow>(&sql)
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
        let sql = format!(
            "SELECT {RUN_SELECT_COLS} FROM runs \
             WHERE parent_run_id = $1 \
             ORDER BY created_at ASC, run_id ASC \
             LIMIT $2"
        );
        let rows = sqlx::query_as::<_, RunRow>(&sql)
            .bind(parent_run_id.as_str())
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter().map(RunRow::into_record).collect()
    }

    async fn list_stalled(
        &self,
        tenant_id: &cairn_domain::TenantId,
        now_ms: u64,
        stale_after_ms: u64,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<RunRecord>, StoreError> {
        // Issue #570: state + staleness + tenant composed at the SQL
        // layer. Previously the handler fetched up to 20 000 rows
        // (Running + Pending across all tenants) and filtered by
        // tenant + staleness in memory on every refresh. The index
        // on `(state, updated_at)` from V003 would serve the
        // staleness predicate cheaply; the tenant filter narrows the
        // set further. `limit + 1` is how callers detect `has_more`.
        //
        // `u64 → i64` is checked (Copilot review on #589): a wrap to
        // negative would flip the `updated_at < cutoff` predicate
        // silently. On a value past `i64::MAX` we fail loud rather
        // than over-return.
        let stale_cutoff = now_ms.saturating_sub(stale_after_ms);
        let stale_cutoff_i64 = i64::try_from(stale_cutoff).map_err(|_| {
            StoreError::Internal(format!(
                "list_stalled: stale_cutoff_ms={stale_cutoff} exceeds i64::MAX"
            ))
        })?;
        let limit_i64 = i64::try_from(limit).map_err(|_| {
            StoreError::Internal(format!("list_stalled: limit={limit} exceeds i64::MAX"))
        })?;
        let offset_i64 = i64::try_from(offset).map_err(|_| {
            StoreError::Internal(format!("list_stalled: offset={offset} exceeds i64::MAX"))
        })?;
        let sql = format!(
            "SELECT {RUN_SELECT_COLS} FROM runs \
             WHERE tenant_id = $1 \
               AND state IN ('running', 'pending') \
               AND updated_at < $2 \
             ORDER BY updated_at ASC, run_id ASC \
             LIMIT $3 OFFSET $4"
        );
        let rows = sqlx::query_as::<_, RunRow>(&sql)
            .bind(tenant_id.as_str())
            .bind(stale_cutoff_i64)
            .bind(limit_i64)
            .bind(offset_i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter().map(RunRow::into_record).collect()
    }
}

// -- RunDescendantsCounter (#670 G4 PR-1b-1) --

#[async_trait]
impl crate::projections::RunDescendantsCounter for PgAdapter {
    async fn try_increment_descendants(
        &self,
        root_run_id: &RunId,
        cap: i64,
    ) -> Result<crate::projections::DescendantsCapOutcome, StoreError> {
        use crate::projections::DescendantsCapOutcome;
        // Atomic compare-and-increment in a single SQL round-trip.
        // The predicate `in_flight_descendants < :cap` evaluates on
        // pg-side; two concurrent spawns against the same root cannot
        // both admit above the cap. `RETURNING` returns the post-
        // increment value so callers can surface it in metrics.
        //
        // Also bump `version` + `updated_at` so stale-run detection
        // and other version-watching consumers observe the change
        // (Copilot review on #676 — without this, a root with
        // active descendants would look idle to stale-run sweeps).
        let now_ms = now_millis_i64();
        let row: Option<(i64,)> = sqlx::query_as(
            "UPDATE runs
                SET in_flight_descendants = in_flight_descendants + 1,
                    version = version + 1,
                    updated_at = $3
              WHERE run_id = $1
                AND in_flight_descendants < $2
           RETURNING in_flight_descendants",
        )
        .bind(root_run_id.as_str())
        .bind(cap)
        .bind(now_ms)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        match row {
            Some((new_count,)) => Ok(DescendantsCapOutcome::Admitted { new_count }),
            None => {
                // Either the root doesn't exist or the cap was
                // reached. Distinguish with a cheap follow-up probe
                // so callers see the right outcome (cap-reached is
                // business logic; not-found is a bug condition).
                let exists: Option<(i64,)> =
                    sqlx::query_as("SELECT in_flight_descendants FROM runs WHERE run_id = $1")
                        .bind(root_run_id.as_str())
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(|e| StoreError::Internal(e.to_string()))?;
                Ok(match exists {
                    Some(_) => DescendantsCapOutcome::CapReached,
                    None => DescendantsCapOutcome::RootNotFound,
                })
            }
        }
    }

    async fn decrement_descendants(
        &self,
        root_run_id: &RunId,
    ) -> Result<crate::projections::DescendantsCapOutcome, StoreError> {
        use crate::projections::DescendantsCapOutcome;
        let now_ms = now_millis_i64();
        let row: Option<(i64,)> = sqlx::query_as(
            "UPDATE runs
                SET in_flight_descendants = in_flight_descendants - 1,
                    version = version + 1,
                    updated_at = $2
              WHERE run_id = $1
           RETURNING in_flight_descendants",
        )
        .bind(root_run_id.as_str())
        .bind(now_ms)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        match row {
            Some((new_count,)) => Ok(DescendantsCapOutcome::Admitted { new_count }),
            None => Ok(DescendantsCapOutcome::RootNotFound),
        }
    }

    async fn list_nonzero_descendant_counters(&self) -> Result<Vec<(RunId, i64)>, StoreError> {
        const RECONCILE_LIMIT: i64 = 10_000;
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT run_id, in_flight_descendants FROM runs \
             WHERE in_flight_descendants <> 0 \
             ORDER BY run_id ASC \
             LIMIT $1",
        )
        .bind(RECONCILE_LIMIT)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(|(id, n)| (RunId::new(id), n))
            .collect())
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
        // RFC-025 Phase 2b.3 m5: replaces the pre-Phase-2b.3 `Ok(None)`
        // stub. The row is keyed on `run_id` and carries no project
        // scope on the write side (the `CheckpointStrategySet` event
        // has no project field); we return the shared sentinel
        // `ProjectKey` on read so pg/sqlite/in-memory agree
        // byte-for-byte.
        let row: Option<(String, i64, i32, i32)> = sqlx::query_as(
            "SELECT strategy_id, interval_ms, max_checkpoints, trigger_on_task_complete
             FROM checkpoint_strategies
             WHERE run_id = $1",
        )
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(row.map(
            |(strategy_id, interval_ms, max_checkpoints, trigger_on_task_complete)| {
                cairn_domain::CheckpointStrategy {
                    strategy_id,
                    project: crate::projections::checkpoint_strategy_sentinel_project(),
                    run_id: run_id.clone(),
                    interval_ms: interval_ms.max(0) as u64,
                    max_checkpoints: max_checkpoints.max(0) as u32,
                    trigger_on_task_complete: trigger_on_task_complete != 0,
                }
            },
        ))
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
    // #670 G4 PR-1b-1: descendants counter + root pointer.
    // `in_flight_descendants` is `BIGINT NOT NULL DEFAULT 0` on the
    // schema; `root_run_id` is `TEXT` (nullable — pre-V069 child rows
    // stay NULL per the RFC 027 backfill spec).
    in_flight_descendants: i64,
    root_run_id: Option<String>,
    // #791: prior-iteration counter incremented on every
    // approval-resume boundary (waiting_approval → running). INTEGER
    // NOT NULL DEFAULT 0 on the schema (V073).
    iteration: i32,
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
            in_flight_descendants: self.in_flight_descendants,
            root_run_id: self.root_run_id.map(RunId::new),
            iteration: u32::try_from(self.iteration).unwrap_or(0),
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
            // Wrap the ciphertext as soon as it leaves the DB driver so
            // the projection heap copy is scrubbed on drop (#579). The
            // sqlx `Vec<u8>` row buffer is consumed here (`self`
            // moves), so the only surviving copy is inside the
            // `RedactedCiphertext` wrapper.
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
// `list_overrides` reads the `entitlement_overrides` table (RFC-025
// Phase 2a.2 milestone 4). Records are sorted by `feature` ASC so the
// result is deterministic for parity tests — the in-memory impl is also
// re-sorted the same way in its applier below.

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
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Vec<cairn_domain::EntitlementOverrideRecord>, StoreError> {
        let rows: Vec<EntitlementOverrideRow> = sqlx::query_as(
            "SELECT tenant_id, feature, allowed, reason, set_at_ms
             FROM entitlement_overrides
             WHERE tenant_id = $1
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
        // Match the in-memory applier exactly: override_id is
        // synthesized from tenant_id+feature, and `entitlement` is
        // hardcoded to `AdvancedAdmin` — these two fields are legacy
        // shape on `EntitlementOverrideRecord`, not real projected
        // columns. Re-computing them here gives byte-equal parity with
        // in-memory.
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
            // Event payload is `u64`; storage is `BIGINT` (i64). `.max(0)`
            // clamps a corrupt negative on read — the write path rejects
            // any value exceeding `i64::MAX` via `try_from`, so a round-
            // tripped value is always non-negative.
            delegated_at_ms: self.delegated_at_ms.max(0) as u64,
            delegation_id: self.delegation_id,
        }
    }
}

#[async_trait]
impl crate::projections::ApprovalDelegationReadModel for PgAdapter {
    async fn list_for_approval(
        &self,
        approval_id: &ApprovalId,
    ) -> Result<Vec<crate::projections::ApprovalDelegationRecord>, StoreError> {
        let rows: Vec<ApprovalDelegationRow> = sqlx::query_as(
            "SELECT approval_id, delegation_id, delegated_to, delegated_at_ms
             FROM approval_delegations
             WHERE approval_id = $1
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
impl crate::projections::GuardrailReadModel for PgAdapter {
    async fn get_policy(
        &self,
        policy_id: &str,
    ) -> Result<Option<cairn_domain::policy::GuardrailPolicy>, StoreError> {
        let row: Option<GuardrailPolicyRow> = sqlx::query_as(
            "SELECT policy_id, name, rules_json, enabled
             FROM guardrail_policies
             WHERE policy_id = $1",
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
             WHERE tenant_id = $1
             ORDER BY policy_id ASC
             LIMIT $2 OFFSET $3",
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
        // Empty-string sentinel ↔ None round-trip so readers see a
        // natural `Option<String>` without carrying the SQL-side hack.
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
impl crate::projections::GuardrailEvaluationReadModel for PgAdapter {
    async fn list_evaluations(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
    ) -> Result<Vec<crate::projections::GuardrailEvaluationRecord>, StoreError> {
        let rows: Vec<GuardrailEvaluationRow> = sqlx::query_as(
            "SELECT policy_id, tenant_id, subject_type, subject_id,
                    action, decision, reason, evaluated_at_ms
             FROM guardrail_evaluations
             WHERE tenant_id = $1
             ORDER BY evaluated_at_ms DESC, policy_id ASC
             LIMIT $2",
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
        // In-memory + event-payload use `0` as the "no cap" sentinel
        // for `max_events_per_entity` (u32). NULL in the SQL row
        // carries the same meaning (the event's `Option::None`), so
        // collapse both to 0 on read.
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
impl crate::projections::RetentionPolicyReadModel for PgAdapter {
    async fn get_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Option<cairn_domain::RetentionPolicy>, StoreError> {
        let row: Option<RetentionPolicyRow> = sqlx::query_as(
            "SELECT tenant_id, policy_id, full_history_days, current_state_days,
                    max_events_per_entity
             FROM retention_policies
             WHERE tenant_id = $1",
        )
        .bind(tenant_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(RetentionPolicyRow::into_record))
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

// ── RFC-025 Phase 2b.1: audit_log_entries read model ────────────────

#[derive(sqlx::FromRow)]
struct AuditLogEntryRow {
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

impl AuditLogEntryRow {
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

const AUDIT_LOG_SELECT_COLS: &str = "entry_id, tenant_id, actor_id, action, \
     resource_type, resource_id, outcome, metadata_json, occurred_at_ms";

#[async_trait]
impl crate::projections::AuditLogReadModel for PgAdapter {
    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        since_ms: Option<u64>,
        before_ms: Option<u64>,
        limit: usize,
    ) -> Result<Vec<cairn_domain::AuditLogEntry>, StoreError> {
        // Newest-first per trait doc; tiebreak on entry_id DESC matches
        // the in-memory ordering so cross-backend parity is stable for
        // identical timestamps. Overflow + unbounded defaults shared
        // with sqlite via `window_bounds_ms` (Gemini PR #573 review).
        let (since, before) = crate::projections::window_bounds_ms(since_ms, before_ms)?;
        let sql = format!(
            "SELECT {AUDIT_LOG_SELECT_COLS} FROM audit_log_entries
             WHERE tenant_id = $1
               AND occurred_at_ms >= $2
               AND occurred_at_ms < $3
             ORDER BY occurred_at_ms DESC, entry_id DESC
             LIMIT $4"
        );
        let rows: Vec<AuditLogEntryRow> = sqlx::query_as(&sql)
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
        // `list_by_resource` returns the full history for the resource
        // (admin dashboard filters further downstream). Cap at the
        // trait-level `LIST_BY_RESOURCE_MAX_ROWS` so pg / sqlite /
        // in-memory converge on the same ceiling (Copilot PR #573
        // review: the in-memory impl used to be unbounded).
        let sql = format!(
            "SELECT {AUDIT_LOG_SELECT_COLS} FROM audit_log_entries
             WHERE resource_type = $1 AND resource_id = $2
             ORDER BY occurred_at_ms DESC, entry_id DESC
             LIMIT $3"
        );
        let rows: Vec<AuditLogEntryRow> = sqlx::query_as(&sql)
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
struct ScheduledTaskRow {
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

impl ScheduledTaskRow {
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

const SCHEDULED_TASK_SELECT_COLS: &str = "scheduled_task_id, tenant_id, name, \
     cron_expression, last_run_at, next_run_at, enabled, created_at, updated_at";

#[async_trait]
impl crate::projections::ScheduledTaskReadModel for PgAdapter {
    async fn get(
        &self,
        id: &cairn_domain::ScheduledTaskId,
    ) -> Result<Option<cairn_domain::ScheduledTaskRecord>, StoreError> {
        let sql = format!(
            "SELECT {SCHEDULED_TASK_SELECT_COLS} FROM scheduled_tasks
             WHERE scheduled_task_id = $1"
        );
        let row: Option<ScheduledTaskRow> = sqlx::query_as(&sql)
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(ScheduledTaskRow::into_record))
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::ScheduledTaskRecord>, StoreError> {
        // Sort tiebreaker matches the in-memory sort_by_key(created_at)
        // with a secondary id tiebreaker so parity tests are stable on
        // identical created_at timestamps.
        let sql = format!(
            "SELECT {SCHEDULED_TASK_SELECT_COLS} FROM scheduled_tasks
             WHERE tenant_id = $1
             ORDER BY created_at ASC, scheduled_task_id ASC
             LIMIT $2 OFFSET $3"
        );
        let rows: Vec<ScheduledTaskRow> = sqlx::query_as(&sql)
            .bind(tenant_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(ScheduledTaskRow::into_record)
            .collect())
    }

    async fn list_due(
        &self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<cairn_domain::ScheduledTaskRecord>, StoreError> {
        let now = i64::try_from(now_ms)
            .map_err(|_| StoreError::Internal("now_ms exceeds i64::MAX".into()))?;
        // Enabled + next_run_at <= now. Sort by next_run_at so callers
        // service the earliest-due first (matches in-memory ordering).
        let sql = format!(
            "SELECT {SCHEDULED_TASK_SELECT_COLS} FROM scheduled_tasks
             WHERE enabled = TRUE AND next_run_at IS NOT NULL AND next_run_at <= $1
             ORDER BY next_run_at ASC, scheduled_task_id ASC
             LIMIT $2"
        );
        let rows: Vec<ScheduledTaskRow> = sqlx::query_as(&sql)
            .bind(now)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(ScheduledTaskRow::into_record)
            .collect())
    }
}

// ── RFC-025 Phase 2b.1 m3: outcomes read model ────────────────────

#[derive(sqlx::FromRow)]
struct OutcomeRow {
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

impl OutcomeRow {
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

const OUTCOME_SELECT_COLS: &str = "outcome_id, run_id, tenant_id, workspace_id, \
     project_id, agent_type, predicted_confidence, actual_outcome, recorded_at";

#[async_trait]
impl crate::projections::OutcomeReadModel for PgAdapter {
    async fn get(
        &self,
        outcome_id: &cairn_domain::OutcomeId,
    ) -> Result<Option<crate::projections::OutcomeRecord>, StoreError> {
        let sql = format!(
            "SELECT {OUTCOME_SELECT_COLS} FROM outcomes
             WHERE outcome_id = $1"
        );
        let row: Option<OutcomeRow> = sqlx::query_as(&sql)
            .bind(outcome_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(OutcomeRow::into_record).transpose()
    }

    async fn list_by_run(
        &self,
        run_id: &cairn_domain::RunId,
        limit: usize,
    ) -> Result<Vec<crate::projections::OutcomeRecord>, StoreError> {
        let sql = format!(
            "SELECT {OUTCOME_SELECT_COLS} FROM outcomes
             WHERE run_id = $1
             ORDER BY recorded_at ASC, outcome_id ASC
             LIMIT $2"
        );
        let rows: Vec<OutcomeRow> = sqlx::query_as(&sql)
            .bind(run_id.as_str())
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter().map(OutcomeRow::into_record).collect()
    }

    async fn list_by_project(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::OutcomeRecord>, StoreError> {
        let sql = format!(
            "SELECT {OUTCOME_SELECT_COLS} FROM outcomes
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3
             ORDER BY recorded_at ASC, outcome_id ASC
             LIMIT $4 OFFSET $5"
        );
        let rows: Vec<OutcomeRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter().map(OutcomeRow::into_record).collect()
    }
}

// ── RFC-025 Phase 2b.1 m4: plan_reviews read model (RFC 018) ─────

#[derive(sqlx::FromRow)]
struct PlanReviewRow {
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

impl PlanReviewRow {
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

const PLAN_REVIEW_SELECT_COLS: &str = "plan_run_id, tenant_id, workspace_id, project_id, \
     session_id, plan_markdown, state, proposed_at, resolved_by, resolved_at, \
     reviewer_comments, rejection_reason, revision_run_id";

#[async_trait]
impl crate::projections::PlanReviewReadModel for PgAdapter {
    async fn get(
        &self,
        plan_run_id: &cairn_domain::RunId,
    ) -> Result<Option<crate::projections::PlanReviewRecord>, StoreError> {
        let sql = format!(
            "SELECT {PLAN_REVIEW_SELECT_COLS} FROM plan_reviews
             WHERE plan_run_id = $1"
        );
        let row: Option<PlanReviewRow> = sqlx::query_as(&sql)
            .bind(plan_run_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(PlanReviewRow::into_record).transpose()
    }

    async fn list_by_project(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::PlanReviewRecord>, StoreError> {
        let sql = format!(
            "SELECT {PLAN_REVIEW_SELECT_COLS} FROM plan_reviews
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3
             ORDER BY proposed_at DESC, plan_run_id DESC
             LIMIT $4 OFFSET $5"
        );
        let rows: Vec<PlanReviewRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter().map(PlanReviewRow::into_record).collect()
    }

    async fn list_pending_by_project(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
    ) -> Result<Vec<crate::projections::PlanReviewRecord>, StoreError> {
        let sql = format!(
            "SELECT {PLAN_REVIEW_SELECT_COLS} FROM plan_reviews
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3
               AND state = 'proposed'
             ORDER BY proposed_at DESC, plan_run_id DESC
             LIMIT $4"
        );
        let rows: Vec<PlanReviewRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter().map(PlanReviewRow::into_record).collect()
    }

    async fn list_by_session(
        &self,
        session_id: &cairn_domain::SessionId,
        limit: usize,
    ) -> Result<Vec<crate::projections::PlanReviewRecord>, StoreError> {
        let sql = format!(
            "SELECT {PLAN_REVIEW_SELECT_COLS} FROM plan_reviews
             WHERE session_id = $1
             ORDER BY proposed_at ASC, plan_run_id ASC
             LIMIT $2"
        );
        let rows: Vec<PlanReviewRow> = sqlx::query_as(&sql)
            .bind(session_id.as_str())
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter().map(PlanReviewRow::into_record).collect()
    }
}

// ── RFC-025 Phase 2b.2 m1: external_workers read model (GAP-005) ──

#[derive(sqlx::FromRow)]
struct ExternalWorkerRow {
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

impl ExternalWorkerRow {
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

const EXTERNAL_WORKER_SELECT_COLS: &str = "worker_id, tenant_id, display_name, status, \
     registered_at, updated_at, last_heartbeat_ms, is_alive, active_task_count, current_task_id";

#[async_trait]
impl crate::projections::ExternalWorkerReadModel for PgAdapter {
    async fn get(
        &self,
        id: &cairn_domain::WorkerId,
    ) -> Result<Option<cairn_domain::workers::ExternalWorkerRecord>, StoreError> {
        let sql = format!(
            "SELECT {EXTERNAL_WORKER_SELECT_COLS} FROM external_workers
             WHERE worker_id = $1"
        );
        let row: Option<ExternalWorkerRow> = sqlx::query_as(&sql)
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(ExternalWorkerRow::into_record))
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::workers::ExternalWorkerRecord>, StoreError> {
        // Sort ASC by registered_at to mirror the in-memory applier's
        // `sort_by_key(|r| r.registered_at)`; tiebreak on worker_id for
        // deterministic pagination at same-ms registration bursts.
        let sql = format!(
            "SELECT {EXTERNAL_WORKER_SELECT_COLS} FROM external_workers
             WHERE tenant_id = $1
             ORDER BY registered_at ASC, worker_id ASC
             LIMIT $2 OFFSET $3"
        );
        let rows: Vec<ExternalWorkerRow> = sqlx::query_as(&sql)
            .bind(tenant_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(ExternalWorkerRow::into_record)
            .collect())
    }
}

// ── RFC-025 Phase 2b.2b m1: resource_shares read model (RFC 008) ──

#[derive(sqlx::FromRow)]
struct ResourceShareRow {
    share_id: String,
    tenant_id: String,
    source_workspace_id: String,
    target_workspace_id: String,
    resource_type: String,
    resource_id: String,
    permissions_json: String,
    shared_at_ms: i64,
}

impl ResourceShareRow {
    fn into_record(self) -> Result<cairn_domain::resource_sharing::SharedResource, StoreError> {
        let permissions: Vec<String> =
            serde_json::from_str(&self.permissions_json).map_err(|err| {
                StoreError::Serialization(format!(
                    "resource_shares.permissions_json decode for share_id={}: {err}",
                    self.share_id
                ))
            })?;
        Ok(cairn_domain::resource_sharing::SharedResource {
            share_id: self.share_id,
            tenant_id: cairn_domain::TenantId::new(self.tenant_id),
            source_workspace_id: cairn_domain::WorkspaceId::new(self.source_workspace_id),
            target_workspace_id: cairn_domain::WorkspaceId::new(self.target_workspace_id),
            resource_type: self.resource_type,
            resource_id: self.resource_id,
            permissions,
            shared_at_ms: self.shared_at_ms.max(0) as u64,
        })
    }
}

const RESOURCE_SHARE_SELECT_COLS: &str =
    "share_id, tenant_id, source_workspace_id, target_workspace_id, \
     resource_type, resource_id, permissions_json, shared_at_ms";

#[async_trait]
impl crate::projections::ResourceSharingReadModel for PgAdapter {
    async fn get_share(
        &self,
        share_id: &str,
    ) -> Result<Option<cairn_domain::resource_sharing::SharedResource>, StoreError> {
        let sql = format!(
            "SELECT {RESOURCE_SHARE_SELECT_COLS} FROM resource_shares
             WHERE share_id = $1"
        );
        let row: Option<ResourceShareRow> = sqlx::query_as(&sql)
            .bind(share_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(ResourceShareRow::into_record).transpose()
    }

    async fn list_shares_for_workspace(
        &self,
        tenant_id: &cairn_domain::TenantId,
        target_workspace_id: &cairn_domain::WorkspaceId,
    ) -> Result<Vec<cairn_domain::resource_sharing::SharedResource>, StoreError> {
        // Sort by (shared_at_ms, share_id) to match the in-memory
        // applier's sort and the tenant-scoped index.
        let sql = format!(
            "SELECT {RESOURCE_SHARE_SELECT_COLS} FROM resource_shares
             WHERE tenant_id = $1 AND target_workspace_id = $2
             ORDER BY shared_at_ms ASC, share_id ASC"
        );
        let rows: Vec<ResourceShareRow> = sqlx::query_as(&sql)
            .bind(tenant_id.as_str())
            .bind(target_workspace_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(ResourceShareRow::into_record)
            .collect()
    }

    async fn get_share_for_resource(
        &self,
        tenant_id: &cairn_domain::TenantId,
        target_workspace_id: &cairn_domain::WorkspaceId,
        resource_type: &str,
        resource_id: &str,
    ) -> Result<Option<cairn_domain::resource_sharing::SharedResource>, StoreError> {
        // Same index as list_shares_for_workspace — tenant-scoped
        // resource-type+resource-id lookup. Pick lowest shared_at_ms
        // deterministically if duplicates ever slip through (the
        // service layer issues unique share_ids per share call, so
        // duplicates are only possible via event-log replay after a
        // Revoke — in which case the Revoke DELETE already won).
        let sql = format!(
            "SELECT {RESOURCE_SHARE_SELECT_COLS} FROM resource_shares
             WHERE tenant_id = $1 AND target_workspace_id = $2
               AND resource_type = $3 AND resource_id = $4
             ORDER BY shared_at_ms ASC, share_id ASC
             LIMIT 1"
        );
        let row: Option<ResourceShareRow> = sqlx::query_as(&sql)
            .bind(tenant_id.as_str())
            .bind(target_workspace_id.as_str())
            .bind(resource_type)
            .bind(resource_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(ResourceShareRow::into_record).transpose()
    }
}

// ── RFC-025 Phase 2b.2b m2: signal_ingestions read model ──

#[derive(sqlx::FromRow)]
struct SignalIngestionRow {
    signal_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    source: String,
    payload_json: String,
    timestamp_ms: i64,
}

impl SignalIngestionRow {
    fn into_record(self) -> Result<cairn_domain::SignalRecord, StoreError> {
        let payload: serde_json::Value =
            serde_json::from_str(&self.payload_json).map_err(|err| {
                StoreError::Serialization(format!(
                    "signal_ingestions.payload_json decode for signal_id={}: {err}",
                    self.signal_id
                ))
            })?;
        Ok(cairn_domain::SignalRecord {
            id: cairn_domain::SignalId::new(self.signal_id),
            project: cairn_domain::tenancy::ProjectKey::new(
                self.tenant_id,
                self.workspace_id,
                self.project_id,
            ),
            source: self.source,
            payload,
            timestamp_ms: self.timestamp_ms.max(0) as u64,
        })
    }
}

const SIGNAL_INGESTION_SELECT_COLS: &str =
    "signal_id, tenant_id, workspace_id, project_id, source, payload_json, timestamp_ms";

#[async_trait]
impl crate::projections::SignalReadModel for PgAdapter {
    async fn get(
        &self,
        signal_id: &cairn_domain::SignalId,
    ) -> Result<Option<cairn_domain::SignalRecord>, StoreError> {
        let sql = format!(
            "SELECT {SIGNAL_INGESTION_SELECT_COLS} FROM signal_ingestions
             WHERE signal_id = $1"
        );
        let row: Option<SignalIngestionRow> = sqlx::query_as(&sql)
            .bind(signal_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(SignalIngestionRow::into_record).transpose()
    }

    async fn list_by_project(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::SignalRecord>, StoreError> {
        // Sort by (timestamp_ms, signal_id) — parity with in-memory
        // and the composite project index.
        let sql = format!(
            "SELECT {SIGNAL_INGESTION_SELECT_COLS} FROM signal_ingestions
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3
             ORDER BY timestamp_ms ASC, signal_id ASC
             LIMIT $4 OFFSET $5"
        );
        let rows: Vec<SignalIngestionRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(SignalIngestionRow::into_record)
            .collect()
    }
}

// ── RFC-025 Phase 2b.2b m3: subagent_spawns read model (RFC 014) ──

#[derive(sqlx::FromRow)]
struct SubagentSpawnRow {
    child_task_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    parent_run_id: String,
    parent_task_id: Option<String>,
    child_session_id: String,
    child_run_id: Option<String>,
    spawned_at_ms: i64,
    // #670 G2: LLM delegation context. Non-nullable with DEFAULT ''
    // on the table (migration V067) so pre-G2 rows surface as empty
    // strings — matches the `#[serde(default)]` replay contract on
    // the domain event.
    goal: String,
    role: String,
}

impl SubagentSpawnRow {
    fn into_record(self) -> crate::projections::SubagentSpawnRecord {
        crate::projections::SubagentSpawnRecord {
            child_task_id: cairn_domain::TaskId::new(self.child_task_id),
            project: cairn_domain::tenancy::ProjectKey::new(
                self.tenant_id,
                self.workspace_id,
                self.project_id,
            ),
            parent_run_id: cairn_domain::RunId::new(self.parent_run_id),
            parent_task_id: self.parent_task_id.map(cairn_domain::TaskId::new),
            child_session_id: cairn_domain::SessionId::new(self.child_session_id),
            child_run_id: self.child_run_id.map(cairn_domain::RunId::new),
            spawned_at_ms: self.spawned_at_ms.max(0) as u64,
            goal: self.goal,
            role: self.role,
        }
    }
}

const SUBAGENT_SPAWN_SELECT_COLS: &str =
    "child_task_id, tenant_id, workspace_id, project_id, parent_run_id, \
     parent_task_id, child_session_id, child_run_id, spawned_at_ms, \
     goal, role";

#[async_trait]
impl crate::projections::SubagentSpawnReadModel for PgAdapter {
    async fn get_by_child_task(
        &self,
        child_task_id: &cairn_domain::TaskId,
    ) -> Result<Option<crate::projections::SubagentSpawnRecord>, StoreError> {
        let sql = format!(
            "SELECT {SUBAGENT_SPAWN_SELECT_COLS} FROM subagent_spawns
             WHERE child_task_id = $1"
        );
        let row: Option<SubagentSpawnRow> = sqlx::query_as(&sql)
            .bind(child_task_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(SubagentSpawnRow::into_record))
    }

    async fn get_by_child_run_id(
        &self,
        child_run_id: &cairn_domain::RunId,
    ) -> Result<Option<crate::projections::SubagentSpawnRecord>, StoreError> {
        // Point lookup on `child_run_id`. Served by the partial
        // index `idx_subagent_spawns_child_run_id` (pg V070) — the
        // terminal hook fires on every child completion so an
        // indexed lookup is load-bearing.
        let sql = format!(
            "SELECT {SUBAGENT_SPAWN_SELECT_COLS} FROM subagent_spawns
             WHERE child_run_id = $1
             LIMIT 1"
        );
        let row: Option<SubagentSpawnRow> = sqlx::query_as(&sql)
            .bind(child_run_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(SubagentSpawnRow::into_record))
    }

    async fn list_by_parent_run(
        &self,
        parent_run_id: &cairn_domain::RunId,
    ) -> Result<Vec<crate::projections::SubagentSpawnRecord>, StoreError> {
        let sql = format!(
            "SELECT {SUBAGENT_SPAWN_SELECT_COLS} FROM subagent_spawns
             WHERE parent_run_id = $1
             ORDER BY spawned_at_ms ASC, child_task_id ASC"
        );
        let rows: Vec<SubagentSpawnRow> = sqlx::query_as(&sql)
            .bind(parent_run_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(SubagentSpawnRow::into_record)
            .collect())
    }
}

// ── Issue #668: LLM completion body read model ──

#[derive(sqlx::FromRow)]
struct LlmCompletionBodyRow {
    trace_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    session_id: String,
    run_id: Option<String>,
    model_id: String,
    system_prompt: String,
    messages_json: String,
    response_text: String,
    tool_calls_json: String,
    tool_defs_json: String,
    recorded_at_ms: i64,
}

impl LlmCompletionBodyRow {
    fn into_record(self) -> crate::projections::LlmCompletionBodyRecord {
        crate::projections::LlmCompletionBodyRecord {
            trace_id: self.trace_id,
            project: cairn_domain::tenancy::ProjectKey::new(
                self.tenant_id,
                self.workspace_id,
                self.project_id,
            ),
            session_id: cairn_domain::SessionId::new(self.session_id),
            run_id: self.run_id.map(cairn_domain::RunId::new),
            model_id: self.model_id,
            system_prompt: self.system_prompt,
            messages_json: self.messages_json,
            response_text: self.response_text,
            tool_calls_json: self.tool_calls_json,
            tool_defs_json: self.tool_defs_json,
            recorded_at_ms: self.recorded_at_ms.max(0) as u64,
        }
    }
}

const LLM_COMPLETION_BODY_SELECT_COLS: &str = "trace_id, tenant_id, workspace_id, project_id, \
     session_id, run_id, model_id, \
     system_prompt, messages_json, \
     response_text, tool_calls_json, tool_defs_json, recorded_at_ms";

#[async_trait]
impl crate::projections::LlmCompletionBodyReadModel for PgAdapter {
    async fn get_by_trace_id(
        &self,
        trace_id: &str,
    ) -> Result<Option<crate::projections::LlmCompletionBodyRecord>, StoreError> {
        let sql = format!(
            "SELECT {LLM_COMPLETION_BODY_SELECT_COLS} FROM llm_completions
             WHERE trace_id = $1"
        );
        let row: Option<LlmCompletionBodyRow> = sqlx::query_as(&sql)
            .bind(trace_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(LlmCompletionBodyRow::into_record))
    }

    async fn list_by_session(
        &self,
        session_id: &cairn_domain::SessionId,
    ) -> Result<Vec<crate::projections::LlmCompletionBodyRecord>, StoreError> {
        let sql = format!(
            "SELECT {LLM_COMPLETION_BODY_SELECT_COLS} FROM llm_completions
             WHERE session_id = $1
             ORDER BY recorded_at_ms ASC, trace_id ASC"
        );
        let rows: Vec<LlmCompletionBodyRow> = sqlx::query_as(&sql)
            .bind(session_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(LlmCompletionBodyRow::into_record)
            .collect())
    }
}

// ── RFC-025 Phase 2b.2b m4: user_messages read model ──

#[derive(sqlx::FromRow)]
struct UserMessageRow {
    run_id: String,
    sequence: i64,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    session_id: String,
    event_id: String,
    content: String,
    appended_at_ms: i64,
}

impl UserMessageRow {
    fn into_record(self) -> crate::projections::UserMessageRecord {
        crate::projections::UserMessageRecord {
            run_id: cairn_domain::RunId::new(self.run_id),
            sequence: self.sequence.max(0) as u64,
            project: cairn_domain::tenancy::ProjectKey::new(
                self.tenant_id,
                self.workspace_id,
                self.project_id,
            ),
            session_id: cairn_domain::SessionId::new(self.session_id),
            event_id: self.event_id,
            content: self.content,
            appended_at_ms: self.appended_at_ms.max(0) as u64,
        }
    }
}

const USER_MESSAGE_SELECT_COLS: &str = "run_id, sequence, tenant_id, workspace_id, project_id, \
     session_id, event_id, content, appended_at_ms";

#[async_trait]
impl crate::projections::UserMessageReadModel for PgAdapter {
    async fn list_by_run(
        &self,
        run_id: &cairn_domain::RunId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::UserMessageRecord>, StoreError> {
        let sql = format!(
            "SELECT {USER_MESSAGE_SELECT_COLS} FROM user_messages
             WHERE run_id = $1
             ORDER BY sequence ASC, appended_at_ms ASC
             LIMIT $2 OFFSET $3"
        );
        let rows: Vec<UserMessageRow> = sqlx::query_as(&sql)
            .bind(run_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows.into_iter().map(UserMessageRow::into_record).collect())
    }

    async fn count_by_run(&self, run_id: &cairn_domain::RunId) -> Result<u64, StoreError> {
        let (count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM user_messages WHERE run_id = $1")
                .bind(run_id.as_str())
                .fetch_one(&self.pool)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(count.max(0) as u64)
    }
}

// ── RFC-025 Phase 2b.2b m5: soul_patches read model ──

#[derive(sqlx::FromRow)]
struct SoulPatchRow {
    patch_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    state: String,
    patch_content: String,
    requires_approval: bool,
    proposed_at_ms: i64,
    applied_at_ms: Option<i64>,
    new_version: Option<i32>,
}

impl SoulPatchRow {
    fn into_record(self) -> Result<crate::projections::SoulPatchRecord, StoreError> {
        let state =
            crate::projections::SoulPatchState::from_str_opt(&self.state).ok_or_else(|| {
                StoreError::Serialization(format!(
                    "soul_patches.state {:?} unknown for patch_id={}",
                    self.state, self.patch_id
                ))
            })?;
        Ok(crate::projections::SoulPatchRecord {
            patch_id: self.patch_id,
            project: cairn_domain::tenancy::ProjectKey::new(
                self.tenant_id,
                self.workspace_id,
                self.project_id,
            ),
            state,
            patch_content: self.patch_content,
            requires_approval: self.requires_approval,
            proposed_at_ms: self.proposed_at_ms.max(0) as u64,
            applied_at_ms: self.applied_at_ms.map(|v| v.max(0) as u64),
            new_version: self.new_version.map(|v| v.max(0) as u32),
        })
    }
}

const SOUL_PATCH_SELECT_COLS: &str =
    "patch_id, tenant_id, workspace_id, project_id, state, patch_content, \
     requires_approval, proposed_at_ms, applied_at_ms, new_version";

#[async_trait]
impl crate::projections::SoulPatchReadModel for PgAdapter {
    async fn get(
        &self,
        patch_id: &str,
    ) -> Result<Option<crate::projections::SoulPatchRecord>, StoreError> {
        let sql = format!("SELECT {SOUL_PATCH_SELECT_COLS} FROM soul_patches WHERE patch_id = $1");
        let row: Option<SoulPatchRow> = sqlx::query_as(&sql)
            .bind(patch_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(SoulPatchRow::into_record).transpose()
    }

    async fn list_by_project(
        &self,
        project: &cairn_domain::tenancy::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::SoulPatchRecord>, StoreError> {
        let sql = format!(
            "SELECT {SOUL_PATCH_SELECT_COLS} FROM soul_patches
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3
             ORDER BY proposed_at_ms DESC, patch_id DESC
             LIMIT $4 OFFSET $5"
        );
        let rows: Vec<SoulPatchRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter().map(SoulPatchRow::into_record).collect()
    }
}

// ── RFC-025 Phase 2b.2b m6: tool_recovery_pauses read model ──

#[derive(sqlx::FromRow)]
struct ToolRecoveryPauseRow {
    tool_call_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    run_id: String,
    task_id: Option<String>,
    tool_name: String,
    reason: String,
    paused_at_ms: i64,
}

impl ToolRecoveryPauseRow {
    fn into_record(self) -> crate::projections::ToolRecoveryPauseRecord {
        crate::projections::ToolRecoveryPauseRecord {
            tool_call_id: self.tool_call_id,
            project: cairn_domain::tenancy::ProjectKey::new(
                self.tenant_id,
                self.workspace_id,
                self.project_id,
            ),
            run_id: cairn_domain::RunId::new(self.run_id),
            task_id: self.task_id.map(cairn_domain::TaskId::new),
            tool_name: self.tool_name,
            reason: self.reason,
            paused_at_ms: self.paused_at_ms.max(0) as u64,
        }
    }
}

const TOOL_RECOVERY_PAUSE_SELECT_COLS: &str =
    "tool_call_id, tenant_id, workspace_id, project_id, run_id, task_id, \
     tool_name, reason, paused_at_ms";

#[async_trait]
impl crate::projections::ToolRecoveryPauseReadModel for PgAdapter {
    async fn get(
        &self,
        tool_call_id: &str,
    ) -> Result<Option<crate::projections::ToolRecoveryPauseRecord>, StoreError> {
        let sql = format!(
            "SELECT {TOOL_RECOVERY_PAUSE_SELECT_COLS} FROM tool_recovery_pauses
             WHERE tool_call_id = $1"
        );
        let row: Option<ToolRecoveryPauseRow> = sqlx::query_as(&sql)
            .bind(tool_call_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(ToolRecoveryPauseRow::into_record))
    }

    async fn list_by_run(
        &self,
        run_id: &cairn_domain::RunId,
    ) -> Result<Vec<crate::projections::ToolRecoveryPauseRecord>, StoreError> {
        let sql = format!(
            "SELECT {TOOL_RECOVERY_PAUSE_SELECT_COLS} FROM tool_recovery_pauses
             WHERE run_id = $1
             ORDER BY paused_at_ms ASC, tool_call_id ASC"
        );
        let rows: Vec<ToolRecoveryPauseRow> = sqlx::query_as(&sql)
            .bind(run_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(ToolRecoveryPauseRow::into_record)
            .collect())
    }
}

// ── RFC-025 Phase 2b.3 m1: ingest_jobs read model (RFC 003) ──

#[derive(sqlx::FromRow)]
struct IngestJobRow {
    job_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    source_id: Option<String>,
    document_count: i32,
    state: String,
    error_message: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

impl IngestJobRow {
    fn into_record(self) -> Result<cairn_domain::IngestJobRecord, StoreError> {
        // i32 → u32 via .max(0) is safe: `document_count` is sourced from
        // a u32 on the write side (see `IngestJobStarted.document_count`
        // → `i32_from_u32`) so negative values indicate projection
        // corruption we cannot recover from anyway.
        Ok(cairn_domain::IngestJobRecord {
            id: cairn_domain::IngestJobId::new(self.job_id),
            project: cairn_domain::tenancy::ProjectKey::new(
                self.tenant_id,
                self.workspace_id,
                self.project_id,
            ),
            source_id: self.source_id.map(cairn_domain::ids::SourceId::new),
            document_count: self.document_count.max(0) as u32,
            state: crate::projections::rehydrate_ingest_job_state(&self.state)?,
            error_message: self.error_message,
            created_at: self.created_at_ms.max(0) as u64,
            updated_at: self.updated_at_ms.max(0) as u64,
        })
    }
}

const INGEST_JOB_SELECT_COLS: &str =
    "job_id, tenant_id, workspace_id, project_id, source_id, document_count, \
     state, error_message, created_at_ms, updated_at_ms";

// ── RFC-025 Phase 2b.3 m3: channels + channel_messages read models ──

#[derive(sqlx::FromRow)]
struct ChannelRow {
    channel_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    name: String,
    capacity: i32,
    created_at_ms: i64,
    updated_at_ms: i64,
}

impl ChannelRow {
    fn into_record(self) -> cairn_domain::ChannelRecord {
        cairn_domain::ChannelRecord {
            channel_id: cairn_domain::ChannelId::new(self.channel_id),
            project: cairn_domain::tenancy::ProjectKey::new(
                self.tenant_id,
                self.workspace_id,
                self.project_id,
            ),
            name: self.name,
            capacity: self.capacity.max(0) as u32,
            created_at: self.created_at_ms.max(0) as u64,
            updated_at: self.updated_at_ms.max(0) as u64,
        }
    }
}

#[derive(sqlx::FromRow)]
struct ChannelMessageRow {
    channel_id: String,
    message_id: String,
    sender_id: String,
    body: String,
    sent_at_ms: i64,
    consumed_by: Option<String>,
    consumed_at_ms: Option<i64>,
}

impl ChannelMessageRow {
    fn into_record(self) -> cairn_domain::ChannelMessage {
        cairn_domain::ChannelMessage {
            channel_id: cairn_domain::ChannelId::new(self.channel_id),
            message_id: self.message_id,
            sender_id: self.sender_id,
            body: self.body,
            sent_at_ms: self.sent_at_ms.max(0) as u64,
            consumed_by: self.consumed_by,
            consumed_at_ms: self.consumed_at_ms.map(|v| v.max(0) as u64),
        }
    }
}

const CHANNEL_SELECT_COLS: &str =
    "channel_id, tenant_id, workspace_id, project_id, name, capacity, \
     created_at_ms, updated_at_ms";

const CHANNEL_MESSAGE_SELECT_COLS: &str =
    "channel_id, message_id, sender_id, body, sent_at_ms, consumed_by, consumed_at_ms";

#[async_trait]
impl crate::projections::ChannelReadModel for PgAdapter {
    async fn get_channel(
        &self,
        channel_id: &cairn_domain::ChannelId,
    ) -> Result<Option<cairn_domain::ChannelRecord>, StoreError> {
        let sql = format!("SELECT {CHANNEL_SELECT_COLS} FROM channels WHERE channel_id = $1");
        let row: Option<ChannelRow> = sqlx::query_as(&sql)
            .bind(channel_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(ChannelRow::into_record))
    }

    async fn list_channels(
        &self,
        project: &cairn_domain::tenancy::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::ChannelRecord>, StoreError> {
        let sql = format!(
            "SELECT {CHANNEL_SELECT_COLS} FROM channels
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3
             ORDER BY created_at_ms ASC, channel_id ASC
             LIMIT $4 OFFSET $5"
        );
        let rows: Vec<ChannelRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows.into_iter().map(ChannelRow::into_record).collect())
    }

    async fn list_messages(
        &self,
        channel_id: &cairn_domain::ChannelId,
        limit: usize,
    ) -> Result<Vec<cairn_domain::ChannelMessage>, StoreError> {
        let sql = format!(
            "SELECT {CHANNEL_MESSAGE_SELECT_COLS} FROM channel_messages
             WHERE channel_id = $1
             ORDER BY sent_at_ms ASC, message_id ASC
             LIMIT $2"
        );
        let rows: Vec<ChannelMessageRow> = sqlx::query_as(&sql)
            .bind(channel_id.as_str())
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(ChannelMessageRow::into_record)
            .collect())
    }
}

// ── RFC-025 Phase 2b.3 m4: notification_preferences + notifications ──

#[derive(sqlx::FromRow)]
struct NotificationPrefRow {
    tenant_id: String,
    operator_id: String,
    pref_id: String,
    event_types_json: String,
    channels_json: String,
}

impl NotificationPrefRow {
    fn into_record(
        self,
    ) -> Result<cairn_domain::notification_prefs::NotificationPreference, StoreError> {
        let event_types: Vec<String> = serde_json::from_str(&self.event_types_json)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        let channels: Vec<cairn_domain::notification_prefs::NotificationChannel> =
            serde_json::from_str(&self.channels_json)
                .map_err(|e| StoreError::Serialization(e.to_string()))?;
        Ok(cairn_domain::notification_prefs::NotificationPreference {
            pref_id: self.pref_id,
            tenant_id: cairn_domain::TenantId::new(self.tenant_id),
            operator_id: self.operator_id,
            event_types,
            channels,
        })
    }
}

#[derive(sqlx::FromRow)]
struct NotificationRecordRow {
    record_id: String,
    tenant_id: String,
    operator_id: String,
    event_type: String,
    channel_kind: String,
    channel_target: String,
    payload_json: String,
    sent_at_ms: i64,
    /// Stored as INTEGER 0/1 for byte-equal parity with the sqlite
    /// schema (Copilot PR #594 review).
    delivered: i32,
    delivery_error: Option<String>,
}

impl NotificationRecordRow {
    fn into_record(
        self,
    ) -> Result<cairn_domain::notification_prefs::NotificationRecord, StoreError> {
        let payload: serde_json::Value = serde_json::from_str(&self.payload_json)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        Ok(cairn_domain::notification_prefs::NotificationRecord {
            record_id: self.record_id,
            tenant_id: cairn_domain::TenantId::new(self.tenant_id),
            operator_id: self.operator_id,
            event_type: self.event_type,
            channel_kind: self.channel_kind,
            channel_target: self.channel_target,
            payload,
            sent_at_ms: self.sent_at_ms.max(0) as u64,
            delivered: self.delivered != 0,
            delivery_error: self.delivery_error,
        })
    }
}

const NOTIFICATION_PREF_COLS: &str =
    "tenant_id, operator_id, pref_id, event_types_json, channels_json";

const NOTIFICATION_RECORD_COLS: &str =
    "record_id, tenant_id, operator_id, event_type, channel_kind, channel_target, \
     payload_json, sent_at_ms, delivered, delivery_error";

#[async_trait]
impl crate::projections::NotificationReadModel for PgAdapter {
    async fn get_preferences(
        &self,
        tenant_id: &cairn_domain::TenantId,
        operator_id: &str,
    ) -> Result<Option<cairn_domain::notification_prefs::NotificationPreference>, StoreError> {
        let sql = format!(
            "SELECT {NOTIFICATION_PREF_COLS} FROM notification_preferences
             WHERE tenant_id = $1 AND operator_id = $2"
        );
        let row: Option<NotificationPrefRow> = sqlx::query_as(&sql)
            .bind(tenant_id.as_str())
            .bind(operator_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(NotificationPrefRow::into_record).transpose()
    }

    async fn list_preferences_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Vec<cairn_domain::notification_prefs::NotificationPreference>, StoreError> {
        let sql = format!(
            "SELECT {NOTIFICATION_PREF_COLS} FROM notification_preferences
             WHERE tenant_id = $1
             ORDER BY operator_id ASC"
        );
        let rows: Vec<NotificationPrefRow> = sqlx::query_as(&sql)
            .bind(tenant_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(NotificationPrefRow::into_record)
            .collect()
    }

    async fn list_sent_notifications(
        &self,
        tenant_id: &cairn_domain::TenantId,
        since_ms: u64,
    ) -> Result<Vec<cairn_domain::notification_prefs::NotificationRecord>, StoreError> {
        let since = i64::try_from(since_ms)
            .map_err(|_| StoreError::Internal(format!("since_ms {since_ms} exceeds i64::MAX")))?;
        let sql = format!(
            "SELECT {NOTIFICATION_RECORD_COLS} FROM notifications
             WHERE tenant_id = $1 AND sent_at_ms >= $2
             ORDER BY sent_at_ms ASC, record_id ASC"
        );
        let rows: Vec<NotificationRecordRow> = sqlx::query_as(&sql)
            .bind(tenant_id.as_str())
            .bind(since)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(NotificationRecordRow::into_record)
            .collect()
    }

    async fn list_failed_notifications(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Vec<cairn_domain::notification_prefs::NotificationRecord>, StoreError> {
        let sql = format!(
            "SELECT {NOTIFICATION_RECORD_COLS} FROM notifications
             WHERE tenant_id = $1 AND delivered = 0
             ORDER BY sent_at_ms ASC, record_id ASC"
        );
        let rows: Vec<NotificationRecordRow> = sqlx::query_as(&sql)
            .bind(tenant_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(NotificationRecordRow::into_record)
            .collect()
    }
}

// ── RFC-025 Phase 2b.3 m2: default_settings read model ──

#[derive(sqlx::FromRow)]
struct DefaultSettingRow {
    scope: String,
    key: String,
    value_json: String,
}

impl DefaultSettingRow {
    fn into_record(self) -> Result<cairn_domain::DefaultSetting, StoreError> {
        let value: serde_json::Value = serde_json::from_str(&self.value_json)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        Ok(cairn_domain::DefaultSetting {
            key: self.key,
            value,
            scope: crate::projections::rehydrate_defaults_scope(&self.scope)?,
        })
    }
}

#[async_trait]
impl crate::projections::DefaultsReadModel for PgAdapter {
    async fn get(
        &self,
        scope: cairn_domain::Scope,
        scope_id: &str,
        key: &str,
    ) -> Result<Option<cairn_domain::DefaultSetting>, StoreError> {
        let row: Option<DefaultSettingRow> = sqlx::query_as(
            "SELECT scope, key, value_json FROM default_settings
             WHERE scope = $1 AND scope_id = $2 AND key = $3",
        )
        .bind(crate::projections::defaults_scope_str(scope))
        .bind(scope_id)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(DefaultSettingRow::into_record).transpose()
    }

    async fn list_by_scope(
        &self,
        scope: cairn_domain::Scope,
        scope_id: &str,
    ) -> Result<Vec<cairn_domain::DefaultSetting>, StoreError> {
        // ORDER BY key ASC for deterministic iteration — the in-memory
        // projection is sorted by the same key to keep byte-equal parity.
        let rows: Vec<DefaultSettingRow> = sqlx::query_as(
            "SELECT scope, key, value_json FROM default_settings
             WHERE scope = $1 AND scope_id = $2
             ORDER BY key ASC",
        )
        .bind(crate::projections::defaults_scope_str(scope))
        .bind(scope_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(DefaultSettingRow::into_record)
            .collect()
    }
}

#[async_trait]
impl crate::projections::IngestJobReadModel for PgAdapter {
    async fn get(
        &self,
        job_id: &cairn_domain::IngestJobId,
    ) -> Result<Option<cairn_domain::IngestJobRecord>, StoreError> {
        let sql = format!("SELECT {INGEST_JOB_SELECT_COLS} FROM ingest_jobs WHERE job_id = $1");
        let row: Option<IngestJobRow> = sqlx::query_as(&sql)
            .bind(job_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(IngestJobRow::into_record).transpose()
    }

    async fn list_by_project(
        &self,
        project: &cairn_domain::tenancy::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::IngestJobRecord>, StoreError> {
        let sql = format!(
            "SELECT {INGEST_JOB_SELECT_COLS} FROM ingest_jobs
             WHERE tenant_id = $1 AND workspace_id = $2 AND project_id = $3
             ORDER BY created_at_ms ASC, job_id ASC
             LIMIT $4 OFFSET $5"
        );
        let rows: Vec<IngestJobRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter().map(IngestJobRow::into_record).collect()
    }
}

// ── Issue #592: pause_schedules read model ─────────────────────────
//
// Replaces the event-log walker in `InMemoryStore` with an indexed
// range scan against `pause_schedules` (populated by the projection
// arm on RunStateChanged). Parity with the in-memory impl: same
// tenant filter, same `resume_at_ms <= before_ms` gate, same
// `ORDER BY resume_at_ms ASC, run_id ASC` tie-breaker, same limit
// semantics.

#[derive(sqlx::FromRow)]
struct PauseScheduleRow {
    run_id: String,
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    resume_at_ms: i64,
    created_at_ms: i64,
}

impl PauseScheduleRow {
    fn into_record(self) -> crate::projections::PauseScheduledRecord {
        // Copilot #595: i64 → u64 via `as` silently reinterpreted
        // negative DB values as huge u64 timestamps. Clamp corrupt
        // negative rows to 0 instead (immediately due + resolve on
        // next sweep) — masking them as u64::MAX would hide them from
        // `list_due` forever, which is the worse failure mode.
        let resume_at_ms = u64::try_from(self.resume_at_ms).unwrap_or(0);
        let created_at_ms = u64::try_from(self.created_at_ms).unwrap_or(0);
        crate::projections::PauseScheduledRecord {
            run_id: RunId::new(self.run_id),
            project: ProjectKey::new(
                self.tenant_id.as_str(),
                self.workspace_id.as_str(),
                self.project_id.as_str(),
            ),
            resume_at_ms,
            created_at_ms,
        }
    }
}

#[async_trait]
impl crate::projections::PauseScheduleReadModel for PgAdapter {
    async fn list_due(
        &self,
        tenant_id: &cairn_domain::TenantId,
        before_ms: u64,
        limit: usize,
    ) -> Result<Vec<crate::projections::PauseScheduledRecord>, StoreError> {
        // Copilot #595: `as i64` on out-of-range u64/usize silently
        // wrapped to negative, which Postgres interprets as "LIMIT -N"
        // (error) and `resume_at_ms <= -N` (filter matches nothing).
        // Clamp to i64::MAX so a caller passing `usize::MAX` becomes
        // "effectively unbounded" — which is the legitimate operator
        // drain pattern in `process_scheduled_run_resumes_handler` —
        // and a pathologically large `before_ms` becomes "all rows"
        // rather than "no rows".
        let before_ms_i64 = i64::try_from(before_ms).unwrap_or(i64::MAX);
        let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
        // Backend-stable ordering so the parity harness compares the
        // same rows in the same order against in-memory and sqlite.
        // Membership/eviction semantics are asserted byte-equal;
        // `resume_at_ms` is compared with sub-second tolerance
        // because append wall-clock can differ slightly per backend.
        let sql = "SELECT run_id, tenant_id, workspace_id, project_id,
                          resume_at_ms, created_at_ms
                   FROM pause_schedules
                   WHERE tenant_id = $1 AND resume_at_ms <= $2
                   ORDER BY resume_at_ms ASC, run_id ASC
                   LIMIT $3";
        let rows: Vec<PauseScheduleRow> = sqlx::query_as(sql)
            .bind(tenant_id.as_str())
            .bind(before_ms_i64)
            .bind(limit_i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(PauseScheduleRow::into_record)
            .collect())
    }
}

// ──────────────────────────────────────────────────────────────────────
// RFC-025 Phase 2b.4: pg read-model impls
// ──────────────────────────────────────────────────────────────────────
//
// The seven traits flipped Projected in this phase (eval catalog x3,
// operator profiles, run costs, run cost alerts, route policies) had
// SqliteAdapter + InMemoryStore impls landed in the initial milestones
// but no PgAdapter impl. Copilot PR #596 review surfaced this gap:
// handlers consume these read-models through `store.as_ref()` on
// `InMemoryStore` today (writes dual-write to pg via the secondary
// log) so production reads never hit pg directly — but the parity
// harness + any future cutover to pg as primary reader needs the impls
// present. Each impl mirrors the sqlite shape, swapping `?N` → `$N`,
// `INTEGER 0/1` → `BOOLEAN`, and `TEXT` JSON → `JSONB` where the pg
// migration (V018 for route_policies) diverges. Negative-int reads
// clamp via `.max(0) as u64` to match the sqlite defensive pattern.

/// Max parameters per chunked `IN ($1, $2, …)` clause on Postgres.
/// Postgres caps bind parameters at 65535, but cairn mirrors the
/// sqlite guard (900) so the two adapters behave identically under the
/// parity harness even on very large `list_by_tenant` result sets.
const PG_IN_CHUNK: usize = 900;

#[async_trait]
impl crate::projections::EvalDatasetReadModel for PgAdapter {
    async fn get_dataset(
        &self,
        dataset_id: &str,
    ) -> Result<Option<cairn_domain::EvalDataset>, StoreError> {
        let row: Option<(String, String, String, String, i64)> = sqlx::query_as(
            "SELECT dataset_id, tenant_id, name, subject_kind, created_at_ms
             FROM eval_datasets WHERE dataset_id = $1",
        )
        .bind(dataset_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        let Some((dataset_id, tenant_id, name, _subject_kind, created_at_ms)) = row else {
            return Ok(None);
        };
        let entries = self.load_eval_dataset_entries_pg(&dataset_id).await?;
        Ok(Some(cairn_domain::EvalDataset {
            dataset_id,
            tenant_id: cairn_domain::TenantId::new(tenant_id),
            name,
            subject_kind: cairn_domain::EvalSubjectKind::PromptRelease,
            entries,
            created_at_ms: created_at_ms.max(0) as u64,
        }))
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::EvalDataset>, StoreError> {
        // Mirrors the sqlite impl: sentinel empty-string tenant means
        // "all rows" (matches the in-memory `|| tenant_id.is_empty()`
        // branch for cross-backend parity).
        let rows: Vec<(String, String, String, String, i64)> = if tenant_id.as_str().is_empty() {
            sqlx::query_as(
                "SELECT dataset_id, tenant_id, name, subject_kind, created_at_ms
                 FROM eval_datasets
                 ORDER BY created_at_ms ASC, dataset_id ASC
                 LIMIT $1 OFFSET $2",
            )
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query_as(
                "SELECT dataset_id, tenant_id, name, subject_kind, created_at_ms
                 FROM eval_datasets
                 WHERE tenant_id = $1
                 ORDER BY created_at_ms ASC, dataset_id ASC
                 LIMIT $2 OFFSET $3",
            )
            .bind(tenant_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        // Chunked bulk-load mirrors sqlite (defensive 900-param guard
        // even though pg allows 65535). Placeholders `$1, $2, …` are
        // numbered per chunk.
        let dataset_ids: Vec<&String> = rows.iter().map(|r| &r.0).collect();
        let mut entries_by_dataset: std::collections::HashMap<
            String,
            Vec<cairn_domain::EvalDatasetEntry>,
        > = std::collections::HashMap::new();
        for chunk in dataset_ids.chunks(PG_IN_CHUNK) {
            let placeholders: Vec<String> = (1..=chunk.len()).map(|i| format!("${i}")).collect();
            let entries_sql = format!(
                "SELECT dataset_id, entry_id FROM eval_dataset_entries
                 WHERE dataset_id IN ({})
                 ORDER BY dataset_id ASC, added_at_ms ASC, entry_id ASC",
                placeholders.join(", ")
            );
            let mut entries_query = sqlx::query_as::<_, (String, String)>(&entries_sql);
            for dataset_id in chunk {
                entries_query = entries_query.bind(*dataset_id);
            }
            let entry_rows: Vec<(String, String)> = entries_query
                .fetch_all(&self.pool)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            for (dataset_id, entry_id) in entry_rows {
                entries_by_dataset.entry(dataset_id).or_default().push(
                    cairn_domain::EvalDatasetEntry {
                        input: serde_json::json!({ "entry_id": entry_id.clone() }),
                        expected_output: None,
                        tags: vec![entry_id],
                    },
                );
            }
        }
        let mut datasets = Vec::with_capacity(rows.len());
        for (dataset_id, tenant_id, name, _subject_kind, created_at_ms) in rows {
            let entries = entries_by_dataset.remove(&dataset_id).unwrap_or_default();
            datasets.push(cairn_domain::EvalDataset {
                dataset_id,
                tenant_id: cairn_domain::TenantId::new(tenant_id),
                name,
                subject_kind: cairn_domain::EvalSubjectKind::PromptRelease,
                entries,
                created_at_ms: created_at_ms.max(0) as u64,
            });
        }
        Ok(datasets)
    }
}

impl PgAdapter {
    /// Mirrors `SqliteAdapter::load_eval_dataset_entries`: reload a
    /// dataset's entries ordered `(added_at_ms ASC, entry_id ASC)`.
    async fn load_eval_dataset_entries_pg(
        &self,
        dataset_id: &str,
    ) -> Result<Vec<cairn_domain::EvalDatasetEntry>, StoreError> {
        let entries: Vec<(String,)> = sqlx::query_as(
            "SELECT entry_id FROM eval_dataset_entries
             WHERE dataset_id = $1
             ORDER BY added_at_ms ASC, entry_id ASC",
        )
        .bind(dataset_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(entries
            .into_iter()
            .map(|(entry_id,)| cairn_domain::EvalDatasetEntry {
                input: serde_json::json!({ "entry_id": entry_id.clone() }),
                expected_output: None,
                tags: vec![entry_id],
            })
            .collect())
    }
}

#[async_trait]
impl crate::projections::EvalRubricReadModel for PgAdapter {
    async fn get_rubric(
        &self,
        rubric_id: &str,
    ) -> Result<Option<cairn_domain::EvalRubric>, StoreError> {
        let row: Option<(String, String, String, i64)> = sqlx::query_as(
            "SELECT rubric_id, tenant_id, name, created_at_ms
             FROM eval_rubrics WHERE rubric_id = $1",
        )
        .bind(rubric_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(
            |(rubric_id, tenant_id, name, created_at_ms)| cairn_domain::EvalRubric {
                rubric_id,
                tenant_id: cairn_domain::TenantId::new(tenant_id),
                name,
                dimensions: vec![],
                created_at_ms: created_at_ms.max(0) as u64,
            },
        ))
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::EvalRubric>, StoreError> {
        let rows: Vec<(String, String, String, i64)> = if tenant_id.as_str().is_empty() {
            sqlx::query_as(
                "SELECT rubric_id, tenant_id, name, created_at_ms
                 FROM eval_rubrics
                 ORDER BY rubric_id ASC
                 LIMIT $1 OFFSET $2",
            )
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query_as(
                "SELECT rubric_id, tenant_id, name, created_at_ms
                 FROM eval_rubrics
                 WHERE tenant_id = $1
                 ORDER BY rubric_id ASC
                 LIMIT $2 OFFSET $3",
            )
            .bind(tenant_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(
                |(rubric_id, tenant_id, name, created_at_ms)| cairn_domain::EvalRubric {
                    rubric_id,
                    tenant_id: cairn_domain::TenantId::new(tenant_id),
                    name,
                    dimensions: vec![],
                    created_at_ms: created_at_ms.max(0) as u64,
                },
            )
            .collect())
    }
}

#[async_trait]
impl crate::projections::EvalBaselineReadModel for PgAdapter {
    async fn get_baseline(
        &self,
        baseline_id: &str,
    ) -> Result<Option<cairn_domain::EvalBaseline>, StoreError> {
        // `locked` is INTEGER on pg (per V063 schema — see comment in
        // that migration about keeping the column shape byte-identical
        // with sqlite for parity harness diffs).
        let row: Option<(String, String, String, String, i64, i64)> = sqlx::query_as(
            "SELECT baseline_id, tenant_id, name, prompt_asset_id, created_at_ms, locked
             FROM eval_baselines WHERE baseline_id = $1",
        )
        .bind(baseline_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(
            |(baseline_id, tenant_id, name, prompt_asset_id, created_at_ms, locked)| {
                cairn_domain::EvalBaseline {
                    baseline_id,
                    tenant_id: cairn_domain::TenantId::new(tenant_id),
                    name,
                    prompt_asset_id: cairn_domain::PromptAssetId::new(prompt_asset_id),
                    metrics: cairn_domain::EvalMetrics::default(),
                    created_at_ms: created_at_ms.max(0) as u64,
                    locked: locked != 0,
                }
            },
        ))
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::EvalBaseline>, StoreError> {
        let rows: Vec<(String, String, String, String, i64, i64)> =
            if tenant_id.as_str().is_empty() {
                sqlx::query_as(
                    "SELECT baseline_id, tenant_id, name, prompt_asset_id, created_at_ms, locked
                     FROM eval_baselines
                     ORDER BY baseline_id ASC
                     LIMIT $1 OFFSET $2",
                )
                .bind(limit as i64)
                .bind(offset as i64)
                .fetch_all(&self.pool)
                .await
            } else {
                sqlx::query_as(
                    "SELECT baseline_id, tenant_id, name, prompt_asset_id, created_at_ms, locked
                     FROM eval_baselines
                     WHERE tenant_id = $1
                     ORDER BY baseline_id ASC
                     LIMIT $2 OFFSET $3",
                )
                .bind(tenant_id.as_str())
                .bind(limit as i64)
                .bind(offset as i64)
                .fetch_all(&self.pool)
                .await
            }
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(
                |(baseline_id, tenant_id, name, prompt_asset_id, created_at_ms, locked)| {
                    cairn_domain::EvalBaseline {
                        baseline_id,
                        tenant_id: cairn_domain::TenantId::new(tenant_id),
                        name,
                        prompt_asset_id: cairn_domain::PromptAssetId::new(prompt_asset_id),
                        metrics: cairn_domain::EvalMetrics::default(),
                        created_at_ms: created_at_ms.max(0) as u64,
                        locked: locked != 0,
                    }
                },
            )
            .collect())
    }
}

#[async_trait]
impl crate::projections::OperatorProfileReadModel for PgAdapter {
    async fn get(
        &self,
        operator_id: &cairn_domain::OperatorId,
    ) -> Result<Option<crate::projections::OperatorProfileRecord>, StoreError> {
        let row: Option<(String, String, String, String, String, i64)> = sqlx::query_as(
            "SELECT operator_id, tenant_id, display_name, email, role, created_at_ms
             FROM operator_profiles WHERE operator_id = $1",
        )
        .bind(operator_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(
            |(operator_id, tenant_id, display_name, email, role, created_at)| {
                crate::projections::OperatorProfileRecord {
                    operator_id: cairn_domain::OperatorId::new(operator_id),
                    tenant_id: cairn_domain::TenantId::new(tenant_id),
                    display_name,
                    // Email is NOT NULL on the projection column;
                    // `None` is reserved for a future event version
                    // that makes email optional. Mirrors sqlite.
                    email: Some(email),
                    role,
                    created_at: created_at.max(0) as u64,
                }
            },
        ))
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::OperatorProfileRecord>, StoreError> {
        let rows: Vec<(String, String, String, String, String, i64)> = sqlx::query_as(
            "SELECT operator_id, tenant_id, display_name, email, role, created_at_ms
             FROM operator_profiles
             WHERE tenant_id = $1
             ORDER BY operator_id ASC
             LIMIT $2 OFFSET $3",
        )
        .bind(tenant_id.as_str())
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(
                |(operator_id, tenant_id, display_name, email, role, created_at)| {
                    crate::projections::OperatorProfileRecord {
                        operator_id: cairn_domain::OperatorId::new(operator_id),
                        tenant_id: cairn_domain::TenantId::new(tenant_id),
                        display_name,
                        email: Some(email),
                        role,
                        created_at: created_at.max(0) as u64,
                    }
                },
            )
            .collect())
    }
}

#[async_trait]
impl crate::projections::OperatorTenantRoleReadModel for PgAdapter {
    async fn get(
        &self,
        tenant_id: &cairn_domain::TenantId,
        operator_id: &cairn_domain::OperatorId,
    ) -> Result<Option<crate::projections::OperatorTenantRoleRecord>, StoreError> {
        let row: Option<(
            String,
            String,
            String,
            i64,
            String,
            Option<i64>,
            Option<String>,
        )> = sqlx::query_as(
            "SELECT tenant_id, operator_id, role, granted_at_ms, granted_by,
                    revoked_at_ms, revoked_by
             FROM operator_tenant_roles
             WHERE tenant_id = $1 AND operator_id = $2",
        )
        .bind(tenant_id.as_str())
        .bind(operator_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(pg_row_to_operator_tenant_role))
    }

    async fn list_by_operator(
        &self,
        operator_id: &cairn_domain::OperatorId,
    ) -> Result<Vec<crate::projections::OperatorTenantRoleRecord>, StoreError> {
        let rows: Vec<(
            String,
            String,
            String,
            i64,
            String,
            Option<i64>,
            Option<String>,
        )> = sqlx::query_as(
            "SELECT tenant_id, operator_id, role, granted_at_ms, granted_by,
                    revoked_at_ms, revoked_by
             FROM operator_tenant_roles
             WHERE operator_id = $1
             ORDER BY tenant_id ASC",
        )
        .bind(operator_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(pg_row_to_operator_tenant_role)
            .collect())
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::OperatorTenantRoleRecord>, StoreError> {
        let rows: Vec<(
            String,
            String,
            String,
            i64,
            String,
            Option<i64>,
            Option<String>,
        )> = sqlx::query_as(
            "SELECT tenant_id, operator_id, role, granted_at_ms, granted_by,
                    revoked_at_ms, revoked_by
             FROM operator_tenant_roles
             WHERE tenant_id = $1
             ORDER BY operator_id ASC
             LIMIT $2 OFFSET $3",
        )
        .bind(tenant_id.as_str())
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(pg_row_to_operator_tenant_role)
            .collect())
    }
}

/// Row → record conversion shared by the three pg reads above. See the
/// sqlite sibling in `sqlite/adapter.rs::row_to_operator_tenant_role` —
/// kept per-backend to avoid leaking sqlx type-tuple signatures through
/// a shared crate-level helper.
fn pg_row_to_operator_tenant_role(
    row: (
        String,
        String,
        String,
        i64,
        String,
        Option<i64>,
        Option<String>,
    ),
) -> crate::projections::OperatorTenantRoleRecord {
    let (tenant_id, operator_id, role, granted_at_ms, granted_by, revoked_at_ms, revoked_by) = row;
    let role: cairn_domain::tenancy::TenantRole = serde_json::from_str(&format!("\"{role}\""))
        .unwrap_or(
            // Defensive: default to ReadOnly (least-privilege) on a
            // corrupt row. Mirrors sqlite.
            cairn_domain::tenancy::TenantRole::ReadOnly,
        );
    crate::projections::OperatorTenantRoleRecord {
        tenant_id: cairn_domain::TenantId::new(tenant_id),
        operator_id: cairn_domain::OperatorId::new(operator_id),
        role,
        granted_at_ms: granted_at_ms.max(0) as u64,
        granted_by,
        revoked_at_ms: revoked_at_ms.map(|v| v.max(0) as u64),
        revoked_by,
    }
}

#[async_trait]
impl crate::projections::RoutePolicyReadModel for PgAdapter {
    async fn get(
        &self,
        policy_id: &str,
    ) -> Result<Option<cairn_domain::providers::RoutePolicy>, StoreError> {
        // route_policies diverges from the other 2b.4 tables: V018 is
        // pre-RFC-025 and uses pg-native `JSONB rules` + `BOOLEAN
        // enabled`. Decoding here pulls `rules` as `serde_json::Value`
        // and `enabled` as `bool`, matching the write path in
        // `pg/projections.rs::RoutePolicyCreated`.
        let row: Option<(String, String, String, serde_json::Value, bool, i64, i64)> =
            sqlx::query_as(
                "SELECT policy_id, tenant_id, name, rules, enabled, created_at, updated_at
             FROM route_policies WHERE policy_id = $1",
            )
            .bind(policy_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(pg_route_policy_row_into_record).transpose()
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::providers::RoutePolicy>, StoreError> {
        // Mirrors the sqlite impl: enabled-only, tenant-scoped,
        // policy_id ASC tiebreaker.
        let rows: Vec<(String, String, String, serde_json::Value, bool, i64, i64)> =
            sqlx::query_as(
                "SELECT policy_id, tenant_id, name, rules, enabled, created_at, updated_at
             FROM route_policies
             WHERE tenant_id = $1 AND enabled = TRUE
             ORDER BY policy_id ASC
             LIMIT $2 OFFSET $3",
            )
            .bind(tenant_id.as_str())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(pg_route_policy_row_into_record)
            .collect()
    }
}

fn pg_route_policy_row_into_record(
    (policy_id, tenant_id, name, rules_json, enabled, _created_at, updated_at): (
        String,
        String,
        String,
        serde_json::Value,
        bool,
        i64,
        i64,
    ),
) -> Result<cairn_domain::providers::RoutePolicy, StoreError> {
    let rules: Vec<cairn_domain::providers::RoutePolicyRule> = serde_json::from_value(rules_json)
        .map_err(|e| {
        StoreError::Serialization(format!(
            "route_policies.rules decode for policy_id={policy_id}: {e}"
        ))
    })?;
    Ok(cairn_domain::providers::RoutePolicy {
        policy_id,
        name,
        enabled,
        tenant_id,
        rules,
        updated_at_ms: updated_at.max(0) as u64,
    })
}

#[async_trait]
impl crate::projections::RunCostReadModel for PgAdapter {
    async fn get_run_cost(
        &self,
        run_id: &cairn_domain::RunId,
    ) -> Result<Option<cairn_domain::providers::RunCostRecord>, StoreError> {
        let row: Option<(String, i64, i64, i64, i64, i64)> = sqlx::query_as(
            "SELECT run_id, total_cost_micros, total_tokens_in, total_tokens_out,
                    provider_calls, updated_at_ms
             FROM run_costs WHERE run_id = $1",
        )
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(pg_run_cost_row_into_record))
    }

    async fn list_by_session(
        &self,
        session_id: &cairn_domain::SessionId,
    ) -> Result<Vec<cairn_domain::providers::RunCostRecord>, StoreError> {
        let rows: Vec<(String, i64, i64, i64, i64, i64)> = sqlx::query_as(
            "SELECT rc.run_id, rc.total_cost_micros, rc.total_tokens_in, rc.total_tokens_out,
                    rc.provider_calls, rc.updated_at_ms
             FROM run_costs rc
             INNER JOIN runs r ON r.run_id = rc.run_id
             WHERE r.session_id = $1
             ORDER BY rc.updated_at_ms DESC, rc.run_id ASC",
        )
        .bind(session_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows.into_iter().map(pg_run_cost_row_into_record).collect())
    }
}

fn pg_run_cost_row_into_record(
    (run_id, total_cost_micros, total_tokens_in, total_tokens_out, provider_calls, _updated_at_ms): (
        String,
        i64,
        i64,
        i64,
        i64,
        i64,
    ),
) -> cairn_domain::providers::RunCostRecord {
    let total_cost_micros = total_cost_micros.max(0) as u64;
    let total_tokens_in = total_tokens_in.max(0) as u64;
    let total_tokens_out = total_tokens_out.max(0) as u64;
    let provider_calls = provider_calls.max(0) as u64;
    cairn_domain::providers::RunCostRecord {
        run_id: cairn_domain::RunId::new(run_id),
        total_cost_micros,
        total_tokens_in,
        total_tokens_out,
        provider_calls,
        token_in: total_tokens_in,
        token_out: total_tokens_out,
    }
}

#[async_trait]
impl crate::projections::RunCostAlertReadModel for PgAdapter {
    async fn get_alert(
        &self,
        run_id: &cairn_domain::RunId,
    ) -> Result<Option<cairn_domain::providers::RunCostAlert>, StoreError> {
        let row: Option<(String, String, i64, i64, i64, i64)> = sqlx::query_as(
            "SELECT run_id, tenant_id, threshold_micros, triggered_at_ms,
                    actual_cost_micros, set_at_ms
             FROM run_cost_alerts WHERE run_id = $1",
        )
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(pg_run_cost_alert_row_into_record))
    }

    async fn list_triggered_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::providers::RunCostAlert>, StoreError> {
        // Mirrors sqlite: only return alerts that have actually
        // triggered (`triggered_at_ms > 0`), newest-first, scoped to
        // the tenant. The pg index
        // `idx_run_cost_alerts_tenant_triggered` (V065) backs this
        // query shape exactly.
        let rows: Vec<(String, String, i64, i64, i64, i64)> = sqlx::query_as(
            "SELECT run_id, tenant_id, threshold_micros, triggered_at_ms,
                    actual_cost_micros, set_at_ms
             FROM run_cost_alerts
             WHERE tenant_id = $1 AND triggered_at_ms > 0
             ORDER BY triggered_at_ms DESC, run_id ASC
             LIMIT $2 OFFSET $3",
        )
        .bind(tenant_id.as_str())
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(pg_run_cost_alert_row_into_record)
            .collect())
    }
}

fn pg_run_cost_alert_row_into_record(
    (run_id, tenant_id, threshold_micros, triggered_at_ms, actual_cost_micros, _set_at_ms): (
        String,
        String,
        i64,
        i64,
        i64,
        i64,
    ),
) -> cairn_domain::providers::RunCostAlert {
    cairn_domain::providers::RunCostAlert {
        run_id: cairn_domain::RunId::new(run_id),
        tenant_id: cairn_domain::TenantId::new(tenant_id),
        threshold_micros: threshold_micros.max(0) as u64,
        triggered_at_ms: triggered_at_ms.max(0) as u64,
        actual_cost_micros: actual_cost_micros.max(0) as u64,
    }
}

// ── RFC 031 PR-B2: AgentRoleReadModel impl ───────────────────────────────

#[derive(sqlx::FromRow)]
struct AgentRoleRow {
    tenant_id: String,
    workspace_id: String,
    project_id: String,
    role_id: String,
    role_json: String,
    shadows_builtin: Option<String>,
    defined_by: String,
    defined_at: i64,
    retracted_at: Option<i64>,
    retracted_by: Option<String>,
}

impl AgentRoleRow {
    fn into_record(self) -> Result<crate::projections::AgentRoleRecord, StoreError> {
        let role: cairn_domain::agent_roles::AgentRole = serde_json::from_str(&self.role_json)
            .map_err(|err| StoreError::Serialization(err.to_string()))?;
        Ok(crate::projections::AgentRoleRecord {
            project: ProjectKey::new(
                self.tenant_id.as_str(),
                self.workspace_id.as_str(),
                self.project_id.as_str(),
            ),
            role_id: self.role_id,
            role,
            shadows_builtin: self.shadows_builtin,
            defined_by: OperatorId::new(self.defined_by),
            defined_at: self.defined_at.max(0) as u64,
            retracted_at: self.retracted_at.map(|v| v.max(0) as u64),
            retracted_by: self.retracted_by.map(OperatorId::new),
        })
    }
}

#[async_trait]
impl crate::projections::AgentRoleReadModel for PgAdapter {
    async fn get_active(
        &self,
        project: &ProjectKey,
        role_id: &str,
    ) -> Result<Option<crate::projections::AgentRoleRecord>, StoreError> {
        let sql = format!(
            "SELECT {cols} FROM project_agent_roles \
             WHERE tenant_id = $1 AND workspace_id = $2 \
               AND project_id = $3 AND role_id = $4 \
               AND retracted_at IS NULL",
            cols = crate::projections::AGENT_ROLE_PROJECTION_COLS
        );
        let row: Option<AgentRoleRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .bind(role_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(AgentRoleRow::into_record).transpose()
    }

    async fn get_any(
        &self,
        project: &ProjectKey,
        role_id: &str,
    ) -> Result<Option<crate::projections::AgentRoleRecord>, StoreError> {
        let sql = format!(
            "SELECT {cols} FROM project_agent_roles \
             WHERE tenant_id = $1 AND workspace_id = $2 \
               AND project_id = $3 AND role_id = $4",
            cols = crate::projections::AGENT_ROLE_PROJECTION_COLS
        );
        let row: Option<AgentRoleRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .bind(role_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        row.map(AgentRoleRow::into_record).transpose()
    }

    async fn list_active(
        &self,
        project: &ProjectKey,
    ) -> Result<Vec<crate::projections::AgentRoleRecord>, StoreError> {
        let sql = format!(
            "SELECT {cols} FROM project_agent_roles \
             WHERE tenant_id = $1 AND workspace_id = $2 \
               AND project_id = $3 AND retracted_at IS NULL \
             ORDER BY role_id ASC",
            cols = crate::projections::AGENT_ROLE_PROJECTION_COLS
        );
        let rows: Vec<AgentRoleRow> = sqlx::query_as(&sql)
            .bind(project.tenant_id.as_str())
            .bind(project.workspace_id.as_str())
            .bind(project.project_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        rows.into_iter().map(AgentRoleRow::into_record).collect()
    }
}
