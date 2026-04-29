use std::time::{SystemTime, UNIX_EPOCH};

use cairn_domain::{EventEnvelope, RuntimeEvent};

use crate::error::StoreError;

/// SQLite-backed synchronous projection applier for local-mode deploys.
///
/// **Coverage gap relative to PgSyncProjection.** This applier implements
/// projections for the core operational state machines (session, run, task,
/// approval, checkpoint, tool_invocation, mailbox) and silently ignores the
/// remaining ~95 `RuntimeEvent` variants. The append path in
/// `SqliteEventLog` invokes this applier inside the insert transaction, but
/// stubbed variants commit only to the `event_log` table — their projection
/// tables either do not exist in the SQLite schema or would be overwritten
/// on replay.
///
/// Each stubbed variant is logged at `tracing::warn!` level so operators
/// running `--db sqlite:…` can see in real time which RFC features are
/// being silently dropped by the local-mode backend. If you land on this
/// warning in a production log, either (a) switch to the Postgres backend,
/// which projects every variant, or (b) extend this applier to cover the
/// variant you care about.
///
/// Audit reference: `.claude/audit-state/review-queue.md` §T2-C2.
pub struct SqliteSyncProjection;

/// Log a received-but-unprojected event variant. Keeps the stub-match arms
/// uniform and makes the coverage gap visible without spamming logs when
/// no stub variants are ever received.
fn log_stub(variant: &'static str) {
    tracing::warn!(
        event_variant = variant,
        "sqlite projection stub: event committed to event_log but no projection table updated \
         (see SqliteSyncProjection docstring for the coverage gap)"
    );
}

impl SqliteSyncProjection {
    /// Async projection application within a SQLite transaction.
    ///
    /// Takes the envelope by reference (not a full `StoredEvent`) so the
    /// hot append path does not need to clone the potentially-large
    /// payload on every event — see #498.
    pub async fn apply_async(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        envelope: &EventEnvelope<RuntimeEvent>,
    ) -> Result<(), StoreError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        match &envelope.payload {
            RuntimeEvent::SessionCreated(e) => {
                // Idempotent: a second SessionCreated event for the same id
                // must not blow up the transaction. The event log is the
                // durable source of truth; the projection is derived and
                // should tolerate duplicates (mirrors PG's ON CONFLICT
                // DO NOTHING shape). Matches pg/projections.rs.
                sqlx::query(
                    "INSERT INTO sessions (session_id, tenant_id, workspace_id, project_id, state, version, created_at, updated_at)
                     VALUES (?, ?, ?, ?, 'open', 1, ?, ?)
                     ON CONFLICT(session_id) DO NOTHING",
                )
                .bind(e.session_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(now)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            }

            RuntimeEvent::SessionStateChanged(e) => {
                let state_str = enum_to_str(&e.transition.to)?;
                sqlx::query(
                    "UPDATE sessions SET state = ?, version = version + 1, updated_at = ? WHERE session_id = ?",
                )
                .bind(state_str)
                .bind(now)
                .bind(e.session_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            }

            RuntimeEvent::RunCreated(e) => {
                sqlx::query(
                    "INSERT INTO runs (run_id, session_id, parent_run_id, tenant_id, workspace_id, project_id, state, version, created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?, ?, 'pending', 1, ?, ?)",
                )
                .bind(e.run_id.as_str())
                .bind(e.session_id.as_str())
                .bind(e.parent_run_id.as_ref().map(|id| id.as_str()))
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(now)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            }

            RuntimeEvent::RunStateChanged(e) => {
                let state_str = enum_to_str(&e.transition.to)?;
                let failure = e.failure_class.as_ref().map(enum_to_str).transpose()?;
                sqlx::query(
                    "UPDATE runs SET state = ?, failure_class = ?, version = version + 1, updated_at = ? WHERE run_id = ?",
                )
                .bind(state_str)
                .bind(failure)
                .bind(now)
                .bind(e.run_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            }

            RuntimeEvent::TaskCreated(e) => {
                // Prefer the session_id on the event; fall back to the
                // parent run's session_id for tasks that carried no binding.
                // COALESCE lets SQLite resolve both in one statement.
                let session_id_on_event = e.session_id.as_ref().map(|s| s.as_str());
                sqlx::query(
                    "INSERT INTO tasks (task_id, tenant_id, workspace_id, project_id, parent_run_id, parent_task_id, session_id, state, version, created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?, ?,
                        COALESCE(?, (SELECT session_id FROM runs WHERE run_id = ?)),
                        'queued', 1, ?, ?)",
                )
                .bind(e.task_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.parent_run_id.as_ref().map(|id| id.as_str()))
                .bind(e.parent_task_id.as_ref().map(|id| id.as_str()))
                .bind(session_id_on_event)
                .bind(e.parent_run_id.as_ref().map(|id| id.as_str()))
                .bind(now)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            }

            RuntimeEvent::TaskLeaseClaimed(e) => {
                sqlx::query(
                    "UPDATE tasks SET state = 'leased', lease_owner = ?, lease_expires_at = ?, lease_version = ?, version = version + 1, updated_at = ? WHERE task_id = ?",
                )
                .bind(&e.lease_owner)
                .bind(e.lease_expires_at_ms as i64)
                .bind(e.lease_token as i64)
                .bind(now)
                .bind(e.task_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            }

            RuntimeEvent::TaskLeaseHeartbeated(e) => {
                sqlx::query(
                    "UPDATE tasks SET lease_expires_at = ?, lease_version = ?, version = version + 1, updated_at = ? WHERE task_id = ?",
                )
                .bind(e.lease_expires_at_ms as i64)
                .bind(e.lease_token as i64)
                .bind(now)
                .bind(e.task_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            }

            RuntimeEvent::TaskStateChanged(e) => {
                let state_str = enum_to_str(&e.transition.to)?;
                let failure = e.failure_class.as_ref().map(enum_to_str).transpose()?;
                sqlx::query(
                    "UPDATE tasks SET state = ?, failure_class = ?, version = version + 1, updated_at = ? WHERE task_id = ?",
                )
                .bind(state_str)
                .bind(failure)
                .bind(now)
                .bind(e.task_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            }

            RuntimeEvent::ApprovalRequested(e) => {
                let requirement_str = enum_to_str(&e.requirement)?;
                sqlx::query(
                    "INSERT INTO approvals (approval_id, tenant_id, workspace_id, project_id, run_id, task_id, requirement, title, description, version, created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
                )
                .bind(e.approval_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.run_id.as_ref().map(|id| id.as_str()))
                .bind(e.task_id.as_ref().map(|id| id.as_str()))
                .bind(requirement_str)
                .bind(e.title.as_deref())
                .bind(e.description.as_deref())
                .bind(now)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            }

            RuntimeEvent::ApprovalResolved(e) => {
                let decision_str = enum_to_str(&e.decision)?;
                sqlx::query(
                    "UPDATE approvals SET decision = ?, version = version + 1, updated_at = ? WHERE approval_id = ?",
                )
                .bind(decision_str)
                .bind(now)
                .bind(e.approval_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            }

            RuntimeEvent::CheckpointRecorded(e) => {
                let disposition_str = enum_to_str(&e.disposition)?;

                if disposition_str == "latest" {
                    sqlx::query(
                        "UPDATE checkpoints SET disposition = 'superseded', version = version + 1 WHERE run_id = ? AND disposition = 'latest'",
                    )
                    .bind(e.run_id.as_str())
                    .execute(&mut **tx)
                    .await
                    .map_err(|e| StoreError::Internal(e.to_string()))?;
                }

                sqlx::query(
                    "INSERT INTO checkpoints (checkpoint_id, tenant_id, workspace_id, project_id, run_id, disposition, version, created_at)
                     VALUES (?, ?, ?, ?, ?, ?, 1, ?)",
                )
                .bind(e.checkpoint_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.run_id.as_str())
                .bind(disposition_str)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            }

            RuntimeEvent::CheckpointRestored(_) => {}

            RuntimeEvent::MailboxMessageAppended(e) => {
                sqlx::query(
                    "INSERT INTO mailbox_messages (message_id, tenant_id, workspace_id, project_id, run_id, task_id, version, created_at)
                     VALUES (?, ?, ?, ?, ?, ?, 1, ?)",
                )
                .bind(e.message_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.run_id.as_ref().map(|id| id.as_str()))
                .bind(e.task_id.as_ref().map(|id| id.as_str()))
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            }

            RuntimeEvent::ToolInvocationStarted(e) => {
                let target = serde_json::to_string(&e.target)
                    .map_err(|e| StoreError::Serialization(e.to_string()))?;
                let exec_class_str = enum_to_str(&e.execution_class)?;
                // F55: SQLite has no native JSONB so we store args as a
                // JSON string — matches the Postgres applier's semantics
                // while staying on the portable-TEXT path.
                let args_text = match &e.args_json {
                    Some(value) => Some(
                        serde_json::to_string(value)
                            .map_err(|err| StoreError::Serialization(err.to_string()))?,
                    ),
                    None => None,
                };

                sqlx::query(
                    "INSERT INTO tool_invocations (invocation_id, tenant_id, workspace_id, project_id, session_id, run_id, task_id, target, execution_class, state, requested_at_ms, started_at_ms, args_json, version, created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'started', ?, ?, ?, 1, ?, ?)",
                )
                .bind(e.invocation_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.session_id.as_ref().map(|id| id.as_str()))
                .bind(e.run_id.as_ref().map(|id| id.as_str()))
                .bind(e.task_id.as_ref().map(|id| id.as_str()))
                .bind(&target)
                .bind(exec_class_str)
                .bind(e.requested_at_ms as i64)
                .bind(e.started_at_ms as i64)
                .bind(args_text)
                .bind(now)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            }

            RuntimeEvent::ToolInvocationCompleted(e) => {
                let outcome_str = enum_to_str(&e.outcome)?;
                sqlx::query(
                    "UPDATE tool_invocations SET state = 'completed', outcome = ?, finished_at_ms = ?, output_preview = COALESCE(?, output_preview), version = version + 1, updated_at = ? WHERE invocation_id = ?",
                )
                .bind(outcome_str)
                .bind(e.finished_at_ms as i64)
                .bind(e.output_preview.as_deref())
                .bind(now)
                .bind(e.invocation_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            }

            RuntimeEvent::ToolInvocationFailed(e) => {
                let outcome_str = enum_to_str(&e.outcome)?;
                // Route terminal state through the same helper PG uses so a
                // canceled outcome lands as `state='canceled'` (not `'failed'`);
                // pre-T2-H5 SQLite hardcoded `'failed'` and mislabeled cancels.
                let state_str = enum_to_str(&e.outcome.terminal_state())?;
                sqlx::query(
                    "UPDATE tool_invocations SET state = ?, outcome = ?, error_message = ?, finished_at_ms = ?, output_preview = COALESCE(?, output_preview), version = version + 1, updated_at = ? WHERE invocation_id = ?",
                )
                .bind(state_str)
                .bind(outcome_str)
                .bind(e.error_message.as_deref())
                .bind(e.finished_at_ms as i64)
                .bind(e.output_preview.as_deref())
                .bind(now)
                .bind(e.invocation_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            }

            // ── UNPROJECTED STUBS ──────────────────────────────────────
            // These variants commit to event_log but do NOT update any
            // projection table on the SQLite backend. See the struct
            // docstring for the coverage-gap rationale and logging.
            RuntimeEvent::ExternalWorkerRegistered(_) => log_stub("ExternalWorkerRegistered"),
            RuntimeEvent::ExternalWorkerReported(_) => log_stub("ExternalWorkerReported"),
            RuntimeEvent::ExternalWorkerSuspended(_) => log_stub("ExternalWorkerSuspended"),
            RuntimeEvent::ExternalWorkerReactivated(_) => log_stub("ExternalWorkerReactivated"),
            RuntimeEvent::SoulPatchProposed(_) => log_stub("SoulPatchProposed"),
            RuntimeEvent::SoulPatchApplied(_) => log_stub("SoulPatchApplied"),
            // F29 CD-2: see pg/projections.rs for the full contract —
            // session/project/workspace rollups update atomically.
            RuntimeEvent::SessionCostUpdated(e) => {
                // Source tenant from the explicit `e.tenant_id` field
                // (matches InMemory + pg). See the pg SessionCostUpdated
                // handler for the full rationale on why this and not
                // `project.tenant_id`.
                upsert_cost_rollups_sqlite(
                    tx,
                    e.session_id.as_str(),
                    e.tenant_id.as_str(),
                    &e.project,
                    e.delta_cost_micros,
                    e.delta_tokens_in,
                    e.delta_tokens_out,
                    e.updated_at_ms,
                )
                .await?;
            }
            RuntimeEvent::RunCostUpdated(_) => log_stub("RunCostUpdated"),
            RuntimeEvent::SpendAlertTriggered(_) => log_stub("SpendAlertTriggered"),
            RuntimeEvent::SubagentSpawned(_) => log_stub("SubagentSpawned"),
            // F39: durable projections for RFC 002 recovery audits.
            // Projection row is keyed on the envelope `event_id` and
            // guarded by `ON CONFLICT(event_id) DO NOTHING`, so a
            // replayed event leaves the row count unchanged. Nullable
            // run_id / task_id / boot_id preserve the struct shape
            // verbatim; the `has_target()` invariant is enforced at
            // the emitter, not here.
            RuntimeEvent::RecoveryAttempted(e) => {
                sqlx::query(
                    "INSERT INTO recovery_attempts
                         (event_id, tenant_id, workspace_id, project_id,
                          run_id, task_id, reason, boot_id, recorded_at_ms)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(event_id) DO NOTHING",
                )
                .bind(envelope.event_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.run_id.as_ref().map(|r| r.as_str()))
                .bind(e.task_id.as_ref().map(|t| t.as_str()))
                .bind(&e.reason)
                .bind(e.boot_id.as_deref())
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::RecoveryCompleted(e) => {
                sqlx::query(
                    "INSERT INTO recovery_completions
                         (event_id, tenant_id, workspace_id, project_id,
                          run_id, task_id, recovered, boot_id, recorded_at_ms)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(event_id) DO NOTHING",
                )
                .bind(envelope.event_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.run_id.as_ref().map(|r| r.as_str()))
                .bind(e.task_id.as_ref().map(|t| t.as_str()))
                // sqlx maps `bool` to SQLite INTEGER (0/1); binding the
                // field directly keeps parity with the pg handler and
                // avoids a manual cast.
                .bind(e.recovered)
                .bind(e.boot_id.as_deref())
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::SignalIngested(_) => log_stub("SignalIngested"),
            RuntimeEvent::UserMessageAppended(_) => log_stub("UserMessageAppended"),
            RuntimeEvent::IngestJobStarted(_) => log_stub("IngestJobStarted"),
            RuntimeEvent::IngestJobCompleted(_) => log_stub("IngestJobCompleted"),
            // RFC-025 Phase 1 (milestone 4): `eval_runs` projection —
            // sqlite parity with the pg applier in
            // `crates/cairn-store/src/pg/projections.rs`. Same
            // last-write-wins + earliest-wins-on-archive semantics,
            // same serde-JSON-in-TEXT shape for metrics / rubric
            // verdict. If the two appliers ever drift, the
            // projection_parity harness below catches it.
            RuntimeEvent::EvalRunStarted(e) => {
                sqlx::query(
                    "INSERT INTO eval_runs
                         (eval_run_id, tenant_id, workspace_id, project_id,
                          subject_kind, evaluator_type,
                          success, error_message, started_at, completed_at,
                          archived_at, metrics_json, rubric_score_json,
                          dataset_id, rubric_id, baseline_id,
                          prompt_asset_id, prompt_version_id, prompt_release_id,
                          created_by)
                     VALUES (?, ?, ?, ?, ?, ?,
                             NULL, NULL, ?, NULL, NULL, NULL, NULL,
                             ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(eval_run_id) DO NOTHING",
                )
                .bind(e.eval_run_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(&e.subject_kind)
                .bind(&e.evaluator_type)
                .bind(e.started_at as i64)
                .bind(e.dataset_id.as_deref())
                .bind(e.rubric_id.as_deref())
                .bind(e.baseline_id.as_deref())
                .bind(e.prompt_asset_id.as_ref().map(|v| v.as_str()))
                .bind(e.prompt_version_id.as_ref().map(|v| v.as_str()))
                .bind(e.prompt_release_id.as_ref().map(|v| v.as_str()))
                .bind(e.created_by.as_ref().map(|v| v.as_str()))
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::EvalRunCompleted(e) => {
                sqlx::query(
                    "UPDATE eval_runs
                     SET success = ?,
                         error_message = ?,
                         completed_at = ?
                     WHERE eval_run_id = ?",
                )
                .bind(e.success)
                .bind(e.error_message.as_deref())
                .bind(e.completed_at as i64)
                .bind(e.eval_run_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::EvalRunArchived(e) => {
                sqlx::query(
                    "UPDATE eval_runs
                     SET archived_at = ?
                     WHERE eval_run_id = ? AND archived_at IS NULL",
                )
                .bind(e.archived_at as i64)
                .bind(e.eval_run_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::EvalRunScored(e) => {
                let metrics_json = serde_json::to_string(&e.metrics)
                    .map_err(|err| StoreError::Internal(err.to_string()))?;
                sqlx::query("UPDATE eval_runs SET metrics_json = ? WHERE eval_run_id = ?")
                    .bind(metrics_json)
                    .bind(e.eval_run_id.as_str())
                    .execute(&mut **tx)
                    .await
                    .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::EvalRubricScored(e) => {
                let summary = crate::projections::EvalRubricScoreSummary {
                    rubric_id: e.rubric_id.clone(),
                    dimension_scores: e.dimension_scores.clone(),
                    overall: e.overall,
                    recorded_at_ms: e.recorded_at_ms,
                };
                let rubric_json = serde_json::to_string(&summary)
                    .map_err(|err| StoreError::Internal(err.to_string()))?;
                sqlx::query("UPDATE eval_runs SET rubric_score_json = ? WHERE eval_run_id = ?")
                    .bind(rubric_json)
                    .bind(e.eval_run_id.as_str())
                    .execute(&mut **tx)
                    .await
                    .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::PromptAssetCreated(e) => {
                sqlx::query(
                    "INSERT INTO prompt_assets
                         (prompt_asset_id, tenant_id, workspace_id, project_id, name, kind,
                          scope, status, created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?, ?, NULL, 'draft', ?, ?)
                     ON CONFLICT(prompt_asset_id) DO NOTHING",
                )
                .bind(e.prompt_asset_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(&e.name)
                .bind(&e.kind)
                .bind(e.created_at as i64)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::PromptVersionCreated(e) => {
                // SQLite has no `SELECT ... FOR UPDATE`. sqlx opens
                // transactions as DEFERRED by default, so concurrent
                // appenders to the same asset could in principle both
                // compute MAX+1 and insert the same version_number. In
                // local-mode there is a single append path serialized
                // by `SqliteEventLog`, which makes this safe in
                // practice. The defensive `UNIQUE(prompt_asset_id,
                // version_number)` constraint in schema.rs (and
                // Postgres V023) converts any future concurrent
                // allocation bug into a hard error instead of silent
                // duplicate rows.
                let version_number: i64 = sqlx::query_scalar(
                    "SELECT COALESCE(MAX(version_number), 0) + 1
                     FROM prompt_versions
                     WHERE prompt_asset_id = ?",
                )
                .bind(e.prompt_asset_id.as_str())
                .fetch_one(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;

                sqlx::query(
                    "INSERT INTO prompt_versions
                         (prompt_version_id, prompt_asset_id, tenant_id, workspace_id, project_id,
                          version_number, content_hash, content, format, created_by, created_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?)
                     ON CONFLICT(prompt_version_id) DO NOTHING",
                )
                .bind(e.prompt_version_id.as_str())
                .bind(e.prompt_asset_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(version_number)
                .bind(&e.content_hash)
                .bind(e.created_at as i64)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::ApprovalPolicyCreated(_) => log_stub("ApprovalPolicyCreated"),
            RuntimeEvent::PromptReleaseCreated(e) => {
                sqlx::query(
                    "INSERT INTO prompt_releases
                         (prompt_release_id, prompt_asset_id, prompt_version_id,
                          tenant_id, workspace_id, project_id,
                          release_tag, state, rollout_target, created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, 'draft', NULL, ?, ?)
                     ON CONFLICT(prompt_release_id) DO NOTHING",
                )
                .bind(e.prompt_release_id.as_str())
                .bind(e.prompt_asset_id.as_str())
                .bind(e.prompt_version_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.release_tag.as_deref())
                .bind(e.created_at as i64)
                .bind(e.created_at as i64)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::PromptReleaseTransitioned(e) => {
                sqlx::query(
                    "UPDATE prompt_releases
                     SET state = ?, updated_at = ?
                     WHERE prompt_release_id = ?",
                )
                .bind(&e.to_state)
                .bind(now)
                .bind(e.prompt_release_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // RFC 001 gradual rollout — state tracked via prompt_releases;
            // no dedicated projection table.
            RuntimeEvent::PromptRolloutStarted(_) => {}
            RuntimeEvent::TenantCreated(e) => {
                sqlx::query(
                    "INSERT INTO tenants (tenant_id, name, created_at, updated_at)
                     VALUES (?, ?, ?, ?)
                     ON CONFLICT(tenant_id) DO NOTHING",
                )
                .bind(e.tenant_id.as_str())
                .bind(&e.name)
                .bind(e.created_at as i64)
                .bind(e.created_at as i64)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::WorkspaceCreated(e) => {
                sqlx::query(
                    "INSERT INTO workspaces (workspace_id, tenant_id, name, created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?)
                     ON CONFLICT(workspace_id) DO NOTHING",
                )
                .bind(e.workspace_id.as_str())
                .bind(e.tenant_id.as_str())
                .bind(&e.name)
                .bind(e.created_at as i64)
                .bind(e.created_at as i64)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::WorkspaceArchived(e) => {
                // `tenant_id` in the WHERE clause is defense-in-depth —
                // the service layer already rejects cross-tenant archives
                // before emitting, but a replay or injected event with a
                // mismatched tenant_id should no-op rather than touch
                // another tenant's row.
                sqlx::query(
                    "UPDATE workspaces
                        SET archived_at = ?, updated_at = ?
                      WHERE workspace_id = ? AND tenant_id = ?",
                )
                .bind(e.archived_at as i64)
                .bind(e.archived_at as i64)
                .bind(e.workspace_id.as_str())
                .bind(e.tenant_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::ProjectCreated(e) => {
                sqlx::query(
                    "INSERT INTO projects (project_id, workspace_id, tenant_id, name, created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?, ?)
                     ON CONFLICT(project_id) DO NOTHING",
                )
                .bind(e.project.project_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(&e.name)
                .bind(e.created_at as i64)
                .bind(e.created_at as i64)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::RouteDecisionMade(e) => {
                let operation_kind = enum_to_str(&e.operation_kind)?;
                let final_status = enum_to_str(&e.final_status)?;
                // selector_context is not carried by the event (mirrors PG).
                let selector_ctx: Option<String> = None;
                sqlx::query(
                    "INSERT INTO route_decisions
                         (route_decision_id, tenant_id, workspace_id, project_id,
                          operation_kind, route_policy_id, terminal_route_attempt_id,
                          selected_provider_binding_id, selected_route_attempt_id,
                          selector_context, attempt_count, fallback_used, final_status,
                          created_at)
                     VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, NULL, ?, ?, ?, ?, ?)
                     ON CONFLICT(route_decision_id) DO NOTHING",
                )
                .bind(e.route_decision_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(operation_kind)
                .bind(
                    e.selected_provider_binding_id
                        .as_ref()
                        .map(|id| id.as_str()),
                )
                .bind(selector_ctx)
                .bind(e.attempt_count as i64)
                .bind(i64::from(e.fallback_used))
                .bind(final_status)
                .bind(e.decided_at as i64)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::ProviderCallCompleted(e) => {
                let operation_kind = enum_to_str(&e.operation_kind)?;
                let status = enum_to_str(&e.status)?;
                let error_class = e.error_class.as_ref().map(enum_to_str).transpose()?;
                let latency_ms: Option<i64> = e.latency_ms.map(|v| v as i64).or_else(|| {
                    if e.started_at > 0 && e.finished_at >= e.started_at {
                        Some((e.finished_at - e.started_at) as i64)
                    } else {
                        None
                    }
                });
                sqlx::query(
                    "INSERT INTO provider_calls
                         (provider_call_id, route_decision_id, route_attempt_id,
                          tenant_id, workspace_id, project_id,
                          operation_kind, provider_binding_id, provider_connection_id,
                          provider_adapter, provider_model_id,
                          task_id, run_id, prompt_release_id, fallback_position,
                          status, latency_ms, input_tokens, output_tokens, cost_micros,
                          error_class, raw_error_message, retry_count, created_at)
                     VALUES
                         (?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?,
                          ?, ?, ?, ?, ?, ?, ?, ?, ?,
                          ?, ?, ?, ?)
                     ON CONFLICT(provider_call_id) DO NOTHING",
                )
                .bind(e.provider_call_id.as_str())
                .bind(e.route_decision_id.as_str())
                .bind(e.route_attempt_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(operation_kind)
                .bind(e.provider_binding_id.as_str())
                .bind(e.provider_connection_id.as_str())
                .bind(e.provider_model_id.as_str())
                .bind(e.task_id.as_ref().map(|id| id.as_str()))
                .bind(e.run_id.as_ref().map(|id| id.as_str()))
                .bind(e.prompt_release_id.as_ref().map(|id| id.as_str()))
                .bind(e.fallback_position as i64)
                .bind(status)
                .bind(latency_ms)
                .bind(e.input_tokens.map(|v| v as i64))
                .bind(e.output_tokens.map(|v| v as i64))
                .bind(e.cost_micros.map(|v| v as i64))
                .bind(error_class)
                .bind(e.raw_error_message.as_deref())
                .bind(e.retry_count as i64)
                .bind(e.completed_at as i64)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;

                // F29 CD-2: fold the call into session/project/workspace
                // cost rollups. Mirrors the pg path — see
                // `upsert_cost_rollups_sqlite` for the monotonic-
                // `updated_at_ms` contract and overflow semantics.
                let effective_session_id: Option<String> = if let Some(sid) = &e.session_id {
                    Some(sid.as_str().to_owned())
                } else if let Some(rid) = &e.run_id {
                    sqlx::query_scalar::<_, Option<String>>(
                        "SELECT session_id FROM runs WHERE run_id = ?",
                    )
                    .bind(rid.as_str())
                    .fetch_optional(&mut **tx)
                    .await
                    .map_err(|err| StoreError::Internal(err.to_string()))?
                    .flatten()
                } else {
                    None
                };
                if let Some(sid) = effective_session_id {
                    upsert_cost_rollups_sqlite(
                        tx,
                        &sid,
                        e.project.tenant_id.as_str(),
                        &e.project,
                        e.cost_micros.unwrap_or(0),
                        e.input_tokens.unwrap_or(0) as u64,
                        e.output_tokens.unwrap_or(0) as u64,
                        e.completed_at,
                    )
                    .await?;
                }
            }
            RuntimeEvent::OutcomeRecorded(_) => log_stub("OutcomeRecorded"),
            RuntimeEvent::ScheduledTaskCreated(_) => log_stub("ScheduledTaskCreated"),
            RuntimeEvent::PlanProposed(_) => log_stub("PlanProposed"),
            RuntimeEvent::PlanApproved(_) => log_stub("PlanApproved"),
            RuntimeEvent::PlanRejected(_) => log_stub("PlanRejected"),
            RuntimeEvent::PlanRevisionRequested(_) => log_stub("PlanRevisionRequested"),
            // RFC-025 Phase 2a.1 milestone 3: provider budgets projection
            // (sqlite parity with pg).
            RuntimeEvent::ProviderBudgetSet(e) => {
                let period_str = provider_budget_period_str_sqlite(&e.period);
                let limit_i64 = i64::try_from(e.limit_micros).map_err(|_| {
                    StoreError::Internal(format!(
                        "ProviderBudgetSet.limit_micros {} exceeds i64::MAX",
                        e.limit_micros
                    ))
                })?;
                // Centralised domain default — see
                // `cairn_domain::providers::DEFAULT_BUDGET_ALERT_THRESHOLD_PERCENT`.
                let threshold_u = e
                    .alert_threshold_percent
                    .unwrap_or(cairn_domain::providers::DEFAULT_BUDGET_ALERT_THRESHOLD_PERCENT);
                let threshold =
                    i32_from_u32_sqlite("ProviderBudgetSet.alert_threshold_percent", threshold_u)?;
                sqlx::query(
                    "INSERT INTO provider_budgets (
                        budget_id, tenant_id, period, limit_micros,
                        alert_threshold_percent, current_spend_micros,
                        alert_triggered_at_ms, exceeded_at_ms, created_at, updated_at
                     ) VALUES (?, ?, ?, ?, ?, 0, NULL, NULL, ?, ?)
                     ON CONFLICT(budget_id) DO UPDATE SET
                        tenant_id               = excluded.tenant_id,
                        period                  = excluded.period,
                        limit_micros            = excluded.limit_micros,
                        alert_threshold_percent = excluded.alert_threshold_percent,
                        updated_at              = excluded.updated_at",
                )
                .bind(e.budget_id.as_str())
                .bind(e.tenant_id.as_str())
                .bind(period_str)
                .bind(limit_i64)
                .bind(threshold)
                .bind(now)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::ChannelCreated(_) => log_stub("ChannelCreated"),
            RuntimeEvent::ChannelMessageSent(_) => log_stub("ChannelMessageSent"),
            RuntimeEvent::ChannelMessageConsumed(_) => log_stub("ChannelMessageConsumed"),
            RuntimeEvent::DefaultSettingSet(_) => log_stub("DefaultSettingSet"),
            RuntimeEvent::DefaultSettingCleared(_) => log_stub("DefaultSettingCleared"),
            // RFC-025 Phase 2a.1 milestone 4: licenses projection
            // (sqlite parity with pg).
            RuntimeEvent::LicenseActivated(e) => {
                let issued_at = i64::try_from(e.valid_from_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "LicenseActivated.valid_from_ms {} exceeds i64::MAX",
                        e.valid_from_ms
                    ))
                })?;
                let expires_at = e
                    .valid_until_ms
                    .map(|v| {
                        i64::try_from(v).map_err(|_| {
                            StoreError::Internal(format!(
                                "LicenseActivated.valid_until_ms {v} exceeds i64::MAX"
                            ))
                        })
                    })
                    .transpose()?;
                let tier_str = product_tier_str_sqlite(&e.tier);
                sqlx::query(
                    "INSERT INTO licenses (
                        tenant_id, license_key, tier, entitlements_json,
                        issued_at, expires_at, created_at, updated_at
                     ) VALUES (?, ?, ?, '[]', ?, ?, ?, ?)
                     ON CONFLICT(tenant_id) DO UPDATE SET
                        license_key       = excluded.license_key,
                        tier              = excluded.tier,
                        entitlements_json = excluded.entitlements_json,
                        issued_at         = excluded.issued_at,
                        expires_at        = excluded.expires_at,
                        updated_at        = excluded.updated_at",
                )
                .bind(e.tenant_id.as_str())
                .bind(e.license_id.as_str())
                .bind(tier_str)
                .bind(issued_at)
                .bind(expires_at)
                .bind(now)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::EntitlementOverrideSet(_) => log_stub("EntitlementOverrideSet"),
            RuntimeEvent::NotificationPreferenceSet(_) => log_stub("NotificationPreferenceSet"),
            RuntimeEvent::NotificationSent(_) => log_stub("NotificationSent"),
            RuntimeEvent::ProviderPoolCreated(_) => log_stub("ProviderPoolCreated"),
            RuntimeEvent::ProviderPoolConnectionAdded(_) => log_stub("ProviderPoolConnectionAdded"),
            RuntimeEvent::ProviderPoolConnectionRemoved(_) => {
                log_stub("ProviderPoolConnectionRemoved")
            }
            // RFC-025 Phase 2a.1 milestone 2: tenant quotas projection
            // (sqlite parity with pg).
            RuntimeEvent::TenantQuotaSet(e) => {
                // Copilot PR #565: fail loudly on u32 → i32 overflow
                // rather than silently wrapping. See pg projection + the
                // shared `i32_from_u32_sqlite` helper.
                let max_concurrent_runs = i32_from_u32_sqlite(
                    "TenantQuotaSet.max_concurrent_runs",
                    e.max_concurrent_runs,
                )?;
                let max_sessions_per_hour = i32_from_u32_sqlite(
                    "TenantQuotaSet.max_sessions_per_hour",
                    e.max_sessions_per_hour,
                )?;
                let max_tasks_per_run =
                    i32_from_u32_sqlite("TenantQuotaSet.max_tasks_per_run", e.max_tasks_per_run)?;
                sqlx::query(
                    "INSERT INTO tenant_quotas (
                        tenant_id, max_concurrent_runs, max_sessions_per_hour,
                        max_tasks_per_run, created_at, updated_at
                     ) VALUES (?, ?, ?, ?, ?, ?)
                     ON CONFLICT(tenant_id) DO UPDATE SET
                        max_concurrent_runs   = excluded.max_concurrent_runs,
                        max_sessions_per_hour = excluded.max_sessions_per_hour,
                        max_tasks_per_run     = excluded.max_tasks_per_run,
                        updated_at            = excluded.updated_at",
                )
                .bind(e.tenant_id.as_str())
                .bind(max_concurrent_runs)
                .bind(max_sessions_per_hour)
                .bind(max_tasks_per_run)
                .bind(now)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::TenantQuotaViolated(e) => {
                let occurred_at = i64::try_from(e.occurred_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "TenantQuotaViolated.occurred_at_ms {} exceeds i64::MAX",
                        e.occurred_at_ms
                    ))
                })?;
                let current = i32_from_u32_sqlite("TenantQuotaViolated.current", e.current)?;
                let limit = i32_from_u32_sqlite("TenantQuotaViolated.limit", e.limit)?;
                sqlx::query(
                    "INSERT INTO tenant_quota_violations (
                        tenant_id, quota_type, occurred_at_ms, current_value, limit_value
                     ) VALUES (?, ?, ?, ?, ?)
                     ON CONFLICT(tenant_id, quota_type, occurred_at_ms) DO NOTHING",
                )
                .bind(e.tenant_id.as_str())
                .bind(e.quota_type.as_str())
                .bind(occurred_at)
                .bind(current)
                .bind(limit)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::RetentionPolicySet(_) => log_stub("RetentionPolicySet"),
            RuntimeEvent::RunCostAlertSet(_) => log_stub("RunCostAlertSet"),
            RuntimeEvent::RunCostAlertTriggered(_) => log_stub("RunCostAlertTriggered"),
            RuntimeEvent::WorkspaceMemberAdded(e) => {
                let role = enum_to_str(&e.role)?;
                sqlx::query(
                    "INSERT INTO workspace_members (workspace_id, operator_id, role, added_at_ms)
                     VALUES (?, ?, ?, ?)
                     ON CONFLICT(workspace_id, operator_id) DO UPDATE SET role = excluded.role",
                )
                .bind(e.workspace_key.workspace_id.as_str())
                .bind(e.member_id.as_str())
                .bind(role)
                .bind(e.added_at_ms as i64)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::WorkspaceMemberRemoved(e) => {
                sqlx::query(
                    "DELETE FROM workspace_members WHERE workspace_id = ? AND operator_id = ?",
                )
                .bind(e.workspace_key.workspace_id.as_str())
                .bind(e.member_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::ApprovalDelegated(_) => log_stub("ApprovalDelegated"),
            RuntimeEvent::AuditLogEntryRecorded(_) => log_stub("AuditLogEntryRecorded"),
            RuntimeEvent::CheckpointStrategySet(_) => log_stub("CheckpointStrategySet"),
            // RFC-025 Phase 2a.1: credentials projection — sqlite parity
            // with pg. Keep `ON CONFLICT DO UPDATE` semantics identical
            // so the parity harness can byte-compare both backends for
            // the same event sequence. `active` is stored as INTEGER
            // 0/1 per sqlx convention.
            RuntimeEvent::CredentialStored(e) => {
                let encrypted_at = i64::try_from(e.encrypted_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "CredentialStored.encrypted_at_ms {} exceeds i64::MAX",
                        e.encrypted_at_ms
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO credentials (
                        credential_id, tenant_id, name, provider_id, credential_type,
                        encrypted_value, key_id, key_version, active,
                        encrypted_at_ms, revoked_at_ms, created_at, updated_at
                     ) VALUES (?, ?, ?, ?, 'api_key', ?, ?, ?, 1, ?, NULL, ?, ?)
                     ON CONFLICT(credential_id) DO UPDATE SET
                        encrypted_value = excluded.encrypted_value,
                        key_id          = excluded.key_id,
                        key_version     = excluded.key_version,
                        encrypted_at_ms = excluded.encrypted_at_ms,
                        updated_at      = excluded.updated_at",
                )
                .bind(e.credential_id.as_str())
                .bind(e.tenant_id.as_str())
                .bind(e.provider_id.as_str())
                .bind(e.provider_id.as_str())
                .bind(&e.encrypted_value)
                .bind(e.key_id.as_deref())
                .bind(e.key_version.as_deref())
                .bind(encrypted_at)
                .bind(encrypted_at)
                .bind(encrypted_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::CredentialRevoked(e) => {
                let revoked_at = i64::try_from(e.revoked_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "CredentialRevoked.revoked_at_ms {} exceeds i64::MAX",
                        e.revoked_at_ms
                    ))
                })?;
                // Latest-wins on revoked_at_ms (parity with pg + in_memory).
                sqlx::query(
                    "UPDATE credentials
                     SET active = 0,
                         revoked_at_ms = ?,
                         updated_at = ?
                     WHERE credential_id = ?",
                )
                .bind(revoked_at)
                .bind(revoked_at)
                .bind(e.credential_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::CredentialKeyRotated(e) => {
                sqlx::query(
                    "INSERT INTO credential_rotations (
                        rotation_id, tenant_id, credential_id,
                        old_key_id, new_key_id, rotated_credentials,
                        started_at_ms, completed_at_ms, rotated_at, rotated_by
                     ) VALUES (?, ?, '', ?, ?, ?, ?, ?, ?, NULL)
                     ON CONFLICT(rotation_id) DO NOTHING",
                )
                .bind(e.rotation_id.as_str())
                .bind(e.tenant_id.as_str())
                .bind(e.old_key_id.as_str())
                .bind(e.new_key_id.as_str())
                .bind(e.credential_ids_rotated.len() as i32)
                .bind(now)
                .bind(now)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::EvalBaselineLocked(_) => log_stub("EvalBaselineLocked"),
            RuntimeEvent::EvalBaselineSet(_) => log_stub("EvalBaselineSet"),
            RuntimeEvent::EvalDatasetCreated(_) => log_stub("EvalDatasetCreated"),
            RuntimeEvent::EvalDatasetEntryAdded(_) => log_stub("EvalDatasetEntryAdded"),
            RuntimeEvent::EvalRubricCreated(_) => log_stub("EvalRubricCreated"),
            RuntimeEvent::EventLogCompacted(_) => log_stub("EventLogCompacted"),
            RuntimeEvent::GuardrailPolicyCreated(_) => log_stub("GuardrailPolicyCreated"),
            RuntimeEvent::GuardrailPolicyEvaluated(_) => log_stub("GuardrailPolicyEvaluated"),
            RuntimeEvent::OperatorIntervention(_) => log_stub("OperatorIntervention"),
            RuntimeEvent::OperatorProfileCreated(_) => log_stub("OperatorProfileCreated"),
            RuntimeEvent::OperatorProfileUpdated(_) => log_stub("OperatorProfileUpdated"),
            RuntimeEvent::PauseScheduled(_) => log_stub("PauseScheduled"),
            RuntimeEvent::PermissionDecisionRecorded(_) => log_stub("PermissionDecisionRecorded"),
            RuntimeEvent::ProviderBindingCreated(_) => log_stub("ProviderBindingCreated"),
            RuntimeEvent::ProviderBindingStateChanged(_) => log_stub("ProviderBindingStateChanged"),
            RuntimeEvent::ProviderBudgetAlertTriggered(e) => {
                let triggered_at = i64::try_from(e.triggered_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "ProviderBudgetAlertTriggered.triggered_at_ms {} exceeds i64::MAX",
                        e.triggered_at_ms
                    ))
                })?;
                let current = i64::try_from(e.current_micros).map_err(|_| {
                    StoreError::Internal(format!(
                        "ProviderBudgetAlertTriggered.current_micros {} exceeds i64::MAX",
                        e.current_micros
                    ))
                })?;
                sqlx::query(
                    "UPDATE provider_budgets
                     SET current_spend_micros = ?,
                         alert_triggered_at_ms = ?,
                         updated_at = ?
                     WHERE budget_id = ?",
                )
                .bind(current)
                .bind(triggered_at)
                .bind(triggered_at)
                .bind(e.budget_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::ProviderBudgetExceeded(e) => {
                let exceeded_at = i64::try_from(e.exceeded_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "ProviderBudgetExceeded.exceeded_at_ms {} exceeds i64::MAX",
                        e.exceeded_at_ms
                    ))
                })?;
                let over = i64::try_from(e.exceeded_by_micros).map_err(|_| {
                    StoreError::Internal(format!(
                        "ProviderBudgetExceeded.exceeded_by_micros {} exceeds i64::MAX",
                        e.exceeded_by_micros
                    ))
                })?;
                sqlx::query(
                    "UPDATE provider_budgets
                     SET current_spend_micros = limit_micros + ?,
                         exceeded_at_ms = COALESCE(exceeded_at_ms, ?),
                         updated_at = ?
                     WHERE budget_id = ?",
                )
                .bind(over)
                .bind(exceeded_at)
                .bind(exceeded_at)
                .bind(e.budget_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::ProviderConnectionRegistered(_) => {
                log_stub("ProviderConnectionRegistered")
            }
            RuntimeEvent::ProviderConnectionDeleted(_) => log_stub("ProviderConnectionDeleted"),
            RuntimeEvent::ProviderHealthChecked(_) => log_stub("ProviderHealthChecked"),
            RuntimeEvent::ProviderHealthScheduleSet(_) => log_stub("ProviderHealthScheduleSet"),
            RuntimeEvent::ProviderHealthScheduleTriggered(_) => {
                log_stub("ProviderHealthScheduleTriggered")
            }
            RuntimeEvent::ProviderMarkedDegraded(_) => log_stub("ProviderMarkedDegraded"),
            RuntimeEvent::ProviderModelRegistered(_) => log_stub("ProviderModelRegistered"),
            RuntimeEvent::ProviderRecovered(_) => log_stub("ProviderRecovered"),
            RuntimeEvent::ProviderRetryPolicySet(_) => log_stub("ProviderRetryPolicySet"),
            RuntimeEvent::RecoveryEscalated(_) => log_stub("RecoveryEscalated"),
            RuntimeEvent::ResourceShareRevoked(_) => log_stub("ResourceShareRevoked"),
            RuntimeEvent::ResourceShared(_) => log_stub("ResourceShared"),
            RuntimeEvent::RoutePolicyCreated(e) => {
                // `rules` is a JSON string (SQLite has no JSONB); serialised
                // on write and parsed wholesale on read by the service layer.
                // `enabled` is projected from the event's `enabled` field —
                // bool maps to SQLite INTEGER (0/1) via sqlx. Symmetric with
                // PG where the column is BOOLEAN.
                let rules = serde_json::to_string(&e.rules)
                    .map_err(|err| StoreError::Serialization(err.to_string()))?;
                sqlx::query(
                    "INSERT INTO route_policies (policy_id, tenant_id, name, rules, enabled, created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(policy_id) DO UPDATE
                     SET name = excluded.name,
                         rules = excluded.rules,
                         enabled = excluded.enabled,
                         updated_at = excluded.updated_at",
                )
                .bind(&e.policy_id)
                .bind(e.tenant_id.as_str())
                .bind(&e.name)
                .bind(rules)
                .bind(e.enabled)
                .bind(now)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // PG's projection does not consume RoutePolicyUpdated — the event
            // is kept for audit, and the rules set is advanced by the next
            // RoutePolicyCreated upsert. SQLite mirrors that shape.
            RuntimeEvent::RoutePolicyUpdated(_) => log_stub("RoutePolicyUpdated"),
            RuntimeEvent::RunSlaBreached(_) => log_stub("RunSlaBreached"),
            RuntimeEvent::RunSlaSet(_) => log_stub("RunSlaSet"),
            RuntimeEvent::SignalRouted(_) => log_stub("SignalRouted"),
            RuntimeEvent::SignalSubscriptionCreated(_) => log_stub("SignalSubscriptionCreated"),
            RuntimeEvent::TriggerCreated(_) => log_stub("TriggerCreated"),
            RuntimeEvent::TriggerEnabled(_) => log_stub("TriggerEnabled"),
            RuntimeEvent::TriggerDisabled(_) => log_stub("TriggerDisabled"),
            RuntimeEvent::TriggerSuspended(_) => log_stub("TriggerSuspended"),
            RuntimeEvent::TriggerResumed(_) => log_stub("TriggerResumed"),
            RuntimeEvent::TriggerDeleted(_) => log_stub("TriggerDeleted"),
            RuntimeEvent::TriggerFired(_) => log_stub("TriggerFired"),
            RuntimeEvent::TriggerSkipped(_) => log_stub("TriggerSkipped"),
            RuntimeEvent::TriggerDenied(_) => log_stub("TriggerDenied"),
            RuntimeEvent::TriggerRateLimited(_) => log_stub("TriggerRateLimited"),
            RuntimeEvent::TriggerPendingApproval(_) => log_stub("TriggerPendingApproval"),
            RuntimeEvent::RunTemplateCreated(_) => log_stub("RunTemplateCreated"),
            RuntimeEvent::RunTemplateDeleted(_) => log_stub("RunTemplateDeleted"),
            RuntimeEvent::SnapshotCreated(_) => log_stub("SnapshotCreated"),
            RuntimeEvent::TaskDependencyAdded(_) => log_stub("TaskDependencyAdded"),
            RuntimeEvent::TaskDependencyResolved(_) => log_stub("TaskDependencyResolved"),
            RuntimeEvent::TaskLeaseExpired(_) => log_stub("TaskLeaseExpired"),
            RuntimeEvent::TaskPriorityChanged(_) => log_stub("TaskPriorityChanged"),
            // #364: durable projection for progress updates. UPSERT keyed
            // by `invocation_id`; we copy the project scope from the
            // existing `tool_invocations` row so the handler can
            // tenant-filter without a second lookup. Events for
            // invocations that do not exist yet (should not happen in
            // practice — started always precedes progress) are a no-op
            // rather than silently fabricating a project scope.
            RuntimeEvent::ToolInvocationProgressUpdated(e) => {
                let scope: Option<(String, String, String)> = sqlx::query_as(
                    "SELECT tenant_id, workspace_id, project_id
                     FROM tool_invocations
                     WHERE invocation_id = ?",
                )
                .bind(e.invocation_id.as_str())
                .fetch_optional(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;

                if let Some((tenant_id, workspace_id, project_id)) = scope {
                    let updated_at = i64::try_from(e.updated_at_ms).map_err(|_| {
                        StoreError::Internal(format!(
                            "ToolInvocationProgressUpdated.updated_at_ms {} exceeds i64::MAX",
                            e.updated_at_ms
                        ))
                    })?;
                    sqlx::query(
                        "INSERT INTO tool_invocation_progress
                             (invocation_id, tenant_id, workspace_id, project_id,
                              progress_pct, message, updated_at_ms)
                         VALUES (?, ?, ?, ?, ?, ?, ?)
                         ON CONFLICT(invocation_id) DO UPDATE SET
                             progress_pct  = excluded.progress_pct,
                             message       = excluded.message,
                             updated_at_ms = excluded.updated_at_ms
                         WHERE excluded.updated_at_ms >= tool_invocation_progress.updated_at_ms",
                    )
                    .bind(e.invocation_id.as_str())
                    .bind(tenant_id)
                    .bind(workspace_id)
                    .bind(project_id)
                    .bind(i64::from(e.progress_pct))
                    .bind(e.message.as_deref())
                    .bind(updated_at)
                    .execute(&mut **tx)
                    .await
                    .map_err(|err| StoreError::Internal(err.to_string()))?;
                }
            }
            // Projection contract: mirrors the pg handler so operator
            // REST queries over cache activity work identically across
            // backends (no-DB-specific-features rule). One row per
            // `invocation_id`; `ON CONFLICT DO NOTHING` absorbs replay.
            RuntimeEvent::ToolInvocationCacheHit(e) => {
                let original_completed_at =
                    i64::try_from(e.original_completed_at_ms).map_err(|_| {
                        StoreError::Internal(format!(
                            "ToolInvocationCacheHit.original_completed_at_ms {} exceeds i64::MAX",
                            e.original_completed_at_ms
                        ))
                    })?;
                let served_at = i64::try_from(e.served_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "ToolInvocationCacheHit.served_at_ms {} exceeds i64::MAX",
                        e.served_at_ms
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO tool_invocation_cache_hits
                         (invocation_id, tenant_id, workspace_id, project_id,
                          run_id, task_id, tool_name, tool_call_id,
                          original_completed_at_ms, served_at_ms)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(invocation_id) DO NOTHING",
                )
                .bind(e.invocation_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.run_id.as_ref().map(|r| r.as_str()))
                .bind(e.task_id.as_ref().map(|t| t.as_str()))
                .bind(e.tool_name.as_str())
                .bind(e.tool_call_id.as_str())
                .bind(original_completed_at)
                .bind(served_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::ToolRecoveryPaused(_) => log_stub("ToolRecoveryPaused"),
            // F39: RFC 019 / RFC 020 decision-cache projection. The
            // in-memory cache is still rebuilt from the event log at
            // boot; these tables give operator tooling a queryable
            // read model. `decision_id` is the projection PK; replay
            // (a distinct envelope carrying the same decision_id) is
            // silently absorbed by `ON CONFLICT(decision_id) DO
            // NOTHING`. `decision_key` and the full `DecisionEvent`
            // payload are stored as TEXT (JSON) for pg/sqlite type
            // portability — no JSONB operators assumed.
            RuntimeEvent::DecisionRecorded(e) => {
                let decision_key_json = serde_json::to_string(&e.decision_key)
                    .map_err(|err| StoreError::Serialization(err.to_string()))?;
                let outcome_kind = decision_outcome_kind(&e.outcome);
                let expires_at = i64::try_from(e.expires_at).map_err(|_| {
                    StoreError::Internal(format!(
                        "DecisionRecorded.expires_at {} exceeds i64::MAX",
                        e.expires_at
                    ))
                })?;
                let decided_at = i64::try_from(e.decided_at).map_err(|_| {
                    StoreError::Internal(format!(
                        "DecisionRecorded.decided_at {} exceeds i64::MAX",
                        e.decided_at
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO decision_records
                         (decision_id, tenant_id, workspace_id, project_id,
                          decision_key_json, outcome_kind, cached,
                          expires_at, decided_at, event_json, recorded_at_ms)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(decision_id) DO NOTHING",
                )
                .bind(e.decision_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(decision_key_json)
                .bind(outcome_kind)
                .bind(e.cached)
                .bind(expires_at)
                .bind(decided_at)
                .bind(&e.event_json)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // F47 PR2: mirror PG projection. Completion_verification is
            // stored as TEXT serde-JSON (SQLite has no native JSONB;
            // portable per the no-DB-specific-features memory). UPDATE-
            // only; missing-row no-ops silently mirroring the
            // RunStateChanged handler.
            RuntimeEvent::RunCompletionAnnotated(e) => {
                let verification_json = serde_json::to_string(&e.verification)
                    .map_err(|err| StoreError::Serialization(err.to_string()))?;
                let annotated_at = i64::try_from(e.occurred_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "RunCompletionAnnotated.occurred_at_ms {} exceeds i64::MAX",
                        e.occurred_at_ms
                    ))
                })?;
                sqlx::query(
                    "UPDATE runs
                        SET completion_summary              = ?,
                            completion_verification_json    = ?,
                            completion_annotated_at_ms      = ?,
                            version                         = version + 1,
                            updated_at                      = ?
                      WHERE run_id = ?",
                )
                .bind(&e.summary)
                .bind(verification_json)
                .bind(annotated_at)
                .bind(now)
                .bind(e.run_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // F64: mirror PG projection for TerminalRecoveryAttempted.
            RuntimeEvent::TerminalRecoveryAttempted(e) => {
                let record = crate::projections::TerminalRecoveryRecord {
                    fcall: e.fcall.clone(),
                    attempts: e.attempts,
                    wall_time_ms: e.wall_time_ms,
                    outcome: e.outcome.clone(),
                    occurred_at_ms: e.occurred_at_ms,
                };
                let json = serde_json::to_string(&record)
                    .map_err(|err| StoreError::Serialization(err.to_string()))?;
                sqlx::query(
                    "UPDATE runs
                        SET terminal_write_recovery_json = ?,
                            version                     = version + 1,
                            updated_at                   = ?
                      WHERE run_id = ?",
                )
                .bind(json)
                .bind(now)
                .bind(e.run_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::DecisionCacheWarmup(e) => {
                let warmed_at = i64::try_from(e.warmed_at).map_err(|_| {
                    StoreError::Internal(format!(
                        "DecisionCacheWarmup.warmed_at {} exceeds i64::MAX",
                        e.warmed_at
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO decision_cache_warmups
                         (warmed_at, cached, expired_and_dropped)
                     VALUES (?, ?, ?)
                     ON CONFLICT(warmed_at) DO NOTHING",
                )
                .bind(warmed_at)
                .bind(i64::from(e.cached))
                .bind(i64::from(e.expired_and_dropped))
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // F39: RFC 020 Track 4 boot-level recovery audit summary.
            // One row per boot_id; ON CONFLICT DO NOTHING preserves the
            // first summary if replay re-projects the same event.
            RuntimeEvent::RecoverySummaryEmitted(e) => {
                let recorded_at = i64::try_from(e.summary_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "RecoverySummaryEmitted.summary_at_ms {} exceeds i64::MAX",
                        e.summary_at_ms
                    ))
                })?;
                let startup_ms = i64::try_from(e.startup_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "RecoverySummaryEmitted.startup_ms {} exceeds i64::MAX",
                        e.startup_ms
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO recovery_summaries
                         (boot_id, tenant_id, workspace_id, project_id,
                          recovered_runs, recovered_tasks, recovered_sandboxes,
                          preserved_sandboxes, orphaned_sandboxes_cleaned,
                          decision_cache_entries, stale_pending_cleared,
                          tool_result_cache_entries, memory_projection_entries,
                          graph_nodes_recovered, graph_edges_recovered,
                          webhook_dedup_entries, trigger_projections,
                          startup_ms, summary_at_ms)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                             ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(boot_id) DO NOTHING",
                )
                .bind(&e.boot_id)
                .bind(e.sentinel_project.tenant_id.as_str())
                .bind(e.sentinel_project.workspace_id.as_str())
                .bind(e.sentinel_project.project_id.as_str())
                // u32 count fields use infallible `i64::from`; see the
                // pg handler for the symmetry rationale.
                .bind(i64::from(e.recovered_runs))
                .bind(i64::from(e.recovered_tasks))
                .bind(i64::from(e.recovered_sandboxes))
                .bind(i64::from(e.preserved_sandboxes))
                .bind(i64::from(e.orphaned_sandboxes_cleaned))
                .bind(i64::from(e.decision_cache_entries))
                .bind(i64::from(e.stale_pending_cleared))
                .bind(i64::from(e.tool_result_cache_entries))
                .bind(i64::from(e.memory_projection_entries))
                .bind(i64::from(e.graph_nodes_recovered))
                .bind(i64::from(e.graph_edges_recovered))
                .bind(i64::from(e.webhook_dedup_entries))
                .bind(i64::from(e.trigger_projections))
                .bind(startup_ms)
                .bind(recorded_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }

            // PR BP-1: tool-call approval foundation events — no
            // projection table yet; a later PR in the wave wires these
            // into the approvals / tool-call projections.
            // PR BP-2: project tool-call approval events into the
            // `tool_call_approvals` table. JSON fields are stored as
            // TEXT because SQLite has no native JSONB.
            RuntimeEvent::ToolCallProposed(e) => {
                let tool_args_text = serde_json::to_string(&e.tool_args)
                    .map_err(|err| StoreError::Serialization(err.to_string()))?;
                let match_policy_text = serde_json::to_string(&e.match_policy)
                    .map_err(|err| StoreError::Serialization(err.to_string()))?;
                let display_summary_opt: Option<&str> = if e.display_summary.is_empty() {
                    None
                } else {
                    Some(e.display_summary.as_str())
                };
                sqlx::query(
                    "INSERT INTO tool_call_approvals (
                         call_id, session_id, run_id, tenant_id, workspace_id, project_id,
                         tool_name, original_tool_args, amended_tool_args, approved_tool_args,
                         display_summary, match_policy, state, operator_id, scope, reason,
                         proposed_at_ms, approved_at_ms, rejected_at_ms, last_amended_at_ms,
                         version, created_at, updated_at
                     )
                     VALUES (
                         ?, ?, ?, ?, ?, ?,
                         ?, ?, NULL, NULL,
                         ?, ?, 'pending', NULL, NULL, NULL,
                         ?, NULL, NULL, NULL,
                         1, ?, ?
                     )
                     ON CONFLICT(call_id) DO NOTHING",
                )
                .bind(e.call_id.as_str())
                .bind(e.session_id.as_str())
                .bind(e.run_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(&e.tool_name)
                .bind(tool_args_text)
                .bind(display_summary_opt)
                .bind(match_policy_text)
                .bind(e.proposed_at_ms as i64)
                .bind(now)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::ToolCallAmended(e) => {
                let new_args_text = serde_json::to_string(&e.new_tool_args)
                    .map_err(|err| StoreError::Serialization(err.to_string()))?;
                sqlx::query(
                    "UPDATE tool_call_approvals
                     SET amended_tool_args = ?,
                         last_amended_at_ms = ?,
                         version = version + 1,
                         updated_at = ?
                     WHERE call_id = ?",
                )
                .bind(new_args_text)
                .bind(e.amended_at_ms as i64)
                .bind(now)
                .bind(e.call_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::ToolCallApproved(e) => {
                let scope_text = serde_json::to_string(&e.scope)
                    .map_err(|err| StoreError::Serialization(err.to_string()))?;
                let approved_args_text: Option<String> = e
                    .approved_tool_args
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()
                    .map_err(|err| StoreError::Serialization(err.to_string()))?;
                sqlx::query(
                    "UPDATE tool_call_approvals
                     SET state = 'approved',
                         operator_id = ?,
                         scope = ?,
                         approved_tool_args = ?,
                         approved_at_ms = ?,
                         version = version + 1,
                         updated_at = ?
                     WHERE call_id = ?",
                )
                .bind(e.operator_id.as_str())
                .bind(scope_text)
                .bind(approved_args_text)
                .bind(e.approved_at_ms as i64)
                .bind(now)
                .bind(e.call_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::ToolCallRejected(e) => {
                sqlx::query(
                    "UPDATE tool_call_approvals
                     SET state = 'rejected',
                         operator_id = ?,
                         reason = ?,
                         rejected_at_ms = ?,
                         version = version + 1,
                         updated_at = ?
                     WHERE call_id = ?",
                )
                .bind(e.operator_id.as_str())
                .bind(e.reason.as_deref())
                .bind(e.rejected_at_ms as i64)
                .bind(now)
                .bind(e.call_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // ── F65 PR-2: orchestrator session redesign projections ────
            // SQLite mirrors the pg writers in `pg/projections.rs`. Column
            // placeholders use SQLite's `?` syntax. Monotonic
            // attempt-count advancement is enforced inline with SQLite's
            // scalar `MAX(a, b)` in the SET clause — `MAX(attempts_used, ?)`
            // keeps the existing counter when a replay arrives with an
            // older attempt number, so replay cannot shrink either
            // counter. Other replay/idempotency properties match the pg
            // path (ON CONFLICT ... DO UPDATE).
            RuntimeEvent::SessionAttemptStarted(e) => {
                // u32 always fits in i64 — infallible conversion.
                let attempt: i64 = e.attempt_number.into();
                let max_attempts: i64 = e.max_attempts.into();
                sqlx::query(
                    "UPDATE sessions
                        SET attempts_used = MAX(attempts_used, ?),
                            max_attempts  = MAX(max_attempts, ?),
                            version       = version + 1,
                            updated_at    = ?
                      WHERE session_id = ?",
                )
                .bind(attempt)
                .bind(max_attempts)
                .bind(now)
                .bind(e.session_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // No dedicated projection (operator observability only). See
            // the matching pg arm for the rationale.
            RuntimeEvent::SessionAttemptCompleted(_) => {}
            RuntimeEvent::CircuitBreakerTripped(_) => {}
            RuntimeEvent::BudgetThresholdCrossed(_) => {}
            RuntimeEvent::CheckpointPersisted(e) => {
                let schema_version: i64 = 1;
                // u32 → i64 is infallible.
                let iteration: i64 = e.iteration.into();
                let at_ms = i64::try_from(e.at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "CheckpointPersisted.at_ms {} exceeds i64::MAX",
                        e.at_ms
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO checkpoints (
                         checkpoint_id, tenant_id, workspace_id, project_id,
                         run_id, disposition, version, created_at,
                         session_id, schema_version, body, body_size_bytes, iteration
                     )
                     VALUES (?, ?, ?, ?, ?, 'latest', 1, ?, ?, ?, '', 0, ?)
                     ON CONFLICT (checkpoint_id) DO UPDATE SET
                         session_id = excluded.session_id,
                         schema_version = excluded.schema_version,
                         iteration = excluded.iteration",
                )
                .bind(e.checkpoint_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.root_run_id.as_str())
                .bind(at_ms)
                .bind(e.session_id.as_str())
                .bind(schema_version)
                .bind(iteration)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::WorkspaceSnapshotCreated(e) => {
                let at_ms = i64::try_from(e.at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "WorkspaceSnapshotCreated.at_ms {} exceeds i64::MAX",
                        e.at_ms
                    ))
                })?;
                let bytes_i64 = i64::try_from(e.bytes).map_err(|_| {
                    StoreError::Internal(format!(
                        "WorkspaceSnapshotCreated.bytes {} exceeds i64::MAX",
                        e.bytes
                    ))
                })?;
                // #482: carry bytes / reflink_used / parent_snapshot_id
                // from the event so replay rebuilds the full row.
                sqlx::query(
                    "INSERT INTO workspace_snapshots (
                         snapshot_id, tenant_id, workspace_scope, project_id,
                         session_id, workspace_id, parent_snapshot_id,
                         snapshot_path, bytes, reflink_used, created_at
                     )
                     VALUES (?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?)
                     ON CONFLICT (snapshot_id) DO NOTHING",
                )
                .bind(e.snapshot_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.session_id.as_str())
                .bind(e.workspace_id.as_str())
                .bind(e.parent_snapshot_id.as_ref().map(|p| p.as_str()))
                .bind(bytes_i64)
                .bind(if e.reflink_used { 1_i64 } else { 0_i64 })
                .bind(at_ms)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::WorkspaceSnapshotReaped(e) => {
                let at_ms = i64::try_from(e.at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "WorkspaceSnapshotReaped.at_ms {} exceeds i64::MAX",
                        e.at_ms
                    ))
                })?;
                sqlx::query(
                    "UPDATE workspace_snapshots
                        SET reaped_at = ?
                      WHERE snapshot_id = ?",
                )
                .bind(at_ms)
                .bind(e.snapshot_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::SessionOutcomeEmitted(e) => {
                let outcome = &e.outcome;
                let termination_kind =
                    crate::projections::termination_reason_kind(&outcome.termination_reason);
                let termination_reason_json = serde_json::to_string(&outcome.termination_reason)
                    .map_err(|err| StoreError::Serialization(err.to_string()))?;
                let cost_micros = i64::try_from(outcome.cost_micros).map_err(|_| {
                    StoreError::Internal(format!(
                        "SessionOutcome.cost_micros {} exceeds i64::MAX",
                        outcome.cost_micros
                    ))
                })?;
                let created_at = i64::try_from(outcome.emitted_at).map_err(|_| {
                    StoreError::Internal(format!(
                        "SessionOutcome.emitted_at {} exceeds i64::MAX",
                        outcome.emitted_at
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO session_outcomes (
                         root_run_id, tenant_id, workspace_scope, project_id,
                         session_id, checkpoint_id, workspace_snapshot_id,
                         termination_reason, termination_reason_json,
                         compacted_summary, next_step_hint,
                         cost_micros, created_at
                     )
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT (root_run_id) DO UPDATE SET
                         workspace_snapshot_id   = excluded.workspace_snapshot_id,
                         termination_reason      = excluded.termination_reason,
                         termination_reason_json = excluded.termination_reason_json,
                         compacted_summary       = excluded.compacted_summary,
                         next_step_hint          = excluded.next_step_hint,
                         cost_micros             = excluded.cost_micros",
                )
                .bind(outcome.root_run_id.as_str())
                .bind(outcome.project.tenant_id.as_str())
                .bind(outcome.project.workspace_id.as_str())
                .bind(outcome.project.project_id.as_str())
                .bind(outcome.session_id.as_str())
                .bind(outcome.checkpoint_id.as_str())
                .bind(outcome.workspace_snapshot_id.as_ref().map(|id| id.as_str()))
                .bind(termination_kind)
                .bind(&termination_reason_json)
                .bind(&outcome.compacted_summary)
                .bind(outcome.next_step_hint.as_deref())
                .bind(cost_micros)
                .bind(created_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::OrchestratorDecisionMade(_) => {}
            RuntimeEvent::SummarizerFallback(_) => {}
            RuntimeEvent::WorkspaceBackendDegraded(_) => {}
            // F65 PR-5 (#359): crash-recovery umount sweep is an operator
            // observability surface (SSE + metrics) with no projection
            // table — the event log itself is the audit trail.
            RuntimeEvent::SandboxCrashRecovered(_) => {}
        }

        Ok(())
    }
}

/// F29 CD-2: SQLite-side mirror of `upsert_cost_rollups_pg`. See the pg
/// helper's docstring for the monotonic-`updated_at_ms` contract, the
/// overflow-to-error rule, and the consistency invariant the three
/// upserts provide.
#[allow(clippy::too_many_arguments)]
async fn upsert_cost_rollups_sqlite(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    session_id: &str,
    tenant_id: &str,
    project: &cairn_domain::ProjectKey,
    delta_cost_micros: u64,
    delta_tokens_in: u64,
    delta_tokens_out: u64,
    updated_at_ms: u64,
) -> Result<(), StoreError> {
    fn to_i64(field: &str, v: u64) -> Result<i64, StoreError> {
        i64::try_from(v).map_err(|_| {
            StoreError::Internal(format!("session cost {field} value {v} exceeds i64::MAX"))
        })
    }
    let delta_cost = to_i64("delta_cost_micros", delta_cost_micros)?;
    let delta_in = to_i64("delta_tokens_in", delta_tokens_in)?;
    let delta_out = to_i64("delta_tokens_out", delta_tokens_out)?;
    let updated_at = to_i64("updated_at_ms", updated_at_ms)?;

    sqlx::query(
        "INSERT INTO session_costs
             (session_id, tenant_id, workspace_id, project_id,
              total_cost_micros, total_tokens_in, total_tokens_out,
              provider_calls, updated_at_ms)
         VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
         ON CONFLICT(session_id) DO UPDATE SET
             total_cost_micros = total_cost_micros + excluded.total_cost_micros,
             total_tokens_in   = total_tokens_in   + excluded.total_tokens_in,
             total_tokens_out  = total_tokens_out  + excluded.total_tokens_out,
             provider_calls    = provider_calls    + 1,
             updated_at_ms     = MAX(updated_at_ms, excluded.updated_at_ms)",
    )
    .bind(session_id)
    .bind(tenant_id)
    .bind(project.workspace_id.as_str())
    .bind(project.project_id.as_str())
    .bind(delta_cost)
    .bind(delta_in)
    .bind(delta_out)
    .bind(updated_at)
    .execute(&mut **tx)
    .await
    .map_err(|err| StoreError::Internal(err.to_string()))?;

    sqlx::query(
        "INSERT INTO project_costs
             (tenant_id, workspace_id, project_id,
              total_cost_micros, total_tokens_in, total_tokens_out,
              provider_calls, updated_at_ms)
         VALUES (?, ?, ?, ?, ?, ?, 1, ?)
         ON CONFLICT(tenant_id, workspace_id, project_id) DO UPDATE SET
             total_cost_micros = total_cost_micros + excluded.total_cost_micros,
             total_tokens_in   = total_tokens_in   + excluded.total_tokens_in,
             total_tokens_out  = total_tokens_out  + excluded.total_tokens_out,
             provider_calls    = provider_calls    + 1,
             updated_at_ms     = MAX(updated_at_ms, excluded.updated_at_ms)",
    )
    .bind(tenant_id)
    .bind(project.workspace_id.as_str())
    .bind(project.project_id.as_str())
    .bind(delta_cost)
    .bind(delta_in)
    .bind(delta_out)
    .bind(updated_at)
    .execute(&mut **tx)
    .await
    .map_err(|err| StoreError::Internal(err.to_string()))?;

    sqlx::query(
        "INSERT INTO workspace_costs
             (tenant_id, workspace_id,
              total_cost_micros, total_tokens_in, total_tokens_out,
              provider_calls, updated_at_ms)
         VALUES (?, ?, ?, ?, ?, 1, ?)
         ON CONFLICT(tenant_id, workspace_id) DO UPDATE SET
             total_cost_micros = total_cost_micros + excluded.total_cost_micros,
             total_tokens_in   = total_tokens_in   + excluded.total_tokens_in,
             total_tokens_out  = total_tokens_out  + excluded.total_tokens_out,
             provider_calls    = provider_calls    + 1,
             updated_at_ms     = MAX(updated_at_ms, excluded.updated_at_ms)",
    )
    .bind(tenant_id)
    .bind(project.workspace_id.as_str())
    .bind(delta_cost)
    .bind(delta_in)
    .bind(delta_out)
    .bind(updated_at)
    .execute(&mut **tx)
    .await
    .map_err(|err| StoreError::Internal(err.to_string()))?;

    Ok(())
}

/// F39: short-form outcome discriminant for `decision_records.outcome_kind`.
/// Stable strings (`"allowed"` / `"denied"`) let operator SQL filter
/// by outcome without parsing `decision_key_json`.
fn decision_outcome_kind(outcome: &cairn_domain::decisions::DecisionOutcome) -> &'static str {
    match outcome {
        cairn_domain::decisions::DecisionOutcome::Allowed => "allowed",
        cairn_domain::decisions::DecisionOutcome::Denied { .. } => "denied",
    }
}

fn enum_to_str<T: serde::Serialize>(val: &T) -> Result<String, StoreError> {
    let v = serde_json::to_value(val).map_err(|e| StoreError::Serialization(e.to_string()))?;
    match v {
        serde_json::Value::String(s) => Ok(s),
        _ => Ok(v.to_string().trim_matches('"').to_owned()),
    }
}

/// Narrow a domain `u32` onto the projection's `INTEGER`/`i32` column
/// without the silent `as i32` wrap (sqlite parity with the pg
/// `i32_from_u32` helper). Quota / budget projections call this.
fn i32_from_u32_sqlite(field: &'static str, value: u32) -> Result<i32, StoreError> {
    i32::try_from(value).map_err(|_| {
        StoreError::Internal(format!(
            "{field} = {value} exceeds i32::MAX; projection column is INTEGER"
        ))
    })
}

/// Stable TEXT encoding of `ProviderBudgetPeriod` for the
/// `provider_budgets.period` column (sqlite parity with pg).
fn provider_budget_period_str_sqlite(
    period: &cairn_domain::providers::ProviderBudgetPeriod,
) -> &'static str {
    match period {
        cairn_domain::providers::ProviderBudgetPeriod::Daily => "daily",
        cairn_domain::providers::ProviderBudgetPeriod::Monthly => "monthly",
    }
}

/// Stable TEXT encoding of `ProductTier` for the `licenses.tier` column
/// (sqlite parity with pg).
fn product_tier_str_sqlite(tier: &cairn_domain::commercial::ProductTier) -> &'static str {
    match tier {
        cairn_domain::commercial::ProductTier::LocalEval => "local_eval",
        cairn_domain::commercial::ProductTier::TeamSelfHosted => "team_self_hosted",
        cairn_domain::commercial::ProductTier::EnterpriseSelfHosted => "enterprise_self_hosted",
    }
}
