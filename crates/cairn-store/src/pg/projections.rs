use std::time::{SystemTime, UNIX_EPOCH};

use cairn_domain::{tool_invocation::ToolInvocationOutcomeKind, EventEnvelope, RuntimeEvent};

use crate::error::StoreError;

/// Postgres-backed synchronous projection applier.
///
/// Dispatches stored events to current-state table upserts.
/// All methods are async and operate within an existing transaction.
pub struct PgSyncProjection;

impl PgSyncProjection {
    /// Async projection application within a transaction.
    ///
    /// This is the real implementation used by `PgEventLog::append` when it
    /// appends events within a transaction. Takes the envelope by reference
    /// (not a full `StoredEvent`) so the hot append path does not need to
    /// clone the potentially-large payload (CheckpointCreated snapshots can
    /// be hundreds of KB per event) — see #497.
    pub async fn apply_async(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        envelope: &EventEnvelope<RuntimeEvent>,
    ) -> Result<(), StoreError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        match &envelope.payload {
            RuntimeEvent::SessionCreated(e) => {
                sqlx::query(
                    "INSERT INTO sessions (session_id, tenant_id, workspace_id, project_id, state, version, created_at, updated_at)
                     VALUES ($1, $2, $3, $4, 'open', 1, $5, $5)",
                )
                .bind(e.session_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            }

            RuntimeEvent::SessionStateChanged(e) => {
                let state_str = enum_to_str(&e.transition.to)?;
                sqlx::query(
                    "UPDATE sessions SET state = $1, version = version + 1, updated_at = $2 WHERE session_id = $3",
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
                     VALUES ($1, $2, $3, $4, $5, $6, 'pending', 1, $7, $7)",
                )
                .bind(e.run_id.as_str())
                .bind(e.session_id.as_str())
                .bind(e.parent_run_id.as_ref().map(|id| id.as_str()))
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            }

            RuntimeEvent::RunStateChanged(e) => {
                let state_str = enum_to_str(&e.transition.to)?;
                let failure = e.failure_class.as_ref().map(enum_to_str).transpose()?;
                sqlx::query(
                    "UPDATE runs SET state = $1, failure_class = $2, version = version + 1, updated_at = $3 WHERE run_id = $4",
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
                let session_id_on_event = e.session_id.as_ref().map(|s| s.as_str());
                sqlx::query(
                    "INSERT INTO tasks (task_id, tenant_id, workspace_id, project_id, parent_run_id, parent_task_id, session_id, state, title, description, version, created_at, updated_at)
                     VALUES ($1, $2, $3, $4, $5, $6,
                        COALESCE($7, (SELECT session_id FROM runs WHERE run_id = $5)),
                        'queued', NULL, NULL, 1, $8, $8)",
                )
                .bind(e.task_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.parent_run_id.as_ref().map(|id| id.as_str()))
                .bind(e.parent_task_id.as_ref().map(|id| id.as_str()))
                .bind(session_id_on_event)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            }

            RuntimeEvent::TaskLeaseClaimed(e) => {
                sqlx::query(
                    "UPDATE tasks SET state = 'leased', lease_owner = $1, lease_expires_at = $2, lease_version = $3, version = version + 1, updated_at = $4 WHERE task_id = $5",
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
                    "UPDATE tasks SET lease_expires_at = $1, lease_version = $2, version = version + 1, updated_at = $3 WHERE task_id = $4",
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
                    "UPDATE tasks SET state = $1, failure_class = $2, version = version + 1, updated_at = $3 WHERE task_id = $4",
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
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 1, $10, $10)",
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
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            }

            RuntimeEvent::ApprovalResolved(e) => {
                let decision_str = enum_to_str(&e.decision)?;
                sqlx::query(
                    "UPDATE approvals SET decision = $1, version = version + 1, updated_at = $2 WHERE approval_id = $3",
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
                        "UPDATE checkpoints SET disposition = 'superseded', version = version + 1 WHERE run_id = $1 AND disposition = 'latest'",
                    )
                    .bind(e.run_id.as_str())
                    .execute(&mut **tx)
                    .await
                    .map_err(|e| StoreError::Internal(e.to_string()))?;
                }

                sqlx::query(
                    "INSERT INTO checkpoints (checkpoint_id, tenant_id, workspace_id, project_id, run_id, disposition, version, created_at)
                     VALUES ($1, $2, $3, $4, $5, $6, 1, $7)",
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

            RuntimeEvent::CheckpointRestored(_e) => {
                // Restore events are linkage records, not state mutations.
                // The checkpoint table does not change disposition on restore.
                // Logged in the event log for replay/audit purposes.
            }

            RuntimeEvent::MailboxMessageAppended(e) => {
                sqlx::query(
                    "INSERT INTO mailbox_messages (message_id, tenant_id, workspace_id, project_id, run_id, task_id, version, created_at)
                     VALUES ($1, $2, $3, $4, $5, $6, 1, $7)",
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
                let target = serde_json::to_value(&e.target)
                    .map_err(|e| StoreError::Serialization(e.to_string()))?;
                let exec_class_str = enum_to_str(&e.execution_class)?;

                // F55: persist captured args so GET /v1/tool-invocations
                // can surface "what cairn ran" without replaying the
                // event log.
                sqlx::query(
                    "INSERT INTO tool_invocations (invocation_id, tenant_id, workspace_id, project_id, session_id, run_id, task_id, target, execution_class, state, requested_at_ms, started_at_ms, args_json, version, created_at, updated_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'started', $10, $11, $12, 1, $13, $13)",
                )
                .bind(e.invocation_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.session_id.as_ref().map(|id| id.as_str()))
                .bind(e.run_id.as_ref().map(|id| id.as_str()))
                .bind(e.task_id.as_ref().map(|id| id.as_str()))
                .bind(target)
                .bind(exec_class_str)
                .bind(e.requested_at_ms as i64)
                .bind(e.started_at_ms as i64)
                .bind(e.args_json.clone())
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            }

            RuntimeEvent::ToolInvocationCompleted(e) => {
                let outcome_str = enum_to_str(&e.outcome)?;
                // F55: persist the truncated output preview so the read
                // path returns "what cairn got back" alongside the
                // lifecycle metadata.
                sqlx::query(
                    "UPDATE tool_invocations SET state = 'completed', outcome = $1, finished_at_ms = $2, output_preview = COALESCE($3, output_preview), version = version + 1, updated_at = $4 WHERE invocation_id = $5",
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
                let state_str = tool_invocation_terminal_state_str(e.outcome)?;
                let outcome_str = enum_to_str(&e.outcome)?;
                sqlx::query(
                    "UPDATE tool_invocations SET state = $1, outcome = $2, error_message = $3, finished_at_ms = $4, output_preview = COALESCE($5, output_preview), version = version + 1, updated_at = $6 WHERE invocation_id = $7",
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
            // projection table on the Postgres backend. Audit reference:
            // `.claude/audit-state/review-queue.md` §T2-H3. If you land on
            // this warning in production, extend this applier to cover the
            // specific variant and its projection table(s).
            RuntimeEvent::ExternalWorkerRegistered(_) => log_stub("ExternalWorkerRegistered"),
            RuntimeEvent::ExternalWorkerReported(_) => log_stub("ExternalWorkerReported"),
            RuntimeEvent::ExternalWorkerSuspended(_) => log_stub("ExternalWorkerSuspended"),
            RuntimeEvent::ExternalWorkerReactivated(_) => log_stub("ExternalWorkerReactivated"),
            RuntimeEvent::SoulPatchProposed(_) => log_stub("SoulPatchProposed"),
            RuntimeEvent::SoulPatchApplied(_) => log_stub("SoulPatchApplied"),
            // F29 CD-2: upsert the per-session record + fold into the
            // project/workspace rollups in the same transaction. All three
            // upserts succeed together or the whole event append rolls
            // back, so read-model consistency is preserved.
            RuntimeEvent::SessionCostUpdated(e) => {
                // InMemory sources tenant from the explicit `e.tenant_id`
                // field, not `e.project.tenant_id`, because fixtures can
                // carry a sentinel project triple (see the budget-
                // blocking test in cairn-runtime). Mirror that choice on
                // the durable path so pg and InMemory agree on which
                // tenant a cost is attributed to.
                upsert_cost_rollups_pg(
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
            // Keys on envelope event_id so replay is idempotent. Nullable
            // run_id/task_id/boot_id mirror the event struct exactly; the
            // `has_target()` invariant is enforced by the emitter, not the
            // projection (an already-appended malformed event still
            // projects, matching InMemory's pass-through behavior).
            RuntimeEvent::RecoveryAttempted(e) => {
                sqlx::query(
                    "INSERT INTO recovery_attempts
                         (event_id, tenant_id, workspace_id, project_id,
                          run_id, task_id, reason, boot_id, recorded_at_ms)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                     ON CONFLICT (event_id) DO NOTHING",
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
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                     ON CONFLICT (event_id) DO NOTHING",
                )
                .bind(envelope.event_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.run_id.as_ref().map(|r| r.as_str()))
                .bind(e.task_id.as_ref().map(|t| t.as_str()))
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
            // RFC-025 Phase 1 (milestone 3): `eval_runs` projection.
            // Upsert on Started so re-emitting the lifecycle edge
            // doesn't clobber prior score/completion state (the
            // in-memory applier uses the same or_insert_with pattern).
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
                     VALUES ($1, $2, $3, $4, $5, $6,
                             NULL, NULL, $7, NULL, NULL, NULL, NULL,
                             $8, $9, $10, $11, $12, $13, $14)
                     ON CONFLICT (eval_run_id) DO NOTHING",
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
                     SET success = $1,
                         error_message = $2,
                         completed_at = $3
                     WHERE eval_run_id = $4",
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
                // Earliest-wins on archived_at (mirrors in-memory
                // `EvalRunService::archive` idempotency: two concurrent
                // DELETEs must not bump the timestamp to the later
                // attempt — Copilot review on PR #336).
                sqlx::query(
                    "UPDATE eval_runs
                     SET archived_at = $1
                     WHERE eval_run_id = $2 AND archived_at IS NULL",
                )
                .bind(e.archived_at as i64)
                .bind(e.eval_run_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::EvalRunScored(e) => {
                // Serialize EvalMetrics as JSON; stored in a TEXT column
                // for cross-backend parity with sqlite (no JSONB).
                let metrics_json = serde_json::to_string(&e.metrics)
                    .map_err(|err| StoreError::Internal(err.to_string()))?;
                sqlx::query(
                    "UPDATE eval_runs SET metrics_json = $1 WHERE eval_run_id = $2",
                )
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
                sqlx::query(
                    "UPDATE eval_runs SET rubric_score_json = $1 WHERE eval_run_id = $2",
                )
                .bind(rubric_json)
                .bind(e.eval_run_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // RFC-025 Phase 2b.1 m3: outcomes projection.
            //
            // ON CONFLICT DO NOTHING — `outcome_id` is globally unique
            // (the eval_score tool mints it via a monotonic sequence),
            // so a replay is a true duplicate. `actual_outcome` is a
            // snake_case enum token via `enum_to_str`.
            RuntimeEvent::OutcomeRecorded(e) => {
                let recorded_at = i64::try_from(e.recorded_at).map_err(|_| {
                    StoreError::Internal(format!(
                        "OutcomeRecorded.recorded_at {} exceeds i64::MAX",
                        e.recorded_at
                    ))
                })?;
                let actual = enum_to_str(&e.actual_outcome)?;
                sqlx::query(
                    "INSERT INTO outcomes (
                        outcome_id, run_id, tenant_id, workspace_id, project_id,
                        agent_type, predicted_confidence, actual_outcome, recorded_at
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                     ON CONFLICT (outcome_id) DO NOTHING",
                )
                .bind(e.outcome_id.as_str())
                .bind(e.run_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(&e.agent_type)
                .bind(e.predicted_confidence)
                .bind(&actual)
                .bind(recorded_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // RFC-025 Phase 2b.1 m2: scheduled_tasks projection.
            //
            // The event is fire-once (no Cancelled / LastRunUpdated
            // companion events yet — those are tracked as Phase 2b.2
            // follow-ups). ON CONFLICT DO NOTHING keeps replay safe;
            // `enabled` / `updated_at` / `last_run_at` are defaulted at
            // first insert to match the in-memory projection
            // (`enabled=true`, `last_run_at=NULL`, `updated_at=created_at`).
            RuntimeEvent::ScheduledTaskCreated(e) => {
                let created_at = i64::try_from(e.created_at).map_err(|_| {
                    StoreError::Internal(format!(
                        "ScheduledTaskCreated.created_at {} exceeds i64::MAX",
                        e.created_at
                    ))
                })?;
                let next_run_at = e
                    .next_run_at
                    .map(i64::try_from)
                    .transpose()
                    .map_err(|_| {
                        StoreError::Internal(
                            "ScheduledTaskCreated.next_run_at exceeds i64::MAX".into(),
                        )
                    })?;
                sqlx::query(
                    "INSERT INTO scheduled_tasks (
                        scheduled_task_id, tenant_id, name, cron_expression,
                        last_run_at, next_run_at, enabled, created_at, updated_at
                     ) VALUES ($1, $2, $3, $4, NULL, $5, TRUE, $6, $6)
                     ON CONFLICT (scheduled_task_id) DO NOTHING",
                )
                .bind(e.scheduled_task_id.as_str())
                .bind(e.tenant_id.as_str())
                .bind(&e.name)
                .bind(&e.cron_expression)
                .bind(next_run_at)
                .bind(created_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // RFC-025 Phase 2b.1 m4: plan_reviews projection (RFC 018).
            //
            // PlanProposed creates the row. PlanApproved / PlanRejected /
            // PlanRevisionRequested mutate the in-place state. Replay of
            // the creation event after a resolution must NOT overwrite
            // the resolver fields — the ON CONFLICT DO NOTHING clause on
            // the creation path enforces that.
            RuntimeEvent::PlanProposed(e) => {
                let proposed_at = i64::try_from(e.proposed_at).map_err(|_| {
                    StoreError::Internal(format!(
                        "PlanProposed.proposed_at {} exceeds i64::MAX",
                        e.proposed_at
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO plan_reviews (
                        plan_run_id, tenant_id, workspace_id, project_id, session_id,
                        plan_markdown, state, proposed_at
                     ) VALUES ($1, $2, $3, $4, $5, $6, 'proposed', $7)
                     ON CONFLICT (plan_run_id) DO NOTHING",
                )
                .bind(e.plan_run_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.session_id.as_str())
                .bind(&e.plan_markdown)
                .bind(proposed_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::PlanApproved(e) => {
                let approved_at = i64::try_from(e.approved_at).map_err(|_| {
                    StoreError::Internal(format!(
                        "PlanApproved.approved_at {} exceeds i64::MAX",
                        e.approved_at
                    ))
                })?;
                // UPDATE is idempotent — applying the same resolution
                // twice is a no-op at the SQL layer. `WHERE state =
                // 'proposed'` guards against a late PlanApproved racing
                // a prior PlanRejected / RevisionRequested; the first
                // resolution wins (matches RFC 018 §"Terminal resolution").
                sqlx::query(
                    "UPDATE plan_reviews
                     SET state             = 'approved',
                         resolved_by       = $1,
                         resolved_at       = $2,
                         reviewer_comments = $3
                     WHERE plan_run_id = $4 AND state = 'proposed'",
                )
                .bind(e.approved_by.as_str())
                .bind(approved_at)
                .bind(e.reviewer_comments.as_deref())
                .bind(e.plan_run_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::PlanRejected(e) => {
                let rejected_at = i64::try_from(e.rejected_at).map_err(|_| {
                    StoreError::Internal(format!(
                        "PlanRejected.rejected_at {} exceeds i64::MAX",
                        e.rejected_at
                    ))
                })?;
                sqlx::query(
                    "UPDATE plan_reviews
                     SET state            = 'rejected',
                         resolved_by      = $1,
                         resolved_at      = $2,
                         rejection_reason = $3
                     WHERE plan_run_id = $4 AND state = 'proposed'",
                )
                .bind(e.rejected_by.as_str())
                .bind(rejected_at)
                .bind(&e.reason)
                .bind(e.plan_run_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::PlanRevisionRequested(e) => {
                let requested_at = i64::try_from(e.requested_at).map_err(|_| {
                    StoreError::Internal(format!(
                        "PlanRevisionRequested.requested_at {} exceeds i64::MAX",
                        e.requested_at
                    ))
                })?;
                sqlx::query(
                    "UPDATE plan_reviews
                     SET state             = 'revision_requested',
                         resolved_at       = $1,
                         reviewer_comments = $2,
                         revision_run_id   = $3
                     WHERE plan_run_id = $4 AND state = 'proposed'",
                )
                .bind(requested_at)
                .bind(&e.reviewer_comments)
                .bind(e.new_plan_run_id.as_str())
                .bind(e.original_plan_run_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // RFC-025 Phase 2a.1 milestone 3: provider_budgets projection.
            //
            // Keyed by `budget_id` so subsequent Alert/Exceeded events can
            // UPDATE the matching row. `alert_threshold_percent` defaults
            // to 80% mirroring the in_memory applier. A replayed
            // `ProviderBudgetSet` preserves the observed spend + flags
            // (operators who reconfigure a budget do not want their
            // running spend erased).
            RuntimeEvent::ProviderBudgetSet(e) => {
                let period_str = provider_budget_period_str(&e.period);
                let limit_i64 = i64::try_from(e.limit_micros).map_err(|_| {
                    StoreError::Internal(format!(
                        "ProviderBudgetSet.limit_micros {} exceeds i64::MAX",
                        e.limit_micros
                    ))
                })?;
                // Centralised domain default lets all three projections
                // agree on the fallback when the event omits the
                // threshold — see
                // `cairn_domain::providers::DEFAULT_BUDGET_ALERT_THRESHOLD_PERCENT`.
                let threshold_u = e
                    .alert_threshold_percent
                    .unwrap_or(cairn_domain::providers::DEFAULT_BUDGET_ALERT_THRESHOLD_PERCENT);
                let threshold = i32_from_u32("ProviderBudgetSet.alert_threshold_percent", threshold_u)?;
                sqlx::query(
                    "INSERT INTO provider_budgets (
                        budget_id, tenant_id, period, limit_micros,
                        alert_threshold_percent, current_spend_micros,
                        alert_triggered_at_ms, exceeded_at_ms, created_at, updated_at
                     ) VALUES ($1, $2, $3, $4, $5, 0, NULL, NULL, $6, $6)
                     ON CONFLICT (budget_id) DO UPDATE SET
                        tenant_id               = EXCLUDED.tenant_id,
                        period                  = EXCLUDED.period,
                        limit_micros            = EXCLUDED.limit_micros,
                        alert_threshold_percent = EXCLUDED.alert_threshold_percent,
                        updated_at              = EXCLUDED.updated_at",
                )
                .bind(e.budget_id.as_str())
                .bind(e.tenant_id.as_str())
                .bind(period_str)
                .bind(limit_i64)
                .bind(threshold)
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
            // RFC-025 Phase 2a.1 milestone 4: licenses projection.
            //
            // At most one row per tenant (upsert). `entitlements_json`
            // ships as an empty array — the in_memory applier
            // initialises `entitlements: vec![]` for `LicenseActivated`,
            // so the projection carries the same shape for byte parity.
            // Future events that carry concrete entitlements will
            // populate the column.
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
                let tier_str = product_tier_str(&e.tier);
                sqlx::query(
                    "INSERT INTO licenses (
                        tenant_id, license_key, tier, entitlements_json,
                        issued_at, expires_at, created_at, updated_at
                     ) VALUES ($1, $2, $3, '[]', $4, $5, $6, $6)
                     ON CONFLICT (tenant_id) DO UPDATE SET
                        license_key       = EXCLUDED.license_key,
                        tier              = EXCLUDED.tier,
                        entitlements_json = EXCLUDED.entitlements_json,
                        issued_at         = EXCLUDED.issued_at,
                        expires_at        = EXCLUDED.expires_at,
                        updated_at        = EXCLUDED.updated_at",
                )
                .bind(e.tenant_id.as_str())
                .bind(e.license_id.as_str())
                .bind(tier_str)
                .bind(issued_at)
                .bind(expires_at)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::EntitlementOverrideSet(_) => log_stub("EntitlementOverrideSet"),
            RuntimeEvent::NotificationPreferenceSet(_) => log_stub("NotificationPreferenceSet"),
            RuntimeEvent::NotificationSent(_) => log_stub("NotificationSent"),
            RuntimeEvent::ProviderPoolCreated(_) => log_stub("ProviderPoolCreated"),
            RuntimeEvent::ProviderPoolConnectionAdded(_) => {
                log_stub("ProviderPoolConnectionAdded")
            }
            RuntimeEvent::ProviderPoolConnectionRemoved(_) => {
                log_stub("ProviderPoolConnectionRemoved")
            }
            // RFC-025 Phase 2a.1: tenant quotas projection.
            //
            // `TenantQuotaSet` upserts the operator-configured baseline.
            // The dynamic current_active_runs / sessions_this_hour
            // counters are computed on read by joining sessions/runs
            // (mirrors the in_memory QuotaReadModel::get_quota impl).
            RuntimeEvent::TenantQuotaSet(e) => {
                // Copilot PR #565: u32 → i32 via `as i32` silently wraps
                // past i32::MAX. Quota limits that big are already a
                // corrupted event, but wrapping a 2^31 limit to −1 then
                // rehydrating via `.max(0) as u32` would resurrect it as
                // 0 and silently disable the cap. Fail loudly instead.
                let max_concurrent_runs = i32_from_u32("TenantQuotaSet.max_concurrent_runs", e.max_concurrent_runs)?;
                let max_sessions_per_hour = i32_from_u32("TenantQuotaSet.max_sessions_per_hour", e.max_sessions_per_hour)?;
                let max_tasks_per_run = i32_from_u32("TenantQuotaSet.max_tasks_per_run", e.max_tasks_per_run)?;
                sqlx::query(
                    "INSERT INTO tenant_quotas (
                        tenant_id, max_concurrent_runs, max_sessions_per_hour,
                        max_tasks_per_run, created_at, updated_at
                     ) VALUES ($1, $2, $3, $4, $5, $5)
                     ON CONFLICT (tenant_id) DO UPDATE SET
                        max_concurrent_runs   = EXCLUDED.max_concurrent_runs,
                        max_sessions_per_hour = EXCLUDED.max_sessions_per_hour,
                        max_tasks_per_run     = EXCLUDED.max_tasks_per_run,
                        updated_at            = EXCLUDED.updated_at",
                )
                .bind(e.tenant_id.as_str())
                .bind(max_concurrent_runs)
                .bind(max_sessions_per_hour)
                .bind(max_tasks_per_run)
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
                let current = i32_from_u32("TenantQuotaViolated.current", e.current)?;
                let limit = i32_from_u32("TenantQuotaViolated.limit", e.limit)?;
                sqlx::query(
                    "INSERT INTO tenant_quota_violations (
                        tenant_id, quota_type, occurred_at_ms, current_value, limit_value
                     ) VALUES ($1, $2, $3, $4, $5)
                     ON CONFLICT (tenant_id, quota_type, occurred_at_ms) DO NOTHING",
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
            RuntimeEvent::ApprovalDelegated(_) => log_stub("ApprovalDelegated"),
            // RFC-025 Phase 2b.1: audit_log_entries projection.
            //
            // ON CONFLICT DO NOTHING keeps replay idempotent. The event
            // carries the full primary-key-identifying tuple (entry_id
            // is globally unique via the AuditServiceImpl sequence) so
            // a second delivery is a true duplicate — no mutable fields
            // to reconcile.
            //
            // `metadata_json` defaults to '{}'. The `AuditLogEntryRecorded`
            // event deliberately does not carry metadata (only Eq-able
            // fields — `serde_json::Value` is not `Eq`); the projection
            // persists the default so the read-model row shape matches
            // the in-memory `AuditLogEntry { metadata: {} }` reconstruction
            // byte-for-byte.
            RuntimeEvent::AuditLogEntryRecorded(e) => {
                let occurred_at = i64::try_from(e.occurred_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "AuditLogEntryRecorded.occurred_at_ms {} exceeds i64::MAX",
                        e.occurred_at_ms
                    ))
                })?;
                let outcome = enum_to_str(&e.outcome)?;
                sqlx::query(
                    "INSERT INTO audit_log_entries (
                        entry_id, tenant_id, actor_id, action, resource_type,
                        resource_id, outcome, occurred_at_ms
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                     ON CONFLICT (entry_id) DO NOTHING",
                )
                .bind(&e.entry_id)
                .bind(e.tenant_id.as_str())
                .bind(&e.actor_id)
                .bind(&e.action)
                .bind(&e.resource_type)
                .bind(&e.resource_id)
                .bind(&outcome)
                .bind(occurred_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::CheckpointStrategySet(_) => log_stub("CheckpointStrategySet"),
            // RFC-025 Phase 2a.1: credentials projection.
            //
            // `CredentialStored` is the create-or-refresh event. Replay
            // preserves the active/revoked state — an event re-applied
            // after a revocation must not silently flip `active` back
            // to true. The ON CONFLICT path keeps the existing
            // revoked_at_ms / active columns intact; live writes replace
            // encrypted material + key bindings + updated_at.
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
                     ) VALUES ($1, $2, $3, $4, 'api_key', $5, $6, $7, TRUE, $8, NULL, $8, $8)
                     ON CONFLICT (credential_id) DO UPDATE SET
                        encrypted_value  = EXCLUDED.encrypted_value,
                        key_id           = EXCLUDED.key_id,
                        key_version      = EXCLUDED.key_version,
                        encrypted_at_ms  = EXCLUDED.encrypted_at_ms,
                        updated_at       = EXCLUDED.updated_at",
                )
                .bind(e.credential_id.as_str())
                .bind(e.tenant_id.as_str())
                .bind(e.provider_id.as_str())
                .bind(e.provider_id.as_str())
                .bind(&e.encrypted_value)
                .bind(e.key_id.as_deref())
                .bind(e.key_version.as_deref())
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
                // Latest-wins on revoked_at_ms to match in_memory
                // semantics (`state.credentials.get_mut(..).revoked_at_ms
                // = Some(e.revoked_at_ms)` in in_memory.rs). Replaying
                // the same event is safe: UPDATE is idempotent against
                // itself, and `active = FALSE` is absorbing.
                sqlx::query(
                    "UPDATE credentials
                     SET active = FALSE,
                         revoked_at_ms = $1,
                         updated_at = $1
                     WHERE credential_id = $2",
                )
                .bind(revoked_at)
                .bind(e.credential_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::CredentialKeyRotated(e) => {
                // One row per rotation event. Rotation is append-only —
                // `ON CONFLICT (rotation_id) DO NOTHING` keeps replay
                // idempotent without mutating the audit trail.
                sqlx::query(
                    "INSERT INTO credential_rotations (
                        rotation_id, tenant_id, credential_id,
                        old_key_id, new_key_id, rotated_credentials,
                        started_at_ms, completed_at_ms, rotated_at, rotated_by
                     ) VALUES ($1, $2, '', $3, $4, $5, $6, $6, $6, NULL)
                     ON CONFLICT (rotation_id) DO NOTHING",
                )
                .bind(e.rotation_id.as_str())
                .bind(e.tenant_id.as_str())
                .bind(e.old_key_id.as_str())
                .bind(e.new_key_id.as_str())
                .bind(e.credential_ids_rotated.len() as i32)
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
            RuntimeEvent::PermissionDecisionRecorded(_) => {
                log_stub("PermissionDecisionRecorded")
            }
            // RFC-025 Phase 3: provider_bindings projection. Project-level
            // routing record linking (project, operation) → (connection,
            // model). `settings_json` carries the full
            // `ProviderBindingSettings` struct as a JSON TEXT column —
            // portable (no JSONB) and compatible with the settings-set
            // growing without a schema change.
            //
            // `ON CONFLICT (provider_binding_id) DO UPDATE` is idempotent
            // on replay: the creation event re-upserts the row *without*
            // overwriting the `active` column, so a later
            // `ProviderBindingStateChanged` stays intact when the log is
            // re-applied. This matches in_memory's semantics (the
            // `ProviderBindingCreated` arm there clobbers active by design
            // because it precedes the state-change arm in the log; our
            // projection preserves that ordering by omitting `active`
            // from the `DO UPDATE SET` list specifically).
            RuntimeEvent::ProviderBindingCreated(e) => {
                let created_at = i64::try_from(e.created_at).map_err(|_| {
                    StoreError::Internal(format!(
                        "ProviderBindingCreated.created_at {} exceeds i64::MAX",
                        e.created_at
                    ))
                })?;
                let operation_kind = enum_to_str(&e.operation_kind)?;
                let settings_json = serde_json::to_string(&e.settings)
                    .map_err(|err| StoreError::Serialization(err.to_string()))?;
                sqlx::query(
                    "INSERT INTO provider_bindings (
                        provider_binding_id, tenant_id, workspace_id, project_id,
                        provider_connection_id, provider_model_id, operation_kind,
                        settings_json, active, created_at
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                     ON CONFLICT (provider_binding_id) DO UPDATE SET
                        provider_connection_id = excluded.provider_connection_id,
                        provider_model_id      = excluded.provider_model_id,
                        operation_kind         = excluded.operation_kind,
                        settings_json          = excluded.settings_json",
                )
                .bind(e.provider_binding_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.provider_connection_id.as_str())
                .bind(e.provider_model_id.as_str())
                .bind(&operation_kind)
                .bind(&settings_json)
                .bind(e.active)
                .bind(created_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::ProviderBindingStateChanged(e) => {
                // UPDATE no-op if binding_id is unknown (mirrors
                // in_memory's `get_mut.map(..)`). Replay is safe because
                // the assignment is overwrite-stable for the same event.
                sqlx::query(
                    "UPDATE provider_bindings
                     SET active = $1
                     WHERE provider_binding_id = $2",
                )
                .bind(e.active)
                .bind(e.provider_binding_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
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
                // UPDATE no-op if budget_id is unknown (mirrors
                // in_memory's `get_mut.map(..)`). Replay is safe because
                // the write is overwrite-stable on the same event.
                sqlx::query(
                    "UPDATE provider_budgets
                     SET current_spend_micros = $1,
                         alert_triggered_at_ms = $2,
                         updated_at = $2
                     WHERE budget_id = $3",
                )
                .bind(current)
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
                // current_spend = limit + exceeded_by (matches in_memory
                // semantics). Use COALESCE so a missing exceeded_at_ms
                // stays at the earliest observed overrun.
                sqlx::query(
                    "UPDATE provider_budgets
                     SET current_spend_micros = limit_micros + $1,
                         exceeded_at_ms = COALESCE(exceeded_at_ms, $2),
                         updated_at = $2
                     WHERE budget_id = $3",
                )
                .bind(over)
                .bind(exceeded_at)
                .bind(e.budget_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // RFC-025 Phase 3: provider_connections projection. Tenant-level
            // endpoint registration. `supported_models_json` is a TEXT
            // column carrying a serde_json array of model identifiers;
            // stays portable (no pg arrays, no JSONB).
            //
            // Upsert on `provider_connection_id` because
            // `ProviderConnectionRegistered` is append-only under normal
            // operation — replay of the same event must not fail.
            RuntimeEvent::ProviderConnectionRegistered(e) => {
                let registered_at = i64::try_from(e.registered_at).map_err(|_| {
                    StoreError::Internal(format!(
                        "ProviderConnectionRegistered.registered_at {} exceeds i64::MAX",
                        e.registered_at
                    ))
                })?;
                let status = enum_to_str(&e.status)?;
                let supported_models_json = serde_json::to_string(&e.supported_models)
                    .map_err(|err| StoreError::Serialization(err.to_string()))?;
                sqlx::query(
                    "INSERT INTO provider_connections (
                        provider_connection_id, tenant_id, provider_family,
                        adapter_type, supported_models_json, status, created_at
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                     ON CONFLICT (provider_connection_id) DO UPDATE SET
                        provider_family       = excluded.provider_family,
                        adapter_type          = excluded.adapter_type,
                        supported_models_json = excluded.supported_models_json,
                        status                = excluded.status",
                )
                .bind(e.provider_connection_id.as_str())
                .bind(e.tenant.tenant_id.as_str())
                .bind(&e.provider_family)
                .bind(&e.adapter_type)
                .bind(&supported_models_json)
                .bind(&status)
                .bind(registered_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::ProviderConnectionDeleted(e) => {
                // F40: hard-remove so the id can be re-created. History
                // stays in the event log for audit. Matches in_memory
                // (which `.remove()`s the key). Replay is safe because
                // DELETE on a missing row is a no-op.
                sqlx::query(
                    "DELETE FROM provider_connections WHERE provider_connection_id = $1",
                )
                .bind(e.provider_connection_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
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
            RuntimeEvent::RoutePolicyUpdated(_) => log_stub("RoutePolicyUpdated"),
            // Projection contract: one row per `ToolInvocationCacheHit`
            // keyed by `invocation_id`. Operators query cache activity
            // via the REST surface (`tool_invocation_cache_hits` table)
            // instead of replaying the event log. `ON CONFLICT DO
            // NOTHING` keeps replay idempotent.
            RuntimeEvent::ToolInvocationCacheHit(e) => {
                let original_completed_at = i64::try_from(e.original_completed_at_ms).map_err(|_| {
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
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                     ON CONFLICT (invocation_id) DO NOTHING",
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

            // #364: durable projection of `ToolInvocationProgressUpdated`
            // so `GET /v1/tool-invocations/:id/progress` can answer
            // tenant-scoped reads in O(1) without scanning the event log.
            // UPSERT keyed by `invocation_id`; we copy the project scope
            // from the existing `tool_invocations` row so the handler can
            // tenant-filter without a second lookup. Events for an
            // invocation that does not exist yet (should not happen —
            // Started precedes progress) are a no-op rather than
            // fabricating a project scope. The `WHERE excluded.updated_at_ms
            // >= ...` clause keeps out-of-order replay idempotent: a
            // stale event never overwrites a newer one.
            RuntimeEvent::ToolInvocationProgressUpdated(e) => {
                let scope: Option<(String, String, String)> = sqlx::query_as(
                    "SELECT tenant_id, workspace_id, project_id
                     FROM tool_invocations
                     WHERE invocation_id = $1",
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
                         VALUES ($1, $2, $3, $4, $5, $6, $7)
                         ON CONFLICT (invocation_id) DO UPDATE SET
                             progress_pct  = EXCLUDED.progress_pct,
                             message       = EXCLUDED.message,
                             updated_at_ms = EXCLUDED.updated_at_ms
                         WHERE EXCLUDED.updated_at_ms >= tool_invocation_progress.updated_at_ms",
                    )
                    .bind(e.invocation_id.as_str())
                    .bind(tenant_id)
                    .bind(workspace_id)
                    .bind(project_id)
                    .bind(i16::from(e.progress_pct))
                    .bind(e.message.as_deref())
                    .bind(updated_at)
                    .execute(&mut **tx)
                    .await
                    .map_err(|err| StoreError::Internal(err.to_string()))?;
                }
            }
            // F39: RFC 020 Track 4 boot-level recovery audit projected to
            // `recovery_summaries` (one row per boot_id). The emitter
            // contract guarantees one summary per boot; ON CONFLICT DO
            // NOTHING keeps replay idempotent.
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
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                             $12, $13, $14, $15, $16, $17, $18, $19)
                     ON CONFLICT (boot_id) DO NOTHING",
                )
                .bind(&e.boot_id)
                .bind(e.sentinel_project.tenant_id.as_str())
                .bind(e.sentinel_project.workspace_id.as_str())
                .bind(e.sentinel_project.project_id.as_str())
                // All count fields are `u32`; `i64::from` is an
                // infallible widening conversion (no silent truncation)
                // and keeps the checked-cast discipline symmetric with
                // the `u64 -> i64` `try_from` calls above.
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

            // PR BP-2: project tool-call approval events into the
            // `tool_call_approvals` table. ToolCallProposed inserts a
            // new pending row; Amended/Approved/Rejected update it.
            RuntimeEvent::ToolCallProposed(e) => {
                let match_policy_json = serde_json::to_value(&e.match_policy)
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
                         $1, $2, $3, $4, $5, $6,
                         $7, $8, NULL, NULL,
                         $9, $10, 'pending', NULL, NULL, NULL,
                         $11, NULL, NULL, NULL,
                         1, $12, $12
                     )
                     ON CONFLICT (call_id) DO NOTHING",
                )
                .bind(e.call_id.as_str())
                .bind(e.session_id.as_str())
                .bind(e.run_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(&e.tool_name)
                .bind(&e.tool_args)
                .bind(display_summary_opt)
                .bind(&match_policy_json)
                .bind(e.proposed_at_ms as i64)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::ToolCallAmended(e) => {
                sqlx::query(
                    "UPDATE tool_call_approvals
                     SET amended_tool_args = $1,
                         last_amended_at_ms = $2,
                         version = version + 1,
                         updated_at = $3
                     WHERE call_id = $4",
                )
                .bind(&e.new_tool_args)
                .bind(e.amended_at_ms as i64)
                .bind(now)
                .bind(e.call_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::ToolCallApproved(e) => {
                let scope_json = serde_json::to_value(&e.scope)
                    .map_err(|err| StoreError::Serialization(err.to_string()))?;
                sqlx::query(
                    "UPDATE tool_call_approvals
                     SET state = 'approved',
                         operator_id = $1,
                         scope = $2,
                         approved_tool_args = $3,
                         approved_at_ms = $4,
                         version = version + 1,
                         updated_at = $5
                     WHERE call_id = $6",
                )
                .bind(e.operator_id.as_str())
                .bind(&scope_json)
                .bind(e.approved_tool_args.as_ref())
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
                         operator_id = $1,
                         reason = $2,
                         rejected_at_ms = $3,
                         version = version + 1,
                         updated_at = $4
                     WHERE call_id = $5",
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

            RuntimeEvent::TenantCreated(e) => {
                sqlx::query(
                    "INSERT INTO tenants (tenant_id, name, created_at, updated_at)
                     VALUES ($1, $2, $3, $3)
                     ON CONFLICT (tenant_id) DO NOTHING",
                )
                .bind(e.tenant_id.as_str())
                .bind(&e.name)
                .bind(e.created_at as i64)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }

            RuntimeEvent::WorkspaceCreated(e) => {
                sqlx::query(
                    "INSERT INTO workspaces (workspace_id, tenant_id, name, created_at, updated_at)
                     VALUES ($1, $2, $3, $4, $4)
                     ON CONFLICT (workspace_id) DO NOTHING",
                )
                .bind(e.workspace_id.as_str())
                .bind(e.tenant_id.as_str())
                .bind(&e.name)
                .bind(e.created_at as i64)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }

            RuntimeEvent::WorkspaceArchived(e) => {
                // `tenant_id` is included in the WHERE clause as
                // defense-in-depth: the service layer already refuses
                // cross-tenant archives before emitting, but if a replay
                // ever presents a mismatched event we'd rather no-op than
                // archive another tenant's row.
                sqlx::query(
                    "UPDATE workspaces
                        SET archived_at = $1, updated_at = $1
                      WHERE workspace_id = $2 AND tenant_id = $3",
                )
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
                     VALUES ($1, $2, $3, $4, $5, $5)
                     ON CONFLICT (project_id) DO NOTHING",
                )
                .bind(e.project.project_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(&e.name)
                .bind(e.created_at as i64)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }

            RuntimeEvent::RoutePolicyCreated(e) => {
                let rules = serde_json::to_value(&e.rules)
                    .map_err(|err| StoreError::Serialization(err.to_string()))?;
                sqlx::query(
                    "INSERT INTO route_policies (policy_id, tenant_id, name, rules, enabled, created_at, updated_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $6)
                     ON CONFLICT (policy_id) DO UPDATE
                     SET name = EXCLUDED.name,
                         rules = EXCLUDED.rules,
                         enabled = EXCLUDED.enabled,
                         updated_at = EXCLUDED.updated_at",
                )
                .bind(&e.policy_id)
                .bind(e.tenant_id.as_str())
                .bind(&e.name)
                .bind(rules)
                .bind(e.enabled)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }

            RuntimeEvent::WorkspaceMemberAdded(e) => {
                let role = enum_to_str(&e.role)?;
                sqlx::query(
                    "INSERT INTO workspace_members (workspace_id, operator_id, role, added_at_ms)
                     VALUES ($1, $2, $3, $4)
                     ON CONFLICT (workspace_id, operator_id) DO UPDATE SET role = EXCLUDED.role",
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
                    "DELETE FROM workspace_members WHERE workspace_id = $1 AND operator_id = $2",
                )
                .bind(e.workspace_key.workspace_id.as_str())
                .bind(e.member_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }

            RuntimeEvent::PromptAssetCreated(e) => {
                sqlx::query(
                    "INSERT INTO prompt_assets
                         (prompt_asset_id, tenant_id, workspace_id, project_id, name, kind,
                          scope, status, created_at, updated_at)
                     VALUES ($1, $2, $3, $4, $5, $6, NULL, 'draft', $7, $8)
                     ON CONFLICT (prompt_asset_id) DO NOTHING",
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
                // Allocate the next version_number under row-level lock.
                //
                // Pre-T2-H4 this used `COUNT(*) + 1` under READ COMMITTED
                // which is a classic race — two concurrent appends for
                // the same asset both read N and try to insert N+1,
                // colliding on the unique constraint or silently producing
                // duplicates.
                //
                // The first serialisation attempt used `SELECT MAX(...)
                // FROM prompt_versions WHERE prompt_asset_id = $1 FOR
                // UPDATE`, but `FOR UPDATE` on an aggregate query with
                // zero matching rows locks nothing. We now take a lock on
                // the parent `prompt_assets` row (which must exist before
                // any version for it can be created) and derive the
                // version number from the versions table under that held
                // lock. Concurrent appends for the same asset serialise
                // behind the parent row lock.
                //
                // Errors propagate (no `unwrap_or` swallowing) so the
                // transaction aborts rather than inserting
                // `version_number = 1` on top of a transient DB failure.
                sqlx::query_scalar::<_, String>(
                    "SELECT prompt_asset_id FROM prompt_assets
                     WHERE prompt_asset_id = $1
                     FOR UPDATE",
                )
                .bind(e.prompt_asset_id.as_str())
                .fetch_optional(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;

                let version_number: i64 = sqlx::query_scalar(
                    "SELECT COALESCE(MAX(version_number), 0) + 1
                     FROM prompt_versions
                     WHERE prompt_asset_id = $1",
                )
                .bind(e.prompt_asset_id.as_str())
                .fetch_one(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;

                sqlx::query(
                    "INSERT INTO prompt_versions
                         (prompt_version_id, prompt_asset_id, tenant_id, workspace_id, project_id,
                          version_number, content_hash, content, format, created_by, created_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, NULL, NULL, $8)
                     ON CONFLICT (prompt_version_id) DO NOTHING",
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

            RuntimeEvent::PromptReleaseCreated(e) => {
                sqlx::query(
                    "INSERT INTO prompt_releases
                         (prompt_release_id, prompt_asset_id, prompt_version_id,
                          tenant_id, workspace_id, project_id,
                          release_tag, state, rollout_target, created_at, updated_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, 'draft', NULL, $8, $8)
                     ON CONFLICT (prompt_release_id) DO NOTHING",
                )
                .bind(e.prompt_release_id.as_str())
                .bind(e.prompt_asset_id.as_str())
                .bind(e.prompt_version_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.release_tag.as_deref())
                .bind(e.created_at as i64)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }

            RuntimeEvent::PromptReleaseTransitioned(e) => {
                sqlx::query(
                    "UPDATE prompt_releases
                     SET state = $1, updated_at = $2
                     WHERE prompt_release_id = $3",
                )
                .bind(&e.to_state)
                .bind(now)
                .bind(e.prompt_release_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }

            RuntimeEvent::RouteDecisionMade(e) => {
                let operation_kind = enum_to_str(&e.operation_kind)?;
                let final_status   = enum_to_str(&e.final_status)?;
                let selector_ctx: Option<serde_json::Value> = None; // not carried by event
                sqlx::query(
                    "INSERT INTO route_decisions
                         (route_decision_id, tenant_id, workspace_id, project_id,
                          operation_kind, route_policy_id, terminal_route_attempt_id,
                          selected_provider_binding_id, selected_route_attempt_id,
                          selector_context, attempt_count, fallback_used, final_status,
                          created_at)
                     VALUES ($1, $2, $3, $4, $5, NULL, NULL, $6, NULL, $7, $8, $9, $10, $11)
                     ON CONFLICT (route_decision_id) DO NOTHING",
                )
                .bind(e.route_decision_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(operation_kind)
                .bind(e.selected_provider_binding_id.as_ref().map(|id| id.as_str()))
                .bind(selector_ctx)
                .bind(e.attempt_count as i32)
                .bind(e.fallback_used)
                .bind(final_status)
                .bind(e.decided_at as i64)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }

            RuntimeEvent::ProviderCallCompleted(e) => {
                let operation_kind = enum_to_str(&e.operation_kind)?;
                let status         = enum_to_str(&e.status)?;
                let error_class    = e.error_class.as_ref().map(enum_to_str).transpose()?;
                // Derive latency from timestamps if not explicit.
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
                         ($1, $2, $3, $4, $5, $6, $7, $8, $9, '', $10,
                          $11, $12, $13, $14, $15, $16, $17, $18, $19,
                          $20, $21, $22, $23)
                     ON CONFLICT (provider_call_id) DO NOTHING",
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
                .bind(e.fallback_position as i32)
                .bind(status)
                .bind(latency_ms)
                .bind(e.input_tokens.map(|v| v as i32))
                .bind(e.output_tokens.map(|v| v as i32))
                .bind(e.cost_micros.map(|v| v as i64))
                .bind(error_class)
                .bind(e.raw_error_message.as_deref())
                .bind(e.retry_count as i32)
                .bind(e.completed_at as i64)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;

                // F29 CD-2: fold the call into session/project/workspace
                // cost rollups. This is the production path — the
                // InMemoryStore derives `SessionCostUpdated` from the
                // same event for the in-process read model, but that
                // derived event never reaches the durable log. Projecting
                // directly here keeps the pg tables in sync with InMemory.
                //
                // Calls with a resolvable session_id contribute to
                // session/project/workspace cost. We mirror the in-
                // memory `apply_projection` logic: prefer the event's
                // own session_id, fall back to the run's session_id,
                // skip rollup when neither is available. No additional
                // success/status filter is applied here — a zero-cost
                // failed call still bumps `provider_calls` so the
                // operator panel reflects actual attempt counts.
                let effective_session_id: Option<String> = if let Some(sid) = &e.session_id {
                    Some(sid.as_str().to_owned())
                } else if let Some(rid) = &e.run_id {
                    sqlx::query_scalar::<_, Option<String>>(
                        "SELECT session_id FROM runs WHERE run_id = $1",
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
                    // ProviderCallCompleted has no top-level `tenant_id`
                    // — the tenant is only carried by `e.project`, so
                    // that's our source of truth here.
                    upsert_cost_rollups_pg(
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

            | RuntimeEvent::RunSlaBreached(_)
            | RuntimeEvent::RunSlaSet(_)
            | RuntimeEvent::SignalRouted(_)
            | RuntimeEvent::SignalSubscriptionCreated(_)
            | RuntimeEvent::SnapshotCreated(_)
            | RuntimeEvent::TaskDependencyAdded(_)
            | RuntimeEvent::TaskDependencyResolved(_)
            | RuntimeEvent::TaskLeaseExpired(_)
            | RuntimeEvent::TaskPriorityChanged(_)
            // RFC 005 approval policies — no durable table yet
            | RuntimeEvent::ApprovalPolicyCreated(_)
            // RFC 001 gradual rollout — state tracked via prompt_releases table
            | RuntimeEvent::PromptRolloutStarted(_) => {}

            // ── RFC-025 Phase 1.5a: trigger + run_template + trigger_fires ─────
            // 13 variants that previously no-op'd in the shared arm above.
            // Eight state-carrying lifecycle edges mutate `triggers` /
            // `run_templates`; five audit edges insert an append-only row
            // into `trigger_fires` (classified Ephemeral in the registry
            // because no runtime state recovers from them at boot, but
            // persisted for observability + rolling-window rate-limit /
            // project-budget counts + duplicate-fire ledger). See
            // `crates/cairn-store/src/pg/migrations/V035__create_trigger_projections.sql`.
            RuntimeEvent::TriggerCreated(e) => {
                sqlx::query(
                    "INSERT INTO triggers
                         (trigger_id, tenant_id, workspace_id, project_id,
                          name, description, signal_type, plugin_id,
                          conditions_json, run_template_id,
                          state, state_reason, suspension_reason, state_since,
                          max_per_minute, max_burst, max_chain_depth,
                          created_by, created_at, updated_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                             'enabled', NULL, NULL, NULL,
                             $11, $12, $13, $14, $15, $15)
                     ON CONFLICT (trigger_id) DO NOTHING",
                )
                .bind(e.trigger_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(&e.name)
                .bind(e.description.as_deref())
                .bind(&e.signal_type)
                .bind(e.plugin_id.as_deref())
                .bind(
                    serde_json::to_string(&e.conditions)
                        .map_err(|err| StoreError::Serialization(err.to_string()))?,
                )
                .bind(e.run_template_id.as_str())
                .bind(e.max_per_minute as i64)
                .bind(e.max_burst as i64)
                .bind(e.max_chain_depth as i32)
                .bind(e.created_by.as_str())
                .bind(e.created_at as i64)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::TriggerEnabled(e) => {
                sqlx::query(
                    "UPDATE triggers
                     SET state = 'enabled',
                         state_reason = NULL,
                         suspension_reason = NULL,
                         state_since = NULL,
                         updated_at = $1
                     WHERE trigger_id = $2",
                )
                .bind(e.at as i64)
                .bind(e.trigger_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::TriggerDisabled(e) => {
                sqlx::query(
                    "UPDATE triggers
                     SET state = 'disabled',
                         state_reason = $1,
                         suspension_reason = NULL,
                         state_since = $2,
                         updated_at = $2
                     WHERE trigger_id = $3",
                )
                .bind(e.reason.as_deref())
                .bind(e.at as i64)
                .bind(e.trigger_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::TriggerSuspended(e) => {
                // Use the short-name helper rather than `enum_to_str` so
                // struct variants (RepeatedFailures { failure_count })
                // don't land as a full JSON object in the column —
                // byte-parity with the in-memory + sqlite appliers
                // depends on this (PR #569 review).
                let reason_str =
                    crate::projections::trigger::suspension_reason_discriminant(&e.reason);
                sqlx::query(
                    "UPDATE triggers
                     SET state = 'suspended',
                         state_reason = NULL,
                         suspension_reason = $1,
                         state_since = $2,
                         updated_at = $2
                     WHERE trigger_id = $3",
                )
                .bind(reason_str)
                .bind(e.at as i64)
                .bind(e.trigger_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::TriggerResumed(e) => {
                sqlx::query(
                    "UPDATE triggers
                     SET state = 'enabled',
                         state_reason = NULL,
                         suspension_reason = NULL,
                         state_since = NULL,
                         updated_at = $1
                     WHERE trigger_id = $2",
                )
                .bind(e.at as i64)
                .bind(e.trigger_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::TriggerDeleted(e) => {
                sqlx::query("DELETE FROM triggers WHERE trigger_id = $1")
                    .bind(e.trigger_id.as_str())
                    .execute(&mut **tx)
                    .await
                    .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::RunTemplateCreated(e) => {
                let plugin_allowlist = e
                    .plugin_allowlist
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()
                    .map_err(|err| StoreError::Serialization(err.to_string()))?;
                let tool_allowlist = e
                    .tool_allowlist
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()
                    .map_err(|err| StoreError::Serialization(err.to_string()))?;
                let required_fields = serde_json::to_string(&e.required_fields)
                    .map_err(|err| StoreError::Serialization(err.to_string()))?;
                let default_mode_str = enum_to_str(&e.default_mode)?;
                sqlx::query(
                    "INSERT INTO run_templates
                         (template_id, tenant_id, workspace_id, project_id,
                          name, description, default_mode, system_prompt,
                          initial_user_message,
                          plugin_allowlist_json, tool_allowlist_json,
                          budget_max_tokens, budget_max_wall_clock_ms,
                          budget_max_iterations, budget_exploration_budget_share,
                          sandbox_hint, required_fields_json,
                          created_by, created_at, updated_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                             $12, $13, $14, $15, $16, $17, $18, $19, $19)
                     ON CONFLICT (template_id) DO NOTHING",
                )
                .bind(e.template_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(&e.name)
                .bind(e.description.as_deref())
                .bind(default_mode_str)
                .bind(&e.system_prompt)
                .bind(e.initial_user_message.as_deref())
                .bind(plugin_allowlist)
                .bind(tool_allowlist)
                .bind(e.budget_max_tokens.map(|v| v as i64))
                .bind(e.budget_max_wall_clock_ms.map(|v| v as i64))
                .bind(e.budget_max_iterations.map(|v| v as i64))
                .bind(e.budget_exploration_budget_share.map(|v| v as f64))
                .bind(e.sandbox_hint.as_deref())
                .bind(required_fields)
                .bind(e.created_by.as_str())
                .bind(e.created_at as i64)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::RunTemplateDeleted(e) => {
                sqlx::query("DELETE FROM run_templates WHERE template_id = $1")
                    .bind(e.template_id.as_str())
                    .execute(&mut **tx)
                    .await
                    .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::TriggerFired(e) => {
                let metadata = serde_json::to_string(&serde_json::json!({
                    "run_id": e.run_id.as_str(),
                    "chain_depth": e.chain_depth,
                }))
                .map_err(|err| StoreError::Serialization(err.to_string()))?;
                insert_trigger_fire_pg(
                    tx,
                    &e.trigger_id,
                    &e.project,
                    e.signal_id.as_str(),
                    "fired",
                    Some(e.signal_type.as_str()),
                    Some(metadata.as_str()),
                    e.fired_at,
                )
                .await?;
            }
            RuntimeEvent::TriggerSkipped(e) => {
                // Short-name discriminant so struct variants (namely
                // `MissingRequiredField { field }`) don't collapse to a
                // JSON-object string — in-memory + pg + sqlite parity
                // (PR #569 review).
                let reason_str =
                    crate::projections::trigger::skip_reason_discriminant(&e.reason);
                // Surface the optional field payload under a separate
                // JSON key so the metadata row is self-describing and
                // portable across backends without overloading `reason`.
                let field =
                    if let cairn_domain::events::TriggerSkipReason::MissingRequiredField {
                        field,
                    } = &e.reason
                    {
                        Some(field.as_str())
                    } else {
                        None
                    };
                let metadata = serde_json::to_string(&serde_json::json!({
                    "reason": reason_str,
                    "field": field,
                }))
                .map_err(|err| StoreError::Serialization(err.to_string()))?;
                insert_trigger_fire_pg(
                    tx,
                    &e.trigger_id,
                    &e.project,
                    e.signal_id.as_str(),
                    "skipped",
                    None,
                    Some(metadata.as_str()),
                    e.skipped_at,
                )
                .await?;
            }
            RuntimeEvent::TriggerDenied(e) => {
                let metadata = serde_json::to_string(&serde_json::json!({
                    "decision_id": e.decision_id.as_str(),
                    "reason": e.reason,
                }))
                .map_err(|err| StoreError::Serialization(err.to_string()))?;
                insert_trigger_fire_pg(
                    tx,
                    &e.trigger_id,
                    &e.project,
                    e.signal_id.as_str(),
                    "denied",
                    None,
                    Some(metadata.as_str()),
                    e.denied_at,
                )
                .await?;
            }
            RuntimeEvent::TriggerRateLimited(e) => {
                let metadata = serde_json::to_string(&serde_json::json!({
                    "bucket_remaining": e.bucket_remaining,
                    "bucket_capacity": e.bucket_capacity,
                }))
                .map_err(|err| StoreError::Serialization(err.to_string()))?;
                insert_trigger_fire_pg(
                    tx,
                    &e.trigger_id,
                    &e.project,
                    e.signal_id.as_str(),
                    "rate_limited",
                    None,
                    Some(metadata.as_str()),
                    e.rate_limited_at,
                )
                .await?;
            }
            RuntimeEvent::TriggerPendingApproval(e) => {
                let metadata = serde_json::to_string(&serde_json::json!({
                    "approval_id": e.approval_id.as_str(),
                }))
                .map_err(|err| StoreError::Serialization(err.to_string()))?;
                insert_trigger_fire_pg(
                    tx,
                    &e.trigger_id,
                    &e.project,
                    e.signal_id.as_str(),
                    "pending_approval",
                    None,
                    Some(metadata.as_str()),
                    e.pending_at,
                )
                .await?;
            }
            // F65 PR-2: events without a dedicated projection table. Listed
            // one-per-line (matching sqlite/in_memory) so a refactor that
            // changes one variant's handling surfaces in review instead of
            // hiding inside a shared or-pattern (#444).
            //
            // `SessionAttemptCompleted` is visible via the event log +
            // the subsequent `SessionOutcomeEmitted` row.
            RuntimeEvent::SessionAttemptCompleted(_) => {}
            // Breaker trips are forensic — on the event log, and mirrored
            // into the session outcome row when the trip terminates the
            // attempt. No dedicated projection table.
            RuntimeEvent::CircuitBreakerTripped(_) => {}
            // Budget-threshold-crossed is purely observability (SSE).
            RuntimeEvent::BudgetThresholdCrossed(_) => {}
            // Orchestrator decisions are operator observability (SSE + audit).
            RuntimeEvent::OrchestratorDecisionMade(_) => {}
            // Summarizer fallback is audit-only (provenance of
            // compacted_summary). Captured on the event log.
            RuntimeEvent::SummarizerFallback(_) => {}
            // Workspace-backend-degraded fires at sandbox init. Operator
            // alerts via SSE + metrics; no projection row.
            RuntimeEvent::WorkspaceBackendDegraded(_) => {}

            // F65 PR-2: bump attempts_used on the session row. Replay-safe
            // via `GREATEST(...)` — we only ever advance the counter, so
            // repeated delivery leaves the row idempotent. Missing row is
            // silently ignored (follows the same contract as
            // `RunCompletionAnnotated` above).
            RuntimeEvent::SessionAttemptStarted(e) => {
                let attempt = i32::try_from(e.attempt_number).map_err(|_| {
                    StoreError::Internal(format!(
                        "SessionAttemptStarted.attempt_number {} exceeds i32::MAX",
                        e.attempt_number
                    ))
                })?;
                let max_attempts = i32::try_from(e.max_attempts).map_err(|_| {
                    StoreError::Internal(format!(
                        "SessionAttemptStarted.max_attempts {} exceeds i32::MAX",
                        e.max_attempts
                    ))
                })?;
                sqlx::query(
                    "UPDATE sessions
                        SET attempts_used = GREATEST(attempts_used, $1),
                            max_attempts  = GREATEST(max_attempts, $2),
                            version       = version + 1,
                            updated_at    = $3
                      WHERE session_id = $4",
                )
                .bind(attempt)
                .bind(max_attempts)
                .bind(now)
                .bind(e.session_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }

            // F65 PR-2: F65 checkpoint extension. Inserts a new checkpoints
            // row keyed by checkpoint_id. `run_id` is the root-Run pointer
            // (satisfies the RFC 005 FK on checkpoints.run_id) and the F65
            // columns carry the orchestrator-resumable shape. Pre-F65
            // rows continue to live alongside with NULL F65 columns. The
            // `ON CONFLICT DO UPDATE` path keeps replay idempotent while
            // preserving the original created_at.
            RuntimeEvent::CheckpointPersisted(e) => {
                let schema_version: i32 = 1;
                let iteration = i32::try_from(e.iteration).map_err(|_| {
                    StoreError::Internal(format!(
                        "CheckpointPersisted.iteration {} exceeds i32::MAX",
                        e.iteration
                    ))
                })?;
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
                     VALUES ($1, $2, $3, $4, $5, 'latest', 1, $6, $7, $8, '', 0, $9)
                     ON CONFLICT (checkpoint_id) DO UPDATE SET
                         session_id = EXCLUDED.session_id,
                         schema_version = EXCLUDED.schema_version,
                         iteration = EXCLUDED.iteration",
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

            // #482: insert the snapshot row with the metadata carried on
            // the event. `snapshot_path` remains empty because it is
            // host-local filesystem detail (see the rustdoc on
            // `cairn_domain::WorkspaceSnapshotCreated`) — operators
            // resolve it via `cairn_domain::session_orchestration::WorkspaceSnapshot`.
            // `bytes`, `reflink_used`, `parent_snapshot_id` now round-trip
            // on the event itself so a fresh replay from an empty DB
            // rebuilds the row with the same metadata the live writer
            // produced, closing the event-sourcing gap audited in #482.
            // The runtime's `WorkspaceSnapshotWriter::stamp_metadata` call
            // still fires once in live mode to fill `snapshot_path`; its
            // `bytes` / `reflink_used` / `parent_snapshot_id` arguments
            // are now redundant with the event body but harmless.
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
                sqlx::query(
                    "INSERT INTO workspace_snapshots (
                         snapshot_id, tenant_id, workspace_scope, project_id,
                         session_id, workspace_id, parent_snapshot_id,
                         snapshot_path, bytes, reflink_used, created_at
                     )
                     VALUES ($1, $2, $3, $4, $5, $6, $7, '', $8, $9, $10)
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
                .bind(e.reflink_used)
                .bind(at_ms)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }

            // F65 PR-2: mark the snapshot reaped in-place. Replay is
            // stable: setting `reaped_at` twice leaves it at the last
            // delivered value (no earlier-wins semantics needed —
            // reaping is terminal).
            RuntimeEvent::WorkspaceSnapshotReaped(e) => {
                let at_ms = i64::try_from(e.at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "WorkspaceSnapshotReaped.at_ms {} exceeds i64::MAX",
                        e.at_ms
                    ))
                })?;
                sqlx::query(
                    "UPDATE workspace_snapshots
                        SET reaped_at = $1
                      WHERE snapshot_id = $2",
                )
                .bind(at_ms)
                .bind(e.snapshot_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }

            // F65 PR-2: persist the rich outcome row. `workspace_snapshot_id`
            // is nullable (arch doc §6.3 — legacy runs predating the sandbox).
            // `compacted_summary` stays empty on pre-PR-6 outcomes; PR-6
            // back-fills it when the summarizer ships. Idempotent on
            // replay via `ON CONFLICT (root_run_id) DO UPDATE` — the later
            // copy wins for the summarizer / next_step_hint columns
            // (summarizer retry can enrich).
            RuntimeEvent::SessionOutcomeEmitted(e) => {
                let outcome = &e.outcome;
                let termination_kind =
                    crate::projections::termination_reason_kind(&outcome.termination_reason);
                // Full payload as JSON-as-TEXT so readers can rehydrate
                // the `ProviderError.message` / `CircuitBreakerTripped.trip`
                // / `Crashed.message` fields without walking the event log.
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
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                     ON CONFLICT (root_run_id) DO UPDATE SET
                         workspace_snapshot_id   = EXCLUDED.workspace_snapshot_id,
                         termination_reason      = EXCLUDED.termination_reason,
                         termination_reason_json = EXCLUDED.termination_reason_json,
                         compacted_summary       = EXCLUDED.compacted_summary,
                         next_step_hint          = EXCLUDED.next_step_hint,
                         cost_micros             = EXCLUDED.cost_micros",
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
            // F39: RFC 019 / RFC 020 decision-cache projection. The
            // in-memory cache is still rebuilt from the event log at boot
            // (see cairn-app warmup); these tables give operator tooling
            // a queryable read model without walking the full stream.
            // `decision_key` + `outcome` are serialized as JSON text for
            // pg/sqlite portability (no JSONB operators).
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
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                     ON CONFLICT (decision_id) DO NOTHING",
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
            // F47 PR2: persist the completion summary + verification
            // sidecar onto the existing runs row. UPDATE-only — the
            // event presumes the run row already exists (the normal
            // completion path runs RunCreated → RunStateChanged →
            // complete() → RunCompletionAnnotated). Silent no-op on
            // missing row mirrors the RunStateChanged handler above:
            // replay of an annotation with no corresponding RunCreated
            // in the same log is a malformed log, not something the
            // projection should hard-fail on. `completion_verification_json`
            // stores serde-JSON as TEXT rather than JSONB so the
            // portable cross-backend contract (per the no-DB-specific-
            // features memory) holds; the value is written and read
            // wholesale, never queried with JSONB operators.
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
                        SET completion_summary              = $1,
                            completion_verification_json    = $2,
                            completion_annotated_at_ms      = $3,
                            version                         = version + 1,
                            updated_at                      = $4
                      WHERE run_id = $5",
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
            // F64: persist the terminal-write recovery outcome on the
            // runs row so operators see it on `GET /v1/runs/:id`. Silent
            // no-op on missing row mirrors `RunCompletionAnnotated` —
            // an orphan recovery event cannot mint a run.
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
                        SET terminal_write_recovery_json = $1,
                            version                     = version + 1,
                            updated_at                   = $2
                      WHERE run_id = $3",
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
                     VALUES ($1, $2, $3)
                     ON CONFLICT (warmed_at) DO NOTHING",
                )
                .bind(warmed_at)
                .bind(i64::from(e.cached))
                .bind(i64::from(e.expired_and_dropped))
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // F65 PR-5 (#359): crash-recovery umount sweep is an operator
            // observability surface (SSE + metrics) with no projection
            // table — the event log itself is the audit trail.
            RuntimeEvent::SandboxCrashRecovered(_) => {}
        }

        Ok(())
    }
}

fn tool_invocation_terminal_state_str(
    outcome: ToolInvocationOutcomeKind,
) -> Result<String, StoreError> {
    enum_to_str(&outcome.terminal_state())
}

/// Narrow a domain `u32` onto the projection's `INTEGER`/`i32` column
/// without the silent `as i32` wrap. Any value above `i32::MAX` surfaces
/// as a loud `StoreError` rather than rehydrating as 0 on read. Used by
/// quota / budget projections — sqlite carries the same helper.
fn i32_from_u32(field: &'static str, value: u32) -> Result<i32, StoreError> {
    i32::try_from(value).map_err(|_| {
        StoreError::Internal(format!(
            "{field} = {value} exceeds i32::MAX; projection column is INTEGER"
        ))
    })
}

/// Stable TEXT encoding of `ProviderBudgetPeriod` for the
/// `provider_budgets.period` column. Kept in lockstep with the sqlite
/// helper (they share the same stored TEXT so parity is byte-equal).
fn provider_budget_period_str(
    period: &cairn_domain::providers::ProviderBudgetPeriod,
) -> &'static str {
    match period {
        cairn_domain::providers::ProviderBudgetPeriod::Daily => "daily",
        cairn_domain::providers::ProviderBudgetPeriod::Monthly => "monthly",
    }
}

/// Stable TEXT encoding of `ProductTier` for the `licenses.tier`
/// column. Matches the serde `rename_all = "snake_case"` contract so
/// existing deserialisers still parse the value.
fn product_tier_str(tier: &cairn_domain::commercial::ProductTier) -> &'static str {
    match tier {
        cairn_domain::commercial::ProductTier::LocalEval => "local_eval",
        cairn_domain::commercial::ProductTier::TeamSelfHosted => "team_self_hosted",
        cairn_domain::commercial::ProductTier::EnterpriseSelfHosted => "enterprise_self_hosted",
    }
}

/// F29 CD-2: fold a (session_id, project, delta) tuple into
/// `session_costs`, `project_costs`, and `workspace_costs`. Called from
/// both the `SessionCostUpdated` projection and the
/// `ProviderCallCompleted` projection so the rollup tables stay in sync
/// with InMemory regardless of which event shape the runtime uses.
///
/// `updated_at_ms` uses `GREATEST(existing, incoming)` so an out-of-
/// order replay can never move the column backwards. Overflow on any
/// `u64 -> i64` cast is a hard error, not silent wrap — wall-clock ms
/// stays well under `i64::MAX` (year 292 million) and cost/token totals
/// reaching 9.2 exabillion micros is a serious anomaly worth halting on.
#[allow(clippy::too_many_arguments)]
async fn upsert_cost_rollups_pg(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
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
         VALUES ($1, $2, $3, $4, $5, $6, $7, 1, $8)
         ON CONFLICT (session_id) DO UPDATE SET
             total_cost_micros = session_costs.total_cost_micros + EXCLUDED.total_cost_micros,
             total_tokens_in   = session_costs.total_tokens_in   + EXCLUDED.total_tokens_in,
             total_tokens_out  = session_costs.total_tokens_out  + EXCLUDED.total_tokens_out,
             provider_calls    = session_costs.provider_calls    + 1,
             updated_at_ms     = GREATEST(session_costs.updated_at_ms, EXCLUDED.updated_at_ms)",
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
         VALUES ($1, $2, $3, $4, $5, $6, 1, $7)
         ON CONFLICT (tenant_id, workspace_id, project_id) DO UPDATE SET
             total_cost_micros = project_costs.total_cost_micros + EXCLUDED.total_cost_micros,
             total_tokens_in   = project_costs.total_tokens_in   + EXCLUDED.total_tokens_in,
             total_tokens_out  = project_costs.total_tokens_out  + EXCLUDED.total_tokens_out,
             provider_calls    = project_costs.provider_calls    + 1,
             updated_at_ms     = GREATEST(project_costs.updated_at_ms, EXCLUDED.updated_at_ms)",
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
         VALUES ($1, $2, $3, $4, $5, 1, $6)
         ON CONFLICT (tenant_id, workspace_id) DO UPDATE SET
             total_cost_micros = workspace_costs.total_cost_micros + EXCLUDED.total_cost_micros,
             total_tokens_in   = workspace_costs.total_tokens_in   + EXCLUDED.total_tokens_in,
             total_tokens_out  = workspace_costs.total_tokens_out  + EXCLUDED.total_tokens_out,
             provider_calls    = workspace_costs.provider_calls    + 1,
             updated_at_ms     = GREATEST(workspace_costs.updated_at_ms, EXCLUDED.updated_at_ms)",
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
/// Stored as a stable string so operator queries can filter without
/// parsing the full `decision_key_json` blob.
fn decision_outcome_kind(outcome: &cairn_domain::decisions::DecisionOutcome) -> &'static str {
    match outcome {
        cairn_domain::decisions::DecisionOutcome::Allowed => "allowed",
        cairn_domain::decisions::DecisionOutcome::Denied { .. } => "denied",
    }
}

/// Log an event variant the PG applier does not project.
///
/// Matches the `log_stub` pattern used by `SqliteSyncProjection`. See
/// `.claude/audit-state/review-queue.md` §T2-H3 for the coverage gap.
fn log_stub(variant: &'static str) {
    tracing::warn!(
        event_variant = variant,
        "pg projection stub: event committed to event_log but no projection table updated \
         (see PgSyncProjection unprojected-stubs list for the coverage gap)"
    );
}

/// Serialize a serde-serializable enum variant to its snake_case string form.
fn enum_to_str<T: serde::Serialize>(val: &T) -> Result<String, StoreError> {
    let v = serde_json::to_value(val).map_err(|e| StoreError::Serialization(e.to_string()))?;
    match v {
        serde_json::Value::String(s) => Ok(s),
        _ => Ok(v.to_string().trim_matches('"').to_owned()),
    }
}

/// RFC-025 Phase 1.5a: shared INSERT into `trigger_fires` for all five
/// audit variants. Kept as a free function so each variant arm above
/// stays a small parameter-building block; factoring out the shared SQL
/// means a schema change touches one site instead of five.
#[allow(clippy::too_many_arguments)]
async fn insert_trigger_fire_pg(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    trigger_id: &cairn_domain::ids::TriggerId,
    project: &cairn_domain::tenancy::ProjectKey,
    signal_id: &str,
    outcome: &str,
    signal_type: Option<&str>,
    metadata_json: Option<&str>,
    at_ms: u64,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO trigger_fires
             (trigger_id, tenant_id, workspace_id, project_id, signal_id,
              outcome, signal_type, metadata_json, at_ms)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(trigger_id.as_str())
    .bind(project.tenant_id.as_str())
    .bind(project.workspace_id.as_str())
    .bind(project.project_id.as_str())
    .bind(signal_id)
    .bind(outcome)
    .bind(signal_type)
    .bind(metadata_json)
    .bind(at_ms as i64)
    .execute(&mut **tx)
    .await
    .map_err(|err| StoreError::Internal(err.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::tool_invocation_terminal_state_str;
    use cairn_domain::tool_invocation::ToolInvocationOutcomeKind;

    #[test]
    fn canceled_tool_outcome_keeps_canceled_terminal_state() {
        let state = tool_invocation_terminal_state_str(ToolInvocationOutcomeKind::Canceled)
            .expect("state string");
        assert_eq!(state, "canceled");
    }

    #[test]
    fn failure_tool_outcome_keeps_failed_terminal_state() {
        let state = tool_invocation_terminal_state_str(ToolInvocationOutcomeKind::PermanentFailure)
            .expect("state string");
        assert_eq!(state, "failed");
    }
}
