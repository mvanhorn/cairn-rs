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
    ///
    /// `event_time_ms` mirrors the pg applier contract: it is the
    /// wall-clock millisecond at which the event was durably logged
    /// (live append = `now_millis()`, rebuild = `StoredEvent.stored_at`).
    /// Projection arms whose row data is semantically tied to the event
    /// time (e.g. `pause_schedules.resume_at_ms`) MUST use it rather
    /// than fabricating `now` at apply time — otherwise a rebuild
    /// silently shifts scheduled resumes forward. Copilot #595.
    pub async fn apply_async(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        envelope: &EventEnvelope<RuntimeEvent>,
        event_time_ms: u64,
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
                // #670 G4 / RFC 027: initialise `root_run_id`. Three
                // shapes, matching the pg projection's sub-SELECT form:
                //   1. Root (no parent) → self-reference.
                //   2. Child with parent row present → inherit the
                //      parent's `root_run_id` so the whole chain
                //      shares one absolute root.
                //   3. Child with parent row missing → NULL. The
                //      decrement path's no-op-on-NULL handles this.
                //
                // Non-determinism note: sub-SELECT reads the parent's
                // current `root_run_id`. Parent events land before
                // child events on replay (parent must exist for the
                // spawn to have succeeded), so the read is stable.
                sqlx::query(
                    "INSERT INTO runs (run_id, session_id, parent_run_id, tenant_id, workspace_id, project_id, state, version, created_at, updated_at, root_run_id) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', 1, ?7, ?7, \
                       CASE \
                         WHEN ?3 IS NULL THEN ?1 \
                         ELSE (SELECT root_run_id FROM runs WHERE run_id = ?3) \
                       END)",
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
                    "UPDATE runs SET state = ?, failure_class = ?, version = version + 1, updated_at = ? WHERE run_id = ?",
                )
                .bind(state_str)
                .bind(failure)
                .bind(now)
                .bind(e.run_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;

                // #670 G4 / RFC 027 §97: on terminal transition of a
                // non-root descendant, decrement the root's counter.
                // Mirrors the pg shape; `?1` stands in for the child
                // run_id, `?2` for the now timestamp.
                if e.transition.to.is_terminal() {
                    sqlx::query(
                        "UPDATE runs \
                            SET in_flight_descendants = in_flight_descendants - 1, \
                                version = version + 1, \
                                updated_at = ?2 \
                          WHERE run_id = ( \
                            SELECT root_run_id FROM runs r \
                             WHERE r.run_id = ?1 \
                               AND r.parent_run_id IS NOT NULL \
                               AND r.root_run_id IS NOT NULL \
                          )",
                    )
                    .bind(e.run_id.as_str())
                    .bind(now)
                    .execute(&mut **tx)
                    .await
                    .map_err(|e| StoreError::Internal(e.to_string()))?;
                }

                // Issue #592: pause_schedules projection — evict-on-resume.
                // Mirror pg (same SQL shape, `?` placeholders).
                // `resume_at_ms` + `created_at_ms` are derived from
                // `event_time_ms` so a rebuild replays scheduled
                // resumes at their original time rather than shifting
                // them forward to the rebuild wall clock. Copilot #595.
                match e.transition.to {
                    cairn_domain::RunState::Paused => {
                        if let Some(reason) = &e.pause_reason {
                            if let Some(resume_after_ms) = reason.resume_after_ms {
                                // Copilot #595: saturating_add guards
                                // pathologically large
                                // `resume_after_ms`; i64::try_from
                                // falls back to i64::MAX so we always
                                // bind a legal INTEGER rather than
                                // wrap-to-negative.
                                let resume_at_ms_u64 =
                                    event_time_ms.saturating_add(resume_after_ms);
                                let resume_at_ms_i64 =
                                    i64::try_from(resume_at_ms_u64).unwrap_or(i64::MAX);
                                let event_time_i64 =
                                    i64::try_from(event_time_ms).unwrap_or(i64::MAX);
                                sqlx::query(
                                    "INSERT INTO pause_schedules (
                                        run_id, tenant_id, workspace_id, project_id,
                                        resume_at_ms, created_at_ms
                                    )
                                     VALUES (?, ?, ?, ?, ?, ?)
                                     ON CONFLICT(run_id) DO UPDATE SET
                                        resume_at_ms = excluded.resume_at_ms,
                                        created_at_ms = excluded.created_at_ms",
                                )
                                .bind(e.run_id.as_str())
                                .bind(e.project.tenant_id.as_str())
                                .bind(e.project.workspace_id.as_str())
                                .bind(e.project.project_id.as_str())
                                .bind(resume_at_ms_i64)
                                .bind(event_time_i64)
                                .execute(&mut **tx)
                                .await
                                .map_err(|e| StoreError::Internal(e.to_string()))?;
                            }
                        }
                    }
                    _ => {
                        sqlx::query("DELETE FROM pause_schedules WHERE run_id = ?")
                            .bind(e.run_id.as_str())
                            .execute(&mut **tx)
                            .await
                            .map_err(|e| StoreError::Internal(e.to_string()))?;
                    }
                }
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

            // ── RFC-025 Phase 2b.2 m1: external_workers (Projected) ──
            //
            // Mirrors the pg applier — see pg/projections.rs for the
            // full semantic contract (status canonicalisation on re-
            // registration, terminal-outcome `current_task_id` clearing,
            // health-column reset on conflict). `is_alive` is INTEGER 0/1
            // on sqlite (no native BOOLEAN).
            RuntimeEvent::ExternalWorkerRegistered(e) => {
                let registered_at = i64::try_from(e.registered_at).map_err(|_| {
                    StoreError::Internal(format!(
                        "ExternalWorkerRegistered.registered_at {} exceeds i64::MAX",
                        e.registered_at
                    ))
                })?;
                // On conflict, reset health + current_task_id to their
                // zero-values so re-registration matches the in-memory
                // applier's whole-record overwrite (Copilot #580).
                sqlx::query(
                    "INSERT INTO external_workers (
                        worker_id, tenant_id, display_name, status,
                        registered_at, updated_at,
                        last_heartbeat_ms, is_alive, active_task_count, current_task_id
                     ) VALUES (?, ?, ?, 'active', ?, ?, 0, 0, 0, NULL)
                     ON CONFLICT (worker_id) DO UPDATE SET
                        tenant_id         = excluded.tenant_id,
                        display_name      = excluded.display_name,
                        status            = excluded.status,
                        registered_at     = excluded.registered_at,
                        updated_at        = excluded.updated_at,
                        last_heartbeat_ms = 0,
                        is_alive          = 0,
                        active_task_count = 0,
                        current_task_id   = NULL",
                )
                .bind(e.worker_id.as_str())
                .bind(e.tenant_id.as_str())
                .bind(&e.display_name)
                .bind(registered_at)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::ExternalWorkerSuspended(e) => {
                sqlx::query(
                    "UPDATE external_workers
                        SET status = 'suspended', updated_at = ?
                      WHERE worker_id = ?",
                )
                .bind(now)
                .bind(e.worker_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::ExternalWorkerReactivated(e) => {
                sqlx::query(
                    "UPDATE external_workers
                        SET status = 'active', updated_at = ?
                      WHERE worker_id = ?",
                )
                .bind(now)
                .bind(e.worker_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::ExternalWorkerReported(e) => {
                let last_hb = i64::try_from(e.report.reported_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "ExternalWorkerReported.reported_at_ms {} exceeds i64::MAX",
                        e.report.reported_at_ms
                    ))
                })?;
                let current_task_id: Option<&str> = if e.report.outcome.is_none() {
                    Some(e.report.task_id.as_str())
                } else {
                    None
                };
                sqlx::query(
                    "UPDATE external_workers
                        SET last_heartbeat_ms = ?,
                            is_alive          = 1,
                            current_task_id   = ?,
                            updated_at        = ?
                      WHERE worker_id = ?",
                )
                .bind(last_hb)
                .bind(current_task_id)
                .bind(now)
                .bind(e.report.worker_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }

            // ── UNPROJECTED STUBS ──────────────────────────────────────
            // These variants commit to event_log but do NOT update any
            // projection table on the SQLite backend. See the struct
            // docstring for the coverage-gap rationale and logging.
            // RFC-025 Phase 2b.2b m5: soul_patches projection (pg V055).
            RuntimeEvent::SoulPatchProposed(e) => {
                let proposed_at = i64::try_from(e.proposed_at).map_err(|_| {
                    StoreError::Internal(format!(
                        "SoulPatchProposed.proposed_at {} exceeds i64::MAX",
                        e.proposed_at
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO soul_patches (
                        patch_id, tenant_id, workspace_id, project_id,
                        state, patch_content, requires_approval,
                        proposed_at_ms
                     ) VALUES (?, ?, ?, ?, 'proposed', ?, ?, ?)
                     ON CONFLICT(patch_id) DO NOTHING",
                )
                .bind(&e.patch_id)
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(&e.patch_content)
                .bind(e.requires_approval)
                .bind(proposed_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::SoulPatchApplied(e) => {
                let applied_at = i64::try_from(e.applied_at).map_err(|_| {
                    StoreError::Internal(format!(
                        "SoulPatchApplied.applied_at {} exceeds i64::MAX",
                        e.applied_at
                    ))
                })?;
                let new_version = i32::try_from(e.new_version).map_err(|_| {
                    StoreError::Internal(format!(
                        "SoulPatchApplied.new_version {} exceeds i32::MAX",
                        e.new_version
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO soul_patches (
                        patch_id, tenant_id, workspace_id, project_id,
                        state, patch_content, requires_approval,
                        proposed_at_ms, applied_at_ms, new_version
                     ) VALUES (?, ?, ?, ?, 'applied', '', 0, 0, ?, ?)
                     ON CONFLICT(patch_id) DO UPDATE SET
                        state         = 'applied',
                        applied_at_ms = excluded.applied_at_ms,
                        new_version   = excluded.new_version",
                )
                .bind(&e.patch_id)
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(applied_at)
                .bind(new_version)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
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
            // RFC-025 Phase 2b.4 m4: run_costs projection (sqlite parity
            // with pg V065). Counter-semantic accumulate. See pg applier.
            RuntimeEvent::RunCostUpdated(e) => {
                let delta_cost = i64::try_from(e.delta_cost_micros).map_err(|_| {
                    StoreError::Internal(format!(
                        "RunCostUpdated.delta_cost_micros {} exceeds i64::MAX",
                        e.delta_cost_micros
                    ))
                })?;
                let delta_in = i64::try_from(e.delta_tokens_in).map_err(|_| {
                    StoreError::Internal(format!(
                        "RunCostUpdated.delta_tokens_in {} exceeds i64::MAX",
                        e.delta_tokens_in
                    ))
                })?;
                let delta_out = i64::try_from(e.delta_tokens_out).map_err(|_| {
                    StoreError::Internal(format!(
                        "RunCostUpdated.delta_tokens_out {} exceeds i64::MAX",
                        e.delta_tokens_out
                    ))
                })?;
                let updated_at = i64::try_from(e.updated_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "RunCostUpdated.updated_at_ms {} exceeds i64::MAX",
                        e.updated_at_ms
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO run_costs (
                        run_id, total_cost_micros, total_tokens_in,
                        total_tokens_out, provider_calls, updated_at_ms
                     ) VALUES (?, ?, ?, ?, 1, ?)
                     ON CONFLICT (run_id) DO UPDATE SET
                        total_cost_micros = run_costs.total_cost_micros + excluded.total_cost_micros,
                        total_tokens_in   = run_costs.total_tokens_in + excluded.total_tokens_in,
                        total_tokens_out  = run_costs.total_tokens_out + excluded.total_tokens_out,
                        provider_calls    = run_costs.provider_calls + 1,
                        updated_at_ms     = excluded.updated_at_ms",
                )
                .bind(e.run_id.as_str())
                .bind(delta_cost)
                .bind(delta_in)
                .bind(delta_out)
                .bind(updated_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // RFC-025 Phase 2b.4 m4: Ephemeral — see pg applier + registry.
            RuntimeEvent::SpendAlertTriggered(_) => {}
            // RFC-025 Phase 2b.2b m3: subagent_spawns projection (pg V053).
            // Parity with pg + in-memory: also UPDATEs the child
            // task's parent linkage on `tasks` (Gemini PR #593 review).
            RuntimeEvent::SubagentSpawned(e) => {
                sqlx::query(
                    "INSERT INTO subagent_spawns (
                        child_task_id, tenant_id, workspace_id, project_id,
                        parent_run_id, parent_task_id, child_session_id,
                        child_run_id, spawned_at_ms, goal, role
                     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(child_task_id) DO NOTHING",
                )
                .bind(e.child_task_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.parent_run_id.as_str())
                .bind(e.parent_task_id.as_ref().map(|t| t.as_str()))
                .bind(e.child_session_id.as_str())
                .bind(e.child_run_id.as_ref().map(|r| r.as_str()))
                .bind(now)
                // #670 G2: LLM delegation context — see pg applier and
                // `SubagentSpawned` in cairn-domain for replay semantics.
                .bind(e.goal.as_str())
                .bind(e.role.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;

                sqlx::query(
                    "UPDATE tasks
                        SET parent_run_id  = ?,
                            parent_task_id = ?,
                            version        = version + 1,
                            updated_at     = ?
                      WHERE task_id = ?",
                )
                .bind(e.parent_run_id.as_str())
                .bind(e.parent_task_id.as_ref().map(|t| t.as_str()))
                .bind(now)
                .bind(e.child_task_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
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
            // RFC-025 Phase 2b.2b m2: signal_ingestions projection (pg V052).
            RuntimeEvent::SignalIngested(e) => {
                let timestamp_ms = i64::try_from(e.timestamp_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "SignalIngested.timestamp_ms {} exceeds i64::MAX",
                        e.timestamp_ms
                    ))
                })?;
                let payload_json = serde_json::to_string(&e.payload).map_err(|err| {
                    StoreError::Serialization(format!("SignalIngested.payload JSON encode: {err}"))
                })?;
                sqlx::query(
                    "INSERT INTO signal_ingestions (
                        signal_id, tenant_id, workspace_id, project_id,
                        source, payload_json, timestamp_ms
                     ) VALUES (?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(signal_id) DO NOTHING",
                )
                .bind(e.signal_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(&e.source)
                .bind(&payload_json)
                .bind(timestamp_ms)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // RFC-025 Phase 2b.2b m4: user_messages projection (pg V054).
            RuntimeEvent::UserMessageAppended(e) => {
                let appended_at = i64::try_from(e.appended_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "UserMessageAppended.appended_at_ms {} exceeds i64::MAX",
                        e.appended_at_ms
                    ))
                })?;
                let sequence = i64::try_from(e.sequence).map_err(|_| {
                    StoreError::Internal(format!(
                        "UserMessageAppended.sequence {} exceeds i64::MAX",
                        e.sequence
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO user_messages (
                        run_id, sequence, tenant_id, workspace_id, project_id,
                        session_id, event_id, content, appended_at_ms
                     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(run_id, sequence) DO NOTHING",
                )
                .bind(e.run_id.as_str())
                .bind(sequence)
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.session_id.as_str())
                .bind(envelope.event_id.as_str())
                .bind(&e.content)
                .bind(appended_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // RFC-025 Phase 2b.3 m1: ingest_jobs parity with pg V057.
            // Started inserts the initial row; Completed updates the
            // existing row with terminal state + error_message. Both
            // keyed on `job_id`.
            RuntimeEvent::IngestJobStarted(e) => {
                let document_count = i32::try_from(e.document_count).map_err(|_| {
                    StoreError::Internal(format!(
                        "IngestJobStarted.document_count {} exceeds i32::MAX",
                        e.document_count
                    ))
                })?;
                let started_at = i64::try_from(e.started_at).map_err(|_| {
                    StoreError::Internal(format!(
                        "IngestJobStarted.started_at {} exceeds i64::MAX",
                        e.started_at
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO ingest_jobs (
                        job_id, tenant_id, workspace_id, project_id,
                        source_id, document_count, state, error_message,
                        created_at_ms, updated_at_ms
                     ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
                     ON CONFLICT(job_id) DO NOTHING",
                )
                .bind(e.job_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.source_id.as_ref().map(|s| s.as_str()))
                .bind(document_count)
                .bind(crate::projections::ingest_job_state_str(
                    cairn_domain::IngestJobState::Processing,
                ))
                .bind(started_at)
                .bind(started_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::IngestJobCompleted(e) => {
                let completed_at = i64::try_from(e.completed_at).map_err(|_| {
                    StoreError::Internal(format!(
                        "IngestJobCompleted.completed_at {} exceeds i64::MAX",
                        e.completed_at
                    ))
                })?;
                let new_state = crate::projections::ingest_job_state_str(if e.success {
                    cairn_domain::IngestJobState::Completed
                } else {
                    cairn_domain::IngestJobState::Failed
                });
                sqlx::query(
                    "UPDATE ingest_jobs
                     SET state         = ?2,
                         error_message = ?3,
                         updated_at_ms = ?4
                     WHERE job_id = ?1",
                )
                .bind(e.job_id.as_str())
                .bind(new_state)
                .bind(e.error_message.as_deref())
                .bind(completed_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
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
            // RFC 026 PR-A2: tenant PATCH edit. Same shape as pg applier
            // above — `COALESCE(?2, name)` leaves the column untouched
            // when the event's `name` is `None`. `updated_at` always
            // advances so the admin UI can show a fresh mtime.
            RuntimeEvent::TenantUpdated(e) => {
                let updated_at = i64::try_from(e.updated_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "TenantUpdated.updated_at_ms {} exceeds i64::MAX",
                        e.updated_at_ms
                    ))
                })?;
                sqlx::query(
                    "UPDATE tenants SET
                        name       = COALESCE(?2, name),
                        updated_at = ?3
                     WHERE tenant_id = ?1",
                )
                .bind(e.tenant_id.as_str())
                .bind(e.name.as_deref())
                .bind(updated_at)
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
            // RFC-025 Phase 2b.1 m3: outcomes parity with pg.
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
                     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            // RFC-025 Phase 2b.1 m2: scheduled_tasks parity with pg.
            RuntimeEvent::ScheduledTaskCreated(e) => {
                let created_at = i64::try_from(e.created_at).map_err(|_| {
                    StoreError::Internal(format!(
                        "ScheduledTaskCreated.created_at {} exceeds i64::MAX",
                        e.created_at
                    ))
                })?;
                let next_run_at = e.next_run_at.map(i64::try_from).transpose().map_err(|_| {
                    StoreError::Internal("ScheduledTaskCreated.next_run_at exceeds i64::MAX".into())
                })?;
                sqlx::query(
                    "INSERT INTO scheduled_tasks (
                        scheduled_task_id, tenant_id, name, cron_expression,
                        last_run_at, next_run_at, enabled, created_at, updated_at
                     ) VALUES (?, ?, ?, ?, NULL, ?, 1, ?, ?)
                     ON CONFLICT (scheduled_task_id) DO NOTHING",
                )
                .bind(e.scheduled_task_id.as_str())
                .bind(e.tenant_id.as_str())
                .bind(&e.name)
                .bind(&e.cron_expression)
                .bind(next_run_at)
                .bind(created_at)
                .bind(created_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // RFC-025 Phase 2b.1 m4: plan_reviews parity with pg.
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
                     ) VALUES (?, ?, ?, ?, ?, ?, 'proposed', ?)
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
                sqlx::query(
                    "UPDATE plan_reviews
                     SET state             = 'approved',
                         resolved_by       = ?,
                         resolved_at       = ?,
                         reviewer_comments = ?
                     WHERE plan_run_id = ? AND state = 'proposed'",
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
                         resolved_by      = ?,
                         resolved_at      = ?,
                         rejection_reason = ?
                     WHERE plan_run_id = ? AND state = 'proposed'",
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
                         resolved_at       = ?,
                         reviewer_comments = ?,
                         revision_run_id   = ?
                     WHERE plan_run_id = ? AND state = 'proposed'",
                )
                .bind(requested_at)
                .bind(&e.reviewer_comments)
                .bind(e.new_plan_run_id.as_str())
                .bind(e.original_plan_run_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
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
            // RFC-025 Phase 2b.3 m3: channels + channel_messages parity
            // with pg V059.
            RuntimeEvent::ChannelCreated(e) => {
                let capacity = i32::try_from(e.capacity).map_err(|_| {
                    StoreError::Internal(format!(
                        "ChannelCreated.capacity {} exceeds i32::MAX",
                        e.capacity
                    ))
                })?;
                let created_at = i64::try_from(e.created_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "ChannelCreated.created_at_ms {} exceeds i64::MAX",
                        e.created_at_ms
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO channels (
                        channel_id, tenant_id, workspace_id, project_id,
                        name, capacity, created_at_ms, updated_at_ms
                     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(channel_id) DO NOTHING",
                )
                .bind(e.channel_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(&e.name)
                .bind(capacity)
                .bind(created_at)
                .bind(created_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::ChannelMessageSent(e) => {
                let sent_at = i64::try_from(e.sent_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "ChannelMessageSent.sent_at_ms {} exceeds i64::MAX",
                        e.sent_at_ms
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO channel_messages (
                        channel_id, message_id, sender_id, body, sent_at_ms,
                        consumed_by, consumed_at_ms
                     ) VALUES (?, ?, ?, ?, ?, NULL, NULL)
                     ON CONFLICT(channel_id, message_id) DO NOTHING",
                )
                .bind(e.channel_id.as_str())
                .bind(&e.message_id)
                .bind(&e.sender_id)
                .bind(&e.body)
                .bind(sent_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::ChannelMessageConsumed(e) => {
                let consumed_at = i64::try_from(e.consumed_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "ChannelMessageConsumed.consumed_at_ms {} exceeds i64::MAX",
                        e.consumed_at_ms
                    ))
                })?;
                sqlx::query(
                    "UPDATE channel_messages
                     SET consumed_by    = ?3,
                         consumed_at_ms = ?4
                     WHERE channel_id = ?1 AND message_id = ?2",
                )
                .bind(e.channel_id.as_str())
                .bind(&e.message_id)
                .bind(&e.consumed_by)
                .bind(consumed_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // RFC-025 Phase 2b.3 m2: default_settings parity with pg V058.
            RuntimeEvent::DefaultSettingSet(e) => {
                let value_json = serde_json::to_string(&e.value)
                    .map_err(|err| StoreError::Serialization(err.to_string()))?;
                sqlx::query(
                    "INSERT INTO default_settings (scope, scope_id, key, value_json)
                     VALUES (?, ?, ?, ?)
                     ON CONFLICT(scope, scope_id, key) DO UPDATE SET
                         value_json = excluded.value_json",
                )
                .bind(crate::projections::defaults_scope_str(e.scope))
                .bind(&e.scope_id)
                .bind(&e.key)
                .bind(value_json)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::DefaultSettingCleared(e) => {
                sqlx::query(
                    "DELETE FROM default_settings
                     WHERE scope = ?1 AND scope_id = ?2 AND key = ?3",
                )
                .bind(crate::projections::defaults_scope_str(e.scope))
                .bind(&e.scope_id)
                .bind(&e.key)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
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
            // RFC-025 Phase 2a.2 milestone 4: entitlement_overrides projection.
            // Parity with pg — same ON CONFLICT DO UPDATE semantics on the
            // composite (tenant_id, feature) key. `allowed` binds as bool
            // (sqlx maps to INTEGER 0/1 on sqlite).
            RuntimeEvent::EntitlementOverrideSet(e) => {
                let set_at_ms = i64::try_from(e.set_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "EntitlementOverrideSet.set_at_ms {} exceeds i64::MAX",
                        e.set_at_ms
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO entitlement_overrides (
                        tenant_id, feature, allowed, reason, set_at_ms,
                        created_at, updated_at
                     ) VALUES (?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(tenant_id, feature) DO UPDATE SET
                        allowed    = excluded.allowed,
                        reason     = excluded.reason,
                        set_at_ms  = excluded.set_at_ms,
                        updated_at = excluded.updated_at",
                )
                .bind(e.tenant_id.as_str())
                .bind(e.feature.as_str())
                .bind(e.allowed)
                .bind(e.reason.as_deref())
                .bind(set_at_ms)
                .bind(now)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // RFC-025 Phase 2b.3 m4: notification_preferences +
            // notifications parity with pg V060.
            RuntimeEvent::NotificationPreferenceSet(e) => {
                let event_types_json = serde_json::to_string(&e.event_types)
                    .map_err(|err| StoreError::Serialization(err.to_string()))?;
                let channels_json = serde_json::to_string(&e.channels)
                    .map_err(|err| StoreError::Serialization(err.to_string()))?;
                let set_at = i64::try_from(e.set_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "NotificationPreferenceSet.set_at_ms {} exceeds i64::MAX",
                        e.set_at_ms
                    ))
                })?;
                let pref_id = format!("{}:{}", e.tenant_id.as_str(), e.operator_id);
                sqlx::query(
                    "INSERT INTO notification_preferences (
                        tenant_id, operator_id, pref_id,
                        event_types_json, channels_json, set_at_ms
                     ) VALUES (?, ?, ?, ?, ?, ?)
                     ON CONFLICT(tenant_id, operator_id) DO UPDATE SET
                         pref_id          = excluded.pref_id,
                         event_types_json = excluded.event_types_json,
                         channels_json    = excluded.channels_json,
                         set_at_ms        = excluded.set_at_ms",
                )
                .bind(e.tenant_id.as_str())
                .bind(&e.operator_id)
                .bind(&pref_id)
                .bind(event_types_json)
                .bind(channels_json)
                .bind(set_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::NotificationSent(e) => {
                let payload_json = serde_json::to_string(&e.payload)
                    .map_err(|err| StoreError::Serialization(err.to_string()))?;
                let sent_at = i64::try_from(e.sent_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "NotificationSent.sent_at_ms {} exceeds i64::MAX",
                        e.sent_at_ms
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO notifications (
                        record_id, tenant_id, operator_id, event_type,
                        channel_kind, channel_target, payload_json, sent_at_ms,
                        delivered, delivery_error
                     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(record_id) DO NOTHING",
                )
                .bind(&e.record_id)
                .bind(e.tenant_id.as_str())
                .bind(&e.operator_id)
                .bind(&e.event_type)
                .bind(&e.channel_kind)
                .bind(&e.channel_target)
                .bind(payload_json)
                .bind(sent_at)
                .bind(if e.delivered { 1_i64 } else { 0_i64 })
                .bind(e.delivery_error.as_deref())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // RFC-025 Phase 2b.4: provider pools are Ephemeral — live
            // HTTP-client state rebuilt from provider_bindings at boot.
            // See `crate::projection_registry` for the rationale.
            RuntimeEvent::ProviderPoolCreated(_)
            | RuntimeEvent::ProviderPoolConnectionAdded(_)
            | RuntimeEvent::ProviderPoolConnectionRemoved(_) => {}
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
            // RFC-025 Phase 2a.2 milestone 3: retention_policies projection.
            // Parity with pg — same ON CONFLICT DO UPDATE semantics and
            // checked-cast discipline.
            RuntimeEvent::RetentionPolicySet(e) => {
                let full_history_days = i32::try_from(e.full_history_days).map_err(|_| {
                    StoreError::Internal(format!(
                        "RetentionPolicySet.full_history_days {} exceeds i32::MAX",
                        e.full_history_days
                    ))
                })?;
                let current_state_days = i32::try_from(e.current_state_days).map_err(|_| {
                    StoreError::Internal(format!(
                        "RetentionPolicySet.current_state_days {} exceeds i32::MAX",
                        e.current_state_days
                    ))
                })?;
                let max_events = match e.max_events_per_entity {
                    Some(v) => Some(i64::try_from(v).map_err(|_| {
                        StoreError::Internal(format!(
                            "RetentionPolicySet.max_events_per_entity {} exceeds i64::MAX",
                            v
                        ))
                    })?),
                    None => None,
                };
                sqlx::query(
                    "INSERT INTO retention_policies (
                        tenant_id, policy_id, full_history_days, current_state_days,
                        max_events_per_entity, created_at, updated_at
                     ) VALUES (?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(tenant_id) DO UPDATE SET
                        policy_id              = excluded.policy_id,
                        full_history_days      = excluded.full_history_days,
                        current_state_days     = excluded.current_state_days,
                        max_events_per_entity  = excluded.max_events_per_entity,
                        updated_at             = excluded.updated_at",
                )
                .bind(e.tenant_id.as_str())
                .bind(e.policy_id.as_str())
                .bind(full_history_days)
                .bind(current_state_days)
                .bind(max_events)
                .bind(now)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // RFC-025 Phase 2b.4 m4: run_cost_alerts projection (sqlite
            // parity with pg V065). See pg applier for rearm-on-set +
            // update-in-place-on-trigger semantics.
            RuntimeEvent::RunCostAlertSet(e) => {
                let threshold = i64::try_from(e.threshold_micros).map_err(|_| {
                    StoreError::Internal(format!(
                        "RunCostAlertSet.threshold_micros {} exceeds i64::MAX",
                        e.threshold_micros
                    ))
                })?;
                let set_at = i64::try_from(e.set_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "RunCostAlertSet.set_at_ms {} exceeds i64::MAX",
                        e.set_at_ms
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO run_cost_alerts (
                        run_id, tenant_id, threshold_micros,
                        triggered_at_ms, actual_cost_micros, set_at_ms
                     ) VALUES (?, ?, ?, 0, 0, ?)
                     ON CONFLICT (run_id) DO UPDATE SET
                        tenant_id           = excluded.tenant_id,
                        threshold_micros    = excluded.threshold_micros,
                        triggered_at_ms     = 0,
                        actual_cost_micros  = 0,
                        set_at_ms           = excluded.set_at_ms",
                )
                .bind(e.run_id.as_str())
                .bind(e.tenant_id.as_str())
                .bind(threshold)
                .bind(set_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::RunCostAlertTriggered(e) => {
                let actual = i64::try_from(e.actual_cost_micros).map_err(|_| {
                    StoreError::Internal(format!(
                        "RunCostAlertTriggered.actual_cost_micros {} exceeds i64::MAX",
                        e.actual_cost_micros
                    ))
                })?;
                let triggered_at = i64::try_from(e.triggered_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "RunCostAlertTriggered.triggered_at_ms {} exceeds i64::MAX",
                        e.triggered_at_ms
                    ))
                })?;
                sqlx::query(
                    "UPDATE run_cost_alerts SET
                        triggered_at_ms    = ?2,
                        actual_cost_micros = ?3
                     WHERE run_id = ?1",
                )
                .bind(e.run_id.as_str())
                .bind(triggered_at)
                .bind(actual)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
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
            // RFC-025 Phase 2a.2 milestone 1: approval_delegations audit
            // projection. Parity with pg — PK (approval_id, delegation_id).
            // Copilot #571 round 4.
            RuntimeEvent::ApprovalDelegated(e) => {
                let delegated_at = i64::try_from(e.delegated_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "ApprovalDelegated.delegated_at_ms {} exceeds i64::MAX",
                        e.delegated_at_ms
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO approval_delegations (
                        approval_id, delegation_id, delegated_to, delegated_at_ms, created_at
                     ) VALUES (?, ?, ?, ?, ?)
                     ON CONFLICT(approval_id, delegation_id) DO NOTHING",
                )
                .bind(e.approval_id.as_str())
                .bind(e.delegation_id.as_str())
                .bind(e.delegated_to.as_str())
                .bind(delegated_at)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // RFC-025 Phase 2b.1: audit_log_entries parity with pg. Same
            // ON CONFLICT DO NOTHING idempotency contract; `metadata_json`
            // defaults to '{}' because `AuditLogEntryRecorded` does not
            // carry metadata on the wire (non-Eq `serde_json::Value`).
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
                     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
            // RFC-025 Phase 2b.3 m5: checkpoint_strategies parity with
            // pg V061.
            RuntimeEvent::CheckpointStrategySet(e) => {
                let Some(run_id) = e.run_id.as_ref() else {
                    return Ok(());
                };
                let max_checkpoints = if e.max_checkpoints > 0 {
                    e.max_checkpoints
                } else {
                    crate::projections::CHECKPOINT_STRATEGY_DEFAULT_MAX_CHECKPOINTS
                };
                let max_checkpoints = i32::try_from(max_checkpoints).map_err(|_| {
                    StoreError::Internal(format!(
                        "CheckpointStrategySet.max_checkpoints {max_checkpoints} exceeds i32::MAX"
                    ))
                })?;
                let interval_ms = i64::try_from(e.interval_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "CheckpointStrategySet.interval_ms {} exceeds i64::MAX",
                        e.interval_ms
                    ))
                })?;
                let set_at = i64::try_from(e.set_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "CheckpointStrategySet.set_at_ms {} exceeds i64::MAX",
                        e.set_at_ms
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO checkpoint_strategies (
                        run_id, strategy_id, interval_ms, max_checkpoints,
                        trigger_on_task_complete, set_at_ms
                     ) VALUES (?, ?, ?, ?, ?, ?)
                     ON CONFLICT(run_id) DO UPDATE SET
                         strategy_id              = excluded.strategy_id,
                         interval_ms              = excluded.interval_ms,
                         max_checkpoints          = excluded.max_checkpoints,
                         trigger_on_task_complete = excluded.trigger_on_task_complete,
                         set_at_ms                = excluded.set_at_ms",
                )
                .bind(run_id.as_str())
                .bind(&e.strategy_id)
                .bind(interval_ms)
                .bind(max_checkpoints)
                .bind(if e.trigger_on_task_complete {
                    1_i64
                } else {
                    0_i64
                })
                .bind(set_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
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
            // RFC-025 Phase 2b.4 m2: eval catalog projections (sqlite
            // parity with pg V063). See pg applier for the full
            // rationale + replay-semantics breakdown.
            RuntimeEvent::EvalDatasetCreated(e) => {
                let created_at = i64::try_from(e.created_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "EvalDatasetCreated.created_at_ms {} exceeds i64::MAX",
                        e.created_at_ms
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO eval_datasets (
                        dataset_id, tenant_id, name, subject_kind, created_at_ms
                     ) VALUES (?, '', ?, 'prompt_release', ?)
                     ON CONFLICT (dataset_id) DO NOTHING",
                )
                .bind(&e.dataset_id)
                .bind(&e.name)
                .bind(created_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::EvalDatasetEntryAdded(e) => {
                let added_at = i64::try_from(e.added_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "EvalDatasetEntryAdded.added_at_ms {} exceeds i64::MAX",
                        e.added_at_ms
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO eval_dataset_entries (
                        dataset_id, entry_id, added_at_ms
                     ) VALUES (?, ?, ?)
                     ON CONFLICT (dataset_id, entry_id) DO NOTHING",
                )
                .bind(&e.dataset_id)
                .bind(&e.entry_id)
                .bind(added_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::EvalRubricCreated(e) => {
                let created_at = i64::try_from(e.created_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "EvalRubricCreated.created_at_ms {} exceeds i64::MAX",
                        e.created_at_ms
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO eval_rubrics (
                        rubric_id, tenant_id, name, dimensions_json, created_at_ms
                     ) VALUES (?, '', ?, '[]', ?)
                     ON CONFLICT (rubric_id) DO NOTHING",
                )
                .bind(&e.rubric_id)
                .bind(&e.name)
                .bind(created_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::EvalBaselineSet(e) => {
                let set_at = i64::try_from(e.set_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "EvalBaselineSet.set_at_ms {} exceeds i64::MAX",
                        e.set_at_ms
                    ))
                })?;
                let display_name = format!("{}[{}={}]", e.baseline_id, e.metric, e.value);
                sqlx::query(
                    "INSERT INTO eval_baselines (
                        baseline_id, tenant_id, name, prompt_asset_id,
                        metrics_json, created_at_ms, locked
                     ) VALUES (?, '', ?, '', '{}', ?, 0)
                     ON CONFLICT (baseline_id) DO UPDATE SET
                        name = excluded.name
                     WHERE eval_baselines.locked = 0",
                )
                .bind(&e.baseline_id)
                .bind(&display_name)
                .bind(set_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::EvalBaselineLocked(e) => {
                sqlx::query(
                    "UPDATE eval_baselines SET locked = 1
                     WHERE baseline_id = ?",
                )
                .bind(&e.baseline_id)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // RFC-025 Phase 2b.2b m6: Ephemeral — see pg applier + registry.
            RuntimeEvent::EventLogCompacted(_) => {}
            // RFC-025 Phase 2a.2 milestone 2: guardrail_policies projection.
            // Parity with pg — same ON CONFLICT DO UPDATE semantics.
            // `enabled` maps BOOL → INTEGER 0/1 per the sqlx convention.
            RuntimeEvent::GuardrailPolicyCreated(e) => {
                let rules_json = serde_json::to_string(&e.rules).map_err(|err| {
                    StoreError::Serialization(format!(
                        "GuardrailPolicyCreated.rules serialize: {err}"
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO guardrail_policies (
                        policy_id, tenant_id, name, rules_json, enabled, created_at, updated_at
                     ) VALUES (?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(policy_id) DO UPDATE SET
                        tenant_id   = excluded.tenant_id,
                        name        = excluded.name,
                        rules_json  = excluded.rules_json,
                        enabled     = excluded.enabled,
                        updated_at  = excluded.updated_at",
                )
                .bind(e.policy_id.as_str())
                .bind(e.tenant_id.as_str())
                .bind(e.name.as_str())
                .bind(&rules_json)
                // `enabled = true` on `GuardrailPolicyCreated` mirrors
                // the in-memory applier. Bind as bool so sqlx handles
                // the INTEGER 0/1 storage mapping; the matching ON
                // CONFLICT DO UPDATE now resets enabled on replay to
                // keep pg/sqlite/in-memory byte-equal for refreshed
                // policies (Gemini review on #571).
                .bind(true)
                .bind(now)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::GuardrailPolicyEvaluated(e) => {
                let evaluated_at = i64::try_from(e.evaluated_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "GuardrailPolicyEvaluated.evaluated_at_ms {} exceeds i64::MAX",
                        e.evaluated_at_ms
                    ))
                })?;
                let subject_type = guardrail_subject_type_str(e.subject_type);
                let decision = guardrail_decision_kind_str(e.decision);
                let subject_id = e.subject_id.clone().unwrap_or_default();
                sqlx::query(
                    "INSERT INTO guardrail_evaluations (
                        policy_id, tenant_id, subject_type, subject_id,
                        action, decision, reason, evaluated_at_ms, created_at
                     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(tenant_id, policy_id, subject_type, subject_id, action, evaluated_at_ms)
                       DO NOTHING",
                )
                .bind(e.policy_id.as_str())
                .bind(e.tenant_id.as_str())
                .bind(subject_type)
                .bind(&subject_id)
                .bind(e.action.as_str())
                .bind(decision)
                .bind(e.reason.as_deref())
                .bind(evaluated_at)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // RFC-025 Phase 2b.4 m3: Ephemeral — see pg applier + registry.
            RuntimeEvent::OperatorIntervention(_) => {}
            // RFC-025 Phase 2b.4 m3: operator_profiles projection
            // (sqlite parity with pg V064). See pg applier for the
            // per-event rationale; ? placeholders + EXCLUDED replaced
            // by `excluded` in the ON CONFLICT UPDATE clause.
            RuntimeEvent::OperatorProfileCreated(e) => {
                // Propagate serialization errors — see pg applier.
                // Copilot PR #596 review.
                let role = enum_to_str(&e.role)?;
                sqlx::query(
                    "INSERT INTO operator_profiles (
                        operator_id, tenant_id, display_name, email, role,
                        created_at_ms
                     ) VALUES (?, ?, ?, ?, ?, ?)
                     ON CONFLICT (operator_id) DO UPDATE SET
                        tenant_id     = excluded.tenant_id,
                        display_name  = excluded.display_name,
                        email         = excluded.email,
                        role          = excluded.role,
                        created_at_ms = excluded.created_at_ms",
                )
                .bind(e.profile_id.as_str())
                .bind(e.tenant_id.as_str())
                .bind(&e.display_name)
                .bind(&e.email)
                .bind(&role)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::OperatorProfileUpdated(e) => {
                // RFC 026 PR-A2: role edit. Same COALESCE pattern as
                // the pg applier above — pre-A2 events deserialize
                // with `role=None` so the column is untouched.
                let role_str = e.role.as_ref().map(|r| {
                    serde_json::to_string(r)
                        .unwrap_or_default()
                        .trim_matches('"')
                        .to_owned()
                });
                sqlx::query(
                    "UPDATE operator_profiles SET
                        display_name = COALESCE(?2, display_name),
                        email        = COALESCE(?3, email),
                        role         = COALESCE(?4, role)
                     WHERE operator_id = ?1",
                )
                .bind(e.profile_id.as_str())
                .bind(e.display_name.as_deref())
                .bind(e.email.as_deref())
                .bind(role_str.as_deref())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // RFC 026 PR-A0: operator_tenant_roles projection (sqlite
            // parity with pg V066). See pg applier for per-event
            // rationale; the `excluded` pseudo-table replaces pg's
            // `EXCLUDED` in the ON CONFLICT clause.
            RuntimeEvent::TenantRoleGranted(e) => {
                let role = enum_to_str(&e.role)?;
                let granted_at = i64::try_from(e.at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "TenantRoleGranted.at_ms {} exceeds i64::MAX",
                        e.at_ms
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO operator_tenant_roles (
                        tenant_id, operator_id, role, granted_at_ms, granted_by,
                        revoked_at_ms, revoked_by
                     ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL)
                     ON CONFLICT (tenant_id, operator_id) DO UPDATE SET
                        role          = excluded.role,
                        granted_at_ms = excluded.granted_at_ms,
                        granted_by    = excluded.granted_by,
                        revoked_at_ms = NULL,
                        revoked_by    = NULL",
                )
                .bind(e.tenant_id.as_str())
                .bind(e.operator_id.as_str())
                .bind(&role)
                .bind(granted_at)
                .bind(&e.granted_by)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::TenantRoleRevoked(e) => {
                let revoked_at = i64::try_from(e.at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "TenantRoleRevoked.at_ms {} exceeds i64::MAX",
                        e.at_ms
                    ))
                })?;
                sqlx::query(
                    "UPDATE operator_tenant_roles SET
                        revoked_at_ms = ?3,
                        revoked_by    = ?4
                     WHERE tenant_id = ?1 AND operator_id = ?2",
                )
                .bind(e.tenant_id.as_str())
                .bind(e.operator_id.as_str())
                .bind(revoked_at)
                .bind(&e.revoked_by)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // PR #595 (issue #592): `PauseScheduled` is Projected via the
            // `RunStateChanged` → `pause_schedules` arm above; explicit
            // no-op keeps the projection-stub-guard CI job green.
            RuntimeEvent::PauseScheduled(_) => {}
            // Durable audit event: the event log itself is the projection.
            // Readers filter `list_events()` by variant — no derived table.
            // See projection_registry.rs entry; reclassified Ephemeral in #574.
            RuntimeEvent::PermissionDecisionRecorded(_) => {}
            // RFC-025 Phase 3: provider_bindings projection (sqlite
            // parity with pg). Keep the ON CONFLICT semantics symmetric
            // with pg — replaying `ProviderBindingCreated` after a later
            // `ProviderBindingStateChanged` must NOT overwrite `active`
            // back to the created/default flag. SQLite's `excluded`
            // pseudo-table behaves the same as pg's.
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
                     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(provider_binding_id) DO UPDATE SET
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
                sqlx::query(
                    "UPDATE provider_bindings
                     SET active = ?
                     WHERE provider_binding_id = ?",
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
            // RFC-025 Phase 3: provider_connections projection (sqlite
            // parity with pg). Upsert-on-id so replay is safe.
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
                     ) VALUES (?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(provider_connection_id) DO UPDATE SET
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
                // Hard-delete so the connection id can be re-used. F40.
                sqlx::query("DELETE FROM provider_connections WHERE provider_connection_id = ?")
                    .bind(e.provider_connection_id.as_str())
                    .execute(&mut **tx)
                    .await
                    .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            // RFC-025 Phase 2b.4: provider health / model / retry events
            // are Ephemeral (sqlite parity with pg). See
            // `crate::projection_registry` for per-variant rationale.
            RuntimeEvent::ProviderHealthChecked(_)
            | RuntimeEvent::ProviderHealthScheduleSet(_)
            | RuntimeEvent::ProviderHealthScheduleTriggered(_)
            | RuntimeEvent::ProviderMarkedDegraded(_)
            | RuntimeEvent::ProviderModelRegistered(_)
            | RuntimeEvent::ProviderRecovered(_)
            | RuntimeEvent::ProviderRetryPolicySet(_) => {}
            // RFC-025 Phase 2b.2b m6: Ephemeral — see pg applier + registry.
            RuntimeEvent::RecoveryEscalated(_) => {}
            // RFC-025 Phase 2b.2b m1: resource_shares projection (pg V051).
            // Parity with pg; permissions stored as JSON string.
            RuntimeEvent::ResourceShared(e) => {
                let shared_at = i64::try_from(e.shared_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "ResourceShared.shared_at_ms {} exceeds i64::MAX",
                        e.shared_at_ms
                    ))
                })?;
                let permissions_json = serde_json::to_string(&e.permissions).map_err(|err| {
                    StoreError::Serialization(format!(
                        "ResourceShared.permissions JSON encode: {err}"
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO resource_shares (
                        share_id, tenant_id, source_workspace_id, target_workspace_id,
                        resource_type, resource_id, permissions_json, shared_at_ms
                     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(share_id) DO NOTHING",
                )
                .bind(&e.share_id)
                .bind(e.tenant_id.as_str())
                .bind(e.source_workspace_id.as_str())
                .bind(e.target_workspace_id.as_str())
                .bind(&e.resource_type)
                .bind(&e.resource_id)
                .bind(&permissions_json)
                .bind(shared_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::ResourceShareRevoked(e) => {
                sqlx::query("DELETE FROM resource_shares WHERE share_id = ?")
                    .bind(&e.share_id)
                    .execute(&mut **tx)
                    .await
                    .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
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
            // RFC-025 Phase 2b.4 m4: route_policies.updated_at bump.
            // Matches pg applier + in-memory `if let Some(p)` guard:
            // missing row is silently ignored.
            RuntimeEvent::RoutePolicyUpdated(e) => {
                let updated_at = i64::try_from(e.updated_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "RoutePolicyUpdated.updated_at_ms {} exceeds i64::MAX",
                        e.updated_at_ms
                    ))
                })?;
                sqlx::query(
                    "UPDATE route_policies SET updated_at = ?2
                     WHERE policy_id = ?1",
                )
                .bind(&e.policy_id)
                .bind(updated_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::RunSlaBreached(_) => log_stub("RunSlaBreached"),
            RuntimeEvent::RunSlaSet(_) => log_stub("RunSlaSet"),
            RuntimeEvent::SignalRouted(_) => log_stub("SignalRouted"),
            RuntimeEvent::SignalSubscriptionCreated(_) => log_stub("SignalSubscriptionCreated"),
            // ── RFC-025 Phase 1.5a: trigger + run_template + trigger_fires ─────
            // Parity with pg/projections.rs + V035 migration. Every
            // lifecycle edge mutates `triggers` / `run_templates`; every
            // audit edge INSERTs into `trigger_fires`. Portable-SQL-only
            // per feedback_no_db_specific_features.md — JSON-in-TEXT for
            // conditions / allowlists / required_fields / metadata.
            RuntimeEvent::TriggerCreated(e) => {
                sqlx::query(
                    "INSERT INTO triggers
                         (trigger_id, tenant_id, workspace_id, project_id,
                          name, description, signal_type, plugin_id,
                          conditions_json, run_template_id,
                          state, state_reason, suspension_reason, state_since,
                          max_per_minute, max_burst, max_chain_depth,
                          created_by, created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                             'enabled', NULL, NULL, NULL,
                             ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(trigger_id) DO NOTHING",
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
                .bind(e.max_chain_depth as i64)
                .bind(e.created_by.as_str())
                .bind(e.created_at as i64)
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
                         updated_at = ?
                     WHERE trigger_id = ?",
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
                         state_reason = ?,
                         suspension_reason = NULL,
                         state_since = ?,
                         updated_at = ?
                     WHERE trigger_id = ?",
                )
                .bind(e.reason.as_deref())
                .bind(e.at as i64)
                .bind(e.at as i64)
                .bind(e.trigger_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::TriggerSuspended(e) => {
                // Short-name discriminant (PR #569 review) — parity
                // with pg + in-memory.
                let reason_str =
                    crate::projections::trigger::suspension_reason_discriminant(&e.reason);
                sqlx::query(
                    "UPDATE triggers
                     SET state = 'suspended',
                         state_reason = NULL,
                         suspension_reason = ?,
                         state_since = ?,
                         updated_at = ?
                     WHERE trigger_id = ?",
                )
                .bind(reason_str)
                .bind(e.at as i64)
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
                         updated_at = ?
                     WHERE trigger_id = ?",
                )
                .bind(e.at as i64)
                .bind(e.trigger_id.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::TriggerDeleted(e) => {
                sqlx::query("DELETE FROM triggers WHERE trigger_id = ?")
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
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                             ?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(template_id) DO NOTHING",
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
                .bind(e.created_at as i64)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
            RuntimeEvent::RunTemplateDeleted(e) => {
                sqlx::query("DELETE FROM run_templates WHERE template_id = ?")
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
                insert_trigger_fire_sqlite(
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
                // Short-name discriminant + optional field metadata (PR
                // #569 review) — parity with pg + in-memory.
                let reason_str = crate::projections::trigger::skip_reason_discriminant(&e.reason);
                let field = if let cairn_domain::events::TriggerSkipReason::MissingRequiredField {
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
                insert_trigger_fire_sqlite(
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
                insert_trigger_fire_sqlite(
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
                insert_trigger_fire_sqlite(
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
                insert_trigger_fire_sqlite(
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
            // RFC-025 Phase 2b.2b m6: tool_recovery_pauses projection (pg V056).
            RuntimeEvent::ToolRecoveryPaused(e) => {
                let paused_at = i64::try_from(e.paused_at_ms).map_err(|_| {
                    StoreError::Internal(format!(
                        "ToolRecoveryPaused.paused_at_ms {} exceeds i64::MAX",
                        e.paused_at_ms
                    ))
                })?;
                sqlx::query(
                    "INSERT INTO tool_recovery_pauses (
                        tool_call_id, tenant_id, workspace_id, project_id,
                        run_id, task_id, tool_name, reason, paused_at_ms
                     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(tool_call_id) DO NOTHING",
                )
                .bind(&e.tool_call_id)
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.run_id.as_str())
                .bind(e.task_id.as_ref().map(|t| t.as_str()))
                .bind(&e.tool_name)
                .bind(&e.reason)
                .bind(paused_at)
                .execute(&mut **tx)
                .await
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }
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
            RuntimeEvent::LlmCompletionRecorded(e) => {
                // Issue #668: persist the LLM round-trip body. Keyed
                // on `trace_id` (UNIQUE); re-application is a no-op
                // via ON CONFLICT DO NOTHING.
                sqlx::query(
                    "INSERT INTO llm_completions
                         (trace_id, tenant_id, workspace_id, project_id,
                          session_id, run_id, model_id,
                          system_prompt, messages_json,
                          response_text, tool_calls_json,
                          recorded_at_ms, created_at)
                     VALUES
                         (?, ?, ?, ?,
                          ?, ?, ?,
                          ?, ?,
                          ?, ?,
                          ?, ?)
                     ON CONFLICT(trace_id) DO NOTHING",
                )
                .bind(e.trace_id.as_str())
                .bind(e.project.tenant_id.as_str())
                .bind(e.project.workspace_id.as_str())
                .bind(e.project.project_id.as_str())
                .bind(e.session_id.as_str())
                .bind(e.run_id.as_ref().map(|r| r.as_str()))
                .bind(e.model_id.as_str())
                .bind(e.system_prompt.as_str())
                .bind(e.messages_json.as_str())
                .bind(e.response_text.as_str())
                .bind(e.tool_calls_json.as_str())
                .bind(e.recorded_at_ms as i64)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            }
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

/// RFC-025 Phase 1.5a: shared INSERT into `trigger_fires` for all five
/// audit variants on sqlite. Mirror of `insert_trigger_fire_pg`.
#[allow(clippy::too_many_arguments)]
async fn insert_trigger_fire_sqlite(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
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
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
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

/// RFC-025 Phase 2a.2 milestone 2: snake_case string for
/// `GuardrailSubjectType`. Parity with pg projections::guardrail_subject_type_str.
fn guardrail_subject_type_str(t: cairn_domain::policy::GuardrailSubjectType) -> &'static str {
    use cairn_domain::policy::GuardrailSubjectType as T;
    match t {
        T::Run => "run",
        T::Task => "task",
        T::Session => "session",
        T::Tool => "tool",
        T::Provider => "provider",
    }
}

/// RFC-025 Phase 2a.2 milestone 2: snake_case string for `GuardrailDecisionKind`.
fn guardrail_decision_kind_str(d: cairn_domain::policy::GuardrailDecisionKind) -> &'static str {
    use cairn_domain::policy::GuardrailDecisionKind as D;
    match d {
        D::Allowed => "allowed",
        D::Denied => "denied",
        D::Warned => "warned",
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
